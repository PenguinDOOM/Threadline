use super::*;

impl ThreadlineJobManager {
    pub fn start_command_json(&self, command: Vec<String>) -> Value {
        self.prune_expired();
        if let Err(error) = self.validate_command(&command) {
            return error;
        }

        let PendingJob {
            context,
            reservation,
        } = match self.admit_job("command") {
            Ok(pending) => pending,
            Err(error) => return error,
        };
        let job_id = context.job_id();
        if spawn_command_worker(move || {
            let mut reservation = reservation;
            let cleanup_confirmed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_command_job(context.clone(), command)
            }))
            .unwrap_or_else(|_| {
                context.fail(
                    "job_worker_cleanup_incomplete",
                    "Threadline could not confirm job worker cleanup.",
                );
                mark_cleanup_incomplete(&context);
                false
            });
            if !cleanup_confirmed {
                reservation.release_on_drop = false;
            }
            drop(reservation);
            #[cfg(test)]
            COMMAND_WORKER_COMPLETIONS.fetch_add(1, Ordering::SeqCst);
        })
        .is_err()
        {
            self.remove_entry(&job_id);
            return stable_error(
                "job_worker_spawn_failed",
                "Threadline could not start the job worker.",
            );
        }

        job_started_json(&job_id)
    }

    fn validate_command(&self, command: &[String]) -> Result<(), Value> {
        if !self.inner.config.jobs_enabled {
            return Err(stable_error(
                "jobs_disabled",
                "Threadline jobs are disabled.",
            ));
        }
        let Some(program) = command.first() else {
            return Err(stable_error(
                "invalid_job_request",
                "threadline_start_job requires a non-empty command array.",
            ));
        };
        if !self.command_allowed(program) {
            return Err(stable_error(
                "job_command_not_allowed",
                "The requested command is not allowed by the configured Threadline job policy.",
            ));
        }
        Ok(())
    }
}

pub(super) fn execute_command_job(context: ManagedJobContext, command: Vec<String>) -> bool {
    context.mark_running();

    let mut child = match spawn_command(&command) {
        Ok(child) => child,
        Err(error) => {
            context.fail(
                "job_command_spawn_failed",
                format!("Threadline could not start the requested command: {error}"),
            );
            return true;
        }
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let child = Arc::new(Mutex::new(child));
    context.attach_child(Arc::clone(&child));

    let (stdout_reader, stderr_reader) =
        match start_command_readers(&context, &child, stdout, stderr) {
            Ok(readers) => readers,
            Err(cleanup_confirmed) => return cleanup_confirmed,
        };
    let status = match observe_command(&context, &child) {
        Ok(status) => status,
        Err(error) => {
            return fail_command_after_cleanup(
                &context,
                &child,
                stdout_reader,
                stderr_reader,
                "job_command_failed",
                format!("Threadline could not observe the command status: {error}"),
            );
        }
    };

    let cleanup_confirmed = cleanup_command_resources(&child, stdout_reader, stderr_reader);
    context.clear_child();
    if !cleanup_confirmed {
        context.fail(
            "job_worker_cleanup_incomplete",
            "Threadline could not confirm job worker cleanup.",
        );
        mark_cleanup_incomplete(&context);
        return false;
    }
    publish_command_result(&context, command, status);
    true
}

type CommandReader = Option<thread::JoinHandle<()>>;

fn start_command_readers(
    context: &ManagedJobContext,
    child: &Arc<Mutex<Child>>,
    stdout: Option<std::process::ChildStdout>,
    stderr: Option<std::process::ChildStderr>,
) -> Result<(CommandReader, CommandReader), bool> {
    let stdout_reader =
        match stdout.map(|stdout| spawn_output_reader(stdout, context.clone(), "stdout")) {
            Some(Ok(reader)) => Some(reader),
            Some(Err(_)) => {
                return Err(fail_command_after_cleanup(
                    context,
                    child,
                    None,
                    None,
                    "job_output_reader_spawn_failed",
                    "Threadline could not start a job output reader.",
                ));
            }
            None => None,
        };
    let stderr_reader =
        match stderr.map(|stderr| spawn_output_reader(stderr, context.clone(), "stderr")) {
            Some(Ok(reader)) => Some(reader),
            Some(Err(_)) => {
                return Err(fail_command_after_cleanup(
                    context,
                    child,
                    stdout_reader,
                    None,
                    "job_output_reader_spawn_failed",
                    "Threadline could not start a job output reader.",
                ));
            }
            None => None,
        };
    Ok((stdout_reader, stderr_reader))
}

fn observe_command(
    context: &ManagedJobContext,
    child: &Arc<Mutex<Child>>,
) -> Result<std::process::ExitStatus, std::io::Error> {
    loop {
        if context.is_cancelled() {
            let _ = child.lock().expect("child lock").kill();
        }

        let observation = {
            let mut child = child.lock().expect("child lock");
            child.try_wait().and_then(inject_command_observation_error)
        };
        match observation {
            Ok(Some(status)) => return Ok(status),
            Ok(None) => thread::sleep(Duration::from_millis(10)),
            Err(error) => return Err(error),
        }
    }
}

fn fail_command_after_cleanup(
    context: &ManagedJobContext,
    child: &Arc<Mutex<Child>>,
    stdout_reader: CommandReader,
    stderr_reader: CommandReader,
    code: &'static str,
    message: impl Into<String>,
) -> bool {
    let cleanup_confirmed = cleanup_command_resources(child, stdout_reader, stderr_reader);
    context.clear_child();
    context.fail(code, message);
    if !cleanup_confirmed {
        mark_cleanup_incomplete(context);
    }
    cleanup_confirmed
}

fn publish_command_result(
    context: &ManagedJobContext,
    command: Vec<String>,
    status: std::process::ExitStatus,
) {
    if context.is_cancelled() {
        return;
    }

    if status.success() {
        context.complete(json!({
            "kind": "command",
            "command": command,
            "exit_code": status.code(),
            "success": true,
        }));
    } else {
        context.fail(
            "job_command_failed",
            format!(
                "The Threadline job command exited unsuccessfully with code {:?}.",
                status.code()
            ),
        );
    }
}

fn spawn_command(command: &[String]) -> Result<Child, std::io::Error> {
    let mut child = Command::new(&command[0]);
    if command.len() > 1 {
        child.args(&command[1..]);
    }

    child.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()
}

fn inject_command_observation_error(
    status: Option<std::process::ExitStatus>,
) -> Result<Option<std::process::ExitStatus>, std::io::Error> {
    #[cfg(test)]
    if FORCE_COMMAND_OBSERVATION_FAILURE.swap(false, Ordering::SeqCst) {
        return Err(std::io::Error::other(
            "injected command observation failure",
        ));
    }

    Ok(status)
}

fn spawn_output_reader<R>(
    reader: R,
    context: ManagedJobContext,
    stream: &'static str,
) -> Result<thread::JoinHandle<()>, std::io::Error>
where
    R: std::io::Read + Send + 'static,
{
    #[cfg(test)]
    {
        let attempt = OUTPUT_READER_SPAWN_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        if attempt == OUTPUT_READER_FAIL_ON_ATTEMPT.load(Ordering::SeqCst) {
            return Err(std::io::Error::other(
                "injected output reader spawn failure",
            ));
        }
    }
    thread::Builder::new()
        .name(format!("threadline-job-{stream}"))
        .spawn(move || {
            let mut reader = BufReader::new(reader);
            let mut pending = Vec::new();
            let mut buffer = [0u8; 1024];

            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read_bytes) => {
                        pending.extend_from_slice(&buffer[..read_bytes]);
                        flush_output_chunks(&context, stream, &mut pending);
                    }
                    Err(_) => break,
                }
            }

            if !pending.is_empty() {
                let text = String::from_utf8_lossy(&pending).to_string();
                push_stream_output(&context, stream, &text);
            }
            #[cfg(test)]
            COMMAND_READER_COMPLETIONS.fetch_add(1, Ordering::SeqCst);
        })
}

fn cleanup_command_resources(
    child: &Arc<Mutex<Child>>,
    stdout_reader: Option<thread::JoinHandle<()>>,
    stderr_reader: Option<thread::JoinHandle<()>>,
) -> bool {
    let child_reaped = {
        let mut child = child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        #[cfg(test)]
        COMMAND_CHILD_KILL_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        let _ = child.kill();
        let child_reaped = child.wait().is_ok();
        #[cfg(test)]
        COMMAND_CHILD_REAPED.store(child_reaped, Ordering::SeqCst);
        child_reaped
    };
    let stdout_joined = join_reader(stdout_reader);
    let stderr_joined = join_reader(stderr_reader);
    let cleanup_confirmed = child_reaped && stdout_joined && stderr_joined;
    #[cfg(test)]
    if FORCE_COMMAND_CLEANUP_FAILURE.load(Ordering::SeqCst) {
        return false;
    }
    cleanup_confirmed
}

fn flush_output_chunks(context: &ManagedJobContext, stream: &'static str, pending: &mut Vec<u8>) {
    loop {
        if let Some(newline_index) = pending.iter().position(|byte| *byte == b'\n') {
            let chunk: Vec<u8> = pending.drain(..=newline_index).collect();
            let text = String::from_utf8_lossy(&chunk).to_string();
            push_stream_output(context, stream, &text);
            continue;
        }

        match std::str::from_utf8(pending) {
            Ok(text) => {
                if !text.is_empty() {
                    push_stream_output(context, stream, text);
                    pending.clear();
                }
                break;
            }
            Err(error) => {
                let valid_up_to = error.valid_up_to();
                if valid_up_to > 0 {
                    let text =
                        std::str::from_utf8(&pending[..valid_up_to]).expect("valid utf-8 prefix");
                    push_stream_output(context, stream, text);
                    pending.drain(..valid_up_to);
                    continue;
                }

                if error.error_len().is_none() {
                    break;
                }

                let text = String::from_utf8_lossy(pending).to_string();
                push_stream_output(context, stream, &text);
                pending.clear();
                break;
            }
        }
    }
}

fn push_stream_output(context: &ManagedJobContext, stream: &'static str, text: &str) {
    if text.is_empty() {
        return;
    }

    match stream {
        "stdout" => context.push_stdout(text),
        "stderr" => context.push_stderr(text),
        _ => {}
    }
}

fn join_reader(reader: Option<thread::JoinHandle<()>>) -> bool {
    if let Some(reader) = reader {
        let joined = reader.join().is_ok();
        #[cfg(test)]
        COMMAND_READER_JOINS.fetch_add(1, Ordering::SeqCst);
        return joined;
    }
    true
}
