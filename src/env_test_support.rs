use std::io::Read;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const TEST_NAME: &str = "THREADLINE_ENV_TEST_NAME";
const TEST_CASE: &str = "THREADLINE_ENV_TEST_CASE";
const CHILD_TIMEOUT: Duration = Duration::from_secs(20);
const CONFIG_KEYS: &[&str] = &[
    "THREADLINE_HOST",
    "THREADLINE_PORT",
    "THREADLINE_UTILITY_PORT",
    "THREADLINE_PROFILE",
    "THREADLINE_CODEX_CLIENT_VERSION",
    "THREADLINE_RETAINED_SESSION_CAPACITY",
    "THREADLINE_MAX_REQUEST_BODY_BYTES",
    "THREADLINE_UPSTREAM_INBOUND_MAX_MESSAGES",
    "THREADLINE_UPSTREAM_INBOUND_MAX_BYTES",
    "THREADLINE_UPSTREAM_CONNECT_TIMEOUT_SECS",
    "THREADLINE_UPSTREAM_PONG_TIMEOUT_SECS",
    "THREADLINE_UPSTREAM_WRITE_TIMEOUT_SECS",
    "THREADLINE_JOBS_ENABLED",
    "THREADLINE_PERSISTENT_REASONING_ENABLED",
    "THREADLINE_JOB_OUTPUT_BUFFER_LIMIT_BYTES",
    "THREADLINE_JOB_RETENTION_TTL_SECS",
    "THREADLINE_JOB_MAX_ACTIVE_JOBS",
    "THREADLINE_JOB_MAX_RETAINED_JOBS",
    "THREADLINE_JOB_ALLOWED_COMMANDS",
    "THREADLINE_LOG_LEVEL",
    "THREADLINE_UPSTREAM_URL",
];

type EnvCase<'a> = (&'a str, &'a [(&'a str, &'a str)]);

pub(crate) fn child_case(test_name: &str, cases: &[&str]) -> Option<String> {
    match (std::env::var(TEST_NAME), std::env::var(TEST_CASE)) {
        (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => None,
        (Ok(name), Ok(case)) => {
            assert_eq!(name, test_name, "unexpected child test");
            assert!(cases.contains(&case.as_str()), "unknown child case");
            Some(case)
        }
        _ => panic!("invalid child test identification"),
    }
}

fn acknowledgement(test_name: &str, case: &str) -> String {
    format!("THREADLINE_ENV_TEST_ACK:{test_name}:{case}")
}

pub(crate) fn acknowledge(test_name: &str, case: &str) {
    println!("\n{}", acknowledgement(test_name, case));
}

pub(crate) fn run_cases(test_name: &str, cases: &[EnvCase<'_>], assertions: impl Fn(&str)) {
    let ids: Vec<_> = cases.iter().map(|(case, _)| *case).collect();
    if let Some(case) = child_case(test_name, &ids) {
        assertions(&case);
        acknowledge(test_name, &case);
        return;
    }
    for (case, environment) in cases {
        run_child(test_name, case, environment).expect("environment child test must pass");
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ChildFailure {
    Timeout,
    Exit,
    MissingAcknowledgement,
}

fn child_command(test_name: &str, case: &str, environment: &[(&str, &str)]) -> Command {
    assert!(
        std::env::var_os(TEST_NAME).is_none(),
        "recursive child launch"
    );
    assert!(
        std::env::var_os(TEST_CASE).is_none(),
        "recursive child launch"
    );
    let mut command = Command::new(std::env::current_exe().expect("test executable"));
    command.args([test_name, "--exact", "--test-threads=1", "--nocapture"]);
    for key in CONFIG_KEYS {
        command.env_remove(key);
    }
    command.env(TEST_NAME, test_name).env(TEST_CASE, case);
    for (key, value) in environment {
        assert!(CONFIG_KEYS.contains(key), "unknown configuration key");
        command.env(key, value);
    }
    command.stdout(Stdio::piped()).stderr(Stdio::null());
    command
}

fn wait_child(child: &mut Child, timeout: Duration) -> (ExitStatus, bool) {
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return (status, false),
            Ok(None) if started.elapsed() < timeout => thread::sleep(Duration::from_millis(10)),
            result => {
                let _ = child.kill();
                let status = child.wait().expect("owned child must be reaped");
                assert!(result.is_ok(), "child status observation failed");
                return (status, true);
            }
        }
    }
}

pub(crate) fn run_child(
    test_name: &str,
    case: &str,
    environment: &[(&str, &str)],
) -> Result<(), ChildFailure> {
    run_child_with_timeout(test_name, case, environment, CHILD_TIMEOUT)
}

fn run_child_with_timeout(
    test_name: &str,
    case: &str,
    environment: &[(&str, &str)],
    timeout: Duration,
) -> Result<(), ChildFailure> {
    let mut child = child_command(test_name, case, environment)
        .spawn()
        .expect("spawn test child");
    let mut stdout = child.stdout.take().expect("child stdout");
    let reader = thread::spawn(move || {
        let mut output = String::new();
        stdout
            .read_to_string(&mut output)
            .expect("read child output");
        output
    });
    let (status, timed_out) = wait_child(&mut child, timeout);
    let output = reader.join().expect("join child output reader");
    if timed_out {
        assert!(child.try_wait().expect("reaped child status").is_some());
        return Err(ChildFailure::Timeout);
    }
    if !status.success() {
        return Err(ChildFailure::Exit);
    }
    if output
        .lines()
        .filter(|line| *line == acknowledgement(test_name, case))
        .count()
        != 1
    {
        return Err(ChildFailure::MissingAcknowledgement);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_runner_rejects_failed_assertions_bad_dispatch_and_recursion() {
        const NAME: &str = "env_test_support::tests::child_runner_rejects_failed_assertions_bad_dispatch_and_recursion";
        if let Some(case) = child_case(NAME, &["assertion", "recursive"]) {
            match case.as_str() {
                "assertion" => assert_eq!(1, 2, "child assertion must fail"),
                "recursive" => {
                    let _ = run_child(NAME, "assertion", &[]);
                }
                _ => unreachable!(),
            }
            acknowledge(NAME, &case);
            return;
        }
        for case in ["assertion", "recursive", "unknown"] {
            assert_eq!(run_child(NAME, case, &[]), Err(ChildFailure::Exit));
        }
        assert_eq!(
            run_child("env_test_support::tests::missing_test", "assertion", &[]),
            Err(ChildFailure::MissingAcknowledgement)
        );
    }

    #[test]
    fn child_runner_times_out_and_reaps_owned_child() {
        const NAME: &str = "env_test_support::tests::child_runner_times_out_and_reaps_owned_child";
        if child_case(NAME, &["timeout"]).is_some() {
            loop {
                thread::park();
            }
        }
        assert_eq!(
            run_child_with_timeout(NAME, "timeout", &[], Duration::from_secs(1)),
            Err(ChildFailure::Timeout)
        );
    }
}
