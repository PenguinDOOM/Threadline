use super::*;

pub(super) fn prepare_capacity(
    entries: &mut HashMap<String, Arc<Mutex<JobEntry>>>,
    now: Instant,
    config: &ThreadlineJobManagerConfig,
) -> Result<(), Value> {
    let active_count = entries
        .values()
        .filter(|entry| !lock_job_entry(entry).execution_finished)
        .count();
    if active_count >= config.max_active_jobs {
        log_capacity_rejected("active", active_count, entries.len(), config);
        return Err(job_capacity_exceeded());
    }
    if config.max_retained_jobs == 0 {
        log_capacity_rejected("retained", active_count, entries.len(), config);
        return Err(job_capacity_exceeded());
    }
    while entries.len() >= config.max_retained_jobs {
        let Some(victim_id) = oldest_removable_entry(entries) else {
            log_capacity_rejected("retained", active_count, entries.len(), config);
            return Err(job_capacity_exceeded());
        };
        let victim = entries
            .remove(&victim_id)
            .expect("selected job entry exists");
        let victim = lock_job_entry(&victim);
        debug!(
            job_id = %victim.job_id,
            terminal_state = ?victim.state.terminal_state(),
            age_secs = victim.finished_at.map(|finished| now.saturating_duration_since(finished).as_secs()),
            entry_count = entries.len(),
            retained_limit = config.max_retained_jobs,
            "job_retention_evicted"
        );
    }
    Ok(())
}

fn oldest_removable_entry(entries: &HashMap<String, Arc<Mutex<JobEntry>>>) -> Option<String> {
    entries
        .iter()
        .filter_map(|(job_id, entry)| {
            let entry = lock_job_entry(entry);
            entry_is_removable(&entry).then(|| {
                (
                    entry.finished_at.expect("removable timestamp"),
                    job_id.clone(),
                )
            })
        })
        .min()
        .map(|(_, job_id)| job_id)
}

fn log_capacity_rejected(
    reason: &'static str,
    active_count: usize,
    entry_count: usize,
    config: &ThreadlineJobManagerConfig,
) {
    warn!(
        reason,
        active_count,
        entry_count,
        max_active_jobs = config.max_active_jobs,
        max_retained_jobs = config.max_retained_jobs,
        "job_capacity_rejected"
    );
}

fn job_capacity_exceeded() -> Value {
    stable_error(
        "job_capacity_exceeded",
        "Threadline job capacity is exhausted.",
    )
}
