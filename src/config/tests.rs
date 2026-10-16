use clap::{Arg, Command, CommandFactory, Parser};

use crate::cli::ThreadlineCli;
use crate::env_test_support::run_cases;
use crate::models::RouteProfile;

use super::{
    DEFAULT_CODEX_CLIENT_VERSION, DEFAULT_MAX_ACTIVE_JOBS, DEFAULT_MAX_REQUEST_BODY_BYTES,
    DEFAULT_MAX_RETAINED_JOBS, THREADLINE_ENV_LOCK, ThreadlineConfig, UpstreamInboundLimits,
    UpstreamWatchdogPolicy, job_manager_config_from_environment,
};

fn arg_by_long_flag<'a>(command: &'a Command, long_flag: &str) -> &'a Arg {
    command
        .get_arguments()
        .find(|arg| arg.get_long() == Some(long_flag))
        .unwrap_or_else(|| panic!("expected --{long_flag} to exist on ThreadlineCli"))
}

mod help;
mod parsing;
mod transport;
