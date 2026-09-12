use std::net::{IpAddr, SocketAddr};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use clap::{Args, Parser};

use crate::jobs::{DEFAULT_MAX_ACTIVE_JOBS, DEFAULT_MAX_RETAINED_JOBS, ThreadlineJobManagerConfig};
use crate::models::RouteProfile;
use crate::ws_pump::{
    DEFAULT_UPSTREAM_INBOUND_MAX_BYTES, DEFAULT_UPSTREAM_INBOUND_MAX_MESSAGES,
    MAX_UPSTREAM_INBOUND_MAX_BYTES, MAX_UPSTREAM_INBOUND_MAX_MESSAGES, UpstreamInboundLimits,
};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8100;
const DEFAULT_PROFILE: RouteProfile = RouteProfile::Main;
const DEFAULT_CODEX_CLIENT_VERSION: &str = "0.136.0";
const DEFAULT_RETAINED_SESSION_CAPACITY: usize = 64;
const DEFAULT_JOBS_ENABLED: bool = false;
const DEFAULT_PERSISTENT_REASONING_ENABLED: bool = false;
const DEFAULT_JOB_OUTPUT_BUFFER_LIMIT_BYTES: usize = 32 * 1024;
const DEFAULT_JOB_RETENTION_TTL_SECS: u64 = 300;
const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 32 * 1024 * 1024;

static ACTIVE_JOB_MANAGER_CONFIG: LazyLock<Mutex<ThreadlineJobManagerConfig>> =
    LazyLock::new(|| Mutex::new(ThreadlineJobManagerConfig::default()));

#[cfg(test)]
pub(crate) static THREADLINE_ENV_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct ThreadlineConfig {
    #[arg(
        long,
        env = "THREADLINE_HOST",
        default_value = DEFAULT_HOST,
        value_name = "IP_ADDRESS",
        help = "Listen address for the downstream HTTP server.",
        long_help = "Listen address for the downstream HTTP server. Use an IP address that Threadline should bind for local /v1/responses requests."
    )]
    pub host: String,

    #[arg(
        long,
        env = "THREADLINE_PORT",
        default_value_t = DEFAULT_PORT,
        value_name = "PORT",
        help = "Listen port for the downstream HTTP server.",
        long_help = "Listen port for the downstream HTTP server. This controls which local TCP port accepts /v1/responses requests."
    )]
    pub port: u16,

    #[arg(
        long,
        env = "THREADLINE_UTILITY_PORT",
        value_name = "PORT",
        help = "Optional port for a second utility listener.",
        long_help = "Optional port for a second utility listener. When set, Threadline can start a separate utility-profile listener on this port in addition to the main listener."
    )]
    pub utility_port: Option<u16>,

    #[arg(
        long,
        env = "THREADLINE_PROFILE",
        default_value_t = DEFAULT_PROFILE,
        value_name = "PROFILE",
        help = "Route profile that controls advertised model aliases.",
        long_help = "Route profile that controls advertised model aliases. Use main for retained-session routes and utility for utility-only model advertisement on this listener."
    )]
    pub profile: RouteProfile,

    #[arg(
        long,
        env = "THREADLINE_CODEX_CLIENT_VERSION",
        default_value = DEFAULT_CODEX_CLIENT_VERSION,
        value_name = "VERSION",
        help = "Codex client version sent to the upstream backend.",
        long_help = "Codex client version sent to the upstream backend. Set this when Threadline must match the Codex client version expected by the backend."
    )]
    pub codex_client_version: String,

    #[arg(
        long,
        env = "THREADLINE_RETAINED_SESSION_CAPACITY",
        default_value_t = DEFAULT_RETAINED_SESSION_CAPACITY,
        value_name = "COUNT",
        help = "Maximum retained session capacity for response continuation.",
        long_help = "Maximum retained session capacity for response continuation. Higher values allow more completed response markers to keep a retained session available for follow-up requests."
    )]
    pub retained_session_capacity: usize,

    #[arg(
        long,
        env = "THREADLINE_MAX_REQUEST_BODY_BYTES",
        default_value_t = DEFAULT_MAX_REQUEST_BODY_BYTES,
        value_parser = parse_max_request_body_bytes,
        value_name = "BYTES",
        help = "Maximum accepted /v1/responses request body size in bytes.",
        long_help = "Maximum accepted /v1/responses request body size in bytes. Requests larger than this finite limit are rejected before Threadline processes them."
    )]
    pub max_request_body_bytes: usize,

    #[arg(
        long,
        env = "THREADLINE_UPSTREAM_INBOUND_MAX_MESSAGES",
        default_value_t = DEFAULT_UPSTREAM_INBOUND_MAX_MESSAGES,
        value_parser = parse_upstream_inbound_max_messages,
        value_name = "COUNT",
        help = "Maximum queued upstream inbound messages per connection.",
        long_help = "Maximum queued upstream inbound messages per connection. Reaching this limit rejects the next inbound data message; very small values can reject ordinary responses."
    )]
    pub upstream_inbound_max_messages: usize,

    #[arg(
        long,
        env = "THREADLINE_UPSTREAM_INBOUND_MAX_BYTES",
        default_value_t = DEFAULT_UPSTREAM_INBOUND_MAX_BYTES,
        value_parser = parse_upstream_inbound_max_bytes,
        value_name = "BYTES",
        help = "Maximum queued upstream inbound UTF-8 payload bytes per connection.",
        long_help = "Maximum queued upstream inbound UTF-8 payload bytes per connection. Reaching this limit rejects the next inbound data message; very small values can reject ordinary responses."
    )]
    pub upstream_inbound_max_bytes: usize,

    #[arg(
        long,
        env = "THREADLINE_JOBS_ENABLED",
        default_value_t = DEFAULT_JOBS_ENABLED,
        value_name = "BOOL",
        help = "Enable local job execution support.",
        long_help = "Enable local job execution support. When enabled, Threadline may expose job tools for long-running local work instead of blocking a response."
    )]
    pub jobs_enabled: bool,

    #[arg(
        long,
        env = "THREADLINE_PERSISTENT_REASONING_ENABLED",
        default_value_t = DEFAULT_PERSISTENT_REASONING_ENABLED,
        help = "Enable persistent reasoning context for eligible Main model requests.",
        long_help = "Enable persistent reasoning context for eligible Main-scope model requests. When enabled, Threadline sets reasoning.context=all_turns only for eligible requests in the Main scope."
    )]
    pub persistent_reasoning_enabled: bool,

    #[arg(
        long,
        env = "THREADLINE_JOB_OUTPUT_BUFFER_LIMIT_BYTES",
        default_value_t = DEFAULT_JOB_OUTPUT_BUFFER_LIMIT_BYTES,
        value_name = "BYTES",
        help = "Maximum buffered job output in bytes.",
        long_help = "Maximum buffered job output in bytes. Older job output is dropped once the retained in-memory job output buffer reaches this byte limit."
    )]
    pub job_output_buffer_limit_bytes: usize,

    #[arg(
        long,
        env = "THREADLINE_JOB_RETENTION_TTL_SECS",
        default_value_t = DEFAULT_JOB_RETENTION_TTL_SECS,
        value_name = "SECONDS",
        help = "Job retention time in seconds after completion.",
        long_help = "Job retention time in seconds after completion. Finished job metadata and buffered output remain available until this retention window expires."
    )]
    pub job_retention_ttl_secs: u64,

    #[arg(
        long,
        env = "THREADLINE_JOB_MAX_ACTIVE_JOBS",
        default_value_t = DEFAULT_MAX_ACTIVE_JOBS,
        value_name = "COUNT",
        help = "Maximum concurrently executing managed jobs.",
        long_help = "Maximum concurrently executing managed jobs. Zero rejects every new job admission."
    )]
    pub job_max_active_jobs: usize,

    #[arg(
        long,
        env = "THREADLINE_JOB_MAX_RETAINED_JOBS",
        default_value_t = DEFAULT_MAX_RETAINED_JOBS,
        value_name = "COUNT",
        help = "Maximum total retained job registry entries.",
        long_help = "Maximum total retained job registry entries, including active jobs. Zero rejects every new job admission."
    )]
    pub job_max_retained_jobs: usize,

    #[arg(
        long,
        env = "THREADLINE_JOB_ALLOWED_COMMANDS",
        value_name = "PROGRAMS",
        help = "Comma-separated exact program names allowed for jobs.",
        long_help = "Comma-separated exact executable or program names allowed for jobs. Each configured entry is matched against the requested program name exactly."
    )]
    pub job_allowed_commands: Option<String>,

    #[arg(
        long,
        env = "THREADLINE_LOG_LEVEL",
        default_value = DEFAULT_LOG_LEVEL,
        value_name = "LEVEL",
        help = "Log verbosity for Threadline diagnostics.",
        long_help = "Log verbosity for Threadline diagnostics. Use standard Rust tracing levels such as error, warn, info, debug, or trace."
    )]
    pub log_level: String,
}

impl Default for ThreadlineConfig {
    fn default() -> Self {
        let config = Self {
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            utility_port: None,
            profile: DEFAULT_PROFILE,
            codex_client_version: DEFAULT_CODEX_CLIENT_VERSION.to_string(),
            retained_session_capacity: DEFAULT_RETAINED_SESSION_CAPACITY,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            upstream_inbound_max_messages: DEFAULT_UPSTREAM_INBOUND_MAX_MESSAGES,
            upstream_inbound_max_bytes: DEFAULT_UPSTREAM_INBOUND_MAX_BYTES,
            jobs_enabled: DEFAULT_JOBS_ENABLED,
            persistent_reasoning_enabled: DEFAULT_PERSISTENT_REASONING_ENABLED,
            job_output_buffer_limit_bytes: DEFAULT_JOB_OUTPUT_BUFFER_LIMIT_BYTES,
            job_retention_ttl_secs: DEFAULT_JOB_RETENTION_TTL_SECS,
            job_max_active_jobs: DEFAULT_MAX_ACTIVE_JOBS,
            job_max_retained_jobs: DEFAULT_MAX_RETAINED_JOBS,
            job_allowed_commands: None,
            log_level: DEFAULT_LOG_LEVEL.to_string(),
        };
        set_active_job_manager_config(config.job_manager_config());
        config
    }
}

impl ThreadlineConfig {
    pub fn from_env() -> Self {
        let config = crate::cli::ThreadlineCli::parse().server;
        set_active_job_manager_config(config.job_manager_config());
        config
    }

    pub fn bind_address(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        let host: IpAddr = self.host.parse()?;
        Ok(SocketAddr::from((host, self.port)))
    }

    pub fn persistent_reasoning_enabled_for_profile(&self) -> bool {
        self.profile == RouteProfile::Main && self.persistent_reasoning_enabled
    }

    pub fn upstream_inbound_limits(&self) -> Result<UpstreamInboundLimits, &'static str> {
        UpstreamInboundLimits::new(
            self.upstream_inbound_max_messages,
            self.upstream_inbound_max_bytes,
        )
    }

    pub fn job_manager_config(&self) -> ThreadlineJobManagerConfig {
        ThreadlineJobManagerConfig {
            jobs_enabled: self.jobs_enabled,
            output_buffer_limit_bytes: self.job_output_buffer_limit_bytes,
            retention_ttl: Duration::from_secs(self.job_retention_ttl_secs),
            max_active_jobs: self.job_max_active_jobs,
            max_retained_jobs: self.job_max_retained_jobs,
            allowed_commands: split_allowed_commands(self.job_allowed_commands.as_deref()),
        }
    }
}

pub fn job_manager_config_from_environment() -> ThreadlineJobManagerConfig {
    ThreadlineJobManagerConfig {
        jobs_enabled: read_bool_env("THREADLINE_JOBS_ENABLED", DEFAULT_JOBS_ENABLED),
        output_buffer_limit_bytes: read_usize_env(
            "THREADLINE_JOB_OUTPUT_BUFFER_LIMIT_BYTES",
            DEFAULT_JOB_OUTPUT_BUFFER_LIMIT_BYTES,
        ),
        retention_ttl: Duration::from_secs(read_u64_env(
            "THREADLINE_JOB_RETENTION_TTL_SECS",
            DEFAULT_JOB_RETENTION_TTL_SECS,
        )),
        max_active_jobs: read_usize_env("THREADLINE_JOB_MAX_ACTIVE_JOBS", DEFAULT_MAX_ACTIVE_JOBS),
        max_retained_jobs: read_usize_env(
            "THREADLINE_JOB_MAX_RETAINED_JOBS",
            DEFAULT_MAX_RETAINED_JOBS,
        ),
        allowed_commands: split_allowed_commands(
            std::env::var("THREADLINE_JOB_ALLOWED_COMMANDS")
                .ok()
                .as_deref(),
        ),
    }
}

pub fn active_job_manager_config() -> ThreadlineJobManagerConfig {
    ACTIVE_JOB_MANAGER_CONFIG
        .lock()
        .expect("job manager config lock")
        .clone()
}

fn read_bool_env(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(default)
}

fn read_usize_env(name: &str, default: usize) -> usize {
    let value = std::env::var(name).ok();
    parse_usize_env_value(value.as_deref(), default)
}

fn parse_usize_env_value(value: Option<&str>, default: usize) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn read_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_upstream_inbound_max_messages(value: &str) -> Result<usize, String> {
    parse_bounded_usize(value, 1, MAX_UPSTREAM_INBOUND_MAX_MESSAGES)
}

fn parse_max_request_body_bytes(value: &str) -> Result<usize, String> {
    parse_bounded_usize(value, 1, usize::MAX)
}

fn parse_upstream_inbound_max_bytes(value: &str) -> Result<usize, String> {
    parse_bounded_usize(value, 1, MAX_UPSTREAM_INBOUND_MAX_BYTES)
}

fn parse_bounded_usize(value: &str, minimum: usize, maximum: usize) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("must be an integer between {minimum} and {maximum}"))?;
    if (minimum..=maximum).contains(&parsed) {
        Ok(parsed)
    } else {
        Err(format!("must be between {minimum} and {maximum}"))
    }
}

fn split_allowed_commands(value: Option<&str>) -> Vec<String> {
    value
        .into_iter()
        .flat_map(|commands| commands.split(','))
        .map(str::trim)
        .filter(|command| !command.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn set_active_job_manager_config(config: ThreadlineJobManagerConfig) {
    *ACTIVE_JOB_MANAGER_CONFIG
        .lock()
        .expect("job manager config lock") = config;
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use clap::{Arg, Command, CommandFactory, Parser};

    use crate::cli::ThreadlineCli;
    use crate::models::RouteProfile;

    use super::{
        DEFAULT_CODEX_CLIENT_VERSION, DEFAULT_MAX_ACTIVE_JOBS, DEFAULT_MAX_REQUEST_BODY_BYTES,
        DEFAULT_MAX_RETAINED_JOBS, ThreadlineConfig, UpstreamInboundLimits,
        job_manager_config_from_environment,
    };

    struct ProfileEnvGuard {
        original: Option<OsString>,
    }

    impl ProfileEnvGuard {
        fn acquire() -> Self {
            Self {
                original: std::env::var_os("THREADLINE_PROFILE"),
            }
        }
    }

    impl Drop for ProfileEnvGuard {
        fn drop(&mut self) {
            match self.original.take() {
                Some(value) => unsafe { std::env::set_var("THREADLINE_PROFILE", value) },
                None => unsafe { std::env::remove_var("THREADLINE_PROFILE") },
            }
        }
    }

    struct PersistentReasoningEnabledEnvGuard {
        original: Option<OsString>,
    }

    impl PersistentReasoningEnabledEnvGuard {
        fn acquire() -> Self {
            Self {
                original: std::env::var_os("THREADLINE_PERSISTENT_REASONING_ENABLED"),
            }
        }
    }

    impl Drop for PersistentReasoningEnabledEnvGuard {
        fn drop(&mut self) {
            match self.original.take() {
                Some(value) => unsafe {
                    std::env::set_var("THREADLINE_PERSISTENT_REASONING_ENABLED", value)
                },
                None => unsafe { std::env::remove_var("THREADLINE_PERSISTENT_REASONING_ENABLED") },
            }
        }
    }

    struct UtilityPortEnvGuard {
        original: Option<OsString>,
    }

    impl UtilityPortEnvGuard {
        fn acquire() -> Self {
            Self {
                original: std::env::var_os("THREADLINE_UTILITY_PORT"),
            }
        }
    }

    impl Drop for UtilityPortEnvGuard {
        fn drop(&mut self) {
            match self.original.take() {
                Some(value) => unsafe { std::env::set_var("THREADLINE_UTILITY_PORT", value) },
                None => unsafe { std::env::remove_var("THREADLINE_UTILITY_PORT") },
            }
        }
    }

    struct UpstreamInboundLimitsEnvGuard {
        messages: Option<OsString>,
        bytes: Option<OsString>,
    }

    struct JobCapacityEnvGuard {
        active: Option<OsString>,
        retained: Option<OsString>,
    }

    struct RequestBodyLimitEnvGuard {
        max_request_body_bytes: Option<OsString>,
    }

    impl JobCapacityEnvGuard {
        fn acquire() -> Self {
            Self {
                active: std::env::var_os("THREADLINE_JOB_MAX_ACTIVE_JOBS"),
                retained: std::env::var_os("THREADLINE_JOB_MAX_RETAINED_JOBS"),
            }
        }
    }

    impl Drop for JobCapacityEnvGuard {
        fn drop(&mut self) {
            restore_env_var("THREADLINE_JOB_MAX_ACTIVE_JOBS", self.active.take());
            restore_env_var("THREADLINE_JOB_MAX_RETAINED_JOBS", self.retained.take());
        }
    }

    impl RequestBodyLimitEnvGuard {
        fn acquire() -> Self {
            Self {
                max_request_body_bytes: std::env::var_os("THREADLINE_MAX_REQUEST_BODY_BYTES"),
            }
        }
    }

    impl Drop for RequestBodyLimitEnvGuard {
        fn drop(&mut self) {
            restore_env_var(
                "THREADLINE_MAX_REQUEST_BODY_BYTES",
                self.max_request_body_bytes.take(),
            );
        }
    }

    impl UpstreamInboundLimitsEnvGuard {
        fn acquire() -> Self {
            Self {
                messages: std::env::var_os("THREADLINE_UPSTREAM_INBOUND_MAX_MESSAGES"),
                bytes: std::env::var_os("THREADLINE_UPSTREAM_INBOUND_MAX_BYTES"),
            }
        }
    }

    impl Drop for UpstreamInboundLimitsEnvGuard {
        fn drop(&mut self) {
            restore_env_var(
                "THREADLINE_UPSTREAM_INBOUND_MAX_MESSAGES",
                self.messages.take(),
            );
            restore_env_var("THREADLINE_UPSTREAM_INBOUND_MAX_BYTES", self.bytes.take());
        }
    }

    fn restore_env_var(name: &str, value: Option<OsString>) {
        match value {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
    }

    fn arg_by_long_flag<'a>(command: &'a Command, long_flag: &str) -> &'a Arg {
        command
            .get_arguments()
            .find(|arg| arg.get_long() == Some(long_flag))
            .unwrap_or_else(|| panic!("expected --{long_flag} to exist on ThreadlineCli"))
    }

    fn argument_help_text(argument: &Arg) -> String {
        [argument.get_help(), argument.get_long_help()]
            .into_iter()
            .flatten()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_string()
    }

    fn assert_help_mentions(argument: &Arg, long_flag: &str, expected_terms: &[&str]) {
        let help_text = argument_help_text(argument);
        let normalized_help = help_text.to_ascii_lowercase();
        let generated_flag_label = long_flag.to_ascii_lowercase();
        let generated_phrase_label = long_flag.replace('-', " ").to_ascii_lowercase();

        assert!(
            !help_text.is_empty(),
            "expected --{long_flag} to have help or long_help text"
        );
        assert!(
            help_text.len() > long_flag.len() + 12,
            "expected --{long_flag} help to be descriptive, got {help_text:?}"
        );
        assert!(
            normalized_help != generated_flag_label && normalized_help != generated_phrase_label,
            "expected --{long_flag} help to add semantics beyond generated-only flag text, got {help_text:?}"
        );

        for term in expected_terms {
            assert!(
                normalized_help.contains(term),
                "expected --{long_flag} help to mention {term:?}, got {help_text:?}"
            );
        }
    }

    #[test]
    fn codex_client_version_defaults_to_installed_version() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let config = ThreadlineCli::parse_from(["threadline"]).server;
        let command = ThreadlineCli::command();
        let argument = command
            .get_arguments()
            .find(|arg| arg.get_long() == Some("codex-client-version"))
            .expect("codex client version arg should exist");
        let default_values: Vec<_> = argument
            .get_default_values()
            .iter()
            .map(|value| value.to_str().expect("utf-8 default value"))
            .collect();

        assert_eq!(config.codex_client_version, DEFAULT_CODEX_CLIENT_VERSION);
        assert_eq!(default_values, vec![DEFAULT_CODEX_CLIENT_VERSION]);
    }

    #[test]
    fn codex_client_version_cli_override_wins() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let config =
            ThreadlineCli::try_parse_from(["threadline", "--codex-client-version", "9.9.9"])
                .expect("threadline config should accept a codex client version cli override")
                .server;

        assert_eq!(config.codex_client_version, "9.9.9");
    }

    #[test]
    fn upstream_inbound_limits_default_override_and_reject_invalid_ranges() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let default_config = ThreadlineCli::parse_from(["threadline"]).server;
        assert_eq!(default_config.upstream_inbound_max_messages, 256);
        assert_eq!(default_config.upstream_inbound_max_bytes, 16 * 1024 * 1024);

        let override_config = ThreadlineCli::try_parse_from([
            "threadline",
            "--upstream-inbound-max-messages",
            "2",
            "--upstream-inbound-max-bytes",
            "3",
        ])
        .expect("valid override")
        .server;
        assert_eq!(
            override_config.upstream_inbound_limits(),
            Ok(UpstreamInboundLimits::new(2, 3).unwrap())
        );

        assert!(
            ThreadlineCli::try_parse_from(["threadline", "--upstream-inbound-max-messages", "0",])
                .is_err()
        );
        assert!(
            ThreadlineCli::try_parse_from([
                "threadline",
                "--upstream-inbound-max-bytes",
                "67108865",
            ])
            .is_err()
        );
        assert!(
            ThreadlineCli::try_parse_from([
                "threadline",
                "--upstream-inbound-max-messages",
                "not-a-number",
            ])
            .is_err()
        );
        assert!(
            ThreadlineCli::try_parse_from(["threadline", "--upstream-inbound-max-bytes", "-1",])
                .is_err()
        );
        assert!(UpstreamInboundLimits::new(0, 1).is_err());
        assert!(UpstreamInboundLimits::new(1, 0).is_err());
    }

    #[test]
    fn upstream_inbound_limits_read_environment_overrides() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let _guard = UpstreamInboundLimitsEnvGuard::acquire();
        unsafe {
            std::env::set_var("THREADLINE_UPSTREAM_INBOUND_MAX_MESSAGES", "7");
            std::env::set_var("THREADLINE_UPSTREAM_INBOUND_MAX_BYTES", "11");
        }

        let config = ThreadlineCli::parse_from(["threadline"]).server;

        assert_eq!(config.upstream_inbound_max_messages, 7);
        assert_eq!(config.upstream_inbound_max_bytes, 11);
    }

    #[test]
    fn request_body_limit_defaults_overrides_and_rejects_invalid_values() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let _guard = RequestBodyLimitEnvGuard::acquire();
        unsafe { std::env::remove_var("THREADLINE_MAX_REQUEST_BODY_BYTES") };

        let default_config = ThreadlineCli::parse_from(["threadline"]).server;
        assert_eq!(
            ThreadlineConfig::default().max_request_body_bytes,
            33_554_432
        );
        assert_eq!(default_config.max_request_body_bytes, 33_554_432);
        assert_eq!(DEFAULT_MAX_REQUEST_BODY_BYTES, 33_554_432);

        unsafe { std::env::set_var("THREADLINE_MAX_REQUEST_BODY_BYTES", "1") };
        let minimum_environment_config = ThreadlineCli::parse_from(["threadline"]).server;
        assert_eq!(minimum_environment_config.max_request_body_bytes, 1);

        unsafe { std::env::set_var("THREADLINE_MAX_REQUEST_BODY_BYTES", "3") };
        let environment_config = ThreadlineCli::parse_from(["threadline"]).server;
        assert_eq!(environment_config.max_request_body_bytes, 3);

        let cli_config =
            ThreadlineCli::try_parse_from(["threadline", "--max-request-body-bytes", "1"])
                .expect("positive body limit should parse")
                .server;
        assert_eq!(cli_config.max_request_body_bytes, 1);

        for invalid in [
            "0",
            "-1",
            "not-a-number",
            "999999999999999999999999999999999999",
        ] {
            assert!(
                ThreadlineCli::try_parse_from(["threadline", "--max-request-body-bytes", invalid])
                    .is_err(),
                "CLI should reject {invalid:?}"
            );
            unsafe { std::env::set_var("THREADLINE_MAX_REQUEST_BODY_BYTES", invalid) };
            assert!(
                ThreadlineCli::try_parse_from(["threadline"]).is_err(),
                "environment should reject {invalid:?}"
            );
        }

        let maximum = usize::MAX.to_string();
        let maximum_config =
            ThreadlineCli::try_parse_from(["threadline", "--max-request-body-bytes", &maximum])
                .expect("usize maximum should parse")
                .server;
        assert_eq!(maximum_config.max_request_body_bytes, usize::MAX);

        unsafe { std::env::set_var("THREADLINE_MAX_REQUEST_BODY_BYTES", &maximum) };
        let maximum_environment_config = ThreadlineCli::parse_from(["threadline"]).server;
        assert_eq!(
            maximum_environment_config.max_request_body_bytes,
            usize::MAX
        );
    }

    #[test]
    fn job_capacity_defaults_cli_values_and_zero_are_preserved() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let _guard = JobCapacityEnvGuard::acquire();
        unsafe {
            std::env::remove_var("THREADLINE_JOB_MAX_ACTIVE_JOBS");
            std::env::remove_var("THREADLINE_JOB_MAX_RETAINED_JOBS");
        }
        let default_config = ThreadlineCli::parse_from(["threadline"]).server;
        assert_eq!(default_config.job_max_active_jobs, 16);
        assert_eq!(default_config.job_max_retained_jobs, 128);

        let configured = ThreadlineCli::try_parse_from([
            "threadline",
            "--job-max-active-jobs",
            "0",
            "--job-max-retained-jobs",
            "0",
        ])
        .expect("capacity values should parse")
        .server;
        assert_eq!(configured.job_max_active_jobs, 0);
        assert_eq!(configured.job_max_retained_jobs, 0);
        assert_eq!(configured.job_manager_config().max_active_jobs, 0);
        assert_eq!(configured.job_manager_config().max_retained_jobs, 0);

        let small_config = ThreadlineCli::try_parse_from([
            "threadline",
            "--job-max-active-jobs",
            "2",
            "--job-max-retained-jobs",
            "3",
        ])
        .expect("small capacity values should parse")
        .server;
        assert_eq!(small_config.job_max_active_jobs, 2);
        assert_eq!(small_config.job_max_retained_jobs, 3);

        for flag in ["--job-max-active-jobs", "--job-max-retained-jobs"] {
            for invalid in ["-1", "not-a-number", "999999999999999999999999999999999999"] {
                assert!(
                    ThreadlineCli::try_parse_from(["threadline", flag, invalid,]).is_err(),
                    "{flag} should reject {invalid:?}"
                );
            }
        }
    }

    #[test]
    fn job_capacity_cli_overrides_environment_values() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let _guard = JobCapacityEnvGuard::acquire();
        unsafe {
            std::env::set_var("THREADLINE_JOB_MAX_ACTIVE_JOBS", "0");
            std::env::set_var("THREADLINE_JOB_MAX_RETAINED_JOBS", "0");
        }

        let environment_config = ThreadlineCli::parse_from(["threadline"]).server;
        assert_eq!(environment_config.job_max_active_jobs, 0);
        assert_eq!(environment_config.job_max_retained_jobs, 0);

        unsafe {
            std::env::set_var("THREADLINE_JOB_MAX_ACTIVE_JOBS", "2");
            std::env::set_var("THREADLINE_JOB_MAX_RETAINED_JOBS", "3");
        }
        let small_environment_config = ThreadlineCli::parse_from(["threadline"]).server;
        assert_eq!(small_environment_config.job_max_active_jobs, 2);
        assert_eq!(small_environment_config.job_max_retained_jobs, 3);

        let cli_config = ThreadlineCli::try_parse_from([
            "threadline",
            "--job-max-active-jobs",
            "4",
            "--job-max-retained-jobs",
            "1",
        ])
        .expect("CLI values should override environment")
        .server;
        assert_eq!(cli_config.job_max_active_jobs, 4);
        assert_eq!(cli_config.job_max_retained_jobs, 1);

        for (name, other_name) in [
            (
                "THREADLINE_JOB_MAX_ACTIVE_JOBS",
                "THREADLINE_JOB_MAX_RETAINED_JOBS",
            ),
            (
                "THREADLINE_JOB_MAX_RETAINED_JOBS",
                "THREADLINE_JOB_MAX_ACTIVE_JOBS",
            ),
        ] {
            for invalid in ["-1", "not-a-number", "999999999999999999999999999999999999"] {
                unsafe {
                    std::env::set_var(name, invalid);
                    std::env::set_var(other_name, "3");
                }
                assert!(
                    ThreadlineCli::try_parse_from(["threadline"]).is_err(),
                    "{name} should reject {invalid:?} during startup parsing"
                );
            }
        }
    }

    #[test]
    fn standalone_job_capacity_environment_helper_preserves_zero_and_falls_back() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let _guard = JobCapacityEnvGuard::acquire();
        unsafe {
            std::env::set_var("THREADLINE_JOB_MAX_ACTIVE_JOBS", "0");
            std::env::set_var("THREADLINE_JOB_MAX_RETAINED_JOBS", "0");
        }
        let configured = job_manager_config_from_environment();
        assert_eq!(configured.max_active_jobs, 0);
        assert_eq!(configured.max_retained_jobs, 0);

        for (active, retained) in [
            ("invalid", "3"),
            ("-1", "3"),
            ("999999999999999999999999999999999999", "3"),
            ("2", "invalid"),
            ("2", "-1"),
            ("2", "999999999999999999999999999999999999"),
        ] {
            unsafe {
                std::env::set_var("THREADLINE_JOB_MAX_ACTIVE_JOBS", active);
                std::env::set_var("THREADLINE_JOB_MAX_RETAINED_JOBS", retained);
            }
            let fallback = job_manager_config_from_environment();
            assert_eq!(
                fallback.max_active_jobs,
                active.parse::<usize>().unwrap_or(DEFAULT_MAX_ACTIVE_JOBS)
            );
            assert_eq!(
                fallback.max_retained_jobs,
                retained
                    .parse::<usize>()
                    .unwrap_or(DEFAULT_MAX_RETAINED_JOBS)
            );
        }
    }

    #[test]
    fn cli_flag_help_describes_supported_configuration() {
        let command = ThreadlineCli::command();

        for (long_flag, expected_terms) in [
            ("host", &["listen", "address"][..]),
            ("port", &["listen", "port"][..]),
            ("utility-port", &["utility", "listener", "port"][..]),
            ("profile", &["profile", "main", "utility"][..]),
            ("codex-client-version", &["codex", "client version"][..]),
            (
                "retained-session-capacity",
                &["retained session", "capacity"][..],
            ),
            (
                "max-request-body-bytes",
                &["/v1/responses", "body", "bytes", "finite"][..],
            ),
            (
                "upstream-inbound-max-messages",
                &["upstream", "inbound", "messages"][..],
            ),
            (
                "upstream-inbound-max-bytes",
                &["upstream", "inbound", "bytes"][..],
            ),
            ("jobs-enabled", &["job", "enable"][..]),
            (
                "persistent-reasoning-enabled",
                &[
                    "persistent",
                    "reasoning",
                    "eligible",
                    "main",
                    "request",
                    "reasoning.context",
                    "all_turns",
                ][..],
            ),
            (
                "job-output-buffer-limit-bytes",
                &["job output", "bytes"][..],
            ),
            (
                "job-retention-ttl-secs",
                &["job", "retention", "seconds"][..],
            ),
            ("job-max-active-jobs", &["concurrently", "job", "zero"][..]),
            (
                "job-max-retained-jobs",
                &["retained", "registry", "zero"][..],
            ),
            (
                "job-allowed-commands",
                &["comma-separated", "exact", "program"][..],
            ),
            ("log-level", &["log", "verbosity"][..]),
        ] {
            let argument = arg_by_long_flag(&command, long_flag);
            assert_help_mentions(argument, long_flag, expected_terms);
        }
    }

    #[test]
    fn profile_defaults_to_main() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let _guard = ProfileEnvGuard::acquire();
        unsafe { std::env::remove_var("THREADLINE_PROFILE") };

        let config = ThreadlineCli::parse_from(["threadline"]).server;
        let command = ThreadlineCli::command();
        let argument = arg_by_long_flag(&command, "profile");
        let default_values: Vec<_> = argument
            .get_default_values()
            .iter()
            .map(|value| value.to_str().expect("utf-8 default value"))
            .collect();

        assert_eq!(config.profile, RouteProfile::Main);
        assert_eq!(default_values, vec!["main"]);
    }

    #[test]
    fn profile_accepts_explicit_utility_value() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let config = ThreadlineCli::try_parse_from(["threadline", "--profile", "utility"])
            .expect("threadline config should accept utility profile")
            .server;

        assert_eq!(config.profile, RouteProfile::Utility);
    }

    #[test]
    fn profile_rejects_invalid_value() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        ThreadlineCli::try_parse_from(["threadline", "--profile", "invalid"])
            .expect_err("threadline config should reject invalid profiles");
    }

    #[test]
    fn profile_reads_threadline_profile_env_var() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let _guard = ProfileEnvGuard::acquire();
        unsafe { std::env::set_var("THREADLINE_PROFILE", "utility") };

        let config = ThreadlineCli::parse_from(["threadline"]).server;

        assert_eq!(config.profile, RouteProfile::Utility);
    }

    #[test]
    fn persistent_reasoning_enabled_defaults_to_false() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let _guard = PersistentReasoningEnabledEnvGuard::acquire();
        unsafe { std::env::remove_var("THREADLINE_PERSISTENT_REASONING_ENABLED") };

        let config = ThreadlineCli::parse_from(["threadline"]).server;

        assert!(!config.persistent_reasoning_enabled);
    }

    #[test]
    fn persistent_reasoning_enabled_is_effective_only_for_main_profile() {
        let main_config = ThreadlineConfig {
            persistent_reasoning_enabled: true,
            ..ThreadlineConfig::default()
        };
        let utility_config = ThreadlineConfig {
            profile: RouteProfile::Utility,
            persistent_reasoning_enabled: true,
            ..ThreadlineConfig::default()
        };

        assert!(main_config.persistent_reasoning_enabled_for_profile());
        assert!(!utility_config.persistent_reasoning_enabled_for_profile());
    }

    #[test]
    fn persistent_reasoning_enabled_accepts_cli_flag() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let config =
            ThreadlineCli::try_parse_from(["threadline", "--persistent-reasoning-enabled"])
                .expect("threadline config should accept the persistent reasoning cli flag")
                .server;

        assert!(config.persistent_reasoning_enabled);
    }

    #[test]
    fn persistent_reasoning_enabled_reads_true_and_false_env_values() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let _guard = PersistentReasoningEnabledEnvGuard::acquire();

        unsafe { std::env::set_var("THREADLINE_PERSISTENT_REASONING_ENABLED", "true") };
        let enabled = ThreadlineCli::parse_from(["threadline"]).server;

        unsafe { std::env::set_var("THREADLINE_PERSISTENT_REASONING_ENABLED", "false") };
        let disabled = ThreadlineCli::parse_from(["threadline"]).server;

        assert!(enabled.persistent_reasoning_enabled);
        assert!(!disabled.persistent_reasoning_enabled);
    }

    #[test]
    fn utility_port_defaults_to_none() {
        let config = ThreadlineConfig::default();

        assert_eq!(config.utility_port, None);
    }

    #[test]
    fn utility_port_accepts_cli_value() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let config = ThreadlineCli::try_parse_from(["threadline", "--utility-port", "8101"])
            .expect("threadline config should accept a utility port cli override")
            .server;

        assert_eq!(config.utility_port, Some(8101));
    }

    #[test]
    fn utility_port_reads_threadline_utility_port_env_var() {
        let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let _guard = UtilityPortEnvGuard::acquire();
        unsafe { std::env::set_var("THREADLINE_UTILITY_PORT", "8101") };

        let config = ThreadlineCli::parse_from(["threadline"]).server;

        assert_eq!(config.utility_port, Some(8101));
    }
}
