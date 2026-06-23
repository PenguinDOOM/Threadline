use std::net::{IpAddr, SocketAddr};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use clap::{Args, Parser};

use crate::jobs::ThreadlineJobManagerConfig;
use crate::models::RouteProfile;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8100;
const DEFAULT_PROFILE: RouteProfile = RouteProfile::Main;
const DEFAULT_CODEX_CLIENT_VERSION: &str = "0.136.0";
const DEFAULT_RETAINED_SESSION_CAPACITY: usize = 64;
const DEFAULT_JOBS_ENABLED: bool = false;
const DEFAULT_JOB_OUTPUT_BUFFER_LIMIT_BYTES: usize = 32 * 1024;
const DEFAULT_JOB_RETENTION_TTL_SECS: u64 = 300;
const DEFAULT_LOG_LEVEL: &str = "info";

static ACTIVE_JOB_MANAGER_CONFIG: LazyLock<Mutex<ThreadlineJobManagerConfig>> =
    LazyLock::new(|| Mutex::new(ThreadlineJobManagerConfig::default()));

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
        env = "THREADLINE_JOBS_ENABLED",
        default_value_t = DEFAULT_JOBS_ENABLED,
        value_name = "BOOL",
        help = "Enable local job execution support.",
        long_help = "Enable local job execution support. When enabled, Threadline may expose job tools for long-running local work instead of blocking a response."
    )]
    pub jobs_enabled: bool,

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
            profile: DEFAULT_PROFILE,
            codex_client_version: DEFAULT_CODEX_CLIENT_VERSION.to_string(),
            retained_session_capacity: DEFAULT_RETAINED_SESSION_CAPACITY,
            jobs_enabled: DEFAULT_JOBS_ENABLED,
            job_output_buffer_limit_bytes: DEFAULT_JOB_OUTPUT_BUFFER_LIMIT_BYTES,
            job_retention_ttl_secs: DEFAULT_JOB_RETENTION_TTL_SECS,
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

    pub fn job_manager_config(&self) -> ThreadlineJobManagerConfig {
        ThreadlineJobManagerConfig {
            jobs_enabled: self.jobs_enabled,
            output_buffer_limit_bytes: self.job_output_buffer_limit_bytes,
            retention_ttl: Duration::from_secs(self.job_retention_ttl_secs),
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
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn read_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
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
    use std::sync::Mutex;

    use clap::{Arg, Command, CommandFactory, Parser};

    use crate::cli::ThreadlineCli;
    use crate::models::RouteProfile;

    use super::DEFAULT_CODEX_CLIENT_VERSION;

    static THREADLINE_PROFILE_ENV_LOCK: Mutex<()> = Mutex::new(());

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
        let config =
            ThreadlineCli::try_parse_from(["threadline", "--codex-client-version", "9.9.9"])
                .expect("threadline config should accept a codex client version cli override")
                .server;

        assert_eq!(config.codex_client_version, "9.9.9");
    }

    #[test]
    fn cli_flag_help_describes_supported_configuration() {
        let command = ThreadlineCli::command();

        for (long_flag, expected_terms) in [
            ("host", &["listen", "address"][..]),
            ("port", &["listen", "port"][..]),
            ("profile", &["profile", "main", "utility"][..]),
            ("codex-client-version", &["codex", "client version"][..]),
            (
                "retained-session-capacity",
                &["retained session", "capacity"][..],
            ),
            ("jobs-enabled", &["job", "enable"][..]),
            (
                "job-output-buffer-limit-bytes",
                &["job output", "bytes"][..],
            ),
            (
                "job-retention-ttl-secs",
                &["job", "retention", "seconds"][..],
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
        let _lock = THREADLINE_PROFILE_ENV_LOCK
            .lock()
            .expect("profile env lock");
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
        let config = ThreadlineCli::try_parse_from(["threadline", "--profile", "utility"])
            .expect("threadline config should accept utility profile")
            .server;

        assert_eq!(config.profile, RouteProfile::Utility);
    }

    #[test]
    fn profile_rejects_invalid_value() {
        ThreadlineCli::try_parse_from(["threadline", "--profile", "invalid"])
            .expect_err("threadline config should reject invalid profiles");
    }

    #[test]
    fn profile_reads_threadline_profile_env_var() {
        let _lock = THREADLINE_PROFILE_ENV_LOCK
            .lock()
            .expect("profile env lock");
        let _guard = ProfileEnvGuard::acquire();
        unsafe { std::env::set_var("THREADLINE_PROFILE", "utility") };

        let config = ThreadlineCli::parse_from(["threadline"]).server;

        assert_eq!(config.profile, RouteProfile::Utility);
    }
}
