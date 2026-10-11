use std::net::{IpAddr, SocketAddr};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use clap::{Args, Parser};

use crate::jobs::{DEFAULT_MAX_ACTIVE_JOBS, DEFAULT_MAX_RETAINED_JOBS, ThreadlineJobManagerConfig};
use crate::models::RouteProfile;
use crate::ws_pump::{
    DEFAULT_UPSTREAM_INBOUND_MAX_BYTES, DEFAULT_UPSTREAM_INBOUND_MAX_MESSAGES,
    MAX_UPSTREAM_INBOUND_MAX_BYTES, MAX_UPSTREAM_INBOUND_MAX_MESSAGES, UpstreamInboundLimits,
    UpstreamWatchdogPolicy,
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
const DEFAULT_UPSTREAM_CONNECT_TIMEOUT_SECS: u64 = 30;
const MAX_UPSTREAM_CONNECT_TIMEOUT_SECS: u64 = 3600;
const DEFAULT_UPSTREAM_PONG_TIMEOUT_SECS: u64 = 60;
const DEFAULT_UPSTREAM_WRITE_TIMEOUT_SECS: u64 = 60;
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
        env = "THREADLINE_UPSTREAM_CONNECT_TIMEOUT_SECS",
        default_value_t = DEFAULT_UPSTREAM_CONNECT_TIMEOUT_SECS,
        value_parser = parse_upstream_connect_timeout_secs,
        value_name = "SECONDS",
        help = "Maximum time allowed to establish the upstream websocket connection.",
        long_help = "Maximum total time allowed to establish the upstream websocket connection, including DNS, TCP, TLS, and HTTP upgrade."
    )]
    pub upstream_connect_timeout_secs: u64,

    #[arg(
        long,
        env = "THREADLINE_UPSTREAM_PONG_TIMEOUT_SECS",
        default_value_t = DEFAULT_UPSTREAM_PONG_TIMEOUT_SECS,
        value_parser = parse_upstream_watchdog_timeout_secs,
        value_name = "SECONDS",
        help = "Maximum time to wait for a matching upstream websocket Pong.",
        long_help = "Maximum time to wait for a matching upstream websocket Pong after Threadline sends a liveness challenge."
    )]
    pub upstream_pong_timeout_secs: u64,

    #[arg(
        long,
        env = "THREADLINE_UPSTREAM_WRITE_TIMEOUT_SECS",
        default_value_t = DEFAULT_UPSTREAM_WRITE_TIMEOUT_SECS,
        value_parser = parse_upstream_watchdog_timeout_secs,
        value_name = "SECONDS",
        help = "Maximum time allowed for an upstream websocket write or control flush.",
        long_help = "Maximum total time allowed for one upstream websocket Text, Ping, or automatic control-frame flush operation."
    )]
    pub upstream_write_timeout_secs: u64,

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

    #[arg(
        long,
        default_value_t = false,
        help = "Write safe upstream websocket terminal diagnostics to stderr.",
        long_help = "Write one safe upstream websocket terminal diagnostic to stderr per connection, independently of tracing and --log-level. Disabled by default; CLI-only with no environment variable."
    )]
    pub ws_close_diagnostics: bool,
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
            upstream_connect_timeout_secs: DEFAULT_UPSTREAM_CONNECT_TIMEOUT_SECS,
            upstream_pong_timeout_secs: DEFAULT_UPSTREAM_PONG_TIMEOUT_SECS,
            upstream_write_timeout_secs: DEFAULT_UPSTREAM_WRITE_TIMEOUT_SECS,
            ws_close_diagnostics: false,
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

    pub fn upstream_connect_timeout(&self) -> Result<Duration, &'static str> {
        if (1..=MAX_UPSTREAM_CONNECT_TIMEOUT_SECS).contains(&self.upstream_connect_timeout_secs) {
            Ok(Duration::from_secs(self.upstream_connect_timeout_secs))
        } else {
            Err("upstream connect timeout must be between 1 and 3600 seconds")
        }
    }

    pub fn upstream_watchdog_policy(&self) -> Result<UpstreamWatchdogPolicy, &'static str> {
        UpstreamWatchdogPolicy::new(
            Duration::from_secs(self.upstream_pong_timeout_secs),
            Duration::from_secs(self.upstream_write_timeout_secs),
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

fn parse_upstream_connect_timeout_secs(value: &str) -> Result<u64, String> {
    parse_upstream_timeout_secs(value)
}

fn parse_upstream_watchdog_timeout_secs(value: &str) -> Result<u64, String> {
    parse_upstream_timeout_secs(value)
}

fn parse_upstream_timeout_secs(value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| "must be an integer between 1 and 3600".to_string())?;
    if (1..=MAX_UPSTREAM_CONNECT_TIMEOUT_SECS).contains(&parsed) {
        Ok(parsed)
    } else {
        Err("must be between 1 and 3600".to_string())
    }
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
mod tests;
