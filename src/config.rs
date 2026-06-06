use std::net::{IpAddr, SocketAddr};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use clap::Parser;

use crate::jobs::ThreadlineJobManagerConfig;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8787;
const DEFAULT_MODEL_ID: &str = "codex-mini-latest";
const DEFAULT_RETAINED_SESSION_CAPACITY: usize = 64;
const DEFAULT_JOBS_ENABLED: bool = false;
const DEFAULT_JOB_OUTPUT_BUFFER_LIMIT_BYTES: usize = 32 * 1024;
const DEFAULT_JOB_RETENTION_TTL_SECS: u64 = 300;
const DEFAULT_LOG_LEVEL: &str = "info";

static ACTIVE_JOB_MANAGER_CONFIG: LazyLock<Mutex<ThreadlineJobManagerConfig>> =
    LazyLock::new(|| Mutex::new(ThreadlineJobManagerConfig::default()));

#[derive(Debug, Clone, Parser)]
#[command(name = "threadline", about = "Threadline BYOK bridge")]
pub struct ThreadlineConfig {
    #[arg(long, env = "THREADLINE_HOST", default_value = DEFAULT_HOST)]
    pub host: String,

    #[arg(long, env = "THREADLINE_PORT", default_value_t = DEFAULT_PORT)]
    pub port: u16,

    #[arg(long, env = "THREADLINE_MODEL_ID", default_value = DEFAULT_MODEL_ID)]
    pub model_id: String,

    #[arg(
        long,
        env = "THREADLINE_RETAINED_SESSION_CAPACITY",
        default_value_t = DEFAULT_RETAINED_SESSION_CAPACITY
    )]
    pub retained_session_capacity: usize,

    #[arg(long, env = "THREADLINE_JOBS_ENABLED", default_value_t = DEFAULT_JOBS_ENABLED)]
    pub jobs_enabled: bool,

    #[arg(
        long,
        env = "THREADLINE_JOB_OUTPUT_BUFFER_LIMIT_BYTES",
        default_value_t = DEFAULT_JOB_OUTPUT_BUFFER_LIMIT_BYTES
    )]
    pub job_output_buffer_limit_bytes: usize,

    #[arg(
        long,
        env = "THREADLINE_JOB_RETENTION_TTL_SECS",
        default_value_t = DEFAULT_JOB_RETENTION_TTL_SECS
    )]
    pub job_retention_ttl_secs: u64,

    #[arg(long, env = "THREADLINE_JOB_ALLOWED_COMMANDS")]
    pub job_allowed_commands: Option<String>,

    #[arg(long, env = "THREADLINE_LOG_LEVEL", default_value = DEFAULT_LOG_LEVEL)]
    pub log_level: String,
}

impl Default for ThreadlineConfig {
    fn default() -> Self {
        let config = Self {
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            model_id: DEFAULT_MODEL_ID.to_string(),
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
        let config = Self::parse();
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
