use std::net::{IpAddr, SocketAddr};

use clap::Parser;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8787;
const DEFAULT_MODEL_ID: &str = "codex-mini-latest";
const DEFAULT_RETAINED_SESSION_CAPACITY: usize = 64;
const DEFAULT_LOG_LEVEL: &str = "info";

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

    #[arg(long, env = "THREADLINE_JOBS_ENABLED", default_value_t = true)]
    pub jobs_enabled: bool,

    #[arg(long, env = "THREADLINE_LOG_LEVEL", default_value = DEFAULT_LOG_LEVEL)]
    pub log_level: String,
}

impl Default for ThreadlineConfig {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            model_id: DEFAULT_MODEL_ID.to_string(),
            retained_session_capacity: DEFAULT_RETAINED_SESSION_CAPACITY,
            jobs_enabled: true,
            log_level: DEFAULT_LOG_LEVEL.to_string(),
        }
    }
}

impl ThreadlineConfig {
    pub fn from_env() -> Self {
        Self::parse()
    }

    pub fn bind_address(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        let host: IpAddr = self.host.parse()?;
        Ok(SocketAddr::from((host, self.port)))
    }
}
