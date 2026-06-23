use std::process::ExitCode;

use clap::Parser;
use threadline::cli::{ThreadlineCli, ThreadlineCliAction};
use threadline::config::ThreadlineConfig;
use threadline::errors::ThreadlineError;
use threadline::http::build_router;
use threadline::models::RouteProfile;
use tracing::info;
use tracing_subscriber::EnvFilter;

const LOGIN_INSTRUCTIONS_MESSAGE: &str = "Threadline does not store credentials. Sign in with Codex Desktop or Codex CLI, then run Threadline again.";
const UTILITY_PORT_REQUIRES_MAIN_PROFILE_MESSAGE: &str =
    "--utility-port can only be used with the main profile";
const UTILITY_PORT_MUST_DIFFER_MESSAGE: &str = "--utility-port must differ from --port";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("threadline startup failed: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = ThreadlineCli::parse();

    match cli.into_action() {
        ThreadlineCliAction::StartServer(config) => run_server(config).await.map_err(Into::into),
        ThreadlineCliAction::LoginInstructions => {
            println!("{}", login_instructions_message());
            Ok(())
        }
    }
}

fn login_instructions_message() -> &'static str {
    LOGIN_INSTRUCTIONS_MESSAGE
}

async fn run_server(config: ThreadlineConfig) -> Result<(), ThreadlineError> {
    init_tracing(&config);

    match config.utility_port {
        Some(utility_port) => run_main_and_utility_servers(config, utility_port).await,
        None => serve_config(config).await,
    }
}

async fn run_main_and_utility_servers(
    main_config: ThreadlineConfig,
    utility_port: u16,
) -> Result<(), ThreadlineError> {
    let (main_config, utility_config) =
        split_main_and_utility_configs(main_config, utility_port)?;

    tokio::try_join!(serve_config(main_config), serve_config(utility_config))?;

    Ok(())
}

async fn serve_config(config: ThreadlineConfig) -> Result<(), ThreadlineError> {
    let bind_address = config
        .bind_address()
        .map_err(|_| ThreadlineError::InvalidBindHost(config.host.clone()))?;
    let profile = config.profile;
    let listener = tokio::net::TcpListener::bind(bind_address)
        .await
        .map_err(|_| ThreadlineError::InvalidBindHost(bind_address.ip().to_string()))?;
    let app = build_router(config);

    info!(address = %bind_address, profile = %profile, "threadline_http_server_started");

    axum::serve(listener, app)
        .await
        .map_err(|_| ThreadlineError::InvalidBindHost(bind_address.ip().to_string()))
}

fn split_main_and_utility_configs(
    main_config: ThreadlineConfig,
    utility_port: u16,
) -> Result<(ThreadlineConfig, ThreadlineConfig), ThreadlineError> {
    if main_config.profile != RouteProfile::Main {
        return Err(ThreadlineError::InvalidServerConfiguration(
            UTILITY_PORT_REQUIRES_MAIN_PROFILE_MESSAGE.to_string(),
        ));
    }

    if main_config.port == utility_port {
        return Err(ThreadlineError::InvalidServerConfiguration(
            UTILITY_PORT_MUST_DIFFER_MESSAGE.to_string(),
        ));
    }

    let mut utility_config = main_config.clone();
    utility_config.port = utility_port;
    utility_config.profile = RouteProfile::Utility;
    utility_config.retained_session_capacity = 0;
    utility_config.jobs_enabled = false;

    Ok((main_config, utility_config))
}

fn init_tracing(config: &ThreadlineConfig) {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(config.log_level.clone()))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::sync::Mutex;

    use clap::Parser;
    use threadline::models::RouteProfile;

    use super::*;

    static THREADLINE_PROFILE_ENV_LOCK: Mutex<()> = Mutex::new(());
    static THREADLINE_UTILITY_PORT_ENV_LOCK: Mutex<()> = Mutex::new(());

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

    fn utility_port_restricted_to_main_message() -> &'static str {
        "--utility-port can only be used with the main profile"
    }

    fn utility_port_must_differ_message() -> &'static str {
        "--utility-port must differ from --port"
    }

    #[test]
    fn split_main_and_utility_configs_accepts_main_profile_with_different_port() {
        let main_config = ThreadlineConfig {
            port: 8100,
            utility_port: Some(8101),
            ..ThreadlineConfig::default()
        };

        let (resolved_main, utility_config) =
            split_main_and_utility_configs(main_config.clone(), 8101).expect("config split");

        assert_eq!(resolved_main, main_config);
        assert_eq!(utility_config.port, 8101);
        assert_eq!(utility_config.profile, RouteProfile::Utility);
    }

    #[test]
    fn split_main_and_utility_configs_rejects_utility_profile() {
        let main_config = ThreadlineConfig {
            profile: RouteProfile::Utility,
            utility_port: Some(8101),
            ..ThreadlineConfig::default()
        };

        let error = split_main_and_utility_configs(main_config, 8101)
            .expect_err("utility profile should be rejected");

        assert!(matches!(
            error,
            ThreadlineError::InvalidServerConfiguration(message)
                if message == utility_port_restricted_to_main_message()
        ));
    }

    #[test]
    fn split_main_and_utility_configs_rejects_same_port() {
        let main_config = ThreadlineConfig {
            port: 8100,
            utility_port: Some(8100),
            ..ThreadlineConfig::default()
        };

        let error = split_main_and_utility_configs(main_config, 8100)
            .expect_err("same utility port should be rejected");

        assert!(matches!(
            error,
            ThreadlineError::InvalidServerConfiguration(message)
                if message == utility_port_must_differ_message()
        ));
    }

    #[test]
    fn split_main_and_utility_configs_derives_stateless_utility_config() {
        let main_config = ThreadlineConfig {
            port: 8100,
            utility_port: Some(8101),
            retained_session_capacity: 9,
            jobs_enabled: true,
            ..ThreadlineConfig::default()
        };

        let (_, utility_config) =
            split_main_and_utility_configs(main_config.clone(), 8101).expect("config split");

        assert_eq!(utility_config.profile, RouteProfile::Utility);
        assert_eq!(utility_config.port, 8101);
        assert_eq!(utility_config.retained_session_capacity, 0);
        assert!(!utility_config.jobs_enabled);

        let mut expected_utility = main_config;
        expected_utility.port = 8101;
        expected_utility.profile = RouteProfile::Utility;
        expected_utility.retained_session_capacity = 0;
        expected_utility.jobs_enabled = false;

        assert_eq!(utility_config, expected_utility);
    }

    #[test]
    fn split_main_and_utility_configs_preserves_main_job_and_retention_settings() {
        let main_config = ThreadlineConfig {
            retained_session_capacity: 11,
            jobs_enabled: true,
            utility_port: Some(8101),
            ..ThreadlineConfig::default()
        };

        let (resolved_main, _) =
            split_main_and_utility_configs(main_config.clone(), 8101).expect("config split");

        assert_eq!(resolved_main.retained_session_capacity, 11);
        assert!(resolved_main.jobs_enabled);
        assert_eq!(resolved_main, main_config);
    }

    #[test]
    fn split_main_and_utility_configs_rejects_env_derived_utility_profile() {
        let _profile_lock = THREADLINE_PROFILE_ENV_LOCK
            .lock()
            .expect("profile env lock");
        let _utility_port_lock = THREADLINE_UTILITY_PORT_ENV_LOCK
            .lock()
            .expect("utility port env lock");
        let _profile_guard = ProfileEnvGuard::acquire();
        let _utility_port_guard = UtilityPortEnvGuard::acquire();

        unsafe { std::env::set_var("THREADLINE_PROFILE", "utility") };
        unsafe { std::env::set_var("THREADLINE_UTILITY_PORT", "8101") };

        let config = ThreadlineCli::parse_from(["threadline"]).server;
        let utility_port = config.utility_port.expect("utility port from env");
        let error = split_main_and_utility_configs(config, utility_port)
            .expect_err("utility profile from env should be rejected");

        assert!(matches!(
            error,
            ThreadlineError::InvalidServerConfiguration(message)
                if message == utility_port_restricted_to_main_message()
        ));
    }
}
