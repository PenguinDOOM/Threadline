use std::process::ExitCode;

use clap::Parser;
use threadline::cli::{ThreadlineCli, ThreadlineCliAction};
use threadline::config::ThreadlineConfig;
use threadline::errors::ThreadlineError;
use threadline::http::build_router;
use tracing::info;
use tracing_subscriber::EnvFilter;

const LOGIN_INSTRUCTIONS_MESSAGE: &str = "Threadline does not store credentials. Sign in with Codex Desktop or Codex CLI, then run Threadline again.";

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

    let bind_address = config
        .bind_address()
        .map_err(|_| ThreadlineError::InvalidBindHost(config.host.clone()))?;
    let listener = tokio::net::TcpListener::bind(bind_address)
        .await
        .map_err(|_| ThreadlineError::InvalidBindHost(bind_address.ip().to_string()))?;
    let app = build_router(config);

    info!(address = %bind_address, "threadline_http_server_started");

    axum::serve(listener, app)
        .await
        .map_err(|_| ThreadlineError::InvalidBindHost(bind_address.ip().to_string()))
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
