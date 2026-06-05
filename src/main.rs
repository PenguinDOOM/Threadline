use std::process::ExitCode;

use threadline::config::ThreadlineConfig;
use threadline::errors::ThreadlineError;
use threadline::http::build_router;
use tracing::info;
use tracing_subscriber::EnvFilter;

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

async fn run() -> Result<(), ThreadlineError> {
    let config = ThreadlineConfig::from_env();
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
