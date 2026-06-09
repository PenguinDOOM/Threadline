use std::process::ExitCode;
use std::{io, io::Read};

use clap::Parser;
use threadline::auth::{
    AuthCommandError, ThreadlineLoginInput, logout_threadline_credentials,
    store_threadline_credentials, threadline_login_status,
};
use threadline::cli::{LoginStoreCommand, ThreadlineCli, ThreadlineCliAction};
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

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = ThreadlineCli::parse();

    match cli.into_action() {
        ThreadlineCliAction::StartServer(config) => run_server(config).await.map_err(Into::into),
        ThreadlineCliAction::LoginStore(command) => {
            let input = read_login_input(command, &mut io::stdin())?;
            let _status = store_threadline_credentials(&input)?;
            println!("Stored Threadline credentials in the OS credential manager.");
            Ok(())
        }
        ThreadlineCliAction::LoginStatus => {
            let status = threadline_login_status()?;
            println!("{}", status.render());
            Ok(())
        }
        ThreadlineCliAction::LoginLogout => {
            let removed = logout_threadline_credentials()?;
            if removed {
                println!("Removed Threadline credentials from the OS credential manager.");
            } else {
                println!("Threadline credentials were not present.");
            }
            Ok(())
        }
    }
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

fn read_login_input(
    command: LoginStoreCommand,
    reader: &mut impl Read,
) -> Result<ThreadlineLoginInput, Box<dyn std::error::Error>> {
    let bearer_token = read_login_token_from_reader(reader)?;

    Ok(ThreadlineLoginInput {
        bearer_token,
        refresh_token: command.refresh_token,
    })
}

fn read_login_token_from_reader(reader: &mut impl Read) -> Result<String, AuthCommandError> {
    let mut buffer = String::new();
    reader
        .read_to_string(&mut buffer)
        .map_err(|_| AuthCommandError::MissingToken)?;

    let token = buffer.trim();
    if token.is_empty() {
        return Err(AuthCommandError::MissingToken);
    }

    Ok(token.to_string())
}
