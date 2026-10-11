use clap::{Parser, Subcommand};

use crate::config::ThreadlineConfig;

#[derive(Debug, Clone, Parser, PartialEq, Eq)]
#[command(
    name = "threadline",
    about = "Bridge VSCode BYOK /v1/responses requests to Codex upstream sessions.",
    long_about = "Bridge VSCode BYOK /v1/responses requests to Codex upstream sessions. Run without a subcommand to start the local downstream server."
)]
pub struct ThreadlineCli {
    #[command(flatten)]
    pub server: ThreadlineConfig,

    #[command(subcommand)]
    pub command: Option<ThreadlineCommand>,
}

#[derive(Debug, Clone, Subcommand, PartialEq, Eq)]
pub enum ThreadlineCommand {
    #[command(
        about = "Show sign in guidance for Codex credentials.",
        long_about = "Show sign in guidance for Codex credentials. This command provides informational instructions only and does not store, delete, or inspect credentials."
    )]
    Login,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadlineCliAction {
    StartServer(ThreadlineConfig),
    LoginInstructions,
}

impl ThreadlineCli {
    pub fn into_action(self) -> ThreadlineCliAction {
        match self.command {
            None => ThreadlineCliAction::StartServer(self.server),
            Some(ThreadlineCommand::Login) => ThreadlineCliAction::LoginInstructions,
        }
    }
}

#[cfg(test)]
mod login_cli_tests;
