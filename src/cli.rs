use std::fmt;

use clap::{Args, Parser, Subcommand};

use crate::config::ThreadlineConfig;

#[derive(Debug, Clone, Parser, PartialEq, Eq)]
#[command(name = "threadline", about = "Threadline BYOK bridge")]
pub struct ThreadlineCli {
    #[command(flatten)]
    pub server: ThreadlineConfig,

    #[command(subcommand)]
    pub command: Option<ThreadlineCommand>,
}

#[derive(Debug, Clone, Subcommand, PartialEq, Eq)]
pub enum ThreadlineCommand {
    Login(LoginCommand),
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct LoginCommand {
    #[command(subcommand)]
    pub action: LoginSubcommand,
}

#[derive(Debug, Clone, Subcommand, PartialEq, Eq)]
pub enum LoginSubcommand {
    Store(LoginStoreCommand),
    Status,
    Logout,
}

#[derive(Clone, Args, PartialEq, Eq)]
pub struct LoginStoreCommand {
    #[arg(long)]
    pub refresh_token: Option<String>,
}

impl fmt::Debug for LoginStoreCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoginStoreCommand")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadlineCliAction {
    StartServer(ThreadlineConfig),
    LoginStore(LoginStoreCommand),
    LoginStatus,
    LoginLogout,
}

impl ThreadlineCli {
    pub fn into_action(self) -> ThreadlineCliAction {
        match self.command {
            None => ThreadlineCliAction::StartServer(self.server),
            Some(ThreadlineCommand::Login(command)) => match command.action {
                LoginSubcommand::Store(command) => ThreadlineCliAction::LoginStore(command),
                LoginSubcommand::Status => ThreadlineCliAction::LoginStatus,
                LoginSubcommand::Logout => ThreadlineCliAction::LoginLogout,
            },
        }
    }
}

#[cfg(test)]
mod login_cli_tests {
    use clap::Parser;

    use super::{ThreadlineCli, ThreadlineCliAction, ThreadlineCommand};

    #[test]
    fn server_starts_by_default_without_subcommand() {
        let cli = ThreadlineCli::try_parse_from(["threadline"]).expect("cli should parse");

        assert!(cli.command.is_none());
        assert!(matches!(
            cli.into_action(),
            ThreadlineCliAction::StartServer(_)
        ));
    }

    #[test]
    fn config_server_flags_survive_subcommand_refactor() {
        let cli = ThreadlineCli::try_parse_from([
            "threadline",
            "--host",
            "0.0.0.0",
            "--port",
            "9100",
            "--model-id",
            "codex-test",
            "--retained-session-capacity",
            "9",
            "--jobs-enabled",
        ])
        .expect("top-level server flags should still parse");

        assert_eq!(cli.server.host, "0.0.0.0");
        assert_eq!(cli.server.port, 9100);
        assert_eq!(cli.server.model_id, "codex-test");
        assert_eq!(cli.server.retained_session_capacity, 9);
        assert!(cli.server.jobs_enabled);
    }

    #[test]
    fn login_command_accepts_bare_login_only() {
        let cli =
            ThreadlineCli::try_parse_from(["threadline", "login"]).expect("login should parse");

        assert!(matches!(
            cli.command,
            Some(ThreadlineCommand::Login(_))
        ));
    }

    #[test]
    fn login_command_rejects_removed_nested_subcommands() {
        for command in [
            ["threadline", "login", "store"],
            ["threadline", "login", "status"],
            ["threadline", "login", "logout"],
        ] {
            ThreadlineCli::try_parse_from(command)
                .expect_err("removed login subcommand should not parse");
        }
    }
}
