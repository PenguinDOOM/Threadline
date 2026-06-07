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

    use super::{LoginSubcommand, ThreadlineCli, ThreadlineCliAction, ThreadlineCommand};

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
    fn login_command_parses_store_status_and_logout_actions() {
        let store = ThreadlineCli::try_parse_from([
            "threadline",
            "login",
            "store",
            "--refresh-token",
            "refresh-value",
        ])
        .expect("store command should parse");
        let status = ThreadlineCli::try_parse_from(["threadline", "login", "status"])
            .expect("status command should parse");
        let logout = ThreadlineCli::try_parse_from(["threadline", "login", "logout"])
            .expect("logout command should parse");

        assert!(matches!(
            store.command,
            Some(ThreadlineCommand::Login(command))
                if matches!(command.action, LoginSubcommand::Store(_))
        ));
        assert!(matches!(
            status.command,
            Some(ThreadlineCommand::Login(command))
                if matches!(command.action, LoginSubcommand::Status)
        ));
        assert!(matches!(
            logout.command,
            Some(ThreadlineCommand::Login(command))
                if matches!(command.action, LoginSubcommand::Logout)
        ));
    }

    #[test]
    fn login_store_rejects_visible_token_flag() {
        let error = ThreadlineCli::try_parse_from([
            "threadline",
            "login",
            "store",
            "--token",
            "token-value",
        ])
        .expect_err("visible token flag should no longer parse");

        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn login_store_command_debug_redacts_refresh_token() {
        let store = ThreadlineCli::try_parse_from([
            "threadline",
            "login",
            "store",
            "--refresh-token",
            "refresh-value",
        ])
        .expect("store command should parse");

        let debug = format!("{store:?}");

        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("refresh-value"));
    }
}
