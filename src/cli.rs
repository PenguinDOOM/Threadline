use clap::{Parser, Subcommand};

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

        assert!(matches!(cli.command, Some(ThreadlineCommand::Login)));
        assert_eq!(cli.into_action(), ThreadlineCliAction::LoginInstructions);
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
