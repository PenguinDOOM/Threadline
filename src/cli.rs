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
    use std::fs;
    use std::path::PathBuf;

    use clap::{CommandFactory, Parser};

    use super::{ThreadlineCli, ThreadlineCliAction, ThreadlineCommand};

    fn removed_model_flag() -> String {
        ["--", "model-id"].concat()
    }

    fn removed_model_env_var() -> String {
        ["THREADLINE", "MODEL", "ID"].join("_")
    }

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
            "--retained-session-capacity",
            "9",
            "--jobs-enabled",
        ])
        .expect("top-level server flags should still parse");

        assert_eq!(cli.server.host, "0.0.0.0");
        assert_eq!(cli.server.port, 9100);
        assert_eq!(cli.server.retained_session_capacity, 9);
        assert!(cli.server.jobs_enabled);
    }

    #[test]
    fn removed_model_id_flag_is_rejected() {
        let removed_flag = removed_model_flag();

        ThreadlineCli::try_parse_from(["threadline", removed_flag.as_str(), "gpt-5.4"])
            .expect_err("removed model-id flag should not parse");
    }

    #[test]
    fn clap_surface_excludes_removed_model_configuration() {
        let command = ThreadlineCli::command();
        let long_flags: Vec<_> = command
            .get_arguments()
            .filter_map(|arg| arg.get_long())
            .collect();
        let env_vars: Vec<_> = command
            .get_arguments()
            .filter_map(|arg| arg.get_env())
            .filter_map(|name| name.to_str())
            .collect();
        let removed_env_var = removed_model_env_var();

        assert!(!long_flags.contains(&"model-id"));
        assert!(!env_vars.contains(&removed_env_var.as_str()));
    }

    #[test]
    fn readme_lists_only_supported_model_ids_without_model_configuration() {
        let readme_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("README.md");
        let readme = fs::read_to_string(readme_path).expect("readme should be readable");
        let removed_flag = removed_model_flag();
        let removed_env_var = removed_model_env_var();

        assert!(!readme.contains(&removed_flag));
        assert!(!readme.contains(&removed_env_var));

        for model_id in ["gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex-spark"] {
            assert!(
                readme.contains(model_id),
                "README should list supported model id {model_id}"
            );
        }
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
