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
mod login_cli_tests {
    use std::fs;
    use std::path::PathBuf;

    use clap::{Command, CommandFactory, Parser};

    use super::{ThreadlineCli, ThreadlineCliAction, ThreadlineCommand};

    fn removed_model_flag() -> String {
        ["--", "model-id"].concat()
    }

    fn removed_model_env_var() -> String {
        ["THREADLINE", "MODEL", "ID"].join("_")
    }

    fn readme_text() -> String {
        let readme_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("README.md");
        fs::read_to_string(readme_path).expect("readme should be readable")
    }

    fn readme_section_containing(readme: &str, needle: &str) -> Option<String> {
        let start = readme.find(needle)?;
        let section_start = readme[..start].rfind("\n\n").map(|idx| idx + 2).unwrap_or(0);
        let section_end = readme[start..]
            .find("\n\n")
            .map(|idx| start + idx)
            .unwrap_or(readme.len());

        Some(readme[section_start..section_end].to_string())
    }

    fn login_subcommand(command: &Command) -> &Command {
        command
            .find_subcommand("login")
            .expect("login subcommand should exist")
    }

    fn command_about_text(command: &Command) -> String {
        [command.get_about(), command.get_long_about()]
            .into_iter()
            .flatten()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_string()
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
        let readme = readme_text();
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
    fn readme_documents_supported_configuration_flags() {
        let readme = readme_text();

        for (flag, env_var, stable_default) in [
            ("--host", "THREADLINE_HOST", Some("127.0.0.1")),
            ("--port", "THREADLINE_PORT", Some("8100")),
            (
                "--codex-client-version",
                "THREADLINE_CODEX_CLIENT_VERSION",
                Some("0.136.0"),
            ),
            (
                "--retained-session-capacity",
                "THREADLINE_RETAINED_SESSION_CAPACITY",
                Some("64"),
            ),
            ("--jobs-enabled", "THREADLINE_JOBS_ENABLED", Some("false")),
            (
                "--job-output-buffer-limit-bytes",
                "THREADLINE_JOB_OUTPUT_BUFFER_LIMIT_BYTES",
                Some("32768"),
            ),
            (
                "--job-retention-ttl-secs",
                "THREADLINE_JOB_RETENTION_TTL_SECS",
                Some("300"),
            ),
            (
                "--job-allowed-commands",
                "THREADLINE_JOB_ALLOWED_COMMANDS",
                None,
            ),
            ("--log-level", "THREADLINE_LOG_LEVEL", Some("info")),
        ] {
            assert!(
                readme.contains(flag),
                "README should document supported flag {flag}"
            );
            assert!(
                readme.contains(env_var),
                "README should document environment variable {env_var}"
            );

            if let Some(stable_default) = stable_default {
                assert!(
                    readme.contains(stable_default),
                    "README should document stable default {stable_default} for {flag}"
                );
            }
        }

        assert!(
            readme.contains("comma-separated"),
            "README should describe --job-allowed-commands as comma-separated"
        );
        assert!(
            readme.contains("exact program names"),
            "README should describe --job-allowed-commands as exact program names"
        );
        let job_allowed_section = readme_section_containing(&readme, "--job-allowed-commands")
            .expect("README should describe --job-allowed-commands in one section");
        let normalized_job_allowed_section = job_allowed_section.to_ascii_lowercase();
        assert!(
            ![
                "supports prefix matching",
                "supports program prefixes",
                "allows program prefixes",
                "prefixes are allowed",
                "matches command prefixes",
            ]
            .iter()
            .any(|phrase| normalized_job_allowed_section.contains(phrase)),
            "README should not describe --job-allowed-commands as prefix-based, got section {job_allowed_section:?}"
        );

        let removed_flag = removed_model_flag();
        let removed_env_var = removed_model_env_var();
        assert!(!readme.contains(&removed_flag));
        assert!(!readme.contains(&removed_env_var));
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

    #[test]
    fn login_help_describes_informational_credentials_guidance() {
        let command = ThreadlineCli::command();
        let login_command = login_subcommand(&command);
        let about_text = command_about_text(login_command);
        let normalized_about = about_text.to_ascii_lowercase();

        assert!(
            !about_text.is_empty(),
            "login subcommand should describe its informational help surface"
        );
        assert!(
            normalized_about.contains("sign in") || normalized_about.contains("login"),
            "login help should mention sign-in guidance, got {about_text:?}"
        );
        assert!(
            normalized_about.contains("instruction")
                || normalized_about.contains("guidance")
                || normalized_about.contains("information"),
            "login help should describe informational-only guidance, got {about_text:?}"
        );
        assert!(
            normalized_about.contains("does not")
                || normalized_about.contains("without storing"),
            "login help should avoid implying credential storage behavior, got {about_text:?}"
        );
    }
}
