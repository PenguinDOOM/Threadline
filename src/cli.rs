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
    use serde_json::Value;

    use super::{ThreadlineCli, ThreadlineCliAction, ThreadlineCommand};
    use crate::config::THREADLINE_ENV_LOCK;

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
        let section_start = readme[..start]
            .rfind("\n\n")
            .map(|idx| idx + 2)
            .unwrap_or(0);
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
    fn ws_close_diagnostics_is_default_off_and_cli_only() {
        let _env_lock = THREADLINE_ENV_LOCK.lock().expect("environment lock");
        assert!(!crate::config::ThreadlineConfig::default().ws_close_diagnostics);
        assert!(
            !ThreadlineCli::try_parse_from(["threadline"])
                .expect("default cli")
                .server
                .ws_close_diagnostics
        );
        let enabled = ThreadlineCli::try_parse_from(["threadline", "--ws-close-diagnostics"])
            .expect("bare diagnostic flag");
        assert!(enabled.server.ws_close_diagnostics);
        let off = ThreadlineCli::try_parse_from([
            "threadline",
            "--log-level",
            "off",
            "--ws-close-diagnostics",
        ])
        .expect("diagnostics with tracing off");
        assert!(off.server.ws_close_diagnostics);
        assert_eq!(off.server.log_level, "off");
        let command = ThreadlineCli::command();
        let flag = command
            .get_arguments()
            .find(|argument| argument.get_long() == Some("ws-close-diagnostics"))
            .expect("diagnostic argument");
        assert!(flag.get_env().is_none());
        assert!(flag.get_long_help().unwrap().to_string().contains("stderr"));
    }

    #[test]
    fn server_starts_by_default_without_subcommand() {
        let _env_lock = THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let cli = ThreadlineCli::try_parse_from(["threadline"]).expect("cli should parse");

        assert!(cli.command.is_none());
        assert!(matches!(
            cli.into_action(),
            ThreadlineCliAction::StartServer(_)
        ));
    }

    #[test]
    fn config_server_flags_survive_subcommand_refactor() {
        let _env_lock = THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let cli = ThreadlineCli::try_parse_from([
            "threadline",
            "--host",
            "0.0.0.0",
            "--port",
            "9100",
            "--utility-port",
            "9101",
            "--profile",
            "utility",
            "--retained-session-capacity",
            "9",
            "--jobs-enabled",
        ])
        .expect("top-level server flags should still parse");

        assert_eq!(cli.server.host, "0.0.0.0");
        assert_eq!(cli.server.port, 9100);
        assert_eq!(cli.server.utility_port, Some(9101));
        assert_eq!(cli.server.profile.to_string(), "utility");
        assert_eq!(cli.server.retained_session_capacity, 9);
        assert!(cli.server.jobs_enabled);
    }

    #[test]
    fn removed_model_id_flag_is_rejected() {
        let _env_lock = THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let removed_flag = removed_model_flag();

        ThreadlineCli::try_parse_from(["threadline", removed_flag.as_str(), "gpt-6-sol"])
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
        let readme = readme_text().replace("\r\n", "\n");
        let removed_flag = removed_model_flag();
        let removed_env_var = removed_model_env_var();
        let supported_aliases_section = readme_section_containing(&readme, "Main profile aliases:")
            .expect("README should document the supported model alias list");
        let utility_aliases_section =
            readme_section_containing(&readme, "Utility profile aliases:")
                .expect("README should document the supported Utility model alias list");
        let raw_upstream_ids_section = readme_section_containing(
            &readme,
            "These visible ids are aliases for VS Code selection and routing.",
        )
        .expect("README should explain visible aliases and raw upstream model ids");
        let persistent_reasoning_scope =
            readme_section_containing(&readme, "Persistent reasoning is opt-in")
                .expect("README should document persistent reasoning eligibility");
        let custom_endpoint_section =
            readme_section_containing(&readme, "\"id\": \"threadline-main-gpt-6-sol\"")
                .expect("README should include the VS Code custom endpoint JSON example");
        let custom_endpoint_json = custom_endpoint_section
            .split_once("```json")
            .and_then(|(_, section)| section.split_once("```"))
            .map(|(json, _)| json.trim())
            .expect("README custom endpoint example should be a fenced JSON block");
        let custom_endpoint_document: Value = serde_json::from_str(custom_endpoint_json)
            .expect("README custom endpoint example should contain valid JSON");
        let custom_endpoints = custom_endpoint_document
            .get("chat.customEndpoints")
            .and_then(Value::as_array)
            .expect("README should define custom endpoints as a JSON array");
        assert_eq!(custom_endpoints.len(), 2);

        let main_endpoint = &custom_endpoints[0];
        assert_eq!(
            main_endpoint.get("uri").and_then(Value::as_str),
            Some("http://127.0.0.1:8100/v1")
        );
        let main_model_ids = main_endpoint
            .get("models")
            .and_then(Value::as_array)
            .expect("README Main endpoint should define models as a JSON array")
            .iter()
            .map(|model| {
                model
                    .get("id")
                    .and_then(Value::as_str)
                    .expect("README Main endpoint models should have string ids")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            main_model_ids,
            [
                "threadline-main-gpt-6.1-sol",
                "threadline-main-gpt-6-astra",
                "threadline-main-gpt-6-sol",
                "threadline-main-gpt-6-luna",
            ]
        );

        let utility_endpoint = &custom_endpoints[1];
        assert_eq!(
            utility_endpoint.get("uri").and_then(Value::as_str),
            Some("http://127.0.0.1:8101/v1")
        );
        let utility_model_ids = utility_endpoint
            .get("models")
            .and_then(Value::as_array)
            .expect("README Utility endpoint should define models as a JSON array")
            .iter()
            .map(|model| {
                model
                    .get("id")
                    .and_then(Value::as_str)
                    .expect("README Utility endpoint models should have string ids")
            })
            .collect::<Vec<_>>();
        assert_eq!(utility_model_ids, ["threadline-utility-gpt-6-luna"]);

        assert!(!readme.contains(&removed_flag));
        assert!(!readme.contains(&removed_env_var));

        for visible_alias in [
            "threadline-main-gpt-6.1-sol",
            "threadline-main-gpt-6-astra",
            "threadline-main-gpt-6-sol",
            "threadline-main-gpt-6-luna",
        ] {
            assert!(
                supported_aliases_section.contains(visible_alias),
                "README should list supported alias {visible_alias} in the Supported Model Aliases section"
            );
            assert!(
                custom_endpoint_json.contains(visible_alias),
                "README should include visible alias {visible_alias} in the VS Code custom endpoint JSON"
            );
        }

        assert!(
            utility_aliases_section.contains("threadline-utility-gpt-6-luna"),
            "README should list the Utility Luna alias in the Utility aliases section"
        );
        let utility_luna_model = utility_endpoint
            .get("models")
            .and_then(Value::as_array)
            .and_then(|models| {
                models.iter().find(|model| {
                    model.get("id").and_then(Value::as_str) == Some("threadline-utility-gpt-6-luna")
                })
            })
            .expect("README should include the Utility Luna model");
        assert_eq!(
            utility_luna_model.get("name").and_then(Value::as_str),
            Some("Threadline Utility GPT-6 Luna")
        );
        assert_eq!(
            utility_luna_model
                .get("supportsReasoningEffort")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(
            custom_endpoint_json.contains(
                "\"chat.utilityModel\": \"customendpoint/threadline-utility-gpt-6-luna\""
            ),
            "README should keep the default Utility model selector on GPT-6 Luna"
        );
        assert!(
            custom_endpoint_json.contains(
                "\"chat.utilitySmallModel\": \"customendpoint/threadline-utility-gpt-6-luna\""
            ),
            "README should keep the small Utility model selector on GPT-6 Luna"
        );

        for raw_model_id in ["gpt-6.1-sol", "gpt-6-astra", "gpt-6-sol", "gpt-6-luna"] {
            assert!(
                raw_upstream_ids_section.contains(raw_model_id),
                "README should explain raw upstream id {raw_model_id} in the upstream-id section"
            );
        }

        assert!(
            raw_upstream_ids_section
                .contains("These visible ids are aliases for VS Code selection and routing."),
            "README should distinguish visible aliases from raw upstream ids"
        );
        for eligible_model_name in ["GPT-6.1 Sol", "GPT-6 Astra, Sol, and Luna"] {
            assert!(
                persistent_reasoning_scope.contains(eligible_model_name),
                "README should describe persistent reasoning eligibility for {eligible_model_name}"
            );
        }
        for retired_model_id in [
            "gpt-5.3-codex-spark",
            "gpt-5.4",
            "gpt-5.4-mini",
            "gpt-5.5",
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "threadline-main-gpt-5.4",
            "threadline-main-gpt-5.5",
            "threadline-main-gpt-5.6-sol",
            "threadline-main-gpt-5.6-terra",
            "threadline-main-gpt-5.6-luna",
            "threadline-utility-gpt-5.4-mini",
            "threadline-utility-gpt-5.6-luna",
            "threadline-utility-gpt-5.3-codex-spark",
        ] {
            assert!(
                !readme.contains(retired_model_id),
                "README must not advertise or describe retired model id {retired_model_id}"
            );
        }
        assert!(persistent_reasoning_scope.contains("Main"));
        assert!(persistent_reasoning_scope.contains("Utility"));
    }

    #[test]
    fn readme_documents_supported_configuration_flags() {
        let readme = readme_text().replace("\r\n", "\n");

        for (flag, env_var, stable_default) in [
            ("--host", "THREADLINE_HOST", Some("127.0.0.1")),
            ("--port", "THREADLINE_PORT", Some("8100")),
            ("--utility-port", "THREADLINE_UTILITY_PORT", Some("None")),
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
                "--persistent-reasoning-enabled",
                "THREADLINE_PERSISTENT_REASONING_ENABLED",
                Some("false"),
            ),
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
                "--job-max-active-jobs",
                "THREADLINE_JOB_MAX_ACTIVE_JOBS",
                Some("16"),
            ),
            (
                "--job-max-retained-jobs",
                "THREADLINE_JOB_MAX_RETAINED_JOBS",
                Some("128"),
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

        let main_utility_scope = readme_section_containing(&readme, "The setting is Main-only.")
            .expect("README should document persistent reasoning as Main-only");
        assert!(
            main_utility_scope.contains("Utility listener does not receive persistent reasoning"),
            "README should exclude persistent reasoning from the Utility listener"
        );
        assert!(
            main_utility_scope
                .contains("standalone Utility process also has an effective value of `false`"),
            "README should document the standalone Utility effective value"
        );

        let persistent_reasoning_scope =
            readme_section_containing(&readme, "Persistent reasoning is opt-in")
                .expect("README should document persistent reasoning eligibility");
        assert!(
            persistent_reasoning_scope.contains("reasoning.context=all_turns")
                && persistent_reasoning_scope.contains("eligible Main requests using"),
            "README should describe persistent reasoning as eligible Main-only injection"
        );
        for advertised_main_model_name in ["GPT-6.1 Sol", "GPT-6 Astra, Sol, and Luna"] {
            assert!(
                persistent_reasoning_scope.contains(advertised_main_model_name),
                "README should identify {advertised_main_model_name} as eligible"
            );
        }
        let client_explicit_scope = readme_section_containing(
            &readme,
            "Threadline's server-side setting is independent of VS Code's",
        )
        .expect("README should document client-explicit persistent reasoning");
        assert!(
            client_explicit_scope
                .contains("Main aliases and raw ids support that client-explicit value")
                && client_explicit_scope.contains("also eligible for server-side injection"),
            "README should document explicit and automatic persistent reasoning for eligible Main ids"
        );
        assert!(
            client_explicit_scope
                .contains("preserve client-explicit `reasoning.context=all_turns`"),
            "README should preserve client-explicit all_turns"
        );
        assert!(
            persistent_reasoning_scope.contains("Utility requests are not automatically eligible"),
            "README should exclude Utility requests from automatic persistent reasoning"
        );
        assert!(
            client_explicit_scope.contains("supporting Utility aliases"),
            "README should identify Utility aliases that support client-explicit all_turns"
        );
        assert!(
            client_explicit_scope
                .contains("preserve client-explicit `reasoning.context=all_turns`",),
            "README should preserve client-explicit all_turns"
        );

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
    fn readme_recommends_one_process_dual_listener_startup() {
        let readme = readme_text();

        assert!(
            readme.contains("threadline --port 8100 --jobs-enabled --utility-port 8101"),
            "README should recommend one-process dual-listener startup"
        );
        assert!(
            readme.contains("second stateless Utility listener in the same process"),
            "README should explain that --utility-port starts a second stateless Utility listener in the same process"
        );
        assert!(
            readme.contains("fallback/debug"),
            "README should keep two-process startup documented as fallback/debug guidance"
        );
    }

    #[test]
    fn login_command_accepts_bare_login_only() {
        let _env_lock = THREADLINE_ENV_LOCK.lock().expect("environment lock");
        let cli =
            ThreadlineCli::try_parse_from(["threadline", "login"]).expect("login should parse");

        assert!(matches!(cli.command, Some(ThreadlineCommand::Login)));
        assert_eq!(cli.into_action(), ThreadlineCliAction::LoginInstructions);
    }

    #[test]
    fn login_command_rejects_removed_nested_subcommands() {
        let _env_lock = THREADLINE_ENV_LOCK.lock().expect("environment lock");
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
            normalized_about.contains("does not") || normalized_about.contains("without storing"),
            "login help should avoid implying credential storage behavior, got {about_text:?}"
        );
    }
}
