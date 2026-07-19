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
        let supported_aliases_section = readme_section_containing(&readme, "Main profile aliases:")
            .expect("README should document the supported model alias list");
        let utility_aliases_section =
            readme_section_containing(&readme, "Utility profile aliases:")
                .expect("README should document the supported Utility model alias list");
        let unreleased_caveat = readme_section_containing(
            &readme,
            "The `gpt-5.6-sol`, `gpt-5.6-terra`, and `gpt-5.6-luna` entries are next models.",
        )
        .expect("README should document the GPT-5.6 unreleased caveat");
        let raw_upstream_ids_section =
            readme_section_containing(&readme, "The upstream model ids sent to Codex remain")
                .expect("README should explain raw upstream model ids");
        let all_turns_caveat =
            readme_section_containing(&readme, "Persistent CoT with `reasoning.context=all_turns`")
                .expect("README should document the raw compatibility all-turns caveat");
        let custom_endpoint_section =
            readme_section_containing(&readme, "\"id\": \"threadline-main-gpt-5.6-sol\"")
                .expect("README should include the VS Code custom endpoint JSON example");
        let custom_endpoint_json = custom_endpoint_section
            .split_once("```json")
            .and_then(|(_, section)| section.split_once("```"))
            .map(|(json, _)| json.trim())
            .expect("README custom endpoint example should be a fenced JSON block");
        let custom_endpoint_document: Value = serde_json::from_str(custom_endpoint_json)
            .expect("README custom endpoint example should contain valid JSON");

        assert!(!readme.contains(&removed_flag));
        assert!(!readme.contains(&removed_env_var));

        for visible_alias in [
            "threadline-main-gpt-5.6-sol",
            "threadline-main-gpt-5.6-terra",
            "threadline-main-gpt-5.6-luna",
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
            utility_aliases_section.contains("threadline-utility-gpt-5.6-luna"),
            "README should list the Utility Luna alias in the Utility aliases section"
        );
        let utility_endpoint = custom_endpoint_document
            .get("chat.customEndpoints")
            .and_then(Value::as_array)
            .and_then(|endpoints| {
                endpoints.iter().find(|endpoint| {
                    endpoint.get("uri").and_then(Value::as_str) == Some("http://127.0.0.1:8101/v1")
                })
            })
            .expect("README should include the Utility custom endpoint");
        let utility_luna_model = utility_endpoint
            .get("models")
            .and_then(Value::as_array)
            .and_then(|models| {
                models.iter().find(|model| {
                    model.get("id").and_then(Value::as_str)
                        == Some("threadline-utility-gpt-5.6-luna")
                })
            })
            .expect("README should include the Utility Luna model");
        assert_eq!(
            utility_luna_model.get("name").and_then(Value::as_str),
            Some("Threadline Utility GPT-5.6 Luna")
        );
        assert_eq!(
            utility_luna_model
                .get("supportsReasoningEffort")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(
            custom_endpoint_json.contains(
                "\"chat.utilityModel\": \"customendpoint/threadline-utility-gpt-5.4-mini\""
            ),
            "README should keep the default Utility model selector on GPT-5.4 Mini"
        );
        assert!(
            custom_endpoint_json.contains(
                "\"chat.utilitySmallModel\": \"customendpoint/threadline-utility-gpt-5.4-mini\""
            ),
            "README should keep the small Utility model selector on GPT-5.4 Mini"
        );

        for raw_model_id in ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"] {
            assert!(
                raw_upstream_ids_section.contains(raw_model_id),
                "README should explain raw upstream id {raw_model_id} in the upstream-id section"
            );
            assert!(
                all_turns_caveat.contains(raw_model_id),
                "README should include raw compatibility id {raw_model_id} in the all-turns caveat"
            );
        }

        assert!(
            raw_upstream_ids_section
                .contains("These visible ids are aliases for VS Code selection and routing."),
            "README should distinguish visible aliases from raw upstream ids"
        );
        assert!(
            raw_upstream_ids_section.contains("The upstream model ids sent to Codex remain `gpt-*` ids such as `gpt-5.6-sol`, `gpt-5.6-terra`, `gpt-5.6-luna`"),
            "README raw upstream-id explanation should explicitly list the raw gpt-5.6 ids"
        );
        assert!(
            unreleased_caveat.contains("Threadline currently covers local advertisement, validation, and `model`-field rewriting for those ids."),
            "README should describe the GPT-5.6 entries as local-only coverage"
        );
        assert!(
            unreleased_caveat.contains("Live upstream behavior remains unverified until upstream release makes direct testing possible."),
            "README should keep the GPT-5.6 caveat explicitly unverified upstream"
        );
        assert!(
            !unreleased_caveat.contains("live upstream verification"),
            "README should not claim live upstream verification for unreleased GPT-5.6 ids"
        );

        for raw_model_id in ["gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex-spark"] {
            assert!(
                raw_upstream_ids_section.contains(raw_model_id),
                "README should list supported raw model id {raw_model_id} in the upstream-id section"
            );
        }

        for all_turns_raw_model_id in ["gpt-5.5", "gpt-5.4"] {
            assert!(
                all_turns_caveat.contains(all_turns_raw_model_id),
                "README should keep raw compatibility id {all_turns_raw_model_id} in the all-turns caveat"
            );
        }
    }

    #[test]
    fn readme_documents_supported_configuration_flags() {
        let readme = readme_text();

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

        let persistent_reasoning_scope = readme_section_containing(
            &readme,
            "The previous experimental unconditional injection is now default-off.",
        )
        .expect("README should document persistent reasoning eligibility");
        assert!(
            persistent_reasoning_scope.contains("reasoning.context=all_turns")
                && persistent_reasoning_scope.contains("only to eligible Main requests"),
            "README should describe persistent reasoning as eligible Main-only injection"
        );
        for advertised_main_alias in [
            "threadline-main-gpt-5.6-sol",
            "threadline-main-gpt-5.6-terra",
            "threadline-main-gpt-5.6-luna",
        ] {
            assert!(
                persistent_reasoning_scope.contains(advertised_main_alias),
                "README should identify advertised Main alias {advertised_main_alias} as eligible"
            );
        }
        for raw_main_compatibility_id in ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"] {
            assert!(
                persistent_reasoning_scope.contains(raw_main_compatibility_id),
                "README should identify raw Main compatibility id {raw_main_compatibility_id} as eligible"
            );
        }
        assert!(
            persistent_reasoning_scope.contains("support that client-explicit value on Main")
                && persistent_reasoning_scope.contains("also eligible for server-side injection"),
            "README should document explicit and automatic persistent reasoning for raw GPT-5.6 Main compatibility ids"
        );
        assert!(
            persistent_reasoning_scope.contains("raw compatibility ids `gpt-5.5` and `gpt-5.4`")
                && persistent_reasoning_scope.contains("remain excluded from automatic injection")
                && persistent_reasoning_scope
                    .contains("continue to reject client-explicit `reasoning.context=all_turns`"),
            "README should retain the GPT-5.5 and GPT-5.4 raw compatibility caveat"
        );
        assert!(
            persistent_reasoning_scope
                .contains("Utility requests remain excluded from automatic injection")
                && persistent_reasoning_scope.contains(
                    "supporting Utility aliases preserve client-explicit `reasoning.context=all_turns`",
                ),
            "README should distinguish Utility automatic ineligibility from client-explicit all_turns support"
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
            normalized_about.contains("does not") || normalized_about.contains("without storing"),
            "login help should avoid implying credential storage behavior, got {about_text:?}"
        );
    }
}
