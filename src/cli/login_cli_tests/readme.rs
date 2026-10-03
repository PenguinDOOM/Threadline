use super::*;

#[test]
fn readme_lists_only_supported_model_ids_without_model_configuration() {
    let readme = readme_text().replace("\r\n", "\n");
    let (custom_endpoint_json, document) = readme_custom_endpoint_example(&readme);
    assert_readme_endpoint_catalog(&document);
    assert!(!readme.contains(&removed_model_flag()));
    assert!(!readme.contains(&removed_model_env_var()));
    assert_readme_visible_aliases(&readme, &custom_endpoint_json);
    assert_readme_utility_model(&document, &custom_endpoint_json);
    assert_readme_model_policies(&readme);
    assert_retired_models_absent(&readme);
}

fn readme_custom_endpoint_example(readme: &str) -> (String, Value) {
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

    (custom_endpoint_json.to_string(), custom_endpoint_document)
}

fn assert_readme_endpoint_catalog(custom_endpoint_document: &Value) {
    let custom_endpoints = custom_endpoint_document["chat.customEndpoints"]
        .as_array()
        .expect("custom endpoints");
    let main_endpoint = &custom_endpoints[0];
    assert_eq!(
        main_endpoint.get("uri").and_then(Value::as_str),
        Some("http://127.0.0.1:8100/v1")
    );
    let main_model_ids = endpoint_model_ids(
        main_endpoint,
        "README Main endpoint should define models as a JSON array",
        "README Main endpoint models should have string ids",
    );
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
    let utility_model_ids = endpoint_model_ids(
        utility_endpoint,
        "README Utility endpoint should define models as a JSON array",
        "README Utility endpoint models should have string ids",
    );
    assert_eq!(utility_model_ids, ["threadline-utility-gpt-6-luna"]);
}

fn endpoint_model_ids<'model>(
    endpoint: &'model Value,
    models_message: &str,
    id_message: &str,
) -> Vec<&'model str> {
    endpoint
        .get("models")
        .and_then(Value::as_array)
        .expect(models_message)
        .iter()
        .map(|model| model.get("id").and_then(Value::as_str).expect(id_message))
        .collect()
}

fn assert_readme_visible_aliases(readme: &str, custom_endpoint_json: &str) {
    let supported_aliases_section = readme_section_containing(&readme, "Main profile aliases:")
        .expect("README should document the supported model alias list");
    let utility_aliases_section = readme_section_containing(&readme, "Utility profile aliases:")
        .expect("README should document the supported Utility model alias list");
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
}

fn assert_readme_utility_model(document: &Value, custom_endpoint_json: &str) {
    let utility_endpoint = &document["chat.customEndpoints"][1];
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
        custom_endpoint_json
            .contains("\"chat.utilityModel\": \"customendpoint/threadline-utility-gpt-6-luna\""),
        "README should keep the default Utility model selector on GPT-6 Luna"
    );
    assert!(
        custom_endpoint_json.contains(
            "\"chat.utilitySmallModel\": \"customendpoint/threadline-utility-gpt-6-luna\""
        ),
        "README should keep the small Utility model selector on GPT-6 Luna"
    );
}

fn assert_readme_model_policies(readme: &str) {
    let raw_upstream_ids_section = readme_section_containing(
        &readme,
        "These visible ids are aliases for VS Code selection and routing.",
    )
    .expect("README should explain visible aliases and raw upstream model ids");
    let persistent_reasoning_scope =
        readme_section_containing(&readme, "Persistent reasoning is opt-in")
            .expect("README should document persistent reasoning eligibility");
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
    assert!(persistent_reasoning_scope.contains("Main"));
    assert!(persistent_reasoning_scope.contains("Utility"));
}

fn assert_retired_models_absent(readme: &str) {
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
}

const README_CONFIGURATION_CASES: &[(&str, &str, Option<&str>)] = &[
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
];

#[test]
fn readme_documents_supported_configuration_flags() {
    let readme = readme_text().replace("\r\n", "\n");
    assert_readme_configuration_flags(&readme);
    assert_main_only_reasoning_scope(&readme);
    assert_eligible_reasoning_scope(&readme);
    assert_client_explicit_reasoning_scope(&readme);
    assert_job_command_policy(&readme);
    assert!(!readme.contains(&removed_model_flag()));
    assert!(!readme.contains(&removed_model_env_var()));
}

fn assert_readme_configuration_flags(readme: &str) {
    for &(flag, env_var, stable_default) in README_CONFIGURATION_CASES {
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
}

fn assert_main_only_reasoning_scope(readme: &str) {
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
}

fn assert_eligible_reasoning_scope(readme: &str) {
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
}

fn assert_client_explicit_reasoning_scope(readme: &str) {
    let persistent_reasoning_scope =
        readme_section_containing(&readme, "Persistent reasoning is opt-in")
            .expect("README should document persistent reasoning eligibility");
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
        client_explicit_scope.contains("preserve client-explicit `reasoning.context=all_turns`"),
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
        client_explicit_scope.contains("preserve client-explicit `reasoning.context=all_turns`",),
        "README should preserve client-explicit all_turns"
    );
}

fn assert_job_command_policy(readme: &str) {
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
