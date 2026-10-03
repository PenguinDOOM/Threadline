use super::*;

fn argument_help_text(argument: &Arg) -> String {
    [argument.get_help(), argument.get_long_help()]
        .into_iter()
        .flatten()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn assert_help_mentions(argument: &Arg, long_flag: &str, expected_terms: &[&str]) {
    let help_text = argument_help_text(argument);
    let normalized_help = help_text.to_ascii_lowercase();
    let generated_flag_label = long_flag.to_ascii_lowercase();
    let generated_phrase_label = long_flag.replace('-', " ").to_ascii_lowercase();

    assert!(
        !help_text.is_empty(),
        "expected --{long_flag} to have help or long_help text"
    );
    assert!(
        help_text.len() > long_flag.len() + 12,
        "expected --{long_flag} help to be descriptive, got {help_text:?}"
    );
    assert!(
        normalized_help != generated_flag_label && normalized_help != generated_phrase_label,
        "expected --{long_flag} help to add semantics beyond generated-only flag text, got {help_text:?}"
    );

    for term in expected_terms {
        assert!(
            normalized_help.contains(term),
            "expected --{long_flag} help to mention {term:?}, got {help_text:?}"
        );
    }
}

#[test]
fn codex_client_version_defaults_to_installed_version() {
    let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
    let config = ThreadlineCli::parse_from(["threadline"]).server;
    let command = ThreadlineCli::command();
    let argument = command
        .get_arguments()
        .find(|arg| arg.get_long() == Some("codex-client-version"))
        .expect("codex client version arg should exist");
    let default_values: Vec<_> = argument
        .get_default_values()
        .iter()
        .map(|value| value.to_str().expect("utf-8 default value"))
        .collect();

    assert_eq!(config.codex_client_version, DEFAULT_CODEX_CLIENT_VERSION);
    assert_eq!(default_values, vec![DEFAULT_CODEX_CLIENT_VERSION]);
}

#[test]
fn codex_client_version_cli_override_wins() {
    let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
    let config = ThreadlineCli::try_parse_from(["threadline", "--codex-client-version", "9.9.9"])
        .expect("threadline config should accept a codex client version cli override")
        .server;

    assert_eq!(config.codex_client_version, "9.9.9");
}

const CONFIGURATION_HELP_CASES: &[(&str, &[&str])] = &[
    ("host", &["listen", "address"]),
    ("port", &["listen", "port"]),
    ("utility-port", &["utility", "listener", "port"]),
    ("profile", &["profile", "main", "utility"]),
    ("codex-client-version", &["codex", "client version"]),
    (
        "retained-session-capacity",
        &["retained session", "capacity"],
    ),
    (
        "max-request-body-bytes",
        &["/v1/responses", "body", "bytes", "finite"],
    ),
    (
        "upstream-inbound-max-messages",
        &["upstream", "inbound", "messages"],
    ),
    (
        "upstream-inbound-max-bytes",
        &["upstream", "inbound", "bytes"],
    ),
    ("jobs-enabled", &["job", "enable"]),
    (
        "persistent-reasoning-enabled",
        &[
            "persistent",
            "reasoning",
            "eligible",
            "main",
            "request",
            "reasoning.context",
            "all_turns",
        ],
    ),
    ("job-output-buffer-limit-bytes", &["job output", "bytes"]),
    ("job-retention-ttl-secs", &["job", "retention", "seconds"]),
    ("job-max-active-jobs", &["concurrently", "job", "zero"]),
    ("job-max-retained-jobs", &["retained", "registry", "zero"]),
    (
        "job-allowed-commands",
        &["comma-separated", "exact", "program"],
    ),
    ("log-level", &["log", "verbosity"]),
];

#[test]
fn cli_flag_help_describes_supported_configuration() {
    let command = ThreadlineCli::command();
    for &(long_flag, expected_terms) in CONFIGURATION_HELP_CASES {
        let argument = arg_by_long_flag(&command, long_flag);
        assert_help_mentions(argument, long_flag, expected_terms);
    }
}
