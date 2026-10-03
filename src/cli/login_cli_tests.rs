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

mod readme;

#[test]
fn login_command_accepts_bare_login_only() {
    let _env_lock = THREADLINE_ENV_LOCK.lock().expect("environment lock");
    let cli = ThreadlineCli::try_parse_from(["threadline", "login"]).expect("login should parse");

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
