use super::*;

#[test]
fn request_body_limit_defaults_overrides_and_rejects_invalid_values() {
    let maximum = usize::MAX.to_string();
    run_cases(
        "config::tests::parsing::request_body_limit_defaults_overrides_and_rejects_invalid_values",
        &[
            ("default", &[]),
            ("minimum", &[("THREADLINE_MAX_REQUEST_BODY_BYTES", "1")]),
            ("override", &[("THREADLINE_MAX_REQUEST_BODY_BYTES", "3")]),
            ("0", &[("THREADLINE_MAX_REQUEST_BODY_BYTES", "0")]),
            ("-1", &[("THREADLINE_MAX_REQUEST_BODY_BYTES", "-1")]),
            (
                "not-a-number",
                &[("THREADLINE_MAX_REQUEST_BODY_BYTES", "not-a-number")],
            ),
            (
                "999999999999999999999999999999999999",
                &[(
                    "THREADLINE_MAX_REQUEST_BODY_BYTES",
                    "999999999999999999999999999999999999",
                )],
            ),
            (
                "maximum",
                &[("THREADLINE_MAX_REQUEST_BODY_BYTES", &maximum)],
            ),
        ],
        |case| assert_request_body_case(case, &maximum),
    );
}

fn assert_request_body_case(case: &str, maximum: &str) {
    match case {
        "default" => {
            let default_config = ThreadlineCli::parse_from(["threadline"]).server;
            assert_eq!(
                ThreadlineConfig::default().max_request_body_bytes,
                33_554_432
            );
            assert_eq!(default_config.max_request_body_bytes, 33_554_432);
            assert_eq!(DEFAULT_MAX_REQUEST_BODY_BYTES, 33_554_432);
        }
        "minimum" => {
            let minimum_environment_config = ThreadlineCli::parse_from(["threadline"]).server;
            assert_eq!(minimum_environment_config.max_request_body_bytes, 1);
        }
        "override" => {
            let environment_config = ThreadlineCli::parse_from(["threadline"]).server;
            assert_eq!(environment_config.max_request_body_bytes, 3);
            let cli_config =
                ThreadlineCli::try_parse_from(["threadline", "--max-request-body-bytes", "1"])
                    .expect("positive body limit should parse")
                    .server;
            assert_eq!(cli_config.max_request_body_bytes, 1);
        }
        "maximum" => assert_maximum_body_limit(maximum),
        invalid => assert_invalid_body_limit(invalid),
    }
}

fn assert_maximum_body_limit(maximum: &str) {
    let maximum_config =
        ThreadlineCli::try_parse_from(["threadline", "--max-request-body-bytes", maximum])
            .expect("usize maximum should parse")
            .server;
    assert_eq!(maximum_config.max_request_body_bytes, usize::MAX);
    let maximum_environment_config = ThreadlineCli::parse_from(["threadline"]).server;
    assert_eq!(
        maximum_environment_config.max_request_body_bytes,
        usize::MAX
    );
}

fn assert_invalid_body_limit(invalid: &str) {
    assert!(
        ThreadlineCli::try_parse_from(["threadline", "--max-request-body-bytes", invalid]).is_err(),
        "CLI should reject {invalid:?}"
    );
    assert!(
        ThreadlineCli::try_parse_from(["threadline"]).is_err(),
        "environment should reject {invalid:?}"
    );
}

#[test]
fn job_capacity_defaults_cli_values_and_zero_are_preserved() {
    run_cases(
        "config::tests::parsing::job_capacity_defaults_cli_values_and_zero_are_preserved",
        &[("default", &[])],
        |_| {
            let default_config = ThreadlineCli::parse_from(["threadline"]).server;
            assert_eq!(default_config.job_max_active_jobs, 16);
            assert_eq!(default_config.job_max_retained_jobs, 128);

            let configured = ThreadlineCli::try_parse_from([
                "threadline",
                "--job-max-active-jobs",
                "0",
                "--job-max-retained-jobs",
                "0",
            ])
            .expect("capacity values should parse")
            .server;
            assert_eq!(configured.job_max_active_jobs, 0);
            assert_eq!(configured.job_max_retained_jobs, 0);
            assert_eq!(configured.job_manager_config().max_active_jobs, 0);
            assert_eq!(configured.job_manager_config().max_retained_jobs, 0);

            let small_config = ThreadlineCli::try_parse_from([
                "threadline",
                "--job-max-active-jobs",
                "2",
                "--job-max-retained-jobs",
                "3",
            ])
            .expect("small capacity values should parse")
            .server;
            assert_eq!(small_config.job_max_active_jobs, 2);
            assert_eq!(small_config.job_max_retained_jobs, 3);

            for flag in ["--job-max-active-jobs", "--job-max-retained-jobs"] {
                for invalid in ["-1", "not-a-number", "999999999999999999999999999999999999"] {
                    assert!(
                        ThreadlineCli::try_parse_from(["threadline", flag, invalid,]).is_err(),
                        "{flag} should reject {invalid:?}"
                    );
                }
            }
        },
    );
}

const CLI_CAPACITY_ENVIRONMENTS: &[(&str, &str, &str)] = &[
    ("zero", "0", "0"),
    ("small", "2", "3"),
    ("active-negative", "-1", "3"),
    ("active-invalid", "not-a-number", "3"),
    (
        "active-overflow",
        "999999999999999999999999999999999999",
        "3",
    ),
    ("retained-negative", "3", "-1"),
    ("retained-invalid", "3", "not-a-number"),
    (
        "retained-overflow",
        "3",
        "999999999999999999999999999999999999",
    ),
];

#[test]
fn job_capacity_cli_overrides_environment_values() {
    run_capacity_cases(
        "config::tests::parsing::job_capacity_cli_overrides_environment_values",
        CLI_CAPACITY_ENVIRONMENTS,
        |case| match case {
            "zero" => {
                let environment_config = ThreadlineCli::parse_from(["threadline"]).server;
                assert_eq!(environment_config.job_max_active_jobs, 0);
                assert_eq!(environment_config.job_max_retained_jobs, 0);
            }
            "small" => {
                let small_environment_config = ThreadlineCli::parse_from(["threadline"]).server;
                assert_eq!(small_environment_config.job_max_active_jobs, 2);
                assert_eq!(small_environment_config.job_max_retained_jobs, 3);
                let cli_config = ThreadlineCli::try_parse_from([
                    "threadline",
                    "--job-max-active-jobs",
                    "4",
                    "--job-max-retained-jobs",
                    "1",
                ])
                .expect("CLI values should override environment")
                .server;
                assert_eq!(cli_config.job_max_active_jobs, 4);
                assert_eq!(cli_config.job_max_retained_jobs, 1);
            }
            _ => assert!(
                ThreadlineCli::try_parse_from(["threadline"]).is_err(),
                "{case} should be rejected during startup parsing"
            ),
        },
    );
}

const STANDALONE_CAPACITY_ENVIRONMENTS: &[(&str, &str, &str)] = &[
    ("zero", "0", "0"),
    ("active-invalid", "invalid", "3"),
    ("active-negative", "-1", "3"),
    (
        "active-overflow",
        "999999999999999999999999999999999999",
        "3",
    ),
    ("retained-invalid", "2", "invalid"),
    ("retained-negative", "2", "-1"),
    (
        "retained-overflow",
        "2",
        "999999999999999999999999999999999999",
    ),
];

#[test]
fn standalone_job_capacity_environment_helper_preserves_zero_and_falls_back() {
    let values = STANDALONE_CAPACITY_ENVIRONMENTS;
    run_capacity_cases(
        "config::tests::parsing::standalone_job_capacity_environment_helper_preserves_zero_and_falls_back",
        values,
        |case| {
            if case == "zero" {
                let configured = job_manager_config_from_environment();
                assert_eq!(configured.max_active_jobs, 0);
                assert_eq!(configured.max_retained_jobs, 0);
                return;
            }
            let (_, active, retained) = values.iter().find(|(id, _, _)| *id == case).unwrap();
            let fallback = job_manager_config_from_environment();
            assert_eq!(
                fallback.max_active_jobs,
                active.parse::<usize>().unwrap_or(DEFAULT_MAX_ACTIVE_JOBS)
            );
            assert_eq!(
                fallback.max_retained_jobs,
                retained
                    .parse::<usize>()
                    .unwrap_or(DEFAULT_MAX_RETAINED_JOBS)
            );
        },
    );
}

fn run_capacity_cases(test_name: &str, values: &[(&str, &str, &str)], assert_case: impl Fn(&str)) {
    let environments: Vec<_> = values
        .iter()
        .map(|(case, active, retained)| {
            (
                *case,
                [
                    ("THREADLINE_JOB_MAX_ACTIVE_JOBS", *active),
                    ("THREADLINE_JOB_MAX_RETAINED_JOBS", *retained),
                ],
            )
        })
        .collect();
    let cases: Vec<_> = environments
        .iter()
        .map(|(case, environment)| (*case, environment.as_slice()))
        .collect();
    run_cases(test_name, &cases, assert_case);
}

#[test]
fn profile_defaults_to_main() {
    crate::env_test_support::run_cases(
        "config::tests::parsing::profile_defaults_to_main",
        &[("default", &[])],
        |_| {
            let config = ThreadlineCli::parse_from(["threadline"]).server;
            let command = ThreadlineCli::command();
            let argument = arg_by_long_flag(&command, "profile");
            let default_values: Vec<_> = argument
                .get_default_values()
                .iter()
                .map(|value| value.to_str().expect("utf-8 default value"))
                .collect();

            assert_eq!(config.profile, RouteProfile::Main);
            assert_eq!(default_values, vec!["main"]);
        },
    );
}

#[test]
fn profile_accepts_explicit_utility_value() {
    let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
    let config = ThreadlineCli::try_parse_from(["threadline", "--profile", "utility"])
        .expect("threadline config should accept utility profile")
        .server;

    assert_eq!(config.profile, RouteProfile::Utility);
}

#[test]
fn profile_rejects_invalid_value() {
    let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
    ThreadlineCli::try_parse_from(["threadline", "--profile", "invalid"])
        .expect_err("threadline config should reject invalid profiles");
}

#[test]
fn profile_reads_threadline_profile_env_var() {
    crate::env_test_support::run_cases(
        "config::tests::parsing::profile_reads_threadline_profile_env_var",
        &[("utility", &[("THREADLINE_PROFILE", "utility")])],
        |_| {
            let config = ThreadlineCli::parse_from(["threadline"]).server;
            assert_eq!(config.profile, RouteProfile::Utility);
        },
    );
}

#[test]
fn persistent_reasoning_enabled_defaults_to_false() {
    run_cases(
        "config::tests::parsing::persistent_reasoning_enabled_defaults_to_false",
        &[("default", &[])],
        |_| {
            let config = ThreadlineCli::parse_from(["threadline"]).server;
            assert!(!config.persistent_reasoning_enabled);
        },
    );
}

#[test]
fn persistent_reasoning_enabled_is_effective_only_for_main_profile() {
    let main_config = ThreadlineConfig {
        persistent_reasoning_enabled: true,
        ..ThreadlineConfig::default()
    };
    let utility_config = ThreadlineConfig {
        profile: RouteProfile::Utility,
        persistent_reasoning_enabled: true,
        ..ThreadlineConfig::default()
    };

    assert!(main_config.persistent_reasoning_enabled_for_profile());
    assert!(!utility_config.persistent_reasoning_enabled_for_profile());
}

#[test]
fn persistent_reasoning_enabled_accepts_cli_flag() {
    let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
    let config = ThreadlineCli::try_parse_from(["threadline", "--persistent-reasoning-enabled"])
        .expect("threadline config should accept the persistent reasoning cli flag")
        .server;

    assert!(config.persistent_reasoning_enabled);
}

#[test]
fn persistent_reasoning_enabled_reads_true_and_false_env_values() {
    run_cases(
        "config::tests::parsing::persistent_reasoning_enabled_reads_true_and_false_env_values",
        &[
            (
                "true",
                &[("THREADLINE_PERSISTENT_REASONING_ENABLED", "true")],
            ),
            (
                "false",
                &[("THREADLINE_PERSISTENT_REASONING_ENABLED", "false")],
            ),
        ],
        |case| {
            let config = ThreadlineCli::parse_from(["threadline"]).server;
            if case == "true" {
                assert!(config.persistent_reasoning_enabled);
            } else {
                assert!(!config.persistent_reasoning_enabled);
            }
        },
    );
}

#[test]
fn utility_port_defaults_to_none() {
    let config = ThreadlineConfig::default();

    assert_eq!(config.utility_port, None);
}

#[test]
fn utility_port_accepts_cli_value() {
    let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
    let config = ThreadlineCli::try_parse_from(["threadline", "--utility-port", "8101"])
        .expect("threadline config should accept a utility port cli override")
        .server;

    assert_eq!(config.utility_port, Some(8101));
}

#[test]
fn utility_port_reads_threadline_utility_port_env_var() {
    run_cases(
        "config::tests::parsing::utility_port_reads_threadline_utility_port_env_var",
        &[("override", &[("THREADLINE_UTILITY_PORT", "8101")])],
        |_| {
            let config = ThreadlineCli::parse_from(["threadline"]).server;
            assert_eq!(config.utility_port, Some(8101));
        },
    );
}
