use super::*;

#[test]
fn upstream_inbound_limits_default_override_and_reject_invalid_ranges() {
    let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
    let default_config = ThreadlineCli::parse_from(["threadline"]).server;
    assert_eq!(default_config.upstream_inbound_max_messages, 256);
    assert_eq!(default_config.upstream_inbound_max_bytes, 16 * 1024 * 1024);

    let override_config = ThreadlineCli::try_parse_from([
        "threadline",
        "--upstream-inbound-max-messages",
        "2",
        "--upstream-inbound-max-bytes",
        "3",
    ])
    .expect("valid override")
    .server;
    assert_eq!(
        override_config.upstream_inbound_limits(),
        Ok(UpstreamInboundLimits::new(2, 3).unwrap())
    );

    assert!(
        ThreadlineCli::try_parse_from(["threadline", "--upstream-inbound-max-messages", "0",])
            .is_err()
    );
    assert!(
        ThreadlineCli::try_parse_from(["threadline", "--upstream-inbound-max-bytes", "67108865",])
            .is_err()
    );
    assert!(
        ThreadlineCli::try_parse_from([
            "threadline",
            "--upstream-inbound-max-messages",
            "not-a-number",
        ])
        .is_err()
    );
    assert!(
        ThreadlineCli::try_parse_from(["threadline", "--upstream-inbound-max-bytes", "-1",])
            .is_err()
    );
    assert!(UpstreamInboundLimits::new(0, 1).is_err());
    assert!(UpstreamInboundLimits::new(1, 0).is_err());
}

#[test]
fn upstream_connect_timeout_defaults_and_accepts_boundaries() {
    let _env_lock = super::THREADLINE_ENV_LOCK.lock().expect("environment lock");
    let default_config = ThreadlineCli::parse_from(["threadline"]).server;
    assert_eq!(default_config.upstream_connect_timeout_secs, 30);
    assert_eq!(
        default_config.upstream_connect_timeout(),
        Ok(std::time::Duration::from_secs(30))
    );

    for value in ["1", "3600"] {
        let config =
            ThreadlineCli::try_parse_from(["threadline", "--upstream-connect-timeout-secs", value])
                .expect("boundary should be accepted")
                .server;
        assert!(config.upstream_connect_timeout().is_ok());
    }

    for value in ["0", "3601", "-1", "not-a-number"] {
        assert!(
            ThreadlineCli::try_parse_from(
                ["threadline", "--upstream-connect-timeout-secs", value,]
            )
            .is_err()
        );
    }

    assert!(
        ThreadlineConfig {
            upstream_connect_timeout_secs: 0,
            ..ThreadlineConfig::default()
        }
        .upstream_connect_timeout()
        .is_err()
    );
    assert!(
        ThreadlineConfig {
            upstream_connect_timeout_secs: 3601,
            ..ThreadlineConfig::default()
        }
        .upstream_connect_timeout()
        .is_err()
    );
}

#[test]
fn upstream_connect_timeout_cli_overrides_environment() {
    run_cases(
        "config::tests::transport::upstream_connect_timeout_cli_overrides_environment",
        &[(
            "override",
            &[("THREADLINE_UPSTREAM_CONNECT_TIMEOUT_SECS", "17")],
        )],
        |_| {
            let from_environment = ThreadlineCli::parse_from(["threadline"]).server;
            let from_cli = ThreadlineCli::try_parse_from([
                "threadline",
                "--upstream-connect-timeout-secs",
                "23",
            ])
            .expect("valid CLI override")
            .server;

            assert_eq!(from_environment.upstream_connect_timeout_secs, 17);
            assert_eq!(from_cli.upstream_connect_timeout_secs, 23);
        },
    );
}

#[test]
fn upstream_watchdog_timeouts_validate_cli_environment_and_policy() {
    run_cases(
        "config::tests::transport::upstream_watchdog_timeouts_validate_cli_environment_and_policy",
        &[
            ("default", &[]),
            (
                "override",
                &[
                    ("THREADLINE_UPSTREAM_PONG_TIMEOUT_SECS", "17"),
                    ("THREADLINE_UPSTREAM_WRITE_TIMEOUT_SECS", "19"),
                ],
            ),
        ],
        |case| {
            if case == "default" {
                let defaults = ThreadlineCli::parse_from(["threadline"]).server;
                assert_eq!(defaults.upstream_pong_timeout_secs, 60);
                assert_eq!(defaults.upstream_write_timeout_secs, 60);
                assert_eq!(
                    defaults.upstream_watchdog_policy(),
                    Ok(UpstreamWatchdogPolicy::DEFAULT)
                );
            } else {
                let from_environment = ThreadlineCli::parse_from(["threadline"]).server;
                assert_eq!(from_environment.upstream_pong_timeout_secs, 17);
                assert_eq!(from_environment.upstream_write_timeout_secs, 19);

                let from_cli = ThreadlineCli::try_parse_from([
                    "threadline",
                    "--upstream-pong-timeout-secs",
                    "23",
                    "--upstream-write-timeout-secs",
                    "29",
                ])
                .expect("valid CLI values")
                .server;
                assert_eq!(from_cli.upstream_pong_timeout_secs, 23);
                assert_eq!(from_cli.upstream_write_timeout_secs, 29);
                assert_eq!(
                    from_cli.upstream_watchdog_policy().unwrap().pong_timeout(),
                    std::time::Duration::from_secs(23)
                );
                assert_eq!(
                    from_cli.upstream_watchdog_policy().unwrap().write_timeout(),
                    std::time::Duration::from_secs(29)
                );
            }
        },
    );
}

#[test]
fn upstream_watchdog_timeouts_reject_invalid_values_and_accept_boundaries() {
    run_cases(
        "config::tests::transport::upstream_watchdog_timeouts_reject_invalid_values_and_accept_boundaries",
        &[("default", &[])],
        |_| {
            for flag in [
                "--upstream-pong-timeout-secs",
                "--upstream-write-timeout-secs",
            ] {
                for value in ["0", "3601", "-1", "not-a-number"] {
                    assert!(ThreadlineCli::try_parse_from(["threadline", flag, value]).is_err());
                }
                for value in ["1", "3600"] {
                    assert!(ThreadlineCli::try_parse_from(["threadline", flag, value]).is_ok());
                }
            }

            for config in [
                ThreadlineConfig {
                    upstream_pong_timeout_secs: 0,
                    ..ThreadlineConfig::default()
                },
                ThreadlineConfig {
                    upstream_pong_timeout_secs: 3601,
                    ..ThreadlineConfig::default()
                },
                ThreadlineConfig {
                    upstream_write_timeout_secs: 0,
                    ..ThreadlineConfig::default()
                },
                ThreadlineConfig {
                    upstream_write_timeout_secs: 3601,
                    ..ThreadlineConfig::default()
                },
            ] {
                assert!(config.upstream_watchdog_policy().is_err());
            }

            assert!(
                UpstreamWatchdogPolicy::new(
                    std::time::Duration::from_millis(1),
                    std::time::Duration::from_millis(1),
                )
                .is_ok()
            );
        },
    );
}

#[test]
fn upstream_inbound_limits_read_environment_overrides() {
    run_cases(
        "config::tests::transport::upstream_inbound_limits_read_environment_overrides",
        &[(
            "override",
            &[
                ("THREADLINE_UPSTREAM_INBOUND_MAX_MESSAGES", "7"),
                ("THREADLINE_UPSTREAM_INBOUND_MAX_BYTES", "11"),
            ],
        )],
        |_| {
            let config = ThreadlineCli::parse_from(["threadline"]).server;
            assert_eq!(config.upstream_inbound_max_messages, 7);
            assert_eq!(config.upstream_inbound_max_bytes, 11);
        },
    );
}
