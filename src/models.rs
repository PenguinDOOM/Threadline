use std::sync::OnceLock;

use clap::ValueEnum;
use serde_json::{Map, Value};

use crate::errors::ThreadlineError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum RouteProfile {
    Main,
    Utility,
}

impl std::fmt::Display for RouteProfile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Main => "main",
            Self::Utility => "utility",
        };

        formatter.write_str(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelAlias {
    pub alias_id: &'static str,
    pub upstream_model_id: &'static str,
    pub profile: RouteProfile,
    pub advertised: bool,
    pub persistent_reasoning_eligible: bool,
    pub supports_reasoning_all_turns: bool,
}

const MODEL_ALIAS_CATALOG: [ModelAlias; 14] = [
    ModelAlias {
        alias_id: "threadline-main-gpt-5.6-sol",
        upstream_model_id: "gpt-5.6-sol",
        profile: RouteProfile::Main,
        advertised: true,
        persistent_reasoning_eligible: true,
        supports_reasoning_all_turns: true,
    },
    ModelAlias {
        alias_id: "threadline-main-gpt-5.6-terra",
        upstream_model_id: "gpt-5.6-terra",
        profile: RouteProfile::Main,
        advertised: true,
        persistent_reasoning_eligible: true,
        supports_reasoning_all_turns: true,
    },
    ModelAlias {
        alias_id: "threadline-main-gpt-5.6-luna",
        upstream_model_id: "gpt-5.6-luna",
        profile: RouteProfile::Main,
        advertised: true,
        persistent_reasoning_eligible: true,
        supports_reasoning_all_turns: true,
    },
    ModelAlias {
        alias_id: "threadline-main-gpt-5.5",
        upstream_model_id: "gpt-5.5",
        profile: RouteProfile::Main,
        advertised: true,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: true,
    },
    ModelAlias {
        alias_id: "threadline-main-gpt-5.4",
        upstream_model_id: "gpt-5.4",
        profile: RouteProfile::Main,
        advertised: true,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: true,
    },
    ModelAlias {
        alias_id: "threadline-utility-gpt-5.4-mini",
        upstream_model_id: "gpt-5.4-mini",
        profile: RouteProfile::Utility,
        advertised: true,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: true,
    },
    ModelAlias {
        alias_id: "threadline-utility-gpt-5.3-codex-spark",
        upstream_model_id: "gpt-5.3-codex-spark",
        profile: RouteProfile::Utility,
        advertised: true,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: false,
    },
    ModelAlias {
        alias_id: "gpt-5.6-sol",
        upstream_model_id: "gpt-5.6-sol",
        profile: RouteProfile::Main,
        advertised: false,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: false,
    },
    ModelAlias {
        alias_id: "gpt-5.6-terra",
        upstream_model_id: "gpt-5.6-terra",
        profile: RouteProfile::Main,
        advertised: false,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: false,
    },
    ModelAlias {
        alias_id: "gpt-5.6-luna",
        upstream_model_id: "gpt-5.6-luna",
        profile: RouteProfile::Main,
        advertised: false,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: false,
    },
    ModelAlias {
        alias_id: "gpt-5.5",
        upstream_model_id: "gpt-5.5",
        profile: RouteProfile::Main,
        advertised: false,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: false,
    },
    ModelAlias {
        alias_id: "gpt-5.4",
        upstream_model_id: "gpt-5.4",
        profile: RouteProfile::Main,
        advertised: false,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: false,
    },
    ModelAlias {
        alias_id: "gpt-5.4-mini",
        upstream_model_id: "gpt-5.4-mini",
        profile: RouteProfile::Main,
        advertised: false,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: false,
    },
    ModelAlias {
        alias_id: "gpt-5.3-codex-spark",
        upstream_model_id: "gpt-5.3-codex-spark",
        profile: RouteProfile::Main,
        advertised: false,
        persistent_reasoning_eligible: false,
        supports_reasoning_all_turns: false,
    },
];

static MAIN_ADVERTISED_MODEL_IDS: OnceLock<Vec<&'static str>> = OnceLock::new();
static UTILITY_ADVERTISED_MODEL_IDS: OnceLock<Vec<&'static str>> = OnceLock::new();

fn advertised_model_ids_cache(profile: RouteProfile) -> &'static OnceLock<Vec<&'static str>> {
    match profile {
        RouteProfile::Main => &MAIN_ADVERTISED_MODEL_IDS,
        RouteProfile::Utility => &UTILITY_ADVERTISED_MODEL_IDS,
    }
}

fn model_alias_by_id(model_id: &str) -> Option<&'static ModelAlias> {
    MODEL_ALIAS_CATALOG
        .iter()
        .find(|alias| alias.alias_id == model_id)
}

fn resolve_model_alias_for_profile(
    model_id: &str,
    profile: RouteProfile,
) -> Result<&'static ModelAlias, ThreadlineError> {
    match model_alias_by_id(model_id) {
        Some(alias) if alias.profile == profile => Ok(alias),
        _ => Err(ThreadlineError::InvalidModel),
    }
}

pub fn supported_model_ids() -> &'static [&'static str] {
    advertised_model_ids_for_profile(RouteProfile::Main)
}

pub fn advertised_model_ids_for_profile(profile: RouteProfile) -> &'static [&'static str] {
    advertised_model_ids_cache(profile)
        .get_or_init(|| {
            MODEL_ALIAS_CATALOG
                .iter()
                .filter(|alias| alias.profile == profile && alias.advertised)
                .map(|alias| alias.alias_id)
                .collect()
        })
        .as_slice()
}

pub fn is_supported_model(model_id: &str) -> bool {
    model_alias_by_id(model_id).is_some()
}

pub fn validate_request_model(payload: &Map<String, Value>) -> Result<&str, ThreadlineError> {
    let alias = resolve_request_model_for_profile(payload, RouteProfile::Main)?;
    Ok(alias.alias_id)
}

pub fn resolve_request_model_for_profile(
    payload: &Map<String, Value>,
    profile: RouteProfile,
) -> Result<&'static ModelAlias, ThreadlineError> {
    let model_id = payload
        .get("model")
        .and_then(Value::as_str)
        .ok_or(ThreadlineError::InvalidModel)?;

    resolve_model_alias_for_profile(model_id, profile)
}

#[cfg(test)]
mod tests {
    use super::{
        RouteProfile, advertised_model_ids_for_profile, is_supported_model,
        resolve_request_model_for_profile, supported_model_ids, validate_request_model,
    };
    use serde_json::json;

    const NEW_MAIN_VISIBLE_MODEL_IDS: [&str; 3] = [
        "threadline-main-gpt-5.6-sol",
        "threadline-main-gpt-5.6-terra",
        "threadline-main-gpt-5.6-luna",
    ];

    const NEW_MAIN_RAW_COMPATIBILITY_IDS: [&str; 3] =
        ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"];

    #[test]
    fn supported_model_ids_match_main_public_contract() {
        assert_eq!(
            supported_model_ids(),
            &[
                "threadline-main-gpt-5.6-sol",
                "threadline-main-gpt-5.6-terra",
                "threadline-main-gpt-5.6-luna",
                "threadline-main-gpt-5.5",
                "threadline-main-gpt-5.4",
            ]
        );
    }

    #[test]
    fn advertised_model_ids_are_filtered_by_profile() {
        assert_eq!(
            advertised_model_ids_for_profile(RouteProfile::Main),
            &[
                "threadline-main-gpt-5.6-sol",
                "threadline-main-gpt-5.6-terra",
                "threadline-main-gpt-5.6-luna",
                "threadline-main-gpt-5.5",
                "threadline-main-gpt-5.4",
            ]
        );
        assert_eq!(
            advertised_model_ids_for_profile(RouteProfile::Utility),
            &[
                "threadline-utility-gpt-5.4-mini",
                "threadline-utility-gpt-5.3-codex-spark",
            ]
        );
    }

    #[test]
    fn supported_model_check_accepts_aliases_and_hidden_main_compatibility_ids() {
        for model_id in NEW_MAIN_VISIBLE_MODEL_IDS {
            assert!(is_supported_model(model_id));
        }
        assert!(is_supported_model("threadline-main-gpt-5.5"));
        assert!(is_supported_model("threadline-main-gpt-5.4"));
        assert!(is_supported_model("threadline-utility-gpt-5.4-mini"));
        assert!(is_supported_model("threadline-utility-gpt-5.3-codex-spark"));
        for model_id in NEW_MAIN_RAW_COMPATIBILITY_IDS {
            assert!(is_supported_model(model_id));
        }
        assert!(is_supported_model("gpt-5.5"));
        assert!(is_supported_model("gpt-5.4"));
        assert!(is_supported_model("gpt-5.4-mini"));
        assert!(is_supported_model("gpt-5.3-codex-spark"));
        assert!(!is_supported_model("codex-mini-latest"));
    }

    #[test]
    fn resolve_request_model_for_profile_rewrites_visible_alias_to_upstream_model() {
        for (alias_id, upstream_model_id) in [
            ("threadline-main-gpt-5.6-sol", "gpt-5.6-sol"),
            ("threadline-main-gpt-5.6-terra", "gpt-5.6-terra"),
            ("threadline-main-gpt-5.6-luna", "gpt-5.6-luna"),
        ] {
            let main = resolve_request_model_for_profile(
                json!({ "model": alias_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert_eq!(main.alias_id, alias_id);
            assert_eq!(main.upstream_model_id, upstream_model_id);
            assert_eq!(main.profile, RouteProfile::Main);
            assert!(main.advertised);
            assert!(main.persistent_reasoning_eligible);
            assert!(main.supports_reasoning_all_turns);
        }

        let main = resolve_request_model_for_profile(
            json!({ "model": "threadline-main-gpt-5.5" })
                .as_object()
                .unwrap(),
            RouteProfile::Main,
        )
        .unwrap();
        assert_eq!(main.alias_id, "threadline-main-gpt-5.5");
        assert_eq!(main.upstream_model_id, "gpt-5.5");
        assert_eq!(main.profile, RouteProfile::Main);
        assert!(main.advertised);
        assert!(!main.persistent_reasoning_eligible);
        assert!(main.supports_reasoning_all_turns);

        let utility = resolve_request_model_for_profile(
            json!({ "model": "threadline-utility-gpt-5.4-mini" })
                .as_object()
                .unwrap(),
            RouteProfile::Utility,
        )
        .unwrap();
        assert_eq!(utility.alias_id, "threadline-utility-gpt-5.4-mini");
        assert_eq!(utility.upstream_model_id, "gpt-5.4-mini");
        assert_eq!(utility.profile, RouteProfile::Utility);
        assert!(utility.advertised);
        assert!(!utility.persistent_reasoning_eligible);
        assert!(utility.supports_reasoning_all_turns);
    }

    #[test]
    fn resolve_request_model_for_profile_rejects_profile_mismatch() {
        for model_id in NEW_MAIN_VISIBLE_MODEL_IDS {
            assert_eq!(
                resolve_request_model_for_profile(
                    json!({ "model": model_id }).as_object().unwrap(),
                    RouteProfile::Utility,
                )
                .unwrap_err()
                .to_string(),
                "The /v1/responses request must include a supported string model."
            );
        }

        assert_eq!(
            resolve_request_model_for_profile(
                json!({ "model": "threadline-utility-gpt-5.4-mini" })
                    .as_object()
                    .unwrap(),
                RouteProfile::Main,
            )
            .unwrap_err()
            .to_string(),
            "The /v1/responses request must include a supported string model."
        );
        assert_eq!(
            resolve_request_model_for_profile(
                json!({ "model": "threadline-main-gpt-5.4" })
                    .as_object()
                    .unwrap(),
                RouteProfile::Utility,
            )
            .unwrap_err()
            .to_string(),
            "The /v1/responses request must include a supported string model."
        );
    }

    #[test]
    fn resolve_request_model_for_profile_rejects_unknown_model() {
        assert_eq!(
            resolve_request_model_for_profile(
                json!({ "model": "codex-mini-latest" }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap_err()
            .to_string(),
            "The /v1/responses request must include a supported string model."
        );
    }

    #[test]
    fn resolve_request_model_for_profile_accepts_hidden_main_compatibility_ids() {
        for model_id in NEW_MAIN_RAW_COMPATIBILITY_IDS {
            let compatibility = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert_eq!(compatibility.alias_id, model_id);
            assert_eq!(compatibility.upstream_model_id, model_id);
            assert_eq!(compatibility.profile, RouteProfile::Main);
            assert!(!compatibility.advertised);
            assert!(!compatibility.persistent_reasoning_eligible);
            assert!(!compatibility.supports_reasoning_all_turns);

            assert_eq!(
                resolve_request_model_for_profile(
                    json!({ "model": model_id }).as_object().unwrap(),
                    RouteProfile::Utility,
                )
                .unwrap_err()
                .to_string(),
                "The /v1/responses request must include a supported string model."
            );
        }

        let compatibility = resolve_request_model_for_profile(
            json!({ "model": "gpt-5.4-mini" }).as_object().unwrap(),
            RouteProfile::Main,
        )
        .unwrap();
        assert_eq!(compatibility.alias_id, "gpt-5.4-mini");
        assert_eq!(compatibility.upstream_model_id, "gpt-5.4-mini");
        assert_eq!(compatibility.profile, RouteProfile::Main);
        assert!(!compatibility.advertised);
        assert!(!compatibility.persistent_reasoning_eligible);
        assert!(!compatibility.supports_reasoning_all_turns);

        assert_eq!(
            resolve_request_model_for_profile(
                json!({ "model": "gpt-5.4-mini" }).as_object().unwrap(),
                RouteProfile::Utility,
            )
            .unwrap_err()
            .to_string(),
            "The /v1/responses request must include a supported string model."
        );
    }

    #[test]
    fn resolve_request_model_for_profile_exposes_reasoning_all_turns_capability_by_alias() {
        for model_id in NEW_MAIN_VISIBLE_MODEL_IDS {
            let main_supported = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert!(main_supported.supports_reasoning_all_turns);
        }

        let main_supported = resolve_request_model_for_profile(
            json!({ "model": "threadline-main-gpt-5.5" })
                .as_object()
                .unwrap(),
            RouteProfile::Main,
        )
        .unwrap();
        assert!(main_supported.supports_reasoning_all_turns);

        let main_supported_secondary = resolve_request_model_for_profile(
            json!({ "model": "threadline-main-gpt-5.4" })
                .as_object()
                .unwrap(),
            RouteProfile::Main,
        )
        .unwrap();
        assert!(main_supported_secondary.supports_reasoning_all_turns);

        let utility_supported = resolve_request_model_for_profile(
            json!({ "model": "threadline-utility-gpt-5.4-mini" })
                .as_object()
                .unwrap(),
            RouteProfile::Utility,
        )
        .unwrap();
        assert!(utility_supported.supports_reasoning_all_turns);

        let utility_unsupported = resolve_request_model_for_profile(
            json!({ "model": "threadline-utility-gpt-5.3-codex-spark" })
                .as_object()
                .unwrap(),
            RouteProfile::Utility,
        )
        .unwrap();
        assert!(!utility_unsupported.supports_reasoning_all_turns);

        let hidden_compatibility = resolve_request_model_for_profile(
            json!({ "model": "gpt-5.5" }).as_object().unwrap(),
            RouteProfile::Main,
        )
        .unwrap();
        assert!(!hidden_compatibility.supports_reasoning_all_turns);

        let hidden_compatibility_secondary = resolve_request_model_for_profile(
            json!({ "model": "gpt-5.4" }).as_object().unwrap(),
            RouteProfile::Main,
        )
        .unwrap();
        assert!(!hidden_compatibility_secondary.supports_reasoning_all_turns);

        let hidden_compatibility_tertiary = resolve_request_model_for_profile(
            json!({ "model": "gpt-5.4-mini" }).as_object().unwrap(),
            RouteProfile::Main,
        )
        .unwrap();
        assert!(!hidden_compatibility_tertiary.supports_reasoning_all_turns);

        let hidden_compatibility_quaternary = resolve_request_model_for_profile(
            json!({ "model": "gpt-5.3-codex-spark" })
                .as_object()
                .unwrap(),
            RouteProfile::Main,
        )
        .unwrap();
        assert!(!hidden_compatibility_quaternary.supports_reasoning_all_turns);
    }

    #[test]
    fn resolve_request_model_for_profile_separates_persistent_reasoning_eligibility() {
        for model_id in NEW_MAIN_VISIBLE_MODEL_IDS {
            let alias = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert!(alias.persistent_reasoning_eligible);
        }

        for model_id in [
            "threadline-main-gpt-5.5",
            "threadline-main-gpt-5.4",
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "gpt-5.5",
            "gpt-5.4",
            "gpt-5.4-mini",
            "gpt-5.3-codex-spark",
        ] {
            let alias = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert!(!alias.persistent_reasoning_eligible);
        }

        for model_id in [
            "threadline-utility-gpt-5.4-mini",
            "threadline-utility-gpt-5.3-codex-spark",
        ] {
            let alias = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Utility,
            )
            .unwrap();
            assert!(!alias.persistent_reasoning_eligible);
        }
    }

    #[test]
    fn resolve_request_model_for_profile_keeps_invalid_model_for_wrong_profile_before_capability_use()
     {
        assert_eq!(
            resolve_request_model_for_profile(
                json!({ "model": "threadline-utility-gpt-5.4-mini" })
                    .as_object()
                    .unwrap(),
                RouteProfile::Main,
            )
            .unwrap_err()
            .to_string(),
            "The /v1/responses request must include a supported string model."
        );
    }

    #[test]
    fn resolve_request_model_for_profile_keeps_invalid_model_for_unknown_alias_before_capability_use()
     {
        assert_eq!(
            resolve_request_model_for_profile(
                json!({ "model": "gpt-5.4-nano" }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap_err()
            .to_string(),
            "The /v1/responses request must include a supported string model."
        );
    }

    #[test]
    fn validate_request_model_requires_main_supported_string_model() {
        assert_eq!(
            validate_request_model(json!({}).as_object().unwrap())
                .unwrap_err()
                .to_string(),
            "The /v1/responses request must include a supported string model."
        );
        assert_eq!(
            validate_request_model(json!({ "model": { "id": "gpt-5.4" } }).as_object().unwrap())
                .unwrap_err()
                .to_string(),
            "The /v1/responses request must include a supported string model."
        );
        assert_eq!(
            validate_request_model(json!({ "model": "codex-mini-latest" }).as_object().unwrap())
                .unwrap_err()
                .to_string(),
            "The /v1/responses request must include a supported string model."
        );
        assert_eq!(
            validate_request_model(
                json!({ "model": "threadline-main-gpt-5.4" })
                    .as_object()
                    .unwrap()
            )
            .unwrap(),
            "threadline-main-gpt-5.4"
        );
        assert_eq!(
            validate_request_model(
                json!({ "model": "threadline-utility-gpt-5.4-mini" })
                    .as_object()
                    .unwrap()
            )
            .unwrap_err()
            .to_string(),
            "The /v1/responses request must include a supported string model."
        );
    }
}
