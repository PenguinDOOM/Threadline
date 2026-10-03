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
}

const MODEL_ALIAS_CATALOG: [ModelAlias; 9] = [
    ModelAlias {
        alias_id: "threadline-main-gpt-6.1-sol",
        upstream_model_id: "gpt-6.1-sol",
        profile: RouteProfile::Main,
        advertised: true,
    },
    ModelAlias {
        alias_id: "threadline-main-gpt-6-astra",
        upstream_model_id: "gpt-6-astra",
        profile: RouteProfile::Main,
        advertised: true,
    },
    ModelAlias {
        alias_id: "threadline-main-gpt-6-sol",
        upstream_model_id: "gpt-6-sol",
        profile: RouteProfile::Main,
        advertised: true,
    },
    ModelAlias {
        alias_id: "threadline-main-gpt-6-luna",
        upstream_model_id: "gpt-6-luna",
        profile: RouteProfile::Main,
        advertised: true,
    },
    ModelAlias {
        alias_id: "threadline-utility-gpt-6-luna",
        upstream_model_id: "gpt-6-luna",
        profile: RouteProfile::Utility,
        advertised: true,
    },
    ModelAlias {
        alias_id: "gpt-6.1-sol",
        upstream_model_id: "gpt-6.1-sol",
        profile: RouteProfile::Main,
        advertised: false,
    },
    ModelAlias {
        alias_id: "gpt-6-astra",
        upstream_model_id: "gpt-6-astra",
        profile: RouteProfile::Main,
        advertised: false,
    },
    ModelAlias {
        alias_id: "gpt-6-sol",
        upstream_model_id: "gpt-6-sol",
        profile: RouteProfile::Main,
        advertised: false,
    },
    ModelAlias {
        alias_id: "gpt-6-luna",
        upstream_model_id: "gpt-6-luna",
        profile: RouteProfile::Main,
        advertised: false,
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

    const ASTRA_MAIN_MODEL_IDS: [&str; 2] = ["threadline-main-gpt-6-astra", "gpt-6-astra"];

    const GPT_6_FAMILY_MAIN_ALIAS_IDS: [&str; 3] = [
        "threadline-main-gpt-6.1-sol",
        "threadline-main-gpt-6-sol",
        "threadline-main-gpt-6-luna",
    ];

    const GPT_6_FAMILY_RAW_IDS: [&str; 3] = ["gpt-6.1-sol", "gpt-6-sol", "gpt-6-luna"];

    #[test]
    fn supported_model_ids_match_main_public_contract() {
        assert_eq!(
            supported_model_ids(),
            &[
                "threadline-main-gpt-6.1-sol",
                "threadline-main-gpt-6-astra",
                "threadline-main-gpt-6-sol",
                "threadline-main-gpt-6-luna",
            ]
        );
    }

    #[test]
    fn advertised_model_ids_are_filtered_by_profile() {
        assert_eq!(
            advertised_model_ids_for_profile(RouteProfile::Main),
            &[
                "threadline-main-gpt-6.1-sol",
                "threadline-main-gpt-6-astra",
                "threadline-main-gpt-6-sol",
                "threadline-main-gpt-6-luna",
            ]
        );
        assert_eq!(
            advertised_model_ids_for_profile(RouteProfile::Utility),
            &["threadline-utility-gpt-6-luna",]
        );
    }

    #[test]
    fn supported_model_check_accepts_aliases_and_hidden_main_compatibility_ids() {
        for model_id in GPT_6_FAMILY_MAIN_ALIAS_IDS {
            assert!(is_supported_model(model_id));
        }
        assert!(is_supported_model("threadline-utility-gpt-6-luna"));
        for model_id in GPT_6_FAMILY_RAW_IDS {
            assert!(is_supported_model(model_id));
        }
        for model_id in ASTRA_MAIN_MODEL_IDS {
            assert!(is_supported_model(model_id));
        }
        for model_id in [
            "gpt-5.6-sol",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "gpt-5.5",
            "gpt-5.4",
            "gpt-5.4-mini",
            "gpt-5.3-codex-spark",
            "threadline-main-gpt-5.6-sol",
            "threadline-main-gpt-5.6-terra",
            "threadline-main-gpt-5.6-luna",
            "threadline-main-gpt-5.5",
            "threadline-main-gpt-5.4",
            "threadline-utility-gpt-5.6-luna",
            "threadline-utility-gpt-5.4-mini",
            "threadline-utility-gpt-5.3-codex-spark",
        ] {
            assert!(!is_supported_model(model_id));
        }
        assert!(!is_supported_model("gpt-6-terra"));
        assert!(!is_supported_model("threadline-main-gpt-6-terra"));
        assert!(!is_supported_model("codex-mini-latest"));
    }

    #[test]
    fn resolve_request_model_for_profile_rewrites_visible_alias_to_upstream_model() {
        for (alias_id, upstream_model_id) in [
            ("threadline-main-gpt-6.1-sol", "gpt-6.1-sol"),
            ("threadline-main-gpt-6-astra", "gpt-6-astra"),
            ("threadline-main-gpt-6-sol", "gpt-6-sol"),
            ("threadline-main-gpt-6-luna", "gpt-6-luna"),
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
        }

        let utility = resolve_request_model_for_profile(
            json!({ "model": "threadline-utility-gpt-6-luna" })
                .as_object()
                .unwrap(),
            RouteProfile::Utility,
        )
        .unwrap();
        assert_eq!(utility.alias_id, "threadline-utility-gpt-6-luna");
        assert_eq!(utility.upstream_model_id, "gpt-6-luna");
        assert_eq!(utility.profile, RouteProfile::Utility);
        assert!(utility.advertised);
    }

    #[test]
    fn astra_main_alias_and_raw_id_support_persistent_reasoning() {
        for (model_id, advertised) in [
            ("threadline-main-gpt-6-astra", true),
            ("gpt-6-astra", false),
        ] {
            assert!(is_supported_model(model_id));

            let main = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert_eq!(main.alias_id, model_id);
            assert_eq!(main.upstream_model_id, "gpt-6-astra");
            assert_eq!(main.profile, RouteProfile::Main);
            assert_eq!(main.advertised, advertised);

            assert!(
                resolve_request_model_for_profile(
                    json!({ "model": model_id }).as_object().unwrap(),
                    RouteProfile::Utility,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn gpt_6_main_routes_are_advertised_and_support_persistent_reasoning() {
        for (alias_id, upstream_model_id) in [
            ("threadline-main-gpt-6.1-sol", "gpt-6.1-sol"),
            ("threadline-main-gpt-6-sol", "gpt-6-sol"),
            ("threadline-main-gpt-6-luna", "gpt-6-luna"),
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
        }

        for model_id in GPT_6_FAMILY_RAW_IDS {
            let main = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert_eq!(main.alias_id, model_id);
            assert_eq!(main.upstream_model_id, model_id);
            assert_eq!(main.profile, RouteProfile::Main);
            assert!(!main.advertised);
        }
    }

    #[test]
    fn gpt_6_luna_utility_route_is_advertised() {
        let utility = resolve_request_model_for_profile(
            json!({ "model": "threadline-utility-gpt-6-luna" })
                .as_object()
                .unwrap(),
            RouteProfile::Utility,
        )
        .unwrap();

        assert_eq!(utility.alias_id, "threadline-utility-gpt-6-luna");
        assert_eq!(utility.upstream_model_id, "gpt-6-luna");
        assert_eq!(utility.profile, RouteProfile::Utility);
        assert!(utility.advertised);
    }

    #[test]
    fn utility_luna_alias_contract_resolves_to_utility_profile() {
        let utility = resolve_request_model_for_profile(
            json!({ "model": "threadline-utility-gpt-6-luna" })
                .as_object()
                .unwrap(),
            RouteProfile::Utility,
        )
        .unwrap();

        assert_eq!(utility.alias_id, "threadline-utility-gpt-6-luna");
        assert_eq!(utility.upstream_model_id, "gpt-6-luna");
        assert_eq!(utility.profile, RouteProfile::Utility);
        assert!(utility.advertised);
    }

    #[test]
    fn utility_luna_alias_is_rejected_by_main_profile() {
        assert_eq!(
            resolve_request_model_for_profile(
                json!({ "model": "threadline-utility-gpt-6-luna" })
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
    fn utility_luna_raw_model_is_rejected_by_utility_profile() {
        assert_eq!(
            resolve_request_model_for_profile(
                json!({ "model": "gpt-6-luna" }).as_object().unwrap(),
                RouteProfile::Utility,
            )
            .unwrap_err()
            .to_string(),
            "The /v1/responses request must include a supported string model."
        );
    }

    #[test]
    fn resolve_request_model_for_profile_rejects_profile_mismatch() {
        for model_id in GPT_6_FAMILY_MAIN_ALIAS_IDS {
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
                json!({ "model": "threadline-utility-gpt-6-luna" })
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
        for model_id in GPT_6_FAMILY_RAW_IDS {
            let compatibility = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert_eq!(compatibility.alias_id, model_id);
            assert_eq!(compatibility.upstream_model_id, model_id);
            assert_eq!(compatibility.profile, RouteProfile::Main);
            assert!(!compatibility.advertised);

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
    }

    #[test]
    fn resolve_request_model_for_profile_accepts_current_main_aliases_and_raw_ids() {
        for model_id in [
            "threadline-main-gpt-6.1-sol",
            "threadline-main-gpt-6-astra",
            "threadline-main-gpt-6-sol",
            "threadline-main-gpt-6-luna",
            "gpt-6.1-sol",
            "gpt-6-astra",
            "gpt-6-sol",
            "gpt-6-luna",
        ] {
            let main = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert_eq!(main.profile, RouteProfile::Main);
        }

        let utility = resolve_request_model_for_profile(
            json!({ "model": "threadline-utility-gpt-6-luna" })
                .as_object()
                .unwrap(),
            RouteProfile::Utility,
        )
        .unwrap();
        assert_eq!(utility.profile, RouteProfile::Utility);
    }

    #[test]
    fn resolve_request_model_for_profile_keeps_current_route_profiles() {
        for model_id in GPT_6_FAMILY_MAIN_ALIAS_IDS {
            let alias = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert_eq!(alias.profile, RouteProfile::Main);
        }

        for model_id in GPT_6_FAMILY_RAW_IDS {
            let alias = resolve_request_model_for_profile(
                json!({ "model": model_id }).as_object().unwrap(),
                RouteProfile::Main,
            )
            .unwrap();
            assert_eq!(alias.profile, RouteProfile::Main);
        }

        let alias = resolve_request_model_for_profile(
            json!({ "model": "threadline-utility-gpt-6-luna" })
                .as_object()
                .unwrap(),
            RouteProfile::Utility,
        )
        .unwrap();
        assert_eq!(alias.profile, RouteProfile::Utility);
    }

    #[test]
    fn resolve_request_model_for_profile_rejects_utility_alias_for_main_profile() {
        assert_eq!(
            resolve_request_model_for_profile(
                json!({ "model": "threadline-utility-gpt-6-luna" })
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
    fn resolve_request_model_for_profile_rejects_unknown_alias() {
        assert_eq!(
            resolve_request_model_for_profile(
                json!({ "model": "gpt-6-nano" }).as_object().unwrap(),
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
            validate_request_model(
                json!({ "model": { "id": "gpt-6-sol" } })
                    .as_object()
                    .unwrap()
            )
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
                json!({ "model": "threadline-main-gpt-6-sol" })
                    .as_object()
                    .unwrap()
            )
            .unwrap(),
            "threadline-main-gpt-6-sol"
        );
        assert_eq!(
            validate_request_model(
                json!({ "model": "threadline-utility-gpt-6-luna" })
                    .as_object()
                    .unwrap()
            )
            .unwrap_err()
            .to_string(),
            "The /v1/responses request must include a supported string model."
        );
    }
}
