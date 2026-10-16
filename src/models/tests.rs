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
