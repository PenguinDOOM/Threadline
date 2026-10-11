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
mod tests;
