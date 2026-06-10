use serde_json::{Map, Value};

use crate::errors::ThreadlineError;

pub const SUPPORTED_MODEL_IDS: [&str; 4] =
    ["gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex-spark"];

pub fn supported_model_ids() -> &'static [&'static str] {
    &SUPPORTED_MODEL_IDS
}

pub fn is_supported_model(model_id: &str) -> bool {
    SUPPORTED_MODEL_IDS.contains(&model_id)
}

pub fn validate_request_model(payload: &Map<String, Value>) -> Result<&str, ThreadlineError> {
    let model_id = payload
        .get("model")
        .and_then(Value::as_str)
        .ok_or(ThreadlineError::InvalidModel)?;

    if is_supported_model(model_id) {
        Ok(model_id)
    } else {
        Err(ThreadlineError::InvalidModel)
    }
}

#[cfg(test)]
mod tests {
    use super::{is_supported_model, supported_model_ids, validate_request_model};
    use serde_json::json;

    #[test]
    fn supported_model_ids_match_public_contract() {
        assert_eq!(
            supported_model_ids(),
            &["gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex-spark",]
        );
    }

    #[test]
    fn supported_model_check_accepts_only_contract_models() {
        assert!(is_supported_model("gpt-5.5"));
        assert!(is_supported_model("gpt-5.4"));
        assert!(is_supported_model("gpt-5.4-mini"));
        assert!(is_supported_model("gpt-5.3-codex-spark"));
        assert!(!is_supported_model("codex-mini-latest"));
    }

    #[test]
    fn validate_request_model_requires_supported_string_model() {
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
            validate_request_model(json!({ "model": "gpt-5.4" }).as_object().unwrap()).unwrap(),
            "gpt-5.4"
        );
    }
}
