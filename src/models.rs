pub const SUPPORTED_MODEL_IDS: [&str; 4] =
    ["gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-5.3-codex-spark"];

pub fn supported_model_ids() -> &'static [&'static str] {
    &SUPPORTED_MODEL_IDS
}

pub fn is_supported_model(model_id: &str) -> bool {
    SUPPORTED_MODEL_IDS.contains(&model_id)
}

#[cfg(test)]
mod tests {
    use super::{is_supported_model, supported_model_ids};

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
}
