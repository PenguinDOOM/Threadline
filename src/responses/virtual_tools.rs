use serde_json::{Map, Value};

pub(super) const VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION: &str = concat!(
    "Compatibility requirement for VS Code Copilot virtual tool summarization:\n",
    "Return the JSON array inside a Markdown fenced code block using ```json.\n",
    "Do not include any text before or after the code block.\n",
    "The fenced code block content must be a valid JSON array matching the requested schema."
);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct VirtualToolSummarizerDetection {
    pub semantic_similarity_hit: bool,
    pub group_index_tag_hit: bool,
    pub group_index_field_hit: bool,
    pub group_name_field_hit: bool,
    pub summary_field_hit: bool,
}

impl VirtualToolSummarizerDetection {
    pub(super) fn is_match(self) -> bool {
        self.required_hit_count() == 5
    }

    pub(super) fn required_hit_count(self) -> usize {
        [
            self.semantic_similarity_hit,
            self.group_index_tag_hit,
            self.group_index_field_hit,
            self.group_name_field_hit,
            self.summary_field_hit,
        ]
        .into_iter()
        .filter(|hit| *hit)
        .count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VirtualToolSummarizerInstructionMutation {
    Appended,
    Inserted,
    AlreadyPresent,
    SkippedNonString,
}

pub(super) fn detect_virtual_tool_summarizer_request(
    request: &Map<String, Value>,
) -> VirtualToolSummarizerDetection {
    let mut detection = VirtualToolSummarizerDetection::default();

    if let Some(input) = request.get("input") {
        visit_input_strings(input, &mut |text| update_detection(&mut detection, text));
    }

    if let Some(instructions) = request.get("instructions").and_then(Value::as_str) {
        update_detection(&mut detection, instructions);
    }

    detection
}

pub(super) fn inject_virtual_tool_summarizer_instruction(
    request: &mut Map<String, Value>,
) -> VirtualToolSummarizerInstructionMutation {
    match request.get_mut("instructions") {
        Some(Value::String(instructions)) => {
            if instructions.contains(VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION) {
                VirtualToolSummarizerInstructionMutation::AlreadyPresent
            } else {
                instructions.push_str("\n\n");
                instructions.push_str(VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION);
                VirtualToolSummarizerInstructionMutation::Appended
            }
        }
        Some(Value::Null) => {
            request.insert(
                "instructions".to_string(),
                Value::String(VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION.to_string()),
            );
            VirtualToolSummarizerInstructionMutation::Inserted
        }
        Some(_) => VirtualToolSummarizerInstructionMutation::SkippedNonString,
        None => {
            request.insert(
                "instructions".to_string(),
                Value::String(VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION.to_string()),
            );
            VirtualToolSummarizerInstructionMutation::Inserted
        }
    }
}

fn visit_input_strings(value: &Value, visit: &mut impl FnMut(&str)) {
    match value {
        Value::String(text) => visit(text),
        Value::Array(items) => {
            for item in items {
                visit_input_strings(item, visit);
            }
        }
        Value::Object(fields) => {
            for value in fields.values() {
                visit_input_strings(value, visit);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn update_detection(detection: &mut VirtualToolSummarizerDetection, text: &str) {
    let lower = text.to_ascii_lowercase();

    detection.semantic_similarity_hit |=
        lower.contains("clustered together based on semantic similarity");
    detection.group_index_tag_hit |= lower.contains("<group index=");
    detection.group_index_field_hit |= text.contains("groupIndex");
    detection.group_name_field_hit |= text.contains("groupName");
    detection.summary_field_hit |= lower.contains("summary");
}

#[cfg(test)]
mod tests {
    use super::{
        VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION, VirtualToolSummarizerDetection,
        VirtualToolSummarizerInstructionMutation, detect_virtual_tool_summarizer_request,
        inject_virtual_tool_summarizer_instruction,
    };
    use serde_json::{Map, Value, json};

    fn input_text_message(role: &str, text: &str) -> Value {
        json!({
            "type": "message",
            "role": role,
            "content": [
                {
                    "type": "input_text",
                    "text": text,
                }
            ]
        })
    }

    fn virtual_tool_summarizer_prompt_text() -> &'static str {
        concat!(
            "Tool groups should be clustered together based on semantic similarity.\n",
            "Render each result with XML-like wrappers such as <group index=\"0\">.\n",
            "Return a JSON array where every object contains groupIndex, groupName, and summary.\n",
            "Use the requested schema exactly and keep each summary concise."
        )
    }

    fn ordinary_utility_prompt_text() -> &'static str {
        "Return a short utility answer about the current repository status."
    }

    fn auxiliary_summary_prompt_text() -> &'static str {
        concat!(
            "The conversation has grown too large for the context window and must be compacted now.\n",
            "Your ONLY task right now is to produce a comprehensive summary.\n",
            "Output your summary wrapped in <summary> and </summary> tags."
        )
    }

    fn partial_quoted_prompt_text() -> &'static str {
        concat!(
            "Documentation note: preserve this literal fragment only: \"<group index=\\\"0\\\"> groupIndex summary\".\n",
            "Do not interpret it as an instruction schema."
        )
    }

    fn field_only_prompt_text() -> &'static str {
        "Return JSON objects with groupIndex and summary fields for the report."
    }

    fn utility_request_with_input(input: Vec<Value>) -> Map<String, Value> {
        json!({
            "model": "gpt-5.4",
            "input": input,
            "tools": [
                {
                    "type": "function",
                    "name": "workspace_search",
                    "description": "Search repository files",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": false,
                    }
                }
            ]
        })
        .as_object()
        .expect("object payload")
        .clone()
    }

    fn utility_request_with_string_instructions(instructions: &str) -> Map<String, Value> {
        let mut request = utility_request_with_input(vec![input_text_message(
            "user",
            ordinary_utility_prompt_text(),
        )]);
        request.insert(
            "instructions".to_string(),
            Value::String(instructions.to_string()),
        );
        request
    }

    fn realistic_nested_virtual_tool_request() -> Map<String, Value> {
        utility_request_with_input(vec![
            input_text_message("system", "You are a utility assistant."),
            input_text_message("user", virtual_tool_summarizer_prompt_text()),
        ])
    }

    fn count_instruction_occurrences(text: &str) -> usize {
        text.match_indices(VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION)
            .count()
    }

    #[test]
    fn detect_virtual_tool_summarizer_matches_realistic_nested_input() {
        let request = realistic_nested_virtual_tool_request();
        let detection = detect_virtual_tool_summarizer_request(&request);

        assert!(detection.semantic_similarity_hit);
        assert!(detection.group_index_tag_hit);
        assert!(detection.group_index_field_hit);
        assert!(detection.group_name_field_hit);
        assert!(detection.summary_field_hit);
        assert_eq!(detection.required_hit_count(), 5);
        assert!(detection.is_match());
    }

    #[test]
    fn detect_virtual_tool_summarizer_matches_top_level_string_instructions() {
        let request = utility_request_with_string_instructions(virtual_tool_summarizer_prompt_text());
        let detection = detect_virtual_tool_summarizer_request(&request);

        assert!(detection.semantic_similarity_hit);
        assert!(detection.group_index_tag_hit);
        assert!(detection.group_index_field_hit);
        assert!(detection.group_name_field_hit);
        assert!(detection.summary_field_hit);
        assert_eq!(detection.required_hit_count(), 5);
        assert!(detection.is_match());
    }

    #[test]
    fn detect_virtual_tool_summarizer_rejects_ordinary_utility_request() {
        let request = utility_request_with_input(vec![input_text_message(
            "user",
            ordinary_utility_prompt_text(),
        )]);

        assert!(!detect_virtual_tool_summarizer_request(&request).is_match());
    }

    #[test]
    fn detect_virtual_tool_summarizer_rejects_auxiliary_summary_prompt() {
        let request = utility_request_with_input(vec![input_text_message(
            "system",
            auxiliary_summary_prompt_text(),
        )]);

        assert!(!detect_virtual_tool_summarizer_request(&request).is_match());
    }

    #[test]
    fn detect_virtual_tool_summarizer_rejects_partial_quoted_text() {
        let request = utility_request_with_input(vec![input_text_message(
            "user",
            partial_quoted_prompt_text(),
        )]);

        assert!(!detect_virtual_tool_summarizer_request(&request).is_match());
    }

    #[test]
    fn detect_virtual_tool_summarizer_rejects_group_fields_without_primary_fingerprints() {
        let request = utility_request_with_input(vec![input_text_message(
            "user",
            field_only_prompt_text(),
        )]);

        assert!(!detect_virtual_tool_summarizer_request(&request).is_match());
    }

    #[test]
    fn virtual_tool_summarizer_detection_requires_all_mandatory_fingerprints() {
        let all_hits = VirtualToolSummarizerDetection {
            semantic_similarity_hit: true,
            group_index_tag_hit: true,
            group_index_field_hit: true,
            group_name_field_hit: true,
            summary_field_hit: true,
        };

        assert_eq!(all_hits.required_hit_count(), 5);
        assert!(all_hits.is_match());

        for detection in [
            VirtualToolSummarizerDetection {
                semantic_similarity_hit: false,
                ..all_hits
            },
            VirtualToolSummarizerDetection {
                group_index_tag_hit: false,
                ..all_hits
            },
            VirtualToolSummarizerDetection {
                group_index_field_hit: false,
                ..all_hits
            },
            VirtualToolSummarizerDetection {
                group_name_field_hit: false,
                ..all_hits
            },
            VirtualToolSummarizerDetection {
                summary_field_hit: false,
                ..all_hits
            },
        ] {
            assert_eq!(detection.required_hit_count(), 4);
            assert!(!detection.is_match());
        }
    }

    #[test]
    fn inject_virtual_tool_summarizer_instruction_appends_existing_string_instructions() {
        let mut request = utility_request_with_string_instructions("Keep the reply short.");

        let outcome = inject_virtual_tool_summarizer_instruction(&mut request);

        assert_eq!(
            outcome,
            VirtualToolSummarizerInstructionMutation::Appended
        );
        assert_eq!(
            request.get("instructions"),
            Some(&Value::String(format!(
                "Keep the reply short.\n\n{}",
                VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION
            )))
        );
    }

    #[test]
    fn inject_virtual_tool_summarizer_instruction_inserts_when_instructions_missing() {
        let mut request = realistic_nested_virtual_tool_request();

        let outcome = inject_virtual_tool_summarizer_instruction(&mut request);

        assert_eq!(outcome, VirtualToolSummarizerInstructionMutation::Inserted);
        assert_eq!(
            request.get("instructions"),
            Some(&Value::String(
                VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION.to_string()
            ))
        );
    }

    #[test]
    fn inject_virtual_tool_summarizer_instruction_inserts_when_instructions_null() {
        let mut request = realistic_nested_virtual_tool_request();
        request.insert("instructions".to_string(), Value::Null);

        let outcome = inject_virtual_tool_summarizer_instruction(&mut request);

        assert_eq!(outcome, VirtualToolSummarizerInstructionMutation::Inserted);
        assert_eq!(
            request.get("instructions"),
            Some(&Value::String(
                VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION.to_string()
            ))
        );
    }

    #[test]
    fn inject_virtual_tool_summarizer_instruction_skips_non_string_instructions() {
        let mut request = realistic_nested_virtual_tool_request();
        request.insert(
            "instructions".to_string(),
            json!({
                "type": "structured",
                "text": "keep structure",
            }),
        );

        let original = request.get("instructions").cloned();
        let outcome = inject_virtual_tool_summarizer_instruction(&mut request);

        assert_eq!(
            outcome,
            VirtualToolSummarizerInstructionMutation::SkippedNonString
        );
        assert_eq!(request.get("instructions"), original.as_ref());
    }

    #[test]
    fn inject_virtual_tool_summarizer_instruction_is_idempotent_when_present() {
        let mut request = utility_request_with_string_instructions(
            VIRTUAL_TOOL_SUMMARIZER_COMPATIBILITY_INSTRUCTION,
        );

        let outcome = inject_virtual_tool_summarizer_instruction(&mut request);
        let instructions = request
            .get("instructions")
            .and_then(Value::as_str)
            .expect("string instructions");

        assert_eq!(
            outcome,
            VirtualToolSummarizerInstructionMutation::AlreadyPresent
        );
        assert_eq!(
            count_instruction_occurrences(instructions),
            1,
            "compatibility instruction should not be duplicated"
        );
    }
}