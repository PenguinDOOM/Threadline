use super::*;

impl SummaryFingerprintHits {
    pub(in crate::responses) fn matches_auxiliary_summary(&self) -> bool {
        self.matches_manual_summary()
            || self.matches_auto_summary()
            || self.matches_new_auto_summary()
    }

    fn matches_manual_summary(&self) -> bool {
        let manual_primary = self.manual_summary_prompt_instruction_like;
        let manual_secondary = self.manual_structure_instruction_instruction_like
            || self.manual_tool_results_instruction_instruction_like
            || self.simple_history_context_instruction_like;
        manual_primary && manual_secondary
    }

    fn matches_auto_summary(&self) -> bool {
        let auto_primary = self.auto_context_too_large_instruction_like;
        let auto_secondary = self.auto_summary_tags_instruction_like
            || self.auto_only_task_instruction_like
            || self.simple_history_context_instruction_like;
        auto_primary && auto_secondary
    }

    fn matches_new_auto_summary(&self) -> bool {
        let new_auto_with_history = self.new_auto_detailed_summary_instruction_like
            && self.new_auto_user_history_hit
            && self.new_auto_user_final_summary_prompt_hit;
        let new_foreground = self.new_auto_detailed_summary_instruction_like
            && self.new_auto_user_final_summary_prompt_hit;
        new_auto_with_history || new_foreground
    }

    fn record_text(&mut self, text: &str, context: SummaryObservationContext<'_>) {
        self.record_text_with_instruction_like(text, context.is_summary_instruction_like());

        if text.contains(NEW_AUTO_DETAILED_SUMMARY_INSTRUCTION) {
            self.new_auto_detailed_summary_hit = true;
            self.new_auto_detailed_summary_instruction_like |=
                context.is_summary_instruction_like();
        }

        if context.is_user_input_text()
            && (text.contains(SIMPLE_HISTORY_CONTEXT_OBSERVED)
                || text.contains(SIMPLE_HISTORY_CONTEXT_CORRECTED))
        {
            self.new_auto_user_history_hit = true;
        }

        if context.is_final_user_input_text()
            && text.contains(MANUAL_SUMMARY_PROMPT)
            && text.contains(MANUAL_STRUCTURE_INSTRUCTION)
            && text.contains(MANUAL_TOOL_RESULTS_INSTRUCTION)
        {
            self.new_auto_user_final_summary_prompt_hit = true;
        }
    }

    fn record_text_with_instruction_like(&mut self, text: &str, instruction_like: bool) {
        let had_instruction_like_hit = self.has_instruction_like_hit();
        self.record_manual_summary_text(text, instruction_like);
        self.record_auto_summary_text(text, instruction_like);
        self.record_history_context_text(text, instruction_like);
        let has_instruction_like_hit = self.has_instruction_like_hit();
        self.summary_instruction_like_hit |=
            instruction_like && (had_instruction_like_hit || has_instruction_like_hit);
    }

    fn has_instruction_like_hit(&self) -> bool {
        self.manual_summary_prompt_instruction_like
            || self.manual_structure_instruction_instruction_like
            || self.manual_tool_results_instruction_instruction_like
            || self.auto_context_too_large_instruction_like
            || self.auto_summary_tags_instruction_like
            || self.auto_only_task_instruction_like
            || self.simple_history_context_instruction_like
    }

    fn record_manual_summary_text(&mut self, text: &str, instruction_like: bool) {
        if text.contains(MANUAL_SUMMARY_PROMPT) {
            self.manual_summary_prompt_hit = true;
            self.manual_summary_prompt_instruction_like |= instruction_like;
        }
        if text.contains(MANUAL_STRUCTURE_INSTRUCTION) {
            self.manual_structure_instruction_hit = true;
            self.manual_structure_instruction_instruction_like |= instruction_like;
        }
        if text.contains(MANUAL_TOOL_RESULTS_INSTRUCTION) {
            self.manual_tool_results_instruction_hit = true;
            self.manual_tool_results_instruction_instruction_like |= instruction_like;
        }
    }

    fn record_auto_summary_text(&mut self, text: &str, instruction_like: bool) {
        if text.contains(AUTO_CONTEXT_TOO_LARGE_PROMPT) {
            self.auto_context_too_large_hit = true;
            self.auto_context_too_large_instruction_like |= instruction_like;
        }
        if text.contains(AUTO_SUMMARY_TAGS_INSTRUCTION) {
            self.auto_summary_tags_hit = true;
            self.auto_summary_tags_instruction_like |= instruction_like;
        }
        if text.contains(AUTO_ONLY_TASK_INSTRUCTION) {
            self.auto_only_task_hit = true;
            self.auto_only_task_instruction_like |= instruction_like;
        }
    }

    fn record_history_context_text(&mut self, text: &str, instruction_like: bool) {
        if text.contains(SIMPLE_HISTORY_CONTEXT_OBSERVED)
            || text.contains(SIMPLE_HISTORY_CONTEXT_CORRECTED)
        {
            self.simple_history_context_hit = true;
            self.simple_history_context_instruction_like |= instruction_like;
        }
    }
}

impl InputSourceCategory {
    fn from_role(role: Option<&str>) -> Self {
        match role {
            Some("system" | "developer") => Self::SummaryInstructionLike,
            Some("user") => Self::OrdinaryUserContent,
            Some(_) | None => Self::UnknownInputContent,
        }
    }

    fn is_summary_instruction_like(self) -> bool {
        matches!(self, Self::SummaryInstructionLike)
    }
}

impl SummaryObservationContext<'_> {
    fn is_summary_instruction_like(self) -> bool {
        self.under_content_array
            && self.content_item_type == Some("input_text")
            && self.source_category.is_summary_instruction_like()
    }

    fn is_user_input_text(self) -> bool {
        self.under_content_array
            && self.content_item_type == Some("input_text")
            && self.source_category == InputSourceCategory::OrdinaryUserContent
    }

    fn is_final_user_input_text(self) -> bool {
        self.final_input_item && self.is_user_input_text()
    }
}

pub(super) fn collect_summary_fingerprints(input: Option<&Value>) -> SummaryFingerprintHits {
    let Some(input) = input else {
        return SummaryFingerprintHits::default();
    };

    let mut fingerprints = SummaryFingerprintHits::default();
    collect_summary_fingerprints_into_input(input, &mut fingerprints);
    fingerprints
}

pub(super) fn collect_conflict_fallback_summary_fingerprints(
    input: &Value,
) -> SummaryFingerprintHits {
    let mut fingerprints = SummaryFingerprintHits::default();

    match input {
        Value::Array(items) => {
            for item in items {
                collect_conflict_fallback_summary_from_input_item(item, &mut fingerprints);
            }
        }
        _ => collect_conflict_fallback_summary_from_input_item(input, &mut fingerprints),
    }

    fingerprints
}

pub(super) fn collect_conflict_fallback_summary_from_input_item(
    value: &Value,
    fingerprints: &mut SummaryFingerprintHits,
) {
    let Some(item) = value.as_object() else {
        return;
    };

    match item.get("type").and_then(Value::as_str) {
        Some("message") => {
            let source_category =
                InputSourceCategory::from_role(item.get("role").and_then(Value::as_str));
            if let Some(content) = item.get("content").and_then(Value::as_array) {
                for content_item in content {
                    collect_conflict_fallback_summary_from_content_item(
                        content_item,
                        source_category,
                        fingerprints,
                    );
                }
            }
        }
        Some("input_text") => {
            let Some(text) = item.get("text").and_then(Value::as_str) else {
                return;
            };

            // Direct top-level input_text summary prompts remain eligible as explicit fallback traffic.
            fingerprints.record_text_with_instruction_like(text, true);
        }
        Some(_) | None => {}
    }
}

pub(super) fn collect_conflict_fallback_summary_from_content_item(
    value: &Value,
    source_category: InputSourceCategory,
    fingerprints: &mut SummaryFingerprintHits,
) {
    let Some(item) = value.as_object() else {
        return;
    };

    if item.get("type").and_then(Value::as_str) != Some("input_text") {
        return;
    }

    let Some(text) = item.get("text").and_then(Value::as_str) else {
        return;
    };

    fingerprints.record_text(
        text,
        SummaryObservationContext {
            content_item_type: Some("input_text"),
            under_content_array: true,
            source_category,
            ..SummaryObservationContext::default()
        },
    );
}

pub(super) fn collect_summary_fingerprints_into_input(
    value: &Value,
    fingerprints: &mut SummaryFingerprintHits,
) {
    match value {
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let context = SummaryObservationContext {
                    final_input_item: index + 1 == items.len(),
                    ..SummaryObservationContext::default()
                };
                collect_summary_fingerprints_from_input_item(item, context, fingerprints);
            }
        }
        _ => collect_summary_fingerprints_into(
            value,
            SummaryObservationContext::default(),
            fingerprints,
        ),
    }
}

pub(super) fn collect_summary_fingerprints_from_input_item<'a>(
    value: &'a Value,
    mut context: SummaryObservationContext<'a>,
    fingerprints: &mut SummaryFingerprintHits,
) {
    if let Some(item) = value.as_object() {
        context.content_item_type = item.get("type").and_then(Value::as_str);
        if context.content_item_type == Some("message") {
            context.message_role = item.get("role").and_then(Value::as_str);
            context.source_category = InputSourceCategory::from_role(context.message_role);

            if let Some(content) = item.get("content").and_then(Value::as_array) {
                for content_item in content {
                    let mut content_context = context;
                    content_context.under_content_array = true;
                    content_context.content_item_type = content_item
                        .as_object()
                        .and_then(|object| object.get("type"))
                        .and_then(Value::as_str);
                    collect_summary_fingerprints_into(content_item, content_context, fingerprints);
                }
                return;
            }
        }
    }

    collect_summary_fingerprints_into(value, context, fingerprints);
}

pub(super) fn collect_summary_fingerprints_into<'a>(
    value: &'a Value,
    context: SummaryObservationContext<'a>,
    fingerprints: &mut SummaryFingerprintHits,
) {
    match value {
        Value::String(text) => fingerprints.record_text(text, context),
        Value::Array(values) => {
            for value in values {
                collect_summary_fingerprints_into(value, context, fingerprints);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_summary_fingerprints_into(value, context, fingerprints);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}
