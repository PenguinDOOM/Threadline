use super::*;

#[test]
fn parse_downstream_request_classifies_interaction_type_conversation_compaction() {
    let request = parse_input_with_metadata(
        vec![input_text_message(
            "user",
            "Please continue the earlier task.",
        )],
        DownstreamRequestMetadata::from_interaction_type_header_value(Some(
            "conversation-compaction",
        )),
    );

    assert_eq!(
        request.routing_diagnostics().interaction_type,
        DownstreamInteractionType::ConversationCompaction
    );
    assert!(
        request
            .routing_diagnostics()
            .interaction_type_compaction_hit
    );
    assert_eq!(
        request.classification,
        DownstreamRequestClassification::AuxiliarySummary
    );
}

#[test]
fn parse_downstream_request_trims_and_lowercases_interaction_type() {
    let request = parse_input_with_metadata(
        vec![input_text_message(
            "user",
            "Please continue the earlier task.",
        )],
        DownstreamRequestMetadata::from_interaction_type_header_value(Some(
            " Conversation-Compaction ",
        )),
    );

    assert_eq!(
        request.routing_diagnostics().interaction_type,
        DownstreamInteractionType::ConversationCompaction
    );
    assert!(
        request
            .routing_diagnostics()
            .interaction_type_compaction_hit
    );
    assert_eq!(
        request.classification,
        DownstreamRequestClassification::AuxiliarySummary
    );
}

#[test]
fn parse_downstream_request_ignores_unknown_empty_non_utf8_or_long_interaction_type() {
    let long_value = "x".repeat(65);

    for (name, metadata, expected_interaction_type) in
        unrecognized_interaction_type_cases(long_value)
    {
        let request = parse_input_with_metadata(
            vec![input_text_message(
                "user",
                "Please continue the earlier task.",
            )],
            metadata,
        );

        assert_eq!(
            request.classification,
            DownstreamRequestClassification::Normal,
            "fixture should remain normal: {name}"
        );
        assert_eq!(
            request.routing_diagnostics().interaction_type,
            expected_interaction_type,
            "fixture should use allowlisted interaction type diagnostics: {name}"
        );
        assert!(
            !request
                .routing_diagnostics()
                .interaction_type_compaction_hit
        );
    }
}

fn unrecognized_interaction_type_cases(
    long_value: String,
) -> [(
    &'static str,
    DownstreamRequestMetadata,
    DownstreamInteractionType,
); 5] {
    [
        (
            "missing",
            DownstreamRequestMetadata::default(),
            DownstreamInteractionType::None,
        ),
        (
            "empty",
            DownstreamRequestMetadata::from_interaction_type_header_value(Some("   ")),
            DownstreamInteractionType::None,
        ),
        (
            "unknown",
            DownstreamRequestMetadata::from_interaction_type_header_value(Some(
                "conversation-start",
            )),
            DownstreamInteractionType::Other,
        ),
        (
            "non_utf8",
            DownstreamRequestMetadata::from_interaction_type_header_bytes(Some(b"\xFF")),
            DownstreamInteractionType::Other,
        ),
        (
            "too_long",
            DownstreamRequestMetadata::from_interaction_type_header_value(Some(&long_value)),
            DownstreamInteractionType::Other,
        ),
    ]
}
