use super::*;

pub(super) fn terminal_observation_cases() -> [UpstreamTerminalState; 3] {
    [
        UpstreamTerminalState::Closed(empty_close_metadata()),
        UpstreamTerminalState::LivenessTimeout(UpstreamLivenessTimeout {
            cause: UpstreamLivenessTimeoutCause::WriteDeadline,
            timeout: Duration::from_secs(1),
            elapsed: Duration::from_secs(1),
            outbound_kind: Some(UpstreamOutboundKind::Text),
        }),
        UpstreamTerminalState::InboundBufferOverflow(InboundBufferOverflow {
            cause: InboundBufferOverflowCause::MessageCount,
            queued_messages: 1,
            queued_bytes: 1,
            incoming_bytes: 1,
            max_messages: 1,
            max_bytes: 1,
        }),
    ]
}
