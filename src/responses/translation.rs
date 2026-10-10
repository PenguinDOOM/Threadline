mod diagnostics;
use diagnostics::*;
mod visible_text;
use visible_text::*;
mod observable_output;
use observable_output::*;
mod queue;
use queue::*;
mod receive;
use receive::*;
mod internal_tools;
use internal_tools::*;
mod progression;
use progression::*;
pub(super) mod recovery;

use std::collections::{HashSet, VecDeque};
use std::convert::Infallible;
use std::mem;
use std::sync::Arc;

use axum::body::Bytes;
use futures_util::stream;
use serde_json::Value;
use tracing::{debug, trace};

use crate::errors::ThreadlineError;
use crate::registry::RetainedSessionLease;
use crate::tools::{
    InternalToolCall, PendingInternalToolOutput, build_followup_input,
    event_contains_internal_tool_name, is_internal_tool_name,
};
use crate::ws_pump::{ConsumerPhase, LiveUpstreamWebSocket};

use super::downstream::{
    safe_scalar_field, sse_done_chunk, sse_error_chunk, sse_json_chunk,
    sse_terminal_response_failed_chunk, sse_terminal_response_incomplete_chunk,
};
use super::upstream::{ThreadlineServices, send_followup_tool_outputs};

fn response_id_from_event(event: &Value) -> Option<&str> {
    event
        .get("response_id")
        .and_then(Value::as_str)
        .or_else(|| {
            event
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
        })
}

fn output_index_from_event(event: &Value) -> Option<u64> {
    event.get("output_index").and_then(Value::as_u64)
}

fn is_upstream_previous_response_not_found_error(
    error_code: Option<&str>,
    error_message: Option<&str>,
) -> bool {
    if error_code == Some("previous_response_not_found") {
        return true;
    }

    error_message.is_some_and(|message| {
        message.contains("Previous response with id") && message.contains("not found")
    })
}

pub(super) enum ResponseStreamLease {
    Retained(RetainedSessionLease),
    TransientAuxiliary,
}

impl ResponseStreamLease {
    fn release(&mut self) {
        if let Self::Retained(lease) = self {
            lease.release();
        }
    }

    fn record_completed_marker_and_disarm(&mut self, response_marker: &str) {
        if let Self::Retained(lease) = self {
            lease.record_completed_marker_and_disarm(response_marker);
        }
    }

    fn disarm_active_turn(&mut self) {
        if let Self::Retained(lease) = self {
            lease.disarm_active_turn();
        }
    }

    fn finalize_recoverable_turn(&mut self) {
        if let Self::Retained(lease) = self {
            lease.finalize_recoverable_turn();
        }
    }

    fn finalize_liveness_timeout_turn(&mut self) {
        if let Self::Retained(lease) = self {
            lease.finalize_liveness_timeout_turn();
        }
    }

    fn finalize_policy_violation_turn(&mut self) {
        if let Self::Retained(lease) = self {
            lease.finalize_policy_violation_turn();
        }
    }

    async fn mark_upstream_terminal(&mut self) {
        if let Self::Retained(lease) = self {
            lease.mark_upstream_terminal().await;
        }
    }
}

const RESPONSES_TRANSLATION_UPSTREAM_EVENT: &str = "responses_translation_upstream_event";
const RESPONSES_TRANSLATION_DOWNSTREAM_SSE_EVENT: &str =
    "responses_translation_downstream_sse_event";
const RESPONSES_TRANSLATION_EVENT_SUPPRESSED: &str = "responses_translation_event_suppressed";
const RESPONSES_TRANSLATION_NO_OBSERVABLE_OUTPUT_GUARD: &str =
    "responses_translation_no_observable_output_guard";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DownstreamTraceAction {
    Forwarded,
    Suppressed,
    Terminal,
    ErrorTranslated,
}

impl DownstreamTraceAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Forwarded => "forwarded",
            Self::Suppressed => "suppressed",
            Self::Terminal => "terminal",
            Self::ErrorTranslated => "error-translated",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct UpstreamEventTraceMetadata {
    event_type: String,
    response_id: Option<String>,
    item_type: Option<String>,
    item_name: Option<String>,
    call_id: Option<String>,
    arguments_length: Option<usize>,
    delta_length: Option<usize>,
    output_index: Option<u64>,
    content_index: Option<u64>,
    item_id: Option<String>,
    is_compaction: bool,
    compaction_id: Option<String>,
    has_encrypted_content: Option<bool>,
}

impl UpstreamEventTraceMetadata {
    fn from_event(event: &Value) -> Self {
        let item = event.get("item");
        let item_type = string_field(item.and_then(|value| value.get("type")))
            .or_else(|| string_field(event.get("item_type")));
        let item_id = string_field(event.get("item_id"))
            .or_else(|| string_field(item.and_then(|value| value.get("id"))));
        let is_compaction = item_type.as_deref() == Some("compaction");
        Self {
            event_type: event
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("message")
                .to_string(),
            response_id: response_id_from_event(event).map(ToString::to_string),
            item_type,
            item_name: string_field(item.and_then(|value| value.get("name")))
                .or_else(|| string_field(event.get("name")))
                .or_else(|| string_field(event.get("tool_name"))),
            call_id: string_field(item.and_then(|value| value.get("call_id")))
                .or_else(|| string_field(event.get("call_id"))),
            arguments_length: string_length_field(
                item.and_then(|value| value.get("arguments"))
                    .or_else(|| event.get("arguments")),
            ),
            delta_length: string_length_field(event.get("delta")),
            output_index: output_index_from_event(event),
            content_index: event.get("content_index").and_then(Value::as_u64),
            item_id: item_id.clone(),
            is_compaction,
            compaction_id: is_compaction.then_some(item_id).flatten(),
            has_encrypted_content: is_compaction.then_some(
                item.and_then(|value| value.get("encrypted_content"))
                    .is_some(),
            ),
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct DownstreamTraceDiagnostics {
    response_id: Option<String>,
    synthetic_delta_source: Option<&'static str>,
    visible_text_delta_count: Option<usize>,
    visible_text_length: Option<usize>,
    sanitized_internal_function_call_count: Option<usize>,
    sanitized_compaction_count: Option<usize>,
    completed_visible_message_count: Option<usize>,
}

#[derive(Debug, PartialEq, Eq)]
struct DownstreamSseTraceMetadata {
    translation_action: &'static str,
    event_type: String,
    response_id: Option<String>,
    item_type: Option<String>,
    item_name: Option<String>,
    call_id: Option<String>,
    arguments_length: Option<usize>,
    delta_length: Option<usize>,
    output_index: Option<u64>,
    content_index: Option<u64>,
    item_id: Option<String>,
    is_compaction: bool,
    compaction_id: Option<String>,
    has_encrypted_content: Option<bool>,
    synthetic_delta_source: Option<&'static str>,
    visible_text_delta_count: Option<usize>,
    visible_text_length: Option<usize>,
    sanitized_internal_function_call_count: Option<usize>,
    sanitized_compaction_count: Option<usize>,
    completed_visible_message_count: Option<usize>,
}

#[derive(Debug, PartialEq, Eq)]
struct NoObservableOutputGuardDiagnostics {
    response_id: Option<String>,
    pending_internal_outputs_count: usize,
    suppressed_internal_tool_call_count: usize,
    forwarded_external_tool_call_count: usize,
    forwarded_compaction_or_marker_count: usize,
    visible_assistant_text_len: usize,
    completed_output_item_types: Vec<String>,
    upstream_last_event_type: Option<String>,
    is_intermediate_completed: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct VisibleTextSourceKey {
    item_id: Option<String>,
    output_index: Option<u64>,
    content_index: Option<u64>,
}

impl VisibleTextSourceKey {
    fn new(item_id: Option<String>, output_index: Option<u64>, content_index: Option<u64>) -> Self {
        Self {
            item_id,
            output_index,
            content_index,
        }
    }

    fn dedupe_identity(&self) -> Option<VisibleTextDedupeIdentity> {
        if let Some(item_id) = self.item_id.as_ref() {
            return Some(VisibleTextDedupeIdentity::ItemId {
                item_id: item_id.clone(),
                content_index: self.content_index,
            });
        }

        self.output_index
            .map(|output_index| VisibleTextDedupeIdentity::OutputIndex {
                output_index,
                content_index: self.content_index,
            })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum VisibleTextDedupeIdentity {
    ItemId {
        item_id: String,
        content_index: Option<u64>,
    },
    OutputIndex {
        output_index: u64,
        content_index: Option<u64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct VisibleAssistantText {
    key: VisibleTextSourceKey,
    text: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct DownstreamObservableOutputState {
    forwarded_visible_text_delta_count: usize,
    forwarded_external_tool_call_count: usize,
    forwarded_image_generation_count: usize,
    forwarded_marker_like_output_count: usize,
    final_visible_message_count: usize,
    final_external_tool_call_count: usize,
    final_image_generation_count: usize,
    final_marker_like_output_count: usize,
    last_upstream_event_type: Option<String>,
}

impl DownstreamObservableOutputState {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn has_observable_output(&self) -> bool {
        self.forwarded_visible_text_delta_count > 0
            || self.forwarded_external_tool_call_count > 0
            || self.forwarded_image_generation_count > 0
            || self.forwarded_marker_like_output_count > 0
            || self.final_visible_message_count > 0
            || self.final_external_tool_call_count > 0
            || self.final_image_generation_count > 0
            || self.final_marker_like_output_count > 0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CompletedSanitizationDiagnostics {
    sanitized_internal_function_call_count: usize,
    sanitized_compaction_count: usize,
    completed_visible_message_count: usize,
}

#[derive(Clone, Debug)]
pub(super) struct QueuedSyntheticOutputTextDelta {
    payload: Value,
    response_id: Option<String>,
    synthetic_delta_source: &'static str,
}

#[derive(Clone, Debug)]
pub(super) struct QueuedCompletedEvent {
    payload: Value,
    diagnostics: CompletedSanitizationDiagnostics,
}

#[derive(Clone, Debug)]
pub(super) struct QueuedForwardedEvent {
    payload: Value,
}

pub(super) struct ResponseStreamState {
    pub(super) services: ThreadlineServices,
    pub(super) upstream: Option<Arc<LiveUpstreamWebSocket>>,
    pub(super) lease: ResponseStreamLease,
    pub(super) base_request: serde_json::Map<String, Value>,
    pub(super) pending_internal_outputs: Vec<PendingInternalToolOutput>,
    pub(super) followup_send_started: bool,
    pub(super) previous_response_id: Option<String>,
    pub(super) execute_internal_tools: bool,
    pub(super) suppressed_internal_output_indexes: HashSet<u64>,
    pub(super) upstream_event_seen: bool,
    pub(super) headers_committed: bool,
    pub(super) replay_prohibited: bool,
    pub(super) recovery_local_tools_only: bool,
    pub(super) pending_upstream_events: VecDeque<String>,
    pub(super) replay_stale_marker_on_pre_first_event_close: bool,
    pub(super) observable_output: DownstreamObservableOutputState,
    pub(super) downstream_visible_text_sources: HashSet<VisibleTextDedupeIdentity>,
    pub(super) downstream_visible_text_delta_count: usize,
    pub(super) visible_assistant_text: Vec<VisibleAssistantText>,
    pub(super) last_unidentified_visible_text: Option<String>,
    pub(super) queued_synthetic_output_text_deltas: VecDeque<QueuedSyntheticOutputTextDelta>,
    pub(super) queued_forwarded_event: Option<QueuedForwardedEvent>,
    pub(super) queued_final_completed: Option<QueuedCompletedEvent>,
    pub(super) final_done_pending: bool,
    pub(super) apply_no_observable_output_failure: bool,
    pub(super) done: bool,
}

pub(super) use recovery::queue_transport_error;

impl ResponseStreamState {
    fn observe_consumer_phase(&self, phase: ConsumerPhase) {
        if let Some(upstream) = &self.upstream {
            upstream.observe_consumer_phase(phase);
        }
    }
}

pub(super) fn response_stream(
    state: ResponseStreamState,
) -> impl futures_util::Stream<Item = Result<Bytes, Infallible>> {
    state.observe_consumer_phase(ConsumerPhase::AwaitingDownstreamPoll);
    stream::unfold(
        (state, InternalToolLedger::default()),
        |(mut state, mut ledger)| {
            let upstream = state.upstream.as_ref().map(Arc::downgrade);
            let progress = async move {
                state.observe_consumer_phase(ConsumerPhase::ProcessingEvent);
                loop {
                    match next_stream_progress(&mut state, &mut ledger).await {
                        StreamProgress::Continue => continue,
                        StreamProgress::Yield(chunk) => {
                            state.observe_consumer_phase(ConsumerPhase::AwaitingDownstreamPoll);
                            return Some((Ok(chunk), (state, ledger)));
                        }
                        StreamProgress::Finished => return None,
                    }
                }
            };
            async move {
                tokio::pin!(progress);
                futures_util::future::poll_fn(|context| {
                    if let Some(upstream) = upstream.as_ref().and_then(std::sync::Weak::upgrade) {
                        upstream.observe_consumer_poll();
                    }
                    std::future::Future::poll(progress.as_mut(), context)
                })
                .await
            }
        },
    )
}

enum StreamProgress {
    Continue,
    Yield(Bytes),
    Finished,
}

#[cfg(test)]
mod tests {
    use std::collections::{HashSet, VecDeque};
    use std::sync::Arc;

    use futures_util::{StreamExt, future::BoxFuture};
    use serde_json::json;

    use super::{
        CompletedSanitizationDiagnostics, DownstreamObservableOutputState, DownstreamTraceAction,
        DownstreamTraceDiagnostics, RESPONSES_TRANSLATION_DOWNSTREAM_SSE_EVENT,
        RESPONSES_TRANSLATION_EVENT_SUPPRESSED, RESPONSES_TRANSLATION_NO_OBSERVABLE_OUTPUT_GUARD,
        RESPONSES_TRANSLATION_UPSTREAM_EVENT, ResponseStreamLease, ResponseStreamState,
        UpstreamEventTraceMetadata, VisibleAssistantText, VisibleTextSourceKey,
        downstream_sse_trace_metadata, final_completion_acceptance_error, response_stream,
        sanitized_completed_event_with_diagnostics,
    };
    use crate::auth::LoadedUpstreamAuth;
    use crate::codex_ws::UpstreamSessionDescriptor;
    use crate::errors::ThreadlineError;
    use crate::registry::{RegistryAcquireError, RetainedSessionRegistry};
    use crate::responses::{
        ConnectedUpstream, ThreadlineServices, UpstreamAuthProvider, UpstreamConnector,
    };
    use crate::ws_pump::{
        InboundBufferOverflow, InboundBufferOverflowCause, LiveUpstreamWebSocket,
        UpstreamCloseMetadata, UpstreamTerminalState,
    };

    struct UnusedAuthProvider;

    impl UpstreamAuthProvider for UnusedAuthProvider {
        fn load(&self) -> Result<LoadedUpstreamAuth, ThreadlineError> {
            Err(ThreadlineError::UpstreamWebSocketConnectFailed)
        }
    }

    struct UnusedConnector;

    impl UpstreamConnector for UnusedConnector {
        fn connect(
            &self,
            _auth: LoadedUpstreamAuth,
            _session: Option<UpstreamSessionDescriptor>,
        ) -> BoxFuture<'static, Result<ConnectedUpstream, ThreadlineError>> {
            Box::pin(async { Err(ThreadlineError::UpstreamWebSocketConnectFailed) })
        }
    }

    mod diagnostics;
    mod lifecycle;
    mod response_state;
    mod sanitization;
}
