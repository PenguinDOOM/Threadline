# Agent Protocol Rules

This document expands Threadline protocol rules for `/v1/responses`, upstream Codex WebSocket sessions, retained sessions, internal tools, jobs, and protocol-facing errors.

Root `AGENTS.md` contains the always-on rules. If this file conflicts with root `AGENTS.md`, follow root `AGENTS.md`.

## Scope

Use this file before changing `/v1/responses` handling, SSE translation, Codex backend WebSocket connection behavior, WebSocket pump or Ping/Pong behavior, retained session registry behavior, `previous_response_id` continuation, internal `threadline_*` tool execution, job tools, public protocol errors, reconnect, or recovery logic.

## Protocol boundaries

`/v1/responses` is the primary API.

Threadline is a VSCode BYOK to Codex ABI translator for `/v1/responses`, bridging VSCode BYOK requests to Codex backend WebSocket sessions while keeping the implementation focused.

Do not turn Threadline into a general-purpose OpenAI-compatible proxy.

Do not add `/v1/chat/completions` unless it is required for VSCode BYOK compatibility.

Do not add unrelated provider compatibility unless explicitly requested.

Use `downstream` for the VSCode BYOK HTTP/SSE client side and `upstream` for the Codex backend WebSocket side.

Downstream clients must not see Threadline-only internal tool calls.

## Core invariants

These invariants should remain true across refactors:

* Live upstream WebSockets are owned by a pump, not by route handlers.
* Retained sessions keep enough state to continue from a completed response marker.
* Idle retained WebSockets keep reading so Ping frames receive Pong responses.
* A completed response marker is not deleted merely because an idle socket later closes.
* Internal `threadline_*` tool calls are executed locally and hidden from downstream clients.
* Intermediate completions for internal tool calls are not final downstream completions.
* Long-running work is represented as jobs, not long blocking tool calls.
* Job completion updates stored state only and does not push a new upstream response by itself.
* Classified auxiliary summary requests are a narrow exception to retained continuation markers and must stay outside retained session registry semantics.
* Public errors are stable and safe to expose.
* Secrets are never logged.

## `/v1/responses` handling

Normalize downstream `/v1/responses` requests before sending protocol messages upstream.

Keep request normalization separate from transport code.

Keep SSE translation separate from upstream WebSocket frame handling.

As a narrow compatibility normalization for the Threadline `/v1/responses` bridge, visible assistant text may be accumulated from `response.output_text.delta`, `response.output_text.done`, assistant `response.output_item.done` messages, and final `response.completed` output.

If Threadline has not already forwarded equivalent visible assistant text downstream, it may emit a synthetic downstream `response.output_text.delta` immediately before forwarding the terminal downstream event that carries the final visible text.

If earlier visible text was streamed but the final completed assistant message would otherwise be missing or incomplete, Threadline may backfill the final completed assistant message from the accumulated visible assistant text.

When forwarding the final downstream `response.completed`, Threadline may sanitize `response.completed.response.output` to remove Threadline-internal `threadline_*` function calls while preserving downstream-consumable output, including preserved compaction, context, or other marker-like items that remain part of the completed downstream output.

Preserved completed `type: "compaction"` items are part of the ordinary downstream protocol contract when Threadline forwards or retains them in `response.completed.response.output`.

Treat preserved compaction items as opaque state markers. Threadline may use stable structural facts such as item counts, `type`, `id`, item position, and field presence for routing or diagnostics, but it must not decrypt, summarize, normalize, or log `encrypted_content`.

For ordinary downstream requests, a successful downstream stream may end with `response.completed` only when Threadline has already produced downstream-observable output for that request.

For this ordinary-request success rule, downstream-observable output is limited to downstream-visible assistant text, forwarded external non-`threadline_*` tool calls, `image_generation_call.result`, and concrete downstream-consumable compaction, context, or state-marker output when Threadline explicitly forwards that output downstream or retains it in the completed downstream output.

The `threadline_no_observable_output` guard exists to prevent empty or effectively invisible ordinary-request successes. It does not reject a response merely because the only observable output is preserved compaction or another preserved downstream-consumable state marker.

Server-side `context_management` compaction remains distinct from client-side auxiliary summary behavior. In the current v1 bridge, Threadline strips downstream `context_management` before every upstream `response.create`. Do not treat preserved compaction, context, or marker-like output as summary-only behavior merely because downstream `context_management` fields were present.

Client-side auxiliary summary remains a separate VS Code behavior used for summary-only prompt shapes. It does not replace retained-session continuation markers, and the v1 bridge does not claim or prove server-side `context_management` passthrough support.

Internal `threadline_*` tool events, intermediate completions that only finish internal-tool work, and other marker-like payloads that Threadline neither forwards downstream nor retains in the completed downstream output do not themselves satisfy the downstream-observable-output requirement.

`image_generation_call.result` remains a valid successful non-text output and may satisfy the downstream-observable-output requirement even when no visible assistant text is present.

If an ordinary request reaches a terminal state without downstream-observable output, including terminal states with only internal `threadline_*` items, only hidden intermediate completions, only compaction-only items that Threadline neither forwards downstream nor retains in the completed downstream output, or other non-observable marker-like payloads, Threadline must end the stream as `response.failed` with a stable `threadline_no_observable_output` failure instead of an empty success.

Auxiliary summary and transient auxiliary behavior remain narrow exceptions to the ordinary no-observable-output failure rule.

When a downstream request includes `previous_response_id`, use it as a continuation marker.

Ordinary downstream requests that include `previous_response_id` keep retained continuation semantics and must not silently start unrelated fresh sessions.

`response.completed.id` is the continuation-safe marker for later `previous_response_id` requests.

If a later turn fails upstream, do not reinterpret that failed turn as a new continuation marker.

A response marker may refer to a retained session that is open, closed but recoverable, missing, or unrecoverable. Handle each state explicitly.

Do not assume that a missing or closed socket means the response marker should be forgotten.

A classified auxiliary summary request is a narrow exception to that rule. This request class is identified by summary-only prompt fingerprints carried in `input`, using the observed summary instruction item shape for auxiliary summarization, and it may also carry a downstream `previous_response_id` as client context.

Classify this request type by its summary-only auxiliary behavior, not by `context_management` fields alone.

When a request is classified as an auxiliary summary request, do not acquire a retained marker, do not forward its downstream `previous_response_id` upstream, do not consume retained registry capacity, and do not register the completed summary response id as a continuation marker.

After terminal completion, failure, or cancellation of an auxiliary summary request, clean up any transient auxiliary state associated with that request.

## WebSocket pump ownership

All live upstream WebSockets must be pump-based.

Route handlers must not directly hold and use `WebSocketStream`.

The pump owns continuous socket IO. Other code communicates with the pump through channels or clearly defined handles.

The pump must support reading upstream frames, writing outbound upstream messages, replying to server Ping frames with Pong, forwarding Text/Binary frames into an inbound queue, accepting outbound Text/Ping/Close commands, recording close/error metadata, and running while a session is retained.

## Idle sessions

A retained session may be idle from the downstream perspective while still needing active upstream IO.

The pump must keep reading while idle.

Do not pause the read loop just because no HTTP request is waiting.

Do not rely on a future downstream request to read pending Ping frames.

If the upstream sends Ping while the retained session is idle, the pump must reply with Pong.

## Pump close behavior

When the upstream WebSocket closes, record close metadata.

Close metadata should distinguish normal close, protocol error, transport error, recoverable idle close, and unrecoverable close if known.

Do not discard the response marker merely because the socket closed after a successful `response.completed`.

If continuation is possible through stored metadata, preserve that metadata.

If continuation is not possible, keep enough information to produce a clear public error.

## Registry purpose and contents

The retained session registry maps completed response markers to upstream session state.

A response marker is the lookup key for later continuation.

The registry should store enough state to continue, reject, or recover a request deterministically.

A registry entry should store the response marker, upstream WebSocket pump handle, session id, thread id, window generation, turn state, in-use flag, close state, recoverable state, and last-used timestamp.

Store only what is needed for correct continuation, diagnostics, and safe cleanup.

Do not store secrets in registry entries.

## Registry lifecycle and conflicts

Create or update registry entries when an upstream response reaches a completed state that can be continued.

Do not create retained registry entries for classified auxiliary summary requests or for their completed summary response ids.

Do not register upstream failed response ids as continuation markers.

A failed response id may still be emitted downstream for diagnostics when the upstream payload provides one.

Mark entries in use while a downstream request is actively continuing through them.

Release the in-use flag when the request finishes, fails, or is cancelled.

Update last-used timestamps when entries are used.

Evict entries only through explicit capacity, TTL, or cleanup policy.

Do not remove a marker as a side effect of observing a post-completion idle close.

If a marker is already in use and the new request cannot safely share it, return a stable conflict error.

A conflict should not corrupt the registry entry.

## Continuation and recovery

When continuing from `previous_response_id`, first resolve the marker in the registry.

If the session is open and usable, continue through the retained pump.

An open retained upstream is a best-effort continuation condition, not a guarantee that upstream still recognizes the marker.

For ordinary downstream requests that include `previous_response_id`, forward that marker upstream only when the same marker still has an open retained upstream being continued. If no open retained upstream exists for that marker, do not send a known-stale marker upstream just because the downstream request supplied it.

If a later upstream turn ends with a recoverable `response.failed`, preserve any earlier completed marker that still identifies the retained session.

If the socket is closed but recoverable metadata exists, attempt recovery or reconnect according to the current protocol implementation for recoverable metadata cases other than ordinary downstream `previous_response_id` continuation where that marker no longer has an open retained upstream.

If the first upstream send for that continued turn fails before any upstream event is observed, or if the retained upstream closes before the first upstream event arrives, surface the stable downstream `previous_response_not_found` replay signal instead of reconnecting and resending the same marker.

If recovery fails, return a stable error and keep enough diagnostic information for logs.

If the marker is unknown, return `previous_response_not_found`.

Do not silently start an unrelated fresh session for a continuation marker.

## Internal tool boundary

Threadline internal tools must use the `threadline_*` prefix.

Internal tool calls must never be forwarded downstream to VSCode.

Internal tool names should be treated as Threadline implementation details unless explicitly documented as public.

Do not let downstream clients invoke arbitrary local tools.

## Internal tool lifecycle

When an upstream response emits a Threadline internal tool call, preserve this order: detect the internal tool, execute it locally, store output as pending, keep reading upstream, wait for the intermediate response to complete, send a follow-up `response.create` with `function_call_output`, continue reading the follow-up response, and forward only the final assistant output downstream.

Do not send follow-up tool outputs before the intermediate response completes.

Do not treat the intermediate response completion as the final downstream completion.

Internal `threadline_*` tool events and intermediate completions that only finish internal-tool work are consumed inside Threadline, stay hidden downstream, and are not final downstream completions.

Visible-text normalization is final-only and applies only to the downstream-visible assistant result after internal-tool follow-up has completed.

Do not expose internal tool call details downstream unless explicitly required for diagnostics and safe to expose.

## Pending internal tool output and failure

Pending internal tool output should be associated with the response or turn that requested it.

Pending output must not be lost if the intermediate response completes normally.

Pending output must not be sent twice.

If local tool execution fails, convert the failure into the expected protocol-level tool output or a stable internal tool error.

Internal tool failures should be handled without panics.

Return stable public errors when the failure affects the downstream request.

Log enough structured metadata to debug the failure without logging secrets.

Use `internal_tool_failed` for expected public error states involving internal tool execution failure.

## Job model and tools

Long-running work should be represented as jobs.

A job should start quickly, return a `job_id`, and continue asynchronously in local Threadline state.

Successful `threadline_start_job` calls should return immediately. The current implementation returns a short `next_action_hint` alongside the initial `starting` status to reinforce that the job keeps running in the background.

Use polling or result retrieval for later status.

Poll or read output at natural checkpoints when status is actually needed. Avoid tight polling loops when other useful work can continue independently.

Do not block a single tool call or HTTP request for work that should continue independently.

Jobs are local Threadline state unless explicitly connected to upstream protocol flow.

Internal job tools should use the `threadline_*` prefix.

Expected job tools include `threadline_start_job`, `threadline_poll_job`, `threadline_read_job_output`, `threadline_get_job_result`, and `threadline_cancel_job`.

Use `threadline_get_job_result` after a terminal status is observed, or before making final claims that depend on success, failure, or cancellation.

These tools are internal and must not be forwarded downstream as normal model-visible tool calls.

## Job lifecycle

A job should have explicit state. In the current implementation and tests, the exposed status strings are `starting`, `running`, `completed`, `failed`, and `cancelled`.

A job should store enough metadata for polling, result retrieval, incremental output, cancellation, and cleanup.

A job must not require the original downstream HTTP request to stay open.

Cancellation should be best effort. A cancelled job should move to a stable cancelled or failed state and should not corrupt stored output.

Unknown job ids should return `job_not_found`.

## Job completion and output

Job completion must not automatically push a new upstream response.

Job completion should update stored job state only.

A later internal tool call or downstream-triggered request may retrieve job status or output.

Do not invent a background upstream response just because a local job completed.

Long job output should be retrievable incrementally through offsets or cursors.

`threadline_read_job_output` returns a finite buffered view of job output, including `items`, `next_offset`, and `truncated_before`.

Callers should pass the returned `next_offset` back on the next incremental read.

If `truncated_before` is greater than a caller's stored offset, older output has already been dropped from the finite buffer and the next read should resume from `truncated_before`.

Buffered output may be available before job completion, but final claims that depend on the terminal outcome should be confirmed with `threadline_get_job_result`.

Do not return unbounded logs in a single response.

Do not expose local paths, credentials, environment secrets, or private machine details through job output.

## Error handling

Prefer typed errors internally.

Public HTTP/SSE errors should be stable and VSCode compatible.

Use clear error codes for expected states.

Do not panic for protocol errors, malformed client input, missing markers, closed sockets, upstream errors, internal tool failures, unknown job ids, or registry conflicts.

Panic only for impossible internal invariants where continuing would be unsafe.

## Public error codes and safety

Use stable error codes for expected states, including `previous_response_not_found`, `retained_session_conflict`, `retained_session_capacity_exceeded`, `upstream_websocket_connect_failed`, `upstream_websocket_closed`, `internal_tool_failed`, and `job_not_found`.

Add new public error codes only when callers can act on them or logs need stable categorization.

Do not expose implementation-only error strings as public contracts.

Public errors must not include tokens, cookies, authorization headers, credential paths, full upstream request bodies, account identifiers, private local machine paths, or transcript-only debugging context.

Prefer concise user-facing messages plus structured internal logs.

## Terminal downstream events and SSE

Raw upstream `error` events may contain sensitive or unstable information.

Log them only at debug or trace level after confirming they do not contain secrets.

Upstream `response.failed` and `response.incomplete` are separate downstream terminal paths from raw upstream `error` events.

Downstream `[DONE]` is only an optional trailer after a terminal downstream event. `[DONE]` alone is never the success signal.

When Threadline receives an upstream `response.failed`, forward it downstream as terminal SSE `event: response.failed`.

When Threadline receives an upstream `response.incomplete`, forward it downstream as terminal SSE `event: response.incomplete`.

Terminal downstream `response.failed` and `response.incomplete` payloads should keep stable, safe Responses-style fields appropriate to the terminal status.

For `response.failed`, use top-level `type` set to `response.failed`, `response.status` set to `failed`, and `response.error.code` plus `response.error.message` populated from stable public error wording.

Include `response.id` when the upstream failure payload provides one.

For `response.incomplete`, preserve safe status-specific fields and do not expose unstable upstream-only internals.

After emitting a terminal downstream `response.failed` or `response.incomplete` event, Threadline may terminate the stream with downstream `[DONE]`.

Successful downstream streams should terminate with `response.completed` only when Threadline has already forwarded or preserved concrete downstream-observable output for that request, such as downstream-visible assistant text, forwarded external non-`threadline_*` tool calls, `image_generation_call.result`, or other downstream-consumable output that Threadline explicitly forwards downstream or retains in the completed downstream output.

A non-empty final `response.completed.response.output` is not by itself the success criterion, and external non-`threadline_*` tool-call-only responses remain valid successful completions when that forwarded tool output is the downstream-observable result.

Emitting a failed `response.id` downstream does not make that id continuation-safe. Only previously completed markers remain valid for later `previous_response_id` requests.

If a prior completed marker exists and the upstream `response.failed` is recoverable, preserve that earlier marker for later resume or retry.

If SSE has already started and upstream later reports `previous_response_not_found`, classify that terminal downstream outcome as `previous_response_not_found` while preserving the prior completed marker or releasing it recoverably. Do not terminal-remove the marker solely because that late not-found was observed.

If an upstream error must be forwarded downstream, normalize it into a stable public error shape.

Do not blindly forward raw upstream errors as public API responses.

Keep raw upstream `error` handling and malformed protocol handling separate from the `response.failed` terminal path unless the protocol implementation is intentionally changed.

Downstream SSE should represent the final client-facing response stream.

Internal tool calls, internal-tool intermediate completions, and assistantless intermediate terminal states should not appear as final assistant output.

If an upstream sequence contains an internal tool call followed by a follow-up response, downstream should observe the final assistant-facing result, not the internal orchestration.

Keep SSE event names and payloads stable for VSCode compatibility.

## Ordering rules

Preserve protocol ordering.

In particular, do not send internal tool output before the intermediate response completes, mark downstream final completion on an intermediate completion, release a retained session before all required upstream events are processed, delete a marker before continuation/recovery decisions are complete, or push job completion upstream without an explicit request path.

Ordering bugs are likely to create hard-to-debug continuation failures.

## Concurrency and logging

Treat retained sessions as shared mutable protocol state.

Protect registry entries from concurrent incompatible use.

Avoid holding locks across network IO when possible.

Avoid route-handler ownership of long-lived transport state.

Prefer message passing for pump IO.

Ensure cleanup paths release in-use flags and do not orphan jobs or pumps.

Use structured tracing for protocol events.

Useful fields include `response_id`, `previous_response_id`, `session_id`, `thread_id`, `job_id`, `tool_name`, `marker`, `generation`, `recoverable`, and `close_code`.

For compaction-sensitive diagnostics, keep logs limited to safe structured facts such as item counts, item types, item ids, booleans, and presence flags like whether downstream `context_management` was present, whether Threadline stripped it upstream, or whether preserved completed output contained a compaction item.

Do not log or echo `encrypted_content`, prompts, tool arguments, tokens, cookies, raw request bodies, or other opaque compaction payload fields.

Never log secrets.

Use stable event names as described in `docs/agent/conventions.md`.

## Manual compaction round-trip probe

Use this checklist when verifying VS Code and Codex round-trip behavior without dumping sensitive payloads:

1. Confirm the incoming downstream `/v1/responses` request includes `context_management` and record only safe facts such as the configured compaction `type`, presence of `compact_threshold`, and whether `previous_response_id` is present.
2. Confirm the upstream `response.create` payload omits `context_management` after Threadline normalization, and confirm only safe structured stripping facts were recorded without logging prompts, tokens, or raw bodies.
3. Confirm the downstream stream or terminal `response.completed` includes a preserved `type: "compaction"` item by checking only safe structure such as item count, item `type`, item `id`, and whether `encrypted_content` is present.
4. Confirm the ordinary-request terminal result matches visibility rules: a preserved compaction-only completion is a valid `response.completed`, while a terminal path with no forwarded or retained observable output must become `threadline_no_observable_output`.
5. Confirm VS Code records the returned compaction item without exposing its opaque payload, using only safe indicators such as a compaction-related event name, presence flag, item id, or count.
6. Confirm the next downstream request round-trips the prior compaction item as input by matching only safe structure such as `type: "compaction"`, item `id`, and presence flags rather than comparing raw encrypted payload bytes in logs.

If the downstream request included `context_management` but no preserved or forwarded compaction item ever returns downstream, do not treat auxiliary summary as covering a server-side compaction contract that the v1 bridge does not send upstream.

## Protocol change checklist

Before finalizing a protocol change, check that live upstream WebSockets remain pump-owned, idle pumps keep reading and answer Ping with Pong, response markers survive completion and recoverable idle closes, internal tool calls stay hidden downstream, tool outputs wait for intermediate completion, intermediate and final completions stay separate, long-running work uses jobs, job completion only updates local state, public errors are stable and safe, malformed inputs/upstream errors do not panic, and logs are structured and secret-free.
