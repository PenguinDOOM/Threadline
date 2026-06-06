# Agent Protocol Rules

This document expands Threadline protocol rules for `/v1/responses`, upstream Codex WebSocket sessions, retained sessions, internal tools, jobs, and protocol-facing errors.

Root `AGENTS.md` contains the always-on rules. If this file conflicts with root `AGENTS.md`, follow root `AGENTS.md`.

## Scope

Use this file before changing:

* `/v1/responses` request handling
* SSE response translation
* Codex backend WebSocket connection behavior
* WebSocket pump behavior
* Ping/Pong handling
* retained session registry behavior
* `previous_response_id` continuation
* internal `threadline_*` tool execution
* job tools
* public protocol errors
* reconnect or recovery logic

## Protocol boundaries

`/v1/responses` is the primary API.

Do not turn Threadline into a general-purpose OpenAI-compatible proxy.

Do not add `/v1/chat/completions` unless it is required for VSCode BYOK compatibility.

Do not add unrelated provider compatibility unless explicitly requested.

Threadline should bridge VSCode BYOK requests to Codex backend WebSocket sessions while keeping the implementation focused.

## Direction terms

Use these direction terms consistently:

* `downstream`: the VSCode BYOK HTTP/SSE client side
* `upstream`: the Codex backend WebSocket side

A downstream request may create, continue, or observe an upstream Codex WebSocket session.

Downstream clients must not see Threadline-only internal tool calls.

## Core invariants

These invariants should remain true across refactors:

* Live upstream WebSockets are owned by a pump, not by route handlers.
* Retained sessions keep enough state to continue from a completed response marker.
* Idle retained WebSockets must continue reading so Ping frames receive Pong responses.
* A completed response marker must not be deleted merely because an idle socket later closes.
* Internal `threadline_*` tool calls are executed locally and hidden from downstream clients.
* Intermediate completions for internal tool calls are not final downstream completions.
* Long-running work is represented as jobs, not long blocking tool calls.
* Job completion updates stored state only and does not push a new upstream response by itself.
* Public errors are stable and safe to expose.
* Secrets are never logged.

## `/v1/responses` handling

Normalize downstream `/v1/responses` requests before sending protocol messages upstream.

Keep request normalization separate from transport code.

Keep SSE translation separate from upstream WebSocket frame handling.

When a downstream request includes `previous_response_id`, use it as a continuation marker.

A response marker may refer to a retained session that is still open, closed but recoverable, or missing.

Handle each state explicitly.

Do not assume that a missing or closed socket means the response marker should be forgotten.

## WebSocket pump ownership

All live upstream WebSockets must be pump-based.

Route handlers must not directly hold and use `WebSocketStream`.

The pump owns continuous socket IO.

The rest of the code should communicate with the pump through channels or clearly defined handles.

The pump must support:

* reading upstream frames
* writing outbound upstream messages
* replying to server Ping frames with Pong
* forwarding Text and Binary frames into an inbound queue
* accepting outbound Text, Ping, and Close commands
* recording close and error metadata
* running while a session is retained, even when no downstream request is active

## Idle sessions

A retained session may be idle from the downstream perspective while still needing active upstream IO.

The pump must keep reading while idle.

Do not pause the read loop just because no HTTP request is currently waiting.

Do not rely on a future downstream request to read pending Ping frames.

If the upstream sends Ping while the retained session is idle, the pump must reply with Pong.

## Pump close behavior

When the upstream WebSocket closes, record close metadata.

Close metadata should distinguish at least:

* normal close
* protocol error
* transport error
* recoverable idle close
* unrecoverable close, if known

Do not discard the response marker merely because the socket closed after a successful `response.completed`.

If continuation is possible through stored metadata, preserve that metadata.

If continuation is not possible, keep enough information to produce a clear public error.

## Registry purpose

The retained session registry maps completed response markers to upstream session state.

A response marker is the lookup key for later continuation.

The registry should store enough state to continue, reject, or recover a request deterministically.

## Registry entry contents

A registry entry should store:

* response marker
* upstream WebSocket pump handle
* session id
* thread id
* window generation
* turn state
* in-use flag
* close state
* recoverable state
* last-used timestamp

Store only what is needed for correct continuation, diagnostics, and safe cleanup.

Do not store secrets in registry entries.

## Registry lifecycle

Create or update registry entries when an upstream response reaches a completed state that can be continued.

Mark entries in use while a downstream request is actively continuing through them.

Release the in-use flag when the request finishes, fails, or is cancelled.

Update last-used timestamps when entries are used.

Evict entries only through explicit capacity, TTL, or cleanup policy.

Do not remove a marker as a side effect of observing a post-completion idle close.

## Registry conflicts

A retained session should not be used concurrently in incompatible ways.

If a marker is already in use and the new request cannot safely share it, return a stable conflict error.

Prefer explicit conflict handling over races.

A conflict should not corrupt the registry entry.

## Continuation and recovery

When continuing from `previous_response_id`, first resolve the marker in the registry.

If the session is open and usable, continue through the retained pump.

If the socket is closed but recoverable metadata exists, attempt recovery or reconnect according to the current protocol implementation.

If recovery fails, return a stable error and keep enough diagnostic information for logs.

If the marker is unknown, return `previous_response_not_found`.

Do not silently start an unrelated fresh session for a continuation marker.

## Internal tool boundary

Threadline internal tools must use the `threadline_*` prefix.

Internal tool calls must never be forwarded downstream to VSCode.

Internal tool names should be treated as Threadline implementation details unless explicitly documented as public.

Do not let downstream clients invoke arbitrary local tools.

## Internal tool lifecycle

When an upstream response emits a Threadline internal tool call:

1. Detect that the tool is internal.
2. Execute the tool locally.
3. Store the output as pending.
4. Keep reading the upstream response.
5. Wait for the intermediate response to complete.
6. Send a follow-up `response.create` with `function_call_output`.
7. Continue reading the follow-up response.
8. Forward only the final assistant output downstream.

Do not send follow-up tool outputs before the intermediate response completes.

Do not treat the intermediate response completion as the final downstream completion.

Do not expose internal tool call details downstream unless explicitly required for diagnostics and safe to expose.

## Pending internal tool output

Pending internal tool output should be associated with the response or turn that requested it.

Pending output must not be lost if the intermediate response completes normally.

Pending output must not be sent twice.

If local tool execution fails, convert the failure into the expected protocol-level tool output or a stable internal tool error.

## Internal tool failure

Internal tool failures should be handled without panics.

Prefer typed internal failures.

Return stable public errors when the failure affects the downstream request.

Log enough structured metadata to debug the failure without logging secrets.

Use `internal_tool_failed` for expected public error states involving internal tool execution failure.

## Job model

Long-running work should be represented as jobs.

A job should start quickly and return a `job_id`.

Use polling or result retrieval for later status.

Do not block a single tool call or HTTP request for work that should continue independently.

Jobs are local Threadline state unless explicitly connected to upstream protocol flow.

## Internal job tools

Internal job tools should use the `threadline_*` prefix.

Expected job tools include:

* `threadline_start_job`
* `threadline_poll_job`
* `threadline_read_job_output`
* `threadline_get_job_result`
* `threadline_cancel_job`

These tools are internal and must not be forwarded downstream as normal model-visible tool calls.

## Job lifecycle

A job should have explicit state.

Useful states include:

* queued
* running
* succeeded
* failed
* cancelled

A job should store enough metadata for polling, result retrieval, incremental output, cancellation, and cleanup.

A job must not require the original downstream HTTP request to stay open.

## Job completion

Job completion must not automatically push a new upstream response.

Job completion should update stored job state only.

A later internal tool call or downstream-triggered request may retrieve job status or output.

Do not invent a background upstream response just because a local job completed.

## Job output

Long job output should be retrievable incrementally.

Use offsets or cursors for large output.

Do not return unbounded logs in a single response.

Do not expose local paths, credentials, environment secrets, or private machine details through job output.

## Job cancellation

Cancellation should be best effort.

A cancelled job should move to a stable cancelled or failed state.

Cancellation should not corrupt stored output already produced.

Polling a cancelled job should return a stable state.

Unknown job ids should return `job_not_found`.

## Error handling

Prefer typed errors internally.

Public HTTP/SSE errors should be stable and VSCode compatible.

Use clear error codes for expected states.

Do not panic for:

* protocol errors
* malformed client input
* missing markers
* closed sockets
* upstream errors
* internal tool failures
* unknown job ids
* registry conflicts

Panic only for impossible internal invariants where continuing would be unsafe.

## Public error codes

Use stable error codes for expected states, such as:

```txt
previous_response_not_found
retained_session_conflict
retained_session_capacity_exceeded
upstream_websocket_connect_failed
upstream_websocket_closed
internal_tool_failed
job_not_found
```

Add new public error codes only when callers can act on them or logs need stable categorization.

Do not expose implementation-only error strings as public contracts.

## Public error safety

Public errors must not include:

* access tokens
* refresh tokens
* cookies
* authorization headers
* local credential paths
* full upstream request bodies
* account identifiers
* private local machine paths
* transcript-only debugging context

Prefer concise user-facing messages plus structured internal logs.

## Upstream error events

Raw upstream `error` events may contain sensitive or unstable information.

Log them only at debug or trace level after confirming they do not contain secrets.

If an upstream error must be forwarded downstream, normalize it into a stable public error shape.

Do not blindly forward raw upstream errors as public API responses.

## SSE translation

Downstream SSE should represent the final client-facing response stream.

Internal tool calls and intermediate completions should not appear as final assistant output.

If an upstream sequence contains an internal tool call followed by a follow-up response, downstream should observe the final assistant-facing result, not the internal orchestration.

Keep SSE event names and payloads stable for VSCode compatibility.

## Ordering rules

Preserve protocol ordering.

In particular:

* do not send internal tool output before the intermediate response completes
* do not mark downstream final completion on an intermediate completion
* do not release a retained session before all required upstream events are processed
* do not delete a marker before continuation or recovery decisions are complete
* do not push job completion upstream without an explicit request path

Ordering bugs are likely to create hard-to-debug continuation failures.

## Concurrency rules

Treat retained sessions as shared mutable protocol state.

Protect registry entries from concurrent incompatible use.

Avoid holding locks across network IO when possible.

Avoid route-handler ownership of long-lived transport state.

Prefer message passing for pump IO.

Ensure cleanup paths release in-use flags and do not orphan jobs or pumps.

## Logging expectations

Use structured tracing for protocol events.

Useful fields include:

* `response_id`
* `previous_response_id`
* `session_id`
* `thread_id`
* `job_id`
* `tool_name`
* `marker`
* `generation`
* `recoverable`
* `close_code`

Never log secrets.

Use stable event names as described in `docs/agent/conventions.md`.

## Protocol change checklist

Before finalizing a protocol change, check:

* Does every live upstream WebSocket remain pump-owned?
* Does the pump keep reading while sessions are idle?
* Are Ping frames answered with Pong?
* Are response markers preserved after successful completion?
* Are recoverable idle closes represented without deleting markers?
* Are internal tool calls hidden from downstream clients?
* Are internal tool outputs sent only after intermediate completion?
* Are intermediate completions kept separate from final downstream completions?
* Are long-running operations represented as jobs?
* Does job completion only update local job state?
* Are public errors stable and safe?
* Are malformed inputs and upstream errors handled without panics?
* Are logs structured and free of secrets?
