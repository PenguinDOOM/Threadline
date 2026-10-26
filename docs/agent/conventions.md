# Agent Conventions

This document expands naming, wording, comments, tests, tracing, logs, commits, and PR conventions for Threadline.

Root `AGENTS.md` contains the always-on rules. If this file conflicts with root `AGENTS.md`, follow root `AGENTS.md`.

## Scope

Use this file before editing names, comments, TODO comments, test names or fixtures, tracing event names, log fields, error wording, commit messages, PR text, or public documentation wording.

## Naming conventions

Use normal Rust naming conventions:

| Item | Convention |
| --- | --- |
| Modules, functions, variables | `snake_case` |
| Types and enum variants | `PascalCase` |
| Constants | `SCREAMING_SNAKE_CASE` |

Prefer precise names that describe durable behavior and protocol concepts.

Good examples:

```rust
RetainedSessionRegistry
UpstreamWebSocketPump
ResponseMarker
ThreadlineJobManager
PendingInternalToolOutput
send_followup_tool_outputs
```

Avoid vague, phase-based, or project-derived names such as `Thing`, `Manager2`, `handle_stuff`, `phase_handler`, `test_new_flow`, or `legacy_cache`.

Threadline may reuse public protocol concepts and design lessons, but it must not preserve ChatMock-specific internal names. Use `threadline_*` for Threadline-owned internal tools and Threadline-specific concepts.

Allowed public protocol names include `response.create`, `previous_response_id`, `session-id`, `thread-id`, and `function_call_output`. Do not rename public protocol concepts just to make them look original.

## Public terminology

Use these terms consistently:

| Term | Meaning |
| --- | --- |
| `upstream` | The Codex backend WebSocket side. |
| `downstream` | The VSCode BYOK HTTP/SSE client side. |
| `response marker` | A completed response id or `previous_response_id` used for continuation. |
| `retained session` | A stored upstream WebSocket plus session metadata managed by the registry. |
| `internal tool` | A `threadline_*` tool call executed locally and hidden from downstream clients. |
| `job` | A long-running local or subprocess task managed by Threadline. |
| `pump` | The task that continuously reads and writes an upstream WebSocket and handles Ping/Pong. |

## Comments

Comments should explain durable design intent, protocol quirks, or safety constraints.

Good comments explain why behavior is required:

```rust
// The pump must keep reading while idle so server Ping frames receive Pong responses.
```

Bad comments describe temporary history, orchestration, or local debugging.

Do not include phase labels, local machine details, personal paths, chat transcript details, short-lived debugging history, model conversation artifacts, branch-specific notes, or vague TODO comments.

If historical context is useful, rewrite it as a general protocol or design reason.

Use TODO comments only when they include a durable reason and a concrete removal condition:

```rust
// TODO: Remove this compatibility branch once VSCode no longer sends empty tool output arrays.
```

Do not use TODO comments to store local planning notes.

## Tests

Test names must describe behavior, not implementation phase or local context.

Prefer:

```txt
<component>_<expected_behavior>_<important_condition>
```

Examples:

```rust
retained_session_reconnects_after_idle_close_before_first_event
websocket_pump_replies_to_server_ping_while_idle
internal_tool_outputs_are_sent_after_intermediate_response_completes
registry_preserves_marker_after_recoverable_idle_close
```

Avoid names tied to dates, branches, previous projects, local logs, or development phases.

Tests should assert stable Threadline behavior. Avoid assertions that depend on local paths, local timestamps, temporary branch names, exact debug strings unless public, model conversation text, or ChatMock-specific implementation details.

## Tracing and logs

Use structured tracing fields when useful:

```rust
tracing::debug!(
    response_id = %response_id,
    session_id = %session_id,
    "retained_session_acquired"
);
```

Useful fields include `response_id`, `previous_response_id`, `session_id`, `thread_id`, `job_id`, `tool_name`, `marker`, `generation`, `close_code`, and `recoverable`.

Never log secrets, including access tokens, refresh tokens, cookies, full authorization headers, account identifiers, credential file contents, production credentials, or raw request bodies that may contain credentials.

Raw upstream `error` events may be logged at debug or trace level only after confirming they do not contain secrets.

## Stable log event names

Log event names should be stable and grep-friendly.

Prefer event names such as:

```txt
ws_pump_started
ws_pump_ping_received
ws_pump_pong_sent
upstream_event_received
internal_tool_detected
internal_tool_followup_sent
final_response_completed
retained_session_acquired
retained_session_released
reconnect_continuation_attempt
reconnect_continuation_failed
upstream_error_event
```

New event names should be lowercase, `snake_case`, behavior-oriented, stable across refactors, and free of branch names, dates, and phase labels.

## Error wording

Public error wording should be stable, clear, and VSCode-compatible.

Prefer typed internal errors and stable public error codes.

Expected public error codes include:

```txt
previous_response_not_found
retained_session_conflict
retained_session_capacity_exceeded
upstream_websocket_connect_failed
upstream_websocket_closed
internal_tool_failed
job_not_found
```

Do not expose local paths, tokens, cookies, private account information, or unstable debug strings in public errors.

Do not panic for protocol errors, malformed client input, missing markers, closed sockets, or upstream errors.

## Commits and PRs

Keep commits focused and behavior-oriented.

Good commit subjects:

```txt
Add pump-based upstream websocket transport
Preserve recoverable retained sessions after idle close
Add internal job tools for long-running tasks
```

Avoid vague, phase-based, transcript-derived, or ChatMock-porting language.

Prefer imperative subject lines. PR titles should describe user-visible or maintainer-visible behavior.

PR descriptions may mention design motivation, validation, and risks, but must not include transcript-only context, private local paths, local debugging logs, phase labels, model-generation wording, or ChatMock porting language.

## Public docs wording

Public docs should describe Threadline directly.

Allowed:

```txt
Threadline bridges VSCode BYOK `/v1/responses` requests to retained Codex backend WebSocket sessions.
```

Avoid describing Threadline as a ChatMock port. It is acceptable to say Threadline is inspired by lessons from prior experiments when relevant, but do not imply code lineage.

## Review checklist

Before finalizing naming, comments, tests, logs, commits, or PR text, check:

* The wording describes durable behavior.
* ChatMock-specific private names and implementation structure are absent.
* Local paths, dates, branches, and transcript-only context are absent.
* Public protocol terms are preserved where they are actually public terms.
* Logs are structured and free of secrets.
* Test names are behavior-oriented.
* Commit and PR messages are focused on behavior.
