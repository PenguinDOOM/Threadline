# Agent Conventions

This document expands the naming, wording, comments, tests, tracing, logs, commits, and PR conventions for Threadline.

Root `AGENTS.md` contains the always-on rules. If this file conflicts with root `AGENTS.md`, follow root `AGENTS.md`.

## Scope

Use this file before editing:

* module names
* type names
* function names
* variable names
* comments
* test names
* tracing event names
* log fields
* error wording
* commit messages
* PR titles and descriptions
* public documentation wording

## Naming conventions

Use normal Rust naming conventions:

* Modules: `snake_case`
* Functions: `snake_case`
* Variables: `snake_case`
* Types: `PascalCase`
* Enum variants: `PascalCase`
* Constants: `SCREAMING_SNAKE_CASE`

Prefer precise names over short names.

Names should describe durable behavior, not the development phase that produced the code.

## Good names

Prefer names like:

```rust
RetainedSessionRegistry
UpstreamWebSocketPump
ResponseMarker
ThreadlineJobManager
PendingInternalToolOutput
send_followup_tool_outputs
```

These names describe stable responsibilities and protocol concepts.

## Bad names

Avoid names like:

```rust
Thing
Manager2
handle_stuff
phase1_handler
test_new_flow
chatmock_cache
chatmock_handler
```

These names are vague, phase-based, or tied to another project.

## Source independence in names

Threadline may reuse protocol concepts and design lessons, but it must not preserve ChatMock-specific internal names.

Do not use `chatmock_*` names for Threadline features.

Use `threadline_*` for Threadline-owned internal tools and Threadline-specific concepts.

Allowed public protocol names include:

* `response.create`
* `previous_response_id`
* `session-id`
* `thread-id`
* `function_call_output`

Do not rename public protocol concepts just to make them look original.

Do not preserve private ChatMock terminology just because a previous experiment used it.

## Public terminology

Use these terms consistently.

### upstream

The Codex backend WebSocket side.

Use `upstream` for code, logs, and comments that refer to the Codex backend connection.

### downstream

The VSCode BYOK HTTP/SSE client side.

Use `downstream` for code, logs, and comments that refer to the client-facing request or response stream.

### response marker

A completed response id or `previous_response_id` used for continuation.

Use this when discussing the key that allows a later request to resume or reconnect a retained session.

### retained session

A stored upstream WebSocket plus session metadata.

Use this for the registry-managed state that survives past a single downstream HTTP request.

### internal tool

A Threadline-handled tool call that is hidden from downstream clients.

Use this for `threadline_*` tools executed locally by Threadline.

### job

A long-running local or subprocess task managed by Threadline.

Use this when work should not block a single tool call or HTTP request.

### pump

The task that continuously reads and writes an upstream WebSocket and handles Ping/Pong.

Use this for the component responsible for keeping an upstream socket alive while retained.

## Comments

Comments should explain durable design intent, protocol quirks, or safety constraints.

Good comments explain why the code must behave a certain way.

```rust
// The pump must keep reading while the session is idle so server Ping frames receive Pong responses.
```

Bad comments describe temporary history, orchestration phases, or local debugging.

```rust
// Phase 2 fix from the ChatMock experiment.
```

```rust
// Local test workaround from today's debugging.
```

Do not include:

* phase labels such as `Phase 1`, `Phase 2`, `rust-test`, or `temporary ChatMock fix`
* local machine details
* personal paths
* chat transcript details
* debugging history that will not matter to future maintainers
* model conversation artifacts
* “Codex told me to...” style comments
* branch-specific notes
* temporary TODOs with no durable owner or reason

If historical context is useful, rewrite it as a general protocol or design reason.

Instead of:

```rust
// ChatMock needed this because the socket died during phase 3 testing.
```

Write:

```rust
// Retained sessions may observe idle upstream closes after completion, so keep recoverable metadata for continuation.
```

## TODO comments

Avoid TODO comments for vague future cleanup.

Allowed TODO comments must include a durable reason and a concrete condition for removal.

Good:

```rust
// TODO: Remove this compatibility branch once VSCode no longer sends empty tool output arrays.
```

Bad:

```rust
// TODO: clean this up later
```

Do not use TODO comments to store local planning notes.

## Test naming

Test names must describe behavior, not implementation phase or local context.

Good:

```rust
retained_session_reconnects_after_idle_close_before_first_event
websocket_pump_replies_to_server_ping_while_idle
internal_tool_outputs_are_sent_after_intermediate_response_completes
job_manager_returns_incremental_output_after_offset
```

Bad:

```rust
phase_3_reconnect_test
chatmock_regression_test
test_from_logs_0605
rust_test_branch_case
```

Prefer names shaped like:

```txt
<component>_<expected_behavior>_<important_condition>
```

Examples:

```rust
registry_preserves_marker_after_recoverable_idle_close
responses_waits_for_internal_tool_followup_before_final_completion
ws_pump_marks_close_metadata_when_upstream_closes
jobs_returns_not_found_for_unknown_job_id
```

## Test content

Tests should assert stable behavior.

Avoid assertions that depend on:

* local paths
* local timestamps
* temporary branch names
* exact debug strings unless the string is a public contract
* model conversation text
* ChatMock-specific implementation details

Use fixtures that describe Threadline behavior, not the history of how the behavior was discovered.

## Tracing and logs

Use structured tracing fields.

Good:

```rust
tracing::debug!(
    response_id = %response_id,
    session_id = %session_id,
    "retained_session_acquired"
);
```

Avoid string-only logs when useful structured fields are available.

Good fields include:

```rust
response_id
session_id
thread_id
job_id
tool_name
marker
generation
close_code
recoverable
```

Do not log secrets.

Never log:

* raw access tokens
* refresh tokens
* cookies
* full authorization headers
* account identifiers
* local credential file contents
* production credentials
* raw request bodies that may contain credentials

Raw upstream `error` events may be logged at debug or trace level only after confirming they do not contain secrets.

## Stable log event names

Log event names should be stable and grep-friendly.

Prefer:

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

New event names should be:

* lowercase
* snake_case
* behavior-oriented
* stable across refactors
* free of branch names, dates, and phase labels

Avoid:

```txt
phase2_fix_started
debug_0605_case
chatmock_retry_path
temporary_ws_patch
```

## Error wording

Public error wording should be stable, clear, and VSCode-compatible.

Prefer typed internal errors and stable public error codes.

Use clear error codes for expected states, such as:

```txt
previous_response_not_found
retained_session_conflict
retained_session_capacity_exceeded
upstream_websocket_connect_failed
upstream_websocket_closed
internal_tool_failed
job_not_found
```

Do not expose local paths, tokens, cookies, or private account information in errors.

Do not panic for protocol errors, malformed client input, missing markers, closed sockets, or upstream errors.

## Commit messages

Keep commits focused.

Commit messages should describe behavior, not orchestration phase.

Good:

```txt
Add pump-based upstream websocket transport
Preserve recoverable retained sessions after idle close
Add internal job tools for long-running tasks
```

Bad:

```txt
Phase 2 fixes
Apply Codex suggestions
Fix bug from local log
Port ChatMock behavior
```

Prefer imperative subject lines.

Good:

```txt
Preserve response markers after recoverable socket close
```

Avoid vague subjects.

Bad:

```txt
Update stuff
```

## PR titles and descriptions

PR titles should describe the user-visible or maintainer-visible behavior.

Good:

```txt
Add retained WebSocket pump for BYOK response continuity
```

Bad:

```txt
First write phase 3
```

PR descriptions may mention design motivation, validation, and risks.

Do not include:

* transcript-only context
* private local paths
* local debugging logs
* phase labels
* “Codex generated this” wording
* ChatMock porting language

## Public docs wording

Public docs should describe Threadline directly.

Do not describe Threadline as a ChatMock port.

Allowed:

```txt
Threadline bridges VSCode BYOK `/v1/responses` requests to retained Codex backend WebSocket sessions.
```

Avoid:

```txt
Threadline is a Rust port of ChatMock.
```

It is acceptable to say Threadline is inspired by lessons from prior experiments when relevant, but do not imply code lineage.

## Review checklist

Before finalizing naming, comments, tests, logs, commits, or PR text, check:

* Does this describe durable behavior rather than a temporary development phase?
* Does this avoid ChatMock-specific private names and implementation structure?
* Does this avoid local paths, dates, branches, and transcript-only context?
* Are public protocol terms preserved where they are actually public protocol terms?
* Are logs structured and free of secrets?
* Are test names behavior-oriented?
* Are commit and PR messages focused on behavior?
