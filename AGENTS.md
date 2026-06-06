# AGENTS.md

## Project

Threadline is a full-Rust bridge between VSCode Copilot BYOK Custom Endpoint requests and the Codex backend WebSocket protocol.

Threadline is inspired by lessons learned from ChatMock experiments, but it must not copy, port, or reuse ChatMock source code. Implementations, module boundaries, names, tests, and comments must be original to this repository.

## Primary goals

* Provide a stable `/v1/responses` endpoint for VSCode BYOK.
* Bridge HTTP/SSE requests to Codex backend WebSocket sessions.
* Preserve `previous_response_id` continuity through retained WebSocket sessions.
* Keep retained upstream WebSockets alive with a pump-based Ping/Pong design.
* Support nested subagent execution without idle WebSocket timeout.
* Support long-running work through background jobs rather than long blocking tool calls.
* Keep the implementation focused and maintainable.

## Non-goals

Do not turn Threadline into a general-purpose OpenAI-compatible proxy.

Avoid adding compatibility for unrelated providers, historical ChatMock behavior, prompt-file injection, or Python ChatMock behavior unless explicitly requested.

Do not implement `/v1/chat/completions` unless it is needed for VSCode BYOK compatibility. `/v1/responses` is the primary API.

## Source independence rule

Do not copy code from ChatMock.

Do not preserve ChatMock-specific module names, function names, comments, test names, or internal terminology unless the term describes a public protocol concept.

Allowed:

* Reusing design lessons learned from prior experiments.
* Reimplementing behavior from scratch.
* Using public protocol names such as `response.create`, `previous_response_id`, `session-id`, and `thread-id`.

Not allowed:

* Copying ChatMock functions or structs.
* Translating ChatMock code line-by-line.
* Keeping ChatMock-only names such as `chatmock_*` for Threadline features.

Use `threadline_*` for internal tools and Threadline-specific concepts.

## Architecture principles

Prefer small modules with one responsibility.

Suggested module boundaries:

* `http`: axum routes and request/response wrappers.
* `responses`: `/v1/responses` request normalization and SSE translation.
* `codex_ws`: Codex WebSocket connector and protocol messages.
* `ws_pump`: WebSocket pump, Ping/Pong, inbound/outbound channels.
* `registry`: retained response/session registry.
* `jobs`: background job manager.
* `tools`: internal Threadline tools.
* `auth`: ChatGPT/Codex authentication loading and refresh.
* `config`: CLI flags and environment configuration.
* `errors`: public error payloads and internal error types.

Keep protocol types separate from transport code.

## Naming conventions

Use Rust naming conventions:

* Modules: `snake_case`
* Functions: `snake_case`
* Variables: `snake_case`
* Types: `PascalCase`
* Enum variants: `PascalCase`
* Constants: `SCREAMING_SNAKE_CASE`

Prefer precise names over short names.

Good:

* `RetainedSessionRegistry`
* `UpstreamWebSocketPump`
* `ResponseMarker`
* `ThreadlineJobManager`
* `PendingInternalToolOutput`
* `send_followup_tool_outputs`

Avoid:

* `Thing`
* `Manager2`
* `handle_stuff`
* `phase1_handler`
* `test_new_flow`
* `chatmock_*`

## Public terminology

Use these terms consistently:

* `upstream`: Codex backend WebSocket side.
* `downstream`: VSCode BYOK HTTP/SSE client side.
* `response marker`: a `previous_response_id` / completed response id used for continuation.
* `retained session`: a stored upstream WebSocket plus session metadata.
* `internal tool`: a Threadline-handled tool hidden from downstream clients.
* `job`: a long-running local or subprocess task managed by Threadline.
* `pump`: the task that continuously reads/writes an upstream WebSocket and handles Ping/Pong.

## Comments

Comments should explain durable design intent, protocol quirks, or safety constraints.

Do not write comments that only describe temporary implementation phases, local experiments, or orchestration history.

Allowed:

```rust
// The pump must keep reading while the session is idle so server Ping frames receive Pong responses.
```

Not allowed:

```rust
// Phase 2 fix from the ChatMock experiment.
```

Not allowed:

```rust
// Local test workaround from today's debugging.
```

Do not include:

* Phase labels such as `Phase 1`, `Phase 2`, `rust-test`, or `temporary ChatMock fix`.
* Local machine details.
* Personal paths.
* Chat transcript details.
* Debugging history that will not matter to future maintainers.
* Model conversation artifacts.
* “Codex told me to...” style comments.

If historical context is useful, write it as a general protocol/design reason.

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

Avoid putting secrets, raw tokens, refresh tokens, cookies, or full authorization headers in logs.

Raw upstream `error` events may be logged at debug/trace level only after confirming they do not contain secrets.

Log event names should be stable and grep-friendly:

* `ws_pump_started`
* `ws_pump_ping_received`
* `ws_pump_pong_sent`
* `upstream_event_received`
* `internal_tool_detected`
* `internal_tool_followup_sent`
* `final_response_completed`
* `retained_session_acquired`
* `retained_session_released`
* `reconnect_continuation_attempt`
* `reconnect_continuation_failed`
* `upstream_error_event`

## Error handling

Prefer typed errors internally.

Public HTTP/SSE errors should be stable and VSCode compatible.

Use clear error codes for expected states:

* `previous_response_not_found`
* `retained_session_conflict`
* `retained_session_capacity_exceeded`
* `upstream_websocket_connect_failed`
* `upstream_websocket_closed`
* `internal_tool_failed`
* `job_not_found`

Do not panic for protocol errors, malformed client input, missing markers, closed sockets, or upstream errors.

## WebSocket rules

All live upstream WebSockets must be pump-based.

Do not directly hold and use `WebSocketStream` from route handlers.

The pump must:

* Run while the session is retained.
* Read frames even when no HTTP request is currently waiting.
* Reply to server Ping frames with Pong.
* Forward Text/Binary frames into an inbound queue.
* Accept outbound Text/Ping/Close commands through a channel.
* Mark close/error metadata when the socket closes.

A retained WebSocket that is idle must still be alive enough to answer Ping/Pong.

## Registry rules

The retained session registry maps completed response markers to upstream session state.

A registry entry should store:

* response marker
* upstream WebSocket handle
* session id
* thread id
* window generation
* turn state
* in-use flag
* close/recoverable state
* last-used timestamp

If a retained socket is closed after a completed response, preserve recoverable metadata when possible.

Do not delete a response marker merely because a socket close was observed after a successful `response.completed`.

## Internal tool rules

Threadline internal tools must use the `threadline_*` prefix.

Internal tool calls must never be forwarded downstream to VSCode.

When an upstream response emits a Threadline internal tool call:

1. Execute the internal tool locally.
2. Store the output as pending.
3. Wait for the intermediate response to complete.
4. Send a follow-up `response.create` with `function_call_output`.
5. Continue reading the follow-up response.
6. Forward only the final assistant output downstream.

Do not send follow-up tool outputs before the intermediate response completes.

Do not treat an intermediate response completion as the final completion.

## Job rules

Long-running work should be represented as jobs.

A job should be started quickly and return a `job_id`.

Use polling or result retrieval for later status.

Internal job tools:

* `threadline_start_job`
* `threadline_poll_job`
* `threadline_read_job_output`
* `threadline_get_job_result`
* `threadline_cancel_job`

Job completion must not automatically push a new upstream response.

Job completion should update stored job state only.

## Security

Never log secrets.

Never commit local credentials, cookies, refresh tokens, access tokens, or account identifiers.

Use `.gitignore` for local state directories.

Recommended ignored paths:

```gitignore
.threadline/
*.local.json
*.local.toml
*.log
```

Do not store production credentials in test fixtures.

## Local-only notes

Local orchestration notes must not be committed unless they are generalized into durable documentation.

Use local-only files such as:

```txt
.threadline/notes.md
.threadline/debug-log.md
.threadline/orchestration.md
```

These files should be ignored by git.

Do not copy local-only context into source comments, test names, public docs, or commit messages.

## Commit and PR guidance

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

## Validation

Before considering a change complete, run the relevant checks:

```sh
cargo fmt
cargo clippy --all-targets --all-features
cargo test
```

If a check cannot be run, record why in the final development summary, not in source comments.

## CI and security scanning

Keep GitHub Actions workflow names stable and descriptive.

Use CodeQL for Rust security scanning. Prefer manual build mode so analysis sees the same crate graph that `cargo build` uses.

Do not add temporary branch names, local phase labels, or orchestration notes to workflow names, job names, or step names.

## Development summary format

When reporting changes, use:

```txt
Changed:
- ...

Validation:
- ...

Risks:
- ...
```

Do not include private local paths, temporary phase labels, or transcript-only context in code or tests.
