# AGENTS.md

## Project

Threadline is a full-Rust bridge between VSCode Copilot BYOK Custom Endpoint requests and the Codex backend WebSocket protocol.

Threadline is informed by lessons from ChatMock experiments, but it must not copy, port, translate, or preserve ChatMock source code, internal names, comments, tests, or implementation structure.

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

Do not add unrelated providers, historical ChatMock behavior, prompt-file injection, or Python ChatMock behavior unless explicitly requested.

Do not implement `/v1/chat/completions` unless it is needed for VSCode BYOK compatibility. `/v1/responses` is the primary API.

## Rule priority

Root `AGENTS.md` contains always-on rules.

Detailed examples and expanded guidance live in `docs/agent/`.

If a root rule conflicts with a detail document, follow the root rule and update the detail document later.

## Read-on-demand docs

Read `docs/agent/protocol.md` before changing WebSocket, retained session, registry, internal tool, job, or error behavior.

Read `docs/agent/architecture.md` before changing module boundaries or doing large refactors.

Read `docs/agent/conventions.md` before editing names, comments, test names, tracing, logs, commits, PR text, or public wording.

Read `docs/agent/workflow.md` before final validation, CI, CodeQL, local-only notes, or development summaries.

## Source independence

Do not copy, port, or translate ChatMock code.

Do not preserve ChatMock-specific module names, function names, comments, test names, or internal terminology unless the term describes a public protocol concept.

Allowed: reusing design lessons, reimplementing behavior from scratch, and using public protocol names such as `response.create`, `previous_response_id`, `session-id`, and `thread-id`.

Use `threadline_*` for Threadline-owned internal tools and Threadline-specific concepts.

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

New code should fit an existing responsibility or introduce a clearly named module with one durable purpose.

## Naming and terminology

Use normal Rust naming conventions: `snake_case` for modules/functions/variables, `PascalCase` for types/enum variants, and `SCREAMING_SNAKE_CASE` for constants.

Prefer precise names over short or phase-based names.

Avoid names such as `Thing`, `Manager2`, `handle_stuff`, `phase1_handler`, `test_new_flow`, or `chatmock_*`.

Use these terms consistently:

* `upstream`: Codex backend WebSocket side.
* `downstream`: VSCode BYOK HTTP/SSE client side.
* `response marker`: a `previous_response_id` or completed response id used for continuation.
* `retained session`: a stored upstream WebSocket plus session metadata.
* `internal tool`: a Threadline-handled tool hidden from downstream clients.
* `job`: a long-running local or subprocess task managed by Threadline.
* `pump`: the task that continuously reads/writes an upstream WebSocket and handles Ping/Pong.

## Comments and tests

Comments should explain durable design intent, protocol quirks, or safety constraints.

Do not write comments that only describe temporary implementation phases, local experiments, orchestration history, or model conversations.

Do not include phase labels, local machine details, personal paths, transcript details, or “Codex told me to...” style text in comments, tests, public docs, commit messages, or PR text.

If historical context is useful, rewrite it as a general protocol or design reason.

Test names must describe behavior, not implementation phase or local context.

## Tracing, logs, and errors

Use structured tracing fields and stable, grep-friendly log event names.

Never log secrets, raw tokens, refresh tokens, cookies, or full authorization headers.

Raw upstream `error` events may be logged at debug/trace level only after confirming they do not contain secrets.

Prefer typed errors internally.

Public HTTP/SSE errors should be stable and VSCode compatible.

Use clear error codes for expected states.

Do not panic for protocol errors, malformed client input, missing markers, closed sockets, or upstream errors.

## WebSocket rules

All live upstream WebSockets must be pump-based.

Do not directly hold and use `WebSocketStream` from route handlers.

The pump must run while the session is retained, read idle frames, reply to Ping with Pong, forward Text/Binary frames into an inbound queue, accept outbound commands through a channel, and mark close/error metadata.

A retained WebSocket that is idle must still be alive enough to answer Ping/Pong.

## Registry rules

The retained session registry maps completed response markers to upstream session state.

A registry entry should preserve the response marker, upstream handle, session id, thread id, window generation, turn state, in-use flag, close/recoverable state, and last-used timestamp.

If a retained socket is closed after a completed response, preserve recoverable metadata when possible.

Do not delete a response marker merely because a socket close was observed after a successful `response.completed`.

## Internal tool rules

Threadline internal tools must use the `threadline_*` prefix.

Internal tool calls must never be forwarded downstream to VSCode.

When an upstream response emits a Threadline internal tool call, execute it locally, store the output as pending, wait for the intermediate response to complete, send a follow-up `response.create` with `function_call_output`, continue reading the follow-up response, and forward only the final assistant output downstream.

Do not send follow-up tool outputs before the intermediate response completes.

Do not treat an intermediate response completion as the final completion.

## Job rules

Long-running work should be represented as jobs.

A job should start quickly and return a `job_id`.

Use polling or result retrieval for later status.

Internal job tools should use the `threadline_*` prefix.

Job completion must update stored job state only and must not automatically push a new upstream response.

## Security and local-only notes

Never commit local credentials, cookies, refresh tokens, access tokens, account identifiers, or production credentials.

Use `.gitignore` for local state such as `.threadline/`, `*.local.json`, `*.local.toml`, and `*.log`.

Local orchestration notes must not be committed unless generalized into durable documentation.

Do not copy local-only context into source comments, test names, public docs, commit messages, or test fixtures.

## Commit and PR guidance

Keep commits focused.

Commit messages should describe behavior, not orchestration phase.

Avoid temporary branch names, phase labels, local debugging notes, model-conversation artifacts, or ChatMock porting language in commits and PRs.

## Validation

Before considering a change complete, run the relevant checks:

* `cargo fmt`
* `cargo clippy --all-targets --all-features`
* `cargo test`

If a check cannot be run, record why in the final development summary, not in source comments.

## CI and security scanning

Keep GitHub Actions workflow names stable and descriptive.

Use CodeQL for Rust security scanning, preferably with manual build mode so analysis sees the same crate graph that `cargo build` uses.

Do not add temporary branch names, local phase labels, or orchestration notes to workflow names, job names, or step names.

## Development summary format

When reporting changes, use this shape:

Changed:

* ...

Validation:

* ...

Risks:

* ...

Do not include private local paths, temporary phase labels, or transcript-only context in code, tests, or summaries.
