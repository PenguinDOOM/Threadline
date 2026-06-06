# Agent Architecture

This document expands Threadline architecture guidance for module boundaries, ownership, dependency direction, and large refactors.

Root `AGENTS.md` contains the always-on rules. If this file conflicts with root `AGENTS.md`, follow root `AGENTS.md`.

## Scope

Use this file before adding or moving modules, changing module responsibilities, changing transport ownership, changing request normalization or protocol type boundaries, introducing shared state/background tasks/IO abstractions, or doing a large refactor.

For protocol sequencing, also read `docs/agent/protocol.md`.

For names, comments, tests, logs, commits, and PR wording, also read `docs/agent/conventions.md`.

## Architecture goals

Threadline should remain focused, maintainable, and specific to VSCode BYOK `/v1/responses` bridging.

The architecture should support:

* HTTP/SSE downstream handling
* Codex backend WebSocket upstream handling
* retained WebSocket sessions and `previous_response_id` continuity
* pump-based Ping/Pong handling while idle
* nested subagent execution without idle timeout
* long-running work through jobs
* safe cleanup and recovery paths

Do not expand Threadline into a general-purpose OpenAI-compatible proxy or unrelated provider framework.

Threadline may reuse lessons from previous experiments, but module boundaries, names, comments, tests, and implementation structure must be original to Threadline. Do not preserve ChatMock-specific architecture names or layering.

## Module principles

Prefer small modules with one durable responsibility.

Keep these boundaries clear:

* protocol types separate from transport code
* request normalization separate from transport code
* downstream SSE translation separate from upstream WebSocket frame handling
* long-lived transport ownership out of route handlers
* local jobs separate from upstream protocol flow unless an internal tool connects them

Avoid catch-all modules such as `util`, `misc`, `common`, or `manager` unless the module has a narrow documented purpose.

## Suggested module boundaries

| Module | Responsibility |
| --- | --- |
| `http` | Axum routes, HTTP wrappers, and public HTTP/SSE error conversion. It must not drive Codex WebSocket IO. |
| `responses` | `/v1/responses` normalization, continuation decisions, lifecycle coordination, downstream SSE translation, and internal tool filtering. |
| `codex_ws` | Codex backend WebSocket connection setup plus upstream protocol message serialization/deserialization. |
| `ws_pump` | Continuous upstream socket IO, outbound commands, Ping/Pong, inbound forwarding, and close/error metadata. All live upstream WebSockets must be pump-owned. |
| `registry` | Completed response marker mapping, retained session metadata, in-use tracking, close/recoverable state, lookup, cleanup, and capacity policy. |
| `jobs` | Local long-running job start/state/poll/output/cancel/cleanup. Jobs must not push upstream responses when they complete. |
| `tools` | `threadline_*` internal tool definitions, validation, local execution, output shaping, and job-tool dispatch. |
| `auth` | ChatGPT/Codex credential loading, refresh, and safe credential access. Secret handling should be centralized here. |
| `config` | CLI flags, environment configuration, defaults, validation, and typed config values. It should not perform network IO. |
| `errors` | Typed internal errors, stable public error codes, safe public payloads, and boundary conversions. |

## Dependency direction

Prefer this dependency direction:

```txt
http -> responses -> registry/tools/jobs/codex_ws/errors
http -> config/errors
tools -> jobs/errors
codex_ws -> auth/config/errors
codex_ws -> ws_pump -> errors
registry -> errors
```

This is a guide, not a rigid graph. Avoid dependency cycles. If a cycle appears, narrow the boundary or move shared protocol/model types into a small purpose-built module.

## Transport ownership

Long-lived upstream WebSocket transport belongs to `ws_pump`.

The pump owns the socket. Other modules communicate with it through handles, channels, or narrow methods.

Do not pass raw `WebSocketStream` into route handlers or high-level response orchestration.

Do not let downstream request lifetime determine whether the upstream socket can answer Ping/Pong.

A retained upstream socket may outlive a downstream HTTP request.

## Protocol type separation

Protocol types should describe protocol shape; transport code should move frames and handle socket mechanics.

Good separation:

* message structs and event enums describe upstream protocol shape
* connector code sends and receives protocol messages
* pump code moves frames and handles Ping/Pong
* response code decides what downstream clients should see

Avoid mixing axum route logic with upstream event parsing, raw WebSocket handling with response normalization, registry mutation with low-level socket reads, or job output storage with SSE formatting.

## Request lifecycle shape

A new downstream `/v1/responses` request should generally flow:

1. `http` receives the request.
2. `responses` normalizes it.
3. `codex_ws` prepares upstream protocol state.
4. `ws_pump` owns live upstream IO.
5. `responses` translates relevant upstream events into downstream SSE.
6. `registry` records continuation metadata after a continuable completion.
7. `http` completes the downstream response.

A continuation request should generally flow:

1. `http` receives `previous_response_id`.
2. `responses` resolves the marker through `registry`.
3. Open retained sessions continue through the retained pump.
4. Closed but recoverable sessions use explicit recovery logic.
5. Missing or unrecoverable markers produce stable public errors.
6. The marker is preserved or updated according to protocol rules.

## Internal tools and jobs

Internal tools are Threadline-owned behavior requested through `threadline_*` tool calls.

Keep detection, local execution, pending output storage, follow-up `response.create`, and downstream filtering as separate concerns.

Do not embed one-off internal tool behavior inside SSE formatting or route handlers.

Internal tools that start or inspect long-running work should call `jobs` rather than managing job state directly.

Jobs are local Threadline state for long-running work.

A job should start quickly, not require the original downstream HTTP request to stay open, have explicit state and retrievable output, support polling/result/output/cancel/cleanup, and update local job state only when it completes.

A later internal tool call or downstream-triggered request may retrieve job state or output. Do not design jobs as hidden background upstream response senders.

## Registry architecture

The registry is the authority for retained response markers.

Registry entries should be updated deliberately and should preserve enough state to continue, reject, or recover deterministically.

Do not scatter marker ownership across unrelated modules.

Do not let the pump silently delete registry entries.

Do not let socket close handling erase continuation metadata without explicit registry logic.

Registry cleanup should be policy-driven, such as TTL, capacity, or explicit invalidation.

## Error, config, and auth architecture

Use typed errors internally and convert them into stable public errors at HTTP, SSE, internal tool, and job boundaries.

Public errors must not expose secrets, local paths, credential details, account identifiers, or unstable debug strings.

Configuration should be typed and validated early. Runtime modules should receive typed config values rather than repeatedly reading environment variables.

Authentication should be isolated. Connection code may need credentials, but unrelated modules should not parse, store, or log credential material.

Do not store production credential material in fixtures, tests, or logs.

## Concurrency architecture

Treat retained sessions as shared mutable protocol state.

Use clear ownership and synchronization.

Avoid holding locks across network IO when possible.

Prefer message passing for pump IO.

Ensure cleanup paths release in-use flags and do not orphan pumps, jobs, or registry entries when downstream requests fail or are cancelled.

## Testing architecture

Place tests near the behavior they verify when possible.

Behavioral tests should focus on durable module contracts.

Add or update tests when changing response marker handling, retained session lifecycle, pump Ping/Pong behavior, idle socket handling, recovery after socket close, registry conflicts, internal tool sequencing, job lifecycle, public error conversion, or SSE translation.

Test names must describe behavior, not implementation phases or local debugging history.

## Adding or moving modules

Before adding or moving a module, check:

* What single responsibility does this module own?
* Why does the current module not fit?
* What public types or functions does it expose?
* Which modules may depend on it?
* Does it introduce a dependency cycle?
* Does it own state or IO?
* Does it need tests?
* Does it preserve Threadline source independence?
* Does it keep protocol types separate from transport code?

Do not add a module just to park temporary code.

A refactor should not change behavior unless the behavior change is explicit and tested.

## Shared types and abstraction

Shared types should have a clear home: protocol message types near `codex_ws`, public error types near `errors`, registry state near `registry`, job state near `jobs`, and config types near `config`.

Avoid a broad `types` module unless it has a narrow documented purpose.

Do not introduce traits, generic providers, plugin systems, or broad compatibility layers unless there is an immediate Threadline need.

A useful abstraction should reduce duplication now, preserve protocol clarity, have a small interface, be easy to test, and not hide ordering or ownership rules.

## Refactor safety

During large refactors:

* preserve current behavior unless explicitly changing it
* keep changes focused
* avoid mixing formatting-only noise with logic changes
* keep commit and PR wording behavior-oriented
* run relevant validation
* call out risks honestly

Do not include phase labels, local debugging history, transcript-only context, or ChatMock porting language in code or docs.

## Architecture checklist

Before finalizing an architecture change, check:

* Each changed module has one clear responsibility.
* Protocol types are separate from transport code.
* Route handlers do not own raw long-lived WebSockets.
* Live upstream sockets are pump-owned.
* Request normalization and SSE translation are separate from raw frame handling.
* Retained session state is owned by the registry.
* Jobs are local state and not hidden upstream push mechanisms.
* Internal tools are hidden from downstream clients.
* Dependency cycles are avoided.
* Secrets are isolated in auth/config boundaries.
* Public errors are stable and safe.
* Tests, names, comments, and logs match the changed behavior.
