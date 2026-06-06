# Agent Architecture

This document expands Threadline architecture guidance for module boundaries, ownership, dependency direction, and large refactors.

Root `AGENTS.md` contains the always-on rules. If this file conflicts with root `AGENTS.md`, follow root `AGENTS.md`.

## Scope

Use this file before:

* adding a new module
* moving code between modules
* changing module responsibilities
* doing a large refactor
* changing transport ownership
* changing request normalization boundaries
* changing protocol type boundaries
* introducing new shared state
* introducing new background tasks
* introducing new abstractions over upstream or downstream IO

For protocol sequencing rules, also read `docs/agent/protocol.md`.

For names, comments, tests, logs, commits, and PR wording, also read `docs/agent/conventions.md`.

## Architecture goals

Threadline should remain focused and maintainable.

The architecture should support:

* a stable `/v1/responses` endpoint for VSCode BYOK
* HTTP/SSE downstream handling
* Codex backend WebSocket upstream handling
* retained WebSocket sessions
* `previous_response_id` continuity
* pump-based Ping/Pong handling
* nested subagent execution without idle timeout
* long-running work through jobs
* safe cleanup and recovery paths

Do not expand the architecture into a general-purpose OpenAI-compatible proxy.

Do not add unrelated provider abstractions unless explicitly requested.

## Source independence

Threadline is not a Rust port of ChatMock.

Architecture may reuse lessons from previous experiments, but module boundaries, names, comments, tests, and implementation structure must be original to Threadline.

Do not preserve ChatMock-specific architecture names or layering.

Use public protocol terms when they are actually public protocol terms.

## Module principles

Prefer small modules with one responsibility.

Keep protocol types separate from transport code.

Keep request normalization separate from transport code.

Keep client-facing SSE translation separate from upstream WebSocket frame handling.

Keep long-lived transport ownership out of route handlers.

Keep local jobs separate from upstream protocol flow unless an explicit internal tool connects them.

A module should have a clear reason to exist and a stable responsibility.

Avoid catch-all modules such as `util`, `misc`, `common`, or `manager` unless there is a narrow, durable purpose.

## Suggested module boundaries

### `http`

Owns axum routes and HTTP request/response wrappers.

Responsibilities:

* expose downstream HTTP routes
* parse HTTP-level inputs
* pass normalized work to response handling
* convert public errors into HTTP/SSE-compatible responses
* avoid owning long-lived upstream WebSocket state

The `http` module should not directly drive Codex WebSocket IO.

### `responses`

Owns `/v1/responses` request normalization and downstream SSE translation.

Responsibilities:

* normalize downstream `/v1/responses` input
* handle `previous_response_id` continuation decisions at the response layer
* coordinate request lifecycle
* translate upstream assistant-facing events into downstream SSE
* avoid exposing Threadline internal tool calls downstream

The `responses` module may coordinate registry, tools, jobs, and upstream sessions, but should not own raw WebSocket read/write loops.

### `codex_ws`

Owns Codex backend WebSocket connector behavior and protocol messages.

Responsibilities:

* connect to the Codex backend WebSocket endpoint
* define or serialize upstream protocol messages
* deserialize upstream protocol events
* isolate backend-specific WebSocket protocol details
* expose connection setup to higher layers

The `codex_ws` module should not contain downstream HTTP route logic.

### `ws_pump`

Owns WebSocket pump behavior.

Responsibilities:

* continuously read upstream frames
* write outbound upstream commands
* reply to Ping frames with Pong
* forward Text/Binary frames into inbound queues
* record close/error metadata
* keep retained sockets alive while idle
* expose a handle or channel interface to other modules

All live upstream WebSockets must be pump-owned.

Route handlers must not directly hold and use `WebSocketStream`.

### `registry`

Owns retained response/session registry state.

Responsibilities:

* map completed response markers to retained session state
* store session id and thread id metadata
* track in-use state
* track turn/window generation
* preserve recoverable close metadata
* support lookup by `previous_response_id`
* support safe cleanup and capacity limits

The registry should not perform raw WebSocket IO.

### `jobs`

Owns local long-running job management.

Responsibilities:

* start local or subprocess jobs quickly
* return `job_id`
* store job state
* support polling
* support incremental output retrieval
* support cancellation
* clean up completed jobs according to policy

Jobs must not automatically push new upstream responses when they complete.

### `tools`

Owns Threadline internal tool definitions and dispatch.

Responsibilities:

* define `threadline_*` internal tools
* execute internal tool calls locally
* validate internal tool inputs
* return tool outputs in the expected internal shape
* avoid forwarding internal tool calls downstream

The `tools` module may call `jobs` for job-related tools.

### `auth`

Owns ChatGPT/Codex authentication loading and refresh behavior.

Responsibilities:

* load configured credentials
* refresh credentials when supported
* expose safe credential access to connection code
* avoid leaking secrets into logs or public errors

The `auth` module should centralize credential handling so other modules do not duplicate secret parsing or logging behavior.

### `config`

Owns CLI flags and environment configuration.

Responsibilities:

* parse configuration
* define defaults
* validate configuration
* expose typed config values
* avoid mixing runtime state with static configuration

The `config` module should not perform network IO.

### `errors`

Owns public error payloads and internal error types.

Responsibilities:

* define typed internal errors
* define stable public error codes
* convert internal failures into safe public errors
* avoid leaking secrets, local paths, or private account details

The `errors` module should be usable by other modules without creating dependency cycles.

## Dependency direction

Prefer this dependency direction:

```txt
http
  -> responses
      -> registry
      -> tools
      -> jobs
      -> codex_ws
          -> ws_pump
      -> errors
  -> config
  -> errors

tools
  -> jobs
  -> errors

codex_ws
  -> auth
  -> config
  -> errors

ws_pump
  -> errors

registry
  -> errors
```

This is a guide, not a rigid graph, but dependency cycles should be avoided.

If a new dependency creates a cycle, reconsider the boundary.

Usually, shared types should move into a narrow protocol or model module rather than forcing two high-level modules to depend on each other.

## Transport ownership

Long-lived upstream WebSocket transport belongs to `ws_pump`.

The pump owns the socket.

Other modules communicate with the pump through handles, channels, or narrow methods.

Do not pass raw `WebSocketStream` into route handlers or high-level response orchestration.

Do not let downstream request lifetime determine whether the upstream socket can answer Ping/Pong.

A retained upstream socket may outlive a downstream HTTP request.

## Protocol type separation

Protocol types should be separate from transport mechanics.

Good separation:

* message structs and event enums describe protocol shape
* connector code sends and receives protocol messages
* pump code moves frames and handles Ping/Pong
* response code decides what downstream clients should see

Avoid mixing:

* axum route logic with upstream event parsing
* raw WebSocket frame handling with response normalization
* registry mutation with low-level socket reads
* job output storage with SSE event formatting

## Request lifecycle shape

A typical non-continuation request should flow like this:

1. `http` receives a downstream `/v1/responses` request.
2. `responses` normalizes the request.
3. `codex_ws` connects or prepares upstream protocol state.
4. `ws_pump` owns live WebSocket IO.
5. `responses` translates relevant upstream events into downstream SSE.
6. `registry` records continuation metadata after a completed response.
7. `http` completes the downstream response.

A typical continuation request should flow like this:

1. `http` receives a downstream request with `previous_response_id`.
2. `responses` resolves the marker through `registry`.
3. If open, the retained pump is used.
4. If closed but recoverable, recovery logic is attempted.
5. If missing or unrecoverable, a stable public error is returned.
6. The marker is preserved or updated according to protocol rules.

## Internal tool architecture

Internal tools are Threadline-owned behavior.

The upstream model may request a `threadline_*` tool.

Threadline executes that tool locally and hides the tool call from downstream VSCode clients.

The architecture should keep detection, execution, pending output storage, and follow-up response creation clearly separated.

Avoid embedding one-off internal tool behavior inside SSE formatting or route handlers.

Internal tools that start or inspect long-running work should call the `jobs` module rather than managing job state themselves.

## Job architecture

Jobs are local Threadline state for long-running work.

A job should not require the original downstream HTTP request to stay open.

A job should have explicit state and retrievable output.

Job completion should update local job state only.

A later internal tool call or downstream-triggered request may retrieve job state or output.

Do not design jobs as hidden background upstream response senders.

## Registry architecture

The registry is the authority for retained response markers.

Registry entries should be updated deliberately.

Do not scatter marker ownership across unrelated modules.

Do not let the pump silently delete registry entries.

Do not let socket close handling erase continuation metadata without passing through explicit registry logic.

Registry cleanup should be policy-driven, such as TTL, capacity, or explicit invalidation.

## Error architecture

Use typed errors internally.

Convert internal errors into public errors at boundaries.

Useful boundaries include:

* HTTP response boundary
* SSE event boundary
* internal tool output boundary
* job polling/result boundary

Do not expose internal debug strings as stable public contracts.

Do not expose secrets, local paths, credential details, or private account identifiers.

## Configuration architecture

Configuration should be typed and validated early.

Runtime modules should receive typed config values rather than repeatedly reading environment variables.

Avoid scattering environment variable parsing through transport, registry, job, or tool modules.

Do not put credentials into general debug output.

## Authentication architecture

Authentication should be isolated.

Connection code may need credentials, but unrelated modules should not parse or log credential material.

Credential refresh behavior should have a narrow interface.

Do not store production credential material in fixtures, tests, or logs.

## Concurrency architecture

Treat retained sessions as shared mutable protocol state.

Use clear ownership and synchronization.

Avoid holding locks across network IO when possible.

Prefer message passing for pump IO.

Ensure cleanup paths release in-use flags.

Avoid orphaning pumps, jobs, or registry entries when downstream requests fail or are cancelled.

## Testing architecture

Place tests near the behavior they verify when possible.

Behavioral tests should focus on durable module contracts.

Add or update tests when changing:

* response marker handling
* retained session lifecycle
* pump Ping/Pong behavior
* idle socket handling
* recovery after socket close
* registry conflict behavior
* internal tool sequencing
* job lifecycle
* public error conversion
* SSE translation

Test names must describe behavior, not implementation phases or local debugging history.

## Adding a new module

Before adding a new module, ask:

* What single responsibility does this module own?
* Why does an existing module not fit?
* What public types or functions does it expose?
* Which modules may depend on it?
* Does it introduce a dependency cycle?
* Does it own state?
* Does it own IO?
* Does it need tests?
* Does it preserve Threadline source independence?
* Does it keep protocol types separate from transport code?

Do not add a module just to park temporary code.

## Moving code between modules

Before moving code, check:

* Is the new location closer to the responsibility?
* Does the move reduce coupling?
* Does the move create a dependency cycle?
* Are public APIs still narrow?
* Are tests still meaningful?
* Are comments still accurate?
* Are log names still stable?
* Are protocol ordering rules unchanged?

A refactor should not change behavior unless the behavior change is explicit and tested.

## Shared types

Shared types should have a clear home.

Options include:

* protocol message types near `codex_ws`
* public error types near `errors`
* registry state types near `registry`
* job state types near `jobs`
* config types near `config`

Avoid creating a broad `types` module unless it has a narrow, documented purpose.

If many modules need the same type, check whether the type is truly shared or whether the boundary is too broad.

## Avoiding over-abstraction

Do not introduce traits, generic providers, plugin systems, or broad compatibility layers unless there is an immediate Threadline need.

Prefer direct, readable code over speculative abstraction.

A useful abstraction should:

* reduce duplication now
* preserve protocol clarity
* have a small interface
* be easy to test
* not hide critical ordering or ownership rules

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

* Does each changed module still have one clear responsibility?
* Are protocol types separate from transport code?
* Are route handlers free of raw long-lived WebSocket ownership?
* Are live upstream sockets still pump-owned?
* Is request normalization separate from transport mechanics?
* Is SSE translation separate from raw upstream frame handling?
* Is retained session state owned by the registry?
* Are jobs local state and not hidden upstream push mechanisms?
* Are internal tools hidden from downstream clients?
* Are dependency cycles avoided?
* Are secrets isolated in auth/config boundaries?
* Are public errors stable and safe?
* Are tests updated for changed behavior?
* Are names and comments free of phase labels and ChatMock-specific implementation structure?
