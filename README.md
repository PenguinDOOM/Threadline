# Threadline

Threadline is a Rust service that bridges VSCode Copilot BYOK Responses API traffic to the Codex backend WebSocket protocol.

Threadline is a BYOK `/v1/responses` bridge. It is not native VS Code Copilot and it does not have native editor, terminal, or extension-host tool integration.

The current implementation exposes these HTTP endpoints:

- `GET /health`
- `GET /v1/models`
- `POST /v1/responses`

Threadline currently supports two route profiles:

- Main: retained-session `/v1/responses` routing for primary assistant turns.
- Utility: stateless one-shot `/v1/responses` routing for utility turns. Utility does not retain upstream sessions, does not use `previous_response_id`, does not keep `context_management`, and does not execute Threadline internal tools or jobs.

## Expected bridge UX

When the `/v1/responses` bridge is implemented, the downstream experience will remain close to VSCode BYOK behavior, but it will not be identical to native Copilot UX.

- Threadline forwards assistant output over `/v1/responses` and SSE, but native VS Code editor and terminal tool integration remains outside Threadline.
- Threadline-owned `threadline_*` internal tools are executed locally and hidden from downstream clients.
- Intermediate completions that exist only to carry internal tool work are consumed by Threadline and used for follow-up requests. Downstream clients should only see the final assistant-facing turn.
- Long-running work is represented as jobs. Job state and job output are read back through job APIs, and incremental output is read by offset rather than pushed as a native editor or terminal stream.

## Observability and job output

Threadline keeps internal tool execution and job state observable without exposing Threadline-only tool calls to downstream clients.

- Job tools return stable identifiers and status so follow-up requests can poll, read buffered output, and fetch final results.
- Successful job starts return immediately and may include a short hint telling the caller to continue independent work before polling or reading output.
- Buffered job output is available before job completion, so partial output can be read incrementally instead of waiting for a single final payload.
- Output reads are append-oriented and offset-based. Repeated reads should pass the returned `next_offset` so later checks continue from the last consumed byte range.
- The output buffer is finite. When older bytes are dropped, `truncated_before` advances and any stored offset older than that value must be treated as no longer recoverable.
- Successful job starts return the existing `starting` status and success hint; a capacity rejection does not include a `job_id`, status, or success hint.
- UI rendering cadence still depends on how often the client or follow-up turn reads job output. Threadline improves partial output availability, but it does not promise native Copilot-identical live rendering cadence.
- Threadline exposes job tools and buffered output only. It does not provide native VS Code terminal, editor, or extension-host tool streaming.

## Non-goals

Threadline is not a general OpenAI-compatible proxy.

It does not implement unrelated providers or historical compatibility layers.

## Configuration

Threadline reads configuration from CLI flags or environment variables.

| Flag | Environment variable | Stable default | Description |
| --- | --- | --- | --- |
| `--host` | `THREADLINE_HOST` | `127.0.0.1` | Listen address for the downstream HTTP server that accepts local `/v1/responses` requests. |
| `--port` | `THREADLINE_PORT` | `8100` | Listen port for the downstream HTTP server. |
| `--utility-port` | `THREADLINE_UTILITY_PORT` | None | Optional port for a second utility listener in the same process. |
| `--profile` | `THREADLINE_PROFILE` | `main` | Route profile for this listener. Use `main` for retained-session routes and `utility` for stateless utility-only model aliases. |
| `--codex-client-version` | `THREADLINE_CODEX_CLIENT_VERSION` | `0.136.0` | Codex client version Threadline sends to the upstream backend for compatibility. |
| `--retained-session-capacity` | `THREADLINE_RETAINED_SESSION_CAPACITY` | `64` | Maximum number of retained sessions kept available for response continuation. |
| `--max-request-body-bytes` | `THREADLINE_MAX_REQUEST_BODY_BYTES` | `33554432` | Maximum HTTP DATA bytes accepted for `POST /v1/responses`. Accepted range: positive `usize`, `1..=usize::MAX`. |
| `--upstream-inbound-max-messages` | `THREADLINE_UPSTREAM_INBOUND_MAX_MESSAGES` | `256` | Maximum number of queued upstream Text/Binary messages per connection. Accepted range: `1..=65536`. |
| `--upstream-inbound-max-bytes` | `THREADLINE_UPSTREAM_INBOUND_MAX_BYTES` | `16777216` | Maximum queued upstream payload bytes per connection after UTF-8 conversion. Accepted range: `1..=67108864`. |
| `--jobs-enabled` | `THREADLINE_JOBS_ENABLED` | `false` | Enables local job execution support for long-running work. |
| `--persistent-reasoning-enabled` | `THREADLINE_PERSISTENT_REASONING_ENABLED` | `false` | Enables server-side persistent reasoning for eligible Main model requests. When enabled, eligible Main requests automatically receive `reasoning.context=all_turns`. |
| `--job-output-buffer-limit-bytes` | `THREADLINE_JOB_OUTPUT_BUFFER_LIMIT_BYTES` | `32768` | Maximum in-memory buffered job output before older output is dropped. |
| `--job-retention-ttl-secs` | `THREADLINE_JOB_RETENTION_TTL_SECS` | `300` | How long completed job metadata and buffered output remain available after completion. |
| `--job-max-active-jobs` | `THREADLINE_JOB_MAX_ACTIVE_JOBS` | `16` | Maximum admitted job workers whose execution reservation has not finished. |
| `--job-max-retained-jobs` | `THREADLINE_JOB_MAX_RETAINED_JOBS` | `128` | Maximum total job registry entries, including starting, running, and terminal jobs. |
| `--job-allowed-commands` | `THREADLINE_JOB_ALLOWED_COMMANDS` | None | comma-separated exact program names allowed for jobs. Threadline compares `command[0]` against each configured entry exactly, without normalizing wrappers, paths, or aliases. |
| `--log-level` | `THREADLINE_LOG_LEVEL` | `info` | Threadline log verbosity. Supported Rust tracing levels include `error`, `warn`, `info`, `debug`, and `trace`. |

`--max-request-body-bytes` limits only the HTTP DATA bytes read from `POST /v1/responses`. It does not measure characters or tokens, and does not include HTTP headers or chunk framing. A body at or below the configured limit passes the size check; a larger body is rejected before JSON or model validation. Missing or invalid `Content-Type` is rejected with HTTP 415 before body size handling, including for an oversized body. With a supported JSON `Content-Type`, an oversized body returns HTTP 413 with this fixed document:

```text
{"error":{"code":"request_body_too_large","type":"invalid_request_error","message":"The /v1/responses request body exceeds the configured byte limit."}}
```
The default is `33554432` bytes (`32 MiB = 32 * 1024 * 1024 bytes`). Values must be positive `usize` integers in the range `1..=usize::MAX`; zero, negative values, non-integers, and values above the platform `usize` maximum are rejected at startup. CLI values take precedence over environment values, and the setting is applied after restarting the application. The same setting is inherited by Main and Utility listeners, including both listeners started by one process; other endpoints remain unchanged.

Examples:

```bash
threadline --max-request-body-bytes 67108864
THREADLINE_MAX_REQUEST_BODY_BYTES=67108864 threadline
```
This is a finite compatibility policy, not a measured optimal limit. It does not guarantee bounds for JSON expansion, clones, allocator overhead, concurrent-request RSS, or upstream acceptance. There is no implicit hard ceiling or unlimited fallback; choose a finite value appropriate for the deployment.

Job capacity limits are finite `usize` values. The defaults are `16` active workers and `128` retained registry entries; the retained limit counts starting, running, and terminal entries. A value of `0` denies new job starts rather than meaning unlimited, and values are not clamped or adjusted when the retained limit is smaller than the active limit. Jobs remain disabled by default, and the output buffer remains `32768` bytes by default.

Retention cleanup is lazy. Each job-manager operation, including a missing-id lookup and a rejected start, first removes eligible entries whose terminal age is at least the `300`-second TTL. An entry is eligible only when it is terminal, its worker execution has finished, and it has a terminal timestamp. Expired entries are removed before capacity decisions. When retained capacity is needed and active capacity permits admission, unexpired eligible terminal entries are evicted in ascending completion-time order, with `job_id` as the deterministic tie-breaker. This is not LRU, reads and repeated cancellation do not refresh the TTL, and no deletion is promised while the API is inactive. Capacity pressure can remove unexpired terminal history, so the TTL is not a minimum-retention guarantee.

Surviving jobs keep their existing result, output, and output-offset behavior. Once an entry is removed, later lookups return `job_not_found`; an operation that already acquired the entry may finish against that acquired entry. Cancellation is best effort and returns the snapshot from the acquired entry even if its registry key is removed concurrently. A cancelled or early-completed job whose worker is still running continues to consume active capacity and cannot be evicted until execution finishes.

If capacity admission is refused, the stable error is `ok: false` with code `job_capacity_exceeded` and message `Threadline job capacity is exhausted.`. It has no success hint. Worker launch failure uses `job_worker_spawn_failed` with message `Threadline could not start the job worker.`, and output-reader launch failure uses `job_output_reader_spawn_failed` with message `Threadline could not start a job output reader.`. Threadline cleans up resources it owns before releasing capacity. If command cleanup cannot be confirmed, the worker remains non-evictable and capacity remains consumed until a disruptive process restart; this is reported with the fixed diagnostic `job_worker_cleanup_incomplete` rather than by silently releasing the slot.

These limits bound managed workers and registry entries, not process RSS, global output/result bytes, allocator overhead, detached work, or arbitrary descendant processes. Cancellation does not forcibly abort an uncooperative future or manage an arbitrary process tree. The standalone environment configuration helper keeps its existing invalid-value fallback-to-default behavior, while startup command-line parsing rejects malformed, negative, and overflowing values; valid zero remains zero.
Threadline does not accept an arbitrary model override through CLI flags or environment variables.
The upstream inbound limits are applied independently to each upstream WebSocket connection. They are immutable for the lifetime of that connection. Values are validated at startup; zero, negative, non-numeric, and out-of-range values are rejected rather than clamped or treated as unlimited. The defaults are finite policy defaults, not performance benchmarks. Very small values can reject otherwise ordinary upstream output, so tune them deliberately within the supported ranges.

These limits bound queued upstream message count and converted UTF-8 payload bytes. They are not a process-wide memory or RSS limit, and they do not bound the downstream SSE stream, outbound queue, payload allocation and conversion working space, or allocator and transport overhead. Binary payloads use lossy UTF-8 conversion for queue accounting. The production connector also applies the byte limit to WebSocket message size and uses a frame limit of `max(bytes, 125)`; callers that provide an already-created stream remain responsible for its transport configuration.

If the next data message cannot fit either limit, Threadline records a sticky terminal overflow and stops the upstream pump without waiting for a graceful close handshake or a downstream consumer. It does not discard older events, reconnect, or replay the request to continue. While the queue is exactly full, a Ping can still be processed; overflow occurs on the first non-fitting data message. The public failure is HTTP 502 with `type=server_error`, code `upstream_inbound_buffer_overflow`, and the fixed message `The upstream websocket inbound buffer overflowed.`. Before SSE headers this is a JSON error; after streaming begins it is emitted as one `response.failed` followed by one `[DONE]` when the downstream observes the terminal state. The error does not include payload contents or size diagnostics.

## Main And Utility Startup

The recommended startup path is one Threadline process with two listener ports:

```bash
threadline --port 8100 --jobs-enabled --utility-port 8101
```

This starts the default Main listener on port `8100` and a second stateless Utility listener in the same process on port `8101`.

The Utility listener remains stateless because the Utility route profile always uses a fresh one-shot upstream connection and never registers or retains upstream session state.

Persistent reasoning is opt-in for the Main listener. Enable it with the flag:

```bash
threadline --port 8100 --jobs-enabled --utility-port 8101 --persistent-reasoning-enabled
```

Or use the equivalent environment variable:

```bash
THREADLINE_PERSISTENT_REASONING_ENABLED=true threadline --port 8100 --jobs-enabled --utility-port 8101
```

The setting is Main-only. In dual-listener mode, the Utility listener does not receive persistent reasoning. A standalone Utility process also has an effective value of `false` even when `--persistent-reasoning-enabled` or `THREADLINE_PERSISTENT_REASONING_ENABLED=true` is configured.

fallback/debug mode still supports two separate Threadline processes with profile-specific ports:

```bash
threadline --port 8100 --jobs-enabled
threadline --port 8101 --profile utility
```

Use the two-process form when startup isolation or process-by-process debugging is more useful than the one-process convenience path.

`--retained-session-capacity 0` is optional hardening for a Main listener that should avoid retained continuation state. It is not the mechanism that makes Utility stateless. Utility is stateless because the Utility route profile always uses a fresh one-shot upstream connection and never registers or retains upstream session state.

`--utility-port` starts a second stateless Utility listener in the same process. Keep Main and Utility on separate endpoint base URLs, even in one-process mode, so clients still target `http://127.0.0.1:8100/v1` for Main and `http://127.0.0.1:8101/v1` for Utility.

## Supported Model Aliases

These are the visible model ids that Threadline advertises from `/v1/models`.

Main profile aliases:
- `threadline-main-gpt-6-astra`
- `threadline-main-gpt-5.6-sol`
- `threadline-main-gpt-5.6-terra`
- `threadline-main-gpt-5.6-luna`
- `threadline-main-gpt-5.5`
- `threadline-main-gpt-5.4`

Utility profile aliases:
- `threadline-utility-gpt-5.6-luna`
- `threadline-utility-gpt-5.4-mini`
- `threadline-utility-gpt-5.3-codex-spark`

The `gpt-5.6-sol`, `gpt-5.6-terra`, and `gpt-5.6-luna` models are supported for live upstream use. Threadline covers advertisement, validation, `model`-field rewriting, and the existing reasoning policy for those ids.

These visible ids are aliases for VS Code selection and routing. The upstream model ids sent to Codex remain `gpt-*` ids such as `gpt-5.6-sol`, `gpt-5.6-terra`, `gpt-5.6-luna`, `gpt-6-astra`, `gpt-5.5`, `gpt-5.4`, `gpt-5.4-mini`, and `gpt-5.3-codex-spark`.

For Main compatibility, Threadline still accepts direct `gpt-*` ids on the Main profile even though `/v1/models` advertises only the `threadline-main-*` aliases. The visible `threadline-main-gpt-6-astra` alias and raw `gpt-6-astra` compatibility input both resolve upstream to exact `gpt-6-astra`, are valid only for Main, and are rejected by Utility. The raw `gpt-6-astra` id is not advertised through `/v1/models`.

Astra support covers local advertisement, profile validation, model rewriting, reasoning policy, scripted upstream serialization contracts, and live upstream use.

## Persistent Reasoning

The previous experimental unconditional injection is now default-off. Users who need server-side automatic injection must opt in with `--persistent-reasoning-enabled` or `THREADLINE_PERSISTENT_REASONING_ENABLED=true`. With the setting enabled, Threadline automatically adds `reasoning.context=all_turns` to eligible Main requests using the GPT-5.6 aliases `threadline-main-gpt-5.6-sol`, `threadline-main-gpt-5.6-terra`, and `threadline-main-gpt-5.6-luna`, their raw compatibility ids `gpt-5.6-sol`, `gpt-5.6-terra`, and `gpt-5.6-luna`, and the Astra alias `threadline-main-gpt-6-astra` or raw compatibility id `gpt-6-astra`. The advertised `threadline-main-gpt-5.5` and `threadline-main-gpt-5.4` aliases, raw compatibility ids `gpt-5.5` and `gpt-5.4`, and Utility requests are not automatically eligible.

Threadline's server-side setting is independent of VS Code's `github.copilot.chat.responsesApi.persistentCoT.enabled` setting. Threadline `false` does not remove a client-explicit `reasoning.context=all_turns`; eligible GPT-5.6 and Astra Main aliases and raw compatibility ids support that client-explicit value and are also eligible for server-side injection when Threadline is `true`. Threadline `true` injects it for eligible Main requests even when the VS Code setting is `false`. The raw compatibility ids `gpt-5.5` and `gpt-5.4` remain excluded from automatic injection and continue to reject client-explicit `reasoning.context=all_turns`. Utility requests remain excluded from automatic injection, while supporting Utility aliases preserve client-explicit `reasoning.context=all_turns` under the existing capability policy. Persistent CoT with `reasoning.context=all_turns` remains rejected for the raw compatibility ids `gpt-5.5` and `gpt-5.4` when requested explicitly by the client.

The Astra alias `threadline-main-gpt-6-astra` and raw compatibility id `gpt-6-astra` participate in the same persistent-reasoning policy as the eligible GPT-5.6 Main routes.

## VS Code Custom Endpoint Setup

Use distinct visible ids and distinct profile-specific URLs so VS Code can keep Main and Utility models separate under `customendpoint/{id}`.

```json
{
	"chat.customEndpoints": [
		{
			"uri": "http://127.0.0.1:8100/v1",
			"models": [
				{
					"id": "threadline-main-gpt-6-astra",
					"name": "Threadline Main GPT-6 Astra"
				},
				{
					"id": "threadline-main-gpt-5.6-sol",
					"name": "Threadline Main GPT-5.6 Sol"
				},
				{
					"id": "threadline-main-gpt-5.6-terra",
					"name": "Threadline Main GPT-5.6 Terra"
				},
				{
					"id": "threadline-main-gpt-5.6-luna",
					"name": "Threadline Main GPT-5.6 Luna"
				},
				{
					"id": "threadline-main-gpt-5.5",
					"name": "Threadline Main GPT-5.5"
				},
				{
					"id": "threadline-main-gpt-5.4",
					"name": "Threadline Main GPT-5.4"
				}
			]
		},
		{
			"uri": "http://127.0.0.1:8101/v1",
			"models": [
				{
					"id": "threadline-utility-gpt-5.6-luna",
					"name": "Threadline Utility GPT-5.6 Luna",
					"supportsReasoningEffort": true
				},
				{
					"id": "threadline-utility-gpt-5.4-mini",
					"name": "Threadline Utility GPT-5.4 Mini",
					"supportsReasoningEffort": true
				},
				{
					"id": "threadline-utility-gpt-5.3-codex-spark",
					"name": "Threadline Utility GPT-5.3 Codex Spark"
				}
			]
		}
	],
	"chat.utilityModel": "customendpoint/threadline-utility-gpt-5.4-mini",
	"chat.utilitySmallModel": "customendpoint/threadline-utility-gpt-5.4-mini"
}
```

The visible ids in this JSON are aliases only. VS Code uses `customendpoint/threadline-main-gpt-5.5` and `customendpoint/threadline-utility-gpt-5.4-mini` as local model selectors, while Threadline rewrites the upstream `model` field to the matching `gpt-*` id.

When a GPT-6 Astra or GPT-5.6-specific prompt selection needs a raw compatibility model, a Main Custom Endpoint configuration can use `gpt-6-astra` or one of the raw GPT-5.6 ids `gpt-5.6-sol`, `gpt-5.6-terra`, or `gpt-5.6-luna` as a compatibility option. The raw `gpt-6-astra` id is a Main-only compatibility input and is not advertised through `/v1/models`. This documents a compatibility option; it does not claim or guarantee how VS Code selects internal prompts.

Utility preserves `reasoning.effort` by default when the client sends it. The `supportsReasoningEffort` model setting only controls whether VS Code shows the effort picker for that visible model id.

Utility remains stateless even when the Main listener enables retained sessions or jobs. Utility does not retain upstream sessions, does not register continuation markers, and does not execute Threadline internal tools or Threadline jobs.

Running `threadline` without a subcommand starts the server. `threadline login` is informational only and prints guidance to sign in with Codex Desktop or Codex CLI.

## Login And Credential Discovery

Before Threadline can authenticate to Codex, sign in with Codex Desktop or Codex CLI. Running `threadline login` only prints that guidance.

Threadline does not acquire, store, delete, or inspect credentials. It relies on Codex-managed authentication sources that are already present on the machine.

At runtime, Threadline uses only the Codex-managed sources it already supports:

1. The Codex OS credential-manager entry, read through the existing compatibility path.
2. Supported `auth.json` compatibility roots.

For Codex interoperability, Threadline can read the same OS credential-manager entry Codex uses: service `Codex Auth` with an account derived from `CODEX_HOME`. Normal Threadline command output does not print that derived account value. Threadline treats the Codex entry as a compatibility input only: it can read those credentials at runtime, but it does not write, rewrite, or delete them.

`CODEX_HOME` affects two runtime compatibility paths:

- It selects the Codex home directory used to derive the Codex keyring account for read-only interoperability.
- It is also one of the file fallback roots for `auth.json` discovery.

If `CODEX_HOME` is unset, Threadline skips the Codex keyring lookup and continues with the remaining supported sources.

When runtime auth checks the OS credential manager, keyring service failures are not always terminal. If the Codex keyring lookup cannot be used at runtime, Threadline may fall through to the supported `auth.json` compatibility roots.

The file fallback search keeps existing compatibility behavior and checks these roots in order:

1. `CHATGPT_LOCAL_HOME`
2. `CODEX_HOME`
3. The default per-user `.chatgpt-local` directory
4. The default per-user `.codex` directory

`auth.json` file fallbacks are read for compatibility only. Threadline does not write, rewrite, or delete them.

## Local validation

Run these commands from the Threadline directory:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```
