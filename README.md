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
| `--jobs-enabled` | `THREADLINE_JOBS_ENABLED` | `false` | Enables local job execution support for long-running work. |
| `--job-output-buffer-limit-bytes` | `THREADLINE_JOB_OUTPUT_BUFFER_LIMIT_BYTES` | `32768` | Maximum in-memory buffered job output before older output is dropped. |
| `--job-retention-ttl-secs` | `THREADLINE_JOB_RETENTION_TTL_SECS` | `300` | How long completed job metadata and buffered output remain available after completion. |
| `--job-allowed-commands` | `THREADLINE_JOB_ALLOWED_COMMANDS` | None | comma-separated exact program names allowed for jobs. Threadline compares `command[0]` against each configured entry exactly, without normalizing wrappers, paths, or aliases. |
| `--log-level` | `THREADLINE_LOG_LEVEL` | `info` | Threadline log verbosity. Supported Rust tracing levels include `error`, `warn`, `info`, `debug`, and `trace`. |

Threadline does not accept an arbitrary model override through CLI flags or environment variables.

## Main And Utility Startup

The recommended startup path is one Threadline process with two listener ports:

```bash
threadline --port 8100 --jobs-enabled --utility-port 8101
```

This starts the default Main listener on port `8100` and a second stateless Utility listener in the same process on port `8101`.

The Utility listener remains stateless because the Utility route profile always uses a fresh one-shot upstream connection and never registers or retains upstream session state.

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

- `threadline-main-gpt-5.6-sol`
- `threadline-main-gpt-5.6-terra`
- `threadline-main-gpt-5.6-luna`
- `threadline-main-gpt-5.5`
- `threadline-main-gpt-5.4`

Utility profile aliases:

- `threadline-utility-gpt-5.4-mini`
- `threadline-utility-gpt-5.3-codex-spark`

The `gpt-5.6-sol`, `gpt-5.6-terra`, and `gpt-5.6-luna` entries are next models. Threadline currently covers local advertisement, validation, and `model`-field rewriting for those ids. Live upstream behavior remains unverified until upstream release makes direct testing possible.

These visible ids are aliases for VS Code selection and routing. The upstream model ids sent to Codex remain `gpt-*` ids such as `gpt-5.6-sol`, `gpt-5.6-terra`, `gpt-5.6-luna`, `gpt-5.5`, `gpt-5.4`, `gpt-5.4-mini`, and `gpt-5.3-codex-spark`.

For Main compatibility, Threadline still accepts direct `gpt-*` ids on the Main profile even though `/v1/models` advertises only the `threadline-main-*` aliases.

Persistent CoT with `reasoning.context=all_turns` does not currently support the raw compatibility ids `gpt-5.6-sol`, `gpt-5.6-terra`, `gpt-5.6-luna`, `gpt-5.5`, and `gpt-5.4`; revisit that later rather than enabling it now. For now, keep `github.copilot.chat.responsesApi.persistentCoT.enabled=false` in VS Code; the current default is already `false`. When supported all-turn reasoning is needed, use the advertised Threadline aliases rather than the raw compatibility ids.

## VS Code Custom Endpoint Setup

Use distinct visible ids and distinct profile-specific URLs so VS Code can keep Main and Utility models separate under `customendpoint/{id}`.

```json
{
	"chat.customEndpoints": [
		{
			"uri": "http://127.0.0.1:8100/v1",
			"models": [
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
