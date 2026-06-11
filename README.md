# Threadline

Threadline is a Rust service that will bridge VSCode Copilot BYOK Responses API traffic to the Codex backend WebSocket protocol.

Threadline is a BYOK `/v1/responses` bridge. It is not native VS Code Copilot and it does not have native editor, terminal, or extension-host tool integration.

The current implementation provides the initial HTTP surface only:

- `GET /health`
- `GET /v1/models`
- `POST /v1/responses` placeholder that returns a stable public error until the bridge is implemented

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
| `--codex-client-version` | `THREADLINE_CODEX_CLIENT_VERSION` | `0.136.0` | Codex client version Threadline sends to the upstream backend for compatibility. |
| `--retained-session-capacity` | `THREADLINE_RETAINED_SESSION_CAPACITY` | `64` | Maximum number of retained sessions kept available for response continuation. |
| `--jobs-enabled` | `THREADLINE_JOBS_ENABLED` | `false` | Enables local job execution support for long-running work. |
| `--job-output-buffer-limit-bytes` | `THREADLINE_JOB_OUTPUT_BUFFER_LIMIT_BYTES` | `32768` | Maximum in-memory buffered job output before older output is dropped. |
| `--job-retention-ttl-secs` | `THREADLINE_JOB_RETENTION_TTL_SECS` | `300` | How long completed job metadata and buffered output remain available after completion. |
| `--job-allowed-commands` | `THREADLINE_JOB_ALLOWED_COMMANDS` | None | comma-separated exact program names allowed for jobs. Threadline compares `command[0]` against each configured entry exactly, without normalizing wrappers, paths, or aliases. |
| `--log-level` | `THREADLINE_LOG_LEVEL` | `info` | Threadline log verbosity. Supported Rust tracing levels include `error`, `warn`, `info`, `debug`, and `trace`. |

Threadline does not accept an arbitrary model override through CLI flags or environment variables.

## Supported models

Threadline advertises and accepts exactly these model ids:

- `gpt-5.5`
- `gpt-5.4`
- `gpt-5.4-mini`
- `gpt-5.3-codex-spark`

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
