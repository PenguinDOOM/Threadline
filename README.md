# Threadline

Threadline is a Rust service that will bridge VSCode Copilot BYOK Responses API traffic to the Codex backend WebSocket protocol.

The current implementation provides the initial HTTP surface only:

- `GET /health`
- `GET /v1/models`
- `POST /v1/responses` placeholder that returns a stable public error until the bridge is implemented

## Non-goals

Threadline is not a general OpenAI-compatible proxy.

It does not implement unrelated providers or historical compatibility layers.

## Configuration

Threadline reads configuration from CLI flags or environment variables:

- `--host` / `THREADLINE_HOST`
- `--port` / `THREADLINE_PORT`
- `--model-id` / `THREADLINE_MODEL_ID`
- `--retained-session-capacity` / `THREADLINE_RETAINED_SESSION_CAPACITY`
- `--jobs-enabled` / `THREADLINE_JOBS_ENABLED`
- `--log-level` / `THREADLINE_LOG_LEVEL`

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
