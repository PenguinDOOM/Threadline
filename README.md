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

Running `threadline` without a subcommand starts the server. Authentication commands live under the `login` subcommand group.

## Login And Credential Discovery

Threadline exposes these login commands:

```bash
threadline login store
threadline login status
threadline login logout
```

`threadline login store` reads the bearer token from stdin and stores it in Threadline's own OS credential-manager entry by default. It does not silently downgrade to file storage. If the OS credential manager is unavailable, the command fails instead of writing credentials somewhere else.

Before Threadline can authenticate to Codex, you need Codex credentials that were already obtained outside Threadline, such as by signing in through the Codex Desktop app or Codex CLI. Threadline does not provide its own standalone interactive login or token-acquisition flow, and `threadline login store` only stores a bearer token that you supply on stdin.

Here, stdin means you pass token text into the command by piping it from another command or redirecting it from a file, rather than typing the bearer token as a command-line flag.

```bash
printf '%s' 'YOUR_CODEX_BEARER_TOKEN' | threadline login store

threadline login store < bearer-token.txt

# Optional: include a refresh token only if you accept command-line exposure.
printf '%s' 'YOUR_CODEX_BEARER_TOKEN' | threadline login store --refresh-token YOUR_REFRESH_TOKEN
```

`threadline login status` reports whether Threadline-owned credentials are available and whether a refresh token is present, without printing token values.

`threadline login logout` deletes only Threadline-owned credentials from the OS credential manager. It does not delete, mutate, or log out Codex credentials.

Threadline's runtime auth lookup uses this precedence order:

1. An explicit bearer-token override, when one is provided by configuration.
2. The Threadline-owned OS credential-manager entry.
3. The Codex OS credential-manager entry, read-only.
4. Existing `auth.json` file fallbacks.

The Threadline-owned keyring entry is separate from Codex and is the default destination for `threadline login store`.

For Codex interoperability, Threadline can read the same OS credential-manager entry Codex uses: service `Codex Auth` with an account derived from `CODEX_HOME`. Normal Threadline command output does not print that derived account value. Threadline treats the Codex entry as a compatibility input only: it can read those credentials at runtime, but it does not write, rewrite, or delete them.

`CODEX_HOME` affects two runtime compatibility paths:

- It selects the Codex home directory used to derive the Codex keyring account for read-only interoperability.
- It is also one of the file fallback roots for `auth.json` discovery.

If `CODEX_HOME` is unset, Threadline skips the Codex keyring lookup and continues with the remaining supported sources.

When runtime auth checks the OS credential manager, keyring service failures are not always terminal. If the Threadline-owned keyring lookup or Codex keyring lookup cannot be used at runtime, Threadline may fall through to later supported sources, including existing file fallbacks.

The file fallback search keeps existing compatibility behavior and checks these roots in order:

1. `CHATGPT_LOCAL_HOME`
2. `CODEX_HOME`
3. The default per-user `.chatgpt-local` directory
4. The default per-user `.codex` directory

`auth.json` file fallbacks are read for compatibility, but `threadline login store` does not write them.

Warning: `--refresh-token` is optional, but if you use it, the refresh token remains visible in process arguments on shared systems and in local process inspection tools. Prefer stdin for the bearer token and use `--refresh-token` only when that tradeoff is acceptable.

## Local validation

Run these commands from the Threadline directory:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --test http_surface
cargo test --all-targets --all-features
```
