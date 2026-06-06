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

## Local validation

Run these commands from the Threadline directory:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --test http_surface
cargo test --all-targets --all-features
```
