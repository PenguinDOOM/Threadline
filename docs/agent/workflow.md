# Agent Workflow

This document expands Threadline workflow rules for local state, validation, CI, security scanning, and final development summaries.

Root `AGENTS.md` contains the always-on rules. If this file conflicts with root `AGENTS.md`, follow root `AGENTS.md`.

## Scope

Use this file before:

* finishing a code change
* deciding which validation commands to run
* editing GitHub Actions workflows
* editing CodeQL configuration
* creating local-only notes
* writing final development summaries
* preparing commit or PR validation notes
* deciding what not to include in source files, tests, docs, commits, or PR text

## Workflow principles

Keep development work reproducible.

Keep committed files free of private local context.

Keep validation results separate from source comments.

Keep temporary orchestration notes out of public docs, tests, code comments, commit messages, and PR text.

Report what changed, what was validated, and what risks remain.

## Before changing files

Before editing code, identify the affected area:

* Protocol behavior: read `docs/agent/protocol.md`.
* Naming, comments, tests, logs, commits, or PR wording: read `docs/agent/conventions.md`.
* Module boundaries or large refactors: read `docs/agent/architecture.md`.
* Validation, CI, CodeQL, local notes, or summary wording: use this file.

For small edits, still follow root `AGENTS.md`.

For large edits, prefer focused changes over broad rewrites.

## Local-only state

Local orchestration notes must not be committed unless generalized into durable documentation.

Use local-only files for scratch notes, debugging notes, and temporary coordination.

Recommended local-only paths:

```txt
.threadline/notes.md
.threadline/debug-log.md
.threadline/orchestration.md
```

These files should be ignored by git.

Do not copy local-only context into:

* source comments
* test names
* public docs
* commit messages
* PR descriptions
* fixtures
* GitHub Actions names
* CodeQL workflow names
* final public summaries

## Git ignore expectations

Use `.gitignore` for local state directories and private local configuration.

Recommended ignored paths:

```gitignore
.threadline/
*.local.json
*.local.toml
*.log
```

Do not commit production credentials, local cookies, refresh tokens, access tokens, or account identifiers.

Do not store production credentials in test fixtures.

## Secret handling

Never commit or log:

* access tokens
* refresh tokens
* cookies
* full authorization headers
* account identifiers
* local credential files
* production credentials
* private local paths that reveal credential locations

Validation output and error reports must also avoid secrets.

When summarizing a failure, describe the failing component and error class without copying sensitive data.

## Development loop

Use this general loop:

1. Understand the affected behavior.
2. Check the relevant agent docs.
3. Make the smallest durable change that fits the existing module responsibilities.
4. Add or update tests when behavior changes.
5. Run formatting.
6. Run static checks.
7. Run tests.
8. Record validation results in the final development summary.

Do not add source comments that merely say which step or phase produced the change.

Do not add temporary branch names, local phase labels, or model-conversation artifacts to committed files.

## Validation baseline

Before considering a change complete, run the relevant checks:

```sh
cargo fmt
cargo clippy --all-targets --all-features
cargo test
```

Prefer running all three for behavior changes.

For documentation-only changes, `cargo fmt`, `cargo clippy`, and `cargo test` may be unnecessary, but the final summary should say that no code validation was needed.

## Formatting

Run:

```sh
cargo fmt
```

Formatting should be clean before final reporting.

Do not leave formatting-only noise mixed into unrelated changes unless the repository already requires it.

## Clippy

Run:

```sh
cargo clippy --all-targets --all-features
```

Treat new Clippy warnings as issues to fix unless there is a clear reason not to.

If a warning is intentionally allowed, use the narrowest possible allow and explain the durable reason in code only if future maintainers need it.

Do not silence warnings just to pass a temporary branch.

## Tests

Run:

```sh
cargo test
```

Add or update tests when changing:

* retained session behavior
* WebSocket pump behavior
* Ping/Pong behavior
* `previous_response_id` continuation
* registry conflict behavior
* internal tool lifecycle
* job lifecycle
* public error handling
* SSE translation behavior
* security-sensitive behavior

Test names should describe stable behavior, not implementation phases.

## Targeted validation

When a full validation run is expensive or unnecessary, run the most relevant targeted checks first.

Examples:

```sh
cargo test registry
cargo test jobs
cargo test responses
cargo test ws_pump
```

After targeted checks pass, prefer running the full baseline before final completion when the change affects shared behavior.

## When validation cannot be run

If a check cannot be run, do not hide it.

Record it in the final development summary.

Include:

* the command that was not run
* the reason it was not run
* any partial validation that was run instead
* the risk that remains

Do not put validation excuses in source comments.

Good final summary wording:

```txt
Validation:
* Not run: cargo test. Reason: Rust toolchain is unavailable in this environment.
```

Bad source comment:

```rust
// cargo test was not run here because this was generated locally.
```

## CI workflow rules

Keep GitHub Actions workflow names stable and descriptive.

Workflow names should describe the durable purpose of the workflow.

Good:

```yaml
name: Rust CI
```

```yaml
name: CodeQL
```

Bad:

```yaml
name: Phase 2 Rust Test
```

```yaml
name: Temporary first-write checks
```

Do not add temporary branch names, local phase labels, orchestration notes, or model-conversation context to workflow names, job names, or step names.

## CI job and step names

Job and step names should be stable and grep-friendly.

Good:

```yaml
jobs:
  test:
    name: Test
```

```yaml
- name: Run cargo test
  run: cargo test
```

Bad:

```yaml
jobs:
  phase_3_test:
    name: Phase 3 local validation
```

```yaml
- name: Apply Codex suggested check
  run: cargo test
```

## CodeQL

Use CodeQL for Rust security scanning.

Prefer manual build mode so analysis sees the same crate graph that `cargo build` uses.

Keep CodeQL workflow names and job names durable.

Do not make CodeQL configuration depend on private local paths or local machine state.

Do not commit temporary CodeQL experiments unless they are generalized into durable CI configuration.

## Development summaries

When reporting changes, use this shape:

```txt
Changed:
* ...

Validation:
* ...

Risks:
* ...
```

Keep summaries factual and brief.

Summaries should mention user-visible or maintainer-visible behavior, not local orchestration steps.

## Changed section

The `Changed` section should describe what was modified.

Good:

```txt
Changed:
* Added pump-owned upstream WebSocket handling for retained sessions.
* Preserved response markers after recoverable idle closes.
```

Bad:

```txt
Changed:
* Applied phase 2 fixes from local debugging.
* Used Codex suggestion from the transcript.
```

## Validation section

The `Validation` section should list commands run and results.

Good:

```txt
Validation:
* cargo fmt
* cargo clippy --all-targets --all-features
* cargo test
```

If validation was partial:

```txt
Validation:
* cargo fmt
* Not run: cargo test. Reason: no Rust toolchain is available in this environment.
```

Do not claim checks were run if they were not run.

## Risks section

The `Risks` section should mention remaining uncertainty.

Good:

```txt
Risks:
* Reconnect behavior depends on upstream close metadata that should be tested against a live backend.
```

Bad:

```txt
Risks:
* None, probably.
```

Use `Risks: None known.` only when there is no specific remaining concern.

## Final summary safety

Final summaries must not include:

* private local paths
* access tokens
* refresh tokens
* cookies
* authorization headers
* account identifiers
* raw credential file contents
* transcript-only context
* temporary phase labels
* local machine details
* “Codex told me to...” wording

Mention public file paths only when they are part of the repository.

## Pull request readiness checklist

Before opening or finalizing a PR, check:

* The change is focused.
* Root `AGENTS.md` rules were followed.
* Relevant split docs were consulted.
* Code comments explain durable design intent only.
* Test names describe behavior.
* Logs use structured fields and do not expose secrets.
* Public errors are stable and safe.
* Local-only files remain ignored.
* CI names are stable and descriptive.
* CodeQL configuration does not depend on private local state.
* Validation results are recorded honestly.
* Remaining risks are called out.

## No background completion claims

Do not say work will be completed later unless an explicit scheduled task or external workflow actually exists.

Do not imply local jobs, CI, or background work have run unless they have actually run.

When reporting status, distinguish clearly between:

* changed
* not changed
* validated
* not validated
* recommended next action
* remaining risk

## Workflow checklist

Before finalizing work, verify:

* No local credentials or account identifiers were added.
* No `.threadline/` scratch notes were committed.
* No `*.local.json`, `*.local.toml`, or `*.log` files were committed.
* No source comment contains phase labels or transcript-only context.
* No test name contains local debugging history.
* No workflow/job/step name contains temporary branch or phase wording.
* Relevant Rust checks were run, or skipped checks are explained.
* The final summary uses `Changed`, `Validation`, and `Risks`.
