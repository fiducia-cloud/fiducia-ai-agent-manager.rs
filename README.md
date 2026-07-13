# fiducia-ai-agent-manager

The AI agent worker for fiducia.cloud. A Rust port of the Node.js `dev-server`.

It runs coding agents (Claude / OpenAI / Gemini / …) inside a **warm, configured
git workspace** and streams sequenced events over **SSE** and **NATS**. Per
thread it prepares/reuses a stable branch `…/<threadId>/<slug>`; per task it
drives the selected provider, then stages → commits → pushes the branch. PR
creation is an explicit action.

## Governance & architecture

The worker is **governed by the [fiducia-ai-agent-control-plane](../fiducia-ai-agent-control-plane)**:
on boot it registers as an agent; per task it **claims the backing work-item**
(receiving a fiducia-node **fencing token**) and reports state transitions. Any
external mutation (git push, PR) verifies the fencing token first, so a stale
worker cannot act. See the repo's `messaging-architecture` note.

| Concern | Mechanism |
| --- | --- |
| Governance / authority | **fiducia-ai-agent-control-plane** over HTTP (`fiducia-clients`), fiducia-node fencing tokens |
| Messaging | **NATS** — Core for live progress, JetStream for durable execution-lifecycle events |
| Event delivery | per-task SSE replay + live, circuit-breaker ingest, log sink |
| Agent runners | CLI runner over the 7-provider taxonomy (`agents`) |
| Shared types | `fiducia-interfaces` |

## HTTP API

All routes except `/healthz` / `/status` / `/agents` require `X-Server-Auth`.

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/tasks` | Queue a task `{ prompt, taskId?, threadId?, provider? }` |
| GET | `/stream/{taskId}` | Server-Sent Events of agent activity (resumable) |
| POST | `/tasks/{taskId}/cancel` | Cancel an in-flight task |
| POST | `/thread/merge-upstream` | Merge base into the thread branch |
| POST | `/thread/make-commit` | Commit + push the workspace |
| POST | `/thread/open-pr` | Open/reuse a draft PR (`gh`) |
| GET | `/tasks` | List tasks |
| GET | `/healthz`, `/status`, `/agents` | Ops surfaces |

## Build & run

```sh
cargo build --release --locked
PORT=8080 SERVER_AUTH_SECRET=… NATS_URL=nats://… \
  CONTROL_PLANE_URL=http://fiducia-ai-agent-control-plane:8080 \
  WORKSPACE_REPO=/home/node/workspace/repo BASE_BRANCH=dev \
  ./target/release/fiducia-ai-agent-manager
```

## Configuration

Every knob is read once at boot from the environment (`src/config.rs`,
`src/agents.rs`). Secrets are marked; never log them.

| Env var | Type | Default | Description |
| --- | --- | --- | --- |
| `HOST` | string | `0.0.0.0` | Bind address |
| `PORT` | integer | `8080` | HTTP/SSE port |
| `WORKSPACE_REPO` | string | `/home/node/workspace/repo` | Warm git checkout shared by all tasks |
| `FIDUCIA_REPO_URL` / `DD_REPO_URL` | string | — | Remote git URL for the workspace |
| `BASE_BRANCH` | string | `dev` | Branch tasks branch from / merge upstream |
| `AGENT_BRANCH_PREFIX` | string | `agent/k8s/openai-5.5` | Prefix for per-thread agent branches |
| `AGENT_PROVIDER` | string | `generic-ai-sdk` | Default agent provider |
| `WORKER_BIND_MODE` | string | auto | `thread` or `repo` |
| `REMOTE_DEV_THREAD_ID` / `THREAD_ID` | string | — | Thread this worker is pinned to |
| `OUTPUTS_DIR` | string | `/home/node/workspace/outputs` | Task artifact directory |
| `LOG_DIR` | string | `/tmp/convos` | Per-conversation log directory |
| `LOG_FORMAT` | string | human | `json` for structured logs |
| `AGENT_RUN_TIMEOUT_MS` | integer | `7200000` | Per-task agent timeout |
| `IDLE_TIMEOUT_MS` | integer | `1800000` | Idle shutdown window |
| `NATS_URL` | string | — | NATS server (live + durable events) |
| `NATS_EVENT_SUBJECT` | string | `fiducia.executions.progress.v1` | Progress event subject |
| `CONTROL_PLANE_URL` / `FIDUCIA_CONTROL_PLANE_URL` | string | — | Control-plane base URL |
| `FIDUCIA_NODE_URL` | string | — | fiducia-node for fencing-token verification |
| `EVENT_INGEST_URL` | string | — | Optional external event ingest endpoint |
| `SERVER_AUTH_SECRET` | string (**secret**) | — | `X-Server-Auth` gate on mutating routes |
| `FIDUCIA_CONTROL_PLANE_SECRET` / `FIDUCIA_INTERNAL_SECRET` | string (**secret**) | — | `x-internal-auth` to the control plane |
| `EVENT_INGEST_SECRET` | string (**secret**) | — | Bearer secret for the ingest endpoint |

Per-provider agent credentials (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`GEMINI_API_KEY`, `GH_PAT`, …) are read only to build the strict, per-provider
child-process env allowlist (`build_agent_env`) and are never forwarded beyond
the provider that needs them.

### flags-2-env

CLI flags map to these env vars through the pinned
[`flags-2-env`](https://github.com/ORESoftware/flags-2-env) parser
(`vendor/flags-2-env` submodule, schema in `.cli-flags.toml`, audited in CI by
`.github/workflows/cli-flags.yml`):

```sh
git submodule update --init --recursive
make -C vendor/flags-2-env all
scripts/with-flags2env.sh --port 8080 --nats-url nats://localhost:4222 -- \
  ./target/release/fiducia-ai-agent-manager
```

## Security

- **Audit:** `cargo audit` is green (`cargo audit` exits 0). See
  `.cargo/audit.toml` for four accepted `rustls-webpki` 0.102.8 advisories
  (RUSTSEC-2026-0104 / 0098 / 0099 / 0049). They are reached only through
  `async-nats v0.38.0` (which hard-pins `rustls-webpki ^0.102`); the fix requires
  async-nats ≥ 0.49, a breaking major bump, so it is accepted with rationale
  rather than forced. The reqwest/HTTP TLS path already uses the patched 0.103.13.
  The residual webpki only verifies the trusted internal NATS broker's TLS cert.
  `rustls-pemfile` (RUSTSEC-2025-0134) is an informational "unmaintained" warning
  reached via reqwest; it is not a vulnerability.
- **Auth:** every mutating route is gated by `X-Server-Auth`; the guard is
  **fail-closed** — requests are rejected with `401` when the secret is
  unconfigured or mismatched.
- **Input handling:** no `unwrap`/`panic` on request-derived input; JSON bodies
  are size-limited and parsed fallibly. Secrets are never written to logs.

## Scope note

The Node service shipped seven provider runners (CLI + vendor SDKs). This port
ships one faithful **CLI runner** driving the CLI-shaped providers; the vendor
SDK runners are out of scope and can be added behind the same `AgentRunner`
trait without touching the orchestration. Likewise, storage ships the `local`
adapter behind the `StorageAdapter` trait (s3/r2/gcs/drive can follow).
