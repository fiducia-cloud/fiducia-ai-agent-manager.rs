# fiducia-ai-agent-manager

The AI agent worker for fiducia.cloud. A Rust port of the Node.js `dev-server`.

It runs coding agents (Claude / OpenAI / Gemini / …) inside a **warm, configured
git workspace** and streams sequenced events over **SSE** and **NATS**. Per
thread it prepares/reuses a stable branch `…/<threadId>/<slug>`; per task it
drives the selected provider, then stages → commits → pushes the branch. PR
creation is an explicit action.

## Governance & architecture

The worker is **governed by the [fiducia-ai-agent-control-plane](../fiducia-ai-agent-control-plane)**:
on boot it registers as an agent and retains the authoritative id returned by
the control plane; per task it **claims the backing work-item** (receiving a
fiducia-node **fencing token**) and reports state transitions. The worker renews
the exact `ai-work:<work-item-id>` election throughout the complete claimed
lifecycle: deterministic edits, provider execution, commit, push, artifact
publication, and the final state transition. It also renews immediately before
push. Direct node calls carry both internal authentication and the same org scope
used by the control plane. A refusal, outage, malformed response, or stale token
fails closed.

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
| POST | `/thread/merge-upstream` | Merge base into the thread branch (standalone mode only) |
| POST | `/thread/make-commit` | Commit + push the workspace (standalone mode only) |
| POST | `/thread/open-pr` | Open/reuse a draft PR (`gh`; standalone mode only) |
| GET | `/tasks` | List tasks |
| GET | `/healthz`, `/status`, `/agents` | Ops surfaces |

## Build & run

```sh
cargo build --release --locked
# Inject these with the deployment secret manager; they intentionally have no
# CLI flag equivalents.
export FIDUCIA_CONTROL_PLANE_SECRET=…
export FIDUCIA_NODE_INTERNAL_SECRET=…
PORT=8080 SERVER_AUTH_SECRET=… NATS_URL=nats://… \
  CONTROL_PLANE_URL=http://fiducia-ai-agent-control-plane:8080 \
  FIDUCIA_NODE_URL=http://fiducia-node:8080 \
  FIDUCIA_NODE_ORG_ID=fiducia-ai-control-plane \
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
| `FIDUCIA_NODE_URL` | string | — | fiducia-node for exact work-election renewal; required with `CONTROL_PLANE_URL` |
| `FIDUCIA_NODE_INTERNAL_SECRET` / `FIDUCIA_INTERNAL_SECRET` | string (**secret; env-only**) | — | `x-fiducia-internal-auth` for the trusted node hop; required with `CONTROL_PLANE_URL` |
| `FIDUCIA_NODE_ORG_ID` | string | `fiducia-ai-control-plane` | Stable `x-fiducia-org-id` scope shared with the control plane |
| `EVENT_INGEST_URL` | string | — | Optional external event ingest endpoint |
| `SERVER_AUTH_SECRET` | string (**secret**) | — | `X-Server-Auth` gate on mutating routes |
| `FIDUCIA_CONTROL_PLANE_SECRET` / `FIDUCIA_INTERNAL_SECRET` | string (**secret; env-only**) | — | `x-internal-auth` to the control plane; required with `CONTROL_PLANE_URL` |
| `EVENT_INGEST_SECRET` | string (**secret**) | — | Bearer secret for the ingest endpoint |

Per-provider agent credentials (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`GEMINI_API_KEY`, `GH_PAT`, …) are read only to build the strict, per-provider
child-process env allowlist (`build_agent_env`) and are never forwarded beyond
the provider that needs them.

### flags-2-env

Non-secret operational flags map to environment variables through the pinned
[`flags-2-env`](https://github.com/ORESoftware/flags-2-env) parser
(`vendor/flags-2-env` submodule, schema in `.cli-flags.toml`, audited in CI by
`.github/workflows/cli-flags.yml`). Credentials, repository URLs, and service
URLs remain environment-only so they cannot leak through process arguments:

```sh
git submodule update --init --recursive
make -C vendor/flags-2-env all
scripts/with-flags2env.sh --port 8080 --log-format json -- \
  ./target/release/fiducia-ai-agent-manager
```

`--fiducia-node-org-id` configures the non-secret org scope. Control-plane and
node internal secrets are listed under `.cli-flags.toml`'s `[env].ignore` and
must be injected as environment variables by a secret manager; they are never
accepted on argv.

### Reproducible cross-repository inputs

CI and the Docker build use `fiducia-interfaces` commit
`487e470c45ab5851e8f6f3b1dc048fe067fbf408` and `fiducia-clients` commit
`bcf2f868697a96d82151c0e4bf0efae258b234e9`. The Dockerfile fetches each full
object id, checks it out detached, verifies `HEAD`, and then builds with
`cargo --locked`; CI checks out the same refs and pins every action and tool
version. Update the Docker arguments and workflow refs together when adopting a
reviewed dependency change.

## Security

- **Audit:** `cargo audit` runs without advisory exceptions. NATS uses the
  current `async-nats` TLS stack with `rustls-webpki` 0.103.x.
- **Auth:** every mutating route is gated by `X-Server-Auth`; the guard is
  **fail-closed** — requests are rejected with `401` when the secret is
  unconfigured or mismatched.
- **Input handling:** no `unwrap`/`panic` on request-derived input; JSON bodies
  are size-limited and parsed fallibly. Secrets are never written to logs.
- **Container privilege:** the runtime is explicitly labelled
  `tool-runner-nonroot` because the worker must spawn Git, `gh`, and provider
  CLIs and therefore cannot use a distroless image. The shipped image still
  runs as numeric uid/gid `65532:65532`, owns only its workspace directories,
  and copies the manager binary with that ownership. Images layered with agent
  tooling must preserve those three invariants and must not switch back to root.
  The base runtime includes Git, OpenSSH, and `gh`; provider-specific CLIs are
  intentionally supplied by the deployment's derived image.
- **Bounded, exact authority checks:** control-plane requests use a 5-second
  connect and 10-second total deadline; optional event ingest uses a 5-second
  connect and 15-second total deadline. Direct fiducia-node election renewals
  are capped at 10 seconds and use the authenticated, org-scoped internal node
  client. Governed startup requires both service URLs, both normalized secrets,
  a valid node org, and the returned registered-agent id. Every claimed task
  renews its exact `ai-work:<id>` election every 10 seconds from its first edit
  through provider, commit, push, artifacts, and the final transition, plus an
  exact renewal immediately before push. Any refusal or malformed
  candidate/token response fails closed.
- **Cancellation and durable lifecycle:** cancellation is checked before each
  filesystem, Git, push, and artifact mutation. Git children use
  `kill_on_drop`, are explicitly killed, and are awaited on cancellation or
  timeout. Once a work item reaches `running`, every normal exit is persisted as
  `awaiting_review`, `failed`, or `cancelled` under the same live claim. The
  unclaimed merge-upstream, manual push, and PR routes are all disabled in
  governed mode.

## Scope note

The Node service shipped seven provider runners (CLI + vendor SDKs). This port
ships one faithful **CLI runner** driving the CLI-shaped providers; the vendor
SDK runners are out of scope and can be added behind the same `AgentRunner`
trait without touching the orchestration. Likewise, storage ships the `local`
adapter behind the `StorageAdapter` trait (s3/r2/gcs/drive can follow).
