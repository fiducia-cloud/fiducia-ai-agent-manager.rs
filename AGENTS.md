# Agent Context — fiducia-ai-agent-manager

Rust AI agent worker (Rust port of the Node.js `dev-server`). HTTP on `:8080`.
Runs coding agents in a warm git workspace and streams sequenced events over SSE
+ NATS. Governed by the **fiducia-ai-agent-control-plane** (HTTP via
`fiducia-clients`); external mutations verify a fiducia-node fencing token.

Build/test: `cargo build --release --locked` and `cargo test`. Path deps resolve
against the sibling `fiducia-interfaces/generated/rust` and
`fiducia-clients/clients/rust` crates.

Module map: `config`, `agents` (provider taxonomy + CLI runner + strict env),
`git` (sh_capture + branch/repo helpers + session prep), `event_bus` (per-task
replay, circuit-breaker ingest, log sink, NATS fan-out), `orchestrator`
(`run_task`: prepare → optimistic edit → agent → commit/push → artifacts),
`control_plane` (register / claim work-item / transition / fencing verify),
`thread_ops` (merge/commit/PR), `storage` (local adapter), `sanitize`, `prompt`,
`http` (axum routes), `state`.

## Git branch policy — temporary

Work directly on `main`. Do not create feature branches or worktrees. Preserve
uncommitted work; stop for operator guidance if switching to `main` is unsafe.

## Command safety — STRICT

Never run destructive/irreversible shell commands (`rm -rf`, raw `mv` of tracked
files, `git stash`, history rewrites). Remove/move files through git so changes
are tracked and recoverable.

## Syncing with the remote

"Sync with the remote" (or just "sync") is a **two-way** exchange — pull the
remote's commits down **and** push yours up. It is never push-only, and a clean
local tree does not by itself mean "synced": you are done only once local and
the remote hold the same commits.

To sync:

1. **Commit your work first** (`git add` + `git commit`) so the tree is clean —
   pull/merge only into a clean tree. `git pull` / `git merge` aborts when an
   incoming change touches a file you have edited, and even when it doesn't it
   buries the merge in your uncommitted work. (Can't commit yet? `git stash`,
   then `git stash pop` after step 3.)
2. `git fetch --all --prune` — safe any time; it only updates tracking refs.
3. `git pull` (fetch + merge) — or `git merge` the upstream branch — to
   integrate the remote's commits.
4. `git push` to publish yours.

Integrate with **`git merge` / `git pull`**. **Never `git rebase` to sync** — it
rewrites history and breaks shared branches.
