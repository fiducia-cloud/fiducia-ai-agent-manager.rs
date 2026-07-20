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

"Sync with the remote" (or just "sync") is **bidirectional and always contacts
the remote** — it fetches *and* pushes, never push-only. A clean local working
tree does **not** by itself mean "synced": a sync is not finished until local
and the remote have exchanged commits in both directions.

How to sync:

1. `git fetch --all --prune` — always safe; it only updates remote-tracking
   refs and never touches your working tree, so run it any time.
2. Make the working tree **clean before you pull/merge**: `git add` +
   `git commit` your work (or `git stash`). **Only `git pull` / `git merge`
   when the tree is not dirty** — pulling into a dirty tree makes git refuse
   the merge or tangle uncommitted edits with the incoming commits.
3. `git pull` (which fetches + merges) — or `git merge` the upstream tracking
   branch — to integrate the remote's commits into your now-clean branch.
4. `git push` — publish your commits so the remote has them too.

Integrate with **`git merge`** / **`git pull`** (which merges). **Never
`git rebase`** to sync — it rewrites history and breaks shared branches.
