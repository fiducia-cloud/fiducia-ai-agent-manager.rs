# Fiducia reference agent fleet

This executable is the reproducible coordination harness for Linear issue
`DEN-868`. It injects the failure modes that matter when several planners,
coders, researchers, outreach workers, and reviewers share one workspace.

Run it from a fresh repository clone:

```sh
cargo test --manifest-path tools/reference-fleet/Cargo.toml --locked
cargo run --manifest-path tools/reference-fleet/Cargo.toml --locked
```

The harness has no network or provider credentials and no third-party
dependencies. A deterministic logical clock makes lease expiry, leader
failover, quota rollover, and heartbeat expiry repeatable in CI.

## Ownership boundary

| Layer | Owns |
| --- | --- |
| Postgres, queue, object store, vector DB | Durable tasks, prompts, transcripts, artifacts, embeddings, and run history |
| Fiducia | Leases, fencing tokens, leader election, semaphores, shared quotas, heartbeats, replicated-cron deduplication, KV/CAS, and watches |
| Agent workers | Reasoning, tool execution, validation, and human escalation |

`Coordinator` is a deterministic model of the decisions made by fiducia-node;
it is not a replacement implementation. Production workers use the existing
`control_plane` and fiducia-node client paths in this repository. Keeping this
harness independent lets CI prove the contract without silently falling back to
process-local locks or requiring a live cluster.

## Scenarios proved

1. Only one worker wins a task claim race.
2. Lease renewal extends live authority but cannot resurrect an expired claim.
3. A superseded holder cannot commit with its stale fencing token.
4. A supervisor fails over only after the previous lease expires.
5. Browser/GPU/tool slots cannot be oversubscribed across workers.
6. LLM/tool quotas are shared and reset only at the distributed window boundary.
7. Workers disappear after their heartbeat TTL.
8. A scheduled job cannot fire twice when leadership changes.
9. Stale KV revisions fail CAS and successful writes emit watch events.
10. Retried external effects require a stable application idempotency key.

## Non-guarantees

Fiducia does not prove semantic correctness, prevent hallucinations, choose the
right model, or provide exactly-once effects in an external system that ignores
idempotency keys and fencing tokens. Applications must still validate outputs,
persist durable history, and make externally visible operations idempotent.
