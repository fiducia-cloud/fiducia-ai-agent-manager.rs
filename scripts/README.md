# scripts

Helper scripts for working with the crate.

- `with-flags2env.sh` — bridges CLI flags to the `FIDUCIA_*` environment
  variables the `fiducia-ai-agent-manager` binary reads. It runs the pinned `flags2env`
  parser against the `.cli-flags.toml` schema, exports the resulting env map,
  then execs the given command (for example,
  `cargo run --bin fiducia-ai-agent-manager`). Control-plane and fiducia-node
  internal secrets intentionally have no flag mapping; inject them through the
  environment or a deployment secret manager.
