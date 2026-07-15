# src

The agent manager: task orchestration (`orchestrator.rs`, `thread_ops.rs`),
HTTP + SSE surface (`http.rs`), the in-process event bus with NATS mirroring
(`event_bus.rs`, `nats.rs` — JetStream with tenant-scoped dedup ids and a
logged core fallback), the shared envelope (`messaging.rs`), and state
(`state.rs`). Logging/tracing comes from the shared `fiducia-telemetry` init.
