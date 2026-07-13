//! fiducia-ai-agent-manager: the AI agent worker.
//!
//! Rust port of the Node.js `dev-server`. It runs coding agents (Claude / OpenAI
//! / …) inside a warm, configured git workspace and streams sequenced events over
//! SSE and NATS. Per thread it prepares/reuses a stable branch and runs the
//! selected provider; PR creation is an explicit action.
//!
//! The worker is *governed* by the **fiducia-ai-agent-control-plane**
//! ([`control_plane`]): it registers as an agent, claims the work-item behind a
//! task (fencing token), and reports transitions. Messaging is NATS (Core for
//! live progress, JetStream for durable lifecycle); external mutations (git push,
//! PR) verify the fencing token first. See the `messaging-architecture` note.

pub mod agents;
pub mod config;
pub mod control_plane;
pub mod event_bus;
pub mod git;
pub mod http;
pub mod messaging;
pub mod nats;
pub mod orchestrator;
pub mod prompt;
pub mod sanitize;
pub mod state;
pub mod storage;
pub mod thread_ops;
pub mod util;

pub use config::Config;
pub use http::router;
pub use state::AppState;
