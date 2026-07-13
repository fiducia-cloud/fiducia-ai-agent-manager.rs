//! Shared runtime state — the Rust analogue of the module-level `tasks` /
//! `sessions` maps and helpers in `server.ts`.
//!
//! A **session** is one thread's warm workspace + stable branch; tasks on the
//! same thread share it and run serially through the session queue. A **task**
//! is one agent run: its own sequenced event stream, cancel token, and lifecycle.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

use crate::agents::AgentProvider;
use crate::config::Config;
use crate::control_plane::ControlPlane;
use crate::event_bus::{BusEvent, EventBus};
use crate::nats::Nats;
use crate::storage::LocalStorage;

/// One thread's warm workspace + branch. The `queue` async-mutex serialises task
/// execution so only one agent mutates the checkout at a time.
pub struct ThreadSession {
    pub session_id: String,
    pub user_id: Option<String>,
    pub workspace_path: String,
    pub branch: String,
    pub queue: AsyncMutex<()>,
    pub ready: AsyncMutex<bool>,
    pub task_ids: Mutex<Vec<String>>,
    pub queued_task_ids: Mutex<Vec<String>>,
    pub running_task_id: Mutex<Option<String>>,
}

/// One agent run.
pub struct TaskState {
    pub task_id: String,
    pub prompt: String,
    pub user_id: Option<String>,
    pub thread_id: Option<String>,
    pub provider: AgentProvider,
    pub branch: String,
    pub worktree_path: String,
    pub session: Arc<ThreadSession>,
    pub cancel: CancellationToken,
    pub seq: AtomicI64,
    pub finished: AtomicBool,
    pub cancelled: AtomicBool,
    pub finished_at: Mutex<Option<i64>>,
}

impl TaskState {
    pub fn next_seq(&self) -> i64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }
}

/// The whole worker's shared state, cloned into every axum handler.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub bus: Arc<EventBus>,
    pub control_plane: ControlPlane,
    pub nats: Arc<Nats>,
    pub storage: Arc<LocalStorage>,
    pub sessions: Arc<Mutex<HashMap<String, Arc<ThreadSession>>>>,
    pub tasks: Arc<Mutex<HashMap<String, Arc<TaskState>>>>,
    pub started_at: String,
    pub instance_id: String,
}

impl AppState {
    /// Emit a wrapped event for a task, allocating its next sequence number and
    /// routing it through the bus (replay + live + ingest + log + NATS).
    pub fn emit(&self, task: &Arc<TaskState>, mut event: Value) {
        if let Value::Object(ref mut m) = event {
            m.entry("provider").or_insert_with(|| Value::String(task.provider.as_str().into()));
        }
        let seq = task.next_seq();
        self.bus.emit(BusEvent {
            task_id: task.task_id.clone(),
            thread_id: task.thread_id.clone(),
            user_id: task.user_id.clone(),
            seq,
            event,
        });
    }

    /// Reclaim tasks finished longer ago than `retain_ms`: drop their replay
    /// buffers from the bus, remove them from the task map, and detach them from
    /// their session. Bounds memory the way the Node service's 1h task GC did.
    /// Returns the number of tasks reclaimed.
    pub fn gc_finished(&self, retain_ms: i64) -> usize {
        let cutoff = chrono::Utc::now().timestamp_millis() - retain_ms.max(0);
        let expired: Vec<String> = {
            let tasks = self.tasks.lock();
            tasks
                .values()
                .filter(|t| t.is_finished() && (*t.finished_at.lock()).is_some_and(|ts| ts < cutoff))
                .map(|t| t.task_id.clone())
                .collect()
        };
        for id in &expired {
            if let Some(task) = self.tasks.lock().remove(id) {
                task.session.task_ids.lock().retain(|t| t != id);
            }
            self.bus.gc_task(id);
        }
        // Drop sessions with no remaining tasks and nothing running.
        let mut sessions = self.sessions.lock();
        sessions.retain(|_, s| !s.task_ids.lock().is_empty() || s.running_task_id.lock().is_some());
        expired.len()
    }

    /// Resolve the session id for a task (`getSessionId`).
    pub fn session_id(&self, thread_id: Option<&str>, task_id: &str) -> String {
        thread_id
            .map(str::to_string)
            .or_else(|| self.config.thread_id.clone())
            .unwrap_or_else(|| task_id.to_string())
    }
}
