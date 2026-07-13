//! Reactive event bus — the Rust port of `event-bus.ts`. It provides the same
//! guarantees the RxJS pipeline did, expressed with Tokio primitives:
//!
//!   1. **Per-task replay** — late SSE subscribers get full history.
//!   2. **Seq dedup** — duplicate `(taskId, seq)` events are dropped.
//!   3. **Live fan-out** — a broadcast channel per task feeds active streams.
//!   4. **Circuit-breaker ingest** — events POST to the Vercel-style ingest URL
//!      with retry + a breaker that opens after sustained failures.
//!   5. **Log sink** — batched append to `{logDir}/{threadId}/thread.log`.
//!   6. **NATS publish** — every event also fans out as an enveloped message.
//!
//! Terminal (`done`) events complete a task's stream; [`EventBus::gc_task`]
//! reclaims a finished task's buffer after the grace period.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::messaging::MessageEnvelope;
use crate::nats::Nats;
use crate::sanitize::sanitize_event_text;

const CIRCUIT_FAILURE_THRESHOLD: u32 = 15;
const CIRCUIT_RESET_MS: i64 = 60_000;
const REPLAY_CAP: usize = 2000;

/// One sequenced, wrapped event (`StoredEvent`).
#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub seq: i64,
    pub event: Value,
}

/// The bus-level event a caller emits (`BusEvent`).
pub struct BusEvent {
    pub task_id: String,
    pub thread_id: Option<String>,
    pub user_id: Option<String>,
    pub seq: i64,
    pub event: Value,
}

struct TaskChannel {
    history: Vec<StoredEvent>,
    seqs: HashSet<i64>,
    tx: broadcast::Sender<StoredEvent>,
    done: bool,
}

#[derive(Default, Clone, Copy)]
struct CircuitState {
    consecutive_failures: u32,
    is_open: bool,
    last_fail_at: i64,
}

struct Ingest {
    url: String,
    secret: String,
}

pub struct EventBus {
    tasks: Mutex<HashMap<String, TaskChannel>>,
    circuit: Mutex<CircuitState>,
    http: reqwest::Client,
    ingest: Option<Ingest>,
    log_dir: String,
    nats: Arc<Nats>,
    subject: String,
    emitted: AtomicU64,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl EventBus {
    pub fn new(
        ingest_url: Option<String>,
        ingest_secret: Option<String>,
        log_dir: String,
        nats: Arc<Nats>,
        subject: String,
    ) -> Arc<Self> {
        let ingest = match (ingest_url, ingest_secret) {
            (Some(url), Some(secret)) => Some(Ingest { url, secret }),
            _ => None,
        };
        Arc::new(EventBus {
            tasks: Mutex::new(HashMap::new()),
            circuit: Mutex::new(CircuitState::default()),
            http: reqwest::Client::new(),
            ingest,
            log_dir,
            nats,
            subject,
            emitted: AtomicU64::new(0),
        })
    }

    /// Emit an event: dedup, append to replay history, fan out live, and kick off
    /// the best-effort ingest / log / NATS side-channels.
    pub fn emit(self: &Arc<Self>, ev: BusEvent) {
        // Sanitize any string content before it can leave the process.
        let event = sanitize_value(&ev.event);
        let stored = StoredEvent {
            seq: ev.seq,
            event: event.clone(),
        };
        let kind = event.get("kind").and_then(|k| k.as_str()).unwrap_or("").to_string();

        {
            let mut tasks = self.tasks.lock();
            let ch = tasks.entry(ev.task_id.clone()).or_insert_with(|| {
                let (tx, _rx) = broadcast::channel(REPLAY_CAP);
                TaskChannel {
                    history: Vec::new(),
                    seqs: HashSet::new(),
                    tx,
                    done: false,
                }
            });
            if ch.seqs.contains(&ev.seq) {
                return; // exact duplicate
            }
            ch.seqs.insert(ev.seq);
            ch.history.push(stored.clone());
            if ch.history.len() > REPLAY_CAP {
                let overflow = ch.history.len() - REPLAY_CAP;
                ch.history.drain(0..overflow);
            }
            let _ = ch.tx.send(stored.clone());
            if kind == "done" {
                ch.done = true;
            }
        }
        self.emitted.fetch_add(1, Ordering::Relaxed);

        // Side-channels (best-effort, off the hot path).
        self.spawn_ingest(&ev.task_id, ev.seq, &event);
        self.spawn_log_sink(ev.thread_id.clone(), &ev.task_id, ev.seq, &kind, &event);
        self.spawn_nats(ev.thread_id.clone(), ev.user_id.clone(), &ev.task_id, ev.seq, &event);
    }

    /// A snapshot of a task's history (seq > `after`) plus a live receiver. The
    /// SSE handler replays the snapshot then streams the receiver until `done`.
    pub fn subscribe(
        &self,
        task_id: &str,
        after: i64,
    ) -> Option<(Vec<StoredEvent>, broadcast::Receiver<StoredEvent>, bool)> {
        let tasks = self.tasks.lock();
        let ch = tasks.get(task_id)?;
        let history: Vec<StoredEvent> = ch.history.iter().filter(|s| s.seq > after).cloned().collect();
        Some((history, ch.tx.subscribe(), ch.done))
    }

    pub fn task_exists(&self, task_id: &str) -> bool {
        self.tasks.lock().contains_key(task_id)
    }

    /// GC a finished task's replay buffer (`gcTask`).
    pub fn gc_task(&self, task_id: &str) {
        self.tasks.lock().remove(task_id);
    }

    // ─── side-channels ──────────────────────────────────────────────────────

    fn circuit_allows(&self) -> bool {
        let mut c = self.circuit.lock();
        if !c.is_open {
            return true;
        }
        if now_ms() - c.last_fail_at > CIRCUIT_RESET_MS {
            *c = CircuitState::default();
            true
        } else {
            false
        }
    }

    fn record_failure(&self) {
        let mut c = self.circuit.lock();
        c.consecutive_failures += 1;
        c.last_fail_at = now_ms();
        if c.consecutive_failures >= CIRCUIT_FAILURE_THRESHOLD {
            c.is_open = true;
        }
    }

    fn record_success(&self) {
        let mut c = self.circuit.lock();
        if c.consecutive_failures > 0 {
            *c = CircuitState::default();
        }
    }

    fn spawn_ingest(self: &Arc<Self>, task_id: &str, seq: i64, event: &Value) {
        let Some(ingest) = self.ingest.as_ref() else {
            return;
        };
        if !self.circuit_allows() {
            return;
        }
        let url = ingest.url.clone();
        let secret = ingest.secret.clone();
        let body = serde_json::json!({ "taskId": task_id, "seq": seq, "event": event });
        let bus = self.clone();
        let http = self.http.clone();
        tokio::spawn(async move {
            // Up to 5 attempts with exponential backoff (1s,2s,4s,… capped 30s).
            for attempt in 0..5u32 {
                let res = http
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .header("X-Agent-Auth", &secret)
                    .json(&body)
                    .send()
                    .await;
                match res {
                    Ok(r) if r.status().is_success() => {
                        bus.record_success();
                        return;
                    }
                    _ => {
                        let backoff = (1000u64 << attempt).min(30_000);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    }
                }
            }
            bus.record_failure();
        });
    }

    fn spawn_log_sink(&self, thread_id: Option<String>, task_id: &str, seq: i64, kind: &str, event: &Value) {
        let dir = match &thread_id {
            Some(t) => format!("{}/{}", self.log_dir, t),
            None => self.log_dir.clone(),
        };
        let detail = match kind {
            "status" => format!(" status={}", event.get("status").and_then(|v| v.as_str()).unwrap_or("?")),
            "error" => {
                let m = event.get("message").and_then(|v| v.as_str()).unwrap_or("");
                format!(" message={}", &m[..m.len().min(200)])
            }
            "done" => format!(" exitReason={}", event.get("exitReason").and_then(|v| v.as_str()).unwrap_or("?")),
            _ => String::new(),
        };
        let line = format!(
            "[{}] task={} seq={} kind={}{}\n",
            chrono::Utc::now().to_rfc3339(),
            task_id,
            seq,
            kind,
            detail
        );
        tokio::spawn(async move {
            let _ = tokio::fs::create_dir_all(&dir).await;
            let path = format!("{dir}/thread.log");
            use tokio::io::AsyncWriteExt;
            if let Ok(mut f) = tokio::fs::OpenOptions::new().create(true).append(true).open(&path).await {
                let _ = f.write_all(line.as_bytes()).await;
            }
        });
    }

    fn spawn_nats(&self, thread_id: Option<String>, user_id: Option<String>, task_id: &str, seq: i64, event: &Value) {
        let nats = self.nats.clone();
        let subject = self.subject.clone();
        let payload = serde_json::json!({
            "taskId": task_id,
            "threadId": thread_id,
            "userId": user_id,
            "seq": seq,
            "event": event,
        });
        let kind = event.get("kind").and_then(|k| k.as_str()).unwrap_or("event").to_string();
        tokio::spawn(async move {
            let envelope = MessageEnvelope::new(format!("execution.{kind}"), payload);
            nats.publish_event(&subject, &envelope).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use serde_json::json;

    fn bus() -> Arc<EventBus> {
        let cfg = Config::from_env(); // no NATS_URL in test env → publisher no-ops
        let nats = Arc::new(Nats::new(&cfg));
        let dir = std::env::temp_dir()
            .join(format!("fiducia-bus-test-{}", uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned();
        EventBus::new(None, None, dir, nats, "test.subject".into())
    }

    fn ev(bus: &Arc<EventBus>, seq: i64, kind: &str) {
        bus.emit(BusEvent {
            task_id: "t1".into(),
            thread_id: None,
            user_id: None,
            seq,
            event: json!({ "kind": kind }),
        });
    }

    #[tokio::test]
    async fn replays_history_dedupes_and_marks_done() {
        let bus = bus();
        ev(&bus, 0, "status");
        ev(&bus, 0, "status"); // duplicate seq — must be dropped
        ev(&bus, 1, "done");

        let (history, _rx, done) = bus.subscribe("t1", -1).unwrap();
        assert_eq!(history.len(), 2, "duplicate seq 0 deduped");
        assert_eq!(history[0].seq, 0);
        assert_eq!(history[1].seq, 1);
        assert!(done, "terminal event marks the task done");
    }

    #[tokio::test]
    async fn subscribe_filters_by_after_seq_for_resume() {
        let bus = bus();
        ev(&bus, 0, "status");
        ev(&bus, 1, "claude");
        ev(&bus, 2, "claude");
        let (history, _rx, _) = bus.subscribe("t1", 0).unwrap();
        assert_eq!(history.len(), 2, "only seq > 0 replayed");
        assert_eq!(history[0].seq, 1);
    }

    #[tokio::test]
    async fn live_subscriber_receives_new_events() {
        let bus = bus();
        ev(&bus, 0, "status");
        let (_history, mut rx, _) = bus.subscribe("t1", -1).unwrap();
        ev(&bus, 1, "claude");
        let live = rx.recv().await.unwrap();
        assert_eq!(live.seq, 1);
    }

    #[tokio::test]
    async fn gc_reclaims_task_buffer() {
        let bus = bus();
        ev(&bus, 0, "done");
        assert!(bus.task_exists("t1"));
        bus.gc_task("t1");
        assert!(!bus.task_exists("t1"));
        assert!(bus.subscribe("t1", -1).is_none());
    }

    #[tokio::test]
    async fn events_are_sanitized_before_replay() {
        let bus = bus();
        bus.emit(BusEvent {
            task_id: "t1".into(),
            thread_id: None,
            user_id: None,
            seq: 0,
            event: json!({ "kind": "stderr", "text": "leaked sk-ant-api03-SECRETKEY value" }),
        });
        let (history, _rx, _) = bus.subscribe("t1", -1).unwrap();
        let text = history[0].event["text"].as_str().unwrap();
        assert!(text.contains("[redacted-anthropic-key]"), "{text}");
        assert!(!text.contains("sk-ant-"));
    }
}

/// Recursively sanitize all string values in an event before it leaves.
fn sanitize_value(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(sanitize_event_text(s)),
        Value::Array(a) => Value::Array(a.iter().map(sanitize_value).collect()),
        Value::Object(o) => {
            Value::Object(o.iter().map(|(k, v)| (k.clone(), sanitize_value(v))).collect())
        }
        other => other.clone(),
    }
}
