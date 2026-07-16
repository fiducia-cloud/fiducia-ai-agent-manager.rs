//! NATS publisher. Disposable live progress uses Core NATS. Execution lifecycle
//! events are written to a local outbox before JetStream publish and are removed
//! only after an acknowledgement. The persisted tenant-scoped deduplication id
//! makes retries after an ambiguous ACK or process restart safe.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use fiducia_messaging::{tenant_scoped_dedup_id, NatsPublisher, Publisher};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::Instant;
use uuid::Uuid;

use crate::config::Config;
use crate::messaging::MessageEnvelope;

const CONNECT_RETRY: Duration = Duration::from_secs(5);
const OUTBOX_RETRY: Duration = Duration::from_secs(5);

pub struct Nats {
    url: Option<String>,
    outbox_dir: PathBuf,
    max_attempts: u32,
    connection: Mutex<ConnectionState>,
    outbox_gate: Mutex<()>,
    connect_attempts: AtomicU64,
    connect_failures: AtomicU64,
    jetstream_published: AtomicU64,
    core_published: AtomicU64,
    publish_failures: AtomicU64,
    serialization_failures: AtomicU64,
    outbox_persist_failures: AtomicU64,
    outbox_pending: AtomicU64,
    outbox_deferred: AtomicU64,
    dead_lettered: AtomicU64,
    unconfigured_skips: AtomicU64,
    unavailable_drops: AtomicU64,
}

#[derive(Default)]
struct ConnectionState {
    client: Option<async_nats::Client>,
    last_attempt: Option<Instant>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct OutboxRecord {
    message_id: Uuid,
    subject: String,
    dedup_id: String,
    payload: Vec<u8>,
    attempts: u32,
    created_at_ms: i64,
    next_attempt_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryOutcome {
    Acknowledged,
    Unacknowledged,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadLetterSummary {
    pub message_id: Uuid,
    pub subject: String,
    pub dedup_id: String,
    pub attempts: u32,
    pub reason: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NatsSnapshot {
    pub configured: bool,
    pub connect_attempts: u64,
    pub connect_failures: u64,
    pub jetstream_published: u64,
    pub core_published: u64,
    pub publish_failures: u64,
    pub serialization_failures: u64,
    pub outbox_persist_failures: u64,
    pub outbox_pending: u64,
    pub outbox_deferred: u64,
    pub dead_lettered: u64,
    pub unconfigured_skips: u64,
    pub unavailable_drops: u64,
}

impl Nats {
    pub fn new(config: &Config) -> Self {
        Self::with_outbox(
            config.nats_url.clone(),
            PathBuf::from(&config.nats_outbox_dir),
            config.nats_outbox_max_attempts,
        )
    }

    fn with_outbox(url: Option<String>, outbox_dir: PathBuf, max_attempts: u32) -> Self {
        Nats {
            url,
            outbox_dir,
            max_attempts: max_attempts.max(1),
            connection: Mutex::new(ConnectionState::default()),
            outbox_gate: Mutex::new(()),
            connect_attempts: AtomicU64::new(0),
            connect_failures: AtomicU64::new(0),
            jetstream_published: AtomicU64::new(0),
            core_published: AtomicU64::new(0),
            publish_failures: AtomicU64::new(0),
            serialization_failures: AtomicU64::new(0),
            outbox_persist_failures: AtomicU64::new(0),
            outbox_pending: AtomicU64::new(0),
            outbox_deferred: AtomicU64::new(0),
            dead_lettered: AtomicU64::new(0),
            unconfigured_skips: AtomicU64::new(0),
            unavailable_drops: AtomicU64::new(0),
        }
    }

    /// Recover persisted lifecycle events and keep retrying them in the
    /// background. A configured publisher fails startup if its outbox cannot be
    /// opened; accepting new work without durable storage would lose events.
    pub async fn start(self: &Arc<Self>) -> io::Result<()> {
        if self.url.is_none() {
            return Ok(());
        }
        self.prepare_directories().await?;
        let pending = self.pending_records().await?;
        let dead_letters = self.dead_letter_records().await?;
        self.outbox_pending
            .store(pending.len() as u64, Ordering::Relaxed);
        self.dead_lettered
            .store(dead_letters.len() as u64, Ordering::Relaxed);
        self.drain_outbox().await;

        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(OUTBOX_RETRY);
            interval.tick().await;
            loop {
                interval.tick().await;
                let Some(nats) = weak.upgrade() else {
                    break;
                };
                nats.drain_outbox().await;
            }
        });
        Ok(())
    }

    async fn client(&self) -> Option<async_nats::Client> {
        let url = self.url.as_ref()?;
        let mut connection = self.connection.lock().await;
        if let Some(client) = connection.client.as_ref() {
            return Some(client.clone());
        }
        if connection
            .last_attempt
            .is_some_and(|attempt| attempt.elapsed() < CONNECT_RETRY)
        {
            return None;
        }

        connection.last_attempt = Some(Instant::now());
        self.connect_attempts.fetch_add(1, Ordering::Relaxed);
        match async_nats::connect(url).await {
            Ok(client) => {
                tracing::info!("connected to NATS");
                connection.client = Some(client.clone());
                Some(client)
            }
            Err(_) => {
                self.connect_failures.fetch_add(1, Ordering::Relaxed);
                // NATS URLs may contain userinfo credentials. Never emit the URL
                // or transport error text.
                tracing::warn!(
                    retry_after_seconds = CONNECT_RETRY.as_secs(),
                    "NATS connect failed; durable events remain in the outbox"
                );
                None
            }
        }
    }

    async fn invalidate_client(&self) {
        self.connection.lock().await.client = None;
    }

    pub fn snapshot(&self) -> NatsSnapshot {
        NatsSnapshot {
            configured: self.url.is_some(),
            connect_attempts: self.connect_attempts.load(Ordering::Relaxed),
            connect_failures: self.connect_failures.load(Ordering::Relaxed),
            jetstream_published: self.jetstream_published.load(Ordering::Relaxed),
            core_published: self.core_published.load(Ordering::Relaxed),
            publish_failures: self.publish_failures.load(Ordering::Relaxed),
            serialization_failures: self.serialization_failures.load(Ordering::Relaxed),
            outbox_persist_failures: self.outbox_persist_failures.load(Ordering::Relaxed),
            outbox_pending: self.outbox_pending.load(Ordering::Relaxed),
            outbox_deferred: self.outbox_deferred.load(Ordering::Relaxed),
            dead_lettered: self.dead_lettered.load(Ordering::Relaxed),
            unconfigured_skips: self.unconfigured_skips.load(Ordering::Relaxed),
            unavailable_drops: self.unavailable_drops.load(Ordering::Relaxed),
        }
    }

    pub fn metrics_text(&self) -> String {
        let s = self.snapshot();
        format!(
            concat!(
                "# TYPE fiducia_agent_nats_connect_attempts_total counter\n",
                "fiducia_agent_nats_connect_attempts_total {}\n",
                "# TYPE fiducia_agent_nats_connect_failures_total counter\n",
                "fiducia_agent_nats_connect_failures_total {}\n",
                "# TYPE fiducia_agent_nats_publish_failures_total counter\n",
                "fiducia_agent_nats_publish_failures_total {}\n",
                "# TYPE fiducia_agent_nats_jetstream_published_total counter\n",
                "fiducia_agent_nats_jetstream_published_total {}\n",
                "# TYPE fiducia_agent_nats_core_published_total counter\n",
                "fiducia_agent_nats_core_published_total {}\n",
                "# TYPE fiducia_agent_nats_serialization_failures_total counter\n",
                "fiducia_agent_nats_serialization_failures_total {}\n",
                "# TYPE fiducia_agent_nats_outbox_persist_failures_total counter\n",
                "fiducia_agent_nats_outbox_persist_failures_total {}\n",
                "# TYPE fiducia_agent_nats_outbox_pending gauge\n",
                "fiducia_agent_nats_outbox_pending {}\n",
                "# TYPE fiducia_agent_nats_outbox_deferred_total counter\n",
                "fiducia_agent_nats_outbox_deferred_total {}\n",
                "# TYPE fiducia_agent_nats_dead_lettered gauge\n",
                "fiducia_agent_nats_dead_lettered {}\n",
                "# TYPE fiducia_agent_nats_unconfigured_skips_total counter\n",
                "fiducia_agent_nats_unconfigured_skips_total {}\n",
                "# TYPE fiducia_agent_nats_unavailable_drops_total counter\n",
                "fiducia_agent_nats_unavailable_drops_total {}\n"
            ),
            s.connect_attempts,
            s.connect_failures,
            s.publish_failures,
            s.jetstream_published,
            s.core_published,
            s.serialization_failures,
            s.outbox_persist_failures,
            s.outbox_pending,
            s.outbox_deferred,
            s.dead_lettered,
            s.unconfigured_skips,
            s.unavailable_drops,
        )
    }

    /// Persist a durable lifecycle event before attempting JetStream. A failed
    /// or ambiguous publish remains queued; Core NATS is never used here.
    pub async fn publish_event<T: Serialize>(&self, subject: &str, envelope: &MessageEnvelope<T>) {
        if self.url.is_none() {
            self.unconfigured_skips.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let payload = match envelope.encode() {
            Ok(payload) => payload,
            Err(error) => {
                self.serialization_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(subject, error = %error, "refusing to queue invalid NATS envelope");
                return;
            }
        };
        let record = OutboxRecord {
            message_id: envelope.message_id,
            subject: subject.to_owned(),
            dedup_id: tenant_scoped_dedup_id(envelope.tenant_id, &envelope.idempotency_key),
            payload,
            attempts: 0,
            created_at_ms: envelope.created_at.timestamp_millis(),
            next_attempt_at_ms: 0,
        };

        let _guard = self.outbox_gate.lock().await;
        if let Err(error) = self.persist_record(&record).await {
            self.outbox_persist_failures.fetch_add(1, Ordering::Relaxed);
            tracing::error!(subject, error = %error, "failed to persist durable NATS event");
            return;
        }
        self.drain_locked().await;
    }

    /// Disposable live progress -> Core NATS (at-most-once, low latency).
    pub async fn publish_live(&self, subject: &str, payload: &[u8]) {
        let Some(client) = self.client().await else {
            if self.url.is_some() {
                self.unavailable_drops.fetch_add(1, Ordering::Relaxed);
            } else {
                self.unconfigured_skips.fetch_add(1, Ordering::Relaxed);
            }
            return;
        };
        match client
            .publish(subject.to_string(), payload.to_vec().into())
            .await
        {
            Ok(()) => {
                self.core_published.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.publish_failures.fetch_add(1, Ordering::Relaxed);
                self.unavailable_drops.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(subject, "Core NATS live publish failed");
                self.invalidate_client().await;
            }
        }
    }

    pub async fn dead_letters(&self) -> io::Result<Vec<DeadLetterSummary>> {
        Ok(self
            .dead_letter_records()
            .await?
            .into_iter()
            .map(|record| DeadLetterSummary {
                message_id: record.message_id,
                subject: record.subject,
                dedup_id: record.dedup_id,
                attempts: record.attempts,
                reason: "JetStream publish was not acknowledged",
            })
            .collect())
    }

    async fn drain_outbox(&self) {
        if self.url.is_none() {
            return;
        }
        let _guard = self.outbox_gate.lock().await;
        self.drain_locked().await;
    }

    async fn drain_locked(&self) {
        let records = match self.pending_records().await {
            Ok(records) => records,
            Err(error) => {
                self.outbox_persist_failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(error = %error, "failed to read durable NATS outbox");
                return;
            }
        };
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut due = Vec::new();
        for record in records {
            if record.attempts >= self.max_attempts {
                if let Err(error) = self.move_to_dead_letter(&record).await {
                    self.outbox_persist_failures.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(error = %error, "failed to quarantine NATS outbox event");
                    return;
                }
            } else if record.next_attempt_at_ms <= now_ms {
                due.push(record);
            }
        }
        if due.is_empty() {
            return;
        }

        let Some(client) = self.client().await else {
            self.outbox_deferred.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let publisher = NatsPublisher::new(jetstream::new(client));
        for record in due {
            let outcome = match publisher
                .publish(&record.subject, &record.dedup_id, &record.payload)
                .await
            {
                Ok(()) => DeliveryOutcome::Acknowledged,
                Err(_) => DeliveryOutcome::Unacknowledged,
            };
            if let Err(error) = self.finalize_delivery(record, outcome, now_ms).await {
                self.outbox_persist_failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(error = %error, "failed to update durable NATS outbox");
                return;
            }
            if outcome == DeliveryOutcome::Unacknowledged {
                self.publish_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("JetStream publish was not acknowledged; event remains durable");
                self.invalidate_client().await;
                return;
            }
        }
    }

    async fn finalize_delivery(
        &self,
        mut record: OutboxRecord,
        outcome: DeliveryOutcome,
        now_ms: i64,
    ) -> io::Result<()> {
        match outcome {
            DeliveryOutcome::Acknowledged => {
                tokio::fs::remove_file(self.pending_path(record.message_id)).await?;
                self.outbox_pending.fetch_sub(1, Ordering::Relaxed);
                self.jetstream_published.fetch_add(1, Ordering::Relaxed);
            }
            DeliveryOutcome::Unacknowledged => {
                record.attempts = record.attempts.saturating_add(1);
                record.next_attempt_at_ms = now_ms.saturating_add(OUTBOX_RETRY.as_millis() as i64);
                self.write_record(&self.pending_path(record.message_id), &record)
                    .await?;
                if record.attempts >= self.max_attempts {
                    self.move_to_dead_letter(&record).await?;
                }
            }
        }
        Ok(())
    }

    async fn persist_record(&self, record: &OutboxRecord) -> io::Result<bool> {
        self.prepare_directories().await?;
        let path = self.pending_path(record.message_id);
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let existing: OutboxRecord =
                    serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                if existing != *record {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "outbox message id collision",
                    ));
                }
                Ok(false)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.write_record(&path, record).await?;
                self.outbox_pending.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            Err(error) => Err(error),
        }
    }

    async fn move_to_dead_letter(&self, record: &OutboxRecord) -> io::Result<()> {
        let source = self.pending_path(record.message_id);
        let destination = self.dead_letter_path(record.message_id);
        if tokio::fs::try_exists(&destination).await? {
            tokio::fs::remove_file(&source).await?;
        } else {
            tokio::fs::rename(&source, &destination).await?;
        }
        self.outbox_pending.fetch_sub(1, Ordering::Relaxed);
        self.dead_lettered.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn prepare_directories(&self) -> io::Result<()> {
        tokio::fs::create_dir_all(&self.outbox_dir).await?;
        tokio::fs::create_dir_all(self.dead_letter_dir()).await
    }

    async fn write_record(&self, path: &Path, record: &OutboxRecord) -> io::Result<()> {
        let bytes = serde_json::to_vec(record).map_err(io::Error::other)?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("outbox.json");
        let temporary = path.with_file_name(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
        tokio::fs::write(&temporary, bytes).await?;
        tokio::fs::rename(temporary, path).await
    }

    async fn pending_records(&self) -> io::Result<Vec<OutboxRecord>> {
        self.read_records(&self.outbox_dir).await
    }

    async fn dead_letter_records(&self) -> io::Result<Vec<OutboxRecord>> {
        self.read_records(&self.dead_letter_dir()).await
    }

    async fn read_records(&self, directory: &Path) -> io::Result<Vec<OutboxRecord>> {
        let mut records = Vec::new();
        let mut entries = tokio::fs::read_dir(directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let bytes = tokio::fs::read(path).await?;
            records.push(serde_json::from_slice(&bytes).map_err(io::Error::other)?);
        }
        records.sort_by_key(|record: &OutboxRecord| (record.created_at_ms, record.message_id));
        Ok(records)
    }

    fn pending_path(&self, message_id: Uuid) -> PathBuf {
        self.outbox_dir.join(format!("{message_id}.json"))
    }

    fn dead_letter_dir(&self) -> PathBuf {
        self.outbox_dir.join("dead-letter")
    }

    fn dead_letter_path(&self, message_id: Uuid) -> PathBuf {
        self.dead_letter_dir().join(format!("{message_id}.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_outbox() -> PathBuf {
        std::env::temp_dir().join(format!("fiducia-nats-outbox-test-{}", Uuid::new_v4()))
    }

    fn bare(url: Option<&str>, outbox_dir: PathBuf, max_attempts: u32) -> Nats {
        Nats::with_outbox(url.map(String::from), outbox_dir, max_attempts)
    }

    fn envelope() -> MessageEnvelope<()> {
        MessageEnvelope::new("execution.test", (), "idem-observability")
    }

    fn record(envelope: &MessageEnvelope<()>) -> OutboxRecord {
        OutboxRecord {
            message_id: envelope.message_id,
            subject: "fiducia.executions.progress.v1".into(),
            dedup_id: tenant_scoped_dedup_id(envelope.tenant_id, &envelope.idempotency_key),
            payload: envelope.encode().unwrap(),
            attempts: 0,
            created_at_ms: envelope.created_at.timestamp_millis(),
            next_attempt_at_ms: 0,
        }
    }

    async fn cleanup(path: &Path) {
        if tokio::fs::try_exists(path).await.unwrap_or(false) {
            tokio::fs::remove_dir_all(path).await.unwrap();
        }
    }

    #[tokio::test]
    async fn unconfigured_publishes_are_counted_as_skips() {
        let path = test_outbox();
        let nats = bare(None, path.clone(), 3);
        nats.publish_event("fiducia.executions.progress.v1", &envelope())
            .await;
        nats.publish_event("fiducia.executions.progress.v1", &envelope())
            .await;

        let snapshot = nats.snapshot();
        assert!(!snapshot.configured);
        assert_eq!(snapshot.unconfigured_skips, 2);
        assert_eq!(snapshot.outbox_pending, 0);
        assert_eq!(snapshot.connect_attempts, 0);
        assert!(!tokio::fs::try_exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn unreachable_broker_defers_durable_events_without_dropping_them() {
        let path = test_outbox();
        let nats = bare(Some("nats://127.0.0.1:1"), path.clone(), 3);
        nats.publish_event("fiducia.executions.progress.v1", &envelope())
            .await;
        nats.publish_event("fiducia.executions.progress.v1", &envelope())
            .await;

        let snapshot = nats.snapshot();
        assert_eq!(snapshot.outbox_pending, 2);
        assert_eq!(snapshot.unavailable_drops, 0);
        assert_eq!(snapshot.outbox_deferred, 2);
        assert_eq!(snapshot.connect_attempts, 1, "reconnect attempts are gated");
        assert_eq!(nats.pending_records().await.unwrap().len(), 2);
        cleanup(&path).await;
    }

    #[tokio::test]
    async fn restart_recovers_pending_event_with_the_same_deduplication_id() {
        let path = test_outbox();
        let first = bare(Some("nats://127.0.0.1:1"), path.clone(), 3);
        let envelope = envelope();
        let expected = record(&envelope);
        first.persist_record(&expected).await.unwrap();
        drop(first);

        let restarted = Arc::new(bare(Some("nats://127.0.0.1:1"), path.clone(), 3));
        restarted.start().await.unwrap();
        let recovered = restarted.pending_records().await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].message_id, expected.message_id);
        assert_eq!(recovered[0].dedup_id, expected.dedup_id);
        assert_eq!(recovered[0].payload, expected.payload);
        cleanup(&path).await;
    }

    #[tokio::test]
    async fn unacknowledged_jetstream_publish_never_falls_back_to_core_nats() {
        let path = test_outbox();
        let nats = bare(Some("nats://configured"), path.clone(), 3);
        let persisted = record(&envelope());
        nats.persist_record(&persisted).await.unwrap();

        nats.finalize_delivery(persisted.clone(), DeliveryOutcome::Unacknowledged, 1_000)
            .await
            .unwrap();

        let pending = nats.pending_records().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].attempts, 1);
        assert_eq!(pending[0].dedup_id, persisted.dedup_id);
        assert_eq!(nats.snapshot().core_published, 0);
        assert_eq!(nats.snapshot().jetstream_published, 0);
        cleanup(&path).await;
    }

    #[tokio::test]
    async fn exhausted_event_moves_to_queryable_dead_letter_store() {
        let path = test_outbox();
        let nats = bare(Some("nats://configured"), path.clone(), 1);
        let persisted = record(&envelope());
        nats.persist_record(&persisted).await.unwrap();

        nats.finalize_delivery(persisted.clone(), DeliveryOutcome::Unacknowledged, 1_000)
            .await
            .unwrap();

        assert!(nats.pending_records().await.unwrap().is_empty());
        let dead_letters = nats.dead_letters().await.unwrap();
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].message_id, persisted.message_id);
        assert_eq!(dead_letters[0].dedup_id, persisted.dedup_id);
        assert_eq!(dead_letters[0].attempts, 1);
        assert_eq!(nats.snapshot().dead_lettered, 1);
        cleanup(&path).await;
    }
}
