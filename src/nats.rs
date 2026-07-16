//! NATS publisher. Live agent progress is high-volume and disposable, so it goes
//! out over **Core NATS**; the enveloped, durable execution-lifecycle stream is
//! published to **JetStream** (with a Core fallback when no stream is bound). An
//! external NATS instance is assumed; an unset `NATS_URL` degrades to a no-op.
//! Initial connection failures are retried on a bounded cadence so a broker that
//! starts after this service does not require a process restart.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_nats::jetstream;
use fiducia_messaging::{tenant_scoped_dedup_id, NatsPublisher, Publisher};
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::config::Config;
use crate::messaging::MessageEnvelope;

pub struct Nats {
    url: Option<String>,
    connection: Mutex<ConnectionState>,
    connect_attempts: AtomicU64,
    connect_failures: AtomicU64,
    jetstream_published: AtomicU64,
    core_published: AtomicU64,
    publish_failures: AtomicU64,
    serialization_failures: AtomicU64,
    unconfigured_skips: AtomicU64,
    unavailable_drops: AtomicU64,
}

#[derive(Default)]
struct ConnectionState {
    client: Option<async_nats::Client>,
    last_attempt: Option<Instant>,
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
    pub unconfigured_skips: u64,
    pub unavailable_drops: u64,
}

impl Nats {
    pub fn new(config: &Config) -> Self {
        Nats {
            url: config.nats_url.clone(),
            connection: Mutex::new(ConnectionState::default()),
            connect_attempts: AtomicU64::new(0),
            connect_failures: AtomicU64::new(0),
            jetstream_published: AtomicU64::new(0),
            core_published: AtomicU64::new(0),
            publish_failures: AtomicU64::new(0),
            serialization_failures: AtomicU64::new(0),
            unconfigured_skips: AtomicU64::new(0),
            unavailable_drops: AtomicU64::new(0),
        }
    }

    async fn client(&self) -> Option<async_nats::Client> {
        let url = self.url.as_ref()?;
        let mut connection = self.connection.lock().await;
        if let Some(client) = connection.client.as_ref() {
            return Some(client.clone());
        }
        if connection
            .last_attempt
            .is_some_and(|attempt| attempt.elapsed() < Duration::from_secs(5))
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
                // NATS URLs may contain userinfo credentials; never emit the
                // configured URL or transport error text.
                tracing::warn!(
                    retry_after_seconds = 5,
                    "NATS connect failed; delivery is degraded"
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
            unconfigured_skips: self.unconfigured_skips.load(Ordering::Relaxed),
            unavailable_drops: self.unavailable_drops.load(Ordering::Relaxed),
        }
    }

    pub fn metrics_text(&self) -> String {
        let s = self.snapshot();
        format!(
            concat!(
                "# HELP fiducia_agent_nats_connect_attempts_total NATS connection attempts.\n",
                "# TYPE fiducia_agent_nats_connect_attempts_total counter\n",
                "fiducia_agent_nats_connect_attempts_total {}\n",
                "# HELP fiducia_agent_nats_connect_failures_total Failed NATS connection attempts.\n",
                "# TYPE fiducia_agent_nats_connect_failures_total counter\n",
                "fiducia_agent_nats_connect_failures_total {}\n",
                "# HELP fiducia_agent_nats_publish_failures_total Failed NATS publishes after fallback.\n",
                "# TYPE fiducia_agent_nats_publish_failures_total counter\n",
                "fiducia_agent_nats_publish_failures_total {}\n",
                "# HELP fiducia_agent_nats_jetstream_published_total JetStream publishes acknowledged.\n",
                "# TYPE fiducia_agent_nats_jetstream_published_total counter\n",
                "fiducia_agent_nats_jetstream_published_total {}\n",
                "# HELP fiducia_agent_nats_core_published_total Core NATS publishes accepted.\n",
                "# TYPE fiducia_agent_nats_core_published_total counter\n",
                "fiducia_agent_nats_core_published_total {}\n",
                "# HELP fiducia_agent_nats_serialization_failures_total Invalid message envelopes rejected before publish.\n",
                "# TYPE fiducia_agent_nats_serialization_failures_total counter\n",
                "fiducia_agent_nats_serialization_failures_total {}\n",
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
            s.unconfigured_skips,
            s.unavailable_drops,
        )
    }

    fn record_no_client(&self) {
        if self.url.is_some() {
            self.unavailable_drops.fetch_add(1, Ordering::Relaxed);
        } else {
            self.unconfigured_skips.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Durable, enveloped lifecycle event → JetStream, Core-NATS fallback.
    pub async fn publish_event<T: Serialize>(&self, subject: &str, envelope: &MessageEnvelope<T>) {
        let Some(client) = self.client().await else {
            self.record_no_client();
            return;
        };
        let bytes = match envelope.encode() {
            Ok(bytes) => bytes,
            Err(error) => {
                self.serialization_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(subject, error = %error, "refusing to publish invalid NATS envelope");
                return;
            }
        };
        let dedup_id = tenant_scoped_dedup_id(envelope.tenant_id, &envelope.idempotency_key);
        let publisher = NatsPublisher::new(jetstream::new(client.clone()));
        if publisher.publish(subject, &dedup_id, &bytes).await.is_ok() {
            self.jetstream_published.fetch_add(1, Ordering::Relaxed);
            return;
        }

        tracing::warn!(
            subject,
            "JetStream publish was not acknowledged; using Core NATS fallback"
        );
        match client.publish(subject.to_string(), bytes.into()).await {
            Ok(()) => {
                self.core_published.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.publish_failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    subject,
                    "Core NATS fallback publish failed; event was not delivered"
                );
                self.invalidate_client().await;
            }
        }
    }

    /// Disposable live progress → Core NATS (at-most-once, low latency).
    pub async fn publish_live(&self, subject: &str, payload: &[u8]) {
        let Some(client) = self.client().await else {
            self.record_no_client();
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
                tracing::warn!(subject, "Core NATS live publish failed");
                self.invalidate_client().await;
            }
        }
    }
}

#[cfg(test)]
mod observability_tests {
    use super::*;

    fn bare(url: Option<&str>) -> Nats {
        Nats {
            url: url.map(String::from),
            connection: Mutex::new(ConnectionState::default()),
            connect_attempts: AtomicU64::new(0),
            connect_failures: AtomicU64::new(0),
            jetstream_published: AtomicU64::new(0),
            core_published: AtomicU64::new(0),
            publish_failures: AtomicU64::new(0),
            serialization_failures: AtomicU64::new(0),
            unconfigured_skips: AtomicU64::new(0),
            unavailable_drops: AtomicU64::new(0),
        }
    }

    fn envelope() -> MessageEnvelope<()> {
        MessageEnvelope::new("execution.test", (), "idem-observability")
    }

    /// The two historically-silent loss paths must be COUNTED: publishing with
    /// no NATS_URL is a deliberate no-op (unconfigured_skips), never a failure.
    #[tokio::test]
    async fn unconfigured_publishes_are_counted_as_skips() {
        let nats = bare(None);
        nats.publish_event("fiducia.executions.progress.v1", &envelope())
            .await;
        nats.publish_event("fiducia.executions.progress.v1", &envelope())
            .await;

        let snapshot = nats.snapshot();
        assert!(!snapshot.configured);
        assert_eq!(snapshot.unconfigured_skips, 2);
        assert_eq!(
            snapshot.unavailable_drops, 0,
            "unconfigured is not an outage"
        );
        assert_eq!(snapshot.connect_attempts, 0, "no URL, no dialing");
        assert!(nats
            .metrics_text()
            .contains("fiducia_agent_nats_unconfigured_skips_total 2\n"));
    }

    /// The live (Core NATS) path shares the same loss accounting as the
    /// durable path: an unconfigured publisher counts unconfigured_skips —
    /// never unavailable_drops — and dials nothing.
    #[tokio::test]
    async fn unconfigured_live_publishes_are_counted_as_skips() {
        let nats = bare(None);
        nats.publish_live("fiducia.executions.live.v1", b"progress")
            .await;
        nats.publish_live("fiducia.executions.live.v1", b"progress")
            .await;

        let snapshot = nats.snapshot();
        assert!(!snapshot.configured);
        assert_eq!(snapshot.unconfigured_skips, 2);
        assert_eq!(
            snapshot.unavailable_drops, 0,
            "unconfigured is not an outage"
        );
        assert_eq!(snapshot.core_published, 0, "nothing was actually published");
        assert_eq!(snapshot.connect_attempts, 0, "no URL, no dialing");
        assert!(nats
            .metrics_text()
            .contains("fiducia_agent_nats_unconfigured_skips_total 2\n"));
    }

    /// A CONFIGURED but unreachable broker is an outage: drops are counted as
    /// unavailable (not skips), and reconnection attempts are gated to the
    /// bounded cadence rather than dialing on every publish.
    #[tokio::test]
    async fn unreachable_broker_drops_are_counted_and_reconnects_are_gated() {
        // Port 1 refuses connections immediately.
        let nats = bare(Some("nats://127.0.0.1:1"));
        nats.publish_event("fiducia.executions.progress.v1", &envelope())
            .await;
        nats.publish_event("fiducia.executions.progress.v1", &envelope())
            .await;

        let snapshot = nats.snapshot();
        assert!(snapshot.configured);
        assert_eq!(
            snapshot.unavailable_drops, 2,
            "both events dropped, visibly"
        );
        assert_eq!(snapshot.unconfigured_skips, 0);
        assert_eq!(snapshot.connect_failures, snapshot.connect_attempts);
        assert_eq!(
            snapshot.connect_attempts, 1,
            "the second publish inside the 5s gate must not re-dial"
        );
    }
}
