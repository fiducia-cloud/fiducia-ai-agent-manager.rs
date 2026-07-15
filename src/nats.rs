//! NATS publisher. The enveloped, durable execution-lifecycle stream is
//! published to **JetStream** (with a Core fallback when no stream is bound). An
//! external NATS instance is assumed; an unset `NATS_URL` degrades to a no-op.
//!
//! Reliability contract:
//!   * the connection self-heals — `retry_on_initial_connect` keeps dialing in
//!     the background, and an outright construction failure is retried on the
//!     next publish instead of being cached for the process lifetime;
//!   * every JetStream publish carries a tenant-scoped `Nats-Msg-Id`, so an
//!     ack-timeout retry or crash-window republish is deduplicated server-side
//!     within the stream's dedup window;
//!   * a durability downgrade (JetStream → Core fallback) is logged at `warn`,
//!     and a fallback that ALSO fails is logged too — an event can no longer
//!     vanish without a trace.
//!
//! NATS URLs may contain userinfo credentials; transport error text can echo
//! them, so failure logs name the failure class and never the error body.

use std::time::Duration;

use async_nats::jetstream;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::messaging::MessageEnvelope;

/// Upper bound on waiting for the JetStream publish acknowledgement before
/// treating the publish as failed and taking the fallback path.
const ACK_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Nats {
    url: Option<String>,
    client: Mutex<Option<async_nats::Client>>,
}

impl Nats {
    pub fn new(config: &Config) -> Self {
        Nats {
            url: config.nats_url.clone(),
            client: Mutex::new(None),
        }
    }

    /// The shared client, (re)connecting if none is cached. Once constructed,
    /// async-nats reconnects internally, so the cached client stays valid across
    /// broker restarts; if construction itself fails, the next publish retries
    /// instead of inheriting a permanently-dead publisher.
    async fn client(&self) -> Option<async_nats::Client> {
        let url = self.url.as_ref()?;
        let mut cached = self.client.lock().await;
        if let Some(client) = cached.as_ref() {
            return Some(client.clone());
        }
        match async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect(url)
            .await
        {
            Ok(client) => {
                tracing::info!("connected to NATS");
                *cached = Some(client.clone());
                Some(client)
            }
            Err(_) => {
                tracing::warn!("NATS client construction failed; will retry on the next publish");
                None
            }
        }
    }

    /// Durable, enveloped lifecycle event → JetStream, Core-NATS fallback.
    pub async fn publish_event<T: Serialize>(&self, subject: &str, envelope: &MessageEnvelope<T>) {
        let Some(client) = self.client().await else {
            return; // NATS_URL unset (documented no-op) or construction failed (logged)
        };
        let bytes = match serde_json::to_vec(envelope) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::error!(subject, %error, "event envelope failed to serialize; event dropped");
                return;
            }
        };

        // Tenant-scoped idempotent publish: JetStream drops a duplicate
        // `Nats-Msg-Id` within the stream's dedup window, which makes the
        // ack-timeout retry below (and any crash-window republish) collapse to
        // one stored message. Scoped by tenant so two tenants reusing the same
        // business key can never suppress each other's events.
        let dedup_id = format!(
            "{}:{}",
            envelope
                .tenant_id
                .map(|t| t.to_string())
                .unwrap_or_else(|| "global".to_string()),
            envelope.idempotency_key
        );
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", dedup_id.as_str());

        let js = jetstream::new(client.clone());
        let attempt = async {
            js.publish_with_headers(subject.to_string(), headers, bytes.clone().into())
                .await?
                .await
        };
        let failure_class = match tokio::time::timeout(ACK_TIMEOUT, attempt).await {
            Ok(Ok(_ack)) => return, // durably stored by the broker
            Ok(Err(_)) => "jetstream publish/ack failed (no stream bound?)",
            Err(_) => "jetstream ack timed out",
        };

        // Durability downgrade: deliver at-most-once over Core NATS rather than
        // not at all — but never silently.
        tracing::warn!(
            subject,
            message_id = %envelope.message_id,
            failure_class,
            "durable JetStream publish failed; falling back to core NATS (at-most-once)"
        );
        if client
            .publish(subject.to_string(), bytes.into())
            .await
            .is_err()
        {
            tracing::warn!(
                subject,
                message_id = %envelope.message_id,
                "core NATS fallback publish also failed; event dropped"
            );
        }
    }
}
