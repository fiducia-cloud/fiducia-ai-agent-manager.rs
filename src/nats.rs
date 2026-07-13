//! NATS publisher. Live agent progress is high-volume and disposable, so it goes
//! out over **Core NATS**; the enveloped, durable execution-lifecycle stream is
//! published to **JetStream** (with a Core fallback when no stream is bound). An
//! external NATS instance is assumed; an unset `NATS_URL` degrades to a no-op.

use async_nats::jetstream;
use serde::Serialize;
use tokio::sync::OnceCell;

use crate::config::Config;
use crate::messaging::MessageEnvelope;

pub struct Nats {
    url: Option<String>,
    client: OnceCell<Option<async_nats::Client>>,
}

impl Nats {
    pub fn new(config: &Config) -> Self {
        Nats {
            url: config.nats_url.clone(),
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Option<&async_nats::Client> {
        self.client
            .get_or_init(|| async {
                let url = self.url.clone()?;
                match async_nats::connect(&url).await {
                    Ok(c) => {
                        // NATS URLs may contain userinfo credentials; never emit
                        // the configured URL or transport error text.
                        tracing::info!("connected to NATS");
                        Some(c)
                    }
                    Err(_) => {
                        tracing::warn!("NATS connect failed; events will no-op");
                        None
                    }
                }
            })
            .await
            .as_ref()
    }

    /// Durable, enveloped lifecycle event → JetStream, Core-NATS fallback.
    pub async fn publish_event<T: Serialize>(&self, subject: &str, envelope: &MessageEnvelope<T>) {
        let Some(client) = self.client().await else {
            return;
        };
        let Ok(bytes) = serde_json::to_vec(envelope) else {
            return;
        };
        let js = jetstream::new(client.clone());
        match js.publish(subject.to_string(), bytes.clone().into()).await {
            Ok(ack) => {
                if ack.await.is_err() {
                    let _ = client.publish(subject.to_string(), bytes.into()).await;
                }
            }
            Err(_) => {
                let _ = client.publish(subject.to_string(), bytes.into()).await;
            }
        }
    }

    /// Disposable live progress → Core NATS (at-most-once, low latency).
    pub async fn publish_live(&self, subject: &str, payload: &[u8]) {
        if let Some(client) = self.client().await {
            let _ = client
                .publish(subject.to_string(), payload.to_vec().into())
                .await;
        }
    }
}
