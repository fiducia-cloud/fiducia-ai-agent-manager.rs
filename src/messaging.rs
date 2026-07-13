//! The standard fiducia message envelope (see the `messaging-architecture`
//! project note). Identical to the lambda service's envelope: NATS carries
//! delivery, fiducia-node carries authority, and every publication is wrapped so
//! consumers get correlation, idempotency, tracing, and an optional fencing
//! token for messages that authorize an external mutation (git push, PR).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelope<T> {
    pub message_id: Uuid,
    pub message_type: String,
    pub schema_version: u32,
    pub correlation_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<Uuid>,
    pub idempotency_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_parent: Option<String>,
    pub payload: T,
}

impl<T> MessageEnvelope<T> {
    pub fn new(message_type: impl Into<String>, payload: T) -> Self {
        let id = Uuid::new_v4();
        MessageEnvelope {
            message_id: id,
            message_type: message_type.into(),
            schema_version: 1,
            correlation_id: id,
            causation_id: None,
            tenant_id: None,
            workflow_id: None,
            execution_id: None,
            idempotency_key: id.to_string(),
            fencing_token: None,
            created_at: chrono::Utc::now(),
            expires_at: None,
            trace_parent: None,
            payload,
        }
    }

    pub fn with_correlation(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = correlation_id;
        self
    }

    pub fn with_fencing_token(mut self, token: u64) -> Self {
        self.fencing_token = Some(token);
        self
    }
}
