//! Client for the **fiducia-ai-agent-control-plane**.
//!
//! Per the operator directive, this AI agent worker is *governed* by the control
//! plane: it registers itself as an agent, claims the work-item backing a task
//! (receiving a fiducia-node **fencing token**), reports state transitions, and
//! posts breadcrumbs. The control plane is the authority for who may act; the
//! worker verifies its fencing token before any external mutation (git push,
//! PR).
//!
//! The control-plane API is plain HTTP/JSON (`POST /v1/agents`,
//! `POST /v1/work-items/{id}/claim`, `POST /v1/work-items/{id}/transition`, …);
//! we call it with reqwest. For direct fiducia-node needs — verifying a fencing
//! token out-of-band — the blocking `fiducia-client` SDK is available and driven
//! from async via `spawn_blocking`.

use std::sync::Arc;

use fiducia_client::FiduciaClient;
use serde_json::{json, Value};

/// A claimed work-item lease: the fencing token authorizing this worker to act.
#[derive(Debug, Clone)]
pub struct WorkClaim {
    pub work_item_id: String,
    pub fencing_token: u64,
}

#[derive(Clone)]
pub struct ControlPlane {
    base: Option<String>,
    http: reqwest::Client,
    /// Shared secret presented as `x-internal-auth` on control-plane mutations.
    internal_secret: Option<String>,
    agent_id: String,
    /// Optional direct fiducia-node handle for fencing-token verification.
    node: Option<Arc<FiduciaClient>>,
}

impl ControlPlane {
    pub fn new(
        base_url: Option<&str>,
        internal_secret: Option<&str>,
        node_url: Option<&str>,
        agent_id: impl Into<String>,
    ) -> Self {
        ControlPlane {
            base: base_url.map(|b| b.trim_end_matches('/').to_string()),
            http: reqwest::Client::new(),
            internal_secret: internal_secret.map(str::to_string),
            agent_id: agent_id.into(),
            node: node_url.map(|u| Arc::new(FiduciaClient::new(u))),
        }
    }

    pub fn enabled(&self) -> bool {
        self.base.is_some()
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    fn req(&self, method: reqwest::Method, path: &str) -> Option<reqwest::RequestBuilder> {
        let base = self.base.as_ref()?;
        let mut b = self.http.request(method, format!("{base}{path}"));
        // The control plane authenticates internal callers via `x-internal-auth`.
        if let Some(secret) = &self.internal_secret {
            b = b.header("x-internal-auth", secret);
        }
        Some(b)
    }

    /// Register this worker as an agent (`POST /v1/agents`). Best-effort: a
    /// control-plane outage must not stop the worker from serving tasks.
    pub async fn register(&self, model: &str, capabilities: &[String]) {
        let Some(req) = self.req(reqwest::Method::POST, "/v1/agents") else {
            return;
        };
        let body = json!({
            "model": model,
            "capabilities": capabilities,
            "metadata": { "agentId": self.agent_id, "kind": "fiducia-ai-agent-manager" },
        });
        match req.json(&body).send().await {
            Ok(r) if r.status().is_success() => {
                tracing::info!(agent_id = %self.agent_id, "registered with control plane")
            }
            Ok(r) => tracing::warn!(status = %r.status(), "control-plane register non-2xx"),
            Err(e) => tracing::warn!(error = %e, "control-plane register failed"),
        }
    }

    /// Claim the work-item backing a task, receiving its fencing token
    /// (`POST /v1/work-items/{id}/claim`). Returns `None` when the control plane
    /// is not configured (single-node/dev) or the claim is refused.
    pub async fn claim_work(&self, work_item_id: &str, ttl_ms: u64) -> Option<WorkClaim> {
        let req = self.req(
            reqwest::Method::POST,
            &format!("/v1/work-items/{work_item_id}/claim"),
        )?;
        let body = json!({ "agent_id": self.agent_id, "ttl_ms": ttl_ms });
        match req.json(&body).send().await {
            Ok(r) if r.status().is_success() => {
                let v: Value = r.json().await.ok()?;
                let token = v.get("fencing_token").and_then(|t| t.as_u64())?;
                Some(WorkClaim {
                    work_item_id: work_item_id.to_string(),
                    fencing_token: token,
                })
            }
            Ok(r) => {
                tracing::warn!(status = %r.status(), work_item_id, "work claim refused");
                None
            }
            Err(e) => {
                tracing::warn!(error = %e, work_item_id, "work claim failed");
                None
            }
        }
    }

    /// Report a work-item state transition (`POST /v1/work-items/{id}/transition`).
    /// The control plane enforces the fencing token against the current lease and
    /// CAS's on generation, so the claim's `fencing_token` is sent with every
    /// transition — a stale worker's transition is rejected.
    pub async fn transition(&self, claim: &WorkClaim, status: &str) {
        let Some(req) = self.req(
            reqwest::Method::POST,
            &format!("/v1/work-items/{}/transition", claim.work_item_id),
        ) else {
            return;
        };
        let body = json!({
            "status": status,
            "agent_id": self.agent_id,
            "fencing_token": claim.fencing_token,
        });
        if let Err(e) = req.json(&body).send().await {
            tracing::warn!(error = %e, work_item_id = %claim.work_item_id, status, "transition failed");
        }
    }

    /// Verify a fencing token still authorizes acting on `resource` before an
    /// external mutation. Uses fiducia-node directly when available; otherwise
    /// trusts the control-plane-issued token (single-node fallback).
    ///
    /// Fail-closed: when fiducia-node *is* configured but we cannot get a
    /// definitive answer (transport failure or a 5xx), we refuse the mutation
    /// rather than risk a stale worker pushing. A 4xx (e.g. 404 "no such lock")
    /// is a definitive answer that nothing supersedes this token, so it is
    /// allowed — that preserves the normal path where the branch is not tracked
    /// as a direct fiducia-node lock.
    pub async fn verify_fencing_token(&self, resource: &str, token: u64) -> bool {
        let Some(node) = self.node.clone() else {
            return true;
        };
        let resource = resource.to_string();
        // fiducia-node exposes the current lock holder; a token below the current
        // fencing token means this worker is stale.
        tokio::task::spawn_blocking(move || match node.lock_get(&resource) {
            Ok(v) => v
                .get("holder")
                .and_then(|h| h.get("fencing_token"))
                .and_then(|t| t.as_u64())
                .map(|current| token >= current)
                .unwrap_or(true),
            // Node answered definitively (4xx, e.g. lock not found) → nothing
            // supersedes this token.
            Err(fiducia_client::Error::Http { status, .. }) if status < 500 => true,
            // Node unreachable/unhealthy (transport error or 5xx) → cannot confirm
            // authority; fail closed.
            Err(_) => false,
        })
        .await
        // The verification task panicked → fail closed.
        .unwrap_or(false)
    }
}
