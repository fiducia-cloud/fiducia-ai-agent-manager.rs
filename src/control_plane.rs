//! Client for the **fiducia-ai-agent-control-plane**.
//!
//! Per the operator directive, this AI agent worker is *governed* by the control
//! plane: it registers itself as an agent, claims the work-item backing a task
//! (receiving a fiducia-node **fencing token**), reports state transitions, and
//! posts breadcrumbs. The control plane is the authority for who may act; the
//! worker renews its exact work election through the whole mutation lifecycle
//! and immediately before git push. Manual PR and thread-mutation routes are
//! disabled while this governance is enabled.
//!
//! The control-plane API is plain HTTP/JSON (`POST /v1/agents`,
//! `POST /v1/work-items/{id}/claim`, `POST /v1/work-items/{id}/transition`, …);
//! we call it with reqwest. For direct fiducia-node needs — verifying a fencing
//! token out-of-band — the blocking `fiducia-client` SDK is available and driven
//! from async via `spawn_blocking`.

use std::{sync::Arc, time::Duration};

use fiducia_client::FiduciaClient;
use serde_json::{json, Value};

const CONTROL_PLANE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_PLANE_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const NODE_FENCING_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const CLAIM_LEASE_TTL_MS: u64 = 30_000;
pub const CLAIM_RENEW_INTERVAL: Duration = Duration::from_secs(10);

fn bounded_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(CONTROL_PLANE_CONNECT_TIMEOUT)
        .timeout(CONTROL_PLANE_REQUEST_TIMEOUT)
        .build()
        .expect("static control-plane HTTP client configuration must be valid")
}

fn node_client(base_url: &str, internal_secret: &str, org_id: &str) -> FiduciaClient {
    let mut client = FiduciaClient::internal(base_url, internal_secret, org_id);
    client.request_timeout = Some(NODE_FENCING_REQUEST_TIMEOUT);
    client
}

fn normalized(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn validate_node_org_id(org_id: &str) -> Result<(), String> {
    if org_id.len() > 128
        || org_id
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control())
    {
        return Err(
            "FIDUCIA_NODE_ORG_ID must be at most 128 bytes and contain no whitespace or control characters"
                .to_string(),
        );
    }
    Ok(())
}

fn response_error(operation: &str, status: reqwest::StatusCode, body: &str) -> String {
    let detail = body.trim();
    if detail.is_empty() {
        format!("control-plane {operation} failed with HTTP {status}")
    } else {
        format!("control-plane {operation} failed with HTTP {status}: {detail}")
    }
}

fn registered_agent_id(value: &Value) -> Result<String, String> {
    value
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| uuid::Uuid::parse_str(id).is_ok())
        .map(str::to_owned)
        .ok_or_else(|| "control-plane registration omitted a valid agent id".to_string())
}

fn claimed_fencing_token(value: &Value) -> Result<u64, String> {
    value
        .get("fencing_token")
        .and_then(Value::as_u64)
        .filter(|token| *token > 0)
        .ok_or_else(|| "control-plane claim omitted a nonzero fencing token".to_string())
}

/// Require the exact election ownership that the control plane minted for a
/// work item. `renewed:true` alone is insufficient: malformed middleware or a
/// future wire-shape drift must not authorize a push.
fn renewal_response_authorizes(value: &Value, agent_id: &str, token: u64) -> bool {
    let output = &value["result"]["output"];
    output["renewed"].as_bool() == Some(true)
        && output["leadership"]["leader"].as_str() == Some(agent_id)
        && output["leadership"]["fencing_token"].as_u64() == Some(token)
}

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
    instance_id: String,
    /// The authoritative id returned by `POST /v1/agents`. The locally generated
    /// instance id is only metadata and must never be used as a work owner.
    agent_id: Option<String>,
    /// Direct fiducia-node handle for renewing the exact `ai-work:<id>` election.
    /// It is mandatory whenever control-plane governance is enabled.
    node: Option<Arc<FiduciaClient>>,
}

impl ControlPlane {
    pub fn new(
        base_url: Option<&str>,
        internal_secret: Option<&str>,
        node_url: Option<&str>,
        node_internal_secret: Option<&str>,
        node_org_id: &str,
        agent_id: impl Into<String>,
    ) -> Result<Self, String> {
        let base_url = normalized(base_url);
        let internal_secret = normalized(internal_secret);
        let node_url = normalized(node_url);
        let node_internal_secret = normalized(node_internal_secret);
        let node_org_id = node_org_id.trim();

        if base_url.is_some() {
            if internal_secret.is_none() {
                return Err(
                    "FIDUCIA_CONTROL_PLANE_SECRET or FIDUCIA_INTERNAL_SECRET is required when control-plane governance is enabled"
                        .to_string(),
                );
            }
            if node_url.is_none() {
                return Err(
                    "FIDUCIA_NODE_URL is required when control-plane governance is enabled"
                        .to_string(),
                );
            }
            if node_internal_secret.is_none() {
                return Err(
                    "FIDUCIA_NODE_INTERNAL_SECRET or FIDUCIA_INTERNAL_SECRET is required when control-plane governance is enabled"
                        .to_string(),
                );
            }
            if node_org_id.is_empty() {
                return Err(
                    "FIDUCIA_NODE_ORG_ID is required when control-plane governance is enabled"
                        .to_string(),
                );
            }
            validate_node_org_id(node_org_id)?;
        }
        let instance_id = agent_id.into();
        Ok(ControlPlane {
            base: base_url.map(|b| b.trim_end_matches('/').to_string()),
            // Bound every control-plane call: `claim_work` is awaited inline in
            // the task path (while the session queue is held), so a hung control
            // plane must not stall the worker indefinitely.
            http: bounded_http_client(),
            internal_secret: internal_secret.map(str::to_string),
            instance_id,
            agent_id: None,
            node: match (
                node_url,
                node_internal_secret,
                normalized(Some(node_org_id)),
            ) {
                (Some(url), Some(secret), Some(org_id)) => {
                    Some(Arc::new(node_client(url, secret, org_id)))
                }
                _ => None,
            },
        })
    }

    pub fn enabled(&self) -> bool {
        self.base.is_some()
    }

    pub fn agent_id(&self) -> Option<&str> {
        self.agent_id.as_deref()
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

    /// Register this worker and retain the authoritative id returned by the
    /// control plane. When governance is configured, registration is mandatory:
    /// using the local instance UUID would violate the `ai_agents` foreign key
    /// and silently divorce later claims from the registered principal.
    pub async fn register(&mut self, model: &str, capabilities: &[String]) -> Result<(), String> {
        let Some(req) = self.req(reqwest::Method::POST, "/v1/agents") else {
            return Ok(());
        };
        let body = json!({
            "model": model,
            "capabilities": capabilities,
            "metadata": { "agentId": self.instance_id, "kind": "fiducia-ai-agent-manager" },
        });
        let response = req
            .json(&body)
            .send()
            .await
            .map_err(|error| format!("control-plane registration failed: {error}"))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| format!("could not read control-plane registration: {error}"))?;
        if !status.is_success() {
            return Err(response_error(
                "registration",
                status,
                &String::from_utf8_lossy(&bytes),
            ));
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid control-plane registration response: {error}"))?;
        let agent_id = registered_agent_id(&value)?;
        tracing::info!(%agent_id, instance_id = %self.instance_id, "registered with control plane");
        self.agent_id = Some(agent_id);
        Ok(())
    }

    /// Claim the work-item backing a task, receiving its fencing token
    /// (`POST /v1/work-items/{id}/claim`). Every refusal, transport error, and
    /// malformed response is fatal while governance is enabled.
    pub async fn claim_work(&self, work_item_id: &str, ttl_ms: u64) -> Result<WorkClaim, String> {
        let req = self
            .req(
                reqwest::Method::POST,
                &format!("/v1/work-items/{work_item_id}/claim"),
            )
            .ok_or_else(|| "control plane is not configured".to_string())?;
        let agent_id = self
            .agent_id()
            .ok_or_else(|| "control-plane agent is not registered".to_string())?;
        let body = json!({ "agent_id": agent_id, "ttl_ms": ttl_ms });
        let response = req
            .json(&body)
            .send()
            .await
            .map_err(|error| format!("control-plane work claim failed: {error}"))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| format!("could not read control-plane work claim: {error}"))?;
        if !status.is_success() {
            return Err(response_error(
                "work claim",
                status,
                &String::from_utf8_lossy(&bytes),
            ));
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid control-plane work claim response: {error}"))?;
        Ok(WorkClaim {
            work_item_id: work_item_id.to_string(),
            fencing_token: claimed_fencing_token(&value)?,
        })
    }

    /// Report a work-item state transition (`POST /v1/work-items/{id}/transition`).
    /// The control plane enforces the fencing token against the current lease and
    /// CAS's on generation, so the claim's `fencing_token` is sent with every
    /// transition — a stale worker's transition is rejected.
    pub async fn transition(&self, claim: &WorkClaim, status: &str) -> Result<(), String> {
        let Some(req) = self.req(
            reqwest::Method::POST,
            &format!("/v1/work-items/{}/transition", claim.work_item_id),
        ) else {
            return Err("control plane is not configured".to_string());
        };
        let agent_id = self
            .agent_id()
            .ok_or_else(|| "control-plane agent is not registered".to_string())?;
        let body = json!({
            "status": status,
            "agent_id": agent_id,
            "fencing_token": claim.fencing_token,
        });
        let response = req
            .json(&body)
            .send()
            .await
            .map_err(|error| format!("control-plane transition to {status} failed: {error}"))?;
        let response_status = response.status();
        if !response_status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(response_error("transition", response_status, &body));
        }
        Ok(())
    }

    /// Renew and verify the exact `ai-work:<work_item_id>` election created by
    /// the control plane. A repository lock is a different primitive and cannot
    /// prove that this registered agent still owns this work item.
    pub async fn renew_claim(&self, claim: &WorkClaim) -> Result<(), String> {
        let node = self
            .node
            .clone()
            .ok_or_else(|| "fiducia-node is unavailable for claim renewal".to_string())?;
        let agent_id = self
            .agent_id()
            .ok_or_else(|| "control-plane agent is not registered".to_string())?
            .to_string();
        let resource = format!("ai-work:{}", claim.work_item_id);
        let token = claim.fencing_token;
        let response = tokio::task::spawn_blocking({
            let agent_id = agent_id.clone();
            move || node.election_renew(&resource, &agent_id, token, Some(CLAIM_LEASE_TTL_MS))
        })
        .await
        .map_err(|error| format!("claim renewal task failed: {error}"))?
        .map_err(|error| format!("claim renewal request failed: {error:?}"))?;
        if !renewal_response_authorizes(&response, &agent_id, token) {
            return Err("stale or malformed work-item election; refusing mutation".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;

    #[test]
    fn governed_mode_requires_direct_node_authority() {
        assert!(ControlPlane::new(
            Some("http://cp.invalid"),
            Some("cp-secret"),
            None,
            Some("node-secret"),
            "fiducia-ai-control-plane",
            "agent-1",
        )
        .is_err());
        assert!(ControlPlane::new(
            Some("http://cp.invalid"),
            Some("cp-secret"),
            Some("http://node.invalid"),
            None,
            "fiducia-ai-control-plane",
            "agent-1",
        )
        .is_err());
        assert!(ControlPlane::new(
            Some("http://cp.invalid"),
            None,
            Some("http://node.invalid"),
            Some("node-secret"),
            "fiducia-ai-control-plane",
            "agent-1",
        )
        .is_err());
    }

    #[tokio::test]
    async fn verify_fails_closed_when_node_unreachable() {
        // Node configured but unreachable (connection refused) → transport error
        // → must fail CLOSED so a stale worker cannot push during an outage.
        let mut cp = ControlPlane::new(
            Some("http://cp.invalid"),
            Some("cp-secret"),
            Some("http://127.0.0.1:9"),
            Some("  node-secret  "),
            "  fiducia-ai-control-plane  ",
            "agent-1",
        )
        .unwrap();
        cp.agent_id = Some("11111111-1111-4111-8111-111111111111".to_string());
        assert!(cp
            .renew_claim(&WorkClaim {
                work_item_id: "22222222-2222-4222-8222-222222222222".to_string(),
                fencing_token: 7,
            })
            .await
            .is_err());
    }

    #[test]
    fn direct_node_client_has_a_request_deadline() {
        let cp = ControlPlane::new(
            Some("http://cp.invalid"),
            Some("cp-secret"),
            Some("http://node.invalid"),
            Some("node-secret"),
            "fiducia-ai-control-plane",
            "agent-1",
        )
        .unwrap();
        assert_eq!(
            cp.node.as_ref().and_then(|node| node.request_timeout),
            Some(NODE_FENCING_REQUEST_TIMEOUT)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn direct_node_renewal_is_authenticated_and_org_scoped() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let count = stream.read(&mut buffer).unwrap();
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            request_tx
                .send(String::from_utf8(request).unwrap())
                .unwrap();
            let body = r#"{"result":{"output":{"renewed":true,"leadership":{"leader":"11111111-1111-4111-8111-111111111111","fencing_token":7}}}}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });

        let mut cp = ControlPlane::new(
            Some("http://cp.invalid"),
            Some("cp-secret"),
            Some(&format!("http://{address}")),
            Some("  node-secret  "),
            "  fiducia-ai-control-plane  ",
            "agent-instance",
        )
        .unwrap();
        cp.agent_id = Some("11111111-1111-4111-8111-111111111111".to_string());
        cp.renew_claim(&WorkClaim {
            work_item_id: "22222222-2222-4222-8222-222222222222".to_string(),
            fencing_token: 7,
        })
        .await
        .unwrap();

        let request = request_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .to_ascii_lowercase();
        assert!(request.contains("x-fiducia-internal-auth: node-secret\r\n"));
        assert!(request.contains("x-fiducia-org-id: fiducia-ai-control-plane\r\n"));
        server.join().unwrap();
    }

    #[test]
    fn governed_mode_rejects_invalid_node_scope() {
        let build = |org| {
            ControlPlane::new(
                Some("http://cp.invalid"),
                Some("cp-secret"),
                Some("http://node.invalid"),
                Some("node-secret"),
                org,
                "agent-1",
            )
        };
        assert!(build("org with spaces").is_err());
        let oversized = "x".repeat(129);
        assert!(build(&oversized).is_err());
        assert!(build("valid-org").is_ok());
    }

    #[test]
    fn parses_authoritative_registration_and_claim_identity() {
        assert_eq!(
            registered_agent_id(&json!({
                "id": "11111111-1111-4111-8111-111111111111"
            }))
            .unwrap(),
            "11111111-1111-4111-8111-111111111111"
        );
        assert!(registered_agent_id(&json!({"id":"local-instance"})).is_err());
        assert_eq!(
            claimed_fencing_token(&json!({"fencing_token": 8})).unwrap(),
            8
        );
        assert!(claimed_fencing_token(&json!({"fencing_token": 0})).is_err());
    }

    #[test]
    fn election_renewal_requires_exact_candidate_and_token() {
        let agent = "11111111-1111-4111-8111-111111111111";
        let response = json!({
            "committed": true,
            "result": { "output": {
                "renewed": true,
                "leadership": { "leader": agent, "fencing_token": 8 }
            }}
        });
        assert!(renewal_response_authorizes(&response, agent, 8));
        assert!(!renewal_response_authorizes(&response, "other", 8));
        assert!(!renewal_response_authorizes(&response, agent, 9));
        assert!(!renewal_response_authorizes(
            &json!({"result":{"output":{"renewed":true}}}),
            agent,
            8
        ));
    }
}
