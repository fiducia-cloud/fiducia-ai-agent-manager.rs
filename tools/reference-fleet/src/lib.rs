//! Deterministic model of the coordination boundary used by a Fiducia agent fleet.
//!
//! The harness deliberately stores no prompts, transcripts, or long-term task history.
//! Those belong in a durable application store. This crate models only the decisions
//! that must be shared across workers: leases, fencing, finite capacity, quotas,
//! liveness, replicated cron ownership, compare-and-swap configuration, and
//! idempotency keys.

use std::collections::{hash_map::Entry, HashMap, HashSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lease {
    pub holder: String,
    pub fencing_token: u64,
    pub expires_at: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClaimError {
    HeldBy { holder: String, expires_at: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CasError {
    RevisionMismatch { expected: u64, actual: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvValue {
    pub revision: u64,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchEvent {
    pub key: String,
    pub revision: u64,
    pub value: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Metrics {
    pub claims_granted: u64,
    pub claims_rejected: u64,
    pub renewals_granted: u64,
    pub renewals_rejected: u64,
    pub stale_commits_rejected: u64,
    pub semaphore_denied: u64,
    pub quota_denied: u64,
    pub heartbeat_expired: u64,
    pub cron_duplicate_suppressed: u64,
    pub cas_conflicts: u64,
    pub side_effect_duplicates_suppressed: u64,
}

#[derive(Clone, Debug)]
struct Semaphore {
    capacity: usize,
    holders: HashMap<String, u64>,
}

#[derive(Clone, Debug)]
struct RateWindow {
    capacity: u64,
    window_seconds: u64,
    starts_at: u64,
    used: u64,
}

/// In-memory deterministic state machine used to inject fleet failure modes.
///
/// Production deployments replace this process-local model with fiducia-node. The
/// method boundaries intentionally mirror the decisions an application must make
/// against the distributed service.
#[derive(Clone, Debug, Default)]
pub struct Coordinator {
    now: u64,
    next_fencing_token: u64,
    leases: HashMap<String, Lease>,
    semaphores: HashMap<String, Semaphore>,
    rate_windows: HashMap<String, RateWindow>,
    heartbeats: HashMap<String, u64>,
    expired_heartbeats_recorded: HashSet<String>,
    cron_fires: HashSet<(String, u64)>,
    kv: HashMap<String, KvValue>,
    watch_events: Vec<WatchEvent>,
    side_effects: HashMap<String, String>,
    metrics: Metrics,
}

impl Coordinator {
    #[must_use]
    pub const fn now(&self) -> u64 {
        self.now
    }

    pub fn advance(&mut self, seconds: u64) {
        self.now = self.now.saturating_add(seconds);
    }

    pub fn claim(
        &mut self,
        resource: &str,
        holder: &str,
        ttl_seconds: u64,
    ) -> Result<Lease, ClaimError> {
        assert!(ttl_seconds > 0, "lease TTL must be positive");

        if let Some(active) = self
            .leases
            .get(resource)
            .filter(|lease| lease.expires_at > self.now)
        {
            self.metrics.claims_rejected += 1;
            return Err(ClaimError::HeldBy {
                holder: active.holder.clone(),
                expires_at: active.expires_at,
            });
        }

        self.next_fencing_token = self.next_fencing_token.saturating_add(1);
        let lease = Lease {
            holder: holder.to_owned(),
            fencing_token: self.next_fencing_token,
            expires_at: self.now.saturating_add(ttl_seconds),
        };
        self.leases.insert(resource.to_owned(), lease.clone());
        self.metrics.claims_granted += 1;
        Ok(lease)
    }

    pub fn renew(
        &mut self,
        resource: &str,
        holder: &str,
        fencing_token: u64,
        ttl_seconds: u64,
    ) -> bool {
        assert!(ttl_seconds > 0, "lease TTL must be positive");

        let Some(lease) = self.leases.get_mut(resource) else {
            self.metrics.renewals_rejected += 1;
            return false;
        };
        if lease.expires_at <= self.now
            || lease.holder != holder
            || lease.fencing_token != fencing_token
        {
            self.metrics.renewals_rejected += 1;
            return false;
        }

        lease.expires_at = self.now.saturating_add(ttl_seconds);
        self.metrics.renewals_granted += 1;
        true
    }

    pub fn commit_if_current(&mut self, resource: &str, holder: &str, fencing_token: u64) -> bool {
        let current = self.leases.get(resource).is_some_and(|lease| {
            lease.expires_at > self.now
                && lease.holder == holder
                && lease.fencing_token == fencing_token
        });
        if !current {
            self.metrics.stale_commits_rejected += 1;
        }
        current
    }

    pub fn acquire_slot(
        &mut self,
        pool: &str,
        holder: &str,
        capacity: usize,
        ttl_seconds: u64,
    ) -> bool {
        assert!(capacity > 0, "semaphore capacity must be positive");
        assert!(ttl_seconds > 0, "slot TTL must be positive");

        let semaphore = self
            .semaphores
            .entry(pool.to_owned())
            .or_insert_with(|| Semaphore {
                capacity,
                holders: HashMap::new(),
            });
        assert_eq!(
            semaphore.capacity, capacity,
            "capacity must be stable per pool"
        );
        semaphore.holders.retain(|_, expiry| *expiry > self.now);

        if let Some(expiry) = semaphore.holders.get_mut(holder) {
            *expiry = self.now.saturating_add(ttl_seconds);
            return true;
        }
        if semaphore.holders.len() >= semaphore.capacity {
            self.metrics.semaphore_denied += 1;
            return false;
        }
        semaphore
            .holders
            .insert(holder.to_owned(), self.now.saturating_add(ttl_seconds));
        true
    }

    pub fn consume_quota(
        &mut self,
        quota: &str,
        units: u64,
        capacity: u64,
        window_seconds: u64,
    ) -> bool {
        assert!(capacity > 0, "quota capacity must be positive");
        assert!(window_seconds > 0, "quota window must be positive");

        let window = self
            .rate_windows
            .entry(quota.to_owned())
            .or_insert(RateWindow {
                capacity,
                window_seconds,
                starts_at: self.now,
                used: 0,
            });
        assert_eq!(
            window.capacity, capacity,
            "capacity must be stable per quota"
        );
        assert_eq!(
            window.window_seconds, window_seconds,
            "window must be stable per quota"
        );
        if self.now >= window.starts_at.saturating_add(window.window_seconds) {
            window.starts_at = self.now;
            window.used = 0;
        }
        if units > window.capacity.saturating_sub(window.used) {
            self.metrics.quota_denied += 1;
            return false;
        }
        window.used += units;
        true
    }

    pub fn heartbeat(&mut self, worker: &str, ttl_seconds: u64) {
        assert!(ttl_seconds > 0, "heartbeat TTL must be positive");
        self.heartbeats
            .insert(worker.to_owned(), self.now.saturating_add(ttl_seconds));
        self.expired_heartbeats_recorded.remove(worker);
    }

    pub fn worker_is_alive(&mut self, worker: &str) -> bool {
        let alive = self
            .heartbeats
            .get(worker)
            .is_some_and(|expires_at| *expires_at > self.now);
        if !alive
            && self.heartbeats.contains_key(worker)
            && self.expired_heartbeats_recorded.insert(worker.to_owned())
        {
            self.metrics.heartbeat_expired += 1;
        }
        alive
    }

    pub fn fire_cron_once(
        &mut self,
        leader_resource: &str,
        leader: &str,
        fencing_token: u64,
        job: &str,
        scheduled_at: u64,
    ) -> bool {
        if !self.commit_if_current(leader_resource, leader, fencing_token) {
            return false;
        }
        if !self.cron_fires.insert((job.to_owned(), scheduled_at)) {
            self.metrics.cron_duplicate_suppressed += 1;
            return false;
        }
        true
    }

    pub fn cas(
        &mut self,
        key: &str,
        expected_revision: u64,
        value: &str,
    ) -> Result<KvValue, CasError> {
        let actual_revision = self.kv.get(key).map_or(0, |current| current.revision);
        if actual_revision != expected_revision {
            self.metrics.cas_conflicts += 1;
            return Err(CasError::RevisionMismatch {
                expected: expected_revision,
                actual: actual_revision,
            });
        }
        let next = KvValue {
            revision: actual_revision.saturating_add(1),
            value: value.to_owned(),
        };
        self.kv.insert(key.to_owned(), next.clone());
        self.watch_events.push(WatchEvent {
            key: key.to_owned(),
            revision: next.revision,
            value: next.value.clone(),
        });
        Ok(next)
    }

    #[must_use]
    pub fn watch_events(&self) -> &[WatchEvent] {
        &self.watch_events
    }

    pub fn perform_side_effect_once(&mut self, idempotency_key: &str, payload: &str) -> bool {
        match self.side_effects.entry(idempotency_key.to_owned()) {
            Entry::Occupied(_) => {
                self.metrics.side_effect_duplicates_suppressed += 1;
                false
            }
            Entry::Vacant(entry) => {
                entry.insert(payload.to_owned());
                true
            }
        }
    }

    #[must_use]
    pub const fn metrics(&self) -> &Metrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::{CasError, ClaimError, Coordinator};

    #[test]
    fn exactly_one_agent_wins_a_task_race() {
        let mut coordinator = Coordinator::default();
        let winner = coordinator.claim("task:42", "coder-a", 10).unwrap();
        assert_eq!(
            coordinator.claim("task:42", "coder-b", 10),
            Err(ClaimError::HeldBy {
                holder: "coder-a".into(),
                expires_at: 10,
            })
        );
        assert!(coordinator.commit_if_current("task:42", "coder-a", winner.fencing_token));
    }

    #[test]
    fn lease_renewal_extends_authority_but_cannot_resurrect_an_expired_claim() {
        let mut coordinator = Coordinator::default();
        let claim = coordinator.claim("task:42", "coder-a", 5).unwrap();
        coordinator.advance(4);
        assert!(coordinator.renew("task:42", "coder-a", claim.fencing_token, 5));
        coordinator.advance(4);
        assert!(coordinator.commit_if_current("task:42", "coder-a", claim.fencing_token));
        coordinator.advance(1);
        assert!(!coordinator.renew("task:42", "coder-a", claim.fencing_token, 5));
    }

    #[test]
    fn superseded_holder_is_rejected_by_fencing_token() {
        let mut coordinator = Coordinator::default();
        let stale = coordinator.claim("task:42", "coder-a", 5).unwrap();
        coordinator.advance(5);
        let current = coordinator.claim("task:42", "coder-b", 5).unwrap();
        assert!(current.fencing_token > stale.fencing_token);
        assert!(!coordinator.commit_if_current("task:42", "coder-a", stale.fencing_token));
        assert!(coordinator.commit_if_current("task:42", "coder-b", current.fencing_token));
    }

    #[test]
    fn supervisor_fails_over_only_after_expiry() {
        let mut coordinator = Coordinator::default();
        coordinator
            .claim("leader:workspace-a", "planner-a", 3)
            .unwrap();
        coordinator.advance(2);
        assert!(coordinator
            .claim("leader:workspace-a", "planner-b", 3)
            .is_err());
        coordinator.advance(1);
        assert_eq!(
            coordinator
                .claim("leader:workspace-a", "planner-b", 3)
                .unwrap()
                .holder,
            "planner-b"
        );
    }

    #[test]
    fn semaphore_capacity_is_shared_across_workers() {
        let mut coordinator = Coordinator::default();
        assert!(coordinator.acquire_slot("browser", "research-a", 2, 5));
        assert!(coordinator.acquire_slot("browser", "research-b", 2, 5));
        assert!(!coordinator.acquire_slot("browser", "research-c", 2, 5));
        coordinator.advance(5);
        assert!(coordinator.acquire_slot("browser", "research-c", 2, 5));
    }

    #[test]
    fn quota_is_shared_and_resets_at_the_window_boundary() {
        let mut coordinator = Coordinator::default();
        assert!(coordinator.consume_quota("llm:org-a", 6, 10, 60));
        assert!(coordinator.consume_quota("llm:org-a", 4, 10, 60));
        assert!(!coordinator.consume_quota("llm:org-a", 1, 10, 60));
        coordinator.advance(60);
        assert!(coordinator.consume_quota("llm:org-a", 10, 10, 60));
    }

    #[test]
    fn dead_worker_disappears_after_heartbeat_ttl() {
        let mut coordinator = Coordinator::default();
        coordinator.heartbeat("coder-a", 5);
        coordinator.advance(4);
        assert!(coordinator.worker_is_alive("coder-a"));
        coordinator.advance(1);
        assert!(!coordinator.worker_is_alive("coder-a"));
        assert_eq!(coordinator.metrics().heartbeat_expired, 1);
        assert!(!coordinator.worker_is_alive("coder-a"));
        assert_eq!(coordinator.metrics().heartbeat_expired, 1);
    }

    #[test]
    fn cron_fire_is_not_duplicated_across_leader_failover() {
        let mut coordinator = Coordinator::default();
        let first = coordinator
            .claim("leader:workspace-a", "planner-a", 2)
            .unwrap();
        assert!(coordinator.fire_cron_once(
            "leader:workspace-a",
            "planner-a",
            first.fencing_token,
            "daily-review",
            86_400
        ));
        coordinator.advance(2);
        let second = coordinator
            .claim("leader:workspace-a", "planner-b", 2)
            .unwrap();
        assert!(!coordinator.fire_cron_once(
            "leader:workspace-a",
            "planner-b",
            second.fencing_token,
            "daily-review",
            86_400
        ));
        assert_eq!(coordinator.metrics().cron_duplicate_suppressed, 1);
    }

    #[test]
    fn cas_rejects_stale_revision_and_emits_watch_event() {
        let mut coordinator = Coordinator::default();
        let first = coordinator.cas("fleet:model", 0, "gpt-primary").unwrap();
        assert_eq!(first.revision, 1);
        assert_eq!(
            coordinator.cas("fleet:model", 0, "stale-write"),
            Err(CasError::RevisionMismatch {
                expected: 0,
                actual: 1,
            })
        );
        let second = coordinator.cas("fleet:model", 1, "gpt-fallback").unwrap();
        assert_eq!(second.revision, 2);
        assert_eq!(coordinator.watch_events().len(), 2);
    }

    #[test]
    fn external_side_effect_requires_an_idempotency_key() {
        let mut coordinator = Coordinator::default();
        assert!(coordinator.perform_side_effect_once("email:task-42", "send"));
        assert!(!coordinator.perform_side_effect_once("email:task-42", "retry"));
        assert_eq!(coordinator.metrics().side_effect_duplicates_suppressed, 1);
    }
}
