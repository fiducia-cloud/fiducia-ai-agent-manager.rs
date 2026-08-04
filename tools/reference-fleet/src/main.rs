use fiducia_reference_fleet::Coordinator;

fn main() {
    let mut coordinator = Coordinator::default();

    let original = coordinator.claim("task:42", "coder-a", 5).unwrap();
    let race_rejected = coordinator.claim("task:42", "coder-b", 5).is_err();
    coordinator.advance(5);
    let successor = coordinator.claim("task:42", "coder-b", 5).unwrap();
    let stale_commit_rejected =
        !coordinator.commit_if_current("task:42", "coder-a", original.fencing_token);

    let leader = coordinator
        .claim("leader:workspace-a", "planner-a", 2)
        .unwrap();
    let first_fire = coordinator.fire_cron_once(
        "leader:workspace-a",
        "planner-a",
        leader.fencing_token,
        "daily-review",
        86_400,
    );
    coordinator.advance(2);
    let replacement = coordinator
        .claim("leader:workspace-a", "planner-b", 2)
        .unwrap();
    let duplicate_fire_rejected = !coordinator.fire_cron_once(
        "leader:workspace-a",
        "planner-b",
        replacement.fencing_token,
        "daily-review",
        86_400,
    );

    assert!(coordinator.acquire_slot("browser", "research-a", 1, 10));
    let oversubscription_rejected = !coordinator.acquire_slot("browser", "research-b", 1, 10);
    assert!(coordinator.consume_quota("llm:org-a", 10, 10, 60));
    let quota_exhaustion_enforced = !coordinator.consume_quota("llm:org-a", 1, 10, 60);

    coordinator.heartbeat("reviewer-a", 1);
    coordinator.advance(1);
    let dead_worker_removed = !coordinator.worker_is_alive("reviewer-a");

    let config = coordinator.cas("fleet:model", 0, "primary").unwrap();
    let stale_cas_rejected = coordinator.cas("fleet:model", 0, "stale").is_err();
    assert!(coordinator.perform_side_effect_once("email:task-42", "send"));
    let duplicate_effect_rejected = !coordinator.perform_side_effect_once("email:task-42", "retry");

    println!("fiducia_reference_fleet_result=pass");
    println!("race_rejected={race_rejected}");
    println!("successor_fencing_token={}", successor.fencing_token);
    println!("stale_commit_rejected={stale_commit_rejected}");
    println!("first_cron_fire={first_fire}");
    println!("duplicate_cron_fire_rejected={duplicate_fire_rejected}");
    println!("oversubscription_rejected={oversubscription_rejected}");
    println!("quota_exhaustion_enforced={quota_exhaustion_enforced}");
    println!("dead_worker_removed={dead_worker_removed}");
    println!("config_revision={}", config.revision);
    println!("watch_events={}", coordinator.watch_events().len());
    println!("stale_cas_rejected={stale_cas_rejected}");
    println!("duplicate_effect_rejected={duplicate_effect_rejected}");
    println!("metrics={:?}", coordinator.metrics());
}
