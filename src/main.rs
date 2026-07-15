use std::collections::HashMap;
use std::sync::Arc;

use fiducia_ai_agent_manager::config::Config;
use fiducia_ai_agent_manager::control_plane::ControlPlane;
use fiducia_ai_agent_manager::event_bus::EventBus;
use fiducia_ai_agent_manager::nats::Nats;
use fiducia_ai_agent_manager::state::AppState;
use fiducia_ai_agent_manager::storage::LocalStorage;
use parking_lot::Mutex;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fiducia_telemetry::init("fiducia-ai-agent-manager");
    let result = run().await;
    fiducia_telemetry::shutdown();
    result
}

async fn run() -> anyhow::Result<()> {
    let config = Config::from_env();
    let instance_id = uuid::Uuid::new_v4().to_string();
    info!(
        host = %config.host,
        port = config.port,
        thread_id = ?config.thread_id,
        repo = ?config.repo_url,
        control_plane = config.control_plane_url.is_some(),
        nats = config.nats_url.is_some(),
        %instance_id,
        "starting fiducia-ai-agent-manager"
    );

    let nats = Arc::new(Nats::new(&config));
    let mut control_plane = ControlPlane::new(
        config.control_plane_url.as_deref(),
        config.control_plane_secret.as_deref(),
        config.fiducia_node_url.as_deref(),
        config.fiducia_node_internal_secret.as_deref(),
        &config.fiducia_node_org_id,
        instance_id.clone(),
    )
    .map_err(anyhow::Error::msg)?;
    // A configured control plane is authoritative. Startup fails closed unless
    // it returns the agent id later used for work ownership and transitions.
    control_plane
        .register(
            config.default_provider.as_str(),
            &[
                "code".into(),
                "git".into(),
                config.default_provider.as_str().into(),
            ],
        )
        .await
        .map_err(anyhow::Error::msg)?;

    let bus = EventBus::new(
        config.event_ingest_url.clone(),
        config.event_ingest_secret.clone(),
        config.log_dir.clone(),
        nats.clone(),
        config.nats_event_subject.clone(),
    );
    let storage = Arc::new(LocalStorage::new(config.outputs_dir.clone()));

    let state = AppState {
        config: Arc::new(config.clone()),
        bus,
        control_plane,
        nats,
        storage,
        sessions: Arc::new(Mutex::new(HashMap::new())),
        tasks: Arc::new(Mutex::new(HashMap::new())),
        started_at: chrono::Utc::now().to_rfc3339(),
        instance_id,
    };

    // Periodic GC of finished tasks + idle sessions (bounds memory; the Node
    // service GC'd finished tasks 1h after completion).
    {
        let gc_state = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                let reclaimed = gc_state.gc_finished(60 * 60 * 1000);
                if reclaimed > 0 {
                    info!(reclaimed, "GC'd finished tasks");
                }
            }
        });
    }

    let addr = std::net::SocketAddr::new(config.host, config.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "listening");

    let app = fiducia_ai_agent_manager::router(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}
