//! Environment-driven configuration — the Rust analogue of the `config` object
//! in the Node.js `server.ts`. Every field is read once at boot with the same
//! default and env-var name as the original where one exists.

use std::net::IpAddr;
use std::time::Duration;

use crate::agents::AgentProvider;

#[derive(Debug, Clone)]
pub struct Config {
    pub host: IpAddr,
    pub port: u16,
    /// The warm, configured git checkout every task on this thread shares.
    pub workspace_repo: String,
    pub repo_url: Option<String>,
    /// Per-thread pods set this; repo-scoped warm workers leave it unset.
    pub thread_id: Option<String>,
    pub thread_title: Option<String>,
    /// `thread` (pinned to one thread) or `repo` (any thread in the repo).
    pub worker_bind_mode: BindMode,
    pub user_id: Option<String>,
    pub outputs_dir: String,
    pub default_storage_provider: String,
    pub agent_run_timeout: Duration,
    pub base_branch: String,
    pub agent_branch_prefix: String,
    pub default_provider: AgentProvider,
    /// Shared secret for server-to-server auth (`X-Server-Auth`).
    pub server_auth_secret: Option<String>,
    /// Vercel-style event ingest endpoint (optional).
    pub event_ingest_url: Option<String>,
    pub event_ingest_secret: Option<String>,
    pub nats_url: Option<String>,
    pub nats_event_subject: String,
    /// The fiducia-ai-agent-control-plane base URL. The worker registers here,
    /// claims work-items (fencing tokens), and reports transitions.
    pub control_plane_url: Option<String>,
    /// Optional fiducia-node URL for direct fencing-token verification.
    pub fiducia_node_url: Option<String>,
    /// Shared secret presented to the control plane (`x-internal-auth`) on
    /// mutating calls (register / claim / transition).
    pub control_plane_secret: Option<String>,
    pub log_dir: String,
    pub processed_tasks_dir: String,
    pub idle_timeout: Duration,
    pub git_author_name: String,
    pub git_author_email: String,
    pub skip_boot_git_sync: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindMode {
    Thread,
    Repo,
}

fn env_opt(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}
fn env_or(key: &str, default: &str) -> String {
    env_opt(key).unwrap_or_else(|| default.to_string())
}
fn env_num<T: std::str::FromStr>(key: &str, default: T) -> T {
    env_opt(key).and_then(|v| v.parse().ok()).unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        let thread_id = env_opt("REMOTE_DEV_THREAD_ID").or_else(|| env_opt("THREAD_ID"));
        let worker_bind_mode = match env_opt("WORKER_BIND_MODE").as_deref() {
            Some("thread") => BindMode::Thread,
            Some("repo") => BindMode::Repo,
            _ if thread_id.is_some() => BindMode::Thread,
            _ => BindMode::Repo,
        };
        let log_dir = env_or("LOG_DIR", "/tmp/convos");
        Config {
            host: env_or("HOST", "0.0.0.0").parse().unwrap_or_else(|_| "0.0.0.0".parse().unwrap()),
            port: env_num("PORT", 8080),
            workspace_repo: env_or("WORKSPACE_REPO", "/home/node/workspace/repo"),
            repo_url: env_opt("DD_REPO_URL").or_else(|| env_opt("FIDUCIA_REPO_URL")),
            thread_id,
            thread_title: env_opt("REMOTE_DEV_THREAD_TITLE").or_else(|| env_opt("THREAD_TITLE")),
            worker_bind_mode,
            user_id: env_opt("USER_ID"),
            outputs_dir: env_or("OUTPUTS_DIR", "/home/node/workspace/outputs"),
            default_storage_provider: env_or("DEFAULT_STORAGE_PROVIDER", "local"),
            agent_run_timeout: Duration::from_millis(env_num("AGENT_RUN_TIMEOUT_MS", 2 * 60 * 60_000)),
            base_branch: env_or("BASE_BRANCH", "dev"),
            agent_branch_prefix: env_or("AGENT_BRANCH_PREFIX", "agent/k8s/openai-5.5"),
            default_provider: AgentProvider::from_env_default(),
            server_auth_secret: env_opt("SERVER_AUTH_SECRET"),
            event_ingest_url: env_opt("EVENT_INGEST_URL"),
            event_ingest_secret: env_opt("EVENT_INGEST_SECRET"),
            nats_url: env_opt("NATS_URL"),
            nats_event_subject: env_or("NATS_EVENT_SUBJECT", "fiducia.executions.progress.v1"),
            control_plane_url: env_opt("CONTROL_PLANE_URL").or_else(|| env_opt("FIDUCIA_CONTROL_PLANE_URL")),
            fiducia_node_url: env_opt("FIDUCIA_NODE_URL"),
            control_plane_secret: env_opt("FIDUCIA_CONTROL_PLANE_SECRET")
                .or_else(|| env_opt("FIDUCIA_INTERNAL_SECRET")),
            processed_tasks_dir: env_opt("PROCESSED_TASKS_DIR")
                .unwrap_or_else(|| format!("{log_dir}/processed-tasks")),
            log_dir,
            idle_timeout: Duration::from_millis(env_num("IDLE_TIMEOUT_MS", 30 * 60 * 1000)),
            git_author_name: env_or("GIT_AUTHOR_NAME", "Fiducia Agent"),
            git_author_email: env_or("GIT_AUTHOR_EMAIL", "agent@fiducia.cloud"),
            skip_boot_git_sync: env_opt("SKIP_BOOT_GIT_SYNC").as_deref() == Some("true"),
        }
    }
}
