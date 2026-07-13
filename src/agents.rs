//! Pluggable agent runners — the Rust analogue of `src/agents/`. One trait,
//! [`AgentRunner`], hides whether the work is done by Claude, Codex, Gemini, or
//! an OpenAI-compatible model, so the orchestration (worktree, git, push, PR,
//! outputs) is identical regardless of who is "doing the work".
//!
//! The Node service shipped seven runners (CLI + provider SDKs). Porting each
//! vendor SDK to Rust is out of scope; instead this crate ships one faithful
//! [`CliRunner`] that drives any of the CLI-shaped providers by spawning their
//! binary against the task worktree and forwarding stdout/stderr as events. The
//! provider taxonomy, selection order, and event shape are preserved so the SDK
//! runners can be added later without touching the orchestration.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

/// The provider taxonomy (`AgentProvider` union in `agents/types.ts`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentProvider {
    ClaudeCli,
    ClaudeSdk,
    GenericAiSdk,
    GeminiSdk,
    OpencodeAiSdk,
    OpenaiCodexCli,
    OpenaiSdk,
}

impl AgentProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentProvider::ClaudeCli => "claude-cli",
            AgentProvider::ClaudeSdk => "claude-sdk",
            AgentProvider::GenericAiSdk => "generic-ai-sdk",
            AgentProvider::GeminiSdk => "gemini-sdk",
            AgentProvider::OpencodeAiSdk => "opencode-ai-sdk",
            AgentProvider::OpenaiCodexCli => "openai-codex-cli",
            AgentProvider::OpenaiSdk => "openai-sdk",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "claude-cli" => AgentProvider::ClaudeCli,
            "claude-sdk" => AgentProvider::ClaudeSdk,
            "generic-ai-sdk" => AgentProvider::GenericAiSdk,
            "gemini-sdk" => AgentProvider::GeminiSdk,
            "opencode-ai-sdk" => AgentProvider::OpencodeAiSdk,
            "openai-codex-cli" => AgentProvider::OpenaiCodexCli,
            "openai-sdk" => AgentProvider::OpenaiSdk,
            _ => return None,
        })
    }

    /// The boot default: `AGENT_PROVIDER` env, else `generic-ai-sdk`
    /// (`resolveAgentProvider` with no override).
    pub fn from_env_default() -> Self {
        std::env::var("AGENT_PROVIDER")
            .ok()
            .and_then(|v| Self::parse(&v))
            .unwrap_or(AgentProvider::GenericAiSdk)
    }

    /// A short human label (`modelLabel` fallback).
    pub fn display_name(&self) -> String {
        self.as_str()
            .replace("-sdk", "")
            .replace("-cli", "")
            .replace('-', " ")
    }

    /// Only gemini-sdk is forbidden from editing the workspace
    /// (`providerCanEditWorkspace`).
    pub fn can_edit_workspace(&self) -> bool {
        !matches!(self, AgentProvider::GeminiSdk)
    }

    pub fn can_use_shell(&self) -> bool {
        matches!(
            self,
            AgentProvider::OpenaiSdk
                | AgentProvider::OpenaiCodexCli
                | AgentProvider::ClaudeSdk
                | AgentProvider::ClaudeCli
        )
    }
}

/// Resolve the provider for a task: a valid per-task override wins, else the
/// boot default (`resolveAgentProvider`).
pub fn resolve_agent_provider(
    per_task_override: Option<&str>,
    default: AgentProvider,
) -> AgentProvider {
    per_task_override
        .and_then(AgentProvider::parse)
        .unwrap_or(default)
}

/// The subset of event kinds a runner emits (`AgentRunnerEvent`). Status / done /
/// artifact events are owned by the orchestrator, not the runner.
#[derive(Debug, Clone)]
pub enum AgentRunnerEvent {
    /// A raw model/CLI event (JSON when the CLI speaks `--output-format json`,
    /// otherwise a `{ "text": <line> }` wrapper).
    Claude(Value),
    Stderr(String),
    Error(String),
}

/// Inputs to a single run.
pub struct AgentRunOpts {
    pub prompt: String,
    pub cwd: String,
    /// Strict env allowlist for the agent process; never inherit the full env.
    pub env: HashMap<String, String>,
    pub timeout: Duration,
    pub cancel: CancellationToken,
}

/// The emit callback the orchestrator supplies; a runner calls it per event.
pub type Emit = Arc<dyn Fn(AgentRunnerEvent) + Send + Sync>;

#[async_trait::async_trait]
pub trait AgentRunner: Send + Sync {
    fn id(&self) -> AgentProvider;
    async fn run(&self, opts: AgentRunOpts, emit: Emit) -> Result<(), String>;
}

/// A generic CLI runner. Spawns `program args… <prompt>` in the worktree with the
/// strict env, streams stdout as `claude` events and stderr as `stderr` events,
/// and resolves on a clean exit. Cancellation kills the child.
pub struct CliRunner {
    provider: AgentProvider,
    program: String,
    base_args: Vec<String>,
}

impl CliRunner {
    /// Build the CLI runner for a provider, honouring per-provider binary
    /// overrides (`AGENT_CLI_<PROVIDER>` env) with sensible defaults.
    pub fn for_provider(provider: AgentProvider) -> Self {
        let (default_program, base_args): (&str, Vec<String>) = match provider {
            AgentProvider::ClaudeCli | AgentProvider::ClaudeSdk => (
                "claude",
                vec![
                    "-p".into(),
                    "--output-format".into(),
                    "stream-json".into(),
                    "--verbose".into(),
                ],
            ),
            AgentProvider::OpenaiCodexCli => ("codex", vec!["exec".into(), "--json".into()]),
            _ => ("opencode", vec!["run".into()]),
        };
        let env_key = format!(
            "AGENT_CLI_{}",
            provider.as_str().to_uppercase().replace('-', "_")
        );
        let program = std::env::var(env_key).unwrap_or_else(|_| default_program.to_string());
        CliRunner {
            provider,
            program,
            base_args,
        }
    }
}

#[async_trait::async_trait]
impl AgentRunner for CliRunner {
    fn id(&self) -> AgentProvider {
        self.provider
    }

    async fn run(&self, opts: AgentRunOpts, emit: Emit) -> Result<(), String> {
        let mut cmd = Command::new(&self.program);
        cmd.args(&self.base_args)
            .arg(&opts.prompt)
            .current_dir(&opts.cwd)
            .env_clear()
            .envs(&opts.env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("failed to spawn {}: {e}", self.program))?;

        let stdout = child.stdout.take().ok_or("no stdout")?;
        let stderr = child.stderr.take().ok_or("no stderr")?;

        let emit_out = emit.clone();
        let out_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let value = serde_json::from_str::<Value>(&line)
                    .unwrap_or_else(|_| serde_json::json!({ "text": line }));
                emit_out(AgentRunnerEvent::Claude(value));
            }
        });

        let emit_err = emit.clone();
        let err_task = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                emit_err(AgentRunnerEvent::Stderr(line));
            }
        });

        let status = tokio::select! {
            _ = opts.cancel.cancelled() => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err("cancelled".into());
            }
            r = tokio::time::timeout(opts.timeout, child.wait()) => match r {
                Ok(Ok(status)) => status,
                Ok(Err(e)) => {
                    let _ = child.start_kill();
                    return Err(format!("agent process error: {e}"));
                }
                Err(_) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    return Err(format!("agent run exceeded {:?}", opts.timeout));
                }
            }
        };

        let _ = out_task.await;
        let _ = err_task.await;

        if status.success() {
            Ok(())
        } else {
            Err(format!(
                "{} exited with status {}",
                self.program,
                status
                    .code()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "signal".into())
            ))
        }
    }
}

/// Build the strict, per-provider env allowlist for an agent process
/// (`buildAgentEnv`). Only the keys a provider needs are forwarded; secrets like
/// GH_PAT / SUPABASE_* never leak into the child.
pub fn build_agent_env(provider: AgentProvider) -> HashMap<String, String> {
    let mut env = HashMap::new();
    // Always pass PATH and HOME so the CLI can find its runtime + config.
    for key in ["PATH", "HOME", "LANG", "TERM"] {
        if let Ok(v) = std::env::var(key) {
            env.insert(key.to_string(), v);
        }
    }
    let forward: &[&str] = match provider {
        AgentProvider::ClaudeCli | AgentProvider::ClaudeSdk => {
            &["ANTHROPIC_API_KEY", "ANTHROPIC_MODEL", "ANTHROPIC_BASE_URL"]
        }
        AgentProvider::GeminiSdk => &["GEMINI_API_KEY", "GEMINI_MODEL", "GOOGLE_API_KEY"],
        AgentProvider::OpenaiCodexCli | AgentProvider::OpenaiSdk => &[
            "OPENAI_API_KEY",
            "OPENAI_MODEL",
            "OPENAI_BASE_URL",
            "CODEX_MODEL",
        ],
        AgentProvider::OpencodeAiSdk => &["OPENCODE_API_KEY", "OPENCODE_MODELS", "OPENCODE_MODEL"],
        AgentProvider::GenericAiSdk => {
            &["GENERIC_AI_SDK_MODELS", "OPENAI_API_KEY", "OPENAI_BASE_URL"]
        }
    };
    for key in forward {
        if let Ok(v) = std::env::var(key) {
            env.insert(key.to_string(), v);
        }
    }
    env
}
