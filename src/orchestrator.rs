//! Task orchestration — the Rust port of `prepareSessionWorkspace` and `runTask`
//! from `server.ts`. This is the provider-agnostic spine: prepare/reuse the warm
//! branch, optionally satisfy a trivial edit deterministically, drive the agent
//! runner, then stage → commit → push (an external mutation gated on the
//! fiducia-node fencing token) and publish any output artifacts. PR creation is
//! an explicit action handled in [`crate::thread_ops`].

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::json;

use crate::agents::{build_agent_env, AgentRunOpts, AgentRunner, AgentRunnerEvent, CliRunner};
use crate::git;
use crate::prompt;
use crate::state::{AppState, TaskState, ThreadSession};
use crate::storage::{PublishOptions, StorageAdapter};

/// Get or create the session for a task's thread (`getOrCreateSession`).
pub fn get_or_create_session(
    state: &AppState,
    task_id: &str,
    thread_id: Option<&str>,
    user_id: Option<&str>,
    branch_hint: Option<&str>,
    title_hint: Option<&str>,
    prompt_hint: Option<&str>,
) -> Result<Arc<ThreadSession>, String> {
    let session_id = state.session_id(thread_id, task_id);
    let desired_branch = git::session_branch(
        &state.config.agent_branch_prefix,
        &session_id,
        branch_hint,
        title_hint,
        prompt_hint,
    )
    .map_err(|e| e.0)?;

    let mut sessions = state.sessions.lock();
    if let Some(existing) = sessions.get(&session_id) {
        return Ok(existing.clone());
    }
    let session = Arc::new(ThreadSession {
        session_id: session_id.clone(),
        user_id: user_id.map(str::to_string),
        workspace_path: state.config.workspace_repo.clone(),
        branch: desired_branch,
        queue: tokio::sync::Mutex::new(()),
        ready: tokio::sync::Mutex::new(false),
        task_ids: parking_lot::Mutex::new(Vec::new()),
        queued_task_ids: parking_lot::Mutex::new(Vec::new()),
        running_task_id: parking_lot::Mutex::new(None),
    });
    sessions.insert(session_id, session.clone());
    Ok(session)
}

/// Prepare (once) or reuse the session's warm workspace: fetch the base branch,
/// switch to (or create) the thread branch, and configure the git identity.
pub async fn prepare_session_workspace(
    state: &AppState,
    session: &Arc<ThreadSession>,
) -> Result<(), String> {
    let mut ready = session.ready.lock().await;
    if *ready {
        return Ok(());
    }
    let cwd = &session.workspace_path;

    if state.config.skip_boot_git_sync {
        git::configure_identity(cwd, &state.config.git_author_name, &state.config.git_author_email)
            .await
            .map_err(|e| e.0)?;
        *ready = true;
        return Ok(());
    }

    git::fetch_remote_branch(cwd, &state.config.base_branch, 1)
        .await
        .map_err(|e| e.0)?;

    let has_remote = git::remote_branch_exists(cwd, &session.branch).await.unwrap_or(false);
    let switch_source = if has_remote {
        if git::fetch_remote_branch(cwd, &session.branch, 1).await.is_err() {
            tracing::warn!(branch = %session.branch, "failed to fetch existing thread branch");
        }
        format!("origin/{}", session.branch)
    } else {
        format!("origin/{}", state.config.base_branch)
    };

    let current = git::current_branch(cwd).await;
    if current.as_deref() != Some(session.branch.as_str()) {
        let status = git::workspace_status(cwd).await.map_err(|e| e.0)?;
        if !status.trim().is_empty() {
            return Err(format!(
                "workspace has uncommitted changes while on {}; refusing to switch to {}",
                current.as_deref().unwrap_or("detached HEAD"),
                session.branch
            ));
        }
        git::sh_capture(
            "git",
            &["switch", "--discard-changes", "-C", &session.branch, &switch_source],
            cwd,
            git::TIMEOUT_GIT_QUICK,
        )
        .await
        .map_err(|e| e.0)?;
    }

    // Refuse to run on the parent branch (`assertSessionOnFeatureBranch`).
    if session.branch == state.config.base_branch {
        return Err(format!(
            "refusing to run {} on parent branch {}",
            session.session_id, state.config.base_branch
        ));
    }

    git::configure_identity(cwd, &state.config.git_author_name, &state.config.git_author_email)
        .await
        .map_err(|e| e.0)?;
    *ready = true;
    Ok(())
}

/// Run one task end-to-end. Emits sequenced events throughout; never panics the
/// caller — failures surface as `error` + `done{failed}` events.
pub async fn run_task(state: AppState, task: Arc<TaskState>) {
    // Serialize execution on the session queue.
    let _guard = task.session.queue.lock().await;
    *task.session.running_task_id.lock() = Some(task.task_id.clone());

    let result = run_task_inner(&state, &task).await;
    let exit_reason = match result {
        Ok(()) => "completed",
        Err(ref err) => {
            state.emit(&task, json!({ "kind": "error", "message": err }));
            "failed"
        }
    };
    if !task.is_finished() {
        task.finished.store(true, Ordering::SeqCst);
        *task.finished_at.lock() = Some(chrono::Utc::now().timestamp_millis());
        state.emit(
            &task,
            json!({ "kind": "done", "branch": task.branch, "exitReason": exit_reason }),
        );
    }
    if *task.session.running_task_id.lock() == Some(task.task_id.clone()) {
        *task.session.running_task_id.lock() = None;
    }
}

async fn run_task_inner(state: &AppState, task: &Arc<TaskState>) -> Result<(), String> {
    state.emit(task, json!({ "kind": "status", "status": "preparing", "message": "Preparing workspace" }));
    prepare_session_workspace(state, &task.session).await?;

    // Claim the backing work-item from the control plane (fencing token).
    let claim = match &task.thread_id {
        Some(tid) if state.control_plane.enabled() => {
            let c = state.control_plane.claim_work(tid, 30_000).await;
            if let Some(ref claim) = c {
                state.control_plane.transition(claim, "running").await;
                state.emit(task, json!({
                    "kind": "status", "status": "claimed",
                    "message": format!("Claimed work-item {} (fencing token {})", claim.work_item_id, claim.fencing_token),
                }));
            }
            c
        }
        _ => None,
    };

    // Optimistic deterministic edit (append-file), when the provider can edit.
    if task.provider.can_edit_workspace() {
        if let Some(edit) = prompt::parse_deterministic_append(&task.prompt) {
            match apply_deterministic_append(state, task, &edit).await {
                Ok(true) => { /* handled deterministically; still run agent for narration */ }
                Ok(false) => {}
                Err(e) => state.emit(task, json!({ "kind": "stderr", "text": format!("deterministic edit failed: {e}") })),
            }
        }
    }

    // Drive the agent runner.
    state.emit(task, json!({ "kind": "status", "status": "running", "message": "Agent running" }));
    run_agent(state, task).await?;

    // Stage + commit + push (external mutation → verify fencing token first).
    let cwd = &task.session.workspace_path;
    git::add_workspace_changes(cwd).await.map_err(|e| e.0)?;
    let status = git::workspace_status(cwd).await.map_err(|e| e.0)?;
    if !status.trim().is_empty() {
        if let Some(ref claim) = claim {
            let resource = format!("repository/{}/branch/{}", repo_display(state), task.branch);
            if !state.control_plane.verify_fencing_token(&resource, claim.fencing_token).await {
                return Err("stale fencing token; refusing to push".into());
            }
        }
        git::commit(cwd, &format!("agent: {}", first_line(&task.prompt))).await.map_err(|e| e.0)?;
        git::push_branch(cwd, &task.branch).await.map_err(|e| e.0)?;
        state.emit(task, json!({ "kind": "status", "status": "pushed", "message": format!("Pushed {}", task.branch) }));
    } else {
        state.emit(task, json!({ "kind": "status", "status": "no-changes", "message": "No workspace changes to commit" }));
    }

    // Publish output artifacts.
    publish_artifacts(state, task).await;

    if let Some(claim) = &claim {
        state.control_plane.transition(claim, "in_review").await;
    }
    Ok(())
}

async fn run_agent(state: &AppState, task: &Arc<TaskState>) -> Result<(), String> {
    let runner = CliRunner::for_provider(task.provider);
    let env = build_agent_env(task.provider);
    let opts = AgentRunOpts {
        prompt: task.prompt.clone(),
        cwd: task.session.workspace_path.clone(),
        env,
        timeout: state.config.agent_run_timeout,
        cancel: task.cancel.clone(),
    };
    let state_cl = state.clone();
    let task_cl = task.clone();
    let emit: crate::agents::Emit = Arc::new(move |ev: AgentRunnerEvent| {
        let event = match ev {
            AgentRunnerEvent::Claude(raw) => json!({ "kind": "claude", "raw": raw }),
            AgentRunnerEvent::Stderr(text) => json!({ "kind": "stderr", "text": text }),
            AgentRunnerEvent::Error(message) => json!({ "kind": "error", "message": message }),
        };
        state_cl.emit(&task_cl, event);
    });
    runner.run(opts, emit).await
}

/// Apply a deterministic append edit safely inside the repo (`applyDeterministicWorkspaceEdit`).
async fn apply_deterministic_append(
    state: &AppState,
    task: &Arc<TaskState>,
    edit: &prompt::AppendFileEdit,
) -> Result<bool, String> {
    let rel = safe_repo_relative(&task.session.workspace_path, &edit.relative_path)?;
    let target = std::path::Path::new(&task.session.workspace_path).join(&rel);
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| e.to_string())?;
    }
    let existing = tokio::fs::read_to_string(&target).await.unwrap_or_default();
    let prefix = if !existing.is_empty() && !existing.ends_with('\n') { "\n" } else { "" };
    let suffix = if edit.text.ends_with('\n') { "" } else { "\n" };
    let mut contents = existing;
    contents.push_str(prefix);
    contents.push_str(&edit.text);
    contents.push_str(suffix);
    tokio::fs::write(&target, contents).await.map_err(|e| e.to_string())?;
    state.emit(task, json!({
        "kind": "status",
        "status": "deterministic-edit:append-file",
        "message": format!("Appended {} character(s) to {}", edit.text.len(), rel),
    }));
    Ok(true)
}

/// Reject unsafe / out-of-repo / generated paths (`safeRepoRelativePath`).
fn safe_repo_relative(workspace: &str, raw: &str) -> Result<String, String> {
    let trimmed = raw.trim().trim_start_matches("./").trim_end_matches([')', ',', '.', ';', ':']);
    if trimmed.is_empty() || trimmed.contains('\0') || std::path::Path::new(trimmed).is_absolute() {
        return Err(format!("refusing unsafe append path: {raw}"));
    }
    let blocked = [".git", "node_modules", ".pnpm-store", ".next", ".turbo"];
    let normalized = std::path::Path::new(trimmed);
    for comp in normalized.components() {
        use std::path::Component;
        match comp {
            Component::ParentDir => return Err(format!("refusing append outside repo: {raw}")),
            Component::Normal(p) => {
                if blocked.iter().any(|b| p == std::ffi::OsStr::new(b)) {
                    return Err(format!("refusing append into generated path: {raw}"));
                }
            }
            _ => {}
        }
    }
    let workspace_root = std::path::Path::new(workspace);
    let resolved = workspace_root.join(normalized);
    let rel = resolved
        .strip_prefix(workspace_root)
        .map_err(|_| format!("refusing append outside repo: {raw}"))?;
    Ok(rel.to_string_lossy().replace('\\', "/"))
}

/// Scan `${OUTPUTS_DIR}/<taskId>/` and publish each file (`publishArtifact` loop).
async fn publish_artifacts(state: &AppState, task: &Arc<TaskState>) {
    let dir = std::path::Path::new(&state.config.outputs_dir).join(&task.task_id);
    let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        match state
            .storage
            .publish(PublishOptions {
                task_id: task.task_id.clone(),
                file_path: path.to_string_lossy().to_string(),
                filename: None,
            })
            .await
        {
            Ok(artifact) => state.emit(task, json!({ "kind": "artifact", "artifact": artifact })),
            Err(e) => state.emit(task, json!({ "kind": "stderr", "text": format!("artifact publish failed: {e}") })),
        }
    }
}

fn repo_display(state: &AppState) -> String {
    state.config.repo_url.clone().unwrap_or_else(|| "unknown-repo".into())
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(72).collect()
}

/// Build the strict env for a provider (re-exported for tests / callers).
pub fn agent_env(provider: crate::agents::AgentProvider) -> HashMap<String, String> {
    build_agent_env(provider)
}
