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
use crate::control_plane::{WorkClaim, CLAIM_LEASE_TTL_MS, CLAIM_RENEW_INTERVAL};
use crate::git;
use crate::prompt;
use crate::state::{AppState, TaskState, ThreadSession};
use crate::storage::{PublishOptions, StorageAdapter};
use tokio_util::sync::CancellationToken;

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
    prepare_session_workspace_inner(state, session, None).await
}

async fn prepare_session_workspace_with_cancel(
    state: &AppState,
    session: &Arc<ThreadSession>,
    cancel: &CancellationToken,
) -> Result<(), String> {
    prepare_session_workspace_inner(state, session, Some(cancel)).await
}

async fn prepare_session_workspace_inner(
    state: &AppState,
    session: &Arc<ThreadSession>,
    cancel: Option<&CancellationToken>,
) -> Result<(), String> {
    let mut ready = session.ready.lock().await;
    if *ready {
        return Ok(());
    }
    let cwd = &session.workspace_path;
    ensure_cancel_active(cancel)?;

    if state.config.skip_boot_git_sync {
        match cancel {
            Some(cancel) => {
                git::configure_identity_with_cancel(
                    cwd,
                    &state.config.git_author_name,
                    &state.config.git_author_email,
                    cancel,
                )
                .await
            }
            None => {
                git::configure_identity(
                    cwd,
                    &state.config.git_author_name,
                    &state.config.git_author_email,
                )
                .await
            }
        }
        .map_err(|e| e.0)?;
        ensure_cancel_active(cancel)?;
        *ready = true;
        return Ok(());
    }

    match cancel {
        Some(cancel) => {
            git::fetch_remote_branch_with_cancel(cwd, &state.config.base_branch, 1, cancel).await
        }
        None => git::fetch_remote_branch(cwd, &state.config.base_branch, 1).await,
    }
    .map_err(|e| e.0)?;
    ensure_cancel_active(cancel)?;

    let has_remote = match cancel {
        Some(cancel) => git::remote_branch_exists_with_cancel(cwd, &session.branch, cancel).await,
        None => git::remote_branch_exists(cwd, &session.branch).await,
    }
    .unwrap_or(false);
    ensure_cancel_active(cancel)?;
    let switch_source = if has_remote {
        let fetched = match cancel {
            Some(cancel) => {
                git::fetch_remote_branch_with_cancel(cwd, &session.branch, 1, cancel).await
            }
            None => git::fetch_remote_branch(cwd, &session.branch, 1).await,
        };
        if fetched.is_err() {
            tracing::warn!(branch = %session.branch, "failed to fetch existing thread branch");
        }
        ensure_cancel_active(cancel)?;
        format!("origin/{}", session.branch)
    } else {
        format!("origin/{}", state.config.base_branch)
    };

    ensure_cancel_active(cancel)?;
    let current = match cancel {
        Some(cancel) => git::current_branch_with_cancel(cwd, cancel)
            .await
            .map_err(|e| e.0)?,
        None => git::current_branch(cwd).await,
    };
    ensure_cancel_active(cancel)?;
    if current.as_deref() != Some(session.branch.as_str()) {
        let status = match cancel {
            Some(cancel) => git::workspace_status_with_cancel(cwd, cancel).await,
            None => git::workspace_status(cwd).await,
        }
        .map_err(|e| e.0)?;
        if !status.trim().is_empty() {
            return Err(format!(
                "workspace has uncommitted changes while on {}; refusing to switch to {}",
                current.as_deref().unwrap_or("detached HEAD"),
                session.branch
            ));
        }
        git::sh_capture_with_cancel(
            "git",
            &[
                "switch",
                "--discard-changes",
                "-C",
                &session.branch,
                &switch_source,
            ],
            cwd,
            git::TIMEOUT_GIT_QUICK,
            cancel,
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

    ensure_cancel_active(cancel)?;
    match cancel {
        Some(cancel) => {
            git::configure_identity_with_cancel(
                cwd,
                &state.config.git_author_name,
                &state.config.git_author_email,
                cancel,
            )
            .await
        }
        None => {
            git::configure_identity(
                cwd,
                &state.config.git_author_name,
                &state.config.git_author_email,
            )
            .await
        }
    }
    .map_err(|e| e.0)?;
    ensure_cancel_active(cancel)?;
    *ready = true;
    Ok(())
}

fn ensure_cancel_active(cancel: Option<&CancellationToken>) -> Result<(), String> {
    if cancel.is_some_and(CancellationToken::is_cancelled) {
        Err("task cancelled by request".to_string())
    } else {
        Ok(())
    }
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
            if task.cancelled.load(Ordering::SeqCst) {
                "cancelled"
            } else {
                "failed"
            }
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
    ensure_task_active(task)?;
    state.emit(
        task,
        json!({ "kind": "status", "status": "preparing", "message": "Preparing workspace" }),
    );
    prepare_session_workspace_with_cancel(state, &task.session, &task.cancel).await?;
    ensure_task_active(task)?;

    // Claim the backing work-item from the control plane (fencing token).
    if state.control_plane.enabled() {
        let work_item_id = task.thread_id.as_deref().ok_or_else(|| {
            "control-plane governance requires a backing work-item id".to_string()
        })?;
        let claim = state
            .control_plane
            .claim_work(work_item_id, CLAIM_LEASE_TTL_MS)
            .await?;
        if let Err(cancelled) = ensure_task_active(task) {
            return match state.control_plane.transition(&claim, "cancelled").await {
                Ok(()) => Err(cancelled),
                Err(transition) => Err(format!(
                    "{cancelled}; additionally could not persist cancelled state: {transition}"
                )),
            };
        }
        state.control_plane.transition(&claim, "running").await?;
        state.emit(task, json!({
            "kind": "status", "status": "claimed",
            "message": format!("Claimed work-item {} (fencing token {})", claim.work_item_id, claim.fencing_token),
        }));
        run_claimed_lifecycle(state, task, &claim).await
    } else {
        run_task_lifecycle(state, task, None).await
    }
}

/// Run every post-claim stage, including local mutations, external push, and
/// artifact publication. In governed mode the caller keeps the lease alive for
/// this entire future and for the terminal control-plane transition.
async fn run_task_lifecycle(
    state: &AppState,
    task: &Arc<TaskState>,
    claim: Option<&WorkClaim>,
) -> Result<(), String> {
    ensure_task_active(task)?;
    // Optimistic deterministic edit (append-file), when the provider can edit.
    if task.provider.can_edit_workspace() {
        if let Some(edit) = prompt::parse_deterministic_append(&task.prompt) {
            match apply_deterministic_append(state, task, &edit).await {
                Ok(true) => { /* handled deterministically; still run agent for narration */ }
                Ok(false) => {}
                Err(e) => state.emit(
                    task,
                    json!({ "kind": "stderr", "text": format!("deterministic edit failed: {e}") }),
                ),
            }
        }
    }
    ensure_task_active(task)?;

    // Drive the agent runner.
    state.emit(
        task,
        json!({ "kind": "status", "status": "running", "message": "Agent running" }),
    );
    run_agent(state, task).await?;
    ensure_task_active(task)?;

    // Stage + commit + push (external mutation → verify fencing token first).
    let cwd = &task.session.workspace_path;
    ensure_task_active(task)?;
    git::add_workspace_changes_with_cancel(cwd, &task.cancel)
        .await
        .map_err(|e| e.0)?;
    ensure_task_active(task)?;
    let status = git::workspace_status_with_cancel(cwd, &task.cancel)
        .await
        .map_err(|e| e.0)?;
    if !status.trim().is_empty() {
        ensure_task_active(task)?;
        git::commit_with_cancel(
            cwd,
            &format!("agent: {}", first_line(&task.prompt)),
            &task.cancel,
        )
        .await
        .map_err(|e| e.0)?;
        ensure_task_active(task)?;
        // Renew immediately before the external mutation. The periodic renewal
        // protects the whole lifecycle; this final exact check closes the gap
        // between its last tick and spawning `git push`.
        if let Some(claim) = claim {
            state.control_plane.renew_claim(claim).await?;
        }
        ensure_task_active(task)?;
        git::push_branch_with_cancel(cwd, &task.branch, &task.cancel)
            .await
            .map_err(|e| e.0)?;
        state.emit(task, json!({ "kind": "status", "status": "pushed", "message": format!("Pushed {}", task.branch) }));
    } else {
        state.emit(task, json!({ "kind": "status", "status": "no-changes", "message": "No workspace changes to commit" }));
    }

    // Publish output artifacts.
    ensure_task_active(task)?;
    publish_artifacts(state, task).await?;

    Ok(())
}

/// Keep the exact work-item election alive from the first post-claim mutation
/// through provider execution, commit, push, artifacts, and the terminal state
/// transition. A renewal failure cancels and drains the active operation before
/// returning so no child process can keep mutating under stale authority.
async fn run_claimed_lifecycle(
    state: &AppState,
    task: &Arc<TaskState>,
    claim: &WorkClaim,
) -> Result<(), String> {
    let lifecycle = async {
        let execution = run_task_lifecycle(state, task, Some(claim)).await;
        let terminal_status =
            claimed_terminal_status(task.cancelled.load(Ordering::SeqCst), execution.is_ok());
        let transition = state.control_plane.transition(claim, terminal_status).await;

        match (execution, transition) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(transition_error)) => Err(format!(
                "{error}; additionally could not persist {terminal_status} state: {transition_error}"
            )),
            (Ok(()), Err(review_error)) => {
                // If the success transition was definitively rejected, do not
                // leave a known-running item behind. A lost response can make
                // this recovery conflict, in which case return both errors.
                match state.control_plane.transition(claim, "failed").await {
                    Ok(()) => Err(format!(
                        "could not persist awaiting_review state; marked work failed: {review_error}"
                    )),
                    Err(failed_error) => Err(format!(
                        "could not persist awaiting_review state: {review_error}; additionally could not persist failed state: {failed_error}"
                    )),
                }
            }
        }
    };
    tokio::pin!(lifecycle);
    let mut renewals = tokio::time::interval(CLAIM_RENEW_INTERVAL);
    renewals.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval`'s first tick is immediate; the transition that put the work in
    // `running` already renewed it, so consume that tick before selecting.
    renewals.tick().await;

    loop {
        tokio::select! {
            result = &mut lifecycle => return result,
            _ = renewals.tick() => {
                if let Err(error) = state.control_plane.renew_claim(claim).await {
                    task.cancel.cancel();
                    let cleanup = lifecycle.await;
                    return match cleanup {
                        Ok(()) => Err(format!("lost work-item authority during task lifecycle: {error}")),
                        Err(cleanup_error) => Err(format!(
                            "lost work-item authority during task lifecycle: {error}; cancellation cleanup: {cleanup_error}"
                        )),
                    };
                }
            }
        }
    }
}

fn claimed_terminal_status(cancelled_by_request: bool, execution_succeeded: bool) -> &'static str {
    if cancelled_by_request {
        "cancelled"
    } else if execution_succeeded {
        "awaiting_review"
    } else {
        "failed"
    }
}

fn ensure_task_active(task: &TaskState) -> Result<(), String> {
    if task.cancel.is_cancelled() {
        if task.cancelled.load(Ordering::SeqCst) {
            Err("task cancelled by request".to_string())
        } else {
            Err("task cancelled after loss of work-item authority".to_string())
        }
    } else {
        Ok(())
    }
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
    ensure_task_active(task)?;
    let rel = safe_repo_relative(&task.session.workspace_path, &edit.relative_path)?;
    let target = std::path::Path::new(&task.session.workspace_path).join(&rel);
    if let Some(parent) = target.parent() {
        ensure_task_active(task)?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| e.to_string())?;
    }
    let existing = tokio::fs::read_to_string(&target).await.unwrap_or_default();
    let prefix = if !existing.is_empty() && !existing.ends_with('\n') {
        "\n"
    } else {
        ""
    };
    let suffix = if edit.text.ends_with('\n') { "" } else { "\n" };
    let mut contents = existing;
    contents.push_str(prefix);
    contents.push_str(&edit.text);
    contents.push_str(suffix);
    ensure_task_active(task)?;
    tokio::fs::write(&target, contents)
        .await
        .map_err(|e| e.to_string())?;
    state.emit(
        task,
        json!({
            "kind": "status",
            "status": "deterministic-edit:append-file",
            "message": format!("Appended {} character(s) to {}", edit.text.len(), rel),
        }),
    );
    Ok(true)
}

/// Reject unsafe / out-of-repo / generated paths (`safeRepoRelativePath`).
fn safe_repo_relative(workspace: &str, raw: &str) -> Result<String, String> {
    let trimmed = raw
        .trim()
        .trim_start_matches("./")
        .trim_end_matches([')', ',', '.', ';', ':']);
    if trimmed.is_empty() || trimmed.contains('\0') || std::path::Path::new(trimmed).is_absolute() {
        return Err(format!("refusing unsafe append path: {raw}"));
    }
    let blocked = [".git", "node_modules", ".pnpm-store", ".next", ".turbo"];
    let normalized = std::path::Path::new(trimmed);
    for comp in normalized.components() {
        use std::path::Component;
        match comp {
            Component::ParentDir => return Err(format!("refusing append outside repo: {raw}")),
            Component::Normal(p) if blocked.iter().any(|b| p == std::ffi::OsStr::new(b)) => {
                return Err(format!("refusing append into generated path: {raw}"));
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
async fn publish_artifacts(state: &AppState, task: &Arc<TaskState>) -> Result<(), String> {
    ensure_task_active(task)?;
    let dir = std::path::Path::new(&state.config.outputs_dir).join(&task.task_id);
    let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
        return Ok(());
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        ensure_task_active(task)?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let publish = state.storage.publish(PublishOptions {
            task_id: task.task_id.clone(),
            file_path: path.to_string_lossy().to_string(),
            filename: None,
        });
        tokio::pin!(publish);
        let result = tokio::select! {
            result = &mut publish => result,
            _ = task.cancel.cancelled() => return ensure_task_active(task),
        };
        match result {
            Ok(artifact) => state.emit(task, json!({ "kind": "artifact", "artifact": artifact })),
            Err(e) => state.emit(
                task,
                json!({ "kind": "stderr", "text": format!("artifact publish failed: {e}") }),
            ),
        }
    }
    Ok(())
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(72).collect()
}

/// Build the strict env for a provider (re-exported for tests / callers).
pub fn agent_env(provider: crate::agents::AgentProvider) -> HashMap<String, String> {
    build_agent_env(provider)
}

#[cfg(test)]
mod tests {
    use super::claimed_terminal_status;

    #[test]
    fn claimed_lifecycle_uses_control_plane_status_spellings() {
        assert_eq!(claimed_terminal_status(false, true), "awaiting_review");
        assert_eq!(claimed_terminal_status(false, false), "failed");
        assert_eq!(claimed_terminal_status(true, true), "cancelled");
        assert_eq!(claimed_terminal_status(true, false), "cancelled");
    }
}
