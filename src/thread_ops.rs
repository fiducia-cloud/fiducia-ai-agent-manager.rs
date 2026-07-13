//! Explicit thread-control actions — the Rust port of the `/thread/*` handlers.
//! These are operator/UI-driven: merge the base branch into the thread branch,
//! commit + push the current workspace, or open a draft PR. Each is an external
//! mutation, so where the control plane is in play the caller should hold a valid
//! fencing token; these helpers focus on the git/gh mechanics.

use serde_json::{json, Value};

use crate::git;
use crate::state::AppState;

/// Merge `origin/<base>` into the current thread branch (`/thread/merge-upstream`).
pub async fn merge_upstream(state: &AppState, branch: &str) -> Result<Value, String> {
    let cwd = &state.config.workspace_repo;
    let base = &state.config.base_branch;
    git::assert_safe_branch(branch, "session branch").map_err(|e| e.0)?;
    let before = git::current_commit(cwd).await.map_err(|e| e.0)?;
    git::fetch_remote_branch(cwd, base, 1).await.map_err(|e| e.0)?;
    git::sh_capture(
        "git",
        &["merge", "--no-edit", &format!("origin/{base}")],
        cwd,
        git::TIMEOUT_GIT_NETWORK,
    )
    .await
    .map_err(|e| e.0)?;
    let after = git::current_commit(cwd).await.map_err(|e| e.0)?;
    Ok(json!({
        "ok": true,
        "branch": branch,
        "baseBranch": base,
        "before": before,
        "after": after,
        "fastForward": before != after,
    }))
}

/// Stage, commit, and push the current workspace (`/thread/make-commit`).
pub async fn make_commit(state: &AppState, branch: &str, message: Option<&str>) -> Result<Value, String> {
    let cwd = &state.config.workspace_repo;
    git::assert_safe_branch(branch, "session branch").map_err(|e| e.0)?;
    let before = git::current_commit(cwd).await.map_err(|e| e.0)?;
    git::add_workspace_changes(cwd).await.map_err(|e| e.0)?;
    let status = git::workspace_status(cwd).await.map_err(|e| e.0)?;
    let committed = !status.trim().is_empty();
    if committed {
        git::commit(cwd, message.unwrap_or("agent: manual commit")).await.map_err(|e| e.0)?;
    }
    git::push_branch(cwd, branch).await.map_err(|e| e.0)?;
    let after = git::current_commit(cwd).await.map_err(|e| e.0)?;
    Ok(json!({
        "ok": true,
        "branch": branch,
        "before": before,
        "after": after,
        "committed": committed,
        "pushed": true,
        "status": status,
    }))
}

/// Open (or reuse) a draft PR via the `gh` CLI (`/thread/open-pr`). Draft-only by
/// policy; never auto-merges.
pub async fn open_pr(state: &AppState, branch: &str, title: Option<&str>) -> Result<Value, String> {
    let cwd = &state.config.workspace_repo;
    let base = &state.config.base_branch;
    git::assert_safe_branch(branch, "session branch").map_err(|e| e.0)?;
    let title = title.unwrap_or("Agent draft").to_string();
    // Reuse an existing PR for the branch if present.
    if let Ok(existing) = git::sh_capture(
        "gh",
        &["pr", "view", branch, "--json", "url", "--jq", ".url"],
        cwd,
        git::TIMEOUT_GIT_NETWORK,
    )
    .await
    {
        let url = existing.trim();
        if !url.is_empty() {
            return Ok(json!({ "ok": true, "branch": branch, "prUrl": url, "title": title, "draft": true, "reused": true }));
        }
    }
    let out = git::sh_capture(
        "gh",
        &["pr", "create", "--draft", "--base", base, "--head", branch, "--title", &title, "--body", "Draft PR opened by fiducia agent."],
        cwd,
        git::TIMEOUT_GIT_NETWORK,
    )
    .await
    .map_err(|e| e.0)?;
    Ok(json!({
        "ok": true,
        "branch": branch,
        "baseBranch": base,
        "prUrl": out.trim(),
        "title": title,
        "draft": true,
        "reused": false,
    }))
}
