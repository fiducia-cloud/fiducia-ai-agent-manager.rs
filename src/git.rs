//! Git plumbing — the Rust port of the `shCapture`-based git helpers in
//! `server.ts`. Every command runs in the configured workspace with a per-op
//! timeout; branch and repo inputs are validated before they reach a shell
//! argument. The workspace is a single warm checkout shared by every task on the
//! thread (the container is pinned to one thread).

use std::time::Duration;

use regex::Regex;
use std::sync::OnceLock;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

/// Per-operation timeouts, mirroring the `TIMEOUT_*` constants.
pub const TIMEOUT_GIT_QUICK: Duration = Duration::from_secs(60);
pub const TIMEOUT_GIT_NETWORK: Duration = Duration::from_secs(5 * 60);

/// Paths the worker owns by contract and never treats as user repo content.
pub const GENERATED_GIT_EXCLUDE_PATHS: &[&str] =
    &[".pnpm-store", "node_modules", ".next", ".turbo"];

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct GitError(pub String);

/// Run `git <args>` (or any command) in `cwd`, capturing stdout, with a hard
/// timeout after which the child is killed (`shCapture`).
pub async fn sh_capture(
    program: &str,
    args: &[&str],
    cwd: &str,
    timeout: Duration,
) -> Result<String, GitError> {
    sh_capture_with_cancel(program, args, cwd, timeout, None).await
}

/// Cancellation-aware command execution. The child is both `kill_on_drop` and
/// explicitly killed + reaped on timeout/cancellation, so a timed-out `git
/// push` cannot outlive the lease-holding task that launched it.
pub async fn sh_capture_with_cancel(
    program: &str,
    args: &[&str],
    cwd: &str,
    timeout: Duration,
    cancel: Option<&CancellationToken>,
) -> Result<String, GitError> {
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|e| GitError(format!("{program}: spawn failed: {e}")))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| GitError(format!("{program}: stdout pipe unavailable")))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| GitError(format!("{program}: stderr pipe unavailable")))?;
    let stdout_reader = tokio::spawn(read_all(stdout));
    let stderr_reader = tokio::spawn(read_all(stderr));

    enum ChildResult {
        Exited(std::io::Result<std::process::ExitStatus>),
        TimedOut,
        Cancelled,
    }

    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    let result = if let Some(cancel) = cancel {
        tokio::select! {
            status = child.wait() => ChildResult::Exited(status),
            _ = &mut deadline => ChildResult::TimedOut,
            _ = cancel.cancelled() => ChildResult::Cancelled,
        }
    } else {
        tokio::select! {
            status = child.wait() => ChildResult::Exited(status),
            _ = &mut deadline => ChildResult::TimedOut,
        }
    };

    let status = match result {
        ChildResult::Exited(Ok(status)) => status,
        ChildResult::Exited(Err(wait_error)) => {
            let reap = terminate_and_reap(&mut child, program, args).await;
            let _ = collect_output(stdout_reader, program, "stdout").await;
            let _ = collect_output(stderr_reader, program, "stderr").await;
            return Err(match reap {
                Ok(()) => GitError(format!("{program} {}: {wait_error}", args.join(" "))),
                Err(reap_error) => GitError(format!(
                    "{program} {}: {wait_error}; {reap_error}",
                    args.join(" ")
                )),
            });
        }
        ChildResult::TimedOut => {
            let reap = terminate_and_reap(&mut child, program, args).await;
            let _ = collect_output(stdout_reader, program, "stdout").await;
            let _ = collect_output(stderr_reader, program, "stderr").await;
            let reason = format!("{program} {} timed out after {:?}", args.join(" "), timeout);
            return Err(match reap {
                Ok(()) => GitError(reason),
                Err(reap_error) => GitError(format!("{reason}; {reap_error}")),
            });
        }
        ChildResult::Cancelled => {
            let reap = terminate_and_reap(&mut child, program, args).await;
            let _ = collect_output(stdout_reader, program, "stdout").await;
            let _ = collect_output(stderr_reader, program, "stderr").await;
            let reason = format!("{program} {} cancelled", args.join(" "));
            return Err(match reap {
                Ok(()) => GitError(reason),
                Err(reap_error) => GitError(format!("{reason}; {reap_error}")),
            });
        }
    };
    let stdout = collect_output(stdout_reader, program, "stdout").await?;
    let stderr = collect_output(stderr_reader, program, "stderr").await?;
    if status.success() {
        Ok(String::from_utf8_lossy(&stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&stderr);
        Err(GitError(format!(
            "{program} {} exited {}: {}",
            args.join(" "),
            status.code().unwrap_or(-1),
            &stderr[..stderr.len().min(1000)]
        )))
    }
}

async fn read_all<R: tokio::io::AsyncRead + Unpin>(mut reader: R) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

async fn collect_output(
    reader: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    program: &str,
    stream: &str,
) -> Result<Vec<u8>, GitError> {
    reader
        .await
        .map_err(|error| GitError(format!("{program}: {stream} reader task failed: {error}")))?
        .map_err(|error| GitError(format!("{program}: could not read {stream}: {error}")))
}

async fn terminate_and_reap(
    child: &mut tokio::process::Child,
    program: &str,
    args: &[&str],
) -> Result<(), GitError> {
    // `start_kill` can race with natural exit. Either way, `wait` is mandatory
    // to reap the child and establish that no external mutation is still live.
    let _ = child.start_kill();
    child.wait().await.map(|_| ()).map_err(|error| {
        GitError(format!(
            "{program} {} could not be reaped: {error}",
            args.join(" ")
        ))
    })
}

async fn git(args: &[&str], cwd: &str, timeout: Duration) -> Result<String, GitError> {
    sh_capture("git", args, cwd, timeout).await
}

async fn git_with_cancel(
    args: &[&str],
    cwd: &str,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<String, GitError> {
    sh_capture_with_cancel("git", args, cwd, timeout, Some(cancel)).await
}

async fn git_optional_cancel(
    args: &[&str],
    cwd: &str,
    timeout: Duration,
    cancel: Option<&CancellationToken>,
) -> Result<String, GitError> {
    match cancel {
        Some(cancel) => git_with_cancel(args, cwd, timeout, cancel).await,
        None => git(args, cwd, timeout).await,
    }
}

/// Reject anything that is not a safe git ref (`assertSafeGitBranchName`).
pub fn assert_safe_branch(branch: &str, label: &str) -> Result<(), GitError> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$").unwrap());
    let invalid = !re.is_match(branch)
        || branch.starts_with('-')
        || branch.starts_with('/')
        || branch.ends_with('/')
        || branch.ends_with(".lock")
        || branch.contains("..")
        || branch.contains("//")
        || branch.contains("@{")
        || branch.contains('\\')
        || branch
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == ".." || p.ends_with(".lock"));
    if invalid {
        Err(GitError(format!("invalid git {label}: {branch}")))
    } else {
        Ok(())
    }
}

/// Slugify a title/prompt into a branch fragment (`slugifyBranchFragment`).
pub fn slugify_branch_fragment(value: &str) -> String {
    static NON_ALNUM: OnceLock<Regex> = OnceLock::new();
    static DASHES: OnceLock<Regex> = OnceLock::new();
    let non_alnum = NON_ALNUM.get_or_init(|| Regex::new(r"[^a-z0-9]+").unwrap());
    let dashes = DASHES.get_or_init(|| Regex::new(r"-{2,}").unwrap());
    let lower = value.to_lowercase();
    let s = non_alnum.replace_all(&lower, "-");
    let s = s.trim_matches('-');
    let s = dashes.replace_all(s, "-");
    let s: String = s.chars().take(80).collect();
    if s.is_empty() {
        "thread".to_string()
    } else {
        s
    }
}

/// Canonicalise a git remote so equivalent forms compare equal
/// (`canonicalRepoKey`).
pub fn canonical_repo_key(input: Option<&str>) -> Option<String> {
    let raw = input?.trim();
    if raw.is_empty() {
        return None;
    }
    let value = raw.strip_prefix("git+").unwrap_or(raw).to_string();
    // scp-style: git@host:owner/repo.git
    static SCP: OnceLock<Regex> = OnceLock::new();
    let scp = SCP.get_or_init(|| Regex::new(r"^([^@\s]+)@([^:\s]+):(.+)$").unwrap());
    let value = if let Some(c) = scp.captures(&value) {
        let host = c.get(2).unwrap().as_str().to_lowercase();
        let path = c.get(3).unwrap().as_str().trim_start_matches('/');
        format!("https://{host}/{path}")
    } else {
        value
    };
    // Try to parse as URL-ish host/path.
    if let Some(rest) = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"))
        .or_else(|| value.strip_prefix("ssh://"))
    {
        let rest = rest.split('@').next_back().unwrap_or(rest); // drop userinfo
        let mut parts = rest.splitn(2, '/');
        let host = parts.next().unwrap_or("").to_lowercase();
        let mut path = parts.next().unwrap_or("").trim_matches('/').to_lowercase();
        if let Some(stripped) = path.strip_suffix(".git") {
            path = stripped.to_string();
        }
        return Some(format!("{host}/{path}"));
    }
    let mut stripped = value.trim_end_matches('/').to_lowercase();
    if let Some(s) = stripped.strip_suffix(".git") {
        stripped = s.to_string();
    }
    Some(stripped)
}

pub fn repo_urls_match(a: Option<&str>, b: Option<&str>) -> bool {
    match (canonical_repo_key(a), canonical_repo_key(b)) {
        (Some(ka), Some(kb)) => ka == kb,
        _ => false,
    }
}

// ─── workspace queries ──────────────────────────────────────────────────────

pub async fn current_branch(cwd: &str) -> Option<String> {
    git(
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        cwd,
        TIMEOUT_GIT_QUICK,
    )
    .await
    .ok()
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty())
}

pub async fn current_branch_with_cancel(
    cwd: &str,
    cancel: &CancellationToken,
) -> Result<Option<String>, GitError> {
    let branch = git_with_cancel(
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        cwd,
        TIMEOUT_GIT_QUICK,
        cancel,
    )
    .await?;
    Ok(Some(branch.trim().to_string()).filter(|branch| !branch.is_empty()))
}

pub async fn current_commit(cwd: &str) -> Result<String, GitError> {
    Ok(git(&["rev-parse", "HEAD"], cwd, TIMEOUT_GIT_QUICK)
        .await?
        .trim()
        .to_string())
}

/// Porcelain status excluding the generated paths (`gitWorkspaceStatus`).
pub async fn workspace_status(cwd: &str) -> Result<String, GitError> {
    workspace_status_inner(cwd, None).await
}

pub async fn workspace_status_with_cancel(
    cwd: &str,
    cancel: &CancellationToken,
) -> Result<String, GitError> {
    workspace_status_inner(cwd, Some(cancel)).await
}

async fn workspace_status_inner(
    cwd: &str,
    cancel: Option<&CancellationToken>,
) -> Result<String, GitError> {
    let mut args: Vec<String> = vec![
        "status".into(),
        "--porcelain".into(),
        "--untracked-files=all".into(),
        "--".into(),
        ".".into(),
    ];
    for p in GENERATED_GIT_EXCLUDE_PATHS {
        args.push(format!(":(exclude){p}"));
    }
    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    git_optional_cancel(&refs, cwd, TIMEOUT_GIT_QUICK, cancel).await
}

pub async fn fetch_remote_branch(cwd: &str, branch: &str, depth: u32) -> Result<(), GitError> {
    fetch_remote_branch_inner(cwd, branch, depth, None).await
}

pub async fn fetch_remote_branch_with_cancel(
    cwd: &str,
    branch: &str,
    depth: u32,
    cancel: &CancellationToken,
) -> Result<(), GitError> {
    fetch_remote_branch_inner(cwd, branch, depth, Some(cancel)).await
}

async fn fetch_remote_branch_inner(
    cwd: &str,
    branch: &str,
    depth: u32,
    cancel: Option<&CancellationToken>,
) -> Result<(), GitError> {
    assert_safe_branch(branch, "remote branch")?;
    let refspec = format!("+refs/heads/{branch}:refs/remotes/origin/{branch}");
    let depth_arg = format!("--depth={depth}");
    git_optional_cancel(
        &[
            "fetch", "--quiet", "--prune", &depth_arg, "origin", &refspec,
        ],
        cwd,
        TIMEOUT_GIT_NETWORK,
        cancel,
    )
    .await
    .map(|_| ())
}

pub async fn remote_branch_exists(cwd: &str, branch: &str) -> Result<bool, GitError> {
    remote_branch_exists_inner(cwd, branch, None).await
}

pub async fn remote_branch_exists_with_cancel(
    cwd: &str,
    branch: &str,
    cancel: &CancellationToken,
) -> Result<bool, GitError> {
    remote_branch_exists_inner(cwd, branch, Some(cancel)).await
}

async fn remote_branch_exists_inner(
    cwd: &str,
    branch: &str,
    cancel: Option<&CancellationToken>,
) -> Result<bool, GitError> {
    assert_safe_branch(branch, "remote branch")?;
    let out = git_optional_cancel(
        &["ls-remote", "--heads", "origin", branch],
        cwd,
        TIMEOUT_GIT_NETWORK,
        cancel,
    )
    .await?;
    Ok(!out.trim().is_empty())
}

pub async fn configure_identity(cwd: &str, name: &str, email: &str) -> Result<(), GitError> {
    configure_identity_inner(cwd, name, email, None).await
}

pub async fn configure_identity_with_cancel(
    cwd: &str,
    name: &str,
    email: &str,
    cancel: &CancellationToken,
) -> Result<(), GitError> {
    configure_identity_inner(cwd, name, email, Some(cancel)).await
}

async fn configure_identity_inner(
    cwd: &str,
    name: &str,
    email: &str,
    cancel: Option<&CancellationToken>,
) -> Result<(), GitError> {
    git_optional_cancel(
        &["config", "user.name", name],
        cwd,
        TIMEOUT_GIT_QUICK,
        cancel,
    )
    .await?;
    git_optional_cancel(
        &["config", "user.email", email],
        cwd,
        TIMEOUT_GIT_QUICK,
        cancel,
    )
    .await?;
    Ok(())
}

/// Stage everything except the generated paths (`gitAddWorkspaceChanges`).
pub async fn add_workspace_changes(cwd: &str) -> Result<(), GitError> {
    add_workspace_changes_inner(cwd, None).await
}

pub async fn add_workspace_changes_with_cancel(
    cwd: &str,
    cancel: &CancellationToken,
) -> Result<(), GitError> {
    add_workspace_changes_inner(cwd, Some(cancel)).await
}

async fn add_workspace_changes_inner(
    cwd: &str,
    cancel: Option<&CancellationToken>,
) -> Result<(), GitError> {
    git_optional_cancel(&["add", "-A", "--", "."], cwd, TIMEOUT_GIT_QUICK, cancel).await?;
    let mut reset: Vec<&str> = vec!["reset", "-q", "HEAD", "--"];
    reset.extend_from_slice(GENERATED_GIT_EXCLUDE_PATHS);
    git_optional_cancel(&reset, cwd, TIMEOUT_GIT_QUICK, cancel).await?;
    Ok(())
}

pub async fn commit(cwd: &str, message: &str) -> Result<(), GitError> {
    git(
        &["commit", "--no-verify", "-m", message],
        cwd,
        TIMEOUT_GIT_QUICK,
    )
    .await
    .map(|_| ())
}

pub async fn commit_with_cancel(
    cwd: &str,
    message: &str,
    cancel: &CancellationToken,
) -> Result<(), GitError> {
    git_with_cancel(
        &["commit", "--no-verify", "-m", message],
        cwd,
        TIMEOUT_GIT_QUICK,
        cancel,
    )
    .await
    .map(|_| ())
}

pub async fn push_branch(cwd: &str, branch: &str) -> Result<(), GitError> {
    assert_safe_branch(branch, "session branch")?;
    git(
        &["push", "--no-verify", "--set-upstream", "origin", branch],
        cwd,
        TIMEOUT_GIT_NETWORK,
    )
    .await
    .map(|_| ())
}

pub async fn push_branch_with_cancel(
    cwd: &str,
    branch: &str,
    cancel: &CancellationToken,
) -> Result<(), GitError> {
    assert_safe_branch(branch, "session branch")?;
    git_with_cancel(
        &["push", "--no-verify", "--set-upstream", "origin", branch],
        cwd,
        TIMEOUT_GIT_NETWORK,
        cancel,
    )
    .await
    .map(|_| ())
}

/// Compute the stable per-session branch name (`getSessionBranch`).
pub fn session_branch(
    prefix: &str,
    session_id: &str,
    branch_hint: Option<&str>,
    title_hint: Option<&str>,
    prompt_hint: Option<&str>,
) -> Result<String, GitError> {
    if let Some(hint) = branch_hint.map(str::trim).filter(|s| !s.is_empty()) {
        assert_safe_branch(hint, "session branch")?;
        return Ok(hint.to_string());
    }
    let title = title_hint
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| prompt_hint.map(str::trim).filter(|s| !s.is_empty()))
        .unwrap_or(session_id);
    let branch = format!("{prefix}/{session_id}/{}", slugify_branch_fragment(title));
    assert_safe_branch(&branch, "session branch")?;
    Ok(branch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_branch_names() {
        assert!(assert_safe_branch("feature/ok-1", "b").is_ok());
        for bad in [
            "-x", "/x", "x/", "a..b", "a//b", "x.lock", "a@{0}", "a\\b", "a/../b",
        ] {
            assert!(assert_safe_branch(bad, "b").is_err(), "should reject {bad}");
        }
    }

    #[test]
    fn slugify_is_branch_safe() {
        assert_eq!(slugify_branch_fragment("Fix the Parser!"), "fix-the-parser");
        assert_eq!(
            slugify_branch_fragment("  multiple   spaces  "),
            "multiple-spaces"
        );
        assert_eq!(slugify_branch_fragment(""), "thread");
        assert!(assert_safe_branch(&slugify_branch_fragment("weird///name"), "b").is_ok());
    }

    #[test]
    fn canonical_repo_key_normalizes_equivalent_forms() {
        let https = canonical_repo_key(Some("https://github.com/Owner/Repo.git"));
        let scp = canonical_repo_key(Some("git@github.com:Owner/Repo.git"));
        let plus = canonical_repo_key(Some("git+https://github.com/owner/repo"));
        assert_eq!(https, scp);
        assert_eq!(https, plus);
        assert_eq!(https.as_deref(), Some("github.com/owner/repo"));
        assert!(canonical_repo_key(None).is_none());
        assert!(canonical_repo_key(Some("  ")).is_none());
    }

    #[test]
    fn repo_urls_match_across_forms_but_not_different_repos() {
        assert!(repo_urls_match(
            Some("git@github.com:acme/api.git"),
            Some("https://github.com/acme/api")
        ));
        assert!(!repo_urls_match(
            Some("https://github.com/acme/api"),
            Some("https://github.com/acme/other")
        ));
        assert!(!repo_urls_match(None, Some("https://github.com/acme/api")));
    }

    #[test]
    fn session_branch_uses_hint_or_slug() {
        // Explicit branch hint is used verbatim (when safe).
        let b = session_branch("agent/k8s", "thread-1", Some("custom/branch"), None, None).unwrap();
        assert_eq!(b, "custom/branch");
        // Otherwise derived from title slug under the prefix/session.
        let d = session_branch("agent/k8s", "t1", None, Some("My Feature"), None).unwrap();
        assert_eq!(d, "agent/k8s/t1/my-feature");
        // An unsafe explicit hint is rejected.
        assert!(session_branch("agent/k8s", "t1", Some("../evil"), None, None).is_err());
    }

    #[tokio::test]
    async fn cancellation_kills_and_reaps_the_child() {
        let cancel = CancellationToken::new();
        let child_cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            child_cancel.cancel();
        });
        let started = std::time::Instant::now();
        let error = sh_capture_with_cancel(
            "/bin/sh",
            &["-c", "exec sleep 30"],
            ".",
            Duration::from_secs(5),
            Some(&cancel),
        )
        .await
        .unwrap_err();
        assert!(error.0.contains("cancelled"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn timeout_kills_and_reaps_the_child() {
        let started = std::time::Instant::now();
        let error = sh_capture(
            "/bin/sh",
            &["-c", "exec sleep 30"],
            ".",
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert!(error.0.contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
