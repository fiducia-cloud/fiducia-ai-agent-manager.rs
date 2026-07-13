//! HTTP surface — the axum port of the Fastify routes in `server.ts`. Same
//! endpoints, same `X-Server-Auth` gate, same task-dispatch → SSE-stream shape.

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::agents::resolve_agent_provider;
use crate::orchestrator;
use crate::state::{AppState, TaskState};
use crate::thread_ops;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/status", get(status))
        .route("/agents", get(agents))
        .route("/tasks", get(list_tasks).post(dispatch_task))
        .route("/stream/{task_id}", get(stream))
        .route("/tasks/{task_id}/cancel", post(cancel_task))
        .route("/thread/merge-upstream", post(merge_upstream))
        .route("/thread/make-commit", post(make_commit))
        .route("/thread/open-pr", post(open_pr))
        .route("/", get(|| async { Redirect::temporary("/status") }))
        // A dispatch body is a prompt (<=64 KiB) plus small metadata; cap the
        // accepted body at 256 KiB so an oversized POST is rejected pre-buffer.
        .layer(DefaultBodyLimit::max(256 * 1024))
        .with_state(state)
}

/// Ceiling on live (unfinished) tasks this worker will hold at once. Prevents an
/// authenticated caller from exhausting memory/PIDs by dispatching without bound.
const MAX_ACTIVE_TASKS: usize = 256;

// ─── auth ───────────────────────────────────────────────────────────────────

/// `X-Server-Auth` gate (`serverAuthSecret`). Returns 401 when unconfigured or
/// mismatched — the same fail-closed behaviour as the Node service.
fn require_server_auth(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    let secret = state.config.server_auth_secret.as_deref();
    let presented = headers.get("x-server-auth").and_then(|v| v.to_str().ok());
    match (secret, presented) {
        (Some(s), Some(p)) if crate::util::constant_time_eq(p.as_bytes(), s.as_bytes()) => Ok(()),
        _ => Err((StatusCode::UNAUTHORIZED, Json(json!({ "error": "unauthorized" }))).into_response()),
    }
}

// ─── health / status / listing ──────────────────────────────────────────────

async fn healthz() -> impl IntoResponse {
    Json(json!({ "ok": true }))
}

async fn status(State(st): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "service": "fiducia-ai-agent-manager",
        "instanceId": st.instance_id,
        "startedAt": st.started_at,
        "threadId": st.config.thread_id,
        "repo": st.config.repo_url,
        "baseBranch": st.config.base_branch,
        "controlPlane": st.control_plane.enabled(),
        "natsConfigured": st.config.nats_url.is_some(),
        "activeTasks": st.tasks.lock().len(),
    }))
}

async fn agents(State(st): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "default": st.config.default_provider.as_str(),
        "providers": [
            "claude-cli","claude-sdk","generic-ai-sdk","gemini-sdk",
            "opencode-ai-sdk","openai-codex-cli","openai-sdk"
        ],
    }))
}

async fn list_tasks(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = require_server_auth(&st, &headers) {
        return r;
    }
    let tasks: Vec<Value> = st
        .tasks
        .lock()
        .values()
        .map(|t| {
            json!({
                "taskId": t.task_id,
                "threadId": t.thread_id,
                "branch": t.branch,
                "provider": t.provider.as_str(),
                "finished": t.is_finished(),
            })
        })
        .collect();
    Json(json!({ "ok": true, "tasks": tasks })).into_response()
}

// ─── dispatch ───────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct DispatchBody {
    prompt: String,
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default, alias = "threadId")]
    thread_id: Option<String>,
    #[serde(default, alias = "userId")]
    user_id: Option<String>,
    #[serde(default)]
    repo: Option<String>,
    #[serde(default, alias = "baseBranch")]
    base_branch: Option<String>,
    #[serde(default)]
    branch: Option<String>,
    #[serde(default, alias = "threadTitle")]
    thread_title: Option<String>,
    #[serde(default)]
    provider: Option<String>,
}

async fn dispatch_task(
    State(st): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<DispatchBody>,
) -> Response {
    if let Err(r) = require_server_auth(&st, &headers) {
        return r;
    }
    if body.prompt.is_empty() || body.prompt.len() > 64_000 {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "prompt is required (1..64000 chars)" }))).into_response();
    }
    let task_id = body.task_id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let thread_id = body.thread_id.clone().or_else(|| st.config.thread_id.clone());
    let user_id = body.user_id.clone().or_else(|| st.config.user_id.clone());

    // Binding guards (thread / repo / base branch / user).
    if let (Some(bound), Some(req)) = (&st.config.thread_id, &thread_id) {
        if bound != req {
            return (StatusCode::CONFLICT, Json(json!({ "error": "container is bound to a different thread", "boundThreadId": bound }))).into_response();
        }
    }
    if st.config.worker_bind_mode == crate::config::BindMode::Repo && thread_id.is_none() {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": "threadId is required for repo-scoped warm workers" }))).into_response();
    }
    if let Some(req_repo) = body.repo.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        if !crate::git::repo_urls_match(Some(req_repo), st.config.repo_url.as_deref()) {
            return (StatusCode::CONFLICT, Json(json!({ "error": "container is bound to a different repo", "boundRepo": st.config.repo_url }))).into_response();
        }
    }
    if let Some(req_base) = body.base_branch.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        if req_base != st.config.base_branch {
            return (StatusCode::CONFLICT, Json(json!({ "error": "container is bound to a different baseBranch", "boundBaseBranch": st.config.base_branch }))).into_response();
        }
    }

    // Idempotent re-dispatch.
    if let Some(existing) = st.tasks.lock().get(&task_id) {
        return Json(json!({
            "taskId": task_id, "branch": existing.branch, "duplicate": true,
            "status": if existing.is_finished() { "finished" } else { "running" },
        }))
        .into_response();
    }

    // Backpressure: reject new work once too many live tasks are in flight.
    let live = st.tasks.lock().values().filter(|t| !t.is_finished()).count();
    if live >= MAX_ACTIVE_TASKS {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({ "error": "worker at capacity; retry later", "activeTasks": live })),
        )
            .into_response();
    }

    let session = match orchestrator::get_or_create_session(
        &st,
        &task_id,
        thread_id.as_deref(),
        user_id.as_deref(),
        body.branch.as_deref(),
        body.thread_title.as_deref(),
        Some(&body.prompt),
    ) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))).into_response(),
    };
    session.task_ids.lock().push(task_id.clone());

    let provider = resolve_agent_provider(body.provider.as_deref(), st.config.default_provider);
    let task = Arc::new(TaskState {
        task_id: task_id.clone(),
        prompt: body.prompt.clone(),
        user_id,
        thread_id: thread_id.clone(),
        provider,
        branch: session.branch.clone(),
        worktree_path: session.workspace_path.clone(),
        session: session.clone(),
        cancel: CancellationToken::new(),
        seq: AtomicI64::new(0),
        finished: AtomicBool::new(false),
        cancelled: AtomicBool::new(false),
        finished_at: parking_lot::Mutex::new(None),
    });
    st.tasks.lock().insert(task_id.clone(), task.clone());
    session.queued_task_ids.lock().push(task_id.clone());

    st.emit(&task, json!({ "kind": "status", "status": "queued", "sessionId": session.session_id }));

    // Run the task in the background; the session queue serializes execution.
    let st_bg = st.clone();
    let task_bg = task.clone();
    tokio::spawn(async move {
        orchestrator::run_task(st_bg, task_bg).await;
    });

    Json(json!({ "taskId": task_id, "branch": task.branch, "queuedBehind": false })).into_response()
}

// ─── SSE stream ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct StreamQuery {
    #[serde(default, alias = "resumeFromId")]
    resume_from_id: Option<i64>,
}

async fn stream(
    State(st): State<AppState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Query(q): Query<StreamQuery>,
) -> Response {
    if let Err(r) = require_server_auth(&st, &headers) {
        return r;
    }
    let after = q
        .resume_from_id
        .or_else(|| {
            headers
                .get("last-event-id")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(-1);

    let Some((history, mut rx, done)) = st.bus.subscribe(&task_id, after) else {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))).into_response();
    };

    let stream = async_stream::stream! {
        for stored in history {
            yield Ok::<Event, Infallible>(sse_event(&stored));
        }
        if done {
            return;
        }
        loop {
            match rx.recv().await {
                Ok(stored) => {
                    let is_done = stored.event.get("kind").and_then(|k| k.as_str()) == Some("done");
                    yield Ok(sse_event(&stored));
                    if is_done {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            }
        }
    };

    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(25)).text("ping"))
        .into_response()
}

fn sse_event(stored: &crate::event_bus::StoredEvent) -> Event {
    let kind = stored.event.get("kind").and_then(|k| k.as_str()).unwrap_or("message");
    Event::default()
        .id(stored.seq.to_string())
        .event(kind)
        .data(stored.event.to_string())
}

// ─── cancel ─────────────────────────────────────────────────────────────────

async fn cancel_task(State(st): State<AppState>, Path(task_id): Path<String>, headers: HeaderMap) -> Response {
    if let Err(r) = require_server_auth(&st, &headers) {
        return r;
    }
    let Some(task) = st.tasks.lock().get(&task_id).cloned() else {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))).into_response();
    };
    task.cancelled.store(true, std::sync::atomic::Ordering::SeqCst);
    task.cancel.cancel();
    st.emit(&task, json!({ "kind": "status", "status": "cancelling", "message": "Cancellation requested" }));
    Json(json!({ "ok": true, "taskId": task_id, "cancelled": true })).into_response()
}

// ─── thread control ─────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct ThreadControlBody {
    #[serde(default)]
    branch: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default, alias = "threadTitle")]
    thread_title: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

fn resolved_branch(st: &AppState, body: &ThreadControlBody) -> Result<String, Response> {
    if let Some(b) = body.branch.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        return Ok(b.to_string());
    }
    // Fall back to the pinned thread's session branch.
    let sessions = st.sessions.lock();
    if let Some(session) = sessions.values().next() {
        return Ok(session.branch.clone());
    }
    Err((StatusCode::BAD_REQUEST, Json(json!({ "error": "no active thread branch; pass branch" }))).into_response())
}

async fn merge_upstream(State(st): State<AppState>, headers: HeaderMap, Json(body): Json<ThreadControlBody>) -> Response {
    if let Err(r) = require_server_auth(&st, &headers) {
        return r;
    }
    let branch = match resolved_branch(&st, &body) {
        Ok(b) => b,
        Err(r) => return r,
    };
    match thread_ops::merge_upstream(&st, &branch).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({ "error": e }))).into_response(),
    }
}

async fn make_commit(State(st): State<AppState>, headers: HeaderMap, Json(body): Json<ThreadControlBody>) -> Response {
    if let Err(r) = require_server_auth(&st, &headers) {
        return r;
    }
    let branch = match resolved_branch(&st, &body) {
        Ok(b) => b,
        Err(r) => return r,
    };
    match thread_ops::make_commit(&st, &branch, body.message.as_deref()).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({ "error": e }))).into_response(),
    }
}

async fn open_pr(State(st): State<AppState>, headers: HeaderMap, Json(body): Json<ThreadControlBody>) -> Response {
    if let Err(r) = require_server_auth(&st, &headers) {
        return r;
    }
    let branch = match resolved_branch(&st, &body) {
        Ok(b) => b,
        Err(r) => return r,
    };
    let title = body.title.as_deref().or(body.thread_title.as_deref());
    match thread_ops::open_pr(&st, &branch, title).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({ "error": e }))).into_response(),
    }
}
