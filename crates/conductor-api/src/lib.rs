use std::future::IntoFuture;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::Router;
use axum::extract::{
    DefaultBodyLimit, Path, Query, State,
    rejection::{JsonRejection, QueryRejection},
};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine;
use conductor_core::{
    DEFAULT_LOG_CHUNK_BYTES, JobId, JobSpec, LogStream, MAX_LOG_CHUNK_BYTES, ReadRunLogs, RunId,
    RunState, WorkerId,
};
use conductor_manager::{
    DEFAULT_PAGE_SIZE, JobService, MAX_PAGE_SIZE, ManagerError, QueryError, WorkerPort,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
const MAX_REQUEST_HEADER_COUNT: usize = 64;
const MAX_REQUEST_HEADER_BYTES: usize = 16 * 1024;
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PENDING_STARTS: usize = 32;
const MAX_PENDING_BLOCKING_OPERATIONS: usize = 64;
const MAX_PENDING_CONTROL_OPERATIONS: usize = 8;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobCollection {
    jobs: Vec<JobSpec>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunCollection {
    runs: Vec<conductor_core::RunObservation>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunLogsResponse {
    run_id: String,
    stream: LogStream,
    offset: u64,
    next_offset: u64,
    data_encoding: &'static str,
    data: String,
    stream_closed: bool,
    eof: bool,
    truncated: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RunFilters {
    job_id: Option<String>,
    state: Option<RunState>,
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PageFilters {
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RunLogFilters {
    stream: LogStream,
    offset: Option<u64>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResolveRunRequest {
    resolution: String,
    reason: String,
}

struct ApiState {
    manager: Arc<JobService>,
    worker: Arc<dyn WorkerPort>,
    worker_id: WorkerId,
    worker_capacity: usize,
    start_admission: Arc<Semaphore>,
    blocking_admission: Arc<Semaphore>,
    control_admission: Arc<Semaphore>,
    recovery_ready: Arc<AtomicBool>,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: &'static str,
    details: &'static [&'static str],
}

fn error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    let body = Json(ErrorEnvelope {
        error: ErrorDetail {
            code,
            message,
            details: &[],
        },
    });
    (status, body).into_response()
}

/// Build the HTTP application served by the combined agent.
pub fn router(manager: Arc<JobService>, worker: Arc<dyn WorkerPort>) -> Router {
    router_with_recovery(
        manager,
        worker,
        Arc::new(AtomicBool::new(true)),
        DEFAULT_REQUEST_TIMEOUT,
    )
}

fn router_with_recovery(
    manager: Arc<JobService>,
    worker: Arc<dyn WorkerPort>,
    recovery_ready: Arc<AtomicBool>,
    request_timeout: Duration,
) -> Router {
    let descriptor = worker.descriptor();
    let state = Arc::new(ApiState {
        manager,
        worker,
        worker_id: descriptor.worker_id().clone(),
        worker_capacity: descriptor.capacity(),
        start_admission: Arc::new(Semaphore::new(MAX_PENDING_STARTS)),
        blocking_admission: Arc::new(Semaphore::new(MAX_PENDING_BLOCKING_OPERATIONS)),
        control_admission: Arc::new(Semaphore::new(MAX_PENDING_CONTROL_OPERATIONS)),
        recovery_ready,
    });
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/jobs", post(create_job).get(list_jobs))
        .route("/v1/jobs/{id}", get(get_job).delete(delete_job))
        .route("/v1/jobs/{id}/runs", post(start_run))
        .route("/v1/workers", get(list_workers))
        .route("/v1/workers/{id}", get(get_worker))
        .route("/v1/runs", get(list_runs))
        .route("/v1/runs/{id}", get(get_run))
        .route("/v1/runs/{id}/logs", get(get_run_logs))
        .route("/v1/runs/{id}/stop", post(stop_run))
        .route("/v1/runs/{id}/resolve", post(resolve_run))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .layer(middleware::from_fn_with_state(
            request_timeout,
            request_bounds,
        ))
        .with_state(state)
}

#[derive(Clone, Copy)]
enum BlockingClass {
    General,
    Control,
}

enum BlockingError {
    Saturated,
    Join,
}

async fn run_blocking<T, F>(
    state: &Arc<ApiState>,
    class: BlockingClass,
    operation: F,
) -> Result<T, BlockingError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let admission = match class {
        BlockingClass::General => Arc::clone(&state.blocking_admission),
        BlockingClass::Control => Arc::clone(&state.control_admission),
    };
    let permit = admission
        .try_acquire_owned()
        .map_err(|_| BlockingError::Saturated)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation()
    })
    .await
    .map_err(|_| BlockingError::Join)
}

fn blocking_error(error_value: BlockingError) -> Response {
    match error_value {
        BlockingError::Saturated => {
            let mut response = error(
                StatusCode::TOO_MANY_REQUESTS,
                "blocking_admission_saturated",
                "too many blocking operations are in progress",
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
        BlockingError::Join => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the blocking operation stopped unexpectedly",
        ),
    }
}

async fn request_bounds(
    axum::extract::State(timeout): axum::extract::State<Duration>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let headers = request.headers();
    let header_bytes = headers.iter().fold(0usize, |size, (name, value)| {
        size.saturating_add(name.as_str().len())
            .saturating_add(value.as_bytes().len())
    });
    if headers.len() > MAX_REQUEST_HEADER_COUNT || header_bytes > MAX_REQUEST_HEADER_BYTES {
        return error(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "request_headers_too_large",
            "request headers exceed the configured count or byte limit",
        );
    }
    match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => error(
            StatusCode::REQUEST_TIMEOUT,
            "request_timeout",
            "the request exceeded the configured processing deadline",
        ),
    }
}

async fn healthz() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        r#"{"status":"ok","role":"all","capabilities":["local_process_execution"]}"#,
    )
}

async fn readyz(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    if state.recovery_ready.load(Ordering::SeqCst)
        && !state.worker.is_draining()
        && !state.worker.is_degraded()
    {
        (
            StatusCode::OK,
            r#"{"status":"ready","role":"all","capabilities":["local_process_execution"]}"#,
        )
    } else if state.worker.is_draining() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"status":"draining","role":"all","capabilities":["local_process_execution"]}"#,
        )
    } else if state.worker.is_degraded() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"status":"degraded","role":"all","capabilities":["local_process_execution"]}"#,
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"status":"recovering","role":"all","capabilities":["local_process_execution"]}"#,
        )
    }
}

async fn list_workers(State(state): State<Arc<ApiState>>) -> Response {
    match query_worker_status(&state).await {
        Ok(status) => (StatusCode::OK, Json(vec![status])).into_response(),
        Err(error) => manager_error(error),
    }
}

async fn get_worker(State(state): State<Arc<ApiState>>, Path(raw_id): Path<String>) -> Response {
    let Ok(id) = WorkerId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_worker_id",
            "the worker ID is invalid",
        );
    };
    if id != state.worker_id {
        return error(
            StatusCode::NOT_FOUND,
            "worker_not_found",
            "the worker does not exist",
        );
    }
    match query_worker_status(&state).await {
        Ok(status) => (StatusCode::OK, Json(status)).into_response(),
        Err(error) => manager_error(error),
    }
}

async fn query_worker_status(
    state: &Arc<ApiState>,
) -> Result<conductor_core::WorkerStatus, ManagerError> {
    let manager = Arc::clone(&state.manager);
    let worker = Arc::clone(&state.worker);
    match run_blocking(state, BlockingClass::General, move || {
        manager.worker_status(worker.as_ref())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(ManagerError::WorkerUnavailable),
    }
}

async fn create_job(
    State(state): State<Arc<ApiState>>,
    payload: Result<Json<JobSpec>, JsonRejection>,
) -> Response {
    let Json(job) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return json_rejection(rejection),
    };
    let result = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        let job = job.clone();
        move || manager.register(job)
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(()) => (StatusCode::CREATED, Json(job)).into_response(),
        Err(ManagerError::DuplicateJob) => error(
            StatusCode::CONFLICT,
            "duplicate_job",
            "a job with this ID is already registered or was deleted",
        ),
        Err(ManagerError::JobLimitReached) => error(
            StatusCode::INSUFFICIENT_STORAGE,
            "job_limit_reached",
            "the job capacity has been reached",
        ),
        Err(ManagerError::StorageUnavailable) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not access job storage",
        ),
        Err(error) => manager_error(error),
    }
}

async fn list_jobs(
    State(state): State<Arc<ApiState>>,
    filters: Result<Query<PageFilters>, QueryRejection>,
) -> Response {
    let Query(filters) = match filters {
        Ok(filters) => filters,
        Err(_) => return invalid_page_query(),
    };
    let limit = filters.limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return invalid_page_query();
    }
    let result = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        let cursor = filters.cursor;
        move || manager.list_page(limit, cursor.as_deref())
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(page) => (
            StatusCode::OK,
            Json(JobCollection {
                jobs: page.items,
                next_cursor: page.next_cursor,
            }),
        )
            .into_response(),
        Err(error) => query_error(error),
    }
}

async fn get_job(State(state): State<Arc<ApiState>>, Path(raw_id): Path<String>) -> Response {
    let Ok(id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let result = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        move || manager.get(&id)
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(Some(job)) => (StatusCode::OK, Json(job)).into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            "job_not_found",
            "the job does not exist",
        ),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not access job storage",
        ),
    }
}

async fn delete_job(State(state): State<Arc<ApiState>>, Path(raw_id): Path<String>) -> Response {
    let Ok(id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let result = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        move || manager.delete(&id)
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => error(
            StatusCode::NOT_FOUND,
            "job_not_found",
            "the job does not exist",
        ),
        Err(ManagerError::JobConflict) => error(
            StatusCode::CONFLICT,
            "job_has_active_run",
            "the job cannot be deleted while a run is active or unresolved",
        ),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not access job storage",
        ),
    }
}

async fn start_run(
    State(state): State<Arc<ApiState>>,
    Path(raw_job_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !state.recovery_ready.load(Ordering::SeqCst) {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "recovery_in_progress",
            "run admission is unavailable until durable state reconciliation completes",
        );
    }
    let Ok(job_id) = JobId::new(raw_job_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let idempotency_key = match parse_idempotency_key(&headers) {
        Ok(key) => key,
        Err(()) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_idempotency_key",
                "Idempotency-Key must be 1-128 ASCII letters, digits, '.', '_', '~', or '-'",
            );
        }
    };
    let idempotency_key = idempotency_key.map(str::to_owned);
    if let Some(key) = idempotency_key.as_deref() {
        match replay_run(&state, job_id.clone(), key.to_owned()).await {
            Err(error) => return manager_error(error),
            Ok(Some(observation)) => return accepted_run(observation),
            Ok(None) => {}
        }
    }
    let permit = match Arc::clone(&state.start_admission).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            if let Some(key) = idempotency_key.as_deref()
                && let Ok(Some(observation)) =
                    replay_run(&state, job_id.clone(), key.to_owned()).await
            {
                return accepted_run(observation);
            }
            let mut response = error(
                StatusCode::TOO_MANY_REQUESTS,
                "start_admission_saturated",
                "too many run submissions are being processed",
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            return response;
        }
    };
    let manager = Arc::clone(&state.manager);
    let worker = Arc::clone(&state.worker);
    let worker_id = state.worker_id.clone();
    let worker_capacity = state.worker_capacity;
    let result = run_blocking(&state, BlockingClass::General, move || {
        let _permit = permit;
        manager.start_run_with_key(
            &job_id,
            &worker_id,
            worker_capacity,
            idempotency_key.as_deref(),
            worker.as_ref(),
        )
    })
    .await;
    match result {
        Ok(Ok(observation)) => accepted_run(observation),
        Ok(Err(error)) => manager_error(error),
        Err(error_value) => blocking_error(error_value),
    }
}

async fn replay_run(
    state: &Arc<ApiState>,
    job_id: JobId,
    key: String,
) -> Result<Option<conductor_core::RunObservation>, ManagerError> {
    let manager = Arc::clone(&state.manager);
    let worker = Arc::clone(&state.worker);
    match run_blocking(state, BlockingClass::General, move || {
        manager.replay_run(&job_id, &key, worker.as_ref())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(ManagerError::WorkerUnavailable),
    }
}

fn accepted_run(observation: conductor_core::RunObservation) -> Response {
    let location = format!("/v1/runs/{}", observation.run_id());
    let mut response = (StatusCode::ACCEPTED, Json(observation)).into_response();
    match HeaderValue::from_str(&location) {
        Ok(value) => {
            response.headers_mut().insert(header::LOCATION, value);
            response
        }
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the run location could not be constructed",
        ),
    }
}

fn parse_idempotency_key(headers: &HeaderMap) -> Result<Option<&str>, ()> {
    let values = headers.get_all("idempotency-key");
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    let value = value.to_str().map_err(|_| ())?;
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._~-".contains(&byte))
    {
        return Err(());
    }
    Ok(Some(value))
}

async fn get_run(State(state): State<Arc<ApiState>>, Path(raw_id): Path<String>) -> Response {
    let Ok(run_id) = RunId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_run_id",
            "the run ID is invalid",
        );
    };
    let result = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        let worker = Arc::clone(&state.worker);
        move || manager.get_run(&run_id, worker.as_ref())
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(Some(observation)) => (StatusCode::OK, Json(observation)).into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            "run_not_found",
            "the run does not exist",
        ),
        Err(error) => manager_error(error),
    }
}

async fn stop_run(State(state): State<Arc<ApiState>>, Path(raw_id): Path<String>) -> Response {
    let Ok(run_id) = RunId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_run_id",
            "the run ID is invalid",
        );
    };
    let result = match run_blocking(&state, BlockingClass::Control, {
        let manager = Arc::clone(&state.manager);
        let worker = Arc::clone(&state.worker);
        move || manager.stop_run(&run_id, worker.as_ref())
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(observation) => {
            let status = if observation.state().is_terminal() {
                StatusCode::OK
            } else {
                StatusCode::ACCEPTED
            };
            (status, Json(observation)).into_response()
        }
        Err(error) => manager_error(error),
    }
}

async fn resolve_run(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
    payload: Result<Json<ResolveRunRequest>, JsonRejection>,
) -> Response {
    let Ok(run_id) = RunId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_run_id",
            "the run ID is invalid",
        );
    };
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return json_rejection(rejection),
    };
    if payload.resolution != "abandon"
        || payload.reason.trim().is_empty()
        || payload.reason.len() > 512
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_resolution",
            "resolution must be 'abandon' with a non-empty reason of at most 512 bytes",
        );
    }
    let result = match run_blocking(&state, BlockingClass::Control, {
        let manager = Arc::clone(&state.manager);
        let worker = Arc::clone(&state.worker);
        let reason = payload.reason;
        move || manager.resolve_unknown_run(&run_id, &reason, worker.as_ref())
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(observation) => (StatusCode::OK, Json(observation)).into_response(),
        Err(ManagerError::ResolutionConflict) => error(
            StatusCode::CONFLICT,
            "resolution_conflict",
            "only unknown runs may be abandoned, and the same reason must be used for retries",
        ),
        Err(error) => manager_error(error),
    }
}

async fn get_run_logs(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
    filters: Result<Query<RunLogFilters>, QueryRejection>,
) -> Response {
    let Query(filters) = match filters {
        Ok(filters) => filters,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_log_query",
                "the log query parameters are invalid",
            );
        }
    };
    let Ok(run_id) = RunId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_run_id",
            "the run ID is invalid",
        );
    };
    let offset = filters.offset.unwrap_or(0);
    let limit = filters.limit.unwrap_or(DEFAULT_LOG_CHUNK_BYTES);
    if limit == 0 || limit > MAX_LOG_CHUNK_BYTES {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_log_limit",
            "limit must be between 1 and 65536 bytes",
        );
    }
    let command = ReadRunLogs::new(run_id, filters.stream, offset, limit);
    let result = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        let worker = Arc::clone(&state.worker);
        move || manager.read_logs(command, worker.as_ref())
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(chunk) => (
            StatusCode::OK,
            Json(RunLogsResponse {
                run_id: chunk.run_id().to_string(),
                stream: chunk.stream(),
                offset: chunk.offset(),
                next_offset: chunk.next_offset(),
                data_encoding: "base64",
                data: match encode_base64(chunk.data()) {
                    Ok(data) => data,
                    Err(()) => {
                        return error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "internal_error",
                            "the bounded log chunk could not be encoded",
                        );
                    }
                },
                stream_closed: chunk.stream_closed(),
                eof: chunk.eof(),
                truncated: chunk.truncated(),
            }),
        )
            .into_response(),
        Err(error_value) => manager_error(error_value),
    }
}

fn encode_base64(bytes: &[u8]) -> Result<String, ()> {
    let encoded_length = bytes.len().checked_add(2).ok_or(())? / 3 * 4;
    let mut encoded = vec![0; encoded_length];
    let written = base64::engine::general_purpose::STANDARD
        .encode_slice(bytes, &mut encoded)
        .map_err(|_| ())?;
    encoded.truncate(written);
    String::from_utf8(encoded).map_err(|_| ())
}

async fn list_runs(
    State(state): State<Arc<ApiState>>,
    filters: Result<Query<RunFilters>, QueryRejection>,
) -> Response {
    let Query(filters) = match filters {
        Ok(filters) => filters,
        Err(_) => return invalid_page_query(),
    };
    let job_id = match filters.job_id {
        Some(raw) => match JobId::new(raw) {
            Ok(id) => Some(id),
            Err(_) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_job_id",
                    "the job ID is invalid",
                );
            }
        },
        None => None,
    };
    let limit = filters.limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return invalid_page_query();
    }
    let page = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        let cursor = filters.cursor;
        move || manager.list_runs_page(job_id.as_ref(), filters.state, limit, cursor.as_deref())
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match page {
        Ok(page) => {
            let mut refreshed = Vec::with_capacity(page.items.len());
            for run in page.items {
                let current = match run_blocking(&state, BlockingClass::General, {
                    let manager = Arc::clone(&state.manager);
                    let worker = Arc::clone(&state.worker);
                    let run_id = run.run_id().clone();
                    move || manager.get_run(&run_id, worker.as_ref())
                })
                .await
                {
                    Ok(current) => current,
                    Err(error_value) => return blocking_error(error_value),
                };
                match current {
                    Ok(Some(current)) => refreshed.push(current),
                    Ok(None) => {}
                    Err(error) => return manager_error(error),
                }
            }
            (
                StatusCode::OK,
                Json(RunCollection {
                    runs: refreshed,
                    next_cursor: page.next_cursor,
                }),
            )
                .into_response()
        }
        Err(error) => query_error(error),
    }
}

fn invalid_page_query() -> Response {
    error(
        StatusCode::BAD_REQUEST,
        "invalid_page_query",
        "limit or cursor is invalid for this collection",
    )
}

fn query_error(error_value: QueryError) -> Response {
    match error_value {
        QueryError::InvalidCursor => error(
            StatusCode::BAD_REQUEST,
            "invalid_cursor",
            "the cursor is malformed or does not match the collection filters",
        ),
        QueryError::StorageUnavailable => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not access collection storage",
        ),
    }
}

fn manager_error(error_value: ManagerError) -> Response {
    match error_value {
        ManagerError::JobNotFound => error(
            StatusCode::NOT_FOUND,
            "job_not_found",
            "the job does not exist",
        ),
        ManagerError::JobConflict => error(
            StatusCode::CONFLICT,
            "job_conflict",
            "the job has an active or unresolved run",
        ),
        ManagerError::IdempotencyConflict => error(
            StatusCode::CONFLICT,
            "idempotency_key_conflict",
            "the Idempotency-Key was already used for a different request",
        ),
        ManagerError::ResolutionConflict => error(
            StatusCode::CONFLICT,
            "resolution_conflict",
            "only unknown runs may be abandoned, and the same reason must be used for retries",
        ),
        ManagerError::CapacityReached => {
            let mut response = error(
                StatusCode::TOO_MANY_REQUESTS,
                "worker_capacity_reached",
                "the local worker has no available capacity",
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
        ManagerError::RunNotFound => error(
            StatusCode::NOT_FOUND,
            "run_not_found",
            "the run does not exist",
        ),
        ManagerError::LogsNotFound => error(
            StatusCode::NOT_FOUND,
            "logs_not_found",
            "the run has no stored output logs",
        ),
        ManagerError::LogsPruned => error(
            StatusCode::GONE,
            "logs_pruned",
            "the run output logs were removed by retention",
        ),
        ManagerError::WorkerUnavailable => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "worker_unavailable",
            "the local worker could not confirm run acceptance",
        ),
        ManagerError::WorkerDraining => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "worker_draining",
            "the local worker is shutting down and no longer accepts runs",
        ),
        ManagerError::StorageUnavailable => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not access run storage",
        ),
        ManagerError::DuplicateJob | ManagerError::JobLimitReached => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not complete the request",
        ),
    }
}

fn json_rejection(rejection: JsonRejection) -> Response {
    let status = rejection.status();
    match status {
        StatusCode::UNSUPPORTED_MEDIA_TYPE => error(
            status,
            "unsupported_media_type",
            "Content-Type must be application/json",
        ),
        StatusCode::PAYLOAD_TOO_LARGE => error(
            status,
            "body_too_large",
            "the request body exceeds the 65536 byte limit",
        ),
        StatusCode::UNPROCESSABLE_ENTITY => {
            error(status, "invalid_job", "the JSON job definition is invalid")
        }
        _ => error(
            status,
            "invalid_json",
            "the request body must be valid JSON",
        ),
    }
}

/// Serve HTTP until the process requests a bounded graceful shutdown.
pub async fn serve(
    listener: tokio::net::TcpListener,
    shutdown: Arc<AtomicBool>,
    manager: Arc<JobService>,
    worker: Arc<dyn WorkerPort>,
    request_timeout: Duration,
) -> io::Result<()> {
    let recovery_ready = Arc::new(AtomicBool::new(false));
    let ready_for_recovery = Arc::clone(&recovery_ready);
    let recovery_manager = Arc::clone(&manager);
    let recovery_worker = Arc::clone(&worker);
    tokio::spawn(async move {
        for attempt in 0..4 {
            let manager = Arc::clone(&recovery_manager);
            let worker = Arc::clone(&recovery_worker);
            let result =
                tokio::task::spawn_blocking(move || manager.reconcile(worker.as_ref())).await;
            if matches!(result, Ok(Ok(()))) {
                ready_for_recovery.store(true, Ordering::SeqCst);
                return;
            }
            if attempt < 3 {
                tokio::time::sleep(Duration::from_millis(50 << attempt)).await;
            }
        }
        eprintln!(
            "{{\"timestampMs\":{},\"severity\":\"error\",\"component\":\"manager\",\"event\":\"reconciliation_failed\"}}",
            timestamp_ms()
        );
    });
    let server_shutdown = Arc::clone(&shutdown);
    let server = axum::serve(
        listener,
        router_with_recovery(manager, worker, recovery_ready, request_timeout),
    )
    .with_graceful_shutdown(wait_for_shutdown(server_shutdown))
    .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result,
        _ = wait_for_shutdown(shutdown) => {
            match tokio::time::timeout(Duration::from_secs(5), &mut server).await {
                Ok(result) => result,
                Err(_) => {
                    eprintln!(
                        "{{\"timestampMs\":{},\"severity\":\"warn\",\"component\":\"api\",\"event\":\"shutdown_deadline_elapsed\",\"timeout_seconds\":5}}",
                        timestamp_ms()
                    );
                    Ok(())
                }
            }
        }
    }
}

async fn wait_for_shutdown(shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default()
}
