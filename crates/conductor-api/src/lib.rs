use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Path, Query, State, rejection::JsonRejection};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use conductor_core::{JobId, JobSpec, RunId, RunState, WorkerId};
use conductor_manager::{JobService, ManagerError, WorkerPort};
use serde::{Deserialize, Serialize};

const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;

#[derive(Serialize)]
struct JobCollection {
    jobs: Vec<JobSpec>,
}

#[derive(Serialize)]
struct RunCollection {
    runs: Vec<conductor_core::RunObservation>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RunFilters {
    job_id: Option<String>,
    state: Option<RunState>,
    limit: Option<usize>,
}

struct ApiState {
    manager: Arc<JobService>,
    worker: Arc<dyn WorkerPort>,
    worker_id: WorkerId,
    worker_capacity: usize,
    process_execution: bool,
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
    let descriptor = worker.descriptor();
    let state = Arc::new(ApiState {
        manager,
        worker,
        worker_id: descriptor.worker_id().clone(),
        worker_capacity: descriptor.capacity(),
        process_execution: descriptor.supports_process_execution(),
    });
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/jobs", post(create_job).get(list_jobs))
        .route("/v1/jobs/{id}", get(get_job).delete(delete_job))
        .route("/v1/jobs/{id}/runs", post(start_run))
        .route("/v1/runs", get(list_runs))
        .route("/v1/runs/{id}", get(get_run))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        r#"{"status":"ok"}"#,
    )
}

async fn create_job(
    State(state): State<Arc<ApiState>>,
    payload: Result<Json<JobSpec>, JsonRejection>,
) -> Response {
    let Json(job) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return json_rejection(rejection),
    };
    match state.manager.register(job.clone()) {
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

async fn list_jobs(State(state): State<Arc<ApiState>>) -> Response {
    match state.manager.list() {
        Ok(jobs) => (StatusCode::OK, Json(JobCollection { jobs })).into_response(),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not access job storage",
        ),
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
    match state.manager.get(&id) {
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
    match state.manager.delete(&id) {
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

async fn start_run(State(state): State<Arc<ApiState>>, Path(raw_job_id): Path<String>) -> Response {
    if !state.process_execution {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "worker_unavailable",
            "the local worker does not advertise process execution",
        );
    }
    let Ok(job_id) = JobId::new(raw_job_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    match state.manager.start_run(
        &job_id,
        &state.worker_id,
        state.worker_capacity,
        state.worker.as_ref(),
    ) {
        Ok(observation) => {
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
        Err(error) => manager_error(error),
    }
}

async fn get_run(State(state): State<Arc<ApiState>>, Path(raw_id): Path<String>) -> Response {
    let Ok(run_id) = RunId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_run_id",
            "the run ID is invalid",
        );
    };
    match state.manager.get_run(&run_id, state.worker.as_ref()) {
        Ok(Some(observation)) => (StatusCode::OK, Json(observation)).into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            "run_not_found",
            "the run does not exist",
        ),
        Err(error) => manager_error(error),
    }
}

async fn list_runs(
    State(state): State<Arc<ApiState>>,
    Query(filters): Query<RunFilters>,
) -> Response {
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
    match state
        .manager
        .list_runs(job_id.as_ref(), filters.state, filters.limit.unwrap_or(100))
    {
        Ok(runs) => {
            let mut refreshed = Vec::with_capacity(runs.len());
            for run in runs {
                match state.manager.get_run(run.run_id(), state.worker.as_ref()) {
                    Ok(Some(current)) => refreshed.push(current),
                    Ok(None) => {}
                    Err(error) => return manager_error(error),
                }
            }
            (StatusCode::OK, Json(RunCollection { runs: refreshed })).into_response()
        }
        Err(error) => manager_error(error),
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
        ManagerError::CapacityReached => error(
            StatusCode::TOO_MANY_REQUESTS,
            "worker_capacity_reached",
            "the local worker has no available capacity",
        ),
        ManagerError::TimeoutUnsupported => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "timeout_unsupported",
            "job timeouts are not supported by this worker yet",
        ),
        ManagerError::WorkerUnavailable => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "worker_unavailable",
            "the local worker could not confirm run acceptance",
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
) -> io::Result<()> {
    let server =
        axum::serve(listener, router(manager, worker)).with_graceful_shutdown(async move {
            while !shutdown.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
    match tokio::time::timeout(Duration::from_secs(5), server).await {
        Ok(result) => result,
        Err(_) => {
            eprintln!("{{\"event\":\"shutdown_deadline_elapsed\",\"timeout_seconds\":5}}");
            Ok(())
        }
    }
}
