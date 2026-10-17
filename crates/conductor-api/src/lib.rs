use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use conductor_core::{JobId, JobSpec};
use conductor_manager::{JobService, ManagerError};
use serde::Serialize;

const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;

#[derive(Serialize)]
struct JobCollection {
    jobs: Vec<JobSpec>,
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
pub fn router(manager: Arc<JobService>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/jobs", post(create_job).get(list_jobs))
        .route("/v1/jobs/{id}", get(get_job).delete(delete_job))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(manager)
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
    State(manager): State<Arc<JobService>>,
    payload: Result<Json<JobSpec>, JsonRejection>,
) -> Response {
    let Json(job) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return json_rejection(rejection),
    };
    match manager.register(job.clone()) {
        Ok(()) => (StatusCode::CREATED, Json(job)).into_response(),
        Err(ManagerError::DuplicateJob) => error(
            StatusCode::CONFLICT,
            "duplicate_job",
            "a job with this ID is already registered or was deleted",
        ),
        Err(ManagerError::JobLimitReached) => error(
            StatusCode::INSUFFICIENT_STORAGE,
            "job_limit_reached",
            "the in-memory job capacity has been reached",
        ),
        Err(ManagerError::StorageUnavailable) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not access job storage",
        ),
    }
}

async fn list_jobs(State(manager): State<Arc<JobService>>) -> Response {
    match manager.list() {
        Ok(jobs) => (StatusCode::OK, Json(JobCollection { jobs })).into_response(),
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not access job storage",
        ),
    }
}

async fn get_job(State(manager): State<Arc<JobService>>, Path(raw_id): Path<String>) -> Response {
    let Ok(id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    match manager.get(&id) {
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

async fn delete_job(
    State(manager): State<Arc<JobService>>,
    Path(raw_id): Path<String>,
) -> Response {
    let Ok(id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    match manager.delete(&id) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => error(
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
) -> io::Result<()> {
    let server = axum::serve(listener, router(manager)).with_graceful_shutdown(async move {
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
