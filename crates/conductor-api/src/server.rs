#[path = "dns.rs"]
pub mod dns;

use std::fmt::Write as FmtWrite;
use std::future::IntoFuture;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::Router;
use axum::body::{Body, to_bytes};
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
    DEFAULT_LOG_CHUNK_BYTES, DeploymentMutation, DeploymentStatus, DeploymentUpdateStrategy, JobId,
    JobRevisionDraft, JobRevisionId, JobSpec, LogStream, MAX_DEPLOYMENT_REPLICAS,
    MAX_LOG_CHUNK_BYTES, NetworkDomainId, OverlayNetworkDomainConfig, ReadRunLogs, RunId, RunState,
    ServiceIdentity, WorkerId, Workload,
};
use conductor_manager::{
    DEFAULT_PAGE_SIZE, ExecutionStoreError, JobService, MAX_PAGE_SIZE, ManagerError,
    OverlayNetworkError, QueryError, ServiceAttemptRecord as ManagerServiceAttemptRecord,
    ServiceCatalogEndpoint, WorkerPort,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
const MAX_REQUEST_HEADER_COUNT: usize = 64;
const MAX_REQUEST_HEADER_BYTES: usize = 16 * 1024;
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PENDING_STARTS: usize = 32;
const MAX_PENDING_BLOCKING_OPERATIONS: usize = 64;
const MAX_PENDING_CONTROL_OPERATIONS: usize = 8;
const MAX_ACTIVE_SERVICE_WATCHES: usize = 16;
const MAX_SERVICE_WATCH_SECONDS: u64 = 30;
const DEFAULT_WORKER_DRAIN_GRACE_SECONDS: u64 = 30;
const MAX_WORKER_DRAIN_GRACE_SECONDS: u64 = 300;

#[path = "structs/job_collection.rs"]
mod job_collection;
use job_collection::JobCollection;

#[path = "structs/run_collection.rs"]
mod run_collection;
use run_collection::RunCollection;

#[path = "structs/service_attempt_collection.rs"]
mod service_attempt_collection;
use service_attempt_collection::ServiceAttemptCollection;

#[path = "structs/service_endpoint_collection.rs"]
mod service_endpoint_collection;
use service_endpoint_collection::ServiceEndpointCollection;

#[path = "structs/service_watch_response.rs"]
mod service_watch_response;
use service_watch_response::ServiceWatchResponse;

#[path = "structs/service_watch_query.rs"]
mod service_watch_query;
use service_watch_query::ServiceWatchQuery;

#[path = "structs/service_endpoint.rs"]
mod service_endpoint;
use service_endpoint::ServiceEndpoint;

enum ServiceCatalogQueryError {
    Storage,
    Blocking(BlockingError),
}

#[path = "structs/run_logs_response.rs"]
mod run_logs_response;
use run_logs_response::RunLogsResponse;

#[path = "structs/run_filters.rs"]
mod run_filters;
use run_filters::RunFilters;

#[path = "structs/page_filters.rs"]
mod page_filters;
use page_filters::PageFilters;

#[path = "structs/run_log_filters.rs"]
mod run_log_filters;
use run_log_filters::RunLogFilters;

#[path = "structs/resolve_run_request.rs"]
mod resolve_run_request;
use resolve_run_request::ResolveRunRequest;

#[path = "structs/deploy_request.rs"]
mod deploy_request;
use deploy_request::DeployRequest;

#[path = "structs/scale_deployment_request.rs"]
mod scale_deployment_request;
use scale_deployment_request::ScaleDeploymentRequest;

#[path = "structs/rollback_request.rs"]
mod rollback_request;
use rollback_request::RollbackRequest;

#[path = "structs/worker_drain_request.rs"]
mod worker_drain_request;
use worker_drain_request::WorkerDrainRequest;

#[path = "structs/api_state.rs"]
mod api_state;
use api_state::ApiState;

#[path = "structs/audit_resource_id.rs"]
mod audit_resource_id;
use audit_resource_id::AuditResourceId;

#[derive(Clone, Copy)]
enum ApiRole {
    All,
    Manager,
}

#[path = "structs/api_readiness.rs"]
mod api_readiness;
use api_readiness::ApiReadiness;

impl ApiRole {
    fn health_body(self) -> &'static str {
        match self {
            Self::All => {
                r#"{"status":"ok","role":"all","capabilities":["local_process_execution"]}"#
            }
            Self::Manager => {
                r#"{"status":"ok","role":"manager","capabilities":["remote_worker_transport"]}"#
            }
        }
    }

    fn readiness_body(self, status: &'static str) -> &'static str {
        match (self, status) {
            (Self::All, "ready") => {
                r#"{"status":"ready","role":"all","capabilities":["local_process_execution"]}"#
            }
            (Self::All, "draining") => {
                r#"{"status":"draining","role":"all","capabilities":["local_process_execution"]}"#
            }
            (Self::All, "degraded") => {
                r#"{"status":"degraded","role":"all","capabilities":["local_process_execution"]}"#
            }
            (Self::All, _) => {
                r#"{"status":"recovering","role":"all","capabilities":["local_process_execution"]}"#
            }
            (Self::Manager, "ready") => {
                r#"{"status":"ready","role":"manager","capabilities":["remote_worker_transport"]}"#
            }
            (Self::Manager, "draining") => {
                r#"{"status":"draining","role":"manager","capabilities":["remote_worker_transport"]}"#
            }
            (Self::Manager, "degraded") => {
                r#"{"status":"degraded","role":"manager","capabilities":["remote_worker_transport"]}"#
            }
            (Self::Manager, _) => {
                r#"{"status":"recovering","role":"manager","capabilities":["remote_worker_transport"]}"#
            }
        }
    }
}

/// Authentication policy for the manager-facing HTTP API.
///
/// Local mode is intended for loopback-only development and combined deployments.
/// Remote mode requires an operator principal on every `/v1` route.
#[derive(Clone)]
pub enum ApiAccessPolicy {
    LocalMode,
    AuthenticatedOperator(Arc<dyn OperatorAuthenticator>),
}

#[path = "structs/manager_recovery.rs"]
mod manager_recovery;
pub use manager_recovery::ManagerRecovery;

/// Verifies operator credentials from HTTP headers without trusting request bodies.
pub trait OperatorAuthenticator: Send + Sync + 'static {
    fn authenticate(&self, headers: &HeaderMap) -> bool;
}

#[path = "structs/bearer_token_authenticator.rs"]
mod bearer_token_authenticator;
pub use bearer_token_authenticator::BearerTokenAuthenticator;

#[path = "structs/invalid_operator_token.rs"]
mod invalid_operator_token;
pub use invalid_operator_token::InvalidOperatorToken;

#[path = "structs/error_envelope.rs"]
mod error_envelope;
use error_envelope::ErrorEnvelope;

#[path = "structs/error_detail.rs"]
mod error_detail;
use error_detail::ErrorDetail;

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
    router_with_access_policy(
        manager,
        worker,
        DEFAULT_REQUEST_TIMEOUT,
        ApiAccessPolicy::LocalMode,
    )
}

/// Build the manager-facing HTTP application with an explicit local or operator policy.
pub fn router_with_access_policy(
    manager: Arc<JobService>,
    worker: Arc<dyn WorkerPort>,
    request_timeout: Duration,
    access_policy: ApiAccessPolicy,
) -> Router {
    router_with_recovery(
        manager,
        worker,
        Arc::new(AtomicBool::new(true)),
        request_timeout,
        access_policy,
        ApiReadiness {
            role: ApiRole::All,
            manager_workers_ready: None,
            recovery_workers: None,
        },
    )
}

fn router_with_recovery(
    manager: Arc<JobService>,
    worker: Arc<dyn WorkerPort>,
    recovery_ready: Arc<AtomicBool>,
    request_timeout: Duration,
    access_policy: ApiAccessPolicy,
    readiness: ApiReadiness,
) -> Router {
    let operator_authenticator = match &access_policy {
        ApiAccessPolicy::LocalMode => None,
        ApiAccessPolicy::AuthenticatedOperator(authenticator) => Some(Arc::clone(authenticator)),
    };
    let descriptor = worker.descriptor();
    let workers = readiness
        .recovery_workers
        .clone()
        .unwrap_or_else(|| vec![Arc::clone(&worker)]);
    let state = Arc::new(ApiState {
        manager,
        worker,
        workers,
        worker_id: descriptor.worker_id().clone(),
        worker_capacity: descriptor.capacity(),
        role: readiness.role,
        manager_workers_ready: readiness.manager_workers_ready,
        start_admission: Arc::new(Semaphore::new(MAX_PENDING_STARTS)),
        blocking_admission: Arc::new(Semaphore::new(MAX_PENDING_BLOCKING_OPERATIONS)),
        control_admission: Arc::new(Semaphore::new(MAX_PENDING_CONTROL_OPERATIONS)),
        service_watch_admission: Arc::new(Semaphore::new(MAX_ACTIVE_SERVICE_WATCHES)),
        recovery_ready,
        operator_authenticator,
    });
    let mut manager_routes = Router::new()
        .route("/v1/metrics", get(metrics))
        .route("/v1/jobs", post(create_job).get(list_jobs))
        .route("/v1/jobs/{id}", get(get_job).delete(delete_job))
        .route("/v1/jobs/{id}/revisions", post(create_revision))
        .route("/v1/jobs/{id}/revisions/{revision}", get(get_revision))
        .route(
            "/v1/jobs/{id}/deployment",
            get(get_deployment)
                .put(deploy_revision)
                .delete(delete_deployment),
        )
        .route("/v1/jobs/{id}/deployment/scale", post(scale_deployment))
        .route("/v1/jobs/{id}/deployment/suspend", post(suspend_deployment))
        .route("/v1/jobs/{id}/deployment/resume", post(resume_deployment))
        .route(
            "/v1/jobs/{id}/deployment/rollback",
            post(rollback_deployment),
        )
        .route("/v1/jobs/{id}/runs", post(start_run))
        .route("/v1/jobs/{id}/service-attempts", get(list_service_attempts))
        .route("/v1/jobs/{id}/endpoints", get(list_service_endpoints))
        .route(
            "/v1/services/{namespace}/{name}/endpoints",
            get(list_catalog_endpoints),
        )
        .route(
            "/v1/services/{namespace}/{name}/watch",
            get(watch_catalog_endpoints),
        )
        .route("/v1/workers", get(list_workers))
        .route("/v1/network-domains", post(create_network_domain))
        .route("/v1/network-domains/{id}", get(get_network_domain))
        .route("/v1/network-domains/{id}/peers", get(list_network_peers))
        .route(
            "/v1/network-domains/{id}/peers/{worker_id}",
            axum::routing::delete(revoke_network_peer),
        )
        .route("/v1/workers/{id}", get(get_worker))
        .route(
            "/v1/workers/{id}/drain",
            get(get_worker_drain)
                .post(request_worker_drain)
                .delete(cancel_worker_drain),
        )
        .route("/v1/runs", get(list_runs))
        .route("/v1/runs/{id}", get(get_run))
        .route("/v1/runs/{id}/logs", get(get_run_logs))
        .route("/v1/runs/{id}/stop", post(stop_run))
        .route("/v1/runs/{id}/resolve", post(resolve_run));
    manager_routes = manager_routes.layer(middleware::from_fn_with_state(
        Arc::clone(&state),
        authenticate_and_audit,
    ));
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .merge(manager_routes)
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

async fn create_revision(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<JobRevisionDraft>, JsonRejection>,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let Json(draft) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return json_rejection(rejection),
    };
    if draft.job_id() != &job_id {
        return error(
            StatusCode::BAD_REQUEST,
            "revision_job_mismatch",
            "the definition ID must match the job ID in the request path",
        );
    }
    let idempotency_key = match required_idempotency_key(&headers) {
        Ok(key) => key,
        Err(()) => return invalid_idempotency_key(),
    };
    let result = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        move || manager.create_revision(&draft, &idempotency_key)
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(revision) => (StatusCode::CREATED, Json(revision)).into_response(),
        Err(error_value) => execution_store_error(error_value),
    }
}

async fn get_revision(
    State(state): State<Arc<ApiState>>,
    Path((raw_id, raw_revision)): Path<(String, String)>,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let Ok(number) = raw_revision.parse::<u64>() else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_revision",
            "the revision must be a positive integer",
        );
    };
    let revision_id = match JobRevisionId::new(job_id, number) {
        Ok(id) => id,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_revision",
                "the revision must be a positive integer",
            );
        }
    };
    let result = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        move || manager.get_revision(&revision_id)
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(Some(revision)) => (StatusCode::OK, Json(revision)).into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            "revision_not_found",
            "the job revision does not exist",
        ),
        Err(error_value) => execution_store_error(error_value),
    }
}

async fn get_deployment(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let workers = query_worker_statuses(&state).await.ok();
    let result = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        move || match workers {
            Some(workers) if !workers.is_empty() => {
                manager.reconcile_deployment_placement(&job_id, &workers)
            }
            _ => manager.get_deployment(&job_id),
        }
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(Some(deployment)) => deployment_response(deployment, false),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            "deployment_not_found",
            "the job has no deployment",
        ),
        Err(error_value) => execution_store_error(error_value),
    }
}

async fn list_service_attempts(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let attempts = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        move || manager.list_service_attempts(&job_id)
    })
    .await
    {
        Ok(Ok(attempts)) => attempts,
        Ok(Err(_)) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "service attempts could not be read",
            );
        }
        Err(error_value) => return blocking_error(error_value),
    };
    Json(ServiceAttemptCollection {
        service_attempts: attempts.into_iter().map(service_attempt_response).collect(),
    })
    .into_response()
}

async fn list_service_endpoints(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let deployment = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        let job_id = job_id.clone();
        move || manager.get_deployment(&job_id)
    })
    .await
    {
        Ok(Ok(Some(deployment))) => deployment,
        Ok(Ok(None)) => {
            return Json(ServiceEndpointCollection { endpoints: vec![] }).into_response();
        }
        Ok(Err(_)) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "service deployment could not be read",
            );
        }
        Err(error_value) => return blocking_error(error_value),
    };
    let revision = match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        let revision_id = deployment.revision_id().clone();
        move || manager.get_revision(&revision_id)
    })
    .await
    {
        Ok(Ok(Some(revision))) => revision,
        Ok(Ok(None)) => {
            return Json(ServiceEndpointCollection { endpoints: vec![] }).into_response();
        }
        Ok(Err(_)) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "service revision could not be read",
            );
        }
        Err(error_value) => return blocking_error(error_value),
    };
    let Workload::Service { identity, .. } = revision.workload() else {
        return Json(ServiceEndpointCollection { endpoints: vec![] }).into_response();
    };
    match query_service_catalog(&state, identity).await {
        Ok(catalog) => Json(ServiceEndpointCollection {
            endpoints: catalog
                .into_iter()
                .filter(|endpoint| endpoint.job_id == job_id && endpoint.eligible)
                .map(service_endpoint_response)
                .collect(),
        })
        .into_response(),
        Err(error_value) => service_catalog_error_response(error_value),
    }
}

async fn list_catalog_endpoints(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> Response {
    let identity = match ServiceIdentity::in_namespace(namespace, name) {
        Ok(identity) => identity,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_service_identity",
                "the service namespace or name is invalid",
            );
        }
    };
    match query_service_catalog(&state, &identity).await {
        Ok(catalog) => Json(ServiceEndpointCollection {
            endpoints: catalog
                .into_iter()
                .filter(|endpoint| endpoint.eligible)
                .map(service_endpoint_response)
                .collect(),
        })
        .into_response(),
        Err(error_value) => service_catalog_error_response(error_value),
    }
}

async fn watch_catalog_endpoints(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    query: Result<Query<ServiceWatchQuery>, QueryRejection>,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(rejection) => return query_rejection(rejection),
    };
    let wait_seconds = query.wait_seconds.unwrap_or(MAX_SERVICE_WATCH_SECONDS);
    if !(1..=MAX_SERVICE_WATCH_SECONDS).contains(&wait_seconds) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_watch_timeout",
            "waitSeconds must be between 1 and 30",
        );
    }
    let identity = match ServiceIdentity::in_namespace(namespace, name) {
        Ok(identity) => identity,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_service_identity",
                "the service namespace or name is invalid",
            );
        }
    };
    let _watch_permit = match Arc::clone(&state.service_watch_admission).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let mut response = error(
                StatusCode::TOO_MANY_REQUESTS,
                "service_watch_capacity_reached",
                "all service watch slots are in use",
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            return response;
        }
    };

    let initial = match query_service_catalog(&state, &identity).await {
        Ok(endpoints) => endpoints,
        Err(error_value) => return service_catalog_error_response(error_value),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_seconds);
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let current = match query_service_catalog(&state, &identity).await {
            Ok(endpoints) => endpoints,
            Err(error_value) => return service_catalog_error_response(error_value),
        };
        let changed = current != initial;
        if changed || tokio::time::Instant::now() >= deadline {
            return Json(ServiceWatchResponse {
                endpoints: current
                    .into_iter()
                    .filter(|endpoint| endpoint.eligible)
                    .map(service_endpoint_response)
                    .collect(),
                changed,
            })
            .into_response();
        }
    }
}

async fn query_service_catalog(
    state: &Arc<ApiState>,
    identity: &ServiceIdentity,
) -> Result<Vec<ServiceCatalogEndpoint>, ServiceCatalogQueryError> {
    let mut statuses = Vec::with_capacity(state.workers.len());
    for worker in &state.workers {
        if let Ok(status) = query_single_worker_status(state, Arc::clone(worker)).await {
            statuses.push(status);
        }
    }
    let identity = identity.clone();
    match run_blocking(state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        move || manager.list_service_catalog(&identity, &statuses)
    })
    .await
    {
        Ok(Ok(endpoints)) => Ok(endpoints),
        Ok(Err(_)) => Err(ServiceCatalogQueryError::Storage),
        Err(error_value) => Err(ServiceCatalogQueryError::Blocking(error_value)),
    }
}

fn service_catalog_error_response(error_value: ServiceCatalogQueryError) -> Response {
    match error_value {
        ServiceCatalogQueryError::Storage => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "service catalog could not be read",
        ),
        ServiceCatalogQueryError::Blocking(error_value) => blocking_error(error_value),
    }
}

fn service_endpoint_response(endpoint: ServiceCatalogEndpoint) -> ServiceEndpoint {
    ServiceEndpoint {
        job_id: endpoint.job_id,
        namespace: endpoint.service_identity.namespace().to_owned(),
        service_name: endpoint.service_identity.name().to_owned(),
        service_instance_id: endpoint.service_instance_id,
        allocation_id: endpoint.allocation_id,
        run_id: endpoint.run_id,
        worker_id: endpoint.worker_id,
        name: endpoint.port.name().to_owned(),
        protocol: endpoint.port.protocol(),
        advertised_address: endpoint.port.advertised_address().to_owned(),
        port: endpoint.port.port(),
        ready: endpoint.readiness == Some(conductor_core::ReadinessState::Ready),
        worker_fresh: endpoint.worker_fresh,
        worker_draining: endpoint.worker_draining,
    }
}

fn worker_drain_response(
    status: conductor_manager::WorkerDrainStatus,
) -> crate::contracts::WorkerDrainStatus {
    crate::contracts::WorkerDrainStatus {
        worker_id: status.worker_id,
        state: status.state.to_owned(),
        requested_at_ms: status.requested_at_ms,
        deadline_at_ms: status.deadline_at_ms,
        completed_at_ms: status.completed_at_ms,
        active_runs: status.active_runs,
        blocked_reason: status.blocked_reason.map(str::to_owned),
    }
}

fn service_attempt_response(
    attempt: ManagerServiceAttemptRecord,
) -> crate::contracts::ServiceAttemptRecord {
    crate::contracts::ServiceAttemptRecord {
        service_instance_id: attempt.service_instance_id,
        replica_index: attempt.replica_index,
        current_allocation_id: attempt.current_allocation_id,
        allocation: attempt.allocation.map(|record| {
            let allocation = record.allocation();
            crate::contracts::AllocationRecord {
                allocation: crate::contracts::Allocation {
                    id: allocation.id().clone(),
                    job_revision_id: allocation.job_revision_id().clone(),
                    service_instance_id: allocation.service_instance_id().cloned(),
                    assigned_worker_id: allocation.assigned_worker_id().cloned(),
                    assignment_generation: allocation.assignment_generation(),
                    state: allocation.state(),
                    reservation: allocation.reservation().clone(),
                },
                resource_domain_id: record.resource_domain_id().cloned(),
            }
        }),
        current_run_id: attempt.current_run_id,
        current_run: attempt.current_run,
        restart_count: attempt.restart_count,
        next_start_at_ms: attempt.next_start_at_ms,
        terminal_failure: attempt.terminal_failure,
        service_ports: attempt.service_ports,
        service_ports_ready: attempt.service_ports_ready,
    }
}

async fn deploy_revision(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<DeployRequest>, JsonRejection>,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return json_rejection(rejection),
    };
    if payload.replicas > MAX_DEPLOYMENT_REPLICAS {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_replica_count",
            "replicas must be between 0 and 10000",
        );
    }
    let strategy = match DeploymentUpdateStrategy::new(
        payload.strategy.max_surge(),
        payload.strategy.max_unavailable(),
        payload.strategy.progress_deadline_seconds(),
    ) {
        Ok(strategy) => strategy,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_rollout_strategy",
                "maxSurge must be zero, maxUnavailable must be positive, and progressDeadlineSeconds must be between 1 and 86400",
            );
        }
    };
    let expected_generation = match deployment_precondition(&headers, true) {
        Ok(generation) => generation,
        Err(error_value) => return deployment_precondition_error(error_value, true),
    };
    let key = match required_idempotency_key(&headers) {
        Ok(key) => key,
        Err(()) => return invalid_idempotency_key(),
    };
    mutate_deployment(
        &state,
        job_id,
        expected_generation,
        DeploymentMutation::Deploy {
            revision: payload.revision,
            replicas: payload.replicas,
            strategy,
        },
        key,
    )
    .await
}

async fn scale_deployment(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<ScaleDeploymentRequest>, JsonRejection>,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return json_rejection(rejection),
    };
    if payload.replicas > MAX_DEPLOYMENT_REPLICAS {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_replica_count",
            "replicas must be between 0 and 10000",
        );
    }
    mutate_deployment_with_headers(
        &state,
        job_id,
        &headers,
        DeploymentMutation::Scale {
            replicas: payload.replicas,
        },
        false,
    )
    .await
}

async fn suspend_deployment(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    mutate_deployment_with_headers(&state, job_id, &headers, DeploymentMutation::Suspend, false)
        .await
}

async fn resume_deployment(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    mutate_deployment_with_headers(&state, job_id, &headers, DeploymentMutation::Resume, false)
        .await
}

async fn rollback_deployment(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<RollbackRequest>, JsonRejection>,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return json_rejection(rejection),
    };
    mutate_deployment_with_headers(
        &state,
        job_id,
        &headers,
        DeploymentMutation::Rollback {
            revision: payload.revision,
        },
        false,
    )
    .await
}

async fn delete_deployment(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Ok(job_id) = JobId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
            "the job ID is invalid",
        );
    };
    mutate_deployment_with_headers(&state, job_id, &headers, DeploymentMutation::Delete, false)
        .await
}

async fn mutate_deployment_with_headers(
    state: &Arc<ApiState>,
    job_id: JobId,
    headers: &HeaderMap,
    mutation: DeploymentMutation,
    allow_create: bool,
) -> Response {
    let expected_generation = match deployment_precondition(headers, allow_create) {
        Ok(generation) => generation,
        Err(error_value) => return deployment_precondition_error(error_value, allow_create),
    };
    let key = match required_idempotency_key(headers) {
        Ok(key) => key,
        Err(()) => return invalid_idempotency_key(),
    };
    mutate_deployment(state, job_id, expected_generation, mutation, key).await
}

async fn mutate_deployment(
    state: &Arc<ApiState>,
    job_id: JobId,
    expected_generation: Option<u64>,
    mutation: DeploymentMutation,
    idempotency_key: String,
) -> Response {
    let workers = match query_worker_statuses(state).await {
        Ok(workers) => workers,
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "worker_status_unavailable",
                "worker status could not be read; deployment intent was not changed",
            );
        }
    };
    let result = match run_blocking(state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        move || {
            manager.mutate_deployment_with_workers(
                &job_id,
                expected_generation,
                &mutation,
                &idempotency_key,
                &workers,
            )
        }
    })
    .await
    {
        Ok(result) => result,
        Err(error_value) => return blocking_error(error_value),
    };
    match result {
        Ok(deployment) => deployment_response(deployment, true),
        Err(error_value) => execution_store_error(error_value),
    }
}

fn deployment_response(
    deployment: conductor_core::JobDeployment,
    accepted_mutation: bool,
) -> Response {
    let status = if accepted_mutation
        && matches!(
            deployment.status(),
            DeploymentStatus::Pending | DeploymentStatus::Deleting
        ) {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    let etag = HeaderValue::from_str(&format!("\"{}\"", deployment.generation()))
        .unwrap_or_else(|_| HeaderValue::from_static("\"1\""));
    let mut response = (status, Json(deployment)).into_response();
    response.headers_mut().insert(header::ETAG, etag);
    response
}

fn required_idempotency_key(headers: &HeaderMap) -> Result<String, ()> {
    match parse_idempotency_key(headers) {
        Ok(Some(key)) => Ok(key.to_owned()),
        Ok(None) | Err(()) => Err(()),
    }
}

fn invalid_idempotency_key() -> Response {
    error(
        StatusCode::BAD_REQUEST,
        "invalid_idempotency_key",
        "Idempotency-Key is required and must contain 1-128 valid ASCII characters",
    )
}

#[derive(Clone, Copy)]
enum PreconditionError {
    Invalid,
    Missing,
}

fn deployment_precondition(
    headers: &HeaderMap,
    allow_create: bool,
) -> Result<Option<u64>, PreconditionError> {
    let if_match = headers.get_all(header::IF_MATCH).iter().collect::<Vec<_>>();
    let if_none_match = headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .collect::<Vec<_>>();
    if if_match.len() > 1
        || if_none_match.len() > 1
        || (!if_match.is_empty() && !if_none_match.is_empty())
    {
        return Err(PreconditionError::Invalid);
    }
    if let Some(value) = if_none_match.first() {
        if allow_create && value.as_bytes() == b"*" {
            return Ok(None);
        }
        return Err(PreconditionError::Invalid);
    }
    if let Some(value) = if_match.first() {
        let Ok(value) = value.to_str() else {
            return Err(PreconditionError::Invalid);
        };
        if let Some(generation) = value
            .strip_prefix('\"')
            .and_then(|value| value.strip_suffix('\"'))
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|generation| *generation > 0)
        {
            return Ok(Some(generation));
        }
        return Err(PreconditionError::Invalid);
    }
    Err(PreconditionError::Missing)
}

fn deployment_precondition_error(error_value: PreconditionError, allow_create: bool) -> Response {
    match error_value {
        PreconditionError::Invalid => error(
            StatusCode::BAD_REQUEST,
            "invalid_precondition",
            "supply exactly one valid deployment generation precondition",
        ),
        PreconditionError::Missing if allow_create => error(
            StatusCode::PRECONDITION_REQUIRED,
            "precondition_required",
            "supply If-None-Match: * to create or If-Match to update the deployment",
        ),
        PreconditionError::Missing => error(
            StatusCode::PRECONDITION_REQUIRED,
            "precondition_required",
            "a current deployment If-Match ETag is required",
        ),
    }
}

fn execution_store_error(error_value: ExecutionStoreError) -> Response {
    match error_value {
        ExecutionStoreError::JobLimitReached => error(
            StatusCode::INSUFFICIENT_STORAGE,
            "job_limit_reached",
            "the manager has reached its active job limit",
        ),
        ExecutionStoreError::JobNotFound => error(
            StatusCode::NOT_FOUND,
            "job_not_found",
            "the job does not exist",
        ),
        ExecutionStoreError::RevisionNotFound => error(
            StatusCode::NOT_FOUND,
            "revision_not_found",
            "the job revision does not exist",
        ),
        ExecutionStoreError::Conflict => error(
            StatusCode::CONFLICT,
            "deployment_conflict",
            "the deployment mutation conflicts with its current state",
        ),
        ExecutionStoreError::PreconditionFailed => error(
            StatusCode::PRECONDITION_FAILED,
            "precondition_failed",
            "the deployment generation does not match the supplied precondition",
        ),
        ExecutionStoreError::IdempotencyConflict => error(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "the Idempotency-Key was already used for a different request",
        ),
        ExecutionStoreError::InvalidRequest => error(
            StatusCode::BAD_REQUEST,
            "invalid_execution_request",
            "the revision or deployment request is invalid",
        ),
        ExecutionStoreError::StorageUnavailable => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not access execution storage",
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
    // Liveness and readiness probes do not perform blocking work. Keep them
    // available while manager requests are saturated, even when the configured
    // processing deadline is intentionally short.
    if matches!(request.uri().path(), "/healthz" | "/readyz") {
        return next.run(request).await;
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

async fn authenticate_and_audit(
    State(state): State<Arc<ApiState>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let principal = if let Some(authenticator) = &state.operator_authenticator {
        if !authenticator.authenticate(request.headers()) {
            let (_, body) = request.into_parts();
            if to_bytes(body, MAX_REQUEST_BODY_BYTES).await.is_err() {
                return error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request_body_too_large",
                    "the request body exceeds the configured byte limit",
                );
            }
            let mut response = error(
                StatusCode::UNAUTHORIZED,
                "operator_authentication_required",
                "a valid operator bearer token is required",
            );
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"conductor\""),
            );
            return response;
        }
        "operator"
    } else {
        "local_operator"
    };
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let descriptor = audit_descriptor(&method, &path)
        .map(|(action, resource_type, id)| (action, resource_type, id.map(str::to_owned)));
    let Some(descriptor) = descriptor else {
        return next.run(request).await;
    };
    // Buffer only bounded mutation bodies before detaching the operation. This
    // lets incomplete clients time out cleanly while completed requests retain
    // the API's existing background-completion behavior.
    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, MAX_REQUEST_BODY_BYTES).await {
        Ok(body) => body,
        Err(_) => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_body_too_large",
                "the request body exceeds the configured byte limit",
            );
        }
    };
    let request = axum::extract::Request::from_parts(parts, Body::from(body));
    let manager = Arc::clone(&state.manager);
    // Keep the handler alive after an HTTP timeout so manager operations that
    // have entered bounded blocking admission can finish and be audited.
    match tokio::spawn(async move {
        let response = next.run(request).await;
        let status = response.status();
        if status != StatusCode::TOO_MANY_REQUESTS {
            let outcome = if status.is_success() {
                "success"
            } else {
                "failure"
            };
            let resource_id = descriptor.2.or_else(|| {
                response
                    .extensions()
                    .get::<AuditResourceId>()
                    .map(|target| target.0.clone())
            });
            let manager = Arc::clone(&manager);
            let audit_result = tokio::task::spawn_blocking(move || {
                manager.record_audit_event(
                    principal,
                    descriptor.0,
                    descriptor.1,
                    resource_id.as_deref(),
                    outcome,
                    status.as_u16(),
                )
            })
            .await;
            if !matches!(audit_result, Ok(Ok(()))) {
                eprintln!(
                    "{{\"timestampMs\":{},\"severity\":\"error\",\"component\":\"audit\",\"event\":\"audit_write_failed\"}}",
                    timestamp_ms()
                );
            }
        }
        response
    })
    .await
    {
        Ok(response) => response,
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the request could not be completed",
        ),
    }
}

fn audit_descriptor<'a>(
    method: &axum::http::Method,
    path: &'a str,
) -> Option<(&'static str, &'static str, Option<&'a str>)> {
    let segments = path.strip_prefix("/v1/")?.split('/').collect::<Vec<_>>();
    match (segments.as_slice(), method.as_str()) {
        (["jobs"], "POST") => Some(("job.create", "job", None)),
        (["jobs", id], "DELETE") => Some(("job.delete", "job", Some(*id))),
        (["jobs", id, "revisions"], "POST") => Some(("job.revision.create", "job", Some(*id))),
        (["jobs", id, "deployment"], "PUT") => Some(("deployment.update", "deployment", Some(*id))),
        (["jobs", id, "deployment"], "DELETE") => {
            Some(("deployment.delete", "deployment", Some(*id)))
        }
        (["jobs", id, "deployment", "scale"], "POST") => {
            Some(("deployment.scale", "deployment", Some(*id)))
        }
        (["jobs", id, "deployment", "suspend"], "POST") => {
            Some(("deployment.suspend", "deployment", Some(*id)))
        }
        (["jobs", id, "deployment", "resume"], "POST") => {
            Some(("deployment.resume", "deployment", Some(*id)))
        }
        (["jobs", id, "deployment", "rollback"], "POST") => {
            Some(("deployment.rollback", "deployment", Some(*id)))
        }
        (["jobs", id, "runs"], "POST") => Some(("run.start", "job", Some(*id))),
        (["workers", id, "drain"], "POST") => Some(("worker.drain.request", "worker", Some(*id))),
        (["workers", id, "drain"], "DELETE") => Some(("worker.drain.cancel", "worker", Some(*id))),
        (["network-domains"], "POST") => Some(("worker.overlay.domain.create", "worker", None)),
        (["network-domains", _id, "peers", worker_id], "DELETE") => {
            Some(("worker.overlay.peer.revoke", "worker", Some(*worker_id)))
        }
        (["runs", id, "stop"], "POST") => Some(("run.stop", "run", Some(*id))),
        (["runs", id, "resolve"], "POST") => Some(("run.resolve", "run", Some(*id))),
        _ => None,
    }
}

async fn metrics(State(state): State<Arc<ApiState>>) -> Response {
    let manager = Arc::clone(&state.manager);
    let operational = match tokio::task::spawn_blocking(move || manager.operational_metrics()).await
    {
        Ok(Ok(metrics)) => metrics,
        _ => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "metrics_unavailable",
                "operational metrics are temporarily unavailable",
            );
        }
    };
    let mut workers_fresh = 0_u64;
    let mut workers_stale = 0_u64;
    let mut workers_freshness_unknown = 0_u64;
    let mut workers_available = 0_u64;
    let mut workers_unavailable = 0_u64;
    let mut workers_degraded = 0_u64;
    let mut workers_draining = 0_u64;
    for worker in &state.workers {
        match query_single_worker_status(&state, Arc::clone(worker)).await {
            Ok(status) => {
                if status.inventory().is_some() {
                    workers_fresh += u64::from(status.fresh());
                    workers_stale += u64::from(!status.fresh());
                } else {
                    workers_freshness_unknown += 1;
                }
                workers_available += u64::from(status.available());
                workers_unavailable += u64::from(!status.available());
                workers_degraded += u64::from(status.degraded());
                workers_draining += u64::from(status.draining());
            }
            Err(_) => workers_unavailable += 1,
        }
    }
    let reconcile = state.manager.reconcile_metrics();
    let bounds = ["0.005", "0.025", "0.1", "0.5", "1", "5", "+Inf"];
    let mut cumulative = 0_u64;
    let mut body = String::with_capacity(1_536);
    let _ = writeln!(
        body,
        "# HELP conductor_deployments Current deployments by lifecycle status."
    );
    let _ = writeln!(body, "# TYPE conductor_deployments gauge");
    for (status, count) in [
        ("pending", operational.deployments_pending),
        ("ready", operational.deployments_ready),
        ("suspended", operational.deployments_suspended),
        ("deleting", operational.deployments_deleting),
    ] {
        let _ = writeln!(body, "conductor_deployments{{status=\"{status}\"}} {count}");
    }
    let _ = writeln!(
        body,
        "# HELP conductor_pending_work Current durable allocations awaiting placement and deployment blockers."
    );
    let _ = writeln!(body, "# TYPE conductor_pending_work gauge");
    let _ = writeln!(
        body,
        "conductor_pending_work{{kind=\"allocations\"}} {}",
        operational.pending_allocations
    );
    let pending_reasons = [
        "no_eligible_worker",
        "worker_capacity_exhausted",
        "worker_disconnected",
        "required_protection_unavailable",
        "shared_host_capacity_exhausted",
        "placement_pending",
        "service_starting",
        "service_restart_backoff",
        "service_restart_budget_exhausted",
        "service_attempt_unresolved",
        "service_readiness_pending",
        "rollout_progress_deadline_exceeded",
    ];
    for (reason, count) in pending_reasons.into_iter().zip(operational.pending_reasons) {
        let _ = writeln!(
            body,
            "conductor_pending_work{{kind=\"deployment\",reason=\"{reason}\"}} {count}"
        );
    }
    let _ = writeln!(
        body,
        "# HELP conductor_service_attempts Current service instances by retry or terminal-failure state."
    );
    let _ = writeln!(body, "# TYPE conductor_service_attempts gauge");
    let _ = writeln!(
        body,
        "conductor_service_attempts{{state=\"retried\"}} {}",
        operational.retried_service_attempts
    );
    let _ = writeln!(
        body,
        "conductor_service_attempts{{state=\"terminal_failure\"}} {}",
        operational.terminal_service_attempts
    );
    let _ = writeln!(
        body,
        "# HELP conductor_workers Current worker connectivity and discovery snapshot freshness."
    );
    let _ = writeln!(body, "# TYPE conductor_workers gauge");
    for (state_name, count) in [
        ("fresh", workers_fresh),
        ("stale", workers_stale),
        ("freshness_unknown", workers_freshness_unknown),
        ("available", workers_available),
        ("unavailable", workers_unavailable),
        ("degraded", workers_degraded),
        ("draining", workers_draining),
    ] {
        let _ = writeln!(body, "conductor_workers{{state=\"{state_name}\"}} {count}");
    }
    let _ = writeln!(
        body,
        "# HELP conductor_discovery_service_owners Current allocated service discovery registrations."
    );
    let _ = writeln!(body, "# TYPE conductor_discovery_service_owners gauge");
    let _ = writeln!(
        body,
        "conductor_discovery_service_owners {}",
        operational.service_catalog_owners
    );
    let _ = writeln!(
        body,
        "# HELP conductor_manager_reconcile_total Service reconciliation passes, including scheduling and placement."
    );
    let _ = writeln!(body, "# TYPE conductor_manager_reconcile_total counter");
    let _ = writeln!(body, "conductor_manager_reconcile_total {}", reconcile.runs);
    let _ = writeln!(
        body,
        "# HELP conductor_manager_reconcile_failures_total Failed service reconciliation passes."
    );
    let _ = writeln!(
        body,
        "# TYPE conductor_manager_reconcile_failures_total counter"
    );
    let _ = writeln!(
        body,
        "conductor_manager_reconcile_failures_total {}",
        reconcile.failures
    );
    let _ = writeln!(
        body,
        "# HELP conductor_manager_reconcile_duration_seconds Reconciliation pass duration histogram."
    );
    let _ = writeln!(
        body,
        "# TYPE conductor_manager_reconcile_duration_seconds histogram"
    );
    for (index, bucket) in reconcile.duration_buckets.iter().enumerate() {
        cumulative = cumulative.saturating_add(*bucket);
        let _ = writeln!(
            body,
            "conductor_manager_reconcile_duration_seconds_bucket{{le=\"{}\"}} {cumulative}",
            bounds[index]
        );
    }
    let _ = writeln!(
        body,
        "conductor_manager_reconcile_duration_seconds_sum {:.9}",
        reconcile.duration_sum_ns as f64 / 1_000_000_000.0
    );
    let _ = writeln!(
        body,
        "conductor_manager_reconcile_duration_seconds_count {}",
        reconcile.runs
    );
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )],
        body,
    )
        .into_response()
}

async fn healthz(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        state.role.health_body(),
    )
}

async fn readyz(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let workers_ready = state
        .manager_workers_ready
        .as_ref()
        .is_none_or(|ready| ready.load(Ordering::SeqCst));
    if state.recovery_ready.load(Ordering::SeqCst)
        && workers_ready
        && !state.worker.is_draining()
        && !state.worker.is_degraded()
    {
        (StatusCode::OK, state.role.readiness_body("ready"))
    } else if state.worker.is_draining() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            state.role.readiness_body("draining"),
        )
    } else if !workers_ready || state.worker.is_degraded() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            state.role.readiness_body("degraded"),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            state.role.readiness_body("recovering"),
        )
    }
}

async fn list_workers(State(state): State<Arc<ApiState>>) -> Response {
    match query_worker_statuses(&state).await {
        Ok(statuses) => (StatusCode::OK, Json(statuses)).into_response(),
        Err(error) => manager_error(error),
    }
}

async fn create_network_domain(
    State(state): State<Arc<ApiState>>,
    payload: Result<Json<OverlayNetworkDomainConfig>, JsonRejection>,
) -> Response {
    let Json(config) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return json_rejection(rejection),
    };
    let manager = Arc::clone(&state.manager);
    let input = config.clone();
    match run_blocking(&state, BlockingClass::Control, move || {
        manager.create_overlay_network_domain(&input, manager_timestamp_ms())
    })
    .await
    {
        Ok(Ok(())) => {
            let mut response = (StatusCode::CREATED, Json(config.clone())).into_response();
            response
                .extensions_mut()
                .insert(AuditResourceId(config.network_domain_id.to_string()));
            response
        }
        Ok(Err(problem)) => overlay_network_error(problem),
        Err(problem) => blocking_error(problem),
    }
}

async fn get_network_domain(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
) -> Response {
    let Ok(network_domain_id) = NetworkDomainId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_network_domain_id",
            "the network domain ID is invalid",
        );
    };
    let manager = Arc::clone(&state.manager);
    match run_blocking(&state, BlockingClass::General, move || {
        manager.get_overlay_network_domain(&network_domain_id)
    })
    .await
    {
        Ok(Ok(Some(domain))) => Json(domain).into_response(),
        Ok(Ok(None)) => error(
            StatusCode::NOT_FOUND,
            "network_domain_not_found",
            "the network domain does not exist",
        ),
        Ok(Err(problem)) => overlay_network_error(problem),
        Err(problem) => blocking_error(problem),
    }
}

async fn list_network_peers(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
) -> Response {
    let Ok(network_domain_id) = NetworkDomainId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_network_domain_id",
            "the network domain ID is invalid",
        );
    };
    let manager = Arc::clone(&state.manager);
    match run_blocking(&state, BlockingClass::General, move || {
        manager.list_overlay_peer_diagnostics(&network_domain_id, 128, manager_timestamp_ms())
    })
    .await
    {
        Ok(Ok(peers)) => Json(peers).into_response(),
        Ok(Err(problem)) => overlay_network_error(problem),
        Err(problem) => blocking_error(problem),
    }
}

async fn revoke_network_peer(
    State(state): State<Arc<ApiState>>,
    Path((raw_domain_id, raw_worker_id)): Path<(String, String)>,
) -> Response {
    let Ok(network_domain_id) = NetworkDomainId::new(raw_domain_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_network_domain_id",
            "the network domain ID is invalid",
        );
    };
    let Ok(worker_id) = WorkerId::new(raw_worker_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_worker_id",
            "the worker ID is invalid",
        );
    };
    let manager = Arc::clone(&state.manager);
    match run_blocking(&state, BlockingClass::Control, move || {
        manager.revoke_overlay_peer(&network_domain_id, &worker_id, manager_timestamp_ms())
    })
    .await
    {
        Ok(Ok(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(problem)) => overlay_network_error(problem),
        Err(problem) => blocking_error(problem),
    }
}

fn overlay_network_error(problem: OverlayNetworkError) -> Response {
    match problem {
        OverlayNetworkError::DomainExists
        | OverlayNetworkError::AddressPoolOverlap
        | OverlayNetworkError::EndpointIdentityConflict
        | OverlayNetworkError::EndpointIdentityReplay
        | OverlayNetworkError::PeerQuarantined
        | OverlayNetworkError::StaleGeneration
        | OverlayNetworkError::ObservationConflict => error(
            StatusCode::CONFLICT,
            "overlay_state_conflict",
            "the requested overlay state conflicts with current manager state",
        ),
        OverlayNetworkError::DomainNotFound | OverlayNetworkError::PeerNotFound => error(
            StatusCode::NOT_FOUND,
            "overlay_resource_not_found",
            "the overlay resource does not exist",
        ),
        OverlayNetworkError::InvalidAddressPool
        | OverlayNetworkError::InvalidLease
        | OverlayNetworkError::InvalidDirectCandidates => error(
            StatusCode::BAD_REQUEST,
            "invalid_overlay_request",
            "the overlay request is invalid",
        ),
        OverlayNetworkError::AddressPoolExhausted | OverlayNetworkError::PeerLimitReached => error(
            StatusCode::CONFLICT,
            "overlay_capacity_reached",
            "the overlay address or peer capacity is exhausted",
        ),
        OverlayNetworkError::IdentityUnavailable => error(
            StatusCode::FORBIDDEN,
            "worker_identity_unavailable",
            "the worker identity is not active",
        ),
        OverlayNetworkError::StorageUnavailable => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "the manager could not access overlay state",
        ),
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
    let Some(worker) = state
        .workers
        .iter()
        .find(|worker| worker.descriptor().worker_id() == &id)
        .cloned()
    else {
        return error(
            StatusCode::NOT_FOUND,
            "worker_not_found",
            "the worker does not exist",
        );
    };
    match query_single_worker_status(&state, worker).await {
        Ok(status) => (StatusCode::OK, Json(status)).into_response(),
        Err(error) => manager_error(error),
    }
}

async fn request_worker_drain(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
    payload: Result<Json<WorkerDrainRequest>, JsonRejection>,
) -> Response {
    let Ok(worker_id) = WorkerId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_worker_id",
            "the worker ID is invalid",
        );
    };
    if !state
        .workers
        .iter()
        .any(|worker| worker.descriptor().worker_id() == &worker_id)
    {
        return error(
            StatusCode::NOT_FOUND,
            "worker_not_found",
            "the worker does not exist",
        );
    }
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return json_rejection(rejection),
    };
    let grace_period_seconds = payload
        .grace_period_seconds
        .unwrap_or(DEFAULT_WORKER_DRAIN_GRACE_SECONDS);
    if !(1..=MAX_WORKER_DRAIN_GRACE_SECONDS).contains(&grace_period_seconds) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_drain_grace_period",
            "gracePeriodSeconds must be between 1 and 300",
        );
    }
    let Ok(grace_period_ms) = i64::try_from(grace_period_seconds.saturating_mul(1_000)) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_drain_grace_period",
            "gracePeriodSeconds must be between 1 and 300",
        );
    };
    match run_blocking(&state, BlockingClass::Control, {
        let manager = Arc::clone(&state.manager);
        move || manager.request_worker_drain(&worker_id, grace_period_ms)
    })
    .await
    {
        Ok(Ok(status)) => {
            (StatusCode::ACCEPTED, Json(worker_drain_response(status))).into_response()
        }
        Ok(Err(error_value)) => manager_error(error_value),
        Err(error_value) => blocking_error(error_value),
    }
}

async fn get_worker_drain(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
) -> Response {
    let Ok(worker_id) = WorkerId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_worker_id",
            "the worker ID is invalid",
        );
    };
    match run_blocking(&state, BlockingClass::General, {
        let manager = Arc::clone(&state.manager);
        move || manager.worker_drain_status(&worker_id)
    })
    .await
    {
        Ok(Ok(Some(status))) => Json(worker_drain_response(status)).into_response(),
        Ok(Ok(None)) => error(
            StatusCode::NOT_FOUND,
            "worker_drain_not_found",
            "no drain has been requested for this worker",
        ),
        Ok(Err(error_value)) => manager_error(error_value),
        Err(error_value) => blocking_error(error_value),
    }
}

async fn cancel_worker_drain(
    State(state): State<Arc<ApiState>>,
    Path(raw_id): Path<String>,
) -> Response {
    let Ok(worker_id) = WorkerId::new(raw_id) else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_worker_id",
            "the worker ID is invalid",
        );
    };
    let status = match run_blocking(&state, BlockingClass::Control, {
        let manager = Arc::clone(&state.manager);
        let worker_id = worker_id.clone();
        move || manager.worker_drain_status(&worker_id)
    })
    .await
    {
        Ok(Ok(Some(status))) => status,
        Ok(Ok(None)) => {
            return error(
                StatusCode::NOT_FOUND,
                "worker_drain_not_found",
                "no drain has been requested for this worker",
            );
        }
        Ok(Err(error_value)) => return manager_error(error_value),
        Err(error_value) => return blocking_error(error_value),
    };
    if status.state != "completed" {
        return error(
            StatusCode::CONFLICT,
            "worker_drain_in_progress",
            "a worker drain can be cleared only after it completes",
        );
    }
    match run_blocking(&state, BlockingClass::Control, {
        let manager = Arc::clone(&state.manager);
        move || manager.cancel_worker_drain(&worker_id)
    })
    .await
    {
        Ok(Ok(true)) => StatusCode::NO_CONTENT.into_response(),
        Ok(Ok(false)) => error(
            StatusCode::CONFLICT,
            "worker_drain_in_progress",
            "the worker drain is still active",
        ),
        Ok(Err(error_value)) => manager_error(error_value),
        Err(error_value) => blocking_error(error_value),
    }
}

async fn query_worker_statuses(
    state: &Arc<ApiState>,
) -> Result<Vec<conductor_core::WorkerStatus>, ManagerError> {
    let mut statuses = Vec::with_capacity(state.workers.len());
    for worker in &state.workers {
        statuses.push(query_single_worker_status(state, Arc::clone(worker)).await?);
    }
    Ok(statuses)
}

async fn query_single_worker_status(
    state: &Arc<ApiState>,
    worker: Arc<dyn WorkerPort>,
) -> Result<conductor_core::WorkerStatus, ManagerError> {
    let manager = Arc::clone(&state.manager);
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
        Ok(()) => {
            let mut response = (StatusCode::CREATED, Json(job.clone())).into_response();
            response
                .extensions_mut()
                .insert(AuditResourceId(job.id().as_str().to_owned()));
            response
        }
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
                data_encoding: "base64".to_owned(),
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
        ManagerError::InvalidDrainRequest => error(
            StatusCode::BAD_REQUEST,
            "invalid_worker_drain_request",
            "the worker drain request is invalid",
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

fn query_rejection(_rejection: QueryRejection) -> Response {
    error(
        StatusCode::BAD_REQUEST,
        "invalid_query",
        "query parameters are invalid",
    )
}

/// Serve HTTP until the process requests a bounded graceful shutdown.
pub async fn serve(
    listener: tokio::net::TcpListener,
    shutdown: Arc<AtomicBool>,
    manager: Arc<JobService>,
    worker: Arc<dyn WorkerPort>,
    request_timeout: Duration,
) -> io::Result<()> {
    serve_with_access_policy(
        listener,
        shutdown,
        manager,
        worker,
        request_timeout,
        ApiAccessPolicy::LocalMode,
    )
    .await
}

/// Serve the manager-facing API with the selected local or authenticated-operator policy.
pub async fn serve_with_access_policy(
    listener: tokio::net::TcpListener,
    shutdown: Arc<AtomicBool>,
    manager: Arc<JobService>,
    worker: Arc<dyn WorkerPort>,
    request_timeout: Duration,
    access_policy: ApiAccessPolicy,
) -> io::Result<()> {
    serve_with_role_and_access_policy(
        listener,
        shutdown,
        manager,
        worker,
        request_timeout,
        access_policy,
        ApiReadiness {
            role: ApiRole::All,
            manager_workers_ready: None,
            recovery_workers: None,
        },
    )
    .await
}

/// Serve the standalone manager API with role-accurate health and readiness responses.
pub async fn serve_manager_with_access_policy(
    listener: tokio::net::TcpListener,
    shutdown: Arc<AtomicBool>,
    manager: Arc<JobService>,
    worker: Arc<dyn WorkerPort>,
    request_timeout: Duration,
    access_policy: ApiAccessPolicy,
    recovery: ManagerRecovery,
) -> io::Result<()> {
    serve_with_role_and_access_policy(
        listener,
        shutdown,
        manager,
        worker,
        request_timeout,
        access_policy,
        ApiReadiness {
            role: ApiRole::Manager,
            manager_workers_ready: Some(recovery.workers_ready),
            recovery_workers: Some(recovery.workers),
        },
    )
    .await
}

async fn serve_with_role_and_access_policy(
    listener: tokio::net::TcpListener,
    shutdown: Arc<AtomicBool>,
    manager: Arc<JobService>,
    worker: Arc<dyn WorkerPort>,
    request_timeout: Duration,
    access_policy: ApiAccessPolicy,
    readiness: ApiReadiness,
) -> io::Result<()> {
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "HTTP API listener must use loopback; terminate TLS at a trusted proxy",
        ));
    }
    let recovery_ready = Arc::new(AtomicBool::new(false));
    let ready_for_recovery = Arc::clone(&recovery_ready);
    let recovery_manager = Arc::clone(&manager);
    let recovery_workers = readiness
        .recovery_workers
        .clone()
        .unwrap_or_else(|| vec![Arc::clone(&worker)]);
    let manager_workers_ready = readiness.manager_workers_ready.clone();
    let recovery_shutdown = Arc::clone(&shutdown);
    tokio::spawn(async move {
        if let Some(workers_ready) = manager_workers_ready {
            let mut backoff = Duration::from_millis(100);
            loop {
                if recovery_shutdown.load(Ordering::Acquire) {
                    return;
                }
                if !workers_ready.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                match reconcile_workers(&recovery_manager, &recovery_workers).await {
                    Ok(()) => {
                        ready_for_recovery.store(true, Ordering::Release);
                        return;
                    }
                    Err(reason) => {
                        eprintln!(
                            "{{\"timestampMs\":{},\"severity\":\"warn\",\"component\":\"manager\",\"event\":\"manager_recovery_retry\",\"reason\":\"{reason}\"}}",
                            timestamp_ms()
                        );
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
        for attempt in 0..4 {
            if reconcile_workers(&recovery_manager, &recovery_workers)
                .await
                .is_ok()
            {
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
    let service_manager = Arc::clone(&manager);
    let service_workers = readiness
        .recovery_workers
        .clone()
        .unwrap_or_else(|| vec![Arc::clone(&worker)]);
    let service_ready = Arc::clone(&recovery_ready);
    let service_shutdown = Arc::clone(&shutdown);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if service_shutdown.load(Ordering::Acquire) {
                return;
            }
            if !service_ready.load(Ordering::Acquire) {
                continue;
            }
            let manager = Arc::clone(&service_manager);
            let workers = service_workers.clone();
            match tokio::task::spawn_blocking(move || manager.reconcile_services(&workers)).await {
                Ok(Ok(())) => {}
                _ => eprintln!(
                    "{{\"timestampMs\":{},\"severity\":\"warn\",\"component\":\"manager\",\"event\":\"service_reconcile_failed\"}}",
                    timestamp_ms()
                ),
            }
        }
    });
    let server_shutdown = Arc::clone(&shutdown);
    let server = axum::serve(
        listener,
        router_with_recovery(
            manager,
            worker,
            recovery_ready,
            request_timeout,
            access_policy,
            readiness,
        ),
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

async fn reconcile_workers(
    manager: &Arc<JobService>,
    workers: &[Arc<dyn WorkerPort>],
) -> Result<(), &'static str> {
    for worker in workers {
        let manager = Arc::clone(manager);
        let worker = Arc::clone(worker);
        match tokio::task::spawn_blocking(move || manager.reconcile(worker.as_ref())).await {
            Ok(Ok(())) => {}
            Ok(Err(ManagerError::WorkerUnavailable)) => return Err("worker_unavailable"),
            Ok(Err(_)) => return Err("manager_reconcile_failed"),
            Err(_) => return Err("reconcile_task_failed"),
        }
    }
    Ok(())
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

fn manager_timestamp_ms() -> i64 {
    timestamp_ms().min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod access_tests {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use conductor_core::{
        InspectRun, ReadRunLogs, ResolveRun, RunLogChunk, StartRun, StopRun, WorkerDescriptor,
        WorkerStatus,
    };
    use conductor_manager::WorkerPortError;
    use tower::ServiceExt;

    use super::*;
    include!("structs/test_worker.rs");

    impl WorkerPort for TestWorker {
        fn descriptor(&self) -> WorkerDescriptor {
            WorkerDescriptor::new(WorkerId::new("worker-a").expect("valid worker id"), 1, true)
        }

        fn start(
            &self,
            _command: StartRun,
        ) -> Result<conductor_core::RunObservation, WorkerPortError> {
            Err(WorkerPortError::Unavailable)
        }

        fn inspect(
            &self,
            _command: InspectRun,
        ) -> Result<Option<conductor_core::RunObservation>, WorkerPortError> {
            Err(WorkerPortError::Unavailable)
        }

        fn stop(
            &self,
            _command: StopRun,
        ) -> Result<conductor_core::RunObservation, WorkerPortError> {
            Err(WorkerPortError::Unavailable)
        }

        fn resolve(
            &self,
            _command: ResolveRun,
        ) -> Result<conductor_core::RunObservation, WorkerPortError> {
            Err(WorkerPortError::Unavailable)
        }

        fn read_logs(&self, _command: ReadRunLogs) -> Result<RunLogChunk, WorkerPortError> {
            Err(WorkerPortError::Unavailable)
        }

        fn status(&self) -> Result<WorkerStatus, WorkerPortError> {
            let capacity = conductor_core::WorkerResourceCapacity::new(1_000, 1_000_000)
                .expect("test capacity");
            let capabilities = conductor_core::WorkerCapabilities::new(
                conductor_core::OperatingSystem::Linux,
                conductor_core::Architecture::X86_64,
                std::collections::BTreeSet::from([conductor_core::ExecutionBackend::NativeProcess]),
                std::collections::BTreeSet::new(),
            )
            .expect("test capabilities");
            let inventory =
                conductor_core::WorkerInventory::new(capabilities, capacity, capacity, 1)
                    .expect("test inventory");
            Ok(WorkerStatus::new(self.descriptor(), 0, 0, true, false, 0)
                .with_inventory(inventory)
                .with_manager_metadata(
                    self.descriptor().worker_id().clone(),
                    capacity,
                    std::collections::BTreeMap::new(),
                    0,
                    true,
                )
                .expect("valid manager metadata"))
        }
    }
    include!("structs/test_database.rs");

    static TEMP_DIR_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    impl TestDatabase {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            let sequence = TEMP_DIR_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "conductor-api-access-{}-{nonce}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir(&directory).expect("create temporary API database directory");
            Self(directory.join("manager.sqlite3"))
        }

        fn manager(&self) -> Arc<JobService> {
            Arc::new(JobService::open_sqlite(&self.0).expect("open test manager database"))
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            if let Some(directory) = self.0.parent() {
                let _ = std::fs::remove_dir_all(directory);
            }
        }
    }

    const TOKEN: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVabcdefghijk";

    fn token_policy() -> ApiAccessPolicy {
        ApiAccessPolicy::AuthenticatedOperator(Arc::new(
            BearerTokenAuthenticator::new(TOKEN).expect("valid 256-bit URL-safe token"),
        ))
    }

    fn request(method: &str, path: &str, token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        builder.body(Body::empty()).expect("valid request")
    }

    fn authorized_request(
        method: &str,
        path: &str,
        body: &str,
        idempotency_key: Option<&str>,
        if_match: Option<&str>,
        if_none_match: bool,
    ) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(path).header(
            header::AUTHORIZATION,
            format!("Bearer {}", "0123456789ABCDEFGHIJKLMNOPQRSTUVabcdefghijk"),
        );
        if let Some(key) = idempotency_key {
            builder = builder.header("idempotency-key", key);
        }
        if let Some(etag) = if_match {
            builder = builder.header(header::IF_MATCH, etag);
        }
        if if_none_match {
            builder = builder.header(header::IF_NONE_MATCH, "*");
        }
        if !body.is_empty() {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
        }
        builder
            .body(Body::from(body.to_owned()))
            .expect("valid authorized request")
    }

    fn router(database: &TestDatabase) -> Router {
        router_with_access_policy(
            database.manager(),
            Arc::new(TestWorker),
            Duration::from_secs(1),
            token_policy(),
        )
    }

    #[tokio::test]
    async fn operator_policy_protects_reads_writes_and_sensitive_worker_diagnostics() {
        let database = TestDatabase::new();
        let router = router(&database);

        for (method, path) in [
            ("GET", "/v1/jobs"),
            ("GET", "/v1/jobs/job-a/service-attempts"),
            ("GET", "/v1/services/default/web/endpoints"),
            ("GET", "/v1/services/default/web/watch?waitSeconds=1"),
            ("POST", "/v1/jobs"),
            ("DELETE", "/v1/jobs/job-a"),
            ("GET", "/v1/workers"),
            ("POST", "/v1/workers/worker-a/drain"),
            ("POST", "/v1/network-domains"),
            ("GET", "/v1/network-domains/overlay-a"),
            ("GET", "/v1/network-domains/overlay-a/peers"),
            ("DELETE", "/v1/network-domains/overlay-a/peers/worker-a"),
        ] {
            let response = router
                .clone()
                .oneshot(request(method, path, None))
                .await
                .expect("unauthenticated API response");
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {path}"
            );
            assert_eq!(
                response.headers().get(header::WWW_AUTHENTICATE),
                Some(&HeaderValue::from_static("Bearer realm=\"conductor\""))
            );
        }

        let invalid = router
            .clone()
            .oneshot(request("GET", "/v1/jobs", Some("wrong")))
            .await
            .expect("invalid token response");
        assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);

        let authorized = router
            .oneshot(request(
                "GET",
                "/v1/workers",
                Some(std::str::from_utf8(TOKEN).unwrap()),
            ))
            .await
            .expect("authorized diagnostics response");
        assert_eq!(authorized.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn overlay_domain_and_peer_status_routes_are_authenticated_and_audited() {
        let database = TestDatabase::new();
        let router = router(&database);
        let body = r#"{"networkDomainId":"overlay-a","workerAddressPool":"10.88.0.0/24","serviceAddressPool":"10.88.1.0/24"}"#;
        let created = router
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/network-domains",
                body,
                None,
                None,
                false,
            ))
            .await
            .expect("create overlay domain");
        assert_eq!(created.status(), StatusCode::CREATED);

        let domain = router
            .clone()
            .oneshot(request(
                "GET",
                "/v1/network-domains/overlay-a",
                Some(std::str::from_utf8(TOKEN).unwrap()),
            ))
            .await
            .expect("read overlay domain");
        assert_eq!(domain.status(), StatusCode::OK);
        let peers = router
            .oneshot(request(
                "GET",
                "/v1/network-domains/overlay-a/peers",
                Some(std::str::from_utf8(TOKEN).unwrap()),
            ))
            .await
            .expect("list overlay peer status");
        assert_eq!(peers.status(), StatusCode::OK);
        let body = to_bytes(peers.into_body(), 16 * 1024)
            .await
            .expect("read bounded peer response");
        assert_eq!(&body[..], b"[]");

        let events = database
            .manager()
            .list_audit_events(10)
            .expect("read audit records");
        assert!(events.iter().any(|event| {
            event.action == "worker.overlay.domain.create"
                && event.resource_id.as_deref() == Some("overlay-a")
                && event.outcome == "success"
        }));
    }

    #[tokio::test]
    async fn worker_drain_is_authenticated_persistent_and_reusable_after_completion() {
        let database = TestDatabase::new();
        let router = router(&database);
        let accepted = router
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/workers/worker-a/drain",
                r#"{"gracePeriodSeconds":30}"#,
                None,
                None,
                false,
            ))
            .await
            .expect("worker drain request response");
        assert_eq!(accepted.status(), StatusCode::ACCEPTED);
        let payload = axum::body::to_bytes(accepted.into_body(), 65_536)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(status["state"], "completed");
        assert_eq!(status["activeRuns"], 0);

        let worker = router
            .clone()
            .oneshot(request(
                "GET",
                "/v1/workers/worker-a",
                Some(std::str::from_utf8(TOKEN).unwrap()),
            ))
            .await
            .expect("worker status response");
        assert_eq!(worker.status(), StatusCode::OK);
        let payload = axum::body::to_bytes(worker.into_body(), 65_536)
            .await
            .unwrap();
        let worker: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(worker["draining"], true);
        assert_eq!(worker["available"], false);

        let cleared = router
            .clone()
            .oneshot(authorized_request(
                "DELETE",
                "/v1/workers/worker-a/drain",
                "",
                None,
                None,
                false,
            ))
            .await
            .expect("completed drain can be cleared");
        assert_eq!(cleared.status(), StatusCode::NO_CONTENT);
        let resumed = router
            .clone()
            .oneshot(request(
                "GET",
                "/v1/workers/worker-a",
                Some(std::str::from_utf8(TOKEN).unwrap()),
            ))
            .await
            .expect("worker status after clearing drain");
        let payload = axum::body::to_bytes(resumed.into_body(), 65_536)
            .await
            .unwrap();
        let resumed: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(resumed["draining"], false);

        let invalid = router
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/workers/worker-a/drain",
                r#"{"gracePeriodSeconds":301,"unexpected":true}"#,
                None,
                None,
                false,
            ))
            .await
            .expect("invalid drain request response");
        assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let out_of_range = router
            .oneshot(authorized_request(
                "POST",
                "/v1/workers/worker-a/drain",
                r#"{"gracePeriodSeconds":301}"#,
                None,
                None,
                false,
            ))
            .await
            .expect("out-of-range drain period response");
        assert_eq!(out_of_range.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn metrics_are_authenticated_aggregated_and_have_bounded_labels() {
        let database = TestDatabase::new();
        let app = router(&database);
        let unauthorized = app
            .clone()
            .oneshot(request("GET", "/v1/metrics", None))
            .await
            .expect("metrics authentication response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(request(
                "GET",
                "/v1/metrics",
                Some(std::str::from_utf8(TOKEN).unwrap()),
            ))
            .await
            .expect("metrics response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let bytes = axum::body::to_bytes(response.into_body(), 65_536)
            .await
            .expect("bounded metrics body");
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("conductor_manager_reconcile_duration_seconds_bucket"));
        assert!(text.contains("reason=\"service_attempt_unresolved\""));
        assert!(text.contains("state=\"freshness_unknown\""));
        assert!(!text.contains("worker-a"));
        assert!(!text.contains(std::str::from_utf8(TOKEN).unwrap()));
        assert!(text.lines().count() < 64);
    }

    #[tokio::test]
    async fn operator_mutations_record_outcomes_without_request_contents() {
        let database = TestDatabase::new();
        let manager = database.manager();
        let app = router(&database);
        let missing = app
            .clone()
            .oneshot(authorized_request(
                "DELETE",
                "/v1/jobs/job-a",
                "",
                None,
                None,
                false,
            ))
            .await
            .expect("failed mutation response");
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        let body = r#"{"id":"job-a","command":["/bin/echo","marker-private-command"],"cwd":"/tmp","terminationGraceMs":5000}"#;
        let created = app
            .oneshot(authorized_request(
                "POST", "/v1/jobs", body, None, None, false,
            ))
            .await
            .expect("create job response");
        assert_eq!(created.status(), StatusCode::CREATED);

        let events = manager
            .list_audit_events(10)
            .expect("audit events persisted");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].action, "job.create");
        assert_eq!(events[0].outcome, "success");
        assert_eq!(events[0].resource_id.as_deref(), Some("job-a"));
        assert_eq!(events[1].action, "job.delete");
        assert_eq!(events[1].outcome, "failure");
        assert_eq!(events[1].status_code, 404);
        assert!(events.iter().all(|event| event.principal == "operator"));
        let serialized = serde_json::to_string(&events).expect("serialize audit records");
        assert!(serialized.contains("job-a"));
        assert!(!serialized.contains("marker-private-command"));
        assert!(!serialized.contains(std::str::from_utf8(TOKEN).unwrap()));
    }

    #[tokio::test]
    async fn health_endpoints_remain_public_under_operator_policy() {
        let database = TestDatabase::new();
        let router = router(&database);

        for path in ["/healthz", "/readyz"] {
            let response = router
                .clone()
                .oneshot(request("GET", path, None))
                .await
                .expect("public health response");
            assert_eq!(response.status(), StatusCode::OK, "GET {path}");
        }
    }

    #[tokio::test]
    async fn service_watch_rejects_timeouts_outside_its_bounded_range() {
        let database = TestDatabase::new();
        let app = router(&database);
        for wait_seconds in ["0", "31"] {
            let response = app
                .clone()
                .oneshot(authorized_request(
                    "GET",
                    &format!("/v1/services/default/backend/watch?waitSeconds={wait_seconds}"),
                    "",
                    None,
                    None,
                    false,
                ))
                .await
                .expect("bounded watch validation response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn operator_token_requires_fixed_length_url_safe_encoding() {
        assert!(BearerTokenAuthenticator::new(b"short").is_err());
        assert!(
            BearerTokenAuthenticator::new(b"0123456789ABCDEFGHIJKLMNOPQRSTUVabcdefghij=").is_err()
        );
    }

    #[tokio::test]
    async fn revision_and_deployment_api_is_strict_durable_and_conditional() {
        let database = TestDatabase::new();
        let revision_body = |argument: &str| {
            format!(
                r#"{{"definition":{{"id":"web","command":["/usr/bin/true","{argument}"],"cwd":"/tmp","terminationGraceMs":5000}},"workload":{{"kind":"service","identity":{{"namespace":"default","name":"web"}}}},"executableDigest":{{"algorithm":"sha256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},"executionRequirements":{{}},"resourceRequest":{{"cpuMillis":250,"memoryBytes":268435456}}}}"#
            )
        };
        let app = router(&database);
        let created = app
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/jobs/web/revisions",
                &revision_body("v1"),
                Some("revision-one"),
                None,
                false,
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let created_bytes = axum::body::to_bytes(created.into_body(), 65_536)
            .await
            .unwrap();
        let revision_one: conductor_core::JobRevision =
            serde_json::from_slice(&created_bytes).unwrap();
        assert_eq!(revision_one.id().revision(), 1);
        assert_eq!(
            revision_one.resource_request_provenance(),
            conductor_core::ResourceRequestProvenance::Explicit
        );

        let initial = app
            .clone()
            .oneshot(authorized_request(
                "PUT",
                "/v1/jobs/web/deployment",
                r#"{"revision":1,"replicas":2}"#,
                Some("deployment-one"),
                None,
                true,
            ))
            .await
            .unwrap();
        assert_eq!(initial.status(), StatusCode::ACCEPTED);
        assert_eq!(initial.headers().get(header::ETAG).unwrap(), "\"1\"");
        let initial_bytes = axum::body::to_bytes(initial.into_body(), 65_536)
            .await
            .unwrap();
        let initial: conductor_core::JobDeployment =
            serde_json::from_slice(&initial_bytes).unwrap();
        assert_eq!(initial.status(), DeploymentStatus::Pending);

        let service_attempts = app
            .clone()
            .oneshot(authorized_request(
                "GET",
                "/v1/jobs/web/service-attempts",
                "",
                None,
                None,
                false,
            ))
            .await
            .unwrap();
        assert_eq!(service_attempts.status(), StatusCode::OK);
        let attempts_bytes = axum::body::to_bytes(service_attempts.into_body(), 65_536)
            .await
            .unwrap();
        let attempts: serde_json::Value = serde_json::from_slice(&attempts_bytes).unwrap();
        let instances = attempts["serviceAttempts"].as_array().unwrap();
        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0]["terminalFailure"], false);

        let endpoints = app
            .clone()
            .oneshot(authorized_request(
                "GET",
                "/v1/jobs/web/endpoints",
                "",
                None,
                None,
                false,
            ))
            .await
            .unwrap();
        assert_eq!(endpoints.status(), StatusCode::OK);
        let endpoints_bytes = axum::body::to_bytes(endpoints.into_body(), 65_536)
            .await
            .unwrap();
        let endpoints: serde_json::Value = serde_json::from_slice(&endpoints_bytes).unwrap();
        assert_eq!(endpoints["endpoints"], serde_json::json!([]));

        let catalog = app
            .clone()
            .oneshot(authorized_request(
                "GET",
                "/v1/services/default/web/endpoints",
                "",
                None,
                None,
                false,
            ))
            .await
            .unwrap();
        assert_eq!(catalog.status(), StatusCode::OK);
        let catalog_bytes = axum::body::to_bytes(catalog.into_body(), 65_536)
            .await
            .unwrap();
        let catalog: serde_json::Value = serde_json::from_slice(&catalog_bytes).unwrap();
        assert_eq!(catalog["endpoints"], serde_json::json!([]));

        let unsupported_namespace = app
            .clone()
            .oneshot(authorized_request(
                "GET",
                "/v1/services/other/web/endpoints",
                "",
                None,
                None,
                false,
            ))
            .await
            .unwrap();
        assert_eq!(unsupported_namespace.status(), StatusCode::BAD_REQUEST);

        drop(app);
        let restarted_app = router(&database);
        let scaled = restarted_app
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/jobs/web/deployment/scale",
                r#"{"replicas":0}"#,
                Some("scale-zero"),
                Some("\"1\""),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(scaled.status(), StatusCode::OK);
        assert_eq!(scaled.headers().get(header::ETAG).unwrap(), "\"2\"");
        let scaled_bytes = axum::body::to_bytes(scaled.into_body(), 65_536)
            .await
            .unwrap();
        let scaled: conductor_core::JobDeployment = serde_json::from_slice(&scaled_bytes).unwrap();
        assert_eq!(scaled.desired_replicas(), 0);

        let replayed = restarted_app
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/jobs/web/deployment/scale",
                r#"{"replicas":0}"#,
                Some("scale-zero"),
                Some("\"1\""),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(replayed.status(), StatusCode::OK);
        assert_eq!(replayed.headers().get(header::ETAG).unwrap(), "\"2\"");

        let suspended = restarted_app
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/jobs/web/deployment/suspend",
                "",
                Some("suspend-zero"),
                Some("\"2\""),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(suspended.status(), StatusCode::OK);
        assert_eq!(suspended.headers().get(header::ETAG).unwrap(), "\"3\"");
        let resumed = restarted_app
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/jobs/web/deployment/resume",
                "",
                Some("resume-zero"),
                Some("\"3\""),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(resumed.status(), StatusCode::OK);
        assert_eq!(resumed.headers().get(header::ETAG).unwrap(), "\"4\"");

        let stale = restarted_app
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/jobs/web/deployment/suspend",
                "",
                Some("stale-suspend"),
                Some("\"1\""),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);

        let second_revision = restarted_app
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/jobs/web/revisions",
                &revision_body("v2"),
                Some("revision-two"),
                None,
                false,
            ))
            .await
            .unwrap();
        assert_eq!(second_revision.status(), StatusCode::CREATED);
        let updated = restarted_app
            .clone()
            .oneshot(authorized_request(
                "PUT",
                "/v1/jobs/web/deployment",
                r#"{"revision":2,"replicas":2,"strategy":{"maxSurge":0,"maxUnavailable":1,"progressDeadlineSeconds":300}}"#,
                Some("deployment-two"),
                Some("\"4\""),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(updated.status(), StatusCode::ACCEPTED);
        assert_eq!(updated.headers().get(header::ETAG).unwrap(), "\"5\"");
        let invalid_strategy = restarted_app
            .clone()
            .oneshot(authorized_request(
                "PUT",
                "/v1/jobs/web/deployment",
                r#"{"revision":2,"replicas":2,"strategy":{"maxSurge":1,"maxUnavailable":1,"progressDeadlineSeconds":300}}"#,
                Some("invalid-surge"),
                Some("\"5\""),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(invalid_strategy.status(), StatusCode::BAD_REQUEST);
        let rolled_back = restarted_app
            .clone()
            .oneshot(authorized_request(
                "POST",
                "/v1/jobs/web/deployment/rollback",
                r#"{"revision":1}"#,
                Some("rollback-one"),
                Some("\"5\""),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(rolled_back.status(), StatusCode::ACCEPTED);
        let rolled_back_bytes = axum::body::to_bytes(rolled_back.into_body(), 65_536)
            .await
            .unwrap();
        let rolled_back: conductor_core::JobDeployment =
            serde_json::from_slice(&rolled_back_bytes).unwrap();
        assert_eq!(rolled_back.revision_id().revision(), 1);
        assert_eq!(rolled_back.desired_replicas(), 2);
        assert!(!rolled_back.suspended());

        let inspection = restarted_app
            .clone()
            .oneshot(request(
                "GET",
                "/v1/jobs/web/deployment",
                Some("0123456789ABCDEFGHIJKLMNOPQRSTUVabcdefghijk"),
            ))
            .await
            .unwrap();
        assert_eq!(inspection.status(), StatusCode::OK);
        assert_eq!(inspection.headers().get(header::ETAG).unwrap(), "\"6\"");

        let deleted = restarted_app
            .clone()
            .oneshot(authorized_request(
                "DELETE",
                "/v1/jobs/web/deployment",
                "",
                Some("delete-deployment"),
                Some("\"6\""),
                false,
            ))
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::OK);
        let deleted_bytes = axum::body::to_bytes(deleted.into_body(), 65_536)
            .await
            .unwrap();
        let deleted: conductor_core::JobDeployment =
            serde_json::from_slice(&deleted_bytes).unwrap();
        assert_eq!(deleted.status(), DeploymentStatus::Deleted);
    }
}
