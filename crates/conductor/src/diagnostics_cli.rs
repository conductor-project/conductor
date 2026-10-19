//! Manager health, readiness, and metrics commands.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand, ValueEnum};
use conductor_api::client::{Client as ApiClient, ClientError};
use conductor_api::contracts::ReadinessResponse;
use conductor_core::WorkerStatus;
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Args)]
pub struct DiagnosticsArgs {
    /// Manager API URL. Plain HTTP is allowed only for loopback addresses.
    #[arg(long, global = true)]
    server: Option<String>,
    /// Named connection context (otherwise the selected default context is used).
    #[arg(long, global = true)]
    context: Option<String>,
    /// Protected file containing the operator bearer token.
    #[arg(long, global = true)]
    token_file: Option<PathBuf>,
    /// PEM CA certificate used to verify the server TLS certificate.
    #[arg(long, global = true)]
    ca_file: Option<PathBuf>,
    /// Per-request timeout in seconds.
    #[arg(long, global = true, value_parser = clap::value_parser!(u64).range(1..=300))]
    timeout_seconds: Option<u64>,
    /// Health/readiness output format. Metrics are raw Prometheus text unless JSON is selected.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Human)]
    output: OutputFormat,
    #[command(subcommand)]
    command: DiagnosticsCommand,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Debug, Subcommand)]
enum DiagnosticsCommand {
    /// Check whether the API responds and the manager process reports healthy.
    Health,
    /// Check manager readiness; a non-ready manager exits unsuccessfully with its state.
    Ready,
    /// Retrieve the manager's Prometheus exposition text.
    Metrics,
    /// Collect a bounded, allowlisted remote diagnostics export.
    Export {
        /// Maximum time allowed for concurrent API collection, in seconds.
        #[arg(long, default_value_t = 15, value_parser = clap::value_parser!(u64).range(1..=300))]
        deadline_seconds: u64,
    },
}

const MAX_DIAGNOSTIC_WORKERS: usize = 128;
const MAX_DIAGNOSTIC_METRICS: usize = 64;
const MAX_DIAGNOSTIC_FINDINGS: usize = 128;
const MAX_DIAGNOSTIC_EXPORT_BYTES: usize = 256 * 1024;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticsExport {
    schema_version: u8,
    generated_at_ms: u64,
    collection_deadline_seconds: u64,
    collection_status: &'static str,
    versions: VersionSummary,
    sections: DiagnosticsSections,
    findings: Vec<DiagnosticFinding>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionSummary {
    binary: &'static str,
    http_api: u8,
    manager_worker_protocol: u16,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticsSections {
    health: Section<HealthSummary>,
    readiness: Section<ReadinessSummary>,
    workers: Section<WorkerSummaryCollection>,
    metrics: Section<MetricsSummary>,
    storage: Section<StorageAvailability>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Section<T> {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthSummary {
    reachable: bool,
    healthy: bool,
    status: String,
    role: String,
    capabilities: Vec<String>,
    http_status: u16,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadinessSummary {
    reachable: bool,
    ready: bool,
    status: String,
    role: String,
    capabilities: Vec<String>,
    http_status: u16,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerSummaryCollection {
    workers: Vec<WorkerSummary>,
    omitted_workers: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerSummary {
    worker_id: String,
    freshness: &'static str,
    available: bool,
    degraded: bool,
    draining: bool,
    capacity: usize,
    used_slots: usize,
    available_slots: usize,
    unknown_runs: usize,
    pending_persistence: usize,
    observed_at_ms: i64,
    observation_age_ms: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MetricsSummary {
    samples: BTreeMap<String, u64>,
    omitted_samples: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StorageAvailability {
    remote_collection: &'static str,
    offline_command: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnosticFinding {
    code: String,
    severity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_id: Option<String>,
    summary: String,
}

pub fn run(args: DiagnosticsArgs) -> ExitCode {
    let output = args.output;
    let client = match super::operator_cli::build_client(
        args.server.as_deref(),
        args.context.as_deref(),
        args.token_file.as_deref(),
        args.ca_file.as_deref(),
        args.timeout_seconds,
    ) {
        Ok(client) => client,
        Err(error) => return report_error(output, "connection_error", error, None),
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            return report_error(
                output,
                "runtime_error",
                "diagnostics runtime could not be started".to_owned(),
                None,
            );
        }
    };
    match runtime.block_on(execute(&client, args.command, output)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => report_error(output, &error.code, error.message, error.partial),
    }
}

struct CommandError {
    code: String,
    message: String,
    partial: Option<Value>,
}

impl CommandError {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            partial: None,
        }
    }

    fn partial(mut self, value: Value) -> Self {
        self.partial = Some(value);
        self
    }
}

fn api_error(error: ClientError) -> CommandError {
    let code = match &error {
        ClientError::Api {
            code: Some(code), ..
        } => code.clone(),
        ClientError::Api { status, .. } => format!("http_{}", status.as_u16()),
        _ => "api_error".to_owned(),
    };
    CommandError::new(code, error.to_string())
}

async fn execute(
    client: &ApiClient,
    command: DiagnosticsCommand,
    output: OutputFormat,
) -> Result<(), CommandError> {
    match command {
        DiagnosticsCommand::Export { deadline_seconds } => {
            let export = collect_export(client, deadline_seconds).await;
            if serde_json::to_vec(&export)
                .map_or(true, |bytes| bytes.len() > MAX_DIAGNOSTIC_EXPORT_BYTES)
            {
                return Err(CommandError::new(
                    "export_limit_exceeded",
                    "diagnostics export exceeded its output size limit",
                ));
            }
            let mut human = format!(
                "diagnostics export {} (schema v{})\nhealth: {}; readiness: {}; workers: {}; metrics: {}; storage: {}",
                export.collection_status,
                export.schema_version,
                export.sections.health.status,
                export.sections.readiness.data.as_ref().map_or_else(
                    || "unavailable".to_owned(),
                    |readiness| format!(
                        "{} ({})",
                        export.sections.readiness.status, readiness.status
                    ),
                ),
                export.sections.workers.status,
                export.sections.metrics.status,
                export.sections.storage.status,
            );
            for finding in &export.findings {
                human.push('\n');
                human.push_str(finding.severity);
                human.push_str(": ");
                human.push_str(&finding.summary);
            }
            emit(output, &export, &human)
        }
        DiagnosticsCommand::Health => {
            let response = client
                .get_response::<ReadinessResponse>("healthz")
                .await
                .map_err(api_error)?;
            let healthy = response.status.is_success() && response.body.status == "ok";
            let value = json!({
                "reachable": true,
                "healthy": healthy,
                "status": response.body.status,
                "role": response.body.role,
                "capabilities": response.body.capabilities,
                "httpStatus": response.status.as_u16(),
            });
            if !healthy {
                return Err(CommandError::new(
                    "health_check_failed",
                    "the manager API responded but its process health is not ok",
                )
                .partial(value));
            }
            emit(
                output,
                &value,
                &format!(
                    "manager API reachable; process health ok ({})",
                    response.body.role
                ),
            )
        }
        DiagnosticsCommand::Ready => {
            let response = client
                .get_response::<ReadinessResponse>("readyz")
                .await
                .map_err(api_error)?;
            let ready = response.status.is_success() && response.body.status == "ready";
            let value = json!({
                "reachable": true,
                "ready": ready,
                "status": response.body.status,
                "role": response.body.role,
                "capabilities": response.body.capabilities,
                "httpStatus": response.status.as_u16(),
            });
            if !ready {
                return Err(CommandError::new(
                    "manager_not_ready",
                    format!(
                        "manager is reachable but not ready ({})",
                        response.body.status
                    ),
                )
                .partial(value));
            }
            emit(
                output,
                &value,
                &format!("manager ready ({})", response.body.role),
            )
        }
        DiagnosticsCommand::Metrics => {
            let metrics = client.get_bytes("v1/metrics").await.map_err(api_error)?;
            match output {
                OutputFormat::Human => io::stdout()
                    .lock()
                    .write_all(&metrics)
                    .map_err(|_| CommandError::new("output_error", "metrics could not be written")),
                OutputFormat::Json => {
                    let metrics = String::from_utf8(metrics).map_err(|_| {
                        CommandError::new("invalid_metrics", "metrics response was not UTF-8")
                    })?;
                    emit(
                        output,
                        &json!({
                            "contentType": "text/plain; version=0.0.4; charset=utf-8",
                            "metrics": metrics,
                        }),
                        "metrics retrieved",
                    )
                }
            }
        }
    }
}

async fn collect_export(client: &ApiClient, deadline_seconds: u64) -> DiagnosticsExport {
    let deadline = Duration::from_secs(deadline_seconds);
    let (health, readiness, workers, metrics) = tokio::join!(
        collect_health(client, deadline),
        collect_readiness(client, deadline),
        collect_workers(client, deadline),
        collect_metrics(client, deadline),
    );
    let workers = workers.unwrap_or_else(|error| Section {
        status: "unavailable",
        data: None,
        error_code: Some(error),
    });
    let metrics = metrics.unwrap_or_else(|error| Section {
        status: "unavailable",
        data: None,
        error_code: Some(error),
    });
    let storage = Section {
        status: "unavailable",
        data: Some(StorageAvailability {
            remote_collection: "not_supported",
            offline_command: "conductor storage inspect",
        }),
        error_code: None,
    };
    let sections = DiagnosticsSections {
        health,
        readiness,
        workers,
        metrics,
        storage,
    };
    let mut findings = Vec::new();
    findings.push(finding(
        "remote_storage_unavailable",
        "info",
        None,
        "Remote storage pressure is not exposed by the API; stop the daemon and run conductor storage inspect locally.".to_owned(),
    ));
    if let Some(data) = &sections.readiness.data
        && !data.ready
    {
        findings.push(finding(
            "manager_not_ready",
            "warning",
            None,
            format!("Manager readiness is reported as '{}'. This does not identify the underlying cause.", data.status),
        ));
    }
    if let Some(data) = &sections.workers.data {
        if data.omitted_workers > 0 {
            findings.push(finding(
                "worker_list_truncated",
                "info",
                None,
                "Worker list exceeded the export item limit; additional workers were omitted."
                    .to_owned(),
            ));
        }
        for worker in &data.workers {
            if worker.freshness == "stale" {
                findings.push(finding("worker_observation_stale", "warning", Some(worker.worker_id.clone()), "The manager reports this worker observation as stale; its current process state is unknown.".to_owned()));
            }
            if worker.available_slots == 0 {
                findings.push(finding(
                    "worker_no_available_slots",
                    "info",
                    Some(worker.worker_id.clone()),
                    "The worker currently reports no available execution slots.".to_owned(),
                ));
            }
            if worker.unknown_runs > 0 {
                findings.push(finding(
                    "worker_unknown_runs",
                    "warning",
                    Some(worker.worker_id.clone()),
                    "The worker reports unresolved runs that require recovery review.".to_owned(),
                ));
            }
            if worker.pending_persistence > 0 {
                findings.push(finding(
                    "worker_pending_persistence",
                    "warning",
                    Some(worker.worker_id.clone()),
                    "The worker reports observations awaiting persistence.".to_owned(),
                ));
            }
        }
    }
    if let Some(data) = &sections.metrics.data {
        let pending = data
            .samples
            .get("conductor_pending_work_allocations")
            .copied()
            .unwrap_or(0);
        if pending > 0 {
            findings.push(finding("placement_pending", "warning", None, format!("The manager reports {pending} allocation(s) awaiting placement; this metric does not identify a specific cause.")));
        }
        for (key, code, description) in [
            (
                "conductor_pending_work_deployment_no_eligible_worker",
                "deployment_no_eligible_worker",
                "No currently eligible worker is reported for this deployment.",
            ),
            (
                "conductor_pending_work_deployment_worker_capacity_exhausted",
                "deployment_worker_capacity_exhausted",
                "Reported worker capacity is exhausted for this deployment.",
            ),
            (
                "conductor_pending_work_deployment_worker_disconnected",
                "deployment_worker_disconnected",
                "A worker required by this deployment is reported disconnected.",
            ),
            (
                "conductor_pending_work_deployment_required_protection_unavailable",
                "deployment_protection_unavailable",
                "A required execution protection is unavailable on eligible workers.",
            ),
            (
                "conductor_pending_work_deployment_shared_host_capacity_exhausted",
                "deployment_shared_host_capacity_exhausted",
                "The shared-host resource budget is reported exhausted.",
            ),
            (
                "conductor_pending_work_deployment_placement_pending",
                "deployment_placement_pending",
                "The deployment is waiting for a placement decision.",
            ),
            (
                "conductor_pending_work_deployment_service_starting",
                "deployment_service_starting",
                "The selected service instance is still starting.",
            ),
            (
                "conductor_pending_work_deployment_service_restart_backoff",
                "deployment_service_restart_backoff",
                "A service restart is waiting for its configured backoff interval.",
            ),
            (
                "conductor_pending_work_deployment_service_restart_budget_exhausted",
                "deployment_restart_budget_exhausted",
                "The service restart budget is exhausted.",
            ),
            (
                "conductor_pending_work_deployment_service_attempt_unresolved",
                "deployment_service_attempt_unresolved",
                "A service attempt has an unresolved outcome.",
            ),
            (
                "conductor_pending_work_deployment_service_readiness_pending",
                "deployment_service_readiness_pending",
                "The service has not yet reported readiness.",
            ),
            (
                "conductor_pending_work_deployment_rollout_progress_deadline_exceeded",
                "deployment_rollout_deadline_exceeded",
                "The rollout progress deadline was exceeded.",
            ),
        ] {
            if data.samples.get(key).is_some_and(|count| *count > 0) {
                findings.push(finding(code, "warning", None, description.to_owned()));
            }
        }
    }
    let collected = sections.health.status == "collected"
        && sections.readiness.status == "collected"
        && sections.workers.status == "collected"
        && sections.metrics.status == "collected";
    if !collected {
        findings.push(finding("diagnostics_partial", "info", None, "One or more remote diagnostic sections could not be collected; see each section status and error code.".to_owned()));
    }
    findings.truncate(MAX_DIAGNOSTIC_FINDINGS);
    let generated_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(u64::MAX as u128) as u64
        });
    // The remote storage API deliberately exposes no database paths or sizes.
    // Local storage inspection remains an explicit offline operation.
    DiagnosticsExport {
        schema_version: 1,
        generated_at_ms,
        collection_deadline_seconds: deadline_seconds,
        collection_status: if collected { "complete" } else { "partial" },
        versions: VersionSummary {
            binary: env!("CARGO_PKG_VERSION"),
            http_api: 1,
            manager_worker_protocol: conductor_core::CURRENT_PROTOCOL_VERSION,
        },
        sections,
        findings,
    }
}

fn finding(
    code: &str,
    severity: &'static str,
    resource_id: Option<String>,
    summary: String,
) -> DiagnosticFinding {
    DiagnosticFinding {
        code: code.to_owned(),
        severity,
        resource_id,
        summary,
    }
}

async fn collect_health(client: &ApiClient, deadline: Duration) -> Section<HealthSummary> {
    match tokio::time::timeout(
        deadline,
        client.get_response::<ReadinessResponse>("healthz"),
    )
    .await
    {
        Err(_) => section_error("deadline_exceeded"),
        Ok(Err(error)) => section_error(client_error_code(error)),
        Ok(Ok(response)) => Section {
            status: "collected",
            data: Some(HealthSummary {
                reachable: true,
                healthy: response.status.is_success() && response.body.status == "ok",
                status: response.body.status,
                role: response.body.role,
                capabilities: response.body.capabilities.into_iter().take(32).collect(),
                http_status: response.status.as_u16(),
            }),
            error_code: None,
        },
    }
}

async fn collect_readiness(client: &ApiClient, deadline: Duration) -> Section<ReadinessSummary> {
    match tokio::time::timeout(deadline, client.get_response::<ReadinessResponse>("readyz")).await {
        Err(_) => section_error("deadline_exceeded"),
        Ok(Err(error)) => section_error(client_error_code(error)),
        Ok(Ok(response)) => Section {
            status: "collected",
            data: Some(ReadinessSummary {
                reachable: true,
                ready: response.status.is_success() && response.body.status == "ready",
                status: response.body.status,
                role: response.body.role,
                capabilities: response.body.capabilities.into_iter().take(32).collect(),
                http_status: response.status.as_u16(),
            }),
            error_code: None,
        },
    }
}

async fn collect_workers(
    client: &ApiClient,
    deadline: Duration,
) -> Result<Section<WorkerSummaryCollection>, String> {
    match tokio::time::timeout(deadline, client.get::<Vec<WorkerStatus>>("v1/workers")).await {
        Err(_) => Err("deadline_exceeded".to_owned()),
        Ok(Err(error)) => Err(client_error_code(error)),
        Ok(Ok(workers)) => {
            let omitted_workers = workers.len().saturating_sub(MAX_DIAGNOSTIC_WORKERS);
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |duration| {
                    duration.as_millis().min(u64::MAX as u128) as u64
                });
            let workers = workers
                .into_iter()
                .take(MAX_DIAGNOSTIC_WORKERS)
                .map(|worker| WorkerSummary {
                    worker_id: worker.worker_id().to_string(),
                    freshness: if worker.fresh() {
                        "fresh"
                    } else if worker.last_seen_at_ms().is_some() {
                        "stale"
                    } else {
                        "unknown"
                    },
                    available: worker.available(),
                    degraded: worker.degraded(),
                    draining: worker.draining(),
                    capacity: worker.capacity(),
                    used_slots: worker.used_slots(),
                    available_slots: worker.available_slots(),
                    unknown_runs: worker.unknown_runs(),
                    pending_persistence: worker.pending_persistence(),
                    observed_at_ms: worker.observed_at_ms(),
                    observation_age_ms: now_ms
                        .saturating_sub(worker.observed_at_ms().max(0) as u64),
                })
                .collect();
            Ok(Section {
                status: "collected",
                data: Some(WorkerSummaryCollection {
                    workers,
                    omitted_workers,
                }),
                error_code: None,
            })
        }
    }
}

async fn collect_metrics(
    client: &ApiClient,
    deadline: Duration,
) -> Result<Section<MetricsSummary>, String> {
    match tokio::time::timeout(deadline, client.get_text("v1/metrics")).await {
        Err(_) => Err("deadline_exceeded".to_owned()),
        Ok(Err(error)) => Err(client_error_code(error)),
        Ok(Ok(text)) => {
            let mut samples = BTreeMap::new();
            let mut omitted_samples = 0;
            for line in text.lines().filter(|line| !line.starts_with('#')) {
                let Some((sample, raw_value)) = line.rsplit_once(' ') else {
                    omitted_samples += 1;
                    continue;
                };
                let value = raw_value
                    .parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite() && *value >= 0.0)
                    .map(|value| value.min(u64::MAX as f64) as u64);
                let Some(value) = value else {
                    omitted_samples += 1;
                    continue;
                };
                let key = match sample {
                    "conductor_pending_work{kind=\"allocations\"}" => {
                        "conductor_pending_work_allocations"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"no_eligible_worker\"}" => {
                        "conductor_pending_work_deployment_no_eligible_worker"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"worker_capacity_exhausted\"}" => {
                        "conductor_pending_work_deployment_worker_capacity_exhausted"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"worker_disconnected\"}" => {
                        "conductor_pending_work_deployment_worker_disconnected"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"required_protection_unavailable\"}" => {
                        "conductor_pending_work_deployment_required_protection_unavailable"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"shared_host_capacity_exhausted\"}" => {
                        "conductor_pending_work_deployment_shared_host_capacity_exhausted"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"placement_pending\"}" => {
                        "conductor_pending_work_deployment_placement_pending"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"service_starting\"}" => {
                        "conductor_pending_work_deployment_service_starting"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"service_restart_backoff\"}" => {
                        "conductor_pending_work_deployment_service_restart_backoff"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"service_restart_budget_exhausted\"}" => {
                        "conductor_pending_work_deployment_service_restart_budget_exhausted"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"service_attempt_unresolved\"}" => {
                        "conductor_pending_work_deployment_service_attempt_unresolved"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"service_readiness_pending\"}" => {
                        "conductor_pending_work_deployment_service_readiness_pending"
                    }
                    "conductor_pending_work{kind=\"deployment\",reason=\"rollout_progress_deadline_exceeded\"}" => {
                        "conductor_pending_work_deployment_rollout_progress_deadline_exceeded"
                    }
                    _ => {
                        omitted_samples += 1;
                        continue;
                    }
                };
                if samples.len() >= MAX_DIAGNOSTIC_METRICS && !samples.contains_key(key) {
                    omitted_samples += 1;
                    continue;
                }
                samples.insert(key.to_owned(), value);
            }
            Ok(Section {
                status: "collected",
                data: Some(MetricsSummary {
                    samples,
                    omitted_samples,
                }),
                error_code: None,
            })
        }
    }
}

fn section_error<T>(code: impl Into<String>) -> Section<T> {
    Section {
        status: "unavailable",
        data: None,
        error_code: Some(code.into()),
    }
}

fn client_error_code(error: ClientError) -> String {
    match error {
        ClientError::Api {
            code: Some(code), ..
        } if code.len() <= 64
            && code
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-') =>
        {
            code
        }
        ClientError::Api { status, .. } => format!("http_{}", status.as_u16()),
        ClientError::Decode => "invalid_response".to_owned(),
        _ => "api_error".to_owned(),
    }
}

fn emit<T: Serialize>(output: OutputFormat, value: &T, human: &str) -> Result<(), CommandError> {
    let mut stdout = io::stdout().lock();
    match output {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut stdout, value).map_err(|_| {
                CommandError::new("output_error", "diagnostics response could not be written")
            })?;
            stdout.write_all(b"\n").map_err(|_| {
                CommandError::new("output_error", "diagnostics response could not be written")
            })
        }
        OutputFormat::Human => stdout
            .write_all(human.as_bytes())
            .and_then(|()| stdout.write_all(b"\n"))
            .map_err(|_| {
                CommandError::new("output_error", "diagnostics response could not be written")
            }),
    }
}

fn report_error(
    output: OutputFormat,
    code: &str,
    message: String,
    partial: Option<Value>,
) -> ExitCode {
    let mut stderr = io::stderr().lock();
    match output {
        OutputFormat::Json => {
            let mut error = json!({"code":code,"message":message});
            if let Some(partial) = partial {
                error["partial"] = partial;
            }
            let _ = serde_json::to_writer(&mut stderr, &json!({"error":error}));
            let _ = stderr.write_all(b"\n");
        }
        OutputFormat::Human => {
            let _ = writeln!(stderr, "conductor diagnostics: {message}");
            if let Some(partial) = partial {
                let _ = writeln!(stderr, "partial result: {partial}");
            }
        }
    }
    ExitCode::FAILURE
}
