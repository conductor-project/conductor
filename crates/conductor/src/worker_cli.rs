//! Worker inspection and graceful drain commands.

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use conductor_api::client::{Client as ApiClient, ClientError};
use conductor_api::contracts::{WorkerDrainRequest, WorkerDrainStatus};
use conductor_core::{WorkerId, WorkerStatus};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Args)]
pub struct WorkerArgs {
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
    /// Output format.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Human)]
    output: OutputFormat,
    #[command(subcommand)]
    command: WorkerCommand,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Debug, Subcommand)]
enum WorkerCommand {
    /// List the manager's current worker inventory; an empty inventory succeeds.
    List,
    /// Inspect one worker's capacity, connectivity, freshness, and drain state.
    Inspect { worker_id: String },
    /// Request a graceful drain. Existing runs may continue while new placement avoids the worker.
    Drain {
        worker_id: String,
        #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=300))]
        grace_period_seconds: u64,
        /// Wait until the manager reports the drain complete.
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        wait_timeout_seconds: u64,
    },
    /// Read the latest drain status. A missing request is reported as an error.
    DrainStatus { worker_id: String },
    /// Continue observing a previously accepted drain request.
    DrainWait {
        worker_id: String,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        timeout_seconds: u64,
    },
    /// Clear a completed drain so the worker can receive new placements.
    Undrain { worker_id: String },
}

pub fn run(args: WorkerArgs) -> ExitCode {
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
                "worker command runtime could not be started".to_owned(),
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
    command: WorkerCommand,
    output: OutputFormat,
) -> Result<(), CommandError> {
    match command {
        WorkerCommand::List => {
            let workers: Vec<WorkerStatus> = client.get("v1/workers").await.map_err(api_error)?;
            emit_workers(output, &workers)
        }
        WorkerCommand::Inspect { worker_id } => {
            let id = parse_worker_id(&worker_id)?;
            let worker: WorkerStatus = client
                .get(&format!("v1/workers/{}", id.as_str()))
                .await
                .map_err(api_error)?;
            emit_worker(output, &worker)
        }
        WorkerCommand::Drain {
            worker_id,
            grace_period_seconds,
            wait,
            wait_timeout_seconds,
        } => {
            let id = parse_worker_id(&worker_id)?;
            let status: WorkerDrainStatus = client
                .post(
                    &format!("v1/workers/{}/drain", id.as_str()),
                    &WorkerDrainRequest {
                        grace_period_seconds: Some(grace_period_seconds),
                    },
                    None,
                )
                .await
                .map_err(api_error)?;
            if wait && status.state != "completed" {
                let final_status =
                    wait_for_drain(client, id, status, wait_timeout_seconds, output).await?;
                emit_drain(output, &final_status, true, true)
            } else {
                let completed = status.state == "completed";
                emit_drain(output, &status, true, completed)
            }
        }
        WorkerCommand::DrainStatus { worker_id } => {
            let id = parse_worker_id(&worker_id)?;
            let status: WorkerDrainStatus = client
                .get(&format!("v1/workers/{}/drain", id.as_str()))
                .await
                .map_err(api_error)?;
            emit_drain(output, &status, true, status.state == "completed")
        }
        WorkerCommand::DrainWait {
            worker_id,
            timeout_seconds,
        } => {
            let id = parse_worker_id(&worker_id)?;
            let status: WorkerDrainStatus = client
                .get(&format!("v1/workers/{}/drain", id.as_str()))
                .await
                .map_err(api_error)?;
            if status.state == "completed" {
                emit_drain(output, &status, true, true)
            } else {
                let status = wait_for_drain(client, id, status, timeout_seconds, output).await?;
                emit_drain(output, &status, true, true)
            }
        }
        WorkerCommand::Undrain { worker_id } => {
            let id = parse_worker_id(&worker_id)?;
            client
                .delete_empty(&format!("v1/workers/{}/drain", id.as_str()), None, None)
                .await
                .map_err(api_error)?;
            emit(
                output,
                &json!({"workerId":id.as_str(),"state":"accepting_placements"}),
                &format!("worker {} accepts new placements", id.as_str()),
            )
        }
    }
}

async fn wait_for_drain(
    client: &ApiClient,
    worker_id: WorkerId,
    accepted: WorkerDrainStatus,
    timeout_seconds: u64,
    output: OutputFormat,
) -> Result<WorkerDrainStatus, CommandError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
    let mut current = accepted;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let path = format!("v1/workers/{}/drain", worker_id.as_str());
    loop {
        if current.state == "completed" {
            return Ok(current);
        }
        if current.state == "blocked" {
            return Err(CommandError::new(
                "drain_blocked",
                format!(
                    "worker {} drain is blocked: {}",
                    worker_id.as_str(),
                    current
                        .blocked_reason
                        .as_deref()
                        .unwrap_or("unknown reason")
                ),
            )
            .partial(drain_partial(&current, true)));
        }
        if output == OutputFormat::Human {
            let _ = writeln!(
                io::stderr().lock(),
                "worker {} drain: {} ({} active runs)",
                worker_id.as_str(),
                current.state,
                current.active_runs
            );
        }
        let fetched = tokio::select! {
            signal = &mut ctrl_c => {
                if signal.is_err() {
                    return Err(CommandError::new("interrupt_handler_error", "could not listen for Ctrl-C while waiting for worker drain")
                        .partial(drain_partial(&current, true)));
                }
                return Err(CommandError::new("drain_interrupted", format!("stopped waiting for worker {} drain after Ctrl-C", worker_id.as_str()))
                    .partial(drain_partial(&current, true)));
            }
            result = tokio::time::timeout_at(deadline, client.get::<WorkerDrainStatus>(&path)) => result,
        };
        current = match fetched {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => return Err(api_error(error).partial(drain_partial(&current, true))),
            Err(_) => {
                return Err(CommandError::new(
                    "drain_timeout",
                    format!("timed out waiting for worker {} drain", worker_id.as_str()),
                )
                .partial(drain_partial(&current, true)));
            }
        };
        if current.state != "completed" && current.state != "draining" && current.state != "blocked"
        {
            return Err(CommandError::new(
                "invalid_drain_status",
                "manager returned an unsupported worker drain state",
            )
            .partial(drain_partial(&current, true)));
        }
        if current.state != "completed" {
            tokio::select! {
                signal = &mut ctrl_c => {
                    if signal.is_err() {
                        return Err(CommandError::new("interrupt_handler_error", "could not listen for Ctrl-C while waiting for worker drain")
                            .partial(drain_partial(&current, true)));
                    }
                    return Err(CommandError::new("drain_interrupted", format!("stopped waiting for worker {} drain after Ctrl-C", worker_id.as_str()))
                        .partial(drain_partial(&current, true)));
                }
                () = tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + Duration::from_millis(200))) => {}
            }
        }
    }
}

fn parse_worker_id(raw: &str) -> Result<WorkerId, CommandError> {
    WorkerId::new(raw.to_owned())
        .map_err(|_| CommandError::new("invalid_worker_id", "worker ID is invalid"))
}

fn worker_value(worker: &WorkerStatus) -> Value {
    serde_json::to_value(worker).unwrap_or(Value::Null)
}

fn emit_workers(output: OutputFormat, workers: &[WorkerStatus]) -> Result<(), CommandError> {
    let values: Vec<Value> = workers.iter().map(worker_value).collect();
    emit(
        output,
        &json!({"workers":values,"count":workers.len()}),
        &if workers.is_empty() {
            "no workers registered".to_owned()
        } else {
            workers
                .iter()
                .map(worker_summary)
                .collect::<Vec<_>>()
                .join("\n")
        },
    )
}

fn emit_worker(output: OutputFormat, worker: &WorkerStatus) -> Result<(), CommandError> {
    emit(output, &worker_value(worker), &worker_summary(worker))
}

fn worker_summary(worker: &WorkerStatus) -> String {
    let availability = if worker.available() {
        "available"
    } else {
        "unavailable"
    };
    let freshness = if worker.fresh() {
        "fresh"
    } else {
        "stale or unknown freshness"
    };
    let drain = if worker.draining() { ", draining" } else { "" };
    format!(
        "worker {}: {}, {}, capacity {}/{}, {} live processes, {} unknown runs{}",
        worker.worker_id().as_str(),
        availability,
        freshness,
        worker.used_slots(),
        worker.capacity(),
        worker.live_processes(),
        worker.unknown_runs(),
        drain
    )
}

fn drain_partial(status: &WorkerDrainStatus, accepted: bool) -> Value {
    json!({
        "workerId": status.worker_id.as_str(),
        "desiredStateAccepted": accepted,
        "drainComplete": status.state == "completed",
        "remoteMutationCancelled": false,
        "drain": status,
    })
}

fn emit_drain(
    output: OutputFormat,
    status: &WorkerDrainStatus,
    desired_state_accepted: bool,
    drain_complete: bool,
) -> Result<(), CommandError> {
    emit(
        output,
        &json!({
            "workerId": status.worker_id.as_str(),
            "state": status.state,
            "requestedAtMs": status.requested_at_ms,
            "deadlineAtMs": status.deadline_at_ms,
            "completedAtMs": status.completed_at_ms,
            "activeRuns": status.active_runs,
            "blockedReason": status.blocked_reason,
            "desiredStateAccepted": desired_state_accepted,
            "drainComplete": drain_complete,
            "drain": status,
        }),
        &format!(
            "worker {} drain {} ({} active runs{})",
            status.worker_id.as_str(),
            status.state,
            status.active_runs,
            status
                .blocked_reason
                .as_ref()
                .map_or(String::new(), |reason| format!(", blocked: {reason}"))
        ),
    )
}

fn emit<T: Serialize>(output: OutputFormat, value: &T, human: &str) -> Result<(), CommandError> {
    let mut stdout = io::stdout().lock();
    match output {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut stdout, value).map_err(|_| {
                CommandError::new("output_error", "worker response could not be written")
            })?;
            stdout.write_all(b"\n").map_err(|_| {
                CommandError::new("output_error", "worker response could not be written")
            })
        }
        OutputFormat::Human => stdout
            .write_all(human.as_bytes())
            .and_then(|()| stdout.write_all(b"\n"))
            .map_err(|_| CommandError::new("output_error", "worker response could not be written")),
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
            let _ = writeln!(stderr, "conductor worker: {message}");
            if let Some(partial) = partial {
                let _ = writeln!(stderr, "partial result: {partial}");
            }
        }
    }
    ExitCode::FAILURE
}
