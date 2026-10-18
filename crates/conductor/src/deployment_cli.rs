//! Conditional deployment desired-state operations.

use std::fs;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use conductor_api::client::{Client as ApiClient, ClientError};
use conductor_api::contracts::{DeployRequest, RollbackRequest, ScaleDeploymentRequest};
use conductor_core::{DeploymentPendingReason, DeploymentStatus, JobDeployment, JobId};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Args)]
pub struct DeploymentArgs {
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
    command: DeploymentCommand,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Debug, Subcommand)]
enum DeploymentCommand {
    /// Create a deployment, or update it only when `--generation` matches.
    Apply {
        job_id: String,
        /// Strict JSON deployment request containing revision, replicas, and optional rollout strategy.
        input: PathBuf,
        /// Expected generation for update; omit to create only (If-None-Match: *).
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        generation: Option<u64>,
        #[arg(long, required = true)]
        idempotency_key: String,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        wait_timeout_seconds: u64,
    },
    /// Inspect deployment intent and its current generation.
    Inspect { job_id: String },
    /// Change desired replica count using the current generation.
    Scale {
        job_id: String,
        #[arg(long, value_parser = clap::value_parser!(u32).range(0..=10_000))]
        replicas: u32,
        #[arg(long, required = true, value_parser = clap::value_parser!(u64).range(1..))]
        generation: u64,
        #[arg(long, required = true)]
        idempotency_key: String,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        wait_timeout_seconds: u64,
    },
    /// Suspend a deployment while retaining its desired replica count.
    Suspend {
        job_id: String,
        #[arg(long, required = true, value_parser = clap::value_parser!(u64).range(1..))]
        generation: u64,
        #[arg(long, required = true)]
        idempotency_key: String,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        wait_timeout_seconds: u64,
    },
    /// Resume a suspended deployment at its retained desired replica count.
    Resume {
        job_id: String,
        #[arg(long, required = true, value_parser = clap::value_parser!(u64).range(1..))]
        generation: u64,
        #[arg(long, required = true)]
        idempotency_key: String,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        wait_timeout_seconds: u64,
    },
    /// Roll back to an explicitly selected immutable revision.
    Rollback {
        job_id: String,
        /// Existing revision number to select.
        #[arg(long, required = true, value_parser = clap::value_parser!(u64).range(1..))]
        revision: u64,
        #[arg(long, required = true, value_parser = clap::value_parser!(u64).range(1..))]
        generation: u64,
        #[arg(long, required = true)]
        idempotency_key: String,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        wait_timeout_seconds: u64,
    },
    /// Delete deployment intent; active instances are stopped and the deployment remains as deleted history.
    Delete {
        job_id: String,
        #[arg(long, required = true, value_parser = clap::value_parser!(u64).range(1..))]
        generation: u64,
        #[arg(long, required = true)]
        idempotency_key: String,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        wait_timeout_seconds: u64,
    },
}

pub fn run(args: DeploymentArgs) -> ExitCode {
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
                "deployment command runtime could not be started".to_owned(),
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
        _ => "api_error".to_owned(),
    };
    CommandError::new(code, error.to_string())
}

async fn execute(
    client: &ApiClient,
    command: DeploymentCommand,
    output: OutputFormat,
) -> Result<(), CommandError> {
    match command {
        DeploymentCommand::Apply {
            job_id,
            input,
            generation,
            idempotency_key,
            wait,
            wait_timeout_seconds,
        } => {
            let id = parse_job_id(&job_id)?;
            validate_key(&idempotency_key)?;
            let request: DeployRequest = read_deploy_request(&input)?;
            let path = format!("v1/jobs/{}/deployment", id.as_str());
            let deployment = match generation {
                Some(expected) => {
                    client
                        .put(
                            &path,
                            &request,
                            Some(&idempotency_key),
                            Some(&etag(expected)),
                        )
                        .await
                }
                None => {
                    client
                        .put_create(&path, &request, Some(&idempotency_key))
                        .await
                }
            }
            .map_err(api_error)?;
            finish_mutation(
                client,
                id,
                deployment,
                wait,
                wait_timeout_seconds,
                target_for,
                output,
            )
            .await
        }
        DeploymentCommand::Inspect { job_id } => {
            let id = parse_job_id(&job_id)?;
            let response = client
                .get_response::<JobDeployment>(&format!("v1/jobs/{}/deployment", id.as_str()))
                .await
                .map_err(api_error)?;
            let generation = response.body.generation();
            let tag = response.etag.unwrap_or_else(|| etag(generation));
            emit_deployment(
                output,
                &response.body,
                &tag,
                true,
                is_complete(response.body.status()),
                &format!(
                    "deployment {} generation {}: {}",
                    id.as_str(),
                    generation,
                    status_name(response.body.status())
                ),
            )
        }
        DeploymentCommand::Scale {
            job_id,
            replicas,
            generation,
            idempotency_key,
            wait,
            wait_timeout_seconds,
        } => {
            let id = parse_job_id(&job_id)?;
            validate_key(&idempotency_key)?;
            let deployment = client
                .post_conditional(
                    &format!("v1/jobs/{}/deployment/scale", id.as_str()),
                    &ScaleDeploymentRequest { replicas },
                    Some(&idempotency_key),
                    Some(&etag(generation)),
                )
                .await
                .map_err(api_error)?;
            finish_mutation(
                client,
                id,
                deployment,
                wait,
                wait_timeout_seconds,
                target_for,
                output,
            )
            .await
        }
        DeploymentCommand::Suspend {
            job_id,
            generation,
            idempotency_key,
            wait,
            wait_timeout_seconds,
        } => {
            let id = parse_job_id(&job_id)?;
            validate_key(&idempotency_key)?;
            let deployment = client
                .post_conditional(
                    &format!("v1/jobs/{}/deployment/suspend", id.as_str()),
                    &json!({}),
                    Some(&idempotency_key),
                    Some(&etag(generation)),
                )
                .await
                .map_err(api_error)?;
            finish_mutation(
                client,
                id,
                deployment,
                wait,
                wait_timeout_seconds,
                |_| WaitTarget::Suspended,
                output,
            )
            .await
        }
        DeploymentCommand::Resume {
            job_id,
            generation,
            idempotency_key,
            wait,
            wait_timeout_seconds,
        } => {
            let id = parse_job_id(&job_id)?;
            validate_key(&idempotency_key)?;
            let deployment = client
                .post_conditional(
                    &format!("v1/jobs/{}/deployment/resume", id.as_str()),
                    &json!({}),
                    Some(&idempotency_key),
                    Some(&etag(generation)),
                )
                .await
                .map_err(api_error)?;
            finish_mutation(
                client,
                id,
                deployment,
                wait,
                wait_timeout_seconds,
                target_for,
                output,
            )
            .await
        }
        DeploymentCommand::Rollback {
            job_id,
            revision,
            generation,
            idempotency_key,
            wait,
            wait_timeout_seconds,
        } => {
            let id = parse_job_id(&job_id)?;
            validate_key(&idempotency_key)?;
            let deployment = client
                .post_conditional(
                    &format!("v1/jobs/{}/deployment/rollback", id.as_str()),
                    &RollbackRequest { revision },
                    Some(&idempotency_key),
                    Some(&etag(generation)),
                )
                .await
                .map_err(api_error)?;
            finish_mutation(
                client,
                id,
                deployment,
                wait,
                wait_timeout_seconds,
                target_for,
                output,
            )
            .await
        }
        DeploymentCommand::Delete {
            job_id,
            generation,
            idempotency_key,
            wait,
            wait_timeout_seconds,
        } => {
            let id = parse_job_id(&job_id)?;
            validate_key(&idempotency_key)?;
            let deployment: JobDeployment = client
                .delete(
                    &format!("v1/jobs/{}/deployment", id.as_str()),
                    Some(&idempotency_key),
                    Some(&etag(generation)),
                )
                .await
                .map_err(api_error)?;
            finish_mutation(
                client,
                id,
                deployment,
                wait,
                wait_timeout_seconds,
                |_| WaitTarget::Deleted,
                output,
            )
            .await
        }
    }
}

fn target_for(deployment: &JobDeployment) -> WaitTarget {
    if deployment.suspended() {
        WaitTarget::Suspended
    } else {
        WaitTarget::Ready
    }
}

#[derive(Clone, Copy)]
enum WaitTarget {
    Ready,
    Suspended,
    Deleted,
}

fn matches_target(deployment: &JobDeployment, target: WaitTarget) -> bool {
    match target {
        WaitTarget::Ready => deployment.status() == DeploymentStatus::Ready,
        WaitTarget::Suspended => deployment.status() == DeploymentStatus::Suspended,
        WaitTarget::Deleted => deployment.status() == DeploymentStatus::Deleted,
    }
}

fn is_complete(status: DeploymentStatus) -> bool {
    matches!(
        status,
        DeploymentStatus::Ready | DeploymentStatus::Suspended | DeploymentStatus::Deleted
    )
}

async fn finish_mutation(
    client: &ApiClient,
    job_id: JobId,
    deployment: JobDeployment,
    wait: bool,
    timeout_seconds: u64,
    target: fn(&JobDeployment) -> WaitTarget,
    output: OutputFormat,
) -> Result<(), CommandError> {
    let generation = deployment.generation();
    let target = target(&deployment);
    let mut etag_value = etag(generation);
    let mut current = deployment;
    if wait {
        current =
            wait_for_deployment(client, &job_id, current, target, timeout_seconds, output).await?;
        etag_value = etag(current.generation());
    }
    let complete = matches_target(&current, target);
    emit_deployment(
        output,
        &current,
        &etag_value,
        true,
        complete,
        &format!(
            "deployment {} generation {}: {}",
            job_id.as_str(),
            current.generation(),
            status_name(current.status())
        ),
    )
}

async fn wait_for_deployment(
    client: &ApiClient,
    job_id: &JobId,
    accepted: JobDeployment,
    target: WaitTarget,
    timeout_seconds: u64,
    output: OutputFormat,
) -> Result<JobDeployment, CommandError> {
    let expected_generation = accepted.generation();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
    let mut previous = None;
    let mut latest = accepted;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let path = format!("v1/jobs/{}/deployment", job_id.as_str());
    loop {
        let response = tokio::select! {
            signal = &mut ctrl_c => {
                if signal.is_err() {
                    return Err(CommandError::new(
                        "interrupt_handler_error",
                        "could not listen for Ctrl-C while waiting for deployment",
                    ).partial(wait_partial(job_id, expected_generation, &latest)));
                }
                return Err(CommandError::new(
                    "rollout_interrupted",
                    format!("stopped waiting for deployment {} generation {} after Ctrl-C", job_id.as_str(), expected_generation),
                ).partial(wait_partial(job_id, expected_generation, &latest)));
            }
            result = tokio::time::timeout_at(deadline, client.get_response::<JobDeployment>(&path)) => result,
        };
        let response = match response {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => return Err(api_error(error).partial(json!({"jobId":job_id.as_str(),"generation":expected_generation,"expectedGeneration":expected_generation}))),
            Err(_) => return Err(CommandError::new(
                "rollout_timeout",
                format!("timed out waiting for deployment {} generation {}", job_id.as_str(), expected_generation),
            ).partial(json!({"jobId":job_id.as_str(),"generation":expected_generation,"expectedGeneration":expected_generation,"remoteMutationCancelled":false}))),
        };
        let deployment = response.body;
        if deployment.generation() != expected_generation {
            return Err(CommandError::new(
                "deployment_generation_changed",
                format!("deployment {} advanced from generation {} to {} while waiting", job_id.as_str(), expected_generation, deployment.generation()),
            ).partial(json!({"jobId":job_id.as_str(),"generation":expected_generation,"expectedGeneration":expected_generation,"deployment":deployment})));
        }
        latest = deployment.clone();
        let progress = (deployment.status(), deployment.pending_reason());
        if Some(progress) != previous && output == OutputFormat::Human {
            let detail = deployment
                .pending_reason()
                .map_or_else(String::new, |reason| format!(" ({reason:?})"));
            let _ = writeln!(
                io::stderr().lock(),
                "deployment {} generation {}: {}{}",
                job_id.as_str(),
                expected_generation,
                status_name(deployment.status()),
                detail
            );
        }
        previous = Some(progress);
        if matches_target(&deployment, target) {
            return Ok(deployment);
        }
        if matches!(
            deployment.pending_reason(),
            Some(
                DeploymentPendingReason::RolloutProgressDeadlineExceeded
                    | DeploymentPendingReason::ServiceRestartBudgetExhausted
                    | DeploymentPendingReason::ServiceAttemptUnresolved
            )
        ) {
            return Err(CommandError::new(
                "rollout_failed",
                format!(
                    "deployment {} made no further rollout progress: {}",
                    job_id.as_str(),
                    deployment
                        .pending_message()
                        .unwrap_or("the manager reported a terminal rollout blocker")
                ),
            )
            .partial(json!({"jobId":job_id.as_str(),"generation":expected_generation,"expectedGeneration":expected_generation,"deployment":deployment})));
        }
        tokio::select! {
            signal = &mut ctrl_c => {
                if signal.is_err() {
                    return Err(CommandError::new(
                        "interrupt_handler_error",
                        "could not listen for Ctrl-C while waiting for deployment",
                    ).partial(wait_partial(job_id, expected_generation, &latest)));
                }
                return Err(CommandError::new(
                    "rollout_interrupted",
                    format!("stopped waiting for deployment {} generation {} after Ctrl-C", job_id.as_str(), expected_generation),
                ).partial(wait_partial(job_id, expected_generation, &latest)));
            }
            () = tokio::time::sleep_until(
                deadline.min(tokio::time::Instant::now() + Duration::from_millis(200)),
            ) => {}
        }
    }
}

fn wait_partial(job_id: &JobId, generation: u64, deployment: &JobDeployment) -> Value {
    json!({
        "jobId": job_id.as_str(),
        "generation": generation,
        "expectedGeneration": generation,
        "remoteMutationCancelled": false,
        "deployment": deployment,
    })
}

fn read_deploy_request(path: &PathBuf) -> Result<DeployRequest, CommandError> {
    let file = fs::File::open(path).map_err(|_| {
        CommandError::new("input_error", "deployment JSON file could not be opened")
    })?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| CommandError::new("input_error", "deployment JSON file could not be read"))?;
    if bytes.len() > 1024 * 1024 {
        return Err(CommandError::new(
            "input_too_large",
            "deployment JSON exceeds the 1 MiB limit",
        ));
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        let text = error.to_string();
        let message = if text.starts_with("missing field `") || text.starts_with("unknown field `")
        {
            text.split(" at line")
                .next()
                .unwrap_or("deployment input is invalid")
                .to_owned()
        } else {
            format!(
                "deployment JSON is invalid at line {}, column {}",
                error.line(),
                error.column()
            )
        };
        CommandError::new("invalid_deployment", message)
    })
}

fn parse_job_id(raw: &str) -> Result<JobId, CommandError> {
    JobId::new(raw.to_owned()).map_err(|_| CommandError::new("invalid_job_id", "job ID is invalid"))
}

fn validate_key(key: &str) -> Result<(), CommandError> {
    if key.is_empty()
        || key.len() > 128
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._~-".contains(&byte))
    {
        return Err(CommandError::new(
            "invalid_idempotency_key",
            "idempotency key must contain 1-128 ASCII letters, digits, '.', '_', '~', or '-'",
        ));
    }
    Ok(())
}

fn etag(generation: u64) -> String {
    format!("\"{generation}\"")
}

fn status_name(status: DeploymentStatus) -> &'static str {
    match status {
        DeploymentStatus::Pending => "pending",
        DeploymentStatus::Ready => "ready",
        DeploymentStatus::Suspended => "suspended",
        DeploymentStatus::Deleting => "deleting",
        DeploymentStatus::Deleted => "deleted",
    }
}

fn deployment_output(
    deployment: &JobDeployment,
    tag: &str,
    desired_state_accepted: bool,
    rollout_complete: bool,
) -> Value {
    json!({
        "jobId": deployment.job_id().as_str(),
        "revision": deployment.revision_id().revision(),
        "generation": deployment.generation(),
        "etag": tag,
        "status": status_name(deployment.status()),
        "desiredReplicas": deployment.desired_replicas(),
        "suspended": deployment.suspended(),
        "pendingReason": deployment.pending_reason(),
        "pendingMessage": deployment.pending_message(),
        "desiredStateAccepted": desired_state_accepted,
        "rolloutComplete": rollout_complete,
        "deployment": deployment,
    })
}

fn emit_deployment(
    output: OutputFormat,
    deployment: &JobDeployment,
    tag: &str,
    desired_state_accepted: bool,
    rollout_complete: bool,
    human: &str,
) -> Result<(), CommandError> {
    emit(
        output,
        &deployment_output(deployment, tag, desired_state_accepted, rollout_complete),
        human,
    )
}

fn emit<T: Serialize>(output: OutputFormat, value: &T, human: &str) -> Result<(), CommandError> {
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    match output {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut lock, value).map_err(|_| {
                CommandError::new("output_error", "deployment response could not be written")
            })?;
            lock.write_all(b"\n").map_err(|_| {
                CommandError::new("output_error", "deployment response could not be written")
            })
        }
        OutputFormat::Human => lock
            .write_all(human.as_bytes())
            .and_then(|()| lock.write_all(b"\n"))
            .map_err(|_| {
                CommandError::new("output_error", "deployment response could not be written")
            }),
    }
}

fn report_error(
    output: OutputFormat,
    code: &str,
    message: String,
    partial: Option<Value>,
) -> ExitCode {
    let stderr = io::stderr();
    let mut lock = stderr.lock();
    match output {
        OutputFormat::Json => {
            let mut error = json!({"code":code,"message":message});
            if let Some(partial) = partial {
                error["partial"] = partial;
            }
            let _ = serde_json::to_writer(&mut lock, &json!({"error":error}));
            let _ = lock.write_all(b"\n");
        }
        OutputFormat::Human => {
            let _ = writeln!(lock, "conductor deployment: {message}");
            if let Some(partial) = partial {
                let _ = writeln!(lock, "partial result: {partial}");
            }
        }
    }
    ExitCode::FAILURE
}
