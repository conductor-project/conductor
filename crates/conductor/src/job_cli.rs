//! Job registration, submission, and immutable revision commands.

use std::fs;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use conductor_api::client::{Client as ApiClient, ClientError};
use conductor_api::contracts::JobCollection;
use conductor_core::{
    JobId, JobRevision, JobRevisionDraft, JobRevisionId, JobSpec, RunObservation,
};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Args)]
pub struct JobArgs {
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
    command: JobCommand,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
#[value(rename_all = "lower")]
enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Debug, Subcommand)]
enum JobCommand {
    /// Register a one-shot job definition without starting a run.
    Register {
        /// Strict JSON job file, or `-` to read standard input.
        input: PathBuf,
    },
    /// Register a one-shot job and start one run. Reuse the same key to safely retry.
    Submit {
        /// Strict JSON job file, or `-` to read standard input.
        input: PathBuf,
        /// Stable idempotency key for the run start; reuse it when retrying this submission.
        #[arg(long, required = true)]
        idempotency_key: String,
        /// Wait for this run to finish; timeout does not stop the remote run.
        #[arg(long)]
        wait: bool,
        /// Maximum seconds to wait when `--wait` is set.
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        wait_timeout_seconds: u64,
    },
    /// List registered jobs using bounded pagination.
    List {
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=100))]
        limit: u32,
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Inspect a job by its stable ID.
    Inspect { job_id: String },
    /// Delete a job that has no active or unresolved run. Its ID remains reserved.
    Delete { job_id: String },
    /// Create or inspect an immutable job revision.
    Revision {
        #[command(subcommand)]
        command: RevisionCommand,
    },
}

#[derive(Debug, Subcommand)]
enum RevisionCommand {
    /// Create a revision from a strict JSON draft file or `-` on standard input.
    Create {
        job_id: String,
        input: PathBuf,
        /// Idempotency key for this revision creation.
        #[arg(long, required = true)]
        idempotency_key: String,
    },
    /// Inspect an immutable revision by job ID and revision number.
    Inspect { job_id: String, revision: u64 },
}

pub fn run(args: JobArgs) -> ExitCode {
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
                "job command runtime could not be started".to_owned(),
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
    command: JobCommand,
    output: OutputFormat,
) -> Result<(), CommandError> {
    match command {
        JobCommand::Register { input } => {
            let spec = read_json::<JobSpec>(&input, "job")?;
            let job: JobSpec = client
                .post("v1/jobs", &spec, None)
                .await
                .map_err(api_error)?;
            emit(
                output,
                &json!({"job": job}),
                &format!("registered job {}", job.id().as_str()),
            )
        }
        JobCommand::Submit {
            input,
            idempotency_key,
            wait,
            wait_timeout_seconds,
        } => {
            validate_idempotency_key(&idempotency_key)?;
            let spec = read_json::<JobSpec>(&input, "job")?;
            let job_id = spec.id().clone();
            let (registered, reused_registration) = match client
                .post::<_, JobSpec>("v1/jobs", &spec, None)
                .await
            {
                Ok(job) => (job, false),
                Err(ClientError::Api { status, code, .. })
                    if status == reqwest::StatusCode::CONFLICT
                        && code.as_deref() == Some("duplicate_job") =>
                {
                    match client
                        .get::<JobSpec>(&format!("v1/jobs/{}", job_id.as_str()))
                        .await
                    {
                        Ok(existing) if existing == spec => (existing, true),
                        Ok(_) => {
                            return Err(CommandError::new(
                                "duplicate_job",
                                "a different job definition already uses this ID; no run was started",
                            ));
                        }
                        Err(ClientError::Api { status, .. })
                            if status == reqwest::StatusCode::NOT_FOUND =>
                        {
                            return Err(CommandError::new(
                                "reserved_job_id",
                                "this job ID is reserved by a deleted job and cannot be reused",
                            ));
                        }
                        Err(error) => return Err(api_error(error)),
                    }
                }
                Err(error) => return Err(api_error(error)),
            };
            let run = match client
                .post::<_, Value>(
                    &format!("v1/jobs/{}/runs", job_id.as_str()),
                    &json!({}),
                    Some(&idempotency_key),
                )
                .await
            {
                Ok(value) => value,
                Err(error) => {
                    let recovery = json!({
                        "jobId": job_id.as_str(),
                        "runStart": "failed_or_unknown",
                        "recovery": "Repeat the same `conductor job submit` command with the same input and same idempotency key; registration is idempotent for an identical job definition.",
                    });
                    let code = match &error {
                        ClientError::Api {
                            code: Some(code), ..
                        } => code.clone(),
                        _ => "run_start_failed".to_owned(),
                    };
                    let message = format!(
                        "job {} is registered but run start failed or has an unknown outcome: {}",
                        job_id.as_str(),
                        error
                    );
                    return Err(CommandError::new(code, message).partial(recovery));
                }
            };
            let run: RunObservation = serde_json::from_value(run).map_err(|_| {
                CommandError::new(
                    "api_decode_error",
                    "the API returned an invalid run response",
                )
            })?;
            let mut result = json!({
                "jobId": registered.id().as_str(),
                "runId": run.run_id().as_str(),
                "run": run,
                "reusedRegistration": reused_registration,
            });
            if wait {
                let completed =
                    match wait_for_run(client, run.run_id().as_str(), wait_timeout_seconds).await {
                        Ok(completed) => completed,
                        Err(error) => {
                            let mut partial = error.partial.unwrap_or_else(|| json!({}));
                            partial["jobId"] = json!(job_id.as_str());
                            partial["runId"] = json!(run.run_id().as_str());
                            partial["remoteRunStopped"] = json!(false);
                            return Err(CommandError {
                                code: error.code,
                                message: error.message,
                                partial: Some(partial),
                            });
                        }
                    };
                result["run"] = serde_json::to_value(&completed).map_err(|_| {
                    CommandError::new(
                        "api_decode_error",
                        "the API run response could not be encoded",
                    )
                })?;
                result["state"] = json!(completed.state().as_str());
                if completed.state() != conductor_core::RunState::Succeeded {
                    return Err(CommandError::new(
                        "run_failed",
                        format!(
                            "run {} ended in {}",
                            completed.run_id().as_str(),
                            completed.state().as_str()
                        ),
                    )
                    .partial(result));
                }
            }
            emit(
                output,
                &result,
                &format!(
                    "submitted job {} as run {}",
                    job_id.as_str(),
                    run.run_id().as_str()
                ),
            )
        }
        JobCommand::List { limit, cursor } => {
            let path = page_path(limit, cursor.as_deref());
            let collection: JobCollection = client.get(&path).await.map_err(api_error)?;
            emit(
                output,
                &json!(collection),
                &format!("{} jobs", collection.jobs.len()),
            )
        }
        JobCommand::Inspect { job_id } => {
            let id = parse_job_id(&job_id)?;
            let job: JobSpec = client
                .get(&format!("v1/jobs/{}", id.as_str()))
                .await
                .map_err(api_error)?;
            emit(
                output,
                &json!({"job": job}),
                &format!("job {}", id.as_str()),
            )
        }
        JobCommand::Delete { job_id } => {
            let id = parse_job_id(&job_id)?;
            client
                .delete_empty(&format!("v1/jobs/{}", id.as_str()), None, None)
                .await
                .map_err(api_error)?;
            emit(
                output,
                &json!({"jobId": id.as_str(), "deleted": true, "idReserved": true}),
                &format!("deleted job {}; its ID remains reserved", id.as_str()),
            )
        }
        JobCommand::Revision { command } => match command {
            RevisionCommand::Create {
                job_id,
                input,
                idempotency_key,
            } => {
                validate_idempotency_key(&idempotency_key)?;
                let id = parse_job_id(&job_id)?;
                let draft = read_json::<JobRevisionDraft>(&input, "job revision")?;
                if draft.job_id() != &id {
                    return Err(CommandError::new(
                        "revision_job_mismatch",
                        "the revision draft job ID must match the job ID argument",
                    ));
                }
                let revision: JobRevision = client
                    .post(
                        &format!("v1/jobs/{}/revisions", id.as_str()),
                        &draft,
                        Some(&idempotency_key),
                    )
                    .await
                    .map_err(api_error)?;
                emit(
                    output,
                    &json!({"jobId": id.as_str(), "revisionId": {"jobId": id.as_str(), "revision": revision.id().revision()}, "revision": revision}),
                    &format!(
                        "created revision {} for job {}",
                        revision.id().revision(),
                        id.as_str()
                    ),
                )
            }
            RevisionCommand::Inspect { job_id, revision } => {
                let id = parse_job_id(&job_id)?;
                let revision_id = JobRevisionId::new(id.clone(), revision).map_err(|_| {
                    CommandError::new("invalid_revision", "revision must be a positive integer")
                })?;
                let value: JobRevision = client
                    .get(&format!(
                        "v1/jobs/{}/revisions/{}",
                        id.as_str(),
                        revision_id.revision()
                    ))
                    .await
                    .map_err(api_error)?;
                emit(
                    output,
                    &json!({"jobId": id.as_str(), "revisionId": {"jobId": id.as_str(), "revision": revision_id.revision()}, "revision": value}),
                    &format!("revision {}", revision_id.revision()),
                )
            }
        },
    }
}

async fn wait_for_run(
    client: &ApiClient,
    run_id: &str,
    timeout_seconds: u64,
) -> Result<RunObservation, CommandError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
    loop {
        let run: RunObservation =
            match tokio::time::timeout_at(deadline, client.get(&format!("v1/runs/{run_id}"))).await
            {
                Ok(Ok(run)) => run,
                Ok(Err(error)) => return Err(api_error(error)),
                Err(_) => {
                    return Err(CommandError::new(
                        "wait_timeout",
                        format!(
                            "timed out waiting for run {run_id}; the remote run was not stopped"
                        ),
                    )
                    .partial(json!({"runId": run_id})));
                }
            };
        if run.state() == conductor_core::RunState::Unknown {
            return Err(CommandError::new(
                "run_outcome_unknown",
                format!("run {run_id} has an unknown outcome and requires operator resolution"),
            )
            .partial(json!({
                "runId": run_id,
                "runState": run.state().as_str(),
            })));
        }
        if run.state().is_terminal() {
            return Ok(run);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(CommandError::new(
                "wait_timeout",
                format!("timed out waiting for run {run_id}; the remote run was not stopped"),
            )
            .partial(json!({
                "runId": run_id,
                "runState": run.state().as_str(),
            })));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn read_json<T: serde::de::DeserializeOwned>(
    input: &PathBuf,
    noun: &str,
) -> Result<T, CommandError> {
    let mut bytes = Vec::new();
    if input.as_os_str() == "-" {
        io::stdin()
            .take((4 * 1024 * 1024 + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| {
                CommandError::new(
                    "input_error",
                    format!("{noun} JSON could not be read from stdin"),
                )
            })?;
    } else {
        let file = fs::File::open(input).map_err(|_| {
            CommandError::new(
                "input_error",
                format!("{noun} JSON file could not be opened"),
            )
        })?;
        file.take((4 * 1024 * 1024 + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| {
                CommandError::new("input_error", format!("{noun} JSON file could not be read"))
            })?;
    }
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(CommandError::new(
            "input_too_large",
            format!("{noun} JSON exceeds the 4 MiB limit"),
        ));
    }
    let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
        CommandError::new(
            "invalid_json",
            format!(
                "{noun} JSON is malformed at line {}, column {}",
                error.line(),
                error.column()
            ),
        )
    })?;
    let revision_draft_shape = value.get("definition").is_some() && value.get("workload").is_some();
    let service_definition_shape =
        value.get("workload").is_some() || value.get("deployment").is_some();
    if noun == "job" && (revision_draft_shape || service_definition_shape) {
        return Err(CommandError::new(
            "service_requires_deployment",
            "service workloads are not one-shot jobs; create a revision with `conductor job revision create` and use `conductor operator deploy`",
        ));
    }
    serde_json::from_value(value).map_err(|error| {
        CommandError::new(
            "invalid_job",
            safe_serde_error(&error.to_string(), error.line(), error.column()),
        )
    })
}

fn safe_serde_error(error: &str, line: usize, column: usize) -> String {
    for prefix in ["missing field `", "unknown field `"] {
        if let Some(rest) = error.strip_prefix(prefix)
            && let Some((field, _)) = rest.split_once('`')
        {
            return format!("invalid input: {prefix}{field}`");
        }
    }
    format!("invalid input at line {line}, column {column}")
}

fn parse_job_id(raw: &str) -> Result<JobId, CommandError> {
    JobId::new(raw.to_owned()).map_err(|_| CommandError::new("invalid_job_id", "job ID is invalid"))
}

fn validate_idempotency_key(key: &str) -> Result<(), CommandError> {
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

fn page_path(limit: u32, cursor: Option<&str>) -> String {
    let mut url = reqwest::Url::parse("http://conductor.invalid/").expect("constant URL is valid");
    url.path_segments_mut()
        .expect("base URL supports path segments")
        .push("v1")
        .push("jobs");
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("limit", &limit.to_string());
        if let Some(cursor) = cursor {
            query.append_pair("cursor", cursor);
        }
    }
    url.as_str()
        .strip_prefix("http://conductor.invalid/")
        .expect("constant URL prefix")
        .to_owned()
}

fn emit<T: Serialize>(output: OutputFormat, value: &T, human: &str) -> Result<(), CommandError> {
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    match output {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut lock, value).map_err(|_| {
                CommandError::new("output_error", "job response could not be written")
            })?;
            lock.write_all(b"\n")
                .map_err(|_| CommandError::new("output_error", "job response could not be written"))
        }
        OutputFormat::Human => lock
            .write_all(human.as_bytes())
            .and_then(|()| lock.write_all(b"\n"))
            .map_err(|_| CommandError::new("output_error", "job response could not be written")),
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
            let mut error = json!({"code": code, "message": message});
            if let Some(partial) = partial {
                error["partial"] = partial;
            }
            let _ = serde_json::to_writer(&mut lock, &json!({"error": error}));
            let _ = lock.write_all(b"\n");
        }
        OutputFormat::Human => {
            let _ = writeln!(lock, "conductor job: {message}");
            if let Some(partial) = partial {
                let _ = writeln!(lock, "partial result: {partial}");
            }
        }
    }
    ExitCode::FAILURE
}
