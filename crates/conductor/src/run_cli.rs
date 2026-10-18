//! Run lifecycle, observation, waiting, and log commands.

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use base64::Engine;
use clap::{Args, Subcommand, ValueEnum};
use conductor_api::client::{Client as ApiClient, ClientError};
use conductor_api::contracts::{RunCollection, RunLogsResponse};
use conductor_core::{JobId, LogStream, RunId, RunObservation, RunState};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Args)]
pub struct RunArgs {
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
    /// Output format; logs are raw bytes in human mode and base64 JSON lines in JSON mode.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Human)]
    output: OutputFormat,
    #[command(subcommand)]
    command: RunCommand,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
#[value(rename_all = "lower")]
enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    /// Start one run of a registered job.
    Start {
        job_id: String,
        /// Idempotency key used to safely retry this run start.
        #[arg(long, required = true)]
        idempotency_key: String,
    },
    /// List runs with optional job and state filters.
    List {
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long, value_enum)]
        state: Option<RunStateArg>,
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=100))]
        limit: u32,
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Inspect a run by its stable ID.
    Inspect { run_id: String },
    /// Request that a run stop. Waiting never stops a remote run.
    Stop { run_id: String },
    /// Resolve an unknown run by explicitly recording that it is abandoned.
    Resolve {
        run_id: String,
        /// Operator explanation for abandoning this unknown run.
        #[arg(long, required = true)]
        reason: String,
    },
    /// Wait for a terminal run outcome; timeout or interruption leaves the remote run alone.
    Wait {
        run_id: String,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        timeout_seconds: u64,
    },
    /// Read raw log bytes, or base64 JSON lines with `--output json`; `--follow` resumes by byte offset.
    Logs {
        run_id: String,
        #[arg(long, value_enum, default_value_t = LogStreamArg::Stdout)]
        stream: LogStreamArg,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 16384, value_parser = clap::value_parser!(u32).range(1..=65536))]
        limit: u32,
        /// Continue polling until the selected log stream reaches EOF.
        #[arg(long)]
        follow: bool,
        /// Poll interval while following, in milliseconds.
        #[arg(long, default_value_t = 200, value_parser = clap::value_parser!(u64).range(50..=5000))]
        poll_interval_ms: u64,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "lower")]
enum RunStateArg {
    Requested,
    Starting,
    Running,
    Succeeded,
    Failed,
    Stopping,
    Cancelled,
    TimedOut,
    Unknown,
    Abandoned,
}

impl From<RunStateArg> for RunState {
    fn from(value: RunStateArg) -> Self {
        match value {
            RunStateArg::Requested => Self::Requested,
            RunStateArg::Starting => Self::Starting,
            RunStateArg::Running => Self::Running,
            RunStateArg::Succeeded => Self::Succeeded,
            RunStateArg::Failed => Self::Failed,
            RunStateArg::Stopping => Self::Stopping,
            RunStateArg::Cancelled => Self::Cancelled,
            RunStateArg::TimedOut => Self::TimedOut,
            RunStateArg::Unknown => Self::Unknown,
            RunStateArg::Abandoned => Self::Abandoned,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "lower")]
enum LogStreamArg {
    Stdout,
    Stderr,
}

impl From<LogStreamArg> for LogStream {
    fn from(value: LogStreamArg) -> Self {
        match value {
            LogStreamArg::Stdout => Self::Stdout,
            LogStreamArg::Stderr => Self::Stderr,
        }
    }
}

pub fn run(args: RunArgs) -> ExitCode {
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
                "run command runtime could not be started".to_owned(),
                None,
            );
        }
    };
    match runtime.block_on(execute(&client, args.command, output)) {
        Ok(code) => code,
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
    command: RunCommand,
    output: OutputFormat,
) -> Result<ExitCode, CommandError> {
    match command {
        RunCommand::Start {
            job_id,
            idempotency_key,
        } => {
            let job_id = parse_job_id(&job_id)?;
            validate_key(&idempotency_key)?;
            let run: RunObservation = client
                .post(
                    &format!("v1/jobs/{}/runs", job_id.as_str()),
                    &json!({}),
                    Some(&idempotency_key),
                )
                .await
                .map_err(api_error)?;
            emit(
                output,
                &run,
                &format!(
                    "started run {} for job {}",
                    run.run_id().as_str(),
                    job_id.as_str()
                ),
            )?;
            Ok(ExitCode::SUCCESS)
        }
        RunCommand::List {
            job_id,
            state,
            limit,
            cursor,
        } => {
            let job_id = job_id.map(|raw| parse_job_id(&raw)).transpose()?;
            let path = run_list_path(
                limit,
                job_id.as_ref().map(JobId::as_str),
                state.map(RunState::from),
                cursor.as_deref(),
            );
            let collection: RunCollection = client.get(&path).await.map_err(api_error)?;
            emit(
                output,
                &collection,
                &format!("{} runs", collection.runs.len()),
            )?;
            Ok(ExitCode::SUCCESS)
        }
        RunCommand::Inspect { run_id } => {
            let id = parse_run_id(&run_id)?;
            let run: RunObservation = client
                .get(&format!("v1/runs/{}", id.as_str()))
                .await
                .map_err(api_error)?;
            emit(
                output,
                &run,
                &format!("run {}: {}", id.as_str(), run.state().as_str()),
            )?;
            Ok(ExitCode::SUCCESS)
        }
        RunCommand::Stop { run_id } => {
            let id = parse_run_id(&run_id)?;
            let run: RunObservation = client
                .post(&format!("v1/runs/{}/stop", id.as_str()), &json!({}), None)
                .await
                .map_err(api_error)?;
            emit(
                output,
                &run,
                &format!("stop requested for run {}", id.as_str()),
            )?;
            Ok(ExitCode::SUCCESS)
        }
        RunCommand::Resolve { run_id, reason } => {
            let id = parse_run_id(&run_id)?;
            if reason.trim().is_empty() || reason.len() > 512 {
                return Err(CommandError::new(
                    "invalid_resolution",
                    "resolution reason must contain 1-512 bytes",
                ));
            }
            let run: RunObservation = client
                .post(
                    &format!("v1/runs/{}/resolve", id.as_str()),
                    &json!({"resolution":"abandon", "reason":reason}),
                    None,
                )
                .await
                .map_err(api_error)?;
            emit(output, &run, &format!("abandoned run {}", id.as_str()))?;
            Ok(ExitCode::SUCCESS)
        }
        RunCommand::Wait {
            run_id,
            timeout_seconds,
        } => {
            let id = parse_run_id(&run_id)?;
            match wait_for_run(client, id.as_str(), timeout_seconds).await {
                Ok(run) if run.state() == RunState::Succeeded => {
                    emit(output, &run, &format!("run {} succeeded", id.as_str()))?;
                    Ok(ExitCode::SUCCESS)
                }
                Ok(run) => {
                    let state = run.state();
                    Err(CommandError::new(wait_outcome_code(state), format!("run {} ended in {}", id.as_str(), state.as_str())).partial(json!({"run":run})))
                }
                Err(WaitFailure::Timeout { run_id, state }) => Err(CommandError::new(
                    "wait_timeout",
                    format!("timed out waiting for run {run_id}; the remote run was not stopped"),
                ).partial(json!({"runId":run_id,"runState":state.map(RunState::as_str),"remoteRunStopped":false}))),
                Err(WaitFailure::Unknown(run)) => Err(CommandError::new(
                    "run_outcome_unknown",
                    format!("run {} has an unknown outcome and requires operator resolution", run.run_id().as_str()),
                ).partial(json!({"run":run}))),
                Err(WaitFailure::Api(error)) => Err(api_error(error)),
            }
        }
        RunCommand::Logs {
            run_id,
            stream,
            offset,
            limit,
            follow,
            poll_interval_ms,
        } => {
            let id = parse_run_id(&run_id)?;
            follow_logs(
                client,
                id,
                LogOptions {
                    stream: stream.into(),
                    offset,
                    limit,
                    follow,
                    poll_interval: Duration::from_millis(poll_interval_ms),
                    output,
                },
            )
            .await?;
            Ok(ExitCode::SUCCESS)
        }
    }
}

#[derive(Debug)]
pub(crate) enum WaitFailure {
    Timeout {
        run_id: String,
        state: Option<RunState>,
    },
    Unknown(Box<RunObservation>),
    Api(ClientError),
}

/// Shared terminal-state polling used by `run wait` and `job submit --wait`.
pub(crate) async fn wait_for_run(
    client: &ApiClient,
    run_id: &str,
    timeout_seconds: u64,
) -> Result<RunObservation, WaitFailure> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
    let mut last_state = None;
    loop {
        let run: RunObservation =
            match tokio::time::timeout_at(deadline, client.get(&format!("v1/runs/{run_id}"))).await
            {
                Ok(Ok(run)) => run,
                Ok(Err(error)) => return Err(WaitFailure::Api(error)),
                Err(_) => {
                    return Err(WaitFailure::Timeout {
                        run_id: run_id.to_owned(),
                        state: last_state,
                    });
                }
            };
        if run.state() == RunState::Unknown {
            return Err(WaitFailure::Unknown(Box::new(run)));
        }
        if run.state().is_terminal() {
            return Ok(run);
        }
        last_state = Some(run.state());
        tokio::time::sleep_until(
            deadline.min(tokio::time::Instant::now() + Duration::from_millis(100)),
        )
        .await;
    }
}

fn wait_outcome_code(state: RunState) -> &'static str {
    match state {
        RunState::Succeeded => "run_succeeded",
        RunState::Failed => "run_failed",
        RunState::Cancelled => "run_cancelled",
        RunState::TimedOut => "run_timed_out",
        RunState::Abandoned => "run_abandoned",
        RunState::Unknown => "run_outcome_unknown",
        RunState::Requested | RunState::Starting | RunState::Running | RunState::Stopping => {
            "run_not_terminal"
        }
    }
}

struct LogOptions {
    stream: LogStream,
    offset: u64,
    limit: u32,
    follow: bool,
    poll_interval: Duration,
    output: OutputFormat,
}

async fn follow_logs(
    client: &ApiClient,
    run_id: RunId,
    options: LogOptions,
) -> Result<(), CommandError> {
    let LogOptions {
        stream,
        mut offset,
        limit,
        follow,
        poll_interval,
        output,
    } = options;
    let mut truncation_reported = false;
    loop {
        let path = run_logs_path(run_id.as_str(), stream, offset, limit);
        let response: RunLogsResponse = client.get(&path).await.map_err(|error| {
            api_error(error).partial(json!({"runId":run_id.as_str(),"stream":stream.as_str(),"nextOffset":offset,"remoteRunStopped":false}))
        })?;
        if response.run_id != run_id.as_str()
            || response.stream != stream
            || response.offset != offset
            || response.data_encoding != "base64"
        {
            return Err(CommandError::new(
                "api_decode_error",
                "the API returned inconsistent run log metadata",
            ));
        }
        let data = base64::engine::general_purpose::STANDARD
            .decode(&response.data)
            .map_err(|_| {
                CommandError::new(
                    "api_decode_error",
                    "the API returned invalid base64 log bytes",
                )
            })?;
        if response.next_offset != offset.saturating_add(data.len() as u64) {
            return Err(CommandError::new(
                "api_decode_error",
                "the API returned inconsistent run log offsets",
            ));
        }
        match output {
            OutputFormat::Human => {
                io::stdout().lock().write_all(&data).map_err(|_| {
                    CommandError::new("output_error", "run log bytes could not be written")
                })?;
                if response.truncated && !truncation_reported {
                    let _ = writeln!(
                        io::stderr().lock(),
                        "run logs were truncated by the worker retention limit"
                    );
                    truncation_reported = true;
                }
            }
            OutputFormat::Json => {
                let stdout = io::stdout();
                let mut lock = stdout.lock();
                serde_json::to_writer(&mut lock, &response).map_err(|_| {
                    CommandError::new("output_error", "run log response could not be written")
                })?;
                lock.write_all(b"\n").map_err(|_| {
                    CommandError::new("output_error", "run log response could not be written")
                })?;
            }
        }
        offset = response.next_offset;
        if response.eof || !follow {
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }
}

fn run_list_path(
    limit: u32,
    job_id: Option<&str>,
    state: Option<RunState>,
    cursor: Option<&str>,
) -> String {
    let mut url = reqwest::Url::parse("http://conductor.invalid/").expect("constant URL is valid");
    url.path_segments_mut()
        .expect("URL supports path segments")
        .push("v1")
        .push("runs");
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("limit", &limit.to_string());
        if let Some(job_id) = job_id {
            query.append_pair("jobId", job_id);
        }
        if let Some(state) = state {
            query.append_pair("state", state.as_str());
        }
        if let Some(cursor) = cursor {
            query.append_pair("cursor", cursor);
        }
    }
    url.as_str()
        .strip_prefix("http://conductor.invalid/")
        .expect("constant URL prefix")
        .to_owned()
}

fn run_logs_path(run_id: &str, stream: LogStream, offset: u64, limit: u32) -> String {
    let mut url = reqwest::Url::parse("http://conductor.invalid/").expect("constant URL is valid");
    url.path_segments_mut()
        .expect("URL supports path segments")
        .push("v1")
        .push("runs")
        .push(run_id)
        .push("logs");
    url.query_pairs_mut()
        .append_pair("stream", stream.as_str())
        .append_pair("offset", &offset.to_string())
        .append_pair("limit", &limit.to_string());
    url.as_str()
        .strip_prefix("http://conductor.invalid/")
        .expect("constant URL prefix")
        .to_owned()
}

fn parse_job_id(raw: &str) -> Result<JobId, CommandError> {
    JobId::new(raw.to_owned()).map_err(|_| CommandError::new("invalid_job_id", "job ID is invalid"))
}

fn parse_run_id(raw: &str) -> Result<RunId, CommandError> {
    RunId::new(raw.to_owned()).map_err(|_| CommandError::new("invalid_run_id", "run ID is invalid"))
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

fn emit<T: Serialize>(output: OutputFormat, value: &T, human: &str) -> Result<(), CommandError> {
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    match output {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut lock, value).map_err(|_| {
                CommandError::new("output_error", "run response could not be written")
            })?;
            lock.write_all(b"\n")
                .map_err(|_| CommandError::new("output_error", "run response could not be written"))
        }
        OutputFormat::Human => lock
            .write_all(human.as_bytes())
            .and_then(|()| lock.write_all(b"\n"))
            .map_err(|_| CommandError::new("output_error", "run response could not be written")),
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
            let _ = writeln!(lock, "conductor run: {message}");
            if let Some(partial) = partial {
                let _ = writeln!(lock, "partial result: {partial}");
            }
        }
    }
    ExitCode::FAILURE
}
