use std::io::{self, Read, Write};
use std::net::IpAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use base64::Engine;
use clap::{Args, Subcommand, ValueEnum};
use conductor_core::{JobId, RunId, WorkerId};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue, IF_MATCH, IF_NONE_MATCH};
use reqwest::{Method, StatusCode, Url};
use serde_json::{Value, json};
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug, Args)]
pub struct OperatorArgs {
    /// Manager API URL. Plain HTTP is allowed only for loopback addresses.
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    server: String,
    /// Protected file containing the operator bearer token.
    #[arg(long)]
    token_file: PathBuf,
    /// Per-request timeout in seconds.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=300))]
    timeout_seconds: u64,
    #[command(subcommand)]
    command: OperatorCommand,
}

#[derive(Debug, Subcommand)]
enum OperatorCommand {
    /// List workers or inspect one stable worker ID.
    Workers {
        #[command(subcommand)]
        command: WorkerCommand,
    },
    /// Show deployment progress, pending placement reason, and stable replica allocations.
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    /// Select an immutable revision for a service deployment.
    Deploy {
        job_id: String,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        replicas: u32,
        /// Set only when creating the first deployment.
        #[arg(long, conflicts_with = "if_match_generation")]
        create: bool,
        /// Current deployment generation from the response or ETag.
        #[arg(long, requires = "idempotency_key")]
        if_match_generation: Option<u64>,
        #[arg(long, required = true)]
        idempotency_key: String,
        #[arg(long, default_value_t = 1)]
        max_unavailable: u32,
        #[arg(long, default_value_t = 300)]
        progress_deadline_seconds: u32,
    },
    /// Change the desired replica count using the deployment generation.
    Scale {
        job_id: String,
        #[arg(long)]
        replicas: u32,
        #[arg(long, required = true)]
        if_match_generation: u64,
        #[arg(long, required = true)]
        idempotency_key: String,
    },
    /// Roll back to an immutable revision using the deployment generation.
    Rollback {
        job_id: String,
        #[arg(long)]
        revision: u64,
        #[arg(long, required = true)]
        if_match_generation: u64,
        #[arg(long, required = true)]
        idempotency_key: String,
    },
    /// Start, inspect, or clear a completed worker drain.
    Drain {
        #[command(subcommand)]
        command: DrainCommand,
    },
    /// List durable run observations with bounded pagination.
    Runs {
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long, value_enum)]
        state: Option<RunFilterState>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        cursor: Option<String>,
    },
    /// Read one bounded base64-backed log chunk and write its raw bytes to stdout.
    Logs {
        run_id: String,
        #[arg(long, value_enum)]
        stream: LogStreamArg,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 4096)]
        limit: usize,
    },
}

#[derive(Debug, Subcommand)]
enum WorkerCommand {
    List,
    Inspect { worker_id: String },
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    Inspect { job_id: String },
}

#[derive(Debug, Subcommand)]
enum DrainCommand {
    Start {
        worker_id: String,
        #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=300))]
        grace_period_seconds: u64,
    },
    Status {
        worker_id: String,
    },
    Clear {
        worker_id: String,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "snake_case")]
enum RunFilterState {
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

impl RunFilterState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Stopping => "stopping",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Unknown => "unknown",
            Self::Abandoned => "abandoned",
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LogStreamArg {
    Stdout,
    Stderr,
}

impl LogStreamArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

struct OperatorClient {
    client: reqwest::Client,
    base_url: Url,
}

struct ResponseData {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl OperatorClient {
    fn new(args: &OperatorArgs) -> Result<Self, String> {
        let base_url = parse_server_url(&args.server)?;
        let mut token = read_operator_token(&args.token_file)?;
        let token_text = std::str::from_utf8(&token)
            .map_err(|_| "operator token file contains an invalid token".to_owned())?;
        let authorization = HeaderValue::from_str(&format!("Bearer {token_text}"))
            .map_err(|_| "operator token file contains an invalid token".to_owned())?;
        token.zeroize();
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, authorization);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(args.timeout_seconds))
            .build()
            .map_err(|_| "operator HTTP client could not be configured".to_owned())?;
        Ok(Self { client, base_url })
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        headers: &[(&str, String)],
        body: Option<Value>,
    ) -> Result<ResponseData, String> {
        let mut url = self
            .base_url
            .join(path)
            .map_err(|_| "operator API path is invalid".to_owned())?;
        if !query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(query.iter().map(|(key, value)| (*key, value.as_str())));
        }
        let mut request = self.client.request(method, url);
        for (name, value) in headers {
            let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| "operator request header is invalid".to_owned())?;
            let value = HeaderValue::from_str(value)
                .map_err(|_| "operator request header is invalid".to_owned())?;
            request = request.header(name, value);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(|_| {
            "operator API request failed; verify server reachability and TLS".to_owned()
        })?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(|_| "operator API response could not be read".to_owned())?
            .to_vec();
        Ok(ResponseData {
            status,
            headers,
            body,
        })
    }
}

pub fn run(args: OperatorArgs) -> ExitCode {
    let result = match OperatorClient::new(&args) {
        Ok(client) => match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime.block_on(execute(&client, args.command)),
            Err(_) => Err("operator async runtime could not be started".to_owned()),
        },
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("conductor operator: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(client: &OperatorClient, command: OperatorCommand) -> Result<(), String> {
    match command {
        OperatorCommand::Workers { command } => match command {
            WorkerCommand::List => {
                let response = client
                    .request(Method::GET, "v1/workers", &[], &[], None)
                    .await?;
                print_json_response(response)
            }
            WorkerCommand::Inspect { worker_id } => {
                let worker_id =
                    WorkerId::new(worker_id).map_err(|_| "worker ID is invalid".to_owned())?;
                let response = client
                    .request(
                        Method::GET,
                        &format!("v1/workers/{}", worker_id.as_str()),
                        &[],
                        &[],
                        None,
                    )
                    .await?;
                print_json_response(response)
            }
        },
        OperatorCommand::Service { command } => match command {
            ServiceCommand::Inspect { job_id } => {
                let job_id = JobId::new(job_id).map_err(|_| "job ID is invalid".to_owned())?;
                let deployment = client
                    .request(
                        Method::GET,
                        &format!("v1/jobs/{}/deployment", job_id.as_str()),
                        &[],
                        &[],
                        None,
                    )
                    .await?;
                let etag = deployment
                    .headers
                    .get(reqwest::header::ETAG)
                    .and_then(|header| header.to_str().ok())
                    .map(str::to_owned);
                let deployment = parse_json_response(deployment)?;
                let attempts = client
                    .request(
                        Method::GET,
                        &format!("v1/jobs/{}/service-attempts", job_id.as_str()),
                        &[],
                        &[],
                        None,
                    )
                    .await?;
                let attempts = parse_json_response(attempts)?;
                write_json(&json!({
                    "jobId": job_id.as_str(),
                    "etag": etag,
                    "deployment": deployment,
                    "serviceAttempts": attempts.get("serviceAttempts").cloned().unwrap_or(Value::Null),
                }))
            }
        },
        OperatorCommand::Deploy {
            job_id,
            revision,
            replicas,
            create,
            if_match_generation,
            idempotency_key,
            max_unavailable,
            progress_deadline_seconds,
        } => {
            let job_id = JobId::new(job_id).map_err(|_| "job ID is invalid".to_owned())?;
            let mut headers = vec![(
                "Idempotency-Key",
                validate_idempotency_key(&idempotency_key)?,
            )];
            if create {
                headers.push((IF_NONE_MATCH.as_str(), "*".to_owned()));
            } else {
                let generation = if_match_generation.ok_or_else(|| {
                    "--if-match-generation is required unless --create is set".to_owned()
                })?;
                headers.push((IF_MATCH.as_str(), format!("\"{generation}\"")));
            }
            let response = client
                .request(
                    Method::PUT,
                    &format!("v1/jobs/{}/deployment", job_id.as_str()),
                    &[],
                    &headers,
                    Some(json!({
                        "revision": revision,
                        "replicas": replicas,
                        "strategy": {
                            "maxSurge": 0,
                            "maxUnavailable": max_unavailable,
                            "progressDeadlineSeconds": progress_deadline_seconds,
                        }
                    })),
                )
                .await?;
            print_json_response(response)
        }
        OperatorCommand::Scale {
            job_id,
            replicas,
            if_match_generation,
            idempotency_key,
        } => {
            let job_id = JobId::new(job_id).map_err(|_| "job ID is invalid".to_owned())?;
            let headers = deployment_mutation_headers(if_match_generation, &idempotency_key)?;
            let response = client
                .request(
                    Method::POST,
                    &format!("v1/jobs/{}/deployment/scale", job_id.as_str()),
                    &[],
                    &headers,
                    Some(json!({ "replicas": replicas })),
                )
                .await?;
            print_json_response(response)
        }
        OperatorCommand::Rollback {
            job_id,
            revision,
            if_match_generation,
            idempotency_key,
        } => {
            let job_id = JobId::new(job_id).map_err(|_| "job ID is invalid".to_owned())?;
            let headers = deployment_mutation_headers(if_match_generation, &idempotency_key)?;
            let response = client
                .request(
                    Method::POST,
                    &format!("v1/jobs/{}/deployment/rollback", job_id.as_str()),
                    &[],
                    &headers,
                    Some(json!({ "revision": revision })),
                )
                .await?;
            print_json_response(response)
        }
        OperatorCommand::Drain { command } => match command {
            DrainCommand::Start {
                worker_id,
                grace_period_seconds,
            } => {
                let worker_id =
                    WorkerId::new(worker_id).map_err(|_| "worker ID is invalid".to_owned())?;
                let response = client
                    .request(
                        Method::POST,
                        &format!("v1/workers/{}/drain", worker_id.as_str()),
                        &[],
                        &[],
                        Some(json!({ "gracePeriodSeconds": grace_period_seconds })),
                    )
                    .await?;
                print_json_response(response)
            }
            DrainCommand::Status { worker_id } => {
                let worker_id =
                    WorkerId::new(worker_id).map_err(|_| "worker ID is invalid".to_owned())?;
                let response = client
                    .request(
                        Method::GET,
                        &format!("v1/workers/{}/drain", worker_id.as_str()),
                        &[],
                        &[],
                        None,
                    )
                    .await?;
                print_json_response(response)
            }
            DrainCommand::Clear { worker_id } => {
                let worker_id =
                    WorkerId::new(worker_id).map_err(|_| "worker ID is invalid".to_owned())?;
                let response = client
                    .request(
                        Method::DELETE,
                        &format!("v1/workers/{}/drain", worker_id.as_str()),
                        &[],
                        &[],
                        None,
                    )
                    .await?;
                if response.status.is_success() {
                    write_json(&json!({ "workerId": worker_id.as_str(), "state": "cleared" }))
                } else {
                    print_json_response(response)
                }
            }
        },
        OperatorCommand::Runs {
            job_id,
            state,
            limit,
            cursor,
        } => {
            if !(1..=100).contains(&limit) {
                return Err("run page limit must be between 1 and 100".to_owned());
            }
            let mut query = vec![("limit", limit.to_string())];
            if let Some(job_id) = job_id {
                let job_id = JobId::new(job_id).map_err(|_| "job ID is invalid".to_owned())?;
                query.push(("jobId", job_id.as_str().to_owned()));
            }
            if let Some(state) = state {
                query.push(("state", state.as_str().to_owned()));
            }
            if let Some(cursor) = cursor {
                query.push(("cursor", cursor));
            }
            let response = client
                .request(Method::GET, "v1/runs", &query, &[], None)
                .await?;
            print_json_response(response)
        }
        OperatorCommand::Logs {
            run_id,
            stream,
            offset,
            limit,
        } => {
            if !(1..=65_536).contains(&limit) {
                return Err("log limit must be between 1 and 65536 bytes".to_owned());
            }
            let run_id = RunId::new(run_id).map_err(|_| "run ID is invalid".to_owned())?;
            let query = [
                ("stream", stream.as_str().to_owned()),
                ("offset", offset.to_string()),
                ("limit", limit.to_string()),
            ];
            let response = client
                .request(
                    Method::GET,
                    &format!("v1/runs/{}/logs", run_id.as_str()),
                    &query,
                    &[],
                    None,
                )
                .await?;
            let value = parse_json_response(response)?;
            let data = value
                .get("data")
                .and_then(Value::as_str)
                .ok_or_else(|| "log response did not contain encoded data".to_owned())?;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(data)
                .map_err(|_| "log response contained invalid encoded data".to_owned())?;
            io::stdout()
                .lock()
                .write_all(&decoded)
                .map_err(|_| "log data could not be written to stdout".to_owned())?;
            let metadata = json!({
                "runId": run_id.as_str(),
                "stream": stream.as_str(),
                "offset": value.get("offset"),
                "nextOffset": value.get("nextOffset"),
                "streamClosed": value.get("streamClosed"),
                "eof": value.get("eof"),
                "truncated": value.get("truncated"),
            });
            write_json_to(&mut io::stderr().lock(), &metadata)
        }
    }
}

fn deployment_mutation_headers(
    generation: u64,
    idempotency_key: &str,
) -> Result<Vec<(&'static str, String)>, String> {
    Ok(vec![
        (IF_MATCH.as_str(), format!("\"{generation}\"")),
        (
            "Idempotency-Key",
            validate_idempotency_key(idempotency_key)?,
        ),
    ])
}

fn validate_idempotency_key(key: &str) -> Result<String, String> {
    if key.is_empty()
        || key.len() > 128
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._~-".contains(&byte))
    {
        return Err(
            "idempotency key must contain 1-128 ASCII letters, digits, or . _ ~ -".to_owned(),
        );
    }
    Ok(key.to_owned())
}

fn parse_server_url(raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw).map_err(|_| "server must be an absolute HTTP(S) URL".to_owned())?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(
            "server must be a base HTTP(S) URL without credentials, path, query, or fragment"
                .to_owned(),
        );
    }
    if url.scheme() == "http" {
        let host = url
            .host_str()
            .unwrap_or_default()
            .trim_start_matches('[')
            .trim_end_matches(']');
        let loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback());
        if !loopback {
            return Err(
                "plain HTTP is allowed only for a loopback server; use HTTPS remotely".to_owned(),
            );
        }
    }
    Ok(url)
}

fn read_operator_token(path: &std::path::Path) -> Result<Zeroizing<Vec<u8>>, String> {
    let file = crate::state_lock::open_secret_file_no_follow(path)
        .map_err(|_| "operator token file could not be accessed securely".to_owned())?;
    let metadata = file
        .metadata()
        .map_err(|_| "operator token file could not be accessed securely".to_owned())?;
    if !metadata.is_file() {
        return Err("operator token path must be a regular file".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(
                "operator token file permissions must deny group and other access".to_owned(),
            );
        }
    }
    let mut token = Zeroizing::new(Vec::with_capacity(44));
    file.take(44)
        .read_to_end(&mut token)
        .map_err(|_| "operator token file could not be accessed securely".to_owned())?;
    conductor_api::BearerTokenAuthenticator::new(&token)
        .map_err(|_| "operator token file contains an invalid token".to_owned())?;
    Ok(token)
}

fn print_json_response(response: ResponseData) -> Result<(), String> {
    let value = parse_json_response(response)?;
    write_json(&value)
}

fn parse_json_response(response: ResponseData) -> Result<Value, String> {
    if response.status.is_success() {
        if response.body.is_empty() {
            return Ok(Value::Null);
        }
        return serde_json::from_slice(&response.body)
            .map_err(|_| "operator API returned an invalid JSON response".to_owned());
    }
    let status = response.status;
    let error = serde_json::from_slice::<Value>(&response.body).ok();
    let detail = error
        .as_ref()
        .and_then(|value| value.get("error"))
        .and_then(|value| value.get("code"))
        .and_then(Value::as_str)
        .map(|code| format!(" ({code})"))
        .unwrap_or_default();
    let message = error
        .as_ref()
        .and_then(|value| value.get("error"))
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .map(|message| format!(": {message}"))
        .unwrap_or_default();
    Err(format!(
        "operator API returned HTTP {status}{detail}{message}"
    ))
}

fn write_json(value: &Value) -> Result<(), String> {
    write_json_to(&mut io::stdout().lock(), value)
}

fn write_json_to(writer: &mut impl Write, value: &Value) -> Result<(), String> {
    serde_json::to_writer_pretty(&mut *writer, value)
        .map_err(|_| "operator response could not be encoded".to_owned())?;
    writer
        .write_all(b"\n")
        .map_err(|_| "operator response could not be written".to_owned())
}

#[cfg(test)]
mod tests {
    use super::{parse_server_url, validate_idempotency_key};

    #[test]
    fn operator_server_url_requires_tls_off_loopback() {
        assert!(parse_server_url("http://127.0.0.1:8080").is_ok());
        assert!(parse_server_url("http://[::1]:8080").is_ok());
        assert!(parse_server_url("https://manager.example.test").is_ok());
        assert!(parse_server_url("http://manager.example.test").is_err());
        assert!(parse_server_url("https://user:password@manager.example.test").is_err());
    }

    #[test]
    fn idempotency_key_is_bounded_and_header_safe() {
        assert!(validate_idempotency_key("scale-job-10").is_ok());
        assert!(validate_idempotency_key("").is_err());
        assert!(validate_idempotency_key(&"x".repeat(129)).is_err());
        assert!(validate_idempotency_key("key with spaces").is_err());
        assert!(validate_idempotency_key("bad\rkey").is_err());
    }
}
