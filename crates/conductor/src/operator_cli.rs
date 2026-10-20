use std::io::{self, Read, Write};
use std::net::IpAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use base64::Engine;
use clap::{Args, Subcommand, ValueEnum};
use conductor_api::client::{Client as ApiClient, ClientConfig};
use conductor_core::{JobId, NetworkDomainId, RunId, WorkerId};
use reqwest::{Method, StatusCode, Url};
use serde_json::{Value, json};
use zeroize::Zeroizing;

#[derive(Debug, Args)]
pub struct OperatorArgs {
    /// Manager API URL. Plain HTTP is allowed only for loopback addresses.
    #[arg(long)]
    server: Option<String>,
    /// Named connection context (otherwise the selected default context is used).
    #[arg(long)]
    context: Option<String>,
    /// Protected file containing the operator bearer token.
    #[arg(long)]
    token_file: Option<PathBuf>,
    /// PEM CA certificate used to verify the server TLS certificate.
    #[arg(long)]
    ca_file: Option<PathBuf>,
    /// Per-request timeout in seconds.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..=300))]
    timeout_seconds: Option<u64>,
    /// Output format for API responses.
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    output: OutputFormat,
    #[command(subcommand)]
    command: OperatorCommand,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
#[value(rename_all = "lower")]
enum OutputFormat {
    #[default]
    Human,
    Json,
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
    /// Configure or inspect worker overlay network domains and peers.
    Overlay {
        #[command(subcommand)]
        command: OverlayCommand,
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
enum OverlayCommand {
    /// Create a network domain with manager-owned worker and service pools.
    Create {
        network_domain_id: String,
        #[arg(long)]
        worker_address_pool: String,
        #[arg(long)]
        service_address_pool: String,
    },
    /// Read one network domain's overlay settings and address pools.
    Inspect { network_domain_id: String },
    /// List desired peer assignments and their lease state.
    Peers { network_domain_id: String },
    /// Revoke a worker peer from the network domain.
    Revoke {
        network_domain_id: String,
        worker_id: String,
    },
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
    client: ApiClient,
    output: OutputFormat,
}

struct ResponseData {
    status: StatusCode,
    etag: Option<String>,
    body: Vec<u8>,
}

impl ResponseData {
    fn success(body: Value) -> Result<Self, String> {
        Ok(Self {
            status: StatusCode::OK,
            etag: None,
            body: serde_json::to_vec(&body)
                .map_err(|_| "operator API response could not be encoded".to_owned())?,
        })
    }
}

struct EffectiveConnection {
    server: String,
    token_file: Option<PathBuf>,
    ca_file: Option<PathBuf>,
    timeout_seconds: u64,
}

fn effective_connection(
    server: Option<&str>,
    token_file: Option<&PathBuf>,
    ca_file: Option<&PathBuf>,
    timeout_seconds: Option<u64>,
    context: Option<crate::contexts::ConnectionContext>,
) -> EffectiveConnection {
    EffectiveConnection {
        server: server
            .map(str::to_owned)
            .or_else(|| context.as_ref().map(|context| context.server_url.clone()))
            .unwrap_or_else(|| "http://127.0.0.1:8080".to_owned()),
        token_file: token_file.cloned().or_else(|| {
            context
                .as_ref()
                .and_then(|context| context.token_file.clone())
        }),
        ca_file: ca_file
            .cloned()
            .or_else(|| context.as_ref().and_then(|context| context.ca_file.clone())),
        timeout_seconds: timeout_seconds
            .or_else(|| context.map(|context| context.timeout_seconds))
            .unwrap_or(10),
    }
}

impl OperatorClient {
    fn new(args: &OperatorArgs) -> Result<Self, String> {
        let client = build_client(
            args.server.as_deref(),
            args.context.as_deref(),
            args.token_file.as_deref(),
            args.ca_file.as_deref(),
            args.timeout_seconds,
        )?;
        Ok(Self {
            client,
            output: args.output,
        })
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        headers: &[(&str, String)],
        body: Option<Value>,
    ) -> Result<ResponseData, String> {
        let path = request_path(path, query)?;
        let idempotency_key = header_value(headers, "Idempotency-Key")?;
        let if_match = header_value(headers, "If-Match")?;
        let if_none_match = header_value(headers, "If-None-Match")?;
        let error = |error: conductor_api::client::ClientError| {
            format!("operator API request failed: {error}")
        };
        let response = match method {
            Method::GET => {
                let response = self
                    .client
                    .get_response::<Value>(&path)
                    .await
                    .map_err(error)?;
                ResponseData {
                    status: response.status,
                    etag: response.etag,
                    body: serde_json::to_vec(&response.body)
                        .map_err(|_| "operator API response could not be encoded".to_owned())?,
                }
            }
            Method::POST => {
                let body = body.unwrap_or(Value::Null);
                let value = self
                    .client
                    .post_conditional(&path, &body, idempotency_key, if_match)
                    .await
                    .map_err(error)?;
                ResponseData::success(value)?
            }
            Method::PUT => {
                let body = body.unwrap_or(Value::Null);
                let value = if let Some(etag) = if_none_match {
                    if etag != "*" {
                        return Err("operator request precondition is invalid".to_owned());
                    }
                    self.client.put_create(&path, &body, idempotency_key).await
                } else {
                    self.client
                        .put(&path, &body, idempotency_key, if_match)
                        .await
                }
                .map_err(error)?;
                ResponseData::success(value)?
            }
            Method::DELETE => {
                self.client
                    .delete_empty(&path, idempotency_key, if_match)
                    .await
                    .map_err(error)?;
                ResponseData {
                    status: StatusCode::NO_CONTENT,
                    etag: None,
                    body: Vec::new(),
                }
            }
            _ => return Err("operator request method is unsupported".to_owned()),
        };
        Ok(response)
    }

    fn print_response(&self, response: ResponseData) -> Result<(), String> {
        let value = parse_json_response(response)?;
        self.write_value(&value)
    }

    fn write_value(&self, value: &Value) -> Result<(), String> {
        match self.output {
            OutputFormat::Json => write_json(value),
            OutputFormat::Human => {
                let rendered = format_human(value);
                let mut stdout = io::stdout().lock();
                stdout
                    .write_all(rendered.as_bytes())
                    .and_then(|()| stdout.write_all(b"\n"))
                    .map_err(|_| "operator response could not be written to stdout".to_owned())
            }
        }
    }
}

/// Construct the shared management API client using the same context and
/// credential precedence as the legacy `operator` command.
pub(crate) fn build_client(
    server: Option<&str>,
    context_name: Option<&str>,
    token_file: Option<&std::path::Path>,
    ca_file: Option<&std::path::Path>,
    timeout_seconds: Option<u64>,
) -> Result<ApiClient, String> {
    let context = crate::contexts::resolve(context_name)?.map(|(_, context)| context);
    let token_file = token_file.map(PathBuf::from);
    let ca_file = ca_file.map(PathBuf::from);
    let effective = effective_connection(
        server,
        token_file.as_ref(),
        ca_file.as_ref(),
        timeout_seconds,
        context,
    );
    let base_url = parse_server_url(&effective.server)?;
    let token_file = effective.token_file.as_ref();
    let is_loopback = base_url.host_str().is_some_and(is_loopback_host);
    if !is_loopback && token_file.is_none() {
        return Err(
            "authenticated remote use requires --token-file or a context token file".to_owned(),
        );
    }
    let bearer_token = if let Some(token_file) = token_file {
        let token = read_operator_token(token_file)?;
        Some(
            std::str::from_utf8(&token)
                .map_err(|_| "operator token file contains an invalid token".to_owned())?
                .to_owned(),
        )
    } else {
        None
    };
    let ca_certificate_pem = effective
        .ca_file
        .as_ref()
        .map(|path| {
            std::fs::read_to_string(path)
                .map_err(|_| "configured CA certificate file could not be read".to_owned())
        })
        .transpose()?;
    let mut config = ClientConfig::new(effective.server);
    config.bearer_token = bearer_token;
    config.ca_certificate_pem = ca_certificate_pem;
    config.timeout = Duration::from_secs(effective.timeout_seconds);
    ApiClient::new(config).map_err(|_| "operator HTTP client could not be configured".to_owned())
}

fn header_value<'a>(headers: &'a [(&str, String)], name: &str) -> Result<Option<&'a str>, String> {
    Ok(headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str()))
}

fn request_path(path: &str, query: &[(&str, String)]) -> Result<String, String> {
    if path.starts_with('/')
        || path.contains('#')
        || path.split('/').any(|segment| matches!(segment, "." | ".."))
    {
        return Err("operator API path is invalid".to_owned());
    }
    let base = Url::parse("http://conductor.invalid/").expect("constant URL is valid");
    let mut url = base
        .join(path)
        .map_err(|_| "operator API path is invalid".to_owned())?;
    if !query.is_empty() {
        url.query_pairs_mut()
            .extend_pairs(query.iter().map(|(key, value)| (*key, value.as_str())));
    }
    url.as_str()
        .strip_prefix("http://conductor.invalid")
        .map(|path_and_query| path_and_query.trim_start_matches('/').to_owned())
        .ok_or_else(|| "operator API path is invalid".to_owned())
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
                client.print_response(response)
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
                client.print_response(response)
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
                let etag = deployment.etag.clone();
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
                client.write_value(&json!({
                    "jobId": job_id.as_str(),
                    "etag": etag,
                    "deployment": deployment,
                    "serviceAttempts": attempts.get("serviceAttempts").cloned().unwrap_or(Value::Null),
                }))
            }
        },
        OperatorCommand::Overlay { command } => match command {
            OverlayCommand::Create {
                network_domain_id,
                worker_address_pool,
                service_address_pool,
            } => {
                let network_domain_id = NetworkDomainId::new(network_domain_id)
                    .map_err(|_| "network domain ID is invalid".to_owned())?;
                let response = client
                    .request(
                        Method::POST,
                        "v1/network-domains",
                        &[],
                        &[],
                        Some(json!({
                            "networkDomainId": network_domain_id,
                            "workerAddressPool": worker_address_pool,
                            "serviceAddressPool": service_address_pool,
                        })),
                    )
                    .await?;
                client.print_response(response)
            }
            OverlayCommand::Inspect { network_domain_id } => {
                let network_domain_id = NetworkDomainId::new(network_domain_id)
                    .map_err(|_| "network domain ID is invalid".to_owned())?;
                let response = client
                    .request(
                        Method::GET,
                        &format!("v1/network-domains/{}", network_domain_id.as_str()),
                        &[],
                        &[],
                        None,
                    )
                    .await?;
                client.print_response(response)
            }
            OverlayCommand::Peers { network_domain_id } => {
                let network_domain_id = NetworkDomainId::new(network_domain_id)
                    .map_err(|_| "network domain ID is invalid".to_owned())?;
                let response = client
                    .request(
                        Method::GET,
                        &format!("v1/network-domains/{}/peers", network_domain_id.as_str()),
                        &[],
                        &[],
                        None,
                    )
                    .await?;
                client.print_response(response)
            }
            OverlayCommand::Revoke {
                network_domain_id,
                worker_id,
            } => {
                let network_domain_id = NetworkDomainId::new(network_domain_id)
                    .map_err(|_| "network domain ID is invalid".to_owned())?;
                let worker_id =
                    WorkerId::new(worker_id).map_err(|_| "worker ID is invalid".to_owned())?;
                let response = client
                    .request(
                        Method::DELETE,
                        &format!(
                            "v1/network-domains/{}/peers/{}",
                            network_domain_id.as_str(),
                            worker_id.as_str()
                        ),
                        &[],
                        &[],
                        None,
                    )
                    .await?;
                if response.status.is_success() {
                    client.write_value(&json!({
                        "networkDomainId": network_domain_id,
                        "workerId": worker_id,
                        "state": "revoked",
                        "reachability": "not_verified",
                    }))
                } else {
                    client.print_response(response)
                }
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
                headers.push(("If-None-Match", "*".to_owned()));
            } else {
                let generation = if_match_generation.ok_or_else(|| {
                    "--if-match-generation is required unless --create is set".to_owned()
                })?;
                headers.push(("If-Match", format!("\"{generation}\"")));
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
            client.print_response(response)
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
            client.print_response(response)
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
            client.print_response(response)
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
                client.print_response(response)
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
                client.print_response(response)
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
                    client
                        .write_value(&json!({ "workerId": worker_id.as_str(), "state": "cleared" }))
                } else {
                    client.print_response(response)
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
            client.print_response(response)
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
        ("If-Match", format!("\"{generation}\"")),
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
    if url.scheme() == "http" && !url.host_str().is_some_and(is_loopback_host) {
        return Err(
            "plain HTTP is allowed only for a loopback server; use HTTPS remotely".to_owned(),
        );
    }
    Ok(url)
}

fn is_loopback_host(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
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

fn parse_json_response(response: ResponseData) -> Result<Value, String> {
    if response.status.is_success() {
        if response.body.is_empty() {
            return Ok(Value::Null);
        }
        return serde_json::from_slice(&response.body)
            .map_err(|_| "operator API returned an invalid JSON response".to_owned());
    }
    let status = response.status;
    Err(format!("operator API returned HTTP {status}"))
}

fn format_human(value: &Value) -> String {
    match value {
        Value::Null => "ok".to_owned(),
        Value::Array(items) => items
            .iter()
            .map(|item| format!("- {}", format_human_item(item)))
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(object) => object
            .iter()
            .map(|(key, value)| format!("{key}: {}", format_human_scalar(value)))
            .collect::<Vec<_>>()
            .join("\n"),
        scalar => format_human_scalar(scalar),
    }
}

fn format_human_item(value: &Value) -> String {
    match value {
        Value::Object(object) => object
            .iter()
            .map(|(key, value)| format!("{key}={}", format_human_scalar(value)))
            .collect::<Vec<_>>()
            .join(" "),
        other => format_human_scalar(other),
    }
}

fn format_human_scalar(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null | Value::Bool(_) | Value::Number(_) => value.to_string(),
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "<unavailable>".to_owned())
        }
    }
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
    use super::{
        OutputFormat, effective_connection, format_human, parse_server_url,
        validate_idempotency_key,
    };
    use clap::ValueEnum;

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

    #[test]
    fn cli_connection_values_override_context_values() {
        let context = crate::contexts::ConnectionContext {
            server_url: "https://context.example.test".to_owned(),
            token_file: Some("/context/token".into()),
            ca_file: Some("/context/ca.pem".into()),
            timeout_seconds: 20,
        };
        let explicit_token = std::path::PathBuf::from("/explicit/token");
        let effective = effective_connection(
            Some("https://override.example.test"),
            Some(&explicit_token),
            None,
            Some(7),
            Some(context),
        );
        assert_eq!(effective.server, "https://override.example.test");
        assert_eq!(effective.token_file, Some(explicit_token));
        assert_eq!(effective.ca_file, Some("/context/ca.pem".into()));
        assert_eq!(effective.timeout_seconds, 7);
    }

    #[test]
    fn no_context_defaults_to_unauthenticated_loopback() {
        let effective = effective_connection(None, None, None, None, None);
        assert_eq!(effective.server, "http://127.0.0.1:8080");
        assert!(effective.token_file.is_none());
        assert_eq!(effective.timeout_seconds, 10);
    }

    #[test]
    fn human_output_is_concise_and_json_output_remains_explicit() {
        let value = serde_json::json!({"runId":"run-1","state":"succeeded"});
        assert_eq!(format_human(&value), "runId: run-1\nstate: succeeded");
        assert_eq!(
            OutputFormat::Json.to_possible_value().unwrap().get_name(),
            "json"
        );
    }
}
