//! Service endpoint lookup and long-poll watch commands.

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use conductor_api::client::{Client as ApiClient, ClientError};
use conductor_api::contracts::{ServiceEndpointCollection, ServiceWatchResponse};
use conductor_core::{JobId, ServiceIdentity, ServicePortProtocol};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Args)]
pub struct ServiceArgs {
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
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Json)]
    output: OutputFormat,
    #[command(subcommand)]
    command: ServiceCommand,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
enum OutputFormat {
    Human,
    #[default]
    Json,
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    /// Look up endpoints belonging to a job deployment.
    Endpoints { job_id: String },
    /// Look up all published endpoints for a supported namespace and service name.
    Lookup { namespace: String, name: String },
    /// Long-poll for a change to a service's published endpoints.
    Watch {
        namespace: String,
        name: String,
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=86400))]
        timeout_seconds: u64,
    },
}

pub fn run(args: ServiceArgs) -> ExitCode {
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
                "service command runtime could not be started".to_owned(),
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
    command: ServiceCommand,
    output: OutputFormat,
) -> Result<(), CommandError> {
    match command {
        ServiceCommand::Endpoints { job_id } => {
            let id = JobId::new(job_id)
                .map_err(|_| CommandError::new("invalid_job_id", "job ID is invalid"))?;
            let response: ServiceEndpointCollection = client
                .get(&format!("v1/jobs/{}/endpoints", id.as_str()))
                .await
                .map_err(api_error)?;
            emit_endpoints(output, &response.endpoints, "job")
        }
        ServiceCommand::Lookup { namespace, name } => {
            let identity = service_identity(namespace, name)?;
            let path = format!(
                "v1/services/{}/{}/endpoints",
                encode_path_segment(identity.namespace()),
                encode_path_segment(identity.name())
            );
            let response: ServiceEndpointCollection = client.get(&path).await.map_err(api_error)?;
            emit_endpoints(output, &response.endpoints, "service")
        }
        ServiceCommand::Watch {
            namespace,
            name,
            timeout_seconds,
        } => {
            let identity = service_identity(namespace, name)?;
            let response = watch_endpoints(client, &identity, timeout_seconds).await?;
            emit(
                output,
                &json!({
                    "namespace": identity.namespace(),
                    "serviceName": identity.name(),
                    "changed": response.changed,
                    "endpoints": response.endpoints,
                    "cursorSupported": false,
                    "versionSupported": false,
                }),
                &format!(
                    "service {}/{} endpoints changed: {}",
                    identity.namespace(),
                    identity.name(),
                    response.changed
                ),
            )
        }
    }
}

fn service_identity(namespace: String, name: String) -> Result<ServiceIdentity, CommandError> {
    ServiceIdentity::in_namespace(namespace, name).map_err(|_| {
        CommandError::new(
            "invalid_service_identity",
            "service identity is invalid or its namespace is not supported",
        )
    })
}

fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

async fn watch_endpoints(
    client: &ApiClient,
    identity: &ServiceIdentity,
    timeout_seconds: u64,
) -> Result<ServiceWatchResponse, CommandError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_seconds);
    let mut latest = Vec::new();
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let path = format!(
        "v1/services/{}/{}/watch",
        encode_path_segment(identity.namespace()),
        encode_path_segment(identity.name())
    );
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(CommandError::new(
                "watch_timeout",
                format!(
                    "timed out watching service {}/{} endpoints",
                    identity.namespace(),
                    identity.name()
                ),
            )
            .partial(watch_partial(identity, &latest)));
        }
        let wait_seconds = remaining.as_secs().clamp(1, 30);
        let request_path = format!("{path}?waitSeconds={wait_seconds}");
        let result = tokio::select! {
            signal = &mut ctrl_c => {
                if signal.is_err() {
                    return Err(CommandError::new("interrupt_handler_error", "could not listen for Ctrl-C while watching service endpoints")
                        .partial(watch_partial(identity, &latest)));
                }
                return Err(CommandError::new("watch_interrupted", format!("stopped watching service {}/{} after Ctrl-C", identity.namespace(), identity.name()))
                    .partial(watch_partial(identity, &latest)));
            }
            response = tokio::time::timeout_at(deadline, client.get::<ServiceWatchResponse>(&request_path)) => response,
        };
        match result {
            Ok(Ok(response)) => {
                latest.clone_from(&response.endpoints);
                if response.changed {
                    return Ok(response);
                }
            }
            Ok(Err(ClientError::Request(_))) => {
                // GET is safe to repeat. Reconnect within the caller's deadline.
            }
            Ok(Err(ClientError::Api { status, .. }))
                if matches!(status.as_u16(), 429 | 502 | 503 | 504) =>
            {
                // The API may temporarily have no watch capacity or be restarting.
            }
            Ok(Err(error)) => return Err(api_error(error)),
            Err(_) => {
                return Err(CommandError::new(
                    "watch_timeout",
                    format!(
                        "timed out watching service {}/{} endpoints",
                        identity.namespace(),
                        identity.name()
                    ),
                )
                .partial(watch_partial(identity, &latest)));
            }
        }
        tokio::select! {
            signal = &mut ctrl_c => {
                if signal.is_err() {
                    return Err(CommandError::new("interrupt_handler_error", "could not listen for Ctrl-C while watching service endpoints")
                        .partial(watch_partial(identity, &latest)));
                }
                return Err(CommandError::new("watch_interrupted", format!("stopped watching service {}/{} after Ctrl-C", identity.namespace(), identity.name()))
                    .partial(watch_partial(identity, &latest)));
            }
            () = tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + Duration::from_millis(250))) => {}
        }
    }
}

fn watch_partial(
    identity: &ServiceIdentity,
    endpoints: &[conductor_api::contracts::ServiceEndpoint],
) -> Value {
    json!({
        "namespace": identity.namespace(),
        "serviceName": identity.name(),
        "changed": false,
        "endpoints": endpoints,
        "cursorSupported": false,
        "versionSupported": false,
    })
}

fn emit_endpoints(
    output: OutputFormat,
    endpoints: &[conductor_api::contracts::ServiceEndpoint],
    selector: &str,
) -> Result<(), CommandError> {
    emit(
        output,
        &json!({"selector":selector,"count":endpoints.len(),"endpoints":endpoints}),
        &if endpoints.is_empty() {
            format!("no service endpoints found for {selector}")
        } else {
            endpoints
                .iter()
                .map(|endpoint| {
                    format!(
                        "{}/{} {} {}://{}:{} ready={} worker={}",
                        endpoint.namespace,
                        endpoint.service_name,
                        endpoint.name,
                        match endpoint.protocol {
                            ServicePortProtocol::Tcp => "tcp",
                        },
                        endpoint.advertised_address,
                        endpoint.port,
                        endpoint.ready,
                        endpoint.worker_id.as_str()
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        },
    )
}

fn emit<T: Serialize>(output: OutputFormat, value: &T, human: &str) -> Result<(), CommandError> {
    let mut stdout = io::stdout().lock();
    match output {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut stdout, value).map_err(|_| {
                CommandError::new("output_error", "service response could not be written")
            })?;
            stdout.write_all(b"\n").map_err(|_| {
                CommandError::new("output_error", "service response could not be written")
            })
        }
        OutputFormat::Human => stdout
            .write_all(human.as_bytes())
            .and_then(|()| stdout.write_all(b"\n"))
            .map_err(|_| {
                CommandError::new("output_error", "service response could not be written")
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
            let _ = writeln!(stderr, "conductor service: {message}");
            if let Some(partial) = partial {
                let _ = writeln!(stderr, "partial result: {partial}");
            }
        }
    }
    ExitCode::FAILURE
}
