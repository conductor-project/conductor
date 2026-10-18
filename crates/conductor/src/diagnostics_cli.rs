//! Manager health, readiness, and metrics commands.

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Subcommand, ValueEnum};
use conductor_api::client::{Client as ApiClient, ClientError};
use conductor_api::contracts::ReadinessResponse;
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
