//! Non-mutating checks of local database upgrade compatibility.

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;

#[derive(Debug, Args)]
pub struct UpgradeArgs {
    #[command(subcommand)]
    command: UpgradeCommand,
}

#[derive(Debug, Subcommand)]
enum UpgradeCommand {
    /// Inspect local manager and/or worker databases without starting the daemon or migrating them.
    Preflight {
        /// Manager data directory containing manager.sqlite3.
        #[arg(long)]
        manager_data_dir: Option<PathBuf>,
        /// Worker data directory containing worker.sqlite3.
        #[arg(long)]
        worker_data_dir: Option<PathBuf>,
        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
        output: OutputFormat,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lower")]
enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DatabaseStatus {
    component: &'static str,
    schema_version: Option<i64>,
    current_schema_version: i64,
    state: &'static str,
    marker_supported: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PreflightReport {
    binary_version: &'static str,
    manager_worker_protocol_version: u16,
    api_version: u8,
    schema_markers_supported: bool,
    databases: Vec<DatabaseStatus>,
    required_maintenance_actions: Vec<&'static str>,
}

pub fn run(args: UpgradeArgs) -> ExitCode {
    let UpgradeCommand::Preflight {
        manager_data_dir,
        worker_data_dir,
        output,
    } = args.command;
    if manager_data_dir.is_none() && worker_data_dir.is_none() {
        return report_error(
            output,
            "provide --manager-data-dir, --worker-data-dir, or both",
        );
    }

    let mut databases = Vec::new();
    if let Some(directory) = manager_data_dir {
        let path = directory.join("manager.sqlite3");
        match conductor_manager::inspect_schema_version(&path) {
            Ok(version) => databases.push(status(
                "manager",
                version,
                conductor_manager::CURRENT_SCHEMA_VERSION,
            )),
            Err(_) => return report_error(output, "manager database could not be inspected"),
        }
    }
    if let Some(directory) = worker_data_dir {
        match conductor_worker::inspect_schema_version(&directory) {
            Ok(version) => databases.push(status(
                "worker",
                version,
                conductor_worker::CURRENT_SCHEMA_VERSION,
            )),
            Err(_) => return report_error(output, "worker database could not be inspected"),
        }
    }

    let schema_markers_supported = databases.iter().all(|database| database.marker_supported);
    let report = PreflightReport {
        binary_version: env!("CARGO_PKG_VERSION"),
        manager_worker_protocol_version: conductor_core::CURRENT_PROTOCOL_VERSION,
        api_version: 1,
        schema_markers_supported,
        required_maintenance_actions: maintenance_actions(&databases),
        databases,
    };
    if emit(output, &report).is_err() {
        return ExitCode::FAILURE;
    }
    if schema_markers_supported {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn maintenance_actions(databases: &[DatabaseStatus]) -> Vec<&'static str> {
    if databases.iter().any(|database| !database.marker_supported) {
        return vec![
            "Do not start the candidate against these databases; use a compatible Conductor binary or restore a matching verified backup.",
        ];
    }
    if databases
        .iter()
        .any(|database| database.state == "migration_required")
    {
        return vec![
            "Drain workers, stop workers before the manager, and create a verified backup of both roles and their identity/configuration before migration.",
            "Install matching manager and worker binaries; start the manager first, then workers, and inspect run and deployment state before resuming work.",
        ];
    }
    Vec::new()
}

fn status(component: &'static str, schema_version: Option<i64>, current: i64) -> DatabaseStatus {
    let (state, marker_supported) = match schema_version {
        None => ("not_initialized", true),
        Some(version) if version < 0 || version > current => ("unsupported", false),
        Some(version) if version == current => ("current", true),
        Some(_) => ("migration_required", true),
    };
    DatabaseStatus {
        component,
        schema_version,
        current_schema_version: current,
        state,
        marker_supported,
    }
}

fn emit(output: OutputFormat, report: &PreflightReport) -> io::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    match output {
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut stdout, report)?;
            stdout.write_all(b"\n")
        }
        OutputFormat::Human => {
            writeln!(
                stdout,
                "Upgrade preflight: {}",
                if report.schema_markers_supported {
                    "schema markers supported"
                } else {
                    "unsupported schema marker"
                }
            )?;
            writeln!(stdout, "Binary version: {}", report.binary_version)?;
            writeln!(
                stdout,
                "Manager-worker protocol: {}",
                report.manager_worker_protocol_version
            )?;
            writeln!(stdout, "HTTP API: v{}", report.api_version)?;
            for database in &report.databases {
                let version = database
                    .schema_version
                    .map_or_else(|| "uninitialized".to_owned(), |version| version.to_string());
                writeln!(
                    stdout,
                    "{} database: {} (current schema {}; {})",
                    database.component, version, database.current_schema_version, database.state
                )?;
            }
            for action in &report.required_maintenance_actions {
                writeln!(stdout, "Required maintenance: {action}")?;
            }
            Ok(())
        }
    }
}

fn report_error(output: OutputFormat, message: &str) -> ExitCode {
    let stderr = io::stderr();
    let mut stderr = stderr.lock();
    match output {
        OutputFormat::Json => {
            let _ = serde_json::to_writer(
                &mut stderr,
                &serde_json::json!({"error":{"code":"upgrade_preflight_failed","message":message}}),
            );
            let _ = stderr.write_all(b"\n");
        }
        OutputFormat::Human => {
            let _ = writeln!(stderr, "conductor upgrade: {message}");
        }
    }
    ExitCode::FAILURE
}
