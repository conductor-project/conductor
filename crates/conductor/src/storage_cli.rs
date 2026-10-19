//! Read-only inspection of manager history retention and SQLite storage.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, Subcommand, ValueEnum};
use conductor_manager::{HistoryRetentionStatus, SqliteJobRepository};
use serde::Serialize;

#[derive(Debug, Args)]
pub struct StorageArgs {
    #[command(subcommand)]
    command: StorageCommand,
}

#[derive(Debug, Subcommand)]
enum StorageCommand {
    /// Inspect manager history retention and SQLite page usage without changing database contents.
    Inspect {
        /// Manager data directory containing manager.sqlite3.
        #[arg(long)]
        manager_data_dir: PathBuf,
        /// Retention count to use for the dry-run cleanup estimate.
        #[arg(long, default_value_t = conductor_manager::DEFAULT_COMPLETED_RUN_RETENTION)]
        completed_run_retention: usize,
        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
        output: OutputFormat,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StorageInspection {
    manager: HistoryRetentionStatus,
    manager_database_file_bytes: u64,
    manager_wal_file_bytes: u64,
    manager_shm_file_bytes: u64,
    manager_total_file_bytes: u64,
}

pub fn run(args: StorageArgs) -> ExitCode {
    match args.command {
        StorageCommand::Inspect {
            manager_data_dir,
            completed_run_retention,
            output,
        } => match inspect(&manager_data_dir, completed_run_retention) {
            Ok(inspection) => match output {
                OutputFormat::Json => match serde_json::to_writer_pretty(io::stdout(), &inspection)
                {
                    Ok(()) => {
                        println!();
                        ExitCode::SUCCESS
                    }
                    Err(_) => {
                        eprintln!("storage inspection output could not be written");
                        ExitCode::FAILURE
                    }
                },
                OutputFormat::Human => {
                    println!(
                        "Manager history: {} completed, {} eligible, {} eligible beyond the retention target.",
                        inspection.manager.completed_runs,
                        inspection.manager.eligible_runs,
                        inspection.manager.eligible_runs_to_prune,
                    );
                    println!(
                        "Protected records: {} idempotency-keyed runs, {} pending commands.",
                        inspection.manager.idempotency_protected_runs,
                        inspection.manager.pending_commands,
                    );
                    println!(
                        "Other retained state: {} active or unresolved runs, {} current service attempts, {} service port leases.",
                        inspection.manager.active_or_unresolved_runs,
                        inspection.manager.current_service_attempts,
                        inspection.manager.service_port_leases,
                    );
                    println!(
                        "Permanent records: {} audit events, {} execution idempotency records.",
                        inspection.manager.audit_events,
                        inspection.manager.execution_idempotency_records,
                    );
                    println!(
                        "SQLite pages: {} total, {} on the freelist at {} bytes per page.",
                        inspection.manager.sqlite_page_count,
                        inspection.manager.sqlite_freelist_pages,
                        inspection.manager.sqlite_page_size_bytes,
                    );
                    println!(
                        "Files: {} bytes database, {} bytes WAL, {} bytes shared memory ({} bytes total).",
                        inspection.manager_database_file_bytes,
                        inspection.manager_wal_file_bytes,
                        inspection.manager_shm_file_bytes,
                        inspection.manager_total_file_bytes,
                    );
                    ExitCode::SUCCESS
                }
            },
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        },
    }
}

fn inspect(
    manager_data_dir: &Path,
    completed_run_retention: usize,
) -> Result<StorageInspection, &'static str> {
    let database_path = manager_data_dir.join("manager.sqlite3");
    let status = SqliteJobRepository::inspect_read_only(&database_path, completed_run_retention)
        .map_err(|_| "manager storage could not be inspected; check the path and schema")?;
    let database_file_bytes = file_len(&database_path)?;
    let wal_file_bytes = file_len(&suffixed_path(&database_path, "-wal"))?;
    let shm_file_bytes = file_len(&suffixed_path(&database_path, "-shm"))?;
    let manager_total_file_bytes = database_file_bytes
        .checked_add(wal_file_bytes)
        .and_then(|size| size.checked_add(shm_file_bytes))
        .ok_or("manager storage size is out of range")?;
    Ok(StorageInspection {
        manager: status,
        manager_database_file_bytes: database_file_bytes,
        manager_wal_file_bytes: wal_file_bytes,
        manager_shm_file_bytes: shm_file_bytes,
        manager_total_file_bytes,
    })
}

fn file_len(path: &Path) -> Result<u64, &'static str> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err("manager storage contains a non-regular SQLite file")
        }
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(_) => Err("manager storage file size could not be read"),
    }
}

fn suffixed_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    value.into()
}
