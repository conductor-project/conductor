//! Read-only inspection of manager and worker history retention and SQLite storage.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, Subcommand, ValueEnum};
use conductor_manager::{HistoryRetentionStatus, SqliteJobRepository};
use conductor_worker::{
    DEFAULT_LOG_MAX_BYTES_PER_STREAM, DEFAULT_LOG_RETENTION_RUNS, LogPolicy, WorkerService,
    WorkerStorageStatus,
};
use serde::Serialize;

#[derive(Debug, Args)]
pub struct StorageArgs {
    #[command(subcommand)]
    command: StorageCommand,
}

#[derive(Debug, Subcommand)]
enum StorageCommand {
    /// Inspect manager and/or worker history and SQLite storage without changing database contents.
    Inspect {
        /// Manager data directory containing manager.sqlite3.
        #[arg(long)]
        manager_data_dir: Option<PathBuf>,
        /// Worker data directory containing worker.sqlite3.
        #[arg(long)]
        worker_data_dir: Option<PathBuf>,
        /// Retention count to use for the dry-run cleanup estimate.
        #[arg(long, default_value_t = conductor_manager::DEFAULT_COMPLETED_RUN_RETENTION)]
        completed_run_retention: usize,
        /// Worker log retention count to use for status reporting.
        #[arg(long, default_value_t = DEFAULT_LOG_RETENTION_RUNS)]
        log_retention_runs: usize,
        /// Per-stream worker log size limit to use for the upper-bound estimate.
        #[arg(long, default_value_t = DEFAULT_LOG_MAX_BYTES_PER_STREAM)]
        log_max_bytes_per_stream: u64,
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
    manager: Option<ManagerStorageReport>,
    worker: Option<WorkerStorageReport>,
}

#[derive(Serialize)]
struct ManagerStorageReport {
    #[serde(flatten)]
    history: HistoryRetentionStatus,
    database_file_bytes: u64,
    wal_file_bytes: u64,
    shm_file_bytes: u64,
    total_file_bytes: u64,
}

#[derive(Serialize)]
struct WorkerStorageReport {
    #[serde(flatten)]
    history: WorkerStorageStatus,
    database_file_bytes: u64,
    wal_file_bytes: u64,
    shm_file_bytes: u64,
    total_file_bytes: u64,
}

pub fn run(args: StorageArgs) -> ExitCode {
    match args.command {
        StorageCommand::Inspect {
            manager_data_dir,
            worker_data_dir,
            completed_run_retention,
            log_retention_runs,
            log_max_bytes_per_stream,
            output,
        } => match inspect(
            manager_data_dir.as_deref(),
            worker_data_dir.as_deref(),
            completed_run_retention,
            log_retention_runs,
            log_max_bytes_per_stream,
        ) {
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
                    if let Some(manager) = &inspection.manager {
                        println!(
                            "Manager history: {} completed, {} eligible, {} eligible beyond the retention target.",
                            manager.history.completed_runs,
                            manager.history.eligible_runs,
                            manager.history.eligible_runs_to_prune,
                        );
                        println!(
                            "Protected records: {} idempotency-keyed runs, {} pending commands.",
                            manager.history.idempotency_protected_runs,
                            manager.history.pending_commands,
                        );
                        println!(
                            "Other retained state: {} active or unresolved runs, {} current service attempts, {} service port leases.",
                            manager.history.active_or_unresolved_runs,
                            manager.history.current_service_attempts,
                            manager.history.service_port_leases,
                        );
                        println!(
                            "Permanent records: {} audit events, {} execution idempotency records.",
                            manager.history.audit_events,
                            manager.history.execution_idempotency_records,
                        );
                        print_sqlite_and_files(
                            "Manager",
                            SqliteFileSummary {
                                page_count: manager.history.sqlite_page_count,
                                freelist_pages: manager.history.sqlite_freelist_pages,
                                page_size_bytes: manager.history.sqlite_page_size_bytes,
                                database_file_bytes: manager.database_file_bytes,
                                wal_file_bytes: manager.wal_file_bytes,
                                shm_file_bytes: manager.shm_file_bytes,
                                total_file_bytes: manager.total_file_bytes,
                            },
                        );
                    }
                    if let Some(worker) = &inspection.worker {
                        println!(
                            "Worker history: {} total runs, {} completed, {} active or unresolved; {} completed log pairs await pruning (target {}).",
                            worker.history.runs,
                            worker.history.completed_runs,
                            worker.history.active_or_unresolved_runs,
                            worker.history.completed_unpruned_log_runs,
                            worker.history.log_retention_runs,
                        );
                        println!(
                            "Worker delivery: {} commands, {} accepted, {} reports awaiting acknowledgement, {} acknowledged; {} report sequence rows, sequence high-water {}.",
                            worker.history.worker_commands,
                            worker.history.accepted_commands,
                            worker.history.reports_pending_acknowledgement,
                            worker.history.acknowledged_commands,
                            worker.history.report_sequence_rows,
                            worker.history.report_sequence_high_water,
                        );
                        println!(
                            "Worker log upper bound: {} bytes at {} bytes per stream; worker runs, commands, and report sequences have no expiry policy.",
                            worker.history.estimated_unpruned_log_bytes_upper_bound,
                            worker.history.log_max_bytes_per_stream,
                        );
                        print_sqlite_and_files(
                            "Worker",
                            SqliteFileSummary {
                                page_count: worker.history.sqlite_page_count,
                                freelist_pages: worker.history.sqlite_freelist_pages,
                                page_size_bytes: worker.history.sqlite_page_size_bytes,
                                database_file_bytes: worker.database_file_bytes,
                                wal_file_bytes: worker.wal_file_bytes,
                                shm_file_bytes: worker.shm_file_bytes,
                                total_file_bytes: worker.total_file_bytes,
                            },
                        );
                    }
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
    manager_data_dir: Option<&Path>,
    worker_data_dir: Option<&Path>,
    completed_run_retention: usize,
    log_retention_runs: usize,
    log_max_bytes_per_stream: u64,
) -> Result<StorageInspection, &'static str> {
    if manager_data_dir.is_none() && worker_data_dir.is_none() {
        return Err("provide --manager-data-dir, --worker-data-dir, or both");
    }
    let manager = manager_data_dir
        .map(|directory| {
            let database_path = directory.join("manager.sqlite3");
            let history =
                SqliteJobRepository::inspect_read_only(&database_path, completed_run_retention)
                    .map_err(
                        |_| "manager storage could not be inspected; check the path and schema",
                    )?;
            manager_report(&database_path, history)
        })
        .transpose()?;
    let worker = worker_data_dir
        .map(|directory| {
            let database_path = directory.join("worker.sqlite3");
            let history = WorkerService::inspect_storage_read_only(
                directory,
                LogPolicy {
                    max_bytes_per_stream: log_max_bytes_per_stream,
                    retention_runs: log_retention_runs,
                },
            )
            .map_err(|_| "worker storage could not be inspected; check the path and schema")?;
            worker_report(&database_path, history)
        })
        .transpose()?;
    Ok(StorageInspection { manager, worker })
}

fn manager_report(
    database_path: &Path,
    history: HistoryRetentionStatus,
) -> Result<ManagerStorageReport, &'static str> {
    let (database_file_bytes, wal_file_bytes, shm_file_bytes, total_file_bytes) =
        storage_file_sizes(database_path, "manager")?;
    Ok(ManagerStorageReport {
        history,
        database_file_bytes,
        wal_file_bytes,
        shm_file_bytes,
        total_file_bytes,
    })
}

fn worker_report(
    database_path: &Path,
    history: WorkerStorageStatus,
) -> Result<WorkerStorageReport, &'static str> {
    let (database_file_bytes, wal_file_bytes, shm_file_bytes, total_file_bytes) =
        storage_file_sizes(database_path, "worker")?;
    Ok(WorkerStorageReport {
        history,
        database_file_bytes,
        wal_file_bytes,
        shm_file_bytes,
        total_file_bytes,
    })
}

fn storage_file_sizes(
    database_path: &Path,
    component: &'static str,
) -> Result<(u64, u64, u64, u64), &'static str> {
    let database_file_bytes = file_len(database_path, component)?;
    let wal_file_bytes = file_len(&suffixed_path(database_path, "-wal"), component)?;
    let shm_file_bytes = file_len(&suffixed_path(database_path, "-shm"), component)?;
    let total_file_bytes = database_file_bytes
        .checked_add(wal_file_bytes)
        .and_then(|size| size.checked_add(shm_file_bytes))
        .ok_or("SQLite storage size is out of range")?;
    Ok((
        database_file_bytes,
        wal_file_bytes,
        shm_file_bytes,
        total_file_bytes,
    ))
}

fn file_len(path: &Path, component: &'static str) -> Result<u64, &'static str> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(match component {
                "manager" => "manager storage contains a non-regular SQLite file",
                _ => "worker storage contains a non-regular SQLite file",
            })
        }
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(_) => Err(match component {
            "manager" => "manager storage file size could not be read",
            _ => "worker storage file size could not be read",
        }),
    }
}

struct SqliteFileSummary {
    page_count: u64,
    freelist_pages: u64,
    page_size_bytes: u64,
    database_file_bytes: u64,
    wal_file_bytes: u64,
    shm_file_bytes: u64,
    total_file_bytes: u64,
}

fn print_sqlite_and_files(label: &str, summary: SqliteFileSummary) {
    println!(
        "{label} SQLite pages: {} total, {} on the freelist at {} bytes per page.",
        summary.page_count, summary.freelist_pages, summary.page_size_bytes,
    );
    println!(
        "{label} files: {} bytes database, {} bytes WAL, {} bytes shared memory ({} bytes total).",
        summary.database_file_bytes,
        summary.wal_file_bytes,
        summary.shm_file_bytes,
        summary.total_file_bytes,
    );
}

fn suffixed_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    value.into()
}
