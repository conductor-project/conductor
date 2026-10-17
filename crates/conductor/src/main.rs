mod state_lock;

use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use clap::{Parser, Subcommand, ValueEnum, error::ErrorKind};
use conductor_core::{InspectRun, MAX_WORKER_CAPACITY, ReadRunLogs, StartRun, StopRun, WorkerId};
use conductor_manager::{WorkerPort, WorkerPortError};
use serde::Deserialize;
use state_lock::StateLocks;

#[derive(Debug, Parser)]
#[command(name = "conductor", version, about = "Conductor job and run agent")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run the Conductor agent.
    Agent {
        /// Path to the strict JSON configuration file.
        #[arg(long)]
        config: PathBuf,
        /// Override the role in the configuration file.
        #[arg(long, value_enum)]
        role: Option<Role>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    role: Role,
    listen: SocketAddr,
    manager_data_dir: PathBuf,
    worker_data_dir: PathBuf,
    worker_id: String,
    worker_concurrency: usize,
    #[serde(default = "default_log_max_bytes_per_stream")]
    log_max_bytes_per_stream: u64,
    #[serde(default = "default_log_retention_runs")]
    log_retention_runs: usize,
}

fn default_log_max_bytes_per_stream() -> u64 {
    conductor_worker::DEFAULT_LOG_MAX_BYTES_PER_STREAM
}

fn default_log_retention_runs() -> usize {
    conductor_worker::DEFAULT_LOG_RETENTION_RUNS
}

#[derive(Clone, Copy, Debug, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum Role {
    Manager,
    Worker,
    All,
}

impl Role {
    fn name(self) -> &'static str {
        match self {
            Self::Manager => "manager",
            Self::Worker => "worker",
            Self::All => "all",
        }
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "{{\"event\":\"startup_failed\",\"error\":{}}}",
                serde_json::to_string(&error.to_string())
                    .unwrap_or_else(|_| "\"unknown error\"".into())
            );
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            print!("{error}");
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let Commands::Agent {
        config: config_path,
        role: role_override,
    } = cli.command;
    let config_text = fs::read_to_string(config_path)?;
    let mut config: Config = serde_json::from_str(&config_text)?;
    if let Some(role) = role_override {
        config.role = role;
    }
    validate(&config)?;
    if !matches!(config.role, Role::All) {
        return Err(format!(
            "standalone role '{}' is unsupported until remote transport is implemented",
            config.role.name()
        )
        .into());
    }
    let _state_locks = StateLocks::acquire(&config.manager_data_dir, &config.worker_data_dir)?;
    let manager = Arc::new(conductor_manager::JobService::open_sqlite(
        config.manager_data_dir.join("manager.sqlite3"),
    )?);
    let worker_id = WorkerId::new(config.worker_id.clone())?;
    let worker = Arc::new(conductor_worker::WorkerService::open_with_log_policy(
        &config.worker_data_dir,
        worker_id.clone(),
        config.worker_concurrency,
        conductor_worker::LogPolicy {
            max_bytes_per_stream: config.log_max_bytes_per_stream,
            retention_runs: config.log_retention_runs,
        },
        Arc::new(conductor_process::HostProcessBackend),
    )?);
    let worker_port: Arc<dyn WorkerPort> = Arc::new(LocalWorkerPort(Arc::clone(&worker)));
    let listener = TcpListener::bind(config.listen)?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = Arc::clone(&shutdown);
    let signal_worker = Arc::clone(&worker);
    ctrlc::set_handler(move || {
        signal_worker.begin_shutdown();
        signal_shutdown.store(true, Ordering::SeqCst);
    })?;
    eprintln!(
        "{{\"event\":\"agent_started\",\"role\":\"all\",\"address\":{}}}",
        serde_json::to_string(&address.to_string())?
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let serve_result = runtime.block_on(async move {
        let listener = tokio::net::TcpListener::from_std(listener)?;
        conductor_api::serve(listener, shutdown, manager, worker_port).await
    });
    let shutdown_result = worker.shutdown();
    serve_result?;
    shutdown_result?;
    Ok(())
}

struct LocalWorkerPort(Arc<conductor_worker::WorkerService>);

impl WorkerPort for LocalWorkerPort {
    fn descriptor(&self) -> conductor_core::WorkerDescriptor {
        self.0.descriptor()
    }

    fn is_draining(&self) -> bool {
        self.0.is_draining()
    }

    fn start(&self, command: StartRun) -> Result<conductor_core::RunObservation, WorkerPortError> {
        self.0.start(command).map_err(map_worker_error)
    }

    fn inspect(
        &self,
        command: InspectRun,
    ) -> Result<Option<conductor_core::RunObservation>, WorkerPortError> {
        self.0.inspect(command).map_err(map_worker_error)
    }

    fn stop(&self, command: StopRun) -> Result<conductor_core::RunObservation, WorkerPortError> {
        self.0.stop(command).map_err(map_worker_error)
    }

    fn read_logs(
        &self,
        command: ReadRunLogs,
    ) -> Result<conductor_core::RunLogChunk, WorkerPortError> {
        self.0.read_logs(command).map_err(map_worker_error)
    }
}

fn map_worker_error(error: conductor_worker::WorkerError) -> WorkerPortError {
    match error {
        conductor_worker::WorkerError::CapacityReached => WorkerPortError::CapacityReached,
        conductor_worker::WorkerError::RunConflict => WorkerPortError::RunConflict,
        conductor_worker::WorkerError::ShuttingDown => WorkerPortError::ShuttingDown,
        conductor_worker::WorkerError::LogsNotFound => WorkerPortError::LogsNotFound,
        conductor_worker::WorkerError::LogsPruned => WorkerPortError::LogsPruned,
        conductor_worker::WorkerError::WorkerMismatch
        | conductor_worker::WorkerError::StorageUnavailable => WorkerPortError::Unavailable,
    }
}

fn validate(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    if !config.listen.ip().is_loopback() {
        return Err("listen address must be an IPv4 or IPv6 loopback address".into());
    }
    if config.worker_id.is_empty()
        || config.worker_id.len() > 128
        || !config
            .worker_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err("worker_id must contain 1-128 ASCII letters, digits, '-', '_' or '.'".into());
    }
    if config.worker_concurrency == 0 {
        return Err("worker_concurrency must be between 1 and 1000".into());
    }
    if config.worker_concurrency > MAX_WORKER_CAPACITY {
        return Err("worker_concurrency must be between 1 and 1000".into());
    }
    if config.manager_data_dir.as_os_str().is_empty()
        || config.worker_data_dir.as_os_str().is_empty()
    {
        return Err("data directories must not be empty".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    #[test]
    fn release_profile_keeps_overflow_and_debug_assertions() {
        if !cfg!(debug_assertions) {
            assert!(
                catch_unwind(|| {
                    let max = std::hint::black_box(u8::MAX);
                    let one = std::hint::black_box(1_u8);
                    let _ = max + one;
                })
                .is_err()
            );
            assert!(
                catch_unwind(|| debug_assert!(false, "release assertions must remain enabled"))
                    .is_err()
            );
        }
    }
}
