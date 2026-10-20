mod backup_cli;
mod contexts;
mod deployment_cli;
mod diagnostics_cli;
mod job_cli;
mod operator_cli;
mod overlay_probe_cli;
mod run_cli;
mod service_cli;
mod state_lock;
mod storage_cli;
mod upgrade_cli;
mod worker_cli;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::net::{SocketAddr, TcpListener};
#[cfg(target_os = "linux")]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use clap::{CommandFactory, Parser, Subcommand, ValueEnum, error::ErrorKind};
use clap_complete::aot::Shell;
#[cfg(target_os = "linux")]
use conductor_core::OverlayEndpointId;
use conductor_core::{
    InspectRun, MAX_WORKER_CAPACITY, NetworkDomainId, ProtocolOffer, ProtocolVersionRange,
    ReadRunLogs, ResolveRun, StartRun, StopRun, WorkerDescriptor, WorkerId, WorkerMetadata,
    WorkerResourceCapacity,
};
use conductor_manager::{WorkerPort, WorkerPortError};
use operator_cli::OperatorArgs;
use serde::Deserialize;
use state_lock::StateLocks;
use tokio::sync::watch;

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
    /// Inspect and operate a remote manager through its authenticated HTTP API.
    Operator(OperatorArgs),
    /// Create and select named operator API connection contexts.
    Context(contexts::ContextArgs),
    /// Register, submit, and inspect one-shot jobs and immutable revisions.
    Job(job_cli::JobArgs),
    /// Start, inspect, wait for, stop, resolve, or read logs from runs.
    Run(run_cli::RunArgs),
    /// Apply and manage service deployments.
    Deployment(deployment_cli::DeploymentArgs),
    /// Inspect workers and manage graceful worker drains.
    Worker(worker_cli::WorkerArgs),
    /// Look up and watch manager-published service endpoints.
    Service(service_cli::ServiceArgs),
    /// Send a bounded HTTP reachability probe through the local worker overlay.
    Overlay(overlay_probe_cli::OverlayArgs),
    /// Check manager health, readiness, and metrics.
    Diagnostics(diagnostics_cli::DiagnosticsArgs),
    /// Inspect local version and database upgrade compatibility.
    Upgrade(upgrade_cli::UpgradeArgs),
    /// Create, inspect, or restore offline state backups.
    Backup(backup_cli::BackupArgs),
    /// Inspect manager storage and history-retention status.
    Storage(storage_cli::StorageArgs),
    /// Generate shell completion scripts for the implemented CLI commands.
    Completions {
        #[arg(value_enum)]
        shell: Shell,
    },
    /// Create a one-time worker enrollment credential in a protected file.
    EnrollWorker {
        /// Manager data directory containing manager.sqlite3.
        #[arg(long)]
        manager_data_dir: PathBuf,
        /// Directory holding the manager CA certificate and protected private key.
        #[arg(long)]
        identity_dir: PathBuf,
        /// Stable worker identifier authorized by this credential.
        #[arg(long)]
        worker_id: String,
        /// Credential lifetime in seconds, from 1 through 86400.
        #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u64).range(1..=86400))]
        valid_for_seconds: u64,
        /// New output file for the raw credential; existing files are never overwritten.
        #[arg(long)]
        output: PathBuf,
    },
    /// Revoke a worker certificate identity and any unused enrollment credentials.
    RevokeWorker {
        /// Manager data directory containing manager.sqlite3.
        #[arg(long)]
        manager_data_dir: PathBuf,
        /// Stable worker identifier to revoke.
        #[arg(long)]
        worker_id: String,
    },
    /// Create a 256-bit operator bearer token in a protected, new file.
    CreateOperatorToken {
        /// New output file for the token; existing files are never overwritten.
        #[arg(long)]
        output: PathBuf,
    },
    /// Internal launcher used only to enter a configured cgroup before exec.
    #[command(name = "__internal-launch", hide = true)]
    InternalLaunch {
        #[arg(long)]
        cgroup_path: Option<PathBuf>,
        #[arg(long)]
        ready_socket: PathBuf,
        #[arg(long)]
        landlock_policy_file: Option<PathBuf>,
        #[arg(long)]
        environment_file: PathBuf,
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(skip)]
    source_config_path: PathBuf,
    role: Role,
    #[serde(default)]
    listen: Option<SocketAddr>,
    #[serde(default)]
    manager_data_dir: Option<PathBuf>,
    #[serde(default)]
    worker_data_dir: Option<PathBuf>,
    #[serde(default)]
    worker_id: Option<String>,
    #[serde(default)]
    worker_concurrency: Option<usize>,
    #[serde(default)]
    api_access: Option<ApiAccessConfig>,
    #[serde(default)]
    worker_transport_listen: Option<SocketAddr>,
    #[serde(default)]
    manager_identity_dir: Option<PathBuf>,
    #[serde(default)]
    manager_server_name: Option<String>,
    #[serde(default)]
    manager_endpoint: Option<String>,
    #[serde(default)]
    worker_identity_file: Option<PathBuf>,
    #[serde(default)]
    worker_overlay_network_domain_id: Option<String>,
    #[serde(default)]
    worker_overlay_helper_binary: Option<PathBuf>,
    #[serde(default)]
    manager_ca_file: Option<PathBuf>,
    #[serde(default)]
    worker_enrollment_credential_file: Option<PathBuf>,
    #[serde(default)]
    worker_transport_capacity: Option<usize>,
    #[serde(default)]
    additional_workers: Vec<RemoteWorkerConfig>,
    #[serde(default)]
    host_resource_domains: Vec<HostResourceDomainConfig>,
    #[serde(default)]
    worker_resource_domain_id: Option<String>,
    #[serde(default)]
    worker_labels: BTreeMap<String, String>,
    #[serde(default)]
    worker_service_network: Option<ServiceNetworkConfig>,
    #[serde(default)]
    service_dns: Option<ServiceDnsListenerConfig>,
    #[serde(default = "default_log_max_bytes_per_stream")]
    log_max_bytes_per_stream: u64,
    #[serde(default = "default_log_retention_runs")]
    log_retention_runs: usize,
    #[serde(default = "default_completed_run_retention")]
    completed_run_retention: usize,
    #[serde(default = "default_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default)]
    cgroup_v2: CgroupV2Config,
    #[serde(default)]
    landlock: LandlockConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceDnsListenerConfig {
    listen: SocketAddr,
    #[serde(default = "default_service_dns_suffix")]
    suffix: String,
    /// Optional separate namespace for overlay-addressed service endpoints.
    #[serde(default)]
    overlay_network_domain_id: Option<String>,
    #[serde(default)]
    overlay_suffix: Option<String>,
    /// Empty means loopback clients only. Non-loopback exposure must be explicit.
    #[serde(default)]
    allowed_clients: Vec<String>,
}

fn default_service_dns_suffix() -> String {
    "conductor.internal".to_owned()
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteWorkerConfig {
    worker_id: String,
    capacity: usize,
    #[serde(default)]
    resource_domain_id: Option<String>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default)]
    service_network: Option<ServiceNetworkConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceNetworkConfig {
    network_domain_id: String,
    port_range_start: u16,
    port_range_end: u16,
    advertised_address: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostResourceDomainConfig {
    id: String,
    cpu_millis: u32,
    memory_bytes: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum ApiAccessConfig {
    Local,
    OperatorTokenFile { token_file: PathBuf },
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CgroupV2Config {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    delegated_root: Option<PathBuf>,
    #[serde(default)]
    ownership_lock_dir: Option<PathBuf>,
    #[serde(default)]
    maximums: conductor_core::ResourceLimits,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct LandlockConfig {
    #[serde(default)]
    mode: LandlockModeConfig,
    #[serde(default)]
    grants: Vec<LandlockGrantConfig>,
    #[serde(default)]
    protected_paths: Vec<PathBuf>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum LandlockModeConfig {
    #[default]
    Disabled,
    Optional,
    Required,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct LandlockGrantConfig {
    id: String,
    path: PathBuf,
    #[serde(default)]
    read: bool,
    #[serde(default)]
    write: bool,
    #[serde(default)]
    execute: bool,
}

fn default_log_max_bytes_per_stream() -> u64 {
    conductor_worker::DEFAULT_LOG_MAX_BYTES_PER_STREAM
}

fn default_log_retention_runs() -> usize {
    conductor_worker::DEFAULT_LOG_RETENTION_RUNS
}

fn default_completed_run_retention() -> usize {
    conductor_manager::DEFAULT_COMPLETED_RUN_RETENTION
}

fn report_run_history_retention(repository: &conductor_manager::SqliteJobRepository) {
    if repository.prune_completed_run_history().is_err() {
        eprintln!(
            "{{\"timestampMs\":{},\"severity\":\"warning\",\"component\":\"manager\",\"event\":\"run_history_retention_failed\"}}",
            timestamp_ms(),
        );
    }
}

fn default_request_timeout_ms() -> u64 {
    conductor_api::DEFAULT_REQUEST_TIMEOUT.as_millis() as u64
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
        Ok(code) => code,
        Err(error) => {
            eprintln!(
                "{{\"timestampMs\":{},\"severity\":\"error\",\"component\":\"agent\",\"event\":\"startup_failed\",\"message\":{}}}",
                timestamp_ms(),
                serde_json::to_string(&error.to_string())
                    .unwrap_or_else(|_| "\"unknown error\"".into())
            );
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<ExitCode, Box<dyn std::error::Error>> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| "TLS crypto provider could not be selected")?;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    close_fds::set_fds_cloexec_threadsafe(3, &[]);
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            print!("{error}");
            return Ok(ExitCode::SUCCESS);
        }
        Err(error) => return Err(error.into()),
    };
    let (config_path, role_override) = match cli.command {
        Commands::Agent { config, role } => (config, role),
        Commands::Operator(args) => return Ok(operator_cli::run(args)),
        Commands::Context(args) => return Ok(contexts::run(args)),
        Commands::Job(args) => return Ok(job_cli::run(args)),
        Commands::Run(args) => return Ok(run_cli::run(args)),
        Commands::Deployment(args) => return Ok(deployment_cli::run(args)),
        Commands::Worker(args) => return Ok(worker_cli::run(args)),
        Commands::Service(args) => return Ok(service_cli::run(args)),
        Commands::Overlay(args) => return Ok(overlay_probe_cli::run(args)),
        Commands::Diagnostics(args) => return Ok(diagnostics_cli::run(args)),
        Commands::Upgrade(args) => return Ok(upgrade_cli::run(args)),
        Commands::Backup(args) => return Ok(backup_cli::run(args)),
        Commands::Storage(args) => return Ok(storage_cli::run(args)),
        Commands::Completions { shell } => {
            let mut command = Cli::command();
            clap_complete::generate(shell, &mut command, "conductor", &mut std::io::stdout());
            return Ok(ExitCode::SUCCESS);
        }
        Commands::EnrollWorker {
            manager_data_dir,
            identity_dir,
            worker_id,
            valid_for_seconds,
            output,
        } => {
            let worker_id = WorkerId::new(worker_id)?;
            let manager_data_dir = state_lock::prepare_directory(&manager_data_dir)?;
            backup_cli::ensure_no_recovery_hold(&manager_data_dir)
                .map_err(|_| "restored manager state requires recovery clearance")?;
            let manager = Arc::new(conductor_manager::SqliteJobRepository::open(
                manager_data_dir.join("manager.sqlite3"),
            )?);
            let authority = Arc::new(conductor_transport::CertificateAuthority::open_or_create(
                identity_dir,
            )?);
            let service = conductor_transport::WorkerEnrollmentService::new(manager, authority);
            let expires_at_ms = timestamp_ms()
                .try_into()
                .ok()
                .and_then(|now: i64| {
                    i64::try_from(valid_for_seconds)
                        .ok()
                        .and_then(|seconds| seconds.checked_mul(1_000))
                        .and_then(|lifetime| now.checked_add(lifetime))
                })
                .ok_or("system time or credential expiration is out of range")?;
            service.create_credential_file(&worker_id, expires_at_ms, &output)?;
            eprintln!(
                "{{\"timestampMs\":{},\"severity\":\"info\",\"component\":\"identity\",\"event\":\"enrollment_credential_created\",\"workerId\":{},\"expiresAtMs\":{},\"credentialFile\":{}}}",
                timestamp_ms(),
                serde_json::to_string(worker_id.as_str())?,
                expires_at_ms,
                serde_json::to_string(&output.to_string_lossy().as_ref())?
            );
            return Ok(ExitCode::SUCCESS);
        }
        Commands::RevokeWorker {
            manager_data_dir,
            worker_id,
        } => {
            let worker_id = WorkerId::new(worker_id)?;
            let manager_data_dir = state_lock::prepare_directory(&manager_data_dir)?;
            backup_cli::ensure_no_recovery_hold(&manager_data_dir)
                .map_err(|_| "restored manager state requires recovery clearance")?;
            let manager = conductor_manager::SqliteJobRepository::open(
                manager_data_dir.join("manager.sqlite3"),
            )?;
            let now_ms = timestamp_ms()
                .try_into()
                .map_err(|_| "system time is out of range")?;
            if !manager
                .revoke_worker_identity(&worker_id, now_ms)
                .map_err(|_| "worker identity storage is unavailable")?
            {
                return Err("worker identity does not exist".into());
            }
            eprintln!(
                "{{\"timestampMs\":{},\"severity\":\"info\",\"component\":\"identity\",\"event\":\"worker_identity_revoked\",\"workerId\":{}}}",
                timestamp_ms(),
                serde_json::to_string(worker_id.as_str())?
            );
            return Ok(ExitCode::SUCCESS);
        }
        Commands::CreateOperatorToken { output } => {
            let mut random = zeroize::Zeroizing::new([0_u8; 32]);
            getrandom::fill(&mut *random).map_err(|_| "secure random token generation failed")?;
            let mut encoded = zeroize::Zeroizing::new([0_u8; 43]);
            let written = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode_slice(*random, &mut *encoded)
                .map_err(|_| "operator token encoding failed")?;
            let write_result = state_lock::write_secret_file_once(&output, &encoded[..written]);
            drop(random);
            drop(encoded);
            write_result.map_err(|_| "operator token file could not be created securely")?;
            eprintln!(
                "{{\"timestampMs\":{},\"severity\":\"info\",\"component\":\"identity\",\"event\":\"operator_token_created\",\"tokenFile\":{}}}",
                timestamp_ms(),
                serde_json::to_string(&output.to_string_lossy().as_ref())?
            );
            return Ok(ExitCode::SUCCESS);
        }
        Commands::InternalLaunch {
            cgroup_path,
            ready_socket,
            landlock_policy_file,
            environment_file,
            command,
        } => {
            return Ok(ExitCode::from(conductor_process::internal_launch(
                cgroup_path.as_deref(),
                &ready_socket,
                landlock_policy_file.as_deref(),
                &environment_file,
                &command,
            ) as u8));
        }
    };
    let source_config_path = fs::canonicalize(&config_path)?;
    let config_text = fs::read_to_string(config_path)?;
    let mut config: Config = serde_json::from_str(&config_text)?;
    config.source_config_path = source_config_path;
    let configured_role = config.role;
    let selected_role = role_override.unwrap_or(config.role);
    resolve_config_paths(
        &mut config,
        selected_role,
        preserve_legacy_paths(configured_role, role_override),
    );
    config.role = selected_role;
    validate(&config)?;
    match config.role {
        Role::All => run_all(config),
        Role::Manager => run_manager(config),
        Role::Worker => run_worker(config),
    }
}

fn preserve_legacy_paths(configured_role: Role, role_override: Option<Role>) -> bool {
    matches!(configured_role, Role::All) && role_override.is_some()
}

fn resolve_config_paths(config: &mut Config, role: Role, preserve_legacy_paths: bool) {
    let base = config
        .source_config_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let mut resolve_from_config = [
        &mut config.manager_identity_dir,
        &mut config.manager_ca_file,
        &mut config.worker_identity_file,
        &mut config.worker_enrollment_credential_file,
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    if !matches!(role, Role::All) && !preserve_legacy_paths {
        resolve_from_config.extend(
            [&mut config.manager_data_dir, &mut config.worker_data_dir]
                .into_iter()
                .flatten(),
        );
    }
    for path in resolve_from_config {
        if !path.is_absolute() {
            *path = base.join(&*path);
        }
    }
}

fn run_all(config: Config) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let api_access = api_access_policy(&config)?;
    let manager_data_dir = required(&config.manager_data_dir, "manager_data_dir")?;
    let worker_data_dir = required(&config.worker_data_dir, "worker_data_dir")?;
    let worker_id = WorkerId::new(required(&config.worker_id, "worker_id")?.clone())?;
    let worker_concurrency = *required(&config.worker_concurrency, "worker_concurrency")?;
    let listen = *required(&config.listen, "listen")?;
    let dns_config = service_dns_config(config.service_dns.as_ref())?;
    let _state_locks = StateLocks::acquire(manager_data_dir, worker_data_dir)?;
    backup_cli::ensure_no_recovery_hold(manager_data_dir)?;
    backup_cli::ensure_no_recovery_hold(worker_data_dir)?;
    let manager_repository = Arc::new(conductor_manager::SqliteJobRepository::open_with_retention(
        manager_data_dir.join("manager.sqlite3"),
        config.completed_run_retention,
    )?);
    report_run_history_retention(&manager_repository);
    let manager = Arc::new(conductor_manager::JobService::new(manager_repository));
    let worker = Arc::new(conductor_worker::WorkerService::open_with_log_policy(
        worker_data_dir,
        worker_id.clone(),
        worker_concurrency,
        conductor_worker::LogPolicy {
            max_bytes_per_stream: config.log_max_bytes_per_stream,
            retention_runs: config.log_retention_runs,
        },
        process_backend(&config)?,
    )?);
    let metadata = worker_metadata(
        &config,
        worker.descriptor().worker_id(),
        config.worker_resource_domain_id.as_deref(),
        &config.worker_labels,
        config.worker_service_network.as_ref(),
    )?;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    close_fds::set_fds_cloexec_threadsafe(3, &[]);
    let worker_port: Arc<dyn WorkerPort> = Arc::new(LocalWorkerPort {
        worker: Arc::clone(&worker),
        metadata,
    });
    let listener = TcpListener::bind(listen)?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = Arc::clone(&shutdown);
    let signal_worker = Arc::clone(&worker);
    ctrlc::set_handler(move || {
        signal_worker.begin_shutdown();
        signal_shutdown.store(true, Ordering::SeqCst);
    })?;
    log_started(Role::All, address, None, &config)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let manager_for_dns = Arc::clone(&manager);
    let dns_workers = vec![Arc::clone(&worker_port)];
    let serve_result = runtime.block_on(async move {
        let listener = tokio::net::TcpListener::from_std(listener)?;
        if let Some(dns_config) = dns_config {
            let dns_shutdown = Arc::clone(&shutdown);
            let mut dns_task = tokio::spawn(conductor_api::dns::serve_service_dns(
                dns_config,
                manager_for_dns,
                dns_workers,
                dns_shutdown,
            ));
            let api_shutdown = Arc::clone(&shutdown);
            let mut api_task = Box::pin(conductor_api::serve_with_access_policy(
                listener,
                api_shutdown,
                manager,
                worker_port,
                Duration::from_millis(config.request_timeout_ms),
                api_access,
            ));
            tokio::select! {
                api_result = &mut api_task => {
                    shutdown.store(true, Ordering::Release);
                    let dns_result = dns_task.await
                        .map_err(std::io::Error::other)?
                        .map_err(|error| std::io::Error::other(error.to_string()));
                    api_result.and(dns_result)
                }
                dns_result = &mut dns_task => {
                    let shutting_down = shutdown.load(Ordering::Acquire);
                    shutdown.store(true, Ordering::Release);
                    let api_result = api_task.await;
                    let dns_result = dns_result
                        .map_err(std::io::Error::other)?
                        .map_err(|error| std::io::Error::other(error.to_string()));
                    if !shutting_down && dns_result.is_ok() {
                        return Err(std::io::Error::other("service DNS server stopped unexpectedly"));
                    }
                    dns_result?;
                    api_result
                }
            }
        } else {
            conductor_api::serve_with_access_policy(
                listener,
                shutdown,
                manager,
                worker_port,
                Duration::from_millis(config.request_timeout_ms),
                api_access,
            )
            .await
        }
    });
    let shutdown_result = worker.shutdown();
    serve_result?;
    shutdown_result?;
    Ok(ExitCode::SUCCESS)
}

fn run_manager(config: Config) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let manager_data_dir = required(&config.manager_data_dir, "manager_data_dir")?;
    let listen = *required(&config.listen, "listen")?;
    let transport_listen = *required(&config.worker_transport_listen, "worker_transport_listen")?;
    let identity_dir = required(&config.manager_identity_dir, "manager_identity_dir")?;
    let server_name = required(&config.manager_server_name, "manager_server_name")?;
    let worker_id = WorkerId::new(required(&config.worker_id, "worker_id")?.clone())?;
    let capacity = *required(
        &config.worker_transport_capacity,
        "worker_transport_capacity",
    )?;
    let dns_config = service_dns_config(config.service_dns.as_ref())?;
    let api_access = api_access_policy(&config)?;
    let _state_locks = StateLocks::acquire_manager(manager_data_dir)
        .map_err(|error| format!("manager state lock could not be acquired: {error}"))?;
    backup_cli::ensure_no_recovery_hold(manager_data_dir)
        .map_err(|_| "restored manager state requires recovery clearance")?;

    let repository = Arc::new(
        conductor_manager::SqliteJobRepository::open_with_retention(
            manager_data_dir.join("manager.sqlite3"),
            config.completed_run_retention,
        )
        .map_err(|error| format!("manager repository could not be opened: {error}"))?,
    );
    report_run_history_retention(&repository);
    let manager = Arc::new(conductor_manager::JobService::new(repository.clone()));
    let authority = Arc::new(
        conductor_transport::CertificateAuthority::open_or_create(identity_dir).map_err(
            |error| format!("manager certificate authority could not be opened: {error}"),
        )?,
    );
    let enrollment = Arc::new(conductor_transport::WorkerEnrollmentService::new(
        repository.clone(),
        Arc::clone(&authority),
    ));
    let hub = conductor_transport::WorkerTransportHub::with_journal(repository.clone());
    let descriptor = WorkerDescriptor::new(worker_id.clone(), capacity, true);
    let offer = ProtocolOffer::new(
        ProtocolVersionRange::current(),
        BTreeSet::from([
            conductor_core::ProtocolCapability::OrderedReports,
            conductor_core::ProtocolCapability::WorkerIncarnations,
            conductor_core::ProtocolCapability::WorkerResourceInventory,
        ]),
        BTreeSet::from([conductor_core::ProtocolCapability::WorkerResourceInventory]),
    )?;
    let mut worker_ports: Vec<Arc<dyn WorkerPort>> =
        Vec::with_capacity(config.additional_workers.len() + 1);
    let metadata = worker_metadata(
        &config,
        &worker_id,
        config.worker_resource_domain_id.as_deref(),
        &config.worker_labels,
        config.worker_service_network.as_ref(),
    )?;
    worker_ports.push(hub.register_worker_with_metadata(descriptor, offer.clone(), metadata)?);
    for additional in &config.additional_workers {
        let additional_id = WorkerId::new(additional.worker_id.clone())?;
        let metadata = worker_metadata(
            &config,
            &additional_id,
            additional.resource_domain_id.as_deref(),
            &additional.labels,
            additional.service_network.as_ref(),
        )?;
        worker_ports.push(hub.register_worker_with_metadata(
            WorkerDescriptor::new(additional_id, additional.capacity, true),
            offer.clone(),
            metadata,
        )?);
    }
    let server_identity = authority
        .issue_server_identity(server_name)
        .map_err(|error| format!("manager server identity could not be issued: {error}"))?;
    let acceptor = authority
        .mtls_acceptor(
            &server_identity.certificate_pem,
            server_identity.private_key_pem(),
        )
        .map_err(|error| format!("manager transport TLS could not be configured: {error}"))?;

    let api_listener = TcpListener::bind(listen)
        .map_err(|error| format!("manager API listener could not bind: {error}"))?;
    api_listener.set_nonblocking(true)?;
    let api_address = api_listener.local_addr()?;
    let transport_listener = TcpListener::bind(transport_listen)
        .map_err(|error| format!("manager worker transport listener could not bind: {error}"))?;
    transport_listener.set_nonblocking(true)?;
    let transport_address = transport_listener.local_addr()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = Arc::clone(&shutdown);
    ctrlc::set_handler(move || signal_shutdown.store(true, Ordering::SeqCst))
        .map_err(|error| format!("manager signal handler could not be installed: {error}"))?;
    log_started(Role::Manager, api_address, Some(transport_address), &config)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let request_timeout = Duration::from_millis(config.request_timeout_ms);
    let dns_manager = Arc::clone(&manager);
    let dns_workers = worker_ports.clone();
    let result: Result<(), Box<dyn std::error::Error>> = runtime.block_on(async move {
        let server_handle = axum_server::Handle::new();
        let mut transport_task = tokio::spawn(
            axum_server::from_tcp(transport_listener)?
                .acceptor(acceptor)
                .handle(server_handle.clone())
                .serve(enrollment.router_with_hub(&hub).into_make_service()),
        );
        let api_listener = tokio::net::TcpListener::from_std(api_listener)?;
        let readiness_ports = worker_ports.clone();
        let recovery_ports = worker_ports.clone();
        let workers_ready = Arc::new(AtomicBool::new(false));
        let ready_for_workers = Arc::clone(&workers_ready);
        let readiness_task = tokio::spawn(async move {
            loop {
                let ready = readiness_ports
                    .iter()
                    .all(|worker| !worker.is_draining() && !worker.is_degraded());
                ready_for_workers.store(ready, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        let mut api_task = Box::pin(conductor_api::serve_manager_with_access_policy(
            api_listener,
            Arc::clone(&shutdown),
            manager,
            worker_ports.remove(0),
            request_timeout,
            api_access,
            conductor_api::ManagerRecovery::new(workers_ready, recovery_ports),
        ));
        let mut dns_task = dns_config.map(|dns_config| {
            tokio::spawn(conductor_api::dns::serve_service_dns(
                dns_config,
                dns_manager,
                dns_workers,
                Arc::clone(&shutdown),
            ))
        });
        let (completed_transport, api_result, transport_result) = tokio::select! {
            api_result = &mut api_task => (false, api_result, None),
            transport_result = &mut transport_task => {
                if !shutdown.load(Ordering::Acquire) {
                    readiness_task.abort();
                    return match transport_result {
                        Ok(Ok(())) => Err("worker transport server stopped unexpectedly".into()),
                        Ok(Err(error)) => Err(format!("worker transport server failed: {error}").into()),
                        Err(error) => Err(format!("worker transport server task failed: {error}").into()),
                    };
                }
                (true, (&mut api_task).await, Some(transport_result))
            },
            dns_result = async {
                match dns_task.as_mut() {
                    Some(task) => Some(task.await),
                    None => std::future::pending().await,
                }
            } => {
                let shutting_down = shutdown.load(Ordering::Acquire);
                if !shutting_down {
                    shutdown.store(true, Ordering::Release);
                }
                server_handle.graceful_shutdown(Some(Duration::from_secs(5)));
                readiness_task.abort();
                let api_result = (&mut api_task).await;
                finish_transport_server(&mut transport_task).await?;
                return match dns_result {
                    Some(Ok(Ok(()))) if shutting_down => {
                        api_result?;
                        Ok(())
                    }
                    Some(Ok(Ok(()))) => Err("service DNS server stopped unexpectedly".into()),
                    Some(Ok(Err(error))) => Err(format!("service DNS server failed: {error}").into()),
                    Some(Err(error)) => Err(format!("service DNS server task failed: {error}").into()),
                    None => Err("service DNS server result is missing".into()),
                };
            }
        };
        shutdown.store(true, Ordering::Release);
        readiness_task.abort();
        if !completed_transport {
            server_handle.graceful_shutdown(Some(Duration::from_secs(5)));
            finish_transport_server(&mut transport_task).await?;
        } else {
            match transport_result {
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(error))) => {
                    return Err(format!("worker transport server failed: {error}").into());
                }
                Some(Err(error)) => {
                    return Err(format!("worker transport server task failed: {error}").into());
                }
                None => return Err("worker transport task result is missing".into()),
            }
        }
        if let Some(task) = dns_task.take() {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(format!("service DNS server failed: {error}").into()),
                Err(error) => return Err(format!("service DNS server task failed: {error}").into()),
            }
        }
        api_result?;
        Ok(())
    });
    result?;
    Ok(ExitCode::SUCCESS)
}

async fn finish_transport_server(
    task: &mut tokio::task::JoinHandle<Result<(), std::io::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    match tokio::time::timeout(Duration::from_secs(6), task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(format!("worker transport server failed: {error}").into()),
        Ok(Err(error)) => Err(format!("worker transport server task failed: {error}").into()),
        Err(_) => Err("worker transport server did not shut down within six seconds".into()),
    }
}

fn run_worker(config: Config) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let worker_data_dir = required(&config.worker_data_dir, "worker_data_dir")?;
    let worker_id = WorkerId::new(required(&config.worker_id, "worker_id")?.clone())?;
    let concurrency = *required(&config.worker_concurrency, "worker_concurrency")?;
    let identity_path = required(&config.worker_identity_file, "worker_identity_file")?;
    let ca_path = required(&config.manager_ca_file, "manager_ca_file")?;
    let endpoint = required(&config.manager_endpoint, "manager_endpoint")?;
    let _state_locks = StateLocks::acquire_worker(worker_data_dir)?;
    backup_cli::ensure_no_recovery_hold(worker_data_dir)
        .map_err(|_| "restored worker state requires recovery clearance")?;
    let ca = fs::read(ca_path).map_err(|_| "manager CA certificate could not be read")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let identity = zeroize::Zeroizing::new(runtime.block_on(load_or_enroll_worker_identity(
        identity_path,
        config.worker_enrollment_credential_file.as_deref(),
        endpoint,
        &ca,
        worker_id.as_str(),
    ))?);
    let worker = Arc::new(conductor_worker::WorkerService::open_with_log_policy(
        worker_data_dir,
        worker_id.clone(),
        concurrency,
        conductor_worker::LogPolicy {
            max_bytes_per_stream: config.log_max_bytes_per_stream,
            retention_runs: config.log_retention_runs,
        },
        process_backend(&config)?,
    )?);
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    close_fds::set_fds_cloexec_threadsafe(3, &[]);
    let client = Arc::new(
        conductor_transport::WorkerTransportClient::from_pem_identity_with_root(
            endpoint, &identity, &ca,
        )?,
    );
    let overlay_domain = config
        .worker_overlay_network_domain_id
        .as_deref()
        .map(|id| NetworkDomainId::new(id.to_owned()))
        .transpose()?;
    #[cfg(not(target_os = "linux"))]
    if overlay_domain.is_some() {
        return Err("worker overlay networking currently requires Linux".into());
    }
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = Arc::clone(&shutdown);
    let shutdown_after_run = Arc::clone(&shutdown);
    let signal_worker = Arc::clone(&worker);
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let overlay_shutdown_sender = shutdown_sender.clone();
    ctrlc::set_handler(move || {
        signal_worker.begin_shutdown();
        signal_shutdown.store(true, Ordering::SeqCst);
        let _ = shutdown_sender.send(true);
    })?;
    eprintln!(
        "{{\"timestampMs\":{},\"severity\":\"info\",\"component\":\"agent\",\"event\":\"agent_started\",\"role\":\"worker\",\"workerId\":{},\"managerEndpoint\":{}}}",
        timestamp_ms(),
        serde_json::to_string(worker.descriptor().worker_id().as_str())?,
        serde_json::to_string(endpoint)?
    );
    let protocol_offer = ProtocolOffer::new(
        ProtocolVersionRange::current(),
        BTreeSet::from([
            conductor_core::ProtocolCapability::OrderedReports,
            conductor_core::ProtocolCapability::WorkerIncarnations,
            conductor_core::ProtocolCapability::WorkerResourceInventory,
        ]),
        BTreeSet::from([conductor_core::ProtocolCapability::WorkerResourceInventory]),
    )?;
    #[cfg(target_os = "linux")]
    let overlay_task = runtime.block_on(async {
        if let Some(domain_id) = overlay_domain {
            let helper_path = required(
                &config.worker_overlay_helper_binary,
                "worker_overlay_helper_binary",
            )?
            .clone();
            let overlay_key = load_or_create_overlay_key(worker_data_dir)?;
            let endpoint_id = OverlayEndpointId::try_from(
                iroh_base::SecretKey::from_bytes(&overlay_key)
                    .public()
                    .to_string(),
            )?;
            client
                .register_overlay_identity(&domain_id, &endpoint_id)
                .await
                .map_err(|_| "worker overlay identity registration failed")?;
            let configuration = client
                .overlay_configuration(&domain_id)
                .await
                .map_err(|_| "worker overlay configuration could not be fetched")?;
            if configuration.network_domain_id != domain_id || configuration.worker_id != worker_id
            {
                return Err(
                    "manager returned an overlay configuration for a different worker or domain"
                        .into(),
                );
            }
            let mut helper = conductor_overlay::LinuxOverlayHelperClient::start(
                &helper_path,
                &worker_id,
                overlay_key,
                &configuration,
            )
            .await
            .map_err(|_| "worker overlay helper could not be started")?;
            let acknowledgement = client
                .advertise_overlay_candidates(
                    &domain_id,
                    configuration.peer_generation,
                    helper.direct_candidates(),
                )
                .await
                .map_err(|_| "worker direct candidates could not be advertised")?;
            let configuration = client
                .overlay_configuration(&domain_id)
                .await
                .map_err(|_| "worker overlay configuration could not be refreshed")?;
            helper
                .reconfigure(&configuration)
                .await
                .map_err(|_| "worker overlay helper could not apply its peer map")?;
            let candidate_refresh_at_ms =
                candidate_refresh_at(acknowledgement.candidates_expires_at_ms);
            let helper_runtime = helper.status().await.ok().flatten();
            report_overlay_observation(
                &client,
                &domain_id,
                &worker_id,
                &configuration,
                conductor_core::OverlayPeerObservationStatus::Applied,
                helper_runtime,
            )
            .await;
            Ok::<_, Box<dyn std::error::Error>>(Some(tokio::spawn(run_worker_overlay(
                Arc::clone(&client),
                domain_id,
                worker_id.clone(),
                helper,
                configuration,
                candidate_refresh_at_ms,
                shutdown_receiver.clone(),
            ))))
        } else {
            Ok(None)
        }
    })?;
    let run_result = runtime.block_on(async {
        #[cfg(target_os = "linux")]
        let result = if let Some(mut overlay_task) = overlay_task {
            tokio::select! {
                result = client.run(Arc::clone(&worker), protocol_offer, shutdown_receiver) => {
                    shutdown_after_run.store(true, Ordering::SeqCst);
                    let _ = overlay_shutdown_sender.send(true);
                    overlay_task.await
                        .map_err(|_| conductor_transport::TransportError::Unavailable)?
                        .map_err(|_| conductor_transport::TransportError::Unavailable)?;
                    result
                }
                overlay_result = &mut overlay_task => {
                    if shutdown.load(Ordering::SeqCst) {
                        Ok(())
                    } else {
                        worker.begin_shutdown();
                        Err(match overlay_result {
                            Ok(Err(_)) | Err(_) => conductor_transport::TransportError::Unavailable,
                            Ok(Ok(())) => conductor_transport::TransportError::Unavailable,
                        })
                    }
                }
            }
        } else {
            client
                .run(Arc::clone(&worker), protocol_offer, shutdown_receiver)
                .await
        };
        #[cfg(not(target_os = "linux"))]
        let result = client
            .run(Arc::clone(&worker), protocol_offer, shutdown_receiver)
            .await;
        shutdown_after_run.store(true, Ordering::SeqCst);
        let _ = overlay_shutdown_sender.send(true);
        result
    });
    let worker_shutdown = worker.shutdown();
    run_result?;
    worker_shutdown?;
    Ok(ExitCode::SUCCESS)
}

#[cfg(target_os = "linux")]
async fn run_worker_overlay(
    client: Arc<conductor_transport::WorkerTransportClient>,
    domain_id: NetworkDomainId,
    worker_id: WorkerId,
    mut helper: conductor_overlay::LinuxOverlayHelperClient,
    mut configuration: conductor_core::OverlayPeerConfiguration,
    mut candidate_refresh_at_ms: i64,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let mut refresh = tokio::time::interval(Duration::from_secs(15));
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    refresh.tick().await;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return helper.shutdown().await.map_err(|_| "worker overlay helper shutdown failed".to_owned());
                }
            }
            _ = refresh.tick() => {
                if overlay_now_ms() >= candidate_refresh_at_ms {
                    if let Ok(acknowledgement) = client.advertise_overlay_candidates(
                        &domain_id,
                        configuration.peer_generation,
                        helper.direct_candidates(),
                    ).await {
                        candidate_refresh_at_ms = candidate_refresh_at(acknowledgement.candidates_expires_at_ms);
                    } else {
                        candidate_refresh_at_ms = overlay_now_ms().saturating_add(30_000);
                    }
                }
                let updated = client.overlay_configuration(&domain_id).await;
                let Ok(updated) = updated else { continue; };
                if updated.network_domain_id != domain_id || updated.worker_id != worker_id {
                    return Err("manager returned an overlay configuration for a different worker or domain".to_owned());
                }
                if updated != configuration {
                    if helper.reconfigure(&updated).await.is_err() {
                        report_overlay_observation(
                            &client,
                            &domain_id,
                            &worker_id,
                            &updated,
                            conductor_core::OverlayPeerObservationStatus::Rejected,
                            None,
                        )
                        .await;
                        return Err("worker overlay helper rejected a manager configuration update".to_owned());
                    }
                    configuration = updated;
                }
                let helper_runtime = helper.status().await.ok().flatten();
                let observation_status = if helper_runtime.is_some() {
                    conductor_core::OverlayPeerObservationStatus::Applied
                } else {
                    conductor_core::OverlayPeerObservationStatus::Unavailable
                };
                report_overlay_observation(
                    &client,
                    &domain_id,
                    &worker_id,
                    &configuration,
                    observation_status,
                    helper_runtime,
                )
                .await;
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn candidate_refresh_at(expires_at_ms: Option<i64>) -> i64 {
    expires_at_ms.map_or_else(
        || overlay_now_ms().saturating_add(60_000),
        |expiry| expiry.saturating_sub(60_000),
    )
}

#[cfg(target_os = "linux")]
fn overlay_now_ms() -> i64 {
    i64::try_from(timestamp_ms()).unwrap_or(i64::MAX)
}

#[cfg(target_os = "linux")]
async fn report_overlay_observation(
    client: &conductor_transport::WorkerTransportClient,
    domain_id: &NetworkDomainId,
    worker_id: &WorkerId,
    configuration: &conductor_core::OverlayPeerConfiguration,
    status: conductor_core::OverlayPeerObservationStatus,
    runtime: Option<conductor_core::OverlayRuntimeDiagnostic>,
) {
    let observation = conductor_core::OverlayPeerObservation {
        network_domain_id: domain_id.clone(),
        worker_id: worker_id.clone(),
        peer_generation: configuration.peer_generation,
        applied_network_generation: configuration.network_generation,
        observed_at_ms: i64::try_from(timestamp_ms()).unwrap_or(i64::MAX),
        status,
        runtime,
    };
    let _ = client
        .report_overlay_observation(domain_id, &observation)
        .await;
}

#[cfg(target_os = "linux")]
fn load_or_create_overlay_key(
    worker_data_dir: &std::path::Path,
) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let path = worker_data_dir.join("overlay-identity.key");
    let mut generated = [0_u8; 32];
    getrandom::fill(&mut generated).map_err(|_| "overlay identity key generation failed")?;
    match state_lock::write_secret_file_once(&path, &generated) {
        Ok(()) => Ok(generated),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let mut file = state_lock::open_secret_file_no_follow(&path)?;
            let metadata = file.metadata()?;
            if !metadata.is_file() {
                return Err("overlay identity key must be a regular file".into());
            }
            #[cfg(unix)]
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err("overlay identity key permissions are too broad".into());
            }
            let mut existing = Vec::new();
            file.read_to_end(&mut existing)?;
            let key: [u8; 32] = existing
                .try_into()
                .map_err(|_| "overlay identity key has an invalid length")?;
            Ok(key)
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerEnrollmentRequest<'a> {
    credential: &'a str,
    csr_pem: &'a str,
}

async fn load_or_enroll_worker_identity(
    identity_path: &std::path::Path,
    credential_path: Option<&std::path::Path>,
    endpoint: &str,
    ca_pem: &[u8],
    worker_id: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if fs::symlink_metadata(identity_path).is_ok() {
        return read_private_pem(identity_path);
    }
    let credential_path = credential_path.ok_or(
        "worker_identity_file is missing and worker_enrollment_credential_file is not configured",
    )?;
    let mut credential_bytes = zeroize::Zeroizing::new(read_private_pem(credential_path)?);
    while credential_bytes.last().is_some_and(u8::is_ascii_whitespace) {
        credential_bytes.pop();
    }
    let credential = std::str::from_utf8(&credential_bytes)
        .map_err(|_| "worker enrollment credential is invalid")?;
    let key = conductor_transport::WorkerKeyMaterial::generate()
        .map_err(|_| "worker identity key could not be generated")?;
    let mut request_body = zeroize::Zeroizing::new(
        serde_json::to_vec(&WorkerEnrollmentRequest {
            credential,
            csr_pem: key.certificate_signing_request_pem(),
        })
        .map_err(|_| "worker enrollment request could not be encoded")?,
    );
    if request_body.len() > conductor_transport::MAX_TRANSPORT_BODY_BYTES {
        return Err("worker enrollment request exceeds its size limit".into());
    }
    let root =
        reqwest::Certificate::from_pem(ca_pem).map_err(|_| "manager CA certificate is invalid")?;
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .tls_certs_only([root])
        .build()
        .map_err(|_| "worker enrollment client could not be initialized")?;
    let enrollment_url = reqwest::Url::parse(endpoint)
        .and_then(|url| url.join("v1/worker/enroll"))
        .map_err(|_| "manager_endpoint is invalid")?;
    let mut response = client
        .post(enrollment_url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(std::mem::take(&mut *request_body))
        .send()
        .await
        .map_err(|_| "worker enrollment request failed")?;
    if !response.status().is_success()
        || response
            .content_length()
            .is_some_and(|length| length > conductor_transport::MAX_TRANSPORT_BODY_BYTES as u64)
    {
        return Err("worker enrollment was rejected".into());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "worker enrollment response failed")?
    {
        if body.len().saturating_add(chunk.len()) > conductor_transport::MAX_TRANSPORT_BODY_BYTES {
            return Err("worker enrollment response exceeds its size limit".into());
        }
        body.extend_from_slice(&chunk);
    }
    let issued: conductor_transport::IssuedWorkerIdentity =
        serde_json::from_slice(&body).map_err(|_| "worker enrollment response is invalid")?;
    if issued.worker_id.as_str() != worker_id {
        return Err("enrolled worker identity does not match configuration".into());
    }
    let mut identity = zeroize::Zeroizing::new(String::with_capacity(
        issued.certificate_pem.len() + key.private_key_pem().len() + 1,
    ));
    identity.push_str(&issued.certificate_pem);
    identity.push_str(key.private_key_pem());
    state_lock::write_secret_file_once(identity_path, identity.as_bytes())
        .map_err(|_| "worker identity file could not be created securely")?;
    fs::remove_file(credential_path)
        .map_err(|_| "used worker enrollment credential could not be removed")?;
    Ok(identity.as_bytes().to_vec())
}

fn read_private_pem(path: &std::path::Path) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let file = state_lock::open_secret_file_no_follow(path)
        .map_err(|_| "worker identity file could not be accessed securely")?;
    let metadata = file
        .metadata()
        .map_err(|_| "worker identity file could not be accessed securely")?;
    if !metadata.is_file() {
        return Err("worker identity path must be a regular file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("worker identity file permissions must deny group and other access".into());
        }
    }
    let mut contents = Vec::new();
    file.take(64 * 1024)
        .read_to_end(&mut contents)
        .map_err(|_| "worker identity file could not be accessed securely")?;
    if contents.len() >= 64 * 1024 {
        return Err("worker identity file exceeds its size limit".into());
    }
    Ok(contents)
}

fn required<'a, T>(value: &'a Option<T>, name: &str) -> Result<&'a T, Box<dyn std::error::Error>> {
    value
        .as_ref()
        .ok_or_else(|| format!("configuration field '{name}' is required for this role").into())
}

fn log_started(
    role: Role,
    address: SocketAddr,
    transport_address: Option<SocketAddr>,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut event = serde_json::json!({
        "timestampMs": timestamp_ms(),
        "severity": "info",
        "component": "agent",
        "event": "agent_started",
        "role": role.name(),
        "address": address.to_string(),
        "apiAccessMode": match config.api_access.as_ref() {
            Some(ApiAccessConfig::Local) | None => "local",
            Some(ApiAccessConfig::OperatorTokenFile { .. }) => "operator_token",
        },
        "cgroupV2Enabled": config.cgroup_v2.enabled,
        "landlockMode": match config.landlock.mode {
            LandlockModeConfig::Disabled => "disabled",
            LandlockModeConfig::Optional => "optional",
            LandlockModeConfig::Required => "required",
        }
    });
    if let Some(address) = transport_address {
        event["workerTransportAddress"] = serde_json::Value::String(address.to_string());
    }
    if let Some(service_dns) = &config.service_dns {
        event["serviceDnsAddress"] = serde_json::Value::String(service_dns.listen.to_string());
        event["serviceDnsSuffix"] = serde_json::Value::String(service_dns.suffix.clone());
        if let Some(suffix) = &service_dns.overlay_suffix {
            event["serviceDnsOverlaySuffix"] = serde_json::Value::String(suffix.clone());
        }
    }
    eprintln!("{}", serde_json::to_string(&event)?);
    Ok(())
}

fn process_backend(
    config: &Config,
) -> Result<Arc<dyn conductor_worker::ExecutionBackend>, Box<dyn std::error::Error>> {
    #[cfg(target_os = "linux")]
    {
        let worker_id = required(&config.worker_id, "worker_id")?;
        let mut backend =
            if config.cgroup_v2.enabled {
                let root =
                    config.cgroup_v2.delegated_root.as_deref().ok_or(
                        "cgroup_v2.delegated_root is required when cgroup_v2.enabled is true",
                    )?;
                let lock_directory = config.cgroup_v2.ownership_lock_dir.as_deref().ok_or(
                    "cgroup_v2.ownership_lock_dir is required when cgroup_v2.enabled is true",
                )?;
                conductor_process::HostProcessBackend::with_cgroup_v2(
                    root,
                    lock_directory,
                    worker_id,
                    config.cgroup_v2.maximums.clone(),
                    std::env::current_exe()?,
                )?
            } else {
                conductor_process::HostProcessBackend::default()
            };
        backend = backend.with_landlock(
            match config.landlock.mode {
                LandlockModeConfig::Disabled => conductor_process::LandlockMode::Disabled,
                LandlockModeConfig::Optional => conductor_process::LandlockMode::Optional,
                LandlockModeConfig::Required => conductor_process::LandlockMode::Required,
            },
            config
                .landlock
                .grants
                .iter()
                .map(|grant| conductor_process::LandlockGrantConfig {
                    id: grant.id.clone(),
                    path: grant.path.clone(),
                    read: grant.read,
                    write: grant.write,
                    execute: grant.execute,
                })
                .collect(),
            &{
                let mut protected = vec![config.source_config_path.clone()];
                protected.extend(config.manager_data_dir.iter().cloned());
                protected.extend(config.worker_data_dir.iter().cloned());
                protected.extend(config.manager_ca_file.iter().cloned());
                protected.extend(config.worker_identity_file.iter().cloned());
                protected.extend(config.worker_enrollment_credential_file.iter().cloned());
                protected.extend(config.landlock.protected_paths.iter().cloned());
                if let Some(home) = std::env::var_os("HOME") {
                    let home = PathBuf::from(home);
                    protected.extend([home.join(".ssh"), home.join(".aws"), home.join(".config")]);
                }
                protected.extend([
                    PathBuf::from("/etc/shadow"),
                    PathBuf::from("/etc/gshadow"),
                    PathBuf::from("/etc/ssh"),
                ]);
                if let Ok(homes) = fs::read_dir("/home") {
                    for home in homes.flatten() {
                        let home = home.path();
                        protected.extend([
                            home.join(".ssh"),
                            home.join(".aws"),
                            home.join(".config"),
                        ]);
                    }
                }
                if config.cgroup_v2.enabled
                    && let Some(root) = &config.cgroup_v2.delegated_root
                {
                    protected.push(root.clone());
                }
                if config.cgroup_v2.enabled
                    && let Some(lock_directory) = &config.cgroup_v2.ownership_lock_dir
                {
                    protected.push(lock_directory.clone());
                }
                protected
            },
            std::env::current_exe()?,
        )?;
        Ok(Arc::new(backend))
    }
    #[cfg(not(target_os = "linux"))]
    {
        if config.landlock.mode != LandlockModeConfig::Disabled || config.cgroup_v2.enabled {
            return Err("cgroup v2 and Landlock enforcement are available only on Linux".into());
        }
        Ok(Arc::new(conductor_process::HostProcessBackend::default()))
    }
}

fn service_dns_config(
    configured: Option<&ServiceDnsListenerConfig>,
) -> Result<Option<conductor_api::dns::ServiceDnsConfig>, Box<dyn std::error::Error>> {
    let Some(configured) = configured else {
        return Ok(None);
    };
    if !configured.listen.ip().is_loopback() && configured.allowed_clients.is_empty() {
        return Err("non-loopback service DNS listeners require explicit allowed_clients".into());
    }
    let allowed_clients = if configured.allowed_clients.is_empty() {
        vec!["127.0.0.0/8".parse()?, "::1/128".parse()?]
    } else {
        configured
            .allowed_clients
            .iter()
            .map(|range| range.parse())
            .collect::<Result<Vec<ipnet::IpNet>, _>>()?
    };
    let service_dns = conductor_api::dns::ServiceDnsConfig {
        listen: configured.listen,
        suffix: configured.suffix.clone(),
        overlay: match (
            configured.overlay_network_domain_id.as_deref(),
            configured.overlay_suffix.as_deref(),
        ) {
            (Some(domain_id), Some(suffix)) => Some(conductor_api::dns::OverlayDnsConfig {
                network_domain_id: NetworkDomainId::new(domain_id.to_owned())?,
                suffix: suffix.to_owned(),
            }),
            (None, None) => None,
            _ => {
                return Err("service DNS overlay_network_domain_id and overlay_suffix must be configured together".into());
            }
        },
        allowed_clients,
    };
    service_dns.validate().map_err(std::io::Error::other)?;
    Ok(Some(service_dns))
}

fn timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default()
}

struct LocalWorkerPort {
    worker: Arc<conductor_worker::WorkerService>,
    metadata: WorkerMetadata,
}

impl WorkerPort for LocalWorkerPort {
    fn descriptor(&self) -> conductor_core::WorkerDescriptor {
        self.worker.descriptor()
    }

    fn is_draining(&self) -> bool {
        self.worker.is_draining()
    }

    fn is_degraded(&self) -> bool {
        self.worker.is_degraded()
    }

    fn start(&self, command: StartRun) -> Result<conductor_core::RunObservation, WorkerPortError> {
        self.worker.start(command).map_err(map_worker_error)
    }

    fn inspect(
        &self,
        command: InspectRun,
    ) -> Result<Option<conductor_core::RunObservation>, WorkerPortError> {
        self.worker.inspect(command).map_err(map_worker_error)
    }

    fn stop(&self, command: StopRun) -> Result<conductor_core::RunObservation, WorkerPortError> {
        self.worker.stop(command).map_err(map_worker_error)
    }

    fn resolve(
        &self,
        command: ResolveRun,
    ) -> Result<conductor_core::RunObservation, WorkerPortError> {
        self.worker
            .resolve_unknown(command)
            .map_err(map_worker_error)
    }

    fn status(&self) -> Result<conductor_core::WorkerStatus, WorkerPortError> {
        let status = self.worker.status().map_err(map_worker_error)?;
        let inventory = status.inventory().ok_or(WorkerPortError::Unavailable)?;
        let domain_capacity = self.metadata.capacity().unwrap_or(inventory.allocatable());
        let last_seen_at_ms =
            i64::try_from(timestamp_ms()).map_err(|_| WorkerPortError::Unavailable)?;
        status
            .with_manager_metadata(
                self.metadata.resource_domain_id().clone(),
                domain_capacity,
                self.metadata.labels().clone(),
                last_seen_at_ms,
                true,
            )
            .and_then(|status| match self.metadata.service_network() {
                Some(network) => status.with_service_network(network.clone()),
                None => Some(status),
            })
            .ok_or(WorkerPortError::Unavailable)
    }

    fn read_logs(
        &self,
        command: ReadRunLogs,
    ) -> Result<conductor_core::RunLogChunk, WorkerPortError> {
        self.worker.read_logs(command).map_err(map_worker_error)
    }
}

fn worker_metadata(
    config: &Config,
    worker_id: &WorkerId,
    configured_domain_id: Option<&str>,
    labels: &BTreeMap<String, String>,
    service_network: Option<&ServiceNetworkConfig>,
) -> Result<WorkerMetadata, Box<dyn std::error::Error>> {
    let domain_id = WorkerId::new(
        configured_domain_id
            .unwrap_or(worker_id.as_str())
            .to_owned(),
    )?;
    let capacity = config
        .host_resource_domains
        .iter()
        .find(|domain| domain.id == domain_id.as_str())
        .map(|domain| {
            WorkerResourceCapacity::new(domain.cpu_millis, domain.memory_bytes)
                .ok_or("host resource domain capacity values must be positive")
        })
        .transpose()?;
    let metadata = WorkerMetadata::new(domain_id, capacity, labels.clone())
        .ok_or("worker labels exceed supported limits or contain invalid characters")?;
    let Some(network) = service_network else {
        return Ok(metadata);
    };
    let network_domain_id = WorkerId::new(network.network_domain_id.clone())?;
    let network = conductor_core::WorkerServiceNetwork::new(
        network_domain_id,
        network.port_range_start,
        network.port_range_end,
        network.advertised_address.clone(),
    )
    .ok_or("worker service network configuration is invalid")?;
    Ok(metadata.with_service_network(network))
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
    let worker_role = matches!(config.role, Role::Worker | Role::All);
    let manager_role = matches!(config.role, Role::Manager | Role::All);

    if manager_role
        && !(conductor_manager::MIN_COMPLETED_RUN_RETENTION
            ..=conductor_manager::MAX_COMPLETED_RUN_RETENTION)
            .contains(&config.completed_run_retention)
    {
        return Err("completed_run_retention is outside the supported range".into());
    }

    match config.role {
        Role::All => {
            if config.worker_transport_listen.is_some()
                || config.manager_identity_dir.is_some()
                || config.manager_server_name.is_some()
                || config.manager_endpoint.is_some()
                || config.worker_identity_file.is_some()
                || config.worker_overlay_network_domain_id.is_some()
                || config.worker_overlay_helper_binary.is_some()
                || config.manager_ca_file.is_some()
                || config.worker_enrollment_credential_file.is_some()
                || config.worker_transport_capacity.is_some()
                || !config.additional_workers.is_empty()
                || !config.host_resource_domains.is_empty()
            {
                return Err("standalone-role fields cannot be used with role 'all'".into());
            }
        }
        Role::Manager => {
            if config.worker_data_dir.is_some()
                || config.worker_concurrency.is_some()
                || config.manager_endpoint.is_some()
                || config.worker_identity_file.is_some()
                || config.worker_overlay_network_domain_id.is_some()
                || config.worker_overlay_helper_binary.is_some()
                || config.manager_ca_file.is_some()
                || config.worker_enrollment_credential_file.is_some()
            {
                return Err("worker-only fields cannot be used with role 'manager'".into());
            }
            if config.cgroup_v2.enabled || config.landlock.mode != LandlockModeConfig::Disabled {
                return Err(
                    "cgroup_v2 and landlock settings apply only to worker processes".into(),
                );
            }
        }
        Role::Worker => {
            if config.listen.is_some()
                || config.manager_data_dir.is_some()
                || config.api_access.is_some()
                || config.worker_transport_listen.is_some()
                || config.manager_identity_dir.is_some()
                || config.manager_server_name.is_some()
                || config.worker_transport_capacity.is_some()
                || !config.additional_workers.is_empty()
                || !config.host_resource_domains.is_empty()
                || config.worker_resource_domain_id.is_some()
                || !config.worker_labels.is_empty()
                || config.worker_service_network.is_some()
                || config.service_dns.is_some()
            {
                return Err("manager-only fields cannot be used with role 'worker'".into());
            }
            if config.worker_overlay_network_domain_id.is_some()
                != config.worker_overlay_helper_binary.is_some()
            {
                return Err(
                    "worker overlay network domain and helper binary must be configured together"
                        .into(),
                );
            }
            if let Some(domain_id) = &config.worker_overlay_network_domain_id {
                NetworkDomainId::new(domain_id.clone())?;
                let helper = required(
                    &config.worker_overlay_helper_binary,
                    "worker_overlay_helper_binary",
                )?;
                if !helper.is_absolute() {
                    return Err("worker_overlay_helper_binary must be an absolute path".into());
                }
            }
        }
    }

    if manager_role {
        if !conductor_core::valid_worker_labels(&config.worker_labels) {
            return Err(
                "worker_labels exceed supported limits or contain invalid characters".into(),
            );
        }
        if let Some(domain_id) = &config.worker_resource_domain_id {
            WorkerId::new(domain_id.clone())?;
        }
        let listen = required(&config.listen, "listen")?;
        if !listen.ip().is_loopback() {
            return Err(
                "HTTP API listener must use loopback; terminate TLS at a trusted proxy".into(),
            );
        }
        if config.api_access.is_none() {
            return Err(
                "configuration field 'api_access' is required for manager and all roles".into(),
            );
        }
        if let Some(ApiAccessConfig::OperatorTokenFile { token_file }) = &config.api_access
            && token_file.as_os_str().is_empty()
        {
            return Err("api_access.token_file must not be empty".into());
        }
        let manager_data_dir = required(&config.manager_data_dir, "manager_data_dir")?;
        if manager_data_dir.as_os_str().is_empty() {
            return Err("manager_data_dir must not be empty".into());
        }
    }

    if worker_role {
        let worker_data_dir = required(&config.worker_data_dir, "worker_data_dir")?;
        if worker_data_dir.as_os_str().is_empty() {
            return Err("worker_data_dir must not be empty".into());
        }
        let worker_id = required(&config.worker_id, "worker_id")?;
        if worker_id.is_empty()
            || worker_id.len() > 128
            || !worker_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(
                "worker_id must contain 1-128 ASCII letters, digits, '-', '_' or '.'".into(),
            );
        }
        let worker_concurrency = *required(&config.worker_concurrency, "worker_concurrency")?;
        if worker_concurrency == 0 || worker_concurrency > MAX_WORKER_CAPACITY {
            return Err("worker_concurrency must be between 1 and 64".into());
        }
    }

    if matches!(config.role, Role::Manager) {
        let transport_listen =
            required(&config.worker_transport_listen, "worker_transport_listen")?;
        if transport_listen.port() == 0 && transport_listen.ip().is_unspecified() {
            return Err("worker_transport_listen cannot use an ephemeral wildcard address".into());
        }
        let identity_dir = required(&config.manager_identity_dir, "manager_identity_dir")?;
        if identity_dir.as_os_str().is_empty() {
            return Err("manager_identity_dir must not be empty".into());
        }
        let server_name = required(&config.manager_server_name, "manager_server_name")?;
        if server_name.is_empty()
            || server_name.len() > 253
            || server_name.bytes().any(|b| b.is_ascii_control())
        {
            return Err("manager_server_name must be a valid non-empty DNS name".into());
        }
        let worker_id = required(&config.worker_id, "worker_id")?;
        WorkerId::new(worker_id.clone())?;
        let capacity = *required(
            &config.worker_transport_capacity,
            "worker_transport_capacity",
        )?;
        if capacity == 0 || capacity > MAX_WORKER_CAPACITY {
            return Err("worker_transport_capacity must be between 1 and 64".into());
        }
        let mut domain_capacities = BTreeMap::new();
        for domain in &config.host_resource_domains {
            let id = WorkerId::new(domain.id.clone())?;
            if domain.cpu_millis == 0 || domain.memory_bytes == 0 {
                return Err("host resource domain capacities must be positive".into());
            }
            if domain_capacities
                .insert(
                    id.as_str().to_owned(),
                    (domain.cpu_millis, domain.memory_bytes),
                )
                .is_some()
            {
                return Err("host resource domain IDs must be unique".into());
            }
        }
        let mut resource_domains = BTreeMap::<String, usize>::new();
        let mut worker_ids = BTreeSet::from([worker_id.as_str().to_owned()]);
        let primary_domain = WorkerId::new(
            config
                .worker_resource_domain_id
                .clone()
                .unwrap_or_else(|| worker_id.clone()),
        )?;
        *resource_domains
            .entry(primary_domain.as_str().to_owned())
            .or_default() += 1;
        for additional in &config.additional_workers {
            let additional_id = WorkerId::new(additional.worker_id.clone())?;
            if !worker_ids.insert(additional_id.as_str().to_owned()) {
                return Err("manager worker IDs must be unique".into());
            }
            if additional.capacity == 0 || additional.capacity > MAX_WORKER_CAPACITY {
                return Err("additional worker capacity must be between 1 and 64".into());
            }
            if !conductor_core::valid_worker_labels(&additional.labels) {
                return Err("additional worker labels exceed supported limits or contain invalid characters".into());
            }
            let domain_id = WorkerId::new(
                additional
                    .resource_domain_id
                    .clone()
                    .unwrap_or_else(|| additional_id.as_str().to_owned()),
            )?;
            *resource_domains
                .entry(domain_id.as_str().to_owned())
                .or_default() += 1;
        }
        for (domain_id, count) in &resource_domains {
            if *count > 1 && !domain_capacities.contains_key(domain_id) {
                return Err(format!(
                    "shared worker resource domain '{domain_id}' requires a host_resource_domains budget"
                ).into());
            }
        }
        for domain_id in domain_capacities.keys() {
            if !resource_domains.contains_key(domain_id) {
                return Err(format!(
                    "host resource domain '{domain_id}' is not assigned to a worker"
                )
                .into());
            }
        }
        if !(100..=120_000).contains(&config.request_timeout_ms) {
            return Err("request_timeout_ms must be between 100 and 120000".into());
        }
    }

    if matches!(config.role, Role::Worker) {
        let endpoint = required(&config.manager_endpoint, "manager_endpoint")?;
        let parsed =
            reqwest::Url::parse(endpoint).map_err(|_| "manager_endpoint must be an HTTPS URL")?;
        if parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.path() != "/"
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(
                "manager_endpoint must be an HTTPS origin without credentials, path, query, or fragment"
                    .into(),
            );
        }
        if required(&config.manager_ca_file, "manager_ca_file")?
            .as_os_str()
            .is_empty()
        {
            return Err("manager_ca_file must not be empty".into());
        }
        if required(&config.worker_identity_file, "worker_identity_file")?
            .as_os_str()
            .is_empty()
        {
            return Err("worker_identity_file must not be empty".into());
        }
    }

    if !matches!(config.role, Role::Manager) && !config.additional_workers.is_empty() {
        return Err("additional_workers is supported only for the manager role".into());
    }

    let mut service_ranges = BTreeMap::<String, Vec<(u16, u16)>>::new();
    let mut configured_networks = Vec::with_capacity(config.additional_workers.len() + 1);
    if let Some(network) = &config.worker_service_network {
        configured_networks.push(network);
    }
    configured_networks.extend(
        config
            .additional_workers
            .iter()
            .filter_map(|worker| worker.service_network.as_ref()),
    );
    for network in configured_networks {
        let id = WorkerId::new(network.network_domain_id.clone())?;
        if conductor_core::WorkerServiceNetwork::new(
            id.clone(),
            network.port_range_start,
            network.port_range_end,
            network.advertised_address.clone(),
        )
        .is_none()
        {
            return Err("worker service network configuration is invalid".into());
        }
        let ranges = service_ranges.entry(id.as_str().to_owned()).or_default();
        if ranges.iter().any(|(start, end)| {
            network.port_range_start <= *end && *start <= network.port_range_end
        }) {
            return Err(
                format!("worker service port ranges overlap for network domain '{id}'").into(),
            );
        }
        ranges.push((network.port_range_start, network.port_range_end));
    }

    if matches!(config.role, Role::All | Role::Worker)
        && !(100..=120_000).contains(&config.request_timeout_ms)
    {
        return Err("request_timeout_ms must be between 100 and 120000".into());
    }

    if config.cgroup_v2.enabled != config.cgroup_v2.delegated_root.is_some() {
        return Err(
            "cgroup_v2 requires enabled=true with delegated_root, or enabled=false without delegated_root".into(),
        );
    }
    if config.cgroup_v2.enabled != config.cgroup_v2.ownership_lock_dir.is_some() {
        return Err(
            "cgroup_v2 requires enabled=true with ownership_lock_dir, or enabled=false without ownership_lock_dir".into(),
        );
    }
    if config
        .cgroup_v2
        .ownership_lock_dir
        .as_ref()
        .is_some_and(|path| !path.is_absolute())
    {
        return Err("cgroup_v2.ownership_lock_dir must be an absolute path".into());
    }
    let maximums = &config.cgroup_v2.maximums;
    conductor_core::ResourceLimits::new(
        maximums.cpu_quota_us(),
        maximums.cpu_period_us(),
        maximums.memory_max_bytes(),
        maximums.memory_swap_max_bytes(),
        maximums.pids_max(),
    )?;
    if !config.cgroup_v2.enabled && !maximums.is_empty() {
        return Err("cgroup_v2.maximums requires cgroup_v2.enabled=true".into());
    }
    Ok(())
}

fn api_access_policy(
    config: &Config,
) -> Result<conductor_api::ApiAccessPolicy, Box<dyn std::error::Error>> {
    match required(&config.api_access, "api_access")? {
        ApiAccessConfig::Local => Ok(conductor_api::ApiAccessPolicy::LocalMode),
        ApiAccessConfig::OperatorTokenFile { token_file } => {
            let configured_path = if token_file.is_absolute() {
                token_file.clone()
            } else {
                config
                    .source_config_path
                    .parent()
                    .unwrap_or_else(|| std::path::Path::new("."))
                    .join(token_file)
            };
            #[cfg(not(unix))]
            {
                let path_metadata = fs::symlink_metadata(&configured_path)
                    .map_err(|_| "operator token file could not be accessed securely")?;
                if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
                    return Err(
                        "operator token path must be a regular file, not a symbolic link".into(),
                    );
                }
            }
            let token_file = state_lock::open_secret_file_no_follow(&configured_path)
                .map_err(|_| "operator token file could not be accessed securely")?;
            let metadata = token_file
                .metadata()
                .map_err(|_| "operator token file could not be accessed securely")?;
            if !metadata.is_file() {
                return Err("operator token path must be a regular file".into());
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o077 != 0 {
                    return Err(
                        "operator token file permissions must deny group and other access".into(),
                    );
                }
            }
            let mut token = zeroize::Zeroizing::new(Vec::with_capacity(44));
            token_file
                .take(44)
                .read_to_end(&mut token)
                .map_err(|_| "operator token file could not be accessed securely")?;
            let authenticator = conductor_api::BearerTokenAuthenticator::new(&token)
                .map_err(|_| "operator token file contains an invalid token")?;
            Ok(conductor_api::ApiAccessPolicy::AuthenticatedOperator(
                Arc::new(authenticator),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::panic::catch_unwind;
    use std::path::PathBuf;

    use super::{
        CgroupV2Config, Config, HostResourceDomainConfig, RemoteWorkerConfig, Role,
        default_request_timeout_ms, validate,
    };

    fn config(listen: &str) -> Config {
        Config {
            role: Role::All,
            listen: Some(listen.parse::<SocketAddr>().unwrap()),
            manager_data_dir: Some(PathBuf::from("manager")),
            worker_data_dir: Some(PathBuf::from("worker")),
            worker_id: Some("test-worker".to_owned()),
            worker_concurrency: Some(1),
            api_access: Some(super::ApiAccessConfig::Local),
            worker_transport_listen: None,
            manager_identity_dir: None,
            manager_server_name: None,
            manager_endpoint: None,
            worker_identity_file: None,
            worker_overlay_network_domain_id: None,
            worker_overlay_helper_binary: None,
            manager_ca_file: None,
            worker_enrollment_credential_file: None,
            worker_transport_capacity: None,
            additional_workers: Vec::new(),
            host_resource_domains: Vec::new(),
            worker_resource_domain_id: None,
            worker_labels: BTreeMap::new(),
            worker_service_network: None,
            service_dns: None,
            log_max_bytes_per_stream: conductor_worker::DEFAULT_LOG_MAX_BYTES_PER_STREAM,
            log_retention_runs: conductor_worker::DEFAULT_LOG_RETENTION_RUNS,
            completed_run_retention: conductor_manager::DEFAULT_COMPLETED_RUN_RETENTION,
            request_timeout_ms: default_request_timeout_ms(),
            cgroup_v2: CgroupV2Config::default(),
            landlock: super::LandlockConfig::default(),
            source_config_path: PathBuf::new(),
        }
    }

    fn manager_config() -> Config {
        let mut config = config("127.0.0.1:8080");
        config.role = Role::Manager;
        config.worker_data_dir = None;
        config.worker_concurrency = None;
        config.worker_transport_listen = Some("127.0.0.1:9443".parse().unwrap());
        config.manager_identity_dir = Some(PathBuf::from("identity"));
        config.manager_server_name = Some("localhost".to_owned());
        config.worker_transport_capacity = Some(2);
        config
    }

    #[test]
    fn completed_run_retention_has_a_bounded_default_and_configured_range() {
        let deserialized: Config = serde_json::from_str(
            r#"{"role":"all","listen":"127.0.0.1:8080","api_access":{"mode":"local"}}"#,
        )
        .expect("all-role configuration uses retention default");
        assert_eq!(
            deserialized.completed_run_retention,
            conductor_manager::DEFAULT_COMPLETED_RUN_RETENTION
        );

        let mut config = config("127.0.0.1:8080");
        validate(&config).expect("default retention is valid");

        config.completed_run_retention = conductor_manager::MIN_COMPLETED_RUN_RETENTION;
        validate(&config).expect("minimum retention is valid");
        config.completed_run_retention = conductor_manager::MAX_COMPLETED_RUN_RETENTION;
        validate(&config).expect("maximum retention is valid");

        config.completed_run_retention = 0;
        assert!(validate(&config).is_err());
        config.completed_run_retention = conductor_manager::MAX_COMPLETED_RUN_RETENTION + 1;
        assert!(validate(&config).is_err());
    }

    #[test]
    fn service_dns_defaults_to_loopback_and_requires_explicit_remote_ranges() {
        let local: super::ServiceDnsListenerConfig =
            serde_json::from_str(r#"{"listen":"127.0.0.1:8053"}"#).expect("valid local DNS config");
        assert_eq!(local.suffix, "conductor.internal");
        let converted = super::service_dns_config(Some(&local))
            .expect("loopback-only access defaults")
            .expect("configured DNS listener");
        assert_eq!(converted.allowed_clients.len(), 2);

        let overlay: super::ServiceDnsListenerConfig = serde_json::from_str(
            r#"{"listen":"127.0.0.1:8053","overlayNetworkDomainId":"workers-east","overlaySuffix":"overlay.internal"}"#,
        )
        .expect("overlay DNS fields are accepted");
        let converted = super::service_dns_config(Some(&overlay))
            .expect("overlay DNS config converts")
            .expect("configured DNS listener");
        assert_eq!(
            converted.overlay.expect("overlay namespace").suffix,
            "overlay.internal"
        );

        let incomplete_overlay: super::ServiceDnsListenerConfig = serde_json::from_str(
            r#"{"listen":"127.0.0.1:8053","overlayNetworkDomainId":"workers-east"}"#,
        )
        .expect("partial overlay config deserializes strictly");
        assert!(super::service_dns_config(Some(&incomplete_overlay)).is_err());

        let remote: super::ServiceDnsListenerConfig =
            serde_json::from_str(r#"{"listen":"192.0.2.53:53"}"#)
                .expect("well-formed remote listener config");
        assert!(super::service_dns_config(Some(&remote)).is_err());
        let remote: super::ServiceDnsListenerConfig =
            serde_json::from_str(r#"{"listen":"192.0.2.53:53","allowedClients":["192.0.2.0/24"]}"#)
                .expect("remote listener with explicit CIDR");
        assert_eq!(
            super::service_dns_config(Some(&remote))
                .expect("valid remote DNS config")
                .expect("configured DNS listener")
                .allowed_clients
                .len(),
            1
        );
    }

    #[test]
    fn config_file_relative_paths_resolve_from_the_config_directory() {
        let mut config = config("127.0.0.1:8080");
        config.role = super::Role::Manager;
        config.source_config_path = PathBuf::from("/tmp/conductor-config/agent.json");
        config.manager_data_dir = Some(PathBuf::from("manager"));
        config.worker_data_dir = Some(PathBuf::from("worker"));
        config.manager_identity_dir = Some(PathBuf::from("manager-identity"));
        config.manager_ca_file = Some(PathBuf::from("ca/manager.pem"));
        config.worker_identity_file = Some(PathBuf::from("worker/identity.pem"));
        config.worker_enrollment_credential_file = Some(PathBuf::from("worker/enrollment"));
        config.api_access = Some(super::ApiAccessConfig::OperatorTokenFile {
            token_file: PathBuf::from("operator-token"),
        });
        config.cgroup_v2.delegated_root = Some(PathBuf::from("cgroup"));
        config.cgroup_v2.ownership_lock_dir = Some(PathBuf::from("cgroup-locks"));
        config.landlock.grants.push(super::LandlockGrantConfig {
            id: "workdir".to_owned(),
            path: PathBuf::from("workdir"),
            read: true,
            write: true,
            execute: false,
        });
        config
            .landlock
            .protected_paths
            .push(PathBuf::from("private"));

        super::resolve_config_paths(&mut config, super::Role::Manager, false);

        let base = PathBuf::from("/tmp/conductor-config");
        assert_eq!(config.manager_data_dir, Some(base.join("manager")));
        assert_eq!(config.worker_data_dir, Some(base.join("worker")));
        assert_eq!(
            config.manager_identity_dir,
            Some(base.join("manager-identity"))
        );
        assert_eq!(config.manager_ca_file, Some(base.join("ca/manager.pem")));
        assert_eq!(
            config.worker_identity_file,
            Some(base.join("worker/identity.pem"))
        );
        assert_eq!(
            config.worker_enrollment_credential_file,
            Some(base.join("worker/enrollment"))
        );
        assert_eq!(
            config.cgroup_v2.delegated_root,
            Some(PathBuf::from("cgroup"))
        );
        assert_eq!(
            config.cgroup_v2.ownership_lock_dir,
            Some(PathBuf::from("cgroup-locks"))
        );
        assert_eq!(config.landlock.grants[0].path, PathBuf::from("workdir"));
        assert_eq!(config.landlock.protected_paths[0], PathBuf::from("private"));
        assert!(matches!(
            config.api_access,
            Some(super::ApiAccessConfig::OperatorTokenFile { token_file })
                if token_file.to_str() == Some("operator-token")
        ));
    }

    #[test]
    fn combined_mode_preserves_legacy_working_directory_state_paths() {
        let mut all_mode = config("127.0.0.1:8080");
        all_mode.source_config_path = PathBuf::from("/tmp/conductor-config/agent.json");
        all_mode.manager_data_dir = Some(PathBuf::from("state/manager"));
        all_mode.worker_data_dir = Some(PathBuf::from("state/worker"));
        all_mode.manager_identity_dir = Some(PathBuf::from("identity"));

        super::resolve_config_paths(&mut all_mode, super::Role::All, false);

        assert_eq!(
            all_mode.manager_data_dir,
            Some(PathBuf::from("state/manager"))
        );
        assert_eq!(
            all_mode.worker_data_dir,
            Some(PathBuf::from("state/worker"))
        );
        assert_eq!(
            all_mode.manager_identity_dir,
            Some(PathBuf::from("/tmp/conductor-config/identity"))
        );

        let mut overridden = config("127.0.0.1:8080");
        overridden.source_config_path = PathBuf::from("/tmp/conductor-config/agent.json");
        overridden.manager_data_dir = Some(PathBuf::from("state/manager"));
        overridden.manager_identity_dir = Some(PathBuf::from("identity"));
        super::resolve_config_paths(&mut overridden, super::Role::Manager, true);
        assert_eq!(
            overridden.manager_data_dir,
            Some(PathBuf::from("state/manager"))
        );
        assert_eq!(
            overridden.manager_identity_dir,
            Some(PathBuf::from("/tmp/conductor-config/identity"))
        );
    }

    #[test]
    fn redundant_standalone_role_override_keeps_config_relative_state_paths() {
        let configured_role = Role::Manager;
        let role_override = Some(Role::Manager);
        assert!(!super::preserve_legacy_paths(
            configured_role,
            role_override
        ));

        let mut manager = config("127.0.0.1:8080");
        manager.role = configured_role;
        manager.source_config_path = PathBuf::from("/tmp/conductor-config/agent.json");
        manager.manager_data_dir = Some(PathBuf::from("state/manager"));
        super::resolve_config_paths(
            &mut manager,
            configured_role,
            super::preserve_legacy_paths(configured_role, role_override),
        );

        assert_eq!(
            manager.manager_data_dir,
            Some(PathBuf::from("/tmp/conductor-config/state/manager"))
        );
    }

    #[test]
    fn configuration_accepts_ipv6_loopback_and_rejects_public_listener_without_binding() {
        assert!(validate(&config("[::1]:8080")).is_ok());
        assert!(validate(&config("192.0.2.1:8080")).is_err());
        let mut remote_operator = config("192.0.2.1:8080");
        remote_operator.api_access = Some(super::ApiAccessConfig::OperatorTokenFile {
            token_file: PathBuf::from("operator-token"),
        });
        assert!(validate(&remote_operator).is_err());
        let mut excessive_capacity = config("127.0.0.1:8080");
        excessive_capacity.worker_concurrency = Some(conductor_core::MAX_WORKER_CAPACITY + 1);
        assert!(validate(&excessive_capacity).is_err());
    }

    #[test]
    fn cgroup_configuration_requires_an_absolute_shared_ownership_lock_directory() {
        let mut config = config("127.0.0.1:8080");
        config.cgroup_v2.enabled = true;
        config.cgroup_v2.delegated_root = Some(PathBuf::from("/sys/fs/cgroup/worker"));
        assert!(validate(&config).is_err());

        config.cgroup_v2.ownership_lock_dir = Some(PathBuf::from("relative/locks"));
        assert!(validate(&config).is_err());

        config.cgroup_v2.ownership_lock_dir =
            Some(PathBuf::from("/var/lib/conductor/cgroup-locks"));
        assert!(validate(&config).is_ok());

        config.cgroup_v2.enabled = false;
        config.cgroup_v2.delegated_root = None;
        assert!(validate(&config).is_err());
    }

    #[test]
    fn shared_worker_domains_require_one_configured_aggregate_budget() {
        let mut config = manager_config();
        config.worker_resource_domain_id = Some("host-a".to_owned());
        config.additional_workers.push(RemoteWorkerConfig {
            worker_id: "worker-two".to_owned(),
            capacity: 2,
            resource_domain_id: Some("host-a".to_owned()),
            labels: BTreeMap::new(),
            service_network: None,
        });
        let error = validate(&config).expect_err("shared host requires a configured budget");
        assert!(error.to_string().contains("host_resource_domains"));

        config.host_resource_domains.push(HostResourceDomainConfig {
            id: "host-a".to_owned(),
            cpu_millis: 1_000,
            memory_bytes: 1_000_000,
        });
        validate(&config).expect("shared host has a configured aggregate budget");
    }

    #[test]
    fn shared_worker_network_domains_reject_overlapping_port_ranges() {
        let mut config = manager_config();
        config.worker_service_network = Some(super::ServiceNetworkConfig {
            network_domain_id: "host-network".to_owned(),
            port_range_start: 31_000,
            port_range_end: 31_999,
            advertised_address: "192.0.2.10".to_owned(),
        });
        config.additional_workers.push(RemoteWorkerConfig {
            worker_id: "worker-two".to_owned(),
            capacity: 2,
            resource_domain_id: None,
            labels: BTreeMap::new(),
            service_network: Some(super::ServiceNetworkConfig {
                network_domain_id: "host-network".to_owned(),
                port_range_start: 31_999,
                port_range_end: 32_999,
                advertised_address: "192.0.2.10".to_owned(),
            }),
        });
        assert!(
            validate(&config)
                .expect_err("overlapping worker network ranges must fail")
                .to_string()
                .contains("port ranges overlap")
        );
    }

    #[test]
    fn worker_labels_and_host_domain_configuration_are_validated() {
        let mut config = manager_config();
        config
            .worker_labels
            .insert("region".to_owned(), "bad\nvalue".to_owned());
        assert!(validate(&config).is_err());

        config.worker_labels.clear();
        config.host_resource_domains.push(HostResourceDomainConfig {
            id: "unassigned-host".to_owned(),
            cpu_millis: 1_000,
            memory_bytes: 1_000_000,
        });
        assert!(validate(&config).is_err());

        config.host_resource_domains[0].cpu_millis = 0;
        assert!(validate(&config).is_err());
    }

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
