mod state_lock;

use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use clap::{Parser, Subcommand, ValueEnum, error::ErrorKind};
use conductor_core::{
    InspectRun, MAX_WORKER_CAPACITY, ProtocolOffer, ProtocolVersionRange, ReadRunLogs, ResolveRun,
    StartRun, StopRun, WorkerDescriptor, WorkerId,
};
use conductor_manager::{WorkerPort, WorkerPortError};
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
    manager_ca_file: Option<PathBuf>,
    #[serde(default)]
    worker_enrollment_credential_file: Option<PathBuf>,
    #[serde(default)]
    worker_transport_capacity: Option<usize>,
    #[serde(default)]
    additional_workers: Vec<RemoteWorkerConfig>,
    #[serde(default = "default_log_max_bytes_per_stream")]
    log_max_bytes_per_stream: u64,
    #[serde(default = "default_log_retention_runs")]
    log_retention_runs: usize,
    #[serde(default = "default_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default)]
    cgroup_v2: CgroupV2Config,
    #[serde(default)]
    landlock: LandlockConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteWorkerConfig {
    worker_id: String,
    capacity: usize,
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
        Commands::EnrollWorker {
            manager_data_dir,
            identity_dir,
            worker_id,
            valid_for_seconds,
            output,
        } => {
            let worker_id = WorkerId::new(worker_id)?;
            let manager_data_dir = state_lock::prepare_directory(&manager_data_dir)?;
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
    let _state_locks = StateLocks::acquire(manager_data_dir, worker_data_dir)?;
    let manager = Arc::new(conductor_manager::JobService::open_sqlite(
        manager_data_dir.join("manager.sqlite3"),
    )?);
    let worker = Arc::new(conductor_worker::WorkerService::open_with_log_policy(
        worker_data_dir,
        worker_id,
        worker_concurrency,
        conductor_worker::LogPolicy {
            max_bytes_per_stream: config.log_max_bytes_per_stream,
            retention_runs: config.log_retention_runs,
        },
        process_backend(&config)?,
    )?);
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    close_fds::set_fds_cloexec_threadsafe(3, &[]);
    let worker_port: Arc<dyn WorkerPort> = Arc::new(LocalWorkerPort(Arc::clone(&worker)));
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
    let serve_result = runtime.block_on(async move {
        let listener = tokio::net::TcpListener::from_std(listener)?;
        conductor_api::serve_with_access_policy(
            listener,
            shutdown,
            manager,
            worker_port,
            Duration::from_millis(config.request_timeout_ms),
            api_access,
        )
        .await
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
    let api_access = api_access_policy(&config)?;
    let _state_locks = StateLocks::acquire_manager(manager_data_dir)
        .map_err(|error| format!("manager state lock could not be acquired: {error}"))?;

    let repository = Arc::new(
        conductor_manager::SqliteJobRepository::open(manager_data_dir.join("manager.sqlite3"))
            .map_err(|error| format!("manager repository could not be opened: {error}"))?,
    );
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
        ]),
        BTreeSet::new(),
    )?;
    let mut worker_ports = Vec::with_capacity(config.additional_workers.len() + 1);
    worker_ports.push(hub.register_worker(descriptor, offer.clone())?);
    for additional in &config.additional_workers {
        worker_ports.push(hub.register_worker(
            WorkerDescriptor::new(
                WorkerId::new(additional.worker_id.clone())?,
                additional.capacity,
                true,
            ),
            offer.clone(),
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
            workers_ready,
        ));
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
            }
        };
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
        worker_id,
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
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = Arc::clone(&shutdown);
    let signal_worker = Arc::clone(&worker);
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
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
    let run_result = runtime.block_on(client.run(
        Arc::clone(&worker),
        ProtocolOffer::new(
            ProtocolVersionRange::current(),
            BTreeSet::from([
                conductor_core::ProtocolCapability::OrderedReports,
                conductor_core::ProtocolCapability::WorkerIncarnations,
            ]),
            BTreeSet::new(),
        )?,
        shutdown_receiver,
    ));
    let worker_shutdown = worker.shutdown();
    run_result?;
    worker_shutdown?;
    Ok(ExitCode::SUCCESS)
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

fn timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default()
}

struct LocalWorkerPort(Arc<conductor_worker::WorkerService>);

impl WorkerPort for LocalWorkerPort {
    fn descriptor(&self) -> conductor_core::WorkerDescriptor {
        self.0.descriptor()
    }

    fn is_draining(&self) -> bool {
        self.0.is_draining()
    }

    fn is_degraded(&self) -> bool {
        self.0.is_degraded()
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

    fn resolve(
        &self,
        command: ResolveRun,
    ) -> Result<conductor_core::RunObservation, WorkerPortError> {
        self.0.resolve_unknown(command).map_err(map_worker_error)
    }

    fn status(&self) -> Result<conductor_core::WorkerStatus, WorkerPortError> {
        self.0.status().map_err(map_worker_error)
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
    let worker_role = matches!(config.role, Role::Worker | Role::All);
    let manager_role = matches!(config.role, Role::Manager | Role::All);

    match config.role {
        Role::All => {
            if config.worker_transport_listen.is_some()
                || config.manager_identity_dir.is_some()
                || config.manager_server_name.is_some()
                || config.manager_endpoint.is_some()
                || config.worker_identity_file.is_some()
                || config.manager_ca_file.is_some()
                || config.worker_enrollment_credential_file.is_some()
                || config.worker_transport_capacity.is_some()
                || !config.additional_workers.is_empty()
            {
                return Err("standalone-role fields cannot be used with role 'all'".into());
            }
        }
        Role::Manager => {
            if config.worker_data_dir.is_some()
                || config.worker_concurrency.is_some()
                || config.manager_endpoint.is_some()
                || config.worker_identity_file.is_some()
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
            {
                return Err("manager-only fields cannot be used with role 'worker'".into());
            }
        }
    }

    if manager_role {
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
        let mut worker_ids = BTreeSet::from([worker_id.as_str().to_owned()]);
        for additional in &config.additional_workers {
            let additional_id = WorkerId::new(additional.worker_id.clone())?;
            if !worker_ids.insert(additional_id.as_str().to_owned()) {
                return Err("manager worker IDs must be unique".into());
            }
            if additional.capacity == 0 || additional.capacity > MAX_WORKER_CAPACITY {
                return Err("additional worker capacity must be between 1 and 64".into());
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
    use std::net::SocketAddr;
    use std::panic::catch_unwind;
    use std::path::PathBuf;

    use super::{CgroupV2Config, Config, Role, default_request_timeout_ms, validate};

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
            manager_ca_file: None,
            worker_enrollment_credential_file: None,
            worker_transport_capacity: None,
            additional_workers: Vec::new(),
            log_max_bytes_per_stream: conductor_worker::DEFAULT_LOG_MAX_BYTES_PER_STREAM,
            log_retention_runs: conductor_worker::DEFAULT_LOG_RETENTION_RUNS,
            request_timeout_ms: default_request_timeout_ms(),
            cgroup_v2: CgroupV2Config::default(),
            landlock: super::LandlockConfig::default(),
            source_config_path: PathBuf::new(),
        }
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
