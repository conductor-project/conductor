use std::collections::BTreeSet;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use conductor_core::{
    JobId, JobSpec, LogStream, ProtocolOffer, ProtocolVersionRange, ReadRunLogs, RunId, RunSpec,
    RunState, StartRun, WorkerDescriptor, WorkerId, WorkerInventoryPage, WorkerStatus,
};
use conductor_manager::{SqliteJobRepository, WorkerPort};
use conductor_process::HostProcessBackend;
use conductor_transport::{
    CertificateAuthority, IssuedWorkerIdentity, WorkerEnrollmentService, WorkerKeyMaterial,
    WorkerPollRequest, WorkerTransportClient, WorkerTransportHub,
};
use conductor_worker::WorkerService;
use serde::{Deserialize, Serialize};

const HELPER_CONFIG_ENV: &str = "CONDUCTOR_MULTIPROCESS_WORKER_CONFIG";
const READINESS_TIMEOUT: Duration = Duration::from_secs(8);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "conductor-macos-workers-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create isolated test directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                .expect("restrict test directory");
        }
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkerHelperConfig {
    worker_id: String,
    state_dir: PathBuf,
    identity_pem: PathBuf,
    ca_pem: PathBuf,
    manager_endpoint: String,
    ready_file: PathBuf,
    stop_file: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkerReady {
    process_id: u32,
    listener: String,
    worker_id: String,
    state_dir: PathBuf,
}

#[derive(Clone)]
struct WorkerHealth {
    worker_id: String,
    process_id: u32,
    state_dir: PathBuf,
}

async fn worker_health(State(state): State<WorkerHealth>) -> Json<WorkerReady> {
    Json(WorkerReady {
        process_id: state.process_id,
        listener: String::new(),
        worker_id: state.worker_id,
        state_dir: state.state_dir,
    })
}

#[test]
fn worker_helper_process() {
    let Ok(config_path) = std::env::var(HELPER_CONFIG_ENV) else {
        return;
    };
    worker_helper_main(Path::new(&config_path));
}

fn worker_helper_main(config_path: &Path) {
    let config: WorkerHelperConfig =
        serde_json::from_slice(&fs::read(config_path).expect("read worker helper config"))
            .expect("decode worker helper config");
    fs::create_dir_all(&config.state_dir).expect("create worker state directory");
    let worker_id = WorkerId::new(config.worker_id.clone()).expect("valid worker identity");
    let identity = fs::read(&config.identity_pem).expect("read worker client identity");
    let ca = fs::read(&config.ca_pem).expect("read manager CA");
    let worker = Arc::new(
        WorkerService::open(
            &config.state_dir,
            worker_id,
            2,
            Arc::new(HostProcessBackend::default()),
        )
        .expect("open worker persistence and process backend"),
    );
    let offer = protocol_offer();
    let client = Arc::new(
        WorkerTransportClient::from_pem_identity_with_root(
            &config.manager_endpoint,
            &identity,
            &ca,
        )
        .expect("construct authenticated HTTPS worker client"),
    );
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind isolated worker health listener");
    listener
        .set_nonblocking(true)
        .expect("make worker listener nonblocking");
    let listener_address = listener.local_addr().expect("read worker listener address");
    let process_id = std::process::id();
    let health = Router::new()
        .route("/healthz", get(worker_health))
        .with_state(WorkerHealth {
            worker_id: config.worker_id.clone(),
            process_id,
            state_dir: config.state_dir.clone(),
        });
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build worker helper runtime");
    runtime.block_on(async {
        let listener =
            tokio::net::TcpListener::from_std(listener).expect("adopt worker health listener");
        let health_task = tokio::spawn(async move {
            axum::serve(listener, health)
                .await
                .expect("serve worker health")
        });
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        let client_task =
            tokio::spawn(Arc::clone(&client).run(Arc::clone(&worker), offer, receiver));
        fs::write(
            &config.ready_file,
            serde_json::to_vec(&WorkerReady {
                process_id,
                listener: listener_address.to_string(),
                worker_id: config.worker_id.clone(),
                state_dir: config.state_dir.clone(),
            })
            .expect("encode worker readiness"),
        )
        .expect("publish worker readiness");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if config.stop_file.exists() {
                break;
            }
            if client_task.is_finished() {
                panic!("worker transport client exited before shutdown");
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "worker stop deadline expired"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let _ = shutdown.send(true);
        client_task
            .await
            .expect("join worker transport client")
            .expect("graceful worker transport shutdown");
        health_task.abort();
    });
    worker.shutdown().expect("gracefully stop native worker");
}

struct WorkerProcess(Child);

impl Drop for WorkerProcess {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_manager_authenticates_and_runs_on_two_native_worker_processes() {
    let directory = TempDir::new();
    let manager_state = directory.0.join("manager-state");
    fs::create_dir_all(&manager_state).expect("create manager state directory");
    let authority = Arc::new(
        CertificateAuthority::open_or_create(directory.0.join("authority"))
            .expect("open manager worker CA"),
    );
    let repository = Arc::new(
        SqliteJobRepository::open(manager_state.join("manager.sqlite3"))
            .expect("open manager identity store"),
    );
    let enrollment = Arc::new(WorkerEnrollmentService::new(
        Arc::clone(&repository),
        Arc::clone(&authority),
    ));
    let hub = WorkerTransportHub::default();
    let offer = protocol_offer();

    let workers = ["mac-worker-a", "mac-worker-b"];
    let mut remote_ports = Vec::new();
    let mut helper_processes = Vec::new();
    let mut helper_pids = Vec::new();
    let mut helper_configs = Vec::new();
    for worker_name in workers {
        let worker_id = WorkerId::new(worker_name).expect("worker id");
        let descriptor = WorkerDescriptor::new(worker_id.clone(), 2, true).with_incarnation(1);
        let remote = hub
            .register_worker(descriptor, offer.clone())
            .expect("register remote worker session");
        let worker_directory = directory.0.join(worker_name);
        fs::create_dir_all(&worker_directory).expect("create worker directory");
        let credential = enrollment
            .create_credential(&worker_id, now_ms() + 60_000)
            .expect("issue one-time worker enrollment credential");
        let worker_key = WorkerKeyMaterial::generate().expect("generate worker key");
        let identity: IssuedWorkerIdentity = enrollment
            .enroll(&credential, worker_key.certificate_signing_request_pem())
            .expect("enroll worker into manager identity store");
        assert_eq!(identity.worker_id, worker_id);
        let identity_path = worker_directory.join("worker-identity.pem");
        write_private_file(
            &identity_path,
            format!(
                "{}{}",
                identity.certificate_pem,
                worker_key.private_key_pem()
            )
            .as_bytes(),
        );
        let ca_path = worker_directory.join("manager-ca.pem");
        fs::write(&ca_path, authority.certificate_pem()).expect("write CA trust anchor");
        helper_configs.push(WorkerHelperConfig {
            worker_id: worker_name.to_owned(),
            state_dir: worker_directory.join("state"),
            identity_pem: identity_path,
            ca_pem: ca_path,
            manager_endpoint: String::new(),
            ready_file: worker_directory.join("ready.json"),
            stop_file: worker_directory.join("stop"),
        });
        remote_ports.push(remote);
    }

    let server_identity = authority
        .issue_server_identity("localhost")
        .expect("issue manager TLS identity");
    let acceptor = authority
        .mtls_acceptor(
            &server_identity.certificate_pem,
            server_identity.private_key_pem(),
        )
        .expect("configure manager mTLS listener");
    let manager_listener = TcpListener::bind("127.0.0.1:0").expect("bind manager listener");
    manager_listener
        .set_nonblocking(true)
        .expect("make manager listener nonblocking");
    let manager_address = manager_listener.local_addr().expect("manager address");
    let server = axum_server::from_tcp(manager_listener)
        .expect("wrap manager listener")
        .acceptor(acceptor)
        .serve(enrollment.router_with_hub(&hub).into_make_service());
    let _server_task = AbortOnDrop(tokio::spawn(server));
    let manager_endpoint = format!("https://localhost:{}/", manager_address.port());

    for config in &mut helper_configs {
        config.manager_endpoint.clone_from(&manager_endpoint);
        let config_path = config
            .state_dir
            .parent()
            .expect("worker root")
            .join("helper.json");
        fs::write(
            &config_path,
            serde_json::to_vec(config).expect("encode helper config"),
        )
        .expect("write helper config");
        let child = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "worker_helper_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(HELPER_CONFIG_ENV, &config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start native worker helper process");
        helper_pids.push(child.id());
        helper_processes.push(WorkerProcess(child));
    }

    let ready = wait_for_workers(&remote_ports, &helper_configs).await;
    assert_eq!(ready.len(), 2);
    assert_ne!(ready[0].process_id, ready[1].process_id);
    assert_ne!(ready[0].worker_id, ready[1].worker_id);
    assert_ne!(ready[0].state_dir, ready[1].state_dir);
    assert_ne!(ready[0].listener, ready[1].listener);
    assert!(
        ready
            .iter()
            .all(|worker| worker.process_id != std::process::id())
    );
    assert_eq!(
        ready
            .iter()
            .map(|worker| worker.process_id)
            .collect::<Vec<_>>(),
        helper_pids,
        "reported worker PIDs must identify the spawned helper processes"
    );

    inject_authenticated_worker_identity_mismatch(
        &helper_configs[0],
        &helper_configs[1],
        &manager_endpoint,
    )
    .await;

    let worker_id = WorkerId::new(&ready[1].worker_id).expect("target worker id");
    let run_id = RunId::new("macos-native-run").expect("run id");
    let job = JobSpec::new(
        JobId::new("macos-native-job").expect("job id"),
        vec![
            "/usr/bin/printf".to_owned(),
            "native-worker-ok\n".to_owned(),
        ],
        directory.0.clone(),
        Default::default(),
        false,
        Some(5_000),
        100,
    )
    .expect("portable native job");
    let spec = RunSpec::new(run_id.clone(), worker_id, job);
    let port = Arc::clone(&remote_ports[1]);
    let started = tokio::task::spawn_blocking(move || port.start(StartRun::new(spec)))
        .await
        .expect("join remote worker start")
        .expect("worker accepted native process");
    assert_eq!(started.worker_id().as_str(), ready[1].worker_id);

    let terminal = wait_for_terminal(&remote_ports[1], run_id.clone()).await;
    assert_eq!(terminal.state(), RunState::Succeeded, "{terminal:?}");
    let port = Arc::clone(&remote_ports[1]);
    let log = tokio::task::spawn_blocking(move || {
        port.read_logs(ReadRunLogs::new(run_id, LogStream::Stdout, 0, 128))
    })
    .await
    .expect("join log retrieval")
    .expect("retrieve native worker output");
    assert_eq!(log.data(), b"native-worker-ok\n");

    for config in &helper_configs {
        fs::write(&config.stop_file, b"stop").expect("request worker shutdown");
    }
    stop_helpers(&mut helper_processes).await;
    assert!(
        ready
            .iter()
            .all(|worker| worker.state_dir.join("worker.sqlite3").is_file())
    );
}

async fn wait_for_workers(
    ports: &[Arc<conductor_transport::RemoteWorkerPort>],
    configs: &[WorkerHelperConfig],
) -> Vec<WorkerReady> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .expect("build local health client");
    let deadline = Instant::now() + READINESS_TIMEOUT;
    loop {
        let mut ready = Vec::new();
        for config in configs {
            if let Ok(bytes) = fs::read(&config.ready_file)
                && let Ok(worker) = serde_json::from_slice::<WorkerReady>(&bytes)
            {
                if worker.worker_id != config.worker_id
                    || worker.state_dir != config.state_dir
                    || worker.process_id == 0
                {
                    panic!("worker readiness identity/state mismatch: {worker:?}");
                }
                if let Ok(response) = client
                    .get(format!("http://{}/healthz", worker.listener))
                    .send()
                    .await
                    && response.status().is_success()
                {
                    ready.push(worker);
                }
            }
        }
        let authenticated = ports.iter().all(|port| {
            port.status()
                .is_ok_and(|status| status.available() && !status.degraded())
        });
        if ready.len() == configs.len() && authenticated {
            return ready;
        }
        assert!(
            Instant::now() < deadline,
            "workers did not become ready before deadline"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn inject_authenticated_worker_identity_mismatch(
    first: &WorkerHelperConfig,
    second: &WorkerHelperConfig,
    manager_endpoint: &str,
) {
    let identity = fs::read(&first.identity_pem).expect("first worker identity");
    let ca = fs::read(&first.ca_pem).expect("CA trust anchor");
    let identity = reqwest::Identity::from_pem(&identity).expect("parse first worker identity");
    let root = reqwest::Certificate::from_pem(&ca).expect("parse manager CA");
    let client = reqwest::Client::builder()
        .https_only(true)
        .identity(identity)
        .add_root_certificate(root)
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build authenticated fault-injection client");
    let first_id = WorkerId::new(&first.worker_id).expect("first id");
    let second_id = WorkerId::new(&second.worker_id).expect("second id");
    let descriptor = WorkerDescriptor::new(first_id, 2, true).with_incarnation(1);
    let request = WorkerPollRequest {
        worker_id: second_id,
        worker_incarnation: 1,
        status: WorkerStatus::new(descriptor, 0, 0, true, false, now_ms()),
        inventory_page: WorkerInventoryPage {
            worker_id: WorkerId::new(&first.worker_id).expect("first id"),
            worker_incarnation: 1,
            snapshot_generation: 0,
            after_sequence: 0,
            entries: Vec::new(),
            next_after_sequence: None,
        },
        protocol_offer: protocol_offer(),
    };
    let response = client
        .post(format!("{manager_endpoint}v1/worker/poll"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(serde_json::to_vec(&request).expect("encode identity mismatch"))
        .send()
        .await
        .expect("authenticated identity mismatch response");
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
}

async fn wait_for_terminal(
    port: &Arc<conductor_transport::RemoteWorkerPort>,
    run_id: RunId,
) -> conductor_core::RunObservation {
    let deadline = Instant::now() + READINESS_TIMEOUT;
    loop {
        let port = Arc::clone(port);
        let id = run_id.clone();
        if let Ok(Ok(Some(observation))) =
            tokio::task::spawn_blocking(move || port.inspect(conductor_core::InspectRun::new(id)))
                .await
            && observation.state().is_terminal()
        {
            return observation;
        }
        assert!(
            Instant::now() < deadline,
            "native job did not finish before deadline"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn stop_helpers(helpers: &mut [WorkerProcess]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut all_exited = true;
        for helper in helpers.iter_mut() {
            match helper.0.try_wait() {
                Ok(Some(status)) => assert!(status.success(), "worker exited with {status}"),
                Ok(None) => all_exited = false,
                Err(error) => panic!("could not poll worker process: {error}"),
            }
        }
        if all_exited {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "worker processes did not stop before deadline"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn protocol_offer() -> ProtocolOffer {
    ProtocolOffer::new(
        ProtocolVersionRange::current(),
        BTreeSet::new(),
        BTreeSet::new(),
    )
    .expect("valid protocol offer")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as i64
}

fn write_private_file(path: &Path, contents: &[u8]) {
    use std::io::Write;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .expect("create private worker identity")
        .write_all(contents)
        .expect("write private worker identity");
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
