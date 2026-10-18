use super::*;

pub(super) struct RunSupervision {
    pub(super) state: Arc<SharedWorkerState>,
    pub(super) pending: Arc<PendingPersistence>,
    pub(super) persistence_admission: Arc<Semaphore>,
    pub(super) shutdown_signal: watch::Receiver<bool>,
    pub(super) backend: Arc<dyn ExecutionBackend>,
    pub(super) run_id: String,
    pub(super) spawned: SpawnGuard,
    pub(super) current: RunObservation,
    pub(super) control: Arc<AtomicU8>,
    pub(super) control_notify: Arc<Notify>,
    pub(super) launched_at: std::time::Instant,
    pub(super) timeout: Option<Duration>,
    pub(super) grace: Duration,
    pub(super) log_directory: PathBuf,
    pub(super) retention_runs: usize,
    pub(super) readiness_probe: Option<ReadinessProbe>,
    pub(super) service_ports: Vec<conductor_core::AssignedServicePort>,
}
