use super::*;

pub(super) struct ApiState {
    pub(super) manager: Arc<JobService>,
    pub(super) worker: Arc<dyn WorkerPort>,
    pub(super) workers: Vec<Arc<dyn WorkerPort>>,
    pub(super) worker_id: WorkerId,
    pub(super) worker_capacity: usize,
    pub(super) role: ApiRole,
    pub(super) manager_workers_ready: Option<Arc<AtomicBool>>,
    pub(super) start_admission: Arc<Semaphore>,
    pub(super) blocking_admission: Arc<Semaphore>,
    pub(super) control_admission: Arc<Semaphore>,
    pub(super) service_watch_admission: Arc<Semaphore>,
    pub(super) recovery_ready: Arc<AtomicBool>,
    pub(super) operator_authenticator: Option<Arc<dyn OperatorAuthenticator>>,
}
