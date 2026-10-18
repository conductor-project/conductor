use super::*;

pub(super) struct ApiReadiness {
    pub(super) role: ApiRole,
    pub(super) manager_workers_ready: Option<Arc<AtomicBool>>,
    pub(super) recovery_workers: Option<Vec<Arc<dyn WorkerPort>>>,
}
