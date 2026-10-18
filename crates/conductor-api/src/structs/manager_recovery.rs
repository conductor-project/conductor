use super::*;

/// Worker set and readiness signal used to recover a standalone manager.
pub struct ManagerRecovery {
    pub(super) workers_ready: Arc<AtomicBool>,
    pub(super) workers: Vec<Arc<dyn WorkerPort>>,
}

impl ManagerRecovery {
    pub fn new(workers_ready: Arc<AtomicBool>, workers: Vec<Arc<dyn WorkerPort>>) -> Self {
        Self {
            workers_ready,
            workers,
        }
    }
}
