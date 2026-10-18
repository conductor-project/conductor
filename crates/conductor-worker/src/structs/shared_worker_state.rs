use super::*;

pub(super) struct SharedWorkerState(pub(super) Mutex<WorkerState>);
