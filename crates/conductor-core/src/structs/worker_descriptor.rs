use super::*;

/// Local worker registration advertised through the manager-worker contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerDescriptor {
    pub(super) worker_id: WorkerId,
    pub(super) capacity: usize,
    pub(super) process_execution: bool,
    #[serde(default)]
    pub(super) incarnation: u64,
}

impl WorkerDescriptor {
    pub fn new(worker_id: WorkerId, capacity: usize, process_execution: bool) -> Self {
        Self {
            worker_id,
            capacity,
            process_execution,
            incarnation: 0,
        }
    }
    pub fn with_incarnation(mut self, incarnation: u64) -> Self {
        self.incarnation = incarnation;
        self
    }
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn supports_process_execution(&self) -> bool {
        self.process_execution
    }
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }
}
