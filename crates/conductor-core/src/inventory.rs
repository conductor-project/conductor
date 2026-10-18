//! Bounded worker inventory pages used during manager-worker reconnect.

use serde::{Deserialize, Serialize};

use crate::{MAX_WORKER_INVENTORY_PAGE_SIZE, RunObservation, WorkerId};

/// One durable worker run record and its worker-database ordering key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInventoryEntry {
    pub sequence: u64,
    pub observation: RunObservation,
}

/// One bounded, keyset-paginated view of a worker's durable run history.
///
/// `snapshot_generation` is the highest worker-database row sequence visible
/// when the first page was read. Later pages use it as an upper bound so new
/// runs cannot shift or extend a scan already in progress.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInventoryPage {
    pub worker_id: WorkerId,
    pub worker_incarnation: u64,
    pub snapshot_generation: u64,
    pub after_sequence: u64,
    pub entries: Vec<WorkerInventoryEntry>,
    pub next_after_sequence: Option<u64>,
}

impl WorkerInventoryPage {
    /// Validate wire bounds and cursor ordering before reconciling a page.
    pub fn is_valid(&self) -> bool {
        if self.entries.len() > MAX_WORKER_INVENTORY_PAGE_SIZE
            || self.after_sequence > self.snapshot_generation
        {
            return false;
        }
        let mut previous = self.after_sequence;
        for entry in &self.entries {
            if entry.sequence <= previous
                || entry.sequence > self.snapshot_generation
                || entry.observation.worker_id() != &self.worker_id
            {
                return false;
            }
            previous = entry.sequence;
        }
        match self.next_after_sequence {
            Some(next) => {
                self.entries
                    .last()
                    .is_some_and(|last| last.sequence == next)
                    && next > self.after_sequence
                    && next < self.snapshot_generation
            }
            None => true,
        }
    }
}
