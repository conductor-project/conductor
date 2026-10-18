use super::*;

pub(super) struct WorkerState {
    pub(super) connection: Connection,
    pub(super) active_groups: BTreeMap<String, ActiveGroup>,
}
