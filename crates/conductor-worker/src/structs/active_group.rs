use super::*;

#[derive(Clone)]
pub(super) struct ActiveGroup {
    pub(super) group_id: u32,
    pub(super) intent: Arc<AtomicU8>,
    pub(super) notify: Arc<Notify>,
}
