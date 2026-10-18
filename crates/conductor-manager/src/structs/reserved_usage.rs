use super::*;

#[derive(Clone, Copy, Default)]
pub(super) struct ReservedUsage {
    pub(super) cpu_millis: u64,
    pub(super) memory_bytes: u64,
    pub(super) slots: usize,
}

impl ReservedUsage {
    pub(super) fn add(&mut self, reservation: &ResourceReservation) {
        self.cpu_millis = self
            .cpu_millis
            .saturating_add(u64::from(reservation.cpu_millis().unwrap_or_default()));
        self.memory_bytes = self
            .memory_bytes
            .saturating_add(reservation.memory_bytes().unwrap_or_default());
        self.slots = self.slots.saturating_add(1);
    }

    pub(super) fn fits(
        self,
        request: &ResourceReservation,
        capacity: WorkerResourceCapacity,
    ) -> bool {
        self.cpu_millis
            .checked_add(u64::from(request.cpu_millis().unwrap_or_default()))
            .is_some_and(|used| used <= u64::from(capacity.cpu_millis()))
            && self
                .memory_bytes
                .checked_add(request.memory_bytes().unwrap_or_default())
                .is_some_and(|used| used <= capacity.memory_bytes())
    }
}
