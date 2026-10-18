use super::*;

pub(super) struct ManagerTelemetry {
    pub(super) reconcile_runs: AtomicU64,
    pub(super) reconcile_failures: AtomicU64,
    pub(super) reconcile_duration_sum_ns: AtomicU64,
    pub(super) reconcile_duration_buckets: [AtomicU64; 7],
}

impl ManagerTelemetry {
    pub(super) fn record_reconcile(&self, elapsed: Duration, failed: bool) {
        self.reconcile_runs.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.reconcile_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.reconcile_duration_sum_ns.fetch_add(
            elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        let bounds = [
            Duration::from_millis(5),
            Duration::from_millis(25),
            Duration::from_millis(100),
            Duration::from_millis(500),
            Duration::from_secs(1),
            Duration::from_secs(5),
        ];
        let bucket = bounds
            .iter()
            .position(|bound| elapsed <= *bound)
            .unwrap_or(bounds.len());
        self.reconcile_duration_buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> ReconcileMetrics {
        ReconcileMetrics {
            runs: self.reconcile_runs.load(Ordering::Relaxed),
            failures: self.reconcile_failures.load(Ordering::Relaxed),
            duration_sum_ns: self.reconcile_duration_sum_ns.load(Ordering::Relaxed),
            duration_buckets: std::array::from_fn(|index| {
                self.reconcile_duration_buckets[index].load(Ordering::Relaxed)
            }),
        }
    }
}

impl Default for ManagerTelemetry {
    fn default() -> Self {
        Self {
            reconcile_runs: AtomicU64::new(0),
            reconcile_failures: AtomicU64::new(0),
            reconcile_duration_sum_ns: AtomicU64::new(0),
            reconcile_duration_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}
