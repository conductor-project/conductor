/// Fixed-bucket counters for manager reconciliation, which performs service
/// scheduling and deployment placement work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconcileMetrics {
    pub runs: u64,
    pub failures: u64,
    pub duration_sum_ns: u64,
    pub duration_buckets: [u64; 7],
}
