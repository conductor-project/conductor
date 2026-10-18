/// Database-derived operational counts. All fields are aggregate counts; IDs
/// and user-provided values are intentionally excluded.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OperationalMetrics {
    pub deployments_pending: u64,
    pub deployments_ready: u64,
    pub deployments_suspended: u64,
    pub deployments_deleting: u64,
    pub pending_allocations: u64,
    pub retried_service_attempts: u64,
    pub terminal_service_attempts: u64,
    pub service_catalog_owners: u64,
    pub pending_reasons: [u64; 12],
}
