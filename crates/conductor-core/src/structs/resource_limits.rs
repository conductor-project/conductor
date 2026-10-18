use super::*;

/// Optional Linux cgroup v2 resource limits requested by an immutable job.
/// These values never grant privileges and are always constrained by the
/// administrator's configured worker policy.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) cpu_quota_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) cpu_period_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) memory_max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) memory_swap_max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) pids_max: Option<u32>,
}

impl ResourceLimits {
    pub fn new(
        cpu_quota_us: Option<u64>,
        cpu_period_us: Option<u64>,
        memory_max_bytes: Option<u64>,
        memory_swap_max_bytes: Option<u64>,
        pids_max: Option<u32>,
    ) -> Result<Self, JobValidationError> {
        if cpu_quota_us.is_some() != cpu_period_us.is_some()
            || cpu_quota_us.is_some_and(|quota| !(1_000..=1_000_000_000).contains(&quota))
            || cpu_period_us.is_some_and(|period| !(1_000..=1_000_000).contains(&period))
        {
            return Err(JobValidationError::InvalidCpuLimit);
        }
        if memory_max_bytes.is_some_and(|bytes| !(4 * 1024 * 1024..=u64::MAX - 1).contains(&bytes))
            || memory_swap_max_bytes.is_some_and(|bytes| bytes == u64::MAX)
        {
            return Err(JobValidationError::InvalidMemoryLimit);
        }
        if pids_max.is_some_and(|pids| !(1..=1_048_576).contains(&pids)) {
            return Err(JobValidationError::InvalidProcessLimit);
        }
        Ok(Self {
            cpu_quota_us,
            cpu_period_us,
            memory_max_bytes,
            memory_swap_max_bytes,
            pids_max,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.cpu_quota_us.is_none()
            && self.cpu_period_us.is_none()
            && self.memory_max_bytes.is_none()
            && self.memory_swap_max_bytes.is_none()
            && self.pids_max.is_none()
    }

    pub fn cpu_quota_us(&self) -> Option<u64> {
        self.cpu_quota_us
    }
    pub fn cpu_period_us(&self) -> Option<u64> {
        self.cpu_period_us
    }
    pub fn memory_max_bytes(&self) -> Option<u64> {
        self.memory_max_bytes
    }
    pub fn memory_swap_max_bytes(&self) -> Option<u64> {
        self.memory_swap_max_bytes
    }
    pub fn pids_max(&self) -> Option<u32> {
        self.pids_max
    }
}
