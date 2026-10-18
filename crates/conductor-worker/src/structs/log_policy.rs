use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogPolicy {
    pub max_bytes_per_stream: u64,
    pub retention_runs: usize,
}

impl Default for LogPolicy {
    fn default() -> Self {
        Self {
            max_bytes_per_stream: DEFAULT_LOG_MAX_BYTES_PER_STREAM,
            retention_runs: DEFAULT_LOG_RETENTION_RUNS,
        }
    }
}
