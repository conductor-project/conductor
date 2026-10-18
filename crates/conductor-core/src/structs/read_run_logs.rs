use super::*;

/// Bounded byte-range read from one run output stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadRunLogs {
    pub(super) run_id: RunId,
    pub(super) stream: LogStream,
    pub(super) offset: u64,
    pub(super) limit: usize,
}

impl ReadRunLogs {
    pub fn new(run_id: RunId, stream: LogStream, offset: u64, limit: usize) -> Self {
        Self {
            run_id,
            stream,
            offset,
            limit,
        }
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn stream(&self) -> LogStream {
        self.stream
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
}
