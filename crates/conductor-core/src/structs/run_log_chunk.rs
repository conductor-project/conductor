use super::*;

/// Reply to a bounded log read. `data` contains raw bytes for transport encoding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunLogChunk {
    pub(super) run_id: RunId,
    pub(super) stream: LogStream,
    pub(super) offset: u64,
    pub(super) next_offset: u64,
    pub(super) data: Vec<u8>,
    pub(super) stream_closed: bool,
    pub(super) eof: bool,
    pub(super) truncated: bool,
}

impl RunLogChunk {
    pub fn new(
        run_id: RunId,
        stream: LogStream,
        offset: u64,
        next_offset: u64,
        data: Vec<u8>,
        status: RunLogStatus,
    ) -> Self {
        Self {
            run_id,
            stream,
            offset,
            next_offset,
            data,
            stream_closed: status.stream_closed,
            eof: status.eof,
            truncated: status.truncated,
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
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
    pub fn data(&self) -> &[u8] {
        &self.data
    }
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
    pub fn stream_closed(&self) -> bool {
        self.stream_closed
    }
    pub fn eof(&self) -> bool {
        self.eof
    }
    pub fn truncated(&self) -> bool {
        self.truncated
    }
}
