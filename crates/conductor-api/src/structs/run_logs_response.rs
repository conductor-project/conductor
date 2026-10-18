use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RunLogsResponse {
    pub(super) run_id: String,
    pub(super) stream: LogStream,
    pub(super) offset: u64,
    pub(super) next_offset: u64,
    pub(super) data_encoding: &'static str,
    pub(super) data: String,
    pub(super) stream_closed: bool,
    pub(super) eof: bool,
    pub(super) truncated: bool,
}
