use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransportLogChunk {
    pub run_id: RunId,
    pub stream: LogStream,
    pub offset: u64,
    pub next_offset: u64,
    pub data_base64: String,
    pub stream_closed: bool,
    pub eof: bool,
    pub truncated: bool,
}

impl TransportLogChunk {
    pub(super) fn from_chunk(chunk: RunLogChunk) -> Self {
        Self {
            run_id: chunk.run_id().clone(),
            stream: chunk.stream(),
            offset: chunk.offset(),
            next_offset: chunk.next_offset(),
            data_base64: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(chunk.data()),
            stream_closed: chunk.stream_closed(),
            eof: chunk.eof(),
            truncated: chunk.truncated(),
        }
    }

    pub(super) fn into_chunk(self) -> Result<RunLogChunk, TransportError> {
        let data = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(self.data_base64.as_bytes())
            .map_err(|_| TransportError::MalformedResponse)?;
        Ok(RunLogChunk::new(
            self.run_id,
            self.stream,
            self.offset,
            self.next_offset,
            data,
            RunLogStatus::new(self.stream_closed, self.eof, self.truncated),
        ))
    }
}
