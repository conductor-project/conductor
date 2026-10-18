use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunLogStatus {
    pub(super) stream_closed: bool,
    pub(super) eof: bool,
    pub(super) truncated: bool,
}

impl RunLogStatus {
    pub fn new(stream_closed: bool, eof: bool, truncated: bool) -> Self {
        Self {
            stream_closed,
            eof,
            truncated,
        }
    }
}
