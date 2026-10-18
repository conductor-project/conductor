use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RunLogFilters {
    pub(super) stream: LogStream,
    pub(super) offset: Option<u64>,
    pub(super) limit: Option<usize>,
}
