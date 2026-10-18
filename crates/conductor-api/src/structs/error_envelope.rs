use super::*;

#[derive(Serialize)]
pub(super) struct ErrorEnvelope {
    pub(super) error: ErrorDetail,
}
