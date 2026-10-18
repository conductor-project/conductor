use super::*;

#[derive(Serialize)]
pub(super) struct ErrorDetail {
    pub(super) code: &'static str,
    pub(super) message: &'static str,
    pub(super) details: &'static [&'static str],
}
