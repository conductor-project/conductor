use super::*;

/// An immutable selection of administrator-approved filesystem grants.
/// Jobs select configured grants by identifier and cannot submit paths.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FilesystemPolicy {
    pub(super) grants: Vec<String>,
}

impl FilesystemPolicy {
    pub fn new(grants: Vec<String>) -> Result<Self, JobValidationError> {
        let mut seen = std::collections::BTreeSet::new();
        if grants.len() > MAX_FILESYSTEM_GRANTS_PER_JOB
            || grants.iter().any(|grant| {
                grant.is_empty()
                    || grant.len() > MAX_IDENTIFIER_LENGTH
                    || !grant
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                    || !seen.insert(grant.as_str())
            })
        {
            return Err(JobValidationError::InvalidFilesystemPolicy);
        }
        Ok(Self { grants })
    }

    pub fn grants(&self) -> &[String] {
        &self.grants
    }
}
