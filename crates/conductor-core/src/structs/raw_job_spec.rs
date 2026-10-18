use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RawJobSpec {
    pub(super) id: JobId,
    pub(super) command: Vec<String>,
    pub(super) cwd: PathBuf,
    #[serde(default)]
    pub(super) env: BTreeMap<String, String>,
    #[serde(default)]
    pub(super) inherit_env: bool,
    #[serde(default)]
    pub(super) timeout_ms: Option<u64>,
    #[serde(default = "default_termination_grace_ms")]
    pub(super) termination_grace_ms: u64,
    #[serde(default)]
    pub(super) resource_limits: ResourceLimits,
    #[serde(default)]
    pub(super) filesystem_policy: Option<FilesystemPolicy>,
    #[serde(default)]
    pub(super) executable_digest: Option<ExecutableDigest>,
}
