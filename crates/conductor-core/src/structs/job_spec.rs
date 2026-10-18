use super::*;

/// An immutable, validated process definition. Registration never executes it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobSpec {
    pub(super) id: JobId,
    pub(super) command: Vec<String>,
    pub(super) cwd: PathBuf,
    pub(super) env: BTreeMap<String, String>,
    pub(super) inherit_env: bool,
    pub(super) timeout_ms: Option<u64>,
    pub(super) termination_grace_ms: u64,
    #[serde(default, skip_serializing_if = "ResourceLimits::is_empty")]
    pub(super) resource_limits: ResourceLimits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) filesystem_policy: Option<FilesystemPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) executable_digest: Option<ExecutableDigest>,
}

impl<'de> Deserialize<'de> for JobSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawJobSpec::deserialize(deserializer)?;
        Self::try_from(raw).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<RawJobSpec> for JobSpec {
    type Error = JobValidationError;

    fn try_from(raw: RawJobSpec) -> Result<Self, Self::Error> {
        Self::new(
            raw.id,
            raw.command,
            raw.cwd,
            raw.env,
            raw.inherit_env,
            raw.timeout_ms,
            raw.termination_grace_ms,
        )?
        .with_resource_limits(raw.resource_limits)?
        .with_filesystem_policy(raw.filesystem_policy)
        .map(|job| job.with_executable_digest(raw.executable_digest))
    }
}

impl JobSpec {
    /// Validate and construct a job specification.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: JobId,
        command: Vec<String>,
        cwd: PathBuf,
        env: BTreeMap<String, String>,
        inherit_env: bool,
        timeout_ms: Option<u64>,
        termination_grace_ms: u64,
    ) -> Result<Self, JobValidationError> {
        if command.is_empty() {
            return Err(JobValidationError::EmptyCommand);
        }
        if !Path::new(&command[0]).is_absolute() {
            return Err(JobValidationError::ExecutableMustBeAbsolute);
        }
        if !cwd.is_absolute() {
            return Err(JobValidationError::WorkingDirectoryMustBeAbsolute);
        }
        if command.iter().any(|part| part.as_bytes().contains(&0))
            || cwd.to_string_lossy().as_bytes().contains(&0)
        {
            return Err(JobValidationError::NulByte);
        }
        if env.keys().any(|name| !valid_environment_name(name)) {
            return Err(JobValidationError::InvalidEnvironmentName);
        }
        if env.values().any(|value| value.as_bytes().contains(&0)) {
            return Err(JobValidationError::InvalidEnvironmentValue);
        }
        if timeout_ms.is_some_and(|value| value == 0 || value > MAX_TIMEOUT_MS) {
            return Err(JobValidationError::InvalidTimeout);
        }
        if termination_grace_ms > MAX_TERMINATION_GRACE_MS {
            return Err(JobValidationError::InvalidTerminationGrace);
        }

        Ok(Self {
            id,
            command,
            cwd,
            env,
            inherit_env,
            timeout_ms,
            termination_grace_ms,
            resource_limits: ResourceLimits::default(),
            filesystem_policy: None,
            executable_digest: None,
        })
    }

    pub fn with_resource_limits(
        mut self,
        resource_limits: ResourceLimits,
    ) -> Result<Self, JobValidationError> {
        self.resource_limits = ResourceLimits::new(
            resource_limits.cpu_quota_us,
            resource_limits.cpu_period_us,
            resource_limits.memory_max_bytes,
            resource_limits.memory_swap_max_bytes,
            resource_limits.pids_max,
        )?;
        Ok(self)
    }

    pub fn id(&self) -> &JobId {
        &self.id
    }
    pub fn command(&self) -> &[String] {
        &self.command
    }
    pub fn cwd(&self) -> &Path {
        &self.cwd
    }
    pub fn env(&self) -> &BTreeMap<String, String> {
        &self.env
    }

    /// Return a copy with validated manager-assigned environment values.
    pub fn with_environment_overrides(
        mut self,
        overrides: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, JobValidationError> {
        for (name, value) in overrides {
            if !valid_environment_name(&name) {
                return Err(JobValidationError::InvalidEnvironmentName);
            }
            if value.as_bytes().contains(&0) {
                return Err(JobValidationError::InvalidEnvironmentValue);
            }
            self.env.insert(name, value);
        }
        Ok(self)
    }
    pub fn inherit_env(&self) -> bool {
        self.inherit_env
    }
    pub fn timeout_ms(&self) -> Option<u64> {
        self.timeout_ms
    }
    pub fn termination_grace_ms(&self) -> u64 {
        self.termination_grace_ms
    }
    pub fn resource_limits(&self) -> &ResourceLimits {
        &self.resource_limits
    }
    pub fn with_filesystem_policy(
        mut self,
        filesystem_policy: Option<FilesystemPolicy>,
    ) -> Result<Self, JobValidationError> {
        self.filesystem_policy = filesystem_policy
            .map(|policy| FilesystemPolicy::new(policy.grants))
            .transpose()?;
        Ok(self)
    }
    pub fn filesystem_policy(&self) -> Option<&FilesystemPolicy> {
        self.filesystem_policy.as_ref()
    }

    /// Attach an operator-provisioned executable digest. Workers verify it
    /// immediately before launching the configured executable.
    pub fn with_executable_digest(mut self, digest: Option<ExecutableDigest>) -> Self {
        self.executable_digest = digest;
        self
    }

    pub fn executable_digest(&self) -> Option<&ExecutableDigest> {
        self.executable_digest.as_ref()
    }
}
