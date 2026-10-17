//! Shared, transport-neutral identifiers and contracts for Conductor.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};

const MAX_IDENTIFIER_LENGTH: usize = 128;
const DEFAULT_TERMINATION_GRACE_MS: u64 = 5_000;
const MAX_TERMINATION_GRACE_MS: u64 = 300_000;
const MAX_TIMEOUT_MS: u64 = 604_800_000;

/// An identifier failed the stable ASCII identifier format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidIdentifier;

impl fmt::Display for InvalidIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("identifier must contain 1-128 ASCII letters, digits, '-', '_' or '.'")
    }
}

impl std::error::Error for InvalidIdentifier {}

macro_rules! stable_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Validate and construct a stable identifier.
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidIdentifier> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > MAX_IDENTIFIER_LENGTH
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                {
                    return Err(InvalidIdentifier);
                }
                Ok(Self(value))
            }

            /// Borrow the identifier without allocating.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidIdentifier;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

stable_id!(JobId);
stable_id!(RunId);
stable_id!(WorkerId);

/// Invalid process/job specification input. Messages never include submitted values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobValidationError {
    EmptyCommand,
    ExecutableMustBeAbsolute,
    WorkingDirectoryMustBeAbsolute,
    NulByte,
    InvalidEnvironmentName,
    InvalidEnvironmentValue,
    InvalidTimeout,
    InvalidTerminationGrace,
}

impl fmt::Display for JobValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyCommand => "command must contain an executable",
            Self::ExecutableMustBeAbsolute => "command executable must be an absolute path",
            Self::WorkingDirectoryMustBeAbsolute => "cwd must be an absolute path",
            Self::NulByte => "process arguments and paths must not contain NUL bytes",
            Self::InvalidEnvironmentName => "environment names must be valid ASCII identifiers",
            Self::InvalidEnvironmentValue => "environment values must not contain NUL bytes",
            Self::InvalidTimeout => "timeoutMs must be between 1 and 604800000",
            Self::InvalidTerminationGrace => "terminationGraceMs must be between 0 and 300000",
        })
    }
}

impl std::error::Error for JobValidationError {}

/// An immutable, validated process definition. Registration never executes it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobSpec {
    id: JobId,
    command: Vec<String>,
    cwd: PathBuf,
    env: BTreeMap<String, String>,
    inherit_env: bool,
    timeout_ms: Option<u64>,
    termination_grace_ms: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawJobSpec {
    id: JobId,
    command: Vec<String>,
    cwd: PathBuf,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    inherit_env: bool,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default = "default_termination_grace_ms")]
    termination_grace_ms: u64,
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
        })
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
    pub fn inherit_env(&self) -> bool {
        self.inherit_env
    }
    pub fn timeout_ms(&self) -> Option<u64> {
        self.timeout_ms
    }
    pub fn termination_grace_ms(&self) -> u64 {
        self.termination_grace_ms
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
        )
    }
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

fn default_termination_grace_ms() -> u64 {
    DEFAULT_TERMINATION_GRACE_MS
}

fn valid_environment_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
