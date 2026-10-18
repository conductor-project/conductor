//! Safe Landlock policy construction and enforcement for fresh launchers.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::os::fd::{AsFd, AsRawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use landlock::{
    ABI, Access, AccessFs, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset, RulesetAttr,
    RulesetCreatedAttr, RulesetStatus,
};
use serde::{Deserialize, Serialize};

const MAX_GRANTS: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LandlockMode {
    Disabled,
    Optional,
    Required,
}

#[derive(Clone, Debug)]
pub struct LandlockGrantConfig {
    pub id: String,
    pub path: PathBuf,
    pub read: bool,
    pub write: bool,
    pub execute: bool,
}

#[derive(Debug)]
struct LandlockGrant {
    path: Arc<PathFd>,
    canonical_path: PathBuf,
    read: bool,
    write: bool,
    execute: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct LandlockBackend {
    mode: LandlockMode,
    grants: Arc<BTreeMap<String, Arc<LandlockGrant>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct LauncherGrant {
    pub(crate) path: PathBuf,
    pub(crate) read: bool,
    pub(crate) write: bool,
    pub(crate) execute: bool,
}

impl LandlockBackend {
    pub(crate) fn supports_enforcement(&self) -> bool {
        self.mode != LandlockMode::Disabled
            && Ruleset::default()
                .set_compatibility(CompatLevel::HardRequirement)
                .handle_access(AccessFs::from_all(ABI::V3))
                .and_then(|ruleset| ruleset.create())
                .is_ok()
    }

    pub(crate) fn open(
        mode: LandlockMode,
        grant_configs: Vec<LandlockGrantConfig>,
        protected_paths: &[PathBuf],
    ) -> io::Result<Self> {
        if mode == LandlockMode::Disabled && !grant_configs.is_empty() {
            return Err(invalid("Landlock grants require optional or required mode"));
        }
        if grant_configs.len() > MAX_GRANTS {
            return Err(invalid("too many configured Landlock grants"));
        }
        let mut grants: BTreeMap<String, Arc<LandlockGrant>> = BTreeMap::new();
        for config in grant_configs {
            if !valid_grant_id(&config.id)
                || !config.path.is_absolute()
                || !(config.read || config.write || config.execute)
            {
                return Err(invalid("invalid administrator Landlock grant"));
            }
            let path = Arc::new(
                PathFd::new(&config.path)
                    .map_err(|_| invalid("administrator Landlock path cannot be opened"))?,
            );
            let descriptor_path =
                PathBuf::from(format!("/proc/self/fd/{}", path.as_fd().as_raw_fd()));
            let canonical_path = fs::canonicalize(&descriptor_path)
                .map_err(|_| invalid("administrator Landlock path cannot be resolved"))?;
            let metadata = fs::metadata(&descriptor_path)
                .map_err(|_| invalid("administrator Landlock path metadata is unavailable"))?;
            if !metadata.is_file() && !metadata.is_dir() {
                return Err(invalid("Landlock grants must name a file or directory"));
            }
            for protected in protected_paths {
                let protected = canonicalize_allowing_missing(protected)
                    .map_err(|_| invalid("protected Conductor path cannot be resolved"))?;
                if canonical_path.starts_with(&protected) || protected.starts_with(&canonical_path)
                {
                    return Err(invalid(
                        "Landlock grants cannot overlap Conductor state or control paths",
                    ));
                }
            }
            let grant = Arc::new(LandlockGrant {
                path,
                canonical_path,
                read: config.read,
                write: config.write,
                execute: config.execute,
            });
            if grants.insert(config.id, grant).is_some() {
                return Err(invalid("duplicate administrator Landlock grant identifier"));
            }
        }
        validate_overlaps(&grants)?;
        Ok(Self {
            mode,
            grants: Arc::new(grants),
        })
    }

    pub(crate) fn launcher_grants(
        &self,
        selected: Option<&conductor_core::FilesystemPolicy>,
    ) -> io::Result<Option<Vec<LauncherGrant>>> {
        let selected = match (self.mode, selected) {
            (LandlockMode::Disabled, None) | (LandlockMode::Optional, None) => return Ok(None),
            (LandlockMode::Disabled, Some(_)) => {
                return Err(invalid("filesystem policy requires Landlock to be enabled"));
            }
            (LandlockMode::Required, None) => {
                return Err(invalid("administrator requires a filesystem policy"));
            }
            (_, Some(selected)) => selected,
        };
        let mut result = Vec::with_capacity(selected.grants().len());
        for id in selected.grants() {
            let grant = self.grants.get(id).ok_or_else(|| {
                invalid("filesystem policy selects an unknown administrator grant")
            })?;
            result.push(LauncherGrant {
                path: PathBuf::from(format!(
                    "/proc/{}/fd/{}",
                    std::process::id(),
                    grant.path.as_fd().as_raw_fd()
                )),
                read: grant.read,
                write: grant.write,
                execute: grant.execute,
            });
        }
        Ok(Some(result))
    }
}

pub(crate) fn apply_launcher_policy(grants: &[LauncherGrant]) -> io::Result<()> {
    let mut ruleset = Ruleset::default()
        .set_compatibility(CompatLevel::HardRequirement)
        .handle_access(AccessFs::from_all(ABI::V3))
        .map_err(landlock_error)?
        .create()
        .map_err(landlock_error)?;
    for grant in grants {
        let path = PathFd::new(&grant.path)
            .map_err(|_| io::Error::other("Landlock grant handle could not be opened"))?;
        let metadata = fs::metadata(&grant.path)
            .map_err(|_| io::Error::other("Landlock grant metadata could not be read"))?;
        let mut access = landlock::BitFlags::<AccessFs>::EMPTY;
        if grant.read {
            access |= AccessFs::ReadFile;
            if metadata.is_dir() {
                access |= AccessFs::ReadDir;
            }
        }
        if grant.write {
            access |= AccessFs::WriteFile | AccessFs::Truncate;
            if metadata.is_dir() {
                access |= AccessFs::RemoveDir
                    | AccessFs::RemoveFile
                    | AccessFs::MakeChar
                    | AccessFs::MakeDir
                    | AccessFs::MakeReg
                    | AccessFs::MakeSock
                    | AccessFs::MakeFifo
                    | AccessFs::MakeBlock
                    | AccessFs::MakeSym
                    | AccessFs::Refer;
            }
        }
        if grant.execute {
            access |= AccessFs::Execute;
        }
        ruleset = ruleset
            .add_rule(
                PathBeneath::new(path, access).set_compatibility(CompatLevel::HardRequirement),
            )
            .map_err(landlock_error)?;
    }
    let status = ruleset.restrict_self().map_err(landlock_error)?;
    if status.ruleset != RulesetStatus::FullyEnforced {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kernel did not fully enforce the requested Landlock policy",
        ));
    }
    Ok(())
}

pub(crate) fn read_launcher_policy(path: &Path) -> io::Result<Vec<LauncherGrant>> {
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(|_| invalid("invalid internal Landlock policy manifest"))
}

fn valid_grant_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

fn canonicalize_allowing_missing(path: &Path) -> io::Result<PathBuf> {
    match fs::canonicalize(path) {
        Ok(canonical) => return Ok(canonical),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut resolved = PathBuf::new();
    let mut has_missing_component = false;

    for component in absolute.components() {
        match component {
            std::path::Component::Prefix(prefix) => resolved.push(prefix.as_os_str()),
            std::path::Component::RootDir => resolved.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir if has_missing_component => {
                return Err(invalid(
                    "protected Conductor path traverses a parent after a missing component",
                ));
            }
            std::path::Component::ParentDir => {
                if let Some(parent) = resolved.parent() {
                    resolved = parent.to_path_buf();
                }
            }
            std::path::Component::Normal(part) if has_missing_component => resolved.push(part),
            std::path::Component::Normal(part) => {
                let candidate = resolved.join(part);
                match fs::canonicalize(&candidate) {
                    Ok(canonical) => resolved = canonical,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        match fs::symlink_metadata(&candidate) {
                            Err(metadata_error)
                                if metadata_error.kind() == io::ErrorKind::NotFound => {}
                            Ok(_) | Err(_) => return Err(error),
                        }
                        resolved = candidate;
                        has_missing_component = true;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }

    Ok(resolved)
}

fn validate_overlaps(grants: &BTreeMap<String, Arc<LandlockGrant>>) -> io::Result<()> {
    let entries: Vec<_> = grants.values().collect();
    for (index, left) in entries.iter().enumerate() {
        for right in entries.iter().skip(index + 1) {
            let left_contains_right = right.canonical_path.starts_with(&left.canonical_path);
            let right_contains_left = left.canonical_path.starts_with(&right.canonical_path);
            if left_contains_right
                && ((right.read && !left.read)
                    || (right.write && !left.write)
                    || (right.execute && !left.execute))
                || right_contains_left
                    && ((left.read && !right.read)
                        || (left.write && !right.write)
                        || (left.execute && !right.execute))
            {
                return Err(invalid(
                    "nested Landlock grants cannot widen access beyond an ancestor grant",
                ));
            }
        }
    }
    Ok(())
}

fn landlock_error(_: impl std::fmt::Debug) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "Landlock policy could not be fully applied",
    )
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
