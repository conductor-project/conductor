//! Linux cgroup v2 operations for one delegated Conductor subtree.
//!
//! This module uses only cgroupfs files and safe standard-library APIs. The
//! delegated root must be prepared by the administrator; this code never
//! enables controllers or changes any ancestor cgroup.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use conductor_core::ResourceLimits;

const MIN_MEMORY_BYTES: u64 = 4 * 1024 * 1024;
const POPULATED_WAIT: Duration = Duration::from_secs(5);
static NEXT_GROUP: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub(crate) struct CgroupV2 {
    root: PathBuf,
    maximums: ResourceLimits,
    _ownership: Arc<File>,
}

impl CgroupV2 {
    pub(crate) fn open(root: &Path, worker_id: &str, maximums: ResourceLimits) -> io::Result<Self> {
        let maximums = ResourceLimits::new(
            maximums.cpu_quota_us(),
            maximums.cpu_period_us(),
            maximums.memory_max_bytes(),
            maximums.memory_swap_max_bytes(),
            maximums.pids_max(),
        )
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cgroup limits"))?;
        let delegated_root = fs::canonicalize(root)?;
        if !is_cgroup2_mountpoint(&delegated_root)? {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "delegated root is not on a cgroup v2 mount",
            ));
        }
        for required in [
            "cgroup.controllers",
            "cgroup.subtree_control",
            "cgroup.events",
            "cgroup.procs",
            "cgroup.kill",
        ] {
            if !delegated_root.join(required).is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "delegated cgroup v2 root is missing required control files",
                ));
            }
        }
        let available = fs::read_to_string(delegated_root.join("cgroup.controllers"))?;
        let enabled = fs::read_to_string(delegated_root.join("cgroup.subtree_control"))?;
        for name in ["cpu", "memory", "pids"] {
            if !available
                .split_ascii_whitespace()
                .any(|value| value == name)
                || !enabled.split_ascii_whitespace().any(|value| value == name)
            {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("required cgroup controller '{name}' is not delegated"),
                ));
            }
        }
        let worker_root_name = format!("conductor-{:016x}", stable_run_hash(worker_id));
        let ownership = acquire_namespace_ownership(&delegated_root, &worker_root_name)?;
        let root = delegated_root.join(worker_root_name);
        match fs::create_dir(&root) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        if !root.join("cgroup.events").is_file() || !root.join("cgroup.kill").is_file() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "worker cgroup is missing required lifecycle controls",
            ));
        }
        fs::write(root.join("cgroup.subtree_control"), "+cpu +memory +pids")?;
        let manager = Self {
            root,
            maximums,
            _ownership: Arc::new(ownership),
        };
        manager.reconcile_owned_groups()?;
        Ok(manager)
    }

    fn reconcile_owned_groups(&self) -> io::Result<()> {
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with("run-")
                && entry.file_type()?.is_dir()
            {
                let path = entry.path();
                Self::kill(&path)?;
                Self::remove_when_empty(&path)?;
            }
        }
        Ok(())
    }

    pub(crate) fn create(&self, run_id: &str, requested: &ResourceLimits) -> io::Result<PathBuf> {
        let limits = self.effective_limits(requested)?;
        let sequence = NEXT_GROUP.fetch_add(1, Ordering::Relaxed);
        let path = self
            .root
            .join(format!("run-{:016x}-{sequence}", stable_run_hash(run_id)));
        fs::create_dir(&path)?;
        let result = self.write_limits(&path, &limits);
        if let Err(error) = result {
            let _ = fs::remove_dir(&path);
            return Err(error);
        }
        for required in ["cgroup.procs", "cgroup.events", "cgroup.kill"] {
            if !path.join(required).is_file() {
                let _ = fs::remove_dir(&path);
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "kernel lacks required cgroup v2 lifecycle controls",
                ));
            }
        }
        Ok(path)
    }

    fn effective_limits(&self, requested: &ResourceLimits) -> io::Result<ResourceLimits> {
        let choose = |request: Option<u64>, maximum: Option<u64>, label: &str| {
            if let (Some(request), Some(maximum)) = (request, maximum)
                && request > maximum
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("job {label} exceeds the administrator limit"),
                ));
            }
            Ok(request.or(maximum))
        };
        let quota = choose(
            requested.cpu_quota_us(),
            self.maximums.cpu_quota_us(),
            "CPU quota",
        )?;
        let period = match (requested.cpu_quota_us(), requested.cpu_period_us()) {
            (Some(_), Some(requested_period)) => Some(requested_period),
            _ => self.maximums.cpu_period_us(),
        };
        if requested.cpu_quota_us().is_some()
            && self.maximums.cpu_quota_us().is_some()
            && u128::from(requested.cpu_quota_us().unwrap())
                * u128::from(self.maximums.cpu_period_us().unwrap_or(100_000))
                > u128::from(self.maximums.cpu_quota_us().unwrap())
                    * u128::from(requested.cpu_period_us().unwrap_or(100_000))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "job CPU rate exceeds the administrator limit",
            ));
        }
        let memory = choose(
            requested.memory_max_bytes(),
            self.maximums.memory_max_bytes(),
            "memory maximum",
        )?;
        let swap = choose(
            requested.memory_swap_max_bytes(),
            self.maximums.memory_swap_max_bytes(),
            "memory swap maximum",
        )?;
        let pids = choose(
            requested.pids_max().map(u64::from),
            self.maximums.pids_max().map(u64::from),
            "process limit",
        )?
        .map(|value| value as u32);
        ResourceLimits::new(quota, period, memory, swap, pids).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid effective cgroup limits",
            )
        })
    }

    fn write_limits(&self, path: &Path, limits: &ResourceLimits) -> io::Result<()> {
        if let (Some(quota), Some(period)) = (limits.cpu_quota_us(), limits.cpu_period_us()) {
            write_control(path, "cpu.max", &format!("{quota} {period}"))?;
        }
        if let Some(bytes) = limits.memory_max_bytes() {
            if bytes < MIN_MEMORY_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "memory limit is too small",
                ));
            }
            write_control(path, "memory.max", &bytes.to_string())?;
        }
        if let Some(bytes) = limits.memory_swap_max_bytes() {
            write_control(path, "memory.swap.max", &bytes.to_string())?;
        }
        if let Some(pids) = limits.pids_max() {
            write_control(path, "pids.max", &pids.to_string())?;
        }
        Ok(())
    }

    pub(crate) fn move_launcher(path: &Path) -> io::Result<()> {
        write_control(path, "cgroup.procs", &std::process::id().to_string())
    }

    pub(crate) fn kill(path: &Path) -> io::Result<()> {
        write_control(path, "cgroup.kill", "1")
    }

    pub(crate) fn remove_when_empty(path: &Path) -> io::Result<()> {
        let deadline = Instant::now() + POPULATED_WAIT;
        loop {
            let events = fs::read_to_string(path.join("cgroup.events"))?;
            if events.lines().any(|line| line == "populated 0") {
                fs::remove_dir(path)?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "cgroup remained populated after the cleanup deadline",
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn acquire_namespace_ownership(delegated_root: &Path, namespace_name: &str) -> io::Result<File> {
    use std::os::unix::fs::MetadataExt;

    const LOCK_DIRECTORY: &str = "/tmp";
    let delegated_root = fs::canonicalize(delegated_root)?;
    let directory = fs::metadata(LOCK_DIRECTORY)?;
    if !directory.is_dir() || directory.mode() & 0o1002 != 0o1002 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "shared cgroup ownership lock directory is not a sticky world-writable directory",
        ));
    }

    let mut hash = 0xcbf29ce484222325_u64;
    for byte in delegated_root
        .as_os_str()
        .as_bytes()
        .iter()
        .copied()
        .chain(std::iter::once(0))
        .chain(namespace_name.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let path = Path::new(LOCK_DIRECTORY).join(format!("conductor-cgroup-{hash:016x}.lock"));
    let mut create = OpenOptions::new();
    create.write(true).read(true).create_new(true).mode(0o666);
    let (file, newly_created) = match create.open(&path) {
        Ok(file) => (file, true),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let mut open = OpenOptions::new();
            open.write(true).read(true);
            (open.open(&path)?, false)
        }
        Err(error) => return Err(error),
    };
    if newly_created {
        file.set_permissions(fs::Permissions::from_mode(0o666))?;
    }

    let metadata = file.metadata()?;
    let path_metadata = fs::symlink_metadata(&path)?;
    if !metadata.is_file()
        || !path_metadata.is_file()
        || metadata.dev() != path_metadata.dev()
        || metadata.ino() != path_metadata.ino()
        || metadata.mode() & 0o666 != 0o666
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "cgroup ownership lock is not a shared regular file",
        ));
    }
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "worker cgroup namespace is already owned by another agent",
        )),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

fn write_control(directory: &Path, name: &str, value: &str) -> io::Result<()> {
    fs::write(directory.join(name), value)
}

fn stable_run_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn is_cgroup2_mountpoint(path: &Path) -> io::Result<bool> {
    let mountinfo = fs::read_to_string("/proc/self/mountinfo")?;
    for line in mountinfo.lines() {
        let Some((mount, filesystem)) = line.split_once(" - ") else {
            continue;
        };
        if filesystem.split_ascii_whitespace().next() != Some("cgroup2") {
            continue;
        }
        let Some(mountpoint) = mount.split_ascii_whitespace().nth(4) else {
            continue;
        };
        let Some(mountpoint) = unescape_mount_path(mountpoint) else {
            continue;
        };
        if path == Path::new(&mountpoint) || path.starts_with(mountpoint) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn unescape_mount_path(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' && index + 3 < bytes.len() {
            let octal = std::str::from_utf8(&bytes[index + 1..index + 4]).ok()?;
            let value = u8::from_str_radix(octal, 8).ok()?;
            decoded.push(value);
            index += 4;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

#[cfg(test)]
mod ownership_tests {
    use super::acquire_namespace_ownership;
    use std::fs;
    use std::io;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn namespace_ownership_excludes_aliases_and_releases_after_drop() {
        let root = std::env::temp_dir().join(format!(
            "conductor-lock-test-{}-{}",
            std::process::id(),
            NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("create delegated root fixture");
        let alias = PathBuf::from(format!("{}-alias", root.display()));
        symlink_root(&root, &alias);

        let owner = acquire_namespace_ownership(&root, "conductor-worker")
            .expect("acquire first namespace owner");
        let alias_error = acquire_namespace_ownership(&alias, "conductor-worker")
            .expect_err("canonical alias must share the ownership lock");
        assert_eq!(alias_error.kind(), io::ErrorKind::AlreadyExists);
        let distinct = acquire_namespace_ownership(&root, "conductor-other-worker")
            .expect("distinct worker namespaces have independent locks");

        drop(owner);
        let recovered = acquire_namespace_ownership(&alias, "conductor-worker")
            .expect("OS lock must be released when the owner exits");
        drop((distinct, recovered));
        fs::remove_file(&alias).expect("remove delegated root alias");
        fs::remove_dir(&root).expect("remove delegated root fixture");
    }

    #[cfg(unix)]
    fn symlink_root(root: &std::path::Path, alias: &std::path::Path) {
        std::os::unix::fs::symlink(root, alias).expect("create delegated root alias");
    }
}
