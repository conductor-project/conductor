use super::*;

#[derive(Clone, Debug, Default)]
pub struct HostProcessBackend {
    #[cfg(target_os = "linux")]
    pub(super) cgroup: Option<CgroupBackend>,
    #[cfg(target_os = "linux")]
    pub(super) landlock: Option<landlock::LandlockBackend>,
    #[cfg(target_os = "linux")]
    pub(super) launcher: Option<PathBuf>,
}

impl ExecutionBackend for HostProcessBackend {
    fn enforcement_capabilities(
        &self,
    ) -> std::collections::BTreeSet<conductor_core::EnforcementCapability> {
        #[allow(unused_mut)]
        let mut capabilities = std::collections::BTreeSet::new();
        #[cfg(target_os = "linux")]
        {
            if self.cgroup.is_some() {
                capabilities.insert(conductor_core::EnforcementCapability::CgroupV2);
            }
            if self
                .landlock
                .as_ref()
                .is_some_and(landlock::LandlockBackend::supports_enforcement)
            {
                capabilities.insert(conductor_core::EnforcementCapability::Landlock);
            }
        }
        capabilities
    }

    fn spawn(&self, run_id: &str, job: &JobSpec) -> io::Result<Child> {
        #[cfg(target_os = "linux")]
        {
            let landlock_grants = self
                .landlock
                .as_ref()
                .map(|policy| policy.launcher_grants(job.filesystem_policy()))
                .transpose()?
                .flatten();
            if self.cgroup.is_some() || landlock_grants.is_some() {
                if self.cgroup.is_none() && !job.resource_limits().is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "job resource limits require enabled cgroup v2 enforcement",
                    ));
                }
                let launcher = self
                    .launcher
                    .as_deref()
                    .ok_or_else(|| io::Error::other("internal launcher is not configured"))?;
                return spawn_sandboxed(
                    self.cgroup.as_ref(),
                    landlock_grants,
                    launcher,
                    run_id,
                    job,
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = run_id;
            if job.filesystem_policy().is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "filesystem policy requires Landlock on Linux",
                ));
            }
        }
        if !job.resource_limits().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "job resource limits require enabled cgroup v2 enforcement",
            ));
        }
        spawn_host_process(job)
    }

    fn terminate_process_group(&self, group_id: u32) -> io::Result<()> {
        #[cfg(unix)]
        {
            use nix::sys::signal::{Signal, killpg};
            use nix::unistd::Pid;

            match killpg(Pid::from_raw(group_id as i32), Signal::SIGTERM) {
                Ok(()) => Ok(()),
                Err(nix::errno::Errno::ESRCH) => Ok(()),
                Err(error) => Err(io::Error::from_raw_os_error(error as i32)),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = group_id;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "process groups are not supported on this platform",
            ))
        }
    }

    fn kill_process_group(&self, group_id: u32) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            let path = cgroup
                .groups
                .lock()
                .map_err(|_| io::Error::other("cgroup registry is unavailable"))?
                .get(&group_id)
                .cloned();
            if let Some(path) = path {
                CgroupV2::kill(&path)?;
            }
        }
        #[cfg(unix)]
        {
            use nix::errno::Errno;
            use nix::sys::signal::{Signal, killpg};
            use nix::unistd::Pid;

            let group_id = i32::try_from(group_id).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid process group")
            })?;
            match killpg(Pid::from_raw(group_id), Signal::SIGKILL) {
                Ok(()) | Err(Errno::ESRCH) => Ok(()),
                Err(error) => Err(io::Error::from_raw_os_error(error as i32)),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = group_id;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "process group cleanup is supported only on Unix",
            ))
        }
    }

    fn cleanup_execution(&self, group_id: u32) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            // Clone the path before touching cgroupfs. Kernel cleanup may block,
            // and holding the registry lock would prevent another run from
            // completing its launcher handshake and registering ownership.
            let path = cgroup
                .groups
                .lock()
                .map_err(|_| io::Error::other("cgroup registry is unavailable"))?
                .get(&group_id)
                .cloned();
            if let Some(path) = path {
                match CgroupV2::kill(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        cgroup
                            .groups
                            .lock()
                            .map_err(|_| io::Error::other("cgroup registry is unavailable"))?
                            .remove(&group_id);
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
                match CgroupV2::remove_when_empty(&path) {
                    Ok(()) => {
                        cgroup
                            .groups
                            .lock()
                            .map_err(|_| io::Error::other("cgroup registry is unavailable"))?
                            .remove(&group_id);
                        return Ok(());
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        cgroup
                            .groups
                            .lock()
                            .map_err(|_| io::Error::other("cgroup registry is unavailable"))?
                            .remove(&group_id);
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        let _ = group_id;
        Ok(())
    }
}

impl HostProcessBackend {
    /// Enable required cgroup v2 enforcement beneath an administrator-delegated root.
    #[cfg(target_os = "linux")]
    pub fn with_cgroup_v2(
        root: &Path,
        lock_directory: &Path,
        worker_id: &str,
        maximums: ResourceLimits,
        launcher: PathBuf,
    ) -> io::Result<Self> {
        Ok(Self {
            cgroup: Some(CgroupBackend {
                manager: CgroupV2::open(root, lock_directory, worker_id, maximums)?,
                groups: Arc::new(Mutex::new(BTreeMap::new())),
            }),
            landlock: None,
            launcher: Some(launcher),
        })
    }

    /// Configure administrator-approved Landlock grants and launch enforcement mode.
    #[cfg(target_os = "linux")]
    pub fn with_landlock(
        mut self,
        mode: LandlockMode,
        grants: Vec<LandlockGrantConfig>,
        protected_paths: &[PathBuf],
        launcher: PathBuf,
    ) -> io::Result<Self> {
        self.landlock = Some(landlock::LandlockBackend::open(
            mode,
            grants,
            protected_paths,
        )?);
        self.launcher = Some(launcher);
        Ok(self)
    }

    /// Cgroup enforcement is available only on Linux.
    #[cfg(not(target_os = "linux"))]
    pub fn with_cgroup_v2(
        _root: &std::path::Path,
        _lock_directory: &std::path::Path,
        _worker_id: &str,
        _maximums: ResourceLimits,
        _launcher: std::path::PathBuf,
    ) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "cgroup v2 enforcement is available only on Linux",
        ))
    }
}
