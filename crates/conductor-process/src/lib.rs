//! Host process mechanics used only by the worker execution backend.

#[cfg(target_os = "linux")]
mod cgroup;
#[cfg(target_os = "linux")]
mod landlock;

use std::io;
use std::process::{Child, Command, Stdio};

use conductor_core::{JobSpec, ResourceLimits};
use conductor_worker::ExecutionBackend;

#[cfg(target_os = "linux")]
pub use landlock::{LandlockGrantConfig, LandlockMode};

#[cfg(target_os = "linux")]
use std::collections::BTreeMap;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::sync::{Arc, Mutex};

#[cfg(target_os = "linux")]
use cgroup::CgroupV2;

#[derive(Clone, Debug, Default)]
pub struct HostProcessBackend {
    #[cfg(target_os = "linux")]
    cgroup: Option<CgroupBackend>,
    #[cfg(target_os = "linux")]
    landlock: Option<landlock::LandlockBackend>,
    #[cfg(target_os = "linux")]
    launcher: Option<PathBuf>,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct CgroupBackend {
    manager: CgroupV2,
    groups: Arc<Mutex<BTreeMap<u32, PathBuf>>>,
}

impl HostProcessBackend {
    /// Enable required cgroup v2 enforcement beneath an administrator-delegated root.
    #[cfg(target_os = "linux")]
    pub fn with_cgroup_v2(
        root: &Path,
        worker_id: &str,
        maximums: ResourceLimits,
        launcher: PathBuf,
    ) -> io::Result<Self> {
        Ok(Self {
            cgroup: Some(CgroupBackend {
                manager: CgroupV2::open(root, worker_id, maximums)?,
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

impl ExecutionBackend for HostProcessBackend {
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

fn spawn_host_process(job: &JobSpec) -> io::Result<Child> {
    let mut command = Command::new(&job.command()[0]);
    command
        .args(&job.command()[1..])
        .current_dir(job.cwd())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if !job.inherit_env() {
        command.env_clear();
    }
    command.envs(job.env());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    command.spawn()
}

#[cfg(target_os = "linux")]
fn spawn_sandboxed(
    cgroup: Option<&CgroupBackend>,
    landlock_grants: Option<Vec<landlock::LauncherGrant>>,
    launcher: &Path,
    run_id: &str,
    job: &JobSpec,
) -> io::Result<Child> {
    use std::io::Read;
    use std::os::unix::fs::DirBuilderExt;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::net::UnixListener;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    static NEXT_SOCKET: AtomicU64 = AtomicU64::new(1);
    let path = if let Some(cgroup) = cgroup {
        trace_cgroup_launch(run_id, "create_run_cgroup");
        let path = cgroup.manager.create(run_id, job.resource_limits())?;
        trace_cgroup_launch(run_id, "run_cgroup_created");
        Some(path)
    } else {
        None
    };
    let private_directory = std::env::temp_dir().join(format!(
        "conductor-launch-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        NEXT_SOCKET.fetch_add(1, Ordering::Relaxed)
    ));
    let mut directory_builder = std::fs::DirBuilder::new();
    directory_builder.mode(0o700);
    if let Err(error) = directory_builder.create(&private_directory) {
        if let Some(path) = &path {
            let _ = CgroupV2::kill(path);
            let _ = CgroupV2::remove_when_empty(path);
        }
        return Err(error);
    }
    let socket_path = private_directory.join("ready.sock");
    let policy_path = private_directory.join("landlock.json");
    let mut spawned: Option<Child> = None;
    let result = (|| {
        if let Some(grants) = &landlock_grants {
            let manifest = serde_json::to_vec(grants)
                .map_err(|_| io::Error::other("failed to encode Landlock grant manifest"))?;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&policy_path)?;
            use std::io::Write;
            file.write_all(&manifest)?;
        }
        trace_cgroup_launch(run_id, "bind_handshake_socket");
        let listener = UnixListener::bind(&socket_path)?;
        listener.set_nonblocking(true)?;
        let mut command = Command::new(launcher);
        command
            .arg("__internal-launch")
            .arg("--ready-socket")
            .arg(&socket_path);
        if let Some(path) = &path {
            command.arg("--cgroup-path").arg(path);
        }
        if landlock_grants.is_some() {
            command.arg("--landlock-policy-file").arg(&policy_path);
        }
        command
            .arg("--")
            .args(job.command())
            .current_dir(job.cwd())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !job.inherit_env() {
            command.env_clear();
        }
        command.envs(job.env());
        use std::os::unix::process::CommandExt;
        command.process_group(0);
        trace_cgroup_launch(run_id, "spawn_internal_launcher");
        spawned = Some(command.spawn()?);
        trace_cgroup_launch(run_id, "internal_launcher_spawned");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    trace_cgroup_launch(run_id, "launcher_connected");
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "cgroup launcher setup exceeded its deadline",
                        ));
                    }
                    stream.set_read_timeout(Some(remaining))?;
                    let mut response = [0; 6];
                    stream.read_exact(&mut response)?;
                    trace_cgroup_launch(run_id, "launcher_handshake_received");
                    if &response != b"READY\n" {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "cgroup launcher setup failed before workload execution",
                        ));
                    }
                    let _ = std::fs::remove_file(&socket_path);
                    if let (Some(cgroup), Some(path)) = (cgroup, &path) {
                        cgroup
                            .groups
                            .lock()
                            .map_err(|_| io::Error::other("cgroup registry is unavailable"))?
                            .insert(
                                spawned
                                    .as_ref()
                                    .ok_or_else(|| {
                                        io::Error::other("cgroup launcher child is missing")
                                    })?
                                    .id(),
                                path.clone(),
                            );
                        trace_cgroup_launch(run_id, "run_cgroup_registered");
                    }
                    return spawned
                        .take()
                        .ok_or_else(|| io::Error::other("cgroup launcher child is missing"));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
            if let Some(status) = spawned
                .as_mut()
                .ok_or_else(|| io::Error::other("cgroup launcher child is missing"))?
                .try_wait()?
            {
                return Err(io::Error::other(format!(
                    "cgroup launcher exited before setup completed ({})",
                    status.code().unwrap_or_default()
                )));
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "cgroup launcher setup exceeded its deadline",
                ));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    })();
    let _ = std::fs::remove_file(&socket_path);
    let _ = std::fs::remove_file(&policy_path);
    let _ = std::fs::remove_dir(&private_directory);
    if result.is_err() {
        if let Some(mut child) = spawned.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(path) = &path {
            let _ = CgroupV2::kill(path);
            let _ = CgroupV2::remove_when_empty(path);
        }
    }
    result
}

#[cfg(target_os = "linux")]
fn trace_cgroup_launch(run_id: &str, phase: &str) {
    if std::env::var_os("CONDUCTOR_CGROUP_TRACE").is_some() {
        eprintln!(
            "{{\"severity\":\"info\",\"component\":\"process\",\"event\":\"cgroup_launch_phase\",\"runId\":\"{run_id}\",\"phase\":\"{phase}\"}}"
        );
    }
}

/// Entered only by a fresh internal launcher before it executes user code.
#[cfg(target_os = "linux")]
pub fn internal_launch(
    cgroup_path: Option<&Path>,
    ready_socket: &Path,
    landlock_policy_file: Option<&Path>,
    command: &[String],
) -> i32 {
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    use std::os::unix::process::CommandExt;

    let grants = match landlock_policy_file
        .map(landlock::read_launcher_policy)
        .transpose()
    {
        Ok(grants) => grants,
        Err(_) => return 125,
    };
    let mut stream = match UnixStream::connect(ready_socket) {
        Ok(stream) => stream,
        Err(_) => return 125,
    };
    if cgroup_path.is_some_and(|path| CgroupV2::move_launcher(path).is_err()) {
        let _ = stream.write_all(b"ERROR\n");
        return 125;
    }
    if grants
        .as_deref()
        .is_some_and(|grants| landlock::apply_launcher_policy(grants).is_err())
    {
        let _ = stream.write_all(b"ERROR\n");
        return 125;
    }
    if stream.write_all(b"READY\n").is_err() {
        return 125;
    }
    drop(stream);
    if command.is_empty() {
        return 125;
    }
    let error = Command::new(&command[0]).args(&command[1..]).exec();
    eprintln!("workload exec failed ({:?})", error.kind());
    127
}

#[cfg(not(target_os = "linux"))]
pub fn internal_launch(
    _cgroup_path: Option<&std::path::Path>,
    _ready_socket: &std::path::Path,
    _landlock_policy_file: Option<&std::path::Path>,
    _command: &[String],
) -> i32 {
    125
}
