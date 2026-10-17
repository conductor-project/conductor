//! Host process mechanics used only by the worker execution backend.

#[cfg(target_os = "linux")]
mod cgroup;

use std::io;
use std::process::{Child, Command, Stdio};

use conductor_core::{JobSpec, ResourceLimits};
use conductor_worker::ExecutionBackend;

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
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct CgroupBackend {
    manager: CgroupV2,
    launcher: PathBuf,
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
                launcher,
                groups: Arc::new(Mutex::new(BTreeMap::new())),
            }),
        })
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
        #[cfg(not(target_os = "linux"))]
        let _ = run_id;
        #[cfg(target_os = "linux")]
        if let Some(cgroup) = &self.cgroup {
            return spawn_in_cgroup(cgroup, run_id, job);
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
        if let Some(cgroup) = &self.cgroup
            && let Some(path) = cgroup
                .groups
                .lock()
                .map_err(|_| io::Error::other("cgroup registry is unavailable"))?
                .get(&group_id)
        {
            CgroupV2::kill(path)?;
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
        if let Some(cgroup) = &self.cgroup
            && let Some(path) = cgroup
                .groups
                .lock()
                .map_err(|_| io::Error::other("cgroup registry is unavailable"))?
                .get(&group_id)
                .cloned()
        {
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
fn spawn_in_cgroup(cgroup: &CgroupBackend, run_id: &str, job: &JobSpec) -> io::Result<Child> {
    use std::io::Read;
    use std::os::unix::fs::DirBuilderExt;
    use std::os::unix::net::UnixListener;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    static NEXT_SOCKET: AtomicU64 = AtomicU64::new(1);
    let path = cgroup.manager.create(run_id, job.resource_limits())?;
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
        let _ = CgroupV2::kill(&path);
        let _ = CgroupV2::remove_when_empty(&path);
        return Err(error);
    }
    let socket_path = private_directory.join("ready.sock");
    let mut spawned: Option<Child> = None;
    let result = (|| {
        let listener = UnixListener::bind(&socket_path)?;
        listener.set_nonblocking(true)?;
        let mut command = Command::new(&cgroup.launcher);
        command
            .arg("__internal-launch")
            .arg("--cgroup-path")
            .arg(&path)
            .arg("--ready-socket")
            .arg(&socket_path)
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
        spawned = Some(command.spawn()?);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut response = [0; 128];
                    let length = stream.read(&mut response)?;
                    if &response[..length] != b"READY\n" {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "cgroup launcher setup failed before workload execution",
                        ));
                    }
                    let _ = std::fs::remove_file(&socket_path);
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
    let _ = std::fs::remove_dir(&private_directory);
    if result.is_err() {
        if let Some(mut child) = spawned.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = CgroupV2::kill(&path);
        let _ = CgroupV2::remove_when_empty(&path);
    }
    result
}

/// Entered only by a fresh internal launcher before it executes user code.
#[cfg(target_os = "linux")]
pub fn internal_launch(cgroup_path: &Path, ready_socket: &Path, command: &[String]) -> i32 {
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    use std::os::unix::process::CommandExt;

    let setup = CgroupV2::move_launcher(cgroup_path);
    let mut stream = match UnixStream::connect(ready_socket) {
        Ok(stream) => stream,
        Err(_) => return 125,
    };
    if setup.is_err() {
        let _ = stream.write_all(b"ERROR\n");
        return 125;
    }
    if stream.write_all(b"READY\n").is_err() {
        return 125;
    }
    if command.is_empty() {
        return 125;
    }
    let error = Command::new(&command[0]).args(&command[1..]).exec();
    eprintln!("workload exec failed ({:?})", error.kind());
    127
}

#[cfg(not(target_os = "linux"))]
pub fn internal_launch(
    _cgroup_path: &std::path::Path,
    _ready_socket: &std::path::Path,
    _command: &[String],
) -> i32 {
    125
}
