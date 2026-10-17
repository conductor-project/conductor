//! Host process mechanics used only by the worker execution backend.

use std::io;
use std::process::{Child, Command, Stdio};

use conductor_core::JobSpec;
use conductor_worker::ExecutionBackend;

#[derive(Clone, Copy, Debug, Default)]
pub struct HostProcessBackend;

impl ExecutionBackend for HostProcessBackend {
    fn spawn(&self, job: &JobSpec) -> io::Result<Child> {
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

    fn kill_process_group(&self, group_id: u32) -> io::Result<()> {
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
}
