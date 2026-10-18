use std::fs::{self, DirBuilder, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

/// Holds OS-backed locks for both local state directories for the agent lifetime.
pub struct StateLocks {
    _manager: File,
    _worker: File,
}

impl StateLocks {
    pub fn acquire(manager_dir: &Path, worker_dir: &Path) -> io::Result<Self> {
        let manager_dir = prepare_directory(manager_dir)?;
        let worker_dir = prepare_directory(worker_dir)?;
        if manager_dir == worker_dir {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "manager_data_dir and worker_data_dir resolve to the same directory",
            ));
        }
        let manager = acquire_directory_lock(&manager_dir)?;
        let worker = acquire_directory_lock(&worker_dir)?;
        Ok(Self {
            _manager: manager,
            _worker: worker,
        })
    }
}

pub(super) fn prepare_directory(path: &Path) -> io::Result<PathBuf> {
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(path)?;
    let metadata = fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "state path must be a directory",
        ));
    }
    let canonical = fs::canonicalize(path)?;
    #[cfg(unix)]
    fs::set_permissions(&canonical, fs::Permissions::from_mode(0o700))?;
    Ok(canonical)
}

pub(super) fn write_secret_file_once(path: &Path, secret: &[u8]) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;
    if let Err(error) = file.write_all(secret).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(error);
    }
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(())
}

pub(super) fn open_secret_file_no_follow(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        use nix::fcntl::{OFlag, open};
        use nix::sys::stat::Mode;

        let descriptor = open(
            path,
            OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(io::Error::from)?;
        Ok(File::from(descriptor))
    }
    #[cfg(not(unix))]
    {
        File::open(path)
    }
}

fn acquire_directory_lock(directory: &Path) -> io::Result<File> {
    let path = directory.join(".conductor.lock");
    if fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "state lock path must not be a symbolic link",
        ));
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options.open(path)?;
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "state directory is already in use by another agent",
        )),
        Err(TryLockError::Error(error)) => Err(error),
    }
}
