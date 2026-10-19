//! Offline backup creation, verification, and restore.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};
use rusqlite::{Connection, OpenFlags, backup::Backup};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tar::{Archive, Builder, EntryType, Header};

use crate::state_lock::StateLocks;

const MANIFEST_PATH: &str = "manifest.json";
const MAX_ARCHIVE_ENTRIES: usize = 100_000;
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Args)]
pub struct BackupArgs {
    #[command(subcommand)]
    command: BackupCommand,
}

#[derive(Debug, Subcommand)]
enum BackupCommand {
    /// Create a consistent offline snapshot of manager/worker state and optional files.
    Create {
        #[arg(long)]
        manager_data_dir: Option<PathBuf>,
        #[arg(long)]
        worker_data_dir: Option<PathBuf>,
        /// Configuration or identity files/directories to include under included/.
        #[arg(long = "include-path")]
        include_paths: Vec<PathBuf>,
        /// New output tar file. Existing paths are never replaced.
        #[arg(long)]
        output: PathBuf,
    },
    /// Validate the manifest and every file checksum without extracting the archive.
    Inspect {
        #[arg(long)]
        input: PathBuf,
    },
    /// Restore into a new directory. Manager restores require explicit recovery clearance.
    Restore {
        #[arg(long)]
        input: PathBuf,
        /// New destination directory. Existing paths are never replaced.
        #[arg(long)]
        destination: PathBuf,
    },
    /// Allow a restored manager to start after workers are fenced and unknown outcomes reviewed.
    RecoveryResolve {
        #[arg(long)]
        manager_data_dir: Option<PathBuf>,
        #[arg(long)]
        worker_data_dir: Option<PathBuf>,
        #[arg(long, required = true)]
        confirm_workers_fenced: bool,
        #[arg(long, required = true)]
        confirm_unknown_outcomes_reviewed: bool,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BackupManifest {
    format_version: u8,
    created_at_unix_seconds: u64,
    binary_version: String,
    manager_included: bool,
    manager_schema_version: Option<i64>,
    worker_included: bool,
    worker_schema_version: Option<i64>,
    files: Vec<FileRecord>,
    included_sources: Vec<IncludedSource>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FileRecord {
    path: String,
    size: u64,
    sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IncludedSource {
    archive_root: String,
    source_path: String,
}

struct SourceFile {
    archive_path: String,
    source_path: PathBuf,
}

struct TempDir(PathBuf);

impl TempDir {
    fn create(parent: &Path, prefix: &str) -> io::Result<Self> {
        for _ in 0..32 {
            let nonce = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(
                ".{prefix}-{}-{}-{nonce}",
                std::process::id(),
                timestamp_seconds().unwrap_or_default()
            ));
            match fs::create_dir(&path) {
                Ok(()) => {
                    set_directory_mode(&path)?;
                    return Ok(Self(path));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "temporary backup path could not be allocated",
        ))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub fn run(args: BackupArgs) -> ExitCode {
    let result = match args.command {
        BackupCommand::Create {
            manager_data_dir,
            worker_data_dir,
            include_paths,
            output,
        } => create_backup(manager_data_dir, worker_data_dir, include_paths, &output),
        BackupCommand::Inspect { input } => inspect_backup(&input).map(|manifest| {
            println!(
                "Backup format v{}: manager={}, worker={}, files={}",
                manifest.format_version,
                manifest.manager_included,
                manifest.worker_included,
                manifest.files.len()
            );
            for source in manifest.included_sources {
                let escaped = source
                    .source_path
                    .chars()
                    .flat_map(char::escape_default)
                    .collect::<String>();
                println!("Included {escaped} at {}", source.archive_root);
            }
        }),
        BackupCommand::Restore { input, destination } => restore_backup(&input, &destination),
        BackupCommand::RecoveryResolve {
            manager_data_dir,
            worker_data_dir,
            confirm_workers_fenced,
            confirm_unknown_outcomes_reviewed,
        } => resolve_recovery(
            manager_data_dir.as_deref(),
            worker_data_dir.as_deref(),
            confirm_workers_fenced,
            confirm_unknown_outcomes_reviewed,
        ),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => {
            eprintln!(
                "conductor backup: operation failed; verify paths, permissions, and archive integrity"
            );
            ExitCode::FAILURE
        }
    }
}

fn create_backup(
    manager_data_dir: Option<PathBuf>,
    worker_data_dir: Option<PathBuf>,
    include_paths: Vec<PathBuf>,
    output: &Path,
) -> io::Result<()> {
    if manager_data_dir.is_none() && worker_data_dir.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "at least one state directory is required",
        ));
    }
    for directory in [&manager_data_dir, &worker_data_dir].into_iter().flatten() {
        if !fs::symlink_metadata(directory)?.is_dir() {
            return Err(invalid("state directory must already exist as a directory"));
        }
    }
    ensure_new_file_destination(output)?;
    let _locks = match (&manager_data_dir, &worker_data_dir) {
        (Some(manager), Some(worker)) => StateLocks::acquire(manager, worker)?,
        (Some(manager), None) => StateLocks::acquire_manager(manager)?,
        (None, Some(worker)) => StateLocks::acquire_worker(worker)?,
        (None, None) => unreachable!(),
    };
    let state_directories = [&manager_data_dir, &worker_data_dir]
        .into_iter()
        .flatten()
        .map(|path| path.canonicalize())
        .collect::<io::Result<Vec<_>>>()?;

    let output_parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let scratch = TempDir::create(&std::env::temp_dir(), "conductor-backup")?;
    let mut source_files = Vec::new();
    let manager_schema_version = if let Some(directory) = manager_data_dir.as_deref() {
        Some(collect_state_directory(
            directory,
            "manager",
            "manager.sqlite3",
            &scratch.0,
            &mut source_files,
        )?)
    } else {
        None
    };
    let worker_schema_version = if let Some(directory) = worker_data_dir.as_deref() {
        Some(collect_state_directory(
            directory,
            "worker",
            "worker.sqlite3",
            &scratch.0,
            &mut source_files,
        )?)
    } else {
        None
    };

    let mut included_sources = Vec::new();
    for (index, path) in include_paths.iter().enumerate() {
        if fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(invalid("included path must not be a symbolic link"));
        }
        let canonical = path.canonicalize()?;
        if state_directories
            .iter()
            .any(|state| canonical.starts_with(state) || state.starts_with(&canonical))
        {
            return Err(invalid(
                "included paths must not overlap manager or worker state",
            ));
        }
        let archive_root = format!("included/{index:04}");
        let metadata = fs::symlink_metadata(&canonical)?;
        let archive_path = if metadata.is_file() {
            let name = canonical
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| invalid("included path contains a non-UTF-8 name"))?;
            format!("{archive_root}/{name}")
        } else {
            archive_root.clone()
        };
        included_sources.push(IncludedSource {
            archive_root: archive_root.clone(),
            source_path: canonical
                .to_str()
                .ok_or_else(|| invalid("included path is not valid UTF-8"))?
                .to_owned(),
        });
        collect_tree(&canonical, &archive_path, &mut source_files)?;
    }
    source_files.sort_by(|left, right| left.archive_path.cmp(&right.archive_path));

    let files = source_files
        .iter()
        .map(record_file)
        .collect::<io::Result<Vec<_>>>()?;
    let manifest = BackupManifest {
        format_version: 1,
        created_at_unix_seconds: timestamp_seconds()?,
        binary_version: env!("CARGO_PKG_VERSION").to_owned(),
        manager_included: manager_data_dir.is_some(),
        manager_schema_version,
        worker_included: worker_data_dir.is_some(),
        worker_schema_version,
        files,
        included_sources,
    };

    let archive_temp = unique_file_path(output_parent, "conductor-backup")?;
    let result = write_archive(&archive_temp, &source_files, &manifest)
        .and_then(|()| inspect_backup(&archive_temp).map(|_| ()))
        .and_then(|()| fs::hard_link(&archive_temp, output));
    let _ = fs::remove_file(&archive_temp);
    result?;
    sync_directory(output_parent)?;
    println!("Backup created and verified: {}", output.display());
    Ok(())
}

fn collect_state_directory(
    directory: &Path,
    archive_root: &str,
    database_name: &str,
    scratch: &Path,
    files: &mut Vec<SourceFile>,
) -> io::Result<i64> {
    let directory = directory.canonicalize()?;
    let database = directory.join(database_name);
    if !fs::symlink_metadata(&database)?.is_file() {
        return Err(invalid("database must be a regular file"));
    }
    let snapshot = scratch.join(format!("{archive_root}-snapshot.sqlite3"));
    let schema_version = snapshot_database(&database, &snapshot)?;
    files.push(SourceFile {
        archive_path: format!("{archive_root}/{database_name}"),
        source_path: snapshot,
    });

    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| invalid("state path contains a non-UTF-8 name"))?;
        if name == ".conductor.lock"
            || name == database_name
            || name == format!("{database_name}-wal")
            || name == format!("{database_name}-shm")
        {
            continue;
        }
        collect_tree(&entry.path(), &format!("{archive_root}/{name}"), files)?;
    }
    Ok(schema_version)
}

fn collect_tree(path: &Path, archive_path: &str, files: &mut Vec<SourceFile>) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(invalid("backup input contains a symbolic link"));
    }
    if metadata.is_file() {
        if path
            .file_name()
            .is_some_and(|name| name == ".conductor.lock")
        {
            return Ok(());
        }
        files.push(SourceFile {
            archive_path: archive_path.to_owned(),
            source_path: path.to_path_buf(),
        });
        return Ok(());
    }
    if !metadata.is_dir() {
        return Err(invalid(
            "backup input contains a non-regular filesystem object",
        ));
    }
    let mut entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| invalid("backup path contains a non-UTF-8 name"))?;
        collect_tree(&entry.path(), &format!("{archive_path}/{name}"), files)?;
    }
    Ok(())
}

fn snapshot_database(source: &Path, destination: &Path) -> io::Result<i64> {
    let resolved = source.canonicalize()?;
    let connection = Connection::open_with_flags(
        resolved,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(|_| invalid("database could not be opened for consistent backup"))?;
    let mut output = Connection::open(destination)
        .map_err(|_| invalid("database snapshot could not be created"))?;
    let backup = Backup::new(&connection, &mut output)
        .map_err(|_| invalid("database snapshot could not be created"))?;
    backup
        .run_to_completion(128, Duration::from_millis(10), None)
        .map_err(|_| invalid("database snapshot could not be created"))?;
    drop(backup);
    drop(output);
    let resolved_destination = destination.canonicalize()?;
    let snapshot = Connection::open_with_flags(
        resolved_destination,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(|_| invalid("database snapshot could not be verified"))?;
    let integrity: String = snapshot
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|_| invalid("database snapshot could not be verified"))?;
    if integrity != "ok" {
        return Err(invalid("database snapshot integrity check failed"));
    }
    let version: i64 = snapshot
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|_| invalid("database schema version could not be inspected"))?;
    set_file_mode(destination)?;
    Ok(version)
}

fn record_file(file: &SourceFile) -> io::Result<FileRecord> {
    let mut source = crate::state_lock::open_secret_file_no_follow(&file.source_path)?;
    let metadata = source.metadata()?;
    if !metadata.is_file() {
        return Err(invalid("backup input is not a regular file"));
    }
    let size = metadata.len();
    let digest = hash_reader(&mut source)?;
    Ok(FileRecord {
        path: file.archive_path.clone(),
        size,
        sha256: hex_digest(&digest),
    })
}

fn write_archive(
    path: &Path,
    source_files: &[SourceFile],
    manifest: &BackupManifest,
) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let output = options.open(path)?;
    let mut archive = Builder::new(output);
    for source in source_files {
        let mut file = crate::state_lock::open_secret_file_no_follow(&source.source_path)?;
        let metadata = file.metadata()?;
        let mut header = Header::new_gnu();
        header.set_path(&source.archive_path)?;
        header.set_size(metadata.len());
        header.set_mode(file_mode(&metadata));
        header.set_mtime(
            metadata
                .modified()
                .ok()
                .and_then(unix_seconds)
                .unwrap_or_default(),
        );
        header.set_entry_type(EntryType::Regular);
        header.set_cksum();
        archive.append_data(&mut header, &source.archive_path, &mut file)?;
    }
    let manifest_json = serde_json::to_vec(manifest)
        .map_err(|_| invalid("backup manifest could not be encoded"))?;
    let mut header = Header::new_gnu();
    header.set_path(MANIFEST_PATH)?;
    header.set_size(manifest_json.len() as u64);
    header.set_mode(0o600);
    header.set_mtime(manifest.created_at_unix_seconds);
    header.set_entry_type(EntryType::Regular);
    header.set_cksum();
    archive.append_data(&mut header, MANIFEST_PATH, manifest_json.as_slice())?;
    let file = archive.into_inner()?;
    file.sync_all()?;
    Ok(())
}

fn inspect_backup(path: &Path) -> io::Result<BackupManifest> {
    let mut source = crate::state_lock::open_secret_file_no_follow(path)?;
    let size = source.metadata()?.len();
    inspect_archive(&mut source, size)
}

fn inspect_archive(
    source: &mut (impl Read + Seek),
    archive_size: u64,
) -> io::Result<BackupManifest> {
    source.seek(SeekFrom::Start(0))?;
    let mut archive = Archive::new(source);
    let mut observed = BTreeMap::<String, (u64, String)>::new();
    let mut manifest_bytes = None;
    let mut total_size = 0_u64;
    let mut entries = archive.entries()?;
    for entry in &mut entries {
        let mut entry = entry?;
        let entry_type = entry.header().entry_type();
        if !entry_type.is_file() {
            return Err(invalid(
                "backup archive contains a link or non-regular entry",
            ));
        }
        let path = entry
            .path()
            .map_err(|_| invalid("backup archive contains an invalid path"))?
            .into_owned();
        let name = safe_archive_path(&path)?;
        if name == MANIFEST_PATH {
            if manifest_bytes.is_some() || entry.size() > 4 * 1024 * 1024 {
                return Err(invalid("backup manifest is duplicated or too large"));
            }
            let mut bytes = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut bytes)?;
            manifest_bytes = Some(bytes);
            continue;
        }
        if observed.len() >= MAX_ARCHIVE_ENTRIES {
            return Err(invalid("backup archive contains too many files"));
        }
        total_size = total_size
            .checked_add(entry.size())
            .filter(|size| *size <= archive_size)
            .ok_or_else(|| invalid("backup archive entry sizes exceed the archive file"))?;
        let size = entry.size();
        let digest = hash_reader(&mut entry)?;
        if observed.insert(name, (size, hex_digest(&digest))).is_some() {
            return Err(invalid("backup archive contains duplicate paths"));
        }
    }
    let bytes = manifest_bytes.ok_or_else(|| invalid("backup manifest is missing"))?;
    let manifest: BackupManifest =
        serde_json::from_slice(&bytes).map_err(|_| invalid("backup manifest is invalid"))?;
    if manifest.format_version != 1 || manifest.files.len() != observed.len() {
        return Err(invalid("backup format or file list is unsupported"));
    }
    let mut expected = BTreeSet::new();
    for record in &manifest.files {
        let safe = safe_archive_path(Path::new(&record.path))?;
        if !expected.insert(safe.clone())
            || observed.get(&safe) != Some(&(record.size, record.sha256.clone()))
            || !(safe.starts_with("manager/")
                || safe.starts_with("worker/")
                || safe.starts_with("included/"))
        {
            return Err(invalid("backup file list or checksum does not match"));
        }
    }
    if manifest.manager_included != expected.iter().any(|path| path.starts_with("manager/"))
        || manifest.worker_included != expected.iter().any(|path| path.starts_with("worker/"))
        || (manifest.manager_included && !expected.contains("manager/manager.sqlite3"))
        || (manifest.worker_included && !expected.contains("worker/worker.sqlite3"))
        || manifest.manager_included != manifest.manager_schema_version.is_some()
        || manifest.worker_included != manifest.worker_schema_version.is_some()
        || manifest.manager_schema_version.is_some_and(|version| {
            !(0..=conductor_manager::CURRENT_SCHEMA_VERSION).contains(&version)
        })
        || manifest.worker_schema_version.is_some_and(|version| {
            !(0..=conductor_worker::CURRENT_SCHEMA_VERSION).contains(&version)
        })
    {
        return Err(invalid(
            "backup component markers do not match its contents",
        ));
    }
    for source in &manifest.included_sources {
        let root = safe_archive_path(Path::new(&source.archive_root))?;
        if !root.starts_with("included/") || !source.source_path.starts_with('/') {
            return Err(invalid("backup included-source metadata is invalid"));
        }
    }
    Ok(manifest)
}

fn restore_backup(input: &Path, destination: &Path) -> io::Result<()> {
    let mut source = crate::state_lock::open_secret_file_no_follow(input)?;
    source.try_lock()?;
    let archive_size = source.metadata()?.len();
    let manifest = inspect_archive(&mut source, archive_size)?;
    ensure_new_directory_destination(destination)?;
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging = TempDir::create(parent, "conductor-restore")?;
    source.seek(SeekFrom::Start(0))?;
    let mut archive = Archive::new(source);
    archive.set_preserve_permissions(false);
    archive.unpack(&staging.0)?;
    normalize_tree(&staging.0)?;
    if manifest.manager_included
        && read_user_version(&staging.0.join("manager/manager.sqlite3"))?
            != manifest.manager_schema_version.unwrap_or_default()
    {
        return Err(invalid(
            "manager database does not match the backup manifest",
        ));
    }
    if manifest.worker_included
        && read_user_version(&staging.0.join("worker/worker.sqlite3"))?
            != manifest.worker_schema_version.unwrap_or_default()
    {
        return Err(invalid(
            "worker database does not match the backup manifest",
        ));
    }

    let _locks = match (manifest.manager_included, manifest.worker_included) {
        (true, true) => Some(StateLocks::acquire(
            &staging.0.join("manager"),
            &staging.0.join("worker"),
        )?),
        (true, false) => Some(StateLocks::acquire_manager(&staging.0.join("manager"))?),
        (false, true) => Some(StateLocks::acquire_worker(&staging.0.join("worker"))?),
        (false, false) => None,
    };
    if manifest.manager_included {
        write_recovery_marker(&staging.0.join("manager"))?;
    }
    if manifest.worker_included {
        write_recovery_marker(&staging.0.join("worker"))?;
    }
    fs::create_dir(destination)?;
    set_directory_mode(destination)?;
    if let Err(error) = fs::rename(&staging.0, destination) {
        let _ = fs::remove_dir(destination);
        return Err(error);
    }
    sync_directory(parent)?;
    println!("Backup restored to {}", destination.display());
    if manifest.manager_included {
        println!("Manager startup is held until backup recovery is resolved.");
    }
    Ok(())
}

fn write_recovery_marker(manager_directory: &Path) -> io::Result<()> {
    let path = manager_directory.join(".conductor-recovery-required");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(b"restored state requires worker fencing and unknown-outcome review\n")?;
    file.sync_all()
}

fn read_user_version(path: &Path) -> io::Result<i64> {
    let resolved = path.canonicalize()?;
    let connection = Connection::open_with_flags(
        resolved,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(|_| invalid("restored database could not be opened"))?;
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|_| invalid("restored database integrity check failed"))?;
    if integrity != "ok" {
        return Err(invalid("restored database integrity check failed"));
    }
    connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|_| invalid("restored database schema version could not be read"))
}

fn resolve_recovery(
    manager_data_dir: Option<&Path>,
    worker_data_dir: Option<&Path>,
    confirm_workers_fenced: bool,
    confirm_unknown_outcomes_reviewed: bool,
) -> io::Result<()> {
    if (!confirm_workers_fenced || !confirm_unknown_outcomes_reviewed)
        || (manager_data_dir.is_none() && worker_data_dir.is_none())
    {
        return Err(invalid("recovery confirmations are required"));
    }
    let _locks = match (manager_data_dir, worker_data_dir) {
        (Some(manager), Some(worker)) => StateLocks::acquire(manager, worker)?,
        (Some(manager), None) => StateLocks::acquire_manager(manager)?,
        (None, Some(worker)) => StateLocks::acquire_worker(worker)?,
        (None, None) => return Err(invalid("at least one restored state directory is required")),
    };
    let directories = [manager_data_dir, worker_data_dir]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let markers = directories
        .iter()
        .map(|directory| directory.join(".conductor-recovery-required"))
        .collect::<Vec<_>>();
    for marker in &markers {
        let metadata = fs::symlink_metadata(marker)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(invalid("recovery marker is not a regular file"));
        }
    }
    for marker in markers {
        fs::remove_file(marker)?;
    }
    for directory in directories {
        sync_directory(directory)?;
    }
    println!("Recovery hold cleared after operator confirmation.");
    Ok(())
}

fn normalize_tree(root: &Path) -> io::Result<()> {
    let mut entries = fs::read_dir(root)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(invalid("restored tree contains a symbolic link"));
        }
        if metadata.is_dir() {
            normalize_tree(&path)?;
            set_directory_mode(&path)?;
        } else if metadata.is_file() {
            set_file_mode(&path)?;
        } else {
            return Err(invalid(
                "restored tree contains a non-regular filesystem object",
            ));
        }
    }
    set_directory_mode(root)
}

fn safe_archive_path(path: &Path) -> io::Result<String> {
    if path.as_os_str().len() > 4096 {
        return Err(invalid("backup archive path is too long"));
    }
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid("backup archive contains an unsafe path"));
    }
    let value = path
        .to_str()
        .ok_or_else(|| invalid("backup archive contains a non-UTF-8 path"))?
        .replace('\\', "/");
    if value.starts_with('/')
        || value
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(invalid("backup archive contains an unsafe path"));
    }
    Ok(value)
}

fn hash_reader(reader: &mut impl Read) -> io::Result<[u8; 32]> {
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hash.finalize().into())
}

fn hex_digest(digest: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn file_mode(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o700
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0o600
    }
}

fn set_file_mode(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn set_directory_mode(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn ensure_new_file_destination(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !fs::symlink_metadata(parent)?.is_dir() || fs::symlink_metadata(path).is_ok() {
        return Err(invalid(
            "output must have an existing parent and must not already exist",
        ));
    }
    Ok(())
}

fn ensure_new_directory_destination(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !fs::symlink_metadata(parent)?.is_dir() || fs::symlink_metadata(path).is_ok() {
        return Err(invalid(
            "destination must have an existing parent and must not already exist",
        ));
    }
    Ok(())
}

fn unique_file_path(parent: &Path, prefix: &str) -> io::Result<PathBuf> {
    for _ in 0..32 {
        let nonce = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(".{prefix}-{}-{nonce}.tmp", std::process::id()));
        if fs::symlink_metadata(&path).is_err() {
            return Ok(path);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "temporary path could not be allocated",
    ))
}

fn timestamp_seconds() -> io::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| invalid("system clock is before the Unix epoch"))
}

fn unix_seconds(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

pub(crate) fn ensure_no_recovery_hold(state_directory: &Path) -> io::Result<()> {
    let marker = state_directory.join(".conductor-recovery-required");
    match fs::symlink_metadata(marker) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(invalid("state recovery marker is invalid"))
        }
        Ok(_) => Err(invalid(
            "restored state requires worker fencing and unknown-outcome review",
        )),
        Err(error) => Err(error),
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
