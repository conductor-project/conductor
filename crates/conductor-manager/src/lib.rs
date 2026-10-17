//! Manager-owned application operations and durable job-definition storage.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, io};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use conductor_core::{JobId, JobSpec};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};

/// Temporary limit for active jobs. Pagination is a later milestone.
pub const MAX_JOBS: usize = 1_000;
const MAX_RESERVED_JOB_IDS: usize = 10_000;
const SCHEMA_VERSION: i64 = 1;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobStoreError {
    DuplicateId,
    LimitExceeded,
    StorageUnavailable,
}

/// Storage port owned by the manager. Implementations preserve deleted IDs.
pub trait JobRepository: Send + Sync {
    fn insert(&self, job: JobSpec) -> Result<(), JobStoreError>;
    fn get(&self, id: &JobId) -> Result<Option<JobSpec>, JobStoreError>;
    fn list(&self) -> Result<Vec<JobSpec>, JobStoreError>;
    fn delete(&self, id: &JobId) -> Result<bool, JobStoreError>;
}

#[derive(Debug)]
pub enum RepositoryOpenError {
    Sqlite(rusqlite::Error),
    Io(io::Error),
    UnsupportedSchemaVersion(i64),
    InvalidLegacySchema,
    TimestampUnavailable,
}

impl std::fmt::Display for RepositoryOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(error) => write!(
                f,
                "manager database could not be opened or migrated: {error}"
            ),
            Self::Io(error) => write!(
                f,
                "manager database file could not be prepared securely: {error}"
            ),
            Self::UnsupportedSchemaVersion(version) => {
                write!(
                    f,
                    "manager database schema version {version} is newer than supported"
                )
            }
            Self::InvalidLegacySchema => {
                f.write_str("manager database has an invalid legacy schema")
            }
            Self::TimestampUnavailable => f.write_str("system time is unavailable"),
        }
    }
}

impl std::error::Error for RepositoryOpenError {}

/// Manager-owned SQLite repository. One serialized connection keeps access bounded and simple.
pub struct SqliteJobRepository {
    connection: Mutex<Connection>,
}

impl SqliteJobRepository {
    /// Open or create the manager database, enable durable local settings, and migrate it.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RepositoryOpenError> {
        prepare_database_file(path.as_ref()).map_err(RepositoryOpenError::Io)?;
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .map_err(RepositoryOpenError::Sqlite)?;
        connection
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .map_err(RepositoryOpenError::Sqlite)?;

        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(RepositoryOpenError::Sqlite)?;
        if version > SCHEMA_VERSION {
            return Err(RepositoryOpenError::UnsupportedSchemaVersion(version));
        }

        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(RepositoryOpenError::Sqlite)?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .map_err(RepositoryOpenError::Sqlite)?;
            let mode: String = connection
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .map_err(RepositoryOpenError::Sqlite)?;
            if !mode.eq_ignore_ascii_case("wal") {
                return Err(RepositoryOpenError::Sqlite(rusqlite::Error::InvalidQuery));
            }
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(RepositoryOpenError::Sqlite)?;
        migrate(&connection).map_err(|error| match error {
            MigrationError::Sqlite(error) => RepositoryOpenError::Sqlite(error),
            MigrationError::InvalidLegacySchema => RepositoryOpenError::InvalidLegacySchema,
        })?;

        Ok(Self {
            connection: Mutex::new(connection),
        })
    }
}

fn prepare_database_file(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "manager database path must be a regular file, not a symbolic link",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut options = fs::OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            options.mode(0o600);
            match options.open(path) {
                Ok(file) => drop(file),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "manager database path must be a regular file, not a symbolic link",
                ));
            }
        }
        Err(error) => return Err(error),
    }
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

enum MigrationError {
    Sqlite(rusqlite::Error),
    InvalidLegacySchema,
}

fn migrate(connection: &Connection) -> Result<(), MigrationError> {
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(MigrationError::Sqlite)?;
    if version == SCHEMA_VERSION {
        // Force a schema read so a falsely versioned/corrupt DB fails at startup.
        connection
            .prepare("SELECT id, spec_json, created_at_ms, deleted_at_ms FROM jobs LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        return Ok(());
    }
    if version != 0 {
        return Err(MigrationError::Sqlite(rusqlite::Error::InvalidQuery));
    }

    let transaction = connection
        .unchecked_transaction()
        .map_err(MigrationError::Sqlite)?;
    let table_exists: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'jobs')",
            [],
            |row| row.get(0),
        )
        .map_err(MigrationError::Sqlite)?;
    if !table_exists {
        let unexpected_tables: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%')",
                [],
                |row| row.get(0),
            )
            .map_err(MigrationError::Sqlite)?;
        if unexpected_tables {
            return Err(MigrationError::InvalidLegacySchema);
        }
        transaction
            .execute_batch(
                "CREATE TABLE jobs (
                    id TEXT PRIMARY KEY NOT NULL,
                    spec_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
                    deleted_at_ms INTEGER CHECK (deleted_at_ms IS NULL OR deleted_at_ms >= 0)
                );",
            )
            .map_err(MigrationError::Sqlite)?;
    } else {
        let columns = table_columns(&transaction).map_err(MigrationError::Sqlite)?;
        if !columns.iter().any(|column| column == "id")
            || !columns.iter().any(|column| column == "spec_json")
            || !columns.iter().any(|column| column == "created_at_ms")
        {
            return Err(MigrationError::InvalidLegacySchema);
        }
        if !columns.iter().any(|column| column == "deleted_at_ms") {
            transaction
                .execute(
                    "ALTER TABLE jobs ADD COLUMN deleted_at_ms INTEGER CHECK (deleted_at_ms IS NULL OR deleted_at_ms >= 0)",
                    [],
                )
                .map_err(MigrationError::Sqlite)?;
        }
    }
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(MigrationError::Sqlite)?;
    transaction.commit().map_err(MigrationError::Sqlite)
}

fn table_columns(connection: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare("PRAGMA table_info(jobs)")?;
    statement.query_map([], |row| row.get(1))?.collect()
}

impl JobRepository for SqliteJobRepository {
    fn insert(&self, job: JobSpec) -> Result<(), JobStoreError> {
        let spec_json =
            serde_json::to_string(&job).map_err(|_| JobStoreError::StorageUnavailable)?;
        let created_at_ms = now_ms().map_err(|_| JobStoreError::StorageUnavailable)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| JobStoreError::StorageUnavailable)?;

        let exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?1)",
                [job.id().as_str()],
                |row| row.get(0),
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        if exists {
            return Err(JobStoreError::DuplicateId);
        }
        let active_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE deleted_at_ms IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let reserved_count: i64 = transaction
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        if active_count >= MAX_JOBS as i64 || reserved_count >= MAX_RESERVED_JOB_IDS as i64 {
            return Err(JobStoreError::LimitExceeded);
        }
        transaction
            .execute(
                "INSERT INTO jobs (id, spec_json, created_at_ms, deleted_at_ms) VALUES (?1, ?2, ?3, NULL)",
                params![job.id().as_str(), spec_json, created_at_ms],
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| JobStoreError::StorageUnavailable)
    }

    fn get(&self, id: &JobId) -> Result<Option<JobSpec>, JobStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let serialized = connection
            .query_row(
                "SELECT spec_json FROM jobs WHERE id = ?1 AND deleted_at_ms IS NULL",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        serialized
            .map(|value| {
                serde_json::from_str(&value).map_err(|_| JobStoreError::StorageUnavailable)
            })
            .transpose()
    }

    fn list(&self) -> Result<Vec<JobSpec>, JobStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT spec_json FROM jobs WHERE deleted_at_ms IS NULL ORDER BY id COLLATE BINARY",
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        rows.map(|row| {
            let serialized = row.map_err(|_| JobStoreError::StorageUnavailable)?;
            serde_json::from_str(&serialized).map_err(|_| JobStoreError::StorageUnavailable)
        })
        .collect()
    }

    fn delete(&self, id: &JobId) -> Result<bool, JobStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let deleted_at_ms = now_ms().map_err(|_| JobStoreError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE jobs SET deleted_at_ms = ?1 WHERE id = ?2 AND deleted_at_ms IS NULL",
                params![deleted_at_ms, id.as_str()],
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        Ok(changed == 1)
    }
}

fn now_ms() -> Result<i64, RepositoryOpenError> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryOpenError::TimestampUnavailable)?
        .as_millis();
    i64::try_from(milliseconds).map_err(|_| RepositoryOpenError::TimestampUnavailable)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagerError {
    DuplicateJob,
    JobLimitReached,
    StorageUnavailable,
}

/// Manager application service for immutable job definitions.
pub struct JobService {
    repository: Arc<dyn JobRepository>,
}

impl JobService {
    pub fn new(repository: Arc<dyn JobRepository>) -> Self {
        Self { repository }
    }

    pub fn open_sqlite(path: impl AsRef<Path>) -> Result<Self, RepositoryOpenError> {
        Ok(Self::new(Arc::new(SqliteJobRepository::open(path)?)))
    }

    pub fn register(&self, job: JobSpec) -> Result<(), ManagerError> {
        match self.repository.insert(job) {
            Ok(()) => Ok(()),
            Err(JobStoreError::DuplicateId) => Err(ManagerError::DuplicateJob),
            Err(JobStoreError::LimitExceeded) => Err(ManagerError::JobLimitReached),
            Err(JobStoreError::StorageUnavailable) => Err(ManagerError::StorageUnavailable),
        }
    }

    pub fn get(&self, id: &JobId) -> Result<Option<JobSpec>, ManagerError> {
        self.repository.get(id).map_err(map_store_error)
    }

    pub fn list(&self) -> Result<Vec<JobSpec>, ManagerError> {
        self.repository.list().map_err(map_store_error)
    }

    pub fn delete(&self, id: &JobId) -> Result<bool, ManagerError> {
        self.repository.delete(id).map_err(map_store_error)
    }
}

fn map_store_error(error: JobStoreError) -> ManagerError {
    match error {
        JobStoreError::DuplicateId => ManagerError::DuplicateJob,
        JobStoreError::LimitExceeded => ManagerError::JobLimitReached,
        JobStoreError::StorageUnavailable => ManagerError::StorageUnavailable,
    }
}
