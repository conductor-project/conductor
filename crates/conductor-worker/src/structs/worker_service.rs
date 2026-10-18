use super::*;

pub struct WorkerService {
    pub(super) worker_id: WorkerId,
    pub(super) capacity: usize,
    pub(super) inventory: WorkerInventory,
    pub(super) incarnation: u64,
    pub(super) state_directory: PathBuf,
    pub(super) log_policy: LogPolicy,
    pub(super) backend: Arc<dyn ExecutionBackend>,
    pub(super) state: Arc<SharedWorkerState>,
    pub(super) pending: Arc<PendingPersistence>,
    pub(super) persistence_admission: Arc<Semaphore>,
    pub(super) shutdown_signal: watch::Sender<bool>,
    pub(super) reader_reaper: ReaderReaper,
    pub(super) cleanup_admission: Arc<Semaphore>,
    pub(super) runtime: Runtime,
    pub(super) supervisors: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    pub(super) lifecycle: Mutex<()>,
    pub(super) draining: AtomicBool,
}

impl WorkerService {
    pub fn open(
        state_directory: impl AsRef<Path>,
        worker_id: WorkerId,
        capacity: usize,
        backend: Arc<dyn ExecutionBackend>,
    ) -> io::Result<Self> {
        Self::open_with_log_policy(
            state_directory,
            worker_id,
            capacity,
            LogPolicy::default(),
            backend,
        )
    }

    pub fn open_with_log_policy(
        state_directory: impl AsRef<Path>,
        worker_id: WorkerId,
        capacity: usize,
        log_policy: LogPolicy,
        backend: Arc<dyn ExecutionBackend>,
    ) -> io::Result<Self> {
        let state_directory = state_directory.as_ref().to_path_buf();
        if capacity == 0 || capacity > MAX_WORKER_CAPACITY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker capacity must be between 1 and 64",
            ));
        }
        let inventory = host_inventory(capacity, backend.as_ref())?;
        if log_policy.max_bytes_per_stream == 0
            || log_policy.max_bytes_per_stream > MAX_LOG_MAX_BYTES_PER_STREAM
            || log_policy.retention_runs > MAX_LOG_RETENTION_RUNS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker log limits are outside the supported range",
            ));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_io()
            .enable_time()
            .build()?;
        let log_directory = state_directory.join("runs");
        match fs::symlink_metadata(&log_directory) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "worker run storage path must be a real directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&log_directory)?;
            }
            Err(error) => return Err(error),
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&log_directory, fs::Permissions::from_mode(0o700))?;
        }
        let database_path = state_directory.join("worker.sqlite3");
        match fs::symlink_metadata(&database_path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "worker database path must be a regular file",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                #[cfg(unix)]
                options.mode(0o600);
                match options.open(&database_path) {
                    Ok(file) => drop(file),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }
        let database_metadata = fs::symlink_metadata(&database_path)?;
        if database_metadata.file_type().is_symlink() || !database_metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker database path must be a regular file",
            ));
        }
        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            database_file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        drop(database_file);
        let connection = Connection::open_with_flags(
            database_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .map_err(storage_error)?;
        connection
            .busy_timeout(BUSY_TIMEOUT)
            .map_err(storage_error)?;
        let schema_version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(storage_error)?;
        if !(0..=SCHEMA_VERSION).contains(&schema_version) {
            let message = if schema_version > SCHEMA_VERSION {
                format!(
                    "worker database schema version {schema_version} is newer than supported version {SCHEMA_VERSION}"
                )
            } else {
                format!("worker database schema version {schema_version} is invalid")
            };
            return Err(io::Error::new(io::ErrorKind::InvalidData, message));
        }
        if schema_version == 0 {
            let existing_tables: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%')",
                    [],
                    |row| row.get(0),
                )
                .map_err(storage_error)?;
            if existing_tables {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "worker database has an unsupported legacy schema",
                ));
            }
            let transaction = connection.unchecked_transaction().map_err(storage_error)?;
            transaction
                .execute_batch(
                    "CREATE TABLE runs (
                    run_id TEXT PRIMARY KEY NOT NULL,
                    spec_json TEXT NOT NULL,
                    observation_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK (revision > 0)
                );
                CREATE TABLE run_logs (
                    run_id TEXT PRIMARY KEY NOT NULL REFERENCES runs(run_id),
                    stdout_closed INTEGER NOT NULL DEFAULT 0 CHECK (stdout_closed IN (0, 1)),
                    stderr_closed INTEGER NOT NULL DEFAULT 0 CHECK (stderr_closed IN (0, 1)),
                    stdout_truncated INTEGER NOT NULL DEFAULT 0 CHECK (stdout_truncated IN (0, 1)),
                    stderr_truncated INTEGER NOT NULL DEFAULT 0 CHECK (stderr_truncated IN (0, 1)),
                    pruned INTEGER NOT NULL DEFAULT 0 CHECK (pruned IN (0, 1)),
                    completed_at_ms INTEGER
                );",
                )
                .map_err(storage_error)?;
            transaction
                .pragma_update(None, "user_version", 2)
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
        } else if schema_version == 1 {
            let transaction = connection.unchecked_transaction().map_err(storage_error)?;
            transaction
                .execute_batch(
                    "CREATE TABLE run_logs (
                    run_id TEXT PRIMARY KEY NOT NULL REFERENCES runs(run_id),
                    stdout_closed INTEGER NOT NULL DEFAULT 1 CHECK (stdout_closed IN (0, 1)),
                    stderr_closed INTEGER NOT NULL DEFAULT 1 CHECK (stderr_closed IN (0, 1)),
                    stdout_truncated INTEGER NOT NULL DEFAULT 0 CHECK (stdout_truncated IN (0, 1)),
                    stderr_truncated INTEGER NOT NULL DEFAULT 0 CHECK (stderr_truncated IN (0, 1)),
                    pruned INTEGER NOT NULL DEFAULT 0 CHECK (pruned IN (0, 1)),
                    completed_at_ms INTEGER
                );
                INSERT INTO run_logs (run_id) SELECT run_id FROM runs;
                PRAGMA user_version = 2;",
                )
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
        } else {
            connection
                .prepare(
                    "SELECT run_id, spec_json, observation_json, state, revision FROM runs LIMIT 0",
                )
                .map_err(storage_error)?;
        }
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(storage_error)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(storage_error)?;

        let incarnation = {
            let transaction = connection.unchecked_transaction().map_err(storage_error)?;
            transaction
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS worker_meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    incarnation INTEGER NOT NULL CHECK (incarnation >= 0)
                );
                INSERT OR IGNORE INTO worker_meta (id, incarnation) VALUES (1, 0);
                CREATE TABLE IF NOT EXISTS worker_commands (
                    command_id TEXT PRIMARY KEY NOT NULL,
                    worker_id TEXT NOT NULL,
                    worker_incarnation INTEGER NOT NULL CHECK (worker_incarnation >= 0),
                    command_json TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('accepted', 'completed', 'acknowledged')),
                    result_json TEXT,
                    report_sequence INTEGER CHECK (report_sequence IS NULL OR report_sequence > 0),
                    accepted_at_ms INTEGER NOT NULL,
                    CHECK ((state = 'accepted' AND result_json IS NULL AND report_sequence IS NULL)
                        OR (state IN ('completed', 'acknowledged') AND result_json IS NOT NULL AND report_sequence IS NOT NULL)),
                    UNIQUE (worker_incarnation, report_sequence)
                );
                CREATE INDEX IF NOT EXISTS worker_commands_pending_reports
                    ON worker_commands (worker_incarnation, report_sequence)
                    WHERE state = 'completed';
                CREATE TABLE IF NOT EXISTS worker_report_state (
                    incarnation INTEGER NOT NULL CHECK (incarnation >= 0),
                    sequence INTEGER NOT NULL CHECK (sequence >= 0),
                    PRIMARY KEY (incarnation)
                );
                INSERT OR IGNORE INTO worker_report_state (incarnation, sequence) VALUES (0, 0);
                PRAGMA user_version = 4;",
                )
                .map_err(storage_error)?;
            let changed = transaction
                .execute(
                    "UPDATE worker_meta SET incarnation = incarnation + 1
                     WHERE id = 1 AND incarnation < 9223372036854775807",
                    [],
                )
                .map_err(storage_error)?;
            if changed == 0 {
                return Err(io::Error::other("worker incarnation is exhausted"));
            }
            let generation = transaction
                .query_row(
                    "SELECT incarnation FROM worker_meta WHERE id = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT OR IGNORE INTO worker_report_state (incarnation, sequence) VALUES (?1, 0)",
                    [generation],
                )
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
            u64::try_from(generation)
                .map_err(|_| io::Error::other("worker incarnation is invalid"))?
        };

        let service = Self {
            worker_id,
            capacity,
            inventory,
            incarnation,
            state_directory,
            log_policy,
            backend,
            state: Arc::new(SharedWorkerState(Mutex::new(WorkerState {
                connection,
                active_groups: BTreeMap::new(),
            }))),
            pending: Arc::new(PendingPersistence {
                observations: Mutex::new(BTreeMap::new()),
                shutdown_exhausted: Mutex::new(BTreeSet::new()),
                degraded: AtomicBool::new(false),
            }),
            persistence_admission: Arc::new(Semaphore::new(PERSISTENCE_BLOCKING_CAPACITY)),
            shutdown_signal: watch::channel(false).0,
            reader_reaper: ReaderReaper::new()?,
            cleanup_admission: Arc::new(Semaphore::new(capacity)),
            runtime,
            supervisors: Mutex::new(Vec::new()),
            lifecycle: Mutex::new(()),
            draining: AtomicBool::new(false),
        };
        service.mark_interrupted_runs_unknown()?;
        service.enforce_legacy_log_limits()?;
        Ok(service)
    }

    pub fn start(&self, command: StartRun) -> Result<RunObservation, WorkerError> {
        let _lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        if self.draining.load(Ordering::SeqCst) {
            return Err(WorkerError::ShuttingDown);
        }
        let spec = command.spec();
        if spec.worker_id() != &self.worker_id {
            return Err(WorkerError::WorkerMismatch);
        }
        let spec_json = serde_json::to_string(spec).map_err(|_| WorkerError::StorageUnavailable)?;
        let now = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let (observation, cleanup_permit) = {
            let mut state = self
                .state
                .0
                .lock()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            let transaction = state
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|_| WorkerError::StorageUnavailable)?;
            let existing = transaction
                .query_row(
                    "SELECT spec_json, observation_json FROM runs WHERE run_id = ?1",
                    [spec.id().as_str()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            if let Some((stored_spec, stored_observation)) = existing {
                if stored_spec != spec_json {
                    return Err(WorkerError::RunConflict);
                }
                return serde_json::from_str(&stored_observation)
                    .map_err(|_| WorkerError::StorageUnavailable);
            }
            if self.is_degraded() {
                return Err(WorkerError::StorageUnavailable);
            }
            let cleanup_permit = Arc::clone(&self.cleanup_admission)
                .try_acquire_owned()
                .map_err(|_| WorkerError::CapacityReached)?;
            let active_count: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM runs WHERE state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
                [],
                |row| row.get(0),
            ).map_err(|_| WorkerError::StorageUnavailable)?;
            if active_count >= self.capacity as i64 {
                return Err(WorkerError::CapacityReached);
            }
            let observation =
                RunObservation::requested(spec, now).with_worker_incarnation(self.incarnation);
            let observation_json =
                serde_json::to_string(&observation).map_err(|_| WorkerError::StorageUnavailable)?;
            transaction.execute(
                "INSERT INTO runs (run_id, spec_json, observation_json, state, revision) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![spec.id().as_str(), spec_json, observation_json, observation.state().as_str(), observation.revision() as i64],
            ).map_err(|_| WorkerError::StorageUnavailable)?;
            transaction
                .execute(
                    "INSERT INTO run_logs (run_id) VALUES (?1)",
                    [spec.id().as_str()],
                )
                .map_err(|_| WorkerError::StorageUnavailable)?;
            transaction
                .commit()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            (observation, cleanup_permit)
        };

        let starting = observation
            .next_revision(RunState::Starting, None, None, None, None, None)
            .ok_or(WorkerError::StorageUnavailable)?;
        self.persist(&starting)?;

        let stdout_path = self
            .state_directory
            .join("runs")
            .join(format!("{}.stdout", spec.id()));
        let stderr_path = self
            .state_directory
            .join("runs")
            .join(format!("{}.stderr", spec.id()));
        let stdout_file = match prepare_log_file(&stdout_path) {
            Ok(file) => file,
            Err(_) => {
                self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                return self.finish_backend_failure(starting);
            }
        };
        let stderr_file = match prepare_log_file(&stderr_path) {
            Ok(file) => file,
            Err(_) => {
                drop(stdout_file);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                return self.finish_backend_failure(starting);
            }
        };

        if let Some(expected) = spec.job().executable_digest() {
            match executable_digest_matches(Path::new(&spec.job().command()[0]), expected.value()) {
                Ok(true) => {}
                Ok(false) => {
                    drop(stdout_file);
                    drop(stderr_file);
                    self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                    self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                    return self
                        .finish_failure(starting, RunFailureReason::ExecutableDigestMismatch);
                }
                Err(_) => {
                    drop(stdout_file);
                    drop(stderr_file);
                    self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                    self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                    return self.finish_backend_failure(starting);
                }
            }
        }

        let job = match spec.job().clone().with_environment_overrides(
            spec.service_ports()
                .iter()
                .map(|port| (port.environment_name(), port.port().to_string())),
        ) {
            Ok(job) => job,
            Err(_) => {
                self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                return self.finish_backend_failure(starting);
            }
        };
        let child = match self.backend.spawn(spec.id().as_str(), &job) {
            Ok(child) => child,
            Err(error) => {
                emit_worker_diagnostic("execution_launch_failed", spec.id().as_str(), &error);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                return self.finish_spawn_failure(starting);
            }
        };
        let launched_at = std::time::Instant::now();
        let group_id = child.id();
        let mut spawned = SpawnGuard {
            backend: Arc::clone(&self.backend),
            reader_reaper: self.reader_reaper.clone(),
            group_id,
            child: Some(child),
            readers: Vec::with_capacity(2),
            cleanup_permit: Some(cleanup_permit),
            cancel_readers: Arc::new(AtomicBool::new(false)),
            cleaned: false,
        };
        let Some(stdout) = spawned.child.as_mut().and_then(|child| child.stdout.take()) else {
            return self.abort_startup(spawned, starting);
        };
        let Some(stderr) = spawned.child.as_mut().and_then(|child| child.stderr.take()) else {
            return self.abort_startup(spawned, starting);
        };
        let stdout_reader = match spawn_capture(
            stdout,
            stdout_file,
            self.log_policy.max_bytes_per_stream,
            Arc::clone(&self.state),
            spec.id().to_string(),
            LogStream::Stdout,
            Arc::clone(&spawned.cancel_readers),
        ) {
            Ok(reader) => reader,
            Err(_) => return self.abort_startup(spawned, starting),
        };
        spawned.readers.push(stdout_reader);
        let stderr_reader = match spawn_capture(
            stderr,
            stderr_file,
            self.log_policy.max_bytes_per_stream,
            Arc::clone(&self.state),
            spec.id().to_string(),
            LogStream::Stderr,
            Arc::clone(&spawned.cancel_readers),
        ) {
            Ok(reader) => reader,
            Err(_) => return self.abort_startup(spawned, starting),
        };
        spawned.readers.push(stderr_reader);
        let started = match now_ms() {
            Ok(started) => started,
            Err(_) => return self.abort_startup(spawned, starting),
        };
        let running = starting
            .next_revision(RunState::Running, Some(started), None, None, None, None)
            .map(|observation| {
                if spec.readiness_probe().is_some() {
                    observation.with_readiness(ReadinessState::Starting)
                } else {
                    observation
                }
            })
            .ok_or(WorkerError::StorageUnavailable);
        let running = match running {
            Ok(running) => running,
            Err(_) => return self.abort_startup(spawned, starting),
        };
        if self.persist(&running).is_err() {
            let _ = self.abort_startup(spawned, running);
            return Err(WorkerError::StorageUnavailable);
        }
        let control_notify = Arc::new(Notify::new());
        {
            let mut state = match self.state.0.lock() {
                Ok(state) => state,
                Err(_) => {
                    let _ = self.abort_startup(spawned, running);
                    return Err(WorkerError::StorageUnavailable);
                }
            };
            state.active_groups.insert(
                spec.id().to_string(),
                ActiveGroup {
                    group_id,
                    intent: Arc::new(AtomicU8::new(INTENT_NONE)),
                    notify: Arc::clone(&control_notify),
                },
            );
        }

        let state = Arc::clone(&self.state);
        let pending = Arc::clone(&self.pending);
        let persistence_admission = Arc::clone(&self.persistence_admission);
        let shutdown_signal = self.shutdown_signal.subscribe();
        let backend = Arc::clone(&self.backend);
        let run_id = spec.id().to_string();
        let control = match self.state.0.lock() {
            Ok(state) => state
                .active_groups
                .get(spec.id().as_str())
                .map(|active| (Arc::clone(&active.intent), Arc::clone(&active.notify)))
                .ok_or(WorkerError::StorageUnavailable),
            Err(_) => Err(WorkerError::StorageUnavailable),
        };
        let (control, control_notify) = match control {
            Ok(control) => control,
            Err(_) => {
                if let Ok(mut state) = self.state.0.lock() {
                    state.active_groups.remove(spec.id().as_str());
                }
                let _ = self.abort_startup(spawned, running);
                return Err(WorkerError::StorageUnavailable);
            }
        };
        let timeout = spec.job().timeout_ms().map(Duration::from_millis);
        let grace = Duration::from_millis(spec.job().termination_grace_ms());
        let log_directory = self.state_directory.join("runs");
        let retention_runs = self.log_policy.retention_runs;
        let supervisor_observation = running.clone();
        let supervisor = self.runtime.spawn(supervise(RunSupervision {
            state,
            pending,
            persistence_admission,
            shutdown_signal,
            backend,
            run_id,
            spawned,
            current: supervisor_observation,
            control,
            control_notify,
            launched_at,
            timeout,
            grace,
            log_directory,
            retention_runs,
            readiness_probe: spec.readiness_probe().cloned(),
            service_ports: spec.service_ports().to_vec(),
        }));
        let mut supervisors = self
            .supervisors
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        supervisors.retain(|old| !old.is_finished());
        supervisors.push(supervisor);
        Ok(running)
    }

    pub fn inspect(&self, command: InspectRun) -> Result<Option<RunObservation>, WorkerError> {
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        state
            .connection
            .query_row(
                "SELECT observation_json FROM runs WHERE run_id = ?1",
                [command.run_id().as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?
            .map(|json| serde_json::from_str(&json).map_err(|_| WorkerError::StorageUnavailable))
            .transpose()
    }

    pub fn read_logs(&self, command: ReadRunLogs) -> Result<RunLogChunk, WorkerError> {
        if command.limit() == 0 || command.limit() > MAX_LOG_CHUNK_BYTES {
            return Err(WorkerError::StorageUnavailable);
        }
        let id = command.run_id();
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let run_exists: bool = state
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runs WHERE run_id = ?1)",
                [id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        if !run_exists {
            return Err(WorkerError::LogsNotFound);
        }
        let log_state: Option<(bool, bool, bool)> = state
            .connection
            .query_row(
                "SELECT CASE WHEN ?2 = 'stdout' THEN stdout_closed ELSE stderr_closed END,
                        CASE WHEN ?2 = 'stdout' THEN stdout_truncated ELSE stderr_truncated END,
                        pruned
                 FROM run_logs WHERE run_id = ?1",
                params![id.as_str(), command.stream().as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let (closed, truncated, pruned) = log_state.ok_or(WorkerError::LogsNotFound)?;
        drop(state);
        if pruned {
            return Err(WorkerError::LogsPruned);
        }
        let path = self.log_path(id.as_str(), command.stream());
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(WorkerError::LogsNotFound);
            }
            Err(_) => return Err(WorkerError::StorageUnavailable),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(WorkerError::StorageUnavailable);
        }
        let file_len = metadata.len();
        let mut file = File::open(path).map_err(|_| WorkerError::StorageUnavailable)?;
        file.seek(SeekFrom::Start(command.offset()))
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let mut data = Vec::with_capacity(
            command
                .limit()
                .min(file_len.saturating_sub(command.offset()) as usize),
        );
        file.take(command.limit() as u64)
            .read_to_end(&mut data)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let next_offset = command.offset().saturating_add(data.len() as u64);
        let eof = closed && next_offset >= file_len;
        Ok(RunLogChunk::new(
            id.clone(),
            command.stream(),
            command.offset(),
            next_offset,
            data,
            RunLogStatus::new(closed, eof, truncated),
        ))
    }

    pub fn stop(&self, command: StopRun) -> Result<RunObservation, WorkerError> {
        let id = command.run_id();
        let current = self
            .inspect(InspectRun::new(id.clone()))?
            .ok_or(WorkerError::RunConflict)?;
        if current.state().is_terminal() {
            return Ok(current);
        }
        let active = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?
            .active_groups
            .get(id.as_str())
            .cloned();
        let Some(active) = active else {
            // No persisted PID/group is authority after recovery. Keep the run unresolved.
            return Ok(current);
        };
        let stopping = if current.state() == RunState::Stopping {
            current
        } else {
            current
                .next_revision(
                    RunState::Stopping,
                    current.started_at_ms(),
                    None,
                    None,
                    None,
                    None,
                )
                .ok_or(WorkerError::StorageUnavailable)?
                .with_termination_reason(RunTerminationReason::Cancelled)
        };
        let stopping = match self.persist(&stopping) {
            Ok(()) => stopping,
            Err(error) => match self.inspect(InspectRun::new(id.clone()))? {
                Some(latest)
                    if latest.state() == RunState::Stopping || latest.state().is_terminal() =>
                {
                    latest
                }
                _ => return Err(error),
            },
        };
        if stopping.state().is_terminal() {
            return Ok(stopping);
        }
        let _ = active.intent.compare_exchange(
            INTENT_NONE,
            INTENT_CANCEL,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        active.notify.notify_one();
        Ok(stopping)
    }

    /// Resolve an ambiguous run after the operator has verified external cleanup.
    pub fn resolve_unknown(&self, command: ResolveRun) -> Result<RunObservation, WorkerError> {
        let id = command.run_id();
        let mut state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let raw = transaction
            .query_row(
                "SELECT observation_json FROM runs WHERE run_id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?
            .ok_or(WorkerError::RunConflict)?;
        let current: RunObservation =
            serde_json::from_str(&raw).map_err(|_| WorkerError::StorageUnavailable)?;
        if current.state() == RunState::Abandoned {
            if current.resolution_reason() == Some(command.reason()) {
                transaction
                    .commit()
                    .map_err(|_| WorkerError::StorageUnavailable)?;
                return Ok(current);
            }
            return Err(WorkerError::RunConflict);
        }
        if current.state() != RunState::Unknown
            || command.reason().trim().is_empty()
            || command.reason().len() > 512
        {
            return Err(WorkerError::RunConflict);
        }
        let finished = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let resolved = current
            .next_revision(
                RunState::Abandoned,
                current.started_at_ms(),
                Some(finished),
                None,
                None,
                None,
            )
            .ok_or(WorkerError::StorageUnavailable)?
            .with_resolution_reason(command.reason().to_owned());
        let json = serde_json::to_string(&resolved).map_err(|_| WorkerError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE runs SET observation_json = ?1, state = ?2, revision = ?3 WHERE run_id = ?4 AND revision = ?5",
                params![json, resolved.state().as_str(), resolved.revision() as i64, id.as_str(), current.revision() as i64],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE run_logs SET stdout_closed = 1, stderr_closed = 1, completed_at_ms = ?1 WHERE run_id = ?2",
                params![finished, id.as_str()],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        Ok(resolved)
    }

    /// Durably accepts a command before its side effect is attempted. A report
    /// returned here is the immutable result of an earlier delivery of the
    /// same command ID and contents.
    pub fn accept_command(
        &self,
        envelope: &CommandEnvelope,
    ) -> Result<Option<StoredWorkerReport>, WorkerError> {
        if envelope.target_worker_id != self.worker_id
            || envelope.target_worker_incarnation != self.incarnation
        {
            return Err(WorkerError::WorkerMismatch);
        }
        let command_json =
            serde_json::to_string(envelope).map_err(|_| WorkerError::StorageUnavailable)?;
        if command_json.len() > 128 * 1024 {
            return Err(WorkerError::StorageUnavailable);
        }
        let mut state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let existing = transaction
            .query_row(
                "SELECT command_json, state, result_json, report_sequence,
                        worker_id, worker_incarnation
                 FROM worker_commands WHERE command_id = ?1",
                [envelope.command_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        if let Some((stored_json, status, result_json, sequence, worker_id, incarnation)) = existing
        {
            if stored_json != command_json
                || worker_id != self.worker_id.as_str()
                || u64::try_from(incarnation).ok() != Some(self.incarnation)
            {
                return Err(WorkerError::RunConflict);
            }
            let report = match (result_json, sequence) {
                (Some(result_json), Some(sequence))
                    if matches!(status.as_str(), "completed" | "acknowledged") =>
                {
                    Some(StoredWorkerReport {
                        command_id: envelope.command_id.clone(),
                        worker_id: self.worker_id.clone(),
                        worker_incarnation: self.incarnation,
                        report_sequence: u64::try_from(sequence)
                            .map_err(|_| WorkerError::StorageUnavailable)?,
                        result_json,
                    })
                }
                (None, None) if status == "accepted" => None,
                _ => return Err(WorkerError::StorageUnavailable),
            };
            transaction
                .commit()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            return Ok(report);
        }
        transaction
            .execute(
                "INSERT INTO worker_commands
                    (command_id, worker_id, worker_incarnation, command_json,
                     state, accepted_at_ms)
                 VALUES (?1, ?2, ?3, ?4, 'accepted', ?5)",
                params![
                    envelope.command_id.as_str(),
                    self.worker_id.as_str(),
                    i64::try_from(self.incarnation).map_err(|_| WorkerError::StorageUnavailable)?,
                    command_json,
                    now_ms().map_err(|_| WorkerError::StorageUnavailable)?
                ],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        Ok(None)
    }

    /// Commits the command result and its monotonically increasing report
    /// sequence in one SQLite transaction.
    pub fn complete_command(
        &self,
        envelope: &CommandEnvelope,
        result_json: &str,
    ) -> Result<StoredWorkerReport, WorkerError> {
        if result_json.len() > 128 * 1024
            || serde_json::from_str::<serde_json::Value>(result_json).is_err()
        {
            return Err(WorkerError::StorageUnavailable);
        }
        let command_json =
            serde_json::to_string(envelope).map_err(|_| WorkerError::StorageUnavailable)?;
        let mut state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let existing = transaction
            .query_row(
                "SELECT command_json, state, result_json, report_sequence,
                        worker_id, worker_incarnation
                 FROM worker_commands WHERE command_id = ?1",
                [envelope.command_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?
            .ok_or(WorkerError::RunConflict)?;
        if existing.0 != command_json
            || existing.4 != self.worker_id.as_str()
            || u64::try_from(existing.5).ok() != Some(envelope.target_worker_incarnation)
        {
            return Err(WorkerError::RunConflict);
        }
        if matches!(existing.1.as_str(), "completed" | "acknowledged") {
            let (Some(result_json), Some(sequence)) = (existing.2, existing.3) else {
                return Err(WorkerError::StorageUnavailable);
            };
            transaction
                .commit()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            return Ok(StoredWorkerReport {
                command_id: envelope.command_id.clone(),
                worker_id: self.worker_id.clone(),
                worker_incarnation: envelope.target_worker_incarnation,
                report_sequence: u64::try_from(sequence)
                    .map_err(|_| WorkerError::StorageUnavailable)?,
                result_json,
            });
        }
        if existing.1 != "accepted" || existing.2.is_some() || existing.3.is_some() {
            return Err(WorkerError::StorageUnavailable);
        }
        let stored_incarnation = i64::try_from(envelope.target_worker_incarnation)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let sequence: i64 = transaction
            .query_row(
                "SELECT sequence FROM worker_report_state WHERE incarnation = ?1",
                [stored_incarnation],
                |row| row.get(0),
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(WorkerError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE worker_report_state SET sequence = ?1
                 WHERE incarnation = ?2 AND sequence = ?3",
                params![next_sequence, stored_incarnation, sequence],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE worker_commands
                 SET state = 'completed', result_json = ?1, report_sequence = ?2
                 WHERE command_id = ?3 AND state = 'accepted'",
                params![result_json, next_sequence, envelope.command_id.as_str()],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        if changed != 1 {
            return Err(WorkerError::StorageUnavailable);
        }
        transaction
            .commit()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        Ok(StoredWorkerReport {
            command_id: envelope.command_id.clone(),
            worker_id: self.worker_id.clone(),
            worker_incarnation: envelope.target_worker_incarnation,
            report_sequence: u64::try_from(next_sequence)
                .map_err(|_| WorkerError::StorageUnavailable)?,
            result_json: result_json.to_owned(),
        })
    }

    /// Returns every completed report not yet acknowledged by the manager for
    /// this worker incarnation, in sequence order.
    pub fn pending_reports(&self) -> Result<Vec<StoredWorkerReport>, WorkerError> {
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let mut statement = state
            .connection
            .prepare(
                "SELECT command_id, worker_id, worker_incarnation, report_sequence, result_json
                 FROM worker_commands
                 WHERE state = 'completed'
                 ORDER BY worker_incarnation, report_sequence",
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(|_| WorkerError::StorageUnavailable)?;
        rows.map(|row| {
            let (command_id, worker_id, incarnation, sequence, result_json) =
                row.map_err(|_| WorkerError::StorageUnavailable)?;
            Ok(StoredWorkerReport {
                command_id: CommandId::new(command_id)
                    .map_err(|_| WorkerError::StorageUnavailable)?,
                worker_id: WorkerId::new(worker_id).map_err(|_| WorkerError::StorageUnavailable)?,
                worker_incarnation: u64::try_from(incarnation)
                    .map_err(|_| WorkerError::StorageUnavailable)?,
                report_sequence: u64::try_from(sequence)
                    .map_err(|_| WorkerError::StorageUnavailable)?,
                result_json,
            })
        })
        .collect()
    }

    /// Read one bounded page of durable run history. The first page captures a
    /// rowid high-water mark; later pages remain bounded to that snapshot.
    pub fn inventory_page(
        &self,
        snapshot_generation: Option<u64>,
        after_sequence: u64,
        limit: usize,
    ) -> Result<WorkerInventoryPage, WorkerError> {
        if limit == 0 || limit > MAX_WORKER_INVENTORY_PAGE_SIZE {
            return Err(WorkerError::StorageUnavailable);
        }
        if snapshot_generation.is_none() && after_sequence != 0 {
            return Err(WorkerError::StorageUnavailable);
        }
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let generation = match snapshot_generation {
            Some(generation) => generation,
            None => {
                let highest: i64 = state
                    .connection
                    .query_row("SELECT COALESCE(MAX(rowid), 0) FROM runs", [], |row| {
                        row.get(0)
                    })
                    .map_err(|_| WorkerError::StorageUnavailable)?;
                u64::try_from(highest).map_err(|_| WorkerError::StorageUnavailable)?
            }
        };
        if after_sequence > generation {
            return Err(WorkerError::StorageUnavailable);
        }
        let after = i64::try_from(after_sequence).map_err(|_| WorkerError::StorageUnavailable)?;
        let generation_i64 =
            i64::try_from(generation).map_err(|_| WorkerError::StorageUnavailable)?;
        let read_limit = i64::try_from(limit + 1).map_err(|_| WorkerError::StorageUnavailable)?;
        let mut statement = state
            .connection
            .prepare(
                "SELECT rowid, observation_json FROM runs
                 WHERE rowid > ?1 AND rowid <= ?2 ORDER BY rowid LIMIT ?3",
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let rows = statement
            .query_map(params![after, generation_i64, read_limit], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let mut entries = rows
            .map(|row| {
                let (sequence, observation) = row.map_err(|_| WorkerError::StorageUnavailable)?;
                Ok(WorkerInventoryEntry {
                    sequence: u64::try_from(sequence)
                        .map_err(|_| WorkerError::StorageUnavailable)?,
                    observation: serde_json::from_str(&observation)
                        .map_err(|_| WorkerError::StorageUnavailable)?,
                })
            })
            .collect::<Result<Vec<_>, WorkerError>>()?;
        let more = entries.len() > limit;
        if more {
            entries.pop();
        }
        let next_after_sequence = more
            .then(|| entries.last().map(|entry| entry.sequence))
            .flatten();
        let page = WorkerInventoryPage {
            worker_id: self.worker_id.clone(),
            worker_incarnation: self.incarnation,
            snapshot_generation: generation,
            after_sequence,
            entries,
            next_after_sequence,
        };
        if !page.is_valid() {
            return Err(WorkerError::StorageUnavailable);
        }
        Ok(page)
    }

    /// Returns accepted commands whose result has not yet been committed. The
    /// transport reconciles them before polling for new work.
    pub fn accepted_commands(&self) -> Result<Vec<CommandEnvelope>, WorkerError> {
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let mut statement = state
            .connection
            .prepare(
                "SELECT command_json FROM worker_commands
                 WHERE state = 'accepted' ORDER BY accepted_at_ms, command_id",
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|_| WorkerError::StorageUnavailable)?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|_| WorkerError::StorageUnavailable)?)
                .map_err(|_| WorkerError::StorageUnavailable)
        })
        .collect()
    }

    /// Records the manager's report acknowledgement so a restart does not
    /// resend a completed report unnecessarily.
    pub fn acknowledge_report(
        &self,
        command_id: &CommandId,
        worker_incarnation: u64,
        report_sequence: u64,
    ) -> Result<(), WorkerError> {
        let mut state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let record = transaction
            .query_row(
                "SELECT state, worker_incarnation, report_sequence
                 FROM worker_commands WHERE command_id = ?1",
                [command_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?
            .ok_or(WorkerError::RunConflict)?;
        if u64::try_from(record.1).ok() != Some(worker_incarnation)
            || record.2.and_then(|value| u64::try_from(value).ok()) != Some(report_sequence)
        {
            return Err(WorkerError::RunConflict);
        }
        if record.0 == "acknowledged" {
            transaction
                .commit()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            return Ok(());
        }
        if record.0 != "completed" {
            return Err(WorkerError::RunConflict);
        }
        transaction
            .execute(
                "UPDATE worker_commands SET state = 'acknowledged'
                 WHERE command_id = ?1 AND state = 'completed'",
                [command_id.as_str()],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerError::StorageUnavailable)
    }

    pub fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.worker_id.clone(), self.capacity, true)
            .with_incarnation(self.incarnation)
    }

    pub fn status(&self) -> Result<WorkerStatus, WorkerError> {
        let draining = self.is_draining();
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let (used, unknown): (i64, i64) = state
            .connection
            .query_row(
                "SELECT COUNT(*), SUM(CASE WHEN state = 'unknown' THEN 1 ELSE 0 END) FROM runs WHERE state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
                [],
                |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0))),
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let used = usize::try_from(used).map_err(|_| WorkerError::StorageUnavailable)?;
        let unknown = usize::try_from(unknown).map_err(|_| WorkerError::StorageUnavailable)?;
        let live_processes = state.active_groups.len();
        let observed_at = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let pending = self
            .pending
            .len()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let degraded = self.is_degraded();
        let cleanup_available = self.cleanup_admission.available_permits() > 0;
        Ok(WorkerStatus::new(
            self.descriptor(),
            used,
            unknown,
            !draining && !degraded && cleanup_available,
            draining,
            observed_at,
        )
        .with_runtime_state(live_processes, pending, degraded)
        .with_inventory(self.inventory.clone()))
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    pub fn is_degraded(&self) -> bool {
        self.pending.degraded.load(Ordering::SeqCst)
    }

    /// Stop admission immediately and notify every live supervisor to begin cleanup.
    pub fn begin_shutdown(&self) {
        self.draining.store(true, Ordering::SeqCst);
        self.shutdown_signal.send_replace(true);
    }

    pub fn shutdown(&self) -> io::Result<()> {
        self.begin_shutdown();
        // Wait for any start already inside the lifecycle gate to either
        // reject startup or transfer its process to a registered supervisor.
        drop(
            self.lifecycle
                .lock()
                .map_err(|_| io::Error::other("worker lifecycle lock poisoned"))?,
        );
        let deadline = std::time::Instant::now() + SHUTDOWN_TIMEOUT;
        loop {
            let active = self
                .state
                .0
                .lock()
                .map_err(|_| io::Error::other("worker state lock poisoned"))?
                .active_groups
                .len();
            let pending = self
                .pending
                .len()
                .map_err(|_| io::Error::other("pending persistence lock poisoned"))?;
            let retries_exhausted = self
                .pending
                .all_shutdown_retries_exhausted()
                .map_err(|_| io::Error::other("pending shutdown set lock poisoned"))?;
            let pending_readers = self.reader_reaper.pending();
            if (active == 0 && (pending == 0 || retries_exhausted) && pending_readers == 0)
                || std::time::Instant::now() >= deadline
            {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let groups = self
            .state
            .0
            .lock()
            .map_err(|_| io::Error::other("worker state lock poisoned"))?
            .active_groups
            .values()
            .map(|active| active.group_id)
            .collect::<Vec<_>>();
        let mut first_error = None;
        for group in groups {
            if let Err(error) = self.backend.kill_process_group(group) {
                first_error.get_or_insert(error);
            }
        }
        let reap_deadline = std::time::Instant::now() + Duration::from_secs(1);
        while std::time::Instant::now() < reap_deadline {
            let active = self
                .state
                .0
                .lock()
                .map_err(|_| io::Error::other("worker state lock poisoned"))?
                .active_groups
                .len();
            let pending = self
                .pending
                .len()
                .map_err(|_| io::Error::other("pending persistence lock poisoned"))?;
            let retries_exhausted = self
                .pending
                .all_shutdown_retries_exhausted()
                .map_err(|_| io::Error::other("pending shutdown set lock poisoned"))?;
            let pending_readers = self.reader_reaper.pending();
            if active == 0 && (pending == 0 || retries_exhausted) && pending_readers == 0 {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        if !self
            .state
            .0
            .lock()
            .map_err(|_| io::Error::other("worker state lock poisoned"))?
            .active_groups
            .is_empty()
            || self
                .pending
                .len()
                .map_err(|_| io::Error::other("pending persistence lock poisoned"))?
                > 0
            || self.reader_reaper.pending() > 0
        {
            first_error.get_or_insert(io::Error::new(
                io::ErrorKind::TimedOut,
                "worker shutdown deadline elapsed",
            ));
        }
        let supervisors = self
            .supervisors
            .lock()
            .map_err(|_| io::Error::other("run supervisor lock poisoned"))?
            .drain(..)
            .collect::<Vec<_>>();
        let task_deadline = std::time::Instant::now() + Duration::from_secs(1);
        while supervisors
            .iter()
            .any(|supervisor| !supervisor.is_finished())
            && std::time::Instant::now() < task_deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        if !supervisors.is_empty() {
            self.runtime.block_on(async {
                for supervisor in supervisors {
                    if supervisor.is_finished() && supervisor.await.is_err() {
                        first_error.get_or_insert(io::Error::other("run supervisor panicked"));
                    }
                }
            });
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(super) fn mark_interrupted_runs_unknown(&self) -> io::Result<()> {
        let mut state = self
            .state
            .0
            .lock()
            .map_err(|_| io::Error::other("worker state lock poisoned"))?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let rows = {
            let mut statement = transaction
                .prepare("SELECT observation_json FROM runs")
                .map_err(storage_error)?;
            let mapped = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(storage_error)?;
            mapped
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage_error)?
        };
        for json in rows {
            let observation: RunObservation = serde_json::from_str(&json).map_err(storage_error)?;
            if !observation.state().is_active_or_unresolved() {
                continue;
            }
            let unknown = observation
                .next_revision(
                    RunState::Unknown,
                    observation.started_at_ms(),
                    None,
                    None,
                    None,
                    Some(RunFailureReason::OutcomeUnknown),
                )
                .ok_or_else(|| io::Error::other("observation revision exhausted"))?
                .with_worker_incarnation(self.incarnation);
            let serialized = serde_json::to_string(&unknown).map_err(storage_error)?;
            transaction.execute(
                "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision = ?5",
                params![serialized, unknown.revision() as i64, unknown.state().as_str(), unknown.run_id().as_str(), observation.revision() as i64],
            ).map_err(storage_error)?;
        }
        transaction.commit().map_err(storage_error)
    }

    pub(super) fn log_path(&self, run_id: &str, stream: LogStream) -> PathBuf {
        self.state_directory
            .join("runs")
            .join(format!("{run_id}.{}", stream.as_str()))
    }

    pub(super) fn mark_log_closed(&self, run_id: &str, stream: LogStream) {
        let Ok(state) = self.state.0.lock() else {
            return;
        };
        let (column, other_column) = match stream {
            LogStream::Stdout => ("stdout_closed", "stderr_closed"),
            LogStream::Stderr => ("stderr_closed", "stdout_closed"),
        };
        let sql = format!(
            "UPDATE run_logs SET {column} = 1,
             completed_at_ms = CASE WHEN {other_column} = 1 THEN ?1 ELSE completed_at_ms END
             WHERE run_id = ?2"
        );
        if let Ok(timestamp) = now_ms() {
            let _ = state.connection.execute(&sql, params![timestamp, run_id]);
        }
    }

    pub(super) fn enforce_legacy_log_limits(&self) -> io::Result<()> {
        let ids = {
            let state = self
                .state
                .0
                .lock()
                .map_err(|_| io::Error::other("worker state lock poisoned"))?;
            let mut statement = state
                .connection
                .prepare("SELECT run_id FROM run_logs WHERE pruned = 0")
                .map_err(storage_error)?;
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(storage_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage_error)?
        };
        for id in ids {
            for stream in [LogStream::Stdout, LogStream::Stderr] {
                let path = self.log_path(&id, stream);
                let length = match fs::symlink_metadata(&path) {
                    Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "worker log path must be a regular file",
                        ));
                    }
                    Ok(metadata) => metadata.len(),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        let file = prepare_log_file(&path)?;
                        drop(file);
                        0
                    }
                    Err(error) => return Err(error),
                };
                if length > self.log_policy.max_bytes_per_stream {
                    let file = OpenOptions::new().write(true).open(&path)?;
                    file.set_len(self.log_policy.max_bytes_per_stream)?;
                    self.mark_log_truncated(&id, stream)?;
                }
            }
            self.mark_log_closed(&id, LogStream::Stdout);
            self.mark_log_closed(&id, LogStream::Stderr);
            if let Ok(state) = self.state.0.lock()
                && let Ok(timestamp) = now_ms()
            {
                let _ = state.connection.execute(
                    "UPDATE run_logs SET completed_at_ms = ?1 WHERE run_id = ?2 AND completed_at_ms IS NULL",
                    params![timestamp, id],
                );
            }
        }
        self.prune_completed_logs()
    }

    pub(super) fn mark_log_truncated(&self, run_id: &str, stream: LogStream) -> io::Result<()> {
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| io::Error::other("worker state lock poisoned"))?;
        let column = match stream {
            LogStream::Stdout => "stdout_truncated",
            LogStream::Stderr => "stderr_truncated",
        };
        let sql = format!("UPDATE run_logs SET {column} = 1 WHERE run_id = ?1");
        state
            .connection
            .execute(&sql, [run_id])
            .map_err(storage_error)?;
        Ok(())
    }

    pub(super) fn prune_completed_logs(&self) -> io::Result<()> {
        prune_completed_logs_shared(
            &self.state,
            &self.state_directory.join("runs"),
            self.log_policy.retention_runs,
        )
    }

    pub(super) fn persist(&self, observation: &RunObservation) -> Result<(), WorkerError> {
        let serialized =
            serde_json::to_string(observation).map_err(|_| WorkerError::StorageUnavailable)?;
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let changed = state.connection.execute(
            "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision < ?2 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
            params![serialized, observation.revision() as i64, observation.state().as_str(), observation.run_id().as_str()],
        ).map_err(|_| WorkerError::StorageUnavailable)?;
        if changed != 1 {
            return Err(WorkerError::StorageUnavailable);
        }
        Ok(())
    }

    pub(super) fn finish_spawn_failure(
        &self,
        current: RunObservation,
    ) -> Result<RunObservation, WorkerError> {
        self.finish_failure(current, RunFailureReason::SpawnFailed)
    }

    pub(super) fn finish_failure(
        &self,
        current: RunObservation,
        failure_reason: RunFailureReason,
    ) -> Result<RunObservation, WorkerError> {
        self.mark_log_closed(current.run_id().as_str(), LogStream::Stdout);
        self.mark_log_closed(current.run_id().as_str(), LogStream::Stderr);
        let finished = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let failed = current
            .next_revision(
                RunState::Failed,
                None,
                Some(finished),
                None,
                None,
                Some(failure_reason),
            )
            .ok_or(WorkerError::StorageUnavailable)?;
        self.persist(&failed)?;
        let _ = self.prune_completed_logs();
        Ok(failed)
    }

    pub(super) fn abort_startup(
        &self,
        mut spawned: SpawnGuard,
        current: RunObservation,
    ) -> Result<RunObservation, WorkerError> {
        let cleanup = spawned.cleanup();
        let cleanup_succeeded = cleanup.is_ok();
        if let Err(error) = cleanup {
            eprintln!(
                "{{\"timestampMs\":{},\"severity\":\"error\",\"component\":\"worker\",\"event\":\"startup_cleanup_failed\",\"runId\":{},\"errorKind\":{}}}",
                now_ms().unwrap_or_default(),
                serde_json::to_string(current.run_id().as_str()).unwrap_or_else(|_| "null".into()),
                serde_json::to_string(&format!("{:?}", error.kind()))
                    .unwrap_or_else(|_| "null".into())
            );
        }
        self.mark_log_closed(current.run_id().as_str(), LogStream::Stdout);
        self.mark_log_closed(current.run_id().as_str(), LogStream::Stderr);
        let finished = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let failed = current
            .next_revision(
                if cleanup_succeeded {
                    RunState::Failed
                } else {
                    RunState::Unknown
                },
                current.started_at_ms(),
                cleanup_succeeded.then_some(finished),
                None,
                None,
                Some(if cleanup_succeeded {
                    RunFailureReason::BackendFailure
                } else {
                    RunFailureReason::OutcomeUnknown
                }),
            )
            .ok_or(WorkerError::StorageUnavailable)?;
        self.persist(&failed)?;
        let _ = self.prune_completed_logs();
        Ok(failed)
    }

    pub(super) fn finish_backend_failure(
        &self,
        current: RunObservation,
    ) -> Result<RunObservation, WorkerError> {
        let finished = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let failed = current
            .next_revision(
                RunState::Failed,
                current.started_at_ms(),
                Some(finished),
                None,
                None,
                Some(RunFailureReason::BackendFailure),
            )
            .ok_or(WorkerError::StorageUnavailable)?;
        self.persist(&failed)?;
        let _ = self.prune_completed_logs();
        Ok(failed)
    }
}

impl Drop for WorkerService {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
