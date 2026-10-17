//! Manager-owned application operations and job-definition storage.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use conductor_core::{JobId, JobSpec};

/// Temporary limit for the in-memory job collection. Pagination is a later milestone.
pub const MAX_JOBS: usize = 1_000;
const MAX_RESERVED_JOB_IDS: usize = 10_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobStoreError {
    DuplicateId,
    LimitExceeded,
    Poisoned,
}

/// Storage port owned by the manager. Implementations must preserve deleted IDs.
pub trait JobRepository: Send + Sync {
    fn insert(&self, job: JobSpec) -> Result<(), JobStoreError>;
    fn get(&self, id: &JobId) -> Result<Option<JobSpec>, JobStoreError>;
    fn list(&self) -> Result<Vec<JobSpec>, JobStoreError>;
    fn delete(&self, id: &JobId) -> Result<bool, JobStoreError>;
}

#[derive(Default)]
struct MemoryState {
    jobs: BTreeMap<JobId, Option<JobSpec>>,
    active_jobs: usize,
}

/// Bounded ephemeral repository used until the persistence milestone replaces it.
#[derive(Default)]
pub struct InMemoryJobRepository {
    state: RwLock<MemoryState>,
}

impl JobRepository for InMemoryJobRepository {
    fn insert(&self, job: JobSpec) -> Result<(), JobStoreError> {
        let mut state = self.state.write().map_err(|_| JobStoreError::Poisoned)?;
        if state.jobs.contains_key(job.id()) {
            return Err(JobStoreError::DuplicateId);
        }
        if state.active_jobs >= MAX_JOBS || state.jobs.len() >= MAX_RESERVED_JOB_IDS {
            return Err(JobStoreError::LimitExceeded);
        }
        state.jobs.insert(job.id().clone(), Some(job));
        state.active_jobs += 1;
        Ok(())
    }

    fn get(&self, id: &JobId) -> Result<Option<JobSpec>, JobStoreError> {
        let state = self.state.read().map_err(|_| JobStoreError::Poisoned)?;
        Ok(state.jobs.get(id).and_then(Option::as_ref).cloned())
    }

    fn list(&self) -> Result<Vec<JobSpec>, JobStoreError> {
        let state = self.state.read().map_err(|_| JobStoreError::Poisoned)?;
        Ok(state.jobs.values().filter_map(Clone::clone).collect())
    }

    fn delete(&self, id: &JobId) -> Result<bool, JobStoreError> {
        let mut state = self.state.write().map_err(|_| JobStoreError::Poisoned)?;
        let Some(job) = state.jobs.get_mut(id) else {
            return Ok(false);
        };
        if job.take().is_some() {
            state.active_jobs -= 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }
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

    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemoryJobRepository::default()))
    }

    pub fn register(&self, job: JobSpec) -> Result<(), ManagerError> {
        match self.repository.insert(job) {
            Ok(()) => Ok(()),
            Err(JobStoreError::DuplicateId) => Err(ManagerError::DuplicateJob),
            Err(JobStoreError::LimitExceeded) => Err(ManagerError::JobLimitReached),
            Err(JobStoreError::Poisoned) => Err(ManagerError::StorageUnavailable),
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
        JobStoreError::Poisoned => ManagerError::StorageUnavailable,
    }
}
