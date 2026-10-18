use super::*;

pub(super) struct PendingPersistence {
    pub(super) observations: Mutex<BTreeMap<String, RunObservation>>,
    pub(super) shutdown_exhausted: Mutex<BTreeSet<String>>,
    pub(super) degraded: AtomicBool,
}

impl PendingPersistence {
    pub(super) fn insert(&self, observation: RunObservation) -> io::Result<()> {
        self.degraded.store(true, Ordering::SeqCst);
        self.observations
            .lock()
            .map_err(|_| io::Error::other("pending persistence lock poisoned"))?
            .insert(observation.run_id().to_string(), observation);
        Ok(())
    }

    pub(super) fn remove(&self, run_id: &str) -> io::Result<()> {
        let mut observations = self
            .observations
            .lock()
            .map_err(|_| io::Error::other("pending persistence lock poisoned"))?;
        observations.remove(run_id);
        self.shutdown_exhausted
            .lock()
            .map_err(|_| io::Error::other("pending shutdown set lock poisoned"))?
            .remove(run_id);
        self.degraded
            .store(!observations.is_empty(), Ordering::SeqCst);
        Ok(())
    }

    pub(super) fn mark_shutdown_exhausted(&self, run_id: &str) -> io::Result<()> {
        self.shutdown_exhausted
            .lock()
            .map_err(|_| io::Error::other("pending shutdown set lock poisoned"))?
            .insert(run_id.to_owned());
        Ok(())
    }

    pub(super) fn all_shutdown_retries_exhausted(&self) -> io::Result<bool> {
        let observations = self
            .observations
            .lock()
            .map_err(|_| io::Error::other("pending persistence lock poisoned"))?;
        let exhausted = self
            .shutdown_exhausted
            .lock()
            .map_err(|_| io::Error::other("pending shutdown set lock poisoned"))?;
        Ok(observations.keys().all(|run_id| exhausted.contains(run_id)))
    }

    pub(super) fn len(&self) -> io::Result<usize> {
        self.observations
            .lock()
            .map(|observations| observations.len())
            .map_err(|_| io::Error::other("pending persistence lock poisoned"))
    }
}
