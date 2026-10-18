use super::*;

/// Owns a spawned process until supervision takes responsibility for it.
/// Dropping the guard is a last-resort cleanup path; callers should use
/// `cleanup` when they need to observe cleanup failures.
pub(super) struct SpawnGuard {
    pub(super) backend: Arc<dyn ExecutionBackend>,
    pub(super) reader_reaper: ReaderReaper,
    pub(super) group_id: u32,
    pub(super) child: Option<Child>,
    pub(super) readers: Vec<JoinHandle<io::Result<()>>>,
    pub(super) cleanup_permit: Option<OwnedSemaphorePermit>,
    pub(super) cancel_readers: Arc<AtomicBool>,
    pub(super) cleaned: bool,
}

impl Drop for SpawnGuard {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        self.cancel_readers.store(true, Ordering::SeqCst);
        let _ = self.backend.kill_process_group(self.group_id);
        if let Some(child) = &mut self.child {
            let _ = child.kill();
        }
        if let Some(cleanup_permit) = self.cleanup_permit.take() {
            self.reader_reaper.enqueue_detached(
                self.child.take(),
                std::mem::take(&mut self.readers),
                cleanup_permit,
                Some(self.cleanup_callback()),
            );
        }
    }
}

impl SpawnGuard {
    pub(super) fn cleanup_callback(&self) -> Box<dyn FnMut() -> io::Result<()> + Send> {
        let backend = Arc::clone(&self.backend);
        let group_id = self.group_id;
        Box::new(move || backend.cleanup_execution(group_id))
    }

    pub(super) fn cleanup(&mut self) -> io::Result<()> {
        if self.cleaned {
            return Ok(());
        }
        self.cancel_readers.store(true, Ordering::SeqCst);
        let mut first_error = self.backend.kill_process_group(self.group_id).err();
        if let Some(child) = &mut self.child {
            if let Err(error) = child.kill()
                && error.kind() != io::ErrorKind::InvalidInput
            {
                first_error.get_or_insert(error);
            }
            if let Err(error) = child.wait() {
                first_error.get_or_insert(error);
            }
            self.child.take();
        }
        if let Err(error) = join_readers(std::mem::take(&mut self.readers)) {
            first_error.get_or_insert(error);
        }
        if first_error.is_none() {
            self.cleaned = true;
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(super) fn enqueue_readers(&mut self) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let cleanup_permit = self
            .cleanup_permit
            .take()
            .ok_or_else(|| io::Error::other("cleanup reservation is missing"))?;
        self.reader_reaper.enqueue(
            self.child.take(),
            std::mem::take(&mut self.readers),
            cleanup_permit,
            Some(self.cleanup_callback()),
        )
    }
}
