use super::*;

#[derive(Clone)]
pub(super) struct ReaderReaper {
    pub(super) sender: SyncSender<ReaderReapRequest>,
    pub(super) pending: Arc<AtomicUsize>,
    pub(super) overflow: Arc<Mutex<VecDeque<ReaderReapRequest>>>,
    #[cfg(test)]
    pub(super) gate: Arc<(Mutex<bool>, Condvar)>,
}

impl ReaderReaper {
    pub(super) fn new() -> io::Result<Self> {
        let (sender, receiver) = sync_channel::<ReaderReapRequest>(MAX_WORKER_CAPACITY);
        let pending = Arc::new(AtomicUsize::new(0));
        let reaper_pending = Arc::clone(&pending);
        let overflow = Arc::new(Mutex::new(VecDeque::new()));
        let reaper_overflow = Arc::clone(&overflow);
        #[cfg(test)]
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        #[cfg(test)]
        let reaper_gate = Arc::clone(&gate);
        thread::Builder::new()
            .name("conductor-reader-reaper".to_owned())
            .spawn(move || {
                loop {
                    let queued = reaper_overflow
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .pop_front();
                    let request = match queued {
                        Some(request) => request,
                        None => match receiver.recv() {
                            Ok(request) => request,
                            Err(_) => break,
                        },
                    };
                    #[cfg(test)]
                    {
                        let (paused, changed) = &*reaper_gate;
                        let mut paused = paused
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        while *paused {
                            paused = changed
                                .wait(paused)
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                        }
                        drop(paused);
                    }
                    finish_reap_request(request, &reaper_pending);
                }
            })?;
        Ok(Self {
            sender,
            pending,
            overflow,
            #[cfg(test)]
            gate,
        })
    }

    pub(super) fn enqueue(
        &self,
        child: Option<Child>,
        readers: Vec<JoinHandle<io::Result<()>>>,
        cleanup_permit: OwnedSemaphorePermit,
        cleanup: Option<Box<dyn FnMut() -> io::Result<()> + Send>>,
    ) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let (completion, receiver) = oneshot::channel();
        if readers.is_empty() && child.is_none() && cleanup.is_none() {
            let _ = completion.send(Ok(()));
            return Ok(receiver);
        }
        self.pending.fetch_add(1, Ordering::SeqCst);
        match self.sender.try_send(ReaderReapRequest {
            child,
            readers,
            completion: Some(completion),
            cleanup_permit,
            cleanup,
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(request)) => {
                self.overflow
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push_back(request);
            }
            Err(TrySendError::Disconnected(request)) => {
                enqueue_detached_fallback(request, Arc::clone(&self.pending), &self.overflow)?;
            }
        }
        Ok(receiver)
    }

    pub(super) fn enqueue_detached(
        &self,
        child: Option<Child>,
        readers: Vec<JoinHandle<io::Result<()>>>,
        cleanup_permit: OwnedSemaphorePermit,
        cleanup: Option<Box<dyn FnMut() -> io::Result<()> + Send>>,
    ) {
        if readers.is_empty() && child.is_none() && cleanup.is_none() {
            return;
        }
        self.pending.fetch_add(1, Ordering::SeqCst);
        let request = ReaderReapRequest {
            child,
            readers,
            completion: None,
            cleanup_permit,
            cleanup,
        };
        match self.sender.try_send(request) {
            Ok(()) => {}
            Err(TrySendError::Full(request)) => {
                self.overflow
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push_back(request);
            }
            Err(TrySendError::Disconnected(request)) => {
                if let Err(error) =
                    enqueue_detached_fallback(request, Arc::clone(&self.pending), &self.overflow)
                {
                    emit_worker_diagnostic("reader_reaper_unavailable", "", &error);
                }
            }
        }
    }

    pub(super) fn pending(&self) -> usize {
        self.pending.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn pause_for_test(&self) {
        *self
            .gate
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    }

    #[cfg(test)]
    pub(super) fn resume_for_test(&self) {
        *self
            .gate
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
        self.gate.1.notify_all();
    }
}
