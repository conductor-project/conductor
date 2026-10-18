use super::*;

#[derive(Clone)]
pub struct WorkerTransportHub {
    pub(super) sessions: Arc<RwLock<BTreeMap<WorkerId, Arc<WorkerSession>>>>,
    pub(super) journal: Option<Arc<dyn CommandJournal>>,
}

impl WorkerTransportHub {
    /// Create a transport hub backed by the manager's durable command journal.
    pub fn with_journal(journal: Arc<dyn CommandJournal>) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(BTreeMap::new())),
            journal: Some(journal),
        }
    }

    pub fn register_worker(
        &self,
        descriptor: WorkerDescriptor,
        protocol_offer: ProtocolOffer,
    ) -> Result<Arc<RemoteWorkerPort>, TransportError> {
        let metadata = WorkerMetadata::automatic(descriptor.worker_id().clone());
        self.register_worker_with_metadata(descriptor, protocol_offer, metadata)
    }

    pub fn register_worker_with_metadata(
        &self,
        descriptor: WorkerDescriptor,
        protocol_offer: ProtocolOffer,
        metadata: WorkerMetadata,
    ) -> Result<Arc<RemoteWorkerPort>, TransportError> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| TransportError::Unavailable)?;
        let worker_id = descriptor.worker_id().clone();
        if sessions.contains_key(&worker_id) {
            return Err(TransportError::WorkerConflict);
        }
        if sessions.len() >= MAX_REGISTERED_WORKERS {
            return Err(TransportError::WorkerLimitReached);
        }
        let session = Arc::new(WorkerSession::new_with_metadata(
            descriptor,
            protocol_offer,
            self.journal.clone(),
            metadata,
        ));
        sessions.insert(worker_id, Arc::clone(&session));
        Ok(Arc::new(RemoteWorkerPort { session }))
    }

    pub fn unregister_worker(&self, worker_id: &WorkerId) -> Result<bool, TransportError> {
        self.sessions
            .write()
            .map(|mut sessions| sessions.remove(worker_id).is_some())
            .map_err(|_| TransportError::Unavailable)
    }

    /// Build routes that always require an identity supplied by the configured
    /// authenticator. No unauthenticated router constructor is provided.
    pub fn router(&self, authenticator: Arc<dyn WorkerAuthenticator>) -> Router {
        Router::new()
            .route("/v1/worker/poll", post(poll_worker))
            .route("/v1/worker/report", post(report_worker))
            .layer(DefaultBodyLimit::max(MAX_TRANSPORT_BODY_BYTES))
            .layer(middleware::from_fn_with_state(
                authenticator,
                authenticate_worker,
            ))
            .with_state(HubState(self.clone()))
    }

    pub(super) fn session(&self, worker_id: &WorkerId) -> Result<Arc<WorkerSession>, StatusCode> {
        self.sessions
            .read()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
            .get(worker_id)
            .cloned()
            .ok_or(StatusCode::NOT_FOUND)
    }
}

impl Default for WorkerTransportHub {
    fn default() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(BTreeMap::new())),
            journal: None,
        }
    }
}
