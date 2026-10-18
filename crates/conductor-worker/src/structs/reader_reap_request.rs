use super::*;

pub(super) struct ReaderReapRequest {
    pub(super) child: Option<Child>,
    pub(super) readers: Vec<JoinHandle<io::Result<()>>>,
    pub(super) completion: Option<oneshot::Sender<io::Result<()>>>,
    pub(super) cleanup_permit: OwnedSemaphorePermit,
    pub(super) cleanup: Option<Box<dyn FnMut() -> io::Result<()> + Send>>,
}
