#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
pub(super) struct CgroupBackend {
    pub(super) manager: CgroupV2,
    pub(super) groups: Arc<Mutex<BTreeMap<u32, PathBuf>>>,
}
