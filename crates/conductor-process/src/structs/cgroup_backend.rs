use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::cgroup::CgroupV2;

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
pub(super) struct CgroupBackend {
    pub(super) manager: CgroupV2,
    pub(super) groups: Arc<Mutex<BTreeMap<u32, PathBuf>>>,
}
