#![cfg(target_os = "linux")]

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use conductor_process::{HostProcessBackend, LandlockGrantConfig, LandlockMode};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "conductor-landlock-paths-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create temporary fixture root");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn grant(path: &Path) -> LandlockGrantConfig {
    LandlockGrantConfig {
        id: "fixture".into(),
        path: path.to_path_buf(),
        read: true,
        write: false,
        execute: false,
    }
}

fn open_with_protected_path(
    grant_path: &Path,
    protected_path: &Path,
) -> io::Result<HostProcessBackend> {
    HostProcessBackend::default().with_landlock(
        LandlockMode::Required,
        vec![grant(grant_path)],
        &[protected_path.to_path_buf()],
        PathBuf::from("/unused-launcher"),
    )
}

#[test]
fn symlink_then_parent_protects_the_filesystem_resolved_lock_directory() {
    let fixture = Fixture::new();
    let secure_runtime = fixture.path().join("secure/runtime");
    fs::create_dir_all(secure_runtime.join("child")).expect("create symlink target");
    let lock_directory = secure_runtime.join("locks");
    fs::create_dir(&lock_directory).expect("create lock directory");
    fs::create_dir(lock_directory.join("nested")).expect("create nested grant directory");

    let alias_parent = fixture.path().join("srv");
    fs::create_dir(&alias_parent).expect("create symlink parent");
    let alias = alias_parent.join("alias");
    std::os::unix::fs::symlink(secure_runtime.join("child"), &alias)
        .expect("create directory symlink");
    let configured_lock_directory = alias.join("../locks");
    assert_eq!(
        fs::canonicalize(&configured_lock_directory).expect("canonicalize configured lock dir"),
        lock_directory
    );

    for overlapping_grant in [
        lock_directory.clone(),
        secure_runtime.clone(),
        lock_directory.join("nested"),
    ] {
        let error = open_with_protected_path(&overlapping_grant, &configured_lock_directory)
            .expect_err("grant overlapping the resolved lock directory must be rejected");
        assert!(
            error
                .to_string()
                .contains("cannot overlap Conductor state or control paths"),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn missing_suffix_after_symlink_parent_is_resolved_and_unrelated_grants_work() {
    let fixture = Fixture::new();
    let secure_runtime = fixture.path().join("secure/runtime");
    fs::create_dir_all(secure_runtime.join("child")).expect("create symlink target");
    let alias_parent = fixture.path().join("srv");
    fs::create_dir(&alias_parent).expect("create symlink parent");
    let alias = alias_parent.join("alias");
    std::os::unix::fs::symlink(secure_runtime.join("child"), &alias)
        .expect("create directory symlink");

    let unrelated = alias_parent.join("missing");
    fs::create_dir(&unrelated).expect("create unrelated grant directory");
    let missing_protected = alias.join("../missing/locks");
    let backend = open_with_protected_path(&unrelated, &missing_protected)
        .expect("missing suffix must retain the resolved symlink parent");
    drop(backend);
}

#[test]
fn parent_after_missing_component_is_rejected_as_ambiguous() {
    let fixture = Fixture::new();
    let unrelated = fixture.path().join("approved");
    fs::create_dir(&unrelated).expect("create unrelated grant directory");
    let ambiguous = fixture.path().join("not-created/../locks");
    let error = open_with_protected_path(&unrelated, &ambiguous)
        .expect_err("parent traversal across an unresolved path must fail closed");
    assert!(
        error
            .to_string()
            .contains("protected Conductor path cannot be resolved"),
        "unexpected error: {error}"
    );
}
