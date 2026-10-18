use std::collections::{BTreeMap, BTreeSet};

use conductor_core::{
    Architecture, EnforcementCapability, ExecutionBackend, OperatingSystem, WorkerCapabilities,
    WorkerDescriptor, WorkerId, WorkerInventory, WorkerResourceCapacity, WorkerStatus,
};

fn capabilities(
    operating_system: OperatingSystem,
    enforcement: BTreeSet<EnforcementCapability>,
) -> WorkerCapabilities {
    WorkerCapabilities::new(
        operating_system,
        Architecture::X86_64,
        BTreeSet::from([ExecutionBackend::NativeProcess]),
        enforcement,
    )
    .expect("valid worker capabilities")
}

fn capacity(cpu_millis: u32, memory_bytes: u64) -> WorkerResourceCapacity {
    WorkerResourceCapacity::new(cpu_millis, memory_bytes).expect("valid capacity")
}

#[test]
fn inventory_bounds_allocatable_capacity_and_slots() {
    let total = capacity(8_000, 8_000);
    let allocatable = capacity(6_000, 6_000);
    assert!(
        WorkerInventory::new(
            capabilities(OperatingSystem::Linux, BTreeSet::new()),
            total,
            capacity(8_001, 6_000),
            1,
        )
        .is_err()
    );
    assert!(
        WorkerInventory::new(
            capabilities(OperatingSystem::Linux, BTreeSet::new()),
            total,
            allocatable,
            0,
        )
        .is_err()
    );
    assert!(
        WorkerInventory::new(
            capabilities(OperatingSystem::Linux, BTreeSet::new()),
            total,
            allocatable,
            65,
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<WorkerResourceCapacity>(r#"{"cpuMillis":0,"memoryBytes":100}"#)
            .is_err()
    );
}

#[test]
fn inventory_rejects_linux_enforcement_on_macos() {
    assert!(
        WorkerCapabilities::new(
            OperatingSystem::Macos,
            Architecture::Aarch64,
            BTreeSet::from([ExecutionBackend::NativeProcess]),
            BTreeSet::from([EnforcementCapability::CgroupV2]),
        )
        .is_err()
    );
}

#[test]
fn manager_metadata_preserves_worker_inventory_and_bounds_shared_budget() {
    let total = capacity(8_000, 8_000);
    let allocatable = capacity(6_000, 6_000);
    let inventory = WorkerInventory::new(
        capabilities(OperatingSystem::Linux, BTreeSet::new()),
        total,
        allocatable,
        2,
    )
    .expect("worker inventory");
    let status = WorkerStatus::new(
        WorkerDescriptor::new(WorkerId::new("worker-a").unwrap(), 2, true),
        0,
        0,
        true,
        false,
        1,
    )
    .with_inventory(inventory);
    let labels = BTreeMap::from([(String::from("region"), String::from("test"))]);
    let status = status
        .clone()
        .with_manager_metadata(
            WorkerId::new("host-a").unwrap(),
            capacity(4_000, 4_000),
            labels.clone(),
            2,
            true,
        )
        .expect("valid manager metadata");
    let inventory = status.inventory().expect("inventory is retained");
    assert_eq!(inventory.total(), total);
    assert_eq!(inventory.allocatable(), allocatable);
    assert_eq!(
        status.resource_domain_capacity(),
        Some(capacity(4_000, 4_000))
    );
    assert_eq!(status.labels(), &labels);
    assert!(status.fresh());
    assert!(
        status
            .clone()
            .with_manager_metadata(
                WorkerId::new("host-a").unwrap(),
                capacity(6_001, 4_000),
                BTreeMap::new(),
                3,
                true,
            )
            .is_none()
    );
    assert!(status.is_valid());
}
