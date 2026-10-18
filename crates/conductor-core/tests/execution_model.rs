use std::collections::{BTreeMap, BTreeSet};

use conductor_core::{
    Allocation, AllocationId, AllocationState, Architecture, AssignedServicePort, AttemptState,
    DeploymentPendingReason, DeploymentStatus, DigestAlgorithm, EnforcementCapability,
    ExecutableDigest, ExecutionAttempt, ExecutionAttemptId, ExecutionBackend, ExecutionModelError,
    ExecutionRequirements, FilesystemPolicy, JobDeployment, JobId, JobRevision, JobRevisionDraft,
    JobRevisionId, JobSpec, OperatingSystem, ResourceLimits, ResourceRequest, ResourceReservation,
    RunId, RunSpec, ServiceIdentity, ServiceInstance, ServiceInstanceId, ServicePort,
    ServicePortProtocol, WorkerCapabilities, WorkerId, Workload,
};

fn job_spec(id: &str) -> JobSpec {
    serde_json::from_str(&format!(
        r#"{{"id":"{id}","command":["/usr/bin/true"],"cwd":"/tmp"}}"#
    ))
    .expect("valid MVP1 job spec")
}

fn revision(workload: Workload) -> JobRevision {
    JobRevision::new(
        JobRevisionId::new(JobId::new("job-1").unwrap(), 1).unwrap(),
        job_spec("job-1"),
        workload,
        ExecutableDigest::sha256("a".repeat(64)).unwrap(),
        ExecutionRequirements::default(),
        ResourceRequest::new(Some(500), Some(1024 * 1024)).unwrap(),
    )
    .unwrap()
}

#[test]
fn revision_submission_rejects_unknown_fields() {
    let draft = JobRevisionDraft::new(
        job_spec("job-1"),
        Workload::OneShot,
        ExecutableDigest::sha256("a".repeat(64)).unwrap(),
        ExecutionRequirements::default(),
        ResourceRequest::default(),
    );
    let mut value = serde_json::to_value(draft).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("unrecognized".to_owned(), serde_json::Value::Bool(true));
    assert!(serde_json::from_value::<JobRevisionDraft>(value).is_err());
}

fn worker_capabilities(
    operating_system: OperatingSystem,
    architecture: Architecture,
    enforcement: &[EnforcementCapability],
) -> WorkerCapabilities {
    WorkerCapabilities::new(
        operating_system,
        architecture,
        BTreeSet::from([ExecutionBackend::NativeProcess]),
        enforcement.iter().copied().collect(),
    )
    .unwrap()
}

#[test]
fn immutable_revision_round_trips_and_legacy_one_shot_jobs_remain_valid() {
    let legacy_json = r#"{"id":"job-1","command":["/usr/bin/true"],"cwd":"/tmp"}"#;
    let legacy: JobSpec = serde_json::from_str(legacy_json).unwrap();
    assert_eq!(legacy.id().as_str(), "job-1");

    let revision = revision(Workload::OneShot);
    assert_eq!(revision.id().revision(), 1);
    assert_eq!(
        revision.executable_digest().algorithm(),
        DigestAlgorithm::Sha256
    );
    let json = serde_json::to_string(&revision).unwrap();
    assert_eq!(
        serde_json::from_str::<JobRevision>(&json).unwrap(),
        revision
    );
    assert!(json.contains("\"kind\":\"one_shot\""));
    assert!(json.contains("\"resourceRequest\""));

    let mismatched = JobRevision::new(
        JobRevisionId::new(JobId::new("another-job").unwrap(), 1).unwrap(),
        legacy,
        Workload::OneShot,
        ExecutableDigest::sha256("b".repeat(64)).unwrap(),
        ExecutionRequirements::default(),
        ResourceRequest::default(),
    );
    assert_eq!(mismatched, Err(ExecutionModelError::RevisionJobMismatch));
    assert_eq!(
        JobRevisionId::new(JobId::new("job-1").unwrap(), 0),
        Err(ExecutionModelError::InvalidRevision)
    );
}

#[test]
fn service_identity_and_replica_slots_have_stable_validated_ids() {
    let service = ServiceIdentity::new("frontend").unwrap();
    assert_eq!(service.namespace(), "default");
    assert_eq!(service.name(), "frontend");
    assert!(ServiceIdentity::new("Frontend").is_err());
    assert!(ServiceIdentity::new("-frontend").is_err());
    assert!(ServiceIdentity::in_namespace("staging", "frontend").is_err());
    assert!(
        serde_json::from_str::<ServiceIdentity>(r#"{"namespace":"other","name":"frontend"}"#)
            .is_err()
    );

    let revision = revision(Workload::Service {
        identity: service.clone(),
        ports: Vec::new(),
    });
    let instance = ServiceInstance::new(
        ServiceInstanceId::new("frontend-slot-0").unwrap(),
        &revision,
        0,
    )
    .unwrap();
    assert_eq!(instance.service(), &service);
    assert_eq!(instance.replica_index(), 0);
    let allocation = Allocation::new(
        AllocationId::new("frontend-allocation-0").unwrap(),
        &revision,
        Some(&instance),
        ResourceReservation::default(),
    )
    .unwrap();
    assert_eq!(allocation.service_instance_id(), Some(instance.id()));
    // Replica counts belong to mutable deployment intent, so a revision does
    // not impose a bound on stable service slot identities.
    let later_slot = ServiceInstance::new(
        ServiceInstanceId::new("frontend-slot-2").unwrap(),
        &revision,
        2,
    )
    .unwrap();
    assert_eq!(later_slot.replica_index(), 2);
    assert!(
        serde_json::from_str::<ServiceIdentity>(r#"{"namespace":"default","name":"x-"}"#)
            .unwrap_err()
            .to_string()
            .contains("DNS-compatible")
    );
}

#[test]
fn deployment_intent_is_mutable_separate_from_immutable_revisions() {
    let service = ServiceIdentity::new("frontend").unwrap();
    let first_revision = revision(Workload::Service {
        identity: service.clone(),
        ports: Vec::new(),
    });
    let pending = JobDeployment::new(
        &first_revision,
        1,
        3,
        false,
        DeploymentStatus::Pending,
        Some(DeploymentPendingReason::PlacementPending),
        Some("placement has not been evaluated yet".to_owned()),
    )
    .unwrap();
    assert_eq!(pending.desired_replicas(), 3);
    assert_eq!(pending.revision_id(), first_revision.id());

    let scaled_to_zero = pending
        .next_generation(
            &first_revision,
            0,
            false,
            DeploymentStatus::Ready,
            None,
            None,
        )
        .unwrap();
    assert_eq!(scaled_to_zero.generation(), 2);
    assert_eq!(scaled_to_zero.desired_replicas(), 0);

    let suspended = scaled_to_zero
        .next_generation(
            &first_revision,
            3,
            true,
            DeploymentStatus::Suspended,
            None,
            None,
        )
        .unwrap();
    assert!(suspended.suspended());
    assert_eq!(suspended.desired_replicas(), 3);
    let encoded = serde_json::to_string(&suspended).unwrap();
    assert_eq!(
        serde_json::from_str::<JobDeployment>(&encoded).unwrap(),
        suspended
    );
    assert!(
        serde_json::from_str::<JobDeployment>(
            &encoded.replace("\"generation\":3", "\"generation\":0")
        )
        .is_err()
    );
}

#[test]
fn allocation_and_attempt_transitions_reject_unsafe_reassignment() {
    let revision = revision(Workload::OneShot);
    let mut allocation = Allocation::new(
        AllocationId::new("allocation-1").unwrap(),
        &revision,
        None,
        ResourceReservation::new(Some(500), Some(1024 * 1024)).unwrap(),
    )
    .unwrap();
    assert_eq!(allocation.state(), AllocationState::Pending);
    assert_eq!(allocation.reservation().cpu_millis(), Some(500));
    assert_eq!(allocation.reservation().memory_bytes(), Some(1024 * 1024));
    assert!(
        allocation
            .assign(WorkerId::new("worker-1").unwrap())
            .is_ok()
    );
    assert_eq!(allocation.assignment_generation(), 1);
    assert!(
        allocation
            .assign(WorkerId::new("worker-2").unwrap())
            .is_err()
    );

    let mut attempt = ExecutionAttempt::new(
        ExecutionAttemptId::new("attempt-1").unwrap(),
        &allocation,
        1,
    )
    .unwrap();
    assert!(attempt.transition_to(AttemptState::Running).is_err());
    attempt.transition_to(AttemptState::Starting).unwrap();
    attempt.transition_to(AttemptState::Running).unwrap();
    attempt.transition_to(AttemptState::Unknown).unwrap();
    assert!(!attempt.state().is_terminal());
    assert_eq!(attempt.revision(), 4);
    attempt.transition_to(AttemptState::Succeeded).unwrap();
    assert!(attempt.state().is_terminal());
    assert!(attempt.transition_to(AttemptState::Failed).is_err());
    let attempt_json = serde_json::to_string(&attempt).unwrap();
    assert_eq!(
        serde_json::from_str::<ExecutionAttempt>(&attempt_json).unwrap(),
        attempt
    );

    allocation.mark_uncertain().unwrap();
    assert!(
        allocation
            .assign(WorkerId::new("worker-2").unwrap())
            .is_err()
    );
    allocation.resolve_uncertain_without_execution().unwrap();
    allocation
        .assign(WorkerId::new("worker-2").unwrap())
        .unwrap();
    assert_eq!(allocation.assignment_generation(), 2);
    assert_eq!(
        allocation.assigned_worker_id().unwrap().as_str(),
        "worker-2"
    );
    allocation.release().unwrap();
    assert!(allocation.release().is_err());
}

#[test]
fn service_revisions_require_complete_requests_and_derive_protection_requirements() {
    let service = Workload::Service {
        identity: ServiceIdentity::new("frontend").unwrap(),
        ports: Vec::new(),
    };
    let digest = ExecutableDigest::sha256("a".repeat(64)).unwrap();
    let id = JobRevisionId::new(JobId::new("job-1").unwrap(), 1).unwrap();
    assert_eq!(
        JobRevision::new(
            id.clone(),
            job_spec("job-1"),
            service.clone(),
            digest.clone(),
            ExecutionRequirements::default(),
            ResourceRequest::default(),
        ),
        Err(ExecutionModelError::IncompleteServiceResourceRequest)
    );

    let definition = job_spec("job-1")
        .with_resource_limits(
            ResourceLimits::new(
                Some(100_000),
                Some(100_000),
                Some(200 * 1024 * 1024),
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap()
        .with_filesystem_policy(Some(
            FilesystemPolicy::new(vec!["runtime".to_owned()]).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        JobRevision::new(
            id.clone(),
            definition.clone(),
            service.clone(),
            digest.clone(),
            ExecutionRequirements::default(),
            ResourceRequest::new(Some(999), Some(200 * 1024 * 1024)).unwrap(),
        ),
        Err(ExecutionModelError::ResourceRequestBelowExecutionLimit)
    );
    let revision = JobRevision::new(
        id.clone(),
        definition.clone(),
        service.clone(),
        digest.clone(),
        ExecutionRequirements::default(),
        ResourceRequest::new(Some(1_000), Some(200 * 1024 * 1024)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        revision.execution_requirements().required_enforcement(),
        &BTreeSet::from([
            EnforcementCapability::CgroupV2,
            EnforcementCapability::Landlock,
        ])
    );
    assert_eq!(
        JobRevision::new(
            id,
            definition,
            service,
            digest,
            ExecutionRequirements::new(Some(OperatingSystem::Macos), None, None, BTreeSet::new(),),
            ResourceRequest::new(Some(1_000), Some(200 * 1024 * 1024)).unwrap(),
        ),
        Err(ExecutionModelError::UnsupportedPlatformCapability)
    );
}

#[test]
fn named_service_ports_validate_names_and_create_run_environment_overrides() {
    let declared = ServicePort::tcp("admin-http").unwrap();
    assert_eq!(declared.environment_name(), "CONDUCTOR_PORT_ADMIN_HTTP");
    assert!(ServicePort::tcp("Admin").is_err());
    assert!(ServicePort::tcp("bad_name").is_err());

    let spec = RunSpec::new(
        RunId::new("run-1").unwrap(),
        WorkerId::new("worker-1").unwrap(),
        job_spec("job-1"),
    )
    .with_service_ports(vec![
        AssignedServicePort::new(
            declared.name(),
            ServicePortProtocol::Tcp,
            31_000,
            "192.0.2.10",
        )
        .unwrap(),
    ])
    .unwrap();
    let process_job = spec
        .job()
        .clone()
        .with_environment_overrides(
            spec.service_ports()
                .iter()
                .map(|port| (port.environment_name(), port.port().to_string())),
        )
        .unwrap();
    assert_eq!(process_job.env()["CONDUCTOR_PORT_ADMIN_HTTP"], "31000");

    assert!(AssignedServicePort::new("http", ServicePortProtocol::Tcp, 0, "192.0.2.10").is_err());
    assert!(
        AssignedServicePort::new("http", ServicePortProtocol::Tcp, 31_000, "127.0.0.1").is_err()
    );
    assert!(
        AssignedServicePort::new("http", ServicePortProtocol::Tcp, 31_000, "localhost").is_err()
    );
}

#[test]
fn platform_requirements_are_explicit_and_reservations_are_not_enforcement() {
    let requirements = ExecutionRequirements::new(
        Some(OperatingSystem::Linux),
        Some(Architecture::X86_64),
        Some(ExecutionBackend::NativeProcess),
        BTreeSet::from([
            EnforcementCapability::CgroupV2,
            EnforcementCapability::Landlock,
        ]),
    );
    let linux = worker_capabilities(
        OperatingSystem::Linux,
        Architecture::X86_64,
        &[
            EnforcementCapability::CgroupV2,
            EnforcementCapability::Landlock,
        ],
    );
    let macos = worker_capabilities(OperatingSystem::Macos, Architecture::Aarch64, &[]);
    assert!(requirements.is_satisfied_by(&linux));
    assert!(!requirements.is_satisfied_by(&macos));

    let portable = ExecutionRequirements::default();
    assert!(portable.is_satisfied_by(&macos));
    assert_eq!(
        WorkerCapabilities::new(
            OperatingSystem::Macos,
            Architecture::Aarch64,
            BTreeSet::from([ExecutionBackend::NativeProcess]),
            BTreeSet::from([EnforcementCapability::Landlock]),
        ),
        Err(ExecutionModelError::UnsupportedPlatformCapability)
    );
    let request = ResourceRequest::new(Some(1000), Some(8 * 1024 * 1024)).unwrap();
    let reservation = ResourceReservation::new(Some(900), Some(8 * 1024 * 1024)).unwrap();
    assert_ne!(request.cpu_millis(), reservation.cpu_millis());
    assert!(ResourceRequest::new(Some(0), None).is_none());
    assert!(ResourceReservation::new(None, Some(0)).is_none());

    assert!(
        serde_json::from_str::<WorkerCapabilities>(
            r#"{"operatingSystem":"macos","architecture":"aarch64","backends":[]}"#
        )
        .is_err()
    );
    assert!(serde_json::from_str::<WorkerCapabilities>(
        r#"{"operatingSystem":"macos","architecture":"aarch64","backends":["native_process"],"unknown":true}"#
    )
    .is_err());
}

#[test]
fn worker_label_constraints_match_exact_manager_labels_and_are_bounded() {
    let constraints = ExecutionRequirements::default()
        .with_worker_labels(BTreeMap::from([
            ("pool".to_owned(), "services".to_owned()),
            ("region".to_owned(), "west".to_owned()),
        ]))
        .unwrap();
    assert!(constraints.labels_are_satisfied_by(&BTreeMap::from([
        ("pool".to_owned(), "services".to_owned()),
        ("region".to_owned(), "west".to_owned()),
        ("extra".to_owned(), "allowed".to_owned()),
    ])));
    assert!(
        !constraints
            .labels_are_satisfied_by(&BTreeMap::from([("pool".to_owned(), "batch".to_owned(),)]))
    );
    assert!(
        ExecutionRequirements::default()
            .with_worker_labels(BTreeMap::from([(
                "invalid key".to_owned(),
                "value".to_owned(),
            )]))
            .is_err()
    );
}

#[test]
fn invalid_digests_and_inconsistent_serialized_allocations_are_rejected() {
    assert!(ExecutableDigest::sha256("A".repeat(64)).is_err());
    assert!(ExecutableDigest::sha256("0".repeat(63)).is_err());
    assert!(
        serde_json::from_str::<ExecutableDigest>(
            r#"{"algorithm":"sha256","value":"not-a-digest"}"#
        )
        .is_err()
    );

    let json = r#"{
        "id":"allocation-1",
        "jobRevisionId":{"jobId":"job-1","revision":1},
        "serviceInstanceId":null,
        "assignedWorkerId":"worker-1",
        "assignmentGeneration":0,
        "state":"pending",
        "reservation":{}
    }"#;
    assert!(serde_json::from_str::<Allocation>(json).is_err());
    assert!(serde_json::from_str::<JobRevision>(
        r#"{"id":{"jobId":"job-1","revision":1},"definition":{"id":"job-1","command":["/usr/bin/true"],"cwd":"/tmp"},"workload":{"kind":"one_shot"},"executableDigest":{"algorithm":"sha256","value":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"resourceRequest":{},"unknown":true}"#
    )
    .is_err());
}
