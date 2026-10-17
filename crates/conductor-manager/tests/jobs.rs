use std::collections::BTreeMap;
use std::sync::Arc;

use conductor_core::{JobId, JobSpec};
use conductor_manager::{InMemoryJobRepository, JobRepository, JobService, MAX_JOBS, ManagerError};

fn job(id: &str) -> JobSpec {
    JobSpec::new(
        JobId::new(id).expect("valid ID"),
        vec!["/usr/bin/true".to_owned()],
        "/tmp".into(),
        BTreeMap::new(),
        false,
        None,
        5_000,
    )
    .expect("valid spec")
}

#[test]
fn repository_reserves_deleted_ids_and_orders_active_jobs() {
    let repository = Arc::new(InMemoryJobRepository::default());
    let service = JobService::new(repository.clone());
    service.register(job("zeta")).expect("register zeta");
    service.register(job("alpha")).expect("register alpha");
    assert_eq!(
        service.register(job("alpha")),
        Err(ManagerError::DuplicateJob)
    );
    assert_eq!(
        service
            .list()
            .expect("list jobs")
            .iter()
            .map(|j| j.id().as_str())
            .collect::<Vec<_>>(),
        ["alpha", "zeta"]
    );
    assert!(service.delete(&JobId::new("alpha").unwrap()).unwrap());
    assert!(
        service
            .get(&JobId::new("alpha").unwrap())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        service.register(job("alpha")),
        Err(ManagerError::DuplicateJob)
    );
    assert_eq!(service.list().unwrap().len(), 1);
}

#[test]
fn repository_contract_reports_missing_reads_and_deletes() {
    let repository = InMemoryJobRepository::default();
    let id = JobId::new("missing").unwrap();
    assert!(repository.get(&id).unwrap().is_none());
    assert!(!repository.delete(&id).unwrap());
}

#[test]
fn active_job_collection_is_bounded_and_deleted_slots_are_reusable() {
    let service = JobService::in_memory();
    for index in 0..MAX_JOBS {
        service
            .register(job(&format!("job-{index}")))
            .expect("register within collection limit");
    }
    assert_eq!(service.list().unwrap().len(), MAX_JOBS);
    assert_eq!(
        service.register(job("over-limit")),
        Err(ManagerError::JobLimitReached)
    );
    assert!(service.delete(&JobId::new("job-0").unwrap()).unwrap());
    service
        .register(job("replacement"))
        .expect("deletion frees an active slot");
    assert_eq!(service.list().unwrap().len(), MAX_JOBS);
}
