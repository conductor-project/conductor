use conductor_core::{JobRevisionDraft, JobSpec};

#[test]
fn one_shot_cli_fixture_is_a_valid_job_definition() {
    let job: JobSpec = serde_json::from_str(include_str!("../../../examples/job.json"))
        .expect("one-shot CLI fixture matches the job contract");

    assert_eq!(job.id().as_str(), "hello");
}

#[test]
fn service_cli_fixtures_match_revision_and_deployment_contracts() {
    let draft: JobRevisionDraft =
        serde_json::from_str(include_str!("../../../examples/cli-service-revision.json"))
            .expect("service CLI fixture matches the revision contract");
    let revision = draft
        .into_revision(1)
        .expect("service CLI fixture forms a valid first revision");
    assert_eq!(revision.id().job_id().as_str(), "web");

    let deployment: serde_json::Value = serde_json::from_str(include_str!(
        "../../../examples/cli-service-deployment.json"
    ))
    .expect("deployment CLI fixture is valid JSON");
    assert_eq!(deployment["revision"], 1);
    assert_eq!(deployment["replicas"], 1);
}
