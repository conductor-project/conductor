use std::collections::BTreeSet;

use conductor_core::{
    ProtocolCapability as Capability, ProtocolNegotiationError as NegotiationError, ProtocolOffer,
    ProtocolVersionRange,
};

fn set(values: &[Capability]) -> BTreeSet<Capability> {
    values.iter().copied().collect()
}

fn offer(min: u16, max: u16, supported: &[Capability], required: &[Capability]) -> ProtocolOffer {
    ProtocolOffer::new(
        ProtocolVersionRange::new(min, max).unwrap(),
        set(supported),
        set(required),
    )
    .unwrap()
}

#[test]
fn negotiates_highest_common_protocol_and_mutual_capabilities() {
    let manager = offer(
        1,
        4,
        &[Capability::CommandDeduplication, Capability::OrderedReports],
        &[Capability::CommandDeduplication],
    );
    let worker = offer(
        2,
        3,
        &[
            Capability::CommandDeduplication,
            Capability::WorkerIncarnations,
        ],
        &[Capability::CommandDeduplication],
    );

    let negotiated = manager.negotiate(&worker).unwrap();
    assert_eq!(negotiated.version(), 3);
    assert_eq!(
        negotiated.capabilities(),
        &set(&[Capability::CommandDeduplication])
    );
}

#[test]
fn rejects_disjoint_versions_and_unsatisfied_required_capabilities() {
    let manager = offer(1, 2, &[Capability::CommandDeduplication], &[]);
    let worker = offer(3, 4, &[Capability::CommandDeduplication], &[]);
    assert_eq!(
        manager.negotiate(&worker),
        Err(NegotiationError::NoCommonVersion)
    );

    let manager = offer(
        1,
        1,
        &[Capability::OrderedReports],
        &[Capability::OrderedReports],
    );
    let worker = offer(1, 1, &[Capability::CommandDeduplication], &[]);
    assert_eq!(
        manager.negotiate(&worker),
        Err(NegotiationError::UnsupportedRequiredCapability)
    );
}

#[test]
fn offers_and_version_ranges_are_validated_on_construction_and_decode() {
    assert_eq!(
        ProtocolVersionRange::new(2, 1),
        Err(NegotiationError::InvalidVersionRange)
    );
    assert_eq!(
        ProtocolVersionRange::new(0, 1),
        Err(NegotiationError::InvalidVersionRange)
    );
    assert!(serde_json::from_str::<ProtocolVersionRange>(r#"{"min":3,"max":2}"#).is_err());
    assert!(serde_json::from_str::<ProtocolVersionRange>(r#"{"min":1,"max":2,"next":3}"#).is_err());

    let offer = offer(
        1,
        1,
        &[Capability::CommandDeduplication],
        &[Capability::CommandDeduplication],
    );
    let encoded = serde_json::to_string(&offer).unwrap();
    assert_eq!(
        serde_json::from_str::<ProtocolOffer>(&encoded).unwrap(),
        offer
    );
    assert!(serde_json::from_str::<ProtocolOffer>(
        r#"{"versions":{"min":1,"max":1},"supportedCapabilities":[],"requiredCapabilities":["command_deduplication"]}"#
    )
    .is_err());
    assert!(serde_json::from_str::<ProtocolOffer>(
        r#"{"versions":{"min":1,"max":1},"supportedCapabilities":["future_mandatory_capability"],"requiredCapabilities":["future_mandatory_capability"]}"#
    )
    .is_err());
    assert!(serde_json::from_str::<ProtocolOffer>(
        r#"{"versions":{"min":1,"max":1},"supportedCapabilities":[],"requiredCapabilities":[],"unknown":true}"#
    )
    .is_err());
}
