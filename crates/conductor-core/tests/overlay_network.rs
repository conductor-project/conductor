use conductor_core::{
    NetworkDomainId, OverlayEndpointId, OverlayNetworkDomainConfig, OverlayPeer,
    OverlayPeerObservation, OverlayPeerObservationStatus, OverlayPeerState, WorkerId,
};

#[test]
fn endpoint_ids_accept_only_canonical_lowercase_hex() {
    assert!(OverlayEndpointId::new("ab".repeat(32)).is_ok());
    assert!(OverlayEndpointId::new("AB".repeat(32)).is_err());
    assert!(OverlayEndpointId::new("ab".repeat(31)).is_err());
}

#[test]
fn overlay_contracts_use_bounded_strict_json_shapes() {
    let domain = OverlayNetworkDomainConfig {
        network_domain_id: NetworkDomainId::new("production").unwrap(),
        worker_address_pool: "10.88.0.0/24".to_owned(),
        service_address_pool: "10.88.1.0/24".to_owned(),
    };
    let json = serde_json::to_string(&domain).unwrap();
    assert!(serde_json::from_str::<OverlayNetworkDomainConfig>(&json).is_ok());
    assert!(serde_json::from_str::<OverlayNetworkDomainConfig>(
        r#"{"networkDomainId":"production","workerAddressPool":"10.88.0.0/24","serviceAddressPool":"10.88.1.0/24","privateKey":"secret"}"#
    )
    .is_err());

    let peer = OverlayPeer {
        worker_id: WorkerId::new("worker-a").unwrap(),
        endpoint_id: OverlayEndpointId::new("cd".repeat(32)).unwrap(),
        worker_address: "10.88.0.1".parse().unwrap(),
        peer_generation: 2,
    };
    assert_eq!(peer.worker_address.to_string(), "10.88.0.1");
    assert_eq!(
        serde_json::to_string(&OverlayPeerState::Active).unwrap(),
        "\"active\""
    );
    let observation = OverlayPeerObservation {
        network_domain_id: domain.network_domain_id,
        worker_id: peer.worker_id,
        peer_generation: peer.peer_generation,
        applied_network_generation: 3,
        observed_at_ms: 10,
        status: OverlayPeerObservationStatus::Applied,
    };
    let observation_json = serde_json::to_value(observation).unwrap();
    assert_eq!(observation_json["status"], "applied");
    assert_eq!(observation_json["observedAtMs"], 10);
}
