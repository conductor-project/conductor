use conductor_core::{
    MAX_OVERLAY_DIRECT_CANDIDATES, NetworkDomainId, OverlayEndpointId, OverlayNetworkDomainConfig,
    OverlayPeer, OverlayPeerObservation, OverlayPeerObservationStatus, OverlayPeerState, WorkerId,
    valid_overlay_direct_candidates,
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
        relay_url: "https://relay.example.test".to_owned(),
        worker_address_pool: "10.88.0.0/24".to_owned(),
        service_address_pool: "10.88.1.0/24".to_owned(),
    };
    let json = serde_json::to_string(&domain).unwrap();
    assert!(serde_json::from_str::<OverlayNetworkDomainConfig>(&json).is_ok());
    assert!(serde_json::from_str::<OverlayNetworkDomainConfig>(
        r#"{"networkDomainId":"production","relayUrl":"https://relay.example.test","workerAddressPool":"10.88.0.0/24","serviceAddressPool":"10.88.1.0/24","privateKey":"secret"}"#
    )
    .is_err());

    let configuration = conductor_core::OverlayPeerConfiguration {
        schema_version: conductor_core::OVERLAY_PEER_CONFIGURATION_SCHEMA_VERSION,
        network_domain_id: domain.network_domain_id.clone(),
        worker_id: WorkerId::new("worker-a").unwrap(),
        relay_url: domain.relay_url.clone(),
        worker_address: "10.88.0.1".parse().unwrap(),
        peer_generation: 2,
        network_generation: 3,
        expires_at_ms: 10_000,
        peers: Vec::new(),
    };
    let configuration_json = serde_json::to_value(&configuration).unwrap();
    assert_eq!(configuration_json["schemaVersion"], 1);
    assert_eq!(configuration_json["relayUrl"], "https://relay.example.test");

    let peer = OverlayPeer {
        worker_id: WorkerId::new("worker-a").unwrap(),
        endpoint_id: OverlayEndpointId::new("cd".repeat(32)).unwrap(),
        worker_address: "10.88.0.1".parse().unwrap(),
        peer_generation: 2,
        direct_candidates: Vec::new(),
        direct_candidates_expires_at_ms: None,
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

#[test]
fn direct_underlay_candidate_list_is_bounded_and_routable() {
    let valid = ["192.168.50.12:42500".parse().unwrap()];
    assert!(valid_overlay_direct_candidates(&valid));
    let loopback = ["127.0.0.1:42500".parse().unwrap()];
    assert!(!valid_overlay_direct_candidates(&loopback));
    let duplicates = [
        "192.168.50.12:42500".parse().unwrap(),
        "192.168.50.12:42500".parse().unwrap(),
    ];
    assert!(!valid_overlay_direct_candidates(&duplicates));
    let oversized = (0..=MAX_OVERLAY_DIRECT_CANDIDATES)
        .map(|index| format!("192.168.50.{}:{}", index + 1, 42500 + index))
        .map(|candidate| candidate.parse().unwrap())
        .collect::<Vec<_>>();
    assert!(!valid_overlay_direct_candidates(&oversized));
}
