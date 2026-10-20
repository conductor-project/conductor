use conductor_core::{OverlayEndpointId, OverlayPeer, WorkerId};
use conductor_overlay::{
    MAX_OVERLAY_PACKET_BYTES, OverlayConfigurationError, OverlayPacketError, OverlayPacketRouter,
    configured_packet_router, parse_ipv4_packet,
};
use iroh_base::EndpointId;
use std::net::Ipv4Addr;

fn peer(id: &str, address: Ipv4Addr) -> OverlayPeer {
    OverlayPeer {
        worker_id: WorkerId::new(format!("worker-{address}")).unwrap(),
        endpoint_id: OverlayEndpointId::new(id.to_owned()).unwrap(),
        worker_address: address,
        peer_generation: 1,
        direct_candidates: Vec::new(),
        direct_candidates_expires_at_ms: None,
    }
}

fn packet(source: Ipv4Addr, destination: Ipv4Addr) -> Vec<u8> {
    let mut bytes = vec![0_u8; 20];
    bytes[0] = 0x45;
    bytes[2..4].copy_from_slice(&20_u16.to_be_bytes());
    bytes[8] = 64;
    bytes[9] = 17;
    bytes[12..16].copy_from_slice(&source.octets());
    bytes[16..20].copy_from_slice(&destination.octets());
    bytes
}

#[test]
fn outbound_routes_only_from_local_address_to_an_authorized_peer() {
    let local = Ipv4Addr::new(10, 90, 0, 1);
    let remote = Ipv4Addr::new(10, 90, 0, 2);
    let router =
        OverlayPacketRouter::new(local, &[peer(&format!("58{}", "66".repeat(31)), remote)])
            .unwrap();
    let routed = router.route_outbound(&packet(local, remote)).unwrap();
    assert_eq!(routed.destination, remote);
    assert_eq!(
        router.route_outbound(&packet(remote, local)),
        Err(OverlayPacketError::InvalidSource)
    );
    assert_eq!(
        router.route_outbound(&packet(local, Ipv4Addr::new(10, 90, 0, 3))),
        Err(OverlayPacketError::UnknownPeer)
    );
}

#[test]
fn inbound_requires_authenticated_endpoint_source_and_local_destination() {
    let local = Ipv4Addr::new(10, 90, 0, 1);
    let remote = Ipv4Addr::new(10, 90, 0, 2);
    let id_text = format!("58{}", "66".repeat(31));
    let id = id_text.parse::<EndpointId>().unwrap();
    let router = OverlayPacketRouter::new(local, &[peer(&id_text, remote)]).unwrap();
    assert!(router.validate_inbound(id, &packet(remote, local)).is_ok());
    assert_eq!(
        router.validate_inbound(id, &packet(local, local)),
        Err(OverlayPacketError::PeerAddressMismatch)
    );
    assert_eq!(
        router.validate_inbound(id, &packet(remote, remote)),
        Err(OverlayPacketError::InvalidDestination)
    );
}

#[test]
fn packet_parser_rejects_truncation_trailing_bytes_and_mtu_excess() {
    let local = Ipv4Addr::new(10, 90, 0, 1);
    let remote = Ipv4Addr::new(10, 90, 0, 2);
    let valid = packet(local, remote);
    assert!(parse_ipv4_packet(&valid).is_ok());
    assert_eq!(
        parse_ipv4_packet(&valid[..19]),
        Err(OverlayPacketError::MalformedIpv4)
    );
    let mut trailing = valid.clone();
    trailing.push(0);
    assert_eq!(
        parse_ipv4_packet(&trailing),
        Err(OverlayPacketError::MalformedIpv4)
    );
    assert_eq!(
        parse_ipv4_packet(&vec![0; MAX_OVERLAY_PACKET_BYTES + 1]),
        Err(OverlayPacketError::PacketTooLarge)
    );
}

#[test]
fn peer_table_rejects_duplicate_addresses_and_endpoint_ids() {
    let local = Ipv4Addr::new(10, 90, 0, 1);
    let endpoint_id = format!("58{}", "66".repeat(31));
    assert!(matches!(
        OverlayPacketRouter::new(
            local,
            &[
                peer(&endpoint_id, Ipv4Addr::new(10, 90, 0, 2)),
                peer(&endpoint_id, Ipv4Addr::new(10, 90, 0, 2)),
            ]
        ),
        Err(OverlayPacketError::DuplicatePeer)
    ));
}

#[test]
fn manager_snapshot_checks_schema_identity_lease_and_candidates() {
    let worker_id = WorkerId::new("worker-a").unwrap();
    let configuration = conductor_core::OverlayPeerConfiguration {
        schema_version: conductor_core::OVERLAY_PEER_CONFIGURATION_SCHEMA_VERSION,
        network_domain_id: conductor_core::NetworkDomainId::new("domain-a").unwrap(),
        worker_id: worker_id.clone(),
        worker_address: Ipv4Addr::new(10, 90, 0, 1),
        peer_generation: 1,
        network_generation: 1,
        expires_at_ms: 10_000,
        peers: vec![],
    };
    assert!(configured_packet_router(&configuration, &worker_id, 9_999).is_ok());

    let mut unsupported = configuration.clone();
    unsupported.schema_version += 1;
    assert!(matches!(
        configured_packet_router(&unsupported, &worker_id, 1),
        Err(OverlayConfigurationError::UnsupportedSchema)
    ));
    assert!(matches!(
        configured_packet_router(&configuration, &worker_id, 10_000),
        Err(OverlayConfigurationError::InvalidLease)
    ));
    assert!(matches!(
        configured_packet_router(&configuration, &WorkerId::new("worker-b").unwrap(), 1),
        Err(OverlayConfigurationError::WrongWorker)
    ));
    let mut invalid_candidates = configuration;
    invalid_candidates.peers.push(OverlayPeer {
        direct_candidates: vec!["127.0.0.1:4242".parse().unwrap()],
        direct_candidates_expires_at_ms: Some(9_999),
        ..peer(
            &format!("58{}", "77".repeat(31)),
            Ipv4Addr::new(10, 90, 0, 2),
        )
    });
    assert!(matches!(
        configured_packet_router(&invalid_candidates, &worker_id, 1),
        Err(OverlayConfigurationError::InvalidCandidates)
    ));
}
