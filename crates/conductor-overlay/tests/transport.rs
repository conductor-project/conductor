use conductor_core::{OverlayEndpointId, OverlayPeer, WorkerId};
use conductor_overlay::{
    MAX_OVERLAY_PACKET_BYTES, OverlayPacketError, OverlayTransportError, accept_peer, connect_peer,
    create_endpoint, packet_router, receive_packet, send_packet,
};
use iroh_base::SecretKey;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

#[tokio::test]
#[ignore = "requires directly reachable non-loopback local UDP candidates"]
async fn two_local_iroh_endpoints_exchange_authenticated_overlay_datagrams() {
    let endpoint_a = create_endpoint(SecretKey::from_bytes(&[41; 32]))
        .await
        .expect("bind relay-free endpoint A");
    let endpoint_b = create_endpoint(SecretKey::from_bytes(&[42; 32]))
        .await
        .expect("bind relay-free endpoint B");
    let candidates_a = direct_ipv4_candidates(&endpoint_a);
    let candidates_b = direct_ipv4_candidates(&endpoint_b);
    assert!(
        !candidates_a.is_empty(),
        "endpoint A has no direct IPv4 address"
    );
    assert!(
        !candidates_b.is_empty(),
        "endpoint B has no direct IPv4 address"
    );

    let overlay_a = Ipv4Addr::new(198, 18, 12, 1);
    let overlay_b = Ipv4Addr::new(198, 18, 12, 2);
    let router_a = packet_router(
        overlay_a,
        &[authorized_peer(
            "local-worker-b",
            endpoint_b.id(),
            overlay_b,
        )],
    )
    .expect("build A's authorized peer table");
    let router_b = packet_router(
        overlay_b,
        &[authorized_peer(
            "local-worker-a",
            endpoint_a.id(),
            overlay_a,
        )],
    )
    .expect("build B's authorized peer table");

    let client_connect = tokio::time::timeout(
        Duration::from_secs(10),
        connect_peer(&endpoint_a, endpoint_b.id(), &candidates_b),
    );
    let server_accept =
        tokio::time::timeout(Duration::from_secs(10), accept_peer(&endpoint_b, &router_b));
    let (client_connection, server_connection) = tokio::join!(client_connect, server_accept);
    let client_connection = client_connection
        .expect("direct Iroh connection stays within ten seconds")
        .expect("connect only to endpoint B's advertised direct candidates");
    let server_connection = server_connection
        .expect("Iroh peer acceptance stays within ten seconds")
        .expect("accept only the manager-authorized endpoint A");

    assert!(client_connection.paths().iter().any(|path| {
        path.is_selected()
            && matches!(path.remote_addr(), iroh::TransportAddr::Ip(address) if candidates_b.contains(address))
    }));

    let forward_packet = ipv4_packet(overlay_a, overlay_b, b"forward-payload");
    send_packet(&client_connection, &forward_packet).expect("send a bounded IPv4 datagram");
    let received = tokio::time::timeout(
        Duration::from_secs(5),
        receive_packet(&server_connection, &router_b),
    )
    .await
    .expect("receive the forward datagram within five seconds")
    .expect("authenticate source address against endpoint A");
    assert_eq!(received.bytes, forward_packet);

    let reverse_packet = ipv4_packet(overlay_b, overlay_a, b"reverse-payload");
    send_packet(&server_connection, &reverse_packet).expect("send the reverse IPv4 datagram");
    let received = tokio::time::timeout(
        Duration::from_secs(5),
        receive_packet(&client_connection, &router_a),
    )
    .await
    .expect("receive the reverse datagram within five seconds")
    .expect("authenticate source address against endpoint B");
    assert_eq!(received.bytes, reverse_packet);

    let spoofed_packet = ipv4_packet(Ipv4Addr::new(198, 18, 12, 99), overlay_b, b"spoof");
    send_packet(&client_connection, &spoofed_packet)
        .expect("the generic packet framing is valid before peer authorization");
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        receive_packet(&server_connection, &router_b),
    )
    .await
    .expect("process the spoofed datagram within five seconds")
    .expect_err("reject a source address not assigned to endpoint A");
    assert!(matches!(
        error,
        OverlayTransportError::InvalidPacket(OverlayPacketError::PeerAddressMismatch)
    ));

    client_connection.close(0_u32.into(), b"test complete");
    server_connection.close(0_u32.into(), b"test complete");
    endpoint_a.close().await;
    endpoint_b.close().await;
}

fn direct_ipv4_candidates(endpoint: &iroh::Endpoint) -> Vec<SocketAddr> {
    let mut candidates = endpoint
        .addr()
        .ip_addrs()
        .filter(|address| matches!(address.ip(), IpAddr::V4(_)))
        .copied()
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates.dedup();
    candidates.retain(|address| conductor_core::valid_overlay_direct_candidates(&[*address]));
    candidates.truncate(conductor_core::MAX_OVERLAY_DIRECT_CANDIDATES);
    candidates
}

fn authorized_peer(
    worker_id: &str,
    endpoint_id: iroh_base::EndpointId,
    address: Ipv4Addr,
) -> OverlayPeer {
    OverlayPeer {
        worker_id: WorkerId::new(worker_id).expect("valid worker ID"),
        endpoint_id: OverlayEndpointId::new(endpoint_id.to_string()).expect("valid endpoint ID"),
        worker_address: address,
        peer_generation: 1,
        direct_candidates: Vec::new(),
        direct_candidates_expires_at_ms: None,
    }
}

fn ipv4_packet(source: Ipv4Addr, destination: Ipv4Addr, payload: &[u8]) -> Vec<u8> {
    let total_length = 20 + payload.len();
    assert!(total_length <= MAX_OVERLAY_PACKET_BYTES);
    let mut packet = vec![0; total_length];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_length as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    packet[20..].copy_from_slice(payload);
    packet
}
