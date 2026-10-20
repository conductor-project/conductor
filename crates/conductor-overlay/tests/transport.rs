use conductor_core::{OverlayEndpointId, OverlayPeer, WorkerId};
use conductor_overlay::{
    MAX_OVERLAY_PACKET_BYTES, OverlayPacketError, OverlayTransportError, accept_peer, connect_peer,
    create_endpoint, packet_router, receive_packet, send_packet,
};
use iroh_base::SecretKey;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

#[cfg(target_os = "linux")]
use conductor_core::{NetworkDomainId, OverlayPeerConfiguration};
#[cfg(target_os = "linux")]
use conductor_overlay::{LinuxOverlayInterface, OverlayRuntimeStatus, run_packet_dataplane};
#[cfg(target_os = "linux")]
use iroh::{Endpoint, RelayMode, endpoint::presets::Minimal};
#[cfg(target_os = "linux")]
use std::net::{Ipv6Addr, SocketAddrV6};
#[cfg(target_os = "linux")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(target_os = "linux")]
use tokio::net::{TcpListener, TcpStream};

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

    let selected_candidate = client_connection
        .paths()
        .iter()
        .find(|path| path.is_selected())
        .and_then(|path| match path.remote_addr() {
            iroh::TransportAddr::Ip(address) => Some(*address),
            _ => None,
        })
        .expect("select an IP underlay path");
    assert!(candidates_b.contains(&selected_candidate));
    let capture_marker = format!(
        "conductor-iroh-capture-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test clock after Unix epoch")
            .as_nanos()
    );
    println!("capture_selected_candidate={selected_candidate}");
    println!("capture_marker={capture_marker}");

    let forward_packet = ipv4_packet(overlay_a, overlay_b, capture_marker.as_bytes());
    send_packet(&client_connection, &forward_packet).expect("send a bounded IPv4 datagram");
    let received = tokio::time::timeout(
        Duration::from_secs(5),
        receive_packet(&server_connection, &router_b),
    )
    .await
    .expect("receive the forward datagram within five seconds")
    .expect("authenticate source address against endpoint A");
    assert_eq!(received.bytes, forward_packet);

    let reverse_marker = format!("{capture_marker}-reverse");
    let reverse_packet = ipv4_packet(overlay_b, overlay_a, reverse_marker.as_bytes());
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

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires two Linux hosts with direct UDP candidates and TUN privileges"]
async fn independent_linux_hosts_exchange_tcp_over_iroh_tun() {
    let role = std::env::var("CONDUCTOR_OVERLAY_TEST_ROLE").expect("test role is configured");
    assert!(role == "a" || role == "b", "test role must be a or b");
    let local_worker = WorkerId::new(if role == "a" {
        "cross-host-a"
    } else {
        "cross-host-b"
    })
    .expect("valid local worker ID");
    let remote_worker = WorkerId::new(if role == "a" {
        "cross-host-b"
    } else {
        "cross-host-a"
    })
    .expect("valid remote worker ID");
    let local_address = if role == "a" {
        Ipv4Addr::new(198, 18, 14, 1)
    } else {
        Ipv4Addr::new(198, 18, 14, 2)
    };
    let remote_address = if role == "a" {
        Ipv4Addr::new(198, 18, 14, 2)
    } else {
        Ipv4Addr::new(198, 18, 14, 1)
    };
    let local_secret = SecretKey::from_bytes(if role == "a" { &[61; 32] } else { &[62; 32] });
    let remote_secret = SecretKey::from_bytes(if role == "a" { &[62; 32] } else { &[61; 32] });
    let local_endpoint = endpoint_on_ports(
        local_secret,
        std::env::var("CONDUCTOR_OVERLAY_TEST_BIND_PORT_V4")
            .expect("test bind port is configured")
            .parse::<u16>()
            .expect("test bind port is valid"),
        std::env::var("CONDUCTOR_OVERLAY_TEST_BIND_PORT_V6")
            .expect("test bind port is configured")
            .parse::<u16>()
            .expect("test bind port is valid"),
    )
    .await;
    let candidate = std::env::var("CONDUCTOR_OVERLAY_TEST_PEER_CANDIDATE")
        .expect("peer candidate is configured")
        .parse::<SocketAddr>()
        .expect("peer candidate is a socket address");
    println!(
        "role={role} endpoint_id={} bound_candidates={:?} peer_candidate={candidate}",
        local_endpoint.id(),
        local_endpoint
            .addr()
            .ip_addrs()
            .copied()
            .collect::<Vec<_>>()
    );
    let peer = OverlayPeer {
        worker_id: remote_worker,
        endpoint_id: OverlayEndpointId::new(remote_secret.public().to_string())
            .expect("valid remote endpoint ID"),
        worker_address: remote_address,
        peer_generation: 1,
        direct_candidates: vec![candidate],
        direct_candidates_expires_at_ms: Some(unix_time_ms().saturating_add(300_000)),
    };
    let configuration = OverlayPeerConfiguration {
        schema_version: conductor_core::OVERLAY_PEER_CONFIGURATION_SCHEMA_VERSION,
        network_domain_id: NetworkDomainId::new("cross-host-test")
            .expect("valid network domain ID"),
        worker_id: local_worker.clone(),
        worker_address: local_address,
        peer_generation: 1,
        network_generation: 1,
        expires_at_ms: unix_time_ms().saturating_add(300_000),
        peers: vec![peer],
    };
    let interface_name = conductor_overlay::interface_name_for_worker(&local_worker);
    let interface = LinuxOverlayInterface::start(&interface_name, local_address, [remote_address])
        .await
        .expect("create owned TUN and peer route");
    let runtime_status = OverlayRuntimeStatus::default();
    let run_configuration = configuration.clone();
    let run_worker = local_worker.clone();
    let run_endpoint = local_endpoint.clone();
    let run_device = interface.device();
    let run_status = runtime_status.clone();
    let mut dataplane = tokio::spawn(async move {
        run_packet_dataplane(
            run_device,
            run_endpoint,
            &run_configuration,
            &run_worker,
            unix_time_ms(),
            run_status,
        )
        .await
    });

    let (result, dataplane_finished) = tokio::select! {
        result = exchange_overlay_tcp(
            role.as_str(),
            local_address,
            remote_address,
            &runtime_status,
        ) => (result, false),
        dataplane_result = &mut dataplane => (
            Err(format!(
                "overlay dataplane exited before application exchange: {dataplane_result:?}"
            ).into()),
            true,
        ),
    };

    if !dataplane_finished {
        dataplane.abort();
        let _ = dataplane.await;
    }
    local_endpoint.close().await;
    let cleanup = interface.shutdown().await;
    cleanup.expect("remove only the test-owned interface and route");
    result.expect("exchange and verify the application payload over the overlay");
}

#[cfg(target_os = "linux")]
async fn endpoint_on_ports(secret_key: SecretKey, ipv4_port: u16, ipv6_port: u16) -> Endpoint {
    Endpoint::builder(Minimal)
        .clear_ip_transports()
        .bind_addr(SocketAddr::from(([0, 0, 0, 0], ipv4_port)))
        .expect("valid IPv4 bind address")
        .bind_addr(SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::UNSPECIFIED,
            ipv6_port,
            0,
            0,
        )))
        .expect("valid IPv6 bind address")
        .secret_key(secret_key)
        .relay_mode(RelayMode::Disabled)
        .alpns(vec![conductor_overlay::OVERLAY_ALPN.to_vec()])
        .bind()
        .await
        .expect("bind relay-free direct endpoint")
}

#[cfg(target_os = "linux")]
async fn exchange_overlay_tcp(
    role: &str,
    local_address: Ipv4Addr,
    remote_address: Ipv4Addr,
    runtime_status: &OverlayRuntimeStatus,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let selected_address = loop {
        if let Some(address) = runtime_status
            .direct_links()
            .await
            .into_iter()
            .find(|link| link.connected)
            .and_then(|link| link.selected_address)
        {
            break address;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "no authenticated direct Iroh path within 30 seconds: {:?}",
                runtime_status.direct_links().await
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    println!("role={role} selected_direct_candidate={selected_address}");

    if role == "b" {
        let listener = TcpListener::bind(SocketAddr::from((local_address, 18080))).await?;
        let (mut stream, _) =
            tokio::time::timeout(Duration::from_secs(30), listener.accept()).await??;
        let mut request = vec![0; 65_536];
        tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut request)).await??;
        if !request.starts_with(b"conductor-cross-host-") {
            return Err("application payload marker was not received".into());
        }
        stream.write_all(&request).await?;
        stream.shutdown().await?;
    } else {
        let marker = format!(
            "conductor-cross-host-{}-{}",
            std::process::id(),
            unix_time_ms()
        );
        println!("capture_marker={marker}");
        let mut payload = vec![0x5a; 65_536];
        payload[..marker.len()].copy_from_slice(marker.as_bytes());
        let mut stream = tokio::time::timeout(
            Duration::from_secs(30),
            TcpStream::connect(SocketAddr::from((remote_address, 18080))),
        )
        .await??;
        stream.write_all(&payload).await?;
        stream.shutdown().await?;
        let mut response = vec![0; payload.len()];
        tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut response)).await??;
        if response != payload {
            return Err("application response payload differs from the request".into());
        }
        println!("application_bytes={} payload_match=true", response.len());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn unix_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is after Unix epoch")
        .as_millis() as i64
}

fn direct_ipv4_candidates(endpoint: &iroh::Endpoint) -> Vec<SocketAddr> {
    // Do not let this single-host simulation silently use a Tailscale/CGNAT
    // candidate when an ordinary LAN candidate is available.
    let mut candidates = endpoint
        .addr()
        .ip_addrs()
        .filter(|address| match address.ip() {
            IpAddr::V4(ip) => {
                let octets = ip.octets();
                !(octets[0] == 100 && (64..=127).contains(&octets[1]))
            }
            IpAddr::V6(_) => false,
        })
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
