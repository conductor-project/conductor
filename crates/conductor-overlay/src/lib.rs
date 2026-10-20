//! Rust-native worker packet transport and overlay packet validation.
//!
//! This crate owns the Iroh data plane. It does not install host routes or
//! grant network-administration privileges; those operations are delegated to
//! the separately scoped network helper.

mod packet;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
mod linux_helper;

pub use packet::{
    MAX_OVERLAY_PACKET_BYTES, OverlayPacketError, OverlayPacketRouter, RoutedPacket,
    parse_ipv4_packet,
};

#[cfg(target_os = "linux")]
pub use linux::LinuxOverlayInterface;
#[cfg(target_os = "linux")]
pub use linux_helper::LinuxOverlayHelperClient;

use bytes::Bytes;
use conductor_core::{
    OVERLAY_PEER_CONFIGURATION_SCHEMA_VERSION, OverlayConnectionAttemptOutcome, OverlayPeer,
    OverlayPeerConfiguration, WorkerId,
};
use iroh::{Endpoint, EndpointAddr, RelayMode, TransportAddr, endpoint::Connection};
use iroh_base::{EndpointId, SecretKey};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc, watch};

/// Protocol identifier for Conductor's IP packet datagrams over Iroh.
pub const OVERLAY_ALPN: &[u8] = b"conductor/overlay/1";
pub const MAX_OVERLAY_PEERS: usize = 128;

/// Live direct-link telemetry collected by the worker's packet loop.
#[derive(Clone, Default)]
pub struct OverlayRuntimeStatus {
    connections: Arc<RwLock<BTreeMap<EndpointId, TrackedConnection>>>,
}

#[derive(Clone, Default)]
struct TrackedConnection {
    connection: Option<Connection>,
    selected_address: Option<std::net::SocketAddr>,
    handshake_at_ms: Option<i64>,
    udp_tx_bytes: u64,
    udp_rx_bytes: u64,
    last_attempt_at_ms: Option<i64>,
    last_attempt_outcome: Option<OverlayConnectionAttemptOutcome>,
}

impl OverlayRuntimeStatus {
    pub async fn clear(&self) {
        self.connections.write().await.clear();
    }

    async fn set_peers(&self, peers: &[OverlayPeer]) -> Result<(), OverlayTransportError> {
        let mut connections = BTreeMap::new();
        for peer in peers {
            let endpoint_id = peer
                .endpoint_id
                .as_str()
                .parse::<EndpointId>()
                .map_err(|_| OverlayTransportError::UnauthorizedPeer)?;
            connections.insert(endpoint_id, TrackedConnection::default());
        }
        *self.connections.write().await = connections;
        Ok(())
    }

    async fn connected(&self, endpoint_id: EndpointId, connection: Connection) {
        let selected_address = selected_direct_address(&connection);
        let now_ms = unix_time_ms();
        self.connections.write().await.insert(
            endpoint_id,
            TrackedConnection {
                connection: Some(connection),
                selected_address,
                handshake_at_ms: Some(now_ms),
                udp_tx_bytes: 0,
                udp_rx_bytes: 0,
                last_attempt_at_ms: Some(now_ms),
                last_attempt_outcome: Some(OverlayConnectionAttemptOutcome::Connected),
            },
        );
    }

    async fn connection_failed(
        &self,
        endpoint_id: EndpointId,
        outcome: OverlayConnectionAttemptOutcome,
    ) {
        let mut connections = self.connections.write().await;
        let entry = connections.entry(endpoint_id).or_insert(TrackedConnection {
            connection: None,
            selected_address: None,
            handshake_at_ms: None,
            udp_tx_bytes: 0,
            udp_rx_bytes: 0,
            last_attempt_at_ms: None,
            last_attempt_outcome: None,
        });
        entry.last_attempt_at_ms = Some(unix_time_ms());
        entry.last_attempt_outcome = Some(outcome);
    }

    async fn disconnected(&self, endpoint_id: EndpointId) {
        if let Some(entry) = self.connections.write().await.get_mut(&endpoint_id)
            && let Some(connection) = entry.connection.take()
        {
            let stats = connection.stats();
            entry.udp_tx_bytes = stats.udp_tx.bytes;
            entry.udp_rx_bytes = stats.udp_rx.bytes;
            entry.selected_address =
                selected_direct_address(&connection).or(entry.selected_address);
        }
    }

    pub async fn direct_links(&self) -> Vec<conductor_core::OverlayDirectLinkDiagnostic> {
        let connections = self.connections.read().await;
        connections
            .iter()
            .filter_map(|(endpoint_id, entry)| {
                let (udp_tx_bytes, udp_rx_bytes, selected_address) =
                    if let Some(connection) = entry.connection.as_ref() {
                        let stats = connection.stats();
                        (
                            stats.udp_tx.bytes,
                            stats.udp_rx.bytes,
                            selected_direct_address(connection).or(entry.selected_address),
                        )
                    } else {
                        (
                            entry.udp_tx_bytes,
                            entry.udp_rx_bytes,
                            entry.selected_address,
                        )
                    };
                Some(conductor_core::OverlayDirectLinkDiagnostic {
                    endpoint_id: conductor_core::OverlayEndpointId::new(endpoint_id.to_string())
                        .ok()?,
                    connected: entry
                        .connection
                        .as_ref()
                        .is_some_and(|connection| connection.close_reason().is_none()),
                    selected_address,
                    last_handshake_at_ms: entry.handshake_at_ms,
                    udp_tx_bytes,
                    udp_rx_bytes,
                    last_attempt_at_ms: entry.last_attempt_at_ms,
                    last_attempt_outcome: entry.last_attempt_outcome,
                })
            })
            .collect()
    }
}

fn selected_direct_address(connection: &Connection) -> Option<std::net::SocketAddr> {
    let paths = connection.paths();
    let path = paths.iter().find(|path| path.is_selected())?;
    match path.remote_addr() {
        TransportAddr::Ip(address) => Some(*address),
        _ => None,
    }
}

fn connection_attempt_outcome(error: &OverlayTransportError) -> OverlayConnectionAttemptOutcome {
    use OverlayConnectionAttemptOutcome as Outcome;
    match error {
        OverlayTransportError::NoDirectCandidates => Outcome::NoCandidates,
        OverlayTransportError::CandidateLeaseExpired => Outcome::CandidatesExpired,
        OverlayTransportError::PeerUnavailable => Outcome::TimedOut,
        OverlayTransportError::UnauthorizedPeer => Outcome::IdentityMismatch,
        OverlayTransportError::Iroh(iroh::endpoint::ConnectError::Connecting {
            source: iroh::endpoint::ConnectingError::HandshakeFailure { .. },
            ..
        }) => Outcome::AuthenticationFailed,
        OverlayTransportError::Iroh(_) => Outcome::UnderlayUnavailable,
        _ => Outcome::OtherFailure,
    }
}

/// Return a stable, bounded, Conductor-prefixed Linux interface name.
pub fn interface_name_for_worker(worker_id: &WorkerId) -> String {
    let digest = Sha256::digest(worker_id.as_str().as_bytes());
    let mut suffix = String::with_capacity(11);
    for byte in digest.iter().take(6) {
        use std::fmt::Write as _;
        let _ = write!(suffix, "{byte:02x}");
    }
    suffix.truncate(11);
    format!("cnd-{suffix}")
}

#[derive(Debug)]
pub enum OverlayTransportError {
    UnauthorizedPeer,
    EndpointClosed,
    PeerUnavailable,
    NoDirectCandidates,
    CandidateLeaseExpired,
    DatagramUnsupported,
    PacketTooLarge,
    Bind(iroh::endpoint::BindError),
    Iroh(iroh::endpoint::ConnectError),
    Accept(iroh::endpoint::ConnectionError),
    AcceptHandshake(iroh::endpoint::ConnectingError),
    Send(iroh::endpoint::SendDatagramError),
    Read(iroh::endpoint::ConnectionError),
    InvalidPacket(OverlayPacketError),
    InvalidConfiguration(OverlayConfigurationError),
    TunnelIo(std::io::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverlayConfigurationError {
    UnsupportedSchema,
    WrongWorker,
    InvalidLease,
    InvalidCandidates,
    TooManyPeers,
    InvalidPeers(OverlayPacketError),
}

/// Create one worker endpoint with no relay or address lookup services.
pub async fn create_endpoint(secret_key: SecretKey) -> Result<Endpoint, OverlayTransportError> {
    Endpoint::builder(iroh::endpoint::presets::Minimal)
        .secret_key(secret_key)
        .relay_mode(RelayMode::Disabled)
        .alpns(vec![OVERLAY_ALPN.to_vec()])
        .bind()
        .await
        .map_err(OverlayTransportError::Bind)
}

/// Establish an authenticated Iroh connection using only manager-distributed
/// direct underlay candidates. The peer key is checked after the QUIC handshake.
pub async fn connect_peer(
    endpoint: &Endpoint,
    endpoint_id: EndpointId,
    candidates: &[std::net::SocketAddr],
) -> Result<Connection, OverlayTransportError> {
    if candidates.is_empty() {
        return Err(OverlayTransportError::NoDirectCandidates);
    }
    if !conductor_core::valid_overlay_direct_candidates(candidates) {
        return Err(OverlayTransportError::UnauthorizedPeer);
    }
    let address = EndpointAddr::new(endpoint_id)
        .with_addrs(candidates.iter().copied().map(TransportAddr::Ip));
    let connection = endpoint
        .connect(address, OVERLAY_ALPN)
        .await
        .map_err(OverlayTransportError::Iroh)?;
    if connection.remote_id() != endpoint_id {
        connection.close(1_u32.into(), b"unexpected overlay identity");
        return Err(OverlayTransportError::UnauthorizedPeer);
    }
    Ok(connection)
}

/// Accept an incoming connection only when its authenticated Iroh identity is
/// present in the current manager-issued peer table.
pub async fn accept_peer(
    endpoint: &Endpoint,
    router: &OverlayPacketRouter,
) -> Result<Connection, OverlayTransportError> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or(OverlayTransportError::EndpointClosed)?;
    let accepting = incoming.accept().map_err(OverlayTransportError::Accept)?;
    let connection = accepting
        .await
        .map_err(OverlayTransportError::AcceptHandshake)?;
    if !router.authorizes_endpoint(connection.remote_id()) {
        connection.close(1_u32.into(), b"unauthorized overlay peer");
        return Err(OverlayTransportError::UnauthorizedPeer);
    }
    Ok(connection)
}

/// Send one complete IPv4 packet in an Iroh QUIC datagram.
pub fn send_packet(connection: &Connection, packet: &[u8]) -> Result<(), OverlayTransportError> {
    parse_ipv4_packet(packet).map_err(OverlayTransportError::InvalidPacket)?;
    if packet.len() > MAX_OVERLAY_PACKET_BYTES {
        return Err(OverlayTransportError::PacketTooLarge);
    }
    let max_datagram_size = connection
        .max_datagram_size()
        .ok_or(OverlayTransportError::DatagramUnsupported)?;
    if packet.len() > max_datagram_size {
        return Err(OverlayTransportError::PacketTooLarge);
    }
    connection
        .send_datagram(Bytes::copy_from_slice(packet))
        .map_err(OverlayTransportError::Send)
}

/// Receive one bounded IPv4 packet and verify that its authenticated Iroh peer
/// is authorized to claim the packet's source address.
pub async fn receive_packet(
    connection: &Connection,
    router: &OverlayPacketRouter,
) -> Result<RoutedPacket, OverlayTransportError> {
    let datagram = connection
        .read_datagram()
        .await
        .map_err(OverlayTransportError::Read)?;
    router
        .validate_inbound(connection.remote_id(), datagram.as_ref())
        .map_err(OverlayTransportError::InvalidPacket)
}

/// A validated local packet is mapped to exactly one authorized Iroh peer.
pub fn route_outbound_packet(
    router: &OverlayPacketRouter,
    packet: &[u8],
) -> Result<(EndpointId, Ipv4Addr), OverlayPacketError> {
    let routed = router.route_outbound(packet)?;
    Ok((routed.endpoint_id, routed.destination))
}

/// Run the bounded TUN-to-Iroh packet loop for one validated manager snapshot.
/// The caller owns interface creation, route installation and shutdown policy;
/// this function only exchanges packets over the supplied TUN and Iroh endpoint.
pub async fn run_packet_dataplane(
    device: Arc<tun_rs::AsyncDevice>,
    endpoint: Endpoint,
    configuration: &OverlayPeerConfiguration,
    expected_worker: &WorkerId,
    now_ms: i64,
    runtime_status: OverlayRuntimeStatus,
) -> Result<(), OverlayTransportError> {
    let router = configured_packet_router(configuration, expected_worker, now_ms)
        .map_err(OverlayTransportError::InvalidConfiguration)?;
    runtime_status.set_peers(&configuration.peers).await?;
    let (inbound_tx, mut inbound_rx) = mpsc::channel::<Vec<u8>>(128);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let connections = Arc::clone(&runtime_status.connections);
    for peer in &configuration.peers {
        let endpoint_id = peer
            .endpoint_id
            .as_str()
            .parse::<EndpointId>()
            .map_err(|_| OverlayTransportError::UnauthorizedPeer)?;
        let endpoint = endpoint.clone();
        let candidates = peer.direct_candidates.clone();
        let candidates_expires_at_ms = peer.direct_candidates_expires_at_ms.unwrap_or(0);
        let router = router.clone();
        let inbound_tx = inbound_tx.clone();
        let runtime_status = runtime_status.clone();
        let mut shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_millis(250);
            loop {
                let connected = tokio::select! {
                    _ = shutdown.changed() => break,
                    connected = async {
                        if candidates.is_empty() {
                            Err(OverlayTransportError::NoDirectCandidates)
                        } else if unix_time_ms() >= candidates_expires_at_ms {
                            Err(OverlayTransportError::CandidateLeaseExpired)
                        } else {
                            tokio::time::timeout(
                                std::time::Duration::from_secs(10),
                                connect_peer(&endpoint, endpoint_id, &candidates),
                            ).await.map_err(|_| OverlayTransportError::PeerUnavailable)?
                        }
                    } => connected,
                };
                if let Ok(connection) = connected {
                    spawn_packet_reader(connection.clone(), router.clone(), inbound_tx.clone());
                    runtime_status
                        .connected(endpoint_id, connection.clone())
                        .await;
                    tokio::select! {
                        _ = shutdown.changed() => break,
                        _ = connection.closed() => {},
                    }
                    runtime_status.disconnected(endpoint_id).await;
                    backoff = std::time::Duration::from_millis(250);
                } else {
                    let outcome = connected
                        .as_ref()
                        .err()
                        .map(connection_attempt_outcome)
                        .unwrap_or(OverlayConnectionAttemptOutcome::OtherFailure);
                    runtime_status.connection_failed(endpoint_id, outcome).await;
                    tokio::select! {
                        _ = shutdown.changed() => break,
                        _ = tokio::time::sleep(backoff) => {},
                    }
                    backoff = backoff
                        .saturating_mul(2)
                        .min(std::time::Duration::from_secs(30));
                }
            }
        });
    }

    let accept_endpoint = endpoint.clone();
    let accept_router = router.clone();
    let mut accept_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        loop {
            let result = tokio::select! {
                _ = accept_shutdown.changed() => break,
                result = accept_peer(&accept_endpoint, &accept_router) => result,
            };
            let connection = match result {
                Ok(connection) => connection,
                Err(OverlayTransportError::EndpointClosed) => break,
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            spawn_packet_reader(connection, accept_router.clone(), inbound_tx.clone());
        }
    });

    let lease_remaining = std::time::Duration::from_millis(
        u64::try_from(configuration.expires_at_ms - now_ms).unwrap_or(0),
    );
    let lease_expiry = tokio::time::sleep(lease_remaining);
    tokio::pin!(lease_expiry);
    let mut packet = vec![0_u8; MAX_OVERLAY_PACKET_BYTES];
    loop {
        tokio::select! {
            _ = &mut lease_expiry => {
                let _ = shutdown_tx.send(true);
                endpoint.close().await;
                return Err(OverlayTransportError::InvalidConfiguration(
                    OverlayConfigurationError::InvalidLease,
                ));
            }
            received = device.recv(&mut packet) => {
                let size = received.map_err(OverlayTransportError::TunnelIo)?;
                let (endpoint_id, _) = route_outbound_packet(&router, &packet[..size])
                    .map_err(OverlayTransportError::InvalidPacket)?;
                let connection = connections
                    .read()
                    .await
                    .get(&endpoint_id)
                    .and_then(|entry| entry.connection.clone());
                let Some(connection) = connection else {
                    continue;
                };
                send_packet(&connection, &packet[..size])?;
            }
            packet = inbound_rx.recv() => {
                let packet = packet.ok_or(OverlayTransportError::EndpointClosed)?;
                let sent = device.send(&packet).await.map_err(OverlayTransportError::TunnelIo)?;
                if sent != packet.len() {
                    return Err(OverlayTransportError::TunnelIo(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "TUN accepted a partial packet",
                    )));
                }
            }
            _ = endpoint.closed() => {
                let _ = shutdown_tx.send(true);
                return Err(OverlayTransportError::EndpointClosed);
            }
        }
    }
}

fn unix_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

fn spawn_packet_reader(
    connection: Connection,
    router: OverlayPacketRouter,
    inbound_tx: mpsc::Sender<Vec<u8>>,
) {
    tokio::spawn(async move {
        while let Ok(packet) = receive_packet(&connection, &router).await {
            if inbound_tx.send(packet.bytes).await.is_err() {
                break;
            }
        }
    });
}

/// Construct the allowed peer table from the manager-issued snapshot.
pub fn packet_router(
    local_address: Ipv4Addr,
    peers: &[OverlayPeer],
) -> Result<OverlayPacketRouter, OverlayPacketError> {
    OverlayPacketRouter::new(local_address, peers)
}

/// Validate the manager-issued snapshot before using it to authorize packets.
pub fn configured_packet_router(
    configuration: &OverlayPeerConfiguration,
    expected_worker: &WorkerId,
    now_ms: i64,
) -> Result<OverlayPacketRouter, OverlayConfigurationError> {
    if configuration.schema_version != OVERLAY_PEER_CONFIGURATION_SCHEMA_VERSION {
        return Err(OverlayConfigurationError::UnsupportedSchema);
    }
    if &configuration.worker_id != expected_worker {
        return Err(OverlayConfigurationError::WrongWorker);
    }
    if now_ms < 0
        || configuration.expires_at_ms <= now_ms
        || configuration.peer_generation == 0
        || configuration.network_generation == 0
    {
        return Err(OverlayConfigurationError::InvalidLease);
    }
    if configuration.peers.len() > MAX_OVERLAY_PEERS {
        return Err(OverlayConfigurationError::TooManyPeers);
    }
    for peer in &configuration.peers {
        if !conductor_core::valid_overlay_direct_candidates(&peer.direct_candidates)
            || (!peer.direct_candidates.is_empty()
                && !peer
                    .direct_candidates_expires_at_ms
                    .is_some_and(|expiry| expiry > now_ms))
        {
            return Err(OverlayConfigurationError::InvalidCandidates);
        }
    }
    OverlayPacketRouter::new(configuration.worker_address, &configuration.peers)
        .map_err(OverlayConfigurationError::InvalidPeers)
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn connection_attempt_failures_have_bounded_diagnostic_categories() {
        assert_eq!(
            connection_attempt_outcome(&OverlayTransportError::NoDirectCandidates),
            OverlayConnectionAttemptOutcome::NoCandidates
        );
        assert_eq!(
            connection_attempt_outcome(&OverlayTransportError::CandidateLeaseExpired),
            OverlayConnectionAttemptOutcome::CandidatesExpired
        );
        assert_eq!(
            connection_attempt_outcome(&OverlayTransportError::PeerUnavailable),
            OverlayConnectionAttemptOutcome::TimedOut
        );
        assert_eq!(
            connection_attempt_outcome(&OverlayTransportError::UnauthorizedPeer),
            OverlayConnectionAttemptOutcome::IdentityMismatch
        );
    }
}
