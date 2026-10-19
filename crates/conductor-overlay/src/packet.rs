use conductor_core::{OverlayEndpointId, OverlayPeer};
use iroh_base::EndpointId;
use std::collections::BTreeMap;
use std::net::Ipv4Addr;

/// Conservative inner IPv4 MTU for QUIC datagrams; larger packets are rejected
/// explicitly instead of relying on outer-path fragmentation.
pub const MAX_OVERLAY_PACKET_BYTES: usize = 1000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverlayPacketError {
    MalformedIpv4,
    PacketTooLarge,
    InvalidSource,
    InvalidDestination,
    UnknownPeer,
    PeerAddressMismatch,
    DuplicatePeer,
    InvalidEndpointId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ipv4Packet<'a> {
    pub source: Ipv4Addr,
    pub destination: Ipv4Addr,
    pub bytes: &'a [u8],
}

/// Parse a complete IPv4 packet from a TUN frame and reject truncation,
/// trailing data, and oversized frames.
pub fn parse_ipv4_packet(bytes: &[u8]) -> Result<Ipv4Packet<'_>, OverlayPacketError> {
    if bytes.len() < 20 || bytes.len() > MAX_OVERLAY_PACKET_BYTES || bytes[0] >> 4 != 4 {
        return Err(if bytes.len() > MAX_OVERLAY_PACKET_BYTES {
            OverlayPacketError::PacketTooLarge
        } else {
            OverlayPacketError::MalformedIpv4
        });
    }
    let header_length = usize::from(bytes[0] & 0x0f) * 4;
    let total_length = usize::from(u16::from_be_bytes([bytes[2], bytes[3]]));
    if header_length < 20 || header_length > total_length || total_length != bytes.len() {
        return Err(OverlayPacketError::MalformedIpv4);
    }
    let source = Ipv4Addr::new(bytes[12], bytes[13], bytes[14], bytes[15]);
    let destination = Ipv4Addr::new(bytes[16], bytes[17], bytes[18], bytes[19]);
    if source.is_unspecified() || source.is_multicast() || source.is_broadcast() {
        return Err(OverlayPacketError::InvalidSource);
    }
    if destination.is_unspecified() || destination.is_multicast() || destination.is_broadcast() {
        return Err(OverlayPacketError::InvalidDestination);
    }
    Ok(Ipv4Packet {
        source,
        destination,
        bytes,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedPacket {
    pub endpoint_id: EndpointId,
    pub source: Ipv4Addr,
    pub destination: Ipv4Addr,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct OverlayPacketRouter {
    local_address: Ipv4Addr,
    by_address: BTreeMap<Ipv4Addr, EndpointId>,
    by_endpoint: BTreeMap<EndpointId, Ipv4Addr>,
}

impl OverlayPacketRouter {
    pub fn new(local_address: Ipv4Addr, peers: &[OverlayPeer]) -> Result<Self, OverlayPacketError> {
        let mut router = Self {
            local_address,
            by_address: BTreeMap::new(),
            by_endpoint: BTreeMap::new(),
        };
        for peer in peers {
            router.insert_peer(&peer.endpoint_id, peer.worker_address)?;
        }
        Ok(router)
    }

    fn insert_peer(
        &mut self,
        endpoint_id: &OverlayEndpointId,
        address: Ipv4Addr,
    ) -> Result<(), OverlayPacketError> {
        let endpoint_id = endpoint_id
            .as_str()
            .parse::<EndpointId>()
            .map_err(|_| OverlayPacketError::InvalidEndpointId)?;
        if address == self.local_address
            || self.by_address.contains_key(&address)
            || self.by_endpoint.contains_key(&endpoint_id)
        {
            return Err(OverlayPacketError::DuplicatePeer);
        }
        self.by_address.insert(address, endpoint_id);
        self.by_endpoint.insert(endpoint_id, address);
        Ok(())
    }

    pub fn route_outbound(&self, bytes: &[u8]) -> Result<RoutedPacket, OverlayPacketError> {
        let packet = parse_ipv4_packet(bytes)?;
        if packet.source != self.local_address {
            return Err(OverlayPacketError::InvalidSource);
        }
        let endpoint_id = self
            .by_address
            .get(&packet.destination)
            .copied()
            .ok_or(OverlayPacketError::UnknownPeer)?;
        Ok(RoutedPacket {
            endpoint_id,
            source: packet.source,
            destination: packet.destination,
            bytes: packet.bytes.to_vec(),
        })
    }

    pub fn authorizes_endpoint(&self, endpoint_id: EndpointId) -> bool {
        self.by_endpoint.contains_key(&endpoint_id)
    }

    pub fn validate_inbound(
        &self,
        authenticated_peer: EndpointId,
        bytes: &[u8],
    ) -> Result<RoutedPacket, OverlayPacketError> {
        let packet = parse_ipv4_packet(bytes)?;
        let expected_source = self
            .by_endpoint
            .get(&authenticated_peer)
            .ok_or(OverlayPacketError::UnknownPeer)?;
        if packet.source != *expected_source {
            return Err(OverlayPacketError::PeerAddressMismatch);
        }
        if packet.destination != self.local_address {
            return Err(OverlayPacketError::InvalidDestination);
        }
        Ok(RoutedPacket {
            endpoint_id: authenticated_peer,
            source: packet.source,
            destination: packet.destination,
            bytes: packet.bytes.to_vec(),
        })
    }
}
