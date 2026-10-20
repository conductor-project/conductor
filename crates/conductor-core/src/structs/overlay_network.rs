use super::*;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Maximum number of direct UDP underlay candidates advertised by one worker.
pub const MAX_OVERLAY_DIRECT_CANDIDATES: usize = 8;

/// Validate a bounded, canonical set of directly reachable UDP socket candidates.
pub fn valid_overlay_direct_candidates(candidates: &[SocketAddr]) -> bool {
    candidates.len() <= MAX_OVERLAY_DIRECT_CANDIDATES
        && candidates.iter().enumerate().all(|(index, candidate)| {
            candidate.port() != 0
                && match candidate.ip() {
                    IpAddr::V4(ip) => {
                        !ip.is_unspecified()
                            && !ip.is_loopback()
                            && !ip.is_multicast()
                            && !ip.is_link_local()
                            && ip != Ipv4Addr::BROADCAST
                    }
                    IpAddr::V6(ip) => {
                        !ip.is_unspecified()
                            && !ip.is_loopback()
                            && !ip.is_multicast()
                            && !ip.is_unicast_link_local()
                    }
                }
                && !candidates[..index].contains(candidate)
        })
}

pub const OVERLAY_PEER_CONFIGURATION_SCHEMA_VERSION: u32 = 1;
pub const MAX_OVERLAY_APPLICATION_PROBE_TIMEOUT_MS: u32 = 5_000;
pub const MAX_OVERLAY_APPLICATION_PROBE_RESPONSE_BYTES: u32 = 65_536;

/// Canonical lowercase hexadecimal Iroh Ed25519 endpoint identity.
///
/// Iroh accepts hexadecimal endpoint identifiers. Keeping this transport-neutral
/// contract as a validated string avoids pulling the networking backend into core.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct OverlayEndpointId(String);

impl OverlayEndpointId {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidIdentifier> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(InvalidIdentifier);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for OverlayEndpointId {
    type Error = InvalidIdentifier;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl AsRef<str> for OverlayEndpointId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for OverlayEndpointId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for OverlayEndpointId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Manager-owned address pools for one encrypted worker overlay.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayNetworkDomainConfig {
    pub network_domain_id: NetworkDomainId,
    pub worker_address_pool: String,
    pub service_address_pool: String,
}

/// One authorized peer in a manager-issued worker configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayPeer {
    pub worker_id: WorkerId,
    pub endpoint_id: OverlayEndpointId,
    pub worker_address: Ipv4Addr,
    pub peer_generation: u64,
    pub direct_candidates: Vec<SocketAddr>,
    pub direct_candidates_expires_at_ms: Option<i64>,
}

/// Durable address and key assignment returned after authenticated enrollment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayPeerAssignment {
    pub network_domain_id: NetworkDomainId,
    pub worker_id: WorkerId,
    pub endpoint_id: OverlayEndpointId,
    pub worker_address: Ipv4Addr,
    pub peer_generation: u64,
    pub network_generation: u64,
    pub expires_at_ms: i64,
    pub state: OverlayPeerState,
    pub quarantine_until_ms: Option<i64>,
}

/// Authenticated worker advertisement of direct UDP underlay addresses.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayDirectCandidateAdvertisement {
    pub peer_generation: u64,
    pub candidates: Vec<SocketAddr>,
}

/// Manager acknowledgement for a generation-scoped candidate update.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayDirectCandidateAcknowledgement {
    pub worker_id: WorkerId,
    pub peer_generation: u64,
    pub network_generation: u64,
    pub candidates_expires_at_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverlayPeerState {
    Active,
    Quarantined,
}

/// Bounded, versioned desired peer state delivered over worker mTLS.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayPeerConfiguration {
    pub schema_version: u32,
    pub network_domain_id: NetworkDomainId,
    pub worker_id: WorkerId,
    pub worker_address: Ipv4Addr,
    pub peer_generation: u64,
    pub network_generation: u64,
    pub expires_at_ms: i64,
    pub peers: Vec<OverlayPeer>,
}

/// Nonsensitive worker acknowledgement of an applied overlay configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayPeerObservation {
    pub network_domain_id: NetworkDomainId,
    pub worker_id: WorkerId,
    pub peer_generation: u64,
    pub applied_network_generation: u64,
    pub observed_at_ms: i64,
    pub status: OverlayPeerObservationStatus,
    #[serde(default)]
    pub runtime: Option<OverlayRuntimeDiagnostic>,
}

/// Result of one bounded HTTP application probe initiated by an enrolled worker.
/// The manager derives the target worker from its active address assignment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayApplicationProbeReport {
    pub network_domain_id: NetworkDomainId,
    pub worker_id: WorkerId,
    pub peer_generation: u64,
    pub network_generation: u64,
    pub target_address: Ipv4Addr,
    pub target_port: u16,
    pub outcome: OverlayApplicationProbeOutcome,
    pub http_status: Option<u16>,
    /// Capped to 65,537 as an explicit over-limit sentinel.
    pub response_bytes: u32,
    pub elapsed_ms: u32,
}

/// Bounded terminal outcomes for a worker-initiated application probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverlayApplicationProbeOutcome {
    Succeeded,
    HttpStatus,
    Timeout,
    ConnectFailed,
    ResponseTooLarge,
    ResponseFailed,
    RequestFailed,
}

impl OverlayApplicationProbeReport {
    pub fn is_valid(&self) -> bool {
        let status_is_success = self
            .http_status
            .is_some_and(|status| (200..300).contains(&status));
        let status_is_failure = self
            .http_status
            .is_some_and(|status| !(200..300).contains(&status));
        self.target_port != 0
            && self.elapsed_ms <= MAX_OVERLAY_APPLICATION_PROBE_TIMEOUT_MS
            && self.response_bytes <= MAX_OVERLAY_APPLICATION_PROBE_RESPONSE_BYTES + 1
            && self
                .http_status
                .is_none_or(|status| (100..=599).contains(&status))
            && match self.outcome {
                OverlayApplicationProbeOutcome::Succeeded => {
                    status_is_success
                        && self.response_bytes <= MAX_OVERLAY_APPLICATION_PROBE_RESPONSE_BYTES
                }
                OverlayApplicationProbeOutcome::HttpStatus => {
                    status_is_failure
                        && self.response_bytes <= MAX_OVERLAY_APPLICATION_PROBE_RESPONSE_BYTES
                }
                OverlayApplicationProbeOutcome::Timeout
                | OverlayApplicationProbeOutcome::ResponseFailed => {
                    self.response_bytes <= MAX_OVERLAY_APPLICATION_PROBE_RESPONSE_BYTES
                }
                OverlayApplicationProbeOutcome::ConnectFailed
                | OverlayApplicationProbeOutcome::RequestFailed => {
                    self.http_status.is_none() && self.response_bytes == 0
                }
                OverlayApplicationProbeOutcome::ResponseTooLarge => {
                    self.http_status.is_some()
                        && self.response_bytes == MAX_OVERLAY_APPLICATION_PROBE_RESPONSE_BYTES + 1
                }
            }
    }
}

/// Latest bounded probe outcome from this worker to another active overlay peer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayApplicationProbeDiagnostic {
    pub target_worker_id: WorkerId,
    pub target_address: Ipv4Addr,
    pub target_port: u16,
    pub outcome: OverlayApplicationProbeOutcome,
    pub http_status: Option<u16>,
    pub response_bytes: u32,
    pub elapsed_ms: u32,
    pub observed_at_ms: i64,
    pub fresh: bool,
}

/// Bounded runtime state observed by the worker's privileged overlay helper.
/// This reports interface and direct transport state; it is not an application
/// reachability assertion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayRuntimeDiagnostic {
    pub interface_name: String,
    pub interface_up: bool,
    pub local_address: Ipv4Addr,
    pub mtu: u16,
    pub installed_routes: Vec<Ipv4Addr>,
    pub direct_links: Vec<OverlayDirectLinkDiagnostic>,
}

/// Current or most recently observed Iroh connection state for an authorized
/// peer. Only direct socket addresses are representable here.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayDirectLinkDiagnostic {
    pub endpoint_id: OverlayEndpointId,
    pub connected: bool,
    pub selected_address: Option<SocketAddr>,
    pub last_handshake_at_ms: Option<i64>,
    pub udp_tx_bytes: u64,
    pub udp_rx_bytes: u64,
}

/// Manager view that keeps desired assignment separate from the worker's
/// bounded application report. A report is not proof of peer reachability.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OverlayPeerDiagnostic {
    pub assignment: OverlayPeerAssignment,
    pub observation_status: Option<OverlayPeerObservationStatus>,
    pub observation_network_generation: Option<u64>,
    pub observation_at_ms: Option<i64>,
    pub observation_fresh: bool,
    pub runtime: Option<OverlayRuntimeDiagnostic>,
    /// Directional result from this worker to the last probed peer.
    pub application_probe: Option<OverlayApplicationProbeDiagnostic>,
    pub reachability: OverlayReachability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverlayReachability {
    NotVerified,
    Verified,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverlayPeerObservationStatus {
    Applied,
    Rejected,
    Unavailable,
}
