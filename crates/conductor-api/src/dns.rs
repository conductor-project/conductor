use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use conductor_core::{
    NetworkDomainId, OverlayPeerState, ServiceIdentity, ServicePortProtocol, WorkerId, WorkerStatus,
};
use conductor_manager::{JobService, WorkerPort};
use hickory_server::{
    net::runtime::Time,
    proto::{
        op::{Header, HeaderCounts, Metadata, ResponseCode},
        rr::{
            Name, RData, Record, RecordType,
            rdata::{A, AAAA, SOA, SRV},
        },
    },
    server::{Request, RequestHandler, ResponseHandler, ResponseInfo, Server},
    zone_handler::MessageResponseBuilder,
};
use ipnet::IpNet;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Semaphore;

const DNS_TTL_SECONDS: u32 = 2;
const DNS_NEGATIVE_TTL_SECONDS: u32 = 1;
const MAX_CONCURRENT_DNS_LOOKUPS: usize = 128;
const DNS_TCP_IDLE_TIMEOUT: Duration = Duration::from_secs(5);
const DNS_TCP_RESPONSE_BUFFER_BYTES: usize = 4096;

/// A DNS listener that answers service discovery queries from the manager catalog.
#[derive(Clone, Debug)]
pub struct ServiceDnsConfig {
    pub listen: SocketAddr,
    /// Suffix without a trailing root dot, e.g. `conductor.internal`.
    pub suffix: String,
    /// Optional overlay publication uses a separate suffix to preserve the
    /// existing host-network DNS namespace and its address semantics.
    pub overlay: Option<OverlayDnsConfig>,
    /// Client address ranges permitted to use this DNS listener.
    pub allowed_clients: Vec<IpNet>,
}

#[derive(Clone, Debug)]
pub struct OverlayDnsConfig {
    pub network_domain_id: NetworkDomainId,
    /// Overlay suffix without a trailing root dot, e.g. `overlay.internal`.
    pub suffix: String,
}

impl ServiceDnsConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !valid_suffix(&self.suffix) {
            return Err(
                "DNS suffix must contain valid lowercase DNS labels, be at most 104 bytes, and have no trailing dot",
            );
        }
        if let Some(overlay) = &self.overlay
            && (!valid_suffix(&overlay.suffix)
                || overlay.suffix == self.suffix
                || overlay.suffix.ends_with(&format!(".{}", self.suffix))
                || self.suffix.ends_with(&format!(".{}", overlay.suffix)))
        {
            return Err(
                "overlay DNS suffix must be valid and separate from the host-network suffix",
            );
        }
        if self.allowed_clients.is_empty() {
            return Err("DNS listener requires at least one allowed client range");
        }
        Ok(())
    }
}

fn valid_suffix(suffix: &str) -> bool {
    !suffix.is_empty()
        && suffix.len() <= 104
        && !suffix.ends_with('.')
        && suffix.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

/// Run the UDP and TCP DNS listeners until the daemon shutdown flag is set.
pub async fn serve_service_dns(
    config: ServiceDnsConfig,
    manager: Arc<JobService>,
    workers: Vec<Arc<dyn WorkerPort>>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    config.validate().map_err(std::io::Error::other)?;
    let udp_socket = UdpSocket::bind(config.listen).await?;
    let tcp_listener = TcpListener::bind(udp_socket.local_addr()?).await?;
    let handler = ServiceDnsHandler {
        manager,
        workers: Arc::from(workers),
        suffix: config.suffix,
        overlay: config.overlay,
        lookup_admission: Arc::new(Semaphore::new(MAX_CONCURRENT_DNS_LOOKUPS)),
    };
    let mut server = Server::with_access(handler, [], config.allowed_clients);
    server.register_socket(udp_socket);
    server.register_listener(
        tcp_listener,
        DNS_TCP_IDLE_TIMEOUT,
        DNS_TCP_RESPONSE_BUFFER_BYTES,
    );
    let cancellation = server.shutdown_token().clone();
    tokio::select! {
        result = server.block_until_done() => result.map_err(|error| Box::<dyn std::error::Error + Send + Sync>::from(error.to_string())),
        _ = wait_for_shutdown(shutdown) => {
            cancellation.cancel();
            server.shutdown_gracefully().await.map_err(|error| Box::<dyn std::error::Error + Send + Sync>::from(error.to_string()))
        }
    }
}

async fn wait_for_shutdown(shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn dns_now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or_default(),
    )
    .unwrap_or(i64::MAX)
}

fn service_zone_contains(query_name: &str, suffix: &str) -> bool {
    let query_name = query_name.trim_end_matches('.');
    let service_zone = format!("service.{suffix}");
    query_name == service_zone || query_name.ends_with(&format!(".{service_zone}"))
}

fn active_overlay_addresses(
    peers: Vec<conductor_core::OverlayPeerAssignment>,
    now_ms: i64,
) -> BTreeMap<WorkerId, String> {
    peers
        .into_iter()
        .filter(|peer| peer.state == OverlayPeerState::Active && peer.expires_at_ms > now_ms)
        .map(|peer| (peer.worker_id, peer.worker_address.to_string()))
        .collect()
}

struct ServiceDnsHandler {
    manager: Arc<JobService>,
    workers: Arc<[Arc<dyn WorkerPort>]>,
    suffix: String,
    overlay: Option<OverlayDnsConfig>,
    lookup_admission: Arc<Semaphore>,
}

#[async_trait]
impl RequestHandler for ServiceDnsHandler {
    async fn handle_request<R: ResponseHandler, T: Time>(
        &self,
        request: &Request,
        mut response_handle: R,
    ) -> ResponseInfo {
        let request_info = match request.request_info() {
            Ok(info)
                if info
                    .query
                    .query_class()
                    .eq(&hickory_server::proto::rr::DNSClass::IN) =>
            {
                info
            }
            _ => {
                return send_dns_response(
                    request,
                    &mut response_handle,
                    ResponseCode::FormErr,
                    Vec::new(),
                    Vec::new(),
                )
                .await;
            }
        };
        let qname = request_info.query.name().to_string().to_ascii_lowercase();
        let overlay_query = self.overlay.as_ref().and_then(|overlay| {
            parse_service_query_name(&qname, &overlay.suffix).map(|query| {
                (
                    query,
                    overlay.suffix.as_str(),
                    Some(overlay.network_domain_id.clone()),
                )
            })
        });
        let host_query = parse_service_query_name(&qname, &self.suffix)
            .map(|query| (query, self.suffix.as_str(), None));
        let Some((query, query_suffix, overlay_domain_id)) = overlay_query.or(host_query) else {
            let matched_suffix = self
                .overlay
                .as_ref()
                .filter(|overlay| service_zone_contains(&qname, &overlay.suffix))
                .map(|overlay| overlay.suffix.as_str())
                .or_else(|| {
                    service_zone_contains(&qname, &self.suffix).then_some(self.suffix.as_str())
                });
            let (code, authorities) = if let Some(suffix) = matched_suffix {
                (
                    ResponseCode::NXDomain,
                    soa_record(suffix).into_iter().collect(),
                )
            } else {
                (ResponseCode::Refused, Vec::new())
            };
            return send_dns_response(request, &mut response_handle, code, Vec::new(), authorities)
                .await;
        };
        let is_overlay_query = overlay_domain_id.is_some();
        let query_suffix = query_suffix.to_owned();
        let now_ms = dns_now_ms();
        let Ok(permit) = Arc::clone(&self.lookup_admission).try_acquire_owned() else {
            return send_dns_response(
                request,
                &mut response_handle,
                ResponseCode::ServFail,
                Vec::new(),
                Vec::new(),
            )
            .await;
        };
        let manager = Arc::clone(&self.manager);
        let workers = Arc::clone(&self.workers);
        let identity_for_query = query.identity.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let statuses = workers
                .iter()
                .filter_map(|worker| manager.worker_status(worker.as_ref()).ok())
                .filter(WorkerStatus::is_valid)
                .collect::<Vec<_>>();
            let exists = manager.service_identity_exists(&identity_for_query)?;
            let endpoints = manager.list_service_catalog(&identity_for_query, &statuses)?;
            let overlay_addresses = if let Some(domain_id) = overlay_domain_id {
                active_overlay_addresses(
                    manager
                        .list_overlay_peers(&domain_id, 128, now_ms)
                        .map_err(|_| conductor_manager::ExecutionStoreError::StorageUnavailable)?,
                    now_ms,
                )
            } else {
                BTreeMap::new()
            };
            Ok::<_, conductor_manager::ExecutionStoreError>((exists, endpoints, overlay_addresses))
        })
        .await;
        let (exists, endpoints, overlay_addresses) = match result {
            Ok(Ok(result)) => result,
            _ => {
                return send_dns_response(
                    request,
                    &mut response_handle,
                    ResponseCode::ServFail,
                    Vec::new(),
                    Vec::new(),
                )
                .await;
            }
        };
        if !exists {
            return send_dns_response(
                request,
                &mut response_handle,
                ResponseCode::NXDomain,
                Vec::new(),
                soa_record(&query_suffix).into_iter().collect(),
            )
            .await;
        }
        let mut answers = Vec::new();
        for endpoint in endpoints.into_iter().filter(|endpoint| endpoint.eligible) {
            let advertised_address = if is_overlay_query {
                let Some(address) = overlay_addresses.get(&endpoint.worker_id) else {
                    continue;
                };
                address.clone()
            } else {
                endpoint.port.advertised_address().to_owned()
            };
            if let Some(record) = service_endpoint_record_for_query(
                &qname,
                request_info.query.query_type(),
                &query,
                &query_suffix,
                DnsEndpoint {
                    port_name: endpoint.port.name(),
                    protocol: endpoint.port.protocol(),
                    advertised_address: &advertised_address,
                    port: endpoint.port.port(),
                },
            ) {
                answers.push(record);
            }
        }
        let authorities = if answers.is_empty() {
            soa_record(&query_suffix).into_iter().collect()
        } else {
            Vec::new()
        };
        send_dns_response(
            request,
            &mut response_handle,
            ResponseCode::NoError,
            answers,
            authorities,
        )
        .await
    }
}

#[derive(Clone, Copy)]
struct DnsEndpoint<'a> {
    port_name: &'a str,
    protocol: ServicePortProtocol,
    advertised_address: &'a str,
    port: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ServiceEndpointKey {
    address: IpAddr,
    port: u16,
}

impl ServiceEndpointKey {
    fn from_advertised_endpoint(address: &str, port: u16) -> Option<Self> {
        Some(Self {
            address: address.parse().ok()?,
            port,
        })
    }

    fn from_label(label: &str) -> Option<Self> {
        let (family, encoded) = if let Some(encoded) = label.strip_prefix("v4-") {
            (4, encoded)
        } else {
            (6, label.strip_prefix("v6-")?)
        };
        let expected_length = if family == 4 { 12 } else { 36 };
        if encoded.len() != expected_length {
            return None;
        }
        let mut bytes = [0_u8; 18];
        let (pairs, []) = encoded.as_bytes().as_chunks::<2>() else {
            return None;
        };
        for (index, pair) in pairs.iter().enumerate() {
            bytes[index] = decode_hex_byte(pair)?;
        }
        let (address, port_bytes) = if family == 4 {
            (
                IpAddr::V4(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])),
                [bytes[4], bytes[5]],
            )
        } else {
            let address_bytes: [u8; 16] = bytes[..16].try_into().ok()?;
            (
                IpAddr::V6(Ipv6Addr::from(address_bytes)),
                [bytes[16], bytes[17]],
            )
        };
        Some(Self {
            address,
            port: u16::from_be_bytes(port_bytes),
        })
    }

    fn label(self) -> String {
        let mut label = String::with_capacity(match self.address {
            IpAddr::V4(_) => 15,
            IpAddr::V6(_) => 39,
        });
        match self.address {
            IpAddr::V4(address) => {
                label.push_str("v4-");
                for byte in address.octets() {
                    write!(label, "{byte:02x}").expect("writing to String cannot fail");
                }
            }
            IpAddr::V6(address) => {
                label.push_str("v6-");
                for byte in address.octets() {
                    write!(label, "{byte:02x}").expect("writing to String cannot fail");
                }
            }
        }
        for byte in self.port.to_be_bytes() {
            write!(label, "{byte:02x}").expect("writing to String cannot fail");
        }
        label
    }
}

fn decode_hex_byte(pair: &[u8]) -> Option<u8> {
    let [high, low] = pair else {
        return None;
    };
    Some(hex_nibble(*high)? << 4 | hex_nibble(*low)?)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

struct ParsedServiceQueryName {
    identity: ServiceIdentity,
    port_name: Option<String>,
    endpoint_key: Option<ServiceEndpointKey>,
}

fn service_endpoint_record_for_query(
    query_name: &str,
    query_type: RecordType,
    query: &ParsedServiceQueryName,
    suffix: &str,
    endpoint: DnsEndpoint<'_>,
) -> Option<Record> {
    let endpoint_key =
        ServiceEndpointKey::from_advertised_endpoint(endpoint.advertised_address, endpoint.port)?;
    if query
        .endpoint_key
        .is_some_and(|requested_key| requested_key != endpoint_key)
    {
        return None;
    }
    match (query_type, query.port_name.as_deref()) {
        (RecordType::A, None) => match endpoint_key.address {
            IpAddr::V4(address) => Some(Record::from_rdata(
                Name::from_ascii(query_name).ok()?,
                DNS_TTL_SECONDS,
                RData::A(A(address)),
            )),
            IpAddr::V6(_) => None,
        },
        (RecordType::AAAA, None) => match endpoint_key.address {
            IpAddr::V6(address) => Some(Record::from_rdata(
                Name::from_ascii(query_name).ok()?,
                DNS_TTL_SECONDS,
                RData::AAAA(AAAA(address)),
            )),
            IpAddr::V4(_) => None,
        },
        (RecordType::SRV, Some(requested_name))
            if endpoint.port_name == requested_name
                && endpoint.protocol == ServicePortProtocol::Tcp =>
        {
            let target_name = endpoint_target_fqdn(&query.identity, suffix, endpoint_key);
            Some(Record::from_rdata(
                Name::from_ascii(query_name).ok()?,
                DNS_TTL_SECONDS,
                RData::SRV(SRV::new(
                    0,
                    1,
                    endpoint.port,
                    Name::from_ascii(&target_name).ok()?,
                )),
            ))
        }
        _ => None,
    }
}

fn parse_service_query_name(query_name: &str, suffix: &str) -> Option<ParsedServiceQueryName> {
    let query_name = query_name.trim_end_matches('.');
    let suffix = suffix.trim_end_matches('.');
    let local_name = query_name.strip_suffix(suffix)?.strip_suffix('.')?;
    let mut labels = local_name.split('.');
    let first = labels.next()?;
    let (service_part, namespace, port_name, endpoint_key) =
        if let Some(port_name) = first.strip_prefix('_') {
            let transport = labels.next()?;
            if !transport.eq_ignore_ascii_case("_tcp") || port_name.is_empty() {
                return None;
            }
            let service = labels.next()?;
            let namespace = labels.next()?;
            if labels.next()? != "service" || labels.next().is_some() {
                return None;
            }
            (service, namespace, Some(port_name.to_owned()), None)
        } else {
            let second = labels.next()?;
            let third = labels.next()?;
            match labels.next() {
                None if third == "service" => (first, second, None, None),
                Some("service") if labels.next().is_none() => (
                    second,
                    third,
                    None,
                    Some(ServiceEndpointKey::from_label(first)?),
                ),
                _ => return None,
            }
        };
    Some(ParsedServiceQueryName {
        identity: ServiceIdentity::in_namespace(namespace, service_part).ok()?,
        port_name,
        endpoint_key,
    })
}

fn service_fqdn(identity: &ServiceIdentity, suffix: &str) -> String {
    format!(
        "{}.{}.service.{}.",
        identity.name(),
        identity.namespace(),
        suffix
    )
}

fn endpoint_target_fqdn(
    identity: &ServiceIdentity,
    suffix: &str,
    endpoint: ServiceEndpointKey,
) -> String {
    format!("{}.{}", endpoint.label(), service_fqdn(identity, suffix))
}

fn dns_name(value: &str) -> Name {
    Name::from_ascii(value).expect("validated service DNS name")
}

fn soa_record(suffix: &str) -> Option<Record> {
    let origin = dns_name(&format!("{}.{}.", "service", suffix));
    let mname = dns_name(&format!("ns.{}.", suffix));
    let rname = dns_name(&format!("hostmaster.{}.", suffix));
    Some(Record::from_rdata(
        origin,
        DNS_NEGATIVE_TTL_SECONDS,
        RData::SOA(SOA::new(
            mname,
            rname,
            1,
            30,
            30,
            300,
            DNS_NEGATIVE_TTL_SECONDS,
        )),
    ))
}

async fn send_dns_response<R: ResponseHandler>(
    request: &Request,
    response_handle: &mut R,
    response_code: ResponseCode,
    answers: Vec<Record>,
    authorities: Vec<Record>,
) -> ResponseInfo {
    let mut metadata = Metadata::response_from_request(&request.metadata);
    metadata.authoritative = true;
    metadata.response_code = response_code;
    let response = MessageResponseBuilder::new(&request.queries, request.edns.as_ref()).build(
        metadata,
        answers.iter(),
        authorities.iter(),
        std::iter::empty(),
        std::iter::empty(),
    );
    response_handle
        .send_response(response)
        .await
        .unwrap_or_else(|_| {
            ResponseInfo::from(Header {
                metadata,
                counts: HeaderCounts::default(),
            })
        })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::time::{SystemTime, UNIX_EPOCH};

    use hickory_server::proto::op::{Message, MessageType, OpCode, ResponseCode};
    use tokio::time::timeout;

    use super::*;

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct IsolatedDnsState(PathBuf);

    struct StopDnsOnDrop(Arc<AtomicBool>);

    impl Drop for StopDnsOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    impl IsolatedDnsState {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "conductor-dns-{}-{nonce}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir(&directory).expect("create isolated DNS test state");
            Self(directory)
        }
    }

    impl Drop for IsolatedDnsState {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn parses_stable_fqdn_and_named_tcp_srv_contract() {
        let query = parse_service_query_name(
            "_http._tcp.backend.default.service.conductor.internal.",
            "conductor.internal",
        )
        .expect("valid SRV query name");
        assert_eq!(query.identity.name(), "backend");
        assert_eq!(query.identity.namespace(), "default");
        assert_eq!(query.port_name.as_deref(), Some("http"));
        assert_eq!(query.endpoint_key, None);

        let endpoint_key = ServiceEndpointKey::from_advertised_endpoint("192.0.2.10", 31_001)
            .expect("valid endpoint tuple");
        let endpoint_name = format!(
            "{}.backend.default.service.conductor.internal.",
            endpoint_key.label()
        );
        let endpoint_query = parse_service_query_name(&endpoint_name, "conductor.internal")
            .expect("valid endpoint target");
        assert_eq!(endpoint_query.identity.name(), "backend");
        assert_eq!(endpoint_query.identity.namespace(), "default");
        assert_eq!(endpoint_query.endpoint_key, Some(endpoint_key));
        assert!(endpoint_query.port_name.is_none());

        assert_eq!(
            ServiceEndpointKey::from_label(&endpoint_key.label()),
            Some(endpoint_key),
            "endpoint labels round-trip without ambiguity"
        );
        assert!(ServiceEndpointKey::from_label("v4-not-hex").is_none());
        assert!(
            parse_service_query_name(
                "_http._udp.backend.default.service.conductor.internal.",
                "conductor.internal",
            )
            .is_none()
        );
        assert!(
            parse_service_query_name(
                "backend.other.service.conductor.internal.",
                "conductor.internal",
            )
            .is_none()
        );
    }

    #[test]
    fn maps_two_replica_addresses_and_dynamic_named_ports_to_dns_records() {
        let owner = "_http._tcp.backend.default.service.conductor.internal.";
        let service_name = "backend.default.service.conductor.internal.";
        let suffix = "conductor.internal";
        let srv_query = parse_service_query_name(owner, suffix).expect("valid SRV owner");
        let service_query =
            parse_service_query_name(service_name, suffix).expect("valid service owner");
        let ipv4_endpoint = DnsEndpoint {
            port_name: "http",
            protocol: ServicePortProtocol::Tcp,
            advertised_address: "192.0.2.10",
            port: 31_001,
        };
        let ipv6_endpoint = DnsEndpoint {
            port_name: "http",
            protocol: ServicePortProtocol::Tcp,
            advertised_address: "2001:db8::10",
            port: 31_002,
        };
        let ipv4 = service_endpoint_record_for_query(
            service_name,
            RecordType::A,
            &service_query,
            suffix,
            ipv4_endpoint,
        )
        .expect("IPv4 A record at service-wide target");
        let RData::A(ipv4_data) = ipv4.data else {
            panic!("expected A record");
        };
        assert_eq!(ipv4_data.0, Ipv4Addr::new(192, 0, 2, 10));
        assert_eq!(ipv4.ttl, DNS_TTL_SECONDS);

        let ipv6 = service_endpoint_record_for_query(
            service_name,
            RecordType::AAAA,
            &service_query,
            suffix,
            ipv6_endpoint,
        )
        .expect("IPv6 AAAA record at service-wide target");
        let RData::AAAA(ipv6_data) = ipv6.data else {
            panic!("expected AAAA record");
        };
        assert_eq!(ipv6_data.0, "2001:db8::10".parse::<Ipv6Addr>().unwrap());
        assert!(
            service_endpoint_record_for_query(
                service_name,
                RecordType::A,
                &service_query,
                suffix,
                ipv6_endpoint,
            )
            .is_none()
        );

        let srv_ipv4 = service_endpoint_record_for_query(
            owner,
            RecordType::SRV,
            &srv_query,
            suffix,
            ipv4_endpoint,
        )
        .expect("named SRV record for IPv4 replica");
        let RData::SRV(srv_ipv4_data) = srv_ipv4.data else {
            panic!("expected SRV data");
        };
        assert_eq!(srv_ipv4_data.port, 31_001);
        let ipv4_target = srv_ipv4_data.target.to_ascii();
        let ipv4_target_query =
            parse_service_query_name(&ipv4_target, suffix).expect("parse IPv4 SRV target");
        assert_eq!(
            ipv4_target_query.endpoint_key,
            Some(ServiceEndpointKey {
                address: "192.0.2.10".parse().unwrap(),
                port: 31_001,
            })
        );
        let ipv4_target_answer = service_endpoint_record_for_query(
            &ipv4_target,
            RecordType::A,
            &ipv4_target_query,
            suffix,
            ipv4_endpoint,
        )
        .expect("IPv4 SRV target resolves to its replica address");
        let RData::A(ipv4_target_data) = ipv4_target_answer.data else {
            panic!("expected endpoint-specific A record");
        };
        assert_eq!(ipv4_target_data.0, Ipv4Addr::new(192, 0, 2, 10));
        assert!(
            service_endpoint_record_for_query(
                &ipv4_target,
                RecordType::A,
                &ipv4_target_query,
                suffix,
                ipv6_endpoint,
            )
            .is_none()
        );

        let srv_ipv6 = service_endpoint_record_for_query(
            owner,
            RecordType::SRV,
            &srv_query,
            suffix,
            ipv6_endpoint,
        )
        .expect("named SRV record for IPv6 replica");
        let RData::SRV(srv_ipv6_data) = srv_ipv6.data else {
            panic!("expected SRV data");
        };
        assert_eq!(srv_ipv6_data.port, 31_002);
        let ipv6_target = srv_ipv6_data.target.to_ascii();
        assert_ne!(ipv6_target, ipv4_target);
        let ipv6_target_query =
            parse_service_query_name(&ipv6_target, suffix).expect("parse IPv6 SRV target");
        let ipv6_target_answer = service_endpoint_record_for_query(
            &ipv6_target,
            RecordType::AAAA,
            &ipv6_target_query,
            suffix,
            ipv6_endpoint,
        )
        .expect("IPv6 SRV target resolves to its replica address");
        let RData::AAAA(ipv6_target_data) = ipv6_target_answer.data else {
            panic!("expected endpoint-specific AAAA record");
        };
        assert_eq!(ipv6_target_data.0, ipv6_data.0);
        assert!(
            service_endpoint_record_for_query(
                owner,
                RecordType::SRV,
                &ParsedServiceQueryName {
                    identity: srv_query.identity.clone(),
                    port_name: Some("admin".to_owned()),
                    endpoint_key: None,
                },
                suffix,
                ipv4_endpoint,
            )
            .is_none()
        );
    }

    #[test]
    fn withdrawn_endpoint_targets_stop_resolving_after_replacement() {
        let suffix = "conductor.internal";
        let identity = ServiceIdentity::in_namespace("default", "backend").unwrap();
        let old_endpoint = DnsEndpoint {
            port_name: "http",
            protocol: ServicePortProtocol::Tcp,
            advertised_address: "192.0.2.10",
            port: 31_001,
        };
        let replacement = DnsEndpoint {
            port_name: "http",
            protocol: ServicePortProtocol::Tcp,
            advertised_address: "192.0.2.11",
            port: 32_001,
        };
        let srv_query = parse_service_query_name(
            "_http._tcp.backend.default.service.conductor.internal.",
            suffix,
        )
        .unwrap();
        let old_srv = service_endpoint_record_for_query(
            "_http._tcp.backend.default.service.conductor.internal.",
            RecordType::SRV,
            &srv_query,
            suffix,
            old_endpoint,
        )
        .unwrap();
        let RData::SRV(old_srv_data) = old_srv.data else {
            panic!("expected SRV data");
        };
        let old_target = old_srv_data.target.to_ascii();
        let old_target_query = parse_service_query_name(&old_target, suffix).unwrap();

        // The next lookup receives a fresh eligible catalog containing only
        // the replacement. The withdrawn endpoint's tuple no longer matches.
        assert!(
            service_endpoint_record_for_query(
                &old_target,
                RecordType::A,
                &old_target_query,
                suffix,
                replacement,
            )
            .is_none()
        );

        let replacement_target = endpoint_target_fqdn(
            &identity,
            suffix,
            ServiceEndpointKey::from_advertised_endpoint(
                replacement.advertised_address,
                replacement.port,
            )
            .unwrap(),
        );
        assert_ne!(replacement_target, old_target);
        let replacement_query = parse_service_query_name(&replacement_target, suffix).unwrap();
        let replacement_answer = service_endpoint_record_for_query(
            &replacement_target,
            RecordType::A,
            &replacement_query,
            suffix,
            replacement,
        )
        .expect("replacement target resolves to the replacement address");
        let RData::A(replacement_address) = replacement_answer.data else {
            panic!("expected replacement A record");
        };
        assert_eq!(replacement_address.0, Ipv4Addr::new(192, 0, 2, 11));
    }

    #[test]
    fn dns_config_requires_valid_suffix_and_an_access_range() {
        let mut config = ServiceDnsConfig {
            listen: "127.0.0.1:8053".parse().expect("socket address"),
            suffix: "conductor.internal".to_owned(),
            overlay: None,
            allowed_clients: vec!["127.0.0.0/8".parse().expect("loopback range")],
        };
        assert!(config.validate().is_ok());
        config.suffix = "Conductor.internal".to_owned();
        assert!(config.validate().is_err());
        config.suffix = "conductor.internal".to_owned();
        config.overlay = Some(OverlayDnsConfig {
            network_domain_id: NetworkDomainId::new("workers-east".to_owned())
                .expect("valid overlay domain ID"),
            suffix: "overlay.internal".to_owned(),
        });
        assert!(config.validate().is_ok());
        config.overlay.as_mut().unwrap().suffix = "conductor.internal".to_owned();
        assert!(config.validate().is_err());
        config.overlay.as_mut().unwrap().suffix = "Overlay.internal".to_owned();
        assert!(config.validate().is_err());
        config.overlay = None;
        config.allowed_clients.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn overlay_dns_srv_target_pairs_the_assigned_ip_with_the_service_port() {
        let suffix = "overlay.internal";
        let endpoint = DnsEndpoint {
            port_name: "http",
            protocol: ServicePortProtocol::Tcp,
            advertised_address: "10.91.0.23",
            port: 31_234,
        };
        let query_name = "_http._tcp.backend.default.service.overlay.internal.";
        let query = parse_service_query_name(query_name, suffix).expect("overlay SRV query");
        let record = service_endpoint_record_for_query(
            query_name,
            RecordType::SRV,
            &query,
            suffix,
            endpoint,
        )
        .expect("overlay SRV record");
        let RData::SRV(srv) = record.data else {
            panic!("expected SRV record");
        };
        assert_eq!(srv.port, 31_234);
        let target = srv.target.to_ascii();
        let target_query = parse_service_query_name(&target, suffix).expect("overlay SRV target");
        let record = service_endpoint_record_for_query(
            &target,
            RecordType::A,
            &target_query,
            suffix,
            endpoint,
        )
        .expect("overlay A record for SRV target");
        let RData::A(address) = record.data else {
            panic!("expected A record");
        };
        assert_eq!(address.0, Ipv4Addr::new(10, 91, 0, 23));
    }

    #[test]
    fn overlay_dns_publishes_only_active_unexpired_worker_addresses() {
        let domain = NetworkDomainId::new("workers-east".to_owned()).expect("domain ID");
        let peers = vec![
            conductor_core::OverlayPeerAssignment {
                network_domain_id: domain.clone(),
                worker_id: WorkerId::new("active-worker").expect("worker ID"),
                endpoint_id: conductor_core::OverlayEndpointId::new("01".repeat(32))
                    .expect("endpoint ID"),
                worker_address: Ipv4Addr::new(10, 91, 0, 23),
                peer_generation: 1,
                network_generation: 1,
                expires_at_ms: 20_000,
                state: OverlayPeerState::Active,
                quarantine_until_ms: None,
                direct_candidates: Vec::new(),
                direct_candidates_expires_at_ms: None,
            },
            conductor_core::OverlayPeerAssignment {
                network_domain_id: domain.clone(),
                worker_id: WorkerId::new("expired-worker").expect("worker ID"),
                endpoint_id: conductor_core::OverlayEndpointId::new("02".repeat(32))
                    .expect("endpoint ID"),
                worker_address: Ipv4Addr::new(10, 91, 0, 24),
                peer_generation: 1,
                network_generation: 1,
                expires_at_ms: 10_000,
                state: OverlayPeerState::Active,
                quarantine_until_ms: None,
                direct_candidates: Vec::new(),
                direct_candidates_expires_at_ms: None,
            },
            conductor_core::OverlayPeerAssignment {
                network_domain_id: domain,
                worker_id: WorkerId::new("quarantined-worker").expect("worker ID"),
                endpoint_id: conductor_core::OverlayEndpointId::new("03".repeat(32))
                    .expect("endpoint ID"),
                worker_address: Ipv4Addr::new(10, 91, 0, 25),
                peer_generation: 1,
                network_generation: 1,
                expires_at_ms: 20_000,
                state: OverlayPeerState::Quarantined,
                quarantine_until_ms: Some(20_000),
                direct_candidates: Vec::new(),
                direct_candidates_expires_at_ms: None,
            },
        ];
        let addresses = active_overlay_addresses(peers, 10_000);
        assert_eq!(addresses.len(), 1);
        assert_eq!(
            addresses.get(&WorkerId::new("active-worker").expect("worker ID")),
            Some(&"10.91.0.23".to_owned())
        );
    }

    #[tokio::test]
    async fn dns_listener_answers_unknown_and_out_of_zone_queries() {
        let state = IsolatedDnsState::new();
        let manager = Arc::new(
            JobService::open_sqlite(state.0.join("manager.sqlite3"))
                .expect("open isolated manager database"),
        );
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve DNS port");
        let listen = probe.local_addr().expect("read reserved port");
        drop(probe);
        let shutdown = Arc::new(AtomicBool::new(false));
        let _stop_on_drop = StopDnsOnDrop(Arc::clone(&shutdown));
        let server_shutdown = Arc::clone(&shutdown);
        let server = tokio::spawn(serve_service_dns(
            ServiceDnsConfig {
                listen,
                suffix: "conductor.internal".to_owned(),
                overlay: None,
                allowed_clients: vec!["127.0.0.0/8".parse().expect("loopback CIDR")],
            },
            manager,
            Vec::new(),
            server_shutdown,
        ));

        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind DNS client");
        let unknown = dns_query("missing.default.service.conductor.internal.");
        let response = dns_exchange(&socket, listen, unknown).await;
        assert_eq!(response.metadata.response_code, ResponseCode::NXDomain);
        assert!(
            response
                .authorities
                .iter()
                .any(|record| record.record_type() == RecordType::SOA)
        );
        assert!(
            response
                .authorities
                .iter()
                .all(|record| record.ttl == DNS_NEGATIVE_TTL_SECONDS)
        );

        let outside = dns_query("example.org.");
        let response = dns_exchange(&socket, listen, outside).await;
        assert_eq!(response.metadata.response_code, ResponseCode::Refused);
        let outside_deployment_suffix = dns_query("example.conductor.internal.");
        let response = dns_exchange(&socket, listen, outside_deployment_suffix).await;
        assert_eq!(response.metadata.response_code, ResponseCode::Refused);
        assert!(response.authorities.is_empty());

        shutdown.store(true, Ordering::Release);
        timeout(Duration::from_secs(2), server)
            .await
            .expect("DNS server stops after shutdown")
            .expect("DNS task joins")
            .expect("DNS server exits cleanly");
        socket
            .send_to(
                &dns_query("backend.default.service.conductor.internal."),
                listen,
            )
            .await
            .expect("send query after manager shutdown");
        let mut buffer = [0_u8; 512];
        assert!(
            timeout(Duration::from_millis(100), socket.recv_from(&mut buffer))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn overlay_dns_suffix_uses_its_configured_network_domain() {
        let state = IsolatedDnsState::new();
        let manager = Arc::new(
            JobService::open_sqlite(state.0.join("manager.sqlite3"))
                .expect("open isolated manager database"),
        );
        let domain_id = NetworkDomainId::new("workers-east".to_owned()).expect("domain ID");
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve DNS port");
        let listen = probe.local_addr().expect("read reserved port");
        drop(probe);
        let shutdown = Arc::new(AtomicBool::new(false));
        let _stop_on_drop = StopDnsOnDrop(Arc::clone(&shutdown));
        let server = tokio::spawn(serve_service_dns(
            ServiceDnsConfig {
                listen,
                suffix: "conductor.internal".to_owned(),
                overlay: Some(OverlayDnsConfig {
                    network_domain_id: domain_id.clone(),
                    suffix: "overlay.internal".to_owned(),
                }),
                allowed_clients: vec!["127.0.0.0/8".parse().expect("loopback CIDR")],
            },
            Arc::clone(&manager),
            Vec::new(),
            Arc::clone(&shutdown),
        ));
        manager
            .create_overlay_network_domain(
                &conductor_core::OverlayNetworkDomainConfig {
                    network_domain_id: domain_id.clone(),
                    worker_address_pool: "10.91.0.0/24".to_owned(),
                    service_address_pool: "10.91.1.0/24".to_owned(),
                },
                1_000,
            )
            .expect("create overlay domain after DNS listener starts");
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind DNS client");
        let response = dns_exchange(
            &socket,
            listen,
            dns_query("missing.default.service.overlay.internal."),
        )
        .await;
        assert_eq!(response.metadata.response_code, ResponseCode::NXDomain);
        assert!(response.authorities.iter().any(|record| {
            record.name.to_ascii() == "service.overlay.internal."
                && record.record_type() == RecordType::SOA
        }));

        shutdown.store(true, Ordering::Release);
        timeout(Duration::from_secs(2), server)
            .await
            .expect("DNS server stops after shutdown")
            .expect("DNS task joins")
            .expect("DNS server exits cleanly");
    }

    fn dns_query(name: &str) -> Vec<u8> {
        let mut message = Message::new(42, MessageType::Query, OpCode::Query);
        message.add_query(hickory_server::proto::op::Query::query(
            Name::from_ascii(name).expect("valid test DNS name"),
            RecordType::A,
        ));
        message.to_vec().expect("encode DNS query")
    }

    async fn dns_exchange(socket: &UdpSocket, server: SocketAddr, query: Vec<u8>) -> Message {
        socket
            .send_to(&query, server)
            .await
            .expect("send DNS query");
        let mut buffer = [0_u8; 2048];
        let (length, _) = timeout(Duration::from_secs(2), socket.recv_from(&mut buffer))
            .await
            .expect("DNS response arrives")
            .expect("receive DNS response");
        Message::from_vec(&buffer[..length]).expect("decode DNS response")
    }
}
