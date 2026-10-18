use std::net::{IpAddr, SocketAddr};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use conductor_core::{ServiceIdentity, ServicePortProtocol, WorkerStatus};
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
    /// Client address ranges permitted to use this DNS listener.
    pub allowed_clients: Vec<IpNet>,
}

impl ServiceDnsConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        let suffix = self.suffix.as_str();
        if suffix.is_empty()
            || suffix.len() > 104
            || suffix.ends_with('.')
            || suffix.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
            })
        {
            return Err(
                "DNS suffix must contain valid lowercase DNS labels, be at most 104 bytes, and have no trailing dot",
            );
        }
        if self.allowed_clients.is_empty() {
            return Err("DNS listener requires at least one allowed client range");
        }
        Ok(())
    }
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

struct ServiceDnsHandler {
    manager: Arc<JobService>,
    workers: Arc<[Arc<dyn WorkerPort>]>,
    suffix: String,
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
        let Some((identity, port_name)) = parse_service_query_name(&qname, &self.suffix) else {
            let service_zone = format!("service.{}.", self.suffix);
            let code = if qname.ends_with(&format!(".{service_zone}")) || qname == service_zone {
                ResponseCode::NXDomain
            } else {
                ResponseCode::Refused
            };
            return send_dns_response(
                request,
                &mut response_handle,
                code,
                Vec::new(),
                if code == ResponseCode::NXDomain {
                    soa_record(&self.suffix).into_iter().collect()
                } else {
                    Vec::new()
                },
            )
            .await;
        };
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
        let identity_for_query = identity.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let statuses = workers
                .iter()
                .filter_map(|worker| manager.worker_status(worker.as_ref()).ok())
                .filter(WorkerStatus::is_valid)
                .collect::<Vec<_>>();
            let exists = manager.service_identity_exists(&identity_for_query)?;
            let endpoints = manager.list_service_catalog(&identity_for_query, &statuses)?;
            Ok::<_, conductor_manager::ExecutionStoreError>((exists, endpoints))
        })
        .await;
        let (exists, endpoints) = match result {
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
                soa_record(&self.suffix).into_iter().collect(),
            )
            .await;
        }
        let endpoint_name = service_fqdn(&identity, &self.suffix);
        let mut answers = Vec::new();
        for endpoint in endpoints.into_iter().filter(|endpoint| endpoint.eligible) {
            if let Some(record) = service_endpoint_record(
                &qname,
                request_info.query.query_type(),
                port_name.as_deref(),
                DnsEndpoint {
                    target_name: &endpoint_name,
                    port_name: endpoint.port.name(),
                    protocol: endpoint.port.protocol(),
                    advertised_address: endpoint.port.advertised_address(),
                    port: endpoint.port.port(),
                },
            ) {
                answers.push(record);
            }
        }
        let authorities = if answers.is_empty() {
            soa_record(&self.suffix).into_iter().collect()
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

struct DnsEndpoint<'a> {
    target_name: &'a str,
    port_name: &'a str,
    protocol: ServicePortProtocol,
    advertised_address: &'a str,
    port: u16,
}

fn service_endpoint_record(
    query_name: &str,
    query_type: RecordType,
    requested_port_name: Option<&str>,
    endpoint: DnsEndpoint<'_>,
) -> Option<Record> {
    match (query_type, requested_port_name) {
        (RecordType::A, None) => match endpoint.advertised_address.parse::<IpAddr>().ok()? {
            IpAddr::V4(address) => Some(Record::from_rdata(
                dns_name(endpoint.target_name),
                DNS_TTL_SECONDS,
                RData::A(A(address)),
            )),
            IpAddr::V6(_) => None,
        },
        (RecordType::AAAA, None) => match endpoint.advertised_address.parse::<IpAddr>().ok()? {
            IpAddr::V6(address) => Some(Record::from_rdata(
                dns_name(endpoint.target_name),
                DNS_TTL_SECONDS,
                RData::AAAA(AAAA(address)),
            )),
            IpAddr::V4(_) => None,
        },
        (RecordType::SRV, Some(requested_name))
            if endpoint.port_name == requested_name
                && endpoint.protocol == ServicePortProtocol::Tcp =>
        {
            Some(Record::from_rdata(
                dns_name(query_name),
                DNS_TTL_SECONDS,
                RData::SRV(SRV::new(
                    0,
                    1,
                    endpoint.port,
                    dns_name(endpoint.target_name),
                )),
            ))
        }
        _ => None,
    }
}

fn parse_service_query_name(
    query_name: &str,
    suffix: &str,
) -> Option<(ServiceIdentity, Option<String>)> {
    let query_name = query_name.trim_end_matches('.');
    let suffix = suffix.trim_end_matches('.');
    let local_name = query_name.strip_suffix(suffix)?.strip_suffix('.')?;
    let mut labels = local_name.split('.');
    let first = labels.next()?;
    let (service_part, port_name) = if let Some(port_name) = first.strip_prefix('_') {
        let transport = labels.next()?;
        if !transport.eq_ignore_ascii_case("_tcp") || port_name.is_empty() {
            return None;
        }
        (labels.next()?, Some(port_name.to_owned()))
    } else {
        (first, None)
    };
    let namespace = labels.next()?;
    if labels.next()? != "service" || labels.next().is_some() {
        return None;
    }
    let identity = ServiceIdentity::in_namespace(namespace, service_part).ok()?;
    Some((identity, port_name))
}

fn service_fqdn(identity: &ServiceIdentity, suffix: &str) -> String {
    format!(
        "{}.{}.service.{}.",
        identity.name(),
        identity.namespace(),
        suffix
    )
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
        let (identity, port) = parse_service_query_name(
            "_http._tcp.backend.default.service.conductor.internal.",
            "conductor.internal",
        )
        .expect("valid SRV query name");
        assert_eq!(identity.name(), "backend");
        assert_eq!(identity.namespace(), "default");
        assert_eq!(port.as_deref(), Some("http"));
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
        let target = "backend.default.service.conductor.internal.";
        let endpoint = |address, port| DnsEndpoint {
            target_name: target,
            port_name: "http",
            protocol: ServicePortProtocol::Tcp,
            advertised_address: address,
            port,
        };
        let ipv4 =
            service_endpoint_record(owner, RecordType::A, None, endpoint("192.0.2.10", 31_001))
                .expect("IPv4 A record");
        assert!(matches!(ipv4.data, RData::A(_)));
        assert_eq!(ipv4.ttl, DNS_TTL_SECONDS);

        let ipv6 = service_endpoint_record(
            owner,
            RecordType::AAAA,
            None,
            endpoint("2001:db8::10", 31_002),
        )
        .expect("IPv6 AAAA record");
        assert!(matches!(ipv6.data, RData::AAAA(_)));
        assert!(
            service_endpoint_record(owner, RecordType::A, None, endpoint("2001:db8::10", 31_002),)
                .is_none()
        );

        let srv = service_endpoint_record(
            owner,
            RecordType::SRV,
            Some("http"),
            endpoint("192.0.2.10", 31_001),
        )
        .expect("named SRV record");
        let RData::SRV(srv_data) = srv.data else {
            panic!("expected SRV data");
        };
        assert_eq!(srv_data.port, 31_001);
        assert_eq!(srv_data.target.to_ascii(), target);
        assert!(
            service_endpoint_record(
                owner,
                RecordType::SRV,
                Some("admin"),
                endpoint("192.0.2.10", 31_001),
            )
            .is_none()
        );
    }

    #[test]
    fn dns_config_requires_valid_suffix_and_an_access_range() {
        let mut config = ServiceDnsConfig {
            listen: "127.0.0.1:8053".parse().expect("socket address"),
            suffix: "conductor.internal".to_owned(),
            allowed_clients: vec!["127.0.0.0/8".parse().expect("loopback range")],
        };
        assert!(config.validate().is_ok());
        config.suffix = "Conductor.internal".to_owned();
        assert!(config.validate().is_err());
        config.suffix = "conductor.internal".to_owned();
        config.allowed_clients.clear();
        assert!(config.validate().is_err());
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
