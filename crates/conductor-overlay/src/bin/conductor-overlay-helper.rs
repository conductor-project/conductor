#[cfg(target_os = "linux")]
use std::net::{Ipv4Addr, SocketAddr};

#[cfg(target_os = "linux")]
use conductor_core::{OverlayPeerConfiguration, WorkerId};
#[cfg(target_os = "linux")]
use serde::{Deserialize, Serialize};
#[cfg(target_os = "linux")]
use tokio::io::{self, AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};

#[cfg(target_os = "linux")]
const MAX_REQUEST_BYTES: usize = 64 * 1024;

#[cfg(target_os = "linux")]
#[derive(Debug, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Start {
        worker_id: WorkerId,
        secret_key: [u8; 32],
        configuration: OverlayPeerConfiguration,
    },
    Reconfigure {
        configuration: OverlayPeerConfiguration,
    },
    Shutdown,
}

#[cfg(target_os = "linux")]
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct Response<'a> {
    event: &'a str,
    worker_id: Option<&'a WorkerId>,
    interface_name: Option<&'a str>,
    local_address: Option<Ipv4Addr>,
    peer_generation: Option<u64>,
    network_generation: Option<u64>,
    expires_at_ms: Option<i64>,
    direct_candidates: &'a [SocketAddr],
}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("conductor overlay helper is supported only on Linux");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    use conductor_overlay::{LinuxOverlayInterface, configured_packet_router};
    use std::collections::BTreeSet;

    let mut input = BufReader::new(io::stdin());
    let first = read_json_line(&mut input)
        .await?
        .ok_or("helper requires a start request")?;
    let Request::Start {
        worker_id,
        secret_key,
        configuration,
    } = first
    else {
        return Err("helper requires a start request".into());
    };
    if configuration.worker_id != worker_id {
        return Err("helper worker identity does not match configuration".into());
    }

    let interface_name = conductor_overlay::interface_name_for_worker(&worker_id);
    configured_packet_router(&configuration, &worker_id, now_ms())
        .map_err(|_| "manager overlay configuration is invalid")?;
    let mut interface = LinuxOverlayInterface::start(
        &interface_name,
        configuration.worker_address,
        configuration.peers.iter().map(|peer| peer.worker_address),
    )
    .await?;
    let mut endpoint = create_endpoint(secret_key).await?;
    let mut direct_candidates = endpoint_direct_candidates(&endpoint);
    write_configuration_response(
        "ready",
        &worker_id,
        &interface,
        &configuration,
        &direct_candidates,
    )
    .await?;

    let mut configuration = configuration;
    loop {
        let run_configuration = configuration.clone();
        let packet_loop = conductor_overlay::run_packet_dataplane(
            interface.device(),
            endpoint.clone(),
            &run_configuration,
            &worker_id,
            now_ms(),
        );
        tokio::pin!(packet_loop);
        tokio::select! {
            result = &mut packet_loop => {
                let event = if result.is_ok() { "stopped" } else { "unavailable" };
                endpoint.close().await;
                write_configuration_response(event, &worker_id, &interface, &configuration, &direct_candidates).await?;
                match read_json_line(&mut input).await? {
                    Some(Request::Reconfigure { configuration: updated }) => {
                        validate_reconfiguration(&updated, &worker_id, &configuration, &interface)?;
                        configured_packet_router(&updated, &worker_id, now_ms())
                            .map_err(|_| "manager overlay configuration is invalid")?;
                        let peers = updated.peers.iter().map(|peer| peer.worker_address).collect::<BTreeSet<_>>();
                        interface.reconcile_routes(&peers).await?;
                        configuration = updated;
                        endpoint = create_endpoint(secret_key).await?;
                        direct_candidates = endpoint_direct_candidates(&endpoint);
                        write_configuration_response("applied", &worker_id, &interface, &configuration, &direct_candidates).await?;
                    }
                    Some(Request::Shutdown) | None => break,
                    Some(Request::Start { .. }) => return Err("duplicate start request".into()),
                }
            }
            request = read_json_line(&mut input) => {
                match request? {
                    Some(Request::Reconfigure { configuration: updated }) => {
                        validate_reconfiguration(&updated, &worker_id, &configuration, &interface)?;
                        configured_packet_router(&updated, &worker_id, now_ms())
                            .map_err(|_| "manager overlay configuration is invalid")?;
                        let peers = updated.peers.iter().map(|peer| peer.worker_address).collect::<BTreeSet<_>>();
                        interface.reconcile_routes(&peers).await?;
                        configuration = updated;
                        write_configuration_response("applied", &worker_id, &interface, &configuration, &direct_candidates).await?;
                    }
                    Some(Request::Shutdown) | None => break,
                    Some(Request::Start { .. }) => return Err("duplicate start request".into()),
                }
            }
        }
    }

    endpoint.close().await;
    let local_address = interface.local_address();
    interface.shutdown().await?;
    write_response(Response {
        event: "stopped",
        worker_id: Some(&worker_id),
        interface_name: Some(&interface_name),
        local_address: Some(local_address),
        peer_generation: Some(configuration.peer_generation),
        network_generation: Some(configuration.network_generation),
        expires_at_ms: Some(configuration.expires_at_ms),
        direct_candidates: &direct_candidates,
    })
    .await?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_reconfiguration(
    updated: &OverlayPeerConfiguration,
    worker_id: &WorkerId,
    current: &OverlayPeerConfiguration,
    interface: &conductor_overlay::LinuxOverlayInterface,
) -> Result<(), Box<dyn std::error::Error>> {
    if &updated.worker_id != worker_id
        || updated.network_domain_id != current.network_domain_id
        || updated.worker_address != interface.local_address()
    {
        return Err("overlay helper identity, domain, or address changed".into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn create_endpoint(
    secret_key: [u8; 32],
) -> Result<iroh::Endpoint, Box<dyn std::error::Error>> {
    conductor_overlay::create_endpoint(iroh_base::SecretKey::from_bytes(&secret_key))
        .await
        .map_err(|_| "could not start the overlay endpoint".into())
}

#[cfg(target_os = "linux")]
fn endpoint_direct_candidates(endpoint: &iroh::Endpoint) -> Vec<SocketAddr> {
    let mut candidates = endpoint.addr().ip_addrs().copied().collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates.dedup();
    candidates.retain(|candidate| {
        conductor_core::valid_overlay_direct_candidates(std::slice::from_ref(candidate))
    });
    candidates.truncate(conductor_core::MAX_OVERLAY_DIRECT_CANDIDATES);
    candidates
}

#[cfg(target_os = "linux")]
async fn write_configuration_response(
    event: &str,
    worker_id: &WorkerId,
    interface: &conductor_overlay::LinuxOverlayInterface,
    configuration: &OverlayPeerConfiguration,
    direct_candidates: &[SocketAddr],
) -> io::Result<()> {
    write_response(Response {
        event,
        worker_id: Some(worker_id),
        interface_name: Some(interface.interface_name()),
        local_address: Some(interface.local_address()),
        peer_generation: Some(configuration.peer_generation),
        network_generation: Some(configuration.network_generation),
        expires_at_ms: Some(configuration.expires_at_ms),
        direct_candidates,
    })
    .await
}

#[cfg(target_os = "linux")]
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
async fn read_json_line<R>(reader: &mut R) -> io::Result<Option<Request>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete helper request",
                ))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.unwrap_or(available.len());
        if line.len().saturating_add(count) > MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "helper request exceeds the size limit",
            ));
        }
        line.extend_from_slice(&available[..count]);
        reader.consume(count + usize::from(newline.is_some()));
        if newline.is_some() {
            let request = serde_json::from_slice(&line).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid helper request")
            })?;
            return Ok(Some(request));
        }
    }
}

#[cfg(target_os = "linux")]
async fn write_response(response: Response<'_>) -> io::Result<()> {
    let mut output = io::stdout();
    let mut encoded = serde_json::to_vec(&response)
        .map_err(|_| io::Error::other("could not encode helper response"))?;
    encoded.push(b'\n');
    output.write_all(&encoded).await?;
    output.flush().await
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn helper_protocol_rejects_unknown_fields_and_oversized_frames() {
        let mut unknown = BufReader::new(
            br#"{"command":"start","worker_id":"worker-a","secret_key":[],"configuration":{},"extra":true}
"#
            .as_slice(),
        );
        assert!(read_json_line(&mut unknown).await.is_err());

        let oversized = vec![b'x'; MAX_REQUEST_BYTES + 1];
        let mut reader = BufReader::new(oversized.as_slice());
        assert_eq!(
            read_json_line(&mut reader)
                .await
                .expect_err("oversized frame must fail")
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
}
