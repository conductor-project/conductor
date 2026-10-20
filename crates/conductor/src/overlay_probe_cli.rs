//! Bounded application reachability checks from the local worker host.

use std::net::Ipv4Addr;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use serde_json::json;

const MAX_PROBE_TIMEOUT_MS: u64 = 5_000;
const MAX_PROBE_RESPONSE_BYTES: usize = 65_536;

#[derive(Debug, Args)]
pub struct OverlayArgs {
    #[command(subcommand)]
    command: OverlayCommand,
}

#[derive(Debug, Subcommand)]
enum OverlayCommand {
    /// Request an explicit HTTP health path through this host's overlay route.
    Probe {
        /// Assigned private IPv4 address of the target worker or service.
        #[arg(long)]
        target_ip: Ipv4Addr,
        /// Dynamically allocated workload port.
        #[arg(long, value_parser = clap::value_parser!(u16).range(1..=65535))]
        port: u16,
        /// Explicit, side-effect-safe HTTP GET path, for example /health.
        #[arg(long)]
        path: String,
        /// Total connection and response deadline in milliseconds.
        #[arg(long, default_value_t = 2_000, value_parser = clap::value_parser!(u64).range(1..=MAX_PROBE_TIMEOUT_MS))]
        timeout_ms: u64,
    },
}

pub fn run(args: OverlayArgs) -> ExitCode {
    match args.command {
        OverlayCommand::Probe {
            target_ip,
            port,
            path,
            timeout_ms,
        } => {
            let Some(error) = validate_probe(target_ip, port, &path) else {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        emit_error("runtime_error", "overlay probe runtime could not start");
                        return ExitCode::FAILURE;
                    }
                };
                return match runtime.block_on(probe(target_ip, port, path, timeout_ms)) {
                    Ok(result) => {
                        println!("{result}");
                        if result["state"] == "succeeded" {
                            ExitCode::SUCCESS
                        } else {
                            ExitCode::FAILURE
                        }
                    }
                    Err((code, message, elapsed_ms)) => {
                        println!(
                            "{}",
                            json!({
                                "targetIp": target_ip,
                                "port": port,
                                "state": "failed",
                                "failure": code,
                                "message": message,
                                "elapsedMs": elapsed_ms,
                                "reachability": "not_verified",
                            })
                        );
                        ExitCode::FAILURE
                    }
                };
            };
            emit_error("invalid_probe", error);
            ExitCode::FAILURE
        }
    }
}

fn validate_probe(target_ip: Ipv4Addr, port: u16, path: &str) -> Option<&'static str> {
    if !target_ip.is_private()
        || target_ip.is_loopback()
        || target_ip.is_link_local()
        || target_ip.is_broadcast()
        || target_ip.is_multicast()
        || target_ip.is_unspecified()
    {
        return Some("target IP must be an assigned private overlay IPv4 address");
    }
    if port == 0 {
        return Some("port must be nonzero");
    }
    if path.is_empty()
        || path.len() > 1024
        || !path.starts_with('/')
        || path.starts_with("//")
        || path.bytes().any(|byte| byte.is_ascii_control())
    {
        return Some("path must be an absolute origin path without control characters");
    }
    None
}

async fn probe(
    target_ip: Ipv4Addr,
    port: u16,
    path: String,
    timeout_ms: u64,
) -> Result<serde_json::Value, (&'static str, &'static str, u64)> {
    let started = Instant::now();
    let timeout = Duration::from_millis(timeout_ms);
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| ("client_error", "HTTP probe client could not start", 0))?;
    let url = format!("http://{target_ip}:{port}{path}");
    let response = client.get(url).send().await.map_err(|error| {
        let code = if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connect_failed"
        } else {
            "request_failed"
        };
        let message = if error.is_timeout() {
            "HTTP probe exceeded its deadline"
        } else if error.is_connect() {
            "target could not be reached through the local overlay route"
        } else {
            "HTTP probe request failed"
        };
        (
            code,
            message,
            started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        )
    })?;
    let status = response.status();
    let mut response = response;
    let mut response_bytes = 0_usize;
    loop {
        let next = response.chunk().await.map_err(|_| {
            (
                "response_failed",
                "HTTP probe response could not be read",
                started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            )
        })?;
        let Some(chunk) = next else { break };
        response_bytes = response_bytes.saturating_add(chunk.len());
        if response_bytes > MAX_PROBE_RESPONSE_BYTES {
            return Err((
                "response_too_large",
                "HTTP probe response exceeded the 65536 byte limit",
                started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            ));
        }
    }
    let elapsed_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
    let succeeded = status.is_success();
    Ok(json!({
        "targetIp": target_ip,
        "port": port,
        "httpStatus": status.as_u16(),
        "responseBytes": response_bytes,
        "elapsedMs": elapsed_ms,
        "state": if succeeded { "succeeded" } else { "failed" },
        "failure": if succeeded { serde_json::Value::Null } else { json!("http_status") },
        "reachability": "application_response_received",
        "scope": "local_worker_overlay_path",
    }))
}

fn emit_error(code: &str, message: &str) {
    println!(
        "{}",
        json!({
            "state": "failed",
            "failure": code,
            "message": message,
            "reachability": "not_verified",
        })
    );
}

#[cfg(test)]
mod tests {
    use super::{probe, validate_probe};
    use axum::{Router, routing::get};
    use std::net::Ipv4Addr;

    #[test]
    fn probe_requires_private_overlay_ip_and_explicit_safe_path() {
        assert_eq!(
            validate_probe(Ipv4Addr::new(10, 20, 0, 8), 8080, "/health"),
            None
        );
        assert!(validate_probe(Ipv4Addr::LOCALHOST, 8080, "/health").is_some());
        assert!(validate_probe(Ipv4Addr::new(8, 8, 8, 8), 8080, "/health").is_some());
        assert!(validate_probe(Ipv4Addr::new(10, 20, 0, 8), 0, "/health").is_some());
        assert!(validate_probe(Ipv4Addr::new(10, 20, 0, 8), 8080, "health").is_some());
        assert!(validate_probe(Ipv4Addr::new(10, 20, 0, 8), 8080, "//other/path").is_some());
        assert!(validate_probe(Ipv4Addr::new(10, 20, 0, 8), 8080, "/a\r\nb").is_some());
    }

    #[tokio::test]
    async fn probe_requires_a_bounded_successful_application_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral test listener");
        let address = listener.local_addr().expect("read listener address");
        let app = Router::new().route("/health", get(|| async { "healthy" }));
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve test response");
        });

        let result = probe(
            address.ip().to_string().parse().unwrap(),
            address.port(),
            "/health".into(),
            1_000,
        )
        .await
        .expect("receive bounded application response");
        assert_eq!(result["state"], "succeeded");
        assert_eq!(result["httpStatus"], 200);
        assert_eq!(result["responseBytes"], 7);
        server.abort();
    }

    #[tokio::test]
    async fn probe_rejects_an_oversized_response_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral test listener");
        let address = listener.local_addr().expect("read listener address");
        let app = Router::new().route(
            "/health",
            get(|| async { "x".repeat(super::MAX_PROBE_RESPONSE_BYTES + 1) }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve test response");
        });

        let result = probe(
            address.ip().to_string().parse().unwrap(),
            address.port(),
            "/health".into(),
            1_000,
        )
        .await;
        assert!(matches!(result, Err(("response_too_large", _, _))));
        server.abort();
    }
}
