#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::net::Ipv4Addr;

use conductor_overlay::LinuxOverlayInterface;

#[tokio::test]
#[ignore = "requires a Linux network namespace, CAP_NET_ADMIN, and /dev/net/tun"]
async fn managed_interface_routes_reconcile_and_restart_cleanly() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        managed_interface_routes_reconcile_and_restart().await;
    })
    .await
    .expect("Linux overlay lifecycle must complete within 15 seconds");
}

#[tokio::test]
#[ignore = "requires a Linux network namespace, CAP_NET_ADMIN, and /dev/net/tun"]
async fn helper_process_owns_tun_and_runs_overlay_dataplane() {
    use conductor_core::{
        NetworkDomainId, OVERLAY_PEER_CONFIGURATION_SCHEMA_VERSION, OverlayPeerConfiguration,
        WorkerId,
    };
    use conductor_overlay::LinuxOverlayHelperClient;

    let worker_id = WorkerId::new("helper-integration").expect("valid worker ID");
    let local = Ipv4Addr::new(198, 18, 11, 1);
    let configuration = OverlayPeerConfiguration {
        schema_version: OVERLAY_PEER_CONFIGURATION_SCHEMA_VERSION,
        network_domain_id: NetworkDomainId::new("overlay-test").expect("valid domain ID"),
        worker_id: worker_id.clone(),
        worker_address: local,
        peer_generation: 1,
        network_generation: 1,
        expires_at_ms: now_ms() + 60_000,
        peers: Vec::new(),
    };
    let helper_path = std::path::Path::new(env!("CARGO_BIN_EXE_conductor-overlay-helper"));
    let secret_key = [7_u8; 32];
    let mut helper =
        LinuxOverlayHelperClient::start(helper_path, &worker_id, secret_key, &configuration)
            .await
            .expect("start privileged overlay data plane");

    let mut refreshed = configuration.clone();
    refreshed.peer_generation = 2;
    refreshed.network_generation = 2;
    refreshed.expires_at_ms = now_ms() + 60_000;
    helper
        .reconfigure(&refreshed)
        .await
        .expect("helper applies a fresh manager snapshot");
    helper
        .shutdown()
        .await
        .expect("helper removes owned routes");

    let restarted =
        LinuxOverlayHelperClient::start(helper_path, &worker_id, secret_key, &refreshed)
            .await
            .expect("helper restart reuses the stable identity and interface");
    restarted
        .shutdown()
        .await
        .expect("clean up after helper restart");
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("test clock after Unix epoch")
        .as_millis()
        .try_into()
        .expect("test timestamp fits i64")
}

async fn managed_interface_routes_reconcile_and_restart() {
    let interface_name = format!("cnd-{:x}", std::process::id());
    let local = Ipv4Addr::new(198, 18, 10, 1);
    let peer = Ipv4Addr::new(198, 18, 10, 2);
    let peers = BTreeSet::from([peer]);

    let mut interface = LinuxOverlayInterface::start(&interface_name, local, [peer])
        .await
        .expect("create owned TUN and peer route");
    assert_eq!(interface.interface_name(), interface_name);
    assert_eq!(interface.local_address(), local);
    interface
        .reconcile_routes(&peers)
        .await
        .expect("reapplying the same route must be idempotent");
    interface
        .reconcile_routes(&BTreeSet::new())
        .await
        .expect("remove only the owned peer route");
    interface.shutdown().await.expect("clean up owned routes");

    let restarted = LinuxOverlayInterface::start(&interface_name, local, [peer])
        .await
        .expect("restart with the same Conductor resources");
    restarted.shutdown().await.expect("clean up after restart");
}
