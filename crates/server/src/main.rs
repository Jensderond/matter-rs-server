use std::sync::Arc;

use clap::Parser;

use matter_rs_controller::stub::StubController;
use matter_rs_server::{config::Config, http, logging};
use matter_rs_wire::server_info::{ServerInfoMessage, MIN_SUPPORTED_SCHEMA_VERSION, SCHEMA_VERSION};

#[tokio::main]
async fn main() {
    let config = Config::parse();
    logging::init(&config);
    config.warn_ignored();

    // Storage dir now (plan 2 stores fabric data in it). Only chmod 0700 when
    // we're the ones creating it — an existing dir keeps whatever permissions
    // it already has.
    let storage_dir_existed = config.storage_path.exists();
    std::fs::create_dir_all(&config.storage_path).expect("cannot create --storage-path");
    #[cfg(unix)]
    {
        if !storage_dir_existed {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&config.storage_path, std::fs::Permissions::from_mode(0o700));
        }
    }

    // Plan 1: stub controller. Plan 2 replaces this with the rs-matter one.
    let info = ServerInfoMessage {
        fabric_id: config.fabric_id,
        compressed_fabric_id: 0,
        fabric_index: None,
        schema_version: SCHEMA_VERSION,
        min_supported_schema_version: MIN_SUPPORTED_SCHEMA_VERSION,
        sdk_version: format!("matter-rs-server/{} (rs-matter/pending)", env!("CARGO_PKG_VERSION")),
        wifi_credentials_set: false,
        wifi_ssid: None,
        thread_credentials_set: false,
        bluetooth_enabled: false,
        ble_proxy_enabled: None,
        controller_node_id: None,
    };
    let controller = Arc::new(StubController::new(info));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let state = http::AppState { controller, shutdown: shutdown_rx };
    let router = http::build_router(state);

    // Bind each --listen-address (or all interfaces when none given).
    let addrs: Vec<String> = if config.listen_address.is_empty() {
        tracing::warn!("no --listen-address given; binding all interfaces");
        vec![format!("[::]:{}", config.port)]
    } else {
        config.listen_address.iter().map(|a| {
            if a.contains(':') { format!("[{}]:{}", a, config.port) } else { format!("{}:{}", a, config.port) }
        }).collect()
    };

    let mut servers = tokio::task::JoinSet::new();
    for addr in addrs {
        let listener = tokio::net::TcpListener::bind(&addr).await
            .unwrap_or_else(|e| panic!("cannot bind {addr}: {e}"));
        println!("listening on {}", listener.local_addr().unwrap());
        let router = router.clone();
        let mut rx = shutdown_tx.subscribe();
        servers.spawn(async move {
            if let Err(e) = axum::serve(listener, router)
                .with_graceful_shutdown(async move { let _ = rx.changed().await; })
                .await
            {
                tracing::error!("listener error: {e}");
            }
        });
    }

    // SIGTERM/SIGINT -> shutdown.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = sigterm.recv() => {},
        _ = tokio::signal::ctrl_c() => {},
    }
    tracing::info!("shutting down");
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while let Some(res) = servers.join_next().await {
            if let Err(e) = res {
                tracing::error!("listener task failed: {e}");
            }
        }
    }).await;
}
