use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

use matter_rs_controller::stack_api::{Stack, StackEvent};
use matter_rs_controller::storage::{normalize_fabric_label, Storage};
use matter_rs_server::{config::Config, http, logging};
use socket2::{Domain, Protocol, Socket, Type};

/// How long the Matter stack gets to establish (or load) the fabric identity
/// before we give up. Generating a CA plus a NOC is fast; a slow or read-only
/// storage path is what this actually guards against.
const STACK_START_TIMEOUT: Duration = Duration::from_secs(60);

/// Time for the HTTP listeners to drain after the `server_shutdown` frames go
/// out. `StackHandle::shutdown` adds up to 10s of its own on top of this (5s for
/// the loop's acknowledgement, then 5s for the thread join), so a clean stop is
/// budgeted at ~13s worst case — well inside systemd's default
/// `TimeoutStopSec=90`.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// Why the serve loop ended. The distinction reaches the exit code: an operator
/// stop is a success, a dead stack is not.
enum StopReason {
    Signal,
    StackDied,
}

/// Report a startup failure the way an operator reads it — without a panic
/// backtrace — and exit non-zero so systemd/docker restarts us.
///
/// Both channels on purpose. `tracing` is what reaches `--log-file`, but a
/// `RUST_LOG` directive can silence our own target (`from_env_lossy` honours it),
/// and a fatal exit with no explanation at all is far worse than one duplicated
/// stderr line.
fn fatal(message: &str) -> ! {
    tracing::error!("{message}");
    eprintln!("fatal: {message}");
    std::process::exit(1);
}

/// Binds `[::]:<port>` (the implicit "no `--listen-address` given" address)
/// with `IPV6_V6ONLY` explicitly cleared, so a lone dual-stack bind also
/// answers IPv4 clients. Plain `TcpListener::bind` leaves that flag at
/// whatever the platform defaults it to: off (dual-stack) on Linux and
/// macOS, but on (v6-only) on some other hosts (some BSDs, and Windows) —
/// which would silently refuse every IPv4 connection there despite the
/// operator asking to bind "all interfaces". Explicit `--listen-address`
/// values skip this: a caller who named specific addresses gets exactly
/// those sockets, not an implicit dual-stack widening of one of them.
///
/// Mirrors `matter-rs-stack::runtime::create_dual_stack_socket`'s dance for
/// its UDP socket (same underlying reason, same `set_only_v6(false)`); this
/// is the TCP analogue. `tokio::net::TcpListener::from_std` requires the
/// socket already be non-blocking, hence `set_nonblocking(true)` last.
fn bind_dual_stack(addr: &str) -> std::io::Result<tokio::net::TcpListener> {
    let sock_addr: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{addr}: {e}")))?;
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_only_v6(false)?;
    socket.set_reuse_address(true)?;
    socket.bind(&sock_addr.into())?;
    // 128: the same backlog std's own `TcpListener::bind` asks for internally
    // on every platform but the 3DS and Haiku. This is a control-plane
    // listener (one connection per WS/HTTP client, not a high-fanout server),
    // so there's no tuning case for deviating from that default.
    socket.listen(128)?;
    socket.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(socket.into())
}

#[tokio::main]
async fn main() {
    let config = Config::parse();
    let log_control = Arc::new(logging::init(&config));
    config.warn_ignored();

    // Storage dir now (plan 2 stores fabric data in it). Only chmod 0700 when
    // we're the ones creating it — an existing dir keeps whatever permissions
    // it already has. Both happen before `Storage::open`, which creates subdirs
    // underneath and would otherwise inherit the looser mode.
    let storage_dir_existed = config.storage_path.exists();
    if let Err(e) = std::fs::create_dir_all(&config.storage_path) {
        fatal(&format!("cannot create --storage-path {}: {e}", config.storage_path.display()));
    }
    #[cfg(unix)]
    {
        if !storage_dir_existed {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&config.storage_path, std::fs::Permissions::from_mode(0o700));
        }
    }

    let storage = match Storage::open(&config.storage_path) {
        Ok(s) => Arc::new(s),
        Err(e) => fatal(&format!("cannot open --storage-path {}: {e}", config.storage_path.display())),
    };

    // The label the fabric boots with. `--default-fabric-label` pins it, and
    // that pin also wins over a stale stored value, so persist it now — before
    // the controller loads config.json — and let `label_locked` refuse later
    // changes. Without the flag the STORED label is the truth: a client's
    // `set_default_fabric_label` wrote it there, and re-imposing the built-in
    // default every boot would revert the fabric while config.json (and so
    // `get_fabric_label`) still reported the client's choice.
    let fabric_label = match &config.default_fabric_label {
        Some(pinned) => {
            let label = normalize_fabric_label(Some(pinned));
            let mut cfg = storage.load_config();
            if cfg.fabric_label != label {
                cfg.fabric_label = label.clone();
                if let Err(e) = storage.save_config(&cfg) {
                    tracing::warn!("could not persist the pinned fabric label: {e}");
                }
            }
            label
        }
        None => storage.load_config().fabric_label,
    };

    let (stack, mut stack_events, ready) = matter_rs_stack::spawn(matter_rs_stack::StackConfig {
        storage: storage.clone(),
        fabric_id: config.fabric_id,
        vendor_id: config.vendor_id,
        fabric_label,
        primary_interface: config.primary_interface.clone(),
    });

    // Three distinct failure modes, all fatal, none of them "still starting":
    // a timeout, a thread that ended without answering (including a `spawn`
    // that never got off the ground — its `ready` sender was dropped with the
    // closure), and an honest error from the boot sequence.
    let ready = match tokio::time::timeout(STACK_START_TIMEOUT, ready).await {
        Err(_) => fatal(&format!(
            "the Matter stack did not finish starting within {}s",
            STACK_START_TIMEOUT.as_secs()
        )),
        Ok(Err(_)) => fatal("the Matter stack thread ended before it reported readiness"),
        Ok(Ok(Err(e))) => fatal(&format!("the Matter stack failed to start: {e}")),
        Ok(Ok(Ok(info))) => info,
    };
    tracing::info!(
        "Matter stack ready: fabric {} (compressed {:#x}), fabric index {}, controller node {}",
        ready.identity.fabric_id,
        ready.identity.compressed_fabric_id,
        ready.fabric_index,
        ready.identity.controller_node_id,
    );

    // The event stream doubles as the stack thread's liveness signal: its sender
    // lives on that thread, so end-of-stream means the thread is gone. The
    // controller must not be the only one to see that — it just stops its
    // consumer loop, leaving the WS server answering `server_info` normally
    // while every Matter command fails forever, with nothing for a supervisor
    // to restart. So relay the events and turn an unexpected end-of-stream into
    // a shutdown with a non-zero exit.
    let (relay_tx, relay_rx) = tokio::sync::mpsc::unbounded_channel::<StackEvent>();
    let (died_tx, died_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        while let Some(ev) = stack_events.recv().await {
            // Only fails once the controller is gone, i.e. we are already
            // tearing down; there is nobody left to relay to either way.
            if relay_tx.send(ev).is_err() {
                return;
            }
        }
        // `died_tx` fails if main already left its select — an expected close
        // during our own shutdown, not news.
        let _ = died_tx.send(());
    });

    let sdk_version = format!("matter-rs-server/{} (rs-matter/03bc8f2)", env!("CARGO_PKG_VERSION"));
    let controller = matter_rs_controller::real::MatterController::new(
        Arc::new(stack.clone()),
        storage,
        ready.identity,
        ready.fabric_index,
        sdk_version,
        config.default_fabric_label.is_some(),
        log_control,
        relay_rx,
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let state = http::AppState { controller, shutdown: shutdown_rx };
    let router = http::build_router(state);

    // Bind each --listen-address (or all interfaces when none given).
    let is_default_bind = config.listen_address.is_empty();
    let addrs: Vec<String> = if is_default_bind {
        tracing::warn!("no --listen-address given; binding all interfaces");
        vec![format!("[::]:{}", config.port)]
    } else {
        config.listen_address.iter().map(|a| {
            if a.contains(':') { format!("[{}]:{}", a, config.port) } else { format!("{}:{}", a, config.port) }
        }).collect()
    };

    let mut servers = tokio::task::JoinSet::new();
    for addr in addrs {
        // Only the implicit "[::]" path needs the dual-stack socket option; an
        // operator who named specific addresses gets exactly those sockets.
        let bound = if is_default_bind {
            bind_dual_stack(&addr)
        } else {
            tokio::net::TcpListener::bind(&addr).await
        };
        let listener = match bound {
            Ok(l) => l,
            Err(e) => {
                // The stack thread is already up and holding the fabric; stop it
                // before leaving, or the process lingers on a non-daemon thread.
                stack.shutdown().await;
                fatal(&format!("cannot bind {addr}: {e}"));
            }
        };
        match listener.local_addr() {
            Ok(bound) => println!("listening on {bound}"),
            Err(e) => tracing::warn!("bound {addr} but could not read it back: {e}"),
        }
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

    // SIGTERM/SIGINT -> clean shutdown; a dead stack -> the same shutdown, then
    // a non-zero exit.
    let stop = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut sigterm) => tokio::select! {
            _ = sigterm.recv() => StopReason::Signal,
            _ = tokio::signal::ctrl_c() => StopReason::Signal,
            _ = died_rx => StopReason::StackDied,
        },
        Err(e) => {
            // Without SIGTERM there is no orderly stop under systemd, so say so
            // loudly rather than silently serving un-stoppably.
            tracing::error!("cannot install a SIGTERM handler ({e}); only Ctrl-C and stack failure will stop the server");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => StopReason::Signal,
                _ = died_rx => StopReason::StackDied,
            }
        }
    };

    match stop {
        StopReason::Signal => tracing::info!("shutting down"),
        StopReason::StackDied => tracing::error!(
            "the Matter stack thread ended unexpectedly; shutting down so the supervisor can restart us"
        ),
    }

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(DRAIN_TIMEOUT, async {
        while let Some(res) = servers.join_next().await {
            if let Err(e) = res {
                tracing::error!("listener task failed: {e}");
            }
        }
    }).await;

    // The `server_shutdown` frames already went out over the watch channel. This
    // is an abrupt stop, not a drain: in-flight detached work (a commissioning
    // attempt, say) is abandoned. See `StackHandle::shutdown`.
    stack.shutdown().await;

    if matches!(stop, StopReason::StackDied) {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for `set_only_v6(false)` specifically: bind `[::]:0`
    /// via `bind_dual_stack`, then connect to that same port over IPv4. A
    /// v6-only socket would refuse (or hang on) that connect instead.
    ///
    /// Caveat: this only distinguishes the two outcomes on a host whose
    /// platform default is v6-only (some BSDs, Windows) — Linux and macOS
    /// already default new `[::]` binds to dual-stack, so on those platforms
    /// this test would also pass with the `set_only_v6(false)` line deleted.
    /// It still pins the function's actual contract (bind once, serve both
    /// families), which is what regressions in the surrounding wiring — the
    /// domain, the bind address, the non-blocking conversion — would break
    /// on every platform, including this one.
    #[tokio::test]
    async fn dual_stack_bind_accepts_an_ipv4_connection() {
        let listener = bind_dual_stack("[::]:0").unwrap();
        let addr = listener.local_addr().unwrap();
        assert!(addr.is_ipv6(), "expected an IPv6-family local_addr, got {addr}");

        let accept = tokio::spawn(async move { listener.accept().await });
        tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(("127.0.0.1", addr.port())))
            .await
            .expect("IPv4 connect to the dual-stack listener timed out")
            .expect("IPv4 connect to the dual-stack listener should succeed");
        accept.await.unwrap().expect("accept should succeed for the IPv4 connection");
    }

    #[test]
    fn bind_dual_stack_rejects_an_unparseable_address() {
        assert!(bind_dual_stack("not an address").is_err());
    }
}
