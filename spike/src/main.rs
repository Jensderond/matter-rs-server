//! Phase 0 spike — see docs/superpowers/specs/2026-08-13-phase0-spike.md
//!
//! Commissions a real Matter device with rs-matter (pinned main) and controls
//! it: mDNS discovery -> PASE -> CA/NOC issuance -> CASE -> read BasicInformation
//! -> OnOff toggle. Throwaway quality by design.
//!
//! Usage:
//!   matter-spike <PAIRING_CODE> [--addr IP:PORT] [--endpoint N] [--no-toggle]
//!
//! PAIRING_CODE is an `MT:` QR string or an 11-digit manual pairing code
//! (as produced by Home Assistant "Share device" / open_commissioning_window).
//!
//! Commissioning flow largely mirrors rs-matter's own
//! `tests/src/bin/commissioner_tests.rs` and `rs-matter/tests/commissioning.rs`.

mod mdns;

use core::num::NonZeroU8;
use core::pin::pin;

use std::net::{SocketAddr, UdpSocket};
use std::process::ExitCode;

use embassy_futures::select::{select, select3, Either, Either3};
use embassy_time::{Duration, Timer};

use log::{error, info, warn};

use rs_matter::cert::gen::VALID_FOREVER;
use rs_matter::cert::{MAX_CERT_TLV_AND_ASN1_LEN, MAX_CERT_TLV_LEN};
use rs_matter::crypto::{
    default_crypto, CanonAeadKey, CanonPkcSecretKey, Crypto, RngCore as _, SecretKey,
    SigningSecretKey,
};
use rs_matter::dm::clusters::app::on_off::OnOffClient as _;
use rs_matter::dm::clusters::decl::basic_information::BasicInformationClient as _;
use rs_matter::dm::devices::test::{DAC_PRIVKEY, TEST_DEV_ATT, TEST_DEV_COMM, TEST_DEV_DET};
use rs_matter::error::{Error, ErrorCode};
use rs_matter::onboard::cac::{IcacGenerator, RcacGenerator};
use rs_matter::onboard::noc::NocGenerator;
use rs_matter::onboard::{CommissionOptions, Commissioner};
use rs_matter::pairing::qr::QrPayload;
use rs_matter::transport::exchange::Exchange;
use rs_matter::transport::network::mdns::CommissionableFilter;
use rs_matter::transport::network::{Address, NoNetwork};
use rs_matter::utils::init::InitMaybeUninit;
use rs_matter::Matter;

use socket2::{Domain, Protocol, Socket, Type};
use static_cell::StaticCell;

const BROWSE_TIMEOUT_MS: u32 = 30_000;
const COMMISSION_TIMEOUT_SECS: u64 = 60;
const IM_TIMEOUT_SECS: u64 = 20;

const FABRIC_ID: u64 = 1;
const CONTROLLER_NODE_ID: u64 = 112233;
const DEVICE_NODE_ID: u64 = 112234;
const ADMIN_VENDOR_ID: u16 = 0xFFF1;

static CTRL_MATTER: StaticCell<Matter> = StaticCell::new();

fn main() -> ExitCode {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = match parse_args() {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("matter-spike: {msg}");
            eprintln!(
                "usage: matter-spike <PAIRING_CODE> [--addr IP:PORT] [--endpoint N] [--no-toggle]"
            );
            return ExitCode::FAILURE;
        }
    };

    match futures_lite::future::block_on(run(args)) {
        Ok(()) => {
            println!("matter-spike: ok");
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!("matter-spike: FAILED — {e:?}");
            ExitCode::FAILURE
        }
    }
}

struct Args {
    code: String,
    addr: Option<SocketAddr>,
    endpoint: u16,
    toggle: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut code = None;
    let mut addr = None;
    let mut endpoint = 1u16;
    let mut toggle = true;

    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--addr" => {
                let v = iter.next().ok_or("--addr needs a value")?;
                addr = Some(
                    v.parse::<SocketAddr>()
                        .map_err(|e| format!("--addr must be IP:PORT (e.g. [fe80::1%2]:5540 or 192.168.1.50:5540): {e}"))?,
                );
            }
            "--endpoint" => {
                let v = iter.next().ok_or("--endpoint needs a value")?;
                endpoint = v.parse().map_err(|e| format!("--endpoint must be u16: {e}"))?;
            }
            "--no-toggle" => toggle = false,
            other if code.is_none() => code = Some(other.to_string()),
            other => return Err(format!("unexpected argument: {other}")),
        }
    }

    Ok(Args {
        code: code.ok_or("missing PAIRING_CODE")?,
        addr,
        endpoint,
        toggle,
    })
}

async fn run(args: Args) -> Result<(), Error> {
    let socket = create_dual_stack_socket()?;
    info!(
        "controller bound on {}",
        socket.get_ref().local_addr().unwrap()
    );

    let crypto = default_crypto(rand::thread_rng(), DAC_PRIVKEY);
    let matter = CTRL_MATTER.uninit().init_with(Matter::init(
        &TEST_DEV_DET,
        TEST_DEV_COMM,
        &TEST_DEV_ATT,
        // local port = 0 -> kernel-picked, matches the ephemeral socket
        0,
    ));

    // Transport pump + mDNS responder must stay alive throughout.
    let transport_fut = matter.run(&crypto, &socket, &socket, NoNetwork);
    let mdns_fut = mdns::run_builtin_mdns(matter, &crypto);
    let flow_fut = flow(matter, &crypto, &args);

    let mut transport_fut = pin!(transport_fut);
    let mut mdns_fut = pin!(mdns_fut);
    let mut flow_fut = pin!(flow_fut);

    match select3(&mut transport_fut, &mut mdns_fut, &mut flow_fut).await {
        Either3::First(r) => {
            error!("transport exited prematurely: {r:?}");
            Err(ErrorCode::NoExchange.into())
        }
        Either3::Second(r) => {
            error!("mDNS exited prematurely: {r:?}");
            Err(ErrorCode::NoExchange.into())
        }
        Either3::Third(result) => result,
    }
}

async fn flow<C: Crypto>(matter: &Matter<'_>, crypto: &C, args: &Args) -> Result<(), Error> {
    // --- Phase 1: parse the pairing code ---------------------------------
    let mut qr_buf = [0u8; 512];
    let (passcode, filter) = parse_code(&args.code, &mut qr_buf)?;
    info!("pairing code parsed: passcode={passcode}, filter={filter:?}");

    // --- Phase 2: discover the commissionable device ----------------------
    let addr = match args.addr {
        Some(sa) => {
            info!("skipping discovery, using {sa}");
            Address::Udp(sa)
        }
        None => {
            info!("browsing mDNS for a matching commissionable device (up to {BROWSE_TIMEOUT_MS} ms)...");
            let (addr, instance_id) = matter
                .transport()
                .browse_commissionable(&filter, &[], BROWSE_TIMEOUT_MS)
                .await?;
            info!("DISCOVERY OK: instance {instance_id:016x} at {addr}");
            addr
        }
    };

    // --- Phase 3: commission ---------------------------------------------
    let (fab_idx, device_node_id) = commission(matter, crypto, addr, passcode).await?;
    info!("COMMISSIONING OK: fabric_index={fab_idx} device_node_id=0x{device_node_id:016x}");

    // --- Phase 4: control over CASE ---------------------------------------
    let vendor = with_timeout(read_vendor_name(matter, crypto, fab_idx, device_node_id)).await?;
    let product = with_timeout(read_product_name(matter, crypto, fab_idx, device_node_id)).await?;
    info!("BASIC INFO READ OK: vendor={vendor:?} product={product:?}");

    if args.toggle {
        match toggle_onoff(matter, crypto, fab_idx, device_node_id, args.endpoint).await {
            Ok((before, after)) => info!("ONOFF TOGGLE OK: {before} -> {after} (and back)"),
            // Not fatal: the device may simply not have OnOff on this endpoint.
            Err(e) => warn!(
                "OnOff toggle on endpoint {} failed (device may not support it): {e:?}",
                args.endpoint
            ),
        }
    }

    info!("=== spike flow complete ===");
    Ok(())
}

fn parse_code(code: &str, buf: &mut [u8]) -> Result<(u32, CommissionableFilter), Error> {
    let code = code.trim();
    if code.starts_with("MT:") {
        let qr = QrPayload::parse(code, buf)?;
        Ok((qr.passcode(), qr.commissionable_filter()))
    } else {
        let manual = QrPayload::parse_pairing_code(code)?;
        Ok((manual.passcode(), manual.commissionable_filter()))
    }
}

/// The commissioning phase, mirroring `commissioner_tests.rs`: offline CA
/// chain, controller NOC + fabric install, then `Commissioner::commission`
/// (PASE + over-PASE config) and `complete_via_case`.
async fn commission<C: Crypto>(
    matter: &Matter<'_>,
    crypto: &C,
    peer: Address,
    passcode: u32,
) -> Result<(NonZeroU8, u64), Error> {
    info!("=== Commissioner::commission (phase 1 — PASE + over PASE) ===");

    // Offline CA chain: RCAC then ICAC; RCAC priv key discarded immediately.
    // SPIKE_NO_ICAC=1 switches to RCAC-direct mode (NOCs signed by the RCAC,
    // no intermediate) — used to isolate ICAC-specific interop failures.
    let no_icac = std::env::var("SPIKE_NO_ICAC").is_ok_and(|v| v == "1");

    let mut rcac_buf = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
    let mut rcac_gen = RcacGenerator::new(&mut rcac_buf);
    let (rcac_priv, rcac) = rcac_gen.generate(crypto, FABRIC_ID, VALID_FOREVER)?;

    let mut icac_buf = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
    let mut icac_gen = IcacGenerator::new(&mut icac_buf);
    let (signing_priv, icac): (_, &[u8]) = if no_icac {
        info!("RCAC-direct mode: no ICAC, NOCs signed by the RCAC");
        (rcac_priv, &[])
    } else {
        let (icac_priv, icac) =
            icac_gen.generate(crypto, rcac_priv.reference(), rcac, VALID_FOREVER)?;
        drop(rcac_priv);
        (icac_priv, icac)
    };

    // Controller operational keypair + CSR + NOC.
    let controller_secret_key = crypto.generate_secret_key()?;
    let mut controller_csr_buf = [0u8; 256];
    let controller_csr = controller_secret_key.csr(&mut controller_csr_buf)?;
    let mut controller_secret_key_canon = CanonPkcSecretKey::new();
    controller_secret_key.write_canon(&mut controller_secret_key_canon)?;

    let mut noc_buf = [0u8; MAX_CERT_TLV_AND_ASN1_LEN];
    let mut noc_generator =
        NocGenerator::create(signing_priv.reference(), rcac, icac, &mut noc_buf)?;

    let controller_noc = noc_generator.generate(
        crypto,
        controller_csr,
        CONTROLLER_NODE_ID,
        &[],
        VALID_FOREVER,
    )?;

    // Fabric IPK: 16 random bytes.
    let mut ipk = CanonAeadKey::new();
    crypto.rand()?.fill_bytes(ipk.access_mut());

    let controller_fab_idx = matter.with_state(|state| {
        state
            .fabrics
            .add(
                crypto,
                controller_secret_key_canon.reference(),
                rcac,
                controller_noc,
                icac,
                Some(ipk.reference()),
                ADMIN_VENDOR_ID,
                CONTROLLER_NODE_ID,
            )
            .map(|f| f.fab_idx())
    })?;

    let mut commissioner_buf = [0u8; MAX_CERT_TLV_LEN];
    let mut commissioner = Commissioner::new(
        matter,
        crypto,
        controller_fab_idx,
        &mut noc_generator,
        &mut commissioner_buf,
    );

    let opts = CommissionOptions {
        // rs-matter has no PAA-chain verification path yet; `true` is the only
        // supported mode (a documented gap we accept for the spike + v1).
        allow_test_attestation: true,
        ..CommissionOptions::default()
    };

    let phase1 = {
        let mut commission_fut = pin!(commissioner.commission(
            peer,
            passcode,
            &opts,
            DEVICE_NODE_ID,
            VALID_FOREVER,
        ));
        let mut timeout = pin!(Timer::after(Duration::from_secs(COMMISSION_TIMEOUT_SECS)));
        match select(&mut commission_fut, &mut timeout).await {
            Either::First(r) => r?,
            Either::Second(_) => {
                error!("commission() timed out after {COMMISSION_TIMEOUT_SECS}s");
                return Err(ErrorCode::RxTimeout.into());
            }
        }
    };
    info!(
        "phase 1 ok: device_fabric_index={}, device_node_id=0x{:016x}",
        phase1.fabric_index, phase1.device_node_id,
    );

    info!("=== complete_via_case (phase 2 — CASE + CommissioningComplete) ===");
    {
        let mut case_fut = pin!(commissioner.complete_via_case(peer, &phase1));
        let mut timeout = pin!(Timer::after(Duration::from_secs(COMMISSION_TIMEOUT_SECS)));
        match select(&mut case_fut, &mut timeout).await {
            Either::First(r) => r?,
            Either::Second(_) => {
                error!("complete_via_case() timed out after {COMMISSION_TIMEOUT_SECS}s");
                return Err(ErrorCode::RxTimeout.into());
            }
        }
    }
    info!("phase 2 ok: CASE established, CommissioningComplete acknowledged");

    Ok((controller_fab_idx, phase1.device_node_id))
}

async fn read_vendor_name<C: Crypto>(
    matter: &Matter<'_>,
    crypto: &C,
    fab_idx: NonZeroU8,
    node: u64,
) -> Result<String, Error> {
    let exchange = Exchange::initiate(matter, crypto, fab_idx, node).await?;
    exchange
        .basic_information()
        .vendor_name_read_with(0, |v| v.map(String::from))
        .await?
}

async fn read_product_name<C: Crypto>(
    matter: &Matter<'_>,
    crypto: &C,
    fab_idx: NonZeroU8,
    node: u64,
) -> Result<String, Error> {
    let exchange = Exchange::initiate(matter, crypto, fab_idx, node).await?;
    exchange
        .basic_information()
        .product_name_read_with(0, |v| v.map(String::from))
        .await?
}

/// Read OnOff, toggle, verify it flipped, toggle back. Returns (before, after).
async fn toggle_onoff<C: Crypto>(
    matter: &Matter<'_>,
    crypto: &C,
    fab_idx: NonZeroU8,
    node: u64,
    endpoint: u16,
) -> Result<(bool, bool), Error> {
    let exchange = Exchange::initiate(matter, crypto, fab_idx, node).await?;
    let before = with_timeout(exchange.on_off().on_off_read(endpoint)).await?;

    let exchange = Exchange::initiate(matter, crypto, fab_idx, node).await?;
    with_timeout(exchange.on_off().toggle(endpoint)).await?;

    Timer::after(Duration::from_secs(1)).await;

    let exchange = Exchange::initiate(matter, crypto, fab_idx, node).await?;
    let after = with_timeout(exchange.on_off().on_off_read(endpoint)).await?;

    // Be a good guest: restore the original state.
    let exchange = Exchange::initiate(matter, crypto, fab_idx, node).await?;
    with_timeout(exchange.on_off().toggle(endpoint)).await?;

    Ok((before, after))
}

async fn with_timeout<T>(
    fut: impl core::future::Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    let mut fut = pin!(fut);
    let mut timeout = pin!(Timer::after(Duration::from_secs(IM_TIMEOUT_SECS)));
    match select(&mut fut, &mut timeout).await {
        Either::First(r) => r,
        Either::Second(_) => {
            warn!("IM operation timed out after {IM_TIMEOUT_SECS}s");
            Err(ErrorCode::RxTimeout.into())
        }
    }
}

/// Dual-stack UDP socket on an ephemeral port (from rs-matter's
/// `tests/commissioning.rs`).
fn create_dual_stack_socket() -> Result<async_io::Async<UdpSocket>, Error> {
    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))
        .map_err(|_| ErrorCode::NoNetworkInterface)?;
    socket
        .set_reuse_address(true)
        .map_err(|_| ErrorCode::NoNetworkInterface)?;
    socket
        .set_only_v6(false)
        .map_err(|_| ErrorCode::NoNetworkInterface)?;
    let bind_addr = std::net::SocketAddrV6::new(std::net::Ipv6Addr::UNSPECIFIED, 0, 0, 0);
    socket
        .bind(&bind_addr.into())
        .map_err(|_| ErrorCode::NoNetworkInterface)?;
    let socket: UdpSocket = socket.into();
    async_io::Async::new_nonblocking(socket).map_err(|_| ErrorCode::NoNetworkInterface.into())
}
