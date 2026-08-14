//! The built-in mDNS responder/browser, ported from `spike/src/mdns.rs` (which
//! in turn adapts rs-matter's `examples/src/common/mdns.rs`, Apache-2.0, Project
//! CHIP Authors). Linux-only paths kept.
//!
//! Three deliberate changes against the spike:
//!
//! 1. the `SPIKE_IFACE` environment variable became an [`Option<&str>`]
//!    parameter, fed from `--primary-interface`;
//! 2. the published hostname is [`HOSTNAME`];
//! 3. `log` became `tracing`.
//!
//! The multicast-join-failure-is-a-warning behaviour is kept verbatim, and so is
//! the reason for it: on a host with IPv6 disabled, or inside a container with an
//! unusual bridge, one of the two joins fails while the other works — and one
//! working family is enough to discover and be discovered.

use std::net::UdpSocket;

use rs_matter::crypto::Crypto;
use rs_matter::error::{Error, ErrorCode};
use rs_matter::transport::network::{Ipv4Addr, Ipv6Addr};
use rs_matter::Matter;

use socket2::{Domain, Protocol, Socket, Type};

/// The hostname published in the A/AAAA records backing our mDNS services.
///
/// Only ever seen by other Matter nodes resolving us, and a fixed string is what
/// makes a packet capture readable.
///
/// **Known limitation:** because it is fixed and `BuiltinMdns` does no
/// name-conflict resolution, two matter-rs-server instances on one LAN segment
/// publish conflicting records for `matter-rs-server.local` and resolution
/// between them becomes a coin flip. Deliberately not repaired here: the plan
/// specifies this exact string and changing it unilaterally would be a silent
/// deviation. The obvious fix, if it ever bites, is to suffix the
/// `compressed_fabric_id` (already carried on `crate::ReadyInfo`), which is
/// unique per controller identity.
const HOSTNAME: &str = "matter-rs-server";

/// The interface addresses mDNS binds and advertises on.
#[derive(Debug)]
struct Iface {
    /// Interface name, for logging only.
    name: String,
    ipv4: std::net::Ipv4Addr,
    ipv6: std::net::Ipv6Addr,
    index: u32,
}

/// Run the built-in mDNS responder until it fails.
///
/// A failure is not fatal to the stack — see the caller in [`crate::runtime`]:
/// without mDNS, discovery and cold-resolve degrade, but already-connected nodes
/// keep working (spike finding 3).
pub(crate) async fn run_builtin_mdns<C: Crypto>(
    matter: &Matter<'_>,
    crypto: C,
    iface: Option<&str>,
) -> Result<(), Error> {
    let all = if_addrs::get_if_addrs().map_err(|e| {
        tracing::error!("enumerating network interfaces failed: {e}");
        Error::from(ErrorCode::StdIoError)
    })?;
    tracing::debug!("available network interfaces: {all:?}");

    let selected = select_interface(&all, iface)?;
    tracing::info!(
        "using network interface {} with {}/{} for mDNS",
        selected.name,
        selected.ipv4,
        selected.ipv6
    );

    let ipv4_addr: Ipv4Addr = selected.ipv4.octets().into();
    let ipv6_addr: Ipv6Addr = selected.ipv6.octets().into();
    let interface = selected.index;

    use rs_matter::transport::network::mdns::builtin::{BuiltinMdns, Host};
    use rs_matter::transport::network::mdns::{
        MDNS_IPV4_BROADCAST_ADDR, MDNS_IPV6_BROADCAST_ADDR, MDNS_SOCKET_DEFAULT_BIND_ADDR,
    };

    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    // Share port 5353 with a system mDNS daemon (avahi, mDNSResponder).
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_only_v6(false)?;
    socket.bind(&MDNS_SOCKET_DEFAULT_BIND_ADDR.into())?;
    let socket = async_io::Async::<UdpSocket>::new_nonblocking(socket.into())?;

    // Tolerate partial multicast setup (e.g. no IPv6 on the interface, or an odd
    // container/VM network): one working family is enough to discover.
    if let Err(e) = socket
        .get_ref()
        .join_multicast_v6(&MDNS_IPV6_BROADCAST_ADDR, interface)
    {
        tracing::warn!("join_multicast_v6 on ifindex {interface} failed: {e}");
    }
    if let Err(e) = socket
        .get_ref()
        .join_multicast_v4(&MDNS_IPV4_BROADCAST_ADDR, &ipv4_addr)
    {
        tracing::warn!("join_multicast_v4 on {ipv4_addr} failed: {e}");
    }

    BuiltinMdns::new()
        .run(
            &socket,
            &socket,
            &Host { hostname: HOSTNAME, ip: ipv4_addr, ipv6: ipv6_addr },
            Some(ipv4_addr),
            Some(interface),
            matter,
            crypto,
        )
        .await
}

/// Whether `ip` is in `fe80::/10`.
///
/// Hand-rolled because `std::net::Ipv6Addr::is_unicast_link_local` is still
/// unstable, and this crate builds on stable.
fn is_unicast_link_local(ip: std::net::Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xffc0) == 0xfe80
}

/// Pick the interface to bind mDNS to.
///
/// `forced` (from `--primary-interface`) short-circuits the heuristic and is a
/// hard error when it does not match: silently falling back would advertise the
/// wrong addresses, which is exactly what the operator passed the flag to
/// prevent.
///
/// Otherwise: an interface that has *both* an IPv6 and a non-loopback IPv4
/// address, preferring link-local IPv6 — that combination is the strongest
/// available signal for "the real LAN interface", since docker/virtual
/// interfaces are typically IPv4-only. The last resort is any non-loopback
/// `eth*`/`eno*`, which gives a usable IPv4-only advertisement.
///
/// Takes the interface list rather than calling `get_if_addrs` so the choice is
/// testable without a network.
fn select_interface(all: &[if_addrs::Interface], forced: Option<&str>) -> Result<Iface, Error> {
    if let Some(forced) = forced {
        let v4 = all.iter().find_map(|ia| match ia.addr {
            if_addrs::IfAddr::V4(ref v4) if ia.name == forced => Some((v4.ip, ia.index.unwrap_or(0))),
            _ => None,
        });
        let v6 = all.iter().find_map(|ia| match ia.addr {
            if_addrs::IfAddr::V6(ref v6) if ia.name == forced => Some(v6.ip),
            _ => None,
        });
        let (ipv4, index) = v4.ok_or_else(|| {
            tracing::error!("--primary-interface {forced} not found or has no IPv4 address");
            Error::from(ErrorCode::StdIoError)
        })?;
        return Ok(Iface {
            name: forced.to_string(),
            ipv4,
            ipv6: v6.unwrap_or(std::net::Ipv6Addr::UNSPECIFIED),
            index,
        });
    }

    let find_ipv6_candidate = |ipv6_filter: fn(std::net::Ipv6Addr) -> bool| {
        all.iter()
            .filter(|ia| !ia.is_loopback())
            .filter_map(|ia| match ia.addr {
                if_addrs::IfAddr::V6(ref v6) if ipv6_filter(v6.ip) => {
                    Some((ia.name.clone(), v6.ip, ia.index.unwrap_or(0)))
                }
                _ => None,
            })
            .find_map(|(iname, ipv6, index)| {
                all.iter()
                    .filter(|ia2| ia2.name == iname)
                    .find_map(|ia2| match ia2.addr {
                        if_addrs::IfAddr::V4(ref v4) => {
                            Some(Iface { name: iname.clone(), ipv4: v4.ip, ipv6, index })
                        }
                        _ => None,
                    })
            })
    };

    let find_fallback_candidate = || {
        all.iter()
            .filter(|ia| !ia.is_loopback())
            .filter(|ia| ia.name.starts_with("eth") || ia.name.starts_with("eno"))
            .map(|ia| match ia.addr {
                if_addrs::IfAddr::V4(ref v4) => Iface {
                    name: ia.name.clone(),
                    ipv4: v4.ip,
                    ipv6: std::net::Ipv6Addr::UNSPECIFIED,
                    index: ia.index.unwrap_or(0),
                },
                if_addrs::IfAddr::V6(ref v6) => Iface {
                    name: ia.name.clone(),
                    ipv4: std::net::Ipv4Addr::UNSPECIFIED,
                    ipv6: v6.ip,
                    index: ia.index.unwrap_or(0),
                },
            })
            .next()
    };

    find_ipv6_candidate(is_unicast_link_local)
        .or_else(|| find_ipv6_candidate(|_| true))
        .or_else(|| {
            tracing::warn!("no network interface with a suitable IPv6 address found");
            find_fallback_candidate()
        })
        .ok_or_else(|| {
            tracing::error!("cannot find a network interface suitable for mDNS");
            Error::from(ErrorCode::StdIoError)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use if_addrs::{IfAddr, IfOperStatus, Interface, Ifv4Addr, Ifv6Addr};
    use std::net::{Ipv4Addr as V4, Ipv6Addr as V6};

    fn v4(name: &str, ip: V4, index: u32) -> Interface {
        Interface {
            name: name.into(),
            addr: IfAddr::V4(Ifv4Addr {
                ip,
                netmask: V4::new(255, 255, 255, 0),
                prefixlen: 24,
                broadcast: None,
            }),
            index: Some(index),
            oper_status: IfOperStatus::Up,
            is_p2p: false,
        }
    }

    fn v6(name: &str, ip: V6, index: u32) -> Interface {
        Interface {
            name: name.into(),
            addr: IfAddr::V6(Ifv6Addr {
                ip,
                netmask: V6::UNSPECIFIED,
                prefixlen: 64,
                broadcast: None,
            }),
            index: Some(index),
            oper_status: IfOperStatus::Up,
            is_p2p: false,
        }
    }

    #[test]
    fn link_local_detection_covers_the_whole_fe80_10_block() {
        assert!(is_unicast_link_local("fe80::1".parse().expect("literal")));
        assert!(is_unicast_link_local("febf::1".parse().expect("literal")));
        assert!(!is_unicast_link_local("fec0::1".parse().expect("literal")));
        assert!(!is_unicast_link_local("2001:db8::1".parse().expect("literal")));
        assert!(!is_unicast_link_local(V6::LOCALHOST));
    }

    /// The heuristic's whole point: a docker bridge with only IPv4 must lose to
    /// the interface that also has a link-local IPv6 address.
    #[test]
    fn a_dual_stack_interface_beats_an_ipv4_only_bridge() {
        let all = [
            v4("docker0", V4::new(172, 17, 0, 1), 3),
            v4("eth0", V4::new(192, 168, 1, 10), 2),
            v6("eth0", "fe80::1".parse().expect("literal"), 2),
        ];
        let picked = select_interface(&all, None).expect("a dual-stack interface exists");
        assert_eq!(picked.name, "eth0");
        assert_eq!(picked.ipv4, V4::new(192, 168, 1, 10));
        assert_eq!(picked.index, 2);
    }

    /// A global IPv6 address is second choice, not no choice.
    #[test]
    fn a_global_ipv6_interface_is_used_when_no_link_local_one_exists() {
        let all = [
            v4("eth0", V4::new(192, 168, 1, 10), 2),
            v6("eth0", "2001:db8::1".parse().expect("literal"), 2),
        ];
        let picked = select_interface(&all, None).expect("a dual-stack interface exists");
        assert_eq!(picked.ipv6, "2001:db8::1".parse::<V6>().expect("literal"));
    }

    /// IPv4-only hosts still have to be able to advertise.
    #[test]
    fn an_ipv4_only_ethernet_interface_is_the_last_resort() {
        let all = [
            v4("lo", V4::LOCALHOST, 1),
            v4("docker0", V4::new(172, 17, 0, 1), 3),
            v4("eth0", V4::new(192, 168, 1, 10), 2),
        ];
        let picked = select_interface(&all, None).expect("eth0 is a fallback candidate");
        assert_eq!(picked.name, "eth0");
        assert_eq!(picked.ipv6, V6::UNSPECIFIED);
    }

    #[test]
    fn nothing_usable_is_an_error_not_a_loopback_advertisement() {
        let all = [v4("lo", V4::LOCALHOST, 1)];
        let e = select_interface(&all, None).expect_err("loopback is not usable for mDNS");
        assert_eq!(e.code(), ErrorCode::StdIoError);
    }

    #[test]
    fn primary_interface_overrides_the_heuristic() {
        let all = [
            v4("eth0", V4::new(192, 168, 1, 10), 2),
            v6("eth0", "fe80::1".parse().expect("literal"), 2),
            v4("wlan0", V4::new(10, 0, 0, 5), 4),
        ];
        // wlan0 has no IPv6 at all, so the heuristic would never pick it.
        let picked = select_interface(&all, Some("wlan0")).expect("wlan0 has an IPv4 address");
        assert_eq!(picked.name, "wlan0");
        assert_eq!(picked.ipv4, V4::new(10, 0, 0, 5));
        assert_eq!(picked.ipv6, V6::UNSPECIFIED);
        assert_eq!(picked.index, 4);
    }

    /// Explicit beats convenient: a misspelled `--primary-interface` must fail
    /// loudly rather than silently advertise a different interface's addresses.
    #[test]
    fn an_unknown_primary_interface_is_an_error_not_a_fallback() {
        let all = [
            v4("eth0", V4::new(192, 168, 1, 10), 2),
            v6("eth0", "fe80::1".parse().expect("literal"), 2),
        ];
        let e = select_interface(&all, Some("eth1")).expect_err("eth1 does not exist");
        assert_eq!(e.code(), ErrorCode::StdIoError);
    }

    /// An interface that exists but is IPv6-only cannot carry the A record the
    /// `Host` needs, so it is treated as "not found" too.
    #[test]
    fn a_primary_interface_without_ipv4_is_rejected() {
        let all = [v6("eth0", "fe80::1".parse().expect("literal"), 2)];
        select_interface(&all, Some("eth0")).expect_err("no IPv4 address to advertise");
    }
}
