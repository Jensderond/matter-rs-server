//! IM operations, one module per family. Everything here runs on the stack
//! thread and takes `&StackCtx` as its first argument.

pub(crate) mod commission;
pub(crate) mod discovery;
pub(crate) mod fabrics;
pub(crate) mod interact;
pub(crate) mod window;

use rs_matter::transport::network::Address;

/// The root endpoint: the only one that carries the node-wide clusters this
/// crate talks to (`OperationalCredentials`, `AdministratorCommissioning`,
/// `BasicInformation`).
pub(crate) const ROOT_ENDPOINT: u16 = 0;

/// `OperationalCredentials` cluster id.
pub(crate) const OP_CREDS: u32 = 62;

/// `"ip:port"`, the shape both `CommissionOutcome::address` and
/// `DiscoveredDevice::address` are documented to carry.
///
/// Not `format!("{addr}")`: `Display for Address`
/// (`rs-matter-ref/rs-matter/src/transport/network.rs:230`) prefixes the
/// transport — `"UDP 192.168.1.50:5540"` — which would reach the client
/// verbatim. Only the BTP variant, which has no socket address at all and which
/// no path here can produce (mDNS browse and the IP-address target both yield
/// `Udp`), falls back to that spelling.
pub(crate) fn addr_to_string(addr: &Address) -> String {
    match addr {
        Address::Udp(sa) | Address::Tcp(sa) => sa.to_string(),
        other => other.to_string(),
    }
}

/// The host part of an `"ip:port"` string, keeping any IPv6 scope id.
///
/// `"192.168.1.50:5540"` -> `"192.168.1.50"`,
/// `"[fe80::1%2]:5540"` -> `"fe80::1%2"`. This is what `ctx.addrs` stores and
/// what `node_addresses` hands the controller (`"ip"` or `"ip%iface"`, no port).
///
/// One line, because the logic itself lives in `controller::addr` and is shared
/// with `commands::commissioning`, which needs the port half as well. It used to
/// be two independent copies, and that cost two bugs fixed twice each (see the
/// module docs over there). The tests below stayed here: they are the ones that
/// pin the shapes *this* crate feeds it.
pub(crate) fn ip_of(addr: &str) -> String {
    matter_rs_controller::addr::ip_of(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};

    #[test]
    fn address_is_rendered_without_the_transport_prefix() {
        let v4 = Address::Udp(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50)), 5540));
        assert_eq!(addr_to_string(&v4), "192.168.1.50:5540");

        // What `Transport::browse_commissionable` builds for a link-local peer:
        // a scoped IPv6 socket address.
        let v6 = Address::Udp(SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
            5540,
            0,
            2,
        )));
        assert_eq!(addr_to_string(&v6), "[fe80::1%2]:5540");
    }

    #[test]
    fn ipv4_with_port_loses_only_the_port() {
        assert_eq!(ip_of("192.168.1.50:5540"), "192.168.1.50");
    }

    /// The plan's version already stripped brackets first, so this pins existing
    /// behaviour rather than a fix: a link-local address whose scope id must
    /// survive, because it is what `node_addresses` reports as `"ip%iface"`.
    /// (The genuine repair is `unbracketed_ipv6_is_not_split_at_its_last_group`
    /// below.)
    #[test]
    fn bracketed_ipv6_keeps_its_scope_id() {
        assert_eq!(ip_of("[fe80::1%2]:5540"), "fe80::1%2");
        assert_eq!(ip_of("[fe80::1%eth0]:5540"), "fe80::1%eth0");
        assert_eq!(ip_of("[2001:db8::1]:5540"), "2001:db8::1");
        // Bracketed but portless.
        assert_eq!(ip_of("[fe80::1%2]"), "fe80::1%2");
    }

    /// The actual fix over the plan's `ip_of`: a bare `rsplit_once(':')` turned
    /// `"fe80::1%2"` into `"fe80:"` — everything but the last group — and that is
    /// the string `node_addresses` would have reported.
    #[test]
    fn unbracketed_ipv6_is_not_split_at_its_last_group() {
        assert_eq!(ip_of("fe80::1%2"), "fe80::1%2");
        assert_eq!(ip_of("2001:db8::1"), "2001:db8::1");
        assert_eq!(ip_of("::1"), "::1");
    }

    #[test]
    fn a_bare_ipv4_without_a_port_passes_through() {
        assert_eq!(ip_of("192.168.1.50"), "192.168.1.50");
        assert_eq!(ip_of(""), "");
    }
}
