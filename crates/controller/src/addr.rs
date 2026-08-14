//! The one implementation of `"ip:port"` splitting, shared by `controller` and
//! `stack`.
//!
//! It lives in `controller` because the crate dependency runs `stack ->
//! controller`: `stack` already imports `controller::storage`, so it can import
//! this too. (An older comment in `commands::commissioning` claimed the
//! duplication was forced because "controller cannot depend on stack" — true,
//! but the wrong direction to reason from.)
//!
//! Consolidated deliberately: this exact logic has produced two bugs so far —
//! brackets surviving into `NodeRecord::addresses` (so a client got the
//! unclosed literal `"[fe80::1"` and `ping_node` handed `ping6` something it
//! cannot parse), and an unbracketed IPv6 literal being split at its last group
//! (`"fe80::1%2"` -> `"fe80:"`). Both had to be fixed twice, once per copy.

/// Splits `"ip:port"` into `(ip, Some(port))`, unwrapping the brackets an IPv6
/// socket address carries; passes through unchanged (`None` port) when there is
/// no port to strip. Any IPv6 scope id is kept (`"fe80::1%eth0"`).
///
/// The bracket branch has to come first. rs-matter renders an IPv6 peer as
/// `"[fe80::1%14]:5540"`, and a bare `rsplit_once(':')` on that leaves the
/// brackets on the host — while on an *unbracketed* IPv6 literal it would cut
/// off the last group.
pub fn split_ip_port(address: &str) -> (String, Option<u16>) {
    if let Some(rest) = address.strip_prefix('[') {
        return match rest.split_once(']') {
            // The host is exactly what the brackets held, whether or not a
            // `:port` follows.
            Some((host, tail)) => {
                (host.to_string(), tail.strip_prefix(':').and_then(|p| p.parse().ok()))
            }
            // Unterminated bracket: not something rs-matter produces, but
            // returning the remainder beats returning the `[`.
            None => (rest.to_string(), None),
        };
    }
    // Unbracketed. More than one colon means an IPv6 literal written without
    // brackets, which therefore cannot carry a port — take it whole.
    if address.matches(':').count() > 1 {
        return (address.to_string(), None);
    }
    match address.rsplit_once(':') {
        Some((ip, port)) => (ip.to_string(), port.parse().ok()),
        None => (address.to_string(), None),
    }
}

/// Just the host half of [`split_ip_port`], for callers that never want the
/// port: `"192.168.1.50:5540"` -> `"192.168.1.50"`,
/// `"[fe80::1%2]:5540"` -> `"fe80::1%2"`.
pub fn ip_of(address: &str) -> String {
    split_ip_port(address).0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every shape rs-matter produces, plus the two malformed ones that must
    /// still never yield a stray `[`.
    #[test]
    fn split_ip_port_handles_every_address_form_rs_matter_produces() {
        let cases: &[(&str, (&str, Option<u16>))] = &[
            ("192.168.1.60:5540", ("192.168.1.60", Some(5540))),
            ("192.168.1.60", ("192.168.1.60", None)),
            ("[fe80::1%14]:5540", ("fe80::1%14", Some(5540))),
            ("[fd12::5]:5540", ("fd12::5", Some(5540))),
            ("[fd12::5]", ("fd12::5", None)),
            // Not shapes rs-matter emits, but they must not produce a `[` either.
            ("[fd12::5", ("fd12::5", None)),
            ("fe80::1", ("fe80::1", None)),
            ("192.168.1.60:notaport", ("192.168.1.60", None)),
        ];
        for (input, (ip, port)) in cases {
            let got = split_ip_port(input);
            assert_eq!((got.0.as_str(), got.1), (*ip, *port), "for {input:?}");
            assert!(!got.0.contains('['), "for {input:?}");
        }
    }

    #[test]
    fn ip_of_is_the_host_half() {
        assert_eq!(ip_of("192.168.1.50:5540"), "192.168.1.50");
        assert_eq!(ip_of("[fe80::1%2]:5540"), "fe80::1%2");
        assert_eq!(ip_of("fe80::1%2"), "fe80::1%2");
        assert_eq!(ip_of(""), "");
    }
}
