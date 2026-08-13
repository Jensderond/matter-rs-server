use std::path::PathBuf;

use clap::Parser;

fn default_storage_path() -> PathBuf {
    dirs_next_home().join(".matter_server")
}

fn dirs_next_home() -> PathBuf {
    std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."))
}

/// CLI compatible with matterjs-server's (docs/cli.md there). Out-of-scope
/// flags are accepted + warned + ignored so existing unit files keep starting.
#[derive(Debug, Parser)]
#[command(name = "matter-rs-server", version)]
pub struct Config {
    #[arg(long, env = "PORT", default_value_t = 5580)]
    pub port: u16,

    /// Repeatable. Empty -> bind all interfaces.
    #[arg(long = "listen-address", env = "LISTEN_ADDRESS")]
    pub listen_address: Vec<String>,

    #[arg(long = "storage-path", env = "STORAGE_PATH", default_value_os_t = default_storage_path())]
    pub storage_path: PathBuf,

    #[arg(long = "vendorid", env = "VENDOR_ID", default_value_t = 0xFFF1)]
    pub vendor_id: u16,

    #[arg(long = "fabricid", env = "FABRIC_ID", default_value_t = 1)]
    pub fabric_id: u64,

    /// fatal|critical|error|warning|warn|notice|info|debug|verbose
    #[arg(long = "log-level", env = "LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    #[arg(long = "log-file", env = "LOG_FILE")]
    pub log_file: Option<PathBuf>,

    #[arg(long = "primary-interface", env = "PRIMARY_INTERFACE")]
    pub primary_interface: Option<String>,

    #[arg(long = "default-fabric-label", env = "DEFAULT_FABRIC_LABEL")]
    pub default_fabric_label: Option<String>,

    // ---- accepted-but-ignored (out of scope in v1; see design spec) ----
    #[arg(long = "bluetooth-adapter", env = "BLUETOOTH_ADAPTER", hide = true)]
    pub bluetooth_adapter: Option<u32>,
    #[arg(long = "ble-proxy", env = "BLE_PROXY", hide = true, default_value_t = false)]
    pub ble_proxy: bool,
    #[arg(long = "disable-ota", hide = true, default_value_t = false)]
    pub disable_ota: bool,
    #[arg(long = "ota-provider-dir", hide = true)]
    pub ota_provider_dir: Option<PathBuf>,
    #[arg(long = "disable-dashboard", hide = true, default_value_t = false)]
    pub disable_dashboard: bool,
    #[arg(long = "enable-test-net-dcl", hide = true, default_value_t = false)]
    pub enable_test_net_dcl: bool,
    #[arg(long = "production-mode", hide = true, default_value_t = false)]
    pub production_mode: bool,
}

impl Config {
    /// Log one warning per supplied out-of-scope flag.
    pub fn warn_ignored(&self) {
        let mut ignored: Vec<&str> = Vec::new();
        if self.bluetooth_adapter.is_some() { ignored.push("--bluetooth-adapter"); }
        if self.ble_proxy { ignored.push("--ble-proxy"); }
        if self.disable_ota { ignored.push("--disable-ota"); }
        if self.ota_provider_dir.is_some() { ignored.push("--ota-provider-dir"); }
        if self.disable_dashboard { ignored.push("--disable-dashboard"); }
        if self.enable_test_net_dcl { ignored.push("--enable-test-net-dcl"); }
        if self.production_mode { ignored.push("--production-mode"); }
        for flag in ignored {
            tracing::warn!("{flag} is not supported by matter-rs-server v1 and is ignored");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn defaults_match_node_server() {
        let c = Config::try_parse_from(["matter-rs-server"]).unwrap();
        assert_eq!(c.port, 5580);
        assert_eq!(c.vendor_id, 0xFFF1);
        assert_eq!(c.fabric_id, 1);
        assert_eq!(c.log_level, "info");
        assert!(c.listen_address.is_empty());
        assert!(c.storage_path.ends_with(".matter_server"));
    }

    #[test]
    fn parses_node_server_style_invocation() {
        let c = Config::try_parse_from([
            "matter-rs-server",
            "--storage-path", "/var/lib/matter-rs-server",
            "--port", "5581",
            "--listen-address", "127.0.0.1",
            "--listen-address", "::1",
            "--log-level", "debug",
        ])
        .unwrap();
        assert_eq!(c.port, 5581);
        assert_eq!(c.listen_address, vec!["127.0.0.1", "::1"]);
        assert_eq!(c.storage_path, std::path::PathBuf::from("/var/lib/matter-rs-server"));
    }

    #[test]
    fn legacy_out_of_scope_flags_parse_and_are_ignored() {
        // An existing matterjs-server unit file must never fail to start.
        let c = Config::try_parse_from([
            "matter-rs-server",
            "--bluetooth-adapter", "0",
            "--ble-proxy",
            "--disable-ota",
            "--ota-provider-dir", "/tmp/ota",
            "--disable-dashboard",
            "--enable-test-net-dcl",
            "--production-mode",
        ])
        .unwrap();
        assert!(c.ble_proxy); // captured, warned about at startup, never acted on
    }
}
