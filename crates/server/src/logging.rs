use tracing::Level;
use tracing_subscriber::prelude::*;

use crate::config::Config;

pub fn map_level(name: &str) -> Option<Level> {
    match name {
        "fatal" | "critical" | "error" => Some(Level::ERROR),
        "warning" | "warn" => Some(Level::WARN),
        "notice" | "info" => Some(Level::INFO),
        "debug" => Some(Level::DEBUG),
        "verbose" => Some(Level::TRACE),
        _ => None,
    }
}

pub fn init(config: &Config) {
    let level = map_level(&config.log_level).unwrap_or(Level::INFO);
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(level.into())
        .from_env_lossy();

    let stderr_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

    if let Some(path) = &config.log_file {
        // Plain append for v1; rotation matching the Node server is a later plan.
        let file = std::fs::OpenOptions::new().create(true).append(true).open(path)
            .expect("cannot open --log-file");
        let file_layer = tracing_subscriber::fmt::layer().with_writer(file).with_ansi(false);
        tracing_subscriber::registry().with(filter).with(stderr_layer).with(file_layer).init();
    } else {
        tracing_subscriber::registry().with(filter).with(stderr_layer).init();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_node_server_level_names() {
        use tracing::Level;
        assert_eq!(map_level("fatal"), Some(Level::ERROR));
        assert_eq!(map_level("critical"), Some(Level::ERROR));
        assert_eq!(map_level("error"), Some(Level::ERROR));
        assert_eq!(map_level("warning"), Some(Level::WARN));
        assert_eq!(map_level("warn"), Some(Level::WARN));
        assert_eq!(map_level("notice"), Some(Level::INFO));
        assert_eq!(map_level("info"), Some(Level::INFO));
        assert_eq!(map_level("debug"), Some(Level::DEBUG));
        assert_eq!(map_level("verbose"), Some(Level::TRACE));
        assert_eq!(map_level("nonsense"), None);
    }
}
