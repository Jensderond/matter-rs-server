use std::sync::Mutex;

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

/// The reload handle for the one `EnvFilter` in the layer stack. `S` is
/// `Registry` because the filter is layered directly onto the registry, before
/// the stderr/file sinks — see [`init`].
type FilterHandle =
    tracing_subscriber::reload::Handle<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>;

/// Live control over the log filter, so a client's `set_loglevel` changes
/// verbosity in place instead of needing a restart.
///
/// Deviation #4: a single `EnvFilter` gates both sinks, so there is no
/// independent file level. `get` therefore reports the file level as equal to
/// the console one (or `null` when there is no `--log-file`), and `set` ignores
/// `file_loglevel`.
pub struct LogControl {
    handle: FilterHandle,
    /// The level name last accepted, spelled the way the client spelled it, so
    /// `get_loglevel` echoes back what a Node-server client would expect
    /// (`"warning"`, not `"WARN"`).
    console: Mutex<String>,
    has_file: bool,
}

impl LogControl {
    /// Read the stored level name, tolerating a poisoned mutex: the only way to
    /// poison it is a panic while swapping a `String`, the value is still
    /// well-formed, and panicking again from a log-level query would be worse.
    fn console_level(&self) -> String {
        match self.console.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn set_console_level(&self, level: String) {
        match self.console.lock() {
            Ok(mut guard) => *guard = level,
            Err(poisoned) => *poisoned.into_inner() = level,
        }
    }
}

impl matter_rs_controller::real::LogLevels for LogControl {
    fn get(&self) -> (String, Option<String>) {
        let console = self.console_level();
        let file = self.has_file.then(|| console.clone());
        (console, file)
    }

    fn set(&self, console: Option<&str>, file: Option<&str>) {
        if let Some(level) = file {
            tracing::warn!(
                "file_loglevel {level:?} ignored: one filter drives both the console and the file"
            );
        }
        let Some(name) = console else { return };
        // An unrecognised name leaves the filter (and the reported level)
        // untouched. The Node server's command has no error channel, so the
        // honest answer is "nothing changed" — quietly dropping to info would
        // make a typo look like a successful downgrade.
        let Some(level) = map_level(name) else {
            tracing::warn!("ignoring unknown log level {name:?}");
            return;
        };
        let filter = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(level.into())
            .from_env_lossy();
        if let Err(e) = self.handle.reload(filter) {
            tracing::error!("could not apply log level {name:?}: {e}");
            return;
        }
        self.set_console_level(name.to_string());
        tracing::info!("log level set to {name}");
    }
}

pub fn init(config: &Config) -> LogControl {
    let level = map_level(&config.log_level).unwrap_or(Level::INFO);
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(level.into())
        .from_env_lossy();
    let (filter_layer, handle) = tracing_subscriber::reload::Layer::new(filter);

    let stderr_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

    // `Option<L>` is itself a `Layer`, so both configurations produce ONE layer
    // stack type. That matters: `FilterHandle`'s `S` has to name the subscriber
    // the reload layer was built against, and a second registry shape would be
    // a second, incompatible handle type.
    let file_layer = config.log_file.as_ref().map(|path| {
        // Plain append for v1; rotation matching the Node server is a later plan.
        let file = std::fs::OpenOptions::new().create(true).append(true).open(path)
            .unwrap_or_else(|e| {
                eprintln!("fatal: cannot open --log-file {}: {e}", path.display());
                std::process::exit(1);
            });
        tracing_subscriber::fmt::layer().with_writer(file).with_ansi(false)
    });

    let has_file = file_layer.is_some();
    tracing_subscriber::registry().with(filter_layer).with(stderr_layer).with(file_layer).init();

    LogControl { handle, console: Mutex::new(config.log_level.clone()), has_file }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matter_rs_controller::real::LogLevels;

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

    /// A `LogControl` can be built without installing a global subscriber, so
    /// these exercise the real reload handle without one test's `init` leaking
    /// into the rest of the binary's tests.
    ///
    /// The layer comes back with it and must stay alive: a `Handle` only holds a
    /// weak reference to the filter the layer owns, so dropping the layer makes
    /// every `reload` fail with `SubscriberGone`. In `init` the layer is moved
    /// into the global subscriber and lives for the whole process.
    fn control(
        start: &str,
        has_file: bool,
    ) -> (LogControl, tracing_subscriber::reload::Layer<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>)
    {
        let filter = tracing_subscriber::EnvFilter::builder()
            .with_default_directive(map_level(start).unwrap_or(Level::INFO).into())
            .parse_lossy("");
        let (layer, handle) = tracing_subscriber::reload::Layer::new(filter);
        (LogControl { handle, console: Mutex::new(start.to_string()), has_file }, layer)
    }

    #[test]
    fn set_console_level_is_reported_back_verbatim() {
        // `set` reads the process environment via `EnvFilter::from_env_lossy`;
        // `config`'s tests write it, so this must hold the crate-wide lock for
        // as long as `set` might run — see `crate::test_env`.
        let _env = crate::test_env::lock();
        let (c, _layer) = control("info", false);
        assert_eq!(c.get(), ("info".to_string(), None));
        c.set(Some("warning"), None);
        // The client's spelling, not tracing's — Node-server clients send and
        // expect "warning".
        assert_eq!(c.get(), ("warning".to_string(), None));
    }

    #[test]
    fn unknown_level_leaves_the_reported_level_unchanged() {
        let (c, _layer) = control("debug", false);
        c.set(Some("chatty"), None);
        assert_eq!(c.get().0, "debug");
    }

    #[test]
    fn file_level_mirrors_the_console_level_only_when_a_log_file_exists() {
        // See `set_console_level_is_reported_back_verbatim`: `set` reads env.
        let _env = crate::test_env::lock();
        let (c, _layer) = control("info", true);
        assert_eq!(c.get(), ("info".to_string(), Some("info".to_string())));
        // Deviation #4: file_loglevel is not independently settable; the
        // console half of the same request still lands.
        c.set(Some("error"), Some("debug"));
        assert_eq!(c.get(), ("error".to_string(), Some("error".to_string())));
    }

    #[test]
    fn file_only_request_changes_nothing() {
        let (c, _layer) = control("info", true);
        c.set(None, Some("verbose"));
        assert_eq!(c.get().0, "info");
    }

    /// The counterpart of the note on `control`: a filter whose layer is gone
    /// cannot be reloaded, and `set` must then leave the reported level alone
    /// rather than claim a change it did not make.
    #[test]
    fn a_failed_reload_does_not_change_the_reported_level() {
        // `set` builds its filter (reading env) before it ever attempts the
        // reload that then fails, so this reads env too — same lock.
        let _env = crate::test_env::lock();
        let (c, layer) = control("info", false);
        drop(layer);
        c.set(Some("debug"), None);
        assert_eq!(c.get().0, "info");
    }
}
