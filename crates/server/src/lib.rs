pub mod config;
pub mod http;
pub mod logging;
pub mod ws;

/// Test-only, crate-wide lock over process-environment access.
///
/// `std::env::set_var`/`remove_var` are `unsafe` (edition 2024+) because
/// mutating the environment while *any other thread* touches it at all — not
/// just the same variable name — can corrupt the process's `environ` table at
/// the libc level. The hazard is concurrent access to the table, not a
/// collision on a specific key.
///
/// Cargo runs every `#[test]` fn in this crate's one `--lib` binary as threads
/// in a single process. `config`'s tests write env vars directly; `logging`'s
/// tests read the environment indirectly via
/// `tracing_subscriber::EnvFilter::from_env_lossy` (reached through
/// `LogControl::set`). A lock local to either module only serializes that
/// module's own tests against each other and does nothing to stop one
/// module's write racing the other's read — hence this lock lives here, where
/// both modules' test code can reach it, rather than in `config`.
#[cfg(test)]
pub(crate) mod test_env {
    use std::sync::{Mutex, MutexGuard};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Acquire the lock, tolerating poison: a panic while holding it does not
    /// leave the environment itself in a bad state, only possibly some vars
    /// still set, which `config`'s `with_clean_env` clears on its next call
    /// regardless.
    pub(crate) fn lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }
}
