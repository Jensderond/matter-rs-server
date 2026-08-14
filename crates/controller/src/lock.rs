//! Poison-tolerant `std::sync::Mutex` access, used for every `Mutex` in this
//! crate.
//!
//! Why not `lock().unwrap()`: every mutex here guards plain data (the node map,
//! the config snapshot, the event history, the fabric-label owner) and none of
//! them has an invariant that a mid-critical-section panic could break in a way
//! a later reader would be fooled by. Poisoning, on the other hand, is
//! permanent and contagious: one panic inside a `Registry` closure would make
//! *every* later command panic on the same lock, turning a single task-level
//! failure into a server that is broken until it is restarted.
//!
//! Same pattern (and same reasoning) as `StackHandle::take_thread` and
//! `LogControl::console_level`.
pub(crate) fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
