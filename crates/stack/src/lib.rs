//! The ONLY crate that imports rs-matter. Everything runs on one dedicated
//! OS thread (rs-matter futures are !Send); the outside world talks to it
//! through `StackHandle` (Task 16) which implements
//! `matter_rs_controller::stack_api::Stack`.

pub(crate) mod ctx;
pub mod identity;
pub(crate) mod ops;
pub(crate) mod reports;
pub mod tlv_json;
