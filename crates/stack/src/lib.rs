//! The ONLY crate that imports rs-matter. Everything runs on one dedicated
//! OS thread (rs-matter futures are !Send); the outside world talks to it
//! through `StackHandle` (Task 16) which implements
//! `matter_rs_controller::stack_api::Stack`.

pub mod identity;
pub mod tlv_json;
