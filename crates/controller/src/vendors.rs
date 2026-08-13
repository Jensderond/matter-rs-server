//! Vendor ID -> display name lookup, used by `get_matter_fabrics`.
//! STUB for this task: Task 11 fills in the real CSA vendor table.

pub fn name(_: u16) -> Option<String> { None }
pub fn all() -> &'static [(u16, &'static str)] { &[] }
