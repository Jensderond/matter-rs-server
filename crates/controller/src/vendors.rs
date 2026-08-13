//! Vendor ID -> display name lookup, used by `get_vendor_names` and `get_matter_fabrics`.
//!
//! matterjs-server's `packages/ws-controller/src/data/VendorIDs.ts` carries a
//! ~1245-entry static table (the historical Zigbee Alliance manufacturer-code
//! range plus CSA-assigned Matter vendor IDs), which it merges at runtime with
//! vendor names fetched live from the CSA Distributed Compliance Ledger (DCL)
//! service. We don't have a DCL client here, and porting the full static
//! table verbatim would include legacy low-numbered manufacturer codes (e.g.
//! id 1 "Panasonic") that are not meaningful Matter vendor IDs in practice.
//!
// TODO(plan3): full table — either port VendorIDs.ts in full plus a DCL
// lookup, or curate a complete CSA-assigned Matter vendor ID list.
//!
//! For now this is the minimal seed table from the task brief: real,
//! commonly-seen Matter vendor IDs, sorted by id ascending for binary search.

pub static VENDORS: &[(u16, &str)] = &[
    (4447, "Nanoleaf"),
    (4476, "IKEA of Sweden"),
    (4488, "Yeelight"),
    (4489, "Innr"),
    (4610, "Aqara"),
    (4631, "TP-Link"),
    (4874, "Eve Systems"),
    (4919, "Tuya"),
    (4937, "Apple Home"),
    (4938, "Apple"),
    (4996, "Signify Netherlands B.V."),
    (24582, "Google LLC"),
    (65521, "Test Vendor"),
];

/// Look up a vendor display name by id.
pub fn name(vendor_id: u16) -> Option<&'static str> {
    VENDORS.binary_search_by_key(&vendor_id, |&(id, _)| id)
        .ok()
        .map(|i| VENDORS[i].1)
}

/// The full vendor id -> name table, sorted by id.
pub fn all() -> &'static [(u16, &'static str)] { VENDORS }
