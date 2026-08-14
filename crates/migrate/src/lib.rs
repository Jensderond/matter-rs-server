//! One-shot matter.js -> matter-rs-server fabric migration. See
//! docs/superpowers/specs/2026-08-14-matterjs-fabric-migration-design.md.

pub mod checks;
pub mod convert;
pub mod decode;
pub mod jsdb;

use std::io;
use std::path::{Path, PathBuf};

use crate::checks::CheckOutcome;
use crate::convert::FabricIndexSource;
use crate::jsdb::JsDb;
use matter_rs_controller::stack_api::StackError;
use matter_rs_controller::storage::Storage;
use matter_rs_stack::migration::{
    identity_from_preserved_ca, rcac_public_key, rcac_serial_is_der_canonical,
};

/// The exact, verbatim explanation the report gives when the migrated RCAC's
/// serial is not canonical DER. A migrated fabric legitimately has one: every
/// device already trusts this exact root, so nothing except commissioning new
/// matter.js-based test devices is affected.
const RCAC_SERIAL_NOTE: &str = "the migrated RCAC's serial number is not canonical DER; the server \
will warn about it at every boot. For a migrated fabric this is EXPECTED and HARMLESS — every \
commissioned device already trusts this exact root. It limits nothing except commissioning new \
matter.js-based test devices.";

pub struct Options {
    pub from: PathBuf,
    pub to: PathBuf,
    pub write: bool,
}

/// Nothing here is sensitive: no key material, just identifiers and check
/// results, so a derived `Debug` (needed by `unwrap_err` in tests) is fine.
#[derive(Debug)]
pub struct Report {
    pub namespace: String,
    pub fabric_id: u64,
    pub compressed_fabric_id: u64,
    pub vendor_id: u16,
    pub controller_node_id: u64,
    pub fabric_label: String,
    pub next_node_id: u64,
    pub nodes: Vec<(u64, u8, FabricIndexSource)>, // (node id, resolved index, how)
    pub checks: Vec<CheckOutcome>,
    pub rcac_serial_note: Option<String>,
    pub ignored_python_leftovers: Vec<String>,
    pub wrote: Option<PathBuf>, // Some(to) only after a successful --write
}

impl Report {
    /// Every self-check passed. The caller (the CLI) turns this into the exit
    /// code; `run` itself never fails just because a check failed.
    pub fn ok(&self) -> bool {
        self.checks.iter().all(|c| c.passed)
    }
}

impl std::fmt::Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "matter.js namespace: {}", self.namespace)?;
        writeln!(f)?;
        writeln!(f, "fabric id:            {}", self.fabric_id)?;
        writeln!(f, "compressed fabric id: {:016x}", self.compressed_fabric_id)?;
        writeln!(f, "vendor id:            {:#06x}", self.vendor_id)?;
        writeln!(f, "controller node id:   {}", self.controller_node_id)?;
        writeln!(f, "fabric label:         {}", self.fabric_label)?;
        writeln!(f, "next node id:         {}", self.next_node_id)?;
        writeln!(f)?;
        if self.nodes.is_empty() {
            writeln!(f, "nodes: none")?;
        } else {
            writeln!(f, "nodes:")?;
            for (id, index, how) in &self.nodes {
                let how = match how {
                    FabricIndexSource::MatchedByRootPublicKey => "matched by root public key".to_string(),
                    FabricIndexSource::FallbackZero(reason) => format!("FALLBACK 0 — {reason}"),
                };
                writeln!(f, "  node {id}: fabric index {index} — {how}")?;
            }
        }
        writeln!(f)?;
        for c in &self.checks {
            let status = if c.passed { "ok" } else { "FAILED" };
            writeln!(f, "{status} {} — {}", c.name, c.detail)?;
        }
        if let Some(note) = &self.rcac_serial_note {
            writeln!(f)?;
            writeln!(f, "{note}")?;
        }
        if !self.ignored_python_leftovers.is_empty() {
            writeln!(f)?;
            writeln!(
                f,
                "ignored python-server leftovers at the top level of --from (not read, not used as source of truth): {}",
                self.ignored_python_leftovers.join(", ")
            )?;
        }
        writeln!(f)?;
        match &self.wrote {
            Some(path) => write!(f, "wrote {}", path.display()),
            None => write!(f, "dry run — nothing written (pass --write to migrate)"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    #[error(transparent)]
    Jsdb(#[from] crate::jsdb::JsdbError),
    #[error(transparent)]
    Convert(#[from] crate::convert::ConvertError),
    #[error("{0}")]
    Stack(String), // StackError flattened: "<kind>: <message>"
    #[error("--from and --to are the same directory")]
    SamePath,
    #[error("writing {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// `StackError` has no `Display`/`Error` impl by design (see
/// `matter_rs_stack::migration`'s header); flatten it here rather than at
/// every call site.
fn stack_err(e: StackError) -> MigrateError {
    MigrateError::Stack(format!("{:?}: {}", e.kind, e.message))
}

fn is_lowercase_hex(c: char) -> bool {
    c.is_ascii_digit() || ('a'..='f').contains(&c)
}

/// A 16-lowercase-hex-character `*.json` file name (the python CHIP server's
/// per-fabric compressed-fabric-id file naming).
fn is_hex16_json(name: &str) -> bool {
    match name.strip_suffix(".json") {
        Some(stem) => stem.len() == 16 && stem.chars().all(is_lowercase_hex),
        None => false,
    }
}

/// Scan the top level of `--from` (never recursing) for artifacts left behind
/// by the python CHIP server, which a reader might otherwise mistake for the
/// source of truth: `chip_*.ini` files, `certificates/` and `credentials/`
/// directories, and 16-lowercase-hex `*.json` files. Names only — their
/// contents are never read.
fn scan_python_leftovers(from: &Path) -> Vec<String> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(from) else { return found };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Ok(file_type) = entry.file_type() else { continue };
        if file_type.is_dir() {
            if name == "certificates" || name == "credentials" {
                found.push(name);
            }
        } else if file_type.is_file()
            && ((name.starts_with("chip_") && name.ends_with(".ini")) || is_hex16_json(&name))
        {
            found.push(name);
        }
    }
    found.sort();
    found
}

/// The whole tool, read-only up to (and including, on a check failure or a
/// dry run) the point where a `Report` is built. See the design's "run()
/// flow" for the exact step order this follows.
pub fn run(opts: &Options) -> Result<Report, MigrateError> {
    // Step 1: refuse to migrate a store into itself.
    if opts.from == opts.to {
        return Err(MigrateError::SamePath);
    }
    if opts.to.exists() {
        if let (Ok(from_canon), Ok(to_canon)) = (opts.from.canonicalize(), opts.to.canonicalize()) {
            if from_canon == to_canon {
                return Err(MigrateError::SamePath);
            }
        }
    }

    // Step 2-3: read the source store and its fabric identity.
    let (db, namespace) = JsDb::open_store(&opts.from)?;
    let source = crate::convert::read_source_fabric(&db)?;

    // Step 4-5: the root public key (also validates the RCAC is real Matter
    // TLV) and the minted identity, from the preserved CA.
    let root_public_key = rcac_public_key(&source.rcac_tlv).map_err(stack_err)?;
    let identity = identity_from_preserved_ca(
        &source.ca_private_key,
        &source.rcac_tlv,
        source.fabric_id,
        source.vendor_id,
        source.controller_node_id,
        &source.ipk_epoch_key,
    )
    .map_err(stack_err)?;

    // Step 6: the planned nodes and target config.
    let nodes = crate::convert::plan_nodes(&db, &root_public_key)?;
    let config = crate::convert::config_from(&source, &nodes);

    // Step 7: the five self-checks, always all five.
    let outcomes = checks::run_all(&identity, &source, &config, &nodes);
    let all_passed = outcomes.iter().all(|c| c.passed);

    // Step 8: the RCAC-serial note, verbatim, when the serial is not
    // canonical DER.
    let rcac_serial_note = if rcac_serial_is_der_canonical(&source.rcac_tlv).map_err(stack_err)? {
        None
    } else {
        Some(RCAC_SERIAL_NOTE.to_string())
    };

    // Step 9: what to ignore, named so a reader is not misled by it.
    let ignored_python_leftovers = scan_python_leftovers(&opts.from);

    let node_rows: Vec<(u64, u8, FabricIndexSource)> = nodes
        .iter()
        .map(|n| (n.record.node_id, n.record.device_fabric_index, n.fabric_index.clone()))
        .collect();

    let mut report = Report {
        namespace,
        fabric_id: source.fabric_id,
        compressed_fabric_id: identity.compressed_fabric_id,
        vendor_id: source.vendor_id,
        controller_node_id: identity.controller_node_id,
        fabric_label: config.fabric_label.clone(),
        next_node_id: config.next_node_id,
        nodes: node_rows,
        checks: outcomes,
        rcac_serial_note,
        ignored_python_leftovers,
        wrote: None,
    };

    // Step 10: a failed check writes nothing, but is still a Report, not an
    // error — the caller decides the exit code from `report.ok()`.
    if !all_passed || !opts.write {
        return Ok(report);
    }

    // Step 11: `--write`, all checks passed. `create_identity` first — its
    // refusal to overwrite an existing server.json is the only overwrite
    // guard we have or need.
    let storage = Storage::open(&opts.to).map_err(|source| MigrateError::Write { path: opts.to.clone(), source })?;
    storage
        .create_identity(&identity)
        .map_err(|source| MigrateError::Write { path: opts.to.join("server.json"), source })?;
    storage
        .save_config(&config)
        .map_err(|source| MigrateError::Write { path: opts.to.join("config.json"), source })?;
    for plan in &nodes {
        storage.save_node(&plan.record).map_err(|source| MigrateError::Write {
            path: opts.to.join("nodes").join(format!("{}.json", plan.record.node_id)),
            source,
        })?;
    }

    // The write half of self-check 4: what actually landed on disk matches
    // what we planned to write.
    let written = storage.load_nodes().len();
    if written != nodes.len() {
        return Err(MigrateError::Write {
            path: opts.to.join("nodes"),
            source: io::Error::other(format!(
                "planned {} node file(s) but found {written} after writing",
                nodes.len()
            )),
        });
    }

    // Step 12.
    report.wrote = Some(opts.to.clone());
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_from_and_to_are_refused() {
        let d = tempfile::tempdir().unwrap();
        let opts = Options { from: d.path().to_path_buf(), to: d.path().to_path_buf(), write: false };
        assert!(matches!(run(&opts), Err(MigrateError::SamePath)));
    }

    #[test]
    fn a_missing_source_store_is_a_named_error_not_a_panic() {
        let d = tempfile::tempdir().unwrap();
        let opts = Options {
            from: d.path().join("does-not-exist"),
            to: d.path().join("out"),
            write: false,
        };
        let err = run(&opts).unwrap_err();
        assert!(err.to_string().contains("does-not-exist") || matches!(err, MigrateError::Jsdb(_)), "{err}");
        assert!(!d.path().join("out").exists(), "dry-run pathing must not create the destination");
    }
}
