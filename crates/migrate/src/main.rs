use clap::Parser;
use std::process::ExitCode;

/// One-shot matter.js -> matter-rs-server fabric migration. Reads a
/// matterjs-server storage directory, self-checks the fabric identity
/// offline, and (with --write) creates a matter-rs-server storage directory
/// serving the same fabric. The source store is never modified.
#[derive(Parser)]
#[command(name = "matter-rs-migrate", version)]
struct Cli {
    /// matter.js store root (e.g. /var/lib/matterjs-server). Opened read-only.
    #[arg(long)]
    from: std::path::PathBuf,
    /// matter-rs-server storage path to create. Refuses to overwrite an
    /// existing fabric (server.json).
    #[arg(long)]
    to: std::path::PathBuf,
    /// Actually write. Without this flag the tool reads, runs every
    /// self-check, prints what it would create, and exits non-zero on any
    /// failed check.
    #[arg(long, default_value_t = false)]
    write: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let opts = matter_rs_migrate::Options { from: cli.from, to: cli.to, write: cli.write };
    match matter_rs_migrate::run(&opts) {
        Ok(report) => {
            println!("{report}");
            if report.ok() { ExitCode::SUCCESS } else { ExitCode::FAILURE }
        }
        Err(e) => {
            eprintln!("error: {e}");
            let mut source = std::error::Error::source(&e);
            while let Some(s) = source {
                eprintln!("  caused by: {s}");
                source = s.source();
            }
            ExitCode::FAILURE
        }
    }
}
