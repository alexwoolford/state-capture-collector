use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use state_capture::apply;
use state_capture::collect::{self, CollectCfg};
use state_capture::snapshot;

#[derive(Parser, Debug)]
#[command(
    name = "state-capture",
    about = "Drain SQLite _outbox to a spool; apply JSONL into local Postgres"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Oracle: drain announced work sqlite `_outbox` into closed JSONL files.
    Collect(CollectArgs),
    /// Mini: load spool JSONL into local Postgres (events + current).
    Apply(ApplyArgs),
}

#[derive(clap::Args, Debug)]
struct CollectArgs {
    #[arg(
        long,
        env = "STATE_CAPTURE_ANNOUNCE_DIR",
        default_value = "/var/lib/state-capture/announce"
    )]
    announce_dir: PathBuf,
    #[arg(
        long,
        env = "STATE_CAPTURE_SPOOL_DIR",
        default_value = "/var/lib/state-capture/spool"
    )]
    spool_dir: PathBuf,
    #[arg(
        long,
        env = "STATE_CAPTURE_SOCK",
        default_value = "/run/state/collect.sock"
    )]
    sock: PathBuf,
    #[arg(long, default_value_t = 60)]
    tick_secs: u64,
    /// Drain every announce file once and exit (timer / tests).
    #[arg(long)]
    once: bool,
    /// Drain `_outbox` under a write lock, then emit `I` events for every
    /// captured table's current rows (re-snapshot). Exits. Does not storm
    /// UPDATE triggers. Blocks writers on that sqlite until commit.
    #[arg(long)]
    snapshot: bool,
    /// With `--snapshot`, only this announce db_name.
    #[arg(long, requires = "snapshot")]
    db: Option<String>,
    /// With `--snapshot --db`, reserve seq above this (Mini watermark may be ahead).
    #[arg(long, requires = "db")]
    min_seq: Option<i64>,
}

#[derive(clap::Args, Debug)]
struct ApplyArgs {
    #[arg(long, env = "STATE_CAPTURE_SPOOL_DIR")]
    spool: Option<PathBuf>,
    #[arg(long)]
    file: Option<PathBuf>,
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    #[arg(long)]
    delete_after: bool,
    #[arg(long)]
    migrate: bool,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    match Cli::parse().cmd {
        Cmd::Collect(a) => {
            let cfg = CollectCfg {
                announce_dir: a.announce_dir,
                spool_dir: a.spool_dir,
                sock: a.sock,
                tick: Duration::from_secs(a.tick_secs),
                snapshot_db: a.db,
                min_seq: a.min_seq,
            };
            if a.snapshot {
                let stats = snapshot::snapshot_all(&cfg)?;
                tracing::info!(batches = stats.len(), "snapshot complete");
                Ok(())
            } else if a.once {
                let stats = collect::drain_all(&cfg)?;
                tracing::info!(batches = stats.len(), "drain complete");
                Ok(())
            } else {
                #[cfg(unix)]
                {
                    collect::serve(&cfg)
                }
                #[cfg(not(unix))]
                {
                    bail!("collect --serve requires unix datagram sockets")
                }
            }
        }
        Cmd::Apply(a) => {
            let mut client = apply::connect(&a.database_url)?;
            if a.migrate {
                apply::migrate(&mut client)?;
            }
            match (a.spool.as_deref(), a.file.as_deref()) {
                (Some(dir), None) => {
                    let s = apply::apply_spool(&mut client, dir, a.delete_after)?;
                    tracing::info!(
                        files = s.files,
                        events = s.events,
                        inserted = s.inserted,
                        "apply complete"
                    );
                }
                (None, Some(file)) => {
                    let s = apply::apply_file(&mut client, file)?;
                    tracing::info!(events = s.events, inserted = s.inserted, "apply complete");
                }
                _ => bail!("apply requires exactly one of --spool or --file"),
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_seq_requires_db() {
        let err = Cli::try_parse_from(["state-capture", "collect", "--snapshot", "--min-seq", "1"])
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("min-seq") && msg.contains("db"),
            "unexpected clap error: {msg}"
        );
    }

    #[test]
    fn min_seq_and_db_require_snapshot() {
        let err = Cli::try_parse_from([
            "state-capture",
            "collect",
            "--db",
            "adsb-trip-journal",
            "--min-seq",
            "1",
        ])
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("snapshot") || msg.contains("db"), "{msg}");
    }
}
