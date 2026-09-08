//! Load JSONL into Postgres `capture.events` and `capture.current`.
//!
//! No per-utility table names. A fifth source needs no applyer change.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};
use postgres::{Client, GenericClient, NoTls};

use crate::event::Event;
use crate::spool;

pub const CAPTURE_SQL: &str = include_str!("../sql/001_capture.sql");

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ApplyStats {
    pub files: usize,
    pub events: usize,
    pub inserted: usize,
}

pub fn connect(database_url: &str) -> Result<Client> {
    Client::connect(database_url, NoTls).context("connect postgres")
}

pub fn migrate(client: &mut Client) -> Result<()> {
    client
        .batch_execute(CAPTURE_SQL)
        .context("apply capture schema")?;
    Ok(())
}

pub fn apply_spool(
    client: &mut Client,
    spool_dir: &Path,
    delete_after: bool,
) -> Result<ApplyStats> {
    let mut stats = ApplyStats::default();
    for path in spool::list_jsonl(spool_dir)? {
        let s = apply_file(client, &path)?;
        stats.files += 1;
        stats.events += s.events;
        stats.inserted += s.inserted;
        if delete_after {
            fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
        }
    }
    Ok(stats)
}

pub fn apply_file(client: &mut Client, path: &Path) -> Result<ApplyStats> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let ev: Event = serde_json::from_str(&line)
            .with_context(|| format!("{}:{}: parse event", path.display(), i + 1))?;
        events.push(ev);
    }
    apply_events(client, &events)
}

pub fn apply_events(client: &mut Client, events: &[Event]) -> Result<ApplyStats> {
    let mut stats = ApplyStats {
        files: 0,
        events: events.len(),
        inserted: 0,
    };
    if events.is_empty() {
        return Ok(stats);
    }
    let mut tx = client.transaction()?;
    for ev in events {
        stats.inserted += insert_event(&mut tx, ev)?;
        apply_current(&mut tx, ev)
            .with_context(|| format!("current {} {} seq {}", ev.src_db, ev.tbl, ev.seq))?;
        bump_watermark(&mut tx, &ev.src_db, ev.seq)?;
    }
    tx.commit()?;
    Ok(stats)
}

fn insert_event<C: GenericClient>(tx: &mut C, ev: &Event) -> Result<usize> {
    let n = tx.execute(
        "INSERT INTO capture.events (src_db, seq, tbl, op, ts, key, before, after)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (src_db, seq) DO NOTHING",
        &[
            &ev.src_db, &ev.seq, &ev.tbl, &ev.op, &ev.ts, &ev.key, &ev.before, &ev.after,
        ],
    )?;
    Ok(n as usize)
}

fn apply_current<C: GenericClient>(tx: &mut C, ev: &Event) -> Result<()> {
    if ev.op == "D" {
        tx.execute(
            "DELETE FROM capture.current WHERE src_db = $1 AND tbl = $2 AND key = $3",
            &[&ev.src_db, &ev.tbl, &ev.key],
        )?;
        return Ok(());
    }
    if ev.after.is_none() {
        return Ok(());
    }
    tx.execute(
        "INSERT INTO capture.current (src_db, tbl, key, after, seq, ts)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (src_db, tbl, key) DO UPDATE SET
            after = EXCLUDED.after,
            seq = EXCLUDED.seq,
            ts = EXCLUDED.ts
         WHERE capture.current.seq < EXCLUDED.seq",
        &[&ev.src_db, &ev.tbl, &ev.key, &ev.after, &ev.seq, &ev.ts],
    )?;
    Ok(())
}

fn bump_watermark<C: GenericClient>(tx: &mut C, src_db: &str, seq: i64) -> Result<()> {
    tx.execute(
        "INSERT INTO capture.watermarks (src_db, last_seq) VALUES ($1, $2)
         ON CONFLICT (src_db) DO UPDATE SET last_seq = GREATEST(capture.watermarks.last_seq, EXCLUDED.last_seq)",
        &[&src_db, &seq],
    )?;
    Ok(())
}
