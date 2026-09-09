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
    let paths = spool::list_jsonl(spool_dir)?;
    let mut stats = ApplyStats::default();
    for path in paths {
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
        let n = insert_event(&mut tx, ev)?;
        stats.inserted += n;
        if n == 0 {
            // Already in capture.events (re-rsync of the whole spool). Do not
            // re-apply I/U after a later D — lexical jsonl names are not seq order.
            bump_watermark(&mut tx, &ev.src_db, ev.seq)?;
            continue;
        }
        apply_current(&mut tx, ev)
            .with_context(|| format!("current {} {} seq {}", ev.src_db, ev.tbl, ev.seq))?;
        bump_watermark(&mut tx, &ev.src_db, ev.seq)?;
    }
    // Re-rsync skip (n=0) does not re-run D. Heal rows a later D already retired.
    retract_stale_current(&mut tx, &events[0].src_db)?;
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
         SELECT $1, $2, $3, $4, $5, $6
         WHERE NOT EXISTS (
           SELECT 1 FROM capture.events e
            WHERE e.src_db = $1 AND e.tbl = $2 AND e.key = $3
              AND e.op = 'D' AND e.seq > $5
         )
         ON CONFLICT (src_db, tbl, key) DO UPDATE SET
            after = EXCLUDED.after,
            seq = EXCLUDED.seq,
            ts = EXCLUDED.ts
         WHERE capture.current.seq < EXCLUDED.seq",
        &[&ev.src_db, &ev.tbl, &ev.key, &ev.after, &ev.seq, &ev.ts],
    )?;
    Ok(())
}

fn retract_stale_current<C: GenericClient>(tx: &mut C, src_db: &str) -> Result<()> {
    tx.execute(
        "DELETE FROM capture.current c
         WHERE c.src_db = $1 AND EXISTS (
           SELECT 1 FROM capture.events e
            WHERE e.src_db = c.src_db AND e.tbl = c.tbl AND e.key = c.key
              AND e.op = 'D' AND e.seq > c.seq
         )",
        &[&src_db],
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
