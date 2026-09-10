//! Drain `_outbox` from announced work sqlite files into closed JSONL spool files.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;

use crate::announce;
use crate::event::{validate_db_name, Event};
use crate::spool;

pub struct CollectCfg {
    pub announce_dir: std::path::PathBuf,
    pub spool_dir: std::path::PathBuf,
    pub sock: std::path::PathBuf,
    pub tick: Duration,
    /// Snapshot only this announce `db_name` (all announced dbs when None).
    pub snapshot_db: Option<String>,
    /// Floor for reserved `_outbox` seq (Mini watermark may be ahead of sqlite).
    pub min_seq: Option<i64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DrainStats {
    pub src_db: String,
    pub rows: usize,
    pub seq_lo: Option<i64>,
    pub seq_hi: Option<i64>,
}

pub fn drain_all(cfg: &CollectCfg) -> Result<Vec<DrainStats>> {
    let announced = announce::load_dir(&cfg.announce_dir)?;
    let mut stats = Vec::new();
    let mut failed = Vec::new();
    for a in announced {
        match drain_named(cfg, &a.db_name) {
            Ok(Some(s)) => stats.push(s),
            Ok(None) => {}
            Err(e) => {
                tracing::error!(db = %a.db_name, error = %e, "drain failed");
                failed.push(a.db_name);
            }
        }
    }
    if !failed.is_empty() {
        anyhow::bail!("drain failed for: {}", failed.join(", "));
    }
    Ok(stats)
}

pub fn drain_named(cfg: &CollectCfg, db_name: &str) -> Result<Option<DrainStats>> {
    validate_db_name(db_name)?;
    let Some(a) = announce::load_named(&cfg.announce_dir, db_name)? else {
        tracing::warn!(db = %db_name, "nudge for unknown db_name (no announce file)");
        return Ok(None);
    };
    drain_sqlite(cfg, &a.db_name, Path::new(&a.sqlite_path))
}

pub fn drain_sqlite(
    cfg: &CollectCfg,
    src_db: &str,
    sqlite_path: &Path,
) -> Result<Option<DrainStats>> {
    validate_db_name(src_db)?;
    if !sqlite_path.is_file() {
        anyhow::bail!("sqlite missing: {}", sqlite_path.display());
    }
    let conn =
        Connection::open(sqlite_path).with_context(|| format!("open {}", sqlite_path.display()))?;
    conn.busy_timeout(Duration::from_millis(5_000))?;
    if !has_outbox(&conn)? {
        tracing::warn!(db = %src_db, path = %sqlite_path.display(), "no _outbox table");
        return Ok(None);
    }
    drain_conn(cfg, src_db, &conn)
}

pub(crate) fn has_outbox(conn: &Connection) -> Result<bool> {
    let exists: Option<String> = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_outbox'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    Ok(exists.is_some())
}

/// Drain `_outbox` on an already-open connection (caller holds any write lock).
pub(crate) fn drain_conn(
    cfg: &CollectCfg,
    src_db: &str,
    conn: &Connection,
) -> Result<Option<DrainStats>> {
    let mut total = 0usize;
    let mut seq_lo = None;
    let mut seq_hi = None;
    let mut after_seq: i64 = 0;
    loop {
        let events = fetch_outbox_page(conn, src_db, after_seq, spool::JSONL_BATCH)?;
        if events.is_empty() {
            break;
        }
        let lo = events[0].seq;
        let hi = events[events.len() - 1].seq;
        spool::write_batch(&cfg.spool_dir, src_db, &events)?;
        conn.execute("DELETE FROM _outbox WHERE seq <= ?1", [hi])?;
        total += events.len();
        if seq_lo.is_none() {
            seq_lo = Some(lo);
        }
        seq_hi = Some(hi);
        after_seq = hi;
        if events.len() < spool::JSONL_BATCH {
            break;
        }
    }
    if total == 0 {
        return Ok(None);
    }
    tracing::info!(
        db = %src_db,
        rows = total,
        seq_lo,
        seq_hi,
        "spooled _outbox"
    );
    Ok(Some(DrainStats {
        src_db: src_db.to_string(),
        rows: total,
        seq_lo,
        seq_hi,
    }))
}

fn fetch_outbox_page(
    conn: &Connection,
    src_db: &str,
    after_seq: i64,
    limit: usize,
) -> Result<Vec<Event>> {
    let mut stmt = conn.prepare(
        "SELECT seq, tbl, op, ts, key, before, after FROM _outbox
          WHERE seq > ?1 ORDER BY seq LIMIT ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![after_seq, limit as i64], |r| {
        Ok(OutboxRow {
            seq: r.get(0)?,
            tbl: r.get(1)?,
            op: r.get(2)?,
            ts: r.get(3)?,
            key: r.get(4)?,
            before: r.get(5)?,
            after: r.get(6)?,
        })
    })?;
    let mut events = Vec::new();
    for row in rows {
        let row = row?;
        events.push(Event {
            src_db: src_db.to_string(),
            seq: row.seq,
            tbl: row.tbl,
            op: row.op,
            ts: row.ts,
            key: required_json(row.seq, "key", &row.key)?,
            before: optional_json(row.seq, "before", row.before.as_deref())?,
            after: optional_json(row.seq, "after", row.after.as_deref())?,
        });
    }
    Ok(events)
}

struct OutboxRow {
    seq: i64,
    tbl: String,
    op: String,
    ts: i64,
    key: String,
    before: Option<String>,
    after: Option<String>,
}

fn required_json(seq: i64, field: &str, raw: &str) -> Result<Value> {
    serde_json::from_str(raw).with_context(|| format!("_outbox seq {seq} {field} is not JSON"))
}

fn optional_json(seq: i64, field: &str, raw: Option<&str>) -> Result<Option<Value>> {
    match raw {
        None => Ok(None),
        Some(s) => Ok(Some(required_json(seq, field, s)?)),
    }
}

#[cfg(unix)]
pub fn serve(cfg: &CollectCfg) -> Result<()> {
    let sock = bind_sock(&cfg.sock)?;
    sock.set_read_timeout(Some(Duration::from_millis(500)))?;
    tracing::info!(sock = %cfg.sock.display(), tick_s = cfg.tick.as_secs(), "collector listening");
    let mut last_tick = Instant::now() - cfg.tick;
    loop {
        if last_tick.elapsed() >= cfg.tick {
            if let Err(e) = drain_all(cfg) {
                tracing::error!(error = %e, "periodic drain failed");
            }
            last_tick = Instant::now();
        }
        let mut buf = [0u8; 256];
        match sock.recv_from(&mut buf) {
            Ok((n, _)) => {
                let name = std::str::from_utf8(&buf[..n]).unwrap_or("").trim();
                if name.is_empty() {
                    continue;
                }
                if let Err(e) = drain_named(cfg, name) {
                    tracing::error!(db = %name, error = %e, "nudge drain failed");
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(unix)]
fn bind_sock(path: &Path) -> Result<std::os::unix::net::UnixDatagram> {
    use std::os::unix::io::FromRawFd;
    use std::os::unix::net::UnixDatagram;

    if let Some(fd) = systemd_listen_fd() {
        // SAFETY: systemd socket activation passes the datagram fd as fd 3
        // when LISTEN_PID matches this process.
        let sock = unsafe { UnixDatagram::from_raw_fd(fd) };
        return Ok(sock);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let _ = std::fs::remove_file(path);
    UnixDatagram::bind(path).with_context(|| format!("bind {}", path.display()))
}

#[cfg(unix)]
fn systemd_listen_fd() -> Option<std::os::unix::io::RawFd> {
    let listen_fds: i32 = std::env::var("LISTEN_FDS").ok()?.parse().ok()?;
    if listen_fds < 1 {
        return None;
    }
    let listen_pid: u32 = match std::env::var("LISTEN_PID")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(pid) => pid,
        None => {
            tracing::warn!("ignoring LISTEN_FDS (LISTEN_PID unset)");
            return None;
        }
    };
    if listen_pid != std::process::id() {
        tracing::warn!(
            listen_pid,
            "ignoring LISTEN_FDS (LISTEN_PID is not this process)"
        );
        return None;
    }
    Some(3)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn setup() -> (tempfile::TempDir, CollectCfg, std::path::PathBuf) {
        let dir = tempdir().unwrap();
        let announce = dir.path().join("announce");
        let spool = dir.path().join("spool");
        fs::create_dir_all(&announce).unwrap();
        let sqlite = dir.path().join("work.sqlite");
        let conn = Connection::open(&sqlite).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE _outbox (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              tbl TEXT NOT NULL,
              op TEXT NOT NULL,
              ts INTEGER NOT NULL,
              key TEXT NOT NULL,
              before TEXT,
              after TEXT
            );
            "#,
        )
        .unwrap();
        fs::write(
            announce.join("adsb-trip-journal.json"),
            format!(
                "{{\n  \"db_name\": \"adsb-trip-journal\",\n  \"sqlite_path\": {}\n}}\n",
                serde_json::to_string(&sqlite.display().to_string()).unwrap()
            ),
        )
        .unwrap();
        let cfg = CollectCfg {
            announce_dir: announce,
            spool_dir: spool,
            sock: dir.path().join("collect.sock"),
            tick: Duration::from_secs(60),
            snapshot_db: None,
            min_seq: None,
        };
        (dir, cfg, sqlite)
    }

    fn insert_row(path: &Path, tbl: &str, op: &str, key: &str, after: &str) {
        let conn = Connection::open(path).unwrap();
        conn.execute(
            "INSERT INTO _outbox (tbl, op, ts, key, after) VALUES (?1, ?2, 1700000000, ?3, ?4)",
            rusqlite::params![tbl, op, key, after],
        )
        .unwrap();
    }

    #[test]
    fn drain_writes_jsonl_and_prunes() {
        let (_d, cfg, sqlite) = setup();
        insert_row(
            &sqlite,
            "trips",
            "I",
            r#"{"icao24":"abcdef","dep_ts":"2024-01-15T12:00:00Z"}"#,
            r#"{"icao24":"abcdef","ticker":"AAA"}"#,
        );
        insert_row(
            &sqlite,
            "trips",
            "U",
            r#"{"icao24":"abcdef","dep_ts":"2024-01-15T12:00:00Z"}"#,
            r#"{"icao24":"abcdef","ticker":"BBB"}"#,
        );
        let stats = drain_all(&cfg).unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].rows, 2);
        let files = spool::list_jsonl(&cfg.spool_dir).unwrap();
        assert_eq!(files.len(), 1);
        let body = fs::read_to_string(&files[0]).unwrap();
        let lines: Vec<_> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        let ev: Event = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(ev.src_db, "adsb-trip-journal");
        assert_eq!(ev.tbl, "trips");
        assert_eq!(ev.op, "I");
        assert_eq!(ev.seq, 1);
        assert!(ev.key.get("icao24").is_some());
        let left: i64 = Connection::open(&sqlite)
            .unwrap()
            .query_row("SELECT count(*) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 0);
    }

    #[test]
    fn drain_pages_above_jsonl_batch() {
        let (_d, cfg, sqlite) = setup();
        let n = spool::JSONL_BATCH + 1;
        let conn = Connection::open(&sqlite).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..n {
            let key = format!(r#"{{"id":{i}}}"#);
            tx.execute(
                "INSERT INTO _outbox (tbl, op, ts, key, after) VALUES ('t', 'I', 1700000000, ?1, ?1)",
                rusqlite::params![key],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let stats = drain_all(&cfg).unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].rows, n);
        let files = spool::list_jsonl(&cfg.spool_dir).unwrap();
        assert_eq!(files.len(), 2);
        let left: i64 = Connection::open(&sqlite)
            .unwrap()
            .query_row("SELECT count(*) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 0);
        let lines0 = std::fs::read_to_string(&files[0]).unwrap().lines().count();
        let lines1 = std::fs::read_to_string(&files[1]).unwrap().lines().count();
        assert_eq!(lines0, spool::JSONL_BATCH);
        assert_eq!(lines1, 1);
    }

    #[test]
    fn empty_outbox_writes_nothing() {
        let (_d, cfg, _sqlite) = setup();
        assert!(drain_all(&cfg).unwrap().is_empty());
        assert!(spool::list_jsonl(&cfg.spool_dir).unwrap().is_empty());
    }

    #[test]
    fn unknown_nudge_is_ok() {
        let (_d, cfg, _) = setup();
        assert!(drain_named(&cfg, "no-such-db").unwrap().is_none());
    }

    #[test]
    fn drain_rejects_malformed_key_json() {
        let (_d, cfg, sqlite) = setup();
        insert_row(&sqlite, "trips", "I", "not-json", r#"{"ok":true}"#);
        let err = drain_all(&cfg).unwrap_err().to_string();
        assert!(
            err.contains("not JSON") || err.contains("adsb-trip-journal"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn drain_all_attempts_remaining_dbs_then_errors() {
        let (_d, cfg, sqlite) = setup();
        insert_row(&sqlite, "trips", "I", r#"{"id":1}"#, r#"{"id":1}"#);
        fs::write(
            cfg.announce_dir.join("broken.json"),
            r#"{"db_name":"broken","sqlite_path":"/no/such/work.sqlite"}"#,
        )
        .unwrap();
        let err = drain_all(&cfg).unwrap_err().to_string();
        assert!(err.contains("broken"), "{err}");
        let files = spool::list_jsonl(&cfg.spool_dir).unwrap();
        assert_eq!(files.len(), 1);
    }
}
