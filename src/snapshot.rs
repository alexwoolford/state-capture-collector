//! One-shot replica of captured tables into JSONL.
//!
//! Live `_outbox` is incremental from when capture was enabled. After a feed
//! break (or first enable on a populated sqlite), emit `I` events for every
//! current row of each table that has `_cap_I_*` triggers. Seq is reserved in
//! `sqlite_sequence` so later trigger inserts cannot collide.
//!
//! Each database is snapshotted under `BEGIN IMMEDIATE`: drain `_outbox`, then
//! emit `I` events, then commit. Writers on that sqlite are blocked until
//! commit so a concurrent `U` cannot land at seq N while snapshot emits a
//! stale `I` at N+1.
//!
//! Does not `UPDATE col=col` (that would look like real changes). Does not
//! open sqlite from the Mini.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use serde_json::{json, Map, Value};

use crate::announce;
use crate::collect::{self, CollectCfg, DrainStats};
use crate::event::{validate_db_name, Event};
use crate::spool;

pub fn snapshot_all(cfg: &CollectCfg) -> Result<Vec<DrainStats>> {
    if let Some(name) = cfg.snapshot_db.as_deref() {
        return match snapshot_named(cfg, name)? {
            Some(s) => Ok(vec![s]),
            None => Ok(vec![]),
        };
    }
    let announced = announce::load_dir(&cfg.announce_dir)?;
    let mut stats = Vec::new();
    let mut failed = Vec::new();
    for a in announced {
        match snapshot_named(cfg, &a.db_name) {
            Ok(Some(s)) => stats.push(s),
            Ok(None) => {}
            Err(e) => {
                tracing::error!(db = %a.db_name, error = %e, "snapshot failed");
                failed.push(a.db_name);
            }
        }
    }
    if !failed.is_empty() {
        bail!("snapshot failed for: {}", failed.join(", "));
    }
    Ok(stats)
}

pub fn snapshot_named(cfg: &CollectCfg, db_name: &str) -> Result<Option<DrainStats>> {
    validate_db_name(db_name)?;
    let Some(a) = announce::load_named(&cfg.announce_dir, db_name)? else {
        tracing::warn!(db = %db_name, "snapshot skipped (no announce file)");
        return Ok(None);
    };
    snapshot_sqlite(cfg, &a.db_name, Path::new(&a.sqlite_path))
}

pub fn snapshot_sqlite(
    cfg: &CollectCfg,
    src_db: &str,
    sqlite_path: &Path,
) -> Result<Option<DrainStats>> {
    validate_db_name(src_db)?;
    if !sqlite_path.is_file() {
        anyhow::bail!("sqlite missing: {}", sqlite_path.display());
    }
    let mut conn =
        Connection::open(sqlite_path).with_context(|| format!("open {}", sqlite_path.display()))?;
    conn.busy_timeout(std::time::Duration::from_millis(5_000))?;
    if !collect::has_outbox(&conn)? {
        tracing::warn!(db = %src_db, path = %sqlite_path.display(), "no _outbox table");
        return Ok(None);
    }

    tracing::info!(
        db = %src_db,
        path = %sqlite_path.display(),
        "snapshot holding write lock (BEGIN IMMEDIATE)"
    );
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("BEGIN IMMEDIATE for snapshot")?;
    let stats = snapshot_locked(cfg, src_db, &tx)?;
    tx.commit().context("commit snapshot")?;
    Ok(stats)
}

fn snapshot_locked(
    cfg: &CollectCfg,
    src_db: &str,
    conn: &Connection,
) -> Result<Option<DrainStats>> {
    if let Some(min) = cfg.min_seq {
        floor_outbox_seq(conn, min)?;
    }

    let drained = collect::drain_conn(cfg, src_db, conn)?;

    let tables = captured_tables(conn)?;
    if tables.is_empty() {
        tracing::warn!(db = %src_db, "no _cap_I_* triggers; nothing to snapshot");
        return Ok(drained);
    }

    let ts = now_ts();
    let mut snap_rows = 0usize;
    for table in &tables {
        let n = snapshot_table(conn, cfg, src_db, table, ts)?;
        if n == 0 {
            continue;
        }
        snap_rows += n;
        tracing::info!(db = %src_db, tbl = %table.name, rows = n, "snapshot table");
    }

    let drain_rows = drained.as_ref().map(|d| d.rows).unwrap_or(0);
    let rows = drain_rows + snap_rows;
    if rows == 0 {
        return Ok(None);
    }
    let hi = current_outbox_seq(conn)?;
    let seq_lo = drained.as_ref().and_then(|d| d.seq_lo).or({
        if snap_rows == 0 {
            None
        } else {
            Some(hi - snap_rows as i64 + 1)
        }
    });

    tracing::info!(
        db = %src_db,
        rows,
        drain_rows,
        snap_rows,
        tables = tables.len(),
        seq_lo,
        seq_hi = hi,
        "snapshot spooled"
    );
    Ok(Some(DrainStats {
        src_db: src_db.to_string(),
        rows,
        seq_lo,
        seq_hi: Some(hi),
    }))
}

struct CapturedTable {
    name: String,
    pk: Vec<String>,
    payload: Vec<String>,
    without_rowid: bool,
}

fn captured_tables(conn: &Connection) -> Result<Vec<CapturedTable>> {
    let mut stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master
         WHERE type = 'trigger' AND name LIKE '_cap_I_%'
         ORDER BY name",
    )?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    let mut out = Vec::new();
    for row in rows {
        let (trig, sql) = row?;
        let name = trig
            .strip_prefix("_cap_I_")
            .ok_or_else(|| anyhow::anyhow!("unexpected trigger {trig}"))?
            .to_string();
        if name == "_outbox" {
            continue;
        }
        validate_ident(&name)?;
        let cols = table_columns(conn, &name)?;
        let pk = pk_columns(&cols);
        let payload = payload_from_insert_trigger(&sql).ok_or_else(|| {
            anyhow::anyhow!(
                "trigger {trig} has no parseable json_object payload (refusing all-column fallback)"
            )
        })?;
        for c in pk.iter().chain(payload.iter()) {
            validate_ident(c)?;
        }
        let without_rowid = is_without_rowid(conn, &name)?;
        out.push(CapturedTable {
            name,
            pk,
            payload,
            without_rowid,
        });
    }
    Ok(out)
}

fn snapshot_table(
    conn: &Connection,
    cfg: &CollectCfg,
    src_db: &str,
    table: &CapturedTable,
    ts: i64,
) -> Result<usize> {
    let mut written = 0usize;
    if table.without_rowid {
        written += snapshot_all_rows(conn, cfg, src_db, table, ts)?;
        return Ok(written);
    }
    let mut after_rowid: i64 = 0;
    loop {
        let batch = fetch_rowid_page(conn, table, after_rowid, spool::JSONL_BATCH)?;
        if batch.is_empty() {
            break;
        }
        after_rowid = batch.last().map(|r| r.rowid).unwrap_or(after_rowid);
        written += emit_batch(conn, cfg, src_db, table, ts, &batch)?;
        if batch.len() < spool::JSONL_BATCH {
            break;
        }
    }
    Ok(written)
}

struct RowSnap {
    rowid: i64,
    key: Value,
    after: Value,
}

fn fetch_rowid_page(
    conn: &Connection,
    table: &CapturedTable,
    after_rowid: i64,
    limit: usize,
) -> Result<Vec<RowSnap>> {
    let select_cols = select_list(table)?;
    let sql = format!(
        "SELECT rowid, {select_cols} FROM \"{}\" WHERE rowid > ?1 ORDER BY rowid LIMIT {limit}",
        table.name
    );
    let mut stmt = conn.prepare(&sql)?;
    let col_count = stmt.column_count();
    let mut rows = stmt.query([after_rowid])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let rowid: i64 = row.get(0)?;
        out.push(row_snap(table, row, 1, col_count, rowid)?);
    }
    Ok(out)
}

fn snapshot_all_rows(
    conn: &Connection,
    cfg: &CollectCfg,
    src_db: &str,
    table: &CapturedTable,
    ts: i64,
) -> Result<usize> {
    let select_cols = select_list(table)?;
    let sql = format!("SELECT {select_cols} FROM \"{}\"", table.name);
    let mut stmt = conn.prepare(&sql)?;
    let col_count = stmt.column_count();
    let mut rows = stmt.query([])?;
    let mut batch = Vec::new();
    let mut written = 0usize;
    while let Some(row) = rows.next()? {
        batch.push(row_snap(table, row, 0, col_count, 0)?);
        if batch.len() >= spool::JSONL_BATCH {
            written += emit_batch(conn, cfg, src_db, table, ts, &batch)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        written += emit_batch(conn, cfg, src_db, table, ts, &batch)?;
    }
    Ok(written)
}

fn select_list(table: &CapturedTable) -> Result<String> {
    let names = ordered_cols(table);
    if names.is_empty() {
        bail!("{} has no key or payload columns", table.name);
    }
    for n in &names {
        validate_ident(n)?;
    }
    Ok(names
        .iter()
        .map(|n| format!("\"{n}\""))
        .collect::<Vec<_>>()
        .join(", "))
}

fn ordered_cols(table: &CapturedTable) -> Vec<String> {
    let mut names: Vec<String> = table.pk.clone();
    for c in &table.payload {
        if !names.iter().any(|n| n == c) {
            names.push(c.clone());
        }
    }
    names
}

fn row_snap(
    table: &CapturedTable,
    row: &rusqlite::Row<'_>,
    offset: usize,
    _col_count: usize,
    rowid: i64,
) -> Result<RowSnap> {
    let cols = ordered_cols(table);
    let mut values = Map::new();
    for (i, name) in cols.iter().enumerate() {
        let v: rusqlite::types::Value = row.get(offset + i)?;
        values.insert(name.clone(), sql_value_to_json(v));
    }
    let key = if table.pk.is_empty() {
        json!({ "rowid": rowid })
    } else {
        let mut k = Map::new();
        for n in &table.pk {
            k.insert(n.clone(), values.get(n).cloned().unwrap_or(Value::Null));
        }
        Value::Object(k)
    };
    let mut after = Map::new();
    for n in &table.payload {
        after.insert(n.clone(), values.get(n).cloned().unwrap_or(Value::Null));
    }
    Ok(RowSnap {
        rowid,
        key,
        after: Value::Object(after),
    })
}

fn emit_batch(
    conn: &Connection,
    cfg: &CollectCfg,
    src_db: &str,
    table: &CapturedTable,
    ts: i64,
    rows: &[RowSnap],
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    let first = reserve_outbox_seq(conn, rows.len() as i64)?;
    let events: Vec<Event> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| Event {
            src_db: src_db.to_string(),
            seq: first + i as i64,
            tbl: table.name.clone(),
            op: "I".into(),
            ts,
            key: r.key.clone(),
            before: None,
            after: Some(r.after.clone()),
        })
        .collect();
    spool::write_batch(&cfg.spool_dir, src_db, &events)?;
    Ok(events.len())
}

fn reserve_outbox_seq(conn: &Connection, n: i64) -> Result<i64> {
    if n <= 0 {
        bail!("reserve_outbox_seq n must be positive");
    }
    let cur = current_outbox_seq(conn)?;
    let first = cur + 1;
    let last = cur + n;
    set_outbox_seq(conn, last)?;
    Ok(first)
}

fn floor_outbox_seq(conn: &Connection, min: i64) -> Result<()> {
    let cur = current_outbox_seq(conn)?;
    if cur >= min {
        return Ok(());
    }
    set_outbox_seq(conn, min)?;
    tracing::info!(min, was = cur, "floored _outbox sqlite_sequence");
    Ok(())
}

fn current_outbox_seq(conn: &Connection) -> Result<i64> {
    let seq_tbl: i64 = conn
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = '_outbox'",
            [],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);
    let seq_out: i64 = conn.query_row("SELECT COALESCE(MAX(seq), 0) FROM _outbox", [], |r| {
        r.get(0)
    })?;
    Ok(seq_tbl.max(seq_out))
}

fn set_outbox_seq(conn: &Connection, seq: i64) -> Result<()> {
    let updated = conn.execute(
        "UPDATE sqlite_sequence SET seq = ?1 WHERE name = '_outbox'",
        [seq],
    )?;
    if updated == 0 {
        conn.execute(
            "INSERT INTO sqlite_sequence(name, seq) VALUES ('_outbox', ?1)",
            [seq],
        )?;
    }
    Ok(())
}

fn payload_from_insert_trigger(sql: &str) -> Option<Vec<String>> {
    let idx = sql.rfind("json_object(")?;
    let rest = &sql[idx + "json_object(".len()..];
    let mut names = Vec::new();
    let bytes = rest.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i += 1;
            let start = i;
            while i < bytes.len() && bytes[i] != b'\'' {
                i += 1;
            }
            if i > start {
                let name = rest[start..i].to_string();
                if validate_ident(&name).is_ok() {
                    names.push(name);
                }
            }
            i += 1;
            continue;
        }
        if bytes[i] == b')' {
            break;
        }
        i += 1;
    }
    if names.is_empty() {
        None
    } else {
        Some(names)
    }
}

fn sql_value_to_json(v: rusqlite::types::Value) -> Value {
    match v {
        rusqlite::types::Value::Null => Value::Null,
        rusqlite::types::Value::Integer(i) => json!(i),
        rusqlite::types::Value::Real(f) => json!(f),
        rusqlite::types::Value::Text(s) => Value::String(s),
        rusqlite::types::Value::Blob(_) => Value::Null,
    }
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

struct Col {
    name: String,
    pk: i64,
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<Col>> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info(\"{table}\")"))
        .with_context(|| format!("table_info {table}"))?;
    let cols = stmt
        .query_map([], |row| {
            Ok(Col {
                name: row.get(1)?,
                pk: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(cols)
}

fn pk_columns(cols: &[Col]) -> Vec<String> {
    let mut pk: Vec<(i64, String)> = cols
        .iter()
        .filter(|c| c.pk > 0)
        .map(|c| (c.pk, c.name.clone()))
        .collect();
    pk.sort_by_key(|(n, _)| *n);
    pk.into_iter().map(|(_, n)| n).collect()
}

fn is_without_rowid(conn: &Connection, table: &str) -> Result<bool> {
    let sql: Option<String> = match conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get::<_, String>(0),
    ) {
        Ok(s) => Some(s),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e).context("without-rowid check"),
    };
    Ok(sql.is_some_and(|s| {
        s.to_ascii_uppercase()
            .replace(['\n', '\t'], " ")
            .contains("WITHOUT ROWID")
    }))
}

fn validate_ident(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if ok {
        Ok(())
    } else {
        bail!("invalid SQL identifier {name:?}")
    }
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
            CREATE TABLE jobs (
              id INTEGER PRIMARY KEY,
              state TEXT NOT NULL,
              secret TEXT
            );
            INSERT INTO jobs (id, state, secret) VALUES (1, 'open', 'x'), (2, 'done', 'y');
            CREATE TRIGGER _cap_I_jobs AFTER INSERT ON jobs BEGIN
              INSERT INTO _outbox(tbl, op, key, before, after)
              VALUES ('jobs', 'I', json_object('id', NEW.id), NULL,
                      json_object('id', NEW.id, 'state', NEW.state));
            END;
            "#,
        )
        .unwrap();
        fs::write(
            announce.join("demo.json"),
            format!(
                "{{\n  \"db_name\": \"demo\",\n  \"sqlite_path\": {}\n}}\n",
                serde_json::to_string(&sqlite.display().to_string()).unwrap()
            ),
        )
        .unwrap();
        let cfg = CollectCfg {
            announce_dir: announce,
            spool_dir: spool,
            sock: dir.path().join("collect.sock"),
            tick: std::time::Duration::from_secs(60),
            snapshot_db: None,
            min_seq: None,
        };
        (dir, cfg, sqlite)
    }

    #[test]
    fn snapshot_writes_current_rows_and_reserves_seq() {
        let (_d, cfg, sqlite) = setup();
        let stats = snapshot_all(&cfg).unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].rows, 2);
        let files = spool::list_jsonl(&cfg.spool_dir).unwrap();
        assert_eq!(files.len(), 1);
        let body = fs::read_to_string(&files[0]).unwrap();
        let lines: Vec<_> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        let ev: Event = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(ev.src_db, "demo");
        assert_eq!(ev.tbl, "jobs");
        assert_eq!(ev.op, "I");
        assert_eq!(ev.seq, 1);
        assert_eq!(ev.key["id"], 1);
        assert_eq!(ev.after.as_ref().unwrap()["state"], "open");
        assert!(ev.after.as_ref().unwrap().get("secret").is_none());
        let seq: i64 = Connection::open(&sqlite)
            .unwrap()
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = '_outbox'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(seq, 2);
    }

    #[test]
    fn snapshot_continues_after_existing_outbox_seq() {
        let (_d, cfg, sqlite) = setup();
        Connection::open(&sqlite)
            .unwrap()
            .execute(
                "INSERT INTO _outbox (tbl, op, ts, key, after) VALUES ('jobs', 'U', 1, '{}', '{}')",
                [],
            )
            .unwrap();
        let stats = snapshot_all(&cfg).unwrap();
        assert_eq!(stats[0].rows, 3);
        assert_eq!(stats[0].seq_lo, Some(1));
        assert_eq!(stats[0].seq_hi, Some(3));
        let left: i64 = Connection::open(&sqlite)
            .unwrap()
            .query_row("SELECT count(*) FROM _outbox", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 0);
        let files = spool::list_jsonl(&cfg.spool_dir).unwrap();
        assert_eq!(files.len(), 2);
        let drain_body = fs::read_to_string(&files[0]).unwrap();
        let drain_ev: Event = serde_json::from_str(drain_body.lines().next().unwrap()).unwrap();
        assert_eq!(drain_ev.op, "U");
        assert_eq!(drain_ev.seq, 1);
        let snap_body = fs::read_to_string(&files[1]).unwrap();
        let snap_ev: Event = serde_json::from_str(snap_body.lines().next().unwrap()).unwrap();
        assert_eq!(snap_ev.op, "I");
        assert_eq!(snap_ev.seq, 2);
    }

    #[test]
    fn snapshot_payload_omits_excluded_column() {
        let names = payload_from_insert_trigger(
            r#"
            CREATE TRIGGER _cap_I_jobs AFTER INSERT ON jobs BEGIN
              INSERT INTO _outbox(tbl, op, key, before, after)
              VALUES ('jobs', 'I', json_object('id', NEW.id), NULL,
                      json_object('id', NEW.id, 'state', NEW.state));
            END;
            "#,
        )
        .unwrap();
        assert_eq!(names, ["id", "state"]);
        assert!(!names.iter().any(|n| n == "secret"));
    }

    #[test]
    fn snapshot_refuses_all_column_fallback_without_json_object() {
        let (_d, cfg, sqlite) = setup();
        Connection::open(&sqlite)
            .unwrap()
            .execute_batch(
                r#"
                DROP TRIGGER _cap_I_jobs;
                CREATE TRIGGER _cap_I_jobs AFTER INSERT ON jobs BEGIN
                  INSERT INTO _outbox(tbl, op, key, before, after)
                  VALUES ('jobs', 'I', '{}', NULL, NULL);
                END;
                "#,
            )
            .unwrap();
        let err = snapshot_all(&cfg).unwrap_err().to_string();
        assert!(
            err.contains("json_object") || err.contains("demo"),
            "unexpected error: {err}"
        );
        assert!(spool::list_jsonl(&cfg.spool_dir).unwrap().is_empty());
        let secret_leaked = spool::list_jsonl(&cfg.spool_dir)
            .unwrap()
            .iter()
            .any(|p| fs::read_to_string(p).unwrap().contains("secret"));
        assert!(!secret_leaked);
    }
}
