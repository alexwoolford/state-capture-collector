//! Live Postgres apply. Requires a local server; skipped if connect fails.
//!
//! Asserts the generic `capture.events` + `capture.current` shape. A new
//! `src_db` / `tbl` must apply without collector or schema changes.

use serde_json::json;
use state_capture::apply;
use state_capture::event::Event;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

static PG: Mutex<()> = Mutex::new(());
// Restart-safe vs prior apply_pg runs on the same mosaic DB (skip-on-duplicate).
static SEQ: AtomicI64 = AtomicI64::new(700_000_000);

fn next_seq() -> i64 {
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn unique_src(prefix: &str) -> String {
    format!("{}-{}-{}", prefix, std::process::id(), next_seq())
}

fn pg_lock() -> std::sync::MutexGuard<'static, ()> {
    PG.lock().unwrap_or_else(|e| e.into_inner())
}

fn url() -> Option<String> {
    std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty())
}

fn ev(
    src_db: &str,
    seq: i64,
    tbl: &str,
    op: &str,
    key: serde_json::Value,
    after: Option<serde_json::Value>,
) -> Event {
    Event {
        src_db: src_db.into(),
        seq,
        tbl: tbl.into(),
        op: op.into(),
        ts: 1_700_000_000,
        key,
        before: None,
        after,
    }
}

#[test]
fn migrate_creates_events_d_key() {
    let Some(url) = url() else {
        eprintln!("skip: set DATABASE_URL for apply integration");
        return;
    };
    let _g = pg_lock();
    let mut client = apply::connect(&url).expect("connect");
    apply::migrate(&mut client).expect("migrate");
    let n: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_indexes
              WHERE schemaname = 'capture' AND indexname = 'events_d_key'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        n, 1,
        "later-D lookup index required or Mini hangs on FAA-sized events"
    );
}

#[test]
fn migrate_insert_upsert_idempotent() {
    let Some(url) = url() else {
        eprintln!("skip: set DATABASE_URL for apply integration");
        return;
    };
    let _g = pg_lock();
    let mut client = apply::connect(&url).expect("connect");
    apply::migrate(&mut client).expect("migrate");
    let src = unique_src("apply-pg");
    let seq = next_seq();
    let key = json!({"icao24":"abcdef","dep_ts":"2024-01-15T12:00:00Z"});
    let event = ev(
        &src,
        seq,
        "trips",
        "I",
        key.clone(),
        Some(json!({
            "icao24": "abcdef",
            "dep_ts": "2024-01-15T12:00:00Z",
            "n_number": "N1",
            "ticker": "AAA",
            "source": "opensky_flights",
            "fetched_at": "2026-08-31T00:00:00Z"
        })),
    );
    let first = apply::apply_events(&mut client, std::slice::from_ref(&event)).unwrap();
    assert_eq!(first.events, 1);
    assert_eq!(first.inserted, 1);
    let second = apply::apply_events(&mut client, std::slice::from_ref(&event)).unwrap();
    assert_eq!(second.inserted, 0);
    let ticker: String = client
        .query_one(
            "SELECT after->>'ticker' FROM capture.current
             WHERE src_db = $1 AND tbl = 'trips' AND key = $2",
            &[&src, &key],
        )
        .unwrap()
        .get(0);
    assert_eq!(ticker, "AAA");
    let n: i64 = client
        .query_one(
            "SELECT count(*) FROM capture.events WHERE src_db = $1 AND seq = $2",
            &[&src, &seq],
        )
        .unwrap()
        .get(0);
    assert_eq!(n, 1);
}

#[test]
fn unknown_src_db_and_tbl_need_no_schema_change() {
    let Some(url) = url() else {
        return;
    };
    let _g = pg_lock();
    let mut client = apply::connect(&url).expect("connect");
    apply::migrate(&mut client).expect("migrate");
    let src = unique_src("fifth-util");
    let seq = next_seq();
    let key = json!({"id": seq});
    apply::apply_events(
        &mut client,
        &[ev(
            &src,
            seq,
            "widgets",
            "I",
            key.clone(),
            Some(json!({"id": seq, "color": "blue"})),
        )],
    )
    .unwrap();
    let color: String = client
        .query_one(
            "SELECT after->>'color' FROM capture.current
             WHERE src_db = $1 AND tbl = 'widgets' AND key = $2",
            &[&src, &key],
        )
        .unwrap()
        .get(0);
    assert_eq!(color, "blue");
}

#[test]
fn current_upsert_delete_and_stale_seq() {
    let Some(url) = url() else {
        return;
    };
    let _g = pg_lock();
    let mut client = apply::connect(&url).expect("connect");
    apply::migrate(&mut client).expect("migrate");
    let src = unique_src("tail-to-ticker");
    let seq_u = next_seq();
    let seq_old = seq_u - 1;
    let seq_d = next_seq();
    let key = json!({"n_number": format!("N{seq_u}")});

    apply::apply_events(
        &mut client,
        &[ev(
            &src,
            seq_u,
            "mappings_current",
            "U",
            key.clone(),
            Some(json!({
                "n_number": key["n_number"],
                "ticker": "WMT",
                "deleted_at": 1_710_000_000
            })),
        )],
    )
    .unwrap();

    let ticker: String = client
        .query_one(
            "SELECT after->>'ticker' FROM capture.current
             WHERE src_db = $1 AND tbl = 'mappings_current' AND key = $2",
            &[&src, &key],
        )
        .unwrap()
        .get(0);
    assert_eq!(ticker, "WMT");
    let deleted: String = client
        .query_one(
            "SELECT after->>'deleted_at' FROM capture.current
             WHERE src_db = $1 AND tbl = 'mappings_current' AND key = $2",
            &[&src, &key],
        )
        .unwrap()
        .get(0);
    assert_eq!(deleted, "1710000000");

    apply::apply_events(
        &mut client,
        &[ev(
            &src,
            seq_old,
            "mappings_current",
            "U",
            key.clone(),
            Some(json!({
                "n_number": key["n_number"],
                "ticker": "OLD"
            })),
        )],
    )
    .unwrap();
    let still: String = client
        .query_one(
            "SELECT after->>'ticker' FROM capture.current
             WHERE src_db = $1 AND tbl = 'mappings_current' AND key = $2",
            &[&src, &key],
        )
        .unwrap()
        .get(0);
    assert_eq!(still, "WMT");

    apply::apply_events(
        &mut client,
        &[ev(&src, seq_d, "mappings_current", "D", key.clone(), None)],
    )
    .unwrap();
    let n: i64 = client
        .query_one(
            "SELECT count(*) FROM capture.current
             WHERE src_db = $1 AND tbl = 'mappings_current' AND key = $2",
            &[&src, &key],
        )
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
}

#[test]
fn replay_insert_after_delete_does_not_resurrect() {
    let Some(url) = url() else {
        eprintln!("skip: set DATABASE_URL for apply integration");
        return;
    };
    let _g = pg_lock();
    let mut client = apply::connect(&url).expect("connect");
    apply::migrate(&mut client).expect("migrate");
    let src = unique_src("replay");
    let key = json!({"id": 1});
    let seq_i = next_seq();
    let seq_d = next_seq();
    apply::apply_events(
        &mut client,
        &[ev(
            &src,
            seq_i,
            "t",
            "I",
            key.clone(),
            Some(json!({"id": 1, "v": "live"})),
        )],
    )
    .unwrap();
    apply::apply_events(&mut client, &[ev(&src, seq_d, "t", "D", key.clone(), None)]).unwrap();
    let replay = apply::apply_events(
        &mut client,
        &[ev(
            &src,
            seq_i,
            "t",
            "I",
            key.clone(),
            Some(json!({"id": 1, "v": "live"})),
        )],
    )
    .unwrap();
    assert_eq!(replay.inserted, 0);
    let n: i64 = client
        .query_one(
            "SELECT count(*) FROM capture.current WHERE src_db = $1 AND tbl = 't' AND key = $2",
            &[&src, &key],
        )
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
}

#[test]
fn later_delete_blocks_earlier_insert_replay() {
    let Some(url) = url() else {
        eprintln!("skip: set DATABASE_URL for apply integration");
        return;
    };
    let _g = pg_lock();
    let mut client = apply::connect(&url).expect("connect");
    apply::migrate(&mut client).expect("migrate");
    let src = unique_src("order");
    let key = json!({"id": 2});
    let seq_i = next_seq();
    let seq_d = next_seq();
    apply::apply_events(
        &mut client,
        &[
            ev(&src, seq_d, "t", "D", key.clone(), None),
            ev(
                &src,
                seq_i,
                "t",
                "I",
                key.clone(),
                Some(json!({"id": 2, "v": "stale"})),
            ),
        ],
    )
    .unwrap();
    let n: i64 = client
        .query_one(
            "SELECT count(*) FROM capture.current WHERE src_db = $1 AND tbl = 't' AND key = $2",
            &[&src, &key],
        )
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
}

#[test]
fn retract_heals_current_when_events_already_logged() {
    let Some(url) = url() else {
        eprintln!("skip: set DATABASE_URL for apply integration");
        return;
    };
    let _g = pg_lock();
    let mut client = apply::connect(&url).expect("connect");
    apply::migrate(&mut client).expect("migrate");
    let src = unique_src("heal");
    let key = json!({"id": 3});
    let seq_i = next_seq();
    let seq_d = next_seq();
    apply::apply_events(
        &mut client,
        &[ev(
            &src,
            seq_i,
            "t",
            "I",
            key.clone(),
            Some(json!({"id": 3, "v": "live"})),
        )],
    )
    .unwrap();
    apply::apply_events(&mut client, &[ev(&src, seq_d, "t", "D", key.clone(), None)]).unwrap();
    client
        .execute(
            "INSERT INTO capture.current (src_db, tbl, key, after, seq, ts)
             VALUES ($1, 't', $2, $3, $4, 1)",
            &[&src, &key, &json!({"id": 3, "v": "resurrected"}), &seq_i],
        )
        .unwrap();
    let replay = apply::apply_events(
        &mut client,
        &[ev(
            &src,
            seq_i,
            "t",
            "I",
            key.clone(),
            Some(json!({"id": 3, "v": "live"})),
        )],
    )
    .unwrap();
    assert_eq!(replay.inserted, 0);
    let n: i64 = client
        .query_one(
            "SELECT count(*) FROM capture.current WHERE src_db = $1 AND tbl = 't' AND key = $2",
            &[&src, &key],
        )
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
}
