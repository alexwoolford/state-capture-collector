//! Closed JSONL batches. Write tmp + fsync + rename so rsync never sees a partial file.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::event::Event;

/// Closed JSONL page size. Snapshot and drain both use this so a large `_outbox`
/// cannot become one Vec / one apply transaction.
pub const JSONL_BATCH: usize = 5_000;

pub fn write_batch(spool_dir: &Path, src_db: &str, events: &[Event]) -> Result<Option<PathBuf>> {
    if events.is_empty() {
        return Ok(None);
    }
    let lo = events[0].seq;
    let hi = events[events.len() - 1].seq;
    let dir = spool_dir.join(src_db);
    fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let name = format!("{lo}-{hi}.jsonl");
    let final_path = dir.join(&name);
    let tmp_path = dir.join(format!("{name}.tmp"));
    {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .with_context(|| format!("open {}", tmp_path.display()))?;
        let mut w = BufWriter::new(file);
        for ev in events {
            serde_json::to_writer(&mut w, ev).context("serialize event")?;
            w.write_all(b"\n")?;
        }
        w.flush()?;
        w.get_ref().sync_all().context("fsync jsonl")?;
    }
    fs::rename(&tmp_path, &final_path)
        .with_context(|| format!("rename {} -> {}", tmp_path.display(), final_path.display()))?;
    sync_dir(&dir)?;
    Ok(Some(final_path))
}

fn sync_dir(dir: &Path) -> Result<()> {
    let f = File::open(dir).with_context(|| format!("open dir {}", dir.display()))?;
    f.sync_all().ok();
    Ok(())
}

pub fn list_jsonl(spool: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !spool.is_dir() {
        return Ok(out);
    }
    walk_jsonl(spool, &mut out)?;
    out.sort_by_key(|p| jsonl_sort_key(p));
    Ok(out)
}

/// `(src_db, seq_lo, seq_hi, path)` so `338-732.jsonl` applies before `10000741-…`.
fn jsonl_sort_key(path: &Path) -> (String, i64, i64, PathBuf) {
    let src_db = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let (lo, hi) = parse_lo_hi(stem).unwrap_or((i64::MAX, i64::MAX));
    (src_db, lo, hi, path.to_path_buf())
}

fn parse_lo_hi(stem: &str) -> Option<(i64, i64)> {
    let (lo, hi) = stem.split_once('-')?;
    Some((lo.parse().ok()?, hi.parse().ok()?))
}

fn walk_jsonl(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for ent in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let ent = ent?;
        let path = ent.path();
        if path.is_dir() {
            walk_jsonl(&path, out)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("jsonl")
            && !path
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|n| n.ends_with(".tmp"))
        {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_jsonl_orders_by_seq_not_path_string() {
        let dir = tempfile::tempdir().unwrap();
        let faa = dir.path().join("faa-registry-mirror");
        std::fs::create_dir_all(&faa).unwrap();
        std::fs::write(faa.join("10000741-10000750.jsonl"), "{}\n").unwrap();
        std::fs::write(faa.join("338-732.jsonl"), "{}\n").unwrap();
        let files = list_jsonl(dir.path()).unwrap();
        let names: Vec<_> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(names, ["338-732.jsonl", "10000741-10000750.jsonl"]);
    }
}
