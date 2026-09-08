//! Closed JSONL batches. Write tmp + fsync + rename so rsync never sees a partial file.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::event::Event;

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
    out.sort();
    Ok(out)
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
