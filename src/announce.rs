//! Announce JSON: `{db_name, sqlite_path}` written by each utility at `open()`.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::event::validate_db_name;

#[derive(Debug, Clone, Deserialize)]
pub struct Announce {
    pub db_name: String,
    pub sqlite_path: String,
}

pub fn load_dir(dir: &Path) -> Result<Vec<Announce>> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    let mut names: Vec<PathBuf> = fs::read_dir(dir)
        .with_context(|| format!("read announce dir {}", dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();
    names.sort();
    for path in names {
        match load_file(&path) {
            Ok(a) => out.push(a),
            Err(e) => tracing::warn!(path = %path.display(), error = %e, "skip announce file"),
        }
    }
    Ok(out)
}

pub fn load_named(dir: &Path, db_name: &str) -> Result<Option<Announce>> {
    validate_db_name(db_name)?;
    let path = dir.join(format!("{db_name}.json"));
    if !path.is_file() {
        return Ok(None);
    }
    Ok(Some(load_file(&path)?))
}

fn load_file(path: &Path) -> Result<Announce> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let a: Announce = serde_json::from_str(&text)
        .with_context(|| format!("parse announce {}", path.display()))?;
    validate_db_name(&a.db_name)?;
    if a.sqlite_path.is_empty() {
        anyhow::bail!("sqlite_path empty in {}", path.display());
    }
    Ok(a)
}
