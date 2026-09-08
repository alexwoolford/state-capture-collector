//! Shared event envelope. Same shape the collector writes and the applyer reads.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub src_db: String,
    pub seq: i64,
    pub tbl: String,
    pub op: String,
    pub ts: i64,
    pub key: Value,
    #[serde(default)]
    pub before: Option<Value>,
    #[serde(default)]
    pub after: Option<Value>,
}

pub fn validate_db_name(name: &str) -> anyhow::Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if ok {
        Ok(())
    } else {
        anyhow::bail!("db_name must be ASCII alphanumeric / hyphen / underscore, got {name:?}")
    }
}
