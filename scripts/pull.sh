#!/usr/bin/env bash
# Mini: SSH-pull closed JSONL from Oracle, apply into local Postgres.
# Does not open a path from Oracle to this machine.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${STATE_CAPTURE_BIN:-$ROOT/target/release/state-capture}"
if [[ ! -x "$BIN" ]]; then
  BIN="$ROOT/target/debug/state-capture"
fi
if [[ ! -x "$BIN" ]]; then
  echo "build first: cargo build --release" >&2
  exit 1
fi

: "${ORACLE_SSH:?set ORACLE_SSH e.g. ct-firehose}"
: "${DATABASE_URL:?set DATABASE_URL for local Postgres}"
REMOTE_SPOOL="${REMOTE_SPOOL:-/var/lib/state-capture/spool}"
LOCAL_SPOOL="${LOCAL_SPOOL:-$HOME/var/state-capture/incoming}"

mkdir -p "$LOCAL_SPOOL"
rsync -a --include='*/' --include='*.jsonl' --exclude='*' \
  "${ORACLE_SSH}:${REMOTE_SPOOL}/" "${LOCAL_SPOOL}/"

"$BIN" apply --migrate --spool "$LOCAL_SPOOL" --database-url "$DATABASE_URL" --delete-after

# Remote files stay until you prune them (store-and-forward). Optional:
#   ssh "$ORACLE_SSH" "find $REMOTE_SPOOL -name '*.jsonl' -mtime +14 -delete"
