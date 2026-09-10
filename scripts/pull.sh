#!/usr/bin/env bash
# Mini: SSH-pull closed JSONL from Oracle, apply into local Postgres.
# Does not open a path from Oracle to this machine.
# Exclusive lock on $LOCAL_SPOOL/.pull.lock so a slow apply cannot overlap
# the next launchd interval (flock(1), else Python fcntl.flock on fd 9).
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
FORGET_SPOOL="${FORGET_SPOOL:-/opt/state-capture-collector/scripts/forget-spool.sh}"

mkdir -p "$LOCAL_SPOOL"
LOCK="$LOCAL_SPOOL/.pull.lock"
exec 9>"$LOCK"
if command -v flock >/dev/null 2>&1; then
  flock -n 9 || {
    echo "pull already running (lock $LOCK)" >&2
    exit 1
  }
else
  python3 -c 'import fcntl; fcntl.flock(9, fcntl.LOCK_EX | fcntl.LOCK_NB)' || {
    echo "pull already running (lock $LOCK)" >&2
    exit 1
  }
fi

rsync -a --include='*/' --include='*.jsonl' --exclude='*' \
  "${ORACLE_SSH}:${REMOTE_SPOOL}/" "${LOCAL_SPOOL}/"

list_rel() {
  (cd "$LOCAL_SPOOL" && find . -name '*.jsonl' | sed 's|^\./||' | sort)
}

before=$(mktemp)
forget=$(mktemp)
trap 'rm -f "$before" "$forget"' EXIT
list_rel >"$before"

"$BIN" apply --migrate --spool "$LOCAL_SPOOL" --database-url "$DATABASE_URL" --delete-after

: >"$forget"
while IFS= read -r rel; do
  [[ -z "$rel" ]] && continue
  if [[ ! -f "$LOCAL_SPOOL/$rel" ]]; then
    printf '%s\n' "$rel"
  fi
done <"$before" >"$forget"

if [[ -s "$forget" ]]; then
  # sudoers on Oracle: NOPASSWD this script only. Missing sudo must fail the
  # pull (do not age-delete as a fallback).
  ssh "$ORACLE_SSH" "sudo -n $FORGET_SPOOL" <"$forget"
fi
