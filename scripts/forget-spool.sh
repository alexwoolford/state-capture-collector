#!/usr/bin/env bash
# Delete applied JSONL under the collector spool. Relative paths on stdin
# (e.g. faa-registry-mirror/1-5000.jsonl). Rejects traversal. Idempotent.
set -euo pipefail

SPOOL="${STATE_CAPTURE_SPOOL:-/var/lib/state-capture/spool}"
SPOOL="${SPOOL%/}"
if [[ ! -d "$SPOOL" ]]; then
  echo "forget-spool: missing spool $SPOOL" >&2
  exit 1
fi

while IFS= read -r rel || [[ -n "${rel:-}" ]]; do
  rel="${rel#"${rel%%[![:space:]]*}"}"
  rel="${rel%"${rel##*[![:space:]]}"}"
  [[ -z "$rel" ]] && continue
  case "$rel" in
    *..*|/*)
      echo "forget-spool: reject $rel" >&2
      exit 1
      ;;
  esac
  if [[ "$rel" != *.jsonl ]]; then
    echo "forget-spool: reject $rel" >&2
    exit 1
  fi
  dest="$SPOOL/$rel"
  case "$dest" in
    "$SPOOL"/*) ;;
    *)
      echo "forget-spool: reject $rel" >&2
      exit 1
      ;;
  esac
  if [[ -f "$dest" ]]; then
    rm -f "$dest"
  fi
done
