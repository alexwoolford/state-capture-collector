#!/usr/bin/env bash
# Install collector under /opt (Linux / Oracle). Build as a normal user first:
#   cargo build --release
#   sudo ./deploy/install.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PREFIX="${STATE_CAPTURE_PREFIX:-/opt/state-capture-collector}"
STATE="${STATE_CAPTURE_STATE_DIR:-/var/lib/state-capture}"
BIN_SRC="$ROOT/target/release/state-capture"
ENV_DST="$PREFIX/etc/state-capture.env"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi

build_release() {
  local build_user="${SUDO_USER:-}"
  if [[ -n "$build_user" && "$build_user" != "root" ]] && id -u "$build_user" >/dev/null 2>&1; then
    echo "== build release (as $build_user) =="
    sudo -u "$build_user" -H bash -lc "cd \"$ROOT\" && source \"\$HOME/.cargo/env\" 2>/dev/null || true; cargo build --release"
    return
  fi
  echo "missing $BIN_SRC — cargo build --release, then sudo ./deploy/install.sh" >&2
  exit 1
}

if [[ -x "$BIN_SRC" && -z "${FORCE_REBUILD:-}" ]]; then
  echo "== using existing release binary: $BIN_SRC =="
else
  build_release
fi

test -x "$BIN_SRC" || {
  echo "missing $BIN_SRC" >&2
  exit 1
}

echo "== dirs =="
mkdir -p "$PREFIX"/{bin,etc,docs,sql,scripts} \
  "$STATE"/{announce,spool} \
  /run/state \
  /etc/systemd/system

echo "== socket group =="
if ! getent group state-capture >/dev/null; then
  groupadd --system state-capture
fi
for u in faa tails adsb entra; do
  if id "$u" >/dev/null 2>&1; then
    usermod -aG state-capture "$u"
  fi
done

echo "== install files =="
install -m 0755 "$BIN_SRC" "$PREFIX/bin/state-capture"
install -m 0644 "$ROOT/docs/DAILY_OPS.md" "$PREFIX/docs/DAILY_OPS.md"
install -m 0644 "$ROOT/sql/001_capture.sql" "$PREFIX/sql/001_capture.sql"
if [[ ! -f "$ENV_DST" ]]; then
  install -m 0600 "$ROOT/deploy/state-capture.env.example" "$ENV_DST"
fi
chmod 0600 "$ENV_DST"
chmod 0755 "$STATE" "$STATE/announce" "$STATE/spool"

install -m 0644 "$ROOT/deploy/systemd/state-capture-collect.service" /etc/systemd/system/state-capture-collect.service
install -m 0644 "$ROOT/deploy/systemd/state-capture-collect.socket" /etc/systemd/system/state-capture-collect.socket

if command -v restorecon >/dev/null 2>&1; then
  restorecon -Rv "$PREFIX" "$STATE" /run/state || true
fi

systemctl daemon-reload
systemctl enable --now state-capture-collect.socket
systemctl enable --now state-capture-collect.service

echo "installed:"
echo "  prefix=$PREFIX"
echo "  announce=$STATE/announce"
echo "  spool=$STATE/spool"
echo "  sock=/run/state/collect.sock"
echo "  env=$ENV_DST"
echo "  journalctl -u state-capture-collect.service -n 50 --no-pager"
echo "  Watch work sqlite only (never published current/)."
