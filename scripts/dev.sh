#!/bin/sh
# Runs the hub and the admin frontend together for local development.
#   - Hub listens on 127.0.0.1:9911 by default, matching the proxy target
#     Vite forwards /api to in web-admin/vite.config.ts. Override with
#     HUB_LISTEN, e.g. `HUB_LISTEN=0.0.0.0:8080 ./scripts/dev.sh`.
#   - Vite serves the panel on its default port and prints the URL.
#
# First run installs web-admin's packages and compiles the hub (cargo fetches
# the pinned theme through scripts/theme.sh). Ctrl+C stops both processes.
set -eu

cd "$(dirname "$0")/.."

HUB_LISTEN="${HUB_LISTEN:-127.0.0.1:9911}"

HUB_PID=
VITE_PID=
cleanup() {
  trap - EXIT INT TERM
  if [ -n "$VITE_PID" ]; then
    kill -0 "$VITE_PID" 2>/dev/null && kill "$VITE_PID" 2>/dev/null || true
  fi
  if [ -n "$HUB_PID" ]; then
    kill -0 "$HUB_PID" 2>/dev/null && kill "$HUB_PID" 2>/dev/null || true
  fi
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# First run installs web-admin packages; cargo fetches Rust deps on its own.
if [ ! -d web-admin/node_modules ]; then
  echo "installing web-admin dependencies (first run)..."
  (cd web-admin && npm install)
fi

# cargo run drives build.rs, which calls scripts/theme.sh to materialise the
# pinned default theme on first build.
echo "starting hub on $HUB_LISTEN..."
cargo run -- --listen "$HUB_LISTEN" &
HUB_PID=$!

echo "starting vite..."
(cd web-admin && npm run dev) &
VITE_PID=$!

wait
