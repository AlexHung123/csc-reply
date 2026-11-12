#!/bin/sh
set -eu
cd "$(dirname "$0")"

pidfile="$PWD/csc-relay.pid"
logfile="$PWD/csc-relay.log"

if [ -f "$pidfile" ]; then
  pid="$(cat "$pidfile" 2>/dev/null || true)"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "csc-relay already running (pid $pid)"
    exit 0
  fi
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found" >&2
  exit 1
fi

nohup python3 server.py >>"$logfile" 2>&1 &
echo $! >"$pidfile"
echo "csc-relay started (pid $(cat "$pidfile"))"
echo "log: $logfile"
