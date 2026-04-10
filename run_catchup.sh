#!/usr/bin/env bash
# Launcher for the catch-up scrape.
#
# Uses `exec python ...` so that after bash exec, the process that owned the
# bash PID becomes the python process with the SAME PID. This means
# `kill -TERM <catchup.pid>` actually signals the python scraper, not a
# disconnected bash wrapper (the source of today's zombie-process problem).
#
# Usage: ./run_catchup.sh [extra args forwarded to download.py]
#   default args: --sync-s3 --max_workers 8

set -u

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_DIR"

mkdir -p logs

# Rotate any existing active log
if [ -s logs/catchup.log ]; then
  mv logs/catchup.log "logs/catchup.$(date +%Y%m%d-%H%M%S).log"
fi

# Refuse to launch if another scrape is already running
if [ -f logs/catchup.pid ]; then
  OLD_PID=$(cat logs/catchup.pid 2>/dev/null || true)
  if [ -n "${OLD_PID:-}" ] && ps -p "$OLD_PID" > /dev/null 2>&1; then
    echo "ERROR: previous scrape still running (pid $OLD_PID). Stop it first with:" >&2
    echo "  kill -TERM $OLD_PID" >&2
    exit 1
  fi
fi

EXTRA_ARGS=("$@")
if [ "${#EXTRA_ARGS[@]}" -eq 0 ]; then
  EXTRA_ARGS=(--sync-s3 --max_workers 8)
fi

# The start-timestamp line is written BEFORE exec so it lands in the log.
# `exec` replaces bash with python, keeping the PID, so SIGTERM on the
# pidfile's pid hits python directly.
nohup bash -c "
source .venv/bin/activate
export AWS_PROFILE=dattam-od
export AWS_REGION=ap-south-1
echo \"=== catch-up started at \$(date -u +%FT%TZ), args: ${EXTRA_ARGS[*]} ===\"
exec python download.py ${EXTRA_ARGS[*]}
" > logs/catchup.log 2>&1 &
LAUNCH_PID=$!
disown

echo "$LAUNCH_PID" > logs/catchup.pid
echo "Started scrape: pid=$LAUNCH_PID, log=logs/catchup.log"
echo "Stop with: kill -TERM $LAUNCH_PID"
