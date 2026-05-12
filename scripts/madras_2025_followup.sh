#!/usr/bin/env bash
set -euo pipefail

cd /opt/ihcj/repo

primary_log="${1:-$(ls -t logs/madras_2025_backfill_retry_*.log | head -1)}"
followup_log="logs/madras_2025_followup_$(date -u +%Y%m%dT%H%M%SZ).log"
failed_dates_file="logs/madras_2025_failed_dates_$(date -u +%Y%m%dT%H%M%SZ).txt"

exec > >(tee -a "$followup_log") 2>&1

echo "primary_log=$primary_log"
echo "followup_log=$followup_log"
echo "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

while pgrep -f "download.py --court_code 33_10 --start_date 2025-01-01" >/dev/null; do
  echo "waiting_for_primary=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sleep 300
done

echo "primary_finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

grep -ao "from_date=2025-[0-9][0-9]-[0-9][0-9]" "$primary_log" \
  | cut -d= -f2 \
  | sort -u > "$failed_dates_file" || true

failed_count="$(wc -l < "$failed_dates_file" | tr -d ' ')"
echo "failed_dates_file=$failed_dates_file"
echo "failed_count=$failed_count"

if [[ "$failed_count" != "0" ]]; then
  while IFS= read -r d; do
    echo "rerun_date=$d started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    ./.venv/bin/python download.py \
      --court_code 33_10 \
      --start_date "$d" \
      --end_date "$d" \
      --day_step 1 \
      --max_workers 1 \
      --compress-pdfs || echo "rerun_date=$d failed"
    echo "rerun_date=$d finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  done < "$failed_dates_file"
fi

echo "final_sync_started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
./.venv/bin/python - <<'PY'
from datetime import date
import download

download._upload_court_to_s3("33~10", date(2025, 12, 31))
PY
echo "final_sync_finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "final_parquet_count_started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
./.venv/bin/python scripts/count_s3_parquet_window.py \
  --court 33_10 \
  --start-date 2025-01-01 \
  --end-date 2025-12-31
echo "finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
