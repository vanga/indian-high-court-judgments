"""Re-fetching PDFs whose stored copy is not a PDF.

Before download.py checked that a response body starts with ``%PDF``, the
portal's error pages (HTML "page not found", JSON ``{"msg":"error",...}``,
short text errors) were saved under the judgment's filename and uploaded as if
they were the judgment. Every skip check in the pipeline is by filename, so
those placeholders are never fetched again.

This module turns a list of such ``data/pdf/`` keys into a targeted re-fetch:
which courts and dates to search, which search rows to download, and a report
of what happened to each key. download.py does the scraping and uploading.
"""

import csv
import re
import threading
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from src.utils.court_utils import from_s3_format, to_s3_format


OUTCOME_REFETCHED = "refetched"
OUTCOME_STILL_NOT_PDF = "still_not_pdf"
OUTCOME_NOT_FOUND = "not_found_in_search"
OUTCOME_ERROR = "error"
OUTCOME_UNSUPPORTED = "unsupported"

# When the same key is seen more than once in a run (overlapping windows, the
# re-read of a search page after a session refresh, a retried task), the most
# conclusive result is kept.
_OUTCOME_RANK = {
    OUTCOME_ERROR: 1,
    OUTCOME_STILL_NOT_PDF: 2,
    OUTCOME_REFETCHED: 3,
}

# download_pdf() failure reasons that mean "the portal answered, and what it
# served for this file is still not a PDF". Anything else is an error that a
# later run may get past.
STILL_NOT_PDF_REASONS = {"empty", "not_found_page", "not_pdf", "http_404"}

REPORT_FIELDS = ["key", "outcome", "detail", "search_date", "local_path", "uploaded"]

_HC_PDF_KEY_RE = re.compile(
    r"^data/pdf/year=(?P<year>\d{4})/court=(?P<court>\d+_\d+)/"
    r"bench=(?P<bench>[^/]+)/(?P<filename>[^/]+\.pdf)$"
)
_FILENAME_DATE_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})\.pdf$", re.IGNORECASE)


@dataclass(frozen=True)
class RefetchTarget:
    """One placeholder object to replace, parsed from its S3 key."""

    key: str
    year: int
    court_code: str  # S3 format, e.g. 27_1
    bench: str
    filename: str
    # Date to search the portal for. The portal date filter matches the
    # Decision Date in the result row (see docs/dataset-reconciliation-notes.md).
    search_date: date

    @property
    def ecourts_court_code(self) -> str:
        return from_s3_format(self.court_code)


@dataclass
class RefetchResult:
    target: RefetchTarget
    outcome: str
    detail: str = ""
    local_path: Optional[Path] = None
    uploaded: bool = False


def filename_date(filename: str) -> Optional[date]:
    """The trailing ``_YYYY-MM-DD.pdf`` date of an eCourts filename, if valid."""
    match = _FILENAME_DATE_RE.search(filename)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def normalize_key(raw: str) -> str:
    """Accept bare keys as well as ``s3://bucket/key`` URIs."""
    key = raw.strip()
    if key.startswith("s3://"):
        _bucket, _sep, key = key[len("s3://"):].partition("/")
    return key.lstrip("/")


def parse_refetch_key(
    raw_key: str, search_date: Optional[str] = None
) -> Tuple[Optional[RefetchTarget], str]:
    """Parse one high-court ``data/pdf/`` key into a RefetchTarget.

    Returns (target, "") on success, or (None, reason) when the key cannot be
    re-fetched by this scraper (e.g. a Supreme Court key, or no usable date).
    The search date is ``search_date`` if given (YYYY-MM-DD), otherwise the
    date at the end of the filename.
    """
    key = normalize_key(raw_key)
    match = _HC_PDF_KEY_RE.match(key)
    if not match:
        return None, "not a high-court data/pdf/year=/court=/bench=/ key"

    filename = match.group("filename")
    if search_date:
        try:
            when = datetime.strptime(search_date.strip(), "%Y-%m-%d").date()
        except ValueError:
            return None, f"invalid search_date {search_date!r}"
    else:
        when = filename_date(filename)
        if when is None:
            return None, "no YYYY-MM-DD date in filename and no search_date given"

    return (
        RefetchTarget(
            key=key,
            year=int(match.group("year")),
            court_code=match.group("court"),
            bench=match.group("bench"),
            filename=filename,
            search_date=when,
        ),
        "",
    )


def parse_refetch_list(
    path: str | Path,
) -> Tuple[List[RefetchTarget], List[Tuple[str, str]]]:
    """Read a re-fetch list.

    Two formats are accepted:
    - CSV with a ``key`` column (e.g. the output of
      scripts/find_placeholder_pdfs.py). An optional ``search_date`` column
      (YYYY-MM-DD) overrides the date taken from the filename.
    - Plain text, one key per line. Blank lines and ``#`` comments are ignored.

    Returns (targets, unsupported) where unsupported is a list of
    (key, reason). Duplicate keys are kept once.
    """
    with open(path, newline="", encoding="utf-8") as f:
        text = f.read()

    lines = text.splitlines()
    first = next((line for line in lines if line.strip()), "")
    header = [col.strip().lower() for col in next(csv.reader([first]))] if first else []

    rows: List[Tuple[str, Optional[str]]] = []
    if "key" in header:
        reader = csv.DictReader(lines)
        reader.fieldnames = [name.strip().lower() for name in reader.fieldnames]
        for row in reader:
            key = (row.get("key") or "").strip()
            if key:
                rows.append((key, (row.get("search_date") or "").strip() or None))
    else:
        for line in lines:
            line = line.strip()
            if line and not line.startswith("#"):
                rows.append((line, None))

    targets: List[RefetchTarget] = []
    unsupported: List[Tuple[str, str]] = []
    seen = set()
    for raw_key, search_date in rows:
        key = normalize_key(raw_key)
        if key in seen:
            continue
        seen.add(key)
        target, reason = parse_refetch_key(key, search_date)
        if target is None:
            unsupported.append((key, reason))
        else:
            targets.append(target)
    return targets, unsupported


def search_window(target: RefetchTarget, pad_days: int = 0) -> Tuple[date, date]:
    pad = timedelta(days=max(pad_days, 0))
    return target.search_date - pad, target.search_date + pad


def dates_to_ranges(dates: Iterable[date], day_step: int = 1) -> List[Tuple[str, str]]:
    """Collapse dates into (from, to) YYYY-MM-DD ranges of consecutive days.

    A range never spans more than ``day_step`` days, matching how
    get_date_ranges_to_process() chunks a normal run. Gaps are never bridged,
    so no date that is not asked for is searched.
    """
    day_step = max(day_step, 1)
    ranges: List[Tuple[str, str]] = []
    start = prev = None
    for current in sorted(set(dates)):
        if (
            start is not None
            and current == prev + timedelta(days=1)
            and (current - start).days < day_step
        ):
            prev = current
            continue
        if start is not None:
            ranges.append((start.isoformat(), prev.isoformat()))
        start = prev = current
    if start is not None:
        ranges.append((start.isoformat(), prev.isoformat()))
    return ranges


def court_date_ranges(
    targets: Iterable[RefetchTarget], day_step: int = 1, pad_days: int = 0
) -> Dict[str, List[Tuple[str, str]]]:
    """Search ranges per court (eCourts ``XX~YY`` format) for the targets."""
    dates_by_court: Dict[str, set] = {}
    for target in targets:
        start, end = search_window(target, pad_days)
        court_dates = dates_by_court.setdefault(target.ecourts_court_code, set())
        day = start
        while day <= end:
            court_dates.add(day)
            day += timedelta(days=1)
    return {
        court: dates_to_ranges(dates, day_step)
        for court, dates in sorted(dates_by_court.items())
    }


def classify_download_failure(reason: Optional[str]) -> str:
    return OUTCOME_STILL_NOT_PDF if reason in STILL_NOT_PDF_REASONS else OUTCOME_ERROR


def is_pdf_file(path: str | Path) -> bool:
    """True if the file exists and starts with the PDF magic bytes."""
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"%PDF"
    except OSError:
        return False


class RefetchTracker:
    """Which search rows to download, and what happened to each listed key.

    Shared by every worker thread of a re-fetch run, so all state is guarded
    by a lock. Rows are matched on (court, bench, filename): the same
    filename can exist under two benches of one court (e.g. Bombay newos and
    newos_spl), and only the listed one is replaced.
    """

    def __init__(
        self,
        targets: Iterable[RefetchTarget],
        unsupported: Iterable[Tuple[str, str]] = (),
        pad_days: int = 0,
    ):
        self._lock = threading.Lock()
        self.pad_days = pad_days
        self.targets: List[RefetchTarget] = list(targets)
        self.unsupported: List[Tuple[str, str]] = list(unsupported)
        self._by_match: Dict[Tuple[str, str, str], RefetchTarget] = {
            (t.court_code, t.bench, t.filename): t for t in self.targets
        }
        self._results: Dict[str, RefetchResult] = {}

    def lookup(self, court_code: str, bench: Optional[str], filename: str) -> Optional[RefetchTarget]:
        if not bench:
            return None
        return self._by_match.get((to_s3_format(court_code), bench, filename))

    def result_for(self, target: RefetchTarget) -> Optional[RefetchResult]:
        with self._lock:
            return self._results.get(target.key)

    def record(
        self,
        target: RefetchTarget,
        outcome: str,
        detail: str = "",
        local_path: Optional[Path] = None,
    ) -> None:
        """Record an outcome, keeping the most conclusive one seen so far."""
        with self._lock:
            current = self._results.get(target.key)
            if current is not None and (
                _OUTCOME_RANK.get(current.outcome, 0) > _OUTCOME_RANK.get(outcome, 0)
            ):
                return
            if current is not None and current.outcome == outcome == OUTCOME_REFETCHED:
                return
            self._results[target.key] = RefetchResult(
                target, outcome, detail, Path(local_path) if local_path else None
            )

    def mark_search_failed(
        self, court_code: str, from_date: str, to_date: str, detail: str
    ) -> None:
        """Record an error for unresolved targets the failed search covered."""
        court = to_s3_format(court_code)
        start = datetime.strptime(from_date, "%Y-%m-%d").date()
        end = datetime.strptime(to_date, "%Y-%m-%d").date()
        for target in self.targets:
            if target.court_code != court:
                continue
            window_start, window_end = search_window(target, self.pad_days)
            if window_end < start or window_start > end:
                continue
            self.record(target, OUTCOME_ERROR, detail)

    def pending_uploads(self) -> List[RefetchResult]:
        with self._lock:
            return [
                r
                for r in self._results.values()
                if r.outcome == OUTCOME_REFETCHED and not r.uploaded
            ]

    def mark_uploaded(self, results: Iterable[RefetchResult]) -> None:
        with self._lock:
            for result in results:
                result.uploaded = True

    def rows(self) -> List[Dict[str, str]]:
        """One report row per listed key. Unseen targets are not_found_in_search."""
        with self._lock:
            rows = []
            for target in self.targets:
                result = self._results.get(target.key)
                if result is None:
                    window_start, window_end = search_window(target, self.pad_days)
                    window = (
                        window_start.isoformat()
                        if window_start == window_end
                        else f"{window_start.isoformat()}..{window_end.isoformat()}"
                    )
                    result = RefetchResult(
                        target,
                        OUTCOME_NOT_FOUND,
                        f"no search row for this bench/filename in {window}",
                    )
                rows.append(
                    {
                        "key": target.key,
                        "outcome": result.outcome,
                        "detail": result.detail,
                        "search_date": target.search_date.isoformat(),
                        "local_path": str(result.local_path) if result.local_path else "",
                        "uploaded": "true" if result.uploaded else "false",
                    }
                )
            for key, reason in self.unsupported:
                rows.append(
                    {
                        "key": key,
                        "outcome": OUTCOME_UNSUPPORTED,
                        "detail": reason,
                        "search_date": "",
                        "local_path": "",
                        "uploaded": "false",
                    }
                )
            return rows

    def summary(self) -> Counter:
        return Counter(row["outcome"] for row in self.rows())

    def write_report(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=REPORT_FIELDS)
            writer.writeheader()
            writer.writerows(self.rows())
        return path
