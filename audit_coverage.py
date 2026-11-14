"""Coverage-gap audit for the published dataset.

Reads the row counts from every ``metadata/parquet/.../metadata.parquet`` footer
in the public S3 bucket (anonymous access, no credentials needed) and prints a
per-court / per-bench judgment count by decision-year. Large year-over-year
drops flag courts/benches that have stopped (or sharply reduced) publishing to
the central ``judgments.ecourts.gov.in`` portal that this dataset scrapes.

Usage:
    python audit_coverage.py                 # all courts, table by court
    python audit_coverage.py --benches       # also break down per bench
    python audit_coverage.py --court 27_1    # one court (S3 underscore format)

Only parquet *footers* are read, so this is cheap (a few hundred range requests),
not a full download of the dataset.
"""

import argparse
import json
import re
from collections import defaultdict

import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem

BUCKET = "indian-high-court-judgments"
PREFIX = "metadata/parquet"
REGION = "ap-south-1"
PATH_RE = re.compile(r"year=(\d+)/court=([0-9_]+)/bench=([^/]+)/")


def load_court_names(path="court-codes.json"):
    """court-codes.json maps the eCourts tilde format (``27~1``) to a name; we
    re-key on the S3 underscore format (``27_1``)."""
    try:
        data = json.load(open(path))
    except FileNotFoundError:
        return {}
    return {code.replace("~", "_"): name for code, name in data.items()}


def collect(fs, court_filter=None):
    """Return {(court, bench, year): num_rows} read from parquet footers."""
    selector = pq  # placeholder to keep linters quiet
    from pyarrow.fs import FileSelector

    counts = {}
    info = fs.get_file_info(FileSelector(f"{BUCKET}/{PREFIX}", recursive=True))
    files = [f.path for f in info if f.path.endswith("metadata.parquet")]
    for i, path in enumerate(sorted(files)):
        m = PATH_RE.search(path)
        if not m:
            continue
        year, court, bench = m.group(1), m.group(2), m.group(3)
        if court_filter and court != court_filter:
            continue
        try:
            n = pq.ParquetFile(fs.open_input_file(path)).metadata.num_rows
        except Exception as exc:  # noqa: BLE001
            print(f"  ERR {path}: {exc}")
            n = 0
        counts[(court, bench, year)] = n
        print(f"  [{i + 1}/{len(files)}] {court}/{bench}/{year}: {n}", flush=True)
    return counts


def years_seen(counts):
    return sorted({y for (_, _, y) in counts})


def print_table(counts, names, per_bench=False, show_years=6):
    all_years = years_seen(counts)
    years = all_years[-show_years:]
    if per_bench:
        grouped = defaultdict(lambda: defaultdict(dict))
        for (c, b, y), n in counts.items():
            grouped[c][b][y] = n
        for c in sorted(grouped):
            print(f"\n=== {c} ({names.get(c, '?')}) ===")
            print("bench".ljust(24) + "".join(y.rjust(9) for y in years))
            for b in sorted(grouped[c]):
                row = grouped[c][b]
                print(b.ljust(24) + "".join(str(row.get(y, 0)).rjust(9) for y in years))
        return

    by_court = defaultdict(lambda: defaultdict(int))
    for (c, _, y), n in counts.items():
        by_court[c][y] += n
    print(f"\n=== judgments by decision-year (all benches; showing last {show_years} years) ===")
    print("court".ljust(8) + "name".ljust(24) + "".join(y.rjust(9) for y in years) + "  recent/peak%")
    for c in sorted(by_court):
        cy = by_court[c]
        peak = max(cy.values())  # peak is over all history, not just shown years
        recent = cy.get(all_years[-2], 0) if len(all_years) >= 2 else 0  # last *complete* year
        pct = (recent / peak * 100) if peak else 0.0
        flag = "  <== COLLAPSE" if pct < 25 else ""
        print(
            c.ljust(8)
            + names.get(c, "?")[:23].ljust(24)
            + "".join(str(cy.get(y, 0)).rjust(9) for y in years)
            + f"{pct:>10.1f}%"
            + flag
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--benches", action="store_true", help="break down per bench")
    ap.add_argument("--court", help="limit to one court in S3 underscore format, e.g. 27_1")
    args = ap.parse_args()

    fs = S3FileSystem(anonymous=True, region=REGION)
    names = load_court_names()
    counts = collect(fs, court_filter=args.court)
    print_table(counts, names, per_bench=args.benches or bool(args.court))


if __name__ == "__main__":
    main()
