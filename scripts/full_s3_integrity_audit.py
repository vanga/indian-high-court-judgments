#!/usr/bin/env python3
"""Full S3 integrity audit for the high-court judgment dataset.

The audit is intentionally partition-oriented: year/court/bench/data_type.
It validates object listings, tar indexes, tar part objects, tar members, and
parquet rows without extracting the whole dataset at once.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import tempfile
import tarfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Iterable

import boto3
import pandas as pd


BUCKET = "indian-high-court-judgments"


@dataclass(frozen=True)
class Partition:
    data_type: str
    year: int
    court: str
    bench: str

    @property
    def raw_prefix(self) -> str:
        storage = "json" if self.data_type == "metadata" else "pdf"
        return (
            f"{self.data_type}/{storage}/year={self.year}/"
            f"court={self.court}/bench={self.bench}/"
        )

    @property
    def tar_prefix(self) -> str:
        return (
            f"{self.data_type}/tar/year={self.year}/"
            f"court={self.court}/bench={self.bench}/"
        )

    @property
    def index_key(self) -> str:
        suffix = "metadata.index.json" if self.data_type == "metadata" else "data.index.json"
        return self.tar_prefix + suffix

    @property
    def raw_suffix(self) -> str:
        return ".json" if self.data_type == "metadata" else ".pdf"

    @property
    def index_suffix(self) -> str:
        return "/metadata.index.json" if self.data_type == "metadata" else "/data.index.json"


@dataclass
class IndexPart:
    name: str
    files: list[str]
    size: int | None = None


@dataclass
class PartitionAudit:
    partition: Partition
    raw_count: int = 0
    raw_bytes: int = 0
    index_file_count: int = 0
    index_part_entries: int = 0
    index_distinct_files: int = 0
    index_duplicate_entries: int = 0
    index_parts: int = 0
    indexed_part_objects: int = 0
    unindexed_tar_objects: int = 0
    missing_indexed_part_objects: int = 0
    tar_member_count: int | None = None
    tar_member_distinct: int | None = None
    tar_missing_index_files: int | None = None
    index_missing_tar_files: int | None = None
    parquet_rows: int | None = None
    parquet_distinct_pdf_names: int | None = None
    parquet_minus_data_index: int | None = None
    data_index_minus_parquet: int | None = None
    status: str = "ok"
    notes: list[str] = field(default_factory=list)


def iter_objects(s3, prefix: str) -> Iterable[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def parse_partition_from_index_key(key: str) -> Partition | None:
    parts = PurePosixPath(key).parts
    if len(parts) < 6 or parts[1] != "tar":
        return None
    data_type = parts[0]
    if data_type not in {"metadata", "data"}:
        return None
    try:
        year = int(parts[2].split("=", 1)[1])
        court = parts[3].split("=", 1)[1]
        bench = parts[4].split("=", 1)[1]
    except (IndexError, ValueError):
        return None
    return Partition(data_type=data_type, year=year, court=court, bench=bench)


def discover_partitions(s3, data_type: str | None = None) -> list[Partition]:
    prefixes = ["metadata/tar/", "data/tar/"] if data_type is None else [f"{data_type}/tar/"]
    partitions: set[Partition] = set()
    for prefix in prefixes:
        for obj in iter_objects(s3, prefix):
            key = obj["Key"]
            if key.endswith("/metadata.index.json") or key.endswith("/data.index.json"):
                partition = parse_partition_from_index_key(key)
                if partition:
                    partitions.add(partition)
    return sorted(partitions, key=lambda p: (p.year, p.court, p.bench, p.data_type))


def list_raw_files(s3, partition: Partition) -> tuple[set[str], int]:
    names = set()
    total_bytes = 0
    for obj in iter_objects(s3, partition.raw_prefix):
        key = obj["Key"]
        if key.endswith(partition.raw_suffix):
            names.add(PurePosixPath(key).name)
            total_bytes += obj["Size"]
    return names, total_bytes


def list_tar_objects(s3, partition: Partition) -> dict[str, int]:
    result = {}
    for obj in iter_objects(s3, partition.tar_prefix):
        name = PurePosixPath(obj["Key"]).name
        if name.endswith(".tar") or name.endswith(".tar.gz"):
            result[name] = obj["Size"]
    return result


def load_index(s3, partition: Partition) -> tuple[list[IndexPart], int, bytes | None]:
    try:
        body = s3.get_object(Bucket=BUCKET, Key=partition.index_key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return [], 0, None
    data = json.loads(body)
    parts = [
        IndexPart(
            name=part.get("name", ""),
            files=[PurePosixPath(name).name for name in part.get("files", [])],
            size=part.get("size"),
        )
        for part in data.get("parts", [])
    ]
    return parts, int(data.get("file_count", 0)), body


def tar_member_names(s3, partition: Partition, part: IndexPart, tmp_dir: Path | None) -> set[str]:
    key = partition.tar_prefix + part.name
    suffix = ".tar.gz" if part.name.endswith(".tar.gz") else ".tar"
    with tempfile.NamedTemporaryFile(suffix=suffix, dir=tmp_dir, delete=True) as handle:
        s3.download_fileobj(BUCKET, key, handle)
        handle.flush()
        handle.seek(0)
        mode = "r:gz" if suffix == ".tar.gz" else "r"
        with tarfile.open(fileobj=handle, mode=mode) as tar:
            return {PurePosixPath(member.name).name for member in tar if member.isfile()}


def parquet_pdf_names(s3, year: int, court: str, bench: str) -> tuple[int, set[str]]:
    prefix = f"metadata/parquet/year={year}/court={court}/bench={bench}/"
    rows = 0
    names = set()
    for obj in iter_objects(s3, prefix):
        if not obj["Key"].endswith("/metadata.parquet"):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        df = pd.read_parquet(io.BytesIO(body), columns=["pdf_link"])
        rows += len(df)
        for value in df["pdf_link"].dropna().astype(str):
            name = PurePosixPath(value).name
            if name:
                names.add(name)
    return rows, names


def write_issue(writer, partition: Partition, issue_type: str, detail: str, sample: Iterable[str] = ()):
    writer.writerow(
        {
            "data_type": partition.data_type,
            "year": partition.year,
            "court": partition.court,
            "bench": partition.bench,
            "issue_type": issue_type,
            "detail": detail,
            "sample": ",".join(list(sample)[:20]),
        }
    )


def audit_partition(
    s3,
    partition: Partition,
    summary_writer,
    issue_writer,
    verify_tar_members: bool,
    tmp_dir: Path | None,
) -> PartitionAudit:
    audit = PartitionAudit(partition=partition)
    raw_names, raw_bytes = list_raw_files(s3, partition)
    audit.raw_count = len(raw_names)
    audit.raw_bytes = raw_bytes

    parts, top_file_count, _index_body = load_index(s3, partition)
    audit.index_file_count = top_file_count
    audit.index_parts = len(parts)
    indexed_files = [name for part in parts for name in part.files]
    indexed_set = set(indexed_files)
    audit.index_part_entries = len(indexed_files)
    audit.index_distinct_files = len(indexed_set)
    audit.index_duplicate_entries = len(indexed_files) - len(indexed_set)

    if top_file_count != len(indexed_files):
        audit.notes.append("index_file_count_mismatch")
        write_issue(
            issue_writer,
            partition,
            "index_file_count_mismatch",
            f"file_count={top_file_count} part_entries={len(indexed_files)}",
        )
    if audit.index_duplicate_entries:
        audit.notes.append("index_duplicates")
        write_issue(
            issue_writer,
            partition,
            "index_duplicate_entries",
            str(audit.index_duplicate_entries),
        )

    raw_missing_index = sorted(raw_names - indexed_set)
    index_missing_raw = sorted(indexed_set - raw_names)
    if raw_missing_index:
        audit.notes.append("raw_missing_index")
        write_issue(issue_writer, partition, "raw_missing_index", str(len(raw_missing_index)), raw_missing_index)
    if index_missing_raw:
        audit.notes.append("index_missing_raw")
        write_issue(issue_writer, partition, "index_missing_raw", str(len(index_missing_raw)), index_missing_raw)

    tar_objects = list_tar_objects(s3, partition)
    indexed_part_names = {part.name for part in parts}
    audit.indexed_part_objects = len(indexed_part_names & set(tar_objects))
    missing_part_objects = sorted(indexed_part_names - set(tar_objects))
    unindexed_part_objects = sorted(set(tar_objects) - indexed_part_names)
    audit.missing_indexed_part_objects = len(missing_part_objects)
    audit.unindexed_tar_objects = len(unindexed_part_objects)
    if missing_part_objects:
        audit.notes.append("missing_indexed_part_objects")
        write_issue(issue_writer, partition, "missing_indexed_part_objects", str(len(missing_part_objects)), missing_part_objects)
    if unindexed_part_objects:
        audit.notes.append("unindexed_tar_objects")
        write_issue(issue_writer, partition, "unindexed_tar_objects", str(len(unindexed_part_objects)), unindexed_part_objects)

    size_mismatches = []
    for part in parts:
        actual_size = tar_objects.get(part.name)
        if actual_size is not None and part.size is not None and int(part.size) != int(actual_size):
            size_mismatches.append(f"{part.name}:{part.size}!={actual_size}")
    if size_mismatches:
        audit.notes.append("part_size_mismatch")
        write_issue(issue_writer, partition, "part_size_mismatch", str(len(size_mismatches)), size_mismatches)

    if verify_tar_members:
        tar_members = set()
        for part in parts:
            if part.name not in tar_objects:
                continue
            members = tar_member_names(s3, partition, part, tmp_dir)
            expected = set(part.files)
            missing_in_tar = sorted(expected - members)
            extra_in_tar = sorted(members - expected)
            if missing_in_tar:
                write_issue(issue_writer, partition, "part_index_files_missing_in_tar", part.name, missing_in_tar)
            if extra_in_tar:
                write_issue(issue_writer, partition, "part_tar_members_missing_in_index", part.name, extra_in_tar)
            tar_members.update(members)
        audit.tar_member_count = sum(1 for _ in tar_members)
        audit.tar_member_distinct = len(tar_members)
        audit.tar_missing_index_files = len(tar_members - indexed_set)
        audit.index_missing_tar_files = len(indexed_set - tar_members)
        if audit.tar_missing_index_files:
            audit.notes.append("tar_members_missing_index")
        if audit.index_missing_tar_files:
            audit.notes.append("index_files_missing_tar")

    if partition.data_type == "metadata":
        rows, parquet_names = parquet_pdf_names(s3, partition.year, partition.court, partition.bench)
        audit.parquet_rows = rows
        audit.parquet_distinct_pdf_names = len(parquet_names)
    else:
        rows, parquet_names = parquet_pdf_names(s3, partition.year, partition.court, partition.bench)
        audit.parquet_rows = rows
        audit.parquet_distinct_pdf_names = len(parquet_names)
        data_index_names = indexed_set
        parquet_missing_data_index = sorted(parquet_names - data_index_names)
        data_index_missing_parquet = sorted(data_index_names - parquet_names)
        audit.parquet_minus_data_index = len(parquet_missing_data_index)
        audit.data_index_minus_parquet = len(data_index_missing_parquet)
        if parquet_missing_data_index:
            audit.notes.append("parquet_missing_data_index")
            write_issue(issue_writer, partition, "parquet_missing_data_index", str(len(parquet_missing_data_index)), parquet_missing_data_index)
        if data_index_missing_parquet:
            audit.notes.append("data_index_missing_parquet")
            write_issue(issue_writer, partition, "data_index_missing_parquet", str(len(data_index_missing_parquet)), data_index_missing_parquet)

    if audit.notes:
        audit.status = "issues"

    summary_writer.writerow(
        {
            "data_type": partition.data_type,
            "year": partition.year,
            "court": partition.court,
            "bench": partition.bench,
            "status": audit.status,
            "raw_count": audit.raw_count,
            "raw_bytes": audit.raw_bytes,
            "index_file_count": audit.index_file_count,
            "index_part_entries": audit.index_part_entries,
            "index_distinct_files": audit.index_distinct_files,
            "index_duplicate_entries": audit.index_duplicate_entries,
            "index_parts": audit.index_parts,
            "indexed_part_objects": audit.indexed_part_objects,
            "unindexed_tar_objects": audit.unindexed_tar_objects,
            "missing_indexed_part_objects": audit.missing_indexed_part_objects,
            "tar_member_count": audit.tar_member_count if audit.tar_member_count is not None else "",
            "tar_member_distinct": audit.tar_member_distinct if audit.tar_member_distinct is not None else "",
            "tar_missing_index_files": audit.tar_missing_index_files if audit.tar_missing_index_files is not None else "",
            "index_missing_tar_files": audit.index_missing_tar_files if audit.index_missing_tar_files is not None else "",
            "parquet_rows": audit.parquet_rows if audit.parquet_rows is not None else "",
            "parquet_distinct_pdf_names": audit.parquet_distinct_pdf_names if audit.parquet_distinct_pdf_names is not None else "",
            "parquet_minus_data_index": audit.parquet_minus_data_index if audit.parquet_minus_data_index is not None else "",
            "data_index_minus_parquet": audit.data_index_minus_parquet if audit.data_index_minus_parquet is not None else "",
            "notes": ",".join(audit.notes),
        }
    )
    return audit


def parse_years(value: str | None) -> set[int] | None:
    if not value:
        return None
    years = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = [int(v) for v in part.split("-", 1)]
            years.update(range(start, end + 1))
        else:
            years.add(int(part))
    return years


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-type", choices=["metadata", "data"], default=None)
    parser.add_argument("--years", default=None, help="Comma list/ranges, e.g. 2025,2026 or 1950-2026")
    parser.add_argument("--courts", default=None, help="Comma-separated S3-format court codes")
    parser.add_argument("--verify-tar-members", action="store_true")
    parser.add_argument("--tmp-dir", type=Path, default=None)
    parser.add_argument("--summary-tsv", required=True, type=Path)
    parser.add_argument("--issues-tsv", required=True, type=Path)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    years = parse_years(args.years)
    courts = {c.strip().replace("~", "_") for c in args.courts.split(",")} if args.courts else None

    s3 = boto3.client("s3")
    partitions = discover_partitions(s3, args.data_type)
    if years is not None:
        partitions = [p for p in partitions if p.year in years]
    if courts is not None:
        partitions = [p for p in partitions if p.court in courts]
    if args.limit:
        partitions = partitions[: args.limit]

    args.summary_tsv.parent.mkdir(parents=True, exist_ok=True)
    args.issues_tsv.parent.mkdir(parents=True, exist_ok=True)
    summary_fields = [
        "data_type",
        "year",
        "court",
        "bench",
        "status",
        "raw_count",
        "raw_bytes",
        "index_file_count",
        "index_part_entries",
        "index_distinct_files",
        "index_duplicate_entries",
        "index_parts",
        "indexed_part_objects",
        "unindexed_tar_objects",
        "missing_indexed_part_objects",
        "tar_member_count",
        "tar_member_distinct",
        "tar_missing_index_files",
        "index_missing_tar_files",
        "parquet_rows",
        "parquet_distinct_pdf_names",
        "parquet_minus_data_index",
        "data_index_minus_parquet",
        "notes",
    ]
    issue_fields = ["data_type", "year", "court", "bench", "issue_type", "detail", "sample"]

    with args.summary_tsv.open("w", newline="") as summary_file, args.issues_tsv.open("w", newline="") as issue_file:
        summary_writer = csv.DictWriter(summary_file, fieldnames=summary_fields, delimiter="\t")
        issue_writer = csv.DictWriter(issue_file, fieldnames=issue_fields, delimiter="\t")
        summary_writer.writeheader()
        issue_writer.writeheader()
        for i, partition in enumerate(partitions, start=1):
            print(
                f"[{i}/{len(partitions)}] {partition.data_type} "
                f"year={partition.year} court={partition.court} bench={partition.bench}",
                flush=True,
            )
            audit_partition(
                s3,
                partition,
                summary_writer,
                issue_writer,
                args.verify_tar_members,
                args.tmp_dir,
            )


if __name__ == "__main__":
    main()
