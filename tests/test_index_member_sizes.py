import importlib.util
import io
import json
import os
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError

from src.models import IndexFileV2, IndexMember, IndexPart
from src.utils import s3_utils

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "backfill_index_member_sizes.py"
_spec = importlib.util.spec_from_file_location("backfill_index_member_sizes", _SCRIPT)
backfill = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = backfill  # dataclasses resolve annotations via sys.modules
_spec.loader.exec_module(backfill)

BENCH = "year=2008/court=27_1/bench=kolhcdb/"
INDEX_KEY = "data/tar/" + BENCH + "data.index.json"
TAR_PREFIX = "data/tar/" + BENCH
PDF_PREFIX = "data/pdf/" + BENCH


class FakeS3:
    """Enough of the boto3 client for index writes, listings and conditional PUTs."""

    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self):
        self.objects = {}
        self.versions = {}
        self.puts = []

    def _store(self, key, data):
        self.objects[key] = data
        self.versions[key] = self.versions.get(key, 0) + 1

    def get_object(self, Bucket, Key, Range=None):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey(Key)
        data = self.objects[Key]
        if Range:
            start, end = Range.split("=")[1].split("-")
            data = data[int(start): int(end) + 1]
        return {"Body": io.BytesIO(data), "ETag": f'"v{self.versions[Key]}"'}

    def put_object(self, Bucket, Key, Body, ContentType=None, IfMatch=None):
        if IfMatch is not None and IfMatch != f'"v{self.versions.get(Key)}"':
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.puts.append(Key)
        self._store(Key, Body.encode() if isinstance(Body, str) else Body.read())

    def upload_file(self, path, bucket, key, ExtraArgs=None, Config=None):
        self._store(key, Path(path).read_bytes())

    def download_file(self, bucket, key, path):
        Path(path).write_bytes(self.objects[key])

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def get_paginator(self, name):
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                yield {
                    "Contents": [
                        {"Key": k, "Size": len(v)}
                        for k, v in sorted(fake.objects.items())
                        if k.startswith(Prefix)
                    ]
                }

        return Paginator()


def index_with(*parts):
    return IndexFileV2(
        file_count=sum(len(p.files) for p in parts),
        tar_size=sum(p.size for p in parts),
        tar_size_human="",
        updated_at="2024-01-01T00:00:00Z",
        parts=list(parts),
    )


class S3TestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        prev = os.getcwd()
        os.chdir(tmp.name)
        self.addCleanup(os.chdir, prev)
        self.s3 = FakeS3()
        patcher = patch.object(s3_utils, "s3_client", self.s3)
        patcher.start()
        self.addCleanup(patcher.stop)

    def put_index(self, index):
        self.s3._store(INDEX_KEY, index.model_dump_json(indent=2).encode())

    def index(self):
        return IndexFileV2.model_validate(json.loads(self.s3.objects[INDEX_KEY]))

    def raw_parts(self):
        return json.loads(self.s3.objects[INDEX_KEY])["parts"]


class ModelTests(unittest.TestCase):
    def test_part_without_sizes_serializes_without_members_key(self):
        part = IndexPart(name="p.tar", files=["a.pdf"], size=1, created_at="2024-01-01T00:00:00Z")
        self.assertNotIn("members", json.loads(part.model_dump_json()))

    def test_members_that_disagree_with_files_are_dropped(self):
        part = IndexPart.model_validate(
            {
                "name": "p.tar",
                "files": ["a.pdf", "b.pdf"],
                "created_at": "2024-01-01T00:00:00Z",
                "members": [{"name": "b.pdf", "size": 2}, {"name": "a.pdf", "size": 1}],
            }
        )
        self.assertIsNone(part.members)
        self.assertEqual(part.files, ["a.pdf", "b.pdf"])

    def test_old_readers_still_see_files_list_unchanged(self):
        part = IndexPart(
            name="p.tar", files=["a.pdf"], created_at="2024-01-01T00:00:00Z",
            members=[IndexMember(name="a.pdf", size=7)],
        )
        raw = json.loads(part.model_dump_json())
        self.assertEqual(raw["files"], ["a.pdf"])
        self.assertEqual(raw["members"], [{"name": "a.pdf", "size": 7}])


class WritePathTests(S3TestCase):
    def test_new_part_records_each_file_size(self):
        Path("a.pdf").write_bytes(b"%PDF-aaaa")
        Path("b.pdf").write_bytes(b"%PDF-b")

        s3_utils.create_and_upload_tar_file("data", 2008, "27_1", "kolhcdb", [Path("a.pdf"), Path("b.pdf")])

        (part,) = self.raw_parts()
        self.assertEqual(
            {m["name"]: m["size"] for m in part["members"]}, {"a.pdf": 9, "b.pdf": 6}
        )
        self.assertEqual([m["name"] for m in part["members"]], part["files"])

    def test_append_to_legacy_part_takes_carried_sizes_from_the_tar(self):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            body = b"%PDF old file"
            info = tarfile.TarInfo("old.pdf")
            info.size = len(body)
            tar.addfile(info, io.BytesIO(body))
        self.s3._store(TAR_PREFIX + "part-old.tar", buf.getvalue())
        target = IndexPart(name="part-old.tar", files=["old.pdf"], size=len(buf.getvalue()),
                           created_at="2024-01-01T00:00:00Z")
        self.put_index(index_with(target))
        Path("new.pdf").write_bytes(b"%PDF new")

        ok = s3_utils._append_to_existing_part(
            "data", 2008, "27_1", "kolhcdb", [Path("new.pdf")], target,
            TAR_PREFIX, ".tar", "w", "application/x-tar",
        )

        self.assertTrue(ok)
        (part,) = self.index().parts
        self.assertEqual(part.files, ["old.pdf", "new.pdf"])
        self.assertEqual([(m.name, m.size) for m in part.members], [("old.pdf", 13), ("new.pdf", 8)])

    def test_refetch_moves_sizes_with_names(self):
        self.put_index(index_with(
            IndexPart(name="part-1.tar", files=["a.pdf", "b.pdf"], size=10,
                      created_at="2024-01-01T00:00:00Z",
                      members=[IndexMember(name="a.pdf", size=124), IndexMember(name="b.pdf", size=5000)]),
        ))
        Path("a.pdf").write_bytes(b"%PDF-1.4 fixed")

        s3_utils.upload_replacement_tar_part("data", 2008, "27_1", "kolhcdb", [Path("a.pdf")])

        old, new = self.index().parts
        self.assertEqual(old.files, ["b.pdf"])
        self.assertEqual([(m.name, m.size) for m in old.members], [("b.pdf", 5000)])
        self.assertEqual([(m.name, m.size) for m in new.members], [("a.pdf", 14)])


class BackfillTests(S3TestCase):
    def setUp(self):
        super().setUp()
        self.s3._store(PDF_PREFIX + "a.pdf", b"%PDF" + b"x" * 2000)
        self.s3._store(PDF_PREFIX + "b.pdf", b"<html>404</html>")
        self.s3._store(PDF_PREFIX + "c.pdf", b"%PDF" + b"y" * 3000)
        self.put_index(index_with(
            IndexPart(name="part-1.tar", files=["a.pdf", "b.pdf"], size=10, created_at="2024-01-01T00:00:00Z"),
            IndexPart(name="part-2.tar", files=["c.pdf", "gone.pdf"], size=10, created_at="2024-02-01T00:00:00Z"),
        ))
        self.before = self.s3.objects[INDEX_KEY]

    def run_backfill(self, apply, **kw):
        return backfill.backfill_index(self.s3, "bucket", INDEX_KEY, apply, **kw)

    def test_dry_run_reports_but_does_not_write(self):
        r = self.run_backfill(apply=False)
        self.assertEqual((r.status, r.parts_filled, r.parts_unresolved), ("would_update", 1, 1))
        self.assertEqual(self.s3.objects[INDEX_KEY], self.before)

    def test_apply_fills_complete_parts_and_leaves_incomplete_ones(self):
        r = self.run_backfill(apply=True)

        self.assertEqual(r.status, "updated")
        self.assertEqual(r.names_without_object, 1)
        p1, p2 = self.index().parts
        self.assertEqual([(m.name, m.size) for m in p1.members], [("a.pdf", 2004), ("b.pdf", 16)])
        self.assertIsNone(p2.members)
        self.assertEqual(self.index().updated_at, "2024-01-01T00:00:00Z")

    def test_second_run_is_a_no_op(self):
        self.run_backfill(apply=True)
        puts = len(self.s3.puts)
        r = self.run_backfill(apply=True)
        self.assertEqual(r.status, "unchanged")
        self.assertEqual(len(self.s3.puts), puts)

    def test_concurrent_index_write_is_not_overwritten(self):
        real_get = self.s3.get_object

        def get_then_pipeline_writes(Bucket, Key, Range=None):
            resp = real_get(Bucket, Key, Range)
            if Key == INDEX_KEY:
                self.s3._store(INDEX_KEY, b'{"updated_at": "2026-01-01T00:00:00Z", "parts": []}')
            return resp

        with patch.object(self.s3, "get_object", side_effect=get_then_pipeline_writes):
            r = self.run_backfill(apply=True)

        self.assertEqual(r.status, "conflict")
        self.assertEqual(json.loads(self.s3.objects[INDEX_KEY])["parts"], [])

    def test_collects_placeholder_candidates_from_the_same_listing(self):
        r = self.run_backfill(apply=False, collect_placeholders=True, verify_magic=True)
        self.assertEqual([p["key"] for p in r.placeholders], [PDF_PREFIX + "b.pdf"])


if __name__ == "__main__":
    unittest.main()
