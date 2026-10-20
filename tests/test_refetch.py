import csv
import io
import json
import os
import tarfile
import tempfile
import unittest
from contextlib import ExitStack
from datetime import date
from pathlib import Path
from unittest.mock import patch

import download
from src.models import IndexFileV2, IndexPart
from src.utils import refetch_utils, s3_utils
from src.utils.refetch_utils import (
    OUTCOME_ERROR,
    OUTCOME_NOT_FOUND,
    OUTCOME_REFETCHED,
    OUTCOME_STILL_NOT_PDF,
    OUTCOME_UNSUPPORTED,
    RefetchTracker,
    parse_refetch_key,
    parse_refetch_list,
)


BOMBAY_KEY = (
    "data/pdf/year=2008/court=27_1/bench=kolhcdb/HCBM070000012008_1_2008-04-28.pdf"
)
BOMBAY_FRAGMENT = "court/cnrorders/kolhcdb/orders/2008/HCBM070000012008_1_2008-04-28.pdf"
COURTS = {
    "27~1": "Bombay High Court",
    "28~2": "High Court of Andhra Pradesh",
    "33~10": "Madras High Court",
}


def _row(fragment):
    html = (
        "<button onclick=\"javascript:open_pdf('1','x','"
        + fragment
        + "#page=')\">Open</button><strong>Judge: Test</strong>"
    )
    return [None, html]


class TempCwdTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.prev_cwd = os.getcwd()
        os.chdir(self.tmpdir.name)
        self.addCleanup(os.chdir, self.prev_cwd)


class ParseRefetchListTests(TempCwdTestCase):
    def test_parses_key_into_court_bench_filename_and_filename_date(self):
        target, reason = parse_refetch_key(BOMBAY_KEY)

        self.assertEqual(reason, "")
        self.assertEqual(target.year, 2008)
        self.assertEqual(target.court_code, "27_1")
        self.assertEqual(target.ecourts_court_code, "27~1")
        self.assertEqual(target.bench, "kolhcdb")
        self.assertEqual(target.filename, "HCBM070000012008_1_2008-04-28.pdf")
        self.assertEqual(target.search_date, date(2008, 4, 28))

    def test_csv_with_key_column_and_search_date_override(self):
        Path("list.csv").write_text(
            "bucket,key,bytes,search_date\n"
            f"indian-high-court-judgments,{BOMBAY_KEY},124,\n"
            "indian-high-court-judgments,data/pdf/year=1993/court=27_1/bench=newos/"
            "HCBM060000601993_2_2014-06-15.pdf,258,1993-03-02\n"
            # Duplicate rows collapse to one target.
            f"indian-high-court-judgments,{BOMBAY_KEY},124,\n"
            "indian-supreme-court-judgments,data/pdf/year=1996/english/"
            "S_1996_2_866_868_EN.pdf,199,\n"
        )

        targets, unsupported = parse_refetch_list("list.csv")

        self.assertEqual([t.key for t in targets][0], BOMBAY_KEY)
        self.assertEqual(len(targets), 2)
        self.assertEqual(targets[1].search_date, date(1993, 3, 2))
        self.assertEqual(len(unsupported), 1)
        self.assertIn("S_1996_2_866_868_EN.pdf", unsupported[0][0])

    def test_plain_list_with_comments_and_s3_uris(self):
        Path("list.txt").write_text(
            "# placeholders\n\n"
            f"s3://indian-high-court-judgments/{BOMBAY_KEY}\n"
            "data/pdf/year=2024/court=7_26/bench=dhcdb/no_date_here.pdf\n"
        )

        targets, unsupported = parse_refetch_list("list.txt")

        self.assertEqual([t.key for t in targets], [BOMBAY_KEY])
        self.assertEqual(unsupported[0][0], "data/pdf/year=2024/court=7_26/bench=dhcdb/no_date_here.pdf")
        self.assertIn("no YYYY-MM-DD date", unsupported[0][1])

    def test_invalid_filename_date_is_unsupported_not_a_crash(self):
        target, reason = parse_refetch_key(
            "data/pdf/year=2017/court=36_29/bench=taphc/HBHC010262202017_1_2047-02-30.pdf"
        )
        self.assertIsNone(target)
        self.assertIn("no YYYY-MM-DD", reason)


class RefetchTaskGenerationTests(unittest.TestCase):
    def _tracker(self, keys, pad_days=0):
        targets = [parse_refetch_key(k)[0] for k in keys]
        return RefetchTracker(targets, pad_days=pad_days)

    def test_consecutive_dates_merge_up_to_day_step_and_gaps_are_never_bridged(self):
        dates = [date(2008, 4, d) for d in (1, 2, 3, 4, 10)]
        self.assertEqual(
            refetch_utils.dates_to_ranges(dates, day_step=1),
            [(f"2008-04-{d:02d}", f"2008-04-{d:02d}") for d in (1, 2, 3, 4, 10)],
        )
        self.assertEqual(
            refetch_utils.dates_to_ranges(dates, day_step=3),
            [("2008-04-01", "2008-04-03"), ("2008-04-04", "2008-04-04"), ("2008-04-10", "2008-04-10")],
        )

    def test_tasks_cover_only_listed_court_dates(self):
        tracker = self._tracker(
            [
                BOMBAY_KEY,
                "data/pdf/year=2008/court=27_1/bench=newos/X_1_2008-04-28.pdf",
                "data/pdf/year=2019/court=28_2/bench=aphc/APHC1_1_2019-07-01.pdf",
            ]
        )
        with patch.object(download, "get_court_codes", return_value=COURTS):
            tasks = download.generate_refetch_tasks(tracker)

        self.assertEqual(
            sorted((t.court_code, t.from_date, t.to_date) for t in tasks),
            [("27~1", "2008-04-28", "2008-04-28"), ("28~2", "2019-07-01", "2019-07-01")],
        )
        self.assertTrue(all(t.refetch is tracker for t in tasks))

    def test_pad_days_widen_the_window_and_default_bench_splits_apply(self):
        tracker = self._tracker(
            ["data/pdf/year=2020/court=33_10/bench=hc_cis_mas/M_1_2020-01-10.pdf"],
            pad_days=1,
        )
        with patch.object(download, "get_court_codes", return_value=COURTS):
            tasks = download.generate_refetch_tasks(tracker, day_step=7)

        self.assertEqual(
            sorted((t.from_date, t.to_date, t.dist_code) for t in tasks),
            [("2020-01-09", "2020-01-11", "1"), ("2020-01-09", "2020-01-11", "2")],
        )

    def test_unknown_court_is_rejected(self):
        tracker = self._tracker(["data/pdf/year=2020/court=99_99/bench=x/A_1_2020-01-10.pdf"])
        with patch.object(download, "get_court_codes", return_value=COURTS):
            with self.assertRaises(ValueError):
                download.generate_refetch_tasks(tracker)


class RefetchRowTests(TempCwdTestCase):
    def setUp(self):
        super().setUp()
        self.tracker = RefetchTracker([parse_refetch_key(BOMBAY_KEY)[0]])
        task = download.CourtDateTask(
            "27~1", "2008-04-28", "2008-04-28", refetch=self.tracker
        )
        with patch.object(download, "get_court_codes", return_value=COURTS):
            self.downloader = download.Downloader(task)
        self.pdf_path = Path("data") / BOMBAY_FRAGMENT

    def test_unlisted_rows_are_skipped_without_download_s3_lookup_or_metadata(self):
        other = "court/cnrorders/kolhcdb/orders/2008/OTHER_1_2008-04-28.pdf"
        # Same filename, different bench of the same court: not listed either.
        other_bench = "court/cnrorders/newos/orders/2008/HCBM070000012008_1_2008-04-28.pdf"
        with (
            patch.object(self.downloader, "download_pdf") as dl,
            patch.object(self.downloader, "check_result_in_s3") as s3_check,
        ):
            outcomes = [
                self.downloader.process_result_row(_row(other), 0),
                self.downloader.process_result_row(_row(other_bench), 1),
            ]

        self.assertEqual(outcomes, ["refetch_skipped", "refetch_skipped"])
        dl.assert_not_called()
        s3_check.assert_not_called()
        self.assertFalse(Path("data").exists(), "no metadata may be written")

    def test_listed_row_is_downloaded_even_though_s3_index_has_it(self):
        def fake_download(fragment, _pos):
            path = self.downloader.get_pdf_output_path(fragment)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"%PDF-1.4 real")
            return True

        with (
            patch.object(self.downloader, "check_result_in_s3", return_value=(True, True)),
            patch.object(download.cache_store, "get", return_value=[self.pdf_path.name]),
            patch.object(self.downloader, "download_pdf", side_effect=fake_download) as dl,
        ):
            outcome = self.downloader.process_result_row(_row(BOMBAY_FRAGMENT), 7)

        self.assertEqual(outcome, "downloaded")
        dl.assert_called_once_with(BOMBAY_FRAGMENT, 7)
        result = self.tracker.result_for(self.tracker.targets[0])
        self.assertEqual(result.outcome, OUTCOME_REFETCHED)
        self.assertEqual(result.local_path, self.pdf_path)
        metadata = json.loads(self.pdf_path.with_suffix(".json").read_text())
        self.assertTrue(metadata["downloaded"])

    def test_page_reread_after_session_refresh_does_not_download_again(self):
        with patch.object(self.downloader, "download_pdf", return_value=True) as dl:
            first = self.downloader.process_result_row(_row(BOMBAY_FRAGMENT), 0)
            second = self.downloader.process_result_row(_row(BOMBAY_FRAGMENT), 0)

        self.assertEqual((first, second), ("downloaded", "skip_local"))
        self.assertEqual(dl.call_count, 1)

    def test_stale_local_placeholder_is_discarded_and_real_local_pdf_reused(self):
        self.pdf_path.parent.mkdir(parents=True, exist_ok=True)
        self.pdf_path.write_bytes(b"Welcome User Search Page not Found")
        self.pdf_path.with_suffix(".json").write_text('{"downloaded": true}')

        with patch.object(self.downloader, "download_pdf", return_value=False) as dl:
            self.downloader.last_pdf_failure = ("not_pdf", "")
            self.downloader.process_result_row(_row(BOMBAY_FRAGMENT), 0)
        dl.assert_called_once()
        self.assertFalse(self.pdf_path.exists())

        # A real PDF left by an earlier local run is reused without a download.
        tracker = RefetchTracker([parse_refetch_key(BOMBAY_KEY)[0]])
        self.downloader.task.refetch = tracker
        self.pdf_path.write_bytes(b"%PDF-1.7 earlier run")
        with patch.object(self.downloader, "download_pdf") as dl:
            outcome = self.downloader.process_result_row(_row(BOMBAY_FRAGMENT), 0)
        dl.assert_not_called()
        self.assertEqual(outcome, "skip_local")
        self.assertEqual(tracker.result_for(tracker.targets[0]).outcome, OUTCOME_REFETCHED)


class RefetchOutcomeTests(TempCwdTestCase):
    """A re-downloaded file still has to pass download_pdf's checks."""

    def _attempt(self, link_json, status_code=200, content=b"", raise_exc=None):
        tracker = RefetchTracker([parse_refetch_key(BOMBAY_KEY)[0]])
        task = download.CourtDateTask("27~1", "2008-04-28", "2008-04-28", refetch=tracker)
        with patch.object(download, "get_court_codes", return_value=COURTS):
            downloader = download.Downloader(task)
        downloader.session_id = "s"
        downloader.ecourts_token = "t"

        class LinkResponse:
            def json(self):
                return link_json

        class PdfResponse:
            pass

        PdfResponse.status_code = status_code
        PdfResponse.content = content
        request_kwargs = (
            {"side_effect": raise_exc} if raise_exc else {"return_value": PdfResponse()}
        )
        with (
            patch.object(downloader, "request_api", return_value=LinkResponse()),
            patch.object(download.requests, "request", **request_kwargs),
        ):
            try:
                downloader.process_result_row(_row(BOMBAY_FRAGMENT), 0)
            except Exception:
                pass
        return tracker.result_for(tracker.targets[0])

    def test_valid_pdf_is_refetched(self):
        result = self._attempt({"outputfile": "/x.pdf"}, content=b"%PDF-1.4 ok")
        self.assertEqual(result.outcome, OUTCOME_REFETCHED)

    def test_html_error_page_is_still_not_pdf(self):
        result = self._attempt(
            {"outputfile": "/x.pdf"}, content=b"Welcome User Search Page not Found"
        )
        self.assertEqual(result.outcome, OUTCOME_STILL_NOT_PDF)
        self.assertIn("not_pdf", result.detail)
        self.assertFalse((Path("data") / BOMBAY_FRAGMENT).exists())

    def test_315_byte_not_found_page_is_still_not_pdf(self):
        result = self._attempt({"outputfile": "/x.pdf"}, content=b"x" * 315)
        self.assertEqual(result.outcome, OUTCOME_STILL_NOT_PDF)

    def test_http_404_is_still_not_pdf_but_500_is_error(self):
        self.assertEqual(
            self._attempt({"outputfile": "/x.pdf"}, 404, b"<html>404</html>").outcome,
            OUTCOME_STILL_NOT_PDF,
        )
        self.assertEqual(
            self._attempt({"outputfile": "/x.pdf"}, 500, b"<html>500</html>").outcome,
            OUTCOME_ERROR,
        )

    def test_pdf_link_error_and_exceptions_are_errors(self):
        self.assertEqual(self._attempt({"msg": "nope"}).outcome, OUTCOME_ERROR)
        result = self._attempt({"outputfile": "/x.pdf"}, raise_exc=RuntimeError("boom"))
        self.assertEqual(result.outcome, OUTCOME_ERROR)
        self.assertIn("boom", result.detail)


class RefetchTrackerTests(unittest.TestCase):
    def setUp(self):
        self.target = parse_refetch_key(BOMBAY_KEY)[0]

    def test_most_conclusive_outcome_wins(self):
        tracker = RefetchTracker([self.target])
        tracker.record(self.target, OUTCOME_REFETCHED, local_path=Path("a.pdf"))
        tracker.record(self.target, OUTCOME_ERROR, "later transient error")
        self.assertEqual(tracker.result_for(self.target).outcome, OUTCOME_REFETCHED)

        tracker = RefetchTracker([self.target])
        tracker.record(self.target, OUTCOME_ERROR, "first try failed")
        tracker.record(self.target, OUTCOME_STILL_NOT_PDF, "not_pdf")
        self.assertEqual(tracker.result_for(self.target).outcome, OUTCOME_STILL_NOT_PDF)

    def test_report_marks_unseen_as_not_found_and_failed_searches_as_error(self):
        other = parse_refetch_key(
            "data/pdf/year=2008/court=27_1/bench=kolhcdb/B_1_2008-05-02.pdf"
        )[0]
        tracker = RefetchTracker([self.target, other], [("sc/key.pdf", "not HC")], pad_days=1)
        tracker.mark_search_failed("27~1", "2008-05-01", "2008-05-01", "search failed: x")

        rows = {r["key"]: r for r in tracker.rows()}
        self.assertEqual(rows[BOMBAY_KEY]["outcome"], OUTCOME_NOT_FOUND)
        self.assertIn("2008-04-27..2008-04-29", rows[BOMBAY_KEY]["detail"])
        self.assertEqual(rows[other.key]["outcome"], OUTCOME_ERROR)
        self.assertEqual(rows["sc/key.pdf"]["outcome"], OUTCOME_UNSUPPORTED)

        with tempfile.TemporaryDirectory() as d:
            path = tracker.write_report(Path(d) / "report.csv")
            with open(path) as f:
                written = list(csv.DictReader(f))
        self.assertEqual(len(written), 3)
        self.assertEqual(list(written[0].keys()), refetch_utils.REPORT_FIELDS)


class RefetchUploadTests(TempCwdTestCase):
    def _refetched_tracker(self):
        tracker = RefetchTracker([parse_refetch_key(BOMBAY_KEY)[0]])
        path = Path("data") / BOMBAY_FRAGMENT
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4 real")
        path.with_suffix(".json").write_text("{}")
        tracker.record(tracker.targets[0], OUTCOME_REFETCHED, local_path=path)
        return tracker, path

    def test_refetched_names_are_uploaded_although_the_index_has_them(self):
        tracker, path = self._refetched_tracker()
        with ExitStack() as stack:
            # The S3 index already lists the placeholder's name.
            stack.enter_context(
                patch.object(download, "get_existing_files_from_s3_v2", return_value=[path.name])
            )
            single = stack.enter_context(
                patch.object(download, "upload_single_file_to_s3", return_value=True)
            )
            tar = stack.enter_context(patch.object(download, "upload_replacement_tar_part"))
            normal = stack.enter_context(patch.object(download, "upload_files_to_s3_v2"))
            stack.enter_context(patch.object(download.cache_store, "invalidate"))

            download._upload_refetched_to_s3(tracker)

        # Partition comes from the listed key, so the placeholder is overwritten.
        single.assert_called_once_with("data", 2008, "27_1", "kolhcdb", path)
        tar.assert_called_once_with("data", 2008, "27_1", "kolhcdb", [path])
        normal.assert_not_called()
        self.assertFalse(path.exists())
        self.assertFalse(path.with_suffix(".json").exists())
        self.assertEqual(tracker.rows()[0]["uploaded"], "true")
        self.assertEqual(tracker.pending_uploads(), [])

    def test_failed_object_upload_keeps_files_and_skips_tar(self):
        tracker, path = self._refetched_tracker()
        with (
            patch.object(download, "upload_single_file_to_s3", return_value=False),
            patch.object(download, "upload_replacement_tar_part") as tar,
        ):
            download._upload_refetched_to_s3(tracker)

        tar.assert_not_called()
        self.assertTrue(path.exists())
        self.assertEqual(tracker.rows()[0]["uploaded"], "false")


class RunRefetchTests(TempCwdTestCase):
    def setUp(self):
        super().setUp()
        Path("list.csv").write_text(
            "bucket,key,bytes\n"
            f"b,{BOMBAY_KEY},124\n"
            "b,data/pdf/year=2008/court=27_1/bench=kolhcdb/GONE_1_2008-04-29.pdf,124\n"
            "b,data/pdf/year=2019/court=28_2/bench=aphc/AP_1_2019-07-01.pdf,228\n"
        )

    def _fake_run_tasks(self, seen):
        def run_tasks(batch, *_args, **_kwargs):
            for task in batch:
                seen.append((task.court_code, task.from_date))
                if task.from_date == "2008-04-28":
                    target = task.refetch.lookup(
                        "27~1", "kolhcdb", "HCBM070000012008_1_2008-04-28.pdf"
                    )
                    path = Path("data") / BOMBAY_FRAGMENT
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(b"%PDF-1.4")
                    task.refetch.record(target, OUTCOME_REFETCHED, local_path=path)
            return [], []

        return run_tasks

    def test_local_run_writes_report_and_uploads_nothing(self):
        seen = []
        with ExitStack() as stack:
            stack.enter_context(patch.object(download, "get_court_codes", return_value=COURTS))
            stack.enter_context(patch.object(download, "S3_ENABLED", False))
            stack.enter_context(
                patch.object(download, "_run_tasks", side_effect=self._fake_run_tasks(seen))
            )
            upload = stack.enter_context(patch.object(download, "_upload_refetched_to_s3"))
            cursor = stack.enter_context(patch.object(download, "write_scraped_through_date"))

            summary = download.run_refetch(
                "list.csv", court_codes=["27_1"], report_path="out/report.csv"
            )

        # --court_code narrows the list; only Bombay's two dates are searched.
        self.assertEqual(sorted(seen), [("27~1", "2008-04-28"), ("27~1", "2008-04-29")])
        upload.assert_not_called()
        cursor.assert_not_called()
        self.assertEqual(summary[OUTCOME_REFETCHED], 1)
        self.assertEqual(summary[OUTCOME_NOT_FOUND], 1)
        with open("out/report.csv") as f:
            rows = {r["key"]: r for r in csv.DictReader(f)}
        self.assertEqual(rows[BOMBAY_KEY]["local_path"], str(Path("data") / BOMBAY_FRAGMENT))
        self.assertTrue((Path("data") / BOMBAY_FRAGMENT).exists())

    def test_s3_run_uploads_after_each_batch(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(download, "get_court_codes", return_value=COURTS))
            stack.enter_context(patch.object(download, "S3_ENABLED", True))
            stack.enter_context(
                patch.object(download, "_run_tasks", side_effect=self._fake_run_tasks([]))
            )
            upload = stack.enter_context(patch.object(download, "_upload_refetched_to_s3"))

            download.run_refetch("list.csv", report_path="r.csv", checkpoint_every=1)

        # 3 single-day tasks, checkpointed one by one.
        self.assertEqual(upload.call_count, 3)

    def test_failed_search_marks_keys_error_writes_report_and_raises(self):
        def failing(batch, *_a, **_k):
            return [(task, RuntimeError("session expired")) for task in batch], []

        with ExitStack() as stack:
            stack.enter_context(patch.object(download, "get_court_codes", return_value=COURTS))
            stack.enter_context(patch.object(download, "S3_ENABLED", False))
            stack.enter_context(patch.object(download, "_run_tasks", side_effect=failing))

            with self.assertRaisesRegex(RuntimeError, "3 failed"):
                download.run_refetch("list.csv", report_path="r.csv")

        with open("r.csv") as f:
            outcomes = {r["outcome"] for r in csv.DictReader(f)}
        self.assertEqual(outcomes, {OUTCOME_ERROR})


class FakeS3:
    """Just enough of the boto3 client for the index and tar helpers."""

    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self):
        self.objects = {}

    def get_object(self, Bucket, Key, Range=None):
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[Key] = Body.encode() if isinstance(Body, str) else Body.read()

    def upload_file(self, path, bucket, key, ExtraArgs=None, Config=None):
        self.objects[key] = Path(path).read_bytes()

    def download_file(self, bucket, key, path):
        Path(path).write_bytes(self.objects[key])

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)


INDEX_KEY = "data/tar/year=2008/court=27_1/bench=kolhcdb/data.index.json"
TAR_PREFIX = "data/tar/year=2008/court=27_1/bench=kolhcdb/"


class ReplacementTarTests(TempCwdTestCase):
    def setUp(self):
        super().setUp()
        self.s3 = FakeS3()
        patcher = patch.object(s3_utils, "s3_client", self.s3)
        patcher.start()
        self.addCleanup(patcher.stop)
        index = IndexFileV2(
            file_count=3,
            tar_size=300,
            tar_size_human="300 B",
            updated_at="2024-01-01T00:00:00Z",
            parts=[
                IndexPart(name="part-20240101T000000Z.tar", files=["a.pdf", "b.pdf"],
                          size=200, created_at="2024-01-01T00:00:00Z"),
                IndexPart(name="part-20240201T000000Z.tar", files=["c.pdf"],
                          size=100, created_at="2024-02-01T00:00:00Z"),
            ],
        )
        self.s3.objects[INDEX_KEY] = index.model_dump_json(indent=2).encode()

    def _index(self):
        return IndexFileV2.model_validate(json.loads(self.s3.objects[INDEX_KEY]))

    def test_index_without_replacements_serializes_unchanged(self):
        raw = json.loads(self.s3.objects[INDEX_KEY])
        self.assertNotIn("superseded_files", raw["parts"][0])

    def test_replacement_part_keeps_already_indexed_names_and_moves_them(self):
        Path("a.pdf").write_bytes(b"%PDF-1.4 fixed a")

        names = s3_utils.upload_replacement_tar_part(
            "data", 2008, "27_1", "kolhcdb", [Path("a.pdf")]
        )

        self.assertEqual(len(names), 1)
        self.assertRegex(names[0], r"^part-\d{8}T\d{6}Z-refetch\.tar$")
        with tarfile.open(fileobj=io.BytesIO(self.s3.objects[TAR_PREFIX + names[0]])) as tar:
            self.assertEqual(tar.getnames(), ["a.pdf"])
            self.assertEqual(tar.extractfile("a.pdf").read(), b"%PDF-1.4 fixed a")

        index = self._index()
        old, untouched, new = index.parts
        self.assertEqual(old.files, ["b.pdf"])
        self.assertEqual(old.superseded_files, ["a.pdf"])
        self.assertEqual(untouched.superseded_files, [])
        self.assertEqual(new.name, names[0])
        self.assertEqual(new.files, ["a.pdf"])
        # Each name in exactly one part's files; totals stay consistent.
        all_files = [n for p in index.parts for n in p.files]
        self.assertEqual(sorted(all_files), ["a.pdf", "b.pdf", "c.pdf"])
        self.assertEqual(index.file_count, 3)
        self.assertEqual(index.tar_size, 300 + new.size)
        # Normal runs still treat the name as present.
        self.assertIn(
            "a.pdf", s3_utils.get_existing_files_from_s3_v2("data", 2008, "27_1", "kolhcdb")
        )

    def test_unreadable_index_fails_instead_of_clobbering_it(self):
        Path("a.pdf").write_bytes(b"%PDF-1.4")

        def broken(*_a, **_k):
            raise RuntimeError("throttled")

        with patch.object(self.s3, "get_object", side_effect=broken):
            with self.assertRaises(RuntimeError):
                s3_utils.replace_files_in_index(
                    "data", 2008, "27_1", "kolhcdb", [Path("a.pdf")], "p.tar", 10
                )
        self.assertEqual(len(self._index().parts), 2)

    def test_append_merge_drops_superseded_members(self):
        """A later in-place append must not resurrect a replaced placeholder."""
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for name, body in (("a.pdf", b"<html>404</html>"), ("b.pdf", b"%PDF b")):
                info = tarfile.TarInfo(name)
                info.size = len(body)
                tar.addfile(info, io.BytesIO(body))
        self.s3.objects[TAR_PREFIX + "part-old.tar"] = buf.getvalue()
        Path("d.pdf").write_bytes(b"%PDF d")
        target = IndexPart(
            name="part-old.tar", files=["b.pdf"], superseded_files=["a.pdf"],
            size=len(buf.getvalue()), created_at="2024-01-01T00:00:00Z",
        )

        ok = s3_utils._append_to_existing_part(
            "data", 2008, "27_1", "kolhcdb", [Path("d.pdf")], target,
            TAR_PREFIX, ".tar", "w", "application/x-tar",
        )

        self.assertTrue(ok)
        merged = [k for k in self.s3.objects if k.startswith(TAR_PREFIX + "part-2")]
        self.assertEqual(len(merged), 1)
        with tarfile.open(fileobj=io.BytesIO(self.s3.objects[merged[0]])) as tar:
            self.assertEqual(sorted(tar.getnames()), ["b.pdf", "d.pdf"])


if __name__ == "__main__":
    unittest.main()
