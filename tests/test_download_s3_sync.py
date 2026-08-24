import math
import os
import tempfile
import time
import unittest
from contextlib import ExitStack
from datetime import date
from pathlib import Path
from unittest.mock import call, patch

import requests
import urllib3

import download


class UploadCourtToS3Tests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.prev_cwd = os.getcwd()
        os.chdir(self.tmpdir.name)
        self.addCleanup(os.chdir, self.prev_cwd)

        bench_dir = Path("data/court/cnrorders/testbench/orders/2025")
        bench_dir.mkdir(parents=True, exist_ok=True)
        self.json_path = bench_dir / "case.json"
        self.pdf_path = bench_dir / "case.pdf"
        self.json_path.write_text('{"raw_html":"<div></div>"}')
        self.pdf_path.write_bytes(b"%PDF-1.4")

    def _common_patches(self, parquet_success=True):
        return [
            patch.object(download, "load_court_bench_mapping", return_value={"testbench": "9_13"}),
            patch.object(download, "get_bench_codes", return_value={}),
            patch.object(download, "extract_decision_date_from_json", return_value=2025),
            patch.object(download, "get_existing_files_from_s3_v2", return_value=[]),
            patch.object(download, "get_existing_judgment_identities_from_parquet", return_value=set()),
            patch.object(download.cache_store, "invalidate"),
            patch.object(
                download,
                "create_and_upload_parquet_files",
                return_value=parquet_success,
            ),
            patch.object(download, "upload_files_to_s3_v2"),
            patch.object(download, "write_scraped_through_date"),
        ]

    def test_parquet_failure_blocks_raw_upload_and_resume_cursor(self):
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in self._common_patches(parquet_success=False)]
            parquet_mock = mocks[6]
            upload_mock = mocks[7]
            cursor_mock = mocks[8]

            with self.assertRaises(RuntimeError):
                download._upload_court_to_s3("9~13", date(2026, 4, 28))

        parquet_mock.assert_called_once()
        upload_mock.assert_not_called()
        cursor_mock.assert_not_called()
        self.assertTrue(self.json_path.exists())
        self.assertTrue(self.pdf_path.exists())

    def test_successful_sync_uploads_and_cleans_up_files(self):
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in self._common_patches()]
            parquet_mock = mocks[6]
            upload_mock = mocks[7]
            cursor_mock = mocks[8]

            download._upload_court_to_s3("9~13", date(2026, 4, 28))

        parquet_mock.assert_called_once()
        self.assertEqual(upload_mock.call_count, 2)
        self.assertEqual(
            cursor_mock.call_args_list,
            [
                call("data", 2025, "9_13", "testbench", "2026-04-28"),
                call("data", 2026, "9_13", "testbench", "2026-04-28"),
            ],
        )
        self.assertFalse(self.json_path.exists())
        self.assertFalse(self.pdf_path.exists())

    def test_partial_year_sync_failure_preserves_failed_year_and_suppresses_cursor(self):
        year_2026_dir = Path("data/court/cnrorders/testbench/orders/2026")
        year_2026_dir.mkdir(parents=True, exist_ok=True)
        json_2026 = year_2026_dir / "case2026.json"
        pdf_2026 = year_2026_dir / "case2026.pdf"
        json_2026.write_text('{"raw_html":"<div></div>"}')
        pdf_2026.write_bytes(b"%PDF-1.4")

        def extract_year(path):
            return 2026 if "2026" in str(path) else 2025

        def parquet_success(year, *_args, **_kwargs):
            return year != 2026

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    download,
                    "load_court_bench_mapping",
                    return_value={"testbench": "9_13"},
                )
            )
            stack.enter_context(patch.object(download, "get_bench_codes", return_value={}))
            stack.enter_context(
                patch.object(
                    download,
                    "extract_decision_date_from_json",
                    side_effect=extract_year,
                )
            )
            stack.enter_context(
                patch.object(download, "get_existing_files_from_s3_v2", return_value=[])
            )
            stack.enter_context(
                patch.object(download, "get_existing_judgment_identities_from_parquet", return_value=set())
            )
            stack.enter_context(patch.object(download.cache_store, "invalidate"))
            parquet_mock = stack.enter_context(
                patch.object(
                    download,
                    "create_and_upload_parquet_files",
                    side_effect=parquet_success,
                )
            )
            upload_mock = stack.enter_context(patch.object(download, "upload_files_to_s3_v2"))
            cursor_mock = stack.enter_context(
                patch.object(download, "write_scraped_through_date")
            )

            with self.assertRaisesRegex(RuntimeError, "S3 sync completed with failures"):
                download._upload_court_to_s3("9~13", date(2026, 4, 28))

        self.assertEqual(parquet_mock.call_count, 2)
        uploaded_years = {call_args.args[1] for call_args in upload_mock.call_args_list}
        self.assertEqual(uploaded_years, {2025})
        cursor_mock.assert_not_called()

        self.assertFalse(self.json_path.exists())
        self.assertFalse(self.pdf_path.exists())
        self.assertTrue(json_2026.exists())
        self.assertTrue(pdf_2026.exists())

    def test_skip_s3_still_materializes_metadata_for_parquet_repair(self):
        task = download.CourtDateTask("9~13", "2026-04-01", "2026-04-01")
        with patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad High Court"}):
            downloader = download.Downloader(task)
            row = [
                None,
                '<button onclick="return false;">Open</button><strong>Judge: Test</strong>',
            ]

            with (
                patch.object(
                    downloader,
                    "extract_pdf_fragment",
                    return_value="court/cnrorders/testbench/orders/2025/skipcase.pdf",
                ),
                patch.object(downloader, "check_result_in_s3", return_value=(True, True)),
            ):
                outcome = downloader.process_result_row(row, 0)

        self.assertEqual(outcome, "skip_s3")
        metadata_path = Path(
            "data/court/cnrorders/testbench/orders/2025/skipcase.json"
        )
        self.assertTrue(metadata_path.exists())


class ScrapeFailureTests(unittest.TestCase):
    def test_terminal_session_expire_is_not_treated_as_empty_results(self):
        task = download.CourtDateTask("9~13", "2025-01-01", "2025-01-07")

        with patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad High Court"}):
            downloader = download.Downloader(task)

        with self.assertRaisesRegex(RuntimeError, "session expired after retries"):
            downloader._raise_for_terminal_search_error(
                {"session_expire": "Y", "message": "Session Expired"}
            )

    def test_process_task_propagates_download_failures(self):
        task = download.CourtDateTask("9~13", "2025-01-01", "2025-01-07")

        with (
            patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad High Court"}),
            patch.object(download.time, "sleep"),
            patch.object(download.Downloader, "download", side_effect=RuntimeError("boom")),
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                download.process_task(task)

    def test_process_task_retries_transient_download_failures(self):
        task = download.CourtDateTask("9~13", "2025-01-01", "2025-01-07")

        with (
            patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad High Court"}),
            patch.object(download.time, "sleep") as sleep_mock,
            patch.object(download.random, "uniform", return_value=0.25),
            patch.object(
                download.Downloader,
                "download",
                side_effect=[RuntimeError("transient"), None],
            ) as download_mock,
        ):
            download.process_task(task)

        self.assertEqual(download_mock.call_count, 2)
        sleep_mock.assert_called_once_with(1.25)

    def test_run_tasks_collects_failures_and_continues(self):
        tasks = [
            download.CourtDateTask("9~13", "2025-01-01", "2025-01-01"),
            download.CourtDateTask("9~13", "2025-01-02", "2025-01-02"),
        ]

        with patch.object(
            download,
            "process_task",
            side_effect=[RuntimeError("bad day"), None],
        ) as process_mock:
            failures, unrun = download._run_tasks(tasks, max_workers=1)

        self.assertEqual(process_mock.call_count, 2)
        self.assertEqual(len(failures), 1)
        self.assertIs(failures[0][0], tasks[0])
        self.assertRegex(str(failures[0][1]), "bad day")
        self.assertEqual(unrun, [])

    def test_download_propagates_terminal_search_errors(self):
        task = download.CourtDateTask("9~13", "2025-01-01", "2025-01-07")

        class Response:
            def json(self):
                return {"session_expire": "Y", "message": "Session Expired"}

        with patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad High Court"}):
            downloader = download.Downloader(task)

        with (
            patch.object(downloader, "init_user_session"),
            patch.object(downloader, "request_api", return_value=Response()),
        ):
            with self.assertRaisesRegex(RuntimeError, "session expired after retries"):
                downloader.download()

    def test_session_refresh_preserves_search_offset(self):
        task = download.CourtDateTask("9~13", "2025-01-01", "2025-01-07")

        with patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad High Court"}):
            downloader = download.Downloader(task)

        payload = {"sEcho": 3, "iDisplayStart": 2000, "app_token": "old"}
        updated = downloader._refresh_search_pagination(payload)

        self.assertIs(updated, payload)
        self.assertEqual(payload["sEcho"], 1)
        self.assertEqual(payload["iDisplayStart"], 2000)
        self.assertEqual(payload["app_token"], "old")

    def test_download_pdf_rejects_html_error_body(self):
        task = download.CourtDateTask("9~13", "2025-01-01", "2025-01-01")

        class LinkResponse:
            def json(self):
                return {"outputfile": "/bad.html"}

        class PdfResponse:
            status_code = 200
            content = b"<!DOCTYPE html>\r\n<html>\r\n<body>expired</body>"

        with patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad High Court"}):
            downloader = download.Downloader(task)
            downloader.session_id = "session"
            downloader.ecourts_token = "token"

        with (
            patch.object(downloader, "request_api", return_value=LinkResponse()),
            patch.object(download.requests, "request", return_value=PdfResponse()),
        ):
            result = downloader.download_pdf(
                "court/cnrorders/testbench/orders/2025/bad.pdf", 0
            )

        self.assertFalse(result)
        self.assertFalse(
            Path("data/court/cnrorders/testbench/orders/2025/bad.pdf").exists()
        )

    def test_download_pdf_writes_valid_pdf_body(self):
        task = download.CourtDateTask("9~13", "2025-01-01", "2025-01-01")

        class LinkResponse:
            def json(self):
                return {"outputfile": "/good.pdf"}

        class PdfResponse:
            status_code = 200
            content = b"%PDF-1.4\nbody"

        with patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad High Court"}):
            downloader = download.Downloader(task)
            downloader.session_id = "session"
            downloader.ecourts_token = "token"

        pdf_path = Path("data/court/cnrorders/testbench/orders/2025/good.pdf")
        with (
            patch.object(downloader, "request_api", return_value=LinkResponse()),
            patch.object(download.requests, "request", return_value=PdfResponse()),
        ):
            result = downloader.download_pdf(
                "court/cnrorders/testbench/orders/2025/good.pdf", 0
            )

        self.assertTrue(result)
        self.assertEqual(pdf_path.read_bytes(), PdfResponse.content)


class ResumeCursorTests(unittest.TestCase):
    """Cursors must advance for every bench of a court, not just busy ones.

    run() resolves a court's start date as the MIN across its benches, so a
    bench that is never given a cursor pins its whole court to an ever-growing
    re-scrape window (issue #30).
    """

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.prev_cwd = os.getcwd()
        os.chdir(self.tmpdir.name)
        self.addCleanup(os.chdir, self.prev_cwd)

        # Only "busybench" has files on disk; "quietbench" produced nothing.
        bench_dir = Path("data/court/cnrorders/busybench/orders/2025")
        bench_dir.mkdir(parents=True, exist_ok=True)
        (bench_dir / "case.json").write_text('{"raw_html":"<div></div>"}')
        (bench_dir / "case.pdf").write_bytes(b"%PDF-1.4")

    def _patches(self):
        return [
            patch.object(
                download,
                "load_court_bench_mapping",
                return_value={"busybench": "27_1", "quietbench": "27_1"},
            ),
            patch.object(download, "get_bench_codes", return_value={}),
            patch.object(download, "extract_decision_date_from_json", return_value=2025),
            patch.object(download, "get_existing_files_from_s3_v2", return_value=[]),
            patch.object(
                download, "get_existing_judgment_identities_from_parquet", return_value=set()
            ),
            patch.object(download.cache_store, "invalidate"),
            patch.object(download, "create_and_upload_parquet_files", return_value=True),
            patch.object(download, "upload_files_to_s3_v2"),
            patch.object(download, "write_scraped_through_date"),
        ]

    def test_bench_with_no_new_files_still_advances_its_cursor(self):
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in self._patches()]
            cursor_mock = mocks[8]

            download._upload_court_to_s3("27~1", date(2026, 8, 24))

        benches_written = {c.args[3] for c in cursor_mock.call_args_list}
        self.assertEqual(benches_written, {"busybench", "quietbench"})
        self.assertIn(
            call("data", 2026, "27_1", "quietbench", "2026-08-24"),
            cursor_mock.call_args_list,
        )

    def test_explicit_scraped_through_overrides_end_date(self):
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in self._patches()]
            cursor_mock = mocks[8]

            download._upload_court_to_s3("27~1", date(2026, 8, 24), "2026-08-10")

        written_dates = {c.args[4] for c in cursor_mock.call_args_list}
        self.assertEqual(written_dates, {"2026-08-10"})

    def test_midloop_failure_still_persists_cursors_for_completed_benches(self):
        """A transient error on a later bench must not discard earlier progress.

        The cursor advance runs once after the bench loop, so an exception
        raised before it (e.g. an S3 read outside the per-year guard) used to
        skip cursor writes for benches that had already synced cleanly.
        """
        with ExitStack() as stack:
            mocks = [stack.enter_context(p) for p in self._patches()]
            cursor_mock = mocks[8]
            # busybench is on disk and syncs; quietbench has no directory, so
            # give the *second* bench directory a file and blow up reading it.
            other = Path("data/court/cnrorders/quietbench/orders/2025")
            other.mkdir(parents=True, exist_ok=True)
            (other / "case.json").write_text('{"raw_html":"<div></div>"}')

            def flaky(data_type, year, court_code, bench, *args, **kwargs):
                if bench == "quietbench":
                    raise RuntimeError("transient S3 read failure")
                return []

            mocks[3].side_effect = flaky

            with self.assertRaises(RuntimeError):
                download._upload_court_to_s3("27~1", date(2026, 8, 24))

        benches_written = {c.args[3] for c in cursor_mock.call_args_list}
        # The bench that completed keeps its cursor; the one that failed does not.
        self.assertIn("busybench", benches_written)
        self.assertNotIn("quietbench", benches_written)


class ContiguousCursorTests(unittest.TestCase):
    def test_full_coverage_uses_end_date(self):
        self.assertEqual(
            download._contiguous_scraped_through("2026-08-24", []), "2026-08-24"
        )

    def test_stops_the_day_before_the_earliest_incomplete_range(self):
        incomplete = [
            download.CourtDateTask("27~1", "2026-08-16", "2026-08-20"),
            download.CourtDateTask("27~1", "2026-08-06", "2026-08-10"),
        ]
        self.assertEqual(
            download._contiguous_scraped_through("2026-08-24", incomplete),
            "2026-08-05",
        )


class RunBudgetTests(unittest.TestCase):
    """The run must stop itself and say so, rather than be killed by CI."""

    def test_run_tasks_cancels_pending_ranges_past_the_deadline(self):
        tasks = [
            download.CourtDateTask("27~1", f"2026-08-{day:02d}", f"2026-08-{day:02d}")
            for day in range(1, 6)
        ]

        # Deadline already elapsed: ranges still queued behind the single
        # worker are cancelled and reported, not silently dropped.
        def slow_task(_task, _compression=False):
            time.sleep(0.1)

        with patch.object(download, "process_task", side_effect=slow_task) as process_mock:
            failures, unrun = download._run_tasks(
                tasks, max_workers=1, deadline=time.monotonic() - 1
            )

        self.assertEqual(failures, [])
        self.assertTrue(unrun, "expected queued ranges to be cancelled")
        self.assertEqual(process_mock.call_count, len(tasks) - len(unrun))
        # Cancelled ranges are the tail of the queue, never a gap in the middle.
        self.assertEqual(
            [t.from_date for t in unrun],
            [t.from_date for t in tasks[len(tasks) - len(unrun):]],
        )

    def test_unattempted_court_fails_the_run(self):
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    download,
                    "get_court_codes",
                    return_value={"9~13": "Allahabad", "27~1": "Bombay"},
                )
            )
            stack.enter_context(
                patch.object(download, "_run_deadline", return_value=time.monotonic() - 1)
            )
            run_mock = stack.enter_context(
                patch.object(download, "_run_tasks", return_value=([], []))
            )

            with self.assertRaisesRegex(RuntimeError, "not attempted"):
                download.run(
                    start_date="2026-08-20",
                    end_date="2026-08-24",
                    day_step=5,
                    max_runtime_minutes=60,
                )

        # Budget was already gone, so no court was even started.
        run_mock.assert_not_called()

    def test_budget_error_does_not_hide_task_failure_detail(self):
        """A run can exhaust its budget *and* hit real failures.

        Raising on the budget alone used to make the per-task failure block
        unreachable, dropping exactly the detail needed to tell a transient
        portal wobble apart from a budget that is simply too small.
        """
        task = download.CourtDateTask("9~13", "2026-08-20", "2026-08-24")
        unrun = download.CourtDateTask("9~13", "2026-08-25", "2026-08-29")

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    download, "get_court_codes", return_value={"9~13": "Allahabad"}
                )
            )
            stack.enter_context(patch.object(download, "S3_ENABLED", False))
            stack.enter_context(
                patch.object(
                    download,
                    "_run_tasks",
                    return_value=([(task, RuntimeError("boom"))], [unrun]),
                )
            )

            with self.assertRaises(RuntimeError) as ctx:
                download.run(
                    start_date="2026-08-20",
                    end_date="2026-08-24",
                    day_step=5,
                    max_runtime_minutes=60,
                )

        message = str(ctx.exception)
        self.assertIn("boom", message)
        self.assertIn("not run", message)


class ConnectivityBreakerTests(unittest.TestCase):
    """A portal that will not connect must not be hammered.

    The 2026-08-24 run sent 3,607 connection attempts to an endpoint that
    never answered, burning two hours to scrape nothing.
    """

    def setUp(self):
        download.connectivity_breaker.reset()
        self.addCleanup(download.connectivity_breaker.reset)

    def _timeout(self):
        return requests.exceptions.ConnectTimeout("timed out")

    def test_connection_errors_are_recognised_through_the_cause_chain(self):
        try:
            try:
                raise urllib3.exceptions.NewConnectionError(None, "no route")
            except Exception as inner:
                raise RuntimeError("wrapped") from inner
        except RuntimeError as e:
            self.assertTrue(download._is_connectivity_error(e))

    def test_http_errors_do_not_trip_the_breaker(self):
        """A response - any response - means the portal is alive."""
        self.assertFalse(
            download._is_connectivity_error(RuntimeError("session expired"))
        )
        self.assertFalse(
            download._is_connectivity_error(requests.exceptions.HTTPError("500"))
        )

    def test_breaker_needs_consecutive_failures(self):
        for _ in range(download.CONNECTIVITY_FAILURE_LIMIT - 1):
            download.connectivity_breaker.record_failure()
        # A single success clears the streak, so an intermittent portal that
        # still answers sometimes keeps working.
        download.connectivity_breaker.record_success()
        self.assertFalse(download.connectivity_breaker.tripped)

        for _ in range(download.CONNECTIVITY_FAILURE_LIMIT):
            download.connectivity_breaker.record_failure()
        self.assertTrue(download.connectivity_breaker.tripped)

    def test_process_task_stops_contacting_the_portal_once_tripped(self):
        task = download.CourtDateTask("9~13", "2026-01-01", "2026-01-05")
        download.connectivity_breaker._tripped = True

        with (
            patch.object(
                download, "get_court_codes", return_value={"9~13": "Allahabad"}
            ),
            patch.object(download.Downloader, "download") as dl,
        ):
            with self.assertRaises(download.PortalUnreachable):
                download.process_task(task)

        dl.assert_not_called()

    def test_sustained_timeouts_abort_instead_of_retrying_every_task(self):
        tasks = [
            download.CourtDateTask("9~13", f"2026-01-{d:02d}", f"2026-01-{d:02d}")
            for d in range(1, 26)
        ]
        attempts = {"n": 0}

        def always_timeout(*_a, **_k):
            attempts["n"] += 1
            raise requests.exceptions.ConnectTimeout("timed out")

        with (
            patch.object(
                download, "get_court_codes", return_value={"9~13": "Allahabad"}
            ),
            patch.object(download.time, "sleep"),
            patch.object(download.Downloader, "download", side_effect=always_timeout),
        ):
            failures, unrun = download._run_tasks(tasks, max_workers=1)

        self.assertTrue(download.connectivity_breaker.tripped)
        # The whole point: without the breaker this is 25 tasks x 3 retries =
        # 75 connection attempts against a dead endpoint. The breaker caps it
        # at the limit and every later task is skipped without being contacted.
        self.assertLessEqual(attempts["n"], download.CONNECTIVITY_FAILURE_LIMIT)
        self.assertLess(attempts["n"], len(tasks) * download.TASK_DOWNLOAD_ATTEMPTS)
        # Nothing is silently counted as done: every range is either a failure
        # or unrun, and both block the resume cursor.
        self.assertEqual(len(failures) + len(unrun), len(tasks))
        skipped = [e for _, e in failures if isinstance(e, download.PortalUnreachable)]
        self.assertTrue(skipped, "later ranges should short-circuit, not retry")


class CheckpointTests(unittest.TestCase):
    """Progress must be durable mid-court, not only once a court finishes.

    The graceful budget only helps when the process gets to exit on its own. A
    run killed outright never reaches the end-of-court upload, so without
    intermediate checkpoints everything that court scraped is discarded and
    re-scraped from its old cursor on the next run.
    """

    START = "2026-01-01"
    END = "2026-03-01"

    def _uploads_for(self, checkpoint_every):
        """Run one court end-to-end, returning (court, scraped_through) per upload."""
        uploads = []

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    download, "get_court_codes", return_value={"9~13": "Allahabad"}
                )
            )
            stack.enter_context(patch.object(download, "S3_ENABLED", True))
            stack.enter_context(
                patch.object(download, "_run_tasks", return_value=([], []))
            )
            stack.enter_context(
                patch.object(
                    download,
                    "_upload_court_to_s3",
                    side_effect=lambda court, _end, through=None: uploads.append(
                        (court, through)
                    ),
                )
            )
            download.run(
                start_date=self.START,
                end_date=self.END,
                day_step=1,
                max_runtime_minutes=None,
                checkpoint_every=checkpoint_every,
            )
        return uploads

    def _task_count(self):
        return len(
            list(download.generate_tasks(["9~13"], self.START, self.END, 1))
        )

    def test_backlog_is_checkpointed_in_batches(self):
        uploads = self._uploads_for(10)
        expected = math.ceil(self._task_count() / 10)

        self.assertEqual(len(uploads), expected)
        self.assertGreater(len(uploads), 1, "a 2-month backlog should checkpoint")

    def test_checkpoint_never_advances_past_unreached_batches(self):
        uploads = self._uploads_for(10)

        # First batch covers 10 daily ranges, so the cursor stops the day before
        # the 11th — never at end_date, which is not covered yet.
        self.assertEqual(uploads[0][1], "2026-01-10")
        # Cursors move strictly forward, and only the final one reaches end_date.
        cursors = [through for _, through in uploads]
        self.assertEqual(cursors, sorted(cursors))
        self.assertEqual(cursors[-1], self.END)

    def test_disabled_checkpointing_uploads_once_per_court(self):
        self.assertEqual(len(self._uploads_for(None)), 1)

    def test_budget_exhausted_midcourt_checkpoints_what_completed(self):
        """A batch cut short still persists the ground it gained."""
        uploads = []

        def fake_run_tasks(batch, *_args, **_kwargs):
            # Budget goes mid-batch: the tail is cancelled, and later batches
            # are never started.
            return ([], list(batch[5:]))

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    download, "get_court_codes", return_value={"9~13": "Allahabad"}
                )
            )
            stack.enter_context(patch.object(download, "S3_ENABLED", True))
            stack.enter_context(
                patch.object(download, "_run_tasks", side_effect=fake_run_tasks)
            )
            stack.enter_context(
                patch.object(
                    download,
                    "_upload_court_to_s3",
                    side_effect=lambda court, _end, through=None: uploads.append(
                        (court, through)
                    ),
                )
            )

            with self.assertRaises(RuntimeError) as ctx:
                download.run(
                    start_date=self.START,
                    end_date=self.END,
                    day_step=1,
                    max_runtime_minutes=None,
                    checkpoint_every=10,
                )

        # Exactly one checkpoint: later batches were abandoned, not attempted.
        self.assertEqual(len(uploads), 1)
        # Cursor stops the day before the first cancelled range, not at the
        # end of the batch and certainly not at end_date.
        self.assertEqual(uploads[0][1], "2026-01-05")
        self.assertIn("not run", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
