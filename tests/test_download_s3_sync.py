import os
import tempfile
import unittest
from contextlib import ExitStack
from datetime import date
from pathlib import Path
from unittest.mock import call, patch

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
            parquet_mock = mocks[5]
            upload_mock = mocks[6]
            cursor_mock = mocks[7]

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
            parquet_mock = mocks[5]
            upload_mock = mocks[6]
            cursor_mock = mocks[7]

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
            patch.object(
                download.Downloader,
                "download",
                side_effect=[RuntimeError("transient"), None],
            ) as download_mock,
        ):
            download.process_task(task)

        self.assertEqual(download_mock.call_count, 2)
        sleep_mock.assert_called_once_with(1)

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
            failures = download._run_tasks(tasks, max_workers=1)

        self.assertEqual(process_mock.call_count, 2)
        self.assertEqual(len(failures), 1)
        self.assertIs(failures[0][0], tasks[0])
        self.assertRegex(str(failures[0][1]), "bad day")

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

    def test_session_refresh_restarts_search_pagination(self):
        task = download.CourtDateTask("9~13", "2025-01-01", "2025-01-07")

        with patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad High Court"}):
            downloader = download.Downloader(task)

        payload = {"sEcho": 3, "iDisplayStart": 2000, "app_token": "old"}
        updated = downloader._restart_search_pagination(payload)

        self.assertIs(updated, payload)
        self.assertEqual(payload["sEcho"], 1)
        self.assertEqual(payload["iDisplayStart"], 0)
        self.assertEqual(payload["app_token"], "old")


if __name__ == "__main__":
    unittest.main()
