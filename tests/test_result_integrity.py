"""Offline regressions for issue #37; no portal or AWS calls."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import download

# Exact form seen in production logs on 2026-11-14 (path anonymised).
LIVE_HANDLER = "open_pdf('0','','court/cnrorders/x/orders/X1_1_2026-11-13.pdf#page=&search=%20');"


class ParserTests(unittest.TestCase):
    def setUp(self):
        self.task = download.CourtDateTask("9~13", "2026-11-01", "2026-11-01")
        with patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad"}):
            self.downloader = download.Downloader(self.task)

    def test_supported_handlers(self):
        cases = {
            LIVE_HANDLER: "court/cnrorders/x/orders/X1_1_2026-11-13.pdf",
            "javascript:open_pdf('a','b','court/file.pdf#page=1')": "court/file.pdf",
            "open_pdf('a','b','court/file.pdf#page=1')": "court/file.pdf",
            ' open_pdf ( "a", "b", "court/file.pdf#page=1" ); ': "court/file.pdf",
            "javascript: open_pdf('a', 'b', 'court/file.pdf')": "court/file.pdf",
        }
        for handler, expected in cases.items():
            with self.subTest(handler=handler):
                self.assertEqual(self.downloader.extract_pdf_fragment(handler), expected)

    def test_unsupported_handlers_are_not_paths(self):
        for handler in (
            "",
            "other('a','b','file.pdf')",
            "open_pdf('a','b','')",
            "open_pdf('a','b','file.pdf)",
        ):
            with self.subTest(handler=handler):
                self.assertIsNone(self.downloader.extract_pdf_fragment(handler))

    def test_bad_handler_stops_before_s3_or_download(self):
        row = [0, '<button onclick="unexpected()">PDF</button>']
        with patch.object(self.downloader, "check_result_in_s3") as check:
            self.assertEqual(self.downloader.process_result_row(row, 0), "parse_failure")
        check.assert_not_called()


class RowFailureTests(unittest.TestCase):
    def setUp(self):
        download.row_failures.reset()
        self.task = download.CourtDateTask("9~13", "2026-11-01", "2026-11-01")
        with patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad"}):
            self.downloader = download.Downloader(self.task)

    def _download_with_outcomes(self, outcomes):
        response = Mock()
        response.json.return_value = {"reportrow": {"aaData": [[0, "x"]] * len(outcomes)}}
        empty = Mock()
        empty.json.return_value = {"reportrow": {"aaData": []}}
        with (
            patch.object(self.downloader, "init_user_session"),
            patch.object(self.downloader, "request_api", side_effect=[response, empty]),
            patch.object(self.downloader, "process_result_row", side_effect=outcomes),
        ):
            self.downloader.download()

    def test_already_present_and_empty_searches_are_clean(self):
        self._download_with_outcomes(["skip_s3", "skip_local", "metadata_only"])
        self._download_with_outcomes([])
        self.assertEqual(download.row_failures.report(), ([], []))

    def test_row_failures_do_not_raise_but_are_recorded(self):
        # Failed rows must not fail the task: that would only re-search the
        # same range three times. The cursor policy treats it as tried.
        self._download_with_outcomes(
            ["downloaded", "parse_failure", "no_download", ValueError("bad row")]
        )
        self.assertEqual(self.downloader.task_stats["row_error"], 1)
        fatal, warnings_ = download.row_failures.report()
        self.assertEqual(fatal, [])
        self.assertEqual(len(warnings_), 1)
        self.assertIn("3/4", warnings_[0][1])

    def test_widespread_row_failure_is_fatal(self):
        # The #37 shape: every listed row errors.
        self._download_with_outcomes([ValueError("no fragment")] * 10)
        fatal, _ = download.row_failures.report()
        self.assertEqual([court for court, _ in fatal], ["9~13"])

    def test_few_failures_in_many_rows_is_a_warning(self):
        download.row_failures.record("27~1", 1000, 6, ["s"])
        fatal, warnings_ = download.row_failures.report()
        self.assertEqual(fatal, [])
        self.assertEqual([c for c, _ in warnings_], ["27~1"])


class RunReportTests(unittest.TestCase):
    """Lost rows fail or warn the run, but the cursor still advances."""

    def setUp(self):
        self.task = download.CourtDateTask("9~13", "2026-11-01", "2026-11-01")

    def _run(self, results, failed):
        def fake_run_tasks(*args, **kwargs):
            download.row_failures.record("9~13", results, failed, ["sample"])
            return [], []

        summary = tempfile.NamedTemporaryFile("r", suffix=".md", delete=False)
        self.addCleanup(os.unlink, summary.name)
        with (
            patch.object(download, "S3_ENABLED", True),
            patch.object(download, "generate_tasks", return_value=[self.task]),
            patch.object(download, "_run_tasks", side_effect=fake_run_tasks),
            patch.object(download, "_upload_court_to_s3") as upload,
            patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": summary.name}),
        ):
            try:
                download.run(
                    court_codes=["9~13"], start_date="2026-11-01", end_date="2026-11-01"
                )
                error = None
            except RuntimeError as e:
                error = e
        return error, upload.call_args.args[2], Path(summary.name).read_text()

    def test_structural_loss_fails_run_but_cursor_advances(self):
        error, cursor, summary = self._run(results=100, failed=100)
        self.assertIsNotNone(error)
        self.assertIn("9~13", str(error))
        self.assertEqual(cursor, "2026-11-01")
        self.assertIn("failing", summary)

    def test_isolated_loss_warns_without_failing(self):
        error, cursor, summary = self._run(results=1000, failed=1)
        self.assertIsNone(error)
        self.assertEqual(cursor, "2026-11-01")
        self.assertIn("warning", summary)

    def test_clean_run_writes_no_summary(self):
        error, _, summary = self._run(results=50, failed=0)
        self.assertIsNone(error)
        self.assertEqual(summary, "")


if __name__ == "__main__":
    unittest.main()
