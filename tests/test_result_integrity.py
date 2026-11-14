"""Offline regressions for issue #37; no portal or AWS calls."""
import unittest
from unittest.mock import Mock, patch

import download


class ResultIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.task = download.CourtDateTask("9~13", "2026-11-01", "2026-11-01")
        with patch.object(download, "get_court_codes", return_value={"9~13": "Allahabad"}):
            self.downloader = download.Downloader(self.task)

    def test_supported_handlers(self):
        for handler in (
            "javascript:open_pdf('a','b','court/file.pdf#page=1')",
            "open_pdf('a','b','court/file.pdf#page=1')",
            ' open_pdf ( "a", "b", "court/file.pdf#page=1" ); ',
            "javascript: open_pdf('a', 'b', 'court/file.pdf')",
        ):
            with self.subTest(handler=handler):
                self.assertEqual(self.downloader.extract_pdf_fragment(handler), "court/file.pdf")

    def test_unsupported_handlers_are_not_paths(self):
        for handler in ("", "other('a','b','file.pdf')", "open_pdf('a','b','')", "open_pdf('a','b','file.pdf)"):
            with self.subTest(handler=handler):
                self.assertIsNone(self.downloader.extract_pdf_fragment(handler))

    def test_bad_handler_stops_before_s3_or_download(self):
        row = [0, '<button onclick="unexpected()">PDF</button>']
        with patch.object(self.downloader, "check_result_in_s3") as check:
            self.assertEqual(self.downloader.process_result_row(row, 0), "parse_failure")
        check.assert_not_called()

    def _download_with_outcomes(self, outcomes):
        response = Mock()
        response.json.return_value = {"reportrow": {"aaData": [[0, "redacted"]] * len(outcomes)}}
        empty = Mock()
        empty.json.return_value = {"reportrow": {"aaData": []}}
        with (
            patch.object(self.downloader, "init_user_session"),
            patch.object(self.downloader, "request_api", side_effect=[response, empty]),
            patch.object(self.downloader, "process_result_row", side_effect=outcomes),
        ):
            self.downloader.download()

    def test_all_already_present_is_success_without_downloads(self):
        self._download_with_outcomes(["skip_s3", "skip_local", "metadata_only"])
        self.assertEqual(self.downloader.task_stats["downloaded"], 0)

    def test_empty_search_is_success(self):
        self._download_with_outcomes([])

    def test_partial_or_total_row_failure_is_fatal(self):
        for outcome in ("parse_failure", "no_download", ValueError("malformed row")):
            with self.subTest(outcome=outcome):
                with self.assertRaises(download.IncompleteResultsError):
                    self._download_with_outcomes(["downloaded", outcome])

    def test_row_failure_cannot_be_tolerated_or_advance_cursor(self):
        error = download.IncompleteResultsError("incomplete row")
        # One failure among ten tasks is below the usual transient tolerance.
        # It still must fail the run and cap its upload cursor before that day.
        with (
            patch.object(download, "S3_ENABLED", True),
            patch.object(download, "generate_tasks", return_value=[self.task] * 10),
            patch.object(download, "_run_tasks", return_value=([(self.task, error)], [])),
            patch.object(download, "_upload_court_to_s3") as upload,
        ):
            with self.assertRaisesRegex(RuntimeError, "incomplete row"):
                download.run(court_codes=["9~13"], start_date="2026-11-01", end_date="2026-11-01")
        self.assertEqual(upload.call_args.args[2], "2026-10-31")


if __name__ == "__main__":
    unittest.main()
