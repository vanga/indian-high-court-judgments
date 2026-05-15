import unittest

import pandas as pd

from src.utils.s3_utils import dedupe_parquet_records, filter_parquet_records_against_keys


class ParquetDedupeTests(unittest.TestCase):
    def test_dedupes_by_cnr_and_decision_date_even_when_pdf_links_differ(self):
        df = pd.DataFrame(
            [
                {"cnr": "ABC123", "pdf_link": "a.pdf", "decision_date": "2025-01-01"},
                {"cnr": "ABC123", "pdf_link": "b.pdf", "decision_date": "2025-01-01"},
                {"cnr": "XYZ999", "pdf_link": "c.pdf", "decision_date": "2025-01-02"},
            ]
        )

        deduped = dedupe_parquet_records(df)

        self.assertEqual(len(deduped), 2)
        self.assertEqual(set(deduped["cnr"]), {"ABC123", "XYZ999"})
        self.assertEqual(
            deduped.loc[deduped["cnr"] == "ABC123", "pdf_link"].iloc[0],
            "b.pdf",
        )

    def test_keeps_same_cnr_with_different_decision_dates(self):
        df = pd.DataFrame(
            [
                {"cnr": "ABC123", "pdf_link": "a.pdf", "decision_date": "2025-01-01"},
                {"cnr": "ABC123", "pdf_link": "b.pdf", "decision_date": "2025-02-01"},
            ]
        )

        deduped = dedupe_parquet_records(df)

        self.assertEqual(len(deduped), 2)
        self.assertEqual(set(deduped["pdf_link"]), {"a.pdf", "b.pdf"})

    def test_falls_back_to_pdf_link_when_cnr_missing(self):
        df = pd.DataFrame(
            [
                {"cnr": "", "pdf_link": "a.pdf", "decision_date": "2025-01-01"},
                {"cnr": None, "pdf_link": "a.pdf", "decision_date": "2025-01-01"},
                {"cnr": "", "pdf_link": "b.pdf", "decision_date": "2025-01-01"},
            ]
        )

        deduped = dedupe_parquet_records(df)

        self.assertEqual(len(deduped), 2)
        self.assertEqual(set(deduped["pdf_link"]), {"a.pdf", "b.pdf"})

    def test_filters_out_records_already_present_in_sibling_benches(self):
        df = pd.DataFrame(
            [
                {"cnr": "ABC123", "pdf_link": "a.pdf", "decision_date": "2025-01-01"},
                {"cnr": "ABC123", "pdf_link": "later.pdf", "decision_date": "2025-01-03"},
                {"cnr": "XYZ999", "pdf_link": "b.pdf", "decision_date": "2025-01-02"},
            ]
        )

        filtered = filter_parquet_records_against_keys(
            df, {"cnr:ABC123|decision_date:2025-01-01"}
        )

        self.assertEqual(len(filtered), 2)
        self.assertEqual(
            set(filtered["pdf_link"]),
            {"later.pdf", "b.pdf"},
        )


if __name__ == "__main__":
    unittest.main()
