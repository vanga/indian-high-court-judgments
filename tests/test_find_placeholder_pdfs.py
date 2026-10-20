import importlib.util
import io
import json
import unittest
from pathlib import Path


_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "find_placeholder_pdfs.py"
_spec = importlib.util.spec_from_file_location("find_placeholder_pdfs", _SCRIPT)
finder = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(finder)


BUCKET = "indian-high-court-judgments"


class FakeS3:
    def __init__(self, bodies):
        self.bodies = bodies
        self.calls = []

    def get_object(self, Bucket, Key, Range=None):
        self.calls.append((Key, Range))
        body = self.bodies[Key]
        if Range:
            end = int(Range.split("-")[1])
            body = body[: end + 1]
        return {"Body": io.BytesIO(body)}


def _obj(key, size):
    return {"Key": key, "Size": size}


class FindPlaceholderTests(unittest.TestCase):
    LISTING = [
        # Bombay HTML "page not found" (124 bytes).
        _obj("data/pdf/year=2008/court=27_1/bench=kolhcdb/A_1_2008-04-28.pdf", 124),
        # Delhi JSON error (61 bytes).
        _obj("data/pdf/year=2024/court=7_26/bench=dhcdb/B_1_2024-01-02.pdf", 61),
        # A genuinely tiny but valid PDF.
        _obj("data/pdf/year=2024/court=7_26/bench=dhcdb/C_1_2024-01-03.pdf", 900),
        # Normal-size PDF, and exactly at the threshold.
        _obj("data/pdf/year=2024/court=7_26/bench=dhcdb/D_1_2024-01-04.pdf", 250_000),
        _obj("data/pdf/year=2024/court=7_26/bench=dhcdb/E_1_2024-01-05.pdf", 1024),
        # Not a PDF key at all.
        _obj("data/pdf/year=2024/court=7_26/bench=dhcdb/notes.txt", 10),
        # Empty object.
        _obj("data/pdf/year=2024/court=7_26/bench=dhcdb/F_1_2024-01-06.pdf", 0),
    ]
    BODIES = {
        LISTING[0]["Key"]: b"Welcome User Search Page not Found",
        LISTING[1]["Key"]: b'{"msg":"error","error":"FPDF error: Unexpected end of stream"}',
        LISTING[2]["Key"]: b"%PDF-1.4 tiny",
    }

    def _keys(self, rows):
        return [Path(r["key"]).name for r in rows]

    def test_size_only_classification(self):
        rows = list(
            finder.find_placeholders(FakeS3(self.BODIES), BUCKET, objects=self.LISTING)
        )
        self.assertEqual(
            self._keys(rows),
            ["A_1_2008-04-28.pdf", "B_1_2024-01-02.pdf", "C_1_2024-01-03.pdf", "F_1_2024-01-06.pdf"],
        )
        self.assertEqual(rows[0], {"bucket": BUCKET, "key": self.LISTING[0]["Key"], "bytes": 124})

    def test_verify_magic_drops_tiny_real_pdfs_and_skips_reading_empty_objects(self):
        s3 = FakeS3(self.BODIES)
        rows = list(
            finder.find_placeholders(s3, BUCKET, verify_magic=True, objects=self.LISTING)
        )
        self.assertEqual(
            self._keys(rows),
            ["A_1_2008-04-28.pdf", "B_1_2024-01-02.pdf", "F_1_2024-01-06.pdf"],
        )
        self.assertNotIn(self.LISTING[6]["Key"], [k for k, _ in s3.calls])
        self.assertTrue(all(r == "bytes=0-7" for _, r in s3.calls))

    def test_is_placeholder(self):
        self.assertTrue(finder.is_placeholder(b"<html>"))
        self.assertTrue(finder.is_placeholder(b""))
        self.assertFalse(finder.is_placeholder(b"%PDF-1.7"))

    def test_resolve_dates_only_for_filename_year_mismatch(self):
        mismatched = "data/pdf/year=1993/court=27_1/bench=newos/HCBM06_2_2014-06-15.pdf"
        listing = [_obj(mismatched, 258), self.LISTING[0]]
        raw_html = (
            '<strong class="caseDetailsTD"><span>Decision Date</span>'
            "<font> 02-03-1993</font></strong>"
        )
        bodies = {
            "metadata/json/year=1993/court=27_1/bench=newos/HCBM06_2_2014-06-15.json":
                json.dumps({"raw_html": raw_html}).encode(),
        }
        s3 = FakeS3(bodies)

        rows = list(
            finder.find_placeholders(s3, BUCKET, resolve_dates=True, objects=listing)
        )

        self.assertEqual(rows[0]["search_date"], "1993-03-02")
        self.assertEqual(rows[1]["search_date"], "")
        self.assertEqual(len(s3.calls), 1, "matching-year keys need no metadata read")

    def test_metadata_key_mapping(self):
        self.assertEqual(
            finder.metadata_key_for("data/pdf/year=2008/court=27_1/bench=x/A.pdf"),
            "metadata/json/year=2008/court=27_1/bench=x/A.json",
        )


if __name__ == "__main__":
    unittest.main()
