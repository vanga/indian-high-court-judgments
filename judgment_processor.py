import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import fitz  # PyMuPDF
from dotenv import load_dotenv
from supabase import create_client, Client
from src.utils.html_utils import parse_judgment_metadata
from datetime import datetime, timezone

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

ORDERS_DIR = BASE_DIR / "data" / "court" / "cnrorders" / "cmis" / "orders"

# Minimum age before a file can be processed.
# This gives the scraper time to finish writing the PDF + JSON.
FILE_BUFFER_SECONDS = 60

# How often we check orders/ when there is nothing ready.
POLL_INTERVAL_SECONDS = 5

# How long to wait between retries after a processing failure.
ERROR_RETRY_SECONDS = 15


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("judgment_processor")


# ============================================================
# SUPABASE
# ============================================================

load_dotenv(BASE_DIR / ".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL is not configured")

if not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY is not configured")

supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY,
)


# ============================================================
# PDF → HTML
# ============================================================


def pdf_to_html(pdf_path: Path) -> tuple[str, str]:
    """
    Convert a PDF into layout-preserving HTML using PyMuPDF.

    Returns:
        (html_content, plain_text)
    """

    logger.info(f"Converting PDF → HTML: {pdf_path.name}")

    doc = fitz.open(pdf_path)

    html_parts = ["""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<style>
html, body {
    margin: 0;
    padding: 0;
}

body {
    background: #e5e5e5;
}

/*
 * PyMuPDF provides the actual page dimensions and
 * text coordinates in its generated HTML.
 */
.page {
    position: relative;
    margin: 0 auto 20px auto;
    background: white;
    page-break-after: always;
    overflow: hidden;
}

/*
 * PyMuPDF positions text using top/left coordinates.
 * Keeping these elements absolutely positioned preserves
 * the original PDF layout.
 */
.page p {
    position: absolute;
    margin: 0;
    padding: 0;
}

@media print {
    body {
        background: white;
    }

    .page {
        margin: 0;
    }
}
</style>

</head>
<body>
"""]

    plain_text_parts = []

    try:
        for page_number, page in enumerate(doc, start=1):
            page_html = page.get_text("html")

            html_parts.append(page_html)

            plain_text_parts.append(page.get_text("text"))

            logger.debug(
                f"Converted page {page_number}/{len(doc)} " f"of {pdf_path.name}"
            )

    finally:
        doc.close()

    html_parts.append("""
</body>
</html>
""")

    html_content = "\n".join(html_parts)
    plain_text = "\n\n".join(plain_text_parts)

    return html_content, plain_text


# ============================================================
# QUEUE DISCOVERY
# ============================================================


def find_ready_judgment() -> Optional[tuple[Path, Path]]:
    """
    Find the oldest PDF + matching JSON pair that is ready.

    FIFO is based on the PDF modification time.

    A judgment is considered ready only when:
      1. PDF exists
      2. Matching JSON exists
      3. PDF is older than FILE_BUFFER_SECONDS
      4. JSON is older than FILE_BUFFER_SECONDS
    """

    if not ORDERS_DIR.exists():
        logger.warning(f"Orders directory does not exist: {ORDERS_DIR}")
        return None

    candidates = []

    for pdf_path in ORDERS_DIR.glob("*.pdf"):

        json_path = pdf_path.with_suffix(".json")

        # We require the matching metadata file.
        if not json_path.exists():
            continue

        try:
            pdf_mtime = pdf_path.stat().st_mtime
            json_mtime = json_path.stat().st_mtime

            now = time.time()

            pdf_age = now - pdf_mtime
            json_age = now - json_mtime

            # Buffer protects us from processing files while the
            # scraper may still be writing them.
            if pdf_age < FILE_BUFFER_SECONDS:
                continue

            if json_age < FILE_BUFFER_SECONDS:
                continue

            candidates.append(
                (
                    min(pdf_mtime, json_mtime),
                    pdf_path,
                    json_path,
                )
            )

        except FileNotFoundError:
            # File disappeared between glob() and stat().
            continue

    if not candidates:
        return None

    # Oldest first = FIFO
    candidates.sort(key=lambda item: item[0])

    _, pdf_path, json_path = candidates[0]

    return pdf_path, json_path


# ============================================================
# FILE STABILITY CHECK
# ============================================================


def is_file_stable(
    pdf_path: Path,
    check_interval: int = 3,
) -> bool:
    """
    Make sure the PDF isn't still being written.

    We check its size, wait a few seconds, then check again.
    """

    try:
        first_size = pdf_path.stat().st_size

        if first_size == 0:
            return False

        time.sleep(check_interval)

        second_size = pdf_path.stat().st_size

        if first_size != second_size:
            logger.info(
                f"PDF still changing: {pdf_path.name} "
                f"({first_size} → {second_size} bytes)"
            )
            return False

        return True

    except FileNotFoundError:
        return False


# ============================================================
# METADATA
# ============================================================


def load_metadata(json_path: Path) -> dict:
    """
    Load the scraper-generated metadata JSON.
    """

    with open(
        json_path,
        "r",
        encoding="utf-8",
    ) as f:
        return json.load(f)


def normalize_date(value):
    """
    Convert dates returned by the existing metadata parser into
    YYYY-MM-DD format suitable for Supabase/PostgreSQL DATE columns.
    """

    if not value:
        return None

    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")

    value = str(value).strip()

    if not value:
        return None

    # Common eCourts formats
    formats = (
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d.%m.%Y",
        "%Y-%m-%d",
        "%Y/%m/%d",
    )

    from datetime import datetime

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Don't silently invent a date.
    # Return None so a malformed date doesn't break the entire judgment.
    logger.warning(f"Could not normalize date: {value!r}")
    return None


def extract_and_map_metadata(metadata: dict) -> dict:
    """
    Extract judgment metadata using the shared metadata parser
    and map it into the Supabase high_court_judgments schema.
    """

    parsed = parse_judgment_metadata(metadata)

    if not parsed:
        raise RuntimeError("Metadata parser returned no data")

    record = {
        # --------------------------------------------------------
        # Metadata from scraper / HTML parser
        # --------------------------------------------------------
        "court_code": parsed.get("court_code"),
        "court_name": parsed.get("court"),
        "cnr": parsed.get("cnr"),
        "case_title": parsed.get("title"),
        "judge": parsed.get("judge"),
        "registration_date": normalize_date(parsed.get("date_of_registration")),
        "decision_date": normalize_date(parsed.get("decision_date")),
        "disposal_nature": parsed.get("disposal_nature"),
        "pdf_link": parsed.get("pdf_link"),
        "raw_html": parsed.get("raw_html"),
        # --------------------------------------------------------
        # Source
        # --------------------------------------------------------
        "source": "ecourts",
    }

    # ------------------------------------------------------------
    # Keep only fields that have actual values.
    # ------------------------------------------------------------
    return {key: value for key, value in record.items() if value is not None}


# ============================================================
# SUPABASE INSERT
# ============================================================


def upload_judgment(
    record: dict,
    filename: str,
) -> str:
    """
    Insert judgment into Supabase and verify that the row exists.

    Returns:
        inserted row UUID
    """

    logger.info(f"Uploading judgment to Supabase: {filename}")

    response = supabase.table("high_court_judgments").insert(record).execute()

    if not response.data:
        raise RuntimeError(f"Supabase INSERT returned no data for {filename}")

    inserted_row = response.data[0]

    inserted_id = inserted_row.get("id")

    if not inserted_id:
        raise RuntimeError(f"Supabase INSERT did not return an id for {filename}")

    # --------------------------------------------------------
    # Explicit verification
    # --------------------------------------------------------

    verification = (
        supabase.table("high_court_judgments")
        .select("id")
        .eq("id", inserted_id)
        .limit(1)
        .execute()
    )

    if not verification.data:
        raise RuntimeError(f"Could not verify inserted judgment {inserted_id}")

    logger.info(f"Supabase verified: {filename} → {inserted_id}")

    return inserted_id


# ============================================================
# PROCESS ONE JUDGMENT
# ============================================================


def process_judgment(
    pdf_path: Path,
    json_path: Path,
) -> bool:
    """
    Process exactly one PDF + JSON pair.

    Returns:
        True  → completely successful
        False → failed; files remain for retry
    """

    logger.info("=" * 70)
    logger.info(f"Processing: {pdf_path.name}")

    try:

        # ----------------------------------------------------
        # 1. Final existence check
        # ----------------------------------------------------

        if not pdf_path.exists():
            logger.warning(f"PDF disappeared: {pdf_path.name}")
            return False

        if not json_path.exists():
            logger.warning(f"JSON disappeared: {json_path.name}")
            return False

        # ----------------------------------------------------
        # 2. Stability check
        # ----------------------------------------------------

        if not is_file_stable(pdf_path):
            logger.info(f"Skipping unstable PDF: {pdf_path.name}")
            return False

        # ----------------------------------------------------
        # 3. Load metadata
        # ----------------------------------------------------

        metadata = load_metadata(json_path)

        # ----------------------------------------------------
        # 4. Extract structured metadata using the existing
        #    project's shared parser
        # ----------------------------------------------------

        record = extract_and_map_metadata(metadata)

        # ----------------------------------------------------
        # 5. Duplicate check by source + pdf_link
        # ----------------------------------------------------
        # This check happens BEFORE the expensive PDF → HTML conversion.
        # The UNIQUE(source, pdf_link) database constraint remains the final
        # safety net for races or duplicates that slip through this check.
        pdf_link = record.get("pdf_link")
        source = record.get("source", "ecourts")

        if pdf_link:
            existing = (
                supabase.table("high_court_judgments")
                .select("id")
                .eq("source", source)
                .eq("pdf_link", pdf_link)
                .limit(1)
                .execute()
            )

            if existing.data:
                existing_id = existing.data[0].get("id")
                logger.info(
                    f"DUPLICATE: {pdf_path.name} already exists in Supabase "
                    f"(id={existing_id}); skipping conversion and upload"
                )
                pdf_path.unlink()
                json_path.unlink()
                logger.info(
                    f"Deleted duplicate source files: {pdf_path.name}, "
                    f"{json_path.name}"
                )
                return True

        # ----------------------------------------------------
        # 6. Convert PDF → HTML
        # ----------------------------------------------------

        html_content, plain_text = pdf_to_html(pdf_path)

        if not html_content.strip():
            raise RuntimeError(f"Generated HTML is empty: {pdf_path.name}")

        # Add judgment content to the Supabase record
        record["html_content"] = html_content
        record["plain_text"] = plain_text
        record["downloaded"] = True
        record["content_status"] = "processed"
        record["content_processed_at"] = datetime.now(timezone.utc).isoformat()

        # ----------------------------------------------------
        # 7. Validate required fields
        # ----------------------------------------------------

        if not record.get("court_code"):
            raise RuntimeError(f"Missing court_code in {json_path.name}")

        if not record.get("court_name"):
            raise RuntimeError(f"Missing court_name in {json_path.name}")

        # ----------------------------------------------------
        # 8. Upload + verify
        # ----------------------------------------------------

        try:
            inserted_id = upload_judgment(
                record=record,
                filename=pdf_path.name,
            )
        except Exception as upload_error:
            # A concurrent worker/run may have inserted the same source
            # between the existence check above and this INSERT. PostgreSQL
            # reports that as SQLSTATE 23505 (unique_violation). Treat it as
            # a successful duplicate skip, not a retryable processing failure.
            if "23505" in str(upload_error):
                logger.info(
                    f"DUPLICATE RACE: {pdf_path.name} was inserted by another "
                    "process; deleting local source files"
                )
                pdf_path.unlink()
                json_path.unlink()
                return True
            raise
        # ----------------------------------------------------
        # 9. ONLY NOW delete source files
        # ----------------------------------------------------

        pdf_path.unlink()
        json_path.unlink()

        logger.info(f"SUCCESS: {pdf_path.name}")
        logger.info(f"Database ID: {inserted_id}")
        logger.info(f"Deleted: {pdf_path.name}")
        logger.info(f"Deleted: {json_path.name}")

        return True

    except Exception as e:

        logger.exception(f"FAILED: {pdf_path.name} — files retained for retry")

        return False


# ============================================================
# MAIN WATCHER
# ============================================================


def run_processor():
    """
    Continuously watch orders/ and process judgments FIFO.
    """

    logger.info("=" * 70)
    logger.info("NyaySaathi Judgment Processor")
    logger.info("=" * 70)

    logger.info(f"Watching: {ORDERS_DIR}")

    logger.info(f"Buffer: {FILE_BUFFER_SECONDS} seconds")

    logger.info(f"Poll interval: {POLL_INTERVAL_SECONDS} seconds")

    logger.info("Mode: FIFO / single worker")

    logger.info("=" * 70)

    ORDERS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    while True:

        try:

            ready_judgment = find_ready_judgment()

            if ready_judgment is None:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            pdf_path, json_path = ready_judgment

            logger.info(f"Next FIFO judgment: {pdf_path.name}")

            success = process_judgment(
                pdf_path,
                json_path,
            )

            if not success:
                # Don't hammer a failing judgment continuously.
                time.sleep(ERROR_RETRY_SECONDS)

        except KeyboardInterrupt:

            logger.info("Processor stopped by user.")
            break

        except Exception:

            logger.exception("Unexpected processor error")

            time.sleep(ERROR_RETRY_SECONDS)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_processor()
