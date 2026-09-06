import argparse
import logging
import signal
import shutil
import subprocess
import sys
import time
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

PROCESSOR_SCRIPT = BASE_DIR / "judgment_processor.py"
SCRAPER_SCRIPT = BASE_DIR / "download.py"

ORDERS_DIR = BASE_DIR / "data" / "court" / "cnrorders" / "cmis" / "orders"

# Temporary directory containing solved captcha files.
CAPTCHA_TMP_DIR = BASE_DIR / "captcha-tmp"

# Give the processor a little time to start watching orders/
PROCESSOR_STARTUP_WAIT_SECONDS = 3

# How often we check whether the queue is empty
QUEUE_CHECK_INTERVAL_SECONDS = 5

# How many consecutive empty checks we require before considering
# the queue drained. This protects against a scraper still writing
# files while the directory happens to be temporarily empty.
QUEUE_EMPTY_CONFIRMATIONS = 3


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("nyaysaathi-runner")


# ============================================================
# GLOBAL PROCESS REFERENCES
# ============================================================

processor_process = None
scraper_process = None


# ============================================================
# QUEUE
# ============================================================


def get_queue_files():
    """
    Return all PDF/JSON files currently present in orders/.

    We deliberately look at both extensions because a judgment
    is represented by a PDF + matching JSON pair.
    """

    if not ORDERS_DIR.exists():
        return []

    return list(ORDERS_DIR.glob("*.pdf")) + list(ORDERS_DIR.glob("*.json"))


def queue_is_empty():
    """
    True when there are no PDF or JSON files remaining in orders/.
    """

    return len(get_queue_files()) == 0


def wait_for_queue_to_drain():
    """
    Wait until orders/ remains empty for several consecutive checks.

    This prevents us from shutting down the processor immediately
    when the queue happens to be empty between scraper writes.
    """

    logger.info("=" * 70)
    logger.info("Scraper finished.")
    logger.info("Waiting for judgment processor to drain orders/...")
    logger.info("=" * 70)

    consecutive_empty_checks = 0

    while True:
        queue_files = get_queue_files()

        if not queue_files:
            consecutive_empty_checks += 1

            logger.info(
                "orders/ is empty "
                f"({consecutive_empty_checks}/"
                f"{QUEUE_EMPTY_CONFIRMATIONS} confirmations)"
            )

            if consecutive_empty_checks >= QUEUE_EMPTY_CONFIRMATIONS:
                logger.info("Queue successfully drained.")
                return True

        else:
            consecutive_empty_checks = 0

            pdf_count = len([f for f in queue_files if f.suffix.lower() == ".pdf"])
            json_count = len([f for f in queue_files if f.suffix.lower() == ".json"])

            logger.info(
                f"Queue still has {pdf_count} PDF(s) + " f"{json_count} JSON file(s)."
            )

        time.sleep(QUEUE_CHECK_INTERVAL_SECONDS)


def cleanup_captcha_tmp():
    """
    Empty captcha-tmp after the scraper and processor have finished.

    The directory itself is preserved; only its contents are removed.
    Cleanup errors are logged as warnings so they do not hide the
    result of an otherwise successful scrape.
    """

    logger.info("=" * 70)
    logger.info("Cleaning captcha-tmp...")
    logger.info("=" * 70)

    if not CAPTCHA_TMP_DIR.exists():
        logger.info("captcha-tmp does not exist. Nothing to clean.")
        return

    removed = 0
    failed = 0

    for item in CAPTCHA_TMP_DIR.iterdir():
        try:
            if item.is_dir() and not item.is_symlink():
                shutil.rmtree(item)
            else:
                item.unlink()
            removed += 1
        except Exception:
            failed += 1
            logger.exception(f"Failed to remove captcha temp item: {item}")

    if failed:
        logger.warning(
            f"captcha-tmp cleanup completed with errors: "
            f"{removed} removed, {failed} failed."
        )
    else:
        logger.info(f"captcha-tmp cleanup complete. Removed {removed} item(s).")


# ============================================================
# PROCESS MANAGEMENT
# ============================================================


def start_processor():
    """
    Start judgment_processor.py as a separate process.
    """

    global processor_process

    logger.info("=" * 70)
    logger.info("Starting judgment processor...")
    logger.info(f"Script: {PROCESSOR_SCRIPT}")
    logger.info("=" * 70)

    processor_process = subprocess.Popen(
        [sys.executable, str(PROCESSOR_SCRIPT)],
        cwd=str(BASE_DIR),
    )

    logger.info(f"Judgment processor started " f"(PID={processor_process.pid})")

    logger.info(
        f"Waiting {PROCESSOR_STARTUP_WAIT_SECONDS}s " "for processor initialization..."
    )

    time.sleep(PROCESSOR_STARTUP_WAIT_SECONDS)

    # Make sure it didn't immediately crash.
    if processor_process.poll() is not None:
        raise RuntimeError(
            "Judgment processor exited during startup "
            f"with code {processor_process.returncode}"
        )


def start_scraper(
    court_codes,
    start_date,
    end_date,
    day_step,
    max_workers,
    dist_code,
    default_dist_codes,
    max_runtime_minutes,
    checkpoint_every,
    compress_pdfs,
):
    """
    Start download.py using its existing CLI.
    """

    global scraper_process

    command = [
        sys.executable,
        str(SCRAPER_SCRIPT),
        "--court_codes",
        court_codes,
        "--start_date",
        start_date,
        "--end_date",
        end_date,
        "--day_step",
        str(day_step),
        "--max_workers",
        str(max_workers),
    ]

    if dist_code:
        command.extend(
            [
                "--dist-code",
                str(dist_code),
            ]
        )

    if default_dist_codes:
        command.append("--default-dist-codes")
    else:
        command.append("--no-default-dist-codes")

    if max_runtime_minutes is not None:
        command.extend(
            [
                "--max-runtime-minutes",
                str(max_runtime_minutes),
            ]
        )

    if checkpoint_every is not None:
        command.extend(
            [
                "--checkpoint-every",
                str(checkpoint_every),
            ]
        )

    if compress_pdfs:
        command.append("--compress-pdfs")
    else:
        command.append("--no-compress-pdfs")

    logger.info("=" * 70)
    logger.info("Starting eCourts scraper...")
    logger.info("=" * 70)
    logger.info("Command:")
    logger.info(" ".join(command))
    logger.info("=" * 70)

    scraper_process = subprocess.Popen(
        command,
        cwd=str(BASE_DIR),
    )

    logger.info(f"Scraper started (PID={scraper_process.pid})")


def stop_processor():
    """
    Gracefully stop the judgment processor.

    On Windows, terminate() is the most reliable simple approach
    for a child Python process.
    """

    global processor_process

    if processor_process is None:
        return

    if processor_process.poll() is not None:
        logger.info(
            "Judgment processor already stopped "
            f"(exit code={processor_process.returncode})"
        )
        return

    logger.info("=" * 70)
    logger.info("Stopping judgment processor...")
    logger.info("=" * 70)

    processor_process.terminate()

    try:
        processor_process.wait(timeout=15)

        logger.info(
            "Judgment processor stopped " f"(exit code={processor_process.returncode})"
        )

    except subprocess.TimeoutExpired:
        logger.warning("Processor did not stop gracefully. " "Killing process...")

        processor_process.kill()
        processor_process.wait()

        logger.info("Judgment processor killed.")


def stop_scraper():
    """
    Stop scraper if it is still running.
    """

    global scraper_process

    if scraper_process is None:
        return

    if scraper_process.poll() is not None:
        return

    logger.warning("Stopping scraper...")

    scraper_process.terminate()

    try:
        scraper_process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        logger.warning("Scraper did not stop gracefully. Killing...")
        scraper_process.kill()
        scraper_process.wait()


# ============================================================
# SIGNAL HANDLING
# ============================================================


def handle_shutdown(signum, frame):
    """
    Handle Ctrl+C / termination.
    """

    logger.warning("=" * 70)
    logger.warning("Shutdown signal received.")
    logger.warning("=" * 70)

    stop_scraper()
    stop_processor()

    sys.exit(1)


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# ============================================================
# ARGUMENTS
# ============================================================


def parse_args():
    parser = argparse.ArgumentParser(
        description=("NyaySaathi scraper + judgment processor orchestrator")
    )

    parser.add_argument(
        "--court_codes",
        required=True,
        help=("Comma-separated court codes. " 'Example: "2~5,3~1"'),
    )

    parser.add_argument(
        "--start_date",
        required=True,
        help="Start date in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--end_date",
        required=True,
        help="End date in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--day_step",
        type=int,
        default=1,
        help="Date step passed to download.py (default: 1)",
    )

    parser.add_argument(
        "--max_workers",
        type=int,
        default=2,
        help="Scraper worker count (default: 2)",
    )

    parser.add_argument(
        "--dist-code",
        default=None,
        help="Optional district code",
    )

    parser.add_argument(
        "--default-dist-codes",
        action="store_true",
        default=False,
        help="Use default district codes",
    )

    parser.add_argument(
        "--no-default-dist-codes",
        action="store_false",
        dest="default_dist_codes",
        help="Disable default district codes",
    )

    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=None,
        help="Optional scraper runtime limit",
    )

    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=None,
        help="Optional scraper checkpoint interval",
    )

    parser.add_argument(
        "--compress-pdfs",
        action="store_true",
        default=False,
        help="Enable PDF compression",
    )

    parser.add_argument(
        "--no-compress-pdfs",
        action="store_false",
        dest="compress_pdfs",
        help="Disable PDF compression",
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================


def main():
    global scraper_process

    args = parse_args()

    logger.info("=" * 70)
    logger.info("NyaySaathi Pipeline Runner")
    logger.info("=" * 70)

    logger.info(f"Courts:       {args.court_codes}")
    logger.info(f"Start date:   {args.start_date}")
    logger.info(f"End date:     {args.end_date}")
    logger.info(f"Day step:     {args.day_step}")
    logger.info(f"Workers:      {args.max_workers}")
    logger.info(f"Orders dir:   {ORDERS_DIR}")
    logger.info("=" * 70)

    # --------------------------------------------------------
    # Make sure the queue directory exists.
    # --------------------------------------------------------

    ORDERS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    try:
        # ----------------------------------------------------
        # 1. Start processor FIRST.
        # ----------------------------------------------------

        start_processor()

        # ----------------------------------------------------
        # 2. Start scraper.
        # ----------------------------------------------------

        start_scraper(
            court_codes=args.court_codes,
            start_date=args.start_date,
            end_date=args.end_date,
            day_step=args.day_step,
            max_workers=args.max_workers,
            dist_code=args.dist_code,
            default_dist_codes=args.default_dist_codes,
            max_runtime_minutes=args.max_runtime_minutes,
            checkpoint_every=args.checkpoint_every,
            compress_pdfs=args.compress_pdfs,
        )

        # ----------------------------------------------------
        # 3. Wait for scraper.
        # ----------------------------------------------------

        scraper_exit_code = scraper_process.wait()

        logger.info("=" * 70)
        logger.info(f"Scraper exited with code {scraper_exit_code}")
        logger.info("=" * 70)

        # ----------------------------------------------------
        # If scraper failed, we still drain whatever it managed
        # to download before exiting.
        # ----------------------------------------------------

        if scraper_exit_code != 0:
            logger.error(
                "Scraper failed. "
                "Will still allow processor to finish "
                "all judgments already downloaded."
            )

        # ----------------------------------------------------
        # 4. Let processor drain the queue.
        # ----------------------------------------------------

        wait_for_queue_to_drain()

        # ----------------------------------------------------
        # 5. Stop processor.
        # ----------------------------------------------------

        stop_processor()

        # ----------------------------------------------------
        # 6. Cleanup temporary captcha files.
        # ----------------------------------------------------

        cleanup_captcha_tmp()

        # ----------------------------------------------------
        # 7. Final status.
        # ----------------------------------------------------

        if scraper_exit_code == 0:
            logger.info("=" * 70)
            logger.info("PIPELINE SUCCESS")
            logger.info("=" * 70)
            return 0

        logger.error("=" * 70)
        logger.error("PIPELINE COMPLETED WITH SCRAPER ERRORS")
        logger.error("=" * 70)

        return scraper_exit_code

    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        stop_scraper()
        stop_processor()
        return 1

    except Exception:
        logger.exception("Pipeline failed unexpectedly.")

        stop_scraper()
        stop_processor()

        return 1


if __name__ == "__main__":
    sys.exit(main())
