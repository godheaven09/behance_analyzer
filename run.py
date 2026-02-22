"""
Behance Analyzer — Entry point
Usage:
    python run.py collect              — scrape primary queries
    python run.py collect --all        — scrape primary + secondary queries
    python run.py analyze              — generate analysis report
    python run.py full                 — collect + analyze
    python run.py full --all           — collect all + analyze
    python run.py init                 — initialize database only
"""
import sys
import logging
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("behance_analyzer.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def cmd_init():
    import db
    db.init_db()
    log.info("Database initialized.")


def cmd_collect(include_secondary=False):
    import scraper
    log.info(f"Starting data collection (secondary={include_secondary})...")
    start = time.time()
    scraper.run_scrape(include_secondary=include_secondary)
    elapsed = time.time() - start
    log.info(f"Collection completed in {elapsed:.0f}s ({elapsed/60:.1f}min)")


def cmd_analyze():
    import analyzer
    log.info("Starting analysis...")
    analyzer.generate_full_report()
    log.info("Analysis completed.")


def cmd_full(include_secondary=False):
    cmd_collect(include_secondary)
    cmd_analyze()


def main():
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        sys.exit(0)

    command = args[0].lower()
    include_all = "--all" in args

    log.info(f"=== Behance Analyzer started at {datetime.utcnow().isoformat()} ===")
    log.info(f"Command: {command}, include_secondary: {include_all}")

    if command == "init":
        cmd_init()
    elif command == "collect":
        cmd_collect(include_all)
    elif command == "analyze":
        cmd_analyze()
    elif command == "full":
        cmd_full(include_all)
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
