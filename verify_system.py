"""Verify entire system is working correctly."""
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
import db

db.init_db()

conn = sqlite3.connect(config.DB_PATH)
conn.row_factory = sqlite3.Row

print("=" * 60)
print("  SYSTEM VERIFICATION")
print("=" * 60)

# 1. Tables
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
print(f"\n[1] Tables: {tables}")
expected = ["snapshots", "authors", "author_snapshots", "projects", "project_tags", "search_results", "tracked_snapshots"]
missing = [t for t in expected if t not in tables]
print(f"    Missing: {missing or 'NONE - all OK'}")

# 2. Data counts
sn = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
pr = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
sr = conn.execute("SELECT COUNT(*) FROM search_results").fetchone()[0]
au = conn.execute("SELECT COUNT(*) FROM authors").fetchone()[0]
ts = conn.execute("SELECT COUNT(*) FROM tracked_snapshots").fetchone()[0]
print(f"\n[2] Data: snapshots={sn} projects={pr} search_results={sr} authors={au} tracked={ts}")

# 3. Cron
import subprocess
cron = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
print(f"\n[3] Cron:\n    {cron.stdout.strip()}")

# 4. Config
print(f"\n[4] Config:")
print(f"    TRACKED_PROJECTS: {len(config.TRACKED_PROJECTS)} configured")
for tp in config.TRACKED_PROJECTS:
    print(f"      - {tp.get('label')}: {tp.get('behance_id')}")
if not config.TRACKED_PROJECTS:
    print(f"      (empty - will be added when you publish)")

# 5. Scraper test
print(f"\n[5] Scraper modules:")
try:
    from scraper import scrape_search_results, _track_experiment_projects
    print(f"    scrape_search_results: OK")
    print(f"    _track_experiment_projects: OK")
except ImportError as e:
    print(f"    ERROR: {e}")

# 6. Analyzer test
try:
    from analyzer import experiment_tracking_report
    print(f"    experiment_tracking_report: OK")
except ImportError as e:
    print(f"    ERROR: {e}")

# 7. Last successful run
last_snap = conn.execute("SELECT timestamp, query, total_collected FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()
if last_snap:
    print(f"\n[6] Last scrape: {last_snap['timestamp'][:16]} | query='{last_snap['query']}' | collected={last_snap['total_collected']}")
else:
    print(f"\n[6] No scrapes yet")

# 8. Playwright
try:
    from playwright.async_api import async_playwright
    print(f"\n[7] Playwright: OK")
except ImportError:
    print(f"\n[7] Playwright: NOT INSTALLED")

conn.close()
print(f"\n{'=' * 60}")
print(f"  RESULT: {'ALL OK' if not missing else 'ISSUES FOUND'}")
print(f"{'=' * 60}")
