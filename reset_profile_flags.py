"""Reset all false-positive profile flags and rescan."""
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

conn = sqlite3.connect(config.DB_PATH)

count = conn.execute("SELECT COUNT(*) FROM authors").fetchone()[0]
print(f"Resetting profile flags for {count} authors...")

conn.execute("UPDATE authors SET has_pro=0, has_services=0, has_banner=0, has_website_link=0, profile_completeness=0")
conn.commit()

after = conn.execute("SELECT SUM(has_pro) FROM authors").fetchone()[0]
print(f"has_pro sum after reset: {after}")

conn.close()
print("Done. Run scraper to re-detect correctly.")
