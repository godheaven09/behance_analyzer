"""Quick rescan of author profiles to fix PRO/banner/services flags."""
import asyncio
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

async def main():
    from playwright.async_api import async_playwright
    from scraper import scrape_author_profile
    import db

    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row

    authors = conn.execute("""
        SELECT DISTINCT a.id, a.username, a.url
        FROM authors a
        WHERE a.id IN (
            SELECT DISTINCT p.author_id FROM search_results sr
            JOIN projects p ON sr.project_id=p.id
            WHERE sr.position <= 50
        )
        AND a.username IS NOT NULL
        ORDER BY a.username
    """).fetchall()
    conn.close()

    print(f"Rescanning {len(authors)} author profiles...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(user_agent=config.USER_AGENT)

        for i, auth in enumerate(authors):
            username = auth["username"]
            print(f"  [{i+1}/{len(authors)}] {username:<30}", end="", flush=True)

            try:
                profile_data = await scrape_author_profile(page, username)
                if profile_data:
                    profile_data["username"] = auth["username"]
                    db.upsert_author(profile_data)
                    pro = profile_data.get("has_pro", 0)
                    svc = profile_data.get("has_services", 0)
                    ban = profile_data.get("has_banner", 0)
                    web = profile_data.get("has_website_link", 0)
                    hire = profile_data.get("hire_status", "")
                    print(f" PRO={pro} Svc={svc} Banner={ban} Web={web} Hire={'yes' if hire else 'no'}")
                else:
                    print(" FAILED (no data)")
            except Exception as e:
                print(f" ERROR: {e}")

            await asyncio.sleep(1.5)

        await browser.close()

    # Summary
    conn2 = sqlite3.connect(config.DB_PATH)
    row = conn2.execute("SELECT SUM(has_pro), SUM(has_services), SUM(has_banner), SUM(has_website_link), COUNT(*) FROM authors").fetchone()
    print(f"\n=== TOTALS ===")
    print(f"  PRO: {row[0]}/{row[4]}")
    print(f"  Services: {row[1]}/{row[4]}")
    print(f"  Banner: {row[2]}/{row[4]}")
    print(f"  Website: {row[3]}/{row[4]}")
    conn2.close()

asyncio.run(main())
