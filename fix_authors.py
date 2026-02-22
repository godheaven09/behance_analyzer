"""Re-scrape all author profiles with fixed parser."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import db
from scraper import scrape_author_profile, _rand_delay


async def fix():
    from playwright.async_api import async_playwright

    conn = db.get_connection()
    authors = conn.execute("SELECT id, username FROM authors").fetchall()
    conn.close()

    print(f"Re-scraping {len(authors)} author profiles...")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=config.USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
        )
        page = await ctx.new_page()
        page.set_default_timeout(config.REQUEST_TIMEOUT)

        fixed = 0
        for i, author in enumerate(authors):
            username = author["username"]
            try:
                profile = await scrape_author_profile(page, username)
                db.upsert_author(profile)

                stats = profile.get("stats", {})
                if stats.get("total_views", 0) > 0:
                    snapshot_id = 1
                    db.upsert_author_snapshot(author["id"], snapshot_id, stats)
                    fixed += 1

                print(f"  [{i+1}/{len(authors)}] {username}: "
                      f"views={stats.get('total_views', 0)} "
                      f"appr={stats.get('total_appreciations', 0)} "
                      f"followers={stats.get('followers', 0)}")
            except Exception as e:
                print(f"  [{i+1}/{len(authors)}] {username}: ERROR {e}")

        await browser.close()

    print(f"\nDone. Fixed {fixed}/{len(authors)} authors with stats.")


if __name__ == "__main__":
    asyncio.run(fix())
