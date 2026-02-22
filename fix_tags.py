"""Re-scrape tags for projects that have none, using new JSON parser."""
import asyncio
import os
import sys
import re
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import db


async def fix():
    from playwright.async_api import async_playwright

    conn = db.get_connection()
    projects = conn.execute("""
        SELECT p.id, p.behance_id, p.url, p.title FROM projects p
        WHERE p.id NOT IN (SELECT DISTINCT project_id FROM project_tags)
        AND p.url IS NOT NULL
    """).fetchall()
    conn.close()

    print(f"Projects without tags: {len(projects)}")

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
        truly_empty = 0

        for i, proj in enumerate(projects):
            url = proj["url"]
            try:
                await page.goto(url, wait_until="networkidle", timeout=config.NAVIGATION_TIMEOUT)
                await asyncio.sleep(1)

                html = await page.content()
                tags = []

                tags_json_match = re.findall(r'"tags"\s*:\s*\[(.*?)\]', html)
                for match in tags_json_match:
                    if not match:
                        continue
                    try:
                        items = json.loads(f"[{match}]")
                        for item in items:
                            if isinstance(item, dict) and "title" in item:
                                tag_title = item["title"].strip()
                                if tag_title and tag_title not in tags:
                                    tags.append(tag_title)
                    except (json.JSONDecodeError, TypeError):
                        pass

                if tags:
                    db.insert_project_tags(proj["id"], tags)
                    fixed += 1
                    print(f"  [{i+1}/{len(projects)}] FIXED {len(tags)} tags: {', '.join(tags[:5])}")
                else:
                    truly_empty += 1
                    print(f"  [{i+1}/{len(projects)}] genuinely no tags")

            except Exception as e:
                print(f"  [{i+1}/{len(projects)}] ERROR: {e}")

        await browser.close()

    print(f"\nDone. Fixed: {fixed}, Truly empty: {truly_empty}, Errors: {len(projects) - fixed - truly_empty}")


if __name__ == "__main__":
    asyncio.run(fix())
