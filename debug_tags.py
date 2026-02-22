"""Debug: check how tags are stored — JSON in script tags vs HTML."""
import asyncio
import os
import sys
import re
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
import db


async def debug():
    from playwright.async_api import async_playwright

    conn = db.get_connection()
    # Get projects: one WITH tags and one WITHOUT
    with_tags = conn.execute("""
        SELECT p.behance_id, p.url, p.title FROM projects p
        JOIN project_tags pt ON pt.project_id = p.id
        GROUP BY p.id LIMIT 1
    """).fetchone()
    without_tags = conn.execute("""
        SELECT p.behance_id, p.url, p.title FROM projects p
        WHERE p.id NOT IN (SELECT DISTINCT project_id FROM project_tags)
        AND p.url IS NOT NULL
        LIMIT 1
    """).fetchone()
    conn.close()

    print(f"Project WITH tags:    {with_tags['title'][:60]}")
    print(f"Project WITHOUT tags: {without_tags['title'][:60]}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=config.USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
        )
        page = await ctx.new_page()

        for label, proj in [("WITH_TAGS", with_tags), ("WITHOUT_TAGS", without_tags)]:
            url = proj["url"]
            print(f"\n{'='*60}")
            print(f"  {label}: {url}")
            print(f"{'='*60}")

            await page.goto(url, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(2)

            # Method 1: Check script tags for JSON data
            scripts = await page.query_selector_all("script")
            for i, script in enumerate(scripts):
                content = await script.inner_html()
                if not content or len(content) < 50:
                    continue
                # Look for tag-related data
                if "tags" in content.lower() or "tag" in content.lower():
                    # Try to find JSON objects
                    for pattern in [r'window\.__APOLLO_STATE__\s*=\s*({.+?});',
                                    r'__NEXT_DATA__.*?({.+})',
                                    r'"tags"\s*:\s*\[([^\]]+)\]']:
                        m = re.search(pattern, content, re.DOTALL)
                        if m:
                            print(f"  SCRIPT [{i}] pattern match: {pattern[:30]}...")
                            print(f"  Content preview: {m.group(0)[:200]}")
                            print()

                    # Also just check if "tags" appears
                    if '"tags"' in content:
                        # Find the tags array
                        tags_match = re.search(r'"tags"\s*:\s*\[([^\]]*)\]', content)
                        if tags_match:
                            print(f"  FOUND TAGS IN SCRIPT [{i}]: {tags_match.group(0)[:300]}")
                            print()

            # Method 2: Check for BeData or similar globals
            try:
                be_data = await page.evaluate("() => { try { return JSON.stringify(window.__BEDATA__ || window.__NEXT_DATA__ || window.__APOLLO_STATE__ || {}).substring(0, 500); } catch(e) { return 'ERROR: ' + e.message; } }")
                if be_data and len(be_data) > 10:
                    print(f"  WINDOW DATA: {be_data[:300]}")
            except Exception as e:
                print(f"  Window data error: {e}")

            # Method 3: Check page source for tags
            html = await page.content()
            tags_in_html = re.findall(r'"tags"\s*:\s*\[(.*?)\]', html)
            if tags_in_html:
                for j, t in enumerate(tags_in_html[:3]):
                    print(f"  HTML TAGS [{j}]: [{t[:200]}]")
            else:
                print(f"  NO 'tags' array found in HTML source")

            # Method 4: Check current CSS-selector based tags
            tag_links = await page.query_selector_all('a[href*="tracking_source=project_tag"]')
            print(f"  CSS selector tags: {len(tag_links)}")
            for tl in tag_links[:5]:
                print(f"    - {await tl.inner_text()}")

            # Method 5: Scroll to bottom and re-check
            print(f"\n  Scrolling to bottom...")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(3)
            tag_links_after = await page.query_selector_all('a[href*="tracking_source=project_tag"]')
            print(f"  CSS selector tags AFTER scroll: {len(tag_links_after)}")
            for tl in tag_links_after[:5]:
                print(f"    - {await tl.inner_text()}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(debug())
