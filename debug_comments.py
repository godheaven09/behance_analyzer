"""Debug: find where comments data lives on a project page."""
import asyncio
import os
import sys
import re
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config


async def debug():
    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=config.USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
        )
        page = await ctx.new_page()
        await page.goto(
            "https://www.behance.net/gallery/242129829/infografikadizajn-kartochek-dlja-Wildberries-Ozon-1",
            wait_until="networkidle",
            timeout=60000,
        )
        await asyncio.sleep(3)

        html = await page.content()

        # 1. Search for "stats" object in JSON
        print("=== STATS in JSON ===")
        stats_matches = re.findall(r'"stats"\s*:\s*\{[^}]{5,200}\}', html)
        for i, m in enumerate(stats_matches[:5]):
            print(f"  [{i}] {m[:200]}")

        # 2. Search for "comment" anywhere in JSON
        print("\n=== COMMENT in JSON ===")
        comment_matches = re.findall(r'"comment\w*"\s*:\s*\d+', html)
        for i, m in enumerate(comment_matches[:10]):
            print(f"  [{i}] {m}")

        # 3. Search for "appreciations" and "views" in JSON to find stats block
        print("\n=== APPRECIATIONS/VIEWS in JSON ===")
        appr_matches = re.findall(r'"appreciations"\s*:\s*\d+', html)
        for i, m in enumerate(appr_matches[:5]):
            print(f"  [{i}] {m}")
        views_matches = re.findall(r'"views"\s*:\s*\d+', html)
        for i, m in enumerate(views_matches[:5]):
            print(f"  [{i}] {m}")

        # 4. Find the full stats block with context
        print("\n=== FULL STATS BLOCKS ===")
        full_stats = re.findall(r'\{[^{}]*"appreciations"\s*:\s*\d+[^{}]*\}', html)
        for i, m in enumerate(full_stats[:3]):
            print(f"  [{i}] {m[:300]}")

        # 5. Check visible text for comments section
        print("\n=== VISIBLE TEXT: comment-related ===")
        text = await page.inner_text("body")
        for line in text.split("\n"):
            ls = line.strip()
            if not ls:
                continue
            low = ls.lower()
            if "комментар" in low or "comment" in low:
                print(f"  TEXT: {repr(ls[:100])}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(debug())
