"""Debug author profile scraping — find actual HTML structure."""
import asyncio
import os
import sys
import re

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
            "https://www.behance.net/Max_Ischenko",
            wait_until="networkidle",
            timeout=60000,
        )
        await asyncio.sleep(3)

        text = await page.inner_text("body")

        print("=== STATS-RELATED LINES ===")
        for line in text.split("\n"):
            ls = line.strip()
            if not ls:
                continue
            low = ls.lower()
            keywords = [
                "view", "просмотр", "оценок", "оценка", "оценки",
                "подписч", "follow", "member", "участник",
                "на behance", "project", "проект",
            ]
            if any(w in low for w in keywords):
                print(f"  LINE: {repr(ls[:120])}")
                print(f"  HEX:  {' '.join(f'{ord(c):04x}' for c in ls[:40])}")
                print()

        # Check for stats table/section
        print("=== TABLE ROWS ===")
        rows = await page.query_selector_all("table tr")
        for r in rows:
            t = (await r.inner_text()).strip()
            if t:
                print(f"  TR: {repr(t[:100])}")

        # Check stats links
        print("\n=== ANALYTICS LINKS ===")
        links = await page.query_selector_all('a[href*="/analytics"]')
        for lnk in links:
            t = (await lnk.inner_text()).strip()
            href = await lnk.get_attribute("href")
            parent_text = await lnk.evaluate(
                "el => el.closest('tr')?.innerText || el.parentElement?.innerText || ''"
            )
            print(f"  LINK: text={repr(t)} href={href}")
            print(f"  PARENT: {repr(parent_text[:100])}")
            print()

        # Check followers/following links
        print("=== FOLLOWER LINKS ===")
        for sel in ['a[href*="/followers"]', 'a[href*="/following"]']:
            els = await page.query_selector_all(sel)
            for el in els:
                t = (await el.inner_text()).strip()
                href = await el.get_attribute("href")
                print(f"  {sel}: text={repr(t)} href={href}")

        # Check member since
        print("\n=== MEMBER SINCE ===")
        for line in text.split("\n"):
            ls = line.strip()
            if any(w in ls for w in ["Member", "Участник", "на Behance"]):
                print(f"  {repr(ls[:100])}")
                print(f"  HEX: {' '.join(f'{ord(c):04x}' for c in ls[:50])}")

        # Check h1
        h1 = await page.query_selector("h1")
        if h1:
            print(f"\n=== H1 === {repr(await h1.inner_text())}")

        # Location
        loc = await page.query_selector('a[href*="search/users"]')
        if loc:
            print(f"=== LOCATION === {repr(await loc.inner_text())}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(debug())
