"""
Behance Analyzer — Scraper (Playwright-based)
Collects search results, project details, and author profiles.
"""
import asyncio
import json
import logging
import random
import re
import urllib.parse
from datetime import datetime

from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PwTimeout

import config
import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rand_delay():
    return random.uniform(config.SCRAPE_DELAY_MIN, config.SCRAPE_DELAY_MAX)


def _parse_number(text: str | None) -> int:
    """Parse '1.2K' / '12,426' / '12426' -> int."""
    if not text:
        return 0
    text = text.strip().replace(",", "").replace("\u00a0", "")
    m = re.match(r"([\d.]+)\s*[kKкК]", text)
    if m:
        return int(float(m.group(1)) * 1000)
    m = re.match(r"([\d.]+)\s*[mMмМ]", text)
    if m:
        return int(float(m.group(1)) * 1_000_000)
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else 0


def _extract_behance_id(url: str) -> str | None:
    """Extract gallery ID from URL like /gallery/242129829/..."""
    m = re.search(r"/gallery/(\d+)/", url)
    return m.group(1) if m else None


def _extract_username(url: str) -> str | None:
    """Extract username from profile URL like /valeriy_maslov"""
    m = re.search(r"behance\.net/([^/?#]+)", url)
    if m:
        name = m.group(1)
        if name not in ("search", "gallery", "for_you", "misc", "blog", "jobs"):
            return name
    return None


def _extract_slug(url: str) -> str | None:
    m = re.search(r"/gallery/\d+/([^?#]+)", url)
    return m.group(1) if m else None


def _keyword_match_score(title: str, query: str) -> float:
    """What fraction of query words appear in the title."""
    if not title or not query:
        return 0.0
    title_lower = title.lower()
    words = query.lower().split()
    if not words:
        return 0.0
    matches = sum(1 for w in words if w in title_lower)
    return round(matches / len(words), 2)


MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    # Russian genitive case
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}


def _parse_behance_date(text: str | None) -> tuple[str | None, int | None, int | None]:
    """
    Parse date from Behance project page.
    EN: 'Published: January 13th 2026'
    RU: 'Опубликовано: 13 января 2026 г.'
    Returns (iso_date, day_of_week, hour).
    """
    if not text:
        return None, None, None

    # Remove prefix keywords
    for prefix in ["Published:", "Опубликовано:"]:
        text = text.replace(prefix, "")
    text = text.replace("г.", "").strip()
    text = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", text)
    parts = text.split()
    if len(parts) < 3:
        return None, None, None

    # Try RU format: "13 января 2026"
    try:
        if parts[1].lower() in MONTH_MAP:
            day = int(parts[0])
            month = MONTH_MAP[parts[1].lower()]
            year = int(parts[2])
            dt = datetime(year, month, day)
            return dt.strftime("%Y-%m-%d"), dt.isoweekday(), None
    except (ValueError, IndexError):
        pass

    # Try EN format: "January 13 2026"
    month_str = parts[0].lower()
    month = MONTH_MAP.get(month_str)
    if not month:
        return None, None, None
    try:
        day = int(parts[1])
        year = int(parts[2])
        dt = datetime(year, month, day)
        return dt.strftime("%Y-%m-%d"), dt.isoweekday(), None
    except (ValueError, IndexError):
        return None, None, None


def _parse_member_since(text: str | None) -> str | None:
    """
    Parse member since date.
    EN: 'Member Since: February 12, 2024'
    RU: 'Участник с: 12 февраля 2024 г.' or similar
    """
    if not text:
        return None
    for prefix in ["Member Since:", "Участник с:", "На Behance с:"]:
        text = text.replace(prefix, "")
    text = text.replace(",", "").replace("г.", "").strip()
    parts = text.split()
    if len(parts) < 3:
        return None

    # Try RU: "12 февраля 2024"
    try:
        if len(parts) >= 3 and parts[1].lower() in MONTH_MAP:
            day = int(parts[0])
            month = MONTH_MAP[parts[1].lower()]
            year = int(parts[2])
            return f"{year}-{month:02d}-{day:02d}"
    except (ValueError, IndexError):
        pass

    # Try EN: "February 12 2024"
    month_str = parts[0].lower()
    month = MONTH_MAP.get(month_str)
    if not month:
        return None
    try:
        day = int(parts[1])
        year = int(parts[2])
        return f"{year}-{month:02d}-{day:02d}"
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Search results scraping
# ---------------------------------------------------------------------------

async def scrape_search_results(page: Page, query: str, max_projects: int = 100) -> list[dict]:
    """
    Scrape Behance search results for a given query.
    Returns list of project dicts with basic info.
    """
    encoded = urllib.parse.quote(query)
    url = config.SEARCH_URL_TEMPLATE.format(query=encoded, sort=config.SORT_TYPE)

    log.info(f"Scraping search: '{query}' -> {url}")
    results = []
    position = 0

    for page_num in range(config.PAGES_PER_QUERY):
        if page_num == 0:
            await page.goto(url, wait_until="networkidle", timeout=config.NAVIGATION_TIMEOUT)
        else:
            next_btn = page.locator('a:has-text("Next")')
            if await next_btn.count() == 0:
                log.info(f"No 'Next' button on page {page_num + 1}, stopping")
                break
            next_url = await next_btn.get_attribute("href")
            if not next_url:
                break
            if not next_url.startswith("http"):
                next_url = config.BEHANCE_BASE_URL + next_url
            await page.goto(next_url, wait_until="networkidle", timeout=config.NAVIGATION_TIMEOUT)

        await asyncio.sleep(_rand_delay())

        # Wait for project cards to load
        try:
            await page.wait_for_selector('[class*="ProjectCover"]', timeout=15000)
        except PwTimeout:
            log.warning(f"No project cards found on page {page_num + 1}")
            break

        cards = await page.query_selector_all('[class*="ProjectCover-root"]')
        if not cards:
            cards = await page.query_selector_all('[class*="ProjectCoverNeue"]')
        if not cards:
            cards = await page.query_selector_all('div[class*="Cover"]')

        log.info(f"Page {page_num + 1}: found {len(cards)} cards")

        for card in cards:
            position += 1
            if position > max_projects:
                break

            project_data = await _parse_search_card(card, position)
            if project_data:
                results.append(project_data)

        if position >= max_projects:
            break

        await asyncio.sleep(_rand_delay())

    log.info(f"Total collected for '{query}': {len(results)} projects")
    return results


async def _parse_search_card(card, position: int) -> dict | None:
    """Parse a single project card from search results."""
    try:
        link_el = await card.query_selector("a[href*='/gallery/']")
        if not link_el:
            return None

        href = await link_el.get_attribute("href") or ""
        behance_id = _extract_behance_id(href)
        if not behance_id:
            return None

        title = await link_el.get_attribute("title") or ""
        if not title:
            title_el = await card.query_selector('[class*="Title"]')
            if title_el:
                title = (await title_el.inner_text()).strip()

        full_url = href if href.startswith("http") else config.BEHANCE_BASE_URL + href

        # Author
        author_el = await card.query_selector('a[href*="behance.net/"]:not([href*="/gallery/"])')
        author_username = None
        author_name = None
        if author_el:
            author_href = await author_el.get_attribute("href") or ""
            author_username = _extract_username(author_href)
            author_name = (await author_el.inner_text()).strip()

        # Stats — parse from card inner text
        # RU: "Оценок: 277 за ..." / "Оценка: 1 за ..." / "Оценки: 3 за ..."
        #     "Просмотров: 2 075 для ..." / non-breaking spaces \xa0
        # EN: "277 appreciations for ..." / "2,075 views for ..."
        appr_val = 0
        views_val = 0

        card_text = await card.inner_text()

        # Russian: match Оценок/Оценка/Оценки (all grammatical forms)
        appr_match = re.search(
            "[Оо]ценок|[Оо]ценка|[Оо]ценки",
            card_text,
        )
        if appr_match:
            # Get the number that follows the colon
            after_keyword = card_text[appr_match.start():]
            num_match = re.search(r":\s*([\d\s\xa0,.]+)", after_keyword)
            if num_match:
                appr_val = _parse_number(num_match.group(1).replace("\xa0", "").strip())

        # Russian: match Просмотров/Просмотр/Просмотра
        views_match = re.search(
            "[Пп]росмотров|[Пп]росмотр[аы]?",
            card_text,
        )
        if views_match:
            after_keyword = card_text[views_match.start():]
            num_match = re.search(r":\s*([\d\s\xa0,.]+)", after_keyword)
            if num_match:
                views_val = _parse_number(num_match.group(1).replace("\xa0", "").strip())

        # English fallback
        if appr_val == 0:
            en_appr = re.search(r"([\d,.]+)\s*appreciations?\s+for", card_text, re.IGNORECASE)
            if en_appr:
                appr_val = _parse_number(en_appr.group(1))

        if views_val == 0:
            en_views = re.search(r"([\d,.]+)\s*views?\s+for", card_text, re.IGNORECASE)
            if en_views:
                views_val = _parse_number(en_views.group(1))

        # Promoted check — look for "promoted" text or link to help article about promoted
        is_promoted = 0
        promoted_el = await card.query_selector('a[href*="promoted"], [class*="romoted"]')
        if promoted_el:
            is_promoted = 1
        else:
            card_full_text = await card.inner_text()
            if "promoted" in card_full_text.lower():
                is_promoted = 1

        # Featured check
        is_featured = 0
        featured_el = await card.query_selector('[class*="Featured"], [class*="featured"], [class*="Curated"]')
        if featured_el:
            is_featured = 1

        # Cover image
        cover_url = None
        img_el = await card.query_selector("img")
        if img_el:
            cover_url = await img_el.get_attribute("src") or await img_el.get_attribute("srcset")

        return {
            "position": position,
            "behance_id": behance_id,
            "title": title,
            "url": full_url,
            "url_slug": _extract_slug(href),
            "author_username": author_username,
            "author_name": author_name,
            "appreciations": appr_val,
            "views": views_val,
            "is_promoted": is_promoted,
            "is_featured": is_featured,
            "cover_image_url": cover_url,
        }
    except Exception as e:
        log.warning(f"Error parsing card at position {position}: {e}")
        return None


# ---------------------------------------------------------------------------
# Project detail scraping
# ---------------------------------------------------------------------------

async def scrape_project_details(page: Page, project_url: str, query: str = "") -> dict:
    """Scrape detailed info from a project page."""
    log.info(f"Scraping project: {project_url}")

    try:
        await page.goto(project_url, wait_until="networkidle", timeout=config.NAVIGATION_TIMEOUT)
    except PwTimeout:
        log.warning(f"Timeout loading project: {project_url}")
        await page.goto(project_url, wait_until="domcontentloaded", timeout=config.NAVIGATION_TIMEOUT)

    await asyncio.sleep(_rand_delay())

    data = {}

    # Published date
    page_text = await page.inner_text("body")

    # RU: "Опубликовано: 13 января 2026 г." or EN: "Published: January 13th 2026"
    date_match = re.search(
        r"(?:Опубликовано|Published):\s*(.+?\d{4})\s*г?\.?",
        page_text,
    )
    if date_match:
        pub_date, dow, hour = _parse_behance_date(date_match.group(0))
        data["published_date"] = pub_date
        data["publish_day_of_week"] = dow
        data["publish_hour"] = hour

    # Tags + Tools — extract from embedded JSON in <script> tags (most reliable)
    html_source = await page.content()
    tags = []
    tools = []

    # Parse tags from JSON: "tags":[{"id":123,"title":"design"},...]
    tags_json_match = re.findall(r'"tags"\s*:\s*\[(.*?)\]', html_source)
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

    # Fallback: CSS selector
    if not tags:
        tag_links = await page.query_selector_all('a[href*="tracking_source=project_tag"]')
        for tl in tag_links:
            tag_text = (await tl.inner_text()).strip()
            if tag_text and tag_text not in tags:
                tags.append(tag_text)

    data["tags"] = tags

    # Parse tools from JSON: "tools":[{"id":123,"title":"Photoshop",...},...]
    tools_json_match = re.findall(r'"tools"\s*:\s*\[(.*?)\]', html_source)
    for match in tools_json_match:
        if not match:
            continue
        try:
            items = json.loads(f"[{match}]")
            for item in items:
                if isinstance(item, dict) and "title" in item:
                    tool_title = item["title"].strip()
                    if tool_title and tool_title not in tools:
                        tools.append(tool_title)
        except (json.JSONDecodeError, TypeError):
            pass

    # Fallback: CSS selector
    if not tools:
        tool_links = await page.query_selector_all('a[href*="tools="]')
        for tl in tool_links:
            tool_text = (await tl.inner_text()).strip()
            if tool_text and tool_text not in tools:
                tools.append(tool_text)

    data["tools_used"] = json.dumps(tools, ensure_ascii=False) if tools else None

    # Modules (content blocks)
    modules = await page.query_selector_all('[class*="Permalink"], [class*="module"]')
    image_count = 0
    video_count = 0
    text_count = 0
    embed_count = 0

    for mod in modules:
        mod_html = await mod.inner_html()
        if "<img" in mod_html or "image" in mod_html.lower():
            image_count += 1
        elif "<video" in mod_html or "video" in mod_html.lower():
            video_count += 1
        elif "<iframe" in mod_html or "embed" in mod_html.lower():
            embed_count += 1

    # Alternative: count by Permalink links (each = 1 module)
    permalink_count = await page.locator('a[href*="/modules/"]').count()
    if permalink_count > 0:
        data["module_count"] = permalink_count
        data["image_count"] = max(image_count, permalink_count - video_count - text_count - embed_count)
    else:
        data["module_count"] = image_count + video_count + text_count + embed_count
        data["image_count"] = image_count

    data["video_count"] = video_count
    data["text_count"] = text_count
    data["embed_count"] = embed_count

    # Description / text blocks
    desc_els = await page.query_selector_all('[class*="Description"], [class*="ProjectText"]')
    desc_text = ""
    for d in desc_els:
        desc_text += (await d.inner_text()).strip() + " "
    desc_text = desc_text.strip()
    data["description_length"] = len(desc_text)
    if query:
        data["description_has_query_keywords"] = 1 if any(
            w.lower() in desc_text.lower() for w in query.split()
        ) else 0

    # External links
    all_links = await page.query_selector_all('a[href]')
    external_count = 0
    for lnk in all_links:
        href = await lnk.get_attribute("href") or ""
        if href and not "behance.net" in href and href.startswith("http"):
            external_count += 1
    data["has_external_links"] = 1 if external_count > 0 else 0
    data["external_link_count"] = external_count

    # Comments count
    comments_el = await page.query_selector('[class*="Comments"] [class*="count"], [class*="comment-count"]')
    if comments_el:
        data["comments_count"] = _parse_number(await comments_el.inner_text())

    # Co-owners
    owner_links = await page.query_selector_all('[class*="Owner"] a[href*="behance.net/"]')
    co_owners = set()
    for ol in owner_links:
        uname = _extract_username(await ol.get_attribute("href") or "")
        if uname:
            co_owners.add(uname)
    data["co_owners_count"] = max(0, len(co_owners) - 1)  # minus primary owner

    # Featured badge
    featured_indicators = await page.query_selector_all('[class*="Featured"], [class*="featured-badge"]')
    data["is_featured"] = 1 if len(featured_indicators) > 0 else 0

    # Creative fields — from JSON or CSS
    creative_fields = []
    fields_json_match = re.findall(r'"fields"\s*:\s*\[(.*?)\]', html_source)
    for match in fields_json_match:
        if not match:
            continue
        try:
            items = json.loads(f"[{match}]")
            for item in items:
                if isinstance(item, dict):
                    label = item.get("label") or item.get("title") or item.get("name", "")
                    if label and label not in creative_fields:
                        creative_fields.append(label.strip())
        except (json.JSONDecodeError, TypeError):
            pass

    if not creative_fields:
        cf_links = await page.query_selector_all('a[href*="field="]')
        for cf in cf_links:
            cf_text = (await cf.inner_text()).strip()
            if cf_text and cf_text not in creative_fields:
                creative_fields.append(cf_text)

    data["creative_fields"] = json.dumps(creative_fields, ensure_ascii=False) if creative_fields else None

    return data


# ---------------------------------------------------------------------------
# Author profile scraping
# ---------------------------------------------------------------------------

async def scrape_author_profile(page: Page, username: str) -> dict:
    """Scrape author profile page for stats and metadata."""
    profile_url = f"{config.BEHANCE_BASE_URL}/{username}"
    log.info(f"Scraping profile: {profile_url}")

    try:
        await page.goto(profile_url, wait_until="networkidle", timeout=config.NAVIGATION_TIMEOUT)
    except PwTimeout:
        log.warning(f"Timeout loading profile: {profile_url}")
        await page.goto(profile_url, wait_until="domcontentloaded", timeout=config.NAVIGATION_TIMEOUT)

    await asyncio.sleep(_rand_delay())

    page_text = await page.inner_text("body")
    data = {"username": username, "url": profile_url}

    # Display name
    h1 = await page.query_selector("h1")
    if h1:
        data["display_name"] = (await h1.inner_text()).strip()

    # Location
    loc_el = await page.query_selector('a[href*="search/users?country"]')
    if loc_el:
        data["location"] = (await loc_el.inner_text()).strip()

    # Member since (EN: "Member Since: February 12, 2024", RU: various)
    ms_match = re.search(
        r"(?:Member Since|Участник с|На Behance с):\s*(.+?\d{4})",
        page_text,
    )
    if ms_match:
        data["member_since"] = _parse_member_since(ms_match.group(0))

    # Bio — try multiple selectors
    bio_text = ""
    for bio_sel in ['[class*="UserInfo-bio"]', '[class*="about"]', '[class*="Bio"]', '[class*="bio"]']:
        bio_el = await page.query_selector(bio_sel)
        if bio_el:
            bio_text = (await bio_el.inner_text()).strip()
            if bio_text and len(bio_text) > 10:
                break
    if not bio_text:
        # Try finding "Обо мне" or "Read More" section
        about_match = re.search(r"(?:Обо мне|About)\n(.+?)(?:\n|Read More|Подробнее)", page_text, re.DOTALL)
        if about_match:
            bio_text = about_match.group(1).strip()
    if bio_text:
        data["bio_text"] = bio_text[:1000]

    # Stats table — RU: "Просмотры проекта", "Оценки", "Подписчики", "Подписки"
    #               EN: "Project Views", "Appreciations", "Followers", "Following"
    stats = {}
    stats_rows = await page.query_selector_all("table tr")
    for row in stats_rows:
        row_text = (await row.inner_text()).strip().replace("\xa0", "")
        nums = re.findall(r"\d+", row_text)
        if not nums:
            continue
        num_val = int("".join(nums))
        row_lower = row_text.lower()
        if any(w in row_lower for w in ["просмотры", "project views", "views"]):
            stats["total_views"] = num_val
        elif any(w in row_lower for w in ["оценки", "appreciat"]):
            stats["total_appreciations"] = num_val
        elif any(w in row_lower for w in ["подписчики", "follower"]) and "подписки" not in row_lower and "following" not in row_lower:
            stats["followers"] = num_val
        elif any(w in row_lower for w in ["подписки", "following"]):
            stats["following"] = num_val

    # Fallback: parse from links
    if not stats.get("total_views"):
        analytics_links = await page.query_selector_all('a[href*="/analytics"]')
        for al in analytics_links:
            al_text = (await al.inner_text()).strip()
            num = _parse_number(al_text)
            if num > 0:
                parent = await al.evaluate("el => el.closest('tr')?.innerText || el.parentElement?.innerText || ''")
                if any(w in parent for w in ["Просмотры", "View", "view"]):
                    stats["total_views"] = num
                elif any(w in parent for w in ["Оценки", "Appreciat", "appreciat"]):
                    stats["total_appreciations"] = num

    for selector, key in [
        ('a[href*="/followers"]', "followers"),
        ('a[href*="/following"]', "following"),
    ]:
        if not stats.get(key):
            el = await page.query_selector(selector)
            if el:
                stats[key] = _parse_number(await el.inner_text())

    data["stats"] = stats

    # Pro badge
    pro_el = await page.query_selector('[class*="Pro"], [class*="pro-badge"]')
    data["has_pro"] = 1 if pro_el else 0

    # Services
    services_el = await page.query_selector('[class*="Service"], [class*="service"]')
    data["has_services"] = 1 if services_el else 0

    # Hire status
    hire_el = await page.query_selector('[class*="Hire"], [class*="Available"]')
    if hire_el:
        data["hire_status"] = (await hire_el.inner_text()).strip()[:200]

    # Banner
    banner_el = await page.query_selector('[class*="banner"], [class*="Banner"]')
    data["has_banner"] = 1 if banner_el else 0

    # Website link
    web_el = await page.query_selector('a[href*="http"]:not([href*="behance.net"])')
    data["has_website_link"] = 1 if web_el else 0

    # Profile completeness score (heuristic)
    score = 0
    if data.get("display_name"):
        score += 15
    if data.get("location"):
        score += 10
    if data.get("bio_text"):
        score += 20
    if data.get("has_banner"):
        score += 10
    if data.get("has_website_link"):
        score += 10
    if data.get("hire_status"):
        score += 10
    if stats.get("total_views", 0) > 0:
        score += 15
    if data.get("has_services"):
        score += 10
    data["profile_completeness"] = min(score, 100)

    # Count visible projects
    project_cards = await page.query_selector_all('a[href*="/gallery/"]')
    project_ids = set()
    for pc in project_cards:
        href = await pc.get_attribute("href") or ""
        pid = _extract_behance_id(href)
        if pid:
            project_ids.add(pid)
    stats["project_count"] = len(project_ids)

    return data


# ---------------------------------------------------------------------------
# My profile projects
# ---------------------------------------------------------------------------

async def scrape_my_projects(page: Page, profile_url: str) -> list[dict]:
    """Scrape all project IDs and basic info from my profile."""
    log.info(f"Scraping my projects: {profile_url}")

    await page.goto(profile_url, wait_until="networkidle", timeout=config.NAVIGATION_TIMEOUT)
    await asyncio.sleep(_rand_delay())

    projects = []
    seen_ids = set()

    while True:
        cards = await page.query_selector_all('a[href*="/gallery/"]')
        new_found = False

        for card in cards:
            href = await card.get_attribute("href") or ""
            behance_id = _extract_behance_id(href)
            if not behance_id or behance_id in seen_ids:
                continue
            seen_ids.add(behance_id)
            new_found = True

            title = await card.get_attribute("title") or ""
            if not title:
                title_el = await card.query_selector('[class*="Title"]')
                if title_el:
                    title = (await title_el.inner_text()).strip()

            full_url = href if href.startswith("http") else config.BEHANCE_BASE_URL + href

            # Stats from card
            parent = await card.evaluate("el => el.closest('[class*=\"ProjectCover\"]')?.innerText || el.parentElement?.innerText || ''")
            appr = 0
            views = 0
            nums = re.findall(r"([\d,.]+[kKмМ]?)", parent)
            if len(nums) >= 2:
                appr = _parse_number(nums[-2])
                views = _parse_number(nums[-1])

            projects.append({
                "behance_id": behance_id,
                "title": title,
                "url": full_url,
                "url_slug": _extract_slug(href),
                "appreciations": appr,
                "views": views,
                "is_my_project": 1,
            })

        # Try to load more (pagination)
        next_btn = await page.query_selector('a:has-text("Next page"), a[class*="next"]')
        if next_btn and new_found:
            next_url = await next_btn.get_attribute("href")
            if next_url:
                if not next_url.startswith("http"):
                    next_url = config.BEHANCE_BASE_URL + next_url
                await page.goto(next_url, wait_until="networkidle", timeout=config.NAVIGATION_TIMEOUT)
                await asyncio.sleep(_rand_delay())
                continue
        break

    log.info(f"Found {len(projects)} projects on my profile")
    return projects


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

async def run_full_scrape(queries: list[str] | None = None, include_secondary: bool = False):
    """
    Full scrape pipeline:
    1. Scrape search results for each query
    2. Scrape project details for each unique project
    3. Scrape author profiles for each unique author
    4. Scrape my projects
    5. Store everything in SQLite
    """
    db.init_db()

    if queries is None:
        queries = list(config.SEARCH_QUERIES["primary"])
        if include_secondary:
            queries.extend(config.SEARCH_QUERIES["secondary"])

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=config.USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
        )
        page = await context.new_page()
        page.set_default_timeout(config.REQUEST_TIMEOUT)

        all_search_data = {}
        all_projects = {}
        all_authors = {}

        # 1. Search results
        for query in queries:
            snapshot_id = db.create_snapshot(query, config.SORT_TYPE)
            results = await scrape_search_results(page, query, config.PROJECTS_PER_QUERY)

            for r in results:
                r["snapshot_id"] = snapshot_id
                r["query"] = query
                r["title_keyword_match"] = _keyword_match_score(r.get("title", ""), query)

                pid = r["behance_id"]
                if pid not in all_projects:
                    all_projects[pid] = r

                uname = r.get("author_username")
                if uname and uname not in all_authors:
                    all_authors[uname] = {"username": uname, "display_name": r.get("author_name")}

            all_search_data[query] = {"snapshot_id": snapshot_id, "results": results}
            db.update_snapshot_count(snapshot_id, len(results))
            log.info(f"Search '{query}': {len(results)} results saved to snapshot {snapshot_id}")

        # 2. My projects
        log.info("Scraping my profile...")
        my_projects = await scrape_my_projects(page, config.MY_PROFILE_URL)
        my_username = config.MY_USERNAME

        for mp in my_projects:
            pid = mp["behance_id"]
            if pid not in all_projects:
                all_projects[pid] = mp
            else:
                all_projects[pid]["is_my_project"] = 1
            # Calculate keyword match for my projects against all queries
            best_match = 0.0
            for q in queries:
                score = _keyword_match_score(mp.get("title", ""), q)
                best_match = max(best_match, score)
            all_projects.setdefault(pid, mp)["title_keyword_match"] = best_match

        if my_username not in all_authors:
            all_authors[my_username] = {"username": my_username}

        # 3. Project details
        log.info(f"Scraping details for {len(all_projects)} projects...")
        project_db_ids = {}

        for i, (pid, pdata) in enumerate(all_projects.items()):
            purl = pdata.get("url")
            if not purl:
                continue

            query_for_project = pdata.get("query", queries[0] if queries else "")

            try:
                details = await scrape_project_details(page, purl, query_for_project)
                pdata.update(details)
            except Exception as e:
                log.warning(f"Error scraping project {pid}: {e}")

            log.info(f"  [{i+1}/{len(all_projects)}] {pdata.get('title', pid)}")

        # 4. Author profiles
        log.info(f"Scraping {len(all_authors)} author profiles...")
        author_db_ids = {}

        for i, (uname, adata) in enumerate(all_authors.items()):
            try:
                profile = await scrape_author_profile(page, uname)
                adata.update(profile)
            except Exception as e:
                log.warning(f"Error scraping author {uname}: {e}")

            author_db_id = db.upsert_author(adata)
            author_db_ids[uname] = author_db_id

            stats = adata.get("stats", {})
            for query, sdata in all_search_data.items():
                db.upsert_author_snapshot(author_db_id, sdata["snapshot_id"], stats)
                break  # one snapshot is enough for author stats

            log.info(f"  [{i+1}/{len(all_authors)}] {adata.get('display_name', uname)}")

        # 5. Save projects to DB
        log.info("Saving projects to database...")
        for pid, pdata in all_projects.items():
            uname = pdata.get("author_username")
            pdata["author_id"] = author_db_ids.get(uname)
            pdata["behance_id"] = pid

            project_db_id = db.upsert_project(pdata)
            project_db_ids[pid] = project_db_id

            if pdata.get("tags"):
                db.insert_project_tags(project_db_id, pdata["tags"])

        # 6. Save search results to DB
        log.info("Saving search results to database...")
        for query, sdata in all_search_data.items():
            for r in sdata["results"]:
                pid = r["behance_id"]
                db_project_id = project_db_ids.get(pid)
                if not db_project_id:
                    continue
                db.insert_search_result(sdata["snapshot_id"], {
                    "project_id": db_project_id,
                    "position": r["position"],
                    "appreciations": r.get("appreciations", 0),
                    "views": r.get("views", 0),
                    "comments": r.get("comments", 0),
                    "is_promoted": r.get("is_promoted", 0),
                    "is_featured": r.get("is_featured", 0),
                    "cover_image_url": r.get("cover_image_url"),
                })

        # 7. Track experiment projects
        if config.TRACKED_PROJECTS:
            log.info(f"Tracking {len(config.TRACKED_PROJECTS)} experiment projects...")
            await _track_experiment_projects(page, all_search_data)

        await browser.close()

    log.info("Full scrape completed!")
    return all_search_data


async def _track_experiment_projects(page: Page, search_data: dict):
    """Scrape current stats for tracked experiment projects and find their positions."""
    from datetime import datetime

    for tp in config.TRACKED_PROJECTS:
        bid = tp.get("behance_id")
        if not bid:
            continue

        label = tp.get("label", bid)
        url = tp.get("url") or f"{config.BEHANCE_BASE_URL}/gallery/{bid}/"
        log.info(f"  Tracking '{label}' ({bid})...")

        appr = 0
        views = 0
        comments = 0
        days_since = None

        try:
            await page.goto(url, wait_until="networkidle", timeout=config.NAVIGATION_TIMEOUT)
            await asyncio.sleep(_rand_delay())

            card_text = await page.inner_text("body")

            # Parse appreciations and views from project page stats
            # Page shows stats in the profile card area or we can count from inner text
            appr_match = re.search(r"[Оо]ценок|[Оо]ценка|[Оо]ценки", card_text)
            if appr_match:
                after = card_text[appr_match.start():]
                nm = re.search(r":\s*([\d\s\xa0,.]+)", after)
                if nm:
                    # This finds per-project appreciations in comments area, not total
                    pass

            # Better: parse from the search card data we already have
            # Use the page's own stat display
            page_html = await page.content()

            # Extract stats from embedded JSON
            stats_match = re.search(r'"stats"\s*:\s*\{([^}]+)\}', page_html)
            if stats_match:
                try:
                    stats_str = "{" + stats_match.group(1) + "}"
                    import json as _json
                    stats_obj = _json.loads(stats_str)
                    appr = stats_obj.get("appreciations", 0)
                    views = stats_obj.get("views", 0)
                    comments = stats_obj.get("comments", 0)
                except Exception:
                    pass

            # Fallback: parse from visible text
            if views == 0:
                views_m = re.search(r"[Пп]росмотров|[Пп]росмотр[аы]?", card_text)
                if views_m:
                    after = card_text[views_m.start():]
                    nm = re.search(r":\s*([\d\s\xa0,.]+)", after)
                    if nm:
                        views = _parse_number(nm.group(1).replace("\xa0", ""))

            # Published date for days_since
            date_match = re.search(
                r"(?:Опубликовано|Published):\s*(.+?\d{4})\s*г?\.?",
                card_text,
            )
            if date_match:
                pub_date, _, _ = _parse_behance_date(date_match.group(0))
                if pub_date:
                    pub_dt = datetime.strptime(pub_date, "%Y-%m-%d")
                    days_since = (datetime.utcnow() - pub_dt).total_seconds() / 86400

        except Exception as e:
            log.warning(f"  Error tracking {bid}: {e}")

        # Find positions in search results
        pos_info = {}
        pos_info["position_infografika"] = None
        pos_info["position_design_cards"] = None

        query_map = {"инфографика": "position_infografika", "дизайн карточек": "position_design_cards"}
        for query, field in query_map.items():
            if query in search_data:
                for r in search_data[query]["results"]:
                    if r["behance_id"] == bid:
                        pos_info[field] = r["position"]
                        break

        db.insert_tracked_snapshot({
            "behance_id": bid,
            "label": label,
            "appreciations": appr,
            "views": views,
            "comments": comments,
            "position_infografika": pos_info["position_infografika"],
            "position_design_cards": pos_info["position_design_cards"],
            "days_since_publish": round(days_since, 2) if days_since else None,
        })

        found_in = []
        if pos_info["position_infografika"]:
            found_in.append(f"инфографика:#{pos_info['position_infografika']}")
        if pos_info["position_design_cards"]:
            found_in.append(f"дизайн карточек:#{pos_info['position_design_cards']}")

        log.info(f"    {label}: appr={appr} views={views} | "
                 f"{'FOUND: ' + ', '.join(found_in) if found_in else 'NOT in top-100'}")


def run_scrape(queries=None, include_secondary=False):
    """Sync wrapper for run_full_scrape."""
    return asyncio.run(run_full_scrape(queries, include_secondary))


if __name__ == "__main__":
    run_scrape()
