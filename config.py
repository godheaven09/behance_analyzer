"""
Behance Analyzer — Configuration
"""
import os
import sys

# Handle both direct execution and cwd-based imports
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
elif '__file__' in dir():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
else:
    BASE_DIR = os.getcwd()

DATA_DIR = os.path.join(BASE_DIR, "data")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
DB_PATH = os.path.join(DATA_DIR, "behance.db")

MY_PROFILE_URL = "https://www.behance.net/valeriy_maslov"
MY_USERNAME = "valeriy_maslov"

SEARCH_QUERIES = {
    "primary": [
        "инфографика",
        "дизайн карточек",
    ],
    "secondary": [
        "карточка товара",
        "инфографика wildberries",
        "дизайн карточек wildberries",
        "инфографика маркетплейс",
    ],
}

# Tracked experiment projects — add behance_id after publishing
# Format: {"label": "description", "behance_id": "123456", "url": "https://..."}
TRACKED_PROJECTS = [
    # {"label": "A_bot_new_title",   "behance_id": "", "url": ""},
    # {"label": "B_organic_new_title", "behance_id": "", "url": ""},
    # {"label": "C_bot_old_title",   "behance_id": "", "url": ""},
]

SORT_TYPE = "recommended"
PROJECTS_PER_QUERY = 100  # ~4 pages
PAGES_PER_QUERY = 5       # with buffer

SCRAPE_DELAY_MIN = 2.0
SCRAPE_DELAY_MAX = 5.0

BEHANCE_BASE_URL = "https://www.behance.net"
SEARCH_URL_TEMPLATE = (
    "https://www.behance.net/search/projects"
    "?search={query}&sort={sort}"
)

REQUEST_TIMEOUT = 30_000  # ms
NAVIGATION_TIMEOUT = 60_000  # ms

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
