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
# Experiment v1 (completed):
#   P1 = boost 100 + comments -> never entered top-100
# Experiment v2 (active): 4 re-uploaded projects, different strategies
#   P2 = control (zero boost)
#   P3 = organic seed (real people, 10-20 appr in 48h)
#   P4 = drip boost (10 appr/day for 10 days, no comments)
#   P5 = burst + pause (50 appr day 1-2, pause, then adapt)
TRACKED_PROJECTS = [
    {"label": "P1_boost100_comments", "behance_id": "245554661", "url": "https://www.behance.net/gallery/245554661/infografika-dizajn-kartochek-dlja-WB-OZON-kabel"},
    {"label": "P2_control_zero",   "behance_id": "247048877", "url": "https://www.behance.net/gallery/247048877/infografika-dizajn-kartochek-dlja-Wb-Ozon-parfjum"},
    {"label": "P3_organic_seed",   "behance_id": "247049331", "url": "https://www.behance.net/gallery/247049331/infografika-dizajn-kartochek-dlja-Wb-Ozon-audiokabel"},
    {"label": "P4_drip_boost",     "behance_id": "247049545", "url": "https://www.behance.net/gallery/247049545/infografika-dizajn-kartochek-dlja-Wb-Ozon-poloski"},
    {"label": "P5_burst_pause",    "behance_id": "247050017", "url": "https://www.behance.net/gallery/247050017/infografika-dizajn-kartochek-dlja-Wb-Ozon-ochki"},
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
