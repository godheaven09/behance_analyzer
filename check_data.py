"""Data quality check — run on server."""
import sqlite3
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "behance.db")
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

print("=" * 70)
print("  BEHANCE ANALYZER — DATA QUALITY CHECK")
print("=" * 70)

# 1. Snapshots
snaps = conn.execute("SELECT * FROM snapshots").fetchall()
print("\n--- SNAPSHOTS ---")
for s in snaps:
    print(f"  id={s['id']}  query={s['query']!r}  collected={s['total_collected']}  time={s['timestamp']}")

# 2. Projects
total = conn.execute("SELECT COUNT(*) c FROM projects").fetchone()["c"]
with_date = conn.execute("SELECT COUNT(*) c FROM projects WHERE published_date IS NOT NULL").fetchone()["c"]
no_date = conn.execute("SELECT COUNT(*) c FROM projects WHERE published_date IS NULL").fetchone()["c"]
with_tags = conn.execute("SELECT COUNT(DISTINCT project_id) c FROM project_tags").fetchone()["c"]
no_tags = total - with_tags
my_count = conn.execute("SELECT COUNT(*) c FROM projects WHERE is_my_project=1").fetchone()["c"]
with_modules = conn.execute("SELECT COUNT(*) c FROM projects WHERE module_count > 0").fetchone()["c"]
with_desc = conn.execute("SELECT COUNT(*) c FROM projects WHERE description_length > 0").fetchone()["c"]
with_tools = conn.execute("SELECT COUNT(*) c FROM projects WHERE tools_used IS NOT NULL").fetchone()["c"]
with_cover = conn.execute("SELECT COUNT(*) c FROM projects WHERE cover_image_url IS NOT NULL").fetchone()["c"]
featured = conn.execute("SELECT COUNT(*) c FROM projects WHERE is_featured = 1").fetchone()["c"]

print("\n--- PROJECTS ---")
print(f"  Total:             {total}")
print(f"  With date:         {with_date}  |  Missing: {no_date}")
print(f"  With tags:         {with_tags}  |  Missing: {no_tags}")
print(f"  With modules:      {with_modules}  |  Missing: {total - with_modules}")
print(f"  With description:  {with_desc}  |  Missing: {total - with_desc}")
print(f"  With tools:        {with_tools}  |  Missing: {total - with_tools}")
print(f"  With cover image:  {with_cover}  |  Missing: {total - with_cover}")
print(f"  Featured:          {featured}")
print(f"  My projects:       {my_count}")

# 3. Authors
authors_total = conn.execute("SELECT COUNT(*) c FROM authors").fetchone()["c"]
with_location = conn.execute("SELECT COUNT(*) c FROM authors WHERE location IS NOT NULL").fetchone()["c"]
with_bio = conn.execute("SELECT COUNT(*) c FROM authors WHERE bio_text IS NOT NULL AND bio_text != ''").fetchone()["c"]
with_member_since = conn.execute("SELECT COUNT(*) c FROM authors WHERE member_since IS NOT NULL").fetchone()["c"]
author_stats_count = conn.execute("SELECT COUNT(DISTINCT author_id) c FROM author_snapshots WHERE total_views > 0").fetchone()["c"]

print("\n--- AUTHORS ---")
print(f"  Total:             {authors_total}")
print(f"  With location:     {with_location}  |  Missing: {authors_total - with_location}")
print(f"  With bio:          {with_bio}  |  Missing: {authors_total - with_bio}")
print(f"  With member_since: {with_member_since}  |  Missing: {authors_total - with_member_since}")
print(f"  With stats (views>0): {author_stats_count}")

# 4. Search results
sr_total = conn.execute("SELECT COUNT(*) c FROM search_results").fetchone()["c"]
sr_with_views = conn.execute("SELECT COUNT(*) c FROM search_results WHERE views > 0").fetchone()["c"]
sr_with_appr = conn.execute("SELECT COUNT(*) c FROM search_results WHERE appreciations > 0").fetchone()["c"]
sr_promoted = conn.execute("SELECT COUNT(*) c FROM search_results WHERE is_promoted = 1").fetchone()["c"]

print("\n--- SEARCH RESULTS ---")
print(f"  Total entries:     {sr_total}")
print(f"  With views > 0:    {sr_with_views}  |  Zero: {sr_total - sr_with_views}")
print(f"  With appr > 0:     {sr_with_appr}  |  Zero: {sr_total - sr_with_appr}")
print(f"  Promoted:          {sr_promoted}")

# 5. Sample: top project from search
print("\n--- TOP-5 PROJECTS (by position, query 1) ---")
top5 = conn.execute("""
    SELECT sr.position, sr.appreciations, sr.views, p.title, p.published_date,
           p.module_count, p.image_count, p.video_count, p.description_length,
           p.is_featured, p.co_owners_count, p.title_keyword_match,
           a.username, a.has_pro
    FROM search_results sr
    JOIN projects p ON sr.project_id = p.id
    LEFT JOIN authors a ON p.author_id = a.id
    WHERE sr.snapshot_id = 1
    ORDER BY sr.position
    LIMIT 5
""").fetchall()
for r in top5:
    print(f"  #{r['position']:>3} | appr={r['appreciations']:>5} views={r['views']:>6} | "
          f"date={r['published_date']} | mods={r['module_count']:>3} img={r['image_count']:>3} "
          f"vid={r['video_count']:>2} | desc={r['description_length']:>4} | "
          f"featured={r['is_featured']} | {r['username']}")

# 6. My projects
print("\n--- MY PROJECTS ---")
my_projects = conn.execute("""
    SELECT behance_id, title, published_date, module_count, image_count,
           video_count, description_length, title_keyword_match,
           tools_used, creative_fields
    FROM projects WHERE is_my_project = 1
""").fetchall()
for m in my_projects:
    tags = conn.execute(
        "SELECT GROUP_CONCAT(tag_name, ', ') t FROM project_tags WHERE project_id = (SELECT id FROM projects WHERE behance_id = ?)",
        (m["behance_id"],)
    ).fetchone()
    print(f"  {m['title'][:55]:<55} | date={m['published_date']} | "
          f"mods={m['module_count']:>3} img={m['image_count']:>3} vid={m['video_count']:>2} | "
          f"desc={m['description_length']:>4} | kw_match={m['title_keyword_match']} | "
          f"tags={tags['t'] or 'NONE'}")

# 7. Tag distribution for my projects vs top
print("\n--- TAG COMPARISON ---")
my_tags = conn.execute("""
    SELECT COUNT(DISTINCT pt.tag_name) c FROM project_tags pt
    JOIN projects p ON pt.project_id = p.id WHERE p.is_my_project = 1
""").fetchone()["c"]
all_tags = conn.execute("SELECT COUNT(DISTINCT tag_name) c FROM project_tags").fetchone()["c"]
print(f"  Unique tags in top-100 projects: {all_tags}")
print(f"  Unique tags in my projects:      {my_tags}")

# 8. Potential issues
print("\n--- POTENTIAL ISSUES ---")
issues = []
if no_date > total * 0.1:
    issues.append(f"  WARNING: {no_date}/{total} projects missing published_date")
if no_tags > total * 0.5:
    issues.append(f"  WARNING: {no_tags}/{total} projects missing tags")
if total - with_modules > total * 0.1:
    issues.append(f"  WARNING: {total - with_modules}/{total} projects missing module_count")
if total - with_cover > total * 0.5:
    issues.append(f"  WARNING: {total - with_cover}/{total} projects missing cover_image_url")
if my_count == 0:
    issues.append(f"  CRITICAL: No 'my projects' found!")
if sr_total - sr_with_appr > sr_total * 0.3:
    issues.append(f"  WARNING: {sr_total - sr_with_appr}/{sr_total} search results have 0 appreciations")

my_in_search = conn.execute("""
    SELECT COUNT(*) c FROM search_results sr
    JOIN projects p ON sr.project_id = p.id WHERE p.is_my_project = 1
""").fetchone()["c"]
if my_in_search == 0:
    issues.append(f"  INFO: None of your projects appear in search results")

if not issues:
    print("  All OK!")
else:
    for i in issues:
        print(i)

conn.close()
print("\n" + "=" * 70)
