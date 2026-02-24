"""Verify data quality across all snapshots."""
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

conn = sqlite3.connect(config.DB_PATH)
conn.row_factory = sqlite3.Row

print("=" * 70)
print("  DATA QUALITY VERIFICATION — ALL SNAPSHOTS")
print("=" * 70)

# 1. All snapshots
snaps = conn.execute("SELECT * FROM snapshots ORDER BY id").fetchall()
print(f"\n--- SNAPSHOTS ({len(snaps)} total) ---")
for s in snaps:
    print(f"  #{s['id']}  {s['timestamp'][:16]}  query={s['query']!r:20}  collected={s['total_collected']}")

# 2. Consistency: same number collected each time?
print(f"\n--- CONSISTENCY CHECK ---")
for query in ["инфографика", "дизайн карточек"]:
    q_snaps = [s for s in snaps if s["query"] == query]
    counts = [s["total_collected"] for s in q_snaps]
    print(f"  '{query}': {len(q_snaps)} snapshots, collected: {counts}")

# 3. Search results per snapshot
print(f"\n--- SEARCH RESULTS PER SNAPSHOT ---")
for s in snaps:
    sr = conn.execute("SELECT COUNT(*) c, AVG(appreciations) avg_appr, AVG(views) avg_views FROM search_results WHERE snapshot_id=?", (s["id"],)).fetchone()
    zeros_appr = conn.execute("SELECT COUNT(*) c FROM search_results WHERE snapshot_id=? AND appreciations=0", (s["id"],)).fetchone()["c"]
    zeros_views = conn.execute("SELECT COUNT(*) c FROM search_results WHERE snapshot_id=? AND views=0", (s["id"],)).fetchone()["c"]
    print(f"  Snap #{s['id']}: {sr['c']} results | avg_appr={sr['avg_appr']:.1f} avg_views={sr['avg_views']:.1f} | zero_appr={zeros_appr} zero_views={zeros_views}")

# 4. Compare top-5 across snapshots for same query
print(f"\n--- TOP-5 STABILITY (инфографика) ---")
inf_snaps = [s for s in snaps if s["query"] == "инфографика"]
for s in inf_snaps[-3:]:
    top5 = conn.execute("""
        SELECT sr.position, sr.appreciations, sr.views, p.title, p.behance_id
        FROM search_results sr JOIN projects p ON sr.project_id=p.id
        WHERE sr.snapshot_id=? ORDER BY sr.position LIMIT 5
    """, (s["id"],)).fetchall()
    print(f"\n  Snap #{s['id']} ({s['timestamp'][:16]}):")
    for r in top5:
        print(f"    #{r['position']:>3} appr={r['appreciations']:>5} views={r['views']:>6} | {r['behance_id']} | {r['title'][:50]}")

# 5. My projects
print(f"\n--- MY PROJECTS ---")
my = conn.execute("SELECT behance_id, title, published_date, module_count, title_keyword_match FROM projects WHERE is_my_project=1").fetchall()
print(f"  Total: {len(my)}")
for m in my[:3]:
    in_search = conn.execute("""
        SELECT sr.snapshot_id, sr.position FROM search_results sr
        JOIN projects p ON sr.project_id=p.id WHERE p.behance_id=?
    """, (m["behance_id"],)).fetchall()
    positions = [f"snap#{r['snapshot_id']}:#{r['position']}" for r in in_search]
    print(f"  {m['title'][:50]} | date={m['published_date']} | kw={m['title_keyword_match']} | in_search: {positions or 'NONE'}")

# 6. Author stats
print(f"\n--- AUTHOR STATS SAMPLE ---")
top_authors = conn.execute("""
    SELECT a.username, a.location, ast.total_views, ast.total_appreciations, ast.followers
    FROM author_snapshots ast JOIN authors a ON ast.author_id=a.id
    ORDER BY ast.total_views DESC LIMIT 5
""").fetchall()
for a in top_authors:
    print(f"  {a['username']:<25} views={a['total_views']:>8} appr={a['total_appreciations']:>6} followers={a['followers']:>5} loc={a['location']}")

# 7. Dates
print(f"\n--- DATE COVERAGE ---")
with_date = conn.execute("SELECT COUNT(*) c FROM projects WHERE published_date IS NOT NULL").fetchone()["c"]
total = conn.execute("SELECT COUNT(*) c FROM projects").fetchone()["c"]
print(f"  Projects with date: {with_date}/{total}")

# 8. Trend data available?
print(f"\n--- TREND DATA ---")
unique_projects_in_search = conn.execute("""
    SELECT COUNT(DISTINCT p.behance_id) c FROM search_results sr
    JOIN projects p ON sr.project_id=p.id
""").fetchone()["c"]
multi_snapshot = conn.execute("""
    SELECT COUNT(*) c FROM (
        SELECT p.behance_id, COUNT(DISTINCT sr.snapshot_id) as snap_count
        FROM search_results sr JOIN projects p ON sr.project_id=p.id
        GROUP BY p.behance_id HAVING snap_count > 1
    )
""").fetchone()["c"]
print(f"  Unique projects ever in search: {unique_projects_in_search}")
print(f"  Projects in 2+ snapshots (trackable trends): {multi_snapshot}")

conn.close()
print(f"\n{'=' * 70}")
