"""Find projects that hold positions for long periods."""
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

conn = sqlite3.connect(config.DB_PATH)
conn.row_factory = sqlite3.Row

for query in ["инфографика", "дизайн карточек"]:
    print(f"\n{'='*80}")
    print(f"  LONG-TIMERS: '{query}' — projects in top-20 for 20+ snapshots")
    print(f"{'='*80}\n")

    stable = conn.execute("""
        SELECT p.behance_id, p.title, p.published_date, p.url,
               a.username, a.display_name,
               COUNT(DISTINCT sr.snapshot_id) total_appearances,
               ROUND(AVG(sr.position), 1) avg_pos,
               MIN(sr.position) best_pos,
               MAX(sr.position) worst_pos,
               MAX(sr.appreciations) latest_appr,
               MAX(sr.views) latest_views,
               MIN(sr.appreciations) first_appr,
               MIN(sr.views) first_views
        FROM search_results sr
        JOIN projects p ON sr.project_id=p.id
        JOIN snapshots s ON sr.snapshot_id=s.id
        LEFT JOIN authors a ON p.author_id=a.id
        WHERE s.query=? AND sr.position <= 20
        GROUP BY p.behance_id
        HAVING total_appearances >= 20
        ORDER BY avg_pos ASC
    """, (query,)).fetchall()

    for r in stable:
        appr_growth = r["latest_appr"] - r["first_appr"]
        views_growth = r["latest_views"] - r["first_views"]
        days_age = "?"
        if r["published_date"]:
            from datetime import datetime
            pub = datetime.strptime(r["published_date"], "%Y-%m-%d")
            days_age = (datetime.utcnow() - pub).days

        print(f"  #{r['avg_pos']:>4} (#{r['best_pos']}-#{r['worst_pos']})  age={days_age}d  snaps={r['total_appearances']}")
        print(f"    appr={r['latest_appr']:>5} (+{appr_growth})  views={r['latest_views']:>6} (+{views_growth})")
        print(f"    author: {r['display_name'] or r['username']}")
        print(f"    {r['title'][:70]}")
        print(f"    {r['url'][:80] if r['url'] else ''}")
        print()

# Also check: who dominates both queries
print(f"\n{'='*80}")
print(f"  AUTHORS IN BOTH QUERIES (top-50)")
print(f"{'='*80}\n")

both = conn.execute("""
    SELECT a.username, a.display_name,
           COUNT(DISTINCT CASE WHEN s.query='инфографика' THEN p.behance_id END) inf_projects,
           COUNT(DISTINCT CASE WHEN s.query='дизайн карточек' THEN p.behance_id END) dk_projects
    FROM search_results sr
    JOIN projects p ON sr.project_id=p.id
    JOIN snapshots s ON sr.snapshot_id=s.id
    LEFT JOIN authors a ON p.author_id=a.id
    WHERE sr.position <= 50
    GROUP BY a.username
    HAVING inf_projects > 0 AND dk_projects > 0
    ORDER BY inf_projects + dk_projects DESC
""").fetchall()

for r in both:
    print(f"  {r['display_name'] or r['username']:<30} инфографика: {r['inf_projects']} projects | дизайн карточек: {r['dk_projects']} projects")

conn.close()
