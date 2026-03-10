import sqlite3, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

conn = sqlite3.connect(config.DB_PATH)
conn.row_factory = sqlite3.Row
rows = conn.execute("""
    SELECT username, display_name, has_pro, has_services, has_banner, has_website_link
    FROM authors
    WHERE id IN (
        SELECT DISTINCT p.author_id FROM search_results sr
        JOIN projects p ON sr.project_id=p.id
        JOIN snapshots s ON sr.snapshot_id=s.id
        WHERE sr.position <= 50
    )
    ORDER BY has_pro DESC, display_name
""").fetchall()

total = len(rows)
pro_count = sum(1 for r in rows if r["has_pro"])
print(f"Total authors in top-50: {total}")
print(f"has_pro=1: {pro_count} ({pro_count/total*100:.0f}%)")
print(f"has_pro=0: {total - pro_count}")
print()
for r in rows:
    print(f"  {r['display_name'] or r['username']:<30} PRO={r['has_pro']}  Svc={r['has_services']}  Banner={r['has_banner']}  Web={r['has_website_link']}")
conn.close()
