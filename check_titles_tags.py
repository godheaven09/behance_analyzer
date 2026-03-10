"""Deep analysis: do titles/tags matter for TOP positions?"""
import sqlite3
import os
import sys
import re

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

conn = sqlite3.connect(config.DB_PATH)
conn.row_factory = sqlite3.Row

for query in ["инфографика", "дизайн карточек"]:
    last_snap = conn.execute(
        "SELECT id FROM snapshots WHERE query=? ORDER BY id DESC LIMIT 1", (query,)
    ).fetchone()["id"]

    print(f"\n{'='*70}")
    print(f"  TITLE & TAG ANALYSIS: '{query}'")
    print(f"{'='*70}")

    results = conn.execute("""
        SELECT sr.position, sr.appreciations, sr.views, p.title, p.behance_id
        FROM search_results sr
        JOIN projects p ON sr.project_id=p.id
        WHERE sr.snapshot_id=?
        ORDER BY sr.position
    """, (last_snap,)).fetchall()

    print(f"  DEBUG: snapshot_id={last_snap}, results={len(results)}")

    # Check how many titles contain query words
    query_words = query.lower().split()

    print(f"\n--- Does title contain '{query}'? ---")
    for tier, lo, hi in [("Top-10", 1, 10), ("Top-20", 11, 20), ("Top-50", 21, 50), ("51-100", 51, 100)]:
        tier_results = [r for r in results if lo <= r["position"] <= hi]
        contains_all = 0
        contains_any = 0
        for r in tier_results:
            title_low = (r["title"] or "").lower()
            if all(w in title_low for w in query_words):
                contains_all += 1
            if any(w in title_low for w in query_words):
                contains_any += 1
        total = len(tier_results) or 1
        print(f"  {tier:<8}: {contains_all}/{len(tier_results)} contain ALL words ({contains_all/total*100:.0f}%) | {contains_any}/{len(tier_results)} contain ANY word ({contains_any/total*100:.0f}%)")

    # Show actual titles for top-20
    print(f"\n--- Top-20 titles ---")
    for r in results[:20]:
        title = (r["title"] or "")[:65]
        has_query = "YES" if all(w in title.lower() for w in query_words) else "no"
        print(f"  #{r['position']:>3} [{has_query:>3}] appr={r['appreciations']:>5} | {title}")

    # Tags analysis for top-20
    print(f"\n--- Top-20 tags containing query words ---")
    for r in results[:20]:
        tags = conn.execute(
            "SELECT GROUP_CONCAT(tag_name, ', ') t FROM project_tags WHERE project_id=(SELECT id FROM projects WHERE behance_id=?)",
            (r["behance_id"],)
        ).fetchone()
        tag_str = tags["t"] or ""
        has_query_tag = "YES" if any(w in tag_str.lower() for w in query_words) else "no"
        print(f"  #{r['position']:>3} [{has_query_tag:>3}] tags: {tag_str[:80] or 'NONE'}")

# My projects title analysis
print(f"\n{'='*70}")
print(f"  MY PROJECTS: title keyword presence")
print(f"{'='*70}")
my = conn.execute("SELECT title FROM projects WHERE is_my_project=1").fetchall()
for m in my:
    title = m["title"] or ""
    has_inf = "YES" if "инфографика" in title.lower() else "no"
    has_dk = "YES" if "дизайн карточек" in title.lower() else "no"
    print(f"  инфографика={has_inf:>3} | дизайн карточек={has_dk:>3} | {title[:55]}")

conn.close()
