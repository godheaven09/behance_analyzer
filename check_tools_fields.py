"""Check tools, creative fields, description correlation with position."""
import sqlite3
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

conn = sqlite3.connect(config.DB_PATH)
conn.row_factory = sqlite3.Row

for query in ["инфографика", "дизайн карточек"]:
    last_snap = conn.execute(
        "SELECT s.id FROM snapshots s WHERE s.query=? AND s.total_collected > 0 AND EXISTS (SELECT 1 FROM search_results sr WHERE sr.snapshot_id=s.id) ORDER BY s.id DESC LIMIT 1", (query,)
    ).fetchone()["id"]

    print(f"\n{'='*70}")
    print(f"  TOOLS & FIELDS: '{query}'")
    print(f"{'='*70}")

    results = conn.execute("""
        SELECT sr.position, p.tools_used, p.creative_fields, p.description_length
        FROM search_results sr
        JOIN projects p ON sr.project_id=p.id
        WHERE sr.snapshot_id=?
        ORDER BY sr.position
    """, (last_snap,)).fetchall()

    # Tools by tier
    for tier, lo, hi in [("Top-10", 1, 10), ("Top-20", 11, 20), ("Top-50", 21, 50)]:
        tier_results = [r for r in results if lo <= r["position"] <= hi]
        tools_counter = {}
        fields_counter = {}
        desc_lengths = []

        for r in tier_results:
            desc_lengths.append(r["description_length"] or 0)

            if r["tools_used"]:
                try:
                    tools = json.loads(r["tools_used"])
                    for t in tools:
                        tools_counter[t] = tools_counter.get(t, 0) + 1
                except (json.JSONDecodeError, TypeError):
                    tools_counter[r["tools_used"]] = tools_counter.get(r["tools_used"], 0) + 1

            if r["creative_fields"]:
                try:
                    fields = json.loads(r["creative_fields"])
                    for f in fields:
                        fields_counter[f] = fields_counter.get(f, 0) + 1
                except (json.JSONDecodeError, TypeError):
                    pass

        total = len(tier_results) or 1
        avg_desc = sum(desc_lengths) / total

        print(f"\n  --- {tier} ({len(tier_results)} projects) ---")
        print(f"  Avg description length: {avg_desc:.0f} chars")
        print(f"  With description (>0): {sum(1 for d in desc_lengths if d > 0)}/{len(tier_results)}")

        print(f"  Tools:")
        for t, c in sorted(tools_counter.items(), key=lambda x: -x[1])[:8]:
            print(f"    {t:<30} {c}/{len(tier_results)} ({c/total*100:.0f}%)")

        if fields_counter:
            print(f"  Creative Fields:")
            for f, c in sorted(fields_counter.items(), key=lambda x: -x[1])[:5]:
                print(f"    {f:<30} {c}/{len(tier_results)} ({c/total*100:.0f}%)")
        else:
            print(f"  Creative Fields: NONE collected")

    # Has Services correlation
    print(f"\n  --- Has Services ---")
    results_full = conn.execute("""
        SELECT sr.position, a.has_services
        FROM search_results sr
        JOIN projects p ON sr.project_id=p.id
        LEFT JOIN authors a ON p.author_id=a.id
        WHERE sr.snapshot_id=?
        ORDER BY sr.position
    """, (last_snap,)).fetchall()

    for tier, lo, hi in [("Top-10", 1, 10), ("Top-20", 11, 20), ("Top-50", 21, 50), ("51-100", 51, 100)]:
        tier_r = [r for r in results_full if lo <= r["position"] <= hi]
        with_services = sum(1 for r in tier_r if r["has_services"])
        total = len(tier_r) or 1
        print(f"  {tier:<8}: {with_services}/{len(tier_r)} have Services ({with_services/total*100:.0f}%)")

conn.close()
