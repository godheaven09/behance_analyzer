"""Analyze thresholds: what numbers needed for each tier."""
import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

conn = sqlite3.connect(config.DB_PATH)

for query in ["инфографика", "дизайн карточек"]:
    last_snap = conn.execute(
        "SELECT id FROM snapshots WHERE query=? ORDER BY id DESC LIMIT 1", (query,)
    ).fetchone()[0]

    print(f"\n{'='*70}")
    print(f"  THRESHOLDS: '{query}'")
    print(f"{'='*70}")
    print(f"{'Tier':<12} {'Avg Appr':>9} {'Avg Views':>10} {'Min Appr':>9} {'Min Views':>10} {'Max Appr':>9} {'Max Views':>10}")
    print("-" * 70)

    for tier, lo, hi in [("Top-10", 1, 10), ("Top-20", 11, 20), ("Top-50", 21, 50), ("Bottom-50", 51, 100)]:
        r = conn.execute("""
            SELECT AVG(sr.appreciations), AVG(sr.views),
                   MIN(sr.appreciations), MIN(sr.views),
                   MAX(sr.appreciations), MAX(sr.views)
            FROM search_results sr
            WHERE sr.snapshot_id=? AND sr.position BETWEEN ? AND ?
        """, (last_snap, lo, hi)).fetchone()
        print(f"{tier:<12} {r[0]:>9.0f} {r[1]:>10.0f} {r[2]:>9} {r[3]:>10} {r[4]:>9} {r[5]:>10}")

# Stability analysis
print(f"\n{'='*70}")
print(f"  STABLE PROJECTS: in top-20 for 25+ snapshots (инфографика)")
print(f"{'='*70}")

stable = conn.execute("""
    SELECT p.title, COUNT(DISTINCT sr.snapshot_id) cnt,
           ROUND(AVG(sr.position),1) avg_pos, MIN(sr.position) best, MAX(sr.position) worst,
           MAX(sr.appreciations) appr, MAX(sr.views) views
    FROM search_results sr
    JOIN projects p ON sr.project_id=p.id
    JOIN snapshots s ON sr.snapshot_id=s.id
    WHERE s.query='инфографика' AND sr.position <= 20
    GROUP BY p.behance_id
    HAVING cnt >= 20
    ORDER BY avg_pos
""").fetchall()

for r in stable:
    print(f"  avg#{r[1]:<3.0f} pos={r[2]:>4} (#{r[3]}-#{r[4]}) appr={r[5]:>5} views={r[6]:>6} | {r[0][:50]}")

# New entrants
print(f"\n{'='*70}")
print(f"  NEW ENTRANTS: appeared in <=5 snapshots (инфографика)")
print(f"{'='*70}")

new = conn.execute("""
    SELECT p.title, MIN(sr.position) best,
           MAX(sr.appreciations) appr, MAX(sr.views) views,
           p.published_date, COUNT(sr.id) appears
    FROM search_results sr
    JOIN projects p ON sr.project_id=p.id
    JOIN snapshots s ON sr.snapshot_id=s.id
    WHERE s.query='инфографика'
    GROUP BY p.behance_id
    HAVING appears <= 5
    ORDER BY best
    LIMIT 15
""").fetchall()

for r in new:
    print(f"  best#{r[1]:>3} appr={r[2]:>5} views={r[3]:>6} pub={r[4]} x{r[5]} | {r[0][:50]}")

# Velocity of top-10 vs rest
print(f"\n{'='*70}")
print(f"  VELOCITY: appr/day and views/day by tier")
print(f"{'='*70}")

for query in ["инфографика", "дизайн карточек"]:
    last_snap = conn.execute(
        "SELECT id FROM snapshots WHERE query=? ORDER BY id DESC LIMIT 1", (query,)
    ).fetchone()[0]

    print(f"\n  '{query}':")
    for tier, lo, hi in [("Top-10", 1, 10), ("Top-20", 11, 20), ("Top-50", 21, 50)]:
        r = conn.execute("""
            SELECT AVG(CAST(sr.appreciations AS REAL) / MAX(julianday('now') - julianday(p.published_date), 1)),
                   AVG(CAST(sr.views AS REAL) / MAX(julianday('now') - julianday(p.published_date), 1))
            FROM search_results sr
            JOIN projects p ON sr.project_id=p.id
            WHERE sr.snapshot_id=? AND sr.position BETWEEN ? AND ?
            AND p.published_date IS NOT NULL
        """, (last_snap, lo, hi)).fetchone()
        if r[0] is not None:
            print(f"    {tier:<12} appr/day={r[0]:>6.1f}  views/day={r[1]:>8.1f}")

conn.close()
