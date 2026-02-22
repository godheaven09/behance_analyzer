import sqlite3, os
db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "behance.db")
c = sqlite3.connect(db)
wt = c.execute("SELECT COUNT(DISTINCT project_id) FROM project_tags").fetchone()[0]
total = c.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
print(f"With tags: {wt}/{total}")
