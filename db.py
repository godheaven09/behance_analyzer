"""
Behance Analyzer — Database layer (SQLite)
"""
import sqlite3
import os
from datetime import datetime
from config import DB_PATH, DATA_DIR


def get_connection() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_connection()
    c = conn.cursor()

    c.executescript("""
    CREATE TABLE IF NOT EXISTS snapshots (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       TEXT    NOT NULL,
        query           TEXT    NOT NULL,
        sort_type       TEXT    NOT NULL DEFAULT 'recommended',
        total_collected INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS authors (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        username        TEXT    UNIQUE NOT NULL,
        display_name    TEXT,
        url             TEXT,
        location        TEXT,
        member_since    TEXT,
        bio_text        TEXT,
        has_pro         INTEGER DEFAULT 0,
        has_services    INTEGER DEFAULT 0,
        hire_status     TEXT,
        has_banner      INTEGER DEFAULT 0,
        has_website_link INTEGER DEFAULT 0,
        profile_completeness INTEGER DEFAULT 0,  -- 0-100 score
        first_seen      TEXT,
        last_seen       TEXT
    );

    CREATE TABLE IF NOT EXISTS author_snapshots (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        author_id       INTEGER NOT NULL,
        snapshot_id     INTEGER NOT NULL,
        total_views     INTEGER DEFAULT 0,
        total_appreciations INTEGER DEFAULT 0,
        followers       INTEGER DEFAULT 0,
        following       INTEGER DEFAULT 0,
        project_count   INTEGER DEFAULT 0,
        FOREIGN KEY (author_id) REFERENCES authors(id),
        FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
    );

    CREATE TABLE IF NOT EXISTS projects (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        behance_id          TEXT    UNIQUE NOT NULL,
        title               TEXT,
        url                 TEXT,
        url_slug            TEXT,
        published_date      TEXT,
        publish_day_of_week INTEGER,  -- 1=Mon...7=Sun
        publish_hour        INTEGER,  -- 0-23 UTC
        author_id           INTEGER,
        module_count        INTEGER DEFAULT 0,
        image_count         INTEGER DEFAULT 0,
        video_count         INTEGER DEFAULT 0,
        text_count          INTEGER DEFAULT 0,
        embed_count         INTEGER DEFAULT 0,
        description_length  INTEGER DEFAULT 0,
        description_has_query_keywords INTEGER DEFAULT 0,
        title_keyword_match REAL    DEFAULT 0.0,  -- 0.0-1.0
        has_external_links  INTEGER DEFAULT 0,
        external_link_count INTEGER DEFAULT 0,
        cover_image_url     TEXT,
        cover_image_width   INTEGER,
        cover_image_height  INTEGER,
        comments_count      INTEGER DEFAULT 0,
        saves_count         INTEGER DEFAULT 0,
        is_featured         INTEGER DEFAULT 0,
        co_owners_count     INTEGER DEFAULT 0,
        creative_fields     TEXT,  -- JSON array
        tools_used          TEXT,  -- JSON array
        is_my_project       INTEGER DEFAULT 0,
        first_seen          TEXT,
        last_seen           TEXT,
        FOREIGN KEY (author_id) REFERENCES authors(id)
    );

    CREATE TABLE IF NOT EXISTS project_tags (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id  INTEGER NOT NULL,
        tag_name    TEXT    NOT NULL,
        FOREIGN KEY (project_id) REFERENCES projects(id),
        UNIQUE(project_id, tag_name)
    );

    CREATE TABLE IF NOT EXISTS search_results (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_id     INTEGER NOT NULL,
        project_id      INTEGER NOT NULL,
        position        INTEGER NOT NULL,
        appreciations   INTEGER DEFAULT 0,
        views           INTEGER DEFAULT 0,
        comments        INTEGER DEFAULT 0,
        is_promoted     INTEGER DEFAULT 0,
        is_featured     INTEGER DEFAULT 0,
        cover_image_url TEXT,
        FOREIGN KEY (snapshot_id) REFERENCES snapshots(id),
        FOREIGN KEY (project_id) REFERENCES projects(id)
    );

    CREATE INDEX IF NOT EXISTS idx_search_results_snapshot
        ON search_results(snapshot_id);
    CREATE INDEX IF NOT EXISTS idx_search_results_project
        ON search_results(project_id);
    CREATE INDEX IF NOT EXISTS idx_projects_author
        ON projects(author_id);
    CREATE INDEX IF NOT EXISTS idx_projects_behance_id
        ON projects(behance_id);
    CREATE INDEX IF NOT EXISTS idx_author_snapshots_author
        ON author_snapshots(author_id);
    CREATE INDEX IF NOT EXISTS idx_snapshots_query
        ON snapshots(query);

    CREATE TABLE IF NOT EXISTS tracked_snapshots (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       TEXT    NOT NULL,
        behance_id      TEXT    NOT NULL,
        label           TEXT,
        appreciations   INTEGER DEFAULT 0,
        views           INTEGER DEFAULT 0,
        comments        INTEGER DEFAULT 0,
        position_infografika    INTEGER,  -- NULL = not in top-100
        position_design_cards   INTEGER,  -- NULL = not in top-100
        days_since_publish      REAL
    );

    CREATE INDEX IF NOT EXISTS idx_tracked_behance_id
        ON tracked_snapshots(behance_id);
    """)

    conn.commit()
    conn.close()


def create_snapshot(query: str, sort_type: str = "recommended") -> int:
    conn = get_connection()
    c = conn.cursor()
    c.execute(
        "INSERT INTO snapshots (timestamp, query, sort_type) VALUES (?, ?, ?)",
        (datetime.utcnow().isoformat(), query, sort_type),
    )
    snapshot_id = c.lastrowid
    conn.commit()
    conn.close()
    return snapshot_id


def update_snapshot_count(snapshot_id: int, count: int):
    conn = get_connection()
    conn.execute(
        "UPDATE snapshots SET total_collected = ? WHERE id = ?",
        (count, snapshot_id),
    )
    conn.commit()
    conn.close()


def upsert_author(data: dict) -> int:
    conn = get_connection()
    c = conn.cursor()
    now = datetime.utcnow().isoformat()

    c.execute("SELECT id FROM authors WHERE username = ?", (data["username"],))
    row = c.fetchone()

    if row:
        author_id = row["id"]
        c.execute("""
            UPDATE authors SET
                display_name = COALESCE(?, display_name),
                url = COALESCE(?, url),
                location = COALESCE(?, location),
                member_since = COALESCE(?, member_since),
                bio_text = COALESCE(?, bio_text),
                has_pro = COALESCE(?, has_pro),
                has_services = COALESCE(?, has_services),
                hire_status = COALESCE(?, hire_status),
                has_banner = COALESCE(?, has_banner),
                has_website_link = COALESCE(?, has_website_link),
                profile_completeness = COALESCE(?, profile_completeness),
                last_seen = ?
            WHERE id = ?
        """, (
            data.get("display_name"), data.get("url"),
            data.get("location"), data.get("member_since"),
            data.get("bio_text"), data.get("has_pro"),
            data.get("has_services"), data.get("hire_status"),
            data.get("has_banner"), data.get("has_website_link"),
            data.get("profile_completeness"), now, author_id,
        ))
    else:
        c.execute("""
            INSERT INTO authors (
                username, display_name, url, location, member_since,
                bio_text, has_pro, has_services, hire_status,
                has_banner, has_website_link, profile_completeness,
                first_seen, last_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["username"], data.get("display_name"), data.get("url"),
            data.get("location"), data.get("member_since"),
            data.get("bio_text"), data.get("has_pro", 0),
            data.get("has_services", 0), data.get("hire_status"),
            data.get("has_banner", 0), data.get("has_website_link", 0),
            data.get("profile_completeness", 0), now, now,
        ))
        author_id = c.lastrowid

    conn.commit()
    conn.close()
    return author_id


def upsert_author_snapshot(author_id: int, snapshot_id: int, stats: dict):
    conn = get_connection()
    conn.execute("""
        INSERT INTO author_snapshots (
            author_id, snapshot_id, total_views, total_appreciations,
            followers, following, project_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        author_id, snapshot_id,
        stats.get("total_views", 0), stats.get("total_appreciations", 0),
        stats.get("followers", 0), stats.get("following", 0),
        stats.get("project_count", 0),
    ))
    conn.commit()
    conn.close()


def upsert_project(data: dict) -> int:
    conn = get_connection()
    c = conn.cursor()
    now = datetime.utcnow().isoformat()

    c.execute(
        "SELECT id FROM projects WHERE behance_id = ?",
        (data["behance_id"],),
    )
    row = c.fetchone()

    fields = [
        "title", "url", "url_slug", "published_date",
        "publish_day_of_week", "publish_hour", "author_id",
        "module_count", "image_count", "video_count", "text_count",
        "embed_count", "description_length", "description_has_query_keywords",
        "title_keyword_match", "has_external_links", "external_link_count",
        "cover_image_url", "cover_image_width", "cover_image_height",
        "comments_count", "saves_count", "is_featured", "co_owners_count",
        "creative_fields", "tools_used", "is_my_project",
    ]

    if row:
        project_id = row["id"]
        set_clauses = ", ".join(
            f"{f} = COALESCE(?, {f})" for f in fields
        )
        values = [data.get(f) for f in fields]
        values.extend([now, project_id])
        c.execute(
            f"UPDATE projects SET {set_clauses}, last_seen = ? WHERE id = ?",
            values,
        )
    else:
        all_fields = fields + ["behance_id", "first_seen", "last_seen"]
        placeholders = ", ".join("?" for _ in all_fields)
        col_names = ", ".join(all_fields)
        values = [data.get(f) for f in fields]
        values.extend([data["behance_id"], now, now])
        c.execute(
            f"INSERT INTO projects ({col_names}) VALUES ({placeholders})",
            values,
        )
        project_id = c.lastrowid

    conn.commit()
    conn.close()
    return project_id


def insert_project_tags(project_id: int, tags: list[str]):
    conn = get_connection()
    for tag in tags:
        conn.execute(
            "INSERT OR IGNORE INTO project_tags (project_id, tag_name) VALUES (?, ?)",
            (project_id, tag.strip()),
        )
    conn.commit()
    conn.close()


def insert_search_result(snapshot_id: int, data: dict):
    conn = get_connection()
    conn.execute("""
        INSERT INTO search_results (
            snapshot_id, project_id, position, appreciations, views,
            comments, is_promoted, is_featured, cover_image_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        snapshot_id, data["project_id"], data["position"],
        data.get("appreciations", 0), data.get("views", 0),
        data.get("comments", 0), data.get("is_promoted", 0),
        data.get("is_featured", 0), data.get("cover_image_url"),
    ))
    conn.commit()
    conn.close()


def get_all_snapshots_for_query(query: str) -> list:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM snapshots WHERE query = ? ORDER BY timestamp DESC",
        (query,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_search_results_for_snapshot(snapshot_id: int) -> list:
    conn = get_connection()
    rows = conn.execute("""
        SELECT sr.*, p.title, p.behance_id, p.published_date,
               p.module_count, p.image_count, p.video_count,
               p.description_length, p.is_featured as p_featured,
               p.co_owners_count, p.tools_used, p.creative_fields,
               p.is_my_project, p.title_keyword_match,
               a.username, a.display_name, a.has_pro, a.has_services
        FROM search_results sr
        JOIN projects p ON sr.project_id = p.id
        LEFT JOIN authors a ON p.author_id = a.id
        WHERE sr.snapshot_id = ?
        ORDER BY sr.position ASC
    """, (snapshot_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_author_stats_latest(author_id: int) -> dict | None:
    conn = get_connection()
    row = conn.execute("""
        SELECT * FROM author_snapshots
        WHERE author_id = ?
        ORDER BY snapshot_id DESC LIMIT 1
    """, (author_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def insert_tracked_snapshot(data: dict):
    conn = get_connection()
    conn.execute("""
        INSERT INTO tracked_snapshots (
            timestamp, behance_id, label, appreciations, views,
            comments, position_infografika, position_design_cards,
            days_since_publish
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        data["behance_id"], data.get("label"),
        data.get("appreciations", 0), data.get("views", 0),
        data.get("comments", 0),
        data.get("position_infografika"),
        data.get("position_design_cards"),
        data.get("days_since_publish"),
    ))
    conn.commit()
    conn.close()


def get_tracked_history(behance_id: str) -> list:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM tracked_snapshots WHERE behance_id = ? ORDER BY timestamp ASC",
        (behance_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_my_projects() -> list:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM projects WHERE is_my_project = 1",
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
