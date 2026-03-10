"""
Deep analysis of long-timer authors — what do they do differently?
"""
import sqlite3
import os
import sys
import json
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

conn = sqlite3.connect(config.DB_PATH)
conn.row_factory = sqlite3.Row

LONGTIMER_THRESHOLD = 20  # min snapshots in top-20

def get_longtimer_authors():
    """Get authors who appear in top-20 for 20+ snapshots in either query."""
    rows = conn.execute("""
        SELECT a.id, a.username, a.display_name, a.url, a.location,
               a.member_since, a.bio_text, a.has_pro, a.has_services,
               a.hire_status, a.has_banner, a.has_website_link,
               a.profile_completeness
        FROM authors a
        WHERE a.id IN (
            SELECT DISTINCT p.author_id
            FROM search_results sr
            JOIN projects p ON sr.project_id = p.id
            JOIN snapshots s ON sr.snapshot_id = s.id
            WHERE sr.position <= 20
            GROUP BY p.author_id, s.query
            HAVING COUNT(DISTINCT sr.snapshot_id) >= ?
        )
        AND a.username IS NOT NULL
    """, (LONGTIMER_THRESHOLD,)).fetchall()
    return [dict(r) for r in rows]


def get_author_latest_stats(author_id):
    row = conn.execute("""
        SELECT total_views, total_appreciations, followers, following, project_count
        FROM author_snapshots WHERE author_id = ?
        ORDER BY snapshot_id DESC LIMIT 1
    """, (author_id,)).fetchone()
    return dict(row) if row else {}


def get_author_stats_history(author_id):
    rows = conn.execute("""
        SELECT asn.*, s.timestamp
        FROM author_snapshots asn
        JOIN snapshots s ON asn.snapshot_id = s.id
        WHERE asn.author_id = ?
        ORDER BY s.timestamp ASC
    """, (author_id,)).fetchall()
    return [dict(r) for r in rows]


def get_author_projects_in_search(author_id):
    """All projects by this author that appeared in search results."""
    rows = conn.execute("""
        SELECT p.*, 
               GROUP_CONCAT(DISTINCT pt.tag_name) as tags,
               COUNT(DISTINCT sr.snapshot_id) as total_appearances,
               MIN(sr.position) as best_pos,
               MAX(sr.position) as worst_pos,
               ROUND(AVG(sr.position), 1) as avg_pos,
               MAX(sr.appreciations) as max_appr,
               MIN(sr.appreciations) as min_appr,
               MAX(sr.views) as max_views,
               MIN(sr.views) as min_views,
               MAX(sr.comments) as max_comments,
               MIN(sr.comments) as min_comments,
               GROUP_CONCAT(DISTINCT s.query) as queries_found_in
        FROM projects p
        JOIN search_results sr ON sr.project_id = p.id
        JOIN snapshots s ON sr.snapshot_id = s.id
        LEFT JOIN project_tags pt ON pt.project_id = p.id
        WHERE p.author_id = ?
        GROUP BY p.id
        ORDER BY total_appearances DESC
    """, (author_id,)).fetchall()
    return [dict(r) for r in rows]


def get_position_trajectory(project_id):
    """How did this project's position change over time?"""
    rows = conn.execute("""
        SELECT s.timestamp, s.query, sr.position, sr.appreciations, sr.views, sr.comments
        FROM search_results sr
        JOIN snapshots s ON sr.snapshot_id = s.id
        WHERE sr.project_id = ?
        ORDER BY s.timestamp ASC
    """, (project_id,)).fetchall()
    return [dict(r) for r in rows]


def get_all_projects_by_author(author_id):
    """All projects, not just those in search."""
    rows = conn.execute("""
        SELECT * FROM projects WHERE author_id = ?
        ORDER BY published_date DESC
    """, (author_id,)).fetchall()
    return [dict(r) for r in rows]


def calc_posting_frequency(projects):
    dates = []
    for p in projects:
        if p.get("published_date"):
            try:
                dates.append(datetime.strptime(p["published_date"], "%Y-%m-%d"))
            except:
                pass
    if len(dates) < 2:
        return None, None, None
    dates.sort()
    gaps = [(dates[i+1] - dates[i]).days for i in range(len(dates)-1)]
    avg_gap = sum(gaps) / len(gaps) if gaps else None
    min_gap = min(gaps) if gaps else None
    total_span = (dates[-1] - dates[0]).days
    return avg_gap, min_gap, total_span


def analyze_engagement_velocity(trajectory):
    """Calc how fast engagement grows per day for a project across snapshots."""
    if len(trajectory) < 2:
        return {}
    first = trajectory[0]
    last = trajectory[-1]
    try:
        t0 = datetime.fromisoformat(first["timestamp"])
        t1 = datetime.fromisoformat(last["timestamp"])
        days = (t1 - t0).total_seconds() / 86400
    except:
        return {}
    if days < 1:
        return {}
    appr_vel = (last["appreciations"] - first["appreciations"]) / days
    views_vel = (last["views"] - first["views"]) / days
    comments_vel = (last["comments"] - first["comments"]) / days if first["comments"] is not None and last["comments"] is not None else 0
    return {
        "appr_per_day": round(appr_vel, 2),
        "views_per_day": round(views_vel, 2),
        "comments_per_day": round(comments_vel, 2),
        "monitoring_days": round(days, 1),
    }


def analyze_position_stability(trajectory, query):
    """How stable is position? Low std = stable."""
    positions = [t["position"] for t in trajectory if t["query"] == query]
    if not positions:
        return {}
    avg = sum(positions) / len(positions)
    variance = sum((p - avg)**2 for p in positions) / len(positions)
    std = variance ** 0.5
    return {
        "count": len(positions),
        "avg": round(avg, 1),
        "std": round(std, 2),
        "min": min(positions),
        "max": max(positions),
        "range": max(positions) - min(positions),
    }


def print_divider(title):
    print(f"\n{'='*90}")
    print(f"  {title}")
    print(f"{'='*90}")


def main():
    authors = get_longtimer_authors()
    print(f"\nНайдено {len(authors)} авторов-долгожителей (top-20, {LONGTIMER_THRESHOLD}+ снимков)\n")

    all_author_data = []

    for auth in authors:
        aid = auth["id"]
        name = auth["display_name"] or auth["username"]
        
        if not name or name == "None":
            continue

        stats = get_author_latest_stats(aid)
        stats_hist = get_author_stats_history(aid)
        search_projects = get_author_projects_in_search(aid)
        all_projects = get_all_projects_by_author(aid)
        
        if not search_projects:
            continue

        print_divider(f"АВТОР: {name} (@{auth['username']})")

        # ─── PROFILE ───
        print(f"\n  📍 Локация: {auth.get('location', '?')}")
        print(f"  📅 На Behance с: {auth.get('member_since', '?')}")
        print(f"  🔑 PRO: {'✅' if auth.get('has_pro') else '❌'}  Services: {'✅' if auth.get('has_services') else '❌'}  Banner: {'✅' if auth.get('has_banner') else '❌'}  Website: {'✅' if auth.get('has_website_link') else '❌'}")
        print(f"  💼 Hire status: {auth.get('hire_status', '?')}")
        
        if stats:
            print(f"\n  📊 Профиль:")
            print(f"     Followers: {stats.get('followers', 0):,}")
            print(f"     Following: {stats.get('following', 0):,}")
            print(f"     Total views: {stats.get('total_views', 0):,}")
            print(f"     Total appr: {stats.get('total_appreciations', 0):,}")
            print(f"     Project count: {stats.get('project_count', 0)}")

        # ─── FOLLOWER GROWTH ───
        if len(stats_hist) >= 2:
            f_first = stats_hist[0].get("followers", 0) or 0
            f_last = stats_hist[-1].get("followers", 0) or 0
            v_first = stats_hist[0].get("total_views", 0) or 0
            v_last = stats_hist[-1].get("total_views", 0) or 0
            print(f"\n  📈 Рост за период мониторинга:")
            print(f"     Followers: {f_first} → {f_last} (+{f_last - f_first})")
            print(f"     Total views: {v_first:,} → {v_last:,} (+{v_last - v_first:,})")

        # ─── POSTING FREQUENCY ───
        avg_gap, min_gap, total_span = calc_posting_frequency(all_projects)
        print(f"\n  📤 Частота публикаций:")
        print(f"     Всего проектов в БД: {len(all_projects)}")
        if avg_gap is not None:
            print(f"     Средний интервал: {avg_gap:.1f} дней")
            print(f"     Минимальный интервал: {min_gap} дней")
            print(f"     Общий период: {total_span} дней")
            if avg_gap > 0:
                print(f"     ≈ {30/avg_gap:.1f} проектов в месяц")

        # ─── PROJECTS IN SEARCH ───
        print(f"\n  🔍 Проектов в поиске: {len(search_projects)}")
        
        for p in search_projects:
            print(f"\n  ┌─ Проект: {p['title'][:75]}")
            print(f"  │  Behance ID: {p['behance_id']}")
            print(f"  │  Опубликован: {p.get('published_date', '?')}")
            
            if p.get("published_date"):
                try:
                    pub = datetime.strptime(p["published_date"], "%Y-%m-%d")
                    age = (datetime.utcnow() - pub).days
                    print(f"  │  Возраст: {age} дней")
                except:
                    pass
            
            print(f"  │  Запросы: {p.get('queries_found_in', '?')}")
            print(f"  │  Появлений в топе: {p['total_appearances']} снимков")
            print(f"  │  Позиция: avg={p['avg_pos']} (#{p['best_pos']}-#{p['worst_pos']})")
            
            # Metrics
            appr_growth = (p["max_appr"] or 0) - (p["min_appr"] or 0)
            views_growth = (p["max_views"] or 0) - (p["min_views"] or 0)
            comments_growth = (p["max_comments"] or 0) - (p["min_comments"] or 0)
            print(f"  │  Appr: {p['max_appr']} (+{appr_growth} за мониторинг)")
            print(f"  │  Views: {p['max_views']} (+{views_growth} за мониторинг)")
            print(f"  │  Comments: {p['max_comments']} (+{comments_growth} за мониторинг)")
            print(f"  │  Comments (project page): {p.get('comments_count', 0)}")
            
            # Content structure
            print(f"  │  Модулей: {p.get('module_count', 0)} | Картинок: {p.get('image_count', 0)} | Видео: {p.get('video_count', 0)} | Текст: {p.get('text_count', 0)} | Embed: {p.get('embed_count', 0)}")
            print(f"  │  Описание: {p.get('description_length', 0)} символов")
            print(f"  │  Title keyword match: {p.get('title_keyword_match', 0)}")
            
            # Tags
            tags = p.get("tags", "")
            print(f"  │  Теги: {tags if tags else 'нет'}")
            
            # Tools & Creative fields
            tools = p.get("tools_used", "")
            fields = p.get("creative_fields", "")
            if tools:
                try:
                    tools = json.loads(tools) if isinstance(tools, str) else tools
                    print(f"  │  Инструменты: {', '.join(tools[:5])}")
                except:
                    print(f"  │  Инструменты: {tools}")
            if fields:
                try:
                    fields = json.loads(fields) if isinstance(fields, str) else fields
                    print(f"  │  Creative fields: {', '.join(fields[:5])}")
                except:
                    print(f"  │  Creative fields: {fields}")

            # Velocity
            trajectory = get_position_trajectory(p["id"])
            vel = analyze_engagement_velocity(trajectory)
            if vel:
                print(f"  │  Velocity: {vel['appr_per_day']} appr/day, {vel['views_per_day']} views/day, {vel['comments_per_day']} comments/day (за {vel['monitoring_days']}д)")
            
            # Position stability per query
            for q in ["инфографика", "дизайн карточек"]:
                stab = analyze_position_stability(trajectory, q)
                if stab.get("count"):
                    print(f"  │  [{q}] стабильность: avg={stab['avg']} std={stab['std']} range={stab['range']} (n={stab['count']})")

            print(f"  └─")

        # ─── TITLE PATTERNS ───
        print(f"\n  📝 Паттерн заголовков:")
        for p in search_projects[:5]:
            print(f"     - {p['title'][:80]}")

        all_author_data.append({
            "name": name,
            "username": auth["username"],
            "stats": stats,
            "search_projects": search_projects,
            "all_projects": all_projects,
        })

    # ─── COMPARATIVE SUMMARY ───
    print_divider("СРАВНИТЕЛЬНАЯ ТАБЛИЦА ДОЛГОЖИТЕЛЕЙ")
    
    print(f"\n{'Автор':<25} {'Followers':>10} {'Projects':>8} {'In Search':>10} {'Comments':>9} {'Avg Post Gap':>13} {'Pro':>4} {'Services':>9}")
    print("-" * 100)
    
    for ad in all_author_data:
        stats = ad["stats"]
        sp = ad["search_projects"]
        avg_gap, _, _ = calc_posting_frequency(ad["all_projects"])
        total_comments = sum(p.get("max_comments", 0) or 0 for p in sp)
        total_comments_page = sum(p.get("comments_count", 0) or 0 for p in sp)
        
        auth_row = conn.execute("SELECT has_pro, has_services FROM authors WHERE username=?", (ad["username"],)).fetchone()
        pro = "✅" if auth_row and auth_row["has_pro"] else "❌"
        svc = "✅" if auth_row and auth_row["has_services"] else "❌"
        
        freq_str = f"{avg_gap:.0f}d" if avg_gap else "?"
        print(f"{ad['name']:<25} {stats.get('followers', 0):>10,} {stats.get('project_count', 0):>8} {len(sp):>10} {total_comments:>9} {freq_str:>13} {pro:>4} {svc:>9}")

    # ─── COMMENT ANALYSIS ───
    print_divider("АНАЛИЗ КОММЕНТАРИЕВ — ГИПОТЕЗА 'Комменты = удержание'")
    
    for ad in all_author_data:
        name = ad["name"]
        for p in ad["search_projects"][:3]:
            trajectory = get_position_trajectory(p["id"])
            if not trajectory:
                continue
            
            comments_data = [(t["timestamp"][:10], t["comments"], t["position"], t["query"]) for t in trajectory if t["comments"] is not None]
            if not comments_data:
                continue
            
            first_c = comments_data[0][1] if comments_data else 0
            last_c = comments_data[-1][1] if comments_data else 0
            
            print(f"\n  {name} — «{p['title'][:50]}»")
            print(f"    Comments: {first_c} → {last_c} (+{last_c - first_c})")
            print(f"    Page comments_count: {p.get('comments_count', 0)}")
            
            # Check if comments correlate with position stability
            for q in ["инфографика", "дизайн карточек"]:
                q_data = [(t[1], t[2]) for t in comments_data if t[3] == q]
                if len(q_data) >= 5:
                    comments_vals = [d[0] for d in q_data]
                    position_vals = [d[1] for d in q_data]
                    c_growing = comments_vals[-1] > comments_vals[0]
                    pos_stable = max(position_vals) - min(position_vals) <= 5
                    print(f"    [{q}] comments growing: {c_growing}, position stable (range<=5): {pos_stable}")

    # ─── KEY PATTERNS SUMMARY ───
    print_divider("КЛЮЧЕВЫЕ ПАТТЕРНЫ ДОЛГОЖИТЕЛЕЙ")
    
    # Title keywords
    title_words = defaultdict(int)
    for ad in all_author_data:
        for p in ad["search_projects"]:
            words = (p.get("title") or "").lower().split()
            for w in words:
                if len(w) > 3:
                    title_words[w] += 1
    
    print("\n  Частые слова в заголовках:")
    for w, cnt in sorted(title_words.items(), key=lambda x: -x[1])[:15]:
        print(f"    {w}: {cnt}")

    # Content patterns
    print("\n  Средние метрики контента (проекты в поиске):")
    all_sp = []
    for ad in all_author_data:
        all_sp.extend(ad["search_projects"])
    
    if all_sp:
        avg_modules = sum(p.get("module_count", 0) or 0 for p in all_sp) / len(all_sp)
        avg_images = sum(p.get("image_count", 0) or 0 for p in all_sp) / len(all_sp)
        avg_videos = sum(p.get("video_count", 0) or 0 for p in all_sp) / len(all_sp)
        avg_desc = sum(p.get("description_length", 0) or 0 for p in all_sp) / len(all_sp)
        avg_comments = sum(p.get("max_comments", 0) or 0 for p in all_sp) / len(all_sp)
        avg_appr = sum(p.get("max_appr", 0) or 0 for p in all_sp) / len(all_sp)
        avg_views = sum(p.get("max_views", 0) or 0 for p in all_sp) / len(all_sp)
        
        print(f"    Модулей: {avg_modules:.1f}")
        print(f"    Картинок: {avg_images:.1f}")
        print(f"    Видео: {avg_videos:.1f}")
        print(f"    Описание: {avg_desc:.0f} символов")
        print(f"    Комментариев: {avg_comments:.1f}")
        print(f"    Appr: {avg_appr:.0f}")
        print(f"    Views: {avg_views:.0f}")

    conn.close()


if __name__ == "__main__":
    main()
