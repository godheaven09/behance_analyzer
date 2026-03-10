"""
ПОЛНЫЙ корреляционный анализ всех метрик vs позиция / стабильность / удержание.
Spearman rank correlation + group comparisons.
"""
import sqlite3
import os
import sys
import json
import re
from datetime import datetime
from collections import defaultdict
from math import isnan

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config

conn = sqlite3.connect(config.DB_PATH)
conn.row_factory = sqlite3.Row


def spearman(x, y):
    """Spearman rank correlation. Returns (rho, n)."""
    pairs = [(a, b) for a, b in zip(x, y) if a is not None and b is not None]
    if len(pairs) < 5:
        return None, len(pairs)
    n = len(pairs)
    
    def rank(vals):
        indexed = sorted(range(len(vals)), key=lambda i: vals[i])
        ranks = [0.0] * len(vals)
        i = 0
        while i < len(indexed):
            j = i
            while j < len(indexed) and vals[indexed[j]] == vals[indexed[i]]:
                j += 1
            avg_rank = (i + j - 1) / 2.0 + 1
            for k in range(i, j):
                ranks[indexed[k]] = avg_rank
            i = j
        return ranks
    
    xv = [p[0] for p in pairs]
    yv = [p[1] for p in pairs]
    rx = rank(xv)
    ry = rank(yv)
    d2 = sum((a - b) ** 2 for a, b in zip(rx, ry))
    rho = 1 - (6 * d2) / (n * (n * n - 1))
    return rho, n


def safe_json_list(val):
    if not val:
        return []
    try:
        parsed = json.loads(val) if isinstance(val, str) else val
        return parsed if isinstance(parsed, list) else []
    except:
        return []


def load_all_project_data():
    """Load enriched project data with all metrics + position stats."""
    rows = conn.execute("""
        SELECT p.*,
               a.username, a.display_name, a.location, a.member_since,
               a.bio_text, a.has_pro, a.has_services, a.has_banner,
               a.has_website_link, a.hire_status,
               COUNT(DISTINCT sr.snapshot_id) as appearance_count,
               ROUND(AVG(sr.position), 2) as avg_position,
               MIN(sr.position) as best_position,
               MAX(sr.position) as worst_position,
               MAX(sr.appreciations) as latest_appr,
               MAX(sr.views) as latest_views,
               MAX(sr.comments) as latest_comments,
               MIN(sr.appreciations) as first_appr,
               MIN(sr.views) as first_views,
               GROUP_CONCAT(DISTINCT s.query) as queries
        FROM projects p
        JOIN search_results sr ON sr.project_id = p.id
        JOIN snapshots s ON sr.snapshot_id = s.id
        LEFT JOIN authors a ON p.author_id = a.id
        GROUP BY p.id
    """).fetchall()
    
    projects = []
    for r in rows:
        d = dict(r)
        
        # Author stats
        if d.get("author_id"):
            astats = conn.execute("""
                SELECT total_views, total_appreciations, followers, following, project_count
                FROM author_snapshots WHERE author_id=? ORDER BY snapshot_id DESC LIMIT 1
            """, (d["author_id"],)).fetchone()
            if astats:
                d["author_total_views"] = astats["total_views"] or 0
                d["author_total_appr"] = astats["total_appreciations"] or 0
                d["author_followers"] = astats["followers"] or 0
                d["author_following"] = astats["following"] or 0
                d["author_project_count"] = astats["project_count"] or 0

        # Tag count
        tag_count = conn.execute("SELECT COUNT(*) FROM project_tags WHERE project_id=?", (d["id"],)).fetchone()[0]
        d["tag_count"] = tag_count
        
        # Derived metrics
        d["age_days"] = None
        if d.get("published_date"):
            try:
                pub = datetime.strptime(d["published_date"], "%Y-%m-%d")
                d["age_days"] = (datetime.utcnow() - pub).days
            except:
                pass

        d["title_length"] = len(d.get("title") or "")
        d["has_video"] = 1 if (d.get("video_count") or 0) > 0 else 0
        d["has_embeds"] = 1 if (d.get("embed_count") or 0) > 0 else 0
        d["has_text_blocks"] = 1 if (d.get("text_count") or 0) > 0 else 0
        d["has_description"] = 1 if (d.get("description_length") or 0) > 0 else 0
        d["has_tags"] = 1 if tag_count > 0 else 0
        d["has_bio"] = 1 if d.get("bio_text") and len(d["bio_text"]) > 5 else 0
        d["has_location"] = 1 if d.get("location") else 0
        d["has_hire"] = 1 if d.get("hire_status") else 0
        
        d["position_range"] = (d["worst_position"] or 0) - (d["best_position"] or 0)
        
        # Engagement velocity (growth during monitoring)
        appr_growth = (d["latest_appr"] or 0) - (d["first_appr"] or 0)
        views_growth = (d["latest_views"] or 0) - (d["first_views"] or 0)
        d["appr_growth"] = appr_growth
        d["views_growth"] = views_growth
        
        # Comments / appr ratio
        comments_page = d.get("comments_count") or 0
        d["comments_page"] = comments_page
        d["comments_per_appr"] = comments_page / max(d["latest_appr"] or 1, 1)
        d["comments_per_view"] = comments_page / max(d["latest_views"] or 1, 1)
        
        # Follower ratios
        followers = d.get("author_followers", 0) or 0
        following = d.get("author_following", 0) or 0
        d["follower_ratio"] = followers / max(following, 1)
        
        # Creative fields count
        fields = safe_json_list(d.get("creative_fields"))
        d["creative_fields_count"] = len(fields)
        d["has_infographics_field"] = 1 if any("нфографик" in f for f in fields) else 0
        d["has_graphic_design_field"] = 1 if any("рафическ" in f for f in fields) else 0
        
        # Tools count
        tools = safe_json_list(d.get("tools_used"))
        d["tools_count"] = len(tools)
        d["uses_photoshop"] = 1 if any("photoshop" in t.lower() for t in tools) else 0
        d["uses_figma"] = 1 if any("figma" in t.lower() for t in tools) else 0
        
        # Content density
        total_content = (d.get("image_count") or 0) + (d.get("video_count") or 0) + (d.get("embed_count") or 0)
        d["total_visual_content"] = total_content
        d["images_per_module"] = (d.get("image_count") or 0) / max(d.get("module_count") or 1, 1)
        
        # In both queries?
        queries = (d.get("queries") or "").split(",")
        d["in_both_queries"] = 1 if len(set(queries)) >= 2 else 0
        
        # Position stability score (lower = more stable)
        d["position_stability"] = d["position_range"]

        projects.append(d)
    
    return projects


def analyze_correlations(projects, target_metric, target_label, filter_fn=None):
    """Run Spearman correlation of ALL metrics against a target metric."""
    if filter_fn:
        filtered = [p for p in projects if filter_fn(p)]
    else:
        filtered = projects
    
    if len(filtered) < 10:
        return []
    
    metrics = [
        ("latest_appr", "Appreciations"),
        ("latest_views", "Views"),
        ("comments_page", "Comments (page)"),
        ("latest_comments", "Comments (search card)"),
        ("comments_per_appr", "Comments / Appr ratio"),
        ("comments_per_view", "Comments / View ratio"),
        ("age_days", "Age (days)"),
        ("module_count", "Modules"),
        ("image_count", "Images"),
        ("video_count", "Videos"),
        ("has_video", "Has video (0/1)"),
        ("embed_count", "Embeds"),
        ("has_embeds", "Has embeds (0/1)"),
        ("text_count", "Text blocks"),
        ("total_visual_content", "Total visual content"),
        ("images_per_module", "Images per module"),
        ("description_length", "Description length"),
        ("has_description", "Has description (0/1)"),
        ("title_length", "Title length"),
        ("title_keyword_match", "Title keyword match"),
        ("tag_count", "Tag count"),
        ("has_tags", "Has tags (0/1)"),
        ("tools_count", "Tools count"),
        ("uses_photoshop", "Uses Photoshop (0/1)"),
        ("uses_figma", "Uses Figma (0/1)"),
        ("creative_fields_count", "Creative fields count"),
        ("has_infographics_field", "Has 'Инфографика' field (0/1)"),
        ("has_graphic_design_field", "Has 'Граф. дизайн' field (0/1)"),
        ("co_owners_count", "Co-owners"),
        ("external_link_count", "External links"),
        ("author_followers", "Author followers"),
        ("author_following", "Author following"),
        ("follower_ratio", "Follower/following ratio"),
        ("author_total_views", "Author total views"),
        ("author_total_appr", "Author total appr"),
        ("author_project_count", "Author project count"),
        ("has_banner", "Has banner (0/1)"),
        ("has_hire", "Has hire status (0/1)"),
        ("has_bio", "Has bio (0/1)"),
        ("has_location", "Has location (0/1)"),
        ("publish_day_of_week", "Publish day of week"),
        ("publish_hour", "Publish hour (UTC)"),
        ("appr_growth", "Appr growth (monitoring)"),
        ("views_growth", "Views growth (monitoring)"),
        ("in_both_queries", "In both queries (0/1)"),
        ("appearance_count", "Appearance count (snapshots)"),
        ("position_range", "Position range (instability)"),
        ("cover_image_width", "Cover image width"),
        ("cover_image_height", "Cover image height"),
    ]
    
    target_values = [p.get(target_metric) for p in filtered]
    
    results = []
    for key, label in metrics:
        if key == target_metric:
            continue
        values = [p.get(key) for p in filtered]
        rho, n = spearman(values, target_values)
        if rho is not None and not isnan(rho):
            results.append((label, rho, n))
    
    results.sort(key=lambda x: abs(x[1]), reverse=True)
    return results


def group_comparison(projects, metric_key, metric_label, groups):
    """Compare metric averages across position groups."""
    print(f"\n  {'Группа':<25}", end="")
    print(f" {'N':>5} {'Avg':>10} {'Median':>10} {'Min':>10} {'Max':>10}")
    print(f"  {'-'*70}")
    
    for group_label, filter_fn in groups:
        vals = [p.get(metric_key) for p in projects if filter_fn(p) and p.get(metric_key) is not None]
        if not vals:
            continue
        vals_sorted = sorted(vals)
        n = len(vals_sorted)
        avg = sum(vals_sorted) / n
        median = vals_sorted[n // 2]
        print(f"  {group_label:<25} {n:>5} {avg:>10.1f} {median:>10.1f} {min(vals_sorted):>10.1f} {max(vals_sorted):>10.1f}")


def binary_feature_analysis(projects, feature_key, feature_label):
    """Compare avg position for feature=1 vs feature=0."""
    group_1 = [p for p in projects if p.get(feature_key) == 1 and p.get("avg_position") is not None]
    group_0 = [p for p in projects if p.get(feature_key) == 0 and p.get("avg_position") is not None]
    
    if len(group_1) < 3 or len(group_0) < 3:
        return None
    
    avg_1 = sum(p["avg_position"] for p in group_1) / len(group_1)
    avg_0 = sum(p["avg_position"] for p in group_0) / len(group_0)
    
    ret_1 = sum(p["appearance_count"] for p in group_1) / len(group_1)
    ret_0 = sum(p["appearance_count"] for p in group_0) / len(group_0)
    
    return {
        "label": feature_label,
        "n_yes": len(group_1),
        "n_no": len(group_0),
        "avg_pos_yes": avg_1,
        "avg_pos_no": avg_0,
        "delta_pos": avg_0 - avg_1,
        "avg_retention_yes": ret_1,
        "avg_retention_no": ret_0,
    }


def print_section(title):
    print(f"\n{'='*90}")
    print(f"  {title}")
    print(f"{'='*90}")


def print_correlations(results, limit=30):
    print(f"\n  {'Метрика':<40} {'Spearman r':>12} {'N':>6} {'Сила':>10}")
    print(f"  {'-'*70}")
    for label, rho, n in results[:limit]:
        strength = ""
        ar = abs(rho)
        if ar >= 0.7:
            strength = "СИЛЬНАЯ"
        elif ar >= 0.4:
            strength = "средняя"
        elif ar >= 0.2:
            strength = "слабая"
        else:
            strength = "нет"
        
        sign = "+" if rho > 0 else "-"
        print(f"  {label:<40} {sign}{ar:>10.3f}  {n:>5}  {strength:>10}")


def main():
    projects = load_all_project_data()
    print(f"Загружено {len(projects)} проектов из поисковой выдачи\n")

    # ────────────────────────────────────────────────────────────
    # 1. КОРРЕЛЯЦИИ С СРЕДНЕЙ ПОЗИЦИЕЙ (все проекты)
    # ────────────────────────────────────────────────────────────
    print_section("1. КОРРЕЛЯЦИИ С СРЕДНЕЙ ПОЗИЦИЕЙ (ниже = лучше, отриц. корр. = хорошо)")
    results = analyze_correlations(projects, "avg_position", "Average Position")
    print_correlations(results)

    # ────────────────────────────────────────────────────────────
    # 2. КОРРЕЛЯЦИИ С УДЕРЖАНИЕМ (кол-во появлений в снимках)
    # ────────────────────────────────────────────────────────────
    print_section("2. КОРРЕЛЯЦИИ С УДЕРЖАНИЕМ (больше появлений = лучше)")
    results = analyze_correlations(projects, "appearance_count", "Appearance Count")
    print_correlations(results)

    # ────────────────────────────────────────────────────────────
    # 3. КОРРЕЛЯЦИИ С СТАБИЛЬНОСТЬЮ ПОЗИЦИИ (меньше range = стабильнее)
    # ────────────────────────────────────────────────────────────
    print_section("3. КОРРЕЛЯЦИИ С СТАБИЛЬНОСТЬЮ (position_range, меньше = стабильнее)")
    stable_projects = [p for p in projects if p["appearance_count"] >= 5]
    results = analyze_correlations(stable_projects, "position_range", "Position Range")
    print_correlations(results)

    # ────────────────────────────────────────────────────────────
    # 4. ТОЛЬКО ТОП-20: корреляции с позицией
    # ────────────────────────────────────────────────────────────
    print_section("4. ТОЛЬКО ТОП-20: корреляции с позицией")
    top20 = [p for p in projects if (p.get("avg_position") or 999) <= 20]
    results = analyze_correlations(top20, "avg_position", "Average Position (top-20)")
    print_correlations(results)

    # ────────────────────────────────────────────────────────────
    # 5. БИНАРНЫЕ ПРИЗНАКИ: сравнение групп
    # ────────────────────────────────────────────────────────────
    print_section("5. БИНАРНЫЕ ПРИЗНАКИ: avg позиция и удержание")
    
    binary_features = [
        ("has_video", "Есть видео"),
        ("has_embeds", "Есть эмбеды"),
        ("has_text_blocks", "Есть текстовые блоки"),
        ("has_description", "Есть описание"),
        ("has_tags", "Есть теги"),
        ("has_infographics_field", "Creative field 'Инфографика'"),
        ("has_graphic_design_field", "Creative field 'Граф. дизайн'"),
        ("uses_photoshop", "Использует Photoshop"),
        ("uses_figma", "Использует Figma"),
        ("has_banner", "Есть баннер профиля"),
        ("has_hire", "Статус 'Hire'"),
        ("has_bio", "Есть био"),
        ("has_location", "Указана локация"),
        ("in_both_queries", "В обоих запросах"),
    ]
    
    print(f"\n  {'Признак':<35} {'Да(N)':>6} {'Нет(N)':>6} {'AvgPos Да':>10} {'AvgPos Нет':>11} {'Δ Pos':>7} {'Ret Да':>8} {'Ret Нет':>8}")
    print(f"  {'-'*95}")
    
    for key, label in binary_features:
        result = binary_feature_analysis(projects, key, label)
        if result:
            delta_sign = "+" if result["delta_pos"] > 0 else ""
            print(f"  {result['label']:<35} {result['n_yes']:>6} {result['n_no']:>6} {result['avg_pos_yes']:>10.1f} {result['avg_pos_no']:>11.1f} {delta_sign}{result['delta_pos']:>6.1f} {result['avg_retention_yes']:>8.1f} {result['avg_retention_no']:>8.1f}")

    # ────────────────────────────────────────────────────────────
    # 6. ГРУППОВОЕ СРАВНЕНИЕ ПО ПОЗИЦИОННЫМ ГРУППАМ
    # ────────────────────────────────────────────────────────────
    print_section("6. СРАВНЕНИЕ ПО ПОЗИЦИОННЫМ ГРУППАМ")
    
    pos_groups = [
        ("TOP 1-5", lambda p: (p.get("avg_position") or 999) <= 5),
        ("TOP 6-10", lambda p: 5 < (p.get("avg_position") or 999) <= 10),
        ("TOP 11-20", lambda p: 10 < (p.get("avg_position") or 999) <= 20),
        ("TOP 21-50", lambda p: 20 < (p.get("avg_position") or 999) <= 50),
        ("50+", lambda p: (p.get("avg_position") or 999) > 50),
    ]
    
    compare_metrics = [
        ("comments_page", "Comments (page)"),
        ("latest_appr", "Appreciations"),
        ("latest_views", "Views"),
        ("age_days", "Age (days)"),
        ("module_count", "Modules"),
        ("image_count", "Images"),
        ("video_count", "Videos"),
        ("tag_count", "Tags"),
        ("description_length", "Description length"),
        ("author_followers", "Author followers"),
        ("author_project_count", "Author project count"),
        ("creative_fields_count", "Creative fields count"),
        ("tools_count", "Tools count"),
        ("title_length", "Title length"),
        ("comments_per_appr", "Comments/Appr ratio"),
        ("appearance_count", "Retention (snapshots)"),
    ]
    
    for metric_key, metric_label in compare_metrics:
        print(f"\n  >>> {metric_label}")
        group_comparison(projects, metric_key, metric_label, pos_groups)

    # ────────────────────────────────────────────────────────────
    # 7. АНАЛИЗ КОММЕНТАРИЕВ ДЕТАЛЬНО
    # ────────────────────────────────────────────────────────────
    print_section("7. ДЕТАЛЬНЫЙ АНАЛИЗ КОММЕНТАРИЕВ")
    
    # Comments distribution by position tier
    for tier_label, tier_fn in pos_groups:
        tier_projects = [p for p in projects if tier_fn(p)]
        if not tier_projects:
            continue
        comments = [p["comments_page"] for p in tier_projects]
        has_comments_pct = sum(1 for c in comments if c > 0) / len(comments) * 100
        avg_c = sum(comments) / len(comments)
        print(f"  {tier_label}: n={len(tier_projects)}, avg_comments={avg_c:.1f}, с_комментами={has_comments_pct:.0f}%")

    # Top-20 projects sorted by comments
    print(f"\n  ТОП-15 проектов по комментариям (в топ-20 позиции):")
    top20_by_comments = sorted(top20, key=lambda p: p["comments_page"], reverse=True)[:15]
    for p in top20_by_comments:
        name = p.get("display_name") or p.get("username") or "?"
        print(f"    #{p['avg_position']:>5.1f}  comments={p['comments_page']:>4}  appr={p['latest_appr']:>5}  «{(p.get('title') or '')[:50]}»  by {name}")

    # ────────────────────────────────────────────────────────────
    # 8. АНАЛИЗ ВОЗРАСТА И СВЕЖЕСТИ
    # ────────────────────────────────────────────────────────────
    print_section("8. ВОЗРАСТ ПРОЕКТА И ПОЗИЦИЯ")
    
    age_groups = [
        ("0-14 дней", lambda p: (p.get("age_days") or 999) <= 14),
        ("15-30 дней", lambda p: 14 < (p.get("age_days") or 999) <= 30),
        ("31-60 дней", lambda p: 30 < (p.get("age_days") or 999) <= 60),
        ("61-120 дней", lambda p: 60 < (p.get("age_days") or 999) <= 120),
        ("120+ дней", lambda p: (p.get("age_days") or 999) > 120),
    ]
    
    print(f"\n  {'Возраст':<20} {'N':>5} {'Avg Pos':>10} {'Avg Appr':>10} {'Avg Views':>10} {'Avg Comments':>12} {'Avg Retention':>14}")
    print(f"  {'-'*85}")
    for label, fn in age_groups:
        grp = [p for p in projects if fn(p)]
        if not grp:
            continue
        avg_pos = sum(p.get("avg_position") or 0 for p in grp) / len(grp)
        avg_appr = sum(p.get("latest_appr") or 0 for p in grp) / len(grp)
        avg_views = sum(p.get("latest_views") or 0 for p in grp) / len(grp)
        avg_comments = sum(p.get("comments_page") or 0 for p in grp) / len(grp)
        avg_ret = sum(p.get("appearance_count") or 0 for p in grp) / len(grp)
        print(f"  {label:<20} {len(grp):>5} {avg_pos:>10.1f} {avg_appr:>10.1f} {avg_views:>10.1f} {avg_comments:>12.1f} {avg_ret:>14.1f}")

    # ────────────────────────────────────────────────────────────
    # 9. АНАЛИЗ ДНЯ НЕДЕЛИ И ЧАСА ПУБЛИКАЦИИ
    # ────────────────────────────────────────────────────────────
    print_section("9. ДЕНЬ НЕДЕЛИ И ЧАС ПУБЛИКАЦИИ")
    
    days = {1: "Пн", 2: "Вт", 3: "Ср", 4: "Чт", 5: "Пт", 6: "Сб", 7: "Вс"}
    print(f"\n  {'День':<5} {'N':>5} {'Avg Pos':>10} {'Avg Retention':>14}")
    print(f"  {'-'*35}")
    for day_num in range(1, 8):
        grp = [p for p in projects if p.get("publish_day_of_week") == day_num]
        if not grp:
            continue
        avg_pos = sum(p.get("avg_position") or 0 for p in grp) / len(grp)
        avg_ret = sum(p.get("appearance_count") or 0 for p in grp) / len(grp)
        print(f"  {days.get(day_num, '?'):<5} {len(grp):>5} {avg_pos:>10.1f} {avg_ret:>14.1f}")

    # Hour analysis
    print(f"\n  {'Час UTC':<8} {'N':>5} {'Avg Pos':>10}")
    print(f"  {'-'*25}")
    hour_data = defaultdict(list)
    for p in projects:
        h = p.get("publish_hour")
        if h is not None:
            hour_data[h].append(p.get("avg_position") or 0)
    for h in sorted(hour_data.keys()):
        if len(hour_data[h]) >= 3:
            avg = sum(hour_data[h]) / len(hour_data[h])
            print(f"  {h:>3}:00   {len(hour_data[h]):>5} {avg:>10.1f}")

    # ────────────────────────────────────────────────────────────
    # 10. CREATIVE FIELDS BREAKDOWN
    # ────────────────────────────────────────────────────────────
    print_section("10. CREATIVE FIELDS: позиция по полям")
    
    field_stats = defaultdict(lambda: {"positions": [], "retentions": []})
    for p in projects:
        fields = safe_json_list(p.get("creative_fields"))
        for f in fields:
            field_stats[f]["positions"].append(p.get("avg_position") or 0)
            field_stats[f]["retentions"].append(p.get("appearance_count") or 0)
    
    print(f"\n  {'Creative Field':<40} {'N':>5} {'Avg Pos':>10} {'Avg Retention':>14}")
    print(f"  {'-'*70}")
    for field, data in sorted(field_stats.items(), key=lambda x: -len(x[1]["positions"])):
        if len(data["positions"]) >= 3:
            avg_pos = sum(data["positions"]) / len(data["positions"])
            avg_ret = sum(data["retentions"]) / len(data["retentions"])
            print(f"  {field:<40} {len(data['positions']):>5} {avg_pos:>10.1f} {avg_ret:>14.1f}")

    # ────────────────────────────────────────────────────────────
    # 11. TOOLS BREAKDOWN
    # ────────────────────────────────────────────────────────────
    print_section("11. TOOLS: позиция по инструментам")
    
    tool_stats = defaultdict(lambda: {"positions": [], "retentions": []})
    for p in projects:
        tools = safe_json_list(p.get("tools_used"))
        for t in tools:
            tool_stats[t]["positions"].append(p.get("avg_position") or 0)
            tool_stats[t]["retentions"].append(p.get("appearance_count") or 0)
    
    print(f"\n  {'Tool':<40} {'N':>5} {'Avg Pos':>10} {'Avg Retention':>14}")
    print(f"  {'-'*70}")
    for tool, data in sorted(tool_stats.items(), key=lambda x: -len(x[1]["positions"])):
        if len(data["positions"]) >= 3:
            avg_pos = sum(data["positions"]) / len(data["positions"])
            avg_ret = sum(data["retentions"]) / len(data["retentions"])
            print(f"  {tool:<40} {len(data['positions']):>5} {avg_pos:>10.1f} {avg_ret:>14.1f}")

    # ────────────────────────────────────────────────────────────
    # 12. TITLE KEYWORD ANALYSIS
    # ────────────────────────────────────────────────────────────
    print_section("12. КЛЮЧЕВЫЕ СЛОВА В ЗАГОЛОВКЕ")
    
    keywords = ["инфографика", "дизайн", "карточек", "карточки", "маркетплейс", 
                 "wildberries", "ozon", "wb", "товар", "упаковка"]
    
    print(f"\n  {'Слово':<20} {'С ним (N)':>10} {'Без (N)':>10} {'Avg Pos С':>10} {'Avg Pos Без':>12} {'Δ':>6}")
    print(f"  {'-'*70}")
    for kw in keywords:
        with_kw = [p for p in projects if kw in (p.get("title") or "").lower()]
        without_kw = [p for p in projects if kw not in (p.get("title") or "").lower()]
        if len(with_kw) >= 3 and len(without_kw) >= 3:
            avg_with = sum(p["avg_position"] for p in with_kw) / len(with_kw)
            avg_without = sum(p["avg_position"] for p in without_kw) / len(without_kw)
            delta = avg_without - avg_with
            print(f"  {kw:<20} {len(with_kw):>10} {len(without_kw):>10} {avg_with:>10.1f} {avg_without:>12.1f} {delta:>+6.1f}")

    # ────────────────────────────────────────────────────────────
    # 13. AUTHOR FREQUENCY vs POSITION
    # ────────────────────────────────────────────────────────────
    print_section("13. КОЛИЧЕСТВО ПРОЕКТОВ АВТОРА В ВЫДАЧЕ vs ПОЗИЦИЯ")
    
    author_project_counts = defaultdict(list)
    for p in projects:
        username = p.get("username") or "unknown"
        author_project_counts[username].append(p)
    
    freq_groups = [
        ("1 проект", lambda cnt: cnt == 1),
        ("2-3 проекта", lambda cnt: 2 <= cnt <= 3),
        ("4-10 проектов", lambda cnt: 4 <= cnt <= 10),
        ("10+ проектов", lambda cnt: cnt > 10),
    ]
    
    print(f"\n  {'Группа':<20} {'Авторов':>8} {'Проектов':>10} {'Avg Pos':>10} {'Avg Best':>10}")
    print(f"  {'-'*60}")
    for label, fn in freq_groups:
        authors_in = {u: ps for u, ps in author_project_counts.items() if fn(len(ps))}
        if not authors_in:
            continue
        all_ps = [p for ps in authors_in.values() for p in ps]
        avg_pos = sum(p["avg_position"] for p in all_ps) / len(all_ps)
        avg_best = sum(p["best_position"] for p in all_ps) / len(all_ps)
        print(f"  {label:<20} {len(authors_in):>8} {len(all_ps):>10} {avg_pos:>10.1f} {avg_best:>10.1f}")

    # ────────────────────────────────────────────────────────────
    # 14. PER-QUERY ANALYSIS
    # ────────────────────────────────────────────────────────────
    for query in ["инфографика", "дизайн карточек"]:
        print_section(f"14. КОРРЕЛЯЦИИ ТОЛЬКО ДЛЯ '{query}'")
        q_projects = [p for p in projects if query in (p.get("queries") or "")]
        results = analyze_correlations(q_projects, "avg_position", f"Avg Position ({query})")
        print_correlations(results, limit=20)

    conn.close()
    print(f"\n{'='*90}")
    print(f"  АНАЛИЗ ЗАВЕРШЕН")
    print(f"{'='*90}")


if __name__ == "__main__":
    main()
