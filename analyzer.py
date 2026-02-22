"""
Behance Analyzer — Statistical analysis, correlations, gap analysis.
"""
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from scipy import stats as scipy_stats

import db
import config

pd.set_option("display.max_columns", 30)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 40)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_latest_snapshot(query: str) -> pd.DataFrame | None:
    snapshots = db.get_all_snapshots_for_query(query)
    if not snapshots:
        return None

    latest = snapshots[0]
    results = db.get_search_results_for_snapshot(latest["id"])
    if not results:
        return None

    df = pd.DataFrame(results)

    conn = db.get_connection()

    # Enrich with author stats
    author_stats = []
    for _, row in df.iterrows():
        if row.get("author_id"):
            astats = db.get_author_stats_latest(row["author_id"])
            author_stats.append(astats or {})
        else:
            author_stats.append({})

    ast_df = pd.DataFrame(author_stats)
    if not ast_df.empty:
        for col in ["total_views", "total_appreciations", "followers", "following", "project_count"]:
            if col in ast_df.columns:
                df[f"author_{col}"] = ast_df[col].values

    # Enrich with tags
    tag_counts = []
    for _, row in df.iterrows():
        tags = conn.execute(
            "SELECT tag_name FROM project_tags WHERE project_id = ?",
            (row["project_id"],),
        ).fetchall()
        tag_counts.append(len(tags))
    df["tag_count"] = tag_counts

    conn.close()

    # Computed metrics
    today = datetime.utcnow()
    if "published_date" in df.columns:
        df["days_since_publish"] = df["published_date"].apply(
            lambda x: (today - datetime.strptime(x, "%Y-%m-%d")).days
            if x and isinstance(x, str) else None
        )
        df["appreciations_per_day"] = df.apply(
            lambda r: r["appreciations"] / max(r["days_since_publish"], 1)
            if r["days_since_publish"] and r["days_since_publish"] > 0 else 0,
            axis=1,
        )
        df["views_per_day"] = df.apply(
            lambda r: r["views"] / max(r["days_since_publish"], 1)
            if r["days_since_publish"] and r["days_since_publish"] > 0 else 0,
            axis=1,
        )

    df["engagement_rate"] = df.apply(
        lambda r: r["appreciations"] / max(r["views"], 1) if r["views"] > 0 else 0,
        axis=1,
    )

    return df


def load_my_projects_df() -> pd.DataFrame:
    projects = db.get_my_projects()
    if not projects:
        return pd.DataFrame()
    df = pd.DataFrame(projects)

    today = datetime.utcnow()
    if "published_date" in df.columns:
        df["days_since_publish"] = df["published_date"].apply(
            lambda x: (today - datetime.strptime(x, "%Y-%m-%d")).days
            if x and isinstance(x, str) else None
        )

    return df


# ---------------------------------------------------------------------------
# Descriptive statistics
# ---------------------------------------------------------------------------

def descriptive_stats(df: pd.DataFrame, query: str) -> str:
    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"  DESCRIPTIVE STATS: '{query}'")
    lines.append(f"  Snapshot: {len(df)} projects")
    lines.append(f"{'='*80}\n")

    # Basic stats
    numeric_cols = [
        "appreciations", "views", "engagement_rate",
        "module_count", "image_count", "video_count",
        "description_length", "tag_count", "co_owners_count",
        "external_link_count", "comments",
    ]
    available = [c for c in numeric_cols if c in df.columns and df[c].notna().any()]

    if available:
        lines.append("--- Basic Statistics ---")
        stats_df = df[available].describe().round(2)
        lines.append(stats_df.to_string())
        lines.append("")

    # Velocity stats
    velocity_cols = ["appreciations_per_day", "views_per_day", "days_since_publish"]
    available_v = [c for c in velocity_cols if c in df.columns and df[c].notna().any()]
    if available_v:
        lines.append("--- Velocity Stats ---")
        lines.append(df[available_v].describe().round(2).to_string())
        lines.append("")

    # Top tags
    conn = db.get_connection()
    all_tags = []
    for _, row in df.iterrows():
        tags = conn.execute(
            "SELECT tag_name FROM project_tags WHERE project_id = ?",
            (row["project_id"],),
        ).fetchall()
        all_tags.extend([t["tag_name"] for t in tags])
    conn.close()

    if all_tags:
        tag_series = pd.Series(all_tags)
        lines.append("--- Top 20 Tags ---")
        lines.append(tag_series.value_counts().head(20).to_string())
        lines.append("")

    # Author frequency
    if "username" in df.columns:
        lines.append("--- Authors appearing multiple times ---")
        author_counts = df["username"].value_counts()
        multi = author_counts[author_counts > 1]
        if not multi.empty:
            lines.append(multi.to_string())
        else:
            lines.append("No author appears more than once")
        lines.append("")

    # Featured ratio
    if "is_featured" in df.columns:
        featured_pct = df["is_featured"].mean() * 100
        lines.append(f"--- Featured: {featured_pct:.1f}% of projects in results ---")
        lines.append("")

    # Promoted ratio
    if "is_promoted" in df.columns:
        promoted_pct = df["is_promoted"].mean() * 100
        lines.append(f"--- Promoted: {promoted_pct:.1f}% of projects in results ---")
        lines.append("")

    # Tools distribution
    if "tools_used" in df.columns:
        tools_all = []
        for t in df["tools_used"].dropna():
            try:
                tools_all.extend(json.loads(t))
            except (json.JSONDecodeError, TypeError):
                pass
        if tools_all:
            lines.append("--- Tools Distribution ---")
            lines.append(pd.Series(tools_all).value_counts().head(10).to_string())
            lines.append("")

    # Pro accounts
    if "has_pro" in df.columns:
        pro_pct = df["has_pro"].mean() * 100
        lines.append(f"--- Pro Accounts: {pro_pct:.1f}% ---")
        lines.append("")

    # Age distribution
    if "days_since_publish" in df.columns:
        lines.append("--- Project Age Distribution ---")
        age = df["days_since_publish"].dropna()
        if not age.empty:
            lines.append(f"  Min: {age.min():.0f} days")
            lines.append(f"  Max: {age.max():.0f} days")
            lines.append(f"  Median: {age.median():.0f} days")
            lines.append(f"  Mean: {age.mean():.0f} days")
            lines.append(f"  < 7 days: {(age < 7).sum()} projects")
            lines.append(f"  7-30 days: {((age >= 7) & (age < 30)).sum()} projects")
            lines.append(f"  30-90 days: {((age >= 30) & (age < 90)).sum()} projects")
            lines.append(f"  90+ days: {(age >= 90).sum()} projects")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Correlation analysis
# ---------------------------------------------------------------------------

def correlation_analysis(df: pd.DataFrame, query: str) -> str:
    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"  CORRELATION ANALYSIS: '{query}'")
    lines.append(f"{'='*80}\n")

    target = "position"
    if target not in df.columns:
        lines.append("ERROR: 'position' column not found")
        return "\n".join(lines)

    factors = [
        ("appreciations", "Appreciations"),
        ("views", "Views"),
        ("engagement_rate", "Engagement Rate (appr/views)"),
        ("appreciations_per_day", "Appreciations per Day (velocity)"),
        ("views_per_day", "Views per Day"),
        ("days_since_publish", "Days Since Publish"),
        ("module_count", "Module Count"),
        ("image_count", "Image Count"),
        ("video_count", "Video Count"),
        ("description_length", "Description Length"),
        ("tag_count", "Tag Count"),
        ("title_keyword_match", "Title Keyword Match"),
        ("co_owners_count", "Co-owners Count"),
        ("external_link_count", "External Links"),
        ("comments", "Comments"),
        ("is_featured", "Is Featured"),
        ("is_promoted", "Is Promoted"),
        ("has_pro", "Has Pro"),
        ("has_services", "Has Services"),
        ("author_total_views", "Author Total Views"),
        ("author_total_appreciations", "Author Total Appreciations"),
        ("author_followers", "Author Followers"),
        ("author_following", "Author Following"),
        ("author_project_count", "Author Project Count"),
    ]

    results = []
    for col, label in factors:
        if col not in df.columns:
            continue
        valid = df[[target, col]].dropna()
        if len(valid) < 5:
            continue
        try:
            corr, pval = scipy_stats.spearmanr(valid[target], valid[col])
            if np.isnan(corr):
                continue
            results.append((label, col, corr, pval))
        except Exception:
            pass

    results.sort(key=lambda x: abs(x[2]), reverse=True)

    lines.append(f"{'Factor':<40} {'Spearman r':>10} {'p-value':>10} {'Strength':>10}")
    lines.append("-" * 72)

    for label, col, corr, pval in results:
        strength = ""
        abs_corr = abs(corr)
        if abs_corr >= 0.7:
            strength = "STRONG"
        elif abs_corr >= 0.4:
            strength = "MEDIUM"
        elif abs_corr >= 0.2:
            strength = "WEAK"
        else:
            strength = "NONE"

        bar = "#" * int(abs_corr * 20)
        sig = "*" if pval < 0.05 else ""
        direction = "(-)" if corr < 0 else "(+)"

        lines.append(
            f"  {label:<38} {corr:>+8.3f}{sig:1} {pval:>10.4f} {strength:>8} {direction} {bar}"
        )

    lines.append("")
    lines.append("Note: Negative correlation with position = GOOD (lower position number = higher rank)")
    lines.append("      * = statistically significant (p < 0.05)")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Gap analysis: You vs Top
# ---------------------------------------------------------------------------

def gap_analysis(df: pd.DataFrame, my_df: pd.DataFrame, query: str) -> str:
    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"  GAP ANALYSIS: You vs Top — '{query}'")
    lines.append(f"{'='*80}\n")

    if my_df.empty:
        lines.append("No projects found for your profile.")
        return "\n".join(lines)

    # Split into tiers
    top10 = df[df["position"] <= 10]
    top20 = df[df["position"] <= 20]
    top50 = df[df["position"] <= 50]

    metrics = [
        ("appreciations", "Appreciations"),
        ("views", "Views"),
        ("engagement_rate", "Engagement Rate"),
        ("appreciations_per_day", "Appr/Day (velocity)"),
        ("views_per_day", "Views/Day"),
        ("module_count", "Modules (content blocks)"),
        ("image_count", "Images"),
        ("video_count", "Videos"),
        ("description_length", "Description Length"),
        ("tag_count", "Tags"),
        ("co_owners_count", "Co-owners"),
        ("external_link_count", "External Links"),
        ("title_keyword_match", "Title Keyword Match"),
    ]

    lines.append(f"{'Metric':<25} {'Top-10':>10} {'Top-20':>10} {'Top-50':>10} {'You (avg)':>10} {'Status':>8}")
    lines.append("-" * 78)

    for col, label in metrics:
        if col not in df.columns:
            continue

        t10_avg = top10[col].mean() if col in top10.columns and not top10[col].isna().all() else 0
        t20_avg = top20[col].mean() if col in top20.columns and not top20[col].isna().all() else 0
        t50_avg = top50[col].mean() if col in top50.columns and not top50[col].isna().all() else 0
        my_avg = my_df[col].mean() if col in my_df.columns and not my_df[col].isna().all() else 0

        if t10_avg > 0:
            ratio = my_avg / t10_avg
        else:
            ratio = 1.0

        if ratio >= 0.8:
            status = "OK"
        elif ratio >= 0.5:
            status = "WARN"
        else:
            status = "CRIT"

        lines.append(
            f"  {label:<23} {t10_avg:>10.1f} {t20_avg:>10.1f} {t50_avg:>10.1f} {my_avg:>10.1f} {status:>8}"
        )

    lines.append("")

    # My projects presence in search
    lines.append("--- Your Projects in Search Results ---")
    my_behance_ids = set(my_df["behance_id"].tolist()) if "behance_id" in my_df.columns else set()

    if my_behance_ids and "behance_id" in df.columns:
        found = df[df["behance_id"].isin(my_behance_ids)]
        if not found.empty:
            for _, row in found.iterrows():
                lines.append(f"  FOUND at #{row['position']}: {row.get('title', 'N/A')}")
        else:
            lines.append("  NONE of your projects found in search results")
    else:
        lines.append("  No data to compare")

    lines.append("")

    # Author stats comparison
    lines.append("--- Author Stats: You vs Top-10 Authors ---")
    author_cols = [
        ("author_total_views", "Total Views"),
        ("author_total_appreciations", "Total Appreciations"),
        ("author_followers", "Followers"),
        ("author_following", "Following"),
        ("author_project_count", "Project Count"),
    ]

    for col, label in author_cols:
        if col in df.columns:
            t10_avg = top10[col].mean() if not top10[col].isna().all() else 0
            lines.append(f"  {label:<25} Top-10 avg: {t10_avg:>10.0f}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Trend analysis (requires multiple snapshots)
# ---------------------------------------------------------------------------

def trend_analysis(query: str) -> str:
    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"  TREND ANALYSIS: '{query}'")
    lines.append(f"{'='*80}\n")

    snapshots = db.get_all_snapshots_for_query(query)
    if len(snapshots) < 2:
        lines.append("Need at least 2 snapshots for trend analysis.")
        lines.append(f"Current snapshots: {len(snapshots)}")
        return "\n".join(lines)

    # Track projects across snapshots
    conn = db.get_connection()
    project_positions = {}

    for snap in snapshots:
        results = conn.execute("""
            SELECT sr.position, p.behance_id, p.title
            FROM search_results sr
            JOIN projects p ON sr.project_id = p.id
            WHERE sr.snapshot_id = ?
            ORDER BY sr.position
        """, (snap["id"],)).fetchall()

        for r in results:
            pid = r["behance_id"]
            if pid not in project_positions:
                project_positions[pid] = {"title": r["title"], "positions": []}
            project_positions[pid]["positions"].append({
                "snapshot_id": snap["id"],
                "timestamp": snap["timestamp"],
                "position": r["position"],
            })

    conn.close()

    # Stable projects (in multiple snapshots)
    stable = {pid: d for pid, d in project_positions.items() if len(d["positions"]) >= 2}
    one_timers = {pid: d for pid, d in project_positions.items() if len(d["positions"]) == 1}

    lines.append(f"Total unique projects seen: {len(project_positions)}")
    lines.append(f"Stable (2+ snapshots): {len(stable)}")
    lines.append(f"One-timers: {len(one_timers)}")
    lines.append("")

    if stable:
        lines.append("--- Stable Projects (appear in multiple snapshots) ---")
        for pid, d in sorted(stable.items(), key=lambda x: x[1]["positions"][-1]["position"]):
            positions = [p["position"] for p in d["positions"]]
            trend = positions[-1] - positions[0]
            trend_str = f"+{trend}" if trend > 0 else str(trend)
            lines.append(
                f"  {d['title'][:50]:<52} "
                f"positions: {positions} "
                f"trend: {trend_str}"
            )
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Experiment tracking
# ---------------------------------------------------------------------------

def experiment_tracking_report() -> str:
    lines = []

    if not config.TRACKED_PROJECTS:
        return ""

    lines.append(f"\n{'='*80}")
    lines.append(f"  EXPERIMENT TRACKING")
    lines.append(f"{'='*80}\n")

    for tp in config.TRACKED_PROJECTS:
        bid = tp.get("behance_id")
        if not bid:
            continue
        label = tp.get("label", bid)

        history = db.get_tracked_history(bid)
        if not history:
            lines.append(f"--- {label} ({bid}) ---")
            lines.append(f"  No data yet\n")
            continue

        lines.append(f"--- {label} ({bid}) ---")

        first = history[0]
        latest = history[-1]
        total_snapshots = len(history)

        lines.append(f"  Snapshots: {total_snapshots}")
        lines.append(f"  First seen: {first['timestamp'][:16]}")
        lines.append(f"  Latest:     {latest['timestamp'][:16]}")
        lines.append(f"  Days since publish: {latest.get('days_since_publish', '?')}")
        lines.append(f"")

        # Velocity
        if total_snapshots >= 2:
            appr_diff = latest["appreciations"] - first["appreciations"]
            views_diff = latest["views"] - first["views"]
            time_diff_hours = (
                datetime.fromisoformat(latest["timestamp"]) -
                datetime.fromisoformat(first["timestamp"])
            ).total_seconds() / 3600
            if time_diff_hours > 0:
                lines.append(f"  Velocity over {time_diff_hours:.0f}h:")
                lines.append(f"    Appr:  {first['appreciations']} -> {latest['appreciations']} (+{appr_diff}, {appr_diff / (time_diff_hours/24):.1f}/day)")
                lines.append(f"    Views: {first['views']} -> {latest['views']} (+{views_diff}, {views_diff / (time_diff_hours/24):.1f}/day)")
        else:
            lines.append(f"  Current: appr={latest['appreciations']} views={latest['views']}")

        # Position history
        lines.append(f"")
        lines.append(f"  Position history ('инфографика'):")
        for h in history:
            pos = h.get("position_infografika")
            ts = h["timestamp"][:16]
            status = f"#{pos}" if pos else "NOT in top-100"
            lines.append(f"    {ts} | {status} | appr={h['appreciations']} views={h['views']}")

        lines.append(f"  Position history ('дизайн карточек'):")
        for h in history:
            pos = h.get("position_design_cards")
            ts = h["timestamp"][:16]
            status = f"#{pos}" if pos else "NOT in top-100"
            lines.append(f"    {ts} | {status} | appr={h['appreciations']} views={h['views']}")

        # Summary
        best_pos_inf = min((h["position_infografika"] for h in history if h["position_infografika"]), default=None)
        best_pos_dc = min((h["position_design_cards"] for h in history if h["position_design_cards"]), default=None)
        times_in_top = sum(1 for h in history if h["position_infografika"] or h["position_design_cards"])

        lines.append(f"")
        lines.append(f"  SUMMARY:")
        lines.append(f"    Best position (инфографика):    {'#' + str(best_pos_inf) if best_pos_inf else 'never in top-100'}")
        lines.append(f"    Best position (дизайн карточек): {'#' + str(best_pos_dc) if best_pos_dc else 'never in top-100'}")
        lines.append(f"    Times found in top-100: {times_in_top}/{total_snapshots} snapshots")
        lines.append(f"")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Full report
# ---------------------------------------------------------------------------

def generate_full_report(queries: list[str] | None = None) -> str:
    if queries is None:
        queries = config.SEARCH_QUERIES["primary"]

    report_parts = []
    report_parts.append(f"\n{'#'*80}")
    report_parts.append(f"  BEHANCE SEARCH ANALYSIS REPORT")
    report_parts.append(f"  Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    report_parts.append(f"{'#'*80}")

    my_df = load_my_projects_df()

    for query in queries:
        df = load_latest_snapshot(query)
        if df is None or df.empty:
            report_parts.append(f"\nNo data for query: '{query}'")
            continue

        report_parts.append(descriptive_stats(df, query))
        report_parts.append(correlation_analysis(df, query))
        report_parts.append(gap_analysis(df, my_df, query))
        report_parts.append(trend_analysis(query))

    report_parts.append(experiment_tracking_report())

    report = "\n".join(report_parts)

    os.makedirs(config.REPORTS_DIR, exist_ok=True)
    filename = f"report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
    filepath = os.path.join(config.REPORTS_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(report)

    print(report)
    print(f"\nReport saved to: {filepath}")

    return report


if __name__ == "__main__":
    generate_full_report()
