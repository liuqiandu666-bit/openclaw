#!/usr/bin/env python3
import argparse
import datetime as dt
import glob
import json
import sqlite3
from pathlib import Path

MEMORY_DIR = Path("/home/<user>/.openclaw/workspace/memory")
DB_PATH = "/home/<user>/.openclaw/workspace/data/astock.db"


def sort_screener_results(items):
    return sorted(
        items,
        key=lambda item: (
            -(item.get("composite_score") or -9999),
            -(item.get("quant_score") or -9999),
            -(item.get("exec_hold_score") or -9999),
            item.get("code") or "",
        ),
    )


def load_latest_screener(date_str=None):
    if date_str:
        path = MEMORY_DIR / f"screener_result_{date_str}.json"
        if not path.exists():
            raise FileNotFoundError(path)
    else:
        files = sorted(glob.glob(str(MEMORY_DIR / "screener_result_*.json")))
        if not files:
            raise FileNotFoundError("No screener_result_*.json found")
        path = Path(files[-1])
    with path.open("r", encoding="utf-8") as f:
        return json.load(f), path


def latest_snap_date():
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute("SELECT MAX(snap_date) FROM market_snapshot").fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()


def days_between(date_text, today):
    if not date_text:
        return None
    try:
        target = dt.date.fromisoformat(str(date_text)[:10])
    except ValueError:
        return None
    return (today - target).days


def fmt_num(value):
    if value is None:
        return "0"
    try:
        f = float(value)
    except Exception:
        return str(value)
    if f.is_integer():
        return str(int(f))
    return str(round(f, 1))


def build_announce(data, report_path, source_path):
    today = dt.date.today()
    result_date = data.get("date", today.isoformat())
    summary = data.get("summary", {})
    sorted_results = sort_screener_results(data.get("results", []))
    top5 = sorted_results[:5]

    b_count = sum(1 for x in sorted_results if x.get("b_class"))
    a_count = max(0, int(summary.get("qualified", len(sorted_results))) - b_count)

    industries = data.get("industry_stats", [])[:3]
    top_industry = "、".join(item.get("industry", "-") for item in industries) if industries else "-"

    snap_date = latest_snap_date()
    stale_days = days_between(snap_date, today)
    if stale_days is not None and stale_days > 7:
        data_state = f"⚠️ 快照过期（截至 {snap_date}，建议更新）"
    else:
        data_state = "正常"

    lines = [
        f"【筛选完成】{result_date}",
        f"数据截至：{result_date}",
        f"扫描总数：{int(summary.get('total_market', 0))} 只",
        f"达标数量：{int(summary.get('qualified', len(sorted_results)))} 只（A类 {a_count} 只，B类 {b_count} 只）",
        f"涉及行业：{int(summary.get('industries', len(data.get('industry_stats', []))))} 个（前三：{top_industry})",
        "前5名：",
    ]

    for idx, item in enumerate(top5, 1):
        lines.append(
            f"  {idx}. {item.get('code', '-')} {item.get('name', '-')}（composite={fmt_num(item.get('composite_score'))}, "
            f"quant={fmt_num(item.get('quant_score'))}, exec={fmt_num(item.get('exec_hold_score'))}）"
        )

    while len(top5) < 5:
        idx = len(top5) + 1
        lines.append(f"  {idx}. - -（composite=0, quant=0, exec=0）")
        top5.append({})

    lines.extend(
        [
            f"数据状态：{data_state}",
            f"报告路径：memory/{Path(report_path).name}",
            f"数据路径：memory/{source_path.name}",
        ]
    )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD, default latest screener")
    parser.add_argument(
        "--report-path",
        help="Report path override, default memory/stock-pick-<date>.md",
    )
    parser.add_argument(
        "--write-file",
        action="store_true",
        help="Write result to workspace-stock-researcher/announce_output.txt",
    )
    args = parser.parse_args()

    data, source_path = load_latest_screener(args.date)
    result_date = data.get("date")
    report_path = args.report_path or f"/home/<user>/.openclaw/workspace/memory/stock-pick-{result_date}.md"
    text = build_announce(data, report_path, source_path)
    print(text)
    if args.write_file:
        output = Path("/home/<user>/.openclaw/workspace-stock-researcher/announce_output.txt")
        output.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
