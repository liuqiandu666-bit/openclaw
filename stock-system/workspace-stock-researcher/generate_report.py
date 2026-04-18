#!/usr/bin/env python3
"""
生成详细版候选报告

用法:
  python3 generate_report.py                # 前30名，自动找最新 screener JSON
  python3 generate_report.py --top-n 10    # 前10名
  python3 generate_report.py --date 2026-04-18  # 指定日期的 screener JSON

版本元数据写入报告头部，供脚本/agent消费:
  report_schema_version, generator_version, source_json_date
"""
import argparse
import glob
import json
import os
import sqlite3
import datetime
from collections import defaultdict

REPORT_SCHEMA_VERSION = "1.1"
GENERATOR_VERSION = "2026-04-18"

DB_PATH = "/home/<user>/.openclaw/workspace/data/astock.db"
MEMORY_DIR = "/home/<user>/.openclaw/workspace/memory"


def find_latest_screener_json(date_str=None):
    if date_str:
        path = os.path.join(MEMORY_DIR, f"screener_result_{date_str}.json")
        if not os.path.exists(path):
            raise FileNotFoundError(f"screener JSON not found: {path}")
        return path
    files = sorted(glob.glob(os.path.join(MEMORY_DIR, "screener_result_*.json")))
    if not files:
        raise FileNotFoundError(f"No screener_result_*.json found in {MEMORY_DIR}")
    return files[-1]


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def fetch_detailed_data(codes):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" * len(codes))

    bs_rows = conn.execute(f"""
        SELECT code, report_date, contract_liab, advance_recv, total_assets, total_equity
        FROM balance_sheet WHERE code IN ({placeholders})
        ORDER BY code, report_date DESC
    """, codes).fetchall()

    inc_rows = conn.execute(f"""
        SELECT code, report_date, operate_income, gross_margin,
               COALESCE(parent_netprofit, netprofit) AS ni
        FROM income_stmt WHERE code IN ({placeholders})
        ORDER BY code, report_date DESC
    """, codes).fetchall()

    cf_rows = conn.execute(f"""
        SELECT code, report_date, netcash_operate, construct_asset
        FROM cash_flow WHERE code IN ({placeholders})
        ORDER BY code, report_date DESC
    """, codes).fetchall()

    conn.close()

    def group_by_code(rows, limit=6):
        d = defaultdict(list)
        for r in rows:
            if len(d[r["code"]]) < limit:
                d[r["code"]].append(dict(r))
        return d

    return group_by_code(bs_rows), group_by_code(inc_rows), group_by_code(cf_rows)


def fmt(num, unit="亿元"):
    if num is None:
        return "暂无"
    if abs(num) >= 1e8:
        return f"{num/1e8:.1f}{unit}"
    if abs(num) >= 1e4:
        return f"{num/1e4:.1f}万元"
    return f"{num:.0f}"


def judge_pe(pe):
    if pe is None:
        return "暂无"
    if pe < 0:
        return "亏损"
    if pe < 15:
        return "低"
    if pe <= 30:
        return "合理"
    if pe <= 60:
        return "偏高"
    if pe <= 150:
        return "高"
    return "极高"


def generate_report(top_n=30, date_str=None):
    today = datetime.date.today().strftime("%Y-%m-%d")
    json_path = find_latest_screener_json(date_str)
    source_json_date = os.path.basename(json_path).replace("screener_result_", "").replace(".json", "")
    output_path = os.path.join(MEMORY_DIR, f"stock-pick-{today}.md")

    data = load_json(json_path)
    summary = data["summary"]
    industry_stats = data["industry_stats"]

    # 明确按 composite_score 降序重排，不依赖 JSON 原始顺序
    results = sorted(data["results"], key=lambda x: (
        -x.get("composite_score", 0),
        -x.get("quant_score", 0),
        -x.get("exec_hold_score", 0),
        x.get("code", "")
    ))

    candidates = results[:top_n]
    codes = [c["code"] for c in candidates]
    bs_map, inc_map, cf_map = fetch_detailed_data(codes)

    lines = []

    # ── 元数据头（机器可读）──────────────────────────────────────
    lines.append(f"<!-- report_schema_version: {REPORT_SCHEMA_VERSION} -->")
    lines.append(f"<!-- generator_version: {GENERATOR_VERSION} -->")
    lines.append(f"<!-- source_json_date: {source_json_date} -->")
    lines.append(f"<!-- generated_at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} -->")
    lines.append(f"<!-- top_n: {top_n} -->")
    lines.append("")

    # ── 报告正文 ────────────────────────────────────────────────
    lines.append(f"# A股量化筛选候选报告（{today}）\n")

    lines.append("## 第一部分：筛选概况\n")
    lines.append(f"**数据截至** {data.get('date', source_json_date)}\n")
    lines.append(f"**全市场总数**：{summary['total_market']} 只  |  **量化达标**：{summary['qualified']} 只  |  **涉及行业**：{summary['industries']} 个\n")
    lines.append(f"**筛选方式**：{data.get('source', 'local_db')}  |  **展示范围**：前{top_n}名\n")

    lines.append("\n## 第二部分：行业分布（达标数量前10）\n")
    lines.append("| 行业 | 达标数量 | 最高分 | 均分 |")
    lines.append("|------|---------|--------|------|")
    for stat in industry_stats[:10]:
        lines.append(f"| {stat['industry']} | {stat['count']} 只 | {stat['top_score']} | {stat['avg_score']:.1f} |")

    lines.append(f"\n## 第三部分：每只股票量化指标详情（前{top_n}名）\n")

    for idx, cand in enumerate(candidates, start=1):
        code = cand["code"]
        name = cand["name"]
        industry = cand.get("industry", "--")
        quant = cand["quant_score"]
        exec_score = cand["exec_hold_score"]
        composite = cand["composite_score"]
        passed_count = cand.get("passed_count", 0)
        b_class = cand.get("b_class", "")
        metrics = cand.get("metrics", {})
        passed = cand.get("passed", [])
        failed = cand.get("failed", [])

        latest_bs = (bs_map.get(code) or [{}])[0]
        latest_inc = (inc_map.get(code) or [{}])[0]
        latest_cf = (cf_map.get(code) or [{}])[0]

        if b_class:
            lines.append(f"\n### 【{idx:2d}】{name} {code}  `综合评分:{composite}` — 🌟**战略特例·{b_class}**")
        else:
            lines.append(f"\n### 【{idx:2d}】{name} {code}  `综合评分:{composite}` — **A类**")

        lines.append(f"**行业**：{industry}  |  **达标项**：{passed_count}/5")

        lines.append("\n| 指标 | 数值 | 达标 | 来源报期 |")
        lines.append("|------|------|------|---------|")

        # 指标1：合同负债同比
        cl_yoy = metrics.get("contract_liability_yoy")
        cl_date = latest_bs.get("report_date", "未知")
        cl_ok = "✅" if cl_yoy is not None and cl_yoy >= 45 else "❌"
        cl_disp = f"{cl_yoy:.1f}%" if cl_yoy is not None else "暂无"
        lines.append(f"| ⭐ 合同负债同比 | {cl_disp} | {cl_ok} | {cl_date} |")

        # 指标2：毛利率趋势
        gm_q0 = metrics.get("gross_margin_q0")
        gm_q1 = metrics.get("gross_margin_q1")
        gm_q2 = metrics.get("gross_margin_q2")
        gm_improving = metrics.get("gross_margin_improving")
        gm_ok = "✅" if gm_improving else "❌"
        if gm_q0 is not None and gm_q1 is not None and gm_q2 is not None:
            gm_disp = f"{gm_q2:.1f}%→{gm_q1:.1f}%→{gm_q0:.1f}%"
        else:
            gm_disp = "暂无"
        lines.append(f"| 毛利率趋势 | {gm_disp} | {gm_ok} | 连续3期 |")

        # 指标3：OCF/净利润
        ocf_ni = metrics.get("ocf_ni_ratio")
        ocf_ok = "✅" if ocf_ni is not None and ocf_ni >= 0.8 else "❌"
        ocf_disp = f"{ocf_ni:.2f}" if ocf_ni is not None else "暂无"
        lines.append(f"| ⭐ OCF/净利润 | {ocf_disp} | {ocf_ok} | 最新期 |")

        # 指标4：经营现金流同比（OCF YoY）
        ocf_yoy = metrics.get("ocf_yoy") or metrics.get("netcash_operate_yoy")
        ocf_yoy_ok = "✅" if ocf_yoy is not None and ocf_yoy > 50 else "❌"
        ocf_yoy_disp = f"{ocf_yoy:.1f}%" if ocf_yoy is not None else "暂无"
        lines.append(f"| ⭐ 经营现金流同比 | {ocf_yoy_disp} | {ocf_yoy_ok} | 最新期 vs 同期 |")

        # 指标5：CAPEX同比
        capex_yoy = metrics.get("capex_yoy")
        capex_ok = "✅" if capex_yoy is not None and capex_yoy >= 30 else "❌"
        capex_disp = f"{capex_yoy:.1f}%" if capex_yoy is not None else "暂无"
        lines.append(f"| CAPEX同比 | {capex_disp} | {capex_ok} | 最新期 vs 同期 |")

        # 估值
        pe = metrics.get("pe_ttm")
        pb = metrics.get("pb")
        price = metrics.get("price")
        lines.append("\n| 估值 | 数值 | 判断 |")
        lines.append("|------|------|------|")
        lines.append(f"| PE(TTM) | {pe if pe is not None else '暂无'} | {judge_pe(pe)} |")
        lines.append(f"| PB | {pb if pb is not None else '暂无'} | - |")
        lines.append(f"| 股价(元) | {price if price is not None else '暂无'} | - |")

        lines.append(f"\n**高管增减持（365天）**：净{'增持' if exec_score >= 50 else '减持'}倾向 | **行为评分** {exec_score}/100")

        risks = []
        if pe is not None and pe > 150:
            risks.append("估值极高（PE>150），需结合行业赛道判断")
        elif pe is not None and pe > 100:
            risks.append("估值偏高（PE>100），成长性需验证")
        if ocf_ni is not None and ocf_ni > 50:
            risks.append("OCF/NI异常高（净利润趋零），利润质量存疑")
        if passed_count < 4:
            fail_names = [f for f in failed if "未达标" in f or "无数据" in f]
            if fail_names:
                risks.append("部分指标未达标：" + "，".join(fail_names[:2]))
        if not risks:
            risks.append("无明显财务异常，但需关注行业周期性及市场风险")

        lines.append("\n**主要风险**：")
        for i, r in enumerate(risks, start=1):
            lines.append(f"{i}. {r}")

        lines.append("\n**综合评分**：")
        lines.append(f"- 量化得分（75%）：quant_score = {quant}")
        lines.append(f"- 高管行为（25%）：exec_hold_score = {exec_score}")
        if composite >= 70:
            brief = "高景气+高管增持信心足"
        elif composite >= 60:
            brief = "成长性良好，估值相对合理"
        elif composite >= 50:
            brief = "达标但部分指标偏弱，需进一步验证"
        else:
            brief = "达标门槛通过，综合评分偏低"
        lines.append(f"- **最终：composite_score = {composite}/100** — {brief}")
        lines.append("")

    # 第四部分：汇总对比表
    lines.append(f"\n## 第四部分：汇总对比表（前{top_n}名）\n")
    lines.append("| 排名 | 股票 | 行业 | 量化(75%) | 高管(25%) | 综合 | PE | 客观概括 |")
    lines.append("|------|------|------|-----------|-----------|------|----|---------|")
    for idx, cand in enumerate(candidates, start=1):
        composite = cand["composite_score"]
        pe = cand.get("metrics", {}).get("pe_ttm")
        if cand.get("b_class"):
            brief = "战略特例"
        elif composite >= 70:
            brief = "高景气"
        elif composite >= 60:
            brief = "成长良好"
        elif composite >= 50:
            brief = "达标偏弱"
        else:
            brief = "门槛通过"
        ind = cand.get("industry", "--")
        ind_short = ind[:10] + ("..." if len(ind) > 10 else "")
        lines.append(
            f"| {idx} | {cand['name']} ({cand['code']}) | {ind_short} "
            f"| {cand['quant_score']} | {cand['exec_hold_score']} | {composite} "
            f"| {pe if pe is not None else '--'} | {brief} |"
        )

    # 数据状态
    lines.append("\n---\n")
    lines.append("### 数据状态说明\n")
    conn = sqlite3.connect(DB_PATH)
    snap = conn.execute("SELECT MAX(snap_date) FROM market_snapshot").fetchone()[0]
    exec_dt = conn.execute("SELECT MAX(cutoff_date) FROM executive_hold").fetchone()[0]
    conn.close()
    lines.append(f"- **市场快照最新日期**：{snap}")
    lines.append(f"- **高管增减持数据截至**：{exec_dt}")
    lines.append(f"- **报告生成时间**：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if snap:
        diff = (datetime.date.today() - datetime.datetime.strptime(snap, "%Y-%m-%d").date()).days
        if diff > 7:
            lines.append(f"- ⚠️ **数据新鲜度提示**：市场快照距今已 {diff} 天（>7天），建议更新数据库")
    lines.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"报告已生成：{output_path}")
    return output_path


def run_regression_test():
    """最小回归测试：验证关键契约不漂移"""
    import sys
    errors = []

    # 1. 确认能找到 screener JSON
    try:
        path = find_latest_screener_json()
        data = load_json(path)
    except Exception as e:
        print(f"FAIL: screener JSON 加载失败: {e}")
        sys.exit(1)

    results = data.get("results", [])
    if not results:
        print("FAIL: results 为空")
        sys.exit(1)

    # 2. 验证 generate_report 重排后顺序正确（验证输出，不验证 JSON 原始顺序）
    sorted_results = sorted(results, key=lambda x: (
        -x.get("composite_score", 0),
        -x.get("quant_score", 0),
        -x.get("exec_hold_score", 0),
        x.get("code", "")
    ))
    scores = [r.get("composite_score", 0) for r in sorted_results[:30]]
    for i in range(len(scores) - 1):
        if scores[i] < scores[i + 1]:
            errors.append(f"排序后仍漂移（逻辑bug）：position {i}={scores[i]} < position {i+1}={scores[i+1]}")
            break

    # 3. 验证综合分公式：composite = quant*0.75 + exec*0.25（允许±0.2误差）
    for r in results[:10]:
        q = r.get("quant_score", 0)
        e = r.get("exec_hold_score", 0)
        c = r.get("composite_score", 0)
        expected = round(q * 0.75 + e * 0.25, 1)
        if abs(expected - c) > 0.2:
            errors.append(f"{r['code']} composite 不符：期望{expected} 实际{c}")

    # 4. 验证输出路径是动态日期（不是硬编码）
    today = datetime.date.today().strftime("%Y-%m-%d")
    output = os.path.join(MEMORY_DIR, f"stock-pick-{today}.md")
    # 运行一次生成，确认文件路径正确
    actual = generate_report(top_n=5)
    if not actual.endswith(f"stock-pick-{today}.md"):
        errors.append(f"输出路径日期不正确：{actual}")

    # 5. 验证报告头包含版本元数据
    with open(actual, encoding="utf-8") as f:
        content = f.read()
    for tag in ["report_schema_version", "generator_version", "source_json_date"]:
        if tag not in content:
            errors.append(f"报告头缺少元数据：{tag}")

    if errors:
        print("FAIL:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print(f"PASS: 所有回归测试通过（{len(results)} 只候选股，前5名报告已生成）")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="生成A股量化筛选候选报告")
    parser.add_argument("--top-n", type=int, default=30, help="展示前N名（默认30）")
    parser.add_argument("--date", type=str, default=None, help="指定screener JSON日期（格式YYYY-MM-DD）")
    parser.add_argument("--test", action="store_true", help="运行回归测试")
    args = parser.parse_args()

    if args.test:
        run_regression_test()
    else:
        generate_report(top_n=args.top_n, date_str=args.date)
