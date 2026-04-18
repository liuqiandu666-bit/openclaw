#!/usr/bin/env python3
"""
A 股量化筛选脚本
================
优先查本地数据库（data/astock.db），若不存在则回退到实时 AkShare API。

用法：
  python3 screener.py              # 正常筛选
  python3 screener.py --api        # 强制使用实时 API（忽略本地库）
  python3 screener.py --build-db   # 先建库再筛选

退出码：0=成功，1=致命错误
"""

import os
import sys
import argparse

os.environ["TQDM_DISABLE"] = "1"

import json
import time
import sqlite3
from datetime import datetime, date

TODAY = date.today().strftime("%Y-%m-%d")
OUTPUT_FILE = f"/home/<user>/.openclaw/workspace/memory/screener_result_{TODAY}.json"
MEMORY_DIR  = "/home/<user>/.openclaw/workspace/memory"
DB_PATH     = "/home/<user>/.openclaw/workspace/data/astock.db"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ══════════════════════════════════════════════════════════════
# 评分与 B 类战略特例标记
# ══════════════════════════════════════════════════════════════

# 国家战略行业（取证监会行业代码前3位）
_STRATEGIC_INDUSTRIES = {
    "C39": "AI算力/半导体",
    "I65": "数字经济/AI软件",
    "D44": "新能源/储能",
    "C38": "新能源设备/电气",
    "C27": "生物医药",
    "C35": "高端制造/机器人",
    "C34": "高端制造/通用设备",
    "C36": "新能源汽车",
    "C37": "铁路船舶/军工装备",
    "W84": "军工/国防",
}


def _compute_score(metrics: dict) -> int:
    """复合评分（0-100），衡量各指标超越阈值的幅度及估值合理性。"""
    score = 0

    # 合同负债同比（0-35分）：超越幅度越大越好
    cl = metrics.get("contract_liability_yoy")
    if cl is not None:
        if cl >= 500:   score += 35
        elif cl >= 200: score += 30
        elif cl >= 100: score += 24
        elif cl >= 45:  score += 16

    # 合同负债/营收比加分（0-8分）：订单饱满度，规模无关
    # CL/Rev 高意味着未来一段时间营收有保障，与公司大小无关
    cl_to_rev = metrics.get("cl_to_rev")
    if cl_to_rev is not None:
        if cl_to_rev >= 50:   score += 8   # 超过半年营收在手订单
        elif cl_to_rev >= 25: score += 5   # 超过一个季度
        elif cl_to_rev >= 10: score += 2   # 有一定在手订单

    # OCF/净利润（0-28分）：含金量核心指标
    ocf = metrics.get("ocf_ni_ratio")
    if ocf is not None and ocf > 0:
        if ocf >= 2.0:  score += 28
        elif ocf >= 1.5: score += 23
        elif ocf >= 1.0: score += 17
        elif ocf >= 0.8: score += 11

    # OCF/营收比加分（0-6分）：现金流利润率，规模无关
    # 反映真实造血效率，高 OCF margin 对大小公司同等有意义
    ocf_to_rev = metrics.get("ocf_to_rev")
    if ocf_to_rev is not None:
        if ocf_to_rev >= 20:  score += 6   # 优秀：每100元营收产生20元以上经营现金
        elif ocf_to_rev >= 10: score += 4  # 良好
        elif ocf_to_rev >= 5:  score += 2  # 尚可

    # 毛利率连续改善（0-20分）：改善幅度
    if metrics.get("gross_margin_improving"):
        gm0 = metrics.get("gross_margin_q0") or 0
        gm2 = metrics.get("gross_margin_q2") or 0
        improvement = gm0 - gm2
        if improvement >= 5:    score += 20
        elif improvement >= 2:  score += 15
        else:                   score += 10

    # CAPEX同比（0-12分）：扩张力度
    capex = metrics.get("capex_yoy")
    if capex is not None:
        if capex >= 200:   score += 12
        elif capex >= 100: score += 9
        elif capex >= 30:  score += 6

    # PE估值调整（-15 to +5）
    pe = metrics.get("pe_ttm")
    if pe is not None:
        if pe < 0:           score -= 15   # 亏损股，强力惩罚
        elif pe > 300:       score -= 15   # 极端泡沫（净利润趋零）
        elif pe > 100:       score -= 8
        elif pe > 60:        score -= 3
        elif 15 <= pe <= 40: score += 5
        elif pe <= 15:       score += 3

    # 季度合同负债持续高增长（0-8分）
    # 近4期中有多个季度环比≥20%，说明订单景气持续，而非昙花一现
    cl_streak = metrics.get("cl_streak_count", 0)
    if cl_streak >= 3:    score += 8   # 3-4期持续高增：强景气
    elif cl_streak == 2:  score += 5   # 2期高增：景气延续
    elif cl_streak == 1:  score += 2   # 1期高增：有苗头

    # 季度营收持续高增长（0-6分）
    # 近4期中多个季度环比≥15%，营收增长持续性强
    rev_streak = metrics.get("rev_streak_count", 0)
    if rev_streak >= 3:   score += 6
    elif rev_streak == 2: score += 4
    elif rev_streak == 1: score += 2

    # OCF/NI 异常截断：>50 视为净利润极低导致的失真，不再加分
    ocf = metrics.get("ocf_ni_ratio") or 0
    if ocf > 50:
        score -= 10  # 净利润几乎为零，利润质量评估失真

    return max(0, min(100, score))


def _exec_hold_score(code: str, conn) -> int:
    """
    基于近180天高管增减持行为打分（0-100）。

    评分逻辑：
    - 净增持 + 参与人数≥3  → 90-100（高管群体性看多）
    - 净增持 + 参与人数2   → 78-85
    - 净增持 + 参与人数1   → 62-70
    - 无数据/无动作        → 50（中性）
    - 净减持 + 非核心管理层 → 32-42
    - 净减持 + 核心管理层减持（董事长/CEO/CFO） → 10-22
    """
    from datetime import date, timedelta
    cutoff_180d = (date.today() - timedelta(days=180)).isoformat()

    try:
        rows = conn.execute("""
            SELECT change_type, shares_changed, person_role
            FROM executive_hold
            WHERE code=? AND cutoff_date >= ?
        """, (code, cutoff_180d)).fetchall()
    except Exception:
        return 50  # executive_hold 表不存在时返回中性

    if not rows:
        return 50

    buy_shares  = sum(r[1] or 0 for r in rows if r[0] == "增持")
    sell_shares = sum(abs(r[1] or 0) for r in rows if r[0] == "减持")
    buy_count   = sum(1 for r in rows if r[0] == "增持")
    sell_count  = sum(1 for r in rows if r[0] == "减持")
    net = buy_shares - sell_shares

    # 核心管理层减持检测（董事长/总经理/CFO/实控人等）
    key_roles = {"董事长", "总经理", "首席执行官", "CEO", "CFO", "实控人", "控股股东", "副董事长"}
    core_sell = any(
        r[0] == "减持" and any(k in (r[2] or "") for k in key_roles)
        for r in rows
    )

    if net > 0:
        if buy_count >= 3:
            score = 92
        elif buy_count >= 2:
            score = 80
        else:
            score = 65
    elif buy_count == 0 and sell_count == 0:
        score = 50
    else:
        if core_sell:
            score = 15
        elif sell_count >= 3:
            score = 28
        else:
            score = 40

    return score


def _final_score(quant_score: int, exec_hold_score: int, claude_score: float | None) -> float:
    """
    三维度加权最终得分（满分100）：
      量化指标    60%
      高管增减持  20%
      Claude投资价值评分 20%（1-10分 → 0-100）

    claude_score 为 None 时用中性值 5（即50分），待 SKILL.md 中 Claude 评分后更新。
    """
    claude_normalized = ((claude_score or 5.0) / 10.0) * 100
    score = quant_score * 0.6 + exec_hold_score * 0.2 + claude_normalized * 0.2
    return round(score, 1)


def _check_b_class(metrics: dict, industry_name: str | None) -> str | None:
    """检测 B 类战略特例：核心指标远超阈值 + 国家战略方向行业。

    提高门槛避免泛滥：CL>150% 或 OCF>2.0，且不亏损。
    """
    pe  = metrics.get("pe_ttm")
    cl  = metrics.get("contract_liability_yoy") or 0
    ocf = metrics.get("ocf_ni_ratio") or 0

    # 亏损股不能是B类特例
    if pe is not None and pe < 0:
        return None

    # OCF/NI>50 说明净利润趋零，不算真正的盈利质量优秀
    if ocf > 50:
        return None

    far_above = cl > 150 or ocf > 2.0  # 提高门槛：原来是 cl>80 or ocf>1.5

    if not far_above:
        return None

    ind = (industry_name or "")[:3]
    return _STRATEGIC_INDUSTRIES.get(ind)

def fatal(msg):
    print(f"[FATAL] {msg}", flush=True)
    sys.exit(1)

os.makedirs(MEMORY_DIR, exist_ok=True)

# ══════════════════════════════════════════════════════════════
# 模式一：查本地数据库（快，几秒完成）
# ══════════════════════════════════════════════════════════════

def screen_from_db():
    log(f"使用本地数据库：{DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 统计
    total = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    log(f"数据库中共 {total} 只股票")

    # 联表查询：每只股票取最新 5 期数据计算各指标（支持季度连续增长分析）
    sql = """
    WITH latest_bs AS (
        -- 最近 9 期资产负债表（rn=1最新，支持最近4期各自做 YoY）
        SELECT code, report_date, contract_liab, advance_recv,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY report_date DESC) AS rn
        FROM balance_sheet
    ),
    bs_pivot AS (
        SELECT code,
               MAX(CASE WHEN rn=1 THEN COALESCE(contract_liab, advance_recv) END) AS cl_latest,
               MAX(CASE WHEN rn=2 THEN COALESCE(contract_liab, advance_recv) END) AS cl_q2,
               MAX(CASE WHEN rn=3 THEN COALESCE(contract_liab, advance_recv) END) AS cl_q3,
               MAX(CASE WHEN rn=4 THEN COALESCE(contract_liab, advance_recv) END) AS cl_q4,
               MAX(CASE WHEN rn=5 THEN COALESCE(contract_liab, advance_recv) END) AS cl_py1,
               MAX(CASE WHEN rn=6 THEN COALESCE(contract_liab, advance_recv) END) AS cl_py2,
               MAX(CASE WHEN rn=7 THEN COALESCE(contract_liab, advance_recv) END) AS cl_py3,
               MAX(CASE WHEN rn=8 THEN COALESCE(contract_liab, advance_recv) END) AS cl_py4
        FROM latest_bs
        WHERE rn <= 8
        GROUP BY code
    ),
    latest_inc AS (
        SELECT code, report_date, gross_margin, netprofit, parent_netprofit,
               operate_income,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY report_date DESC) AS rn
        FROM income_stmt
    ),
    inc_pivot AS (
        SELECT code,
               MAX(CASE WHEN rn=1 THEN gross_margin      END) AS gm0,
               MAX(CASE WHEN rn=2 THEN gross_margin      END) AS gm1,
               MAX(CASE WHEN rn=3 THEN gross_margin      END) AS gm2,
               -- 累计营收 rn=1..9（用于还原单季度 + YoY，中国季报为累计值）
               MAX(CASE WHEN rn=1 THEN operate_income    END) AS rev0,
               MAX(CASE WHEN rn=2 THEN operate_income    END) AS rev1,
               MAX(CASE WHEN rn=3 THEN operate_income    END) AS rev2,
               MAX(CASE WHEN rn=4 THEN operate_income    END) AS rev3,
               MAX(CASE WHEN rn=5 THEN operate_income    END) AS rev4,
               MAX(CASE WHEN rn=6 THEN operate_income    END) AS rev5,
               MAX(CASE WHEN rn=7 THEN operate_income    END) AS rev6,
               MAX(CASE WHEN rn=8 THEN operate_income    END) AS rev7,
               MAX(CASE WHEN rn=9 THEN operate_income    END) AS rev8,
               -- 对应 report_date（用于识别年报边界）
               MAX(CASE WHEN rn=1 THEN report_date       END) AS rd0,
               MAX(CASE WHEN rn=2 THEN report_date       END) AS rd1,
               MAX(CASE WHEN rn=3 THEN report_date       END) AS rd2,
               MAX(CASE WHEN rn=4 THEN report_date       END) AS rd3,
               MAX(CASE WHEN rn=5 THEN report_date       END) AS rd4,
               MAX(CASE WHEN rn=6 THEN report_date       END) AS rd5,
               MAX(CASE WHEN rn=7 THEN report_date       END) AS rd6,
               MAX(CASE WHEN rn=8 THEN report_date       END) AS rd7,
               MAX(CASE WHEN rn=9 THEN report_date       END) AS rd8,
               MAX(CASE WHEN rn=1 THEN netprofit         END) AS netprofit,
               MAX(CASE WHEN rn=1 THEN parent_netprofit  END) AS parent_netprofit
        FROM latest_inc
        WHERE rn <= 9
        GROUP BY code
    ),
    latest_cf AS (
        SELECT code, report_date, netcash_operate, construct_asset,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY report_date DESC) AS rn
        FROM cash_flow
    ),
    cf_pivot AS (
        SELECT code,
               MAX(CASE WHEN rn=1 THEN netcash_operate  END) AS ocf_latest,
               MAX(CASE WHEN rn=2 THEN netcash_operate  END) AS ocf_q2,
               MAX(CASE WHEN rn=3 THEN netcash_operate  END) AS ocf_q3,
               MAX(CASE WHEN rn=4 THEN netcash_operate  END) AS ocf_q4,
               MAX(CASE WHEN rn=1 THEN construct_asset  END) AS capex_latest,
               MAX(CASE WHEN rn=5 THEN construct_asset  END) AS capex_prev
        FROM latest_cf
        WHERE rn <= 5
        GROUP BY code
    )
    SELECT
        s.code, s.name,
        bs.cl_latest, bs.cl_q2, bs.cl_q3, bs.cl_q4,
        bs.cl_py1, bs.cl_py2, bs.cl_py3, bs.cl_py4,
        inc.gm0, inc.gm1, inc.gm2,
        inc.rev0, inc.rev1, inc.rev2, inc.rev3, inc.rev4,
        inc.rev5, inc.rev6, inc.rev7, inc.rev8,
        inc.rd0, inc.rd1, inc.rd2, inc.rd3, inc.rd4,
        inc.rd5, inc.rd6, inc.rd7, inc.rd8,
        inc.netprofit, inc.parent_netprofit,
        cf.ocf_latest, cf.ocf_q2, cf.ocf_q3, cf.ocf_q4,
        cf.capex_latest, cf.capex_prev,
        COALESCE(inc.parent_netprofit, inc.netprofit) AS ni_latest,
        m.price, m.pe_ttm, m.pb, m.snap_date,
        ind.industry_name
    FROM stocks s
    LEFT JOIN bs_pivot        bs  ON s.code = bs.code
    LEFT JOIN inc_pivot       inc ON s.code = inc.code
    LEFT JOIN cf_pivot        cf  ON s.code = cf.code
    LEFT JOIN market_snapshot m   ON s.code = m.code
    LEFT JOIN industry        ind ON s.code = ind.code
    """

    rows = conn.execute(sql).fetchall()
    conn.close()

    results = []
    checked = 0

    for r in rows:
        checked += 1
        code = r["code"]
        name = r["name"]

        metrics = {}
        passed  = []
        failed  = []

        # 合同负债同比
        cl_latest    = r["cl_latest"]
        cl_prev_year = r["cl_py1"]
        if cl_latest is not None and cl_prev_year and cl_prev_year != 0:
            yoy = round((cl_latest - cl_prev_year) / abs(cl_prev_year) * 100, 1)
            metrics["contract_liability_yoy"]    = yoy
            metrics["contract_liability_latest"] = cl_latest
            # 合同负债 / 营收比：订单饱满度，规模无关，避免大市值天然满分
            rev_latest = r["rev0"]
            if rev_latest and rev_latest > 0:
                metrics["cl_to_rev"] = round(cl_latest / rev_latest * 100, 1)  # 百分比
            (passed if yoy >= 45 else failed).append(f"合同负债同比 {yoy}%")
        else:
            failed.append("合同负债同比 无数据")

        # 毛利率连续改善
        gm0, gm1, gm2 = r["gm0"], r["gm1"], r["gm2"]
        if gm0 is not None and gm1 is not None and gm2 is not None:
            improving = gm0 > gm1 > gm2
            metrics["gross_margin_q0"] = gm0
            metrics["gross_margin_q1"] = gm1
            metrics["gross_margin_q2"] = gm2
            metrics["gross_margin_improving"] = improving
            if improving:
                passed.append(f"毛利率连续改善 {gm2}%→{gm1}%→{gm0}%")
            else:
                failed.append(f"毛利率连续改善 未达标 {gm2}%→{gm1}%→{gm0}%")
        else:
            failed.append("毛利率连续改善 无数据")

        # OCF / 归母净利润（优先用 parent_netprofit，无则退回 netprofit）
        ocf = r["ocf_latest"]
        ni  = r["ni_latest"]
        if ocf is not None and ocf > 0:
            # OCF / 营收比：现金流利润率，规模无关，避免大市值天然满分
            rev_latest = r["rev0"]
            if rev_latest and rev_latest > 0:
                metrics["ocf_to_rev"] = round(ocf / rev_latest * 100, 1)  # 百分比
        if ocf is not None and ni is not None and ni != 0:
            if ni < 0:
                # 亏损股：OCF/NI 无意义，直接失败
                metrics["ocf_ni_ratio"] = None
                failed.append(f"OCF/净利润 亏损股跳过(净利润={round(ni/1e8,1)}亿)")
            else:
                ratio = round(ocf / ni, 2)
                metrics["ocf_ni_ratio"] = ratio
                (passed if ratio >= 0.8 else failed).append(f"OCF/净利润 {ratio}")
        else:
            failed.append("OCF/净利润 无数据")

        # CAPEX 同比
        cap_now  = r["capex_latest"]
        cap_prev = r["capex_prev"]
        if cap_now is not None and cap_prev and cap_prev != 0:
            capex_yoy = round((abs(cap_now) - abs(cap_prev)) / abs(cap_prev) * 100, 1)
            metrics["capex_yoy"] = capex_yoy
            (passed if capex_yoy >= 30 else failed).append(f"CAPEX同比 {capex_yoy}%")
        else:
            failed.append("CAPEX同比 无数据")

        # 合同负债季度同比：rn=1 vs rn=5，rn=2 vs rn=6，rn=3 vs rn=7，rn=4 vs rn=8
        # 资产负债表是时点值，同一报告期同比（如Q3 2025 vs Q3 2024）直接相比即可
        # 已有 rn=1..5，再从 bs_pivot 取 rn=6,7,8（需扩展 SQL）
        # 合同负债季度同比：rn=1 vs rn=5，rn=2 vs rn=6，rn=3 vs rn=7，rn=4 vs rn=8
        # 资产负债表是时点值，同一报告期直接同比（Q3 2025 vs Q3 2024）
        cl_recent = [r["cl_latest"], r["cl_q2"], r["cl_q3"], r["cl_q4"]]
        cl_prev   = [r["cl_py1"],   r["cl_py2"], r["cl_py3"], r["cl_py4"]]
        cl_yoy_list = []
        for cur, prev in zip(cl_recent, cl_prev):
            if cur is not None and prev and prev > 0:
                cl_yoy_list.append(round((cur - prev) / prev * 100, 1))
        if cl_yoy_list:
            high_cl_quarters = sum(1 for g in cl_yoy_list if g >= 30)
            metrics["cl_streak_count"] = high_cl_quarters
            metrics["cl_streak_yoy"]   = cl_yoy_list

        # 季度营收单季同比：今年某季度 vs 去年同季度
        # 中国季报为累计值，需先还原单季度，再做 YoY 比较
        # 还原规则：
        #   一季报(03-31): 单季 = 累计值（本身就是单季）
        #   中报(06-30):   单季 = 累计 - 上期(03-31)
        #   三季报(09-30): 单季 = 累计 - 上期(06-30)
        #   年报(12-31):   单季(Q4) = 全年 - 三季报
        rev_cum  = [r[f"rev{i}"] for i in range(9)]
        rev_date = [r[f"rd{i}"]  for i in range(9)]

        def _single_q(cum_vals, dates, idx):
            """从累计值中还原 idx 期的单季度营收"""
            v = cum_vals[idx]
            d = dates[idx]
            if v is None or d is None:
                return None
            month = d[5:7]
            if month == "03":           # 一季报：本身就是单季
                return v
            # 其他季报：减去上一期累计（找相邻上期）
            if idx + 1 < len(cum_vals):
                prev = cum_vals[idx + 1]
                prev_d = dates[idx + 1]
                # 确保上一期和本期是同年（避免跨年错误相减）
                if prev is not None and prev_d and prev_d[:4] == d[:4]:
                    return v - prev if v > prev else None
            return None  # 找不到上一期

        # 从最新9期中还原单季度，然后按季度类型（03/06/09/12）配对做 YoY
        quarter_singles = {}  # key: "YYYY-QN" → value: 单季营收
        for i in range(len(rev_date)):
            d = rev_date[i]
            if not d:
                continue
            year, month = d[:4], d[5:7]
            q_label = {"03": "Q1", "06": "Q2", "09": "Q3", "12": "Q4"}.get(month)
            if q_label:
                sq = _single_q(rev_cum, rev_date, i)
                if sq and sq > 0:
                    quarter_singles[f"{year}-{q_label}"] = sq

        # 计算最近 4 个季度的单季 YoY（基于实际数据中最新年份，避免当年报告未出的问题）
        all_years_in_data = sorted({k.split("-")[0] for k in quarter_singles}, reverse=True)
        rev_yoy_list = []
        if len(all_years_in_data) >= 2:
            cur_year  = all_years_in_data[0]
            prev_year = all_years_in_data[1]
            for q in ["Q1", "Q2", "Q3", "Q4"]:
                cur_key  = f"{cur_year}-{q}"
                prev_key = f"{prev_year}-{q}"
                if cur_key in quarter_singles and prev_key in quarter_singles:
                    cur_v, prev_v = quarter_singles[cur_key], quarter_singles[prev_key]
                    if prev_v > 0:
                        yoy = round((cur_v - prev_v) / prev_v * 100, 1)
                        rev_yoy_list.append((q, yoy))

        if rev_yoy_list:
            high_rev_quarters = sum(1 for _, g in rev_yoy_list if g >= 25)
            metrics["rev_streak_count"]  = high_rev_quarters
            metrics["rev_streak_yoy"]    = {q: g for q, g in rev_yoy_list}

        # 市场数据（来自 market_snapshot + industry）
        pe = r["pe_ttm"]
        metrics["price"]         = r["price"]
        metrics["pe_ttm"]        = pe
        metrics["pb"]            = r["pb"]
        metrics["snap_date"]     = r["snap_date"]
        metrics["industry_name"] = r["industry_name"]
        # PE 异常标注
        if pe is not None:
            if pe < 0:
                metrics["pe_warning"] = "亏损（PE<0）"
            elif pe > 100:
                metrics["pe_warning"] = f"估值偏高（PE={pe:.0f}）"

        if len(passed) >= 3:
            quant_score     = _compute_score(metrics)
            exec_score      = _exec_hold_score(code, conn)
            final           = _final_score(quant_score, exec_score, None)  # claude=None→中性5分
            b_class         = _check_b_class(metrics, r["industry_name"])
            results.append({
                "code": code, "name": name,
                "industry": r["industry_name"],
                "passed_count": len(passed),
                "quant_score":      quant_score,    # 量化指标得分（0-100），占60%
                "exec_hold_score":  exec_score,     # 高管增减持得分（0-100），占20%
                "claude_score":     None,           # Claude投资价值评分（1-10），待SKILL.md填入
                "composite_score":  final,          # 最终综合得分（claude=5中性占位）
                "b_class": b_class,
                "passed": passed, "failed": failed,
                "metrics": metrics,
            })

    log(f"筛选完成：检查 {checked} 只，达标 {len(results)} 只")
    return results, checked, total

# ══════════════════════════════════════════════════════════════
# 模式二：实时 AkShare API（慢，约 20 分钟）
# ══════════════════════════════════════════════════════════════

def screen_from_api():
    import akshare as ak

    log("使用实时 AkShare API（约需 20-30 分钟，请勿中断）...")

    all_stocks = None
    for attempt in range(1, 4):
        try:
            all_stocks = ak.stock_zh_a_spot()
            if all_stocks is not None and len(all_stocks) > 1000:
                break
        except Exception as e:
            log(f"  第 {attempt} 次拉取失败：{e}，10 秒后重试...")
            time.sleep(10)

    if all_stocks is None or len(all_stocks) < 1000:
        fatal("全市场数据拉取失败")

    total = len(all_stocks)
    log(f"✅ 全市场 {total} 只")

    candidates = all_stocks[
        ~all_stocks["名称"].str.contains("ST", na=False) &
        all_stocks["代码"].str.startswith(("sh", "sz"))
    ].copy()
    log(f"过滤后：{len(candidates)} 只")

    results = []
    errors  = []
    checked = 0
    MAX_STOCKS = 800

    for _, row in candidates.head(MAX_STOCKS).iterrows():
        full_code    = str(row["代码"])
        display_code = full_code.replace("sh", "").replace("sz", "")
        name         = str(row["名称"])
        checked     += 1

        if checked % 50 == 0:
            log(f"  进度：{checked}/{MAX_STOCKS}，达标 {len(results)} 只")

        try:
            metrics = {}

            bal = ak.stock_balance_sheet_by_report_em(symbol=full_code)
            if bal is not None and not bal.empty:
                for col in ["CONTRACT_LIAB", "ADVANCE_RECEIVABLES"]:
                    if col in bal.columns:
                        vals = bal[col].dropna()
                        if len(vals) >= 2:
                            l = float(vals.iloc[0])
                            p = float(vals.iloc[4] if len(vals) > 4 else vals.iloc[-1])
                            if p != 0:
                                metrics["contract_liability_yoy"]    = round((l - p) / abs(p) * 100, 1)
                                metrics["contract_liability_latest"] = l
                        break
            time.sleep(0.2)

            profit = ak.stock_profit_sheet_by_report_em(symbol=full_code)
            if profit is not None and not profit.empty:
                if "OPERATE_INCOME" in profit.columns and "OPERATE_COST" in profit.columns:
                    rev  = profit["OPERATE_INCOME"].dropna().astype(float)
                    cost = profit["OPERATE_COST"].dropna().astype(float)
                    if len(rev) >= 3:
                        gm = [round((r - c) / r * 100, 1) if r != 0 else None
                              for r, c in zip(rev[:3], cost[:3])]
                        metrics["gross_margin_q0"] = gm[0]
                        metrics["gross_margin_q1"] = gm[1]
                        metrics["gross_margin_q2"] = gm[2]
                        if all(g is not None for g in gm):
                            metrics["gross_margin_improving"] = gm[0] > gm[1] > gm[2]
            time.sleep(0.2)

            cf = ak.stock_cash_flow_sheet_by_report_em(symbol=full_code)
            if cf is not None and not cf.empty:
                if "NETCASH_OPERATE" in cf.columns and "NETPROFIT" in cf.columns:
                    ocf = float(cf["NETCASH_OPERATE"].dropna().iloc[0])
                    ni  = float(cf["NETPROFIT"].dropna().iloc[0])
                    if ni != 0:
                        metrics["ocf_ni_ratio"] = round(ocf / ni, 2)
                if "CONSTRUCT_LONG_ASSET" in cf.columns:
                    cap = cf["CONSTRUCT_LONG_ASSET"].dropna().astype(float)
                    if len(cap) >= 5:
                        metrics["capex_yoy"] = round(
                            (abs(float(cap.iloc[0])) - abs(float(cap.iloc[4]))) / abs(float(cap.iloc[4])) * 100, 1
                        )
            time.sleep(0.1)

            metrics["price"]      = row.get("最新价")
            metrics["pe_ttm"]     = row.get("市盈率-动态")
            metrics["pb"]         = row.get("市净率")
            metrics["market_cap"] = row.get("总市值")

            passed, failed = [], []

            cl_yoy = metrics.get("contract_liability_yoy")
            if cl_yoy is not None:
                (passed if cl_yoy >= 45 else failed).append(f"合同负债同比 {cl_yoy}%")
            else:
                failed.append("合同负债同比 无数据")

            gm_ok = metrics.get("gross_margin_improving")
            if gm_ok is True:
                passed.append(f"毛利率连续改善 {metrics.get('gross_margin_q2')}%→{metrics.get('gross_margin_q1')}%→{metrics.get('gross_margin_q0')}%")
            elif gm_ok is False:
                failed.append(f"毛利率连续改善 未达标")
            else:
                failed.append("毛利率连续改善 无数据")

            ocf_ni = metrics.get("ocf_ni_ratio")
            if ocf_ni is not None:
                (passed if ocf_ni >= 0.8 else failed).append(f"OCF/净利润 {ocf_ni}")
            else:
                failed.append("OCF/净利润 无数据")

            capex_yoy = metrics.get("capex_yoy")
            if capex_yoy is not None:
                (passed if capex_yoy >= 30 else failed).append(f"CAPEX同比 {capex_yoy}%")
            else:
                failed.append("CAPEX同比 无数据")

            if len(passed) >= 3:
                results.append({
                    "code": display_code, "name": name,
                    "passed_count": len(passed),
                    "passed": passed, "failed": failed,
                    "metrics": metrics,
                })

        except Exception as e:
            errors.append({"code": display_code, "name": name, "error": str(e)})
            time.sleep(0.5)

    return results, checked, total

# ══════════════════════════════════════════════════════════════
# 主入口
# ══════════════════════════════════════════════════════════════

def _is_disqualified(r: dict) -> str | None:
    """检查是否存在硬性排除条件，返回原因字符串或 None。"""
    m = r.get("metrics", {})
    pe  = m.get("pe_ttm")
    ocf = m.get("ocf_ni_ratio") or 0

    if pe is not None and pe < 0:
        return f"亏损股(PE={pe:.1f})"
    if pe is not None and pe > 500:
        return f"极端估值泡沫(PE={pe:.0f})"
    if ocf > 50:
        return f"OCF/NI异常={ocf:.1f}(净利润趋零)"
    return None


def _pick_top_candidates(sorted_results: list, industry_cap: int = 5, total_cap: int = 40) -> list:
    """从排序后的结果中挑选行业分散的优质候选（B类优先，共享 total_cap）。

    硬性排除：亏损股、PE>500极端泡沫、OCF/NI>50异常值。
    """
    from collections import defaultdict
    industry_count: dict = defaultdict(int)
    selected = []
    seen_codes: set = set()

    # 第一轮：优先放入 B 类特例
    for r in sorted_results:
        if len(selected) >= total_cap:
            break
        if not r.get("b_class"):
            continue
        code = r["code"]
        if code in seen_codes:
            continue
        if _is_disqualified(r):
            continue
        ind = r.get("industry") or "未知"
        if industry_count[ind] < industry_cap:
            selected.append(r)
            seen_codes.add(code)
            industry_count[ind] += 1

    # 第二轮：剩余名额填入 A 类
    for r in sorted_results:
        if len(selected) >= total_cap:
            break
        code = r["code"]
        if code in seen_codes:
            continue
        if _is_disqualified(r):
            continue
        ind = r.get("industry") or "未知"
        if industry_count[ind] < industry_cap:
            selected.append(r)
            seen_codes.add(code)
            industry_count[ind] += 1

    return selected


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api",      action="store_true", help="强制使用实时 API")
    parser.add_argument("--build-db", action="store_true", help="先建库再筛选")
    args = parser.parse_args()

    if args.build_db:
        log("启动数据库建库...")
        os.system(f"python3 {os.path.dirname(__file__)}/../data/build_db.py")

    # 选择模式
    use_db = os.path.exists(DB_PATH) and not args.api
    if use_db:
        results, checked, total = screen_from_db()
        source = "local_db"
    else:
        if not args.api:
            log(f"本地数据库不存在（{DB_PATH}），回退到实时 API")
            log("提示：运行 python3 data/build_db.py 可建立本地库，之后筛选只需几秒")
        results, checked, total = screen_from_api()
        source = "akshare_api"

    # 按最终综合得分排序（passed_count 相同时按 composite_score 决胜）
    sorted_results = sorted(
        results,
        key=lambda x: (x["passed_count"], x.get("composite_score", 0)),
        reverse=True,
    )
    log(f"高管增减持评分样本（前3只）："
        + " | ".join(f"{r['name']}={r['exec_hold_score']}" for r in sorted_results[:3]))

    # top_candidates：行业分散，每行业最多 5 只，总数上限 40
    top_candidates = _pick_top_candidates(sorted_results, industry_cap=5, total_cap=40)

    # 输出 JSON
    output = {
        "date":   TODAY,
        "status": "success",
        "source": source,
        "summary": {
            "total_market":    total,
            "batch_checked":   checked,
            "qualified":       len(results),
            "top_candidates":  len(top_candidates),
        },
        "top_candidates": top_candidates,
        "results": sorted_results,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log(f"✅ 完成！达标 {len(results)} 只，来源={source}，结果→ {OUTPUT_FILE}")
    print(f"SCREENER_DONE status=success qualified={len(results)} source={source} file={OUTPUT_FILE}")
