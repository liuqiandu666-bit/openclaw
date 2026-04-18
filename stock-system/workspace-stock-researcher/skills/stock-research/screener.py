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


QUARTER_WEIGHTS = [1.4, 1.0, 1.0]
GROSS_MARGIN_PASS_RATIO_THRESHOLD = 55.0


def _compute_score(metrics: dict) -> int:
    """复合评分（0-100），衡量各指标超越阈值的幅度及估值合理性。"""
    score = 0

    # 合同负债同比（0-24分）：订单/预收款景气度的先行信号
    cl = metrics.get("contract_liability_yoy")
    if cl is not None:
        if cl >= 500:   score += 24
        elif cl >= 200: score += 20
        elif cl >= 100: score += 16
        elif cl >= 45:  score += 10

    # OCF/净利润（0-24分）：利润含金量核心指标，与订单/经营现金流同级
    ocf = metrics.get("ocf_ni_ratio")
    if ocf is not None and ocf > 0:
        if ocf >= 2.0:   score += 24
        elif ocf >= 1.5: score += 19
        elif ocf >= 1.0: score += 14
        elif ocf >= 0.8: score += 10

    # 经营现金流净流入净额同比（0-24分）：现金流扩张速度，与合同负债同比同权
    ocf_yoy = metrics.get("operating_cashflow_yoy")
    if ocf_yoy is not None:
        if ocf_yoy >= 300: score += 24
        elif ocf_yoy >= 150: score += 20
        elif ocf_yoy > 80: score += 14
        elif ocf_yoy > 50: score += 10

    # 毛利率连续改善（0-16分）：改善幅度
    if metrics.get("gross_margin_improving"):
        gm0 = metrics.get("gross_margin_q0") or 0
        gm2 = metrics.get("gross_margin_q2") or 0
        improvement = gm0 - gm2
        if improvement >= 5:    score += 16
        elif improvement >= 2:  score += 12
        else:                   score += 8

    # CAPEX同比（0-10分）：扩张力度
    capex = metrics.get("capex_yoy")
    if capex is not None:
        if capex >= 200:   score += 10
        elif capex >= 100: score += 7
        elif capex >= 30:  score += 5

    # PE估值调整（-15 to +6）
    pe = metrics.get("pe_ttm")
    if pe is not None:
        if pe < 0:           score -= 15
        elif pe > 300:       score -= 15
        elif pe > 100:       score -= 8
        elif pe > 60:        score -= 3
        elif 15 <= pe <= 40: score += 6
        elif pe <= 15:       score += 4

    # OCF/NI 异常截断：>50 视为净利润极低导致的失真，不再加分
    ocf = metrics.get("ocf_ni_ratio") or 0
    if ocf > 50:
        score -= 10

    return max(0, min(100, score))


def _weighted_average(values: list[float | int | None], digits: int = 1) -> float | None:
    valid = [(float(v), QUARTER_WEIGHTS[i]) for i, v in enumerate(values[: len(QUARTER_WEIGHTS)]) if v is not None]
    if not valid:
        return None
    weighted = sum(v * w for v, w in valid) / sum(w for _, w in valid)
    return round(weighted, digits)


def _weighted_true_ratio(values: list[bool | None], digits: int = 1) -> float | None:
    valid = [(bool(v), QUARTER_WEIGHTS[i]) for i, v in enumerate(values[: len(QUARTER_WEIGHTS)]) if v is not None]
    if not valid:
        return None
    ratio = sum((1 if v else 0) * w for v, w in valid) / sum(w for _, w in valid)
    return round(ratio * 100, digits)


def _gross_margin_improvement(metrics: dict | None) -> float | None:
    if not metrics:
        return None
    gm0 = metrics.get("gross_margin_q0")
    gm2 = metrics.get("gross_margin_q2")
    if gm0 is None or gm2 is None:
        return None
    return round(gm0 - gm2, 1)


def _summarize_multi_quarter_metrics(metrics_list: list[dict]) -> dict:
    return {
        "contract_liability_yoy_3q": _weighted_average([m.get("contract_liability_yoy") for m in metrics_list]),
        "ocf_ni_ratio_3q": _weighted_average([m.get("ocf_ni_ratio") for m in metrics_list], digits=2),
        "operating_cashflow_yoy_3q": _weighted_average([m.get("operating_cashflow_yoy") for m in metrics_list]),
        "capex_yoy_3q": _weighted_average([m.get("capex_yoy") for m in metrics_list]),
        "gross_margin_pass_ratio_3q": _weighted_true_ratio([m.get("gross_margin_improving") for m in metrics_list]),
        "gross_margin_improvement_3q": _weighted_average([_gross_margin_improvement(m) for m in metrics_list]),
    }


def _evaluate_multi_quarter_screen(metrics_list: list[dict], ni_latest) -> tuple[dict, list[str], list[str]]:
    summary = _summarize_multi_quarter_metrics(metrics_list)
    passed: list[str] = []
    failed: list[str] = []

    cl_3q = summary.get("contract_liability_yoy_3q")
    if cl_3q is not None:
        (passed if cl_3q >= 45 else failed).append(f"合同负债同比 三季加权 {cl_3q}%")
    else:
        failed.append("合同负债同比 三季加权无数据")

    gm_ratio = summary.get("gross_margin_pass_ratio_3q")
    gm_improvement = summary.get("gross_margin_improvement_3q")
    if gm_ratio is not None:
        gm_text = f"毛利率连续改善 三季达标率 {gm_ratio}%"
        if gm_improvement is not None:
            gm_text += f"，三季加权改善 {gm_improvement:+.1f}pct"
        (passed if gm_ratio >= GROSS_MARGIN_PASS_RATIO_THRESHOLD else failed).append(gm_text)
    else:
        failed.append("毛利率连续改善 三季加权无数据")

    ocf_3q = summary.get("ocf_ni_ratio_3q")
    if ocf_3q is not None:
        (passed if ocf_3q >= 0.8 else failed).append(f"OCF/净利润 三季加权 {ocf_3q}")
    elif ni_latest is not None and ni_latest < 0:
        failed.append(f"OCF/净利润 三季加权缺失(最新季度亏损，净利润={round(ni_latest/1e8,1)}亿)")
    else:
        failed.append("OCF/净利润 三季加权无数据")

    ocf_yoy_3q = summary.get("operating_cashflow_yoy_3q")
    if ocf_yoy_3q is not None:
        (passed if ocf_yoy_3q > 50 else failed).append(f"经营现金流净流入净额同比 三季加权 {ocf_yoy_3q}%")
    else:
        failed.append("经营现金流净流入净额同比 三季加权无数据")

    capex_3q = summary.get("capex_yoy_3q")
    if capex_3q is not None:
        (passed if capex_3q >= 30 else failed).append(f"CAPEX同比 三季加权 {capex_3q}%")
    else:
        failed.append("CAPEX同比 三季加权无数据")

    return summary, passed, failed


def _exec_hold_score(code: str, conn) -> int:
    """
    基于近365天高管主动增减持行为打分（0-100）。

    只计入真实的主动市场交易，排除：
    - change_reason 为 nan（年报持股汇总披露，非实际交易）
    - 股权激励实施 / 分红送转（被动机械动作）
    - 协议转让 / 可转债转股 / 新股申购等

    评分逻辑：
    - 净增持 + 参与人数≥3  → 92（高管群体性看多）
    - 净增持 + 参与人数2   → 80
    - 净增持 + 参与人数1   → 65
    - 无数据/无主动动作    → 50（中性）
    - 净减持 + 核心管理层  → 15（董事长/总经理/CFO/实控人）
    - 净减持 + 人数≥3     → 28
    - 净减持 + 其他        → 40
    """
    from datetime import date, timedelta
    cutoff_365d = (date.today() - timedelta(days=365)).isoformat()

    # 仅计入主动市场交易
    VALID_REASONS = ("竞价交易", "二级市场买卖", "大宗交易", "盘后定价")

    try:
        rows = conn.execute("""
            SELECT change_type, shares_changed, person_role
            FROM executive_hold
            WHERE code=? AND cutoff_date >= ?
              AND change_reason IN (?, ?, ?, ?)
        """, (code, cutoff_365d, *VALID_REASONS)).fetchall()
    except Exception:
        return 50

    if not rows:
        return 50

    buy_shares  = sum(r[1] or 0 for r in rows if r[0] == "增持")
    sell_shares = sum(abs(r[1] or 0) for r in rows if r[0] == "减持")
    buy_count   = sum(1 for r in rows if r[0] == "增持")
    sell_count  = sum(1 for r in rows if r[0] == "减持")
    net = buy_shares - sell_shares

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


def _compute_multi_quarter_score(metrics_list: list[dict]) -> int:
    """
    三季度加权量化评分（0-100）。

    metrics_list[0] = 最新季度，[1] = 上季度，[2] = 再上季度。
    权重：最新季 41.2%（= 1.4/3.4），另两季各 29.4%（= 1/3.4）。
    只有有效数据的季度才参与加权，权重动态重归一化。
    """
    scores = []
    weights = []

    for i, metrics in enumerate(metrics_list):
        if metrics is None:
            continue
        s = _compute_score(metrics)
        has_data = any(metrics.get(k) is not None for k in (
            "contract_liability_yoy", "ocf_ni_ratio", "operating_cashflow_yoy",
            "gross_margin_improving", "capex_yoy"
        ))
        if has_data:
            scores.append(s)
            weights.append(QUARTER_WEIGHTS[i])

    if not scores:
        return 0

    total_w = sum(weights)
    weighted = sum(s * w for s, w in zip(scores, weights)) / total_w
    return max(0, min(100, round(weighted)))


def _final_score(quant_score: int, exec_hold_score: int) -> float:
    """
    二维度加权最终得分（满分100）：
      量化指标    75%
      高管增减持  25%
    """
    score = quant_score * 0.75 + exec_hold_score * 0.25
    return round(score, 1)


def _check_b_class(metrics: dict, industry_name: str | None) -> str | None:
    """检测 B 类战略特例：核心指标远超阈值 + 国家战略方向行业。

    提高门槛避免泛滥：CL>150% 或 OCF>2.0，且不亏损。
    """
    pe  = metrics.get("pe_ttm")
    cl  = metrics.get("contract_liability_yoy_3q") or metrics.get("contract_liability_yoy") or 0
    ocf = metrics.get("ocf_ni_ratio_3q") or metrics.get("ocf_ni_ratio") or 0

    # 亏损股不能是B类特例
    if pe is not None and pe < 0:
        return None

    # OCF/NI>50 说明净利润趋零，不算真正的盈利质量优秀
    if ocf > 50:
        return None

    far_above = cl > 150 or ocf > 2.0  # 以三季综合口径优先，避免单季脉冲噪音

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

    # 联表查询：取最近 7 期数据，支持 3 个季度各自的同比计算
    # bs/cf: rn=1~3（最近3季）+ rn=5~7（去年同期，用于同比）
    # inc:   rn=1~5（最近3季毛利率各需3期连续数据）
    sql = """
    WITH latest_bs AS (
        SELECT code, report_date, contract_liab, advance_recv,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY report_date DESC) AS rn
        FROM balance_sheet
    ),
    bs_pivot AS (
        SELECT code,
               MAX(CASE WHEN rn=1 THEN COALESCE(contract_liab, advance_recv) END) AS cl_q1,
               MAX(CASE WHEN rn=2 THEN COALESCE(contract_liab, advance_recv) END) AS cl_q2,
               MAX(CASE WHEN rn=3 THEN COALESCE(contract_liab, advance_recv) END) AS cl_q3,
               MAX(CASE WHEN rn=5 THEN COALESCE(contract_liab, advance_recv) END) AS cl_q1_prev,
               MAX(CASE WHEN rn=6 THEN COALESCE(contract_liab, advance_recv) END) AS cl_q2_prev,
               MAX(CASE WHEN rn=7 THEN COALESCE(contract_liab, advance_recv) END) AS cl_q3_prev
        FROM latest_bs
        WHERE rn <= 7
        GROUP BY code
    ),
    latest_inc AS (
        SELECT code, report_date, gross_margin, netprofit, parent_netprofit,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY report_date DESC) AS rn
        FROM income_stmt
    ),
    inc_pivot AS (
        SELECT code,
               -- 毛利率：rn=1~5 支持 Q1(1,2,3)、Q2(2,3,4)、Q3(3,4,5) 的连续改善判断
               MAX(CASE WHEN rn=1 THEN gross_margin     END) AS gm1,
               MAX(CASE WHEN rn=2 THEN gross_margin     END) AS gm2,
               MAX(CASE WHEN rn=3 THEN gross_margin     END) AS gm3,
               MAX(CASE WHEN rn=4 THEN gross_margin     END) AS gm4,
               MAX(CASE WHEN rn=5 THEN gross_margin     END) AS gm5,
               -- 净利润（只取最新3季，用于 OCF/NI 计算）
               MAX(CASE WHEN rn=1 THEN COALESCE(parent_netprofit, netprofit) END) AS ni_q1,
               MAX(CASE WHEN rn=2 THEN COALESCE(parent_netprofit, netprofit) END) AS ni_q2,
               MAX(CASE WHEN rn=3 THEN COALESCE(parent_netprofit, netprofit) END) AS ni_q3,
               -- 用于 passed_count 判断的最新期归母净利润
               MAX(CASE WHEN rn=1 THEN parent_netprofit  END) AS parent_netprofit,
               MAX(CASE WHEN rn=1 THEN netprofit         END) AS netprofit
        FROM latest_inc
        WHERE rn <= 5
        GROUP BY code
    ),
    latest_cf AS (
        SELECT code, report_date, netcash_operate, construct_asset,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY report_date DESC) AS rn
        FROM cash_flow
    ),
    cf_pivot AS (
        SELECT code,
               MAX(CASE WHEN rn=1 THEN netcash_operate END) AS ocf_q1,
               MAX(CASE WHEN rn=2 THEN netcash_operate END) AS ocf_q2,
               MAX(CASE WHEN rn=3 THEN netcash_operate END) AS ocf_q3,
               MAX(CASE WHEN rn=5 THEN netcash_operate END) AS ocf_q1_prev,
               MAX(CASE WHEN rn=6 THEN netcash_operate END) AS ocf_q2_prev,
               MAX(CASE WHEN rn=7 THEN netcash_operate END) AS ocf_q3_prev,
               MAX(CASE WHEN rn=1 THEN construct_asset END) AS capex_q1,
               MAX(CASE WHEN rn=2 THEN construct_asset END) AS capex_q2,
               MAX(CASE WHEN rn=3 THEN construct_asset END) AS capex_q3,
               MAX(CASE WHEN rn=5 THEN construct_asset END) AS capex_q1_prev,
               MAX(CASE WHEN rn=6 THEN construct_asset END) AS capex_q2_prev,
               MAX(CASE WHEN rn=7 THEN construct_asset END) AS capex_q3_prev
        FROM latest_cf
        WHERE rn <= 7
        GROUP BY code
    )
    SELECT
        s.code, s.name,
        bs.cl_q1, bs.cl_q2, bs.cl_q3,
        bs.cl_q1_prev, bs.cl_q2_prev, bs.cl_q3_prev,
        inc.gm1, inc.gm2, inc.gm3, inc.gm4, inc.gm5,
        inc.ni_q1, inc.ni_q2, inc.ni_q3,
        inc.parent_netprofit, inc.netprofit,
        cf.ocf_q1, cf.ocf_q2, cf.ocf_q3,
        cf.ocf_q1_prev, cf.ocf_q2_prev, cf.ocf_q3_prev,
        cf.capex_q1, cf.capex_q2, cf.capex_q3,
        cf.capex_q1_prev, cf.capex_q2_prev, cf.capex_q3_prev,
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

    results = []
    checked = 0

    for r in rows:
        checked += 1
        code = r["code"]
        name = r["name"]

        # ── 按季度分别计算各项指标 ──────────────────────────────
        # 辅助函数：计算单季度的 5 项量化指标
        def _calc_quarter_metrics(cl_cur, cl_pre, gm_a, gm_b, gm_c, ocf, ocf_pre, ni, cap_cur, cap_pre, pe, pb):
            """返回单季度的 metrics dict，供 _compute_score 使用。"""
            m = {}
            # 合同负债同比
            if cl_cur is not None and cl_pre and cl_pre != 0:
                m["contract_liability_yoy"]    = round((cl_cur - cl_pre) / abs(cl_pre) * 100, 1)
                m["contract_liability_latest"] = cl_cur
            # 毛利率连续改善（gm_a 最新，gm_c 最旧）
            if gm_a is not None and gm_b is not None and gm_c is not None:
                m["gross_margin_q0"] = gm_a
                m["gross_margin_q1"] = gm_b
                m["gross_margin_q2"] = gm_c
                m["gross_margin_improving"] = gm_a > gm_b > gm_c
            # OCF/NI
            if ocf is not None and ni is not None and ni != 0 and ni > 0:
                m["ocf_ni_ratio"] = round(ocf / ni, 2)
            elif ni is not None and ni < 0:
                m["ocf_ni_ratio"] = None  # 亏损股标记
            # 经营现金流净流入净额同比
            if ocf is not None and ocf_pre is not None and ocf_pre != 0:
                m["operating_cashflow_yoy"] = round((ocf - ocf_pre) / abs(ocf_pre) * 100, 1)
            # CAPEX 同比
            if cap_cur is not None and cap_pre and cap_pre != 0:
                m["capex_yoy"] = round((abs(cap_cur) - abs(cap_pre)) / abs(cap_pre) * 100, 1)
            # 估值（只挂在最新季度，其余季度不重复计算）
            m["pe_ttm"] = pe
            m["pb"]     = pb
            return m

        pe = r["pe_ttm"]
        pb = r["pb"]

        # 三个季度的 metrics（Q1=最新，Q2=上季，Q3=再上季）
        mq = [
            _calc_quarter_metrics(
                r["cl_q1"], r["cl_q1_prev"],
                r["gm1"], r["gm2"], r["gm3"],
                r["ocf_q1"], r["ocf_q1_prev"], r["ni_q1"],
                r["capex_q1"], r["capex_q1_prev"],
                pe, pb,
            ),
            _calc_quarter_metrics(
                r["cl_q2"], r["cl_q2_prev"],
                r["gm2"], r["gm3"], r["gm4"],
                r["ocf_q2"], r["ocf_q2_prev"], r["ni_q2"],
                r["capex_q2"], r["capex_q2_prev"],
                None, None,  # 估值只用最新期
            ),
            _calc_quarter_metrics(
                r["cl_q3"], r["cl_q3_prev"],
                r["gm3"], r["gm4"], r["gm5"],
                r["ocf_q3"], r["ocf_q3_prev"], r["ni_q3"],
                r["capex_q3"], r["capex_q3_prev"],
                None, None,
            ),
        ]

        # ── passed_count 改为按最近三季综合判断（5 项中满足 4 项入选）────
        metrics = dict(mq[0])  # 保留最新季度原始值，用于展示
        summary_metrics, passed, failed = _evaluate_multi_quarter_screen(mq, r["ni_latest"])
        metrics.update(summary_metrics)

        # 补充市场数据字段到主 metrics
        metrics["price"]         = r["price"]
        metrics["snap_date"]     = r["snap_date"]
        metrics["industry_name"] = r["industry_name"]
        if pe is not None:
            if pe < 0:
                metrics["pe_warning"] = "亏损（PE<0）"
            elif pe > 100:
                metrics["pe_warning"] = f"估值偏高（PE={pe:.0f}）"

        if len(passed) >= 4:
            # 三季度加权量化评分
            quant_score = _compute_multi_quarter_score(mq)
            exec_score  = _exec_hold_score(code, conn)
            final       = _final_score(quant_score, exec_score)
            b_class     = _check_b_class(metrics, r["industry_name"])
            results.append({
                "code": code, "name": name,
                "industry": r["industry_name"],
                "passed_count": len(passed),
                "quant_score":     quant_score,
                "exec_hold_score": exec_score,
                "composite_score": final,
                "b_class": b_class,
                "passed": passed, "failed": failed,
                "metrics": metrics,
            })

    conn.close()
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
    log(f"⚠️  API 模式仅筛前 {MAX_STOCKS} 只股票（全市场 {len(candidates)} 只），覆盖不完整。建议建立本地库后重试。")

    for _, row in candidates.head(MAX_STOCKS).iterrows():
        full_code    = str(row["代码"])
        display_code = full_code.replace("sh", "").replace("sz", "")
        name         = str(row["名称"])
        checked     += 1

        if checked % 50 == 0:
            log(f"  进度：{checked}/{MAX_STOCKS}，达标 {len(results)} 只")

        try:
            mq = [dict(), dict(), dict()]

            bal = ak.stock_balance_sheet_by_report_em(symbol=full_code)
            if bal is not None and not bal.empty:
                for col in ["CONTRACT_LIAB", "ADVANCE_RECEIVABLES"]:
                    if col in bal.columns:
                        vals = bal[col].dropna().astype(float).tolist()
                        for idx, (cur_i, prev_i) in enumerate(((0, 4), (1, 5), (2, 6))):
                            if len(vals) > prev_i and vals[prev_i] != 0:
                                mq[idx]["contract_liability_yoy"] = round((vals[cur_i] - vals[prev_i]) / abs(vals[prev_i]) * 100, 1)
                                if idx == 0:
                                    mq[idx]["contract_liability_latest"] = vals[cur_i]
                        break
            time.sleep(0.2)

            profit = ak.stock_profit_sheet_by_report_em(symbol=full_code)
            if profit is not None and not profit.empty and "OPERATE_INCOME" in profit.columns and "OPERATE_COST" in profit.columns:
                rev = profit["OPERATE_INCOME"].dropna().astype(float).tolist()
                cost = profit["OPERATE_COST"].dropna().astype(float).tolist()
                gm = [round((r - c) / r * 100, 1) if r != 0 else None for r, c in zip(rev, cost)]
                for idx, (a, b, c) in enumerate(((0, 1, 2), (1, 2, 3), (2, 3, 4))):
                    if len(gm) > c and all(g is not None for g in (gm[a], gm[b], gm[c])):
                        mq[idx]["gross_margin_q0"] = gm[a]
                        mq[idx]["gross_margin_q1"] = gm[b]
                        mq[idx]["gross_margin_q2"] = gm[c]
                        mq[idx]["gross_margin_improving"] = gm[a] > gm[b] > gm[c]
            time.sleep(0.2)

            cf = ak.stock_cash_flow_sheet_by_report_em(symbol=full_code)
            ni_latest = None
            if cf is not None and not cf.empty:
                ocf_series = cf["NETCASH_OPERATE"].dropna().astype(float).tolist() if "NETCASH_OPERATE" in cf.columns else []
                ni_series = cf["NETPROFIT"].dropna().astype(float).tolist() if "NETPROFIT" in cf.columns else []
                for idx in range(3):
                    if len(ocf_series) > idx and len(ni_series) > idx:
                        ocf = float(ocf_series[idx])
                        ni = float(ni_series[idx])
                        if idx == 0:
                            ni_latest = ni
                        if ni > 0:
                            mq[idx]["ocf_ni_ratio"] = round(ocf / ni, 2)
                        elif ni < 0:
                            mq[idx]["ocf_ni_ratio"] = None
                    if len(ocf_series) > idx + 4 and float(ocf_series[idx + 4]) != 0:
                        mq[idx]["operating_cashflow_yoy"] = round(
                            (float(ocf_series[idx]) - float(ocf_series[idx + 4])) / abs(float(ocf_series[idx + 4])) * 100, 1
                        )
                if "CONSTRUCT_LONG_ASSET" in cf.columns:
                    cap = cf["CONSTRUCT_LONG_ASSET"].dropna().astype(float).tolist()
                    for idx in range(3):
                        if len(cap) > idx + 4 and float(cap[idx + 4]) != 0:
                            mq[idx]["capex_yoy"] = round(
                                (abs(float(cap[idx])) - abs(float(cap[idx + 4]))) / abs(float(cap[idx + 4])) * 100, 1
                            )
            time.sleep(0.1)

            metrics = dict(mq[0])
            metrics["price"] = row.get("最新价")
            metrics["pe_ttm"] = row.get("市盈率-动态")
            metrics["pb"] = row.get("市净率")
            metrics["market_cap"] = row.get("总市值")
            mq[0]["pe_ttm"] = metrics["pe_ttm"]
            mq[0]["pb"] = metrics["pb"]

            summary_metrics, passed, failed = _evaluate_multi_quarter_screen(mq, ni_latest)
            metrics.update(summary_metrics)

            if len(passed) >= 4:
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


def _build_industry_stats(results: list[dict]) -> list[dict]:
    """基于给定结果列表汇总行业统计，保持 JSON/报告/announce 口径一致。"""
    from collections import defaultdict

    industry_map: dict = defaultdict(list)
    for r in results:
        ind = r.get("industry") or "未知"
        industry_map[ind].append(r)

    return [
        {
            "industry": ind,
            "count": len(stocks),
            "top_score": round(max(s.get("composite_score", 0) for s in stocks), 1),
            "avg_score": round(sum(s.get("composite_score", 0) for s in stocks) / len(stocks), 1),
        }
        for ind, stocks in sorted(
            industry_map.items(),
            key=lambda kv: (-len(kv[1]), -max(s.get("composite_score", 0) for s in kv[1])),
        )
    ]


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

    # 硬性排除：亏损股、极端估值泡沫、OCF/NI异常（在统计之前就剔除）
    clean_results = [r for r in results if not _is_disqualified(r)]
    excluded_count = len(results) - len(clean_results)
    if excluded_count:
        log(f"硬性排除 {excluded_count} 只（亏损/极端泡沫/OCF异常）")

    # 按综合得分降序排列（passed_count 相同时按 composite_score 决胜）
    sorted_results = sorted(
        clean_results,
        key=lambda x: (x["passed_count"], x.get("composite_score", 0)),
        reverse=True,
    )
    log(f"高管增减持评分样本（前3只）："
        + " | ".join(f"{r['name']}={r['exec_hold_score']}" for r in sorted_results[:3]))

    # 口径统一：results / summary / industry_stats 都保留“全量达标”视角。
    # 另外单独输出精选候选，供后续深度研究时按行业分散优先参考。
    all_results = sorted_results
    top_candidates = _pick_top_candidates(sorted_results, industry_cap=5, total_cap=40)

    industry_stats = _build_industry_stats(all_results)
    shortlist_industry_stats = _build_industry_stats(top_candidates)

    # 给每只股票附上市场行业排名；精选候选额外保留 shortlist 行业排名。
    industry_rank = {stat["industry"]: i + 1 for i, stat in enumerate(industry_stats)}
    shortlist_rank = {stat["industry"]: i + 1 for i, stat in enumerate(shortlist_industry_stats)}
    for r in all_results:
        r["industry_rank"] = industry_rank.get(r.get("industry") or "未知", 0)
    for r in top_candidates:
        r["shortlist_industry_rank"] = shortlist_rank.get(r.get("industry") or "未知", 0)

    output = {
        "date": TODAY,
        "status": "success",
        "source": source,
        "summary": {
            "total_market": total,
            "batch_checked": checked,
            "raw_qualified": len(clean_results),
            "qualified": len(all_results),
            "shortlisted": len(top_candidates),
            "excluded": excluded_count,
            "industries": len(industry_stats),
            "shortlist_industries": len(shortlist_industry_stats),
        },
        "industry_stats": industry_stats,
        "shortlist_industry_stats": shortlist_industry_stats,
        "results": all_results,
        "research_candidates": top_candidates,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log(
        f"✅ 完成！达标 {len(all_results)} 只，精选候选 {len(top_candidates)} 只，"
        f"涉及 {len(industry_stats)} 个行业，结果→ {OUTPUT_FILE}"
    )
    print(
        f"SCREENER_DONE status=success qualified={len(all_results)} shortlisted={len(top_candidates)} "
        f"industries={len(industry_stats)} source={source} file={OUTPUT_FILE}"
    )
