#!/usr/bin/env python3
"""
A 股本地财务数据库构建脚本（并发版）
======================================
用法：
  python3 build_db.py              # 建库 / 续跑
  python3 build_db.py --retry      # 重试失败股票
  python3 build_db.py --status     # 查看进度
  python3 build_db.py --workers N  # 指定并发数（默认 5）

性能：
  单线程：~135 秒/只 × 5012 只 ≈ 188 小时
  5 线程：~37 小时（各股并发，东方财富限速友好）
  8 线程：~23 小时（激进，有概率触发限速）

数据库：data/astock.db（SQLite WAL 模式，线程安全，单文件可迁移）
"""

import os
import sys
import sqlite3
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date

os.environ["TQDM_DISABLE"] = "1"
import requests as _requests
# 给所有 HTTP 请求加默认 timeout（akshare 不设超时会永久阻塞）
_orig_request = _requests.Session.request
def _request_with_timeout(self, method, url, **kwargs):
    kwargs.setdefault("timeout", 45)
    return _orig_request(self, method, url, **kwargs)
_requests.Session.request = _request_with_timeout
import akshare as ak

DB_PATH     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "astock.db")
KEEP_PERIODS = 8     # 保留最近 N 期（YoY 需要 5 期，多存几期供扩展）
SLEEP_OK     = 0.5   # 成功后等待（秒）
SLEEP_ERR    = 3.0   # 失败后等待（秒）
DEFAULT_WORKERS = 5

# 全局进度计数器
_lock    = threading.Lock()
_counter = {"done": 0, "skip": 0, "err": 0, "total": 0}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── 数据库 ────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS stocks (
    code        TEXT PRIMARY KEY,
    full_code   TEXT,
    name        TEXT,
    exchange    TEXT,
    updated_at  TEXT
);
CREATE TABLE IF NOT EXISTS balance_sheet (
    code                    TEXT,
    report_date             TEXT,
    monetary_fund           REAL,
    accounts_rece           REAL,
    inventory               REAL,
    total_current_assets    REAL,
    contract_liab           REAL,
    advance_recv            REAL,
    short_loan              REAL,
    total_current_liab      REAL,
    long_loan               REAL,
    total_assets            REAL,
    total_liab              REAL,
    total_equity            REAL,
    parent_equity           REAL,
    goodwill                REAL,
    fetched_at              TEXT,
    PRIMARY KEY (code, report_date)
);
CREATE TABLE IF NOT EXISTS income_stmt (
    code                TEXT,
    report_date         TEXT,
    operate_income      REAL,
    operate_cost        REAL,
    gross_margin        REAL,
    operate_profit      REAL,
    netprofit           REAL,
    parent_netprofit    REAL,
    deduct_parent_netprofit REAL,
    basic_eps           REAL,
    research_expense    REAL,
    sale_expense        REAL,
    manage_expense      REAL,
    finance_expense     REAL,
    fetched_at          TEXT,
    PRIMARY KEY (code, report_date)
);
CREATE TABLE IF NOT EXISTS cash_flow (
    code                TEXT,
    report_date         TEXT,
    netcash_operate     REAL,
    construct_asset     REAL,
    netcash_invest      REAL,
    netcash_finance     REAL,
    netprofit_cf        REAL,
    fetched_at          TEXT,
    PRIMARY KEY (code, report_date)
);
CREATE TABLE IF NOT EXISTS market_snapshot (
    code            TEXT PRIMARY KEY,
    snap_date       TEXT,
    price           REAL,
    pe_ttm          REAL,
    pb              REAL,
    total_mktcap    REAL,
    float_mktcap    REAL,
    turnover_rate   REAL,
    chg_pct_60d     REAL,
    chg_pct_ytd     REAL,
    updated_at      TEXT
);
CREATE TABLE IF NOT EXISTS industry (
    code            TEXT PRIMARY KEY,
    industry_name   TEXT,
    updated_at      TEXT
);
CREATE TABLE IF NOT EXISTS fetch_log (
    code        TEXT,
    table_name  TEXT,
    status      TEXT,
    attempts    INTEGER DEFAULT 0,
    error_msg   TEXT,
    last_at     TEXT,
    PRIMARY KEY (code, table_name)
);
CREATE INDEX IF NOT EXISTS idx_log_status ON fetch_log (status);
"""

def get_db():
    """每个线程创建独立连接"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")  # 等待锁最多 10 秒
    return conn

MIGRATIONS = [
    # income_stmt 新增列
    "ALTER TABLE income_stmt ADD COLUMN operate_profit REAL",
    "ALTER TABLE income_stmt ADD COLUMN parent_netprofit REAL",
    "ALTER TABLE income_stmt ADD COLUMN deduct_parent_netprofit REAL",
    "ALTER TABLE income_stmt ADD COLUMN basic_eps REAL",
    "ALTER TABLE income_stmt ADD COLUMN research_expense REAL",
    "ALTER TABLE income_stmt ADD COLUMN sale_expense REAL",
    "ALTER TABLE income_stmt ADD COLUMN manage_expense REAL",
    "ALTER TABLE income_stmt ADD COLUMN finance_expense REAL",
    # balance_sheet 新增列
    "ALTER TABLE balance_sheet ADD COLUMN monetary_fund REAL",
    "ALTER TABLE balance_sheet ADD COLUMN accounts_rece REAL",
    "ALTER TABLE balance_sheet ADD COLUMN inventory REAL",
    "ALTER TABLE balance_sheet ADD COLUMN total_current_assets REAL",
    "ALTER TABLE balance_sheet ADD COLUMN contract_liab REAL",
    "ALTER TABLE balance_sheet ADD COLUMN advance_recv REAL",
    "ALTER TABLE balance_sheet ADD COLUMN short_loan REAL",
    "ALTER TABLE balance_sheet ADD COLUMN total_current_liab REAL",
    "ALTER TABLE balance_sheet ADD COLUMN long_loan REAL",
    "ALTER TABLE balance_sheet ADD COLUMN parent_equity REAL",
    "ALTER TABLE balance_sheet ADD COLUMN goodwill REAL",
    # cash_flow 新增列
    "ALTER TABLE cash_flow ADD COLUMN netcash_invest REAL",
    "ALTER TABLE cash_flow ADD COLUMN netcash_finance REAL",
]

def init_db():
    conn = get_db()
    conn.executescript(SCHEMA)
    # 迁移：给已有旧表添加新列（忽略"列已存在"错误）
    for sql in MIGRATIONS:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # 列已存在，跳过
    conn.commit()
    conn.close()
    log(f"数据库已初始化：{DB_PATH}")

# ── 工具 ──────────────────────────────────────────────────────

def _f(row, col):
    try:
        v = row[col]
        return float(v) if v is not None else None
    except (KeyError, TypeError, ValueError):
        return None

def _safe_float(v):
    try:
        f = float(v)
        return None if (f != f) else f  # NaN check
    except (TypeError, ValueError):
        return None

def get_status(conn, code, table):
    r = conn.execute(
        "SELECT status, attempts FROM fetch_log WHERE code=? AND table_name=?",
        (code, table)
    ).fetchone()
    return r

def set_status(conn, code, table, status, error_msg=None, attempts=1):
    conn.execute(
        """INSERT OR REPLACE INTO fetch_log
           (code, table_name, status, attempts, error_msg, last_at)
           VALUES (?,?,?,?,?,?)""",
        (code, table, status, attempts, error_msg, datetime.now().isoformat())
    )
    conn.commit()

# ── 三张财务报表抓取 ──────────────────────────────────────────

def fetch_balance_sheet(conn, code, full_code):
    bal = ak.stock_balance_sheet_by_report_em(symbol=full_code)
    if bal is None or bal.empty:
        raise ValueError("返回空数据")
    rows, now = [], datetime.now().isoformat()
    for _, r in bal.head(KEEP_PERIODS).iterrows():
        rd = str(r.get("REPORT_DATE", ""))[:10]
        if not rd:
            continue
        rows.append((code, rd,
            _f(r, "MONETARYFUNDS"), _f(r, "ACCOUNTS_RECE"),
            _f(r, "INVENTORY"), _f(r, "TOTAL_CURRENT_ASSETS"),
            _f(r, "CONTRACT_LIAB"), _f(r, "ADVANCE_RECEIVABLES"),
            _f(r, "SHORT_LOAN"), _f(r, "TOTAL_CURRENT_LIAB"),
            _f(r, "LONG_LOAN"), _f(r, "TOTAL_ASSETS"),
            _f(r, "TOTAL_LIABILITIES"), _f(r, "TOTAL_EQUITY"),
            _f(r, "TOTAL_PARENT_EQUITY"), _f(r, "GOODWILL"), now))
    conn.executemany(
        "INSERT OR REPLACE INTO balance_sheet "
        "(code,report_date,monetary_fund,accounts_rece,inventory,total_current_assets,"
        "contract_liab,advance_recv,short_loan,total_current_liab,long_loan,"
        "total_assets,total_liab,total_equity,parent_equity,goodwill,fetched_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    return len(rows)

def fetch_income_stmt(conn, code, full_code):
    inc = ak.stock_profit_sheet_by_report_em(symbol=full_code)
    if inc is None or inc.empty:
        raise ValueError("返回空数据")
    rows, now = [], datetime.now().isoformat()
    for _, r in inc.head(KEEP_PERIODS).iterrows():
        rd = str(r.get("REPORT_DATE", ""))[:10]
        if not rd:
            continue
        rev  = _f(r, "OPERATE_INCOME")
        cost = _f(r, "OPERATE_COST")
        gm   = round((rev - cost) / rev * 100, 2) if rev and cost and rev != 0 else None
        rows.append((code, rd, rev, cost, gm,
            _f(r, "OPERATE_PROFIT"), _f(r, "NETPROFIT"),
            _f(r, "PARENT_NETPROFIT"), _f(r, "DEDUCT_PARENT_NETPROFIT"),
            _f(r, "BASIC_EPS"),
            _f(r, "RESEARCH_EXPENSE"), _f(r, "SALE_EXPENSE"),
            _f(r, "MANAGE_EXPENSE"), _f(r, "FINANCE_EXPENSE"), now))
    conn.executemany(
        "INSERT OR REPLACE INTO income_stmt "
        "(code,report_date,operate_income,operate_cost,gross_margin,"
        "operate_profit,netprofit,parent_netprofit,deduct_parent_netprofit,"
        "basic_eps,research_expense,sale_expense,manage_expense,finance_expense,fetched_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    return len(rows)

def fetch_cash_flow(conn, code, full_code):
    cf = ak.stock_cash_flow_sheet_by_report_em(symbol=full_code)
    if cf is None or cf.empty:
        raise ValueError("返回空数据")
    rows, now = [], datetime.now().isoformat()
    for _, r in cf.head(KEEP_PERIODS).iterrows():
        rd = str(r.get("REPORT_DATE", ""))[:10]
        if not rd:
            continue
        rows.append((code, rd,
            _f(r, "NETCASH_OPERATE"), _f(r, "CONSTRUCT_LONG_ASSET"),
            _f(r, "NETCASH_INVEST"), _f(r, "NETCASH_FINANCE"),
            _f(r, "NETPROFIT"), now))
    conn.executemany(
        "INSERT OR REPLACE INTO cash_flow "
        "(code,report_date,netcash_operate,construct_asset,netcash_invest,netcash_finance,netprofit_cf,fetched_at) "
        "VALUES (?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    return len(rows)

FETCHERS = {
    "balance_sheet": fetch_balance_sheet,
    "income_stmt":   fetch_income_stmt,
    "cash_flow":     fetch_cash_flow,
}

# ── 市场快照 + 行业分类 - 逐股拉取（稳健模式）─────────────────────
# 使用 stock_individual_info_em()（emweb.securities.eastmoney.com）
# 返回：总市值、流通市值、行业、最新价
# PE_TTM / PB 由本地财务数据计算：
#   PE_TTM = 总市值 / 近 4 季归母净利润之和
#   PB     = 总市值 / 最新归母净资产

def _compute_pe_pb(conn, code, total_mktcap):
    """
    从本地财务数据计算 PE 和 PB。

    PE：使用最新一期年报（12-31）的归母净利润。
        若最新期为三季报（09-30），用公式：TTM = 9M_cur + Annual_prev - 9M_prev
        若最新期为半年报（06-30）：TTM = H1_cur + Annual_prev - H1_prev
        若最新期为一季报（03-31）：TTM = Q1_cur + Annual_prev - Q1_prev
    PB：使用最新一期年报或最新期归母净资产。
    注：中国分季报为当年累计值，不可直接相加。
    """
    try:
        r = conn.execute(
            "SELECT parent_equity FROM balance_sheet WHERE code=? AND parent_equity IS NOT NULL "
            "ORDER BY report_date DESC LIMIT 1", (code,)
        ).fetchone()
        pb = round(total_mktcap / r[0], 2) if r and r[0] and r[0] > 0 else None
    except Exception:
        pb = None

    pe = None
    try:
        rows = conn.execute(
            "SELECT parent_netprofit, report_date FROM income_stmt WHERE code=? AND parent_netprofit IS NOT NULL "
            "ORDER BY report_date DESC LIMIT 8", (code,)
        ).fetchall()
        if not rows:
            return pe, pb

        latest_date  = rows[0][1]   # e.g. "2025-09-30"
        latest_val   = rows[0][0]
        latest_month = latest_date[5:7]   # "03"/"06"/"09"/"12"

        if latest_month == "12":
            # 年报：直接使用
            ttm = latest_val
        else:
            # 找上一年同期和上一年年报
            latest_year = int(latest_date[:4])
            prev_same   = f"{latest_year - 1}-{latest_date[5:]}"
            prev_annual = f"{latest_year - 1}-12-31"
            same_prev_val   = next((r[0] for r in rows if r[1] == prev_same),   None)
            annual_prev_val = next((r[0] for r in rows if r[1] == prev_annual), None)
            if same_prev_val is not None and annual_prev_val is not None:
                ttm = latest_val + annual_prev_val - same_prev_val
            elif annual_prev_val is not None:
                ttm = annual_prev_val   # 退而求其次：使用上年年报
            else:
                ttm = None

        pe = round(total_mktcap / ttm, 2) if ttm and ttm > 0 else None
    except Exception:
        pe = None

    return pe, pb


def fetch_market_snapshot(conn):
    """
    逐股调用 stock_individual_info_em()（使用 emweb，不受 push2 限速影响）
    同时写入 market_snapshot 和 industry 两张表。
    约 5000 只 × 0.4s ≈ 33 分钟。
    """
    stocks = conn.execute("SELECT code, full_code FROM stocks ORDER BY code").fetchall()
    total = len(stocks)
    log(f"开始逐股拉取市场快照+行业分类（共 {total} 只，约 33 分钟）...")

    now       = datetime.now().isoformat()
    snap_date = date.today().isoformat()
    done = 0
    err  = 0

    snap_rows = []
    ind_rows  = []

    for i, (code, full_code) in enumerate(stocks):
        try:
            df = ak.stock_individual_info_em(symbol=code)
            if df is None or df.empty:
                err += 1
                continue
            info = dict(zip(df["item"], df["value"]))

            def _s(key):
                try:
                    v = float(info.get(key, None))
                    return None if (v != v) else v
                except (TypeError, ValueError):
                    return None

            total_mktcap  = _s("总市值")
            float_mktcap  = _s("流通市值")
            industry_name = str(info.get("行业", "")).strip() or None

            # 最新价：总市值 / 总股本（避免额外 HTTP 请求）
            price = None
            total_shares = _s("总股本")
            if total_mktcap and total_shares and total_shares > 0:
                price = round(total_mktcap / total_shares, 2)

            pe, pb = _compute_pe_pb(conn, code, total_mktcap) if total_mktcap else (None, None)

            snap_rows.append((code, snap_date, price, pe, pb,
                              total_mktcap, float_mktcap, None, None, None, now))
            if industry_name:
                ind_rows.append((code, industry_name, now))
            done += 1
            time.sleep(0.4)

        except Exception as e:
            err += 1

        if (i + 1) % 200 == 0:
            log(f"  进度 {i+1}/{total}，成功={done}，失败={err}")
            # 批量写入，减少内存占用
            if snap_rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO market_snapshot "
                    "(code,snap_date,price,pe_ttm,pb,total_mktcap,float_mktcap,"
                    "turnover_rate,chg_pct_60d,chg_pct_ytd,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)", snap_rows)
            if ind_rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO industry (code,industry_name,updated_at) VALUES (?,?,?)",
                    ind_rows)
            conn.commit()
            snap_rows.clear()
            ind_rows.clear()

    # 写入剩余
    if snap_rows:
        conn.executemany(
            "INSERT OR REPLACE INTO market_snapshot "
            "(code,snap_date,price,pe_ttm,pb,total_mktcap,float_mktcap,"
            "turnover_rate,chg_pct_60d,chg_pct_ytd,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)", snap_rows)
    if ind_rows:
        conn.executemany(
            "INSERT OR REPLACE INTO industry (code,industry_name,updated_at) VALUES (?,?,?)",
            ind_rows)
    conn.commit()
    log(f"市场快照写入完成：{done} 只（失败 {err} 只）")
    log(f"行业分类写入完成：{len(ind_rows) + (done - len(snap_rows))} 只")
    return done


# ── 行业分类 + 市值（通过 stock_individual_info_em 逐股，走 emweb 不走 push2）────

def fetch_industry_data(conn, limit=None, interval=2.0):
    """
    逐股调用 stock_individual_info_em()，获取行业分类和总市值。
    - 走 emweb.securities.eastmoney.com（不受 push2 限速影响）
    - 每 200 只批量提交一次，断点续跑（跳过已有行业数据的股票）
    - 同时本地计算 PB = 总市值 / 归母净资产，PE_TTM = 总市值 / 近4季净利润
    - limit: 本次最多处理 N 只（分批调用避免限速）
    - interval: 请求间隔秒数（默认 2.0s）
    """
    stocks = conn.execute(
        "SELECT s.code, s.full_code FROM stocks s "
        "LEFT JOIN industry i ON s.code=i.code "
        "WHERE i.code IS NULL ORDER BY s.code"
    ).fetchall()

    if limit:
        stocks = stocks[:limit]

    total = len(stocks)
    already = conn.execute("SELECT COUNT(*) FROM industry").fetchone()[0]
    log(f"行业分类：已有 {already} 只，本批处理 {total} 只（间隔 {interval}s），约 {total*interval/3600:.1f} 小时...")

    if total == 0:
        log("行业分类已全部完成，跳过。")
        return already

    snap_date = date.today().isoformat()
    now_str   = datetime.now().isoformat()
    done = 0
    err  = 0
    ind_batch  = []
    snap_batch = []

    for i, (code, full_code) in enumerate(stocks):
        try:
            df = ak.stock_individual_info_em(symbol=code)
            if df is None or df.empty:
                err += 1
                time.sleep(interval)
                continue

            info = dict(zip(df["item"], df["value"]))
            industry_name = str(info.get("行业", "")).strip() or None
            total_mktcap  = _safe_float(info.get("总市值"))

            if industry_name:
                ind_batch.append((code, industry_name, now_str))

            # 本地计算 PB 和 PE_TTM
            pe, pb = _compute_pe_pb(conn, code, total_mktcap) if total_mktcap else (None, None)
            snap_batch.append((code, snap_date, None, pe, pb,
                               total_mktcap, None, None, None, None, now_str))
            done += 1

        except Exception as e:
            err += 1
            time.sleep(interval)
            continue

        time.sleep(interval)

        # 每 200 只批量提交
        if (i + 1) % 200 == 0:
            if ind_batch:
                conn.executemany(
                    "INSERT OR REPLACE INTO industry (code,industry_name,updated_at) VALUES (?,?,?)",
                    ind_batch)
            if snap_batch:
                conn.executemany(
                    "INSERT OR REPLACE INTO market_snapshot "
                    "(code,snap_date,price,pe_ttm,pb,total_mktcap,float_mktcap,"
                    "turnover_rate,chg_pct_60d,chg_pct_ytd,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)", snap_batch)
            conn.commit()
            log(f"  进度 {i+1}/{total}，成功={done}，失败={err}")
            ind_batch.clear()
            snap_batch.clear()

    # 写入剩余
    if ind_batch:
        conn.executemany(
            "INSERT OR REPLACE INTO industry (code,industry_name,updated_at) VALUES (?,?,?)",
            ind_batch)
    if snap_batch:
        conn.executemany(
            "INSERT OR REPLACE INTO market_snapshot "
            "(code,snap_date,price,pe_ttm,pb,total_mktcap,float_mktcap,"
            "turnover_rate,chg_pct_60d,chg_pct_ytd,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)", snap_batch)
    conn.commit()

    total_ind = conn.execute("SELECT COUNT(*) FROM industry").fetchone()[0]
    log(f"行业分类完成：新增 {done} 只，失败 {err} 只，累计 {total_ind} 只")
    return total_ind

# ── 单只股票处理（在线程中运行）────────────────────────────────

def process_stock(args):
    code, full_code, name, retry_only = args
    conn   = get_db()
    done   = 0
    skip   = 0
    errors = 0

    for table, fetcher in FETCHERS.items():
        st       = get_status(conn, code, table)
        attempts = (st["attempts"] if st else 0) + 1

        if st and st["status"] == "success" and not retry_only:
            skip += 1
            continue

        try:
            fetcher(conn, code, full_code)
            set_status(conn, code, table, "success", attempts=attempts)
            done += 1
            time.sleep(SLEEP_OK)
        except Exception as e:
            err_msg = str(e)[:200]
            set_status(conn, code, table, "error", error_msg=err_msg, attempts=attempts)
            errors += 1
            time.sleep(SLEEP_ERR)

    conn.close()

    with _lock:
        _counter["done"] += done
        _counter["skip"] += skip
        _counter["err"]  += errors
        finished = _counter["done"] + _counter["skip"] + _counter["err"]
        total    = _counter["total"]
        # 每完成 50 只打印一次
        stock_done = finished // 3  # 每只 3 张表
        if stock_done % 50 == 0 and stock_done > 0:
            eta_h = ((total * 3 - finished) * 135 / 5) / 3600  # 粗估
            log(f"进度 {stock_done}/{total//3}  成功={_counter['done']} "
                f"跳过={_counter['skip']} 失败={_counter['err']}  "
                f"预计剩余 {eta_h:.1f}h")

    return code, done, skip, errors

# ── 主流程 ─────────────────────────────────────────────────────

def load_stock_list():
    log("拉取全市场股票列表（约 30-60 秒）...")
    for attempt in range(1, 4):
        try:
            df = ak.stock_zh_a_spot()
            if df is not None and len(df) > 1000:
                break
            log(f"  第 {attempt} 次返回数量异常，重试...")
        except Exception as e:
            log(f"  第 {attempt} 次失败：{e}，10 秒后重试...")
            time.sleep(10)
    else:
        log("[FATAL] 全市场数据拉取失败")
        sys.exit(1)

    filtered = df[
        ~df["名称"].str.contains("ST", na=False) &
        df["代码"].str.startswith(("sh", "sz"))
    ].copy()

    stocks = []
    for _, row in filtered.iterrows():
        fc   = str(row["代码"])
        ex   = "sh" if fc.startswith("sh") else "sz"
        code = fc.replace("sh", "").replace("sz", "")
        stocks.append((code, fc, str(row["名称"]), ex))

    log(f"有效股票：{len(stocks)} 只（已排除北交所 + ST）")
    return stocks

def upsert_stocks(stocks):
    conn = get_db()
    now  = datetime.now().isoformat()
    conn.executemany(
        "INSERT OR REPLACE INTO stocks (code,full_code,name,exchange,updated_at) VALUES (?,?,?,?,?)",
        [(c, fc, n, ex, now) for c, fc, n, ex in stocks]
    )
    conn.commit()
    conn.close()

def run(retry_only=False, n_workers=DEFAULT_WORKERS):
    init_db()
    stocks = load_stock_list()
    upsert_stocks(stocks)

    if retry_only:
        conn = get_db()
        failed_codes = {r[0] for r in conn.execute(
            "SELECT DISTINCT code FROM fetch_log WHERE status='error'"
        ).fetchall()}
        conn.close()
        stocks = [(c, fc, n, ex) for c, fc, n, ex in stocks if c in failed_codes]
        log(f"重试模式：{len(stocks)} 只有失败记录")

    _counter.update({"done": 0, "skip": 0, "err": 0, "total": len(stocks) * 3})

    log(f"开始并发抓取，workers={n_workers}，预计 {len(stocks)*135/n_workers/3600:.1f} 小时")
    log("（中断后重新运行会自动续跑，不丢数据）")

    tasks = [(c, fc, n, retry_only) for c, fc, n, _ in stocks]

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(process_stock, t): t[0] for t in tasks}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log(f"  未捕获异常：{e}")

    conn = get_db()
    ok  = conn.execute("SELECT COUNT(*) FROM fetch_log WHERE status='success'").fetchone()[0]
    err = conn.execute("SELECT COUNT(*) FROM fetch_log WHERE status='error'").fetchone()[0]
    conn.close()

    log(f"\n✅ 完成！成功={ok} 失败={err}")
    log(f"数据库：{DB_PATH}  大小：{os.path.getsize(DB_PATH)/1024/1024:.1f} MB")
    if err > 0:
        log(f"提示：python3 build_db.py --retry 可重试失败项")

def show_status():
    if not os.path.exists(DB_PATH):
        print("数据库不存在，请先运行 python3 build_db.py")
        return
    init_db()   # 确保新表存在
    conn = get_db()
    total   = conn.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    bs      = conn.execute("SELECT COUNT(DISTINCT code) FROM balance_sheet").fetchone()[0]
    inc     = conn.execute("SELECT COUNT(DISTINCT code) FROM income_stmt").fetchone()[0]
    cf      = conn.execute("SELECT COUNT(DISTINCT code) FROM cash_flow").fetchone()[0]
    mkt     = conn.execute("SELECT COUNT(*) FROM market_snapshot").fetchone()[0]
    mkt_dt  = conn.execute("SELECT MAX(snap_date) FROM market_snapshot").fetchone()[0]
    ind     = conn.execute("SELECT COUNT(*) FROM industry").fetchone()[0]
    log_s   = conn.execute(
        "SELECT status, COUNT(*) FROM fetch_log GROUP BY status"
    ).fetchall()
    latest  = conn.execute(
        "SELECT MAX(last_at) FROM fetch_log WHERE status='success'"
    ).fetchone()[0]
    sz      = os.path.getsize(DB_PATH) / 1024 / 1024
    conn.close()

    print(f"\n{'='*45}")
    print(f"数据库：{DB_PATH}  ({sz:.1f} MB)")
    print(f"股票总数：{total}")
    print(f"财务报表  资产负债表：{bs} 只  利润表：{inc} 只  现金流：{cf} 只  覆盖率：{bs/max(total,1)*100:.1f}%")
    print(f"市场快照  {mkt} 只（最新日期：{mkt_dt or '未抓取'}）")
    print(f"行业分类  {ind} 只（{'已完成' if ind > 1000 else '未抓取或不完整'}）")
    print(f"\n财务报表抓取状态：")
    for s, n in log_s:
        print(f"  {s:10s}: {n}")
    print(f"\n最后更新：{latest}")
    print("="*45)

def fetch_market_snapshot_baostock(conn):
    """用 baostock 批量更新市场快照（PE/PB/收盘价），稳定不限速。"""
    try:
        import baostock as bs
    except ImportError:
        log("[ERROR] baostock 未安装，运行: pip install baostock --break-system-packages")
        return 0

    stocks = conn.execute("SELECT code, full_code FROM stocks ORDER BY code").fetchall()
    lg = bs.login()
    if lg.error_code != "0":
        log(f"[ERROR] baostock 登录失败: {lg.error_msg}")
        return 0

    # 取最近5个交易日，拿最新有数据的一天
    from datetime import timedelta
    snap_date = date.today()
    start_str = (snap_date - timedelta(days=7)).isoformat()
    end_str   = snap_date.isoformat()

    now = datetime.now().isoformat()
    rows = []
    fail = 0

    def _bs_code(full_code):
        return full_code[:2].lower() + "." + full_code[2:]

    def _f(v):
        try:
            return float(v) if v and v.strip() else None
        except (ValueError, AttributeError):
            return None

    for i, (code, full_code) in enumerate(stocks):
        rs = bs.query_history_k_data_plus(
            _bs_code(full_code), "date,close,peTTM,pbMRQ",
            start_date=start_str, end_date=end_str,
            frequency="d", adjustflag="3")
        data = []
        while rs.error_code == "0" and rs.next():
            data.append(rs.get_row_data())
        if data:
            d = data[-1]
            rows.append((code, d[0], _f(d[1]), _f(d[2]), _f(d[3]),
                         None, None, None, None, None, now))
        else:
            fail += 1

        if (i + 1) % 500 == 0:
            conn.executemany(
                "INSERT OR REPLACE INTO market_snapshot "
                "(code,snap_date,price,pe_ttm,pb,total_mktcap,float_mktcap,"
                "turnover_rate,chg_pct_60d,chg_pct_ytd,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
            conn.commit()
            log(f"  进度 {i+1}/{len(stocks)} 成功={len(rows)} 失败={fail}")
            rows.clear()

    if rows:
        conn.executemany(
            "INSERT OR REPLACE INTO market_snapshot "
            "(code,snap_date,price,pe_ttm,pb,total_mktcap,float_mktcap,"
            "turnover_rate,chg_pct_60d,chg_pct_ytd,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
        conn.commit()

    bs.logout()
    total = conn.execute("SELECT COUNT(*) FROM market_snapshot").fetchone()[0]
    log(f"市场快照(baostock)完成：总计 {total} 只，本次失败 {fail} 只")
    return total


def refresh_financials(n_workers=DEFAULT_WORKERS):
    """清除财务报表成功记录，强制重新抓取（用于季报/年报发布后更新）。"""
    init_db()
    conn = get_db()
    conn.execute("DELETE FROM fetch_log WHERE status='success'")
    conn.commit()
    cleared = conn.execute("SELECT changes()").fetchone()[0]
    conn.close()
    log(f"已清除 {cleared} 条成功记录，开始重新抓取...")
    run(retry_only=False, n_workers=n_workers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A 股本地财务数据库构建")
    parser.add_argument("--retry",     action="store_true", help="重试失败的财务报表")
    parser.add_argument("--refresh",   action="store_true", help="强制重抓所有财务报表（季报/年报更新用）")
    parser.add_argument("--status",    action="store_true", help="查看进度")
    parser.add_argument("--market",    action="store_true", help="刷新市场快照（akshare，东方财富）")
    parser.add_argument("--market-bs", action="store_true", help="刷新市场快照（baostock，稳定）", dest="market_bs")
    parser.add_argument("--industry",  action="store_true", help="刷新行业分类")
    parser.add_argument("--workers",   type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--limit",     type=int, default=None, help="行业/市值：本批最多处理N只（分批续跑）")
    parser.add_argument("--interval",  type=float, default=2.0, help="行业/市值：请求间隔秒数（默认2.0）")
    args = parser.parse_args()

    if args.status:
        show_status()
    elif args.market_bs:
        init_db()
        conn = get_db()
        fetch_market_snapshot_baostock(conn)
        conn.close()
    elif args.market:
        init_db()
        conn = get_db()
        fetch_market_snapshot(conn)
        conn.close()
    elif args.industry:
        init_db()
        conn = get_db()
        fetch_industry_data(conn, limit=args.limit, interval=args.interval)
        conn.close()
    elif args.refresh:
        refresh_financials(n_workers=args.workers)
    else:
        run(retry_only=args.retry, n_workers=args.workers)
