-- A股本地财务数据库 Schema
-- 由 build_db.py 自动建表，此文件仅供参考和新环境初始化
-- 最后更新：2026-04

CREATE TABLE IF NOT EXISTS stocks (
    code        TEXT PRIMARY KEY,  -- 纯数字代码，如 600519
    full_code   TEXT,              -- 带前缀，如 sh600519
    name        TEXT,
    exchange    TEXT,              -- sh / sz
    updated_at  TEXT
);

CREATE TABLE IF NOT EXISTS income_stmt (
    code                    TEXT,
    report_date             TEXT,   -- 如 2024-09-30（累计值，非单季）
    operate_income          REAL,   -- 营业收入
    operate_cost            REAL,   -- 营业成本
    gross_margin            REAL,   -- 毛利率 %（预计算）
    operate_profit          REAL,   -- 营业利润
    netprofit               REAL,   -- 净利润
    parent_netprofit        REAL,   -- 归母净利润（归属于上市公司股东）
    deduct_parent_netprofit REAL,   -- 扣非归母净利润
    basic_eps               REAL,   -- 基本每股收益
    research_expense        REAL,   -- 研发费用
    sale_expense            REAL,   -- 销售费用
    manage_expense          REAL,   -- 管理费用
    finance_expense         REAL,   -- 财务费用
    fetched_at              TEXT,
    PRIMARY KEY (code, report_date)
);

CREATE TABLE IF NOT EXISTS balance_sheet (
    code                    TEXT,
    report_date             TEXT,
    monetary_fund           REAL,   -- 货币资金
    accounts_rece           REAL,   -- 应收账款
    inventory               REAL,   -- 存货
    total_current_assets    REAL,   -- 流动资产合计
    contract_liab           REAL,   -- 合同负债（新准则）
    advance_recv            REAL,   -- 预收账款（旧准则）
    short_loan              REAL,   -- 短期借款
    total_current_liab      REAL,   -- 流动负债合计
    long_loan               REAL,   -- 长期借款
    total_assets            REAL,   -- 总资产
    total_liab              REAL,   -- 总负债
    total_equity            REAL,   -- 所有者权益合计
    parent_equity           REAL,   -- 归母净资产
    goodwill                REAL,   -- 商誉
    fetched_at              TEXT,
    PRIMARY KEY (code, report_date)
);

CREATE TABLE IF NOT EXISTS cash_flow (
    code                TEXT,
    report_date         TEXT,
    netcash_operate     REAL,   -- 经营活动净现金流（OCF）
    construct_asset     REAL,   -- 购建固定资产（CAPEX）
    netcash_invest      REAL,   -- 投资活动净现金流
    netcash_finance     REAL,   -- 筹资活动净现金流
    netprofit_cf        REAL,   -- 现金流量表中的净利润（补充资料）
    fetched_at          TEXT,
    PRIMARY KEY (code, report_date)
);

CREATE TABLE IF NOT EXISTS market_snapshot (
    code            TEXT PRIMARY KEY,
    snap_date       TEXT,
    price           REAL,   -- 最新价（来自总市值/总股本）
    pe_ttm          REAL,   -- 市盈率 TTM（本地计算）
    pb              REAL,   -- 市净率（本地计算）
    total_mktcap    REAL,   -- 总市值（元）
    float_mktcap    REAL,   -- 流通市值（元）
    turnover_rate   REAL,   -- 换手率 %
    chg_pct_60d     REAL,   -- 60日涨跌幅 %
    chg_pct_ytd     REAL,   -- 年初至今涨跌幅 %
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS industry (
    code            TEXT PRIMARY KEY,
    industry_name   TEXT,   -- 东方财富行业分类（申万二级）
    updated_at      TEXT
);

CREATE TABLE IF NOT EXISTS fetch_log (
    code        TEXT,
    table_name  TEXT,       -- balance_sheet / income_stmt / cash_flow
    status      TEXT,       -- success / error / pending
    attempts    INTEGER DEFAULT 0,
    error_msg   TEXT,
    last_at     TEXT,
    PRIMARY KEY (code, table_name)
);

CREATE INDEX IF NOT EXISTS idx_log_status ON fetch_log (status);
