-- A股财务数据库 schema
-- 用法: python3 build_db.py 会自动初始化，也可手动执行此文件

CREATE INDEX idx_bs_code   ON balance_sheet (code);

CREATE INDEX idx_cf_code   ON cash_flow     (code);

CREATE INDEX idx_inc_code  ON income_stmt   (code);

CREATE INDEX idx_log_status ON fetch_log    (status);

CREATE TABLE balance_sheet (
    code            TEXT,
    report_date     TEXT,          -- 如 2024-09-30
    contract_liab   REAL,          -- 合同负债（新准则）
    advance_recv    REAL,          -- 预收账款（旧准则）
    total_assets    REAL,
    total_liab      REAL,
    total_equity    REAL,
    fetched_at      TEXT, monetary_fund REAL, accounts_rece REAL, inventory REAL, total_current_assets REAL, short_loan REAL, total_current_liab REAL, long_loan REAL, parent_equity REAL, goodwill REAL,
    PRIMARY KEY (code, report_date)
);

CREATE TABLE cash_flow (
    code                TEXT,
    report_date         TEXT,
    netcash_operate     REAL,      -- 经营活动净现金流（OCF）
    construct_asset     REAL,      -- 购建固定资产（CAPEX，原始值为负）
    netprofit_cf        REAL,      -- 现金流量表中的净利润（补充资料）
    fetched_at          TEXT, netcash_invest REAL, netcash_finance REAL,
    PRIMARY KEY (code, report_date)
);

CREATE TABLE fetch_log (
    code        TEXT,
    table_name  TEXT,              -- balance_sheet / income_stmt / cash_flow
    status      TEXT,              -- success / error / skip
    attempts    INTEGER DEFAULT 0,
    error_msg   TEXT,
    last_at     TEXT,
    PRIMARY KEY (code, table_name)
);

CREATE TABLE income_stmt (
    code            TEXT,
    report_date     TEXT,
    operate_income  REAL,          -- 营业收入
    operate_cost    REAL,          -- 营业成本
    gross_margin    REAL,          -- 毛利率 %（预计算）
    netprofit       REAL,          -- 净利润
    fetched_at      TEXT, operate_profit REAL, parent_netprofit REAL, deduct_parent_netprofit REAL, basic_eps REAL, research_expense REAL, sale_expense REAL, manage_expense REAL, finance_expense REAL,
    PRIMARY KEY (code, report_date)
);

CREATE TABLE industry (
    code            TEXT PRIMARY KEY,
    industry_name   TEXT,
    updated_at      TEXT
);

CREATE TABLE market_snapshot (
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

CREATE TABLE stocks (
    code        TEXT PRIMARY KEY,  -- 纯数字代码，如 600519
    full_code   TEXT,              -- 带前缀，如 sh600519
    name        TEXT,
    exchange    TEXT,              -- sh / sz
    updated_at  TEXT
);

