# A股量化筛选调研

基于本地 SQLite 数据库对全市场 5000+ 只 A 股进行量化筛选，**3秒出结果**，无需依赖外部 API 实时请求。

## 功能

- 全市场 5000+ 只股票多维度量化筛选
- 合同负债同比、毛利率趋势、OCF质量、CAPEX扩张等核心指标
- PE/PB/行业分类一并输出
- 配套深度调研技能（`stock-deep-analysis`）做估值建模

## 文件说明

| 文件          | 说明                                 |
| ------------- | ------------------------------------ |
| `SKILL.md`    | 筛选技能提示词（发给 openclaw 使用） |
| `screener.py` | 筛选脚本，3秒跑完全市场              |
| `build_db.py` | 数据库构建/更新脚本                  |

## 快速开始

### 1. 安装依赖

```bash
pip install akshare baostock --break-system-packages
```

### 2. 建立本地数据库（首次使用，约需 5-8 小时）

```bash
# 建库（全量抓取财务报表，5线程并发）
python3 build_db.py

# 同时抓取市场快照（PE/PB/股价）
python3 -c "
import baostock as bs, sqlite3
from datetime import datetime, date, timedelta

DB = 'astock.db'   # 替换为实际路径
conn = sqlite3.connect(DB)
stocks = conn.execute('SELECT code, full_code FROM stocks').fetchall()
lg = bs.login()
snap_date = date.today()
start = (snap_date - timedelta(days=7)).isoformat()
end = snap_date.isoformat()
rows = []
now = datetime.now().isoformat()
for code, full_code in stocks:
    bs_code = full_code[:2].lower() + '.' + full_code[2:]
    rs = bs.query_history_k_data_plus(bs_code, 'date,close,peTTM,pbMRQ',
        start_date=start, end_date=end, frequency='d', adjustflag='3')
    data = []
    while rs.error_code == '0' and rs.next(): data.append(rs.get_row_data())
    if data:
        d = data[-1]
        def f(v):
            try: return float(v) if v else None
            except: return None
        rows.append((code, d[0], f(d[1]), f(d[2]), f(d[3]), None,None,None,None,None, now))
conn.executemany('INSERT OR REPLACE INTO market_snapshot '
    '(code,snap_date,price,pe_ttm,pb,total_mktcap,float_mktcap,turnover_rate,chg_pct_60d,chg_pct_ytd,updated_at) '
    'VALUES (?,?,?,?,?,?,?,?,?,?,?)', rows)
conn.commit()
bs.logout()
print(f'完成：{len(rows)} 只')
"

# 抓取行业分类（一次完成，无需重复）
python3 -c "
import baostock as bs, sqlite3
from datetime import datetime
conn = sqlite3.connect('astock.db')   # 替换为实际路径
lg = bs.login()
rs = bs.query_stock_industry()
data = []
while rs.error_code == '0' and rs.next(): data.append(rs.get_row_data())
bs.logout()
now = datetime.now().isoformat()
rows = [(r[1][3:], r[3], now) for r in data if r[3].strip() and len(r[1]) == 9]
conn.executemany('INSERT OR REPLACE INTO industry (code,industry_name,updated_at) VALUES (?,?,?)', rows)
conn.commit()
print(f'行业分类写入：{len(rows)} 只')
"
```

### 3. 运行筛选

```bash
python3 screener.py
# 输出：~/.openclaw/workspace/memory/screener_result_YYYY-MM-DD.json
```

### 4. 查看进度与状态

```bash
python3 build_db.py --status
```

## 数据更新

| 命令                              | 用途             | 频率建议                 |
| --------------------------------- | ---------------- | ------------------------ |
| `python3 build_db.py --market-bs` | 更新PE/PB/股价   | 每个交易日收盘后         |
| `python3 build_db.py --refresh`   | 重抓所有财务报表 | 每季报告季（3/4/8/10月） |
| `python3 build_db.py --retry`     | 重试失败的股票   | 建库后补跑               |

## 迁移到新机器

只需拷贝以下文件：

```
~/.openclaw/workspace/data/astock.db          # 主数据库（~25MB）
~/.openclaw/workspace/data/build_db.py        # 本文件
~/.openclaw/workspace/skills/stock-research/screener.py
~/.openclaw/workspace/skills/stock-research/SKILL.md
~/.openclaw/workspace/skills/stock-deep-analysis/SKILL.md
```

拷贝后在新机器上安装依赖即可，无需重新建库。

## 定时自动更新（openclaw cron）

在 `~/.openclaw/cron/jobs.json` 中添加以下任务，openclaw 会自动执行：

```json
{
  "name": "A股市场快照每日刷新",
  "schedule": { "kind": "cron", "expr": "13 18 * * 1-5", "tz": "Asia/Shanghai" },
  "payload": {
    "kind": "agentTurn",
    "message": "运行 python3 ~/.openclaw/workspace/data/build_db.py --market-bs，完成后告诉我快照只数和日期。",
    "model": "deepseek/deepseek-chat"
  }
},
{
  "name": "A股财报季增量更新",
  "schedule": { "kind": "cron", "expr": "7 3 * 3,4,8,10 *", "tz": "Asia/Shanghai" },
  "payload": {
    "kind": "agentTurn",
    "message": "运行 python3 ~/.openclaw/workspace/data/build_db.py --refresh --workers 5，完成后汇报结果。",
    "model": "deepseek/deepseek-chat"
  }
}
```

## 数据库表结构

| 表                | 内容                               |
| ----------------- | ---------------------------------- |
| `stocks`          | 股票基本信息（代码、名称、交易所） |
| `balance_sheet`   | 资产负债表（最近8期）              |
| `income_stmt`     | 利润表（最近8期）                  |
| `cash_flow`       | 现金流量表（最近8期）              |
| `market_snapshot` | 每日快照（股价/PE/PB）             |
| `industry`        | 证监会行业分类                     |
| `fetch_log`       | 抓取状态记录（支持断点续跑）       |

## 筛选指标说明

| 指标             | 阈值            | 数据来源   |
| ---------------- | --------------- | ---------- |
| 合同负债同比增长 | ≥ 45%           | 资产负债表 |
| 毛利率连续改善   | 连续2季环比提升 | 利润表     |
| OCF / 净利润     | ≥ 0.8           | 现金流量表 |
| CAPEX 同比增长   | ≥ 30%           | 现金流量表 |
