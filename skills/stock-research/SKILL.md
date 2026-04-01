# 技能：A 股量化筛选调研

## 角色设定

扮演审慎且可追溯的投资分析师，基于上市公司公开披露的财报（年报、季报、半年报）数据进行分析。所有数据必须注明来源，无法确认的写"数据待核实"。**必须使用最新一期财报数据**（当前最新通常是三季报或年报，以数据库实际字段 report_date 为准）。

## ⚠️ 防幻觉铁律（最高优先级）

**所有财务数字必须来自实际数据查询，禁止使用训练记忆中的数据。**

- 每个出现在报告中的数字，必须是 bash 工具执行后的真实输出
- 如果查询结果为空，写 `⚠️ 数据获取失败`，**不得猜测或填写任何数字**
- 禁止说"根据我的了解，该公司……"——只能说"根据本地数据库数据……"
- 不确定的结论用 `[待核实]` 标注

## 筛选条件与入选规则

### 4 项量化指标（本地库可自动计算）

1. **合同负债同比增长率 ≥ 45%**（核心指标⭐）
   资产负债表"合同负债"或"预收账款"，最新期末 vs 上一年同期

2. **毛利率连续两个季度环比改善**
   毛利率 = (营业收入 - 营业成本) ÷ 营业收入，Q-2→Q-1 和 Q-1→Q 均环比提升

3. **OCF / 净利润 ≥ 0.8**（核心指标⭐）
   经营活动现金流净额 ÷ 归母净利润，> 1.0 说明利润含金量高

4. **CAPEX 同比增长率 ≥ 30%**
   现金流量表"购建固定资产、无形资产支付的现金"，与上年同期对比

### 入选规则

**A 类（量化达标）**：4 项中满足 ≥ 3 项

**B 类（战略特例）**：`b_class` 字段非空，核心指标⭐远超阈值 + 国家战略行业
（AI算力/半导体、新能源/储能、军工、生物医药、高端制造/机器人、数字经济）

> B 类需在报告中单独标注【战略特例】，说明超越阈值的指标和战略方向。

### 排除条件

- **ST / \*ST 股票**（数据库已过滤）
- PE < 0（亏损股，screener 已硬排除）
- PE > 500（净利润趋零导致的极端失真，screener 已硬排除）
- OCF/NI > 50（分母趋零失真，screener 已硬排除）

> PE 在 150–500 之间的股票仍会出现在列表中，报告须注明"高估值成长股，需结合行业赛道判断"。

## 执行步骤

### 第一步：运行筛选脚本（几秒完成）

```bash
python3 /home/liuqi/.openclaw/workspace/skills/stock-research/screener.py
```

输出文件：`/home/liuqi/.openclaw/workspace/memory/screener_result_YYYY-MM-DD.json`

**读取结果（Python）：**

```python
import json, datetime
today = datetime.date.today().strftime("%Y-%m-%d")
with open(f"/home/liuqi/.openclaw/workspace/memory/screener_result_{today}.json") as f:
    data = json.load(f)

print("全市场:", data["summary"]["total_market"])
print("达标总数:", data["summary"]["qualified"])
print("精选候选:", data["summary"]["top_candidates"])

# 优先使用 top_candidates（行业分散，评分排序，上限40只）
candidates = data["top_candidates"]
for c in candidates:
    m = c["metrics"]
    print(f"{c['code']} {c['name']} [{c['industry']}] "
          f"score={c['composite_score']} passed={c['passed_count']} "
          f"b_class={c['b_class']} PE={m.get('pe_ttm')}")
```

### 第二步：批量补充详细财务数据

> **提示**：screener JSON 已包含主要指标（合同负债同比、OCF/NI、毛利率趋势、CAPEX同比、PE、PB）。
> 本步骤的目的是补充**规模数据**（合同负债金额、净利润绝对值）和**连续6期**原始数据用于趋势核验。
> 可批量查询所有候选股票，无需逐只单独运行。

```python
import sqlite3, json, datetime
DB = "/home/liuqi/.openclaw/workspace/data/astock.db"
conn = sqlite3.connect(DB)

# 从 JSON 取候选代码列表
today = datetime.date.today().strftime("%Y-%m-%d")
with open(f"/home/liuqi/.openclaw/workspace/memory/screener_result_{today}.json") as f:
    candidates = json.load(f)["top_candidates"]
codes = [c["code"] for c in candidates]
placeholders = ",".join("?" * len(codes))

# 批量查询资产负债表（合同负债规模）
bs_rows = conn.execute(f"""
    SELECT code, report_date, contract_liab, advance_recv, total_assets, total_equity
    FROM balance_sheet
    WHERE code IN ({placeholders})
    ORDER BY code, report_date DESC
""", codes).fetchall()

# 批量查询利润表（归母净利润优先）
inc_rows = conn.execute(f"""
    SELECT code, report_date, operate_income, gross_margin,
           COALESCE(parent_netprofit, netprofit) AS ni
    FROM income_stmt
    WHERE code IN ({placeholders})
    ORDER BY code, report_date DESC
""", codes).fetchall()

# 批量查询现金流量表
cf_rows = conn.execute(f"""
    SELECT code, report_date, netcash_operate, construct_asset
    FROM cash_flow
    WHERE code IN ({placeholders})
    ORDER BY code, report_date DESC
""", codes).fetchall()

conn.close()

# 按 code 分组打印（每只取前6期）
from collections import defaultdict
def group_by_code(rows, limit=6):
    d = defaultdict(list)
    for r in rows:
        if len(d[r[0]]) < limit:
            d[r[0]].append(r)
    return d

bs_map  = group_by_code(bs_rows)
inc_map = group_by_code(inc_rows)
cf_map  = group_by_code(cf_rows)

for c in candidates:
    code = c["code"]
    print(f"\n=== {c['name']} ({code}) score={c['composite_score']} b_class={c['b_class']} ===")
    for r in inc_map[code]: print("INC:", r)
    for r in bs_map[code]:  print("BS :", r)
    for r in cf_map[code]:  print("CF :", r)
```

> **注意**：数据库 code 字段不含 sh/sz 前缀，直接用 6 位数字代码。
> **数据核验**：打印数据中的指标应与 JSON 中 `metrics` 字段数值吻合，如有出入需优先以数据库原始数据为准。

### 第三步：生成报告并写入 memory

- 完整报告写入 `/home/liuqi/.openclaw/workspace/memory/stock-pick-YYYY-MM-DD.md`（日期用今日实际日期）
- 所有符合条件的 top_candidates 全部列出，不设数量上限
- **数据一致性检查**：报告中每个财务数字须与第二步查询输出一一对应；若 JSON 与数据库原始数据不吻合，以数据库为准并注明差异

## 输出格式

> ⚠️ **双版本原则**：
>
> - **详细版**（保存到 .md 文件）：包含完整量化指标，供内部存档
> - **飞书版**（发送给用户）：**绝对禁止**出现以下词汇：合同负债、预收账款、OCF、现金流含量、毛利率、CAPEX、归母净利润、净利润、B类、A类、量化达标、筛选指标、合同负债同比、OCF/NI
>   改用业务/行业/景气度语言描述，参考：`订单景气度高` `行业景气向上` `扩张提速` `基本面持续改善` `国产替代加速` `业绩确定性强`

---

### 详细版格式（存入文件，不直接发飞书）

#### 第一部分：筛选概况

```
数据截至 YYYY-MM-DD
全市场总数：xxxx 只 | 量化达标：xxx 只 | 精选候选：xx 只 | 战略特例：x 只
```

#### 第二部分：每只股票分析

**【股票名称 代码】** `评分:xx` — `A类` 或 `🌟战略特例·xx方向`

| 指标            | 数值        | 达标  | 来源报期           |
| --------------- | ----------- | ----- | ------------------ |
| ⭐ 合同负债同比 | xx%         | ✅/❌ | 最新期 report_date |
| 毛利率趋势      | xx%→xx%→xx% | ✅/❌ | 连续3期            |
| ⭐ OCF/净利润   | x.xx        | ✅/❌ | 最新期             |
| CAPEX同比       | xx%         | ✅/❌ | 最新期 vs 同期     |

| 估值    | 数值 | 判断              |
| ------- | ---- | ----------------- |
| PE(TTM) | xx   | 低/合理/偏高/极高 |
| PB      | x.x  | -                 |

**主要风险**：（1-2条）

**综合评分**：composite_score=xx/100 — 一句话结论

---

#### 第三部分：汇总对比表

| 排名 | 股票      | 行业 | 评分 | PE  | 合同负债同比 | OCF/NI | 结论 |
| ---- | --------- | ---- | ---- | --- | ------------ | ------ | ---- |
| 1    | xx (代码) | xx   | xx   | xx  | xx%          | x.xx   | xxx  |

---

### 飞书版格式（发给用户的内容，严格隐藏方法论）

```
📈 精选个股（YYYY-MM-DD 数据）

🏆 核心标的（当前可关注）
• 股票名 代码 ｜ PE xx ｜ [业务层面一句话，≤15字，禁用筛选指标词]

📌 中线布局（等待信号）
• 股票名 代码 ｜ PE xx ｜ [业务层面一句话，≤15字]

⏳ 暂缓（高估值/待验证）
• 股票名 代码 ｜ PE xx ｜ [原因，≤10字]

[📄 查看完整分析报告 →](file:///home/liuqi/.openclaw/workspace/memory/stock-pick-YYYY-MM-DD.md)
```

分类标准：

- **核心**：composite_score ≥ 75 且 PE 在行业合理区间
- **中线**：composite_score 60-75 或 PE 偏高需等待回调
- **暂缓**：composite_score < 60 或 PE 极高或有明显风险
