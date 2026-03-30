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
- PE < 0（亏损股，screener 已标注 pe_warning）
- PE > 150（极端泡沫估值，除非有特别说明）

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

### 第二步：对 top_candidates 逐一补充详细财务数据

```python
import sqlite3
DB = "/home/liuqi/.openclaw/workspace/data/astock.db"
conn = sqlite3.connect(DB)

code = "600519"  # 6位数字，不含 sh/sz 前缀

# 资产负债表（合同负债）
bs = conn.execute("""
    SELECT report_date, contract_liab, advance_recv, total_assets, total_liab, total_equity
    FROM balance_sheet WHERE code=? ORDER BY report_date DESC LIMIT 6
""", (code,)).fetchall()

# 利润表（毛利率、净利润）
inc = conn.execute("""
    SELECT report_date, operate_income, operate_cost, gross_margin, netprofit
    FROM income_stmt WHERE code=? ORDER BY report_date DESC LIMIT 6
""", (code,)).fetchall()

# 现金流量表（OCF / CAPEX）
cf = conn.execute("""
    SELECT report_date, netcash_operate, construct_asset
    FROM cash_flow WHERE code=? ORDER BY report_date DESC LIMIT 6
""", (code,)).fetchall()

# 市场快照（股价 / PE / PB）
snap = conn.execute("""
    SELECT m.price, m.pe_ttm, m.pb, m.snap_date, i.industry_name
    FROM market_snapshot m LEFT JOIN industry i ON m.code=i.code
    WHERE m.code=?
""", (code,)).fetchone()

for r in bs:  print("BS :", r)
for r in inc: print("INC:", r)
for r in cf:  print("CF :", r)
print("MKT:", snap)
```

> **注意**：数据库 code 字段不含 sh/sz 前缀，直接用 6 位数字代码。

### 第三步：生成报告并写入 memory

- 格式严格按下方输出格式
- 完整报告写入 `/home/liuqi/.openclaw/workspace/memory/stock-pick-YYYY-MM-DD.md`（日期用今日实际日期）
- 所有符合条件的 top_candidates 全部列出，不设数量上限

## 输出格式

### 第一部分：筛选概况（必须）

```
📊 筛选概况（数据截至 YYYY-MM-DD）
- 全市场总数：xxxx 只
- 量化达标总数：xxx 只（4项中≥3项）
- 精选候选（行业分散Top）：xx 只
- 其中 B 类战略特例：x 只
```

### 第二部分：每只股票分析

**【股票名称 代码】** `评分:xx` — `A类` 或 `🌟B类·战略特例·xx方向`

| 指标            | 数值        | 达标  | 来源报期           |
| --------------- | ----------- | ----- | ------------------ |
| ⭐ 合同负债同比 | xx%         | ✅/❌ | 最新期 report_date |
| 毛利率趋势      | xx%→xx%→xx% | ✅/❌ | 连续3期            |
| ⭐ OCF/净利润   | x.xx        | ✅/❌ | 最新期             |
| CAPEX同比       | xx%         | ✅/❌ | 最新期 vs 同期     |

| 估值         | 数值 | 判断              |
| ------------ | ---- | ----------------- |
| PE(TTM)      | xx   | 低/合理/偏高/极高 |
| PB           | x.x  | -                 |
| 合同负债规模 | xx亿 | -                 |

**主要风险**：（1-2条）

**综合评分**：composite_score=xx/100 — 一句话结论

---

### 第三部分：汇总对比表（必须）

| 排名 | 股票      | 行业 | 评分 | PE  | 合同负债同比 | OCF/NI | 类别 | 结论 |
| ---- | --------- | ---- | ---- | --- | ------------ | ------ | ---- | ---- |
| 1    | xx (代码) | xx   | xx   | xx  | xx%          | x.xx   | A/B  | xxx  |
