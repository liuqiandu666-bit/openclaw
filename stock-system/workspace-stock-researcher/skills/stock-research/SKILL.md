# 技能：A 股量化筛选

## 角色设定

扮演审慎且可追溯的投资分析师，基于上市公司公开披露的财报（年报、季报、半年报）数据进行量化筛选。所有数据必须注明来源，无法确认的写"数据待核实"。**必须使用最新一期财报数据**（当前最新通常是三季报或年报，以数据库实际字段 report_date 为准）。

## ⚠️ 防幻觉铁律（最高优先级）

**所有财务数字必须来自实际数据查询，禁止使用训练记忆中的数据。**

**绝对禁止修改任何 `.py` 脚本文件。** 脚本报错时只报告错误，不得自行修改脚本。

- 每个出现在报告中的数字，必须是 bash 工具执行后的真实输出
- 如果查询结果为空，写 `⚠️ 数据获取失败`，**不得猜测或填写任何数字**
- 禁止说"根据我的了解，该公司……"——只能说"根据本地数据库数据……"
- 不确定的结论用 `[待核实]` 标注
- **严禁使用 `sqlite3` 命令行工具**；数据库查询只允许运行现成 `python3` 脚本或 `python3 -c "..."` 片段

## 筛选条件与入选规则

> 完整的指标定义、阈值和入选规则见 `AGENTS.md` 的「量化筛选指标」章节。
> 本技能文件聚焦执行步骤和代码，不重复定义指标。
> 当前筛选口径为最近三季综合判断：最新季权重 41.2%，前两季各 29.4%。

### 排除条件

- **ST / \*ST 股票**（数据库已过滤）
- PE < 0（亏损股，screener 已硬排除）
- PE > 500（净利润趋零导致的极端失真，screener 已硬排除）
- OCF/NI > 50（分母趋零失真，screener 已硬排除）

> PE 在 150–500 之间的股票仍会出现在列表中，报告须注明"高估值成长股，需结合行业赛道判断"。

## 执行步骤

### 第一步：运行筛选脚本（几秒完成）

```bash
python3 /home/<user>/.openclaw/workspace-stock-researcher/skills/stock-research/screener.py
```

输出文件：`/home/<user>/.openclaw/workspace/memory/screener_result_YYYY-MM-DD.json`

**禁止的读取方式：**

- 禁止使用 `jq`（当前环境可能未安装）
- 禁止使用带 `for` 循环或复杂 f-string 的单行 `python3 -c "..."` 临时解析 JSON
- 禁止为了“先看看前几名”而手写临时 shell 解析；优先使用下面现成脚本
- 禁止使用 `sqlite3` 命令行直接查库

**优先使用的现成脚本：**

```bash
# 快速查看前30名候选及批量明细
python3 /home/<user>/.openclaw/workspace-stock-researcher/step2_query.py

# 查看前几只样例的近4期财务明细（调试/抽查用）
python3 /home/<user>/.openclaw/workspace-stock-researcher/query_details.py
```

**读取结果（Python）：**

```python
import json, datetime
today = datetime.date.today().strftime("%Y-%m-%d")
with open(f"/home/<user>/.openclaw/workspace/memory/screener_result_{today}.json") as f:
    data = json.load(f)

print("全市场:", data["summary"]["total_market"])
print("达标总数:", data["summary"]["qualified"])
print("涉及行业:", data["summary"]["industries"])

# 行业统计（按达标数量降序）
for stat in data["industry_stats"][:10]:
    print(f"  [{stat['industry']}] 达标{stat['count']}只 最高分={stat['top_score']} 均分={stat['avg_score']}")

# 全部达标股票（按综合得分降序）
for c in data["results"][:20]:
    m = c["metrics"]
    print(f"{c['code']} {c['name']} [{c['industry']}] "
          f"quant={c['quant_score']} exec={c['exec_hold_score']} "
          f"composite={c['composite_score']} passed={c['passed_count']} "
          f"b_class={c['b_class']} PE={m.get('pe_ttm')}")
```

### 第二步：批量补充详细财务数据

> **提示**：screener JSON 已包含主要指标（合同负债同比、OCF/NI、经营现金流净流入净额同比、毛利率趋势、CAPEX同比、PE、PB）。
> 本步骤的目的是补充**规模数据**（合同负债金额、净利润绝对值）和**连续6期**原始数据用于趋势核验。
> 可批量查询所有候选股票，无需逐只单独运行。
> 若只是执行标准日报流程，**优先直接运行现成脚本**，不要临时手写查询：

```bash
python3 /home/<user>/.openclaw/workspace-stock-researcher/step2_query.py
```

```python
import sqlite3, json, datetime
DB = "/home/<user>/.openclaw/workspace/data/astock.db"
conn = sqlite3.connect(DB)

# 从 JSON 取前30名候选代码列表
today = datetime.date.today().strftime("%Y-%m-%d")
with open(f"/home/<user>/.openclaw/workspace/memory/screener_result_{today}.json") as f:
    data = json.load(f)
results = sorted(
    data["results"],
    key=lambda item: (
        -(item.get("composite_score") or -9999),
        -(item.get("quant_score") or -9999),
        -(item.get("exec_hold_score") or -9999),
        item.get("code") or "",
    ),
)
candidates = results[:30]
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

---

### 第三步：生成详细版报告并写入 memory

- 标准流程优先直接运行：

```bash
python3 /home/<user>/.openclaw/workspace-stock-researcher/generate_report.py
```

- 完整报告写入 `/home/<user>/.openclaw/workspace/memory/stock-pick-YYYY-MM-DD.md`（日期用今日实际日期）
- **数据一致性检查**：报告中每个财务数字须与第二步查询输出一一对应；若 JSON 与数据库原始数据不吻合，以数据库为准并注明差异
- **不生成飞书简约版**——飞书版由 stock-director 根据本报告和 JSON 数据自行组织

### 第四步：生成 announce（必须使用脚本）

```bash
python3 /home/<user>/.openclaw/workspace-stock-researcher/build_announce.py --write-file
cat /home/<user>/.openclaw/workspace-stock-researcher/announce_output.txt
```

- announce 文本必须逐字使用脚本输出
- 脚本已内置与 `stock-director` 一致的前5排序键，禁止手工拼接前5
- 禁止使用 `python -c` 一行命令临时拼接 announce，避免语法错误和口径漂移

## 详细版报告格式

> 此为唯一输出格式。完成后按 `AGENTS.md` 定义的 announce 格式汇报给总监。

### 第一部分：筛选概况

```
数据截至 YYYY-MM-DD
全市场总数：xxxx 只 | 量化达标：xxx 只 | 涉及行业：xx 个
```

### 第二部分：行业分布

| 行业 | 达标数量 | 最高分 | 均分 |
| ---- | -------- | ------ | ---- |
| xx   | xx 只    | xx     | xx   |

### 第三部分：每只股票量化指标详情（前30名）

**【股票名称 代码】** `综合评分:xx` — `A类` 或 `🌟战略特例·xx方向`

| 指标                        | 数值                            | 达标  | 来源报期           |
| --------------------------- | ------------------------------- | ----- | ------------------ |
| ⭐ 合同负债同比             | 最新xx% / 三季加权xx%           | ✅/❌ | 最新期 report_date |
| 毛利率趋势                  | 最新xx%→xx%→xx% / 三季达标率xx% | ✅/❌ | 连续3期            |
| ⭐ OCF/净利润               | 最新x.xx / 三季加权x.xx         | ✅/❌ | 最新期             |
| ⭐ 经营现金流净流入净额同比 | 最新xx% / 三季加权xx%           | ✅/❌ | 最新期 vs 上年同期 |
| CAPEX同比                   | 最新xx% / 三季加权xx%           | ✅/❌ | 最新期 vs 同期     |

| 估值    | 数值 | 判断              |
| ------- | ---- | ----------------- |
| PE(TTM) | xx   | 低/合理/偏高/极高 |
| PB      | x.x  | -                 |

| 高管增减持（365天） | 净增持/净减持 | 行为评分 xx/100 |

**主要风险**：（1-2 条，纯客观描述，不做入场建议或目标价预测）

**综合评分**：

- 量化得分（75%）：quant_score=xx
- 高管行为（25%）：exec_hold_score=xx
- **最终：composite_score=xx/100** — 一句话客观概括（如"高景气+低估值"或"高成长但估值偏高"，不做买卖建议）

---

### 第四部分：汇总对比表

| 排名 | 股票      | 行业 | 量化(75%) | 高管(25%) | 综合 | PE  | 客观概括 |
| ---- | --------- | ---- | --------- | --------- | ---- | --- | -------- |
| 1    | xx (代码) | xx   | xx        | xx        | xx   | xx  | xxx      |
