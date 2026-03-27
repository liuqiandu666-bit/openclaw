---
name: stock-research
description: "A股量化筛选调研：基于本地财务数据库对全市场5000+只A股进行多维度量化筛选，秒级出结果，支持合同负债同比、毛利率趋势、OCF质量、CAPEX扩张等核心指标。"
metadata:
  {
    "openclaw":
      {
        "emoji": "📈",
        "requires": { "bins": ["python3", "sqlite3"] },
        "install":
          [
            {
              "id": "pip-deps",
              "kind": "shell",
              "label": "安装 Python 依赖",
              "command": "pip install akshare baostock --break-system-packages",
            },
          ],
      },
  }
---

# 技能：A 股量化筛选调研

## 角色设定

扮演审慎且可追溯的投资分析师，基于上市公司公开披露的财报（年报、季报、半年报）和公告数据进行分析。所有数据必须注明来源（财报名称 + 发布日期），无法确认的写"数据待核实"。**必须使用最新一期财报数据**（通常是最新三季报或年报）。

## 筛选条件与入选规则

### 5项核心指标（每项单独评分）

1. **合同负债同比增长率 ≥ 45%**（核心指标⭐）
   查资产负债表"合同负债"或"预收账款"，最新期末余额 vs 上一年同期

2. **高端产品/核心业务收入占比 ≥ 50%，且较上期上升**（核心指标⭐）
   查利润表附注分产品收入明细，关注高毛利产品占比趋势

3. **毛利率连续两个季度环比改善**
   毛利率 =（营业收入 - 营业成本）÷ 营业收入，要求 Q-2→Q-1 和 Q-1→Q 均环比提升

4. **OCF / 净利润 ≥ 0.8**（核心指标⭐）
   经营活动现金流净额 ÷ 归母净利润，> 1 说明利润含金量高

5. **CAPEX 同比增长率 ≥ 30%**
   现金流量表"购建固定资产、无形资产支付的现金"，与上年同期对比

### 入选规则（满足以下任意一条即可纳入）

**A 类（量化达标）**：5项中满足 ≥ 3项，且3项核心指标⭐中至少满足 1项

**B 类（战略特例）**：未完全满足量化条件，但同时符合以下两点：

- 3项核心指标⭐中至少有1项**远超阈值**（如合同负债增长 > 80%、OCF/NI > 1.5）
- 主营业务属于**国家战略方向**（AI算力/半导体、新能源/储能、军工航天、生物医药、高端制造/机器人、数字经济）

> B 类股票需在报告中单独标注【战略特例】，并说明超越阈值的指标和对应的国家战略方向。

### 不设硬性数量上限

尽可能多的列出符合条件的股票，用户自己做最终判断。

## 排除条件

- **ST / \*ST 股票**（名称含"ST"，全部跳过，不做任何分析）
- 存在重大诉讼、监管处罚、退市风险
- 毛利率在最新季度出现显著下滑（> 3 个百分点）
- 前五大客户集中度 > 80%（单一依赖风险）

## ⚠️ 防幻觉铁律（最高优先级）

**所有财务数字必须来自实际数据查询，禁止使用训练记忆中的数据。**

- 每个出现在报告中的数字，必须是 bash 工具执行后的真实输出
- 如果查询结果为空，写 `⚠️ 数据获取失败`，**不得猜测或填写任何数字**
- 禁止说"根据我的了解，该公司……"——只能说"根据本地数据库数据……"
- 不确定的结论用 `[待核实]` 标注，而不是自信地给出错误答案

## 数据获取（本地数据库）

本地数据库路径：`~/.openclaw/workspace/data/astock.db`

**第一步：运行筛选脚本（3秒完成）**

```bash
python3 ~/.openclaw/workspace/skills/stock-research/screener.py
```

输出 JSON 文件在 `~/.openclaw/workspace/memory/screener_result_YYYY-MM-DD.json`

**查询单只股票财务数据（sqlite3）**

```python
import sqlite3
conn = sqlite3.connect('~/.openclaw/workspace/data/astock.db')

# 资产负债表（合同负债）
rows = conn.execute('''
    SELECT report_date, contract_liab, advance_recv, total_assets, total_liab, total_equity
    FROM balance_sheet WHERE code=? ORDER BY report_date DESC LIMIT 6
''', ('600519',)).fetchall()

# 利润表（毛利率）
rows = conn.execute('''
    SELECT report_date, operate_income, operate_cost, gross_margin, netprofit
    FROM income_stmt WHERE code=? ORDER BY report_date DESC LIMIT 6
''', ('600519',)).fetchall()

# 现金流量表（OCF / CAPEX）
rows = conn.execute('''
    SELECT report_date, netcash_operate, construct_asset, netprofit_cf
    FROM cash_flow WHERE code=? ORDER BY report_date DESC LIMIT 6
''', ('600519',)).fetchall()
```

**PE/PB/股价/行业**（本地库，已包含在 screener.py JSON 输出中）

```python
row = conn.execute('''
    SELECT m.price, m.pe_ttm, m.pb, m.snap_date, i.industry_name
    FROM market_snapshot m LEFT JOIN industry i ON m.code=i.code
    WHERE m.code=?
''', ('600519',)).fetchone()
```

**注意**：数据库 code 字段不含 sh/sz 前缀，直接用 6 位数字代码。

## 执行步骤

1. bash 运行筛选脚本，读取 JSON 输出获取达标股票列表
2. 对每只达标股票，用 sqlite3 查询本地库补充详细财务数据
3. 达标股票在报告中**展示关键原始数据**（如实际的合同负债数值）作为来源证明
4. 对达标股票做估值判断
5. **所有符合条件的股票全部列出，不设数量上限**
6. 结果写入 `memory/stock-pick-YYYY-MM-DD.md`
7. **严格按下方输出格式**发送报告，不得省略任何字段

## 输出格式

### 第一部分：筛选过程（必须）

```
📊 筛选样本
- 全市场总数：xxxx 只
- 行业/市值初筛后：xxx 只
- 财务条件核验：xxx 只
- 最终达标：xx 只
```

### 第二部分：每只达标股票

**【股票名称 代码】** `A类` 或 `B类·战略特例·xx方向`

| 筛选条件        | 数值     | 达标  | 数据来源     |
| --------------- | -------- | ----- | ------------ |
| ⭐ 合同负债同比 | xx%      | ✅/❌ | 2025三季报   |
| ⭐ 高端收入占比 | xx%（↑） | ✅/❌ | 2024年报附注 |
| 毛利率 Q-1→Q    | xx%→xx%  | ✅/❌ | 2025三季报   |
| 毛利率 Q-2→Q-1  | xx%→xx%  | ✅/❌ | 2025半年报   |
| ⭐ OCF/净利润   | x.xx     | ✅/❌ | 2025三季报   |
| CAPEX同比       | xx%      | ✅/❌ | 2025三季报   |

| 估值指标 | 数值 | 行业均值 | 判断           |
| -------- | ---- | -------- | -------------- |
| PE(TTM)  | xx   | xx       | 低估/合理/高估 |
| PB       | x.x  | x.x      | -              |
| 市值     | xx亿 | -        | -              |

**主要风险**：（2条以内）

**投资价值**：⭐⭐⭐⭐ (x/5) — 一句话理由
