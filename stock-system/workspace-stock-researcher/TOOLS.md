# TOOLS.md - 工具路径速查

## 核心脚本

- **筛选脚本**：`/home/<user>/.openclaw/workspace-stock-researcher/skills/stock-research/screener.py`
  - 用法：`python3 screener.py`（正常筛选，几秒完成）
  - 用法：`python3 screener.py --api`（强制实时 API，约20分钟，覆盖不完整）

## 数据库（只读）

- **A股数据库**：`/home/<user>/.openclaw/workspace/data/astock.db`
  - 表：`stocks`, `balance_sheet`, `income_stmt`, `cash_flow`, `market_snapshot`, `executive_hold`, `industry`
  - code 字段：6位数字，不含 sh/sz 前缀

## 输出目录

- **筛选 JSON**：`/home/<user>/.openclaw/workspace/memory/screener_result_YYYY-MM-DD.json`
- **候选报告**：`/home/<user>/.openclaw/workspace/memory/stock-pick-YYYY-MM-DD.md`

## JSON 结构速查

```
{
  "date": "YYYY-MM-DD",
  "summary": { "total_market", "batch_checked", "qualified", "industries" },
  "industry_stats": [ { "industry", "count", "top_score", "avg_score" } ],
  "results": [                          ← 全部达标股票，按 composite_score 降序
    {
      "code", "name", "industry",
      "passed_count",                   ← 满足指标数（≥4 入选）
      "quant_score",                    ← 量化评分（0-100，权重75%）
      "exec_hold_score",                ← 高管增减持评分（0-100，权重25%）
      "composite_score",                ← 综合得分 = quant×0.75 + exec×0.25
      "b_class",                        ← 战略特例标签，null 表示 A 类
      "industry_rank",                  ← 所在行业在达标数排行中的位次
      "metrics": { pe_ttm, pb, price, contract_liability_yoy,
                   contract_liability_yoy_3q, ocf_ni_ratio, ocf_ni_ratio_3q,
                   operating_cashflow_yoy, operating_cashflow_yoy_3q,
                   gross_margin_improving, gross_margin_pass_ratio_3q, gross_margin_improvement_3q,
                   capex_yoy, capex_yoy_3q }
    }
  ]
}
```

## 数据新鲜度检查

```python
import sqlite3
conn = sqlite3.connect("/home/<user>/.openclaw/workspace/data/astock.db")
snap = conn.execute("SELECT MAX(snap_date) FROM market_snapshot").fetchone()[0]
exec_dt = conn.execute("SELECT MAX(cutoff_date) FROM executive_hold").fetchone()[0]
print(f"快照日期: {snap}  高管数据: {exec_dt}")
conn.close()
```

> 若快照日期 > 7天，在报告末尾提示总监安排 stock-fetcher 更新。
