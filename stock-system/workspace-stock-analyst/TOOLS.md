# TOOLS.md - Local Notes

## 数据库

- **A股数据库**：`/home/<user>/.openclaw/workspace/data/astock.db`
- code 字段：6位数字，不含 sh/sz 前缀
- **查询方式：只能用 `python3 -c "..."` 或 `python3 script.py`，严禁使用 `sqlite3` 命令行工具**

### 表结构（列名必须完全匹配，不得猜测）

| 表名              | 关键列                                                                                                                              |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `market_snapshot` | `code`, `snap_date`, `price`, `pe_ttm`, `pb`, `total_mktcap`, `float_mktcap`                                                        |
| `income_stmt`     | `code`, `report_date`, `operate_income`, `operate_cost`, `gross_margin`, `netprofit`, `parent_netprofit`, `basic_eps`               |
| `balance_sheet`   | `code`, `report_date`, `contract_liab`, `advance_recv`, `total_assets`, `total_liab`, `total_equity`, `long_loan`, `parent_equity`  |
| `cash_flow`       | `code`, `report_date`, `netcash_operate`, `construct_asset`, `netprofit_cf`                                                         |
| `executive_hold`  | `code`, `announce_date`, `cutoff_date`, `person_name`, `person_role`, `change_type`, `shares_changed`, `avg_price`, `change_reason` |

⚠️ `executive_hold` 无 `date` 列，排序用 `cutoff_date`；`market_snapshot` 无 `report_date`，日期列为 `snap_date`。

## 输出目录

- **分析报告**：`/home/<user>/.openclaw/workspace/memory/stock-deep-YYYY-MM-DD.md`
- **筛选结果**（只读）：`/home/<user>/.openclaw/workspace/memory/screener_result_YYYY-MM-DD.json`

## Web 工具就绪检查

- 会话开始先运行：`python3 /home/<user>/.openclaw/workspace-stock-analyst/check_web_tools.py`
- 返回 `WEB_SEARCH_READY`：可以正常使用 `web_search`
- 返回 `WEB_SEARCH_FALLBACK_AVAILABLE`：环境里可能已有 `PERPLEXITY_API_KEY` 或 `OPENROUTER_API_KEY`，但当前默认 provider 不是它
- 返回 `WEB_SEARCH_UNAVAILABLE`：本次会话不要再尝试 `web_search`，直接按 AGENTS.md 的降级流程输出“未核实/无法评估”
