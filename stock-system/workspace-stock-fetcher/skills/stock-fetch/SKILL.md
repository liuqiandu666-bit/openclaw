# 技能：A 股财务数据采集

数据来源：东方财富（AkShare）、巨潮资讯、baostock。

---

## Step 1：检查数据状态

```bash
python3 /home/<user>/.openclaw/workspace/data/build_db.py --status
```

再跑细查脚本确认各表最新报期：

```python
import sqlite3
DB = "/home/<user>/.openclaw/workspace/data/astock.db"
conn = sqlite3.connect(DB)

print("=== 财报最新报期 ===")
for t in ["income_stmt", "balance_sheet", "cash_flow"]:
    r = conn.execute(f"SELECT MAX(report_date) FROM {t}").fetchone()[0]
    print(f"  {t}: {r}")

snap = conn.execute("SELECT MAX(snap_date) FROM market_snapshot").fetchone()[0]
exec_dt = conn.execute("SELECT MAX(cutoff_date) FROM executive_hold").fetchone()[0]
print(f"\n  market_snapshot: {snap}")
print(f"  executive_hold:  {exec_dt}")

print("\n=== 抓取状态 ===")
for row in conn.execute("SELECT status, COUNT(*) FROM fetch_log GROUP BY status").fetchall():
    print(f"  {row[0]}: {row[1]}")
conn.close()
```

根据结果 + 下表判断是否需要更新：

| 报期   | report_date | 通常披露截止 |
| ------ | ----------- | ------------ |
| 年报   | 12-31       | 次年 4 月底  |
| 一季报 | 03-31       | 4 月底       |
| 半年报 | 06-30       | 8 月底       |
| 三季报 | 09-30       | 10 月底      |

若 `MAX(report_date)` 已是当前应有最新报期，财报无需更新。

---

## Step 2：按需执行

```bash
SCRIPT="python3 /home/<user>/.openclaw/workspace/data/build_db.py"
```

| 场景                       | 命令                                            | 耗时     |
| -------------------------- | ----------------------------------------------- | -------- |
| 重试失败                   | `$SCRIPT --retry`                               | 视失败数 |
| 季报/年报后全量重抓        | `$SCRIPT --refresh --workers 5`                 | ~37h ⚠️  |
| 市场快照（baostock，推荐） | `$SCRIPT --market-bs`                           | ~数分钟  |
| 市场快照（东方财富）       | `$SCRIPT --market`                              | ~33min   |
| 高管增减持                 | `$SCRIPT --executive-hold`                      | ~30s     |
| 行业分类（分批）           | `$SCRIPT --industry --limit 500 --interval 1.5` | ~数小时  |
| 全量首次建库               | `$SCRIPT --workers 5`                           | ~37h ⚠️  |

> ⚠️ 标记项必须后台运行，不要前台等待。

---

## Step 3：抽样验证（每次抓取后必做）

```python
import sqlite3, random
DB = "/home/<user>/.openclaw/workspace/data/astock.db"
conn = sqlite3.connect(DB)

codes = [r[0] for r in conn.execute(
    "SELECT DISTINCT code FROM income_stmt ORDER BY RANDOM() LIMIT 5"
).fetchall()]
print(f"抽样验证：{codes}\n")

issues = []
for code in codes:
    inc = conn.execute(
        "SELECT report_date, operate_income, parent_netprofit, gross_margin "
        "FROM income_stmt WHERE code=? ORDER BY report_date DESC LIMIT 1", (code,)
    ).fetchone()
    bs = conn.execute(
        "SELECT report_date, contract_liab, total_assets, total_equity "
        "FROM balance_sheet WHERE code=? ORDER BY report_date DESC LIMIT 1", (code,)
    ).fetchone()
    cf = conn.execute(
        "SELECT report_date, netcash_operate FROM cash_flow WHERE code=? ORDER BY report_date DESC LIMIT 1", (code,)
    ).fetchone()
    snap = conn.execute(
        "SELECT snap_date, price, pe_ttm, pb FROM market_snapshot WHERE code=?", (code,)
    ).fetchone()
    name = conn.execute("SELECT name FROM stocks WHERE code=?", (code,)).fetchone()

    label = f"{name[0] if name else '?'}（{code}）"
    print(f"── {label} ──")

    for tbl, val in [("income_stmt", inc), ("balance_sheet", bs), ("cash_flow", cf)]:
        if not val:
            issues.append(f"{code}: {tbl} 无数据")
            print(f"  ❌ {tbl} 无数据")

    if not all([inc, bs, cf]):
        print()
        continue

    print(f"  利润表  {inc[0]}: 营收={inc[1]:,.0f}  归母净利={inc[2]:,.0f}  毛利率={inc[3]}%")
    print(f"  资产负债{bs[0]}: 合同负债={bs[1]}  总资产={bs[2]:,.0f}  净资产={bs[3]:,.0f}")
    print(f"  现金流  {cf[0]}: 经营现金流={cf[1]:,.0f}")
    if snap:
        print(f"  快照    {snap[0]}: 价格={snap[1]}  PE={snap[2]}  PB={snap[3]}")

    # 合理性校验
    if inc[1] and inc[2] and inc[2] > inc[1]:
        issues.append(f"{code}: 净利润 > 营收，疑似字段错位")
        print("  ⚠️ 净利润 > 营收")
    if inc[3] and (inc[3] < -100 or inc[3] > 100):
        issues.append(f"{code}: 毛利率={inc[3]}% 超范围")
        print(f"  ⚠️ 毛利率异常")
    if bs[2] and bs[3] and bs[3] > bs[2]:
        issues.append(f"{code}: 净资产 > 总资产")
        print("  ⚠️ 净资产 > 总资产")

    dates = [inc[0], bs[0], cf[0]]
    if len(set(dates)) > 1:
        print(f"  ℹ️ 三表报期不一致: {dates}")
    print()

conn.close()
print("=" * 40)
if issues:
    print(f"❌ {len(issues)} 个问题：")
    for i in issues:
        print(f"  - {i}")
else:
    print("✅ 抽样验证通过")
```

---

## Step 4：汇报

⚠️ **必须先完成 Step 1–3，再执行本步骤。announce 必须是最后一个动作。**

先定义输出文件并准备写入：

```bash
OUT="/home/<user>/.openclaw/workspace/memory/stock-fetch-status-$(date +%F).md"
```

announce 内容**严格使用以下格式**，不得增删字段，不得省略任何行：

```
数据库更新完成（YYYY-MM-DD HH:MM）
  财报最新期：YYYY-MM-DD（Q?报）
  市场快照：YYYY-MM-DD（距今N天）
  高管增减持：YYYY-MM-DD
  本次是否执行更新：是（执行了XXX）/ 否（数据已是最新）
  覆盖股票：N 只（成功 N / 失败 N）
  抽样验证：✅ 通过 / ❌ 发现 N 个异常（见下） / ⚪ 未执行（原因：XXX）
  数据可供筛选：是 / 否（原因：XXX）
  结论：[数据正常，可供筛选使用 / 仍有 N 只失败建议 --retry / 后台抓取已启动，请稍后复查 / 执行失败，需人工复核]
```

**各字段填写规则：**

- `财报最新期`：`MAX(report_date)` 值，报期换算（03-31→Q1报，06-30→Q2报，09-30→Q3报，12-31→年报）
- `市场快照`：`MAX(snap_date)` + 计算距今天数
- `高管增减持`：`MAX(cutoff_date)`
- `覆盖股票`：从 `fetch_log` 表统计 `status='ok'` 和 `status='error'` 数量
- 如果本次没有执行任何更新（仅查看状态），「本次是否执行更新」写「否（数据已是最新）」

如果抽样验证发现异常，在格式末尾追加：

```
  异常详情：
    - [code] [问题描述]
```

**落盘 + 回传要求：**

1. 先把最终摘要完整写入 `$OUT`。示例：

```bash
cat > "$OUT" <<'EOF'
数据库更新完成（2026-04-16 09:30）
  财报最新期：2026-03-31（Q1报）
  市场快照：2026-04-16（距今0天）
  高管增减持：2026-04-15
  本次是否执行更新：否（数据已是最新）
  覆盖股票：5350 只（成功 5321 / 失败 29）
  抽样验证：✅ 通过
  数据可供筛选：是
  结论：数据正常，可供筛选使用
EOF
```

2. 最后一条 assistant 输出必须与 `$OUT` 内容逐字一致，不得附加解释，不得输出空字符串。
3. 如果出现以下任一情况，仍然必须生成“当前状态摘要”并写入 `$OUT`，然后原样输出：
   - 抓取脚本报错
   - 会话被中断或超时
   - `--refresh` / `--workers` 这类后台任务刚启动，还没跑完
4. 失败或进行中时，统一这样写：
   - `抽样验证：⚪ 未执行（原因：后台任务仍在运行 / 脚本报错）`
   - `数据可供筛选：否（原因：后台任务仍在运行 / 脚本执行失败）`
   - `结论：后台抓取已启动，请稍后复查` 或 `结论：执行失败，需人工复核`
