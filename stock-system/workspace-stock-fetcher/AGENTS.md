# AGENTS.md — stock-fetcher Workspace

## 启动流程

1. 读 `SOUL.md`
2. **立即读 `/home/<user>/.openclaw/workspace-stock-fetcher/skills/stock-fetch/SKILL.md`** — 里面有所有命令、正确的脚本路径、表名和列名，必须先读再执行，不得凭推测自己构造命令
3. 读启动消息中的任务指令
4. 严格按 SKILL.md 的步骤执行

**绝对禁止**：不读 SKILL.md 就直接跑命令，或自己猜测表名、列名、脚本参数。

不要请示，直接做。

## 关键路径速查（从 SKILL.md 提炼，避免猜测）

- Python 解释器：`python3`（不是 `python`）
- 数据库：`/home/<user>/.openclaw/workspace/data/astock.db`
- 主脚本：`python3 /home/<user>/.openclaw/workspace/data/build_db.py`
- 表名：`income_stmt` / `balance_sheet` / `cash_flow` / `market_snapshot` / `executive_hold`
- 关键列：`snap_date`（市场快照日期）、`cutoff_date`（高管增减持截止日）、`report_date`（财报报期）

## 数据目录

- 数据库：`/home/<user>/.openclaw/workspace/data/astock.db`
- 脚本：`/home/<user>/.openclaw/workspace/data/build_db.py`

## Safety

- `--refresh --workers 5` 耗时约 37 小时，必须后台运行（`nohup` / `screen`），启动前在汇报中注明
- 不执行任务范围外的工作
- 不确定时，把疑问写进输出，不要猜

## 输出规范

先定义输出文件：`OUT="/home/<user>/.openclaw/workspace/memory/stock-fetch-status-$(date +%F).md"`。

完成后输出以下摘要（严格遵守格式），并且要先写入 `$OUT`，再把**完全相同**的内容作为最后一条 assistant 输出：

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

硬性要求：

- **最后一条 assistant 输出不得为空，不得只写 `NO_REPLY`，不得省略任何一行。**
- 如果命令报错、会话被中断、或后台任务仍在运行，也要基于当前数据库状态和已知错误生成一份“当前状态摘要”，先写入 `$OUT`，再原样输出。
- 只有在你已经把摘要写入 `$OUT` 之后，才算完成任务。
