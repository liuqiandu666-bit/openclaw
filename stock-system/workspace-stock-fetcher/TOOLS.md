# TOOLS.md - Local Notes

## 核心脚本

- **抓取脚本**：`/home/<user>/.openclaw/workspace/data/build_db.py`
- **数据库**：`/home/<user>/.openclaw/workspace/data/astock.db`

## 完整参数列表

| 参数                    | 功能                                    | 耗时       |
| ----------------------- | --------------------------------------- | ---------- |
| `--status`              | 查看进度和状态                          | 秒级       |
| `--retry`               | 重试失败的财务报表                      | 按失败数量 |
| `--refresh --workers N` | **季报/年报后用**：清除成功记录强制重抓 | ~37h       |
| `--workers N`           | 全量首次建库（默认5线程）               | ~37h       |
| `--market`              | 刷新市场快照（东方财富）                | ~33 分钟   |
| `--market-bs`           | 刷新市场快照（baostock，更稳定）        | 数分钟     |
| `--industry`            | 刷新行业分类                            | 数小时     |
| `--executive-hold`      | 刷新高管增减持（巨潮资讯）              | ~30 秒     |
| `--limit N`             | 行业/市值限制本批处理数量               | —          |
| `--interval N`          | 请求间隔秒数（默认 2.0）                | —          |

## 季报更新推荐流程

1. `--refresh --workers 5`（财务三张表）
2. `--market-bs`（市场快照）
3. `--executive-hold`（高管增减持）
4. 抽样验证（见 SKILL.md 第三步）

## 性能参考

| 并发数 | 预估耗时                   |
| ------ | -------------------------- |
| 5 线程 | ~37 小时                   |
| 8 线程 | ~23 小时（有概率触发限速） |
