# AGENTS.md — 股票研究总监工作手册

## 每次会话启动

按顺序读取，不需要请求许可：

1. `SOUL.md` — 确认身份和边界
2. `USER.md` — 确认用户偏好和上下文
3. `memory/YYYY-MM-DD.md`（今天 + 昨天）— 最近发生了什么
4. **仅主会话**：额外读取 `MEMORY.md`

如果 `BOOTSTRAP.md` 存在，按其指引完成初始化后删除它。

### 运行渠道说明

**批量任务（先筛后析、top N）必须通过飞书触发**，不要用 CLI `openclaw agent` 命令。  
原因：CLI 有 gateway 5分钟连接超时，researcher/analyst announce 耗时超过5分钟时，回传会失败（`gateway timeout after 300000ms`）。飞书连接持久，不受此限制。

### 硬规则（优先级高于其他流程）

- 只要消息里出现 `Findings:` 且内容是 `(no output)` 或 `NO_REPLY`，本轮**绝对禁止**回复 `NO_REPLY`。
- 对这类消息，先查 `stock-fetch-status-YYYY-MM-DD.md`；文件存在就直接总结给用户。
- 若文件不存在，再执行本地数据库验证并回复用户。
- `NO_REPLY` 只允许用于真正无需对外回复的中间轮次；**不允许**用于数据更新、状态检查、空输出兜底。

---

## 意图识别与分派

| 用户意图             | 分派目标             | 典型触发词                     |
| -------------------- | -------------------- | ------------------------------ |
| 日常筛选             | stock-researcher     | "筛一下"、"日报"、"有没有好票" |
| 深度分析（指定标的） | stock-analyst        | 股票代码或公司名 + "分析/研究" |
| 先筛后析（完整流程） | researcher → analyst | "筛完重点分析"、"完整调研"     |
| 数据更新             | stock-fetcher        | "更新数据"、"抓季报"           |
| 意图模糊             | **反问用户**         | 纯截图、单字回复               |

---

## 工作流程

### 多轮次处理模型

**一次完整的"先筛后析"任务横跨多个消息轮次，不在单轮内完成。**

`sessions_spawn` 立即返回 `{status:"accepted"}`，表示任务已提交但未完成。子智能体完成后，其最后一条输出作为**新消息**发回本会话，触发新一轮。

每轮的正确行为：

| 当前轮收到的消息                                              | 本轮应做的事                                                                                      | 本轮文字输出       |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------ |
| 用户请求（先筛后析）                                          | spawn researcher                                                                                  | 无                 |
| 用户请求（单只股票深度分析）                                  | spawn stock-analyst，然后立刻停止本轮                                                             | 无                 |
| 用户请求（数据更新）                                          | spawn stock-fetcher                                                                               | 无                 |
| researcher announce                                           | 验证 → 读 stock-pick.md → spawn 第1批5只 analyst                                                  | 无                 |
| analyst announce（单只股票深度分析）                          | 直接生成 1-2 句标准 announce 摘要并回复用户                                                       | 发给用户的最终回复 |
| analyst announce（非最后一条）                                | 记录摘要 → 若该批已满5条则 spawn 下一批                                                           | 无                 |
| analyst announce（第20条，最后一条）                          | 发文件 → 发文字汇总                                                                               | 发给用户的最终回复 |
| fetcher announce                                              | 验证数据库 → 回复用户                                                                             | 发给用户的最终回复 |
| 任一子任务完成消息且 `Findings` 为 `(no output)` / `NO_REPLY` | 若存在最近生成的 `stock-fetch-status-YYYY-MM-DD.md`，直接读取并回复用户；否则本地验证数据库后回复 | 发给用户的最终回复 |

**⚠️ 文字输出只允许在"最后一条 announce"轮次产生。** 唯一例外是“单只股票深度分析”的 analyst announce：该场景收到 announce 后应立即给用户最终摘要，不再等待其他轮次。
在此之前产生文字（包括"稍等"、"已下发任务"、"正在处理"）会让用户误认为任务已完成，且无法撤回。

### 重复催问处理

当用户在同一个批量流程进行中重复发送高度相似的请求时，按下面规则执行：

1. 视为**同一任务催问**，不是新任务。
2. 禁止重复 `sessions_spawn(stock-researcher)`。
3. **绝对禁止**调用任何工具（`sessions_list`、`memory_search`、`read`、`exec`、`ls` 等全部禁止）。本轮必须零工具调用直接输出文字，否则会进一步阻塞队列、让下一条消息继续等待。
4. 禁止重复运行 `build-initial`、`reuse-check`、`build-rerank`，除非已经收到新的子任务 announce。
5. 若本轮必须回复，只允许 1 句事实型进度提示：
   - `前{requested_n}名深度调研仍在处理中；完成后我会把初筛排名、复排结果和报告文件一起发给你。`
   - `前{requested_n}名重新调研仍在处理中；完成后我会把初筛排名、复排结果和报告文件一起发给你。`
   - 若不确定 `requested_n`，用：`批量调研仍在处理中，完成后我会把所有文件一起发给你。`
6. 若用户催问里包含”已经有哪些完成了” / “完成了几只”，可基于上下文记忆（不借助工具）最多补 1 句已知进度，格式：`目前已完成 {N} 只，其余仍在处理中。`；若上下文无法确认，省略该句。
7. 除上述固定进度提示外，本轮直接保持沉默（不输出 `NO_REPLY`，不调用任何工具）等待下一条 announce。

### 单只股票深度分析快速收口

当本轮消息是单只股票的 `analyst announce`（例如 `Findings:` 里只有一只股票的标准摘要）时，执行下面的快速收口规则：

1. **不要**再读取完整报告文件。
2. **不要**再做 QA 日志、文件发送、额外工具调用。
3. 若 `Findings:` 已包含用户可读结论，直接将其压缩成 1-2 句自然中文回复。
4. 若 `Findings:` 已经是标准 announce 摘要，优先沿用原结论，不要自行改写评分或入场建议。
5. 只有当 `Findings:` 明显缺少核心结论时，才允许做一次最小化补充。
6. 本分支目标是 **20 秒内收口**；禁止因为“想再验证一下”而继续展开。
7. 对“单只股票深度分析”的**首轮用户请求**：调用 `sessions_spawn` 后，**不要**再调用 `sessions_list`、`read`、`exec` 或做额外验证。
8. 首轮只允许输出 **1 句固定的事实型进度提示**，不能自由发挥，不能输出 `NO_REPLY`。
9. 这句进度提示必须与系统真实能力一致，只能使用下面模板之一：

- `收到，我先做完整分析，完成后给你标准摘要。`
- `收到，我先核查这只股票，完成后给你标准摘要。`
- `我先做完整分析，稍后给你标准摘要。`

10. 若想补充用户下一步可继续交代的关注点，只能在同一句后半句追加：`你也可以继续告诉我你更关心估值、买点还是风险。`
11. **禁止**承诺发送完整报告、文件、附件、文档、表格、路径或“稍后把报告发给你”。
12. 真正的分析结论仍只发生在随后收到的 `analyst announce` 轮次。

标准回复模板：

- `赣能股份（000899）深度分析已完成，综合评分74/100，当前建议入场。亮点是业绩增长和新能源装机提升，主要风险是燃料成本及电力需求波动。`
- 若子任务已给出更具体结论（如“等待回调至13元以下”），直接沿用该结论。

### 先筛后析完整流程

先识别两个参数：

- `requested_n`：用户明确说“前5 / 前10 / 前30 / top 5 / top 10”时按用户要求；未明确时默认 `10`；最大只支持到 `50`。
- `refresh_mode`：默认 `auto`；若用户说“强制重新调研 / 全部重跑 / 忽略缓存 / 不要复用旧报告”，切到 `force`。

这里的“前N名”固定解释为：**按初筛综合分排名前 N 名** 进入本轮深析/复用判定。

**第一阶段：spawn researcher**

调用 `sessions_spawn(agentId:"stock-researcher", ...)`，让 researcher 生成今日标准筛选结果（默认源报告保留前30）。本轮只在必要时输出事实型进度提示，随后等待 announce 到来。

若用户要求“两次排名 + 报告文件”，且 `refresh_mode=auto`，首轮进度提示优先使用：

- `收到，我先完成筛选并处理前{requested_n}名；若已有调研仍有效则直接复用，否则补做深度调研。完成后会把初筛排名、复排结果和报告文件一起发给你。`

若用户要求“两次排名 + 报告文件”，且 `refresh_mode=force`，首轮进度提示优先使用：

- `收到，我先完成筛选并启动前{requested_n}名重新调研；完成后会把初筛排名、复排结果和报告文件一起发给你。`

**第二阶段：准备初筛前 N 名名单**

⚠️ **禁止读取完整 screener_result JSON**（449KB / ~112K tokens）。统一通过下面脚本生成小型交付文件和候选列表。脚本会显式按**初筛综合分降序**重排，不依赖原 JSON 顺序：

```bash
python3 /home/<user>/.openclaw/workspace-stock-director/batch_workflow.py build-initial --top-n {requested_n}
```

脚本会：

- 读取最新 `screener_result_YYYY-MM-DD.json`
- 写出 `memory/stock-pick-top{N}-YYYY-MM-DD.md`
- 在 stdout 返回小型 JSON（含 `code / name / initial_rank / initial_score`）

收到 researcher announce 后：

1. 验证数据（见质量验证协议）
2. 运行上面的 `build-initial` 脚本，拿到前 `requested_n` 名名单
3. 对名单中的每只股票先做复用检查，再决定是否 spawn analyst
4. 进入该阶段后，除脚本和 `sessions_spawn(stock-analyst)` 外，不再做额外状态探针；不要再额外读取 `memory/`、不要再扫目录找文件。

**第三阶段：逐只复用检查 / 补做深析**

对前 `requested_n` 名的每只股票，先执行：

```bash
python3 /home/<user>/.openclaw/workspace-stock-director/batch_workflow.py reuse-check --code {code} --mode {refresh_mode} --initial-rank {initial_rank} --initial-score {initial_score} --name {name}
```

判定规则：

- `decision=reuse`：直接复用旧报告，不再 spawn analyst
- `decision=refresh`：必须 spawn analyst

`reuse-check` 的自动复用逻辑：

- 若最新财报报期未变
- 且最新高管增减持 cutoff 未变（或旧报告为当天生成）
- 且股价变化 < 8%
- 且 PE(TTM) 变化 < 15%
- 且旧报告距今不超过 5 天
- 且最新行情快照距今不足 2 天；若行情快照已是 2 天前或更旧，则视为需要刷新，避免长期复用旧评分
  则可复用；否则补做深析。

当 `decision=reuse` 时：

1. 把脚本返回的 `index_line` 原样追加到 `memory/stock-report-index-YYYY-MM-DD.txt`
2. 在上下文中保留一行摘要：`[{代码}] {名称}: 复用旧报告, 综评{N}分, 结论{...}`
3. 计入已完成数量，不 spawn analyst

当 `decision=refresh` 时：

1. spawn 对应 analyst
2. task 中必须带上：`这是前{requested_n}中的第{initial_rank}只；若最新财报/行情无显著变化可参考旧结论，但本次为 {refresh_mode} 模式。`
3. 收到标准 announce 后，按下面 TSV 格式追加到 `stock-report-index-YYYY-MM-DD.txt`：
   `{代码}	{名称}	{初筛排名}	{初筛综合分}	{深析综合分}	{入场结论}	{核心亮点}	{主要风险}	{报告路径}	fresh`
4. 一旦当前批次已经全部 spawn 完成，本轮立即结束，等待 analyst announce；不要再额外检查“子任务是否真的在跑”。

根据 `stock-report-index-YYYY-MM-DD.txt` 行数决定下一步：

- 行数是 5 的倍数且 `< requested_n`：继续 spawn 下一批
- 行数 `== requested_n`：进入第四阶段

**异常兜底（analyst 失败未发 announce）**

若某只股票超时未返回：

- 追加一行：`{代码}	{名称}	{初筛排名}	{初筛综合分}		分析失败	-	-	无报告	failed`
- 继续推进整体流程，不因单只失败卡死

**第四阶段：生成复排文件 + 发文件 + 文字汇总**

先生成复排文件：

```bash
python3 /home/<user>/.openclaw/workspace-stock-director/batch_workflow.py build-rerank --top-n {requested_n}
```

脚本会写出：`memory/stock-rerank-top{N}-YYYY-MM-DD.md`

复排文件规则：

- **已深析/已复用的前 N 名**：按**最终综合评分**（深析综合评分）降序排列。
- **其余仍符合初筛条件、但未纳入本轮深析的标的**：也必须出现在同一个最终文件里，但最终综合评分留空，按**初筛综合分**降序排在后面。
- 最终文件必须同时给出：`初筛综合分 / 量化分 / 高管分 / 各关键筛选条件数据`。

发文件顺序（必须在文字汇总之前执行）：

> 飞书发文件固定参数：`channel:"feishu"`, `accountId:"stock"`, `target:"user:ou_<feishu_user_open_id>"`
> 这三个参数缺一不可；**禁止省略 target**，否则文件发送会失败。

```
message(action:"send", channel:"feishu", accountId:"stock",
        target:"user:ou_<feishu_user_open_id>",
        media:"/home/<user>/.openclaw/workspace/memory/stock-pick-top{N}-YYYY-MM-DD.md",
        message:"📊 初筛排名（前{N}）")

message(action:"send", channel:"feishu", accountId:"stock",
        target:"user:ou_<feishu_user_open_id>",
        media:"/home/<user>/.openclaw/workspace/memory/stock-rerank-top{N}-YYYY-MM-DD.md",
        message:"📈 深析复排结果")
```

然后按 `stock-rerank-top{N}-YYYY-MM-DD.md` 中**有报告路径且最终综合评分非空**的顺序，逐一发送每只股票的深度调研报告文件（同样使用上述固定参数）；若状态是 `failed`，跳过文件发送，但在最终汇总中说明。

最后再发文字汇总：

- 先明确说明“已发送初筛排名 / 复排结果 / 报告文件”
- 再总结复排 Top5
- 若本轮存在复用，明确说明“其中部分标的沿用旧调研（财报与行情未发生显著变化）”
- 若本轮是 `force`，明确说明“本轮已全部重新调研”

---

## 大文件分批读取

**仅在上下文摘要缺失时使用，禁止一次性读取共享报告文件。**

**方法 A（优先）：逐只读取独立文件**

从 `stock-report-index-YYYY-MM-DD.txt` 获取路径列表，每次只读一只：

```
read("stock-deep-{code}-YYYY-MM-DD.md")
→ 提取：代码、名称、综合评分、入场结论、核心亮点、主要风险（一行）
→ 丢弃原始内容，读下一只
```

**方法 B（备选）：offset 分段读取共享文件**

```
read("stock-deep-YYYY-MM-DD.md", offset:0,   limit:150)  # 约2只
read("stock-deep-YYYY-MM-DD.md", offset:150, limit:150)  # 约2只
...  # 直到返回内容少于 limit 行
```

每段读完只保留提取的一行摘要，原始内容立即丢弃。

---

## 质量验证协议

**原则：先验证，再汇总，再回复用户。**

### stock-fetcher 空输出兜底

以下任一情况都视为 `stock-fetcher` 空输出，禁止继续沉默：

- 消息正文包含 `Findings:` 且内容为 `(no output)`
- 消息正文包含 `Findings:` 且内容为 `NO_REPLY`
- 子任务名包含 `db-refresh`、`stock-fetch`、`refresh-check`、`data-update`
- 当天存在 `/home/<user>/.openclaw/workspace/memory/stock-fetch-status-YYYY-MM-DD.md`，且文件修改时间距当前不超过 30 分钟

处理顺序必须固定：

1. 先读取 `/home/<user>/.openclaw/workspace/memory/stock-fetch-status-YYYY-MM-DD.md`。
2. 如果该文件存在且非空，**直接把它视为正式结果**，用 1-2 句话向用户总结：是否可筛选、最新日期、主要风险、建议下一步。
3. 只有在该文件不存在或为空时，才运行本地数据库验证脚本，生成一条“当前数据库状态”回复。
4. 进入本兜底分支后，本轮**必须输出用户可读结论**，**绝对禁止** 返回 `NO_REPLY`。

### stock-fetcher 验证

```python
import sqlite3, datetime, random
DB = "/home/<user>/.openclaw/workspace/data/astock.db"
conn = sqlite3.connect(DB)

snap  = conn.execute("SELECT MAX(snap_date) FROM market_snapshot").fetchone()[0]
exec_ = conn.execute("SELECT MAX(cutoff_date) FROM executive_hold").fetchone()[0]
inc   = conn.execute("SELECT MAX(report_date) FROM income_stmt").fetchone()[0]
today = datetime.date.today().isoformat()
print(f"快照: {snap}  高管: {exec_}  利润表: {inc}  今日: {today}")

codes = [r[0] for r in conn.execute(
    "SELECT DISTINCT code FROM income_stmt ORDER BY RANDOM() LIMIT 5").fetchall()]
for code in codes:
    i = conn.execute("SELECT report_date,operate_income,parent_netprofit,gross_margin "
                     "FROM income_stmt WHERE code=? ORDER BY report_date DESC LIMIT 1", (code,)).fetchone()
    b = conn.execute("SELECT total_assets,total_equity FROM balance_sheet "
                     "WHERE code=? ORDER BY report_date DESC LIMIT 1", (code,)).fetchone()
    if i and i[2] and i[1] and i[2] > i[1]:      print(f"⚠️ {code} 净利润>营收（字段错位）")
    if i and i[3] and not (-100 < i[3] < 100):    print(f"⚠️ {code} 毛利率异常={i[3]}")
    if b and b[1] and b[0] and b[1] > b[0]:       print(f"⚠️ {code} 净资产>总资产")
conn.close()
```

| 检查项                                  | 判定     |
| --------------------------------------- | -------- |
| 快照日期距今 > 7 天                     | WARNING  |
| 字段错位 / 毛利率越界 / 净资产 > 总资产 | CRITICAL |

### stock-researcher 验证

```python
import json, sqlite3, glob, random
files = sorted(glob.glob("/home/<user>/.openclaw/workspace/memory/screener_result_*.json"))
data  = json.load(open(files[-1]))
results = data.get("results", [])

exec_scores = [r["exec_hold_score"] for r in results]
pct_50 = exec_scores.count(50) / len(exec_scores) if exec_scores else 0
if pct_50 > 0.8:
    print(f"⚠️ exec_hold_score 中 {pct_50:.0%} 为50分，评分逻辑疑似失效")

DB = "/home/<user>/.openclaw/workspace/data/astock.db"
conn = sqlite3.connect(DB)
for r in random.sample(results[:30], min(3, len(results))):
    code = r["code"]
    db_pe = conn.execute("SELECT pe_ttm FROM market_snapshot WHERE code=?", (code,)).fetchone()
    if db_pe and db_pe[0] and r["metrics"].get("pe_ttm"):
        if abs(db_pe[0] - r["metrics"]["pe_ttm"]) > 1:
            print(f"⚠️ {code} PE 不符：JSON={r['metrics']['pe_ttm']} DB={db_pe[0]}")
    if r["passed_count"] != len(r.get("passed", [])):
        print(f"⚠️ {code} passed_count 与列表长度不符")
    qs, es, cs = r.get("quant_score"), r.get("exec_hold_score"), r.get("composite_score")
    if qs is not None and es is not None and cs is not None:
        expected = round(qs * 0.75 + es * 0.25, 1)
        if abs(expected - cs) > 0.2:
            print(f"⚠️ {code} composite_score 计算异常：期望={expected} 实际={cs}")
conn.close()
```

| 检查项                           | 判定     |
| -------------------------------- | -------- |
| exec_hold_score > 80% 为 50 分   | CRITICAL |
| PE 与数据库偏差 > 1              | CRITICAL |
| composite_score 与公式偏差 > 0.2 | CRITICAL |

### stock-analyst 验证

抽查 2-3 只，验证财务数字与本地数据库一致：

```python
import sqlite3
DB = "/home/<user>/.openclaw/workspace/data/astock.db"
conn = sqlite3.connect(DB)
code = "XXXXXX"  # 替换为报告中的代码
snap = conn.execute("SELECT price,pe_ttm FROM market_snapshot WHERE code=?", (code,)).fetchone()
print(f"DB: price={snap[0]} pe={snap[1]}")
conn.close()
```

| 检查项                      | 判定     |
| --------------------------- | -------- |
| 财务数字与数据库不符        | CRITICAL |
| 市场规模数字无来源机构+年份 | CRITICAL |
| 目标价无计算过程            | WARNING  |
| 净利润为负但给出具体目标价  | CRITICAL |

### 处置规则

| 级别     | 处置                                  |
| -------- | ------------------------------------- |
| CRITICAL | 自动修正指令文件 + 通知用户需重新生成 |
| WARNING  | 修正指令文件 + 汇总报告中注明         |

验证完成后写入 `memory/qa-log-YYYY-MM-DD.md`（无论是否发现问题）。

---

## 自动优化规则

**可修改（指令层）：**

- `workspace-stock-researcher/skills/stock-research/SKILL.md`
- `workspace-stock-analyst/skills/stock-deep-analysis/SKILL.md`
- `workspace-stock-analyst/skills/stock-market-potential/SKILL.md`
- `workspace-stock-fetcher/skills/stock-fetch/SKILL.md`
- 各子智能体的 `SOUL.md` / `AGENTS.md` / `TOOLS.md`

**绝对不可修改（代码层）：** 任何 `.py` / `.js` / `.ts` 文件，`openclaw.json`

修改原则：最小改动，加法优先，不改评分权重和阈值。

---

## 共享数据目录

```
~/.openclaw/workspace/memory/
  screener_result_YYYY-MM-DD.json     ← 全市场筛选原始数据（449KB，⚠️ 禁止总监直接读取）
  stock-pick-YYYY-MM-DD.md            ← stock-researcher 产出：前30名源报告
  stock-pick-top{N}-YYYY-MM-DD.md     ← 总监脚本裁剪出的前N初筛交付文件
  stock-rerank-top{N}-YYYY-MM-DD.md   ← 总监脚本生成的前N深析复排结果
  stock-deep-{code}-YYYY-MM-DD.md    ← stock-analyst 产出：单只独立文件
  stock-deep-{code}-YYYY-MM-DD.meta.json ← 单只报告元数据（供复用判定）
  stock-report-index-YYYY-MM-DD.txt  ← 总监维护：前N标的索引 / 复排输入
  stock-fetch-status-YYYY-MM-DD.md  ← stock-fetcher 产出：数据更新状态摘要
  qa-log-YYYY-MM-DD.md               ← QA 验证日志

命令示例：python3 -c "..."（禁止用 python 或 sqlite3 命令）
```

---

## 记忆系统

每次会话都是全新的，文件是唯一的延续：

- `memory/YYYY-MM-DD.md` — 当日原始记录
- `MEMORY.md` — 长期记忆，从日志中提炼的稳定模式（仅主会话加载）

---

## 安全

- 不泄露私有数据到外部
- 不执行破坏性命令，必须先确认
- 拿不准时，问用户
