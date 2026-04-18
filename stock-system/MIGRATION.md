# Stock System Migration Guide

This document is written so another AI operator can migrate the stock OpenClaw system to a new Linux machine with as little guesswork as possible.

Use this guide together with the git-tracked `stock-system/` bundle in the repo.
During execution, also keep `stock-system/CHECKLIST.md` open and tick items off as each phase completes.

## Goal

Re-create the same stock workflow on another machine:

- database build and refresh (`build_db.py` + `astock.db`)
- stock screener (`screener.py`)
- report generation (`generate_report.py`, `build_announce.py`)
- batch top-N orchestration (`batch_workflow.py`)
- four stock agents (`stock-director`, `stock-researcher`, `stock-analyst`, `stock-fetcher`)
- Feishu delivery for messages and files

## Important Boundary

- Use Feishu to trigger long-running batch requests such as `top 5`, `top 10`, `top 30`, or `screen -> research -> rerank -> send files`.
- Do not use CLI for long batch jobs. The current gateway path is still connection-coupled for long subagent announces, so CLI is only reliable for short single-turn checks and short single-stock analysis.
- If a batch run is started from CLI and the caller disconnects, later subagent announces may time out before they can route back to the director.

## What Must Be Present On The Target Machine

The repo now tracks a curated stock bundle under `stock-system/`.

Required bundle files:

- `stock-system/data/build_db.py`
- `stock-system/data/schema.sql`
- `stock-system/data/astock.db`
- `stock-system/workspace/AGENTS.md`
- `stock-system/workspace-stock-director/AGENTS.md`
- `stock-system/workspace-stock-director/SOUL.md`
- `stock-system/workspace-stock-director/TOOLS.md`
- `stock-system/workspace-stock-director/USER.md`
- `stock-system/workspace-stock-director/batch_workflow.py`
- `stock-system/workspace-stock-researcher/AGENTS.md`
- `stock-system/workspace-stock-researcher/SOUL.md`
- `stock-system/workspace-stock-researcher/TOOLS.md`
- `stock-system/workspace-stock-researcher/USER.md`
- `stock-system/workspace-stock-researcher/generate_report.py`
- `stock-system/workspace-stock-researcher/build_announce.py`
- `stock-system/workspace-stock-researcher/check_freshness.py`
- `stock-system/workspace-stock-researcher/check_json.py`
- `stock-system/workspace-stock-researcher/skills/**`
- `stock-system/workspace-stock-analyst/AGENTS.md`
- `stock-system/workspace-stock-analyst/SOUL.md`
- `stock-system/workspace-stock-analyst/TOOLS.md`
- `stock-system/workspace-stock-analyst/USER.md`
- `stock-system/workspace-stock-analyst/check_web_tools.py`
- `stock-system/workspace-stock-analyst/analyze_stock.py`
- `stock-system/workspace-stock-analyst/calc_metrics.py`
- `stock-system/workspace-stock-analyst/skills/**`
- `stock-system/workspace-stock-fetcher/AGENTS.md`
- `stock-system/workspace-stock-fetcher/SOUL.md`
- `stock-system/workspace-stock-fetcher/TOOLS.md`
- `stock-system/workspace-stock-fetcher/USER.md`
- `stock-system/workspace-stock-fetcher/skills/**`
- `stock-system/core-repo/skills/stock-research/screener.py`
- `stock-system/config/openclaw.example.json`
- `stock-system/config/.env.example`
- `stock-system/config/jobs.example.json`

Note:

- `workspace/data/screener.py` is not a real file and should not be referenced.
- The real repo-side screener lives at `skills/stock-research/screener.py`.

## Target Directory Layout

Recommended target layout:

```text
/home/<user>/projects/openclaw
/home/<user>/.openclaw
/home/<user>/.config/systemd/user/openclaw-gateway.service
```

Within `~/.openclaw`, create or restore:

```text
/home/<user>/.openclaw/workspace
/home/<user>/.openclaw/workspace/data
/home/<user>/.openclaw/workspace/memory
/home/<user>/.openclaw/workspace-stock-director
/home/<user>/.openclaw/workspace-stock-researcher
/home/<user>/.openclaw/workspace-stock-analyst
/home/<user>/.openclaw/workspace-stock-fetcher
/home/<user>/.openclaw/cron
```

## Dependencies

Install baseline runtime dependencies first:

```bash
sudo apt update
sudo apt install -y git curl rsync sqlite3 python3 python3-pip
corepack enable
```

Install repo dependencies:

```bash
cd /home/<user>/projects/openclaw
pnpm install
pnpm exec tsdown
```

Install Python packages used by the data pipeline:

```bash
python3 -m pip install --upgrade pip
python3 -m pip install akshare pandas requests baostock
```

Do not try to `pip install sqlite3`; it is part of Python's standard library.

## Copy Steps

From the cloned repo root:

```bash
cd /home/<user>/projects/openclaw
mkdir -p /home/<user>/.openclaw/workspace/data
mkdir -p /home/<user>/.openclaw/workspace/memory
mkdir -p /home/<user>/.openclaw/workspace-stock-director
mkdir -p /home/<user>/.openclaw/workspace-stock-researcher
mkdir -p /home/<user>/.openclaw/workspace-stock-analyst
mkdir -p /home/<user>/.openclaw/workspace-stock-fetcher
mkdir -p /home/<user>/.openclaw/cron
mkdir -p /home/<user>/.config/systemd/user

rsync -av stock-system/data/ /home/<user>/.openclaw/workspace/data/
rsync -av stock-system/workspace/ /home/<user>/.openclaw/workspace/
rsync -av stock-system/workspace-stock-director/ /home/<user>/.openclaw/workspace-stock-director/
rsync -av stock-system/workspace-stock-researcher/ /home/<user>/.openclaw/workspace-stock-researcher/
rsync -av stock-system/workspace-stock-analyst/ /home/<user>/.openclaw/workspace-stock-analyst/
rsync -av stock-system/workspace-stock-fetcher/ /home/<user>/.openclaw/workspace-stock-fetcher/
rsync -av stock-system/core-repo/skills/stock-research/screener.py /home/<user>/projects/openclaw/skills/stock-research/screener.py
```

## Config Files To Fill Before Starting

The repo tracks templates, not live secrets.

1. Copy `stock-system/config/openclaw.example.json` to `~/.openclaw/openclaw.json`
2. Copy `stock-system/config/.env.example` to `~/.openclaw/.env`
3. Copy `stock-system/config/jobs.example.json` to `~/.openclaw/cron/jobs.json` only if you want cron jobs on the new machine

```bash
cp stock-system/config/openclaw.example.json /home/<user>/.openclaw/openclaw.json
cp stock-system/config/.env.example /home/<user>/.openclaw/.env
cp stock-system/config/jobs.example.json /home/<user>/.openclaw/cron/jobs.json
```

Then edit them and replace placeholders:

- `/home/<user>` paths
- API keys in `~/.openclaw/.env`
- Feishu `accountId`
- Feishu `openId` placeholder `ou_<feishu_user_open_id>`
- any stock channel binding or delivery target placeholders

## Gateway Service

If the target machine uses systemd user services, create or update `~/.config/systemd/user/openclaw-gateway.service` so it points at the target user's Node path and repo path.

Minimum checks after editing:

- `ExecStart=` points to the correct Node executable and `dist/index.js`
- `WorkingDirectory=` points to `/home/<user>/projects/openclaw`
- `Environment=HOME=/home/<user>` is correct
- `PATH` includes pnpm and Node locations

Then reload and restart:

```bash
systemctl --user daemon-reload
systemctl --user enable openclaw-gateway
systemctl --user restart openclaw-gateway
```

## Model And Auth Notes

After copying config, log in again on the target machine for any provider that uses local OAuth state.

Most likely needed:

```bash
cd /home/<user>/projects/openclaw
pnpm openclaw models auth login --provider openai-codex
```

If Feishu app credentials are not embedded in the config template, fill them locally before testing delivery.

## Data Pipeline Smoke Tests

Run these in order.

### 1. Database status

```bash
cd /home/<user>/.openclaw/workspace/data
python3 build_db.py --status
```

Expected outcome:

- no import errors
- database opens successfully
- status output shows latest financial, market, and executive-hold dates

### 2. Screener

```bash
cd /home/<user>/projects/openclaw
python3 skills/stock-research/screener.py
```

Expected outcome:

- writes `~/.openclaw/workspace/memory/screener_result_YYYY-MM-DD.json`
- ends without fatal errors

### 3. Report generator contract test

```bash
cd /home/<user>/.openclaw/workspace-stock-researcher
python3 generate_report.py --test
```

Expected outcome:

- pass message confirming sorting, formulas, dates, and metadata contract

### 4. Report generation

```bash
cd /home/<user>/.openclaw/workspace-stock-researcher
python3 generate_report.py --top-n 30
python3 build_announce.py --date $(date +%F) --top-n 5 --write-file
```

Expected outcome:

- writes `stock-pick-YYYY-MM-DD.md`
- writes announce output based on the same ordering contract

### 5. Web-search probe for analyst

```bash
cd /home/<user>/.openclaw/workspace-stock-analyst
python3 check_web_tools.py
```

Expected outcome:

- should not return auth failure for the configured web-search provider

## Delivery Smoke Tests

### Feishu message path

Send a short test from the stock account and confirm it reaches the user.

### Feishu file path

Ask the director to send a known local markdown file and confirm the file arrives successfully.

### Batch path

Trigger the batch path from Feishu, not CLI. A good acceptance request is:

```text
执行一次筛选任务，然后把初筛综合分排名前5名做深度调研；如果旧调研仍有效就复用，否则重新调研。最后把初筛排名、最终复排结果和深度调研报告文件一起发给我。
```

Expected outcome:

- first reply is a factual progress message
- final delivery includes initial ranking file, rerank file, and report files in final-score order

## Logs To Watch During Migration

Watch the gateway while testing:

```bash
journalctl --user -u openclaw-gateway -f
```

High-priority errors worth fixing immediately:

- `Perplexity API error (401)` or any web-search auth error
- `Delivery failed` for the stock Feishu route
- `Subagent announce failed: gateway timeout after 300000ms`
- `No reply from agent`
- `lane wait exceeded` during normal load

Lower-priority but still useful to note:

- harmless one-off tool stderr from scratch scripts
- reconnect noise that self-recovers

## What Another AI Should Not Change During Migration

Do not rewrite these workflow scripts during migration unless a test proves they are broken and you are explicitly repairing them:

- `build_db.py`
- `batch_workflow.py`
- `generate_report.py`
- `build_announce.py`
- `skills/stock-research/screener.py`

These files are now treated as fixed workflow assets, not ad-hoc scratch pads.

## Fast Acceptance Checklist

Migration is complete only if all of the following are true:

- `python3 build_db.py --status` works
- `python3 skills/stock-research/screener.py` works
- `python3 generate_report.py --test` passes
- `python3 check_web_tools.py` reports ready status
- Feishu can receive a plain text message
- Feishu can receive a file
- a Feishu-triggered top-5 batch can finish and deliver ranking + rerank + report files

## Suggested Workflow For Another AI Operator

1. Clone repo
2. Copy `stock-system/` into the target machine's runtime paths using the commands above
3. Fill `openclaw.json`, `.env`, and optional `jobs.json`
4. Build the repo and restart the gateway
5. Run the smoke tests in order
6. Trigger the batch acceptance test from Feishu
7. Only after all tests pass, mark the migration complete
