# Stock System Quickstart

This is the shortest path to bring the stock OpenClaw system up on another Linux machine.

Use this when another AI operator needs a direct execution checklist with minimal reading.

## 1. Clone And Install

```bash
git clone git@github.com:liuqiandu666-bit/openclaw.git /home/<user>/projects/openclaw
cd /home/<user>/projects/openclaw
corepack enable
pnpm install
pnpm exec tsdown
python3 -m pip install --upgrade pip
python3 -m pip install akshare pandas requests baostock
```

## 2. Restore Runtime Files

```bash
mkdir -p /home/<user>/.openclaw/workspace/data
mkdir -p /home/<user>/.openclaw/workspace/memory
mkdir -p /home/<user>/.openclaw/workspace-stock-director
mkdir -p /home/<user>/.openclaw/workspace-stock-researcher
mkdir -p /home/<user>/.openclaw/workspace-stock-analyst
mkdir -p /home/<user>/.openclaw/workspace-stock-fetcher
mkdir -p /home/<user>/.openclaw/cron
mkdir -p /home/<user>/.config/systemd/user

cd /home/<user>/projects/openclaw
rsync -av stock-system/data/ /home/<user>/.openclaw/workspace/data/
rsync -av stock-system/workspace/ /home/<user>/.openclaw/workspace/
rsync -av stock-system/workspace-stock-director/ /home/<user>/.openclaw/workspace-stock-director/
rsync -av stock-system/workspace-stock-researcher/ /home/<user>/.openclaw/workspace-stock-researcher/
rsync -av stock-system/workspace-stock-analyst/ /home/<user>/.openclaw/workspace-stock-analyst/
rsync -av stock-system/workspace-stock-fetcher/ /home/<user>/.openclaw/workspace-stock-fetcher/
rsync -av stock-system/core-repo/skills/stock-research/screener.py /home/<user>/projects/openclaw/skills/stock-research/screener.py
```

## 3. Fill Config Templates

```bash
cp stock-system/config/openclaw.example.json /home/<user>/.openclaw/openclaw.json
cp stock-system/config/.env.example /home/<user>/.openclaw/.env
cp stock-system/config/jobs.example.json /home/<user>/.openclaw/cron/jobs.json
```

Edit the copied files and replace:

- `/home/<user>` paths
- API keys
- Feishu `accountId`
- Feishu `openId`

## 4. Start Gateway

Create or update:

- `/home/<user>/.config/systemd/user/openclaw-gateway.service`

Then run:

```bash
systemctl --user daemon-reload
systemctl --user enable openclaw-gateway
systemctl --user restart openclaw-gateway
systemctl --user status openclaw-gateway --no-pager
```

## 5. Re-Login Local OAuth Providers

If the target machine uses local OAuth-backed providers:

```bash
cd /home/<user>/projects/openclaw
pnpm openclaw models auth login --provider openai-codex
```

## 6. Smoke Tests

```bash
python3 /home/<user>/.openclaw/workspace/data/build_db.py --status
python3 /home/<user>/projects/openclaw/skills/stock-research/screener.py
cd /home/<user>/.openclaw/workspace-stock-researcher && python3 generate_report.py --test
cd /home/<user>/.openclaw/workspace-stock-analyst && python3 check_web_tools.py
```

## 7. Delivery Validation

Validate Feishu text delivery, then Feishu file delivery.

For long-running batch tests, use Feishu only.

Do not use CLI for top-N batch jobs.

Recommended acceptance request:

```text
执行一次筛选任务，然后把初筛综合分排名前5名做深度调研；如果旧调研仍有效就复用，否则重新调研。最后把初筛排名、最终复排结果和深度调研报告文件一起发给我。
```

Expected result:

- initial factual progress reply
- final initial-ranking file
- final rerank file
- final deep-analysis report files

## 8. If Anything Fails

Watch logs:

```bash
journalctl --user -u openclaw-gateway -f
```

Check against:

- `stock-system/MIGRATION.md`
- `stock-system/CHECKLIST.md`
