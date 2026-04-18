# Stock System Migration Checklist

Use this checklist together with `stock-system/MIGRATION.md` when another AI or operator migrates the stock system to a new machine.

## 1. Repo And Runtime Layout

- [ ] Repo cloned to `/home/<user>/projects/openclaw`
- [ ] Runtime root created at `/home/<user>/.openclaw`
- [ ] `stock-system/data/*` copied to `/home/<user>/.openclaw/workspace/data/`
- [ ] `stock-system/workspace/*` copied to `/home/<user>/.openclaw/workspace/`
- [ ] `stock-system/workspace-stock-director/*` copied to `/home/<user>/.openclaw/workspace-stock-director/`
- [ ] `stock-system/workspace-stock-researcher/*` copied to `/home/<user>/.openclaw/workspace-stock-researcher/`
- [ ] `stock-system/workspace-stock-analyst/*` copied to `/home/<user>/.openclaw/workspace-stock-analyst/`
- [ ] `stock-system/workspace-stock-fetcher/*` copied to `/home/<user>/.openclaw/workspace-stock-fetcher/`
- [ ] `stock-system/core-repo/skills/stock-research/screener.py` copied to `/home/<user>/projects/openclaw/skills/stock-research/screener.py`

## 2. Config Templates Filled

- [ ] `stock-system/config/openclaw.example.json` copied to `/home/<user>/.openclaw/openclaw.json`
- [ ] `stock-system/config/.env.example` copied to `/home/<user>/.openclaw/.env`
- [ ] `stock-system/config/jobs.example.json` copied to `/home/<user>/.openclaw/cron/jobs.json` only if cron jobs are desired
- [ ] All `/home/<user>` placeholders replaced with the target username/path
- [ ] All API keys filled in locally
- [ ] Feishu `accountId` filled in locally
- [ ] Feishu `openId` placeholder `ou_<feishu_user_open_id>` replaced with the real target user openId

## 3. Dependencies Installed

- [ ] `pnpm install` completed in `/home/<user>/projects/openclaw`
- [ ] `pnpm exec tsdown` completed
- [ ] Python packages installed: `akshare`, `pandas`, `requests`, `baostock`

## 4. Gateway Ready

- [ ] `~/.config/systemd/user/openclaw-gateway.service` points to the correct Node path
- [ ] `WorkingDirectory` points to `/home/<user>/projects/openclaw`
- [ ] `systemctl --user daemon-reload` completed
- [ ] `systemctl --user restart openclaw-gateway` completed
- [ ] `systemctl --user status openclaw-gateway --no-pager` shows the service is active

## 5. Provider Auth Ready

- [ ] Any local OAuth provider needed by the stock system was logged in again on the new machine
- [ ] `pnpm openclaw models auth login --provider openai-codex` completed if Codex is used on the new machine

## 6. Data Pipeline Smoke Tests

- [ ] `python3 /home/<user>/.openclaw/workspace/data/build_db.py --status` succeeds
- [ ] `python3 /home/<user>/projects/openclaw/skills/stock-research/screener.py` succeeds
- [ ] `python3 /home/<user>/.openclaw/workspace-stock-researcher/generate_report.py --test` passes
- [ ] `python3 /home/<user>/.openclaw/workspace-stock-researcher/generate_report.py --top-n 30` succeeds
- [ ] `python3 /home/<user>/.openclaw/workspace-stock-researcher/build_announce.py --date $(date +%F) --top-n 5 --write-file` succeeds
- [ ] `python3 /home/<user>/.openclaw/workspace-stock-analyst/check_web_tools.py` reports ready status

## 7. Delivery Checks

- [ ] Feishu text message delivery succeeds
- [ ] Feishu file delivery succeeds
- [ ] No fresh `Unknown channel: whatsapp` errors appear in logs
- [ ] No fresh web-search auth failures appear in logs

## 8. Batch Acceptance Test

Trigger this from Feishu, not CLI:

```text
执行一次筛选任务，然后把初筛综合分排名前5名做深度调研；如果旧调研仍有效就复用，否则重新调研。最后把初筛排名、最终复排结果和深度调研报告文件一起发给我。
```

- [ ] First reply is a factual progress message
- [ ] Final output includes initial ranking file
- [ ] Final output includes rerank file
- [ ] Final output includes deep-research report files
- [ ] Report files are ordered by final score

## 9. Log Watch During Acceptance

Monitor while testing:

```bash
journalctl --user -u openclaw-gateway -f
```

Flag these immediately if they appear:

- [ ] `Perplexity API error (401)`
- [ ] `Delivery failed`
- [ ] `Subagent announce failed: gateway timeout after 300000ms`
- [ ] `No reply from agent`
- [ ] `lane wait exceeded`

## 10. Final Sign-Off

- [ ] The stock system can screen, rerank, analyze, and deliver files on the target machine
- [ ] Feishu is the confirmed path for long-running batch jobs
- [ ] CLI is only used for short smoke tests or short single-stock analysis
- [ ] Migration is documented locally for the target machine operator
