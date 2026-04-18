# Stock System Bundle

This directory is the curated, git-trackable snapshot of the local OpenClaw stock system.

Use `MIGRATION.md` as the primary runbook for another machine or another AI operator.
Use `CHECKLIST.md` as the step-by-step acceptance sheet during execution.
Use `QUICKSTART.md` for the shortest direct-execution path.

Contents:

- `config/`: sanitized templates (`openclaw.example.json`, `.env.example`, `jobs.example.json`)
- `data/`: database builder, schema, and the current `astock.db` snapshot
- `workspace*`: agent prompts, rules, and workflow scripts
- `core-repo/skills/stock-research/screener.py`: the repo-side screener snapshot required by the stock workflow
