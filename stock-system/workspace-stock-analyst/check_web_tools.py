#!/usr/bin/env python3
"""Check whether stock-analyst can use web_search in the current environment."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

CONFIG_PATH = Path('/home/<user>/.openclaw/openclaw.json')


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text(encoding='utf-8'))
    except Exception as exc:  # pragma: no cover - best effort helper
        print(f'WEB_SEARCH_UNAVAILABLE reason=config_parse_failed detail={exc}')
        sys.exit(1)


def main() -> int:
    cfg = load_config()
    tools = cfg.get('tools') or {}
    web = tools.get('web') or {}
    search = web.get('search') or {}

    enabled = search.get('enabled', True)
    provider = (search.get('provider') or 'brave').strip().lower()
    brave_key = (search.get('apiKey') or os.getenv('BRAVE_API_KEY') or '').strip()
    perplexity_cfg = search.get('perplexity') or {}
    perplexity_key = (
        perplexity_cfg.get('apiKey')
        or os.getenv('PERPLEXITY_API_KEY')
        or os.getenv('OPENROUTER_API_KEY')
        or ''
    ).strip()

    if enabled is False:
        print('WEB_SEARCH_UNAVAILABLE reason=disabled provider=' + provider)
        return 1

    if provider == 'perplexity':
        if perplexity_key:
            print('WEB_SEARCH_READY provider=perplexity source=' + ('config' if perplexity_cfg.get('apiKey') else 'env'))
            return 0
        print('WEB_SEARCH_UNAVAILABLE reason=missing_perplexity_api_key provider=perplexity')
        return 1

    if brave_key:
        print('WEB_SEARCH_READY provider=brave source=' + ('config' if search.get('apiKey') else 'env'))
        return 0

    if perplexity_key:
        print('WEB_SEARCH_FALLBACK_AVAILABLE provider=perplexity source=' + ('config' if perplexity_cfg.get('apiKey') else 'env'))
        return 2

    print('WEB_SEARCH_UNAVAILABLE reason=missing_brave_api_key provider=brave')
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
