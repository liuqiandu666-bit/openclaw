#!/usr/bin/env python3
import argparse
import datetime as dt
import glob
import json
import math
import os
import re
import sqlite3
from pathlib import Path

MEMORY_DIR = Path('/home/<user>/.openclaw/workspace/memory')
DIRECTOR_MEMORY_DIR = Path(__file__).resolve().parent / 'memory'
DB_PATH = '/home/<user>/.openclaw/workspace/data/astock.db'
PRICE_THRESHOLD_PCT = 8.0
PE_THRESHOLD_PCT = 15.0
MAX_REPORT_AGE_DAYS = 5
MAX_SNAPSHOT_STALE_DAYS = 2


def normalize_date(text):
    if text is None:
        return None
    text = str(text).strip().replace('‑', '-').replace('–', '-').replace('—', '-')
    m = re.search(r'(\d{4}-\d{2}-\d{2})', text)
    return m.group(1) if m else None


def round_half_up(value):
    if value is None:
        return None
    return int(math.floor(float(value) + 0.5))


def normalize_name(text):
    if not text:
        return None
    text = re.sub(r'^[#\s【\[]+', '', str(text).strip())
    text = re.sub(r'[】\]]+$', '', text)
    return text.strip(' -:：') or None


def infer_name_and_code(text, report_path):
    patterns = [
        re.compile(r'^###\s*(\d{6})\s+(.+)$', re.M),
        re.compile(r'^###\s*(.+?)[（(](\d{6})[）)]\s*$', re.M),
        re.compile(r'【\s*(.+?)[（(](\d{6})[）)]', re.M),
    ]
    for pattern in patterns:
        m = pattern.search(text)
        if not m:
            continue
        if pattern.pattern.startswith('^###\\s*(\\d{6})'):
            return normalize_name(m.group(2)), m.group(1)
        return normalize_name(m.group(1)), m.group(2)

    filename = Path(report_path).name
    m = re.search(r'stock-deep-(\d{6})-(\d{4}-\d{2}-\d{2})\.md$', filename)
    code = m.group(1) if m else None
    fallback = re.search(r'([\u4e00-\u9fffA-Za-z0-9()（）·]+)[（(](\d{6})[）)]', text)
    if fallback:
        return normalize_name(fallback.group(1)), fallback.group(2)
    return None, code


def extract_date_near_keywords(text, keywords):
    for keyword in keywords:
        pattern = re.compile(rf'{keyword}[^\n]{{0,40}}?(\d{{4}}[-‑–—]\d{{2}}[-‑–—]\d{{2}})')
        m = pattern.search(text)
        if m:
            return normalize_date(m.group(1))
    return None


def extract_date_by_patterns(text, patterns):
    for pattern in patterns:
        m = re.search(pattern, text, re.M)
        if m:
            return normalize_date(m.group(1))
    return None


def extract_deep_score(text):
    label = r'(?:综合(?:投资价值)?(?:得分|评分)?|综合投资价值评分)'
    patterns = [
        (re.compile(rf'\|\s*\*\*{label}\*\*\s*\|[^\n]*?\|\s*\*\*(\d+(?:\.\d+)?)\s*/\s*100\*\*\s*\|'), 1),
        (re.compile(rf'\|\s*\*\*{label}\*\*\s*\|[^\n]*?\|\s*\*\*(\d+(?:\.\d+)?)\s*/\s*10\*\*\s*\|'), 10),
        (re.compile(r'百分制(?:换算|转换)?[^\n]{0,60}?\*\*(\d+(?:\.\d+)?)\s*/\s*100\*\*'), 1),
        (re.compile(rf'{label}(?:（百分制）|百分制)?[^\n]{{0,40}}?(\d+(?:\.\d+)?)\s*/\s*100'), 1),
        (re.compile(rf'{label}(?:（百分制）|百分制)?[^\n]{{0,40}}?(\d+(?:\.\d+)?)\s*/\s*10'), 10),
        (re.compile(rf'\|\s*\*\*{label}\*\*\s*\|[^\n]*?\|\s*\*\*(\d+(?:\.\d+)?)\*\*\s*\|'), 1),
    ]
    for pattern, scale in patterns:
        m = pattern.search(text)
        if not m:
            continue
        value = safe_float(m.group(1))
        if value is None:
            continue
        return round_half_up(value * scale)
    return None


def load_latest_screener(date_str=None):
    if date_str:
        path = MEMORY_DIR / f'screener_result_{date_str}.json'
        if not path.exists():
            raise FileNotFoundError(path)
        with path.open('r', encoding='utf-8') as f:
            return json.load(f), path
    files = sorted(glob.glob(str(MEMORY_DIR / 'screener_result_*.json')))
    if not files:
        raise FileNotFoundError('No screener_result_*.json found')
    path = Path(files[-1])
    with path.open('r', encoding='utf-8') as f:
        return json.load(f), path


def list_matching_files(patterns):
    seen = set()
    results = []
    for pattern in patterns:
        for raw in sorted(glob.glob(str(MEMORY_DIR / pattern))):
            path = Path(raw)
            if path in seen:
                continue
            seen.add(path)
            results.append(path)
    return results


def build_daily_memory_note(date_str=None, output_path=None):
    date_str = normalize_date(date_str) or dt.date.today().isoformat()
    DIRECTOR_MEMORY_DIR.mkdir(parents=True, exist_ok=True)
    out = Path(output_path) if output_path else DIRECTOR_MEMORY_DIR / f'{date_str}.md'

    today_patterns = [
        f'screener_result_{date_str}.json',
        f'stock-pick-{date_str}.md',
        f'stock-pick-top*-{date_str}.md',
        f'stock-rerank-top*-{date_str}.md',
        f'stock-fetch-status-{date_str}.md',
        f'stock-report-index-{date_str}.txt',
        f'stock-report-index-{date_str}.tsv',
        f'stock-deep-*-{date_str}.md',
        f'qa-log-{date_str}.md',
    ]
    today_files = list_matching_files(today_patterns)

    recent_patterns = [
        'stock-pick-*.md',
        'stock-pick-top*-*.md',
        'stock-rerank-top*-*.md',
        'stock-fetch-status-*.md',
        'stock-report-index-*.txt',
        'stock-report-index-*.tsv',
        'qa-log-*.md',
    ]
    recent_files = list_matching_files(recent_patterns)
    recent_files = sorted(recent_files, key=lambda path: path.stat().st_mtime, reverse=True)
    recent_files = [path for path in recent_files if path.name not in {item.name for item in today_files}][:8]

    director_today = sorted(DIRECTOR_MEMORY_DIR.glob(f'*{date_str}*'))
    director_today = [path for path in director_today if path.name != out.name]

    lines = [
        f'# {date_str} 工作日志',
        '',
        f'- 生成时间：{dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '- 说明：这是总监工作区的自动占位日志，用来避免新的一天首轮批量任务因缺少当日日志而直接读文件失败。',
        '',
        '## 今日共享产物',
    ]
    if today_files:
        lines.extend([f'- `{path.name}`' for path in today_files])
    else:
        lines.append('- 今日尚未发现共享 memory 产物。')

    lines.extend([
        '',
        '## 最近可用共享产物',
    ])
    if recent_files:
        for path in recent_files:
            modified = dt.datetime.fromtimestamp(path.stat().st_mtime).strftime('%Y-%m-%d %H:%M')
            lines.append(f'- `{path.name}`（更新于 {modified}）')
    else:
        lines.append('- 暂无历史共享产物。')

    lines.extend([
        '',
        '## 总监工作区现状',
    ])
    if director_today:
        lines.extend([f'- `{path.name}`' for path in director_today])
    else:
        lines.append('- 今日尚未生成总监侧补充记录。')

    lines.extend([
        '',
        '## 启动建议',
        '- 若今日已有 `stock-fetch-status-YYYY-MM-DD.md`，优先据此回答数据更新或空输出兜底问题。',
        '- 若今日已有 `stock-pick-YYYY-MM-DD.md` 或 `stock-pick-topN-YYYY-MM-DD.md`，优先据此推进批量筛选/复排流程。',
        '- 若今日仍无共享产物，把这份文件视为占位上下文即可，不要因为缺少当日日志而中断首轮任务。',
    ])

    out.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return {
        'date': date_str,
        'output_path': str(out),
        'today_files': [str(path) for path in today_files],
        'recent_files': [str(path) for path in recent_files],
        'director_files': [str(path) for path in director_today],
    }


def find_latest_report(code):
    files = sorted(glob.glob(str(MEMORY_DIR / f'stock-deep-{code}-*.md')))
    return Path(files[-1]) if files else None


def report_meta_path(report_path):
    report_path = resolve_report_path(report_path)
    return report_path.with_suffix('.meta.json')


def resolve_report_path(report_path):
    report_path = Path(report_path)
    if report_path.is_absolute():
        return report_path
    if str(report_path).startswith('memory/'):
        return MEMORY_DIR / report_path.name
    return MEMORY_DIR / report_path.name


def current_state(code):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT price, pe_ttm, snap_date FROM market_snapshot WHERE code=? ORDER BY snap_date DESC LIMIT 1",
        (code,),
    ).fetchone()
    snap = {'price': row[0], 'pe_ttm': row[1], 'market_snap_date': row[2]} if row else {}
    fin_dates = []
    for table in ('income_stmt', 'balance_sheet', 'cash_flow'):
        value = conn.execute(f"SELECT MAX(report_date) FROM {table} WHERE code=?", (code,)).fetchone()[0]
        if value:
            fin_dates.append(value)
    exec_cutoff = conn.execute("SELECT MAX(cutoff_date) FROM executive_hold WHERE code=?", (code,)).fetchone()[0]
    conn.close()
    return {
        'financial_report_date': max(fin_dates) if fin_dates else None,
        'exec_cutoff_date': exec_cutoff,
        **snap,
    }


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def fmt_pct(value):
    return None if value is None else round(value, 2)


def fmt_value(value, digits=1):
    if value is None:
        return '-'
    try:
        return f'{float(value):.{digits}f}'
    except Exception:
        return str(value)


def fmt_pair(latest, weighted, digits=1):
    latest_text = fmt_value(latest, digits)
    weighted_text = fmt_value(weighted, digits)
    if latest_text == '-' and weighted_text == '-':
        return '-'
    return f'{latest_text} / {weighted_text}'


def format_metric_columns(metrics):
    metrics = metrics or {}
    return {
        'contract': fmt_pair(metrics.get('contract_liability_yoy'), metrics.get('contract_liability_yoy_3q')),
        'ocf_ni': fmt_pair(metrics.get('ocf_ni_ratio'), metrics.get('ocf_ni_ratio_3q'), 2),
        'operating_cf': fmt_pair(metrics.get('operating_cashflow_yoy'), metrics.get('operating_cashflow_yoy_3q')),
        'gross_margin': fmt_pair(metrics.get('gross_margin_improvement_3q'), metrics.get('gross_margin_pass_ratio_3q')),
        'capex': fmt_pair(metrics.get('capex_yoy'), metrics.get('capex_yoy_3q')),
        'pe_ttm': fmt_value(metrics.get('pe_ttm'), 1),
    }


def sort_screener_results(items):
    return sorted(
        items,
        key=lambda item: (
            -(item.get('composite_score') or -9999),
            -(item.get('quant_score') or -9999),
            -(item.get('exec_hold_score') or -9999),
            item.get('code') or '',
        ),
    )


def days_between(date_text, today=None):
    date_text = normalize_date(date_text)
    if not date_text:
        return None
    try:
        target = dt.date.fromisoformat(date_text)
    except ValueError:
        return None
    today = today or dt.date.today()
    return (today - target).days


def parse_report(report_path):
    report_path = resolve_report_path(report_path)
    text = report_path.read_text(encoding='utf-8', errors='ignore')
    inferred_name, inferred_code = infer_name_and_code(text, report_path)
    data = {
        'report_path': str(resolve_report_path(report_path)),
        'generated_at': dt.datetime.fromtimestamp(report_path.stat().st_mtime).isoformat(),
        'report_date': normalize_date(report_path.name),
        'name': inferred_name,
        'code': inferred_code,
        'price': None,
        'pe_ttm': None,
        'market_snap_date': None,
        'financial_report_date': None,
        'exec_cutoff_date': None,
        'deep_score': None,
        'conclusion': None,
        'highlight': None,
        'risk': None,
    }
    m = re.search(r'当前股价[:：]\s*([0-9.]+)\s*元', text)
    if m:
        data['price'] = safe_float(m.group(1))
    m = re.search(r'PE\s*(?:（|\()\s*TTM\s*(?:）|\))[:：]?\s*([0-9.]+)', text)
    if m:
        data['pe_ttm'] = safe_float(m.group(1))
    data['market_snap_date'] = extract_date_by_patterns(text, [
        r'基于本地数据库\s*(\d{4}[-‑–—]\d{2}[-‑–—]\d{2})\s*(?:行情)?快照',
        r'数据来源[:：]\s*本地数据库[^\n]{0,60}?(\d{4}[-‑–—]\d{2}[-‑–—]\d{2})',
        r'估值面(?:分析)?[^\n]{0,40}?数据截至\s*(\d{4}[-‑–—]\d{2}[-‑–—]\d{2})',
    ])
    data['financial_report_date'] = extract_date_by_patterns(text, [
        r'最新财务数据截至\s*(\d{4}[-‑–—]\d{2}[-‑–—]\d{2})',
        r'具体依据（数据截至\s*(\d{4}[-‑–—]\d{2}[-‑–—]\d{2})）',
        r'最新财务数据截至\s*(\d{4}[-‑–—]\d{2}[-‑–—]\d{2})',
    ])
    data['exec_cutoff_date'] = extract_date_by_patterns(text, [
        r'executive_hold[^\n]{0,30}?(\d{4}[-‑–—]\d{2}[-‑–—]\d{2})',
        r'高管[增减]?持[^\n]{0,30}?(\d{4}[-‑–—]\d{2}[-‑–—]\d{2})',
    ])
    m = re.search(r'\*\*入场时机结论\*\*[:：](.+)', text)
    if m:
        data['conclusion'] = m.group(1).strip()
    data['deep_score'] = extract_deep_score(text)
    m = re.search(r'\*\*基本面节点\*\*[\s\S]*?\n-\s+(.+)', text)
    if m:
        data['highlight'] = m.group(1).strip()
    m = re.search(r'\|\s*行业风险\s*\|\s*([^|]+)\|', text)
    if m:
        data['risk'] = m.group(1).strip()
    else:
        m = re.search(r'\*\*主要不确定因素\*\*[\s\S]*?\n-\s+(.+)', text)
        if m:
            data['risk'] = m.group(1).strip()
    return data


def load_or_build_meta(report_path):
    meta_path = report_meta_path(report_path)
    parsed = parse_report(report_path)
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding='utf-8'))
            merged = {**meta, **{k: v for k, v in parsed.items() if v not in (None, '', [])}}
            important = ('deep_score', 'conclusion', 'highlight', 'risk', 'financial_report_date', 'market_snap_date')
            if any(merged.get(k) in (None, '') for k in important):
                merged = {**parsed, **meta, **{k: v for k, v in parsed.items() if v not in (None, '', [])}}
            meta_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding='utf-8')
            return merged, meta_path
        except Exception:
            pass
    meta = parsed
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    return meta, meta_path


def decision_for_reuse(code, mode):
    report_path = find_latest_report(code)
    if mode == 'force':
        return {'decision': 'refresh', 'reason': 'force mode', 'report_path': str(resolve_report_path(report_path)) if report_path else None}
    if report_path is None:
        return {'decision': 'refresh', 'reason': 'no existing report', 'report_path': None}

    meta, meta_path = load_or_build_meta(report_path)
    now = current_state(code)
    reasons = []
    today_obj = dt.date.today()
    today = today_obj.isoformat()
    report_same_day = normalize_date(meta.get('report_date')) == today

    report_age_days = days_between(meta.get('report_date'), today_obj)
    if report_age_days is not None and report_age_days > MAX_REPORT_AGE_DAYS:
        reasons.append(f'report older than {MAX_REPORT_AGE_DAYS} days: {report_age_days}d')

    current_snap_date = normalize_date(now.get('market_snap_date'))
    cached_snap_date = normalize_date(meta.get('market_snap_date'))
    snap_age_days = days_between(current_snap_date, today_obj)
    same_day_same_snapshot = (
        report_same_day
        and current_snap_date
        and cached_snap_date
        and current_snap_date == cached_snap_date
    )
    if snap_age_days is not None and snap_age_days >= MAX_SNAPSHOT_STALE_DAYS and not same_day_same_snapshot:
        reasons.append(f'market snapshot stale: {current_snap_date}')

    cached_fin = normalize_date(meta.get('financial_report_date'))
    current_fin = normalize_date(now.get('financial_report_date'))
    if current_fin and cached_fin and current_fin > cached_fin:
        reasons.append(f'financial report updated: {cached_fin} -> {current_fin}')
    elif current_fin and not cached_fin and not report_same_day:
        reasons.append('missing cached financial date')

    cached_exec = normalize_date(meta.get('exec_cutoff_date'))
    current_exec = normalize_date(now.get('exec_cutoff_date'))
    if current_exec and cached_exec and current_exec > cached_exec:
        reasons.append(f'exec hold updated: {cached_exec} -> {current_exec}')
    elif current_exec and not cached_exec and not report_same_day:
        reasons.append('missing cached executive cutoff date')

    cached_price = safe_float(meta.get('price'))
    current_price = safe_float(now.get('price'))
    price_change = None
    if cached_price and current_price:
        price_change = abs(current_price - cached_price) / cached_price * 100.0
        if price_change >= PRICE_THRESHOLD_PCT:
            reasons.append(f'price changed {price_change:.2f}%')
    elif not report_same_day:
        reasons.append('missing cached price')

    cached_pe = safe_float(meta.get('pe_ttm'))
    current_pe = safe_float(now.get('pe_ttm'))
    pe_change = None
    if cached_pe and current_pe:
        pe_change = abs(current_pe - cached_pe) / cached_pe * 100.0
        if pe_change >= PE_THRESHOLD_PCT:
            reasons.append(f'PE changed {pe_change:.2f}%')
    elif not report_same_day:
        reasons.append('missing cached PE')

    decision = 'reuse' if not reasons else 'refresh'
    return {
        'decision': decision,
        'reason': '; '.join(reasons) if reasons else 'financial/market data unchanged within thresholds',
        'report_path': str(report_path),
        'meta_path': str(meta_path),
        'price_change_pct': fmt_pct(price_change),
        'pe_change_pct': fmt_pct(pe_change),
        'report_age_days': report_age_days,
        'market_snap_age_days': snap_age_days,
        **meta,
        **now,
    }


def build_initial(top_n, date_str=None):
    data, source = load_latest_screener(date_str)
    date_str = data['date']
    top_n = max(1, min(int(top_n), 30))
    sorted_results = sort_screener_results(data.get('results', []))
    items = []
    for idx, item in enumerate(sorted_results[:top_n], 1):
        metric_cols = format_metric_columns(item.get('metrics'))
        items.append({
            'initial_rank': idx,
            'code': item['code'],
            'name': item['name'],
            'industry': item['industry'],
            'initial_score': item['composite_score'],
            'quant_score': item['quant_score'],
            'exec_score': item['exec_hold_score'],
            'category': item.get('b_class') or 'A类',
            **metric_cols,
        })
    out = MEMORY_DIR / f'stock-pick-top{top_n}-{date_str}.md'
    lines = [
        f'# 初筛排名（前{top_n}）',
        '',
        f'数据截至：{date_str}',
        f'来源文件：{source.name}',
        '排序规则：按初筛综合分降序；同分时按量化分、高管分降序。',
        '',
        '| 初筛排名 | 代码 | 名称 | 行业 | 初筛综合分 | 量化分 | 高管分 | 类别 | 合同负债同比(最新/3Q) | OCF/NI(最新/3Q) | 经营现金流同比(最新/3Q) | 毛利率改善(3Q改善/通过率) | CAPEX同比(最新/3Q) | PE(TTM) |',
        '|----------|------|------|------|------------|--------|--------|------|------------------------|------------------|----------------------------|------------------------------|----------------------|---------|',
    ]
    for item in items:
        lines.append(
            f"| {item['initial_rank']} | {item['code']} | {item['name']} | {item['industry']} | {item['initial_score']} | {item['quant_score']} | {item['exec_score']} | {item['category']} | {item['contract']} | {item['ocf_ni']} | {item['operating_cf']} | {item['gross_margin']} | {item['capex']} | {item['pe_ttm']} |"
        )
    out.write_text('\n'.join(lines), encoding='utf-8')
    print(json.dumps({'date': date_str, 'top_n': top_n, 'output_path': str(out), 'items': items}, ensure_ascii=False))


def parse_index_line(line):
    line = line.strip()
    if not line:
        return None
    if '\t' in line:
        parts = line.split('\t')
        if len(parts) >= 10:
            if parts[0] == '代码':
                return None
            return {
                'code': parts[0], 'name': parts[1], 'initial_rank': int(parts[2]), 'initial_score': safe_float(parts[3]),
                'deep_score': safe_float(parts[4]), 'conclusion': parts[5], 'highlight': parts[6], 'risk': parts[7],
                'report_path': parts[8], 'status': parts[9],
            }
    if ': ' in line:
        code, report_path = line.split(': ', 1)
        return {'code': code.strip(), 'report_path': report_path.strip(), 'status': 'legacy'}
    return None


def load_index_entries(date_str):
    entries = {}
    index_paths = [
        MEMORY_DIR / f'stock-report-index-{date_str}.tsv',
        MEMORY_DIR / f'stock-report-index-{date_str}.txt',
    ]
    for index_path in index_paths:
        if not index_path.exists():
            continue
        for raw in index_path.read_text(encoding='utf-8').splitlines():
            parsed = parse_index_line(raw)
            if not parsed:
                continue
            code = parsed['code']
            current = entries.get(code)
            if current is None:
                entries[code] = parsed
                continue
            current_score = sum(1 for k in ('deep_score', 'conclusion', 'highlight', 'risk') if current.get(k) not in (None, '', '-'))
            parsed_score = sum(1 for k in ('deep_score', 'conclusion', 'highlight', 'risk') if parsed.get(k) not in (None, '', '-'))
            current_bonus = 1 if current.get('status') not in ('legacy', None, '') else 0
            parsed_bonus = 1 if parsed.get('status') not in ('legacy', None, '') else 0
            if parsed_score + parsed_bonus >= current_score + current_bonus:
                entries[code] = parsed
    return entries


def make_index_line(parsed, status):
    return '\t'.join([
        parsed['code'],
        parsed.get('name') or parsed['code'],
        str(parsed.get('initial_rank') or ''),
        str(parsed.get('initial_score') or ''),
        str(parsed.get('deep_score') or ''),
        parsed.get('conclusion') or '',
        parsed.get('highlight') or '',
        parsed.get('risk') or '',
        parsed.get('report_path') or '',
        status,
    ])


def resume_batch(top_n, mode='auto', date_str=None):
    data, _ = load_latest_screener(date_str)
    date_str = data['date']
    top_n = max(1, min(int(top_n), 50))
    sorted_results = sort_screener_results(data.get('results', []))
    target_items = []
    for idx, item in enumerate(sorted_results[:top_n], 1):
        target_items.append({
            'initial_rank': idx,
            'code': item['code'],
            'name': item['name'],
            'industry': item.get('industry'),
            'initial_score': item.get('composite_score'),
            'quant_score': item.get('quant_score'),
            'exec_score': item.get('exec_hold_score'),
            'metrics': item.get('metrics') or {},
        })

    indexed = load_index_entries(date_str)
    completed = []
    pending = []
    recovered_index_lines = []

    for item in target_items:
        code = item['code']
        existing = indexed.get(code)
        if existing and existing.get('status') in ('reused', 'fresh', 'failed'):
            merged = {**item, **existing}
            completed.append(merged)
            continue

        decision = decision_for_reuse(code, mode)
        decision.update({
            'code': code,
            'name': decision.get('name') or item['name'],
            'initial_rank': item['initial_rank'],
            'initial_score': item['initial_score'],
        })
        if decision.get('decision') == 'reuse':
            recovered_index_lines.append(decision['index_line'])
            completed.append({
                **item,
                **decision,
                'status': 'reused',
            })
        else:
            pending.append({
                **item,
                **decision,
            })

    next_batch = pending[:5]
    result = {
        'date': date_str,
        'top_n': top_n,
        'mode': mode,
        'completed_count': len(completed),
        'pending_count': len(pending),
        'indexed_count': len(indexed),
        'recovered_index_lines': recovered_index_lines,
        'completed': completed,
        'pending': pending,
        'next_batch': next_batch,
        'index_path': str(MEMORY_DIR / f'stock-report-index-{date_str}.txt'),
        'rerank_path': str(MEMORY_DIR / f'stock-rerank-top{top_n}-{date_str}.md'),
        'top_pick_path': str(MEMORY_DIR / f'stock-pick-top{top_n}-{date_str}.md'),
    }
    print(json.dumps(result, ensure_ascii=False))


def build_rerank(top_n, date_str=None):
    data, _ = load_latest_screener(date_str)
    date_str = data['date']
    top_n = max(1, min(int(top_n), 30))
    sorted_results = sort_screener_results(data.get('results', []))
    initial = sorted_results[:top_n]
    initial_map = {}
    for idx, item in enumerate(sorted_results, 1):
        initial_map[item['code']] = {
            'initial_rank': idx,
            'initial_score': item['composite_score'],
            'name': item['name'],
            'quant_score': item.get('quant_score'),
            'exec_score': item.get('exec_hold_score'),
            'category': item.get('b_class') or 'A类',
            'metrics': item.get('metrics') or {},
            'targeted': idx <= top_n,
        }
    entries = load_index_entries(date_str)
    for code, parsed in list(entries.items()):
        if code not in initial_map or not initial_map[code]['targeted']:
            continue
        if parsed.get('report_path') and parsed.get('report_path') != '无报告':
            meta, _ = load_or_build_meta(Path(parsed['report_path']))
            parsed.update({
                'name': meta.get('name') or parsed.get('name') or initial_map[code]['name'],
                'deep_score': parsed.get('deep_score') if parsed.get('deep_score') is not None else meta.get('deep_score'),
                'conclusion': parsed.get('conclusion') or meta.get('conclusion') or '',
                'highlight': parsed.get('highlight') or meta.get('highlight') or '',
                'risk': parsed.get('risk') or meta.get('risk') or '',
            })
            entries[code] = parsed
    rows = []
    for item in sorted_results:
        code = item['code']
        base = initial_map[code]
        entry = entries.get(code, {})
        metric_cols = format_metric_columns(base.get('metrics'))
        final_score = entry.get('deep_score')
        targeted = base['targeted']
        if targeted:
            status = entry.get('status') or 'pending'
        else:
            status = 'not_researched'
        row = {
            'initial_rank': base['initial_rank'],
            'code': code,
            'name': entry.get('name') or item['name'],
            'initial_score': base['initial_score'],
            'quant_score': base['quant_score'],
            'exec_score': base['exec_score'],
            'deep_score': final_score,
            'final_score': final_score,
            'conclusion': entry.get('conclusion') or ('待补充' if targeted else '未纳入本轮深析'),
            'highlight': entry.get('highlight') or '-',
            'risk': entry.get('risk') or '-',
            'report_path': entry.get('report_path') or ('无报告' if targeted else '-'),
            'status': status,
            'targeted': targeted,
            **metric_cols,
        }
        rows.append(row)
    rows.sort(
        key=lambda x: (
            x['final_score'] is None,
            -(x['final_score'] if x['final_score'] is not None else x['initial_score']),
            x['initial_rank'],
        )
    )
    for idx, row in enumerate(rows, 1):
        row['rerank'] = idx
    out = MEMORY_DIR / f'stock-rerank-top{top_n}-{date_str}.md'
    targeted_count = sum(1 for row in rows if row['targeted'])
    scored_count = sum(1 for row in rows if row['final_score'] is not None)
    unresearched_count = len(rows) - targeted_count
    pending_count = targeted_count - scored_count
    lines = [
        f'# 深析复排结果（前{top_n}）',
        '',
        f'数据截至：{date_str}',
        f'本轮深析范围：初筛综合分排名前{top_n}名。',
        '排序规则：有最终综合评分（深析综合评分）的标的按最终综合评分降序；未深析标的因无最终综合评分，排在后面并按初筛综合分降序列示。',
        f'统计：本轮目标深析/复用 {targeted_count} 只，已形成最终综合评分 {scored_count} 只，仍在等待结果 {pending_count} 只，未纳入本轮深析但仍符合初筛条件 {unresearched_count} 只。',
        '',
        '| 复排 | 初筛 | 代码 | 名称 | 初筛分 | 量化分 | 高管分 | 最终综合分 | 状态 | 结论 | 合同负债同比(最新/3Q) | OCF/NI(最新/3Q) | 经营现金流同比(最新/3Q) | 毛利率改善(3Q改善/通过率) | CAPEX同比(最新/3Q) | PE(TTM) | 报告路径 |',
        '|------|------|------|------|--------|--------|--------|------------|------|------|------------------------|------------------|----------------------------|------------------------------|----------------------|---------|----------|',
    ]
    for row in rows:
        lines.append(
            f"| {row['rerank']} | {row['initial_rank']} | {row['code']} | {row['name']} | {row['initial_score']} | {row['quant_score']} | {row['exec_score']} | {row['final_score'] if row['final_score'] is not None else '-'} | {row['status']} | {row['conclusion']} | {row['contract']} | {row['ocf_ni']} | {row['operating_cf']} | {row['gross_margin']} | {row['capex']} | {row['pe_ttm']} | {row['report_path']} |"
        )
    out.write_text('\n'.join(lines), encoding='utf-8')
    print(json.dumps({'date': date_str, 'top_n': top_n, 'output_path': str(out), 'rows': rows}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description='Stock batch workflow helpers')
    sub = parser.add_subparsers(dest='cmd', required=True)

    p = sub.add_parser('build-initial')
    p.add_argument('--top-n', type=int, required=True)
    p.add_argument('--date')

    p = sub.add_parser('reuse-check')
    p.add_argument('--code', required=True)
    p.add_argument('--mode', choices=['auto', 'force'], default='auto')
    p.add_argument('--initial-rank', type=int)
    p.add_argument('--initial-score', type=float)
    p.add_argument('--name')

    p = sub.add_parser('build-rerank')
    p.add_argument('--top-n', type=int, required=True)
    p.add_argument('--date')

    p = sub.add_parser('resume-batch')
    p.add_argument('--top-n', type=int, required=True)
    p.add_argument('--mode', choices=['auto', 'force'], default='auto')
    p.add_argument('--date')

    p = sub.add_parser('ensure-daily-memory')
    p.add_argument('--date')
    p.add_argument('--output')

    args = parser.parse_args()
    if args.cmd == 'build-initial':
        build_initial(args.top_n, args.date)
    elif args.cmd == 'reuse-check':
        result = decision_for_reuse(args.code, args.mode)
        result['code'] = args.code
        if args.initial_rank is not None:
            result['initial_rank'] = args.initial_rank
        if args.initial_score is not None:
            result['initial_score'] = args.initial_score
        if args.name:
            result['name'] = result.get('name') or args.name
        if result.get('decision') == 'reuse' and args.initial_rank is not None and args.initial_score is not None:
            result['index_line'] = '	'.join([
                args.code,
                result.get('name') or args.name or args.code,
                str(args.initial_rank),
                str(args.initial_score),
                str(result.get('deep_score') or ''),
                result.get('conclusion') or '',
                result.get('highlight') or '',
                result.get('risk') or '',
                result.get('report_path') or '',
                'reused',
            ])
        print(json.dumps(result, ensure_ascii=False))
    elif args.cmd == 'ensure-daily-memory':
        print(json.dumps(build_daily_memory_note(args.date, args.output), ensure_ascii=False))
    elif args.cmd == 'resume-batch':
        resume_batch(args.top_n, args.mode, args.date)
    else:
        build_rerank(args.top_n, args.date)


if __name__ == '__main__':
    main()
