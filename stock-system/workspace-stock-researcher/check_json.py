import json

with open('/home/<user>/.openclaw/workspace/memory/screener_result_2026-04-18.json') as f:
    data = json.load(f)

print('达标总数:', data['summary']['qualified'])
print('前30名:')
for i, c in enumerate(data['results'][:30]):
    print(f"{i+1}. {c['code']} {c['name']} [{c['industry']}] quant={c['quant_score']} exec={c['exec_hold_score']} composite={c['composite_score']} passed={c['passed_count']} b_class={c.get('b_class', 'None')}")