import sqlite3
import datetime

DB = "/home/<user>/.openclaw/workspace/data/astock.db"
conn = sqlite3.connect(DB)

# 检查快照日期
snap_date = conn.execute("SELECT MAX(snap_date) FROM market_snapshot").fetchone()[0]
exec_dt = conn.execute("SELECT MAX(cutoff_date) FROM executive_hold").fetchone()[0]
conn.close()

today = datetime.date.today()
snap = datetime.datetime.strptime(snap_date, "%Y-%m-%d").date() if snap_date else None
exec_date = datetime.datetime.strptime(exec_dt, "%Y-%m-%d").date() if exec_dt else None

print(f"快照日期: {snap_date}  高管数据: {exec_dt}")
if snap:
    delta = (today - snap).days
    if delta > 7:
        print(f"⚠️ 快照过期（截至 {snap_date}，距今 {delta} 天）")
    else:
        print(f"✅ 快照新鲜（{delta} 天前）")
else:
    print("⚠️ 无快照数据")

if exec_date:
    delta2 = (today - exec_date).days
    print(f"高管数据 {delta2} 天前")
else:
    print("⚠️ 无高管数据")