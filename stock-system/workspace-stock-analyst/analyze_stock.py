import sqlite3
import pandas as pd
import numpy as np

# 连接到数据库
db_path = '/home/<user>/.openclaw/workspace/data/astock.db'
conn = sqlite3.connect(db_path)

# 获取最新市场数据
market = pd.read_sql("SELECT * FROM market_snapshot WHERE code='601778' ORDER BY snap_date DESC LIMIT 1;", conn)
# 获取最新财务数据（2025Q3）
balance = pd.read_sql("SELECT * FROM balance_sheet WHERE code='601778' ORDER BY report_date DESC LIMIT 1;", conn)
income = pd.read_sql("SELECT * FROM income_stmt WHERE code='601778' ORDER BY report_date DESC LIMIT 1;", conn)
cashflow = pd.read_sql("SELECT * FROM cash_flow WHERE code='601778' ORDER BY report_date DESC LIMIT 1;", conn)

# 获取历史数据用于趋势分析
balance_history = pd.read_sql("SELECT * FROM balance_sheet WHERE code='601778' ORDER BY report_date DESC LIMIT 8;", conn)
income_history = pd.read_sql("SELECT * FROM income_stmt WHERE code='601778' ORDER BY report_date DESC LIMIT 8;", conn)

print("=== 晶科科技(601778) 财务分析 ===")
print(f"当前股价: ¥{market['price'].values[0]} (截至 {market['snap_date'].values[0]})")
print(f"PE(TTM): {market['pe_ttm'].values[0]:.2f}")
print(f"PB: {market['pb'].values[0]:.2f}")

# 计算关键财务指标
latest_date = balance['report_date'].values[0]
print(f"\n=== 最新财务数据 ({latest_date}) ===")

# 1. 盈利能力
revenue = income['operate_income'].values[0]
net_profit = income['netprofit'].values[0]
gross_margin = income['gross_margin'].values[0]
operate_profit = income['operate_profit'].values[0]
net_margin = net_profit / revenue * 100 if revenue > 0 else 0
operate_margin = operate_profit / revenue * 100 if revenue > 0 else 0

print(f"营业收入: ¥{revenue/1e8:.2f}亿元")
print(f"净利润: ¥{net_profit/1e8:.2f}亿元")
print(f"毛利率: {gross_margin:.2f}%")
print(f"净利率: {net_margin:.2f}%")
print(f"营业利润率: {operate_margin:.2f}%")

# 2. 资产负债结构
total_assets = balance['total_assets'].values[0]
total_liab = balance['total_liab'].values[0]
total_equity = balance['total_equity'].values[0]
debt_ratio = total_liab / total_assets * 100
equity_ratio = total_equity / total_assets * 100

print(f"\n总资产: ¥{total_assets/1e8:.2f}亿元")
print(f"总负债: ¥{total_liab/1e8:.2f}亿元")
print(f"净资产: ¥{total_equity/1e8:.2f}亿元")
print(f"资产负债率: {debt_ratio:.2f}%")
print(f"权益比率: {equity_ratio:.2f}%")

# 3. 营运能力（简化）
# 计算ROE
roe = net_profit / total_equity * 100 if total_equity > 0 else 0
print(f"ROE(单季年化): {roe:.2f}%")

# 4. 现金流分析
netcash_operate = cashflow['netcash_operate'].values[0]
netcash_invest = cashflow['netcash_invest'].values[0]
netcash_finance = cashflow['netcash_finance'].values[0]

print(f"\n经营活动现金流: ¥{netcash_operate/1e8:.2f}亿元")
print(f"投资活动现金流: ¥{netcash_invest/1e8:.2f}亿元")
print(f"筹资活动现金流: ¥{netcash_finance/1e8:.2f}亿元")

# 5. 估值计算
price = market['price'].values[0]
total_equity = balance['total_equity'].values[0]
# 计算每股净资产
# 需要总股本数据，假设从市场数据推断
# 如果没有总市值，尝试估算
if not pd.isna(market['total_mktcap'].values[0]):
    total_mktcap = market['total_mktcap'].values[0]
else:
    # 使用PB估算
    pb = market['pb'].values[0]
    total_mktcap = total_equity * pb

print(f"\n=== 估值分析 ===")
print(f"总市值估算: ¥{total_mktcap/1e8:.2f}亿元")
print(f"净资产: ¥{total_equity/1e8:.2f}亿元")
print(f"PB(市净率): {market['pb'].values[0]:.2f}")

# 计算PE
pe = market['pe_ttm'].values[0]
print(f"PE(TTM): {pe:.2f}")

# 历史趋势分析
print("\n=== 历史趋势分析 ===")
print("营业收入趋势:")
for idx, row in income_history.iterrows():
    print(f"  {row['report_date']}: ¥{row['operate_income']/1e8:.2f}亿元, 净利润: ¥{row['netprofit']/1e8:.2f}亿元, 毛利率: {row['gross_margin']:.2f}%")

print("\n资产负债趋势:")
for idx, row in balance_history.iterrows():
    debt_ratio = row['total_liab'] / row['total_assets'] * 100
    print(f"  {row['report_date']}: 总资产¥{row['total_assets']/1e8:.2f}亿元, 负债率: {debt_ratio:.2f}%")

conn.close()

# 基于数据给出初步判断
print("\n=== 初步分析 ===")
print("1. 财务健康状况:")
if debt_ratio > 70:
    print("   警告：资产负债率较高 (>70%)")
elif debt_ratio > 60:
    print("   注意：资产负债率偏高 (60-70%)")
else:
    print("   良好：资产负债率合理")

print("\n2. 盈利能力:")
if net_margin < 5:
    print("   较低：净利率低于5%")
else:
    print("   合理：净利率可接受")

print("\n3. 估值水平:")
if pe > 30:
    print("   较高：PE(TTM)超过30倍")
elif pe > 20:
    print("   适中：PE(TTM)在20-30倍")
else:
    print("   较低：PE(TTM)低于20倍")

print("\n4. 现金流:")
if netcash_operate > 0:
    print("   良好：经营活动现金流为正")
else:
    print("   警示：经营活动现金流为负")