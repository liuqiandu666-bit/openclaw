import sqlite3
import pandas as pd

db_path = '/home/<user>/.openclaw/workspace/data/astock.db'
conn = sqlite3.connect(db_path)

code = '688195'

# 获取市场快照
market_df = pd.read_sql_query(f"""
    SELECT price, total_mktcap, float_mktcap, pb, pe_ttm
    FROM market_snapshot 
    WHERE code = '{code}' 
    ORDER BY snap_date DESC 
    LIMIT 1
""", conn)

# 获取资产负债表最新数据（计算净资产）
balance_df = pd.read_sql_query(f"""
    SELECT total_equity
    FROM balance_sheet 
    WHERE code = '{code}' 
    ORDER BY report_date DESC 
    LIMIT 1
""", conn)

# 获取利润表最新数据（计算EPS）
income_df = pd.read_sql_query(f"""
    SELECT basic_eps, netprofit
    FROM income_stmt 
    WHERE code = '{code}' 
    ORDER BY report_date DESC 
    LIMIT 1
""", conn)

conn.close()

print("=== 关键指标计算 ===\n")

if not market_df.empty:
    price = market_df.iloc[0]['price']
    pb = market_df.iloc[0]['pb']
    pe_ttm = market_df.iloc[0]['pe_ttm']
    
    print(f"当前价格: ¥{price:.2f}")
    print(f"PB: {pb:.2f}")
    print(f"PE(TTM): {pe_ttm:.2f}")
    
    # 计算总股本（如果总市值已知）
    if pd.notna(market_df.iloc[0]['total_mktcap']):
        total_mktcap = market_df.iloc[0]['total_mktcap']
        print(f"总市值: {total_mktcap/1e8:.2f} 亿元")
        # 计算总股本
        total_shares = total_mktcap / price
        print(f"总股本: {total_shares/1e6:.2f} 百万股")
    else:
        # 通过PB和净资产计算
        if not balance_df.empty:
            total_equity = balance_df.iloc[0]['total_equity']
            print(f"净资产: {total_equity/1e8:.2f} 亿元")
            # 计算总市值 = PB * 净资产
            estimated_mktcap = pb * total_equity
            print(f"估算总市值: {estimated_mktcap/1e8:.2f} 亿元")
            total_shares = estimated_mktcap / price
            print(f"估算总股本: {total_shares/1e6:.2f} 百万股")
            
            # 计算每股净资产
            bps = total_equity / total_shares
            print(f"每股净资产(BPS): ¥{bps:.2f}")
            
            # 验证PB计算
            print(f"验证PB: {price/bps:.2f} (应为 {pb})")

if not income_df.empty:
    basic_eps = income_df.iloc[0]['basic_eps']
    netprofit = income_df.iloc[0]['netprofit']
    print(f"\n最新报告期基本EPS: {basic_eps}")
    print(f"最新报告期净利润: {netprofit/1e6:.2f} 百万元")
    
    # 如果知道总股本，可以计算每股收益
    if 'total_shares' in locals():
        eps_calculated = netprofit / total_shares
        print(f"计算EPS: ¥{eps_calculated:.2f}")
        print(f"与basic_eps差异: {(eps_calculated - basic_eps)/basic_eps*100:.2f}%")
        
        # 计算PE
        pe_calculated = price / eps_calculated
        print(f"计算PE: {pe_calculated:.2f}")
        print(f"与PE(TTM)差异: {(pe_calculated - pe_ttm)/pe_ttm*100:.2f}%")

print("\n=== 估值估算 ===")
# 假设不同情景下的PE和EPS增长
# 由于PE(TTM)高达597.5，说明市场预期极高成长性
# 需要基于合理PE进行估值

# 行业平均PE？计算机、通信和其他电子设备制造业平均PE可能在30-50倍
# 但考虑到公司可能的高成长性，给予溢价

print("\n假设情景:")
print("1. 保守情景: PE=40 (行业平均偏上)")
print("2. 中性情景: PE=60 (考虑成长性溢价)")
print("3. 乐观情景: PE=80 (高成长预期)")

# 估算2025年全年EPS
# 2024年EPS为0.54元，2025年前三季度累计净利润66.17百万
# 假设Q4与Q3持平，全年净利润约94.34百万
if 'total_shares' in locals():
    # 估算2025年EPS
    est_2025_netprofit = 94.34e6  # 单位：元
    est_2025_eps = est_2025_netprofit / total_shares
    print(f"\n估算2025年EPS: ¥{est_2025_eps:.2f}")
    
    # 计算目标价
    print("\n基于2025年EPS的目标价:")
    print(f"保守(PE=40): ¥{40 * est_2025_eps:.2f}")
    print(f"中性(PE=60): ¥{60 * est_2025_eps:.2f}")
    print(f"乐观(PE=80): ¥{80 * est_2025_eps:.2f}")
    
    # 当前价格对比
    print(f"\n当前价格: ¥{price:.2f}")
    print(f"隐含PE(基于2025年EPS): {price/est_2025_eps:.2f}")