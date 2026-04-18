import akshare as ak
import pandas as pd

# 获取股价和均线
def get_stock_price_and_ma(symbol):
    df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date="20230101", adjust="qfq")
    df['MA60'] = df['收盘'].rolling(60).mean()
    df['MA250'] = df['收盘'].rolling(250).mean()
    return df

# 深度分析函数
def deep_analysis(symbol):
    df = get_stock_price_and_ma(symbol)
    print(f"{symbol} 最近5日股价和均线")
    print(df.tail(5)[['日期','收盘','MA60','MA250','成交量']])
    # 添加其他分析代码
    pass

# 股票代码列表
stock_symbols = ["002415.SZ", "300750.SZ", "002049.SZ"]

# 对每只股票进行深度分析
for symbol in stock_symbols:
    deep_analysis(symbol)