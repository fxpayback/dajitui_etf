import numpy as np
import pandas as pd
import akshare as ak
import warnings
import time
from datetime import datetime

# 忽略警告
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# 修复 quantstats 中的问题
def safe_product(arr):
    return np.prod(arr)

from quantstats.stats import _np
_np.product = safe_product

def get_etf_data(symbol, end_date=None):
    """获取ETF/LOF数据"""
    max_retries = 3
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    for retry in range(max_retries):
        try:
            df = ak.fund_etf_hist_em(
                    symbol=symbol, 
                    period="daily", 
                    start_date="20121210", 
                    end_date=end_date,
                    adjust="qfq"
                )    
            # 只保留需要的列并重命名
            df = df.rename(columns={
                '日期': 'date',
                '收盘': 'close',
            })
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True)
            
            return df, symbol
        except Exception as e:
            if retry < max_retries - 1:
                print(f"第{retry + 1}次获取数据失败，正在重试...")
                time.sleep(5)
            else:
                print(f"获取数据失败: {str(e)}")
                raise

# 200日波动率
def calculate_volatility(symbol, window=200):
    df, _ = get_etf_data(symbol)
    # 计算日收益率
    daily_returns = df['close'].pct_change()
    # 计算滚动标准差
    rolling_std = daily_returns.rolling(window=window).std()
    # 转换为年化波动率 (假设一年252个交易日)
    annual_vol = rolling_std * np.sqrt(252)
    return annual_vol

# 网格间隔
def calculate_grid_spacing(symbol, window=200):
    """计算网格间隔"""
    volatility = calculate_volatility(symbol, window)
    grid_spacing = volatility / 8
    return grid_spacing

# 网格总区间
def calculate_grid_range(symbol):
    """计算网格总区间"""
    df, _ = get_etf_data(symbol)
    close = df['close']
    
    # 计算100日和500日的高点和低点
    high_100 = close.rolling(window=200).max()
    low_100 = close.rolling(window=200).min()
    high_500 = close.rolling(window=800).max() 
    low_500 = close.rolling(window=800).min()
    
    # 根据公式计算上下限
    H = 0.7 * high_100 + 0.3 * high_500
    L = 0.7 * low_100 + 0.3 * low_500
    
    # 返回一个 DataFrame，确保索引与 df 一致
    return pd.DataFrame({'H_val': H, 'L_val': L}, index=df.index)


# 使用示例
if __name__ == "__main__":
    symbols = [
            {"name": "沪深 300ETF", "code": "510300"},
            {"name": "中证 500ETF", "code": "510500"},
            {"name": "科创 50ETF", "code": "588000"},
            {"name": "创业板 ETF", "code": "159915"},
            {"name": "人工智能 ETF", "code": "159819"},
            {"name": "半导体 ETF", "code": "512480"},
            {"name": "新能源车 ETF", "code": "515030"},
            {"name": "5G 通信 ETF", "code": "515050"},
            {"name": "酒 ETF", "code": "512690"},
            {"name": "消费 50ETF", "code": "159936"},
            {"name": "旅游 ETF", "code": "159766"},
            {"name": "医药 ETF", "code": "512010"},
            {"name": "创新药 ETF", "code": "159992"},
            {"name": "银行 ETF", "code": "512800"},
            {"name": "证券 ETF", "code": "512880"},
            {"name": "煤炭 ETF", "code": "515220"},
            {"name": "纳指 ETF", "code": "513100"},
            {"name": "恒生指数 ETF", "code": "159920"},
            {"name": "日经 225ETF", "code": "513520"},
            {"name": "法国 CAC40ETF", "code": "513080"},
            {"name": "中概互联 ETF", "code": "513050"},
            {"name": "环保 ETF", "code": "512580"},
            {"name": "房地产 ETF", "code": "512200"},
            {"name": "游戏 ETF", "code": "159869"},
            {"name": "红利低波 ETF", "code": "512890"},
            {"name": "10 年国债 ETF", "code": "511260"},
            {"name": "短债 ETF", "code": "511010"},
            {"name": "可转债 ETF", "code": "511380"},
            {"name": "美元债 LOF", "code": "501300"},
            {"name": "原油 ETF", "code": "162411"},
            {"name": "黄金 ETF", "code": "518800"},
            {"name": "豆粕 ETF", "code": "159985"},
        ]
    # 示例：获取指定日期的数据
    specific_date = "20250310"  # 指定日期
    for symbol_info in symbols:
        print(f"\n正在处理 {symbol_info['name']} ({symbol_info['code']})")
        symbol = symbol_info['code']
        # 获取指定日期的数据
        df, _ = get_etf_data(symbol, end_date=specific_date)
        
        # 计算波动率
        volatility = calculate_volatility(symbol)
        print(f"最新波动率: {round(volatility.iloc[-1] * 100)}%")
        # 计算网格间隔
        grid_spacing = calculate_grid_spacing(symbol)
        print(f"最新网格间隔: {round(grid_spacing.iloc[-1] * 100, 1)}%")
        # 计算网格总区间
        grid_range = calculate_grid_range(symbol)
        print(f"最新网格总区间: 上限 {grid_range['H_val'].iloc[-1]:.2f}, 下限 {grid_range['L_val'].iloc[-1]:.2f}")
        # 计算总区间百分比
        range_pct = 2 * (grid_range['H_val'].iloc[-1] - grid_range['L_val'].iloc[-1]) / (grid_range['H_val'].iloc[-1] + grid_range['L_val'].iloc[-1])
        print(f"总区间百分比: {round(range_pct * 100)}%")
        # 计算网格层数
        grid_levels = round(range_pct / grid_spacing.iloc[-1])
        print(f"网格层数: {grid_levels}")
        # 计算当前价格
        current_price = df['close'].iloc[-1]
        print(f"当前价格: {current_price:.2f}")
        
        # 计算当前所处的网格层数
        if current_price <= grid_range['L_val'].iloc[-1]:
            current_level = 0
        elif current_price >= grid_range['H_val'].iloc[-1]:
            current_level = grid_levels
        else:
            current_level = round((current_price - grid_range['L_val'].iloc[-1]) / (grid_spacing.iloc[-1] * grid_range['L_val'].iloc[-1]))
        print(f"当前所处网格层数: {current_level}")
        
        # 计算当前仓位
        total_levels = grid_levels
        position = 1 - current_level / total_levels
        position = max(0, min(1, position))  # 将仓位限制在0-1之间
        print(f"当前仓位: {round(position * 100)}%")
