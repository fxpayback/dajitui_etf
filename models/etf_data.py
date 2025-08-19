# 从您的原始代码导入所需函数
import numpy as np
import pandas as pd
import akshare as ak
import warnings
import time
from datetime import datetime
import logging

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
    logger = logging.getLogger(__name__)
    
    max_retries = 3
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    # 设置起始日期为5年前，尽可能获取更多历史数据
    start_date = (datetime.now().replace(year=datetime.now().year - 5)).strftime("%Y%m%d")
    
    for retry in range(max_retries):
        try:
            df = ak.fund_etf_hist_em(
                    symbol=symbol, 
                    period="daily", 
                    start_date=start_date, 
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
            
            # 记录获取到的数据量
            data_days = len(df)
            logger.info(f"获取ETF {symbol} 数据: {data_days}天, 从 {df.index.min()} 到 {df.index.max()}")
            print(f"获取ETF {symbol} 数据: {data_days}天, 从 {df.index.min()} 到 {df.index.max()}")
            
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
    import logging
    logger = logging.getLogger(__name__)
    
    df, _ = get_etf_data(symbol)
    close = df['close']
    
    # 记录数据长度
    data_days = len(df)
    logger.info(f"计算 {symbol} 网格范围，共有 {data_days} 天数据")
    print(f"计算 {symbol} 网格范围，共有 {data_days} 天数据")
    
    # 根据数据长度调整窗口大小
    if data_days < 500:
        high_window = min(100, int(data_days * 0.3))  # 使用可用数据的30%
        low_window = high_window
        high_long_window = min(300, int(data_days * 0.8))  # 使用可用数据的80%
        low_long_window = high_long_window
        logger.info(f"数据量较少，调整窗口: 短期={high_window}, 长期={high_long_window}")
        print(f"数据量较少，调整窗口: 短期={high_window}, 长期={high_long_window}")
    else:
        high_window = 100
        low_window = 100
        high_long_window = 500
        low_long_window = 500
    
    # 计算高点和低点
    high_100 = close.rolling(window=high_window).max()
    low_100 = close.rolling(window=low_window).min()
    high_500 = close.rolling(window=high_long_window).max() 
    low_500 = close.rolling(window=low_long_window).min()
    
    # 根据公式计算上下限
    H = 0.7 * high_100 + 0.3 * high_500
    L = 0.7 * low_100 + 0.3 * low_500
    
    # 如果计算结果最后一个值是NaN，使用可用数据
    latest_date = df.index[-1]
    if pd.isna(H.loc[latest_date]) or pd.isna(L.loc[latest_date]):
        logger.warning(f"{symbol} 网格计算结果有NaN值，尝试使用最新可用数据")
        print(f"{symbol} 网格计算结果有NaN值，尝试使用最新可用数据")
        
        # 找到H中最后一个非NaN值
        last_valid_H = H.last_valid_index()
        last_valid_L = L.last_valid_index()
        
        if last_valid_H is not None and last_valid_L is not None:
            # 用最后一个有效值填充
            H.loc[latest_date] = H.loc[last_valid_H]
            L.loc[latest_date] = L.loc[last_valid_L]
            logger.info(f"使用最后有效值: H={H.loc[latest_date]}, L={L.loc[latest_date]}")
            print(f"使用最后有效值: H={H.loc[latest_date]}, L={L.loc[latest_date]}")
        else:
            # 如果没有有效值，使用简单估计
            latest_price = close.iloc[-1]
            H.loc[latest_date] = latest_price * 1.2
            L.loc[latest_date] = latest_price * 0.8
            logger.info(f"使用简单估计: H={H.loc[latest_date]}, L={L.loc[latest_date]}")
            print(f"使用简单估计: H={H.loc[latest_date]}, L={L.loc[latest_date]}")
    
    # 返回一个 DataFrame，确保索引与 df 一致
    return pd.DataFrame({'H_val': H, 'L_val': L}, index=df.index) 