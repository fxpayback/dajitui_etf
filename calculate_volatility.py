import numpy as np
import pandas as pd
from datetime import datetime

from models.etf_data import get_etf_data

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


def backtest_grid_strategy(symbol, start_date, end_date, initial_capital=100000, grid_levels=10):
    """使用简单网格策略对ETF进行回测"""
    df, _ = get_etf_data(symbol)
    df = df[(df.index >= start_date) & (df.index <= end_date)].copy()
    if df.empty:
        raise ValueError("指定日期范围内无数据")

    grid_range = calculate_grid_range(symbol)
    grid_spacing = calculate_grid_spacing(symbol)

    lower = grid_range['L_val'].iloc[-1]
    spacing = grid_spacing.iloc[-1]
    invest_per_level = initial_capital / grid_levels

    cash = initial_capital
    position = 0.0
    trades = []

    prev_level = int((df['close'].iloc[0] - lower) / (spacing * lower))

    for date, price in df['close'].items():
        level = int((price - lower) / (spacing * lower))

        while level < prev_level and cash >= invest_per_level:
            qty = invest_per_level / price
            cash -= invest_per_level
            position += qty
            trades.append({"date": date.strftime('%Y-%m-%d'), "type": 'buy', "price": round(price, 2), "quantity": round(qty, 2)})
            prev_level -= 1

        while level > prev_level and position > 0:
            qty = invest_per_level / price
            cash += qty * price
            position -= qty
            trades.append({"date": date.strftime('%Y-%m-%d'), "type": 'sell', "price": round(price, 2), "quantity": round(qty, 2)})
            prev_level += 1

    final_equity = cash + position * df['close'].iloc[-1]
    return {
        'final_equity': final_equity,
        'return_pct': (final_equity / initial_capital - 1) * 100,
        'trades': trades,
    }


def optimize_grid_strategy(symbols, start_date, end_date, grid_levels_options, initial_capital=100000):
    """遍历多支ETF和网格层数，寻找回测收益最高的组合"""
    best = None
    for symbol in symbols:
        for levels in grid_levels_options:
            result = backtest_grid_strategy(
                symbol,
                start_date,
                end_date,
                initial_capital=initial_capital,
                grid_levels=levels,
            )
            if best is None or result["final_equity"] > best["final_equity"]:
                best = {"symbol": symbol, "grid_levels": levels, **result}
    return best


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

        # 回测网格策略
        try:
            result = backtest_grid_strategy(symbol, '20200101', specific_date, grid_levels=grid_levels)
            print(f"回测收益率: {result['return_pct']:.2f}%")
        except Exception as e:
            print(f"回测失败: {e}")
