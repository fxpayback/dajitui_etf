import logging
import time
from datetime import datetime

import numpy as np
import pandas as pd
import akshare as ak
import yfinance as yf
import warnings

# 忽略警告
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)


def safe_product(arr):
    """修复 quantstats 中的问题"""
    return np.prod(arr)


from quantstats.stats import _np

_np.product = safe_product


def get_etf_data(symbol, end_date=None):
    """获取ETF/LOF数据，优先使用 AkShare，失败则回退到 yfinance"""
    logger = logging.getLogger(__name__)

    max_retries = 3
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    start_date = (
        datetime.now().replace(year=datetime.now().year - 5).strftime("%Y%m%d")
    )

    for retry in range(max_retries):
        try:
            df = ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            df = df.rename(columns={"日期": "date", "收盘": "close"})
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)
            df.sort_index(inplace=True)

            data_days = len(df)
            logger.info(
                f"获取ETF {symbol} 数据: {data_days}天, 从 {df.index.min()} 到 {df.index.max()}"
            )
            print(
                f"获取ETF {symbol} 数据: {data_days}天, 从 {df.index.min()} 到 {df.index.max()}"
            )
            return df, symbol
        except Exception as e:
            if retry < max_retries - 1:
                print(f"第{retry + 1}次获取数据失败，正在重试...")
                time.sleep(5)
            else:
                logger.warning(f"AkShare 获取失败，尝试使用 yfinance: {e}")
                try:
                    start_dt = datetime.strptime(start_date, "%Y%m%d")
                    end_dt = datetime.strptime(end_date, "%Y%m%d")
                    yahoo_symbol = (
                        symbol
                        if symbol.endswith((".SS", ".SZ"))
                        else f"{symbol}.SS"
                    )
                    df = yf.download(
                        yahoo_symbol,
                        start=start_dt.strftime("%Y-%m-%d"),
                        end=end_dt.strftime("%Y-%m-%d"),
                        progress=False,
                    )
                    if df.empty:
                        raise ValueError("yfinance 未返回数据")
                    df.reset_index(inplace=True)
                    df = df.rename(columns={"Date": "date", "Close": "close"})
                    df["date"] = pd.to_datetime(df["date"])
                    df.set_index("date", inplace=True)
                    df.sort_index(inplace=True)
                    data_days = len(df)
                    logger.info(
                        f"使用 yfinance 获取ETF {symbol} 数据: {data_days}天, 从 {df.index.min()} 到 {df.index.max()}"
                    )
                    return df, symbol
                except Exception as ye:
                    print(f"获取数据失败: {ye}")
                    raise


def calculate_volatility(symbol, window=200):
    """200 日波动率"""
    df, _ = get_etf_data(symbol)
    daily_returns = df["close"].pct_change()
    rolling_std = daily_returns.rolling(window=window).std()
    annual_vol = rolling_std * np.sqrt(252)
    return annual_vol


def calculate_grid_spacing(symbol, window=200):
    """计算网格间隔"""
    volatility = calculate_volatility(symbol, window)
    grid_spacing = volatility / 8
    return grid_spacing


def calculate_grid_range(symbol):
    """计算网格总区间"""
    logger = logging.getLogger(__name__)

    df, _ = get_etf_data(symbol)
    close = df["close"]

    data_days = len(df)
    logger.info(f"计算 {symbol} 网格范围，共有 {data_days} 天数据")
    print(f"计算 {symbol} 网格范围，共有 {data_days} 天数据")

    if data_days < 500:
        high_window = min(100, int(data_days * 0.3))
        low_window = high_window
        high_long_window = min(300, int(data_days * 0.8))
        low_long_window = high_long_window
        logger.info(
            f"数据量较少，调整窗口: 短期={high_window}, 长期={high_long_window}"
        )
        print(
            f"数据量较少，调整窗口: 短期={high_window}, 长期={high_long_window}"
        )
    else:
        high_window = low_window = 100
        high_long_window = low_long_window = 500

    high_100 = close.rolling(window=high_window).max()
    low_100 = close.rolling(window=low_window).min()
    high_500 = close.rolling(window=high_long_window).max()
    low_500 = close.rolling(window=low_long_window).min()

    H = 0.7 * high_100 + 0.3 * high_500
    L = 0.7 * low_100 + 0.3 * low_500

    latest_date = df.index[-1]
    if pd.isna(H.loc[latest_date]) or pd.isna(L.loc[latest_date]):
        logger.warning(f"{symbol} 网格计算结果有NaN值，尝试使用最新可用数据")
        print(f"{symbol} 网格计算结果有NaN值，尝试使用最新可用数据")
        last_valid_H = H.last_valid_index()
        last_valid_L = L.last_valid_index()
        if last_valid_H is not None and last_valid_L is not None:
            H.loc[latest_date] = H.loc[last_valid_H]
            L.loc[latest_date] = L.loc[last_valid_L]
            logger.info(
                f"使用最后有效值: H={H.loc[latest_date]}, L={L.loc[latest_date]}"
            )
            print(
                f"使用最后有效值: H={H.loc[latest_date]}, L={L.loc[latest_date]}"
            )
        else:
            latest_price = close.iloc[-1]
            H.loc[latest_date] = latest_price * 1.2
            L.loc[latest_date] = latest_price * 0.8
            logger.info(
                f"使用简单估计: H={H.loc[latest_date]}, L={L.loc[latest_date]}"
            )
            print(
                f"使用简单估计: H={H.loc[latest_date]}, L={L.loc[latest_date]}"
            )

    return pd.DataFrame({"H_val": H, "L_val": L}, index=df.index)

