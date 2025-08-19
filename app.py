from flask import Flask, render_template, request, jsonify, Response, redirect, url_for, flash, session, abort, send_file, send_from_directory, g, make_response, get_flashed_messages
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import sqlite3
import os
import io
import csv
import logging
import threading
import queue
import secrets
from functools import wraps
from werkzeug.security import generate_password_hash
import requests
import akshare as ak
from logging.handlers import RotatingFileHandler
import time
import uuid
import hashlib

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 导入您的ETF数据处理函数
from models.etf_data import (
    get_etf_data, 
    calculate_volatility, 
    calculate_grid_spacing, 
    calculate_grid_range
)

# 导入用户相关模型和身份验证功能
from models.user import User, Portfolio, FavoriteETF, CustomETF, UserSetting, create_user_tables
from models.auth import login_user, logout_user, get_current_user, check_csrf_token, login_required, get_user_id

# 导入ETF管理模块
from models.etf_admin import (
    get_all_etfs, get_etf_by_symbol, add_etf, update_etf, delete_etf,
    get_etf_data_count, get_etf_date_range, clear_etf_data
)

app = Flask(__name__)
app.config['APP_VERSION'] = 'V0.9'
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev_key_for_testing')

# 设置日志系统
if not app.debug:
    # 确保日志目录存在
    if not os.path.exists('logs'):
        os.mkdir('logs')
    
    # 配置文件处理器
    file_handler = RotatingFileHandler('logs/error.log', maxBytes=10240, backupCount=10)
    
    # 设置日志格式
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    
    # 设置日志级别
    file_handler.setLevel(logging.ERROR)
    
    # 添加到应用
    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.ERROR)
    app.logger.info('Application startup')

# 添加异常处理
@app.errorhandler(Exception)
def handle_exception(e):
    # 记录错误到日志
    app.logger.error(f'未处理的异常: {str(e)}', exc_info=True)
    try:
        # 尝试渲染错误模板
        return render_template('error.html', error=str(e), now=datetime.now()), 500
    except Exception as render_error:
        # 如果渲染错误模板失败，返回简单文本响应
        app.logger.error(f'渲染错误页面失败: {str(render_error)}', exc_info=True)
        return f"服务器错误: {str(e)}", 500

# 添加一个上下文处理器，使版本号在所有模板中可用
@app.context_processor
def inject_version():
    return {'app_version': app.config['APP_VERSION']}

# 添加一个上下文处理器，使当前用户信息在所有模板中可用
@app.context_processor
def inject_user():
    return {'current_user': get_current_user()}

# 添加一个上下文处理器，使CSRF令牌在所有模板中可用
@app.context_processor
def inject_csrf_token():
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(16)
    return {'csrf_token': session['csrf_token']}





# 创建数据库连接
def get_db_connection():
    try:
        conn = sqlite3.connect('database/etf_history.db')
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
        raise

# ETF列表
def get_official_etf_list():
    """获取官方ETF列表，每次都从数据库获取最新的列表"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
        SELECT symbol, name, description, category, correlation, volatility_type, weight, is_official 
        FROM etf_list 
        WHERE is_official = 1
        ORDER BY category, name
        ''')
        etfs = cursor.fetchall()
        
        result = []
        for etf in etfs:
            result.append({
                "code": etf['symbol'],
                "name": etf['name'],
                "description": etf['description'],
                "category": etf['category'],
                "correlation": etf['correlation'],
                "volatility_type": etf['volatility_type'],
                "weight": f"{etf['weight']}%",
                "is_official": etf['is_official']
            })
        
        # 验证返回的ETF列表非空
        if not result:
            logger.warning("从数据库获取的官方ETF列表为空，可能存在数据库问题")
            
        return result
    except Exception as e:
        logger.error(f"从数据库获取ETF列表失败: {e}")
        return []
    finally:
        if conn:
            conn.close()

# 初始化ETF列表
SYMBOLS = get_official_etf_list()
# 如果数据库中没有ETF列表，使用默认列表
if not SYMBOLS:
    logger.warning("数据库中没有官方ETF列表，使用默认列表")


# 初始化数据库
def init_db():
    try:
        if not os.path.exists('database'):
            os.makedirs('database')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 创建ETF历史数据表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS etf_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            symbol TEXT,
            price REAL,
            volatility REAL,
            grid_spacing REAL,
            upper_limit REAL,
            lower_limit REAL,
            current_level INTEGER,
            total_levels INTEGER,
            position REAL
        )
        ''')
        
        # 添加索引以提高查询效率
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_etf_data_symbol_date ON etf_data (symbol, date)')
        
        conn.commit()
        conn.close()
        logger.info("ETF数据库初始化成功")
        
        # 创建用户相关表
        create_user_tables()
        logger.info("用户数据库初始化成功")
    except Exception as e:
        logger.error(f"数据库初始化失败: {str(e)}")
        raise

# 重置数据库（清空所有数据）
def reset_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 清空ETF历史数据表
        cursor.execute('DELETE FROM etf_data')
        
        conn.commit()
        conn.close()
        logger.info("数据库已重置（所有数据已清空）")
        return True
    except Exception as e:
        logger.error(f"数据库重置失败: {str(e)}")
        return False

# API端点：重置数据库
@app.route('/api/reset_db', methods=['POST'])
def api_reset_db():
    try:
        success = reset_db()
        if success:
            return jsonify({"status": "success", "message": "数据库已成功重置"}), 200
        else:
            return jsonify({"status": "error", "message": "数据库重置失败"}), 500
    except Exception as e:
        logger.error(f"重置数据库失败: {str(e)}")
        return jsonify({"status": "error", "message": f"发生错误: {str(e)}"}), 500

# 简单的内存缓存
etf_data_cache = {}
cache_expiry = {}

def get_cached_etf_data(symbol):
    """获取缓存的ETF数据，如果缓存过期或不存在则重新获取"""
    import time
    current_time = time.time()
    
    # 缓存有效期为10分钟
    cache_valid_time = 600
    
    if symbol in etf_data_cache and symbol in cache_expiry:
        if current_time - cache_expiry[symbol] < cache_valid_time:
            logger.info(f"使用缓存数据: {symbol}")
            return etf_data_cache[symbol]
    
    logger.info(f"重新获取数据: {symbol}")
    df, _ = get_etf_data(symbol)
    etf_data_cache[symbol] = df
    cache_expiry[symbol] = current_time
    return df

# 路由：主页
@app.route('/')
def index():
    # 仅传递官方ETF列表，不包含用户自定义ETF
    return render_template('index.html', symbols=SYMBOLS)

# 路由：获取ETF数据
@app.route('/api/etf/<symbol>')
def get_etf_info(symbol):
    try:
        logger.info(f"正在处理ETF数据请求: {symbol}")
        
        # 导入datetime模块，确保它在整个函数中可用
        from datetime import datetime, timedelta
        
        # 获取请求来源页面和上下文
        referer = request.headers.get('Referer', '')
        page_context = request.args.get('context', '')  # 页面上下文参数
        portfolio_id = request.args.get('portfolio_id', '')  # 投资组合ID参数
        
        # 刷新全局ETF列表，确保使用最新的官方ETF列表
        global SYMBOLS
        SYMBOLS = get_official_etf_list()
        
        # 获取当前用户(如果已登录)
        user = get_current_user()
        user_id = None
        if user:
            user_id = get_user_id(user)
        
        # 验证ETF访问权限
        access_allowed = False
        
        # 检查是否是官方ETF
        is_official = any(s['code'] == symbol for s in SYMBOLS)
        
        if is_official:
            # 官方ETF在所有页面都可以访问
            access_allowed = True
            etf_name = next((s['name'] for s in SYMBOLS if s['code'] == symbol), '')
            etf_category = next((s.get('category', '未分类') for s in SYMBOLS if s['code'] == symbol), '未分类')
            etf_correlation = next((s.get('correlation', '未知') for s in SYMBOLS if s['code'] == symbol), '未知')
            etf_volatility_type = next((s.get('volatility_type', '未知') for s in SYMBOLS if s['code'] == symbol), '未知')
            etf_weight = next((s.get('weight', 1.0) for s in SYMBOLS if s['code'] == symbol), 1.0)
            is_custom = False
        else:
            # 对于非官方ETF (自定义ETF)，根据上下文和用户权限进行验证
            is_custom = True
            
            # 设置默认值
            etf_name = ''
            etf_category = '自定义ETF'
            etf_correlation = '未分类'
            etf_volatility_type = '未知'
            etf_weight = 1.0
            
            # 判断页面上下文
            if 'dashboard' in referer or 'dashboard' == page_context or 'history' in referer or 'history' == page_context:
                # Dashboard和History页面仅允许官方ETF
                # 明确拒绝非官方ETF访问
                logger.warning(f"拒绝非官方ETF {symbol} 在Dashboard/History页面的访问请求")
                access_allowed = False
                return jsonify({'error': f'ETF {symbol} 不是官方支持的ETF，无法在Dashboard/History页面访问', 'symbol': symbol}), 403
            elif 'public_backtest' in referer or 'public_backtest' == page_context:
                # Public_backtest页面允许官方ETF和当前用户自己的ETF
                if user_id:
                    # 检查用户是否有权限访问该ETF
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute('SELECT * FROM custom_etfs WHERE symbol = ? AND user_id = ?', (symbol, user_id))
                    custom_etf = cursor.fetchone()
                    conn.close()
                    
                    if custom_etf:
                        access_allowed = True
                        etf_name = custom_etf['name']
                        if 'description' in custom_etf and custom_etf['description']:
                            etf_category = custom_etf['description']
            elif 'portfolio' in referer or 'portfolio' == page_context:
                # Portfolio页面根据是否有portfolio_id参数决定
                if portfolio_id and user_id:
                    # 验证该ETF是否在用户的投资组合中
                    portfolio = Portfolio.get_by_id(portfolio_id, user_id)
                    if portfolio:
                        # 检查该ETF是否在投资组合中
                        portfolio_etfs = [etf['symbol'] for etf in portfolio['etfs']]
                        if symbol in portfolio_etfs:
                            # 从custom_etfs表获取ETF信息
                            conn = get_db_connection()
                            cursor = conn.cursor()
                            cursor.execute('SELECT * FROM custom_etfs WHERE symbol = ? AND user_id = ?', (symbol, user_id))
                            custom_etf = cursor.fetchone()
                            conn.close()
                            
                            if custom_etf:
                                access_allowed = True
                                etf_name = custom_etf['name']
                                if 'description' in custom_etf and custom_etf['description']:
                                    etf_category = custom_etf['description']
        
        # 如果无权访问，返回错误
        if not access_allowed:
            return jsonify({'error': '无权访问该ETF数据', 'symbol': symbol}), 403
        
        # 检查是否强制刷新数据
        force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'
        
        # 如果不强制刷新，先尝试从数据库获取最新数据
        if not force_refresh:
            logger.info(f"尝试从数据库获取{symbol}的最新数据")
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 获取最新的一条记录
            cursor.execute('''
            SELECT * FROM etf_data 
            WHERE symbol = ? 
            ORDER BY date DESC LIMIT 1
            ''', (symbol,))
            
            latest_record = cursor.fetchone()
            
            # 如果找到记录，检查是否是最近的有效交易日数据
            if latest_record:
                record_date = datetime.strptime(latest_record['date'], '%Y-%m-%d').date()
                today = datetime.now().date()
                
                # 判断今天是否是交易日
                is_weekend = today.weekday() >= 5  # 5是周六，6是周日
                
                # 计算最近有效交易日的日期
                if is_weekend:
                    # 如果是周末，上一个交易日可能是周五或之前
                    days_to_subtract = today.weekday() - 4  # 减去到周五的天数
                    last_trading_day = today - timedelta(days=days_to_subtract)
                else:
                    # 工作日，上一个交易日就是今天或昨天
                    last_trading_day = today
                
                # 如果数据库记录日期是最近有效交易日或更近，直接使用数据库数据
                if record_date >= last_trading_day or (record_date >= today - timedelta(days=1)):
                    logger.info(f"使用数据库中的最新数据: {symbol}, 日期: {record_date}")
                    
                    # 构建返回数据
                    result = {
                        'symbol': symbol,
                        'name': etf_name,
                        'current_price': float(latest_record['price']),
                        'volatility': round(float(latest_record['volatility'] * 100), 2),
                        'grid_spacing': round(float(latest_record['grid_spacing'] * 100), 2),
                        'upper_limit': round(float(latest_record['upper_limit']), 2),
                        'lower_limit': round(float(latest_record['lower_limit']), 2),
                        'range_percentage': round(2 * (float(latest_record['upper_limit']) - float(latest_record['lower_limit'])) / (float(latest_record['upper_limit']) + float(latest_record['lower_limit'])) * 100, 2),
                        'grid_levels': int(latest_record['total_levels']),
                        'current_level': int(latest_record['current_level']),
                        'position': round(float(latest_record['position'] * 100)),  # 四舍五入为整数
                        'data_timestamp': record_date.strftime('%Y-%m-%d %H:%M:%S'),  # 添加数据时间戳
                        'is_cached': True,  # 标记为数据库缓存数据
                        'is_custom': is_custom,  # 标记是否为自定义ETF
                        'category': etf_category,
                        'correlation': etf_correlation,
                        'volatility_type': etf_volatility_type,
                        'weight': etf_weight
                    }
                    
                    # 获取历史数据
                    cursor.execute('''
                    SELECT date, price, volatility, grid_spacing, position 
                    FROM etf_data 
                    WHERE symbol = ? 
                    ORDER BY date DESC LIMIT 30
                    ''', (symbol,))
                    
                    history_records = cursor.fetchall()
                    
                    # 构建历史数据
                    history_data = {
                        "dates": [],
                        "prices": [],
                        "volatility": [],
                        "grid_spacing": [],
                        "positions": []
                    }
                    
                    for record in reversed(history_records):  # 反转以使日期按升序排列
                        history_data["dates"].append(record['date'])
                        history_data["prices"].append(float(record['price']))
                        history_data["volatility"].append(float(record['volatility'] * 100))
                        history_data["grid_spacing"].append(float(record['grid_spacing'] * 100))
                        history_data["positions"].append(float(record['position'] * 100))
                    
                    result['historical_data'] = history_data
                    result['is_cached'] = True
                    
                    conn.close()  # 在这里关闭连接，确保所有数据库操作完成后才关闭
                    return jsonify(result)
            
            conn.close()  # 如果没有使用缓存数据，关闭连接
        
        try:
            # 如果强制刷新或没有找到有效的本地数据，从远程获取
            logger.info(f"从远程获取最新数据: {symbol}")
            
            # 获取ETF数据
            df, _ = get_etf_data(symbol)
            
            # 获取当前价格
            current_price = df['close'].iloc[-1]
            
            # 计算波动率
            volatility_series = calculate_volatility(symbol)
            volatility = volatility_series.iloc[-1]
            
            # 计算网格间隔
            grid_spacing = volatility / 8
            
            # 计算网格区间
            grid_range = calculate_grid_range(symbol)
            upper_limit = grid_range['H_val'].iloc[-1]
            lower_limit = grid_range['L_val'].iloc[-1]
            
            # 计算网格层数和当前所处网格层
            range_percentage = 2 * (upper_limit - lower_limit) / (upper_limit + lower_limit)
            grid_levels = round(range_percentage / grid_spacing)
            
            # 计算当前所处层数
            if current_price <= lower_limit:
                current_level = 0
            elif current_price >= upper_limit:
                current_level = grid_levels
            else:
                current_level = round((current_price - lower_limit) / (grid_spacing * lower_limit))
            
            # 计算建议仓位（百分比）
            position = 1 - current_level / grid_levels
            position = max(0, min(1, position))  # 将仓位限制在0-1之间
            
            # 构建返回数据
            result = {
                'symbol': symbol,
                'name': etf_name,
                'current_price': float(current_price),
                'volatility': round(float(volatility * 100), 2),
                'grid_spacing': round(float(grid_spacing * 100), 2),
                'upper_limit': round(float(upper_limit), 2),
                'lower_limit': round(float(lower_limit), 2),
                'range_percentage': round(float(range_percentage * 100), 2),
                'grid_levels': int(grid_levels),
                'current_level': int(current_level),
                'position': round(float(position * 100)),  # 四舍五入为整数
                'data_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # 添加数据时间戳
                'is_custom': is_custom,
                'category': etf_category,
                'correlation': etf_correlation,
                'volatility_type': etf_volatility_type,
                'weight': etf_weight
            }
            
            # 获取历史数据
            history_data = {
                "dates": [],
                "prices": [],
                "volatility": [],
                "grid_spacing": [],
                "positions": []
            }
            
            # 从df中提取最近30天的价格数据
            recent_df = df.iloc[-30:].copy() if len(df) >= 30 else df.copy()
            for idx, row in recent_df.iterrows():
                date_str = idx.strftime('%Y-%m-%d')
                price = float(row['close'])
                
                # 为日期和价格添加数据
                history_data["dates"].append(date_str)
                history_data["prices"].append(price)
                
                # 如果波动率数据可用，添加波动率和网格间隔
                vol_idx = volatility_series.index.get_indexer([idx], method='nearest')[0]
                if vol_idx >= 0:
                    vol = float(volatility_series.iloc[vol_idx])
                    history_data["volatility"].append(float(vol * 100))
                    history_data["grid_spacing"].append(float(vol * 100 / 8))
                    
                    # 计算历史仓位
                    range_idx = grid_range.index.get_indexer([idx], method='nearest')[0]
                    if range_idx >= 0:
                        h_val = grid_range['H_val'].iloc[range_idx]
                        l_val = grid_range['L_val'].iloc[range_idx]
                        
                        # 计算网格层数
                        range_pct = 2 * (h_val - l_val) / (h_val + l_val)
                        levels = range_pct / (vol / 8)
                        
                        # 计算当前层数
                        if price <= l_val:
                            level = 0
                        elif price >= h_val:
                            level = levels
                        else:
                            level = (price - l_val) / ((h_val - l_val) / levels)
                            
                        # 计算仓位
                        pos = 1 - level / levels
                        pos = max(0, min(1, pos))
                        history_data["positions"].append(float(pos * 100))
                    else:
                        history_data["positions"].append(float(position * 100))
                else:
                    history_data["volatility"].append(float(volatility * 100))
                    history_data["grid_spacing"].append(float(volatility * 100 / 8))
                    history_data["positions"].append(float(position * 100))
            
            result['historical_data'] = history_data
            
            # 保存到数据库
            save_historical_data(symbol, result)
            
            return jsonify(result)
        except Exception as e:
            # 如果是自定义ETF且获取数据失败，返回一个默认值对象
            if is_custom:
                logger.warning(f"获取自定义ETF {symbol} 数据失败，返回默认值: {str(e)}")
                
                # 构建默认返回数据
                default_result = {
                    'symbol': symbol,
                    'name': etf_name,
                    'current_price': 1.0,
                    'volatility': 10.0,
                    'grid_spacing': 1.25,
                    'upper_limit': 1.2,
                    'lower_limit': 0.8,
                    'range_percentage': 40.0,
                    'grid_levels': 6,
                    'current_level': 3,
                    'position': 50,  # 默认仓位50%
                    'data_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'is_custom': True,
                    'category': etf_category,
                    'correlation': etf_correlation,
                    'volatility_type': etf_volatility_type,
                    'weight': etf_weight,
                    'historical_data': {
                        "dates": [datetime.now().strftime('%Y-%m-%d')],
                        "prices": [1.0],
                        "volatility": [10.0],
                        "grid_spacing": [1.25],
                        "positions": [50]
                    }
                }
                return jsonify(default_result)
            else:
                raise
    except Exception as e:
        logger.error(f"获取ETF数据失败: {str(e)}")
        return jsonify({"error": f"获取ETF数据失败: {str(e)}"}), 500

# 保存ETF历史数据到数据库
def save_historical_data(symbol, data):
    """保存ETF历史数据到数据库"""
    from datetime import datetime
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取当前时间
        current_time = datetime.now().strftime('%Y-%m-%d')
        
        # 保存最新数据
        cursor.execute('''
        INSERT OR REPLACE INTO etf_data (
            date, symbol, price, volatility, grid_spacing, upper_limit, lower_limit, 
            current_level, total_levels, position
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            current_time, 
            symbol, 
            data['current_price'], 
            data['volatility'] / 100,  # 转换为小数存储
            data['grid_spacing'] / 100,  # 转换为小数存储
            data['upper_limit'], 
            data['lower_limit'], 
            data['current_level'], 
            data['grid_levels'], 
            data['position'] / 100  # 转换为小数存储
        ))
        
        # 如果有历史数据，也保存到数据库
        if 'historical_data' in data and isinstance(data['historical_data'], dict):
            hist_data = data['historical_data']
            # 检查历史数据的完整性
            if all(key in hist_data for key in ['dates', 'prices', 'volatility', 'grid_spacing']):
                # 检查数据库中是否已有该symbol的数据
                cursor.execute('SELECT COUNT(*) FROM etf_data WHERE symbol = ?', (symbol,))
                count = cursor.fetchone()[0]
                
                # 获取数据库中该symbol最新的日期
                latest_date = None
                if count > 0:
                    cursor.execute('SELECT MAX(date) FROM etf_data WHERE symbol = ?', (symbol,))
                    latest_date = cursor.fetchone()[0]
                
                if count < 10:  # 首次获取或数据不足，保存完整历史数据
                    logger.info(f"首次获取或数据不足，保存{len(hist_data['dates'])}天的完整历史数据")
                    # 将历史数据全部保存
                    for i in range(len(hist_data['dates'])):
                        date_str = hist_data['dates'][i]
                        
                        # 跳过当天的数据（已经保存过）和未来的数据
                        if date_str == current_time or date_str > current_time:
                            continue
                        
                        try:
                            price = hist_data['prices'][i]
                            volatility = hist_data['volatility'][i] / 100 if hist_data['volatility'][i] else 0
                            grid_spacing = hist_data['grid_spacing'][i] / 100 if hist_data['grid_spacing'][i] else 0
                            position = hist_data['positions'][i] / 100 if hist_data['positions'][i] else 0
                            
                            # 估算网格层数和级别
                            # 简化处理：使用当前的网格上下限和层数
                            current_price = price
                            upper_limit = data['upper_limit']
                            lower_limit = data['lower_limit']
                            grid_levels = data['grid_levels']
                            
                            # 计算当前所处网格层
                            if current_price <= lower_limit:
                                current_level = 0
                            elif current_price >= upper_limit:
                                current_level = grid_levels
                            else:
                                current_level = round((current_price - lower_limit) / (upper_limit - lower_limit) * grid_levels)
                            
                            cursor.execute('''
                            INSERT OR REPLACE INTO etf_data (
                                date, symbol, price, volatility, grid_spacing, upper_limit, lower_limit, 
                                current_level, total_levels, position
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                date_str, 
                                symbol, 
                                price, 
                                volatility,
                                grid_spacing,
                                upper_limit, 
                                lower_limit, 
                                current_level, 
                                grid_levels, 
                                position
                            ))
                        except (IndexError, TypeError) as e:
                            logger.warning(f"保存历史数据索引{i}时出错: {str(e)}")
                            continue
                else:  # 已有足够数据，只保存增量数据
                    # 从最新的日期开始，保存尚未存储的数据
                    logger.info(f"已有足够数据，仅保存增量数据（最新日期: {latest_date}）")
                    for i in range(len(hist_data['dates'])):
                        date_str = hist_data['dates'][i]
                        
                        # 跳过已存在的数据
                        if date_str <= latest_date or date_str == current_time or date_str > current_time:
                            continue
                        
                        try:
                            price = hist_data['prices'][i]
                            volatility = hist_data['volatility'][i] / 100 if hist_data['volatility'][i] else 0
                            grid_spacing = hist_data['grid_spacing'][i] / 100 if hist_data['grid_spacing'][i] else 0
                            position = hist_data['positions'][i] / 100 if hist_data['positions'][i] else 0
                            
                            # 估算网格层数和级别
                            # 简化处理：使用当前的网格上下限和层数
                            current_price = price
                            upper_limit = data['upper_limit']
                            lower_limit = data['lower_limit']
                            grid_levels = data['grid_levels']
                            
                            # 计算当前所处网格层
                            if current_price <= lower_limit:
                                current_level = 0
                            elif current_price >= upper_limit:
                                current_level = grid_levels
                            else:
                                current_level = round((current_price - lower_limit) / (upper_limit - lower_limit) * grid_levels)
                            
                            cursor.execute('''
                            INSERT OR REPLACE INTO etf_data (
                                date, symbol, price, volatility, grid_spacing, upper_limit, lower_limit, 
                                current_level, total_levels, position
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                date_str, 
                                symbol, 
                                price, 
                                volatility,
                                grid_spacing,
                                upper_limit, 
                                lower_limit, 
                                current_level, 
                                grid_levels, 
                                position
                            ))
                            logger.info(f"为{symbol}增量保存日期: {date_str}")
                        except (IndexError, TypeError) as e:
                            logger.warning(f"保存增量数据索引{i}时出错: {str(e)}")
                            continue
        
        conn.commit()
        logger.info(f"成功保存ETF历史数据: {symbol}")
        return True
    except Exception as e:
        logger.error(f"保存ETF历史数据失败: {str(e)}")
        # 发生错误时回滚事务
        if conn:
                conn.rollback()
        return False
    finally:
        if conn:
                conn.close()

# 路由：获取历史数据
@app.route('/api/history/<symbol>')
def get_history(symbol):
    try:
        # 获取请求来源页面和上下文
        referer = request.headers.get('Referer', '')
        page_context = request.args.get('context', '')  # 页面上下文参数
        portfolio_id = request.args.get('portfolio_id', '')  # 投资组合ID参数
        
        # 获取当前用户(如果已登录)
        user = get_current_user()
        user_id = None
        if user:
            user_id = get_user_id(user)
        
        # 验证ETF访问权限
        access_allowed = False
        
        # 检查是否是官方ETF
        is_official = any(s['code'] == symbol for s in SYMBOLS)
        
        if is_official:
            # 官方ETF在所有页面都可以访问
            access_allowed = True
        else:
            # 对于非官方ETF (自定义ETF)，根据上下文和用户权限进行验证
            # 判断页面上下文
            if 'dashboard' in referer or 'dashboard' == page_context or 'history' in referer or 'history' == page_context:
                # Dashboard和History页面仅允许官方ETF
                access_allowed = False
            elif 'public_backtest' in referer or 'public_backtest' == page_context:
                # Public_backtest页面允许官方ETF和当前用户自己的ETF
                if user_id:
                    # 检查用户是否有权限访问该ETF
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute('SELECT * FROM custom_etfs WHERE symbol = ? AND user_id = ?', (symbol, user_id))
                    custom_etf = cursor.fetchone()
                    conn.close()
                    
                    if custom_etf:
                        access_allowed = True
            elif 'portfolio' in referer or 'portfolio' == page_context:
                # Portfolio页面根据是否有portfolio_id参数决定
                if portfolio_id and user_id:
                    # 验证该ETF是否在用户的投资组合中
                    portfolio = Portfolio.get_by_id(portfolio_id, user_id)
                    if portfolio:
                        # 检查该ETF是否在投资组合中
                        portfolio_etfs = [etf['symbol'] for etf in portfolio['etfs']]
                        if symbol in portfolio_etfs:
                            access_allowed = True
            
        # 如果无权访问，返回错误
        if not access_allowed:
            return jsonify({'error': '无权访问该ETF历史数据', 'symbol': symbol}), 403
        
        # 获取历史数据的日期范围
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 转换符号，确保与数据库一致
        symbol = symbol.upper()
        
        # 使用索引提高查询效率
        if start_date and end_date:
            cursor.execute('''
            SELECT * FROM etf_data 
            WHERE symbol = ? AND date BETWEEN ? AND ?
            ORDER BY date ASC
            ''', (symbol, start_date, end_date))
        else:
            cursor.execute('''
            SELECT * FROM etf_data 
            WHERE symbol = ? 
            ORDER BY date ASC
            ''', (symbol,))
        
        records = cursor.fetchall()
        conn.close()
        
        if not records:
            return jsonify({
                'error': f'找不到{symbol}的历史数据',
                'message': '请先在仪表盘页面查看该ETF，系统会自动获取数据'
            }), 404
        
        # 构建历史数据
        dates = []
        prices = []
        volatilities = []
        grid_spacings = []
        positions = []
        
        for record in records:
            dates.append(record['date'])
            prices.append(record['price'])
            volatilities.append(record['volatility'] * 100)  # 转为百分比
            grid_spacings.append(record['grid_spacing'] * 100)  # 转为百分比
            positions.append(record['position'] * 100)  # 转为百分比
        
        return jsonify({
            'symbol': symbol,
            'dates': dates,
            'prices': prices,
            'volatilities': volatilities,
            'grid_spacings': grid_spacings,
            'positions': positions
        })
    
    except Exception as e:
        logger.error(f"获取历史数据失败: {str(e)}")
        return jsonify({'error': f'获取历史数据失败: {str(e)}'}), 500

# 如果数据库中没有数据，实时计算历史数据
def generate_history_data(symbol):
    try:
        from datetime import datetime
        logger.info(f"正在实时计算{symbol}的历史数据")
        
        # 获取ETF数据
        df, _ = get_etf_data(symbol)
        
        # 获取最近30天的数据
        last_30_days = df.iloc[-30:]
        
        # 计算30天的波动率
        volatility = calculate_volatility(symbol)
        volatility_30d = volatility.iloc[-30:]
        
        # 计算30天的网格间隔
        grid_spacing = calculate_grid_spacing(symbol)
        grid_spacing_30d = grid_spacing.iloc[-30:]
        
        # 计算30天的网格总区间
        grid_range = calculate_grid_range(symbol)
        grid_range_30d = grid_range.iloc[-30:]
        
        # 计算30天的历史仓位
        history = []
        
        for i in range(len(last_30_days)):
            idx = last_30_days.index[i]
            price = float(last_30_days['close'].iloc[i])
            vol = float(volatility_30d.iloc[i] * 100)
            spacing = round(float(grid_spacing_30d.iloc[i] * 100), 1)  # 保留一位小数
            upper_limit = float(grid_range_30d['H_val'].iloc[i])
            lower_limit = float(grid_range_30d['L_val'].iloc[i])
            
            # 计算总区间百分比
            range_pct = 2 * (upper_limit - lower_limit) / (upper_limit + lower_limit)
            
            # 计算网格层数
            grid_levels = int(round(range_pct / (grid_spacing_30d.iloc[i])))
            
            # 计算当前所处的网格层数
            if price <= lower_limit:
                current_level = 0
            elif price >= upper_limit:
                current_level = grid_levels
            else:
                current_level = int(round((price - lower_limit) / (grid_spacing_30d.iloc[i] * lower_limit)))
            
            # 计算当前仓位
            position = 1 - current_level / max(1, grid_levels)  # 避免除以零
            position = max(0, min(1, position))  # 将仓位限制在0-1之间
            position = round(position * 100)  # 转换为百分比并四舍五入为整数
            
            history.append({
                'date': idx.strftime('%Y-%m-%d'),
                'price': price,
                'volatility': vol,
                'grid_spacing': spacing,
                'upper_limit': upper_limit,
                'lower_limit': lower_limit,
                'current_level': current_level,
                'total_levels': grid_levels,
                'position': position
            })
        
        # 尝试保存到数据库
        try:
            save_calculated_history(symbol, history)
        except Exception as save_error:
            logger.error(f"保存计算的历史数据失败: {str(save_error)}")
            # 继续返回数据，不中断API响应
        
        logger.info(f"成功计算{len(history)}条历史数据")
        return jsonify(history)
    
    except Exception as e:
        logger.error(f"计算历史数据失败: {str(e)}")
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"错误详情: {error_details}")
        return jsonify({'error': str(e), 'details': error_details}), 500

# 保存计算的历史数据到数据库
def save_calculated_history(symbol, history):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 开始事务
        conn.execute('BEGIN TRANSACTION')
        
        for item in history:
            # 检查是否已存在当天数据
            cursor.execute(
                "SELECT id FROM etf_data WHERE date = ? AND symbol = ?", 
                (item['date'], symbol)
            )
            
            if cursor.fetchone() is None:
                # 插入新记录
                cursor.execute('''
                INSERT INTO etf_data 
                (date, symbol, price, volatility, grid_spacing, upper_limit, 
                 lower_limit, current_level, total_levels, position)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    item['date'], 
                    symbol, 
                    item['price'], 
                    item['volatility'] / 100,  # 存储为小数
                    item['grid_spacing'] / 100,  # 存储为小数
                    item['upper_limit'], 
                    item['lower_limit'], 
                    item['current_level'], 
                    item['total_levels'], 
                    item['position'] / 100  # 存储为小数
                ))
            else:
                # 更新现有记录
                cursor.execute('''
                UPDATE etf_data 
                SET price = ?, volatility = ?, grid_spacing = ?, upper_limit = ?, 
                    lower_limit = ?, current_level = ?, total_levels = ?, position = ?
                WHERE date = ? AND symbol = ?
                ''', (
                    item['price'], 
                    item['volatility'] / 100,  # 存储为小数
                    item['grid_spacing'] / 100,  # 存储为小数
                    item['upper_limit'], 
                    item['lower_limit'], 
                    item['current_level'], 
                    item['total_levels'], 
                    item['position'] / 100,  # 存储为小数
                    item['date'], 
                    symbol
                ))
        
        # 提交事务
        conn.execute('COMMIT')
        logger.info(f"成功保存{len(history)}条计算的历史数据")
        return True
        
    except Exception as e:
        logger.error(f"保存计算的历史数据失败: {str(e)}")
        # 发生错误时回滚事务
        if conn:
            try:
                conn.execute('ROLLBACK')
            except:
                pass
        return False
    finally:
        # 确保连接始终被关闭
        if conn:
            try:
                conn.close()
            except:
                pass

# 获取单个ETF的最新数据（从数据库）
def get_latest_etf_data(symbol):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
        SELECT symbol, price, volatility, grid_spacing, position 
        FROM etf_data 
        WHERE symbol = ? 
        ORDER BY date DESC LIMIT 1
        ''', (symbol,))
        data = cursor.fetchone()
        return data
    except Exception as e:
        logger.error(f"获取 {symbol} 最新数据失败: {e}")
        return None
    finally:
        if conn:
            conn.close()

# 路由：仪表盘页面
@app.route('/dashboard')
def dashboard():
    # 直接获取官方ETF列表，而不是使用全局SYMBOLS变量
    symbols = get_official_etf_list()
    
    # 再次确保列表中只有官方ETF
    symbols = [s for s in symbols if s.get('is_official', 1) == 1]
    
    # 添加日志，记录传递给模板的ETF数量和代码
    logger.info(f"传递给dashboard页面的官方ETF数量: {len(symbols)}")
    logger.debug(f"ETF代码列表: {[s['code'] for s in symbols]}")
    
    symbol = request.args.get('symbol')

    if symbol:
        # --- 显示单个ETF的详细仪表盘 --- 
        # 确保选中的ETF是官方ETF
        is_official = any(s['code'] == symbol for s in symbols)
        if not is_official:
            # 如果不是官方ETF，重定向到列表页
            flash(f"ETF {symbol} 不是官方支持的ETF", "warning")
            return redirect(url_for('dashboard'))
        
        # 检查数据库中是否有最新数据
        from datetime import datetime, timedelta
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取最新的一条记录
        cursor.execute('''
        SELECT * FROM etf_data 
        WHERE symbol = ? 
        ORDER BY date DESC LIMIT 1
        ''', (symbol,))
        
        latest_record = cursor.fetchone()
        
        # 判断是否需要从远程获取最新数据
        need_update = True
        data_status = ''  # 数据状态
        
        if latest_record:
            record_date = datetime.strptime(latest_record['date'], '%Y-%m-%d').date()
            today = datetime.now().date()
            
            # 判断今天是否是交易日
            is_weekend = today.weekday() >= 5  # 5是周六，6是周日
            
            # 计算最近有效交易日的日期
            if is_weekend:
                days_to_subtract = today.weekday() - 4
                last_trading_day = today - timedelta(days=days_to_subtract)
            else:
                last_trading_day = today
            
            # 如果数据库记录日期是最近有效交易日或更近，无需更新
            if record_date >= last_trading_day or (record_date >= today - timedelta(days=1)):
                need_update = False
                data_status = '使用本地数据'
        
        conn.close()
        
        # 如果需要更新数据，从远程获取
        if need_update:
            logger.info(f"仪表盘访问：从远程获取{symbol}的最新数据")
            try:
                df, _ = get_etf_data(symbol)
                current_price = df['close'].iloc[-1]
                volatility_series = calculate_volatility(symbol)
                volatility = volatility_series.iloc[-1]
                grid_spacing = volatility / 8
                grid_range = calculate_grid_range(symbol)
                upper_limit = grid_range['H_val'].iloc[-1]
                lower_limit = grid_range['L_val'].iloc[-1]
                range_percentage = 2 * (upper_limit - lower_limit) / (upper_limit + lower_limit)
                grid_levels = round(range_percentage / grid_spacing)
                
                range_diff = upper_limit - lower_limit
                if range_diff > 0:
                    level_fraction = max(0, min(1, (current_price - lower_limit) / range_diff))
                    current_level = round(level_fraction * grid_levels)
                    position = 1 - level_fraction
                else:
                    current_level = grid_levels // 2
                    position = 0.5
                
                position = max(0, min(1, position))
                
                result = {
                    'symbol': symbol,
                    'name': next((s['name'] for s in symbols if s['code'] == symbol), ''),
                    'current_price': float(current_price),
                    'volatility': round(float(volatility * 100), 2),
                    'grid_spacing': round(float(grid_spacing * 100), 2),
                    'upper_limit': round(float(upper_limit), 2),
                    'lower_limit': round(float(lower_limit), 2),
                    'range_percentage': round(float(range_percentage * 100), 2),
                    'grid_levels': int(grid_levels),
                    'current_level': int(current_level),
                    'position': round(float(position * 100)),
                    'data_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                save_historical_data(symbol, result)
                data_status = '数据已更新'
                
            except Exception as e:
                logger.error(f"从远程获取{symbol}数据时出错: {str(e)}")
                import traceback
                traceback.print_exc()
                data_status = '更新失败'
        
        # 明确使用全局SYMBOLS变量，确保只显示官方ETF
        # 渲染单个ETF的仪表盘
        return render_template('dashboard.html', symbols=symbols, selected=symbol, data_status=data_status)

    else:
        # --- 显示ETF列表页面 --- 
        etf_list_data = []
        # 明确获取官方ETF列表，而不是从任何其他来源
        official_etfs = get_official_etf_list() # 获取官方ETF列表
        
        for etf in official_etfs:
            latest_data = get_latest_etf_data(etf['code'])
            etf_info = {
                'name': etf['name'],
                'code': etf['code'],
                'volatility': round(latest_data['volatility'] * 100, 2) if latest_data and latest_data['volatility'] is not None else 'N/A',
                'position': round(latest_data['position'] * 100) if latest_data and latest_data['position'] is not None else 'N/A',
                'grid_spacing': round(latest_data['grid_spacing'] * 100, 2) if latest_data and latest_data['grid_spacing'] is not None else 'N/A',
            }
            etf_list_data.append(etf_info)
            
        return render_template('etf_list.html', etf_list=etf_list_data)

# 路由：历史数据页面
@app.route('/history')
def history():
    # 刷新全局ETF列表，确保使用最新的官方ETF列表
    global SYMBOLS
    SYMBOLS = get_official_etf_list()
    
    symbol = request.args.get('symbol', '510300')  # 默认为沪深300
    
    # 确保选中的ETF是官方ETF
    is_official = any(s['code'] == symbol for s in SYMBOLS)
    if not is_official:
        # 如果不是官方ETF，重定向到默认ETF
        flash(f"ETF {symbol} 不是官方支持的ETF", "warning")
        return redirect(url_for('history', symbol='510300'))
    
    # 明确使用全局SYMBOLS变量，确保只传递官方ETF列表
    return render_template('history.html', symbols=SYMBOLS, selected=symbol)

# 路由：投资组合分配页面
@app.route('/portfolio')
def portfolio():
    # 刷新全局ETF列表，确保使用最新的官方ETF列表
    global SYMBOLS
    SYMBOLS = get_official_etf_list()
    
    # 检查是否有portfolio_id参数
    portfolio_id = request.args.get('portfolio_id')
    
    if portfolio_id:
        # 获取用户信息
        user = get_current_user()
        if not user:
            flash("请先登录以访问您的自定义投资组合", "error")
            return redirect(url_for('login', next=request.url))
        
        # 获取用户ID
        user_id = get_user_id(user)
        
        # 获取指定的投资组合
        portfolio = Portfolio.get_by_id(portfolio_id, user_id)
        if not portfolio:
            flash("找不到指定的投资组合或您没有权限访问", "error")
            return redirect(url_for('my_portfolios'))
        
        # 对ETF列表进行处理，包含官方ETF和自定义ETF
        etf_list = []
        for etf in portfolio['etfs']:
            # 尝试查找官方ETF
            is_official = False
            for symbol_info in SYMBOLS:
                if symbol_info['code'] == etf['symbol']:
                    is_official = True
                    etf_info = symbol_info.copy()
                    etf_info['weight'] = f"{etf['weight']}%"  # 使用自定义权重
                    etf_list.append(etf_info)
                    break
            
            # 如果不是官方ETF，可能是自定义ETF
            if not is_official:
                # 获取自定义ETF信息
                custom_etf = CustomETF.get_custom_etf(user_id, etf['symbol'])
                if custom_etf:
                    # 创建与官方ETF格式兼容的字典
                    etf_info = {
                        'code': custom_etf['symbol'],
                        'name': custom_etf['display_name'] if 'display_name' in custom_etf and custom_etf['display_name'] else custom_etf['name'],
                        'category': '自定义ETF',  # 分类标记为自定义
                        'correlation': '未分类',
                        'volatility_type': '未知',
                        'weight': f"{etf['weight']}%",
                        'is_custom': True  # 标记为自定义ETF
                    }
                    etf_list.append(etf_info)
                    logger.info(f"添加自定义ETF到投资组合显示: {etf['symbol']}")
                else:
                    logger.warning(f"投资组合中包含未知ETF: {etf['symbol']}，无法在用户ETF列表中找到")
        
        # 检查是否有ETF
        has_etfs = len(etf_list) > 0
        
        # 将ETF列表转换为JSON格式，包含所有ETF的code字段
        symbols_json = json.dumps([etf['code'] for etf in etf_list])
        
        # 创建包含data-custom和data-name属性的ETF元素数据
        etf_elements = []
        for etf in etf_list:
            etf_element = {
                'symbol': etf['code'],
                'weight': etf['weight'],
                'name': etf['name'],
                'is_custom': etf.get('is_custom', False)
            }
            etf_elements.append(etf_element)
        
        # 将ETF元素数据转换为JSON格式
        etf_elements_json = json.dumps(etf_elements)
        
        return render_template(
            'portfolio.html', 
            symbols=etf_list, 
            symbols_json=symbols_json,
            etf_elements=etf_elements_json,
            portfolio=portfolio,
            is_custom_portfolio=True,
            has_etfs=has_etfs
        )
    else:
        # 默认使用所有官方ETF，不包含自定义ETF
        # 注意：仅使用刚刚刷新的全局SYMBOLS列表，确保只有官方ETF
        etf_elements = []
        
        # 使用集合来跟踪已添加的ETF代码，避免重复
        added_symbols = set()
        
        # 仅使用全局SYMBOLS变量，不从数据库中获取ETF列表
        for symbol in SYMBOLS:
            # 如果ETF代码已经添加过，则跳过
            if symbol['code'] in added_symbols:
                logger.warning(f"跳过重复的官方ETF代码: {symbol['code']}")
                continue
                
            etf_element = {
                'symbol': symbol['code'],
                'weight': symbol.get('weight', '1.0%'),
                'name': symbol['name'],
                'is_custom': False
            }
            etf_elements.append(etf_element)
            
            # 记录已添加的ETF代码
            added_symbols.add(symbol['code'])
        
        # 获取唯一的symbols_json，仅包含SYMBOLS中的代码
        unique_symbols = [symbol['code'] for symbol in SYMBOLS if symbol['code'] in added_symbols]
        symbols_json = json.dumps(unique_symbols)
        
        # 将ETF元素数据转换为JSON格式
        etf_elements_json = json.dumps(etf_elements)
    
        # 使用SYMBOLS列表传递给模板，确保只有官方ETF
        return render_template('portfolio.html', symbols=SYMBOLS, symbols_json=symbols_json, etf_elements=etf_elements_json, is_custom_portfolio=False, portfolio={})

# 路由：数据库诊断API
@app.route('/api/diagnostics/db')
def db_diagnostics():
    """数据库诊断API"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='etf_data'")
        tables = cursor.fetchall()
        
        # 获取记录数
        record_count = 0
        if tables:
            cursor.execute("SELECT COUNT(*) as count FROM etf_data")
            record_count = cursor.fetchone()['count']
        
        # 获取最近的记录
        recent_records = []
        if record_count > 0:
            cursor.execute("SELECT * FROM etf_data ORDER BY date DESC LIMIT 5")
            rows = cursor.fetchall()
            for row in rows:
                recent_records.append({
                    'date': row['date'],
                    'symbol': row['symbol'],
                    'price': row['price']
                })
        
        conn.close()
        
        return jsonify({
            'status': 'ok',
            'database_exists': os.path.exists('database/etf_history.db'),
            'tables': [table['name'] for table in tables],
            'record_count': record_count,
            'recent_records': recent_records
        })
    
    except Exception as e:
        logger.error(f"数据库诊断失败: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

def validate_etf_data(df, symbol):
    """验证ETF数据是否有效"""
    if df is None or df.empty:
        raise ValueError(f"获取到的{symbol}数据为空")
    
    if 'close' not in df.columns:
        raise ValueError(f"{symbol}数据缺少'close'列")
    
    if len(df) < 30:
        logger.warning(f"警告: {symbol}数据少于30天，只有{len(df)}天")
    
    return True

@app.route('/api/generate_history/<symbol>')
def generate_history(symbol):
    try:
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        
        logger.info(f"生成{symbol}的历史数据")
        
        # 获取ETF数据
        df, _ = get_etf_data(symbol)
        
        if df is None or len(df) == 0:
            logger.error(f"未找到{symbol}的行情数据")
            return jsonify({"error": "未找到ETF行情数据"}), 404
        
        # 先检查最新数据是否在数据库中，如果有则直接返回
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取最新数据日期
        latest_date = df.index[-1].strftime('%Y-%m-%d')
        
        cursor.execute('''
        SELECT * FROM etf_data 
        WHERE symbol = ? AND date = ?
        ''', (symbol, latest_date))
        
        latest_record = cursor.fetchone()
        
        if latest_record:
            # 已经有最新数据，直接从数据库获取历史数据
            cursor.execute('''
            SELECT * FROM etf_data 
            WHERE symbol = ? 
            ORDER BY date DESC LIMIT 30
            ''', (symbol,))
            
            rows = cursor.fetchall()
            conn.close()
            
            if rows:
                history = []
                for row in rows:
                    history.append({
                        'date': row['date'],
                        'price': float(row['price']),
                        'volatility': float(row['volatility'] * 100),  # 转换为百分比
                        'grid_spacing': float(row['grid_spacing'] * 100),  # 转换为百分比
                        'upper_limit': float(row['upper_limit']),
                        'lower_limit': float(row['lower_limit']),
                        'current_level': int(row['current_level']),
                        'total_levels': int(row['total_levels']),
                        'position': float(row['position'] * 100)  # 转换为百分比
                    })
                
                # 按日期升序排序
                history.reverse()
                
                logger.info(f"从数据库获取了{len(history)}条历史数据")
                return jsonify(history)
        
        # 计算历史波动率
        volatility = calculate_volatility(symbol)
        
        # 计算历史网格间隔
        grid_spacing = calculate_grid_spacing(symbol)
        
        # 计算历史网格范围
        grid_range = calculate_grid_range(symbol)
        
        # 生成历史数据
        history = []
        
        # 使用最近30天数据
        date_range = min(30, len(df))
        recent_df = df.iloc[-date_range:]
        
        for i in range(len(recent_df)):
            idx = recent_df.index[i]
            date_str = idx.strftime('%Y-%m-%d')
            price = float(recent_df['close'].iloc[i])
            
            # 获取对应日期的波动率
            vol_idx = volatility.index.get_indexer([idx], method='nearest')[0]
            vol = float(volatility.iloc[vol_idx])
            spacing = vol / 8
            
            # 获取对应日期的网格区间
            range_idx = grid_range.index.get_indexer([idx], method='nearest')[0]
            upper_limit = float(grid_range['H_val'].iloc[range_idx])
            lower_limit = float(grid_range['L_val'].iloc[range_idx])
            
            # 计算网格层数
            range_percentage = 2 * (upper_limit - lower_limit) / (upper_limit + lower_limit)
            grid_levels = int(round(range_percentage / spacing))
            
            # 计算当前层数与仓位
            if price <= lower_limit:
                current_level = 0
                position = 100
            elif price >= upper_limit:
                current_level = grid_levels
                position = 0
            else:
                current_level = int(round((price - lower_limit) / (spacing * lower_limit)))
                position = 100 * (1 - current_level / grid_levels)
            
            # 确保值在有效范围内
            current_level = max(0, min(grid_levels, current_level))
            position = max(0, min(100, position))
            
            # 添加到历史数据中
            history.append({
                'date': date_str,
                'price': price,
                'volatility': round(vol * 100, 1),  # 转换为百分比并保留一位小数
                'grid_spacing': round(spacing * 100, 1),  # 转换为百分比并保留一位小数
                'upper_limit': round(upper_limit, 2),
                'lower_limit': round(lower_limit, 2),
                'current_level': current_level,
                'total_levels': grid_levels,
                'position': round(position)  # 四舍五入为整数
            })
            
            # 同时保存到数据库
            save_calculated_history(symbol, {'date': date_str, 'price': price, 'volatility': vol, 
                                            'grid_spacing': spacing, 'upper_limit': upper_limit, 
                                            'lower_limit': lower_limit, 'current_level': current_level, 
                                            'total_levels': grid_levels, 'position': position / 100})
        
        logger.info(f"成功生成{len(history)}条历史数据")
        return jsonify(history)
    
    except Exception as e:
        logger.error(f"生成历史数据失败: {str(e)}")
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"错误详情: {error_details}")
        return jsonify({'error': str(e), 'details': error_details}), 500

@app.route('/api/mock_history/<symbol>')
def mock_history(symbol):
    """生成模拟历史数据，用于测试"""
    try:
        import random
        from datetime import datetime, timedelta
        
        print(f"生成{symbol}的模拟历史数据")
        
        # 生成30天的日期
        end_date = datetime.now()
        dates = [(end_date - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(29, -1, -1)]
        
        # 生成模拟价格数据
        base_price = 10.0  # 基础价格
        if symbol == '510300':  # 沪深300
            base_price = 3.5
        elif symbol == '510500':  # 中证500
            base_price = 6.2
        elif symbol == '588000':  # 科创50
            base_price = 1.0
        
        # 生成波动价格
        prices = []
        price = base_price
        for _ in range(30):
            price = price * (1 + random.uniform(-0.02, 0.02))  # 每天±2%的波动
            prices.append(round(price, 2))
        
        # 生成其他数据
        volatility = [round(random.uniform(15, 25), 1) for _ in range(30)]  # 15%-25%的波动率
        grid_spacing = [round(vol / 8, 1) for vol in volatility]  # 网格间隔为波动率/8
        
        # 计算上下限
        upper_limits = [round(price * 1.2, 2) for price in prices]  # 上限为价格的1.2倍
        lower_limits = [round(price * 0.8, 2) for price in prices]  # 下限为价格的0.8倍
        
        # 计算网格层数和仓位
        grid_levels = [random.randint(8, 12) for _ in range(30)]
        current_levels = [random.randint(0, level) for level in grid_levels]
        positions = [round(100 * (1 - level / max(1, total))) for level, total in zip(current_levels, grid_levels)]
        
        # 组装历史数据
        history = []
        for i in range(30):
            history.append({
                'date': dates[i],
                'price': prices[i],
                'volatility': volatility[i],
                'grid_spacing': grid_spacing[i],
                'upper_limit': upper_limits[i],
                'lower_limit': lower_limits[i],
                'current_level': current_levels[i],
                'total_levels': grid_levels[i],
                'position': positions[i]
            })
        
        print(f"成功生成{len(history)}条模拟历史数据")
        return jsonify(history)
    
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"生成模拟历史数据失败: {str(e)}")
        print(f"错误详情: {error_details}")
        return jsonify({'error': str(e), 'details': error_details}), 500

# 添加回测页面路由
@app.route('/backtest')
def backtest():
    return render_template('backtest.html', symbols=SYMBOLS)

# 添加运行回测API
@app.route('/run_backtest', methods=['POST'])
def run_backtest():
    try:
        # 获取回测参数
        data = request.json
        logger.info(f"收到回测请求: {data}")
        
        mode = data.get('mode')
        symbols = data.get('symbols')
        initial_capital = float(data.get('initial_capital'))
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        grid_levels = int(data.get('grid_levels'))
        grid_type = data.get('grid_type')
        
        # 检查参数有效性
        if not symbols:
            return jsonify({'error': '未提供ETF代码'}), 400
        
        # 转换日期格式
        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
            
            # 限制回测日期范围不超过1年
            date_diff = (end_date - start_date).days
            if date_diff > 365:
                logger.warning(f"回测日期范围过长: {date_diff}天，已自动调整为最近1年")
                start_date = end_date - timedelta(days=365)
        except ValueError as e:
            return jsonify({'error': f'日期格式错误: {str(e)}'}), 400
        
        # 添加超时机制
        result_queue = queue.Queue()
        
        def run_backtest_with_timeout():
            try:
                if mode == 'single':
                    # 单ETF回测
                    logger.info(f"开始单ETF回测: {symbols[0]}")
                    result = backtest_single_etf(symbols[0], initial_capital, start_date, end_date, grid_levels, grid_type)
                else:
                    # ETF组合回测
                    logger.info(f"开始ETF组合回测: {symbols}")
                    result = backtest_portfolio(symbols, initial_capital, start_date, end_date, grid_levels, grid_type)
                
                result_queue.put(result)
            except Exception as e:
                logger.error(f"回测线程中出错: {str(e)}")
                result_queue.put({'error': f'回测失败: {str(e)}'})
        
        # 启动回测线程
        backtest_thread = threading.Thread(target=run_backtest_with_timeout)
        backtest_thread.daemon = True
        backtest_thread.start()
        
        # 等待回测完成，最多等待60秒
        backtest_thread.join(timeout=60)
        
        if backtest_thread.is_alive():
            # 回测超时
            logger.error("回测超时，已强制终止")
            return jsonify({'error': '回测超时，请尝试缩小日期范围或减少ETF数量'}), 504
        
        # 获取回测结果
        result = result_queue.get()
        
        # 检查结果是否包含错误
        if 'error' in result:
            logger.error(f"回测失败: {result['error']}")
            return jsonify(result), 500
        
        logger.info("回测完成")
        return jsonify(result)
    except Exception as e:
        logger.error(f"回测过程中出错: {str(e)}")
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"错误详情: {error_details}")
        return jsonify({'error': f'回测失败: {str(e)}', 'details': error_details}), 500

# 添加导出CSV功能
@app.route('/export_backtest_csv')
def export_backtest_csv():
    # 获取参数
    mode = request.args.get('mode')
    symbols = request.args.get('symbols').split(',')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # 转换日期格式
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # 获取交易记录
    if mode == 'single':
        trades = get_backtest_trades(symbols[0], start_date, end_date)
    else:
        trades = get_backtest_trades_portfolio(symbols, start_date, end_date)
    
    # 创建CSV文件
    output = io.StringIO()
    writer = csv.writer(output)
    
    # 写入标题行
    writer.writerow(['日期', 'ETF代码', '交易类型', '价格', '数量', '金额', '利润'])
    
    # 写入交易记录
    for trade in trades:
        writer.writerow([
            trade['date'],
            trade['symbol'],
            trade['type'],
            trade['price'],
            trade['quantity'],
            trade['amount'],
            trade.get('profit', '0.00')  # 使用get方法，如果没有profit字段则默认为0
        ])
    
    # 设置响应头
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-disposition": f"attachment; filename=backtest_{datetime.now().strftime('%Y%m%d')}.csv"}
    )

# 单ETF回测函数
def backtest_single_etf(symbol, initial_capital, start_date, end_date, grid_levels, grid_type, volatility=None, grid_spacing=None, grid_range_upper=None, grid_range_lower=None):
    try:
        logger.info(f"开始回测 {symbol}，初始资金: {initial_capital}，开始日期: {start_date}，结束日期: {end_date}")
        
        # 获取ETF数据 - 确保获取完整数据
        logger.info(f"正在获取 {symbol} 的数据...")
        df, _ = get_etf_data(symbol)
        
        # 首先清理数据，移除close列中的NaN值
        df = df.dropna(subset=['close'])
        
        # 确保数据连续性，检查是否有缺失日期
        all_dates = pd.date_range(start=start_date, end=end_date, freq='B')  # 'B'表示工作日
        # 通过reindex扩展数据帧，确保所有工作日都包含在内，缺失的用前值填充
        df_expanded = df.reindex(all_dates.intersection(pd.date_range(start=df.index.min(), end=df.index.max(), freq='B')))
        df_expanded = df_expanded.fillna(method='ffill')
        
        # 筛选日期范围
        df = df_expanded[(df_expanded.index >= start_date) & (df_expanded.index <= end_date)]
        
        # 再次检查并移除可能的NaN值 - 特别是开始边界的数据
        df = df.dropna(subset=['close'])
        
        logger.info(f"筛选后的数据范围: {df.index[0] if not df.empty else '无数据'} 到 {df.index[-1] if not df.empty else '无数据'}，共 {len(df)} 条记录")
        
        # 检查数据集是否为空
        if df.empty:
            return {
                'error': '回测日期范围内无有效数据，请选择其他日期范围或其他ETF'
            }
        
        # 确保有足够的数据进行回测
        if len(df) < 10:
            return {
                'error': '回测日期范围内数据不足，请选择更长的时间范围'
            }
        
        # 初始化变量
        cash = initial_capital  # 初始现金
        cash_reserve = initial_capital * 0.5  # 预留一半资金作为后续买入储备
        initial_investment = initial_capital - cash_reserve  # 初始投资金额
        position = 0  # 初始持仓数量
        trades = []  # 交易记录
        
        # 资金曲线数据
        dates = []
        total_equity = []
        invested_capital = []
        profit_values = []
        
        # 回测统计
        buy_count = 0
        sell_count = 0
        win_count = 0
        
        # 定义网格重置频率（每30个交易日重新评估网格）
        grid_reset_frequency = 30
        days_since_reset = 0
        
        # 获取第一天的价格
        first_day_price = df['close'].iloc[0]
        logger.info(f"第一天价格: {first_day_price}")
        
        # 检查第一天价格是否为NaN
        if pd.isna(first_day_price):
            return {
                'error': '回测日期范围内第一天价格数据无效，请选择其他日期范围或检查ETF数据'
            }
            
        # 记录当前月份，用于判断是否需要重新评估网格
        current_month = df.index[0].month
        
        # 初始化网格相关变量
        grid_prices = []
        grid_states = []
        grid_buy_prices = {}
        grid_trade_shares = {}
        
        # ----------- 修改算法开始 -----------
        
        # 划分初始网格函数
        def setup_grid(current_date, current_price, remaining_capital):
            nonlocal grid_prices, grid_states, grid_buy_prices, grid_trade_shares
            
            # 获取波动率数据
            day_volatility = volatility
            if day_volatility is None:
                try:
                    volatility_series = calculate_volatility(symbol)
                    if current_date in volatility_series.index:
                        day_volatility = volatility_series.loc[current_date]
                    else:
                        # 使用最接近的日期
                        closest_date = min(volatility_series.index, key=lambda x: abs(x - current_date))
                        day_volatility = volatility_series.loc[closest_date]
                    
                    # 检查是否为NaN
                    if pd.isna(day_volatility):
                        logger.warning(f"日期 {current_date.strftime('%Y-%m-%d')} 的波动率为NaN，使用默认值0.2")
                        day_volatility = 0.2  # 默认波动率20%
                    else:
                        logger.info(f"日期 {current_date.strftime('%Y-%m-%d')} 使用历史波动率: {day_volatility:.4f}")
                except Exception as e:
                    logger.warning(f"无法获取历史波动率，使用默认值: {str(e)}")
                    day_volatility = 0.2  # 默认波动率20%
            
            # 获取网格间隔
            day_grid_spacing = grid_spacing
            if day_grid_spacing is None:
                day_grid_spacing = day_volatility / 8
                # 检查是否为NaN或异常值
                if pd.isna(day_grid_spacing) or day_grid_spacing <= 0:
                    logger.warning(f"计算的网格间隔无效: {day_grid_spacing}，使用默认值0.025")
                    day_grid_spacing = 0.025  # 默认间隔2.5%
                else:
                    logger.info(f"计算的网格间隔: {day_grid_spacing:.4f}")
            
            # 确定网格上下限
            day_upper_limit = grid_range_upper
            day_lower_limit = grid_range_lower
            
            if day_upper_limit is None or day_lower_limit is None:
                try:
                    grid_range_data = calculate_grid_range(symbol)
                    if current_date in grid_range_data.index:
                        range_data = grid_range_data.loc[current_date]
                    else:
                        # 使用最接近的日期
                        closest_date = min(grid_range_data.index, key=lambda x: abs(x - current_date))
                        range_data = grid_range_data.loc[closest_date]
                    
                    day_upper_limit = range_data['H_val']
                    day_lower_limit = range_data['L_val']
                    
                    # 检查是否有NaN值
                    if pd.isna(day_upper_limit) or pd.isna(day_lower_limit):
                        logger.warning(f"计算的网格范围包含NaN值，使用当前价格的倍数替代")
                        day_upper_limit = current_price * 1.3
                        day_lower_limit = current_price * 0.7
                    else:
                        logger.info(f"使用计算的网格范围: 上限={day_upper_limit:.4f}, 下限={day_lower_limit:.4f}")
                except Exception as e:
                    logger.warning(f"无法获取网格范围，使用默认值: {str(e)}")
                    # 设置更宽的价格范围，尤其是下跌空间
                    day_upper_limit = current_price * 1.3
                    day_lower_limit = current_price * 0.6  # 为下跌预留更大空间
            
            # 确保当前价格在网格范围内，如果不在则调整范围
            if current_price >= day_upper_limit:
                day_upper_limit = current_price * 1.1
            if current_price <= day_lower_limit:
                day_lower_limit = current_price * 0.9
            
            # 划分网格
            grid_prices = []
            if grid_type == 'arithmetic':
                # 等差网格
                step = (day_upper_limit - day_lower_limit) / (grid_levels - 1)
                grid_prices = [day_lower_limit + i * step for i in range(grid_levels)]
                logger.info(f"创建等差网格，步长: {step:.4f}")
            elif grid_type == 'geometric':
                # 等比网格 - 在下跌趋势中表现更好
                ratio = (day_upper_limit / day_lower_limit) ** (1 / (grid_levels - 1))
                grid_prices = [day_lower_limit * (ratio ** i) for i in range(grid_levels)]
                logger.info(f"创建等比网格，比例: {ratio:.4f}")
            else:  # 波动率网格
                # 使用波动率计算的网格间隔
                mid_price = (day_upper_limit + day_lower_limit) / 2
                half_levels = grid_levels // 2
                
                # 计算中心价格上下的网格价格
                grid_prices = [mid_price]
                for i in range(1, half_levels + 1):
                    grid_prices.append(mid_price * (1 + i * day_grid_spacing))
                    grid_prices.insert(0, mid_price * (1 - i * day_grid_spacing))
                
                # 确保网格数量正确
                while len(grid_prices) > grid_levels:
                    grid_prices.pop()  # 移除多余的网格
                while len(grid_prices) < grid_levels:
                    # 如果网格数量不足，添加额外的网格
                    grid_prices.append(grid_prices[-1] * (1 + day_grid_spacing))
                
                logger.info(f"创建波动率网格，中心价格: {mid_price:.4f}，间隔比例: {day_grid_spacing:.4f}")
            
            # 对网格价格排序
            grid_prices.sort()
            
            # 检查网格价格中是否有NaN值
            if any(pd.isna(price) for price in grid_prices):
                logger.warning("网格价格中存在NaN值，使用等差网格重新计算")
                step = (current_price * 1.3 - current_price * 0.7) / (grid_levels - 1)
                grid_prices = [current_price * 0.7 + i * step for i in range(grid_levels)]
                logger.info(f"创建替代等差网格，步长: {step:.4f}")
            
            # 初始化网格状态
            grid_states = [False] * grid_levels
            grid_buy_prices = {}
            
            # 计算每个网格的交易份额 - 确保资金分配更均匀，尤其在下跌趋势中有足够资金买入
            # 对于下跌趋势，我们希望低价位的网格有更多资金
            total_weight = sum(range(1, grid_levels + 1))
            grid_trade_shares = {}
            
            for level in range(grid_levels):
                # 网格权重：低价位网格有更高权重
                weight = (level + 1) / total_weight
                # 分配资金比例，确保低价位网格有更多资金
                grid_capital = remaining_capital * weight
                
                grid_price = grid_prices[level]
                # 每个网格的交易股数，确保是100的整数倍
                shares = int(grid_capital / grid_price / 100) * 100
                shares = max(100, shares)  # 确保至少100股
                grid_trade_shares[level] = shares
                logger.info(f"网格 {level+1}: 价格={grid_price:.4f}, 交易股数={shares}, 资金占比={weight*100:.2f}%")
            
            # 找出当前价格所在的网格
            current_grid = 0
            for i in range(grid_levels):
                if current_price <= grid_prices[i]:
                    current_grid = i
                    break
            if current_price > grid_prices[-1]:
                current_grid = grid_levels - 1
                
            return current_grid

        # 初始设置网格
        prev_grid = setup_grid(df.index[0], first_day_price, initial_investment)
        
        # 初始买入 - 根据第一天价格在网格中的位置和波动率动态计算初始仓位
        # 计算价格在网格中的相对位置 (0表示在最底部，1表示在最顶部)
        price_position = 0
        if len(grid_prices) > 1:
            price_position = (first_day_price - grid_prices[0]) / (grid_prices[-1] - grid_prices[0])
            price_position = max(0, min(1, price_position))  # 确保在0-1之间
        
        # 使用与dashboard相同的计算仓位逻辑
        # 价格位置越低（接近下轨），仓位越高；价格位置越高（接近上轨），仓位越低
        position_ratio = 1 - price_position
        position_ratio = max(0.3, min(0.9, position_ratio))  # 确保仓位在30%-90%之间
        
        logger.info(f"根据价格位置计算初始仓位: 价格={first_day_price:.4f}, 价格位置={price_position:.2f}, 仓位比例={position_ratio:.2f}")
        
        initial_buy_amount = initial_investment * position_ratio
        
        logger.info(f"动态计算初始仓位: 价格位置={price_position:.2f}, 仓位比例={position_ratio:.2f}")
        
        # 计算买入数量，确保是100的整数倍
        buy_quantity = int(initial_buy_amount / first_day_price / 100) * 100
        
        if buy_quantity > 0:
            # 执行买入
            cost = buy_quantity * first_day_price
            cash -= cost
            position += buy_quantity
            
            # 计算持仓金额
            position_value = position * first_day_price
            
            # 记录交易
            trade = {
                'date': df.index[0].strftime('%Y-%m-%d'),
                'symbol': symbol,
                'type': '买入',
                'price': f'{first_day_price:.4f}',
                'quantity': buy_quantity,
                'amount': f'{cost:.2f}',
                'profit': '0.00',
                'position_ratio': f'{position_ratio:.2%}',
                'position_value': f'{position_value:.2f}'
            }
            trades.append(trade)
            buy_count += 1
            logger.info(f"初始买入: {buy_quantity}股，价格: {first_day_price:.4f}，金额: {cost:.2f}，持仓金额: {position_value:.2f}")
        
        # 记录第一天的资金数据
        first_day = df.index[0]
        equity = cash + position * first_day_price
        invested = initial_capital - cash
        profit = equity - initial_capital
        
        dates.append(first_day.strftime('%Y-%m-%d'))
        total_equity.append(equity)
        invested_capital.append(invested)
        profit_values.append(profit)
        
        # 遍历后续交易日，执行网格交易
        logger.info("开始遍历后续交易日...")
        
        # 从第二天开始遍历
        for day_idx in range(1, len(df)):
            current_day = df.index[day_idx]
            current_price = df['close'].iloc[day_idx]
            days_since_reset += 1
            
            # 检查是否需要重置网格（每月或每30个交易日）
            if current_day.month != current_month or days_since_reset >= grid_reset_frequency:
                logger.info(f"重置网格: 日期={current_day.strftime('%Y-%m-%d')}, 价格={current_price:.4f}")
                prev_grid = setup_grid(current_day, current_price, cash)
                current_month = current_day.month
                days_since_reset = 0
            
            # 找出当前价格所在的网格
            current_grid = 0
            for i in range(grid_levels):
                if current_price <= grid_prices[i]:
                    current_grid = i
                    break
            if current_price > grid_prices[-1]:
                current_grid = grid_levels - 1
            
            # 检查是否穿越网格
            if current_grid != prev_grid:
                logger.info(f"日期: {current_day.strftime('%Y-%m-%d')}, 价格: {current_price:.4f}, 从网格 {prev_grid+1} 移动到网格 {current_grid+1}")
                
                # 向上穿越（卖出）
                if current_grid > prev_grid:
                    # 穿越了多个网格，逐个处理
                    for grid in range(prev_grid + 1, current_grid + 1):
                        # 确保有持仓可卖
                        if position > 0:
                            # 计算卖出数量（每个网格对应的交易量）
                            sell_quantity = min(position, grid_trade_shares[grid])
                            
                            if sell_quantity > 0:
                                # 查看是否有这个网格的买入价格记录
                                grid_buy_price = grid_buy_prices.get(grid, 0)
                                
                                # 网格交易策略中，不限制必须高于买入价格，而是遵循网格规则
                                
                                # 执行卖出
                                sale_amount = sell_quantity * current_price
                                cash += sale_amount
                                position -= sell_quantity
                                
                                # 计算当前持仓金额
                                position_value = position * current_price
                                
                                # 计算利润（允许负值，记录真实利润）
                                if grid_buy_price > 0:
                                    trade_profit = (current_price - grid_buy_price) * sell_quantity
                                else:
                                    # 如果没有买入记录，使用更准确的平均成本计算方法
                                    # 查找之前的买入交易，计算平均买入成本
                                    buy_trades = [t for t in trades if t['type'] == '买入']
                                    if buy_trades:
                                        total_buy_amount = sum(float(t['amount']) for t in buy_trades)
                                        total_buy_quantity = sum(int(t['quantity']) for t in buy_trades)
                                        avg_cost = total_buy_amount / total_buy_quantity if total_buy_quantity > 0 else 0
                                    else:
                                        avg_cost = 0
                                    
                                    trade_profit = (current_price - avg_cost) * sell_quantity
                                    # 确保利润不会过大
                                    max_reasonable_profit = sale_amount * 0.2  # 限制单笔利润最大为成交金额的20%
                                    if trade_profit > max_reasonable_profit:
                                        trade_profit = max_reasonable_profit
                                
                                # 记录交易
                                trade = {
                                    'date': current_day.strftime('%Y-%m-%d'),
                                    'symbol': symbol,
                                    'type': '卖出',
                                    'price': f'{current_price:.4f}',
                                    'quantity': sell_quantity,
                                    'amount': f'{sale_amount:.2f}',
                                    'profit': f'{trade_profit:.2f}',
                                    'grid': grid + 1,
                                    'position_value': f'{position_value:.2f}'
                                }
                                trades.append(trade)
                                sell_count += 1
                                win_count += 1
                                
                                logger.info(f"网格上穿卖出: 网格={grid+1}, 价格={current_price:.4f}, 数量={sell_quantity}, 金额={sale_amount:.2f}, 利润={trade_profit:.2f}, 持仓金额={position_value:.2f}")
                
                # 向下穿越（买入）- 特别关注持续下跌情况
                elif current_grid < prev_grid:
                    # 穿越了多个网格，逐个处理
                    for grid in range(prev_grid, current_grid, -1):
                        # 计算买入数量
                        buy_quantity = grid_trade_shares[grid-1]  # 注意索引
                        
                        # 在价格持续下跌时，确保有足够资金继续买入
                        # 如果资金不足，减少买入数量但确保至少买入一些
                        if cash < buy_quantity * current_price:
                            # 至少买入1/3的计划数量，或资金允许的最大数量
                            min_buy = max(buy_quantity // 3, 100)
                            buy_quantity = min(int(cash / current_price / 100) * 100, buy_quantity)
                            buy_quantity = max(min_buy, buy_quantity)
                            logger.info(f"资金不足，调整买入数量: {buy_quantity}股")
                        
                        # 检查资金是否足够
                        cost = buy_quantity * current_price
                        if cash >= cost and buy_quantity > 0:
                            # 执行买入
                            cash -= cost
                            position += buy_quantity
                            
                            # 计算当前持仓金额
                            position_value = position * current_price
                            
                            # 记录买入价格（用于后续计算卖出利润）
                            grid_buy_prices[grid-1] = current_price
                            
                            # 记录交易
                            trade = {
                                'date': current_day.strftime('%Y-%m-%d'),
                                'symbol': symbol,
                                'type': '买入',
                                'price': f'{current_price:.4f}',
                                'quantity': buy_quantity,
                                'amount': f'{cost:.2f}',
                                'profit': '0.00',
                                'grid': grid,
                                'position_value': f'{position_value:.2f}'
                            }
                            trades.append(trade)
                            buy_count += 1
                            
                            logger.info(f"网格下穿买入: 网格={grid}, 价格={current_price:.4f}, 数量={buy_quantity}, 金额={cost:.2f}, 持仓金额={position_value:.2f}")
                
                # 更新当前网格
                prev_grid = current_grid
            
            # 更新当天的资金数据
            equity = cash + position * current_price
            invested = initial_capital - cash
            profit = equity - initial_capital
            
            dates.append(current_day.strftime('%Y-%m-%d'))
            total_equity.append(equity)
            invested_capital.append(invested)
            profit_values.append(profit)
        
        # 确保回测到结束日期
        # 如果数据的最后一天不是回测的结束日期，继续计算资金曲线到结束日期
        if df.index[-1] < end_date:
            logger.info(f"数据的最后日期 {df.index[-1]} 早于回测结束日期 {end_date}，将最后一天的资产价值延续到结束日期")
            
            # 获取最后一天的资产价值
            last_equity = total_equity[-1]
            last_invested = invested_capital[-1]
            last_profit = profit_values[-1]
            
            # 从最后一个数据日期到回测结束日期，按天生成数据点
            current_date = df.index[-1] + timedelta(days=1)
            while current_date <= end_date:
                if current_date.weekday() < 5:  # 只处理工作日（周一到周五）
                    dates.append(current_date.strftime('%Y-%m-%d'))
                    total_equity.append(last_equity)
                    invested_capital.append(last_invested)
                    profit_values.append(last_profit)
                current_date += timedelta(days=1)
        
        # 回测结束前检查是否有未平仓的持仓，尝试在最后一天进行平仓以实现利润
        if position > 0:
            logger.info(f"回测结束时仍有 {position} 股未平仓，检查是否可以平仓获利")
            
            # 获取最后一天的价格
            final_price = df['close'].iloc[-1]
            
            # 计算成本价
            # 使用更准确的平均成本计算方法
            buy_trades = [t for t in trades if t['type'] == '买入']
            if buy_trades:
                total_buy_amount = sum(float(t['amount']) for t in buy_trades)
                total_buy_quantity = sum(int(t['quantity']) for t in buy_trades)
                avg_cost = total_buy_amount / total_buy_quantity if total_buy_quantity > 0 else 0
            else:
                avg_cost = 0
            
            logger.info(f"执行回测结束平仓操作，价格: {final_price:.4f}, 平均成本: {avg_cost:.4f}")
            
            # 计算平仓收益
            sale_amount = position * final_price
            cash += sale_amount
            
            # 计算平仓利润
            trade_profit = (final_price - avg_cost) * position
            # 确保利润不会过大
            max_reasonable_profit = sale_amount * 0.2  # 限制单笔利润最大为成交金额的20%
            if trade_profit > max_reasonable_profit:
                trade_profit = max_reasonable_profit
            
            # 记录交易
            trade = {
                'date': end_date.strftime('%Y-%m-%d') if isinstance(end_date, pd.Timestamp) else end_date,
                'symbol': symbol,
                'type': '卖出(平仓)',
                'price': f'{final_price:.4f}',
                'quantity': position,
                'amount': f'{sale_amount:.2f}',
                'profit': f'{trade_profit:.2f}',
                'position_value': '0.00'  # 平仓后持仓金额为0
            }
            trades.append(trade)
            sell_count += 1
            if trade_profit > 0:
                win_count += 1
            
            # 更新资金曲线最后一天的数据
            final_equity = cash  # 全部卖出后资金全部变为现金
            total_equity[-1] = final_equity
            invested_capital[-1] = 0
            profit_values[-1] = final_equity - initial_capital
            
            # 更新持仓
            final_position = position  # 记录平仓前的持仓数量用于日志输出
            position = 0
            
            logger.info(f"回测结束平仓完成: 卖出 {final_position} 股，价格 {final_price:.4f}，利润 {trade_profit:.2f}")
        
        # ----------- 修改算法结束 -----------
        
        # 计算回测统计指标
        total_days = len(df)
        total_years = total_days / 252  # 假设一年252个交易日
        
        # 年化收益率
        final_equity = total_equity[-1]
        total_return = final_equity - initial_capital
        annual_return = ((final_equity / initial_capital) ** (1 / total_years) - 1) * 100 if total_years > 0 else 0
        
        # 计算每日收益率，用于计算夏普比率
        daily_returns = []
        for i in range(1, len(total_equity)):
            daily_return = (total_equity[i] - total_equity[i-1]) / total_equity[i-1]
            daily_returns.append(daily_return)
        
        # 计算夏普比率（无风险利率假设为3%）
        risk_free_rate = 0.03 / 252  # 日化无风险利率
        if len(daily_returns) > 0:
            avg_daily_return = sum(daily_returns) / len(daily_returns)
            daily_return_std = (sum((r - avg_daily_return) ** 2 for r in daily_returns) / len(daily_returns)) ** 0.5
            sharpe_ratio = ((avg_daily_return - risk_free_rate) / daily_return_std) * (252 ** 0.5) if daily_return_std > 0 else 0
        else:
            sharpe_ratio = 0
        
        # 计算最大回撤
        max_equity = total_equity[0]
        max_drawdown = 0
        
        for equity in total_equity:
            max_equity = max(max_equity, equity)
            drawdown = (max_equity - equity) / max_equity * 100
            max_drawdown = max(max_drawdown, drawdown)
        
        # 计算胜率 - 网格交易理论上应该100%胜率
        win_rate = 100.0  # 所有卖出都是盈利的
        
        # 计算总收益金额和收益率
        total_return_amount = final_equity - initial_capital  # 总收益金额
        total_return_percentage = (final_equity / initial_capital - 1) * 100  # 总收益率(%)
        
        # 使用总收益金额作为position_profit
        position_profit = total_return_amount
        
        # 计算网格交易收益率 - 使用收益金额除以平均占用资金的比率
        # 计算平均占用资金
        avg_invested_capital = sum(invested_capital) / len(invested_capital) if invested_capital else initial_capital
        grid_profit = (total_return_amount / avg_invested_capital) * 100 if avg_invested_capital > 0 else 0
        
        # 返回回测结果
        return {
            'dates': dates,
            'total_equity': total_equity,
            'invested_capital': invested_capital,
            'profit_values': profit_values,
            'annual_return': round(annual_return, 2),
            'sharpe_ratio': round(sharpe_ratio, 2),
            'max_drawdown': round(max_drawdown, 2),
            'win_rate': round(win_rate, 2),
            'total_trades': buy_count + sell_count,
            'buy_count': buy_count,
            'sell_count': sell_count,
            'win_count': win_count,
            'position_profit': position_profit,  # 总收益金额
            'grid_profit': round(grid_profit, 2),  # 总收益率(%)
            'initial_position': position_ratio,
            'final_position': position * df['close'].iloc[-1] / final_equity,
            'grid_prices': grid_prices,
            'trades': trades
        }
    except Exception as e:
        logger.error(f"回测出错: {str(e)}", exc_info=True)
        return {
            'error': f'回测过程中发生错误: {str(e)}'
        }

# ETF组合回测函数
def backtest_portfolio(symbols, initial_capital, start_date, end_date, grid_levels, grid_type):
    # 为每个ETF分配资金
    per_etf_capital = initial_capital / len(symbols)
    
    # 存储每个ETF的回测结果
    etf_results = []
    all_trades = []
    
    # 对每个ETF进行回测
    for symbol in symbols:
        result = backtest_single_etf(symbol, per_etf_capital, start_date, end_date, grid_levels, grid_type)
        etf_results.append(result)
        all_trades.extend(result['trades'])
    
    # 合并网格详情数据
    grid_prices = []
    grid_trade_shares = {}
    
    # 使用第一个ETF的网格详情
    if etf_results and 'grid_prices' in etf_results[0]:
        grid_prices = etf_results[0]['grid_prices']
        # 提取交易股数
        for level in range(grid_levels):
            grid_trade_shares[level] = etf_results[0]['grid_trade_shares'][level] if level < len(etf_results[0]['grid_trade_shares']) else 100
    
    # 合并资金曲线数据
    # 首先找出所有唯一的日期
    all_dates = set()
    for result in etf_results:
        all_dates.update(result['dates'])
    
    all_dates = sorted(list(all_dates))
    
    # 初始化合并后的资金曲线数据
    combined_total_equity = {date: 0 for date in all_dates}
    combined_invested_capital = {date: 0 for date in all_dates}
    combined_profit = {date: 0 for date in all_dates}
    
    # 合并每个ETF的资金曲线数据
    for result in etf_results:
        dates = result['dates']
        total_equity = [item['y'] for item in result['total_equity']]
        invested_capital = [item['y'] for item in result['invested_capital']]
        profit = [item['y'] for item in result['profit_values']]
        
        # 将每个ETF的数据添加到合并数据中
        for i, date in enumerate(dates):
            combined_total_equity[date] += total_equity[i]
            combined_invested_capital[date] += invested_capital[i]
            combined_profit[date] += profit[i]
    
    # 转换为列表格式
    dates_list = list(combined_total_equity.keys())
    total_equity_list = [{'x': date, 'y': combined_total_equity[date]} for date in dates_list]
    invested_capital_list = [{'x': date, 'y': combined_invested_capital[date]} for date in dates_list]
    profit_list = [{'x': date, 'y': combined_profit[date]} for date in dates_list]
    
    # 计算组合回测指标
    # 1. 总收益率
    initial_equity = initial_capital
    final_equity = combined_total_equity[dates_list[-1]] if dates_list else initial_capital
    total_return = (final_equity / initial_equity - 1) * 100
    
    # 2. 年化收益率
    days = (end_date - start_date).days
    annual_return = ((1 + total_return / 100) ** (365 / days) - 1) * 100 if days > 0 else 0
    
    # 3. 最大回撤
    max_drawdown = 0
    peak = initial_equity
    
    for date in dates_list:
        value = combined_total_equity[date]
        if value > peak:
            peak = value
        drawdown = (peak - value) / peak * 100
        max_drawdown = max(max_drawdown, drawdown)
    
    # 4. 夏普比率
    if len(dates_list) > 1:
        # 计算日收益率
        daily_returns = []
        prev_equity = combined_total_equity[dates_list[0]]
        
        for i in range(1, len(dates_list)):
            curr_equity = combined_total_equity[dates_list[i]]
            daily_return = (curr_equity / prev_equity) - 1
            daily_returns.append(daily_return)
            prev_equity = curr_equity
        
        # 计算年化收益率和标准差
        avg_daily_return = np.mean(daily_returns)
        std_daily_return = np.std(daily_returns)
        
        # 年化
        risk_free_rate = 0.03
        sharpe_ratio = (avg_daily_return * 252 - risk_free_rate) / (std_daily_return * np.sqrt(252)) if std_daily_return > 0 else 0
    else:
        sharpe_ratio = 0
    
    # 5. 胜率
    win_trades = sum(1 for result in etf_results for trade in result['trades'] if trade['type'] == '卖出' and float(trade['amount']) > float(trade['price']) * float(trade['quantity']))
    total_trades = sum(1 for result in etf_results for trade in result['trades'] if trade['type'] == '卖出')
    win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
    
    # 按日期排序交易记录
    all_trades.sort(key=lambda x: x['date'])
    
    # 构建组合回测结果
    result = {
        'annual_return': f'{annual_return:.2f}%',
        'sharpe_ratio': f'{sharpe_ratio:.2f}',
        'total_return': f'{total_return:.2f}%',
        'max_drawdown': f'{max_drawdown:.2f}%',
        'win_rate': f'{win_rate:.2f}%',
        'equity_curve': {
            'dates': dates_list,
            'total_equity': total_equity_list,
            'invested_capital': invested_capital_list,
            'profit': profit_list
        },
        'trades': all_trades[:100],  # 限制返回的交易记录数量
        'grid_details': {
            'grid_prices': [round(price, 2) for price in grid_prices],
            'grid_trade_shares': [grid_trade_shares.get(level, 100) for level in range(grid_levels)]
        }
    }
    
    return result

# 获取回测交易记录函数
def get_backtest_trades(symbol, start_date, end_date):
    try:
        logger.info(f"获取 {symbol} 的交易记录，日期范围: {start_date} 到 {end_date}")
        result = backtest_single_etf(symbol, 100000, start_date, end_date, 10, 'volatility')
        if 'error' in result:
            logger.error(f"回测失败: {result['error']}")
            return []
        return result['trades']
    except Exception as e:
        logger.error(f"获取交易记录失败: {str(e)}")
        return []

# 获取组合回测交易记录函数
def get_backtest_trades_portfolio(symbols, start_date, end_date):
    # 这里简化处理，实际应该从数据库或缓存中获取
    result = backtest_portfolio(symbols, 100000, start_date, end_date, 10, 'volatility')
    return result['trades']

# 添加计算网格参数的API
@app.route('/calculate_grid_params', methods=['POST'])
def calculate_grid_params():
    try:
        data = request.json
        symbols = data.get('symbols', [])
        
        if not symbols:
            return jsonify({'error': '未提供ETF代码'}), 400
        
        # 如果是多个ETF，取平均值
        volatility_sum = 0
        grid_spacing_sum = 0
        grid_levels_sum = 0
        grid_range_upper_sum = 0
        grid_range_lower_sum = 0
        
        for symbol in symbols:
            # 计算波动率
            volatility = calculate_volatility(symbol)
            latest_vol = volatility.iloc[-1] if not volatility.empty else 0.2
            volatility_sum += latest_vol
            
            # 计算网格间隔
            grid_spacing = calculate_grid_spacing(symbol)
            latest_spacing = grid_spacing.iloc[-1] if not grid_spacing.empty else latest_vol / 8
            grid_spacing_sum += latest_spacing
            
            # 计算网格总区间
            grid_range = calculate_grid_range(symbol)
            latest_range = grid_range.iloc[-1] if not grid_range.empty else None
            
            if latest_range is not None:
                upper_price = latest_range['H_val']
                lower_price = latest_range['L_val']
                grid_range_upper_sum += upper_price
                grid_range_lower_sum += lower_price
                
                # 计算总区间百分比
                range_pct = 2 * (upper_price - lower_price) / (upper_price + lower_price)
                # 计算网格层数
                grid_levels = round(range_pct / latest_spacing)
                grid_levels_sum += grid_levels
        
        # 计算平均值
        count = len(symbols)
        avg_volatility = volatility_sum / count
        avg_grid_spacing = grid_spacing_sum / count
        avg_grid_levels = max(3, min(50, round(grid_levels_sum / count)))  # 限制在3-50之间
        avg_upper_price = grid_range_upper_sum / count
        avg_lower_price = grid_range_lower_sum / count
        
        return jsonify({
            'volatility': f'{avg_volatility * 100:.2f}%',
            'grid_spacing': f'{avg_grid_spacing * 100:.2f}%',
            'grid_range': f'{avg_lower_price:.2f} - {avg_upper_price:.2f}',
            'grid_levels': avg_grid_levels,
            # 添加原始值以便在回测中使用
            'volatility_raw': avg_volatility,
            'grid_spacing_raw': avg_grid_spacing,
            'grid_range_upper': avg_upper_price,
            'grid_range_lower': avg_lower_price
        })
    except Exception as e:
        print(f"计算网格参数时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'计算失败: {str(e)}'}), 500

# 用户相关路由
@app.route('/login', methods=['GET', 'POST'])
def login():
    # 如果用户已登录，重定向到主页
    if 'user_id' in session:
        return redirect(url_for('index'))
    
    # 如果是POST请求，处理登录表单
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        # 验证用户凭据
        success, user = User.authenticate(username, password)
        
        if success:
            # 获取用户ID，使用辅助函数
            user_id = get_user_id(user)
            
            # 登录用户
            login_user(user_id)
            flash("登录成功！欢迎回来。", "success")
            
            # 重定向到之前请求的页面或首页
            next_page = request.args.get('next')
            if next_page:
                return redirect(next_page)
            return redirect(url_for('index'))
        else:
            flash(f"登录失败: {user}", "error")
    
    # 检查是否有成功或错误消息需要显示
    message = request.args.get('message')
    message_type = request.args.get('type', 'success')
    
    # 渲染登录页面
    return render_template('login.html', message=message, message_type=message_type)

@app.route('/register', methods=['GET', 'POST'])
def register():
    error = None
    
    if request.method == 'POST':
        # 检查CSRF令牌
        if not check_csrf_token(request.form.get('csrf_token')):
            return "CSRF验证失败", 400
        
        # 获取表单数据
        username = request.form.get('username')
        email = request.form.get('email')
        password = request.form.get('password')
        confirm_password = request.form.get('confirm_password')
        agree = request.form.get('agree') == 'on'
        
        # 简单的表单验证
        if not (username and email and password and confirm_password):
            error = "所有字段都是必填的"
        elif password != confirm_password:
            error = "两次输入的密码不一致"
        elif len(password) < 8:
            error = "密码长度必须至少为8个字符"
        elif not agree:
            error = "您必须同意服务条款和隐私政策"
        else:
            # 尝试创建用户
            success, result = User.create(username, email, password)
            if success:
                # 注册成功，自动登录
                login_user(result)
                flash("注册成功", "success")
                return redirect(url_for('index'))
            else:
                error = result  # 错误消息
    
    return render_template('register.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    flash('您已成功退出登录', 'success')
    return redirect(url_for('login'))

@app.route('/settings', methods=['GET'])
@login_required
def settings():
    user = get_current_user()
    
    # 获取用户ID，使用辅助函数
    user_id = get_user_id(user)
    
    settings = UserSetting.get(user_id)
    favorites = FavoriteETF.get_user_favorites(user_id)
    custom_etfs = CustomETF.get_user_custom_etfs(user_id)
    
    # 获取所有ETF供用户选择添加到自选
    all_symbols = SYMBOLS
    
    # 检查是否有成功或错误消息需要显示
    message = request.args.get('message')
    message_type = request.args.get('type', 'success')
    
    return render_template(
        'settings.html', 
        user=user, 
        settings=settings, 
        favorites=favorites, 
        custom_etfs=custom_etfs,
        all_symbols=all_symbols,
        message=message,
        message_type=message_type
    )

@app.route('/update_profile', methods=['POST'])
@login_required
def update_profile():
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    email = request.form.get('email')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 更新用户资料
    cursor.execute('UPDATE users SET email = ? WHERE id = ?', 
                  (email, user_id))
    
    conn.commit()
    conn.close()
    
    flash("个人资料已更新", "success")
    return redirect(url_for('settings', message="个人资料已更新", type="success"))

@app.route('/update_settings', methods=['POST'])
@login_required
def update_settings():
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    theme = request.form.get('theme', 'light')
    default_view = request.form.get('default_view', 'grid')
    notification = request.form.get('notification') == 'on'
    
    # 更新用户设置
    UserSetting.set(user_id, 'theme', theme)
    UserSetting.set(user_id, 'default_view', default_view)
    UserSetting.set(user_id, 'notification', notification)
    
    flash("设置已更新", "success")
    return redirect(url_for('settings', message="设置已更新", type="success"))

@app.route('/add_favorite', methods=['POST'])
@login_required
def add_favorite():
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    symbol = request.form.get('symbol')
    
    # 添加到自选
    success = FavoriteETF.add(user_id, symbol)
    
    if success:
        return redirect(url_for('settings', message="已添加到自选", type="success"))
    else:
        return redirect(url_for('settings', message="添加失败，可能已在自选中", type="error"))

@app.route('/remove_favorite', methods=['POST'])
@login_required
def remove_favorite():
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    symbol = request.form.get('symbol')
    
    # 从自选中移除
    success = FavoriteETF.remove(user_id, symbol)
    
    if success:
        return redirect(url_for('settings', message="已从自选中移除", type="success"))
    else:
        return redirect(url_for('settings', message="移除失败", type="error"))

@app.route('/change_password', methods=['POST'])
@login_required
def change_password():
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    current_password = request.form.get('current_password')
    new_password = request.form.get('new_password')
    confirm_password = request.form.get('confirm_password')
    
    # 验证当前密码
    success, result = User.authenticate(user.username, current_password)
    if not success:
        return redirect(url_for('settings', message="当前密码错误", type="error"))
    
    # 验证新密码
    if new_password != confirm_password:
        return redirect(url_for('settings', message="新密码与确认密码不一致", type="error"))
    
    # 更新密码
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE users SET password_hash = ? WHERE id = ?',
        (User.hash_password(new_password), user_id)
    )
    conn.commit()
    conn.close()
    
    flash("密码已更新", "success")
    return redirect(url_for('settings', message="密码已更新", type="success"))

@app.route('/delete_account', methods=['POST'])
@login_required
def delete_account():
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    password = request.form.get('password')
    
    # 验证密码
    success, result = User.authenticate(user.username, password)
    if not success:
        return redirect(url_for('settings', message="密码错误，账户删除失败", type="error"))
    
    # 删除用户所有相关数据
    conn = get_db_connection()
    try:
        # 开启事务
        conn.execute("BEGIN TRANSACTION")
        
        # 删除用户的收藏ETF
        cursor = conn.cursor()
        cursor.execute('DELETE FROM favorite_etfs WHERE user_id = ?', (user_id,))
        
        # 删除用户的自定义ETF
        cursor.execute('DELETE FROM custom_etfs WHERE user_id = ?', (user_id,))
        
        # 删除用户设置
        cursor.execute('DELETE FROM user_settings WHERE user_id = ?', (user_id,))
        
        # 删除用户的所有投资组合中的ETF
        cursor.execute('''
            DELETE FROM portfolio_etfs 
            WHERE portfolio_id IN (SELECT id FROM portfolios WHERE user_id = ?)
        ''', (user_id,))
        
        # 删除用户的所有投资组合
        cursor.execute('DELETE FROM portfolios WHERE user_id = ?', (user_id,))
        
        # 最后删除用户本身
        cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
        
        # 提交事务
        conn.commit()
        
        # 退出登录
        logout_user()
        
        flash("您的账户已成功注销", "success")
        return redirect(url_for('index'))
    except Exception as e:
        # 发生错误时回滚事务
        conn.execute('ROLLBACK')
        logger.error(f"删除账户失败: {str(e)}")
        return redirect(url_for('settings', message="删除账户失败，请稍后再试", type="error"))
    finally:
        conn.close()

@app.route('/my_portfolios', methods=['GET'])
@login_required
def my_portfolios():
    user = get_current_user()
    
    # 获取用户ID，使用辅助函数
    user_id = get_user_id(user)
    
    portfolios = Portfolio.get_user_portfolios(user_id)
    
    # 检查是否有成功或错误消息需要显示
    message = request.args.get('message')
    message_type = request.args.get('type', 'success')
    
    return render_template(
        'my_portfolios.html', 
        portfolios=portfolios,
        message=message,
        message_type=message_type
    )

@app.route('/create_portfolio', methods=['POST'])
@login_required
def create_portfolio():
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    name = request.form.get('name')
    description = request.form.get('description', '')
    total_amount = float(request.form.get('total_amount', 0))
    
    # 创建投资组合
    portfolio_id = Portfolio.create(user_id, name, description, total_amount)
    
    if portfolio_id:
        return redirect(url_for('edit_portfolio', portfolio_id=portfolio_id, message="投资组合已创建", type="success"))
    else:
        return redirect(url_for('my_portfolios', message="创建投资组合失败", type="error"))

@app.route('/edit_portfolio/<int:portfolio_id>', methods=['GET'])
@login_required
def edit_portfolio(portfolio_id):
    user = get_current_user()
    user_id = get_user_id(user)
    
    # 获取投资组合详情
    portfolio = Portfolio.get_by_id(portfolio_id, user_id)
    if not portfolio:
        flash("投资组合不存在或您没有权限访问", "error")
        return redirect(url_for('my_portfolios'))
    
    # 获取所有官方ETF列表
    all_symbols = SYMBOLS
    
    # 用于检查ETF代码是否已存在
    existing_codes = set(symbol['code'] for symbol in all_symbols)
    
    # 获取用户自定义ETF列表
    custom_etfs = CustomETF.get_user_custom_etfs(user_id)
    
    # 将自定义ETF转换为与官方ETF相同的格式并合并，避免重复
    for etf in custom_etfs:
        # 检查ETF代码是否已存在于列表中
        if etf['symbol'] not in existing_codes:
            all_symbols.append({
                'code': etf['symbol'],
                'name': etf['display_name'] if 'display_name' in etf and etf['display_name'] else etf['name']
            })
            existing_codes.add(etf['symbol'])
    
    # 检查是否有成功或错误消息需要显示
    message = request.args.get('message')
    message_type = request.args.get('type', 'success')
    
    # 获取ETF数据已经包含在portfolio['etfs']中
    etfs = portfolio['etfs']
    
    return render_template(
        'edit_portfolio.html', 
        portfolio=portfolio,
        etfs=etfs,
        all_symbols=all_symbols,
        message=message,
        message_type=message_type
    )

@app.route('/update_portfolio/<int:portfolio_id>', methods=['POST'])
@login_required
def update_portfolio(portfolio_id):
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    # 获取投资组合
    portfolio = Portfolio.get_by_id(portfolio_id, user_id)
    if not portfolio:
        flash("投资组合不存在或您没有权限访问", "error")
        return redirect(url_for('my_portfolios'))
    
    # 更新投资组合信息
    name = request.form.get('name')
    description = request.form.get('description', '')
    total_amount = float(request.form.get('total_amount', 0))
    
    success = Portfolio.update(portfolio_id, name, description, total_amount)
    
    if success:
        flash("投资组合已更新", "success")
        return redirect(url_for('edit_portfolio', portfolio_id=portfolio_id, message="投资组合已更新", type="success"))
    else:
        flash("更新投资组合失败", "error")
        return redirect(url_for('edit_portfolio', portfolio_id=portfolio_id, message="更新失败", type="error"))

@app.route('/add_etf_to_portfolio/<int:portfolio_id>', methods=['POST'])
@login_required
def add_etf_to_portfolio(portfolio_id):
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    portfolio = Portfolio.get_by_id(portfolio_id, user_id)
    
    if not portfolio:
        flash("找不到指定的投资组合或您没有权限访问", "error")
        return redirect(url_for('my_portfolios'))
    
    symbol = request.form.get('symbol')
    weight = float(request.form.get('weight', 0))
    
    # 验证ETF是否存在（官方或用户自定义）
    is_official = False
    for etf in SYMBOLS:
        if etf['code'] == symbol:
            is_official = True
            break
            
    if not is_official:
        # 检查是否为用户的自定义ETF
        custom_etf = CustomETF.get_custom_etf(user_id, symbol)
        if not custom_etf:
            flash(f"添加失败：ETF代码 {symbol} 不存在或不属于您", "error")
            return redirect(url_for('edit_portfolio', portfolio_id=portfolio_id))
    
    # 验证：计算添加此ETF后的总权重
    total_weight = sum(etf['weight'] for etf in portfolio['etfs'])
    
    # 检查是否已存在此ETF，如果存在需要减去其当前权重
    existing_etf = next((etf for etf in portfolio['etfs'] if etf['symbol'] == symbol), None)
    if existing_etf:
        total_weight -= existing_etf['weight']
    
    new_total_weight = total_weight + weight
    
    # 验证总权重不超过100%
    if new_total_weight > 100:
        flash(f"添加失败：添加此ETF后总权重将达到{new_total_weight:.1f}%，超过100%。请先调整其他ETF的权重。", "error")
        return redirect(url_for('edit_portfolio', portfolio_id=portfolio_id))
    
    # 添加ETF到投资组合
    success = Portfolio.add_etf(portfolio_id, symbol, weight)
    
    if success:
        flash("ETF已成功添加到投资组合", "success")
    else:
        flash("添加ETF失败，请稍后重试", "error")
        
    return redirect(url_for('edit_portfolio', portfolio_id=portfolio_id))

@app.route('/remove_etf_from_portfolio/<int:portfolio_id>', methods=['POST'])
@login_required
def remove_etf_from_portfolio(portfolio_id):
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    portfolio = Portfolio.get_by_id(portfolio_id, user_id)
    
    if not portfolio:
        flash("找不到指定的投资组合或您没有权限访问", "error")
        return redirect(url_for('my_portfolios'))
    
    symbol = request.form.get('symbol')
    
    # 从投资组合中移除ETF
    success = Portfolio.remove_etf(portfolio_id, symbol)
    
    flash("ETF已成功从投资组合中移除", "success")
    return redirect(url_for('edit_portfolio', portfolio_id=portfolio_id))

@app.route('/update_etf_weight/<int:portfolio_id>', methods=['POST'])
@login_required
def update_etf_weight(portfolio_id):
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    portfolio = Portfolio.get_by_id(portfolio_id, user_id)
    
    if not portfolio:
        flash("找不到指定的投资组合或您没有权限访问", "error")
        return redirect(url_for('my_portfolios'))
    
    symbol = request.form.get('symbol')
    weight = float(request.form.get('weight', 0))
    
    # 验证：计算更新此ETF后的总权重
    total_weight = sum(etf['weight'] for etf in portfolio['etfs'])
    
    # 减去当前ETF的权重
    existing_etf = next((etf for etf in portfolio['etfs'] if etf['symbol'] == symbol), None)
    if existing_etf:
        total_weight -= existing_etf['weight']
    
    new_total_weight = total_weight + weight
    
    # 验证总权重不超过100%
    if new_total_weight > 100:
        flash(f"更新失败：更新此ETF后总权重将达到{new_total_weight:.1f}%，超过100%。请先调整其他ETF的权重。", "error")
        return redirect(url_for('edit_portfolio', portfolio_id=portfolio_id))
    
    # 更新ETF权重
    success = Portfolio.add_etf(portfolio_id, symbol, weight)  # 使用add_etf方法，会自动更新权重
    
    flash("ETF权重已成功更新", "success")
    return redirect(url_for('edit_portfolio', portfolio_id=portfolio_id))

@app.route('/set_default_portfolio', methods=['POST'])
@login_required
def set_default_portfolio():
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    portfolio_id = request.form.get('portfolio_id')
    
    # 设置默认投资组合
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 先清除当前默认
    cursor.execute(
        'UPDATE portfolios SET is_default = 0 WHERE user_id = ?',
        (user_id,)
    )
    
    # 设置新的默认组合
    cursor.execute(
        'UPDATE portfolios SET is_default = 1 WHERE id = ? AND user_id = ?',
        (portfolio_id, user_id)
    )
    
    conn.commit()
    conn.close()
    
    flash("默认投资组合已设置", "success")
    return redirect(url_for('my_portfolios', message="默认投资组合已设置", type="success"))

@app.route('/delete_portfolio', methods=['POST'])
@login_required
def delete_portfolio():
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        flash("无效的请求", "error")
        return redirect(url_for('my_portfolios'))
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    portfolio_id = request.form.get('portfolio_id')
    
    # 删除投资组合
    conn = get_db_connection()
    try:
        # 开启事务
        conn.execute("BEGIN TRANSACTION")
        
        # 删除投资组合中的ETF
        cursor = conn.cursor()
        cursor.execute('DELETE FROM portfolio_etfs WHERE portfolio_id = ?', (portfolio_id,))
        
        # 删除投资组合本身
        cursor.execute('DELETE FROM portfolios WHERE id = ? AND user_id = ?', (portfolio_id, user_id))
        
        # 提交事务
        conn.commit()
        
        flash("投资组合已删除", "success")
        return redirect(url_for('my_portfolios', message="投资组合已删除", type="success"))
    except Exception as e:
        # 发生错误时回滚事务
        conn.execute('ROLLBACK')
        logger.error(f"删除投资组合失败: {str(e)}")
        
        flash("删除投资组合失败", "error")
        return redirect(url_for('my_portfolios', message="删除失败，请稍后再试", type="error"))
    finally:
        conn.close()

# 管理员权限验证装饰器
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = get_current_user()
        
        # 调试信息
        if user:
            print(f"当前用户: ID={getattr(user, 'id', None)}, 用户名={getattr(user, 'username', None)}")
            print(f"is_admin 属性值: {getattr(user, 'is_admin', None)}")
        else:
            print("未获取到当前用户")
        
        # 检查用户是否存在且是否为管理员
        is_admin = False
        if user:
            # 先尝试作为属性访问
            if hasattr(user, 'is_admin'):
                is_admin = bool(user.is_admin)
            # 如果是字典类型
            elif isinstance(user, dict) and 'is_admin' in user:
                is_admin = bool(user['is_admin'])
            # 如果是sqlite Row对象
            elif hasattr(user, 'keys') and 'is_admin' in user.keys():
                is_admin = bool(user['is_admin'])
        
        if not user or not is_admin:
            flash('您没有管理员权限！', 'error')
            return redirect(url_for('login'))
            
        return f(*args, **kwargs)
    return decorated_function

# 管理员控制台路由
@app.route('/admin')
@login_required
@admin_required
def admin_console():
    # 获取查询参数
    search = request.args.get('search', '')
    page = int(request.args.get('page', 1))
    per_page = 20  # 每页显示的用户数
    
    # 连接数据库
    conn = sqlite3.connect('database/etf_history.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 计算总用户数和总页数
    if search:
        count_query = "SELECT COUNT(*) FROM users WHERE username LIKE ? OR email LIKE ?"
        cursor.execute(count_query, (f'%{search}%', f'%{search}%'))
    else:
        count_query = "SELECT COUNT(*) FROM users"
        cursor.execute(count_query)
    
    total_users = cursor.fetchone()[0]
    total_pages = (total_users + per_page - 1) // per_page
    
    # 限制页码范围
    page = max(1, min(page, total_pages)) if total_pages > 0 else 1
    
    # 计算偏移量
    offset = (page - 1) * per_page
    
    # 查询用户数据，包括每个用户的投资组合数量
    if search:
        user_query = """
            SELECT u.*, COUNT(p.id) as portfolio_count
            FROM users u
            LEFT JOIN portfolios p ON u.id = p.user_id
            WHERE u.username LIKE ? OR u.email LIKE ?
            GROUP BY u.id
            ORDER BY u.id DESC
            LIMIT ? OFFSET ?
        """
        cursor.execute(user_query, (f'%{search}%', f'%{search}%', per_page, offset))
    else:
        user_query = """
            SELECT u.*, COUNT(p.id) as portfolio_count
            FROM users u
            LEFT JOIN portfolios p ON u.id = p.user_id
            GROUP BY u.id
            ORDER BY u.id DESC
            LIMIT ? OFFSET ?
        """
        cursor.execute(user_query, (per_page, offset))
    
    # 获取用户数据并转换为可修改的字典列表
    users = [dict(user) for user in cursor.fetchall()]
    
    # 处理created_at字段
    for user in users:
        if 'created_at' in user and user['created_at']:
            try:
                user['created_at'] = datetime.fromisoformat(user['created_at'].replace('Z', '+00:00'))
            except:
                # 如果转换失败，使用当前时间
                user['created_at'] = datetime.now()
        else:
            user['created_at'] = datetime.now()
    
    conn.close()
    
    return render_template('admin.html', 
                          users=users, 
                          total_users=total_users,
                          page=page,
                          total_pages=total_pages,
                          search=search)

# 管理员查看用户投资组合
@app.route('/admin/users/<int:user_id>/portfolios')
@login_required
@admin_required
def admin_user_portfolios(user_id):
    # 连接数据库
    conn = get_db_connection()
    
    try:
        # 查询用户信息
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        user = cursor.fetchone()
        
        if not user:
            flash('用户不存在', 'error')
            return redirect(url_for('admin_console'))
        
        # 查询用户的投资组合
        cursor.execute('''
        SELECT p.*, 
               (SELECT COUNT(*) FROM portfolio_etfs WHERE portfolio_id = p.id) as etf_count 
        FROM portfolios p 
        WHERE p.user_id = ? 
        ORDER BY p.created_at DESC
        ''', (user_id,))
        portfolios = cursor.fetchall()
        
        # 转换为字典列表
        portfolio_list = []
        for portfolio in portfolios:
            portfolio_dict = dict(portfolio)
            portfolio_list.append(portfolio_dict)
        
        return render_template('admin_user_portfolios.html', user=user, portfolios=portfolio_list)
    except Exception as e:
        logger.error(f"获取用户投资组合失败: {e}")
        flash(f"获取用户投资组合失败: {e}", "error")
        return redirect(url_for('admin_console'))
    finally:
        conn.close()

# 管理员查看投资组合详情
@app.route('/admin/portfolios/<int:portfolio_id>')
@login_required
@admin_required
def admin_view_portfolio(portfolio_id):
    """管理员查看特定投资组合详情"""
    logger.info(f"访问投资组合详情，ID: {portfolio_id}")
    conn = get_db_connection()
    
    try:
        # 查询投资组合信息
        cursor = conn.cursor()
        cursor.execute('''
        SELECT p.*, u.username, u.id as user_id
        FROM portfolios p
        JOIN users u ON p.user_id = u.id
        WHERE p.id = ?
        ''', (portfolio_id,))
        portfolio = cursor.fetchone()
        
        if not portfolio:
            logger.warning(f"投资组合不存在，ID: {portfolio_id}")
            flash('投资组合不存在', 'error')
            return redirect(url_for('admin_console'))
        
        logger.info(f"找到投资组合: {portfolio['name']}, 用户ID: {portfolio['user_id']}")
        
        # 查询投资组合中的ETF
        cursor.execute('''
        SELECT pe.*, e.symbol, e.name, e.category, e.is_official, e.volatility_type
        FROM portfolio_etfs pe
        JOIN etf_list e ON pe.symbol = e.symbol
        WHERE pe.portfolio_id = ?
        ORDER BY pe.weight DESC
        ''', (portfolio_id,))
        portfolio_etfs = cursor.fetchall()
        
        # 转换为字典列表
        etf_list = []
        symbols_list = []
        for etf in portfolio_etfs:
            etf_dict = dict(etf)
            etf_list.append(etf_dict)
            symbols_list.append(etf_dict['symbol'])
        
        logger.info(f"找到 {len(etf_list)} 个ETF在投资组合中")
        
        # 转换投资组合对象为字典
        portfolio_dict = dict(portfolio)
        
        # 添加etfs属性到portfolio对象
        portfolio_dict['etfs'] = etf_list
        
        # 检查是否有ETFs
        has_etfs = len(etf_list) > 0
        
        # 创建symbols_json
        import json
        symbols_json = json.dumps(symbols_list)
        
        logger.info(f"准备渲染模板，传递参数: portfolio={portfolio_dict['name']}, is_admin=True, is_custom_portfolio=True, has_etfs={has_etfs}")
        return render_template('portfolio.html', portfolio=portfolio_dict, etfs=etf_list, is_admin=True, is_custom_portfolio=True, has_etfs=has_etfs, symbols_json=symbols_json)
    except Exception as e:
        logger.error(f"获取投资组合详情失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        flash(f"获取投资组合详情失败: {e}", "error")
        
        # 如果portfolio对象不存在，返回admin_console，否则返回该用户的投资组合列表
        if 'portfolio' in locals() and portfolio and 'user_id' in portfolio:
            logger.info(f"重定向到用户投资组合列表，用户ID: {portfolio['user_id']}")
            return redirect(url_for('admin_user_portfolios', user_id=portfolio['user_id']))
        else:
            logger.info("重定向到管理控制台")
            return redirect(url_for('admin_console'))
    finally:
        conn.close()

# API端点：获取用户列表
@app.route('/api/admin/users')
@login_required
@admin_required
def api_get_users():
    # 连接数据库
    conn = sqlite3.connect('database/etf_history.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 查询用户列表
    cursor.execute("""
        SELECT u.*, COUNT(p.id) as portfolio_count
        FROM users u
        LEFT JOIN portfolios p ON u.id = p.user_id
        GROUP BY u.id
        ORDER BY u.id DESC
    """)
    
    users = [dict(user) for user in cursor.fetchall()]
    conn.close()
    
    return jsonify(users)

# API端点：获取单个用户信息
@app.route('/api/admin/users/<int:user_id>')
@login_required
@admin_required
def api_get_user(user_id):
    # 连接数据库
    conn = sqlite3.connect('database/etf_history.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 查询用户信息
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = cursor.fetchone()
    
    if not user:
        conn.close()
        return jsonify({"error": "用户不存在"}), 404
    
    # 转换为字典并移除密码
    user_dict = dict(user)
    if 'password' in user_dict:
        del user_dict['password']
    
    conn.close()
    
    return jsonify(user_dict)

# API端点：创建新用户
@app.route('/api/admin/users', methods=['POST'])
@login_required
@admin_required
@check_csrf_token
def api_create_user():
    data = request.get_json()
    
    # 验证必要字段
    required_fields = ['username', 'email', 'password']
    for field in required_fields:
        if field not in data or not data[field]:
            return jsonify({"error": f"缺少必要字段: {field}"}), 400
    
    username = data['username']
    email = data['email']
    password = data['password']
    is_admin = data.get('is_admin', False)
    
    # 连接数据库
    conn = sqlite3.connect('database/etf_history.db')
    cursor = conn.cursor()
    
    # 检查用户名或邮箱是否已存在
    cursor.execute("SELECT * FROM users WHERE username = ? OR email = ?", (username, email))
    existing_user = cursor.fetchone()
    
    if existing_user:
        conn.close()
        return jsonify({"error": "用户名或邮箱已存在"}), 400
    
    # 密码加密
    hashed_password = generate_password_hash(password)
    
    # 插入新用户
    try:
        cursor.execute(
            "INSERT INTO users (username, email, password, is_admin, created_at) VALUES (?, ?, ?, ?, ?)",
            (username, email, hashed_password, 1 if is_admin else 0, datetime.now().isoformat())
        )
        conn.commit()
        user_id = cursor.lastrowid
        
        conn.close()
        return jsonify({"message": "用户创建成功", "id": user_id}), 201
    except Exception as e:
        conn.close()
        return jsonify({"error": f"创建用户失败: {str(e)}"}), 500

# API端点：更新用户信息
@app.route('/api/admin/users/<int:user_id>', methods=['PUT'])
@login_required
@admin_required
@check_csrf_token
def api_update_user(user_id):
    data = request.get_json()
    
    # 验证必要字段
    if 'username' not in data or not data['username'] or 'email' not in data or not data['email']:
        return jsonify({"error": "缺少必要字段: username 或 email"}), 400
    
    username = data['username']
    email = data['email']
    is_admin = data.get('is_admin', False)
    password = data.get('password')
    
    # 连接数据库
    conn = sqlite3.connect('database/etf_history.db')
    cursor = conn.cursor()
    
    # 检查用户是否存在
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = cursor.fetchone()
    
    if not user:
        conn.close()
        return jsonify({"error": "用户不存在"}), 404
    
    # 检查用户名或邮箱是否与其他用户冲突
    cursor.execute("SELECT * FROM users WHERE (username = ? OR email = ?) AND id != ?", 
                 (username, email, user_id))
    existing_user = cursor.fetchone()
    
    if existing_user:
        conn.close()
        return jsonify({"error": "用户名或邮箱已被其他用户使用"}), 400
    
    # 更新用户信息
    try:
        if password:
            # 如果提供了新密码，则更新密码
            hashed_password = generate_password_hash(password)
            cursor.execute(
                "UPDATE users SET username = ?, email = ?, password = ?, is_admin = ? WHERE id = ?",
                (username, email, hashed_password, 1 if is_admin else 0, user_id)
            )
        else:
            # 否则只更新其他信息
            cursor.execute(
                "UPDATE users SET username = ?, email = ?, is_admin = ? WHERE id = ?",
                (username, email, 1 if is_admin else 0, user_id)
            )
        
        conn.commit()
        conn.close()
        return jsonify({"message": "用户信息更新成功"}), 200
    except Exception as e:
        conn.close()
        return jsonify({"error": f"更新用户失败: {str(e)}"}), 500

# API端点：删除用户
@app.route('/api/admin/users/<int:user_id>', methods=['DELETE'])
@login_required
@admin_required
@check_csrf_token
def api_delete_user(user_id):
    # 连接数据库
    conn = sqlite3.connect('database/etf_history.db')
    cursor = conn.cursor()
    
    # 检查用户是否存在
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = cursor.fetchone()
    
    if not user:
        conn.close()
        return jsonify({"error": "用户不存在"}), 404
    
    # 删除用户相关数据
    try:
        # 开启事务
        conn.execute("BEGIN TRANSACTION")
        
        # 删除用户的投资组合中的ETF
        cursor.execute("""
            DELETE FROM portfolio_etfs
            WHERE portfolio_id IN (SELECT id FROM portfolios WHERE user_id = ?)
        """, (user_id,))
        
        # 删除用户的投资组合
        cursor.execute("DELETE FROM portfolios WHERE user_id = ?", (user_id,))
        
        # 删除用户的收藏ETF
        cursor.execute("DELETE FROM favorite_etfs WHERE user_id = ?", (user_id,))
        
        # 删除用户的设置
        cursor.execute("DELETE FROM user_settings WHERE user_id = ?", (user_id,))
        
        # 最后删除用户本身
        cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
        
        # 提交事务
        conn.commit()
        conn.close()
        
        return jsonify({"message": "用户及相关数据已成功删除"}), 200
    except Exception as e:
        # 发生错误时回滚事务
        conn.rollback()
        conn.close()
        return jsonify({"error": f"删除用户失败: {str(e)}"}), 500

# API端点：管理员删除投资组合
@app.route('/api/admin/portfolios/<int:portfolio_id>', methods=['DELETE'])
@login_required
@admin_required
@check_csrf_token
def api_delete_portfolio(portfolio_id):
    # 连接数据库
    conn = sqlite3.connect('database/etf_history.db')
    cursor = conn.cursor()
    
    # 检查投资组合是否存在
    cursor.execute("SELECT * FROM portfolios WHERE id = ?", (portfolio_id,))
    portfolio = cursor.fetchone()
    
    if not portfolio:
        conn.close()
        return jsonify({"error": "投资组合不存在"}), 404
    
    # 删除投资组合
    try:
        # 开启事务
        conn.execute("BEGIN TRANSACTION")
        
        # 先删除投资组合中的ETF
        cursor.execute("DELETE FROM portfolio_etfs WHERE portfolio_id = ?", (portfolio_id,))
        
        # 再删除投资组合本身
        cursor.execute("DELETE FROM portfolios WHERE id = ?", (portfolio_id,))
        
        # 提交事务
        conn.commit()
        conn.close()
        
        return jsonify({"message": "投资组合已成功删除"}), 200
    except Exception as e:
        # 发生错误时回滚事务
        conn.rollback()
        conn.close()
        return jsonify({"error": f"删除投资组合失败: {str(e)}"}), 500

# ETF管理页面
@app.route('/admin/etfs')
@login_required
@admin_required
def admin_etfs():
    # 获取查询参数
    search = request.args.get('search', '')
    tab = request.args.get('tab', 'official')  # 默认显示官方ETF
    
    # 获取所有ETF
    etfs = get_all_etfs()
    
    # 获取所有用户自定义ETF
    custom_etfs = CustomETF.get_all_custom_etfs()
    
    # 如果有搜索条件，过滤ETF列表
    if search:
        etfs = [dict(etf) for etf in etfs if search.lower() in etf['symbol'].lower() or 
                (etf['name'] and search.lower() in etf['name'].lower())]
        custom_etfs = [etf for etf in custom_etfs if search.lower() in etf['symbol'].lower() or 
                       (etf['name'] and search.lower() in etf['name'].lower())]
    else:
        # 将所有ETF转换为字典列表
        etfs = [dict(etf) for etf in etfs]
    
    # 确保只在官方ETF列表中显示官方ETF（is_official=1）
    etfs = [etf for etf in etfs if etf['is_official'] == 1]
    
    # 为每个ETF添加数据统计信息
    for etf in etfs:
        etf['data_count'] = get_etf_data_count(etf['symbol'])
        if etf['data_count'] > 0:
            start_date, end_date = get_etf_date_range(etf['symbol'])
            etf['start_date'] = start_date
            etf['end_date'] = end_date
    
    # 为自定义ETF添加数据统计信息
    for etf in custom_etfs:
        etf['data_count'] = get_etf_data_count(etf['symbol'])
        if etf['data_count'] > 0:
            start_date, end_date = get_etf_date_range(etf['symbol'])
            etf['start_date'] = start_date
            etf['end_date'] = end_date
    
    return render_template('admin_etf.html', etfs=etfs, custom_etfs=custom_etfs, tab=tab)

# ETF管理API端点
@app.route('/api/admin/etfs')
@login_required
@admin_required
def api_get_etfs():
    etfs = get_all_etfs()
    # 将ETF转换为字典列表
    etfs = [dict(etf) for etf in etfs]
    return jsonify(etfs)

@app.route('/api/admin/etfs/<symbol>')
@login_required
@admin_required
def api_get_etf(symbol):
    etf = get_etf_by_symbol(symbol)
    if not etf:
        return jsonify({'error': 'ETF不存在'}), 404
    return jsonify(etf)

@app.route('/api/admin/etfs', methods=['POST'])
@login_required
@admin_required
@check_csrf_token
def api_add_etf():
    data = request.json
    
    # 验证必要的字段
    required_fields = ['symbol', 'name']
    for field in required_fields:
        if field not in data or not data[field]:
            return jsonify({'error': f'缺少必要字段: {field}'}), 400
    
    # 添加ETF
    success, message = add_etf(
        symbol=data['symbol'],
        name=data['name'],
        description=data.get('description', ''),
        is_official=data.get('is_official', 0),
        category=data.get('category', ''),
        correlation=data.get('correlation', ''),
        volatility_type=data.get('volatility_type', ''),
        weight=float(data.get('weight', 1.0))
    )
    
    if success:
        return jsonify({'message': message}), 201
    else:
        return jsonify({'error': message}), 400

@app.route('/api/admin/etfs/<path:symbol>', methods=['PUT', 'POST'])
@login_required
@admin_required
@check_csrf_token
def api_update_etf(symbol):
    # 检查是否是PUT请求或带有_method=PUT参数的POST请求
    is_put_request = (request.method == 'PUT' or 
                     (request.method == 'POST' and 
                     (request.args.get('_method') == 'PUT' or request.form.get('_method') == 'PUT')))
    
    if not is_put_request:
        return jsonify({'error': f'不支持的请求方法: {request.method}'}), 405
    
    # 验证ETF是否存在
    etf = get_etf_by_symbol(symbol)
    if not etf:
        return jsonify({'error': 'ETF不存在'}), 404
    
    # 获取数据（支持JSON和表单数据）
    if request.is_json:
        data = request.json
    else:
        data = request.form
    
    # 获取新的symbol值（如果有变化）
    new_symbol = data.get('symbol')
    
    # 更新ETF
    success, message = update_etf(
        symbol=symbol,
        name=data.get('name', etf['name']),
        description=data.get('description', etf['description']),
        is_official=data.get('is_official', etf['is_official']),
        category=data.get('category', etf['category']),
        correlation=data.get('correlation', etf['correlation']),
        volatility_type=data.get('volatility_type', etf['volatility_type']),
        weight=float(data.get('weight', etf['weight'])),
        new_symbol=new_symbol if new_symbol and new_symbol != symbol else None
    )
    
    if success:
        # 如果是表单提交，重定向到ETF管理页面
        if request.method == 'POST':
            flash(message, "success")
            return redirect(url_for('admin_etfs', tab='official'))
        # 如果是API请求，返回JSON响应
        return jsonify({'message': message})
    else:
        if request.method == 'POST':
            flash(message, "error")
            return redirect(url_for('admin_etfs', tab='official'))
        return jsonify({'error': message}), 400

@app.route('/api/admin/etfs/<path:symbol>', methods=['DELETE', 'POST'])
@login_required
@admin_required
@check_csrf_token
def api_delete_etf(symbol):
    """删除官方ETF"""
    # 检查是否是DELETE请求或带有_method=DELETE参数的POST请求
    is_delete_request = (request.method == 'DELETE' or 
                        (request.method == 'POST' and 
                        (request.args.get('_method') == 'DELETE' or request.form.get('_method') == 'DELETE')))
    
    if not is_delete_request:
        return jsonify({'error': f'不支持的请求方法: {request.method}'}), 405
    
    # 验证ETF是否存在
    etf = get_etf_by_symbol(symbol)
    if not etf:
        return jsonify({'error': 'ETF不存在'}), 404
    
    # 删除ETF
    success, message, prompt, data_count = delete_etf(symbol)
    
    if success:
        # 如果是表单提交，重定向到ETF管理页面
        if request.method == 'POST':
            flash(message, "success")
            return redirect(url_for('admin_etfs', tab='official'))
        # 如果是API请求，返回JSON响应
        return jsonify({'message': message})
    else:
        if request.method == 'POST':
            flash(message, "error")
            return redirect(url_for('admin_etfs', tab='official'))
        return jsonify({'error': message}), 400

@app.route('/api/admin/etfs/<path:symbol>/data', methods=['DELETE', 'POST'])
@login_required
@admin_required
@check_csrf_token
def api_clear_etf_data(symbol):
    # 检查是否是DELETE请求或带有_method=DELETE参数的POST请求
    is_delete_request = (request.method == 'DELETE' or 
                        (request.method == 'POST' and 
                        (request.args.get('_method') == 'DELETE' or request.form.get('_method') == 'DELETE')))
    
    if not is_delete_request:
        return jsonify({'error': f'不支持的请求方法: {request.method}'}), 405
    
    # 验证ETF是否存在
    etf = get_etf_by_symbol(symbol)
    if not etf:
        return jsonify({'error': 'ETF不存在'}), 404
    
    # 清除ETF数据
    success, message = clear_etf_data(symbol)
    
    if success:
        # 如果是表单提交，重定向到ETF管理页面
        if request.method == 'POST':
            flash(message, "success")
            return redirect(url_for('admin_etfs', tab='official'))
        # 如果是API请求，返回JSON响应
        return jsonify({'message': message})
    else:
        if request.method == 'POST':
            flash(message, "error")
            return redirect(url_for('admin_etfs', tab='official'))
        return jsonify({'error': message}), 400

@app.route('/api/admin/etfs/data', methods=['DELETE', 'POST'])
@login_required
@admin_required
@check_csrf_token
def api_clear_all_etf_data():
    # 检查是否是DELETE请求或带有_method=DELETE参数的POST请求
    is_delete_request = (request.method == 'DELETE' or 
                        (request.method == 'POST' and 
                        (request.args.get('_method') == 'DELETE' or request.form.get('_method') == 'DELETE')))
    
    if not is_delete_request:
        return jsonify({'error': f'不支持的请求方法: {request.method}'}), 405
    
    # 清除所有ETF数据
    success, message = clear_etf_data()
    
    if success:
        # 如果是表单提交，重定向到ETF管理页面
        if request.method == 'POST':
            flash(message, "success")
            return redirect(url_for('admin_etfs', tab='official'))
        # 如果是API请求，返回JSON响应
        return jsonify({'message': message})
    else:
        if request.method == 'POST':
            flash(message, "error")
            return redirect(url_for('admin_etfs', tab='official'))
        return jsonify({'error': message}), 400

# 用于从东方财富网获取ETF名称的辅助函数
def get_etf_name_from_eastmoney(symbol):
    """从东方财富网获取ETF名称"""
    try:
        logger.info(f"尝试从东方财富获取ETF {symbol} 信息...")
        etf_data = ak.fund_etf_spot_em()
        
        # 查找对应ETF
        etf_info = etf_data[etf_data['代码'] == symbol]
        
        if not etf_info.empty:
            name = etf_info.iloc[0]['名称']
            logger.info(f"从东方财富获取到ETF名称: {name}")
            return name
        else:
            logger.warning(f"在东方财富数据中未找到ETF {symbol}")
            return f"{symbol} ETF"
                
    except Exception as e:
        logger.error(f"从东方财富获取ETF名称失败: {str(e)}", exc_info=True)
        return f"{symbol} ETF"

@app.route('/api/etf_name/<path:symbol>', methods=['GET'])
def api_etf_name(symbol):
    if not symbol:
        return jsonify({'error': 'ETF代码不能为空'}), 400
    
    try:
        # 直接从东方财富网获取ETF名称
        name = get_etf_name_from_eastmoney(symbol)
        
        return jsonify({
            'symbol': symbol,
            'name': name,
            'success': True
        })
        
    except Exception as e:
        logger.error(f"获取ETF名称出错: {str(e)}")
        return jsonify({
            'symbol': symbol,
            'name': f"{symbol} ETF", 
            'error': f"获取ETF名称失败: {str(e)}",
            'success': False
        })

@app.route('/add_custom_etf', methods=['POST'])
@login_required
def add_custom_etf():
    # 检查CSRF令牌
    if not check_csrf_token(request.form.get('csrf_token')):
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    symbol = request.form.get('symbol')
    
    if not symbol:
        return redirect(url_for('user_etf_data', message="ETF代码不能为空", type="error"))
    
    # 检查是否与官方ETF重复
    official_etfs = get_official_etf_list()
    for etf in official_etfs:
        if etf['code'] == symbol:
            etf_name = etf['name']
            return redirect(url_for('user_etf_data', message=f"您添加的{etf_name}({symbol})已在官方ETF列表中，无需添加为自定义ETF", type="error"))
    
    # 验证是否为有效的ETF/LOF代码
    try:
        # 尝试获取ETF数据以验证代码有效性
        df, _ = get_etf_data(symbol)
        if df.empty:
            return redirect(url_for('user_etf_data', message=f"无法获取{symbol}的数据，请确认是有效的ETF或LOF代码", type="error"))
        
        # 从东方财富网获取ETF名称
        name = get_etf_name_from_eastmoney(symbol)
        
        # 添加到自定义ETF列表
        success = CustomETF.add(user_id, symbol, name)
        
        if success:
            return redirect(url_for('user_etf_data', message=f"ETF {name} ({symbol}) 已成功添加!", type="success"))
        else:
            return redirect(url_for('user_etf_data', message=f"ETF {symbol} 已存在于您的自定义ETF列表中", type="info"))
    except Exception as e:
        logger.error(f"添加自定义ETF失败: {str(e)}")
        return redirect(url_for('user_etf_data', message=f"添加ETF失败: {str(e)}", type="error"))

# 用于从数据库或新浪接口获取ETF名称的辅助函数
def get_etf_name_from_db_or_sina(symbol):
    # 直接使用东方财富网获取
    return get_etf_name_from_eastmoney(symbol)

@app.route('/remove_custom_etf', methods=['POST'])
@login_required
def remove_custom_etf():
    # 检查CSRF令牌
    csrf_token = request.form.get('csrf_token')
    if not check_csrf_token(csrf_token):
        logger.error(f"CSRF验证失败: {csrf_token}")
        return "CSRF验证失败", 400
    
    user = get_current_user()
    user_id = get_user_id(user)
    
    symbol = request.form.get('symbol')
    logger.info(f"尝试删除自定义ETF: 用户ID={user_id}, symbol={symbol}")
    
    if not symbol:
        logger.error("删除自定义ETF失败: 未提供ETF代码")
        return redirect(url_for('user_etf_data', message="删除失败: 未提供ETF代码", type="error"))
    
    try:
        # 从自定义ETF中移除
        success = CustomETF.remove(user_id, symbol)
        
        if success:
            logger.info(f"已从自定义ETF列表中移除: 用户ID={user_id}, symbol={symbol}")
            return redirect(url_for('user_etf_data', message=f"已删除用户自定义ETF: {symbol}", type="success"))
        else:
            logger.warning(f"移除自定义ETF失败，可能不存在: 用户ID={user_id}, symbol={symbol}")
            return redirect(url_for('user_etf_data', message="移除失败，该ETF可能不在您的列表中", type="error"))
    except Exception as e:
        logger.error(f"删除自定义ETF时发生异常: {str(e)}", exc_info=True)
        return redirect(url_for('user_etf_data', message=f"删除失败: {str(e)}", type="error"))

@app.route('/user_etf_data')
@login_required
def user_etf_data():
    user = get_current_user()
    user_id = get_user_id(user)
    
    symbol = request.args.get('symbol')
    
    # 如果没有指定ETF代码，则显示用户的所有自定义ETF列表
    if not symbol:
        # 如果是GET请求，检查是否有message和type参数，如果有则显示flash消息
        message = request.args.get('message')
        msg_type = request.args.get('type', 'info')  # 默认为info类型
        
        if message:
            flash(message, msg_type)
            # 重定向到没有参数的URL，以避免刷新页面时再次显示消息
            if 'message' in request.args or 'type' in request.args:
                clean_args = dict(request.args)
                if 'message' in clean_args: del clean_args['message']
                if 'type' in clean_args: del clean_args['type']
                return redirect(url_for('user_etf_data', **clean_args))
        
        custom_etfs = CustomETF.get_user_custom_etfs(user_id)
        return render_template(
            'user_custom_etfs.html',
            custom_etfs=custom_etfs
        )
    
    # 验证该ETF是否在用户的自定义列表中
    custom_etf = CustomETF.get_custom_etf(user_id, symbol)
    if not custom_etf:
        flash("该ETF不在您的自定义列表中", "error")
        return redirect(url_for('user_etf_data'))
    
    try:
        # 获取ETF数据
        df, _ = get_etf_data(symbol)
        if df.empty:
            flash(f"获取{symbol}的数据失败", "error")
            return redirect(url_for('user_etf_data'))
        
        # 检查是否为官方ETF
        is_official = False
        official_etfs = get_official_etf_list()
        for etf in official_etfs:
            if etf['code'] == symbol:
                is_official = True
                break
        
        # 计算波动率
        volatility = calculate_volatility(symbol)
        # 计算网格间距
        grid_spacing = calculate_grid_spacing(symbol)
        # 计算网格范围
        grid_range = calculate_grid_range(symbol)
        
        # 获取最新数据
        latest_date = df.index[-1]
        latest_price = df.loc[latest_date, 'close']
        
        # 计算网格上下限
        upper_limit = grid_range.loc[latest_date, 'H_val']
        lower_limit = grid_range.loc[latest_date, 'L_val']
        
        # 添加安全检查，确保值不是NaN
        data_insufficient = False
        data_days = len(df)
        
        # 检查数据量是否足够（理论上需要800天数据，但实际上500天通常也足够计算）
        min_required_days = 500
        if data_days < min_required_days:
            logger.info(f"ETF {symbol} 数据量不足: {data_days} < {min_required_days}")
            data_insufficient = True
            # 提供默认值以便页面可以正常显示
            upper_limit = latest_price * 1.2 if not pd.isna(latest_price) else 0
            lower_limit = latest_price * 0.8 if not pd.isna(latest_price) else 0
            grid_spacing_value = 0.02  # 默认2%
            volatility_value = 0.20  # 默认20%
            total_levels = 10
            current_level = 5
            position = 50
        elif pd.isna(upper_limit) or pd.isna(lower_limit) or pd.isna(latest_price) or pd.isna(grid_spacing.iloc[-1]):
            # 如果有数据但计算结果有NaN，可能是因为特定日期的计算问题
            logger.info(f"ETF {symbol} 计算结果含NaN值，虽然有 {data_days} 天数据")
            data_insufficient = True
            # 提供默认值以便页面可以正常显示
            upper_limit = latest_price * 1.2 if not pd.isna(latest_price) else 0
            lower_limit = latest_price * 0.8 if not pd.isna(latest_price) else 0
            grid_spacing_value = 0.02  # 默认2%
            volatility_value = 0.20  # 默认20%
            total_levels = 10
            current_level = 5
            position = 50
        else:
            # 计算总网格数
            grid_spacing_value = grid_spacing.iloc[-1]
            volatility_value = volatility.iloc[-1]
            total_levels = max(5, int((upper_limit - lower_limit) / (latest_price * grid_spacing_value)))
            
            range_diff = upper_limit - lower_limit
            if range_diff > 0:
                level_fraction = max(0, min(1, (latest_price - lower_limit) / range_diff))
                current_level = round(level_fraction * total_levels)
                position = 100 * (1 - level_fraction)
            else:
                current_level = total_levels // 2
                position = 50
            
            current_level = min(max(0, current_level), total_levels)  # 确保在范围内
        
        # 处理数据用于前端展示
        df['date_str'] = df.index.strftime('%Y-%m-%d')
        price_data = df[['date_str', 'close']].values.tolist()
        
        return render_template(
            'user_etf_data.html',
            etf=custom_etf,
            price=latest_price,
            volatility=volatility_value * 100,  # 转为百分比
            grid_spacing=grid_spacing_value * 100,  # 转为百分比
            upper_limit=upper_limit,
            lower_limit=lower_limit,
            total_levels=total_levels,
            current_level=current_level,
            position=position,
            price_data=json.dumps(price_data),
            is_official=is_official,
            data_insufficient=data_insufficient
        )
    except Exception as e:
        logger.error(f"获取自定义ETF数据出错: {str(e)}")
        flash(f"获取ETF数据失败: {str(e)}", "error")
        return redirect(url_for('user_etf_data'))

# 添加用户自定义ETF管理端点
@app.route('/api/admin/custom_etfs/<int:id>', methods=['DELETE', 'POST'])
@login_required
@admin_required
@check_csrf_token
def api_delete_custom_etf(id):
    # 检查是否是DELETE请求或带有_method=DELETE参数的POST请求
    is_delete_request = (request.method == 'DELETE' or 
                        (request.method == 'POST' and 
                        (request.args.get('_method') == 'DELETE' or request.form.get('_method') == 'DELETE')))
    
    if not is_delete_request:
        return jsonify({'error': f'不支持的请求方法: {request.method}'}), 405
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 查找自定义ETF
    cursor.execute('SELECT * FROM custom_etfs WHERE id = ?', (id,))
    etf = cursor.fetchone()
    
    if not etf:
        conn.close()
        return jsonify({'error': '自定义ETF不存在'}), 404
    
    # 删除自定义ETF
    cursor.execute('DELETE FROM custom_etfs WHERE id = ?', (id,))
    conn.commit()
    conn.close()
    
    # 如果是表单提交，重定向到ETF管理页面
    if request.method == 'POST':
        flash(f'已删除用户自定义ETF: {etf["symbol"]}', "success")
        return redirect(url_for('admin_etfs', tab='custom'))
    # 如果是API请求，返回JSON响应
    return jsonify({'message': f'已删除用户自定义ETF: {etf["symbol"]}'})

# 获取ETF信息API
@app.route('/api/etf_info/<path:symbol>', methods=['GET'])
def api_etf_info(symbol):
    if not symbol:
        return jsonify({'error': 'ETF代码不能为空'}), 400
    
    try:
        # 尝试获取ETF数据以验证代码有效性
        df, _ = get_etf_data(symbol)
        if df.empty:
            return jsonify({'error': f"无法获取{symbol}的数据，请确认是有效的ETF或LOF代码"}), 404
        
        # 获取ETF名称和最新价格
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询etf_list表获取官方名称
        cursor.execute("SELECT name FROM etf_list WHERE symbol = ?", (symbol,))
        etf_info = cursor.fetchone()
        
        if etf_info and etf_info['name']:
            name = etf_info['name']
        else:
            # 如果数据库中没有该ETF的信息，尝试从数据中获取名称或使用默认名称
            try:
                # 查询该代码的历史数据中是否有名称信息
                cursor.execute("SELECT name FROM etf_daily_data WHERE code = ? AND name IS NOT NULL LIMIT 1", (symbol,))
                name_result = cursor.fetchone()
                if name_result and name_result['name']:
                    name = name_result['name']
                else:
                    name = f"{symbol} ETF"
            except:
                name = f"{symbol} ETF"
        
        # 获取最新价格
        latest_price = None
        if not df.empty:
            latest_price = float(df.iloc[-1]['close'])
        
        conn.close()
        
        return jsonify({
            'symbol': symbol,
            'name': name,
            'price': latest_price,
            'found': True
        })
        
    except Exception as e:
        logger.error(f"获取ETF信息出错: {str(e)}")
        return jsonify({'error': f"获取ETF信息失败: {str(e)}"}), 500

# 测试API页面路由
@app.route('/test_api')
def test_api():
    return render_template('test_api.html')

@app.route('/test_error')
def test_error():
    """测试错误处理路由"""
    app.logger.info('正在测试错误处理...')
    # 故意引发一个异常
    raise Exception("这是一个测试错误，用于检查错误处理功能是否正常工作")

# ---------- ETF管理API路由 ---------- #
@app.route('/api/admin/etfs/id/<int:id>', methods=['DELETE', 'POST'])
@login_required
@admin_required
@check_csrf_token
def api_delete_etf_by_id(id):
    """通过ID删除官方ETF"""
    # 检查是否是DELETE请求或带有_method=DELETE参数的POST请求
    is_delete_request = (request.method == 'DELETE' or 
                        (request.method == 'POST' and 
                        (request.args.get('_method') == 'DELETE' or request.form.get('_method') == 'DELETE')))
    
    if not is_delete_request:
        return jsonify({'error': f'不支持的请求方法: {request.method}'}), 405
    
    # 根据ID获取ETF信息
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM etf_list WHERE id = ?', (id,))
    etf = cursor.fetchone()
    conn.close()
    
    if not etf:
        return jsonify({'error': 'ETF不存在'}), 404
    
    # 获取ETF代码
    symbol = etf['symbol']
    
    # 删除ETF
    success, message, prompt, data_count = delete_etf(symbol)
    
    if success:
        # 如果是表单提交，重定向到ETF管理页面
        if request.method == 'POST':
            flash(message, "success")
            return redirect(url_for('admin_etfs', tab='official'))
        # 如果是API请求，返回JSON响应
        return jsonify({'message': message})
    else:
        if request.method == 'POST':
            flash(message, "error")
            return redirect(url_for('admin_etfs', tab='official'))
        return jsonify({'error': message}), 400

# 公共回测页面
@app.route('/public_backtest')
def public_backtest():
    # 获取官方ETF列表
    symbols = get_official_etf_list()
    
    # 检查用户是否已登录，如果已登录，将其自定义ETF添加到列表中
    user = get_current_user()
    custom_symbols = []
    
    if user:
        user_id = get_user_id(user)
        # 获取用户自定义ETF
        custom_etfs = CustomETF.get_user_custom_etfs(user_id)
        
        # 将自定义ETF添加到列表中
        for etf in custom_etfs:
            custom_symbols.append({
                "code": etf['symbol'],
                "name": etf['display_name'] if 'display_name' in etf and etf['display_name'] else etf['name'],
                "category": "自定义ETF",
                "correlation": "未分类",
                "volatility_type": "未知",
                "weight": "0%",
                "is_custom": True
            })
    
    # 合并官方ETF和自定义ETF列表
    all_symbols = symbols + custom_symbols
    
    return render_template('public_backtest.html', symbols=all_symbols)

# 获取ETF参数API
@app.route('/api/etf_params')
def get_etf_params():
    symbol = request.args.get('symbol')
    if not symbol:
        return jsonify({'error': '未提供ETF代码'}), 400
    
    # 获取请求来源页面和上下文
    referer = request.headers.get('Referer', '')
    page_context = request.args.get('context', '')  # 页面上下文参数
    portfolio_id = request.args.get('portfolio_id', '')  # 投资组合ID参数
    
    # 获取当前用户(如果已登录)
    user = get_current_user()
    user_id = None
    if user:
        user_id = get_user_id(user)
    
    # 验证ETF访问权限
    access_allowed = False
    
    # 检查是否是官方ETF
    is_official = any(s['code'] == symbol for s in SYMBOLS)
    
    if is_official:
        # 官方ETF在所有页面都可以访问
        access_allowed = True
    else:
        # 对于非官方ETF (自定义ETF)，根据上下文和用户权限进行验证
        # 判断页面上下文
        if 'dashboard' in referer or 'dashboard' == page_context or 'history' in referer or 'history' == page_context:
            # Dashboard和History页面仅允许官方ETF
            access_allowed = False
        elif 'public_backtest' in referer or 'public_backtest' == page_context:
            # Public_backtest页面允许官方ETF和当前用户自己的ETF
            if user_id:
                # 检查用户是否有权限访问该ETF
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM custom_etfs WHERE symbol = ? AND user_id = ?', (symbol, user_id))
                custom_etf = cursor.fetchone()
                conn.close()
                
                if custom_etf:
                    access_allowed = True
        elif 'portfolio' in referer or 'portfolio' == page_context:
            # Portfolio页面根据是否有portfolio_id参数决定
            if portfolio_id and user_id:
                # 验证该ETF是否在用户的投资组合中
                portfolio = Portfolio.get_by_id(portfolio_id, user_id)
                if portfolio:
                    # 检查该ETF是否在投资组合中
                    portfolio_etfs = [etf['symbol'] for etf in portfolio['etfs']]
                    if symbol in portfolio_etfs:
                        access_allowed = True
        
    # 如果无权访问，返回错误
    if not access_allowed:
        return jsonify({'error': '无权访问该ETF参数', 'symbol': symbol}), 403
    
    try:
        # 计算波动率
        volatility_series = calculate_volatility(symbol)
        latest_volatility = volatility_series.iloc[-1]
        latest_volatility_percentage = round(latest_volatility * 100, 2)
        
        # 计算网格间隔
        grid_spacing_series = calculate_grid_spacing(symbol)
        latest_grid_spacing = grid_spacing_series.iloc[-1]
        latest_grid_spacing_percentage = round(latest_grid_spacing * 100, 2)
        
        # 计算网格范围
        grid_range = calculate_grid_range(symbol)
        latest_range = grid_range.iloc[-1]
        upper_limit = latest_range['H_val']
        lower_limit = latest_range['L_val']
        
        return jsonify({
            'symbol': symbol,
            'volatility': latest_volatility_percentage,
            'grid_spacing': latest_grid_spacing_percentage,
            'upper_limit': float(upper_limit),
            'lower_limit': float(lower_limit)
        })
    except Exception as e:
        app.logger.error(f"获取ETF参数失败: {str(e)}", exc_info=True)
        return jsonify({'error': f'获取ETF参数失败: {str(e)}'}), 500

# 调试API端点用于检查回测参数
@app.route('/api/debug_backtest_params', methods=['POST'])
def debug_backtest_params():
    """用于调试回测参数的API端点"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': '无法解析请求数据'}), 400
        
        # 提取参数
        symbol = data.get('symbol')
        
        result = {}
        if symbol:
            # 获取ETF数据
            try:
                df, _ = get_etf_data(symbol)
                result['data_status'] = 'success'
                result['data_count'] = len(df)
                result['date_range'] = {
                    'min': df.index.min().strftime('%Y-%m-%d') if not df.empty else None,
                    'max': df.index.max().strftime('%Y-%m-%d') if not df.empty else None
                }
            except Exception as e:
                result['data_status'] = 'error'
                result['data_error'] = str(e)
        
            # 获取波动率
            try:
                volatility_series = calculate_volatility(symbol)
                result['volatility_status'] = 'success'
                result['volatility_count'] = len(volatility_series)
                result['latest_volatility'] = float(volatility_series.iloc[-1]) if not volatility_series.empty else None
            except Exception as e:
                result['volatility_status'] = 'error'
                result['volatility_error'] = str(e)
            
            # 获取网格间隔
            try:
                grid_spacing_series = calculate_grid_spacing(symbol)
                result['grid_spacing_status'] = 'success'
                result['grid_spacing_count'] = len(grid_spacing_series)
                result['latest_grid_spacing'] = float(grid_spacing_series.iloc[-1]) if not grid_spacing_series.empty else None
            except Exception as e:
                result['grid_spacing_status'] = 'error'
                result['grid_spacing_error'] = str(e)
            
            # 获取网格范围
            try:
                grid_range = calculate_grid_range(symbol)
                result['grid_range_status'] = 'success'
                result['grid_range_count'] = len(grid_range)
                if not grid_range.empty:
                    latest_range = grid_range.iloc[-1]
                    result['latest_upper_limit'] = float(latest_range['H_val'])
                    result['latest_lower_limit'] = float(latest_range['L_val'])
            except Exception as e:
                result['grid_range_status'] = 'error'
                result['grid_range_error'] = str(e)
        
        # 处理日期参数
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if start_date and end_date:
            try:
                start_date_parsed = pd.to_datetime(start_date)
                end_date_parsed = pd.to_datetime(end_date)
                result['date_parsing'] = 'success'
                result['parsed_dates'] = {
                    'start': start_date_parsed.strftime('%Y-%m-%d'),
                    'end': end_date_parsed.strftime('%Y-%m-%d')
                }
                
                # 检查日期是否在有效范围内
                if symbol and not df.empty:
                    in_range = ((start_date_parsed >= df.index.min()) and 
                               (end_date_parsed <= df.index.max()))
                    result['dates_in_data_range'] = in_range
            except Exception as e:
                result['date_parsing'] = 'error'
                result['date_error'] = str(e)
        
        # 返回所有原始参数以供参考
        result['original_params'] = data
        
        return jsonify(result)
        
    except Exception as e:
        app.logger.error(f"调试API错误: {str(e)}", exc_info=True)
        return jsonify({'error': f'调试失败: {str(e)}'}), 500

# 公共回测API
@app.route('/api/public_backtest', methods=['POST'])
def api_public_backtest():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': '无法解析请求数据'}), 400
        
        app.logger.info(f"接收到回测请求: {data}")
        
        # 提取参数
        symbol = data.get('symbol')
        if not symbol:
            return jsonify({'error': '未提供ETF代码'}), 400
        
        # 验证ETF访问权限
        access_allowed = False
        
        # 获取当前用户(如果已登录)
        user = get_current_user()
        user_id = None
        if user:
            user_id = get_user_id(user)
        
        # 检查是否是官方ETF
        is_official = any(s['code'] == symbol for s in SYMBOLS)
        
        if is_official:
            # 官方ETF在所有页面都可以访问
            access_allowed = True
        else:
            # 对于非官方ETF (自定义ETF)，只有当前用户自己的ETF才能访问
            if user_id:
                # 检查用户是否有权限访问该ETF
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM custom_etfs WHERE symbol = ? AND user_id = ?', (symbol, user_id))
                custom_etf = cursor.fetchone()
                conn.close()
                
                if custom_etf:
                    access_allowed = True
        
        # 如果无权访问，返回错误
        if not access_allowed:
            return jsonify({'error': '无权访问该ETF进行回测', 'symbol': symbol}), 403
            
        # 初始资金
        try:
            initial_capital = float(data.get('initial_capital', 100000))
        except (ValueError, TypeError):
            return jsonify({'error': '初始资金参数无效'}), 400
        
        # 日期范围
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        if not start_date or not end_date:
            return jsonify({'error': '未提供回测日期范围'}), 400
        
        # 网格层数
        try:
            grid_levels = int(data.get('grid_levels', 10))
            if grid_levels < 3 or grid_levels > 50:
                return jsonify({'error': '网格层数必须在3-50之间'}), 400
        except (ValueError, TypeError):
            return jsonify({'error': '网格层数参数无效'}), 400
        
        # 网格类型
        grid_type = data.get('grid_type', 'volatility')
        if grid_type not in ['volatility', 'arithmetic', 'geometric']:
            return jsonify({'error': '无效的网格类型'}), 400
        
        # 可选参数（如果提供）
        volatility = None
        grid_spacing = None
        grid_range_upper = None
        grid_range_lower = None
        
        # 处理自定义波动率
        if 'volatility' in data and data['volatility']:
            try:
                volatility = float(data.get('volatility')) / 100  # 百分比转为小数
            except (ValueError, TypeError):
                return jsonify({'error': '自定义波动率参数无效'}), 400
        
        # 处理自定义网格间隔
        if 'grid_spacing' in data and data['grid_spacing']:
            try:
                grid_spacing = float(data.get('grid_spacing')) / 100  # 百分比转为小数
            except (ValueError, TypeError):
                return jsonify({'error': '自定义网格间隔参数无效'}), 400
        
        # 处理自定义上下限
        if 'grid_range_upper' in data and data['grid_range_upper']:
            try:
                grid_range_upper = float(data.get('grid_range_upper'))
            except (ValueError, TypeError):
                return jsonify({'error': '自定义上限价格参数无效'}), 400
                
        if 'grid_range_lower' in data and data['grid_range_lower']:
            try:
                grid_range_lower = float(data.get('grid_range_lower'))
            except (ValueError, TypeError):
                return jsonify({'error': '自定义下限价格参数无效'}), 400
        
        # 将日期字符串转换为日期对象
        try:
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)
        except Exception:
            return jsonify({'error': '日期格式无效'}), 400
        
        # 确保日期范围有效
        if start_date >= end_date:
            return jsonify({'error': '开始日期必须早于结束日期'}), 400
        
        app.logger.info(f"准备执行回测: symbol={symbol}, initial_capital={initial_capital}, dates={start_date} to {end_date}, grid_levels={grid_levels}, grid_type={grid_type}")
        
        # 执行回测
        result = backtest_single_etf(
            symbol, 
            initial_capital, 
            start_date, 
            end_date, 
            grid_levels, 
            grid_type,
            volatility,
            grid_spacing,
            grid_range_upper,
            grid_range_lower
        )
        
        # 检查是否有错误
        if 'error' in result:
            app.logger.error(f"回测失败: {result['error']}")
            return jsonify({'error': result['error']}), 400
        
        # 处理日期数据以便JSON序列化
        if 'dates' in result:
            result['dates'] = [d.strftime('%Y-%m-%d') if isinstance(d, pd.Timestamp) else d for d in result['dates']]
        
        # 计算并添加更多指标
        if 'total_equity' in result and len(result['total_equity']) > 0:
            total_profit = result['total_equity'][-1] - initial_capital
            total_return = (total_profit / initial_capital) * 100
            result['total_return'] = round(total_return, 2)
        else:
            result['total_return'] = 0
            
        # 添加持仓和网格收益（如果存在）
        if 'position_profit' in result:
            # 保持position_profit为金额形式，不转为百分比
            result['position_profit'] = round(result['position_profit'], 2)
        else:
            result['position_profit'] = 0
            
        if 'grid_profit' in result:
            # 保持grid_profit为百分比形式，不做额外转换
            result['grid_profit'] = round(result['grid_profit'], 2) 
        else:
            result['grid_profit'] = 0
        
        # 交易次数
        if 'trades' in result:
            result['trade_count'] = len(result['trades'])
        else:
            result['trade_count'] = 0
            result['trades'] = []
        
        app.logger.info(f"回测完成: {symbol}, 年化收益={result.get('annual_return', '未知')}%, 夏普比率={result.get('sharpe_ratio', '未知')}")
        
        return jsonify(result)
        
    except Exception as e:
        app.logger.error(f"公共回测API错误: {str(e)}", exc_info=True)
        return jsonify({'error': f'回测失败: {str(e)}'}), 500

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)