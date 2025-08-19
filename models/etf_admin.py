import sqlite3
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect('database/etf_history.db', timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def get_all_etfs():
    """获取所有ETF列表"""
    conn = get_db_connection()
    etfs = conn.execute('SELECT * FROM etf_list ORDER BY symbol').fetchall()
    conn.close()
    return etfs

def get_etf_by_symbol(symbol):
    """根据代码获取ETF信息"""
    conn = get_db_connection()
    etf = conn.execute('SELECT * FROM etf_list WHERE symbol = ?', (symbol,)).fetchone()
    conn.close()
    return etf

def add_etf(symbol, name, description, is_official=0, category='', correlation='', volatility_type='', weight=1.0):
    """添加新的ETF"""
    conn = get_db_connection()
    try:
        # 检查ETF是否已存在
        existing = conn.execute('SELECT 1 FROM etf_list WHERE symbol = ?', (symbol,)).fetchone()
        if existing:
            conn.close()
            return False, "ETF代码已存在"
        
        # 添加新ETF
        conn.execute('''
            INSERT INTO etf_list 
            (symbol, name, description, is_official, category, correlation, volatility_type, weight, created_at, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        ''', (symbol, name, description, 1 if is_official else 0, category, correlation, volatility_type, weight))
        conn.commit()
        conn.close()
        return True, "ETF添加成功"
    except Exception as e:
        conn.rollback()
        conn.close()
        logger.error(f"添加ETF失败: {str(e)}")
        return False, f"添加ETF失败: {str(e)}"

def update_etf(symbol, name, description, is_official, category, correlation, volatility_type, weight, new_symbol=None):
    """更新ETF信息，如果提供new_symbol则同时更改ETF代码"""
    conn = get_db_connection()
    try:
        # 检查是否需要更新symbol
        if new_symbol and new_symbol != symbol:
            # 检查新symbol是否已经存在
            existing = conn.execute('SELECT 1 FROM etf_list WHERE symbol = ?', (new_symbol,)).fetchone()
            if existing:
                conn.close()
                return False, f"ETF代码 {new_symbol} 已存在，无法更新"
            
            # 更新ETF基本信息和代码
            conn.execute('''
                UPDATE etf_list 
                SET symbol = ?, name = ?, description = ?, is_official = ?, category = ?, 
                    correlation = ?, volatility_type = ?, weight = ?, last_updated = datetime('now')
                WHERE symbol = ?
            ''', (new_symbol, name, description, 1 if is_official else 0, category, correlation, volatility_type, weight, symbol))
            
            # 更新相关的历史数据的symbol
            conn.execute('UPDATE etf_data SET symbol = ? WHERE symbol = ?', (new_symbol, symbol))
            
            # 更新投资组合中的ETF引用
            conn.execute('UPDATE portfolio_etfs SET symbol = ? WHERE symbol = ?', (new_symbol, symbol))
        else:
            # 只更新基本信息，不更改代码
            conn.execute('''
                UPDATE etf_list 
                SET name = ?, description = ?, is_official = ?, category = ?, 
                    correlation = ?, volatility_type = ?, weight = ?, last_updated = datetime('now')
                WHERE symbol = ?
            ''', (name, description, 1 if is_official else 0, category, correlation, volatility_type, weight, symbol))
        
        conn.commit()
        conn.close()
        return True, "ETF更新成功"
    except Exception as e:
        conn.rollback()
        conn.close()
        logger.error(f"更新ETF失败: {str(e)}")
        return False, f"更新ETF失败: {str(e)}"

def delete_etf(symbol):
    """删除ETF"""
    conn = get_db_connection()
    try:
        # 先检查是否有关联的数据
        data_count = conn.execute('SELECT COUNT(*) FROM etf_data WHERE symbol = ?', (symbol,)).fetchone()[0]
        
        # 删除ETF记录
        conn.execute('DELETE FROM etf_list WHERE symbol = ?', (symbol,))
        
        # 可选：删除相关历史数据
        if data_count > 0:
            prompt = f"该ETF有{data_count}条历史数据记录，是否一并删除？"
        else:
            prompt = None
            
        conn.commit()
        conn.close()
        return True, "ETF删除成功", prompt, data_count
    except Exception as e:
        conn.rollback()
        conn.close()
        logger.error(f"删除ETF失败: {str(e)}")
        return False, f"删除ETF失败: {str(e)}", None, 0

def get_etf_data_count(symbol):
    """获取指定ETF的历史数据记录数量"""
    conn = get_db_connection()
    
    # 从etf_data表中查询数据量
    count = conn.execute('SELECT COUNT(*) FROM etf_data WHERE symbol = ?', (symbol,)).fetchone()[0]
    
    conn.close()
    return count

def get_etf_date_range(symbol):
    """获取指定ETF的数据日期范围"""
    conn = get_db_connection()
    result = conn.execute('''
        SELECT MIN(date) as start_date, MAX(date) as end_date 
        FROM etf_data 
        WHERE symbol = ?
    ''', (symbol,)).fetchone()
    conn.close()
    
    if result and result['start_date'] and result['end_date']:
        return result['start_date'], result['end_date']
    return None, None

def clear_etf_data(symbol=None):
    """清除ETF历史数据"""
    conn = get_db_connection()
    try:
        if symbol:
            # 清除指定ETF的数据
            conn.execute('DELETE FROM etf_data WHERE symbol = ?', (symbol,))
            message = f"已清除{symbol}的所有历史数据"
        else:
            # 清除所有ETF数据
            conn.execute('DELETE FROM etf_data')
            message = "已清除所有ETF的历史数据"
        
        conn.commit()
        conn.close()
        return True, message
    except Exception as e:
        conn.rollback()
        conn.close()
        logger.error(f"清除ETF数据失败: {str(e)}")
        return False, f"清除ETF数据失败: {str(e)}" 