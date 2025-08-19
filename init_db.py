import os
import sqlite3
import logging
from models.user import create_user_tables

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect('database/etf_history.db', timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """初始化数据库"""
    try:
        # 确保database目录存在
        if not os.path.exists('database'):
            os.makedirs('database')
            logger.info("已创建database目录")
        
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
        
        # 创建ETF列表管理表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS etf_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT UNIQUE,
            name TEXT,
            description TEXT,
            is_official INTEGER DEFAULT 1,
            category TEXT,
            correlation TEXT,
            volatility_type TEXT,
            weight REAL DEFAULT 1.0,
            created_at TEXT,
            last_updated TEXT
        )
        ''')
        
        # 创建官方ETF列表
        official_etfs_file = 'data/official_etfs.sql'
        if os.path.exists(official_etfs_file):
            with open(official_etfs_file, 'r', encoding='utf-8') as f:
                sql_script = f.read()
                cursor.executescript(sql_script)
                logger.info("已导入官方ETF列表")
        
        conn.commit()
        conn.close()
        logger.info("ETF数据表初始化成功")
        
        # 创建用户相关表
        create_user_tables()
        logger.info("用户相关表初始化成功")
        
        print("数据库初始化完成! 所有必要的表已创建。")
        return True
    except Exception as e:
        logger.error(f"数据库初始化失败: {str(e)}")
        print(f"错误: 数据库初始化失败 - {str(e)}")
        return False

if __name__ == "__main__":
    print("开始初始化数据库...")
    init_db() 