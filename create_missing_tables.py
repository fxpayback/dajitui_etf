import sqlite3
import os
import time
from datetime import datetime

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect('database/etf_history.db')
    conn.row_factory = sqlite3.Row
    return conn

def check_and_create_tables():
    """检查并创建缺失的表"""
    print("开始检查数据库表...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 获取当前所有表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = [row['name'] for row in cursor.fetchall()]
    print(f"现有表: {', '.join(existing_tables)}")
    
    # 定义所有需要的表和它们的创建语句
    tables = {
        'users': '''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_login TEXT,
            is_active INTEGER DEFAULT 1,
            is_admin INTEGER DEFAULT 0
        )
        ''',
        
        'portfolios': '''
        CREATE TABLE IF NOT EXISTS portfolios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            total_amount REAL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            is_default INTEGER DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        ''',
        
        'portfolio_etfs': '''
        CREATE TABLE IF NOT EXISTS portfolio_etfs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            weight REAL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
        )
        ''',
        
        'favorite_etfs': '''
        CREATE TABLE IF NOT EXISTS favorite_etfs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            added_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id),
            UNIQUE(user_id, symbol)
        )
        ''',
        
        'custom_etfs': '''
        CREATE TABLE IF NOT EXISTS custom_etfs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            added_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id),
            UNIQUE(user_id, symbol)
        )
        ''',
        
        'user_settings': '''
        CREATE TABLE IF NOT EXISTS user_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            setting_key TEXT NOT NULL,
            setting_value TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id),
            UNIQUE(user_id, setting_key)
        )
        '''
    }
    
    # 索引定义
    indexes = {
        'idx_portfolios_user_id': 'CREATE INDEX IF NOT EXISTS idx_portfolios_user_id ON portfolios (user_id)',
        'idx_portfolio_etfs_portfolio_id': 'CREATE INDEX IF NOT EXISTS idx_portfolio_etfs_portfolio_id ON portfolio_etfs (portfolio_id)',
        'idx_favorite_etfs_user_id': 'CREATE INDEX IF NOT EXISTS idx_favorite_etfs_user_id ON favorite_etfs (user_id)',
        'idx_custom_etfs_user_id': 'CREATE INDEX IF NOT EXISTS idx_custom_etfs_user_id ON custom_etfs (user_id)',
        'idx_user_settings_user_id': 'CREATE INDEX IF NOT EXISTS idx_user_settings_user_id ON user_settings (user_id)'
    }
    
    try:
        # 创建缺失的表
        for table_name, create_statement in tables.items():
            if table_name not in existing_tables:
                print(f"创建表 {table_name}...")
                cursor.execute(create_statement)
                print(f"表 {table_name} 创建成功!")
            else:
                print(f"表 {table_name} 已存在，跳过")
        
        # 创建索引
        for index_name, create_statement in indexes.items():
            print(f"创建索引 {index_name}...")
            cursor.execute(create_statement)
        
        conn.commit()
        print("所有表和索引创建完成!")
    except Exception as e:
        conn.rollback()
        print(f"创建表时出错: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    # 确保数据库目录存在
    os.makedirs('database', exist_ok=True)
    
    # 检查并创建表
    check_and_create_tables()
    
    print("脚本执行完成。") 