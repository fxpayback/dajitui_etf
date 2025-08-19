import sqlite3
import os

# 检查数据库文件是否存在
db_path = 'database/etf_history.db'
print(f"数据库文件路径: {db_path}")
print(f"数据库文件是否存在: {os.path.exists(db_path)}")

if os.path.exists(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 查看所有表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"数据库中的表: {tables}")
        
        # 如果 etf_list 表存在，查看其结构和数据
        if any('etf_list' in table for table in tables):
            # 查看表结构
            cursor.execute("PRAGMA table_info(etf_list)")
            columns = cursor.fetchall()
            print(f"etf_list 表结构: {columns}")
            
            # 查看 510300 的数据
            cursor.execute("SELECT * FROM etf_list WHERE symbol = '510300'")
            data = cursor.fetchall()
            print(f"510300 的数据: {data}")
            
            # 查看所有官方 ETF 的 symbol 和 weight
            cursor.execute("SELECT symbol, weight FROM etf_list WHERE is_official = 1 LIMIT 10")
            all_data = cursor.fetchall()
            print(f"前10个官方ETF的symbol和weight: {all_data}")
        else:
            print("etf_list 表不存在")
        
        conn.close()
    except Exception as e:
        print(f"数据库操作出错: {e}")
else:
    print("数据库文件不存在") 