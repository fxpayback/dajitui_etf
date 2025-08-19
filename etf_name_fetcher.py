import akshare as ak
import pandas as pd
import sqlite3
import logging
import sys
import time
import os
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_fetcher.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class ETFNameFetcher:
    def __init__(self, db_path='database/etf_history.db'):
        self.db_path = db_path
        # 确保数据库目录存在
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        # 预定义的ETF映射表（备用）
        self.etf_dict = {
            
        }
    
    def get_etf_name_from_eastmoney(self, symbol):
        """从东方财富网获取ETF名称"""
        try:
            logging.info(f"尝试从东方财富获取ETF {symbol} 信息...")
            etf_data = ak.fund_etf_spot_em()
            
            # 调试：打印列名和数据形状
            logging.info(f"获取到ETF数据，形状: {etf_data.shape}, 列名: {list(etf_data.columns)}")
            
            # 检查具体内容
            if not etf_data.empty:
                sample = etf_data.head(3)
                logging.info(f"数据样例:\n{sample}")

            # 查找对应ETF
            etf_info = etf_data[etf_data['代码'] == symbol]
            
            if not etf_info.empty:
                name = etf_info.iloc[0]['名称']
                logging.info(f"从东方财富获取到ETF名称: {name}")
                return name, True
            else:
                logging.warning(f"在东方财富数据中未找到ETF {symbol}")
                return None, False
                
        except Exception as e:
            logging.error(f"从东方财富获取ETF名称失败: {str(e)}", exc_info=True)
            return None, False
    
    def get_etf_name(self, symbol):
        """获取ETF名称，优先东方财富，失败则使用预定义映射"""
        # 1. 尝试从东方财富网获取
        name, success = self.get_etf_name_from_eastmoney(symbol)
        if success and name:
            return name
            
        # 2. 使用预定义的ETF字典
        if symbol in self.etf_dict:
            name = self.etf_dict[symbol]
            logging.info(f"使用预定义ETF名称: {symbol} = {name}")
            return name
            
        # 3. 最后的备选方案
        logging.warning(f"无法获取ETF {symbol} 的名称，使用默认名称")
        return f"{symbol} ETF"
    
    def check_table_columns(self):
        """检查etf_list表结构并添加缺少的列"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 检查etf_list表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='etf_list'")
            if not cursor.fetchone():
                # 不存在则创建表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS etf_list (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    category TEXT,
                    updated_at TEXT
                )
                ''')
                logging.info("创建etf_list表成功")
                conn.commit()
                conn.close()
                return True
            
            # 表存在，检查列
            cursor.execute("PRAGMA table_info(etf_list)")
            columns = [column[1] for column in cursor.fetchall()]
            logging.info(f"当前表结构: {columns}")
            
            # 检查并添加updated_at列
            if 'updated_at' not in columns:
                logging.info("添加缺失的updated_at列")
                cursor.execute("ALTER TABLE etf_list ADD COLUMN updated_at TEXT")
            
            # 检查并添加category列
            if 'category' not in columns:
                logging.info("添加缺失的category列")
                cursor.execute("ALTER TABLE etf_list ADD COLUMN category TEXT")
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"检查表结构失败: {str(e)}", exc_info=True)
            return False
    
    def save_etf_to_db(self, symbol, name):
        """将ETF信息保存到数据库"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 检查是否存在updated_at列
            cursor.execute("PRAGMA table_info(etf_list)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'updated_at' in columns:
                # 如果有updated_at列
                cursor.execute("""
                INSERT OR REPLACE INTO etf_list (symbol, name, updated_at)
                VALUES (?, ?, datetime('now', 'localtime'))
                """, (symbol, name))
            else:
                # 如果没有updated_at列
                cursor.execute("""
                INSERT OR REPLACE INTO etf_list (symbol, name)
                VALUES (?, ?)
                """, (symbol, name))
            
            conn.commit()
            conn.close()
            logging.info(f"保存ETF {symbol}:{name} 到数据库成功")
            return True
        except Exception as e:
            logging.error(f"保存ETF到数据库失败: {str(e)}", exc_info=True)
            return False
    
    def fetch_and_save_etfs(self, symbols=None):
        """获取并保存多个ETF信息"""
        # 检查并更新表结构
        if not self.check_table_columns():
            logging.error("检查表结构失败，尝试继续操作")
        
        # 如果没有提供symbols，尝试获取所有ETF列表
        if not symbols:
            try:
                logging.info("尝试从东方财富获取所有ETF列表...")
                etf_data = ak.fund_etf_spot_em()
                symbols = etf_data['代码'].tolist()
                logging.info(f"获取到 {len(symbols)} 个ETF代码")
            except Exception as e:
                logging.error(f"获取所有ETF列表失败: {str(e)}", exc_info=True)
                # 使用预定义ETF列表
                symbols = list(self.etf_dict.keys())
                logging.info(f"使用预定义的 {len(symbols)} 个ETF代码")
        
        success_count = 0
        fail_count = 0
        
        for symbol in symbols:
            try:
                name = self.get_etf_name(symbol)
                if self.save_etf_to_db(symbol, name):
                    success_count += 1
                else:
                    fail_count += 1
                # 添加延迟，避免请求过快
                time.sleep(0.2)
            except Exception as e:
                logging.error(f"处理ETF {symbol} 失败: {str(e)}", exc_info=True)
                fail_count += 1
        
        logging.info(f"ETF获取和保存完成: 成功 {success_count}, 失败 {fail_count}")
        return success_count, fail_count

def main():
    logging.info(f"ETF名称获取脚本开始运行 - {datetime.now()}")
    logging.info(f"Python版本: {sys.version}")
    logging.info(f"AKSHARE版本: {ak.__version__}")
    
    # 创建ETF名称获取器
    fetcher = ETFNameFetcher()
    
    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == "all":
            # 获取所有ETF信息
            fetcher.fetch_and_save_etfs()
        else:
            # 获取指定ETF信息
            symbols = sys.argv[1:]
            fetcher.fetch_and_save_etfs(symbols)
    else:
        # 默认获取所有ETF
        fetcher.fetch_and_save_etfs()
    
    logging.info("ETF名称获取脚本运行完成")

if __name__ == "__main__":
    main() 