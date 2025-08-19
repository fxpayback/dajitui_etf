import sqlite3
import json
from datetime import datetime

# ETF列表
SYMBOLS = [
    {"name": "沪深 300ETF", "code": "510300", "volatility_type": "中低波动", "correlation": "大盘蓝筹基准", "weight": "5%", "category": "宽基指数"},
    {"name": "中证 500ETF", "code": "510500", "volatility_type": "中高波动", "correlation": "中小盘均衡", "weight": "5%", "category": "宽基指数"},
    {"name": "科创 50ETF", "code": "588000", "volatility_type": "高波动", "correlation": "硬科技核心", "weight": "4%", "category": "宽基指数"},
    {"name": "创业板 ETF", "code": "159915", "volatility_type": "高波动", "correlation": "成长风格", "weight": "4%", "category": "宽基指数"},
    {"name": "人工智能 ETF", "code": "159819", "volatility_type": "极高波动", "correlation": "AI全产业链", "weight": "4%", "category": "科技成长"},
    {"name": "半导体 ETF", "code": "512480", "volatility_type": "极高波动", "correlation": "国产替代周期", "weight": "2%", "category": "科技成长"},
    {"name": "新能源车 ETF", "code": "515030", "volatility_type": "高波动", "correlation": "碳中和主线", "weight": "4%", "category": "科技成长"},
    {"name": "5G 通信 ETF", "code": "515050", "volatility_type": "高波动", "correlation": "通信基础设施", "weight": "3%", "category": "科技成长"},
    {"name": "酒 ETF", "code": "512690", "volatility_type": "中高波动", "correlation": "高端消费韧性", "weight": "4%", "category": "消费升级"},
    {"name": "消费 50ETF", "code": "159936", "volatility_type": "中高波动", "correlation": "必选消费龙头", "weight": "4%", "category": "消费升级"},
    {"name": "旅游 ETF", "code": "159766", "volatility_type": "中高波动", "correlation": "文旅复苏主题", "weight": "2%", "category": "消费升级"},
    {"name": "医药 ETF", "code": "512010", "volatility_type": "中波动", "correlation": "全医药行业覆盖", "weight": "5%", "category": "医疗健康"},
    {"name": "创新药 ETF", "code": "159992", "volatility_type": "极高波动", "correlation": "前沿疗法/CXO", "weight": "2%", "category": "医疗健康"},
    {"name": "银行 ETF", "code": "512800", "volatility_type": "低波动", "correlation": "高股息防御", "weight": "4%", "category": "周期价值"},
    {"name": "证券 ETF", "code": "512880", "volatility_type": "高波动", "correlation": "市场情绪放大器", "weight": "3%", "category": "周期价值"},
    {"name": "煤炭 ETF", "code": "515220", "volatility_type": "中高波动", "correlation": "能源安全+高分红", "weight": "3%", "category": "周期价值"},
    {"name": "纳指 ETF", "code": "513100", "volatility_type": "高波动", "correlation": "美股科技龙头", "weight": "4%", "category": "跨境对冲"},
    {"name": "恒生指数 ETF", "code": "159920", "volatility_type": "中波动", "correlation": "港股估值洼地", "weight": "3%", "category": "跨境对冲"},
    {"name": "日经 225ETF", "code": "513520", "volatility_type": "中波动", "correlation": "日元资产对冲", "weight": "2%", "category": "跨境对冲"},
    {"name": "法国 CAC40ETF", "code": "513080", "volatility_type": "中波动", "correlation": "欧洲经济分散", "weight": "1%", "category": "跨境对冲"},
    {"name": "中概互联 ETF", "code": "513050", "volatility_type": "高波动", "correlation": "海外中国互联网", "weight": "3%", "category": "跨境对冲"},
    {"name": "环保 ETF", "code": "512580", "volatility_type": "中波动", "correlation": "双碳政策驱动", "weight": "3%", "category": "另类主题"},
    {"name": "房地产 ETF", "code": "512200", "volatility_type": "中波动", "correlation": "政策博弈品种", "weight": "2%", "category": "另类主题"},
    {"name": "游戏 ETF", "code": "159869", "volatility_type": "极高波动", "correlation": "元宇宙/云游戏", "weight": "3%", "category": "另类主题"},
    {"name": "红利低波 ETF", "code": "512890", "volatility_type": "低波动", "correlation": "高股息防御", "weight": "2%", "category": "另类主题"},
    {"name": "易方达新综债LOF", "code": "161119", "volatility_type": "低波动", "correlation": "利率债压舱石/国债平替", "weight": "3%", "category": "债券及现金"},
    {"name": "中欧纯债LOF", "code": "161010", "volatility_type": "极低波动", "correlation": "流动性缓冲/短债平替", "weight": "3%", "category": "债券及现金"},
    {"name": "可转债 ETF", "code": "511380", "volatility_type": "中波动", "correlation": "股债混合属性", "weight": "2%", "category": "债券及现金"},
    {"name": "美元债 LOF", "code": "501300", "volatility_type": "低波动", "correlation": "美债利率对冲", "weight": "2%", "category": "债券及现金"},
    {"name": "原油 ETF", "code": "162411", "volatility_type": "极高波动", "correlation": "地缘政治+通胀对冲", "weight": "3%", "category": "商品与通胀"},
    {"name": "黄金 ETF", "code": "518800", "volatility_type": "中波动", "correlation": "避险资产核心", "weight": "4%", "category": "商品与通胀"},
    {"name": "豆粕 ETF", "code": "159985", "volatility_type": "中高波动", "correlation": "农产品周期", "weight": "2%", "category": "商品与通胀"},
]

def get_db_connection():
    conn = sqlite3.connect('database/etf_history.db')
    conn.row_factory = sqlite3.Row
    return conn

def import_etfs():
    conn = get_db_connection()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # 检查etf_list表是否存在，如果不存在则创建
        conn.execute('''
        CREATE TABLE IF NOT EXISTS etf_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT,
            is_official INTEGER DEFAULT 1,
            category TEXT,
            correlation TEXT,
            volatility_type TEXT,
            weight TEXT,
            created_at TEXT,
            last_updated TEXT
        )
        ''')
        
        # 添加索引
        conn.execute('CREATE INDEX IF NOT EXISTS idx_etf_symbol ON etf_list (symbol)')
        
        # 导入ETF
        for etf in SYMBOLS:
            # 从权重中移除百分号并转换为浮点数
            weight_value = etf["weight"].replace("%", "")
            
            # 插入ETF
            conn.execute('''
            INSERT INTO etf_list (symbol, name, description, is_official, category, correlation, volatility_type, weight, created_at, last_updated)
            VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
            ''', (
                etf["code"],
                etf["name"],
                f"{etf['name']} - {etf['correlation']}",
                etf["category"],
                etf["correlation"],
                etf["volatility_type"],
                weight_value,
                current_time,
                current_time
            ))
        
        conn.commit()
        print(f"成功导入 {len(SYMBOLS)} 个ETF到etf_list表")
    except Exception as e:
        conn.rollback()
        print(f"导入ETF时出错: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    import_etfs() 