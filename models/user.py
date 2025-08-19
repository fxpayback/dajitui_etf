import sqlite3
import datetime
import hashlib
import os
import uuid

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect('database/etf_history.db', timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def create_user_tables():
    """创建用户相关数据表"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 创建用户表
    cursor.execute('''
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
    ''')
    
    # 创建投资组合表
    cursor.execute('''
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
    ''')
    
    # 创建投资组合ETF关系表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS portfolio_etfs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        portfolio_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        weight REAL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
    )
    ''')
    
    # 创建用户自选ETF表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS favorite_etfs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        added_at TEXT NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users (id),
        UNIQUE(user_id, symbol)
    )
    ''')
    
    # 创建用户自定义ETF表
    cursor.execute('''
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
    ''')
    
    # 创建用户设置表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        setting_key TEXT NOT NULL,
        setting_value TEXT,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users (id),
        UNIQUE(user_id, setting_key)
    )
    ''')
    
    # 添加索引提高查询效率
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_portfolios_user_id ON portfolios (user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_portfolio_etfs_portfolio_id ON portfolio_etfs (portfolio_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_favorite_etfs_user_id ON favorite_etfs (user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_custom_etfs_user_id ON custom_etfs (user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_settings_user_id ON user_settings (user_id)')
    
    conn.commit()
    conn.close()

def hash_password(password):
    """密码加密"""
    salt = os.environ.get('PASSWORD_SALT', 'default_salt_value')
    return hashlib.sha256((password + salt).encode()).hexdigest()

def verify_password(password, password_hash):
    """验证密码"""
    return hash_password(password) == password_hash

class User:
    """用户模型"""
    
    @staticmethod
    def create(username, email, password, is_admin=False):
        """创建新用户"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 检查用户名和邮箱是否已存在
        cursor.execute('SELECT * FROM users WHERE username = ? OR email = ?', (username, email))
        if cursor.fetchone():
            conn.close()
            return False, "用户名或邮箱已存在"
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            'INSERT INTO users (username, email, password_hash, created_at, is_admin) VALUES (?, ?, ?, ?, ?)',
            (username, email, hash_password(password), now, 1 if is_admin else 0)
        )
        
        user_id = cursor.lastrowid
        
        # 创建默认设置
        default_settings = {
            'theme': 'light',
            'default_view': 'dashboard',
            'notification': 'on'
        }
        
        for key, value in default_settings.items():
            cursor.execute(
                'INSERT INTO user_settings (user_id, setting_key, setting_value, updated_at) VALUES (?, ?, ?, ?)',
                (user_id, key, value, now)
            )
        
        # 创建默认投资组合
        cursor.execute(
            'INSERT INTO portfolios (user_id, name, description, created_at, updated_at, is_default) VALUES (?, ?, ?, ?, ?, ?)',
            (user_id, '默认组合', '自动创建的默认投资组合', now, now, 1)
        )
        
        conn.commit()
        conn.close()
        
        return True, user_id
    
    @staticmethod
    def authenticate(username, password):
        """用户认证，返回(成功标志, 用户对象或错误消息)"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询用户
        cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
        user = cursor.fetchone()
        
        if not user:
            conn.close()
            return False, "用户名不存在"
        
        if not verify_password(password, user['password_hash']):
            conn.close()
            return False, "密码不正确"
        
        # 更新最后登录时间
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('UPDATE users SET last_login = ? WHERE id = ?', (now, user['id']))
        conn.commit()
        
        conn.close()
        
        # 将行转换为字典
        user_dict = dict(user)
        
        # 创建一个User对象，并为其添加属性
        user_obj = type('User', (), user_dict)
        
        return True, user_obj
    
    @staticmethod
    def get_by_id(user_id):
        """通过ID获取用户"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        user_data = cursor.fetchone()
        
        conn.close()
        
        if not user_data:
            return None
            
        # 将行转换为字典
        user_dict = dict(user_data)
        
        # 检查是否存在is_admin字段，如果不存在则加入默认值
        if 'is_admin' not in user_dict:
            # 重新连接数据库，检查是否需要添加is_admin列
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 检查users表是否有is_admin列
            cursor.execute("PRAGMA table_info(users)")
            columns = cursor.fetchall()
            has_is_admin = any(col['name'] == 'is_admin' for col in columns)
            
            if not has_is_admin:
                # 添加is_admin列
                cursor.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0")
                conn.commit()
            
            conn.close()
            user_dict['is_admin'] = 0
        
        # 创建一个User对象，并为其添加属性
        user = type('User', (), user_dict)
        
        return user

class Portfolio:
    """投资组合模型"""
    
    @staticmethod
    def create(user_id, name, description="", total_amount=0):
        """创建投资组合"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            'INSERT INTO portfolios (user_id, name, description, total_amount, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)',
            (user_id, name, description, total_amount, now, now)
        )
        
        portfolio_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return portfolio_id
    
    @staticmethod
    def get_user_portfolios(user_id):
        """获取用户的所有投资组合"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM portfolios WHERE user_id = ? ORDER BY is_default DESC, name', (user_id,))
        portfolios = cursor.fetchall()
        
        result = []
        for portfolio in portfolios:
            portfolio_dict = dict(portfolio)
            
            # 获取组合中的ETF
            cursor.execute('SELECT * FROM portfolio_etfs WHERE portfolio_id = ?', (portfolio['id'],))
            etfs = cursor.fetchall()
            portfolio_dict['etfs'] = [dict(etf) for etf in etfs]
            
            result.append(portfolio_dict)
        
        conn.close()
        return result
    
    @staticmethod
    def get_by_id(portfolio_id, user_id=None):
        """获取投资组合详情，包括ETF列表"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询投资组合
        if user_id:
            cursor.execute('SELECT * FROM portfolios WHERE id = ? AND user_id = ?', (portfolio_id, user_id))
        else:
            cursor.execute('SELECT * FROM portfolios WHERE id = ?', (portfolio_id,))
        
        portfolio = cursor.fetchone()
        
        if not portfolio:
            conn.close()
            return None
        
        portfolio_dict = dict(portfolio)
        
        # 获取组合中的ETF，包括ETF信息
        cursor.execute('''
        SELECT pe.*, e.name as etf_name, e.category, e.volatility_type, e.is_official,
               CASE WHEN e.name IS NULL THEN 
                   (SELECT c.name FROM custom_etfs c WHERE c.symbol = pe.symbol AND c.user_id = ?)
               ELSE e.name END as name
        FROM portfolio_etfs pe
        LEFT JOIN etf_list e ON pe.symbol = e.symbol
        WHERE pe.portfolio_id = ?
        ORDER BY pe.weight DESC
        ''', (portfolio_dict['user_id'], portfolio_id))
        
        etfs = cursor.fetchall()
        
        # 转换ETF为字典列表并计算总权重
        etf_list = []
        total_weight = 0
        for etf in etfs:
            etf_dict = dict(etf)
            etf_list.append(etf_dict)
            total_weight += etf_dict.get('weight', 0)
        
        portfolio_dict['etfs'] = etf_list
        portfolio_dict['total_weight'] = total_weight
        
        conn.close()
        return portfolio_dict

    @staticmethod
    def add_etf(portfolio_id, symbol, weight=0):
        """向投资组合添加ETF"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 检查ETF是否已存在于组合中
        cursor.execute('SELECT * FROM portfolio_etfs WHERE portfolio_id = ? AND symbol = ?', (portfolio_id, symbol))
        if cursor.fetchone():
            # 更新权重
            cursor.execute(
                'UPDATE portfolio_etfs SET weight = ?, updated_at = ? WHERE portfolio_id = ? AND symbol = ?',
                (weight, now, portfolio_id, symbol)
            )
        else:
            # 添加新ETF
            cursor.execute(
                'INSERT INTO portfolio_etfs (portfolio_id, symbol, weight, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
                (portfolio_id, symbol, weight, now, now)
            )
        
        # 更新组合的更新时间
        cursor.execute('UPDATE portfolios SET updated_at = ? WHERE id = ?', (now, portfolio_id))
        
        conn.commit()
        conn.close()
        return True
    
    @staticmethod
    def remove_etf(portfolio_id, symbol):
        """从投资组合中移除ETF"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute('DELETE FROM portfolio_etfs WHERE portfolio_id = ? AND symbol = ?', (portfolio_id, symbol))
        
        # 更新组合的更新时间
        cursor.execute('UPDATE portfolios SET updated_at = ? WHERE id = ?', (now, portfolio_id))
        
        conn.commit()
        conn.close()
        return True

    @staticmethod
    def get_portfolio_etfs(portfolio_id):
        """获取投资组合中的ETF列表"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 首先获取投资组合信息以获取user_id
        cursor.execute('SELECT user_id FROM portfolios WHERE id = ?', (portfolio_id,))
        portfolio = cursor.fetchone()
        
        if not portfolio:
            conn.close()
            return []
            
        user_id = portfolio['user_id']
        
        # 查询投资组合中的ETF，使用LEFT JOIN以支持自定义ETF
        cursor.execute('''
        SELECT pe.*, e.name as etf_name, e.category, e.volatility_type, e.is_official,
               CASE WHEN e.name IS NULL THEN 
                   (SELECT c.name FROM custom_etfs c WHERE c.symbol = pe.symbol AND c.user_id = ?)
               ELSE e.name END as name
        FROM portfolio_etfs pe
        LEFT JOIN etf_list e ON pe.symbol = e.symbol
        WHERE pe.portfolio_id = ?
        ORDER BY pe.weight DESC
        ''', (user_id, portfolio_id))
        
        etfs = cursor.fetchall()
        
        # 转换为字典列表
        etf_list = []
        for etf in etfs:
            etf_dict = dict(etf)
            etf_list.append(etf_dict)
        
        conn.close()
        return etf_list

class FavoriteETF:
    """用户自选ETF模型"""
    
    @staticmethod
    def add(user_id, symbol):
        """添加自选ETF"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            cursor.execute(
                'INSERT INTO favorite_etfs (user_id, symbol, added_at) VALUES (?, ?, ?)',
                (user_id, symbol, now)
            )
            conn.commit()
            success = True
        except sqlite3.IntegrityError:
            # ETF已经在自选中
            success = False
        
        conn.close()
        return success
    
    @staticmethod
    def remove(user_id, symbol):
        """移除自选ETF"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM favorite_etfs WHERE user_id = ? AND symbol = ?', (user_id, symbol))
        
        conn.commit()
        conn.close()
        return cursor.rowcount > 0
    
    @staticmethod
    def get_user_favorites(user_id):
        """获取用户所有自选ETF"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM favorite_etfs WHERE user_id = ? ORDER BY added_at DESC', (user_id,))
        favorites = cursor.fetchall()
        
        conn.close()
        return [dict(fav) for fav in favorites]

class CustomETF:
    """用户自定义ETF模型"""
    
    @staticmethod
    def add(user_id, symbol, name, description=""):
        """添加自定义ETF"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            cursor.execute(
                'INSERT INTO custom_etfs (user_id, symbol, name, description, added_at) VALUES (?, ?, ?, ?, ?)',
                (user_id, symbol, name, description, now)
            )
            conn.commit()
            success = True
        except sqlite3.IntegrityError:
            # ETF已经存在
            success = False
        
        conn.close()
        return success
    
    @staticmethod
    def remove(user_id, symbol):
        """移除自定义ETF"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM custom_etfs WHERE user_id = ? AND symbol = ?', (user_id, symbol))
        
        conn.commit()
        conn.close()
        return cursor.rowcount > 0
    
    @staticmethod
    def get_user_custom_etfs(user_id):
        """获取用户所有自定义ETF，包括名称信息"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT c.*, IFNULL(e.name, c.name) as display_name 
            FROM custom_etfs c
            LEFT JOIN etf_list e ON c.symbol = e.symbol
            WHERE c.user_id = ? 
            ORDER BY c.added_at DESC
        ''', (user_id,))
        custom_etfs = cursor.fetchall()
        
        conn.close()
        return [dict(etf) for etf in custom_etfs]
    
    @staticmethod
    def get_custom_etf(user_id, symbol):
        """获取特定自定义ETF，包括真实名称"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT c.*, IFNULL(e.name, c.name) as display_name 
            FROM custom_etfs c
            LEFT JOIN etf_list e ON c.symbol = e.symbol
            WHERE c.user_id = ? AND c.symbol = ?
        ''', (user_id, symbol))
        etf = cursor.fetchone()
        
        conn.close()
        return dict(etf) if etf else None
    
    @staticmethod
    def get_all_custom_etfs():
        """获取所有用户的自定义ETF"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT c.*, u.username 
            FROM custom_etfs c
            JOIN users u ON c.user_id = u.id
            ORDER BY c.added_at DESC
        ''')
        custom_etfs = cursor.fetchall()
        
        conn.close()
        return [dict(etf) for etf in custom_etfs]

class UserSetting:
    """用户设置模型"""
    
    @staticmethod
    def get(user_id, key=None):
        """获取用户设置"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if key:
            cursor.execute('SELECT * FROM user_settings WHERE user_id = ? AND setting_key = ?', (user_id, key))
            setting = cursor.fetchone()
            conn.close()
            return dict(setting) if setting else None
        else:
            cursor.execute('SELECT * FROM user_settings WHERE user_id = ?', (user_id,))
            settings = cursor.fetchall()
            conn.close()
            
            # 转换为字典格式 {key: value}
            settings_dict = {}
            for setting in settings:
                settings_dict[setting['setting_key']] = setting['setting_value']
            
            return settings_dict
    
    @staticmethod
    def set(user_id, key, value):
        """设置用户设置"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 检查设置是否已存在
        cursor.execute('SELECT * FROM user_settings WHERE user_id = ? AND setting_key = ?', (user_id, key))
        if cursor.fetchone():
            # 更新设置
            cursor.execute(
                'UPDATE user_settings SET setting_value = ?, updated_at = ? WHERE user_id = ? AND setting_key = ?',
                (value, now, user_id, key)
            )
        else:
            # 添加新设置
            cursor.execute(
                'INSERT INTO user_settings (user_id, setting_key, setting_value, updated_at) VALUES (?, ?, ?, ?)',
                (user_id, key, value, now)
            )
        
        conn.commit()
        conn.close()
        return True 