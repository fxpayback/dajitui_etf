import sqlite3
import sys

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect('database/etf_history.db', timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def list_users():
    """列出所有用户"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 检查是否有is_admin列
        cursor.execute("PRAGMA table_info(users)")
        columns = cursor.fetchall()
        has_is_admin = any(col['name'] == 'is_admin' for col in columns)
        print(f"列信息: {[dict(col) for col in columns]}")
        print(f"是否有is_admin列: {has_is_admin}")
        
        # 根据是否有is_admin列执行不同的查询
        if has_is_admin:
            cursor.execute('SELECT id, username, email, is_admin FROM users ORDER BY id')
        else:
            cursor.execute('SELECT id, username, email FROM users ORDER BY id')
        
        users = cursor.fetchall()
        print(f"查询到 {len(users)} 个用户")
        
        conn.close()
        
        if not users:
            print("当前没有任何用户。")
            return
        
        print("\n当前用户列表:")
        print("-" * 70)
        print(f"{'ID':<5} {'用户名':<20} {'邮箱':<30} {'管理员':<5}")
        print("-" * 70)
        
        for user in users:
            user_dict = dict(user)
            is_admin = user_dict.get('is_admin', 0)
            print(f"{user_dict['id']:<5} {user_dict['username']:<20} {user_dict['email']:<30} {'是' if is_admin else '否':<5}")
        
        print("-" * 70)
    except Exception as e:
        print(f"列出用户时发生错误: {str(e)}")

def make_admin(user_id):
    """将指定用户升级为管理员"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 检查是否存在is_admin列
    cursor.execute("PRAGMA table_info(users)")
    columns = cursor.fetchall()
    has_is_admin = any(col['name'] == 'is_admin' for col in columns)
    
    # 如果is_admin列不存在，添加该列
    if not has_is_admin:
        print("添加is_admin列到users表...")
        cursor.execute('ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0')
        conn.commit()
    
    # 检查用户是否存在
    cursor.execute('SELECT id FROM users WHERE id = ?', (user_id,))
    user = cursor.fetchone()
    
    if not user:
        conn.close()
        print(f"错误: 用户ID {user_id} 不存在。")
        return False
    
    # 更新用户为管理员
    cursor.execute('UPDATE users SET is_admin = 1 WHERE id = ?', (user_id,))
    conn.commit()
    conn.close()
    
    print(f"用户ID {user_id} 已被成功设置为管理员。")
    return True

def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python make_admin.py list              # 列出所有用户")
        print("  python make_admin.py make <user_id>    # 将指定ID的用户设置为管理员")
        return
    
    command = sys.argv[1]
    
    if command == "list":
        list_users()
    elif command == "make" and len(sys.argv) >= 3:
        try:
            user_id = int(sys.argv[2])
            make_admin(user_id)
        except ValueError:
            print("错误: 用户ID必须是一个整数。")
    else:
        print("无效的命令。使用 'list' 列出用户或 'make <user_id>' 将用户设置为管理员。")

if __name__ == "__main__":
    main() 