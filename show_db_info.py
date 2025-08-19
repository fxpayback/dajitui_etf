import sqlite3
import os

def get_db_path():
    return 'database/etf_history.db'

def show_database_info():
    db_path = get_db_path()
    
    # 检查数据库文件是否存在
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件 '{db_path}' 不存在")
        return
    
    # 连接数据库
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 获取所有表名
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()
    
    if not tables:
        print("数据库中没有表")
        conn.close()
        return
    
    print(f"数据库路径: {db_path}")
    print(f"数据库中有 {len(tables)} 个表\n")
    
    # 遍历每个表
    for table_idx, table in enumerate(tables, 1):
        table_name = table[0]
        print(f"表 {table_idx}/{len(tables)}: {table_name}")
        print("=" * 80)
        
        # 获取表结构
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        print(f"表结构:")
        print("-" * 80)
        print(f"{'ID':<3} | {'列名':<20} | {'类型':<12} | {'可空':<4} | {'默认值':<15} | {'主键'}")
        print("-" * 80)
        
        for col in columns:
            print(f"{col['cid']:<3} | {col['name']:<20} | {col['type']:<12} | {'否' if col['notnull'] else '是':<4} | "
                  f"{str(col['dflt_value'] or ''):<15} | {'是' if col['pk'] else '否'}")
        
        # 获取记录数
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        print(f"\n记录数: {count}")
        
        # 获取表内容（最多显示前10条）
        if count > 0:
            try:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
                rows = cursor.fetchall()
                
                print("\n表内容预览(前10条):")
                print("-" * 120)
                
                # 列名
                headers = [col['name'] for col in columns]
                print(" | ".join(f"{header:<15}" for header in headers))
                print("-" * 120)
                
                # 数据行
                for row in rows:
                    row_data = []
                    for header in headers:
                        value = row[header]
                        # 处理长文本
                        if isinstance(value, str) and len(value) > 15:
                            value = value[:12] + "..."
                        row_data.append(f"{str(value or ''):<15}")
                    print(" | ".join(row_data))
                
                if count > 10:
                    print(f"\n... 只显示了前10条记录，共有{count}条记录")
            except Exception as e:
                print(f"无法获取表内容: {str(e)}")
        
        print("\n" + "=" * 80 + "\n")
    
    conn.close()

def show_specific_table(table_name):
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 检查表是否存在
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cursor.fetchone():
        print(f"表 '{table_name}' 不存在")
        conn.close()
        return
        
    # 获取表结构
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    
    print(f"表 '{table_name}' 结构:")
    print("-" * 80)
    print(f"{'ID':<3} | {'列名':<20} | {'类型':<12} | {'可空':<4} | {'默认值':<15} | {'主键'}")
    print("-" * 80)
    
    for col in columns:
        print(f"{col['cid']:<3} | {col['name']:<20} | {col['type']:<12} | {'否' if col['notnull'] else '是':<4} | "
              f"{str(col['dflt_value'] or ''):<15} | {'是' if col['pk'] else '否'}")
    
    # 获取所有记录
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    
    print(f"\n共有 {len(rows)} 条记录")
    
    if rows:
        print("\n表内容:")
        print("-" * 120)
        
        # 列名
        headers = [col['name'] for col in columns]
        print(" | ".join(f"{header:<15}" for header in headers))
        print("-" * 120)
        
        # 数据行
        for row in rows:
            row_data = []
            for header in headers:
                value = row[header]
                # 处理长文本
                if isinstance(value, str) and len(value) > 15:
                    value = value[:12] + "..."
                row_data.append(f"{str(value or ''):<15}")
            print(" | ".join(row_data))
    
    conn.close()


# 使用示例
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        show_specific_table(sys.argv[1])
    else:
        show_database_info()