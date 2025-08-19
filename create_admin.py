from models.user import User
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_admin_user(username, email, password):
    """创建一个管理员用户"""
    try:
        success, result = User.create(username, email, password, is_admin=True)
        if success:
            print(f"✅ 管理员用户创建成功! ID: {result}")
            return True
        else:
            print(f"❌ 管理员用户创建失败: {result}")
            return False
    except Exception as e:
        logger.error(f"创建管理员用户失败: {str(e)}")
        print(f"❌ 创建管理员用户出错: {str(e)}")
        return False

if __name__ == "__main__":
    print("正在创建管理员用户...")
    
    # 设置管理员账户信息
    admin_username = "admin"
    admin_email = "admin@example.com"
    admin_password = "admin123"  # 在实际环境中使用更强的密码
    
    create_admin_user(admin_username, admin_email, admin_password)
    
    print("完成! 请使用以下信息登录:")
    print(f"用户名: {admin_username}")
    print(f"密码: {admin_password}") 