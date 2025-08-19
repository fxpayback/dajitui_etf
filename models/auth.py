from flask import session, redirect, url_for, request, flash, jsonify
import functools
import secrets
import time
from .user import User

def login_required(view):
    """登录验证装饰器"""
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login', next=request.url))
        return view(**kwargs)
    return wrapped_view

def login_user(user_id):
    """将用户登录状态保存到会话"""
    session.clear()
    session['user_id'] = user_id
    session['login_time'] = int(time.time())
    # 生成CSRF令牌
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(16)
    return True

def logout_user():
    """清除用户登录状态"""
    session.clear()
    return True

def get_current_user():
    """获取当前登录用户信息"""
    if 'user_id' in session:
        user = User.get_by_id(session['user_id'])
        # 确保有is_admin属性
        if user and not hasattr(user, 'is_admin'):
            # 如果返回的是字典
            if isinstance(user, dict):
                user['is_admin'] = user.get('is_admin', 0)
            else:
                # 如果是自定义对象但没有is_admin属性
                setattr(user, 'is_admin', 0)
        return user
    return None

def check_csrf_token(view):
    """CSRF令牌验证装饰器"""
    @functools.wraps(view)
    def wrapped_view(*args, **kwargs):
        if request.method in ['POST', 'PUT', 'DELETE', 'PATCH']:
            # 从请求中获取CSRF令牌
            token = None
            
            # 从JSON数据中获取
            if request.is_json and request.get_json():
                token = request.get_json().get('csrf_token')
                
            # 从表单数据中获取
            if not token and request.form:
                token = request.form.get('csrf_token')
                
            # 从请求头中获取
            if not token:
                token = request.headers.get('X-CSRF-Token')
                
            # 验证令牌
            if not token or token != session.get('csrf_token'):
                flash('CSRF验证失败，请刷新页面重试', 'error')
                if request.is_json:
                    return jsonify({'error': 'CSRF验证失败'}), 403
                return redirect(request.referrer or url_for('dashboard'))
                
        return view(*args, **kwargs)
    return wrapped_view 

def get_user_id(user):
    """从用户对象获取ID，兼容不同的对象类型"""
    if user is None:
        return None
        
    # 如果是字典类型
    if isinstance(user, dict):
        return user.get('id')
    
    # 如果是sqlite3.Row对象
    if hasattr(user, 'keys') and 'id' in user.keys():
        return user['id']
    
    # 如果是自定义对象
    if hasattr(user, 'id'):
        return user.id
    
    # 尝试通过getattr获取
    return getattr(user, 'id', None) 