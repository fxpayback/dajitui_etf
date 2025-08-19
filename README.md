# ETF网格交易助手

一个基于Flask的ETF网格交易分析工具，帮助投资者进行ETF网格交易策略分析和回测。

## 🚀 功能特性

### 核心功能
- **ETF数据分析**: 实时获取ETF历史数据，计算波动率和技术指标
- **网格交易策略**: 自动计算网格间隔、层数和仓位建议
- **投资组合管理**: 创建和管理多个投资组合，支持权重分配
- **回测系统**: 完整的网格交易策略回测功能
- **用户系统**: 完整的用户注册、登录和权限管理

### 数据功能
- **实时数据获取**: 基于akshare获取最新ETF数据
- **历史数据分析**: 支持最长5年的历史数据分析
- **波动率计算**: 基于200日移动窗口计算波动率
- **网格参数计算**: 智能计算网格间隔和交易层数

### 管理功能
- **ETF管理**: 管理员可以添加、编辑、删除ETF产品
- **用户管理**: 完整的用户管理系统
- **数据管理**: 支持ETF数据的清理和重置

## 📋 系统要求

- Python 3.8+
- SQLite 3
- 现代浏览器支持

## 🛠️ 安装部署

### 1. 克隆项目
```bash
git clone <repository-url>
cd 网格交易模型
```

### 2. 安装依赖
```bash
pip install flask pandas numpy akshare quantstats werkzeug requests
```

### 3. 初始化数据库
```bash
python init_db.py
```

### 4. 创建管理员账户
```bash
python create_admin.py
```

### 5. 运行应用

#### 开发环境
```bash
python app.py
```

#### 生产环境 (使用Gunicorn)
```bash
gunicorn -c gunicorn_conf.py app:app
```

应用将在 `http://localhost:5000` (开发环境) 或 `http://localhost:8001` (生产环境) 启动。

## 📁 项目结构

```
网格交易模型/
├── app.py                      # 主应用文件
├── models/                     # 数据模型
│   ├── auth.py                # 身份验证模块
│   ├── etf_admin.py           # ETF管理模块
│   ├── etf_data.py            # ETF数据处理模块
│   └── user.py                # 用户模型
├── templates/                  # HTML模板
│   ├── base.html              # 基础模板
│   ├── index.html             # 首页
│   ├── dashboard.html         # 数据仪表盘
│   ├── portfolio.html         # 投资组合
│   └── ...                    # 其他页面模板
├── static/                     # 静态文件
│   ├── favicon.ico
│   └── images/
├── database/                   # 数据库文件
│   └── etf_history.db
├── logs/                       # 日志文件
├── gunicorn_conf.py           # Gunicorn配置
├── uwsgi.ini                  # uWSGI配置
└── README.md                  # 项目说明
```

## 🎯 使用指南

### 基础使用

1. **注册账户**: 访问 `/register` 创建新账户
2. **登录系统**: 使用账户登录系统
3. **选择ETF**: 在首页选择要分析的ETF产品
4. **查看分析**: 在仪表盘查看波动率、网格参数等分析结果
5. **创建组合**: 在"我的投资组合"中创建自定义投资组合
6. **回测策略**: 使用回测功能验证网格交易策略

### 管理员功能

管理员可以通过 `/admin` 访问管理界面：
- 管理ETF产品列表
- 管理用户账户
- 查看系统统计信息
- 清理和维护数据

## 🔧 配置说明

### 环境变量
- `SECRET_KEY`: Flask应用密钥，生产环境请设置为随机字符串

### 数据库配置
项目使用SQLite数据库，数据库文件位于 `database/etf_history.db`

### 日志配置
- 错误日志: `logs/error.log`
- 日志轮转: 最大10MB，保留10个备份文件

## 📊 核心算法

### 波动率计算
使用200日移动窗口计算年化波动率：
```python
volatility = df['close'].pct_change().rolling(window=200).std() * np.sqrt(252)
```

### 网格间隔计算
基于波动率计算合理的网格间隔：
```python
grid_spacing = volatility * adjustment_factor
```

### 仓位建议
根据当前价格在网格中的位置计算建议仓位。

## 🚀 部署建议

### 生产环境部署

1. **使用Gunicorn**: 推荐使用Gunicorn作为WSGI服务器
2. **反向代理**: 配置Nginx作为反向代理
3. **SSL证书**: 配置HTTPS加密
4. **数据备份**: 定期备份SQLite数据库
5. **监控日志**: 监控应用日志和错误信息

### 性能优化

- 配置适当的worker进程数
- 启用数据库连接池
- 配置静态文件缓存
- 使用CDN加速静态资源

## 🤝 贡献指南

欢迎提交Issue和Pull Request来改进项目！

### 开发流程
1. Fork项目
2. 创建功能分支
3. 提交更改
4. 创建Pull Request

### 代码规范
- 遵循PEP 8代码风格
- 添加适当的注释和文档
- 编写单元测试

## 📄 许可证

本项目采用MIT许可证，详见LICENSE文件。

## ⚠️ 免责声明

本工具仅供学习和研究使用，不构成投资建议。投资有风险，请谨慎决策。

## 📞 联系方式

如有问题或建议，请通过以下方式联系：
- 提交Issue
- 发送邮件
- VX：Code_Mvp

## 🔄 版本历史

### v0.9 (当前版本)
- 完整的用户系统
- ETF数据分析功能
- 投资组合管理
- 网格交易回测
- 管理员后台

---

**感谢使用ETF网格交易助手！** 🎉
