"""
“一中心一基地”信息管理系统 - Streamlit版本

使用说明：
1. 安装依赖：pip install streamlit pandas openpyxl plotly reportlab
2. 运行应用：streamlit run enterprise_support_streamlit.py
"""

import streamlit as st
import pandas as pd
import sqlite3
import json
import hashlib
from datetime import datetime
import os
import io
import plotly.express as px
from plotly.subplots import make_subplots

# 设置页面配置（必须在所有Streamlit命令之前）
st.set_page_config(
    page_title="“一中心一基地”信息管理系统 ",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS样式
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #2c3e50;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .sub-header {
        font-size: 1.8rem;
        color: #3498db;
        margin-top: 1.5rem;
        font-weight: bold;
        border-bottom: 2px solid #3498db;
        padding-bottom: 0.5rem;
    }
    .card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        margin-bottom: 20px;
        border-left: 5px solid #3498db;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stat-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 10px;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .permission-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.8rem;
        margin: 2px;
    }

    .permission-allowed {
        background-color: #d4edda;
        color: #155724;
        border: 1px solid #c3e6cb;
    }

    .permission-denied {
        background-color: #f8d7da;
        color: #721c24;
        border: 1px solid #f5c6cb;
    }

    .role-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 15px;
        font-size: 0.9rem;
        font-weight: bold;
        margin: 2px;
    }

    .role-admin {
        background-color: #007bff;
        color: white;
    }

    .role-manager {
        background-color: #28a745;
        color: white;
    }

    .role-user {
        background-color: #6c757d;
        color: white;
    }

    .role-viewer {
        background-color: #17a2b8;
        color: white;
    }

    .user-status-active {
        color: #28a745;
        font-weight: bold;
    }

    .user-status-inactive {
        color: #dc3545;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)


class EnterpriseSupportSystem:
    """“一中心一基地”信息管理系统 核心类"""

    # 权限常量定义
    PERMISSIONS = {
        # 仪表盘权限
        "view_dashboard": "查看仪表盘",

        # 数据管理权限
        "view_data": "查看数据",
        "add_data": "添加数据",
        "edit_data": "编辑数据",
        "delete_data": "删除数据",
        "import_data": "导入数据",
        "export_data": "导出数据",

        # 表单管理权限
        "view_forms": "查看表单",
        "create_form": "创建表单",
        "edit_form": "编辑表单",
        "delete_form": "删除表单",

        # 统计分析权限
        "view_reports": "查看报表",
        "create_report": "创建报表",
        "export_report": "导出报表",

        # 系统管理权限
        "manage_users": "管理用户",
        "manage_roles": "管理角色",
        "view_logs": "查看日志",
        "system_settings": "系统设置",

        # 特殊权限
        "all": "所有权限"
    }

    # 角色默认权限配置
    ROLE_PERMISSIONS = {
        "admin": ["all"],
        "manager": ["view_dashboard", "view_data", "add_data", "edit_data",
                    "export_data", "view_forms", "view_reports", "export_report"],
        "user": ["view_dashboard", "view_data", "add_data", "edit_own_data"],
        "viewer": ["view_dashboard", "view_data", "view_forms", "view_reports"]
    }

    def __init__(self):
        self.db_path = "enterprise_support.db"
        self.init_database()

    def init_database(self):
        """初始化数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 创建增强的用户表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                full_name TEXT,
                email TEXT,
                phone TEXT,
                department TEXT,
                role TEXT DEFAULT 'user',
                permissions TEXT DEFAULT '[]',
                is_active INTEGER DEFAULT 1,
                last_login TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 创建角色权限表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS role_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                role_name TEXT UNIQUE NOT NULL,
                permissions TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 创建登录日志表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS login_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ip_address TEXT,
                user_agent TEXT,
                status TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')

        # 创建操作日志表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS operation_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                operation TEXT NOT NULL,
                target_type TEXT,
                target_id INTEGER,
                details TEXT,
                ip_address TEXT,
                operation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')

        # 创建表单定义表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS form_definitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                form_name TEXT NOT NULL,
                form_config TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 创建数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS form_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                form_id INTEGER NOT NULL,
                data_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (form_id) REFERENCES form_definitions(id)
            )
        ''')

        # 检查是否已存在管理员账户
        cursor.execute("SELECT COUNT(*) FROM users WHERE username='admin'")
        if cursor.fetchone()[0] == 0:
            hashed_password = hashlib.sha256("admin123".encode()).hexdigest()
            cursor.execute('''
                INSERT INTO users (username, password, full_name, email, role, permissions) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ("admin", hashed_password, "系统管理员", "admin@example.com", "admin",
                  json.dumps(["all"], ensure_ascii=False)))

            # 初始化默认角色权限
            default_roles = [
                ("admin", "系统管理员", json.dumps(["all"], ensure_ascii=False)),
                ("manager", "部门经理", json.dumps([
                    "view_dashboard", "manage_data", "view_reports", "export_data"
                ], ensure_ascii=False)),
                ("user", "普通用户", json.dumps([
                    "view_dashboard", "view_data", "add_data", "edit_own_data"
                ], ensure_ascii=False)),
                ("viewer", "只读用户", json.dumps([
                    "view_dashboard", "view_data"
                ], ensure_ascii=False))
            ]

            for role_name, description, permissions in default_roles:
                cursor.execute('''
                    INSERT OR IGNORE INTO role_permissions (role_name, description, permissions) 
                    VALUES (?, ?, ?)
                ''', (role_name, description, permissions))

        # 检查示例表单是否存在
        cursor.execute("SELECT COUNT(*) FROM form_definitions WHERE form_name='帮扶总台账'")
        if cursor.fetchone()[0] == 0:
            # 创建示例表单
            example_form_config = {
                "form_name": "帮扶总台账",
                "fields": [
                    {"name": "企业名称", "type": "text", "required": True},
                    {"name": "投资评级", "type": "text", "required": True},
                    {"name": "打款金额", "type": "number", "required": True},
                    {"name": "基本情况", "type": "textarea", "required": True},
                    {"name": "联系人", "type": "text", "required": True},
                    {"name": "联系电话", "type": "text", "required": True},
                    {"name": "地址", "type": "text", "required": True},
                    {"name": "主要诉求", "type": "textarea", "required": True},
                    {"name": "诉求分类", "type": "text", "required": True},
                    {"name": "帮扶分类", "type": "text", "required": True},
                    {"name": "责任单位", "type": "text", "required": True}
                ]
            }
            cursor.execute("INSERT INTO form_definitions (form_name, form_config) VALUES (?, ?)",
                           ("帮扶总台账", json.dumps(example_form_config, ensure_ascii=False)))

            form_id = cursor.lastrowid

            # 创建示例数据
            example_data = [
                {
                    "企业名称": "湖南尔玺文化传播有限公司",
                    "投资评级": "B",
                    "打款金额": 30,
                    "基本情况": "湖南师范大学毕业学生创业公司，团队以'新艺术消费领导者'的理念...",
                    "联系人": "王升",
                    "联系电话": "15110302868",
                    "地址": "湖南省长沙岳麓区白云路793号",
                    "主要诉求": "希望建立与湖南省演艺集团、湖南省博物馆等文创需求单位建立沟通联系渠道",
                    "诉求分类": "市场拓展",
                    "帮扶分类": "Ⅱ级企业",
                    "责任单位": "宣传工作部"
                },
                {
                    "企业名称": "湖南慧眼云端医疗科技有限公司",
                    "投资评级": "B",
                    "打款金额": 50,
                    "基本情况": "中南大学湘雅医学院学生创业项目。公司聚焦病理数字化和智能诊断服务...",
                    "联系人": "白冰倩",
                    "联系电话": "15211194305",
                    "地址": "大学科技园创业大厦A448卡座",
                    "主要诉求": "公司处于研发阶段，设备费用高，需要更多资金支持",
                    "诉求分类": "融资需求",
                    "帮扶分类": "Ⅱ级企业",
                    "责任单位": "湘江新区国有资本投资有限公司"
                },
                {
                    "企业名称": "长沙光翼泽兴科技有限公司",
                    "投资评级": "B",
                    "打款金额": 50,
                    "基本情况": "北京大学在校大学生创业项目。项目团队由北京大学、南开大学、浙江大学硕博研究生共创...",
                    "联系人": "曹天兴",
                    "联系电话": "18904413653",
                    "地址": "大学科技园创业大厦A479卡座",
                    "主要诉求": "协调科技厅、工信厅等部门提供项目申报的专项指导",
                    "诉求分类": "政策支持",
                    "帮扶分类": "Ⅱ级企业",
                    "责任单位": "科技创新和产业促进局"
                }
            ]

            for data in example_data:
                cursor.execute("INSERT INTO form_data (form_id, data_json) VALUES (?, ?)",
                               (form_id, json.dumps(data, ensure_ascii=False)))

        conn.commit()
        conn.close()

    def login(self, username, password, ip_address=None, user_agent=None):
        """用户登录"""
        hashed_password = hashlib.sha256(password.encode()).hexdigest()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # 首先检查表结构
            cursor.execute("PRAGMA table_info(users)")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]

            # 构建查询语句，动态检查列是否存在
            if 'is_active' in column_names:
                # 如果表中有is_active列，使用原来的查询
                cursor.execute('''
                    SELECT id, username, role, is_active 
                    FROM users 
                    WHERE username=? AND password=?
                ''', (username, hashed_password))
            else:
                # 如果表中没有is_active列，使用简化的查询
                cursor.execute('''
                    SELECT id, username, role 
                    FROM users 
                    WHERE username=? AND password=?
                ''', (username, hashed_password))

            user = cursor.fetchone()
            conn.close()

            if user:
                if 'is_active' in column_names and len(user) == 4:
                    user_id, username, role, is_active = user
                    if not is_active:
                        # 记录失败的登录尝试
                        self.log_login(None, username, "failed_account_inactive", ip_address, user_agent)
                        return None
                else:
                    user_id, username, role = user
                    is_active = 1  # 默认为激活状态

                # 记录成功登录
                self.log_login(user_id, username, "success", ip_address, user_agent)

                return {"id": user_id, "username": username, "role": role}
            else:
                # 记录失败的登录尝试
                self.log_login(None, username, "failed_wrong_credentials", ip_address, user_agent)
                return None
        except Exception as e:
            conn.close()
            print(f"登录错误: {e}")
            return None

    # ... 保持其他方法不变 ...
    def has_permission(self, user_id, permission):
        """检查用户是否拥有特定权限"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
               SELECT u.role, u.permissions, rp.permissions as role_perms
               FROM users u
               LEFT JOIN role_permissions rp ON u.role = rp.role_name
               WHERE u.id = ?
           ''', (user_id,))

        result = cursor.fetchone()
        conn.close()

        if not result:
            return False

        role, user_perms_json, role_perms_json = result

        # 检查用户特定权限
        if user_perms_json:
            try:
                user_perms = json.loads(user_perms_json)
                if "all" in user_perms or permission in user_perms:
                    return True
            except:
                pass

        # 检查角色权限
        if role_perms_json:
            try:
                role_perms = json.loads(role_perms_json)
                if "all" in role_perms or permission in role_perms:
                    return True
            except:
                pass

        # 特殊权限检查
        if role == "admin":
            return True

        return False

    def get_user_permissions(self, user_id):
        """获取用户所有权限"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
               SELECT u.role, u.permissions, rp.permissions as role_perms
               FROM users u
               LEFT JOIN role_permissions rp ON u.role = rp.role_name
               WHERE u.id = ?
           ''', (user_id,))

        result = cursor.fetchone()
        conn.close()

        all_permissions = set()

        if result:
            role, user_perms_json, role_perms_json = result

            # 添加用户特定权限
            if user_perms_json:
                try:
                    user_perms = json.loads(user_perms_json)
                    all_permissions.update(user_perms)
                except:
                    pass

            # 添加角色权限
            if role_perms_json:
                try:
                    role_perms = json.loads(role_perms_json)
                    all_permissions.update(role_perms)
                except:
                    pass

            # 管理员拥有所有权限
            if role == "admin":
                all_permissions.add("all")

        return list(all_permissions)

    def log_operation(self, user_id, username, operation, target_type=None, target_id=None, details=None,
                      ip_address=None):
        """记录用户操作日志"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
               INSERT INTO operation_logs 
               (user_id, username, operation, target_type, target_id, details, ip_address)
               VALUES (?, ?, ?, ?, ?, ?, ?)
           ''', (user_id, username, operation, target_type, target_id,
                 json.dumps(details, ensure_ascii=False) if details else None, ip_address))

        conn.commit()
        conn.close()

    def log_login(self, user_id, username, status="success", ip_address=None, user_agent=None):
        """记录用户登录日志"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
               INSERT INTO login_logs 
               (user_id, username, status, ip_address, user_agent)
               VALUES (?, ?, ?, ?, ?)
           ''', (user_id, username, status, ip_address, user_agent))

        # 更新用户最后登录时间
        cursor.execute('''
               UPDATE users SET last_login = CURRENT_TIMESTAMP 
               WHERE id = ?
           ''', (user_id,))

        conn.commit()
        conn.close()

    def get_all_users(self):
        """获取所有用户"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
               SELECT id, username, full_name, email, phone, department, 
                      role, is_active, last_login, created_at
               FROM users
               ORDER BY created_at DESC
           ''')

        users = cursor.fetchall()
        conn.close()

        return users

    def get_user_by_id(self, user_id):
        """根据ID获取用户"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
               SELECT id, username, full_name, email, phone, department, 
                      role, permissions, is_active, last_login, created_at
               FROM users
               WHERE id = ?
           ''', (user_id,))

        user = cursor.fetchone()
        conn.close()

        return user

    def create_user(self, username, password, full_name="", email="", phone="",
                    department="", role="user", permissions=None, is_active=True):
        """创建新用户"""
        hashed_password = hashlib.sha256(password.encode()).hexdigest()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # 首先检查表结构
            cursor.execute("PRAGMA table_info(users)")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]

            if 'is_active' in column_names:
                cursor.execute('''
                    INSERT INTO users 
                    (username, password, full_name, email, phone, department, 
                     role, permissions, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (username, hashed_password, full_name, email, phone,
                      department, role,
                      json.dumps(permissions or [], ensure_ascii=False),
                      1 if is_active else 0))
            else:
                cursor.execute('''
                    INSERT INTO users 
                    (username, password, full_name, email, phone, department, 
                     role, permissions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (username, hashed_password, full_name, email, phone,
                      department, role,
                      json.dumps(permissions or [], ensure_ascii=False)))

            user_id = cursor.lastrowid
            conn.commit()

            # 记录操作日志
            self.log_operation(
                user_id=None,  # 系统操作
                username="system",
                operation="create_user",
                target_type="user",
                target_id=user_id,
                details={"username": username, "role": role}
            )

            return user_id
        except sqlite3.IntegrityError:
            raise ValueError("用户名已存在")
        finally:
            conn.close()

    def update_user(self, user_id, **kwargs):
        """更新用户信息"""
        # 首先检查表结构
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(users)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        conn.close()

        allowed_fields = ['full_name', 'email', 'phone', 'department',
                          'role', 'permissions']

        # 如果表中有is_active列，添加到允许的字段
        if 'is_active' in column_names:
            allowed_fields.append('is_active')

        update_fields = []
        update_values = []

        for field, value in kwargs.items():
            if field in allowed_fields:
                if field == 'permissions':
                    value = json.dumps(value, ensure_ascii=False)
                elif field == 'is_active':
                    value = 1 if value else 0

                update_fields.append(f"{field} = ?")
                update_values.append(value)

        if not update_fields:
            return

        update_values.append(user_id)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        sql = f'''
               UPDATE users 
               SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
               WHERE id = ?
           '''

        cursor.execute(sql, update_values)
        conn.commit()

        # 记录操作日志
        self.log_operation(
            user_id=st.session_state.get('current_user', {}).get('id'),
            username=st.session_state.get('current_user', {}).get('username'),
            operation="update_user",
            target_type="user",
            target_id=user_id,
            details=kwargs
        )

        conn.close()

    def delete_user(self, user_id):
        """删除用户"""
        # 不能删除自己
        current_user_id = st.session_state.get('current_user', {}).get('id')
        if current_user_id and user_id == current_user_id:
            raise ValueError("不能删除当前登录的用户")

        # 检查是否还有管理员用户
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM users WHERE role = ? AND id != ?', ("admin", user_id))
        admin_count = cursor.fetchone()[0]

        if admin_count == 0:
            conn.close()
            raise ValueError("删除此用户后系统将没有管理员，请先创建另一个管理员账户")

        # 删除用户
        cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
        conn.commit()
        conn.close()

        # 记录操作日志
        self.log_operation(
            user_id=current_user_id,
            username=st.session_state.get('current_user', {}).get('username'),
            operation="delete_user",
            target_type="user",
            target_id=user_id,
            details={"user_id": user_id}
        )

    def get_all_roles(self):
        """获取所有角色"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
               SELECT role_name, description, permissions
               FROM role_permissions
               ORDER BY role_name
           ''')

        roles = cursor.fetchall()
        conn.close()

        return roles

    def update_role_permissions(self, role_name, permissions, description=None):
        """更新角色权限"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if description:
            cursor.execute('''
                   UPDATE role_permissions 
                   SET permissions = ?, description = ?
                   WHERE role_name = ?
               ''', (json.dumps(permissions, ensure_ascii=False), description, role_name))
        else:
            cursor.execute('''
                   UPDATE role_permissions 
                   SET permissions = ?
                   WHERE role_name = ?
               ''', (json.dumps(permissions, ensure_ascii=False), role_name))

        conn.commit()
        conn.close()

        # 记录操作日志
        self.log_operation(
            user_id=st.session_state.get('current_user', {}).get('id'),
            username=st.session_state.get('current_user', {}).get('username'),
            operation="update_role",
            target_type="role",
            target_id=None,
            details={"role_name": role_name, "permissions": permissions}
        )

    def get_operation_logs(self, limit=100, user_id=None, operation=None):
        """获取操作日志"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = '''
               SELECT ol.id, ol.user_id, ol.username, ol.operation, 
                      ol.target_type, ol.target_id, ol.details, 
                      ol.ip_address, ol.operation_time,
                      u.full_name
               FROM operation_logs ol
               LEFT JOIN users u ON ol.user_id = u.id
           '''

        params = []
        conditions = []

        if user_id:
            conditions.append("ol.user_id = ?")
            params.append(user_id)

        if operation:
            conditions.append("ol.operation = ?")
            params.append(operation)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY ol.operation_time DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        logs = cursor.fetchall()
        conn.close()

        return logs

    def get_forms(self):
        """获取所有表单"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, form_name FROM form_definitions")
        forms = cursor.fetchall()
        conn.close()
        return forms

    def get_form_data(self, form_id):
        """获取表单数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 获取表单定义
        cursor.execute("SELECT form_config FROM form_definitions WHERE id=?", (form_id,))
        result = cursor.fetchone()

        if not result:
            conn.close()
            return pd.DataFrame()

        form_config = json.loads(result[0])
        fields = form_config.get("fields", [])

        # 获取数据
        cursor.execute("SELECT id, data_json FROM form_data WHERE form_id=?", (form_id,))
        rows = cursor.fetchall()
        conn.close()

        # 转换为DataFrame
        data = []
        for row_id, data_json in rows:
            try:
                record = json.loads(data_json)
                record["id"] = row_id
                data.append(record)
            except:
                continue

        if data:
            return pd.DataFrame(data)
        else:
            return pd.DataFrame()

    def save_form_data(self, form_id, df):
        """保存表单数据"""
        if df.empty:
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 删除现有数据
        cursor.execute("DELETE FROM form_data WHERE form_id=?", (form_id,))

        # 插入新数据
        for _, row in df.iterrows():
            record_data = row.to_dict()
            if "id" in record_data:
                del record_data["id"]

            # 处理NaN值
            for key in list(record_data.keys()):
                if pd.isna(record_data[key]):
                    record_data[key] = ""

            cursor.execute("INSERT INTO form_data (form_id, data_json) VALUES (?, ?)",
                           (form_id, json.dumps(record_data, ensure_ascii=False)))

        conn.commit()
        conn.close()

    def create_form(self, form_name, fields):
        """创建新表单"""
        form_config = {
            "form_name": form_name,
            "fields": fields
        }

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO form_definitions (form_name, form_config) VALUES (?, ?)",
                       (form_name, json.dumps(form_config, ensure_ascii=False)))
        form_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return form_id

    def get_database_stats(self):
        """获取数据库统计信息"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 获取基本统计
        cursor.execute("SELECT COUNT(*) FROM form_definitions")
        form_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM form_data")
        data_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM role_permissions")
        role_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM login_logs")
        login_log_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM operation_logs")
        operation_log_count = cursor.fetchone()[0]

        conn.close()

        db_size = os.path.getsize(self.db_path) / 1024  # KB

        return {
            "form_count": form_count,
            "data_count": data_count,
            "user_count": user_count,
            "role_count": role_count,
            "login_log_count": login_log_count,
            "operation_log_count": operation_log_count,
            "db_size_kb": db_size
        }


# 创建系统实例
system = EnterpriseSupportSystem()


# ============= 辅助函数 =============
def check_permission(permission):
    """检查权限的通用函数"""
    if not st.session_state.get('logged_in'):
        return False

    current_user = st.session_state.get('current_user')
    if not current_user:
        return False

    return system.has_permission(current_user['id'], permission)


# ============= 页面显示函数 =============
def show_welcome_page():
    """显示欢迎页面"""
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("### 📋 系统介绍")
        st.write("""
        本系统用于管理”一中心一基地“信息台账，支持：

        - 📊 **数据管理**: 新增、编辑、删除企业帮扶数据
        - 📋 **表单管理**: 自定义表单字段，创建新的数据表
        - 📈 **统计分析**: 自定义统计字段，生成统计图表
        - 📤 **导入导出**: 支持Excel格式的数据导入导出
        - 🖨️ **打印功能**: 支持数据表格打印功能
        - 🔒 **安全登录**: 用户权限管理，数据安全存储
        """)


        # 显示数据库统计
        stats = system.get_database_stats()
        st.markdown("### 📊 系统统计")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("表单数量", stats["form_count"])
        with col2:
            st.metric("数据记录", stats["data_count"])
        with col3:
            st.metric("数据库大小", f"{stats['db_size_kb']:.1f} KB")
        st.markdown('</div>', unsafe_allow_html=True)


def show_dashboard():
    """显示仪表盘"""
    st.markdown('<h2 class="sub-header">🏠 系统仪表盘</h2>', unsafe_allow_html=True)

    # 检查权限
    if not check_permission("view_dashboard"):
        st.error("需要查看仪表盘权限")
        return

    # 获取统计信息
    stats = system.get_database_stats()

    # 统计卡片 - 根据用户权限显示不同的卡片
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
            <div class="stat-card">
                <h3>{stats['form_count']}</h3>
                <p>表单数量</p>
            </div>
            """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
            <div class="stat-card">
                <h3>{stats['data_count']}</h3>
                <p>数据记录</p>
            </div>
            """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
            <div class="stat-card">
                <h3>{stats['user_count']}</h3>
                <p>用户数量</p>
            </div>
            """, unsafe_allow_html=True)

    with col4:
        if check_permission("view_logs"):
            st.markdown(f"""
                <div class="stat-card">
                    <h3>{stats['login_log_count'] + stats['operation_log_count']}</h3>
                    <p>操作日志</p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div class="stat-card">
                    <h3>{stats['db_size_kb']:.1f}</h3>
                    <p>数据库大小(KB)</p>
                </div>
                """, unsafe_allow_html=True)

    # 用户信息卡片（如果用户有个人信息）
    if 'current_user' in st.session_state and st.session_state.current_user:
        current_user = st.session_state.current_user
        user_info = system.get_user_by_id(current_user['id'])

        if user_info:
            st.markdown("### 👤 我的信息")

            col1, col2 = st.columns([1, 2])

            with col1:
                st.markdown("""
                <div style="text-align: center; padding: 20px;">
                    <div style="font-size: 48px;">👤</div>
                </div>
                """, unsafe_allow_html=True)

            with col2:
                # 检查表结构以确定有哪些字段
                conn = sqlite3.connect(system.db_path)
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(users)")
                columns = cursor.fetchall()
                column_names = [col[1] for col in columns]
                conn.close()

                user_info_text = f"""
                **用户名:** {user_info[1]}  
                **姓名:** {user_info[2] or '未设置'}  
                **角色:** {user_info[6]}  
                """

                if 'department' in column_names and len(user_info) > 5:
                    user_info_text += f"**部门:** {user_info[5] or '未设置'}  \n"

                if 'last_login' in column_names and len(user_info) > 9:
                    user_info_text += f"**最后登录:** {user_info[9] or '从未登录'}  \n"

                if 'is_active' in column_names and len(user_info) > 8:
                    user_info_text += f"**账户状态:** {'✅ 活跃' if user_info[8] == 1 else '❌ 禁用'}"

                st.markdown(user_info_text)

    # 快速操作 - 根据权限显示
    st.markdown("### ⚡ 快速操作")

    col_count = 0
    cols = []

    if check_permission("view_data"):
        col_count += 1
    if check_permission("create_form"):
        col_count += 1
    if check_permission("view_reports"):
        col_count += 1
    if check_permission("manage_users"):
        col_count += 1

    if col_count > 0:
        cols = st.columns(col_count)
        col_index = 0

        if check_permission("view_data"):
            with cols[col_index]:
                if st.button("📊 管理数据", use_container_width=True, type="primary"):
                    st.session_state.selected_menu = "📊 数据管理"
                    st.rerun()
            col_index += 1

        if check_permission("create_form"):
            with cols[col_index]:
                if st.button("📋 新建表单", use_container_width=True):
                    st.session_state.selected_menu = "📋 表单管理"
                    st.rerun()
            col_index += 1

        if check_permission("view_reports"):
            with cols[col_index]:
                if st.button("📈 统计分析", use_container_width=True):
                    st.session_state.selected_menu = "📈 统计分析"
                    st.rerun()
            col_index += 1

        if check_permission("manage_users"):
            with cols[col_index]:
                if st.button("👥 用户管理", use_container_width=True):
                    st.session_state.selected_menu = "⚙️ 系统设置"
                    st.rerun()
            col_index += 1

    # 最近表单 - 如果有查看表单权限
    if check_permission("view_forms"):
        st.markdown("### 📝 最近表单")

        forms = system.get_forms()
        if forms:
            for form_id, form_name in forms[:3]:  # 显示前3个表单
                with st.expander(f"📋 {form_name}"):
                    df = system.get_form_data(form_id)
                    if not df.empty:
                        st.dataframe(df.head(3), use_container_width=True)
                        st.caption(f"共 {len(df)} 条记录")
                    else:
                        st.info("暂无数据")


def apply_filters(df, form_id, fields):
    """应用筛选条件到数据框"""
    if df.empty:
        return df

    filtered_df = df.copy()
    filtered_indices = list(df.index)
    filter_summary = []

    # 遍历所有字段应用筛选条件
    for field in fields:
        field_name = field['name']
        field_type = field.get('type', 'text')
        filter_key = f"filter_{form_id}_{field_name}"

        # 文本字段筛选
        if field_type in ['text', 'textarea']:
            filter_value = st.session_state.get(filter_key, "")
            if filter_value:
                exact_match = st.session_state.get('exact_match_checkbox', False)
                if exact_match:
                    # 精确匹配
                    mask = filtered_df[field_name].astype(str) == filter_value
                    filter_summary.append(f"{field_name} = '{filter_value}'")
                else:
                    # 模糊匹配
                    mask = filtered_df[field_name].astype(str).str.contains(filter_value, case=False, na=False)
                    filter_summary.append(f"{field_name} 包含 '{filter_value}'")

                filtered_df = filtered_df[mask]
                filtered_indices = [idx for idx, keep in zip(filtered_indices, mask) if keep]

        # 数字字段筛选
        elif field_type == 'number':
            min_key = f"{filter_key}_min"
            max_key = f"{filter_key}_max"
            min_val = st.session_state.get(min_key)
            max_val = st.session_state.get(max_key)

            if min_val is not None or max_val is not None:
                # 转换为数字类型
                try:
                    numeric_series = pd.to_numeric(filtered_df[field_name], errors='coerce')

                    if min_val is not None:
                        mask = numeric_series >= min_val
                        filter_summary.append(f"{field_name} ≥ {min_val}")
                    else:
                        mask = pd.Series([True] * len(filtered_df))

                    if max_val is not None:
                        mask = mask & (numeric_series <= max_val)
                        filter_summary.append(f"{field_name} ≤ {max_val}")

                    filtered_df = filtered_df[mask]
                    filtered_indices = [idx for idx, keep in zip(filtered_indices, mask) if keep]
                except:
                    pass  # 如果转换失败，跳过该字段筛选

        # 日期字段筛选
        elif field_type == 'date':
            start_key = f"{filter_key}_start"
            end_key = f"{filter_key}_end"
            start_date = st.session_state.get(start_key)
            end_date = st.session_state.get(end_key)

            if start_date or end_date:
                try:
                    # 尝试将字符串转换为日期
                    date_series = pd.to_datetime(filtered_df[field_name], errors='coerce')

                    if start_date:
                        mask = date_series >= pd.Timestamp(start_date)
                        filter_summary.append(f"{field_name} ≥ {start_date}")
                    else:
                        mask = pd.Series([True] * len(filtered_df))

                    if end_date:
                        mask = mask & (date_series <= pd.Timestamp(end_date))
                        filter_summary.append(f"{field_name} ≤ {end_date}")

                    filtered_df = filtered_df[mask]
                    filtered_indices = [idx for idx, keep in zip(filtered_indices, mask) if keep]
                except:
                    pass  # 如果转换失败，跳过该字段筛选

    # 保存筛选结果到session state
    st.session_state.filtered_df = filtered_df.reset_index(drop=True)
    st.session_state.filtered_indices = filtered_indices
    st.session_state.filters_applied = True
    st.session_state.filter_summary = filter_summary

    return filtered_df


def clear_filters(form_id):
    """清除筛选条件"""
    # 清除所有与该表单相关的筛选条件
    for key in list(st.session_state.keys()):
        if key.startswith(f"filter_{form_id}_"):
            del st.session_state[key]

    # 清除筛选结果
    if 'filtered_df' in st.session_state:
        del st.session_state['filtered_df']
    if 'filtered_indices' in st.session_state:
        del st.session_state['filtered_indices']
    if 'filter_summary' in st.session_state:
        del st.session_state['filter_summary']

    st.session_state.filters_applied = False


def show_print_preview(df, form_name):
    """显示打印预览"""
    st.markdown(f"### 🖨️ 打印预览 - {form_name}")
    st.markdown(f"**记录数:** {len(df)}")
    st.markdown(f"**生成时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 显示打印友好的表格
    st.dataframe(df, use_container_width=True)

    # 打印按钮
    if st.button("🖨️ 打印"):
        st.info("打印功能需要浏览器支持，请使用浏览器的打印功能 (Ctrl+P)")


def create_sample_data(form_id):
    """创建示例数据"""
    # 获取表单字段
    conn = sqlite3.connect(system.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT form_config FROM form_definitions WHERE id=?", (form_id,))
    form_config = json.loads(cursor.fetchone()[0])
    fields = form_config["fields"]
    conn.close()

    # 创建示例数据
    example_data = []
    for i in range(3):
        record = {}
        for field in fields:
            field_name = field["name"]
            field_type = field.get("type", "text")

            if field_name == "企业名称":
                record[field_name] = f"示例企业{i + 1}"
            elif field_name == "联系人":
                record[field_name] = f"张{i + 1}"
            elif field_name == "联系电话":
                record[field_name] = f"1380013800{i}"
            elif field_type == "number":
                record[field_name] = (i + 1) * 10
            elif field_type == "date":
                record[field_name] = f"2023-0{i + 1}-0{i + 1}"
            else:
                record[field_name] = f"示例{field_name}数据{i + 1}"
        example_data.append(record)

    # 保存数据
    df = pd.DataFrame(example_data)
    system.save_form_data(form_id, df)

    # 更新session state
    data_key = f"form_data_{form_id}"
    if 'form_data' not in st.session_state:
        st.session_state.form_data = {}
    st.session_state.form_data[data_key] = df

    st.success("示例数据已创建！")


def show_add_record_form(form_id, current_df, form_config):
    """显示添加新记录的表单"""
    st.markdown("#### ➕ 添加新记录")

    with st.form(key=f"add_record_form_{form_id}"):
        new_record = {}

        for field in form_config['fields']:
            field_name = field['name']
            field_type = field.get('type', 'text')
            required = field.get('required', False)

            if field_type == 'textarea':
                new_record[field_name] = st.text_area(
                    f"{field_name}{' *' if required else ''}",
                    key=f"new_{field_name}_{form_id}"
                )
            elif field_type == 'number':
                new_record[field_name] = st.number_input(
                    f"{field_name}{' *' if required else ''}",
                    key=f"new_{field_name}_{form_id}"
                )
            elif field_type == 'date':
                new_record[field_name] = st.date_input(
                    f"{field_name}{' *' if required else ''}",
                    key=f"new_{field_name}_{form_id}"
                )
            else:
                new_record[field_name] = st.text_input(
                    f"{field_name}{' *' if required else ''}",
                    key=f"new_{field_name}_{form_id}"
                )

        if st.form_submit_button("保存新记录"):
            # 检查必填字段
            missing_fields = []
            for field in form_config['fields']:
                if field.get('required', False):
                    if not new_record.get(field['name']):
                        missing_fields.append(field['name'])

            if missing_fields:
                st.error(f"请填写必填字段: {', '.join(missing_fields)}")
            else:
                # 添加到DataFrame
                new_df = pd.concat([current_df, pd.DataFrame([new_record])], ignore_index=True)

                # 保存到数据库
                system.save_form_data(form_id, new_df)

                # 更新session state
                data_key = f"form_data_{form_id}"
                st.session_state.form_data[data_key] = new_df

                st.success("记录添加成功！")
                return True

    return False


def show_data_management():
    """显示数据管理页面"""
    st.markdown('<h2 class="sub-header">📊 数据管理</h2>', unsafe_allow_html=True)

    # 检查权限
    if not check_permission("view_data"):
        st.error("需要查看数据权限")
        return

    # 表单选择
    forms = system.get_forms()
    if not forms:
        st.warning("暂无表单，请先创建表单")
        return

    form_options = {f"{form_name} (ID: {form_id})": form_id for form_id, form_name in forms}
    selected_form_key = st.selectbox("选择表单", list(form_options.keys()), key="data_management_form_select")

    if selected_form_key:
        form_id = form_options[selected_form_key]

        # 初始化session state
        if 'form_data' not in st.session_state:
            st.session_state.form_data = {}

        # 从数据库加载数据
        df = system.get_form_data(form_id)

        # 保存到session state
        data_key = f"form_data_{form_id}"
        if data_key not in st.session_state.form_data:
            st.session_state.form_data[data_key] = df.copy()

        # 获取当前数据 - 这是原始完整数据
        current_df = st.session_state.form_data[data_key].copy()

        # 编辑模式状态
        edit_mode_key = f"edit_mode_{form_id}"
        if edit_mode_key not in st.session_state:
            st.session_state[edit_mode_key] = False

        if not current_df.empty:
            # 显示数据统计
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("总记录数", len(current_df))
            with col2:
                st.metric("字段数量", len(current_df.columns))
            with col3:
                filtered_count = len(current_df) if 'filtered_df' not in st.session_state else len(
                    st.session_state.filtered_df)
                st.metric("筛选后记录", filtered_count)
            with col4:
                edit_status = "编辑中" if st.session_state[edit_mode_key] else "查看中"
                st.metric("当前状态", edit_status)

            # ============= 编辑模式控制 =============
            st.markdown("### ✏️ 编辑控制")

            col1, col2, col3 = st.columns([2, 2, 6])

            with col1:
                if not st.session_state[edit_mode_key]:
                    if st.button("✏️ 进入编辑模式", type="primary", use_container_width=True,
                                 key=f"enter_edit_{form_id}"):
                        st.session_state[edit_mode_key] = True
                        # 保存编辑前的数据副本
                        st.session_state[f"original_data_{form_id}"] = current_df.copy()
                        st.rerun()
                else:
                    if st.button("🚫 退出编辑模式", type="secondary", use_container_width=True,
                                 key=f"exit_edit_{form_id}"):
                        st.session_state[edit_mode_key] = False
                        st.rerun()

            with col2:
                if st.session_state[edit_mode_key]:
                    if st.button("💾 保存修改", type="primary", use_container_width=True,
                                 key=f"save_edits_{form_id}"):
                        try:
                            # 获取编辑后的数据
                            edited_key = f"edited_data_{form_id}"
                            if edited_key in st.session_state:
                                edited_df = st.session_state[edited_key]
                            else:
                                # 如果没有找到编辑后的数据，使用当前数据
                                edited_df = current_df.copy()

                            # 关键修复：检查是否有筛选应用
                            if 'filters_applied' in st.session_state and st.session_state.filters_applied:
                                # 如果应用了筛选，需要合并数据
                                st.info("检测到筛选条件，正在合并数据...")

                                # 获取原始完整数据
                                original_df = st.session_state.form_data[data_key].copy()

                                # 获取筛选后的数据（编辑前的）
                                if 'filtered_df' in st.session_state:
                                    filtered_before_edit = st.session_state.filtered_df.copy()

                                    # 创建映射：找到筛选后数据在原始数据中的索引
                                    # 由于筛选后的数据可能改变了顺序，我们需要通过内容匹配
                                    original_indices = []
                                    for _, filtered_row in filtered_before_edit.iterrows():
                                        # 在原始数据中查找匹配的行
                                        mask = (original_df == filtered_row).all(axis=1)
                                        if mask.any():
                                            idx = original_df[mask].index[0]
                                            original_indices.append(idx)
                                        else:
                                            # 如果没有找到，可能是新添加的行
                                            original_indices.append(None)

                                    # 创建一个原始数据的副本用于更新
                                    updated_df = original_df.copy()

                                    # 更新或添加行
                                    new_rows_added = 0
                                    for i, (idx, edited_row) in enumerate(zip(original_indices, edited_df.iterrows())):
                                        if idx is not None:
                                            # 更新现有行
                                            updated_df.loc[idx] = edited_df.iloc[i]
                                        else:
                                            # 添加新行 - 使用concat而不是append
                                            new_row_df = pd.DataFrame([edited_df.iloc[i]])
                                            updated_df = pd.concat([updated_df, new_row_df], ignore_index=True)
                                            new_rows_added += 1

                                    # 检查是否有行被删除（编辑后的数据行数少于编辑前的筛选数据）
                                    rows_deleted = len(filtered_before_edit) - len(edited_df) + new_rows_added
                                    if rows_deleted > 0:
                                        st.warning(f"检测到删除了 {rows_deleted} 行，筛选条件可能已改变")

                                    final_df = updated_df
                                else:
                                    # 如果没有筛选数据，使用编辑后的数据
                                    final_df = edited_df
                            else:
                                # 没有应用筛选，直接使用编辑后的数据
                                final_df = edited_df

                            # 保存到数据库
                            system.save_form_data(form_id, final_df)

                            # 更新session state中的数据
                            st.session_state.form_data[data_key] = final_df

                            # 退出编辑模式
                            st.session_state[edit_mode_key] = False

                            # 清理临时数据
                            if edited_key in st.session_state:
                                del st.session_state[edited_key]
                            if f"original_data_{form_id}" in st.session_state:
                                del st.session_state[f"original_data_{form_id}"]

                            st.success("✅ 数据保存成功！")
                            st.rerun()
                        except Exception as e:
                            st.error(f"保存失败: {str(e)}")

            with col3:
                if st.session_state[edit_mode_key]:
                    st.info("📝 编辑模式已激活，您可以修改表格中的任何单元格")
                elif 'filters_applied' in st.session_state and st.session_state.filters_applied:
                    st.warning("⚠️ 当前已应用筛选条件，编辑保存时会自动合并数据")

            # ============= 数据筛选功能 =============
            st.markdown("### 🔍 数据筛选")

            with st.expander("展开筛选条件", expanded=False):
                # 获取表单字段信息
                conn = sqlite3.connect(system.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT form_config FROM form_definitions WHERE id=?", (form_id,))
                form_config = json.loads(cursor.fetchone()[0])
                conn.close()

                fields = form_config.get('fields', [])

                # 创建筛选条件
                col_num = 2  # 每行显示2个字段
                for i in range(0, len(fields), col_num):
                    cols = st.columns(col_num)
                    for j in range(col_num):
                        if i + j < len(fields):
                            field = fields[i + j]
                            field_name = field['name']
                            field_type = field.get('type', 'text')

                            with cols[j]:
                                if field_type == 'number':
                                    # 数字字段范围筛选
                                    col_min, col_max = st.columns(2)
                                    with col_min:
                                        min_val = st.number_input(
                                            f"{field_name} 最小值",
                                            key=f"min_{form_id}_{field_name}",
                                            value=None,
                                            placeholder="最小值"
                                        )
                                    with col_max:
                                        max_val = st.number_input(
                                            f"{field_name} 最大值",
                                            key=f"max_{form_id}_{field_name}",
                                            value=None,
                                            placeholder="最大值"
                                        )
                                else:
                                    # 文本字段筛选
                                    filter_val = st.text_input(
                                        f"{field_name}",
                                        key=f"filter_{form_id}_{field_name}",
                                        placeholder=f"输入{field_name}筛选"
                                    )

                # 筛选操作按钮
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ 应用筛选", type="primary", key=f"apply_filter_{form_id}"):
                        # 应用筛选逻辑
                        filtered_df = current_df.copy()

                        for field in fields:
                            field_name = field['name']
                            field_type = field.get('type', 'text')

                            if field_type == 'number':
                                min_key = f"min_{form_id}_{field_name}"
                                max_key = f"max_{form_id}_{field_name}"

                                if min_key in st.session_state and st.session_state[min_key] is not None:
                                    try:
                                        min_val = float(st.session_state[min_key])
                                        filtered_df = filtered_df[
                                            pd.to_numeric(filtered_df[field_name], errors='coerce') >= min_val]
                                    except:
                                        pass

                                if max_key in st.session_state and st.session_state[max_key] is not None:
                                    try:
                                        max_val = float(st.session_state[max_key])
                                        filtered_df = filtered_df[
                                            pd.to_numeric(filtered_df[field_name], errors='coerce') <= max_val]
                                    except:
                                        pass
                            else:
                                filter_key = f"filter_{form_id}_{field_name}"
                                if filter_key in st.session_state and st.session_state[filter_key]:
                                    filter_val = st.session_state[filter_key]
                                    if filter_val:
                                        filtered_df = filtered_df[
                                            filtered_df[field_name].astype(str).str.contains(filter_val, case=False,
                                                                                             na=False)]

                        st.session_state['filtered_df'] = filtered_df
                        st.session_state['filters_applied'] = True
                        st.success("筛选条件已应用！")
                        st.rerun()

                with col2:
                    if st.button("❌ 清除筛选", key=f"clear_filter_{form_id}"):
                        # 清除筛选条件
                        for key in list(st.session_state.keys()):
                            if key.startswith(f'filter_{form_id}_') or key.startswith(
                                    f'min_{form_id}_') or key.startswith(f'max_{form_id}_'):
                                del st.session_state[key]

                        if 'filtered_df' in st.session_state:
                            del st.session_state['filtered_df']
                        if 'filters_applied' in st.session_state:
                            del st.session_state['filters_applied']

                        st.success("筛选条件已清除！")
                        st.rerun()

            # ============= 数据表格显示 =============
            st.markdown("### 📝 数据表格")

            # 确定显示的数据
            if 'filters_applied' in st.session_state and st.session_state.filters_applied:
                display_df = st.session_state.get('filtered_df', current_df)
            else:
                display_df = current_df

            if len(display_df) == 0:
                st.warning("没有找到符合条件的数据")
            else:
                if st.session_state[edit_mode_key]:
                    # 编辑模式
                    st.markdown("#### 📋 编辑表格（双击单元格进行编辑）")

                    # 使用data_editor
                    edited_df = st.data_editor(
                        display_df,
                        key=f"data_editor_{form_id}",
                        num_rows="dynamic",
                        use_container_width=True,
                        column_config=None
                    )

                    # 保存编辑后的数据到session state
                    if not edited_df.equals(display_df):
                        st.session_state[f"edited_data_{form_id}"] = edited_df
                        st.success("✅ 检测到修改，请点击保存按钮保存更改")

                    # 显示数据操作提示
                    st.info("""
                    **编辑操作说明：**
                    1. **双击单元格**：进入编辑模式
                    2. **按Enter键**：保存当前单元格的修改
                    3. **按Esc键**：取消当前单元格的修改
                    4. **删除整行**：选中行后按Delete键
                    5. **添加新行**：滚动到表格底部，在空白行中输入数据

                    **重要提示：** 如果使用了筛选功能，编辑保存时会自动合并数据，确保未筛选的数据不会丢失。
                    """)
                else:
                    # 查看模式
                    st.markdown("#### 📋 数据预览（只读模式）")
                    st.dataframe(display_df, use_container_width=True, height=400)

            # ============= 数据操作按钮 =============
            st.markdown("### 💾 数据操作")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                # 导出CSV
                csv = display_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 导出CSV",
                    data=csv,
                    file_name=f"{selected_form_key.split('(')[0].strip()}_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )

            with col2:
                # 导出Excel
                try:
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        display_df.to_excel(writer, index=False, sheet_name='Sheet1')
                    excel_data = output.getvalue()

                    st.download_button(
                        label="📥 导出Excel",
                        data=excel_data,
                        file_name=f"{selected_form_key.split('(')[0].strip()}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                except:
                    st.info("请安装openpyxl库以导出Excel")

            with col3:
                # 添加新记录
                if st.button("➕ 添加新记录", use_container_width=True, key=f"add_record_{form_id}"):
                    show_add_record_form(form_id, current_df, form_config)

            with col4:
                # 刷新数据
                if st.button("🔄 刷新数据", use_container_width=True, key=f"refresh_{form_id}"):
                    # 重新从数据库加载数据
                    df = system.get_form_data(form_id)
                    st.session_state.form_data[data_key] = df
                    st.rerun()

        else:
            st.info("该表单暂无数据")

            # 创建示例数据按钮
            if st.button("创建示例数据", type="primary"):
                create_sample_data(form_id)
                st.rerun()


def show_form_management():
    """显示表单管理页面"""
    st.markdown('<h2 class="sub-header">📋 表单管理</h2>', unsafe_allow_html=True)

    # 检查权限
    if not check_permission("view_forms"):
        st.error("需要查看表单权限")
        return

    # 初始化session state
    if 'form_fields' not in st.session_state:
        st.session_state.form_fields = []
    if 'editing_form_id' not in st.session_state:
        st.session_state.editing_form_id = None
    if 'editing_form_name' not in st.session_state:
        st.session_state.editing_form_name = ""

    tab1, tab2, tab3 = st.tabs(["📋 现有表单", "➕ 新建表单", "✏️ 编辑表单"])

    with tab1:
        st.markdown("### 现有表单列表")

        forms = system.get_forms()
        if forms:
            for form_id, form_name in forms:
                with st.expander(f"📋 {form_name} (ID: {form_id})"):
                    col1, col2, col3 = st.columns([3, 1, 1])

                    with col1:
                        # 显示表单信息
                        conn = sqlite3.connect(system.db_path)
                        cursor = conn.cursor()
                        cursor.execute("SELECT form_config, created_at FROM form_definitions WHERE id=?", (form_id,))
                        form_config_data = cursor.fetchone()
                        conn.close()

                        form_config = json.loads(form_config_data[0])
                        created_at = form_config_data[1]

                        # 显示表单详情
                        st.markdown(f"**创建时间:** {created_at}")
                        st.markdown(f"**字段数量:** {len(form_config.get('fields', []))}")

                        # 显示前几个字段
                        fields = form_config.get('fields', [])
                        if fields:
                            st.markdown("**字段预览:**")
                            for i, field in enumerate(fields[:3]):  # 只显示前3个字段
                                field_type = field.get('type', 'text')
                                required = "✓" if field.get('required', False) else "✗"
                                st.text(f"• {field['name']} ({field_type}) - 必填: {required}")
                            if len(fields) > 3:
                                st.caption(f"... 还有 {len(fields) - 3} 个字段")
                        else:
                            st.text("• 无字段定义")

                    with col2:
                        # 数据统计
                        df = system.get_form_data(form_id)
                        record_count = len(df)
                        st.metric("记录数", record_count)

                    with col3:
                        # 操作按钮区域
                        st.markdown("**操作**")

                        # 设置当前表单按钮
                        if st.button("选择", key=f"select_{form_id}", use_container_width=True):
                            st.session_state.current_form = form_id
                            st.success(f"已选择表单: {form_name}")

                        # 编辑表单按钮
                        if st.button("✏️ 编辑", key=f"edit_{form_id}", use_container_width=True,
                                     type="secondary", help="编辑表单字段"):
                            # 加载表单配置到session state
                            conn = sqlite3.connect(system.db_path)
                            cursor = conn.cursor()
                            cursor.execute("SELECT form_config FROM form_definitions WHERE id=?", (form_id,))
                            form_config = json.loads(cursor.fetchone()[0])
                            conn.close()

                            st.session_state.editing_form_id = form_id
                            st.session_state.editing_form_name = form_name
                            st.session_state.form_fields = form_config.get('fields', [])

                            # 切换到编辑表单tab
                            st.success(f"已加载表单 '{form_name}' 的字段配置")
                            # 注意：Streamlit不能直接切换tab，但我们可以通过JS或重新渲染来实现
                            # 这里我们显示消息，用户需要手动切换到"编辑表单"tab

                        # 删除表单按钮 - 只有管理员可以操作
                        if st.session_state.get('current_user', {}).get('role') == 'admin':
                            # 使用唯一的key来防止冲突
                            delete_key = f"delete_btn_{form_id}"

                            if st.button("🗑️ 删除", key=delete_key, use_container_width=True,
                                         type="secondary", help="删除表单及其所有数据"):
                                # 设置确认状态
                                st.session_state[f"confirm_delete_{form_id}"] = True
                                st.rerun()

                            # 确认删除对话框
                            if st.session_state.get(f"confirm_delete_{form_id}", False):
                                st.warning(f"⚠️ 确认删除表单 '{form_name}' 吗？")
                                st.error("此操作将永久删除表单及其所有数据，无法恢复！")

                                col_confirm1, col_confirm2 = st.columns(2)

                                with col_confirm1:
                                    if st.button("✅ 确认删除", key=f"confirm_delete_yes_{form_id}",
                                                 type="primary", use_container_width=True):
                                        # 执行删除操作
                                        try:
                                            conn = sqlite3.connect(system.db_path)
                                            cursor = conn.cursor()

                                            # 先删除表单数据
                                            cursor.execute("DELETE FROM form_data WHERE form_id=?", (form_id,))

                                            # 再删除表单定义
                                            cursor.execute("DELETE FROM form_definitions WHERE id=?", (form_id,))

                                            # 提交事务
                                            conn.commit()

                                            # 获取删除的行数
                                            data_deleted = cursor.rowcount
                                            cursor.execute("SELECT changes()")
                                            form_deleted = cursor.fetchone()[0]

                                            conn.close()

                                            # 清理session state
                                            if f"confirm_delete_{form_id}" in st.session_state:
                                                del st.session_state[f"confirm_delete_{form_id}"]

                                            # 清理可能存在的表单数据缓存
                                            data_key = f"form_data_{form_id}"
                                            if 'form_data' in st.session_state and data_key in st.session_state.form_data:
                                                del st.session_state.form_data[data_key]

                                            # 如果删除的是当前选中的表单，清除选择
                                            if st.session_state.get('current_form') == form_id:
                                                st.session_state.current_form = None

                                            st.success(f"✅ 表单 '{form_name}' 已成功删除！")
                                            st.info(f"删除数据记录: {data_deleted} 条")

                                            # 添加短暂延迟后刷新页面
                                            st.rerun()

                                        except Exception as e:
                                            st.error(f"删除失败: {str(e)}")

                                with col_confirm2:
                                    if st.button("❌ 取消", key=f"confirm_delete_no_{form_id}",
                                                 use_container_width=True):
                                        # 取消删除
                                        if f"confirm_delete_{form_id}" in st.session_state:
                                            del st.session_state[f"confirm_delete_{form_id}"]
                                        st.rerun()
                        else:
                            st.info("仅管理员可删除")

        else:
            st.info("暂无表单")

    with tab2:
        st.markdown("### 创建新表单")

        # 显示现有字段
        if st.session_state.form_fields:
            st.markdown("#### 已定义的字段:")
            for i, field in enumerate(st.session_state.form_fields):
                field_type = field.get('type', 'text')
                required = "✓" if field.get('required', False) else "✗"
                st.text(f"{i + 1}. {field['name']} ({field_type}) - 必填: {required}")

        # 添加字段按钮
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("➕ 添加字段", key="add_field_button_new"):
                st.session_state.form_fields.append({
                    "name": "",
                    "type": "text",
                    "required": False
                })
                st.rerun()

        with col2:
            if st.button("🗑️ 清空字段", key="clear_fields_button_new"):
                st.session_state.form_fields = []
                st.rerun()

        # 创建新字段的表单
        with st.form("create_form", clear_on_submit=True):
            st.markdown("#### 添加新字段")

            col1, col2, col3 = st.columns([3, 2, 1])

            with col1:
                new_field_name = st.text_input("字段名", key="new_field_name", placeholder="例如: 企业名称")

            with col2:
                new_field_type = st.selectbox(
                    "字段类型",
                    ["text", "textarea", "number", "date", "select"],
                    key="new_field_type_select"
                )

            with col3:
                new_field_required = st.checkbox("必填", key="new_field_required_check")

            # 使用form_submit_button替代普通的button
            add_field_submitted = st.form_submit_button("添加字段到列表", type="secondary")

            if add_field_submitted:
                if not new_field_name:
                    st.error("请输入字段名")
                else:
                    # 检查字段名是否重复
                    existing_names = [f["name"] for f in st.session_state.form_fields]
                    if new_field_name in existing_names:
                        st.error(f"字段名 '{new_field_name}' 已存在")
                    else:
                        st.session_state.form_fields.append({
                            "name": new_field_name,
                            "type": new_field_type,
                            "required": new_field_required
                        })
                        st.success(f"字段 '{new_field_name}' 已添加到列表")
                        st.rerun()

        # 创建表单的表单
        st.markdown("---")
        st.markdown("#### 创建新表单")

        with st.form("create_main_form"):
            form_name = st.text_input("表单名称", key="form_name_input", placeholder="例如: 帮扶总台账")

            if st.session_state.form_fields:
                st.markdown("##### 将要创建的字段:")
                for i, field in enumerate(st.session_state.form_fields):
                    field_type = field.get('type', 'text')
                    required = "必填" if field.get('required', False) else "非必填"
                    st.text(f"{i + 1}. {field['name']} ({field_type}) - {required}")

            # 使用form_submit_button
            create_submitted = st.form_submit_button("📝 创建表单", type="primary")

            if create_submitted:
                if not form_name:
                    st.error("请输入表单名称")
                elif not st.session_state.form_fields:
                    st.error("请至少添加一个字段")
                else:
                    # 验证字段名
                    field_names = [f["name"] for f in st.session_state.form_fields]
                    if "" in field_names:
                        st.error("所有字段必须有名称")
                    elif len(set(field_names)) != len(field_names):
                        st.error("字段名不能重复")
                    else:
                        # 检查表单名是否已存在
                        existing_forms = system.get_forms()
                        existing_form_names = [name for _, name in existing_forms]
                        if form_name in existing_form_names:
                            st.error(f"表单名称 '{form_name}' 已存在，请使用其他名称")
                        else:
                            form_id = system.create_form(form_name, st.session_state.form_fields)
                            st.session_state.form_fields = []
                            st.success(f"表单 '{form_name}' 创建成功！ID: {form_id}")
                            st.rerun()

    with tab3:
        st.markdown("### ✏️ 编辑表单字段")

        # 检查是否有正在编辑的表单
        if st.session_state.editing_form_id is None:
            st.info("请在'现有表单'选项卡中选择一个表单进行编辑")
            st.markdown("""
            **编辑表单操作说明:**
            1. 在"现有表单"选项卡中找到要编辑的表单
            2. 点击表单右下角的"✏️ 编辑"按钮
            3. 系统会自动切换到本选项卡并加载表单字段
            4. 您可以修改字段属性、添加新字段或删除字段
            5. 保存修改后，表单定义将更新，但现有数据不受影响
            """)
        else:
            st.markdown(
                f"**正在编辑表单:** {st.session_state.editing_form_name} (ID: {st.session_state.editing_form_id})")

            # 显示当前字段
            if st.session_state.form_fields:
                st.markdown("#### 📝 当前字段列表")

                # 使用表单来编辑字段
                with st.form("edit_fields_form"):
                    # 创建一个可编辑的字段列表
                    edited_fields = []

                    for i, field in enumerate(st.session_state.form_fields):
                        st.markdown(f"---")
                        st.markdown(f"**字段 {i + 1}**")

                        col1, col2, col3, col4 = st.columns([3, 2, 1, 1])

                        with col1:
                            field_name = st.text_input(
                                "字段名",
                                value=field.get('name', ''),
                                key=f"edit_field_name_{i}",
                                help="字段名称，不能为空"
                            )

                        with col2:
                            field_type = st.selectbox(
                                "字段类型",
                                ["text", "textarea", "number", "date", "select"],
                                index=["text", "textarea", "number", "date", "select"].index(field.get('type', 'text')),
                                key=f"edit_field_type_{i}",
                                help="字段的数据类型"
                            )

                        with col3:
                            field_required = st.checkbox(
                                "必填",
                                value=field.get('required', False),
                                key=f"edit_field_required_{i}",
                                help="是否必填字段"
                            )

                        with col4:
                            # 删除字段的复选框
                            delete_field = st.checkbox(
                                "删除",
                                key=f"delete_field_{i}",
                                help="标记此字段为删除"
                            )

                        # 如果不是删除状态，则添加到编辑后的字段列表
                        if not delete_field and field_name:  # 确保字段名不为空
                            edited_fields.append({
                                "name": field_name,
                                "type": field_type,
                                "required": field_required
                            })

                    # 添加新字段的选项
                    st.markdown("---")
                    st.markdown("#### ➕ 添加新字段")

                    col1, col2, col3 = st.columns([3, 2, 1])

                    with col1:
                        new_field_name = st.text_input(
                            "新字段名",
                            key="edit_new_field_name",
                            placeholder="例如: 新字段名称"
                        )

                    with col2:
                        new_field_type = st.selectbox(
                            "新字段类型",
                            ["text", "textarea", "number", "date", "select"],
                            key="edit_new_field_type"
                        )

                    with col3:
                        new_field_required = st.checkbox(
                            "必填",
                            key="edit_new_field_required"
                        )

                    # 添加新字段按钮
                    add_new_field = st.checkbox(
                        "添加此新字段",
                        key="add_new_field_checkbox",
                        help="勾选以添加此新字段到表单"
                    )

                    # 表单提交按钮
                    col_save1, col_save2 = st.columns([1, 1])

                    with col_save1:
                        save_changes = st.form_submit_button("💾 保存修改", type="primary", use_container_width=True)

                    with col_save2:
                        cancel_edit = st.form_submit_button("❌ 取消编辑", use_container_width=True)

                    if save_changes:
                        # 处理添加新字段
                        if add_new_field and new_field_name:
                            # 检查字段名是否重复
                            existing_names = [f["name"] for f in edited_fields]
                            if new_field_name in existing_names:
                                st.error(f"字段名 '{new_field_name}' 已存在")
                            else:
                                edited_fields.append({
                                    "name": new_field_name,
                                    "type": new_field_type,
                                    "required": new_field_required
                                })
                                st.success(f"新字段 '{new_field_name}' 已添加")

                        # 验证字段
                        if not edited_fields:
                            st.error("表单必须至少包含一个字段")
                        else:
                            # 检查字段名是否重复
                            field_names = [f["name"] for f in edited_fields]
                            if len(set(field_names)) != len(field_names):
                                st.error("字段名不能重复")
                            else:
                                try:
                                    # 更新表单定义
                                    conn = sqlite3.connect(system.db_path)
                                    cursor = conn.cursor()

                                    # 获取原始表单配置
                                    cursor.execute("SELECT form_config FROM form_definitions WHERE id=?",
                                                   (st.session_state.editing_form_id,))
                                    original_config = json.loads(cursor.fetchone()[0])

                                    # 创建新的表单配置
                                    new_config = {
                                        "form_name": original_config.get("form_name",
                                                                         st.session_state.editing_form_name),
                                        "fields": edited_fields
                                    }

                                    # 更新数据库
                                    cursor.execute("UPDATE form_definitions SET form_config = ? WHERE id = ?",
                                                   (json.dumps(new_config, ensure_ascii=False),
                                                    st.session_state.editing_form_id))

                                    conn.commit()

                                    # 更新现有数据 - 需要处理字段变化
                                    cursor.execute("SELECT id, data_json FROM form_data WHERE form_id=?",
                                                   (st.session_state.editing_form_id,))
                                    rows = cursor.fetchall()

                                    old_field_names = [field["name"] for field in original_config.get("fields", [])]
                                    new_field_names = [field["name"] for field in edited_fields]

                                    # 找出新增的字段和删除的字段
                                    added_fields = set(new_field_names) - set(old_field_names)
                                    removed_fields = set(old_field_names) - set(new_field_names)

                                    for row_id, data_json in rows:
                                        try:
                                            record = json.loads(data_json)

                                            # 删除已移除的字段
                                            for field in removed_fields:
                                                if field in record:
                                                    del record[field]

                                            # 添加新字段（设为空值）
                                            for field in added_fields:
                                                if field not in record:
                                                    # 根据字段类型设置默认值
                                                    field_type = next(
                                                        (f["type"] for f in edited_fields if f["name"] == field),
                                                        "text")
                                                    if field_type == "number":
                                                        record[field] = 0
                                                    else:
                                                        record[field] = ""

                                            # 更新数据库
                                            cursor.execute("UPDATE form_data SET data_json = ? WHERE id = ?",
                                                           (json.dumps(record, ensure_ascii=False), row_id))
                                        except:
                                            continue

                                    conn.commit()
                                    conn.close()

                                    # 更新session state
                                    st.session_state.form_fields = edited_fields

                                    st.success(f"✅ 表单 '{st.session_state.editing_form_name}' 已成功更新！")
                                    st.info(
                                        f"字段变更: 新增 {len(added_fields)} 个字段，删除 {len(removed_fields)} 个字段")

                                    # 显示变更详情
                                    if added_fields:
                                        st.markdown("**新增字段:**")
                                        for field in added_fields:
                                            st.text(f"• {field}")

                                    if removed_fields:
                                        st.markdown("**删除字段:**")
                                        for field in removed_fields:
                                            st.text(f"• {field}")

                                    # 清理编辑状态
                                    st.session_state.editing_form_id = None
                                    st.session_state.editing_form_name = ""
                                    st.rerun()

                                except Exception as e:
                                    st.error(f"更新失败: {str(e)}")

                    if cancel_edit:
                        # 取消编辑，清理session state
                        st.session_state.editing_form_id = None
                        st.session_state.editing_form_name = ""
                        st.session_state.form_fields = []
                        st.rerun()

            else:
                st.warning("当前表单没有字段定义")

                # 快速添加字段选项
                st.markdown("#### 快速添加字段")

                quick_fields = [
                    {"name": "企业名称", "type": "text", "required": True},
                    {"name": "联系人", "type": "text", "required": True},
                    {"name": "联系电话", "type": "text", "required": True},
                    {"name": "地址", "type": "text", "required": False},
                    {"name": "创建时间", "type": "date", "required": False}
                ]

                col1, col2 = st.columns(2)

                with col1:
                    if st.button("📋 添加常用字段", use_container_width=True):
                        st.session_state.form_fields = quick_fields
                        st.success("已添加常用字段模板")
                        st.rerun()

                with col2:
                    if st.button("✏️ 手动添加字段", use_container_width=True):
                        st.session_state.form_fields.append({
                            "name": "",
                            "type": "text",
                            "required": False
                        })
                        st.rerun()


def show_statistical_analysis():
    """显示统计分析页面"""
    st.markdown('<h2 class="sub-header">📈 统计分析</h2>', unsafe_allow_html=True)

    forms = system.get_forms()
    if not forms:
        st.warning("暂无表单数据")
        return

    # 使用选项卡分离单表单统计和关联统计
    tab1, tab2 = st.tabs(["📊 单表单统计", "🔗 表单关联分析"])

    with tab1:
        # 原有的单表单统计功能
        form_options = {f"{form_name} (ID: {form_id})": form_id for form_id, form_name in forms}
        selected_form_key = st.selectbox("选择表单", list(form_options.keys()), key="stat_analysis_form_select")

        if selected_form_key:
            form_id = form_options[selected_form_key]
            df = system.get_form_data(form_id)

            if df.empty:
                st.info("该表单暂无数据")
                return

            st.markdown(f"### 📊 数据概览 - {selected_form_key.split('(')[0].strip()}")
            st.dataframe(df, use_container_width=True)

            # 基本统计
            st.markdown("### 📈 基本统计")

            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("总记录数", len(df))

            with col2:
                numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
                st.metric("数值字段数", len(numeric_cols))

            with col3:
                text_cols = df.select_dtypes(include=['object']).columns.tolist()
                st.metric("文本字段数", len(text_cols))

            # 数值字段统计
            if numeric_cols:
                st.markdown("### 🔢 数值字段统计")
                selected_num_col = st.selectbox("选择数值字段", numeric_cols, key="num_field_select")

                if selected_num_col in df.columns:
                    col_stats = df[selected_num_col].describe()

                    # 显示统计表格
                    stats_data = {
                        "统计项": ["数量", "平均值", "标准差", "最小值", "25%分位数", "中位数", "75%分位数", "最大值"],
                        "值": [
                            col_stats.get('count', 0),
                            col_stats.get('mean', 0),
                            col_stats.get('std', 0),
                            col_stats.get('min', 0),
                            col_stats.get('25%', 0),
                            col_stats.get('50%', 0),
                            col_stats.get('75%', 0),
                            col_stats.get('max', 0)
                        ]
                    }

                    st.dataframe(pd.DataFrame(stats_data), use_container_width=True)

                    # 显示分布图
                    try:
                        fig = px.histogram(df, x=selected_num_col, title=f"{selected_num_col} 分布")
                        st.plotly_chart(fig, use_container_width=True)
                    except:
                        st.info("安装plotly库以显示图表: `pip install plotly`")

            # 文本字段统计
            if text_cols:
                st.markdown("### 📝 文本字段统计")
                selected_text_col = st.selectbox("选择文本字段", text_cols, key="text_field_select")

                if selected_text_col in df.columns:
                    value_counts = df[selected_text_col].value_counts().head(10)
                    st.bar_chart(value_counts)

    with tab2:
        # ============= 新增：表单关联分析功能 =============
        st.markdown("### 🔗 表单关联分析")
        st.markdown("""
        <div class="card">
        <strong>关联分析说明:</strong><br>
        1. 选择两个表单进行关联分析<br>
        2. 选择一个关联字段（两个表单中名称相同的字段）<br>
        3. 系统将自动合并两个表单的数据<br>
        4. 支持交叉统计和关联分析<br>
        5. 显示未成功关联的数据信息
        </div>
        """, unsafe_allow_html=True)

        # 第一步：选择两个表单
        st.markdown("#### 步骤1：选择要关联的表单")
        col1, col2 = st.columns(2)

        with col1:
            form_options = {f"{form_name} (ID: {form_id})": form_id for form_id, form_name in forms}
            selected_form1_key = st.selectbox("选择第一个表单", list(form_options.keys()), key="form1_select")

        with col2:
            selected_form2_key = st.selectbox("选择第二个表单", list(form_options.keys()), key="form2_select")

        if selected_form1_key and selected_form2_key and selected_form1_key != selected_form2_key:
            form1_id = form_options[selected_form1_key]
            form2_id = form_options[selected_form2_key]

            # 获取表单数据
            df1 = system.get_form_data(form1_id)
            df2 = system.get_form_data(form2_id)

            if df1.empty or df2.empty:
                st.warning("请确保两个表单都有数据")
            else:
                # 第二步：选择关联字段
                st.markdown("#### 步骤2：选择关联字段")

                # 获取两个表单的共同字段
                common_columns = list(set(df1.columns) & set(df2.columns))

                if not common_columns:
                    st.warning("两个表单没有共同的字段，无法进行关联分析")
                else:
                    # 移除ID字段（如果有）
                    if 'id' in common_columns:
                        common_columns.remove('id')

                    # 让用户选择关联字段
                    join_column = st.selectbox(
                        "选择关联字段（两个表单都有的字段）",
                        common_columns,
                        help="选择用于关联两个表单的字段，如企业名称、联系人等",
                        key="join_column_select"
                    )

                    if join_column:
                        # ============= 新增：数据质量检查和关联统计 =============
                        st.markdown("#### 步骤3：关联统计概览")

                        # 检查关联字段的数据质量
                        df1_non_null = df1[join_column].notna().sum()
                        df2_non_null = df2[join_column].notna().sum()

                        # 统计唯一值数量
                        df1_unique = df1[join_column].nunique()
                        df2_unique = df2[join_column].nunique()

                        # 使用不同的连接方式获取数据
                        try:
                            # 内连接：获取成功关联的数据
                            merged_df_inner = pd.merge(df1, df2, on=join_column, how='inner',
                                                       suffixes=('_表单1', '_表单2'))

                            # 左连接：获取表单1的所有数据，包括未匹配的表单2数据
                            merged_df_left = pd.merge(df1, df2, on=join_column, how='left',
                                                      suffixes=('_表单1', '_表单2'), indicator=True)

                            # 右连接：获取表单2的所有数据，包括未匹配的表单1数据
                            merged_df_right = pd.merge(df1, df2, on=join_column, how='right',
                                                       suffixes=('_表单1', '_表单2'), indicator=True)

                            # 计算统计指标
                            matched_count = len(merged_df_inner)
                            unmatched_form1_count = len(merged_df_left[merged_df_left['_merge'] == 'left_only'])
                            unmatched_form2_count = len(merged_df_right[merged_df_right['_merge'] == 'right_only'])
                            total_form1_count = len(df1)
                            total_form2_count = len(df2)

                            # 创建关联统计卡片
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric(
                                    "成功关联记录",
                                    matched_count,
                                    f"{matched_count / total_form1_count * 100:.1f}%" if total_form1_count > 0 else "0%"
                                )

                            with col2:
                                st.metric(
                                    "表单1未匹配",
                                    unmatched_form1_count,
                                    f"{unmatched_form1_count / total_form1_count * 100:.1f}%" if total_form1_count > 0 else "0%"
                                )

                            with col3:
                                st.metric(
                                    "表单2未匹配",
                                    unmatched_form2_count,
                                    f"{unmatched_form2_count / total_form2_count * 100:.1f}%" if total_form2_count > 0 else "0%"
                                )

                            with col4:
                                st.metric(
                                    "总匹配率",
                                    f"{(matched_count / (total_form1_count + total_form2_count - matched_count)) * 100:.1f}%" if (
                                                                                                                                         total_form1_count + total_form2_count - matched_count) > 0 else "0%",
                                    delta=f"{matched_count}条"
                                )

                            # 使用选项卡显示不同类型的数据
                            data_tab1, data_tab2, data_tab3 = st.tabs([
                                "✅ 成功关联数据",
                                "⚠️ 表单1未匹配数据",
                                "⚠️ 表单2未匹配数据"
                            ])

                            with data_tab1:
                                st.markdown("##### ✅ 成功关联的数据")

                                if matched_count == 0:
                                    st.warning("没有找到匹配的记录，请检查关联字段的值是否一致")
                                else:
                                    # 显示合并后的数据预览
                                    with st.expander("查看成功关联的数据", expanded=False):
                                        st.dataframe(merged_df_inner.head(10), use_container_width=True)
                                        st.caption(f"共 {matched_count} 条关联记录")

                                    # 第四步：关联分析选项
                                    st.markdown("#### 步骤4：关联分析选项")

                                    analysis_type = st.radio(
                                        "选择分析类型",
                                        ["交叉统计", "关联对比", "相关性分析"],
                                        horizontal=True,
                                        key="analysis_type"
                                    )

                                    if analysis_type == "交叉统计":
                                        # 交叉统计：选择两个字段进行交叉分析
                                        st.markdown("##### 交叉统计")

                                        # 获取可用的字段（排除关联字段）
                                        available_columns = [col for col in merged_df_inner.columns if
                                                             col != join_column]

                                        col1, col2 = st.columns(2)
                                        with col1:
                                            x_column = st.selectbox(
                                                "选择X轴字段",
                                                available_columns,
                                                key="x_column_select"
                                            )

                                        with col2:
                                            y_column = st.selectbox(
                                                "选择Y轴字段",
                                                available_columns,
                                                key="y_column_select"
                                            )

                                        if x_column and y_column and x_column != y_column:
                                            try:
                                                # 创建交叉表
                                                if pd.api.types.is_numeric_dtype(
                                                        merged_df_inner[x_column]) and pd.api.types.is_numeric_dtype(
                                                    merged_df_inner[y_column]):
                                                    # 两个都是数值字段：散点图
                                                    fig = px.scatter(
                                                        merged_df_inner,
                                                        x=x_column,
                                                        y=y_column,
                                                        title=f"{x_column} vs {y_column} 散点图",
                                                        hover_data=[join_column]
                                                    )
                                                    st.plotly_chart(fig, use_container_width=True)

                                                    # 计算相关系数
                                                    correlation = merged_df_inner[x_column].corr(
                                                        merged_df_inner[y_column])
                                                    st.metric("相关系数", f"{correlation:.3f}")
                                                else:
                                                    # 至少有一个是非数值字段：交叉表
                                                    if pd.api.types.is_numeric_dtype(merged_df_inner[y_column]):
                                                        # Y是数值，X是非数值：分组统计
                                                        grouped = merged_df_inner.groupby(x_column)[y_column].agg(
                                                            ['mean', 'count', 'sum']).reset_index()
                                                        st.dataframe(grouped, use_container_width=True)

                                                        # 柱状图
                                                        fig = px.bar(
                                                            grouped,
                                                            x=x_column,
                                                            y='mean',
                                                            title=f"{x_column} 分组下 {y_column} 的平均值",
                                                            hover_data=['count', 'sum']
                                                        )
                                                        st.plotly_chart(fig, use_container_width=True)
                                                    else:
                                                        # 两个都是非数值字段：交叉频率表
                                                        cross_tab = pd.crosstab(merged_df_inner[x_column],
                                                                                merged_df_inner[y_column])
                                                        st.dataframe(cross_tab, use_container_width=True)

                                                        # 热力图
                                                        try:
                                                            fig = px.imshow(
                                                                cross_tab,
                                                                title=f"{x_column} 与 {y_column} 交叉热力图",
                                                                labels=dict(x=x_column, y=y_column, color="频次")
                                                            )
                                                            st.plotly_chart(fig, use_container_width=True)
                                                        except:
                                                            st.info("热力图显示需要足够的数据")
                                            except Exception as e:
                                                st.error(f"分析时出错: {str(e)}")

                                    elif analysis_type == "关联对比":
                                        # 关联对比：对比两个表单中的相同字段
                                        st.markdown("##### 关联对比分析")

                                        # 找出两个表单中名称相同但来源不同的字段
                                        form1_cols = [col for col in df1.columns if col != 'id']
                                        form2_cols = [col for col in df2.columns if col != 'id']
                                        common_field_names = list(set(form1_cols) & set(form2_cols) - {join_column})

                                        if not common_field_names:
                                            st.info("两个表单没有除关联字段外的共同字段")
                                        else:
                                            compare_field = st.selectbox(
                                                "选择要对比的字段",
                                                common_field_names,
                                                key="compare_field_select"
                                            )

                                            if compare_field:
                                                # 创建对比数据
                                                comparison_data = []
                                                for _, row in merged_df_inner.iterrows():
                                                    comparison_data.append({
                                                        join_column: row[join_column],
                                                        f"{selected_form1_key.split('(')[0].strip()}": row.get(
                                                            f"{compare_field}_表单1", row.get(compare_field)),
                                                        f"{selected_form2_key.split('(')[0].strip()}": row.get(
                                                            f"{compare_field}_表单2", row.get(compare_field))
                                                    })

                                                comparison_df = pd.DataFrame(comparison_data)

                                                # 显示对比表格
                                                st.dataframe(comparison_df, use_container_width=True)

                                                # 对比分析
                                                col1, col2 = st.columns(2)

                                                try:
                                                    # 转换为数值类型进行比较
                                                    col1_data = pd.to_numeric(comparison_df.iloc[:, 1], errors='coerce')
                                                    col2_data = pd.to_numeric(comparison_df.iloc[:, 2], errors='coerce')

                                                    with col1:
                                                        if not col1_data.isna().all():
                                                            st.metric(
                                                                f"{selected_form1_key.split('(')[0].strip()} 平均值",
                                                                f"{col1_data.mean():.2f}"
                                                            )
                                                    with col2:
                                                        if not col2_data.isna().all():
                                                            st.metric(
                                                                f"{selected_form2_key.split('(')[0].strip()} 平均值",
                                                                f"{col2_data.mean():.2f}"
                                                            )

                                                    # 差异分析
                                                    if not col1_data.isna().all() and not col2_data.isna().all():
                                                        # 计算差异
                                                        differences = col1_data - col2_data
                                                        st.metric("平均差异", f"{differences.mean():.2f}")

                                                        # 差异分布图
                                                        fig = px.histogram(
                                                            x=differences.dropna(),
                                                            title=f"{compare_field} 差异分布",
                                                            labels={"x": "差异值"}
                                                        )
                                                        st.plotly_chart(fig, use_container_width=True)
                                                except:
                                                    # 如果是文本字段，显示频次对比
                                                    st.info("当前字段为文本类型，显示频次对比")
                                                    freq1 = comparison_df.iloc[:, 1].value_counts().head(10)
                                                    freq2 = comparison_df.iloc[:, 2].value_counts().head(10)

                                                    col1, col2 = st.columns(2)
                                                    with col1:
                                                        st.write(
                                                            f"**{selected_form1_key.split('(')[0].strip()} 频次:**")
                                                        st.dataframe(freq1, use_container_width=True)
                                                    with col2:
                                                        st.write(
                                                            f"**{selected_form2_key.split('(')[0].strip()} 频次:**")
                                                        st.dataframe(freq2, use_container_width=True)

                                    elif analysis_type == "相关性分析":
                                        # 相关性分析：计算数值字段之间的相关性
                                        st.markdown("##### 相关性分析")

                                        # 获取所有数值字段
                                        numeric_columns = merged_df_inner.select_dtypes(
                                            include=['int64', 'float64']).columns.tolist()

                                        if len(numeric_columns) < 2:
                                            st.warning("合并数据中至少需要2个数值字段进行相关性分析")
                                        else:
                                            # 让用户选择要分析的字段
                                            selected_columns = st.multiselect(
                                                "选择要分析的数值字段",
                                                numeric_columns,
                                                default=numeric_columns[:min(5, len(numeric_columns))],
                                                key="correlation_columns"
                                            )

                                            if len(selected_columns) >= 2:
                                                # 计算相关系数矩阵
                                                corr_matrix = merged_df_inner[selected_columns].corr()

                                                # 显示相关系数矩阵
                                                st.markdown("**相关系数矩阵:**")
                                                st.dataframe(
                                                    corr_matrix.style.background_gradient(cmap='coolwarm', axis=None),
                                                    use_container_width=True)

                                                # 创建相关性热力图
                                                try:
                                                    fig = px.imshow(
                                                        corr_matrix,
                                                        title="相关性热力图",
                                                        labels=dict(color="相关系数"),
                                                        x=selected_columns,
                                                        y=selected_columns,
                                                        color_continuous_scale='RdBu',
                                                        zmin=-1,
                                                        zmax=1
                                                    )
                                                    st.plotly_chart(fig, use_container_width=True)
                                                except Exception as e:
                                                    st.error(f"创建热力图时出错: {str(e)}")

                                                # 显示强相关性对
                                                st.markdown("**强相关性分析 (|r| > 0.7):**")
                                                strong_correlations = []
                                                for i in range(len(selected_columns)):
                                                    for j in range(i + 1, len(selected_columns)):
                                                        corr_value = corr_matrix.iloc[i, j]
                                                        if abs(corr_value) > 0.7:
                                                            strong_correlations.append({
                                                                "字段1": selected_columns[i],
                                                                "字段2": selected_columns[j],
                                                                "相关系数": f"{corr_value:.3f}",
                                                                "关系": "强正相关" if corr_value > 0 else "强负相关"
                                                            })

                                                if strong_correlations:
                                                    st.dataframe(pd.DataFrame(strong_correlations),
                                                                 use_container_width=True)
                                                else:
                                                    st.info("未发现强相关性 (|r| > 0.7)")

                            with data_tab2:
                                st.markdown(f"##### ⚠️ 表单1未匹配的数据")
                                st.info(f"表单1中有 {unmatched_form1_count} 条记录在表单2中没有匹配项")

                                if unmatched_form1_count > 0:
                                    # 获取未匹配的数据
                                    unmatched_form1_df = merged_df_left[merged_df_left['_merge'] == 'left_only'].copy()
                                    # 移除_indicator列
                                    if '_merge' in unmatched_form1_df.columns:
                                        unmatched_form1_df = unmatched_form1_df.drop('_merge', axis=1)

                                    # 显示未匹配数据
                                    st.dataframe(unmatched_form1_df, use_container_width=True)

                                    # 分析未匹配原因
                                    st.markdown("##### 🔍 未匹配原因分析")

                                    col1, col2 = st.columns(2)
                                    with col1:
                                        # 查看关联字段的分布
                                        if join_column in unmatched_form1_df.columns:
                                            value_counts = unmatched_form1_df[join_column].value_counts().head(10)
                                            st.write(f"**未匹配的 {join_column} 分布 (Top 10):**")
                                            st.dataframe(value_counts, use_container_width=True)

                                    with col2:
                                        # 导出未匹配数据
                                        csv1 = unmatched_form1_df.to_csv(index=False).encode('utf-8-sig')
                                        st.download_button(
                                            label=f"📥 导出表单1未匹配数据 ({unmatched_form1_count}条)",
                                            data=csv1,
                                            file_name=f"未匹配数据_{selected_form1_key.split('(')[0].strip()}_表单1_{datetime.now().strftime('%Y%m%d')}.csv",
                                            mime="text/csv",
                                            use_container_width=True
                                        )

                                    # 建议改进措施
                                    st.markdown("##### 💡 改进建议")
                                    st.write("""
                                    1. **检查关联字段的格式**：确保两个表单中的关联字段格式一致（如大小写、空格等）
                                    2. **检查数据准确性**：核实表单1中的关联字段值在表单2中是否存在
                                    3. **考虑使用其他关联字段**：如果当前字段匹配率低，可以尝试其他共同字段
                                    4. **数据清洗**：清理关联字段中的异常值、空白字符等
                                    """)

                                else:
                                    st.success("表单1中的所有记录都在表单2中找到了匹配项！")

                            with data_tab3:
                                st.markdown(f"##### ⚠️ 表单2未匹配的数据")
                                st.info(f"表单2中有 {unmatched_form2_count} 条记录在表单1中没有匹配项")

                                if unmatched_form2_count > 0:
                                    # 获取未匹配的数据
                                    unmatched_form2_df = merged_df_right[
                                        merged_df_right['_merge'] == 'right_only'].copy()
                                    # 移除_indicator列
                                    if '_merge' in unmatched_form2_df.columns:
                                        unmatched_form2_df = unmatched_form2_df.drop('_merge', axis=1)

                                    # 显示未匹配数据
                                    st.dataframe(unmatched_form2_df, use_container_width=True)

                                    # 分析未匹配原因
                                    st.markdown("##### 🔍 未匹配原因分析")

                                    col1, col2 = st.columns(2)
                                    with col1:
                                        # 查看关联字段的分布
                                        if join_column in unmatched_form2_df.columns:
                                            value_counts = unmatched_form2_df[join_column].value_counts().head(10)
                                            st.write(f"**未匹配的 {join_column} 分布 (Top 10):**")
                                            st.dataframe(value_counts, use_container_width=True)

                                    with col2:
                                        # 导出未匹配数据
                                        csv2 = unmatched_form2_df.to_csv(index=False).encode('utf-8-sig')
                                        st.download_button(
                                            label=f"📥 导出表单2未匹配数据 ({unmatched_form2_count}条)",
                                            data=csv2,
                                            file_name=f"未匹配数据_{selected_form2_key.split('(')[0].strip()}_表单2_{datetime.now().strftime('%Y%m%d')}.csv",
                                            mime="text/csv",
                                            use_container_width=True
                                        )

                                    # 建议改进措施
                                    st.markdown("##### 💡 改进建议")
                                    st.write("""
                                    1. **检查关联字段的格式**：确保两个表单中的关联字段格式一致
                                    2. **检查数据完整性**：核实表单2中的关联字段值是否在表单1中存在
                                    3. **考虑使用其他关联字段**：如果当前字段匹配率低，可以尝试其他共同字段
                                    4. **数据清洗**：清理关联字段中的异常值、空白字符等
                                    """)

                                else:
                                    st.success("表单2中的所有记录都在表单1中找到了匹配项！")

                            # 数据导出功能
                            st.markdown("---")
                            st.markdown("#### 💾 数据导出")

                            col1, col2, col3 = st.columns(3)

                            with col1:
                                # 导出成功关联数据
                                if matched_count > 0:
                                    csv_matched = merged_df_inner.to_csv(index=False).encode('utf-8-sig')
                                    st.download_button(
                                        label=f"📥 导出成功关联数据",
                                        data=csv_matched,
                                        file_name=f"成功关联_{selected_form1_key.split('(')[0].strip()}_vs_{selected_form2_key.split('(')[0].strip()}_{datetime.now().strftime('%Y%m%d')}.csv",
                                        mime="text/csv",
                                        use_container_width=True
                                    )

                            with col2:
                                # 导出表单1未匹配数据
                                if unmatched_form1_count > 0:
                                    csv_unmatched1 = unmatched_form1_df.to_csv(index=False).encode('utf-8-sig')
                                    st.download_button(
                                        label=f"📥 导出表单1未匹配数据",
                                        data=csv_unmatched1,
                                        file_name=f"表单1未匹配_{selected_form1_key.split('(')[0].strip()}_{datetime.now().strftime('%Y%m%d')}.csv",
                                        mime="text/csv",
                                        use_container_width=True
                                    )

                            with col3:
                                # 导出表单2未匹配数据
                                if unmatched_form2_count > 0:
                                    csv_unmatched2 = unmatched_form2_df.to_csv(index=False).encode('utf-8-sig')
                                    st.download_button(
                                        label=f"📥 导出表单2未匹配数据",
                                        data=csv_unmatched2,
                                        file_name=f"表单2未匹配_{selected_form2_key.split('(')[0].strip()}_{datetime.now().strftime('%Y%m%d')}.csv",
                                        mime="text/csv",
                                        use_container_width=True
                                    )

                        except Exception as e:
                            st.error(f"分析时出错: {str(e)}")
        else:
            if selected_form1_key and selected_form2_key and selected_form1_key == selected_form2_key:
                st.warning("请选择两个不同的表单进行关联分析")


def show_import_export():
    """显示导入导出页面"""
    st.markdown('<h2 class="sub-header">📤 导入导出</h2>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["📥 数据导入", "📤 数据导出"])

    with tab1:
        st.markdown("### 数据导入")

        st.markdown("""
        <div class="card">
        <strong>导入说明:</strong><br>
        1. 支持导入 Excel (.xlsx, .xls) 和 CSV (.csv) 格式文件<br>
        2. 文件第一行应为列标题，与表单字段对应<br>
        3. 可以选择导入到现有表单或新建表单<br>
        4. 导入前请确保数据格式正确
        </div>
        """, unsafe_allow_html=True)

        # 文件上传
        uploaded_file = st.file_uploader("选择文件", type=['csv', 'xlsx', 'xls'])

        if uploaded_file is not None:
            try:
                # 读取文件
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file, encoding='utf-8')
                else:
                    df = pd.read_excel(uploaded_file)

                st.success(f"成功读取 {len(df)} 行数据")
                st.dataframe(df.head(), use_container_width=True)

                # 选择导入到哪个表单
                forms = system.get_forms()
                if forms:
                    form_options = {f"{form_name} (ID: {form_id})": form_id for form_id, form_name in forms}
                    selected_form_key = st.selectbox("导入到表单", list(form_options.keys()))

                    if st.button("开始导入", type="primary"):
                        form_id = form_options[selected_form_key]
                        system.save_form_data(form_id, df)
                        st.success(f"成功导入 {len(df)} 条记录")
                else:
                    st.warning("请先创建表单")

            except Exception as e:
                st.error(f"导入失败: {str(e)}")

    with tab2:
        st.markdown("### 数据导出")

        forms = system.get_forms()
        if forms:
            form_options = {f"{form_name} (ID: {form_id})": form_id for form_id, form_name in forms}
            selected_form_key = st.selectbox("选择要导出的表单", list(form_options.keys()), key="export_form_select")

            if selected_form_key:
                form_id = form_options[selected_form_key]
                df = system.get_form_data(form_id)

                if not df.empty:
                    st.info(f"共 {len(df)} 条记录")

                    # 导出格式选择
                    export_format = st.radio("导出格式", ["CSV", "Excel"], horizontal=True)

                    if export_format == "CSV":
                        csv = df.to_csv(index=False).encode('utf-8-sig')
                        st.download_button(
                            label="下载CSV文件",
                            data=csv,
                            file_name=f"{selected_form_key.split('(')[0].strip()}_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv"
                        )
                    else:
                        try:
                            output = io.BytesIO()
                            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                df.to_excel(writer, index=False, sheet_name='Sheet1')
                            excel_data = output.getvalue()

                            st.download_button(
                                label="下载Excel文件",
                                data=excel_data,
                                file_name=f"{selected_form_key.split('(')[0].strip()}_{datetime.now().strftime('%Y%m%d')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        except:
                            st.error("请安装openpyxl库: `pip install openpyxl`")


def show_user_management():
    """显示用户管理页面"""
    st.markdown("### 👥 用户管理")

    # 检查权限
    if not check_permission("manage_users"):
        st.error("需要用户管理权限")
        return

    # 用户列表
    st.markdown("#### 📋 用户列表")

    users = system.get_all_users()

    if users:
        # 创建用户数据表格
        user_data = []
        for user in users:
            user_data.append({
                "ID": user[0],
                "用户名": user[1],
                "姓名": user[2] or "-",
                "邮箱": user[3] or "-",
                "部门": user[5] or "-",
                "角色": user[6],
                "状态": "活跃" if user[7] == 1 else "禁用",
                "最后登录": user[8] or "从未登录",
                "创建时间": user[9]
            })

        df = pd.DataFrame(user_data)
        st.dataframe(df, use_container_width=True, height=400)

        # 用户操作
        st.markdown("#### ⚙️ 用户操作")

        col1, col2 = st.columns(2)

        with col1:
            # 添加用户
            with st.expander("➕ 添加新用户", expanded=False):
                with st.form("add_user_form"):
                    new_username = st.text_input("用户名*", key="new_username")
                    new_password = st.text_input("密码*", type="password", key="new_password")
                    confirm_password = st.text_input("确认密码*", type="password", key="confirm_password")
                    full_name = st.text_input("姓名", key="full_name")
                    email = st.text_input("邮箱", key="email")
                    phone = st.text_input("电话", key="phone")
                    department = st.text_input("部门", key="department")

                    # 获取可用角色
                    roles = system.get_all_roles()
                    role_options = {role[0]: role[1] for role in roles}
                    selected_role = st.selectbox("角色", list(role_options.keys()),
                                                 format_func=lambda x: f"{x} - {role_options.get(x, '')}")

                    is_active = st.checkbox("启用账户", value=True, key="is_active")

                    if st.form_submit_button("添加用户", type="primary"):
                        if not new_username or not new_password:
                            st.error("用户名和密码为必填项")
                        elif new_password != confirm_password:
                            st.error("两次输入的密码不一致")
                        else:
                            try:
                                user_id = system.create_user(
                                    username=new_username,
                                    password=new_password,
                                    full_name=full_name,
                                    email=email,
                                    phone=phone,
                                    department=department,
                                    role=selected_role,
                                    is_active=is_active
                                )
                                st.success(f"用户 '{new_username}' 添加成功！ID: {user_id}")
                                st.rerun()
                            except ValueError as e:
                                st.error(str(e))

        with col2:
            # 编辑用户
            with st.expander("✏️ 编辑用户", expanded=False):
                user_options = {f"{user[1]} (ID: {user[0]})": user[0] for user in users}
                selected_user_key = st.selectbox("选择用户", list(user_options.keys()), key="edit_user_select")

                if selected_user_key:
                    user_id = user_options[selected_user_key]
                    user = system.get_user_by_id(user_id)

                    if user:
                        with st.form("edit_user_form"):
                            full_name = st.text_input("姓名", value=user[2] or "", key="edit_full_name")
                            email = st.text_input("邮箱", value=user[3] or "", key="edit_email")
                            phone = st.text_input("电话", value=user[4] or "", key="edit_phone")
                            department = st.text_input("部门", value=user[5] or "", key="edit_department")

                            # 获取可用角色
                            roles = system.get_all_roles()
                            role_options = {role[0]: role[1] for role in roles}

                            # 获取当前角色
                            current_role = user[6]
                            selected_role = st.selectbox("角色", list(role_options.keys()),
                                                         index=list(role_options.keys()).index(current_role)
                                                         if current_role in role_options else 0,
                                                         format_func=lambda x: f"{x} - {role_options.get(x, '')}")

                            # 用户特定权限
                            permissions_json = user[7]
                            if permissions_json:
                                try:
                                    current_permissions = json.loads(permissions_json)
                                except:
                                    current_permissions = []
                            else:
                                current_permissions = []

                            # 权限选择
                            st.markdown("**用户特定权限**")
                            selected_permissions = []

                            for perm_code, perm_name in system.PERMISSIONS.items():
                                if perm_code != "all":  # 排除all权限
                                    is_checked = st.checkbox(
                                        perm_name,
                                        value=perm_code in current_permissions,
                                        key=f"perm_{user_id}_{perm_code}"
                                    )
                                    if is_checked:
                                        selected_permissions.append(perm_code)

                            is_active = st.checkbox("启用账户", value=user[8] == 1, key="edit_is_active")

                            col_save, col_delete = st.columns(2)

                            with col_save:
                                if st.form_submit_button("💾 保存修改", type="primary"):
                                    system.update_user(
                                        user_id,
                                        full_name=full_name,
                                        email=email,
                                        phone=phone,
                                        department=department,
                                        role=selected_role,
                                        permissions=selected_permissions,
                                        is_active=is_active
                                    )
                                    st.success(f"用户信息已更新")
                                    st.rerun()

                            with col_delete:
                                if st.form_submit_button("🗑️ 删除用户", type="secondary"):
                                    try:
                                        system.delete_user(user_id)
                                        st.success(f"用户已删除")
                                        st.rerun()
                                    except ValueError as e:
                                        st.error(str(e))

    else:
        st.info("暂无用户数据")


def show_role_management():
    """显示角色管理页面"""
    st.markdown("### 🎭 角色管理")

    # 检查权限
    if not check_permission("manage_roles"):
        st.error("需要角色管理权限")
        return

    roles = system.get_all_roles()

    if roles:
        # 角色列表
        st.markdown("#### 📋 角色列表")

        role_data = []
        for role in roles:
            role_name, description, permissions_json = role
            try:
                permissions = json.loads(permissions_json)
                permission_count = len(permissions)
            except:
                permissions = []
                permission_count = 0

            role_data.append({
                "角色名称": role_name,
                "描述": description or "-",
                "权限数量": permission_count,
                "权限": ", ".join([system.PERMISSIONS.get(p, p) for p in permissions[:3]]) +
                        ("..." if len(permissions) > 3 else "")
            })

        df = pd.DataFrame(role_data)
        st.dataframe(df, use_container_width=True)

        # 编辑角色权限
        st.markdown("#### ⚙️ 编辑角色权限")

        role_options = {role[0]: role[1] for role in roles}
        selected_role = st.selectbox("选择角色", list(role_options.keys()),
                                     format_func=lambda x: f"{x} - {role_options.get(x, '')}",
                                     key="role_select")

        if selected_role:
            # 获取当前角色的权限
            current_role = next((r for r in roles if r[0] == selected_role), None)

            if current_role:
                role_name, description, permissions_json = current_role

                try:
                    current_permissions = json.loads(permissions_json)
                except:
                    current_permissions = []

                with st.form("edit_role_form"):
                    new_description = st.text_input("角色描述", value=description or "", key="role_description")

                    st.markdown("**角色权限**")

                    # 按类别分组显示权限
                    permission_categories = {}
                    for perm_code, perm_name in system.PERMISSIONS.items():
                        if perm_code == "all":
                            category = "特殊权限"
                        elif perm_code.startswith("view_"):
                            category = "查看权限"
                        elif perm_code.startswith("manage_"):
                            category = "管理权限"
                        elif perm_code.startswith("create_"):
                            category = "创建权限"
                        elif perm_code.startswith("edit_"):
                            category = "编辑权限"
                        elif perm_code.startswith("delete_"):
                            category = "删除权限"
                        elif perm_code.startswith("import_") or perm_code.startswith("export_"):
                            category = "导入导出权限"
                        else:
                            category = "其他权限"

                        if category not in permission_categories:
                            permission_categories[category] = []

                        permission_categories[category].append((perm_code, perm_name))

                    selected_permissions = []

                    for category, perms in permission_categories.items():
                        with st.expander(f"📁 {category} ({len(perms)}个权限)", expanded=False):
                            for perm_code, perm_name in perms:
                                is_checked = st.checkbox(
                                    perm_name,
                                    value=perm_code in current_permissions,
                                    key=f"role_perm_{role_name}_{perm_code}"
                                )
                                if is_checked:
                                    selected_permissions.append(perm_code)

                    if st.form_submit_button("💾 保存角色权限", type="primary"):
                        system.update_role_permissions(
                            role_name,
                            selected_permissions,
                            new_description
                        )
                        st.success(f"角色 '{role_name}' 权限已更新")
                        st.rerun()

    else:
        st.info("暂无角色数据")


def show_operation_logs():
    """显示操作日志页面"""
    st.markdown("### 📋 操作日志")

    # 检查权限
    if not check_permission("view_logs"):
        st.error("需要查看日志权限")
        return

    # 日志筛选选项
    col1, col2, col3 = st.columns(3)

    with col1:
        log_limit = st.number_input("显示条数", min_value=10, max_value=1000, value=100, step=10)

    with col2:
        operation_filter = st.selectbox(
            "操作类型",
            ["所有", "login", "logout", "create_user", "update_user", "delete_user",
             "create_form", "update_form", "delete_form", "create_data", "update_data", "delete_data"]
        )

    with col3:
        if st.button("🔄 刷新日志", use_container_width=True):
            st.rerun()

    # 获取日志
    operation = None if operation_filter == "所有" else operation_filter
    logs = system.get_operation_logs(limit=log_limit, operation=operation)

    if logs:
        # 处理日志数据
        log_data = []
        for log in logs:
            log_id, user_id, username, operation, target_type, target_id, details_json, ip_address, operation_time, full_name = log

            # 解析详情
            details = ""
            if details_json:
                try:
                    details_obj = json.loads(details_json)
                    details = ", ".join([f"{k}: {v}" for k, v in details_obj.items()])
                except:
                    details = details_json

            log_data.append({
                "时间": operation_time,
                "用户": f"{username} ({full_name or '未知'})",
                "操作": operation,
                "目标类型": target_type or "-",
                "目标ID": target_id or "-",
                "详情": details[:50] + "..." if len(details) > 50 else details,
                "IP地址": ip_address or "-"
            })

        df = pd.DataFrame(log_data)
        st.dataframe(df, use_container_width=True, height=500)

        # 日志统计
        st.markdown("#### 📊 日志统计")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_logs = len(logs)
            st.metric("总日志数", total_logs)

        with col2:
            unique_users = len(set(log[2] for log in logs))
            st.metric("操作用户数", unique_users)

        with col3:
            unique_operations = len(set(log[3] for log in logs))
            st.metric("操作类型数", unique_operations)

        with col4:
            # 导出日志按钮
            csv = df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 导出日志",
                data=csv,
                file_name=f"操作日志_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )

    else:
        st.info("暂无操作日志")


def show_database_management():
    """显示数据库管理页面"""
    st.markdown("### 🗄️ 数据库管理")

    # 检查权限
    if not check_permission("system_settings"):
        st.error("需要系统设置权限")
        return

    # 数据库信息
    stats = system.get_database_stats()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("数据库大小", f"{stats['db_size_kb']:.2f} KB")

    with col2:
        st.metric("表单数量", stats['form_count'])

    with col3:
        st.metric("数据记录", stats['data_count'])

    with col4:
        st.metric("用户数量", stats['user_count'])

    # 数据库操作
    st.markdown("#### ⚙️ 数据库操作")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("💾 备份数据库", type="primary", use_container_width=True):
            import shutil
            import time

            backup_dir = "backups"
            os.makedirs(backup_dir, exist_ok=True)

            timestamp = time.strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(backup_dir, f"企业帮扶台账_备份_{timestamp}.db")

            try:
                shutil.copy2(system.db_path, backup_path)
                st.success(f"数据库已备份到: {backup_path}")

                # 记录操作
                system.log_operation(
                    user_id=st.session_state.current_user['id'],
                    username=st.session_state.current_user['username'],
                    operation="database_backup",
                    details={"backup_path": backup_path}
                )
            except Exception as e:
                st.error(f"备份失败: {str(e)}")

    with col2:
        # 清理旧数据选项
        with st.expander("🧹 数据清理", expanded=False):
            days_to_keep = st.number_input("保留天数", min_value=1, max_value=365, value=30, key="days_to_keep")

            if st.button("清理旧日志", use_container_width=True):
                try:
                    conn = sqlite3.connect(system.db_path)
                    cursor = conn.cursor()

                    # 清理旧的操作日志
                    cursor.execute('''
                        DELETE FROM operation_logs 
                        WHERE julianday('now') - julianday(operation_time) > ?
                    ''', (days_to_keep,))

                    deleted_logs = cursor.rowcount

                    # 清理旧的登录日志
                    cursor.execute('''
                        DELETE FROM login_logs 
                        WHERE julianday('now') - julianday(login_time) > ?
                    ''', (days_to_keep,))

                    deleted_login_logs = cursor.rowcount

                    conn.commit()
                    conn.close()

                    st.success(f"清理完成！删除操作日志: {deleted_logs} 条，登录日志: {deleted_login_logs} 条")

                    # 记录操作
                    system.log_operation(
                        user_id=st.session_state.current_user['id'],
                        username=st.session_state.current_user['username'],
                        operation="cleanup_old_data",
                        details={"days_to_keep": days_to_keep, "deleted_logs": deleted_logs,
                                 "deleted_login_logs": deleted_login_logs}
                    )

                except Exception as e:
                    st.error(f"清理失败: {str(e)}")

    with col3:
        # 数据库优化
        if st.button("⚡ 优化数据库", use_container_width=True):
            try:
                conn = sqlite3.connect(system.db_path)
                cursor = conn.cursor()

                # 执行VACUUM命令优化数据库
                cursor.execute("VACUUM")

                conn.commit()
                conn.close()

                st.success("数据库优化完成！")

                # 记录操作
                system.log_operation(
                    user_id=st.session_state.current_user['id'],
                    username=st.session_state.current_user['username'],
                    operation="database_optimize"
                )

            except Exception as e:
                st.error(f"优化失败: {str(e)}")


def show_system_info():
    """显示系统信息页面"""
    st.markdown("### ℹ️ 系统信息")

    info = {
        "系统名称": "“一中心一基地”信息管理系统 ",
        "版本": "2.0.0",
        "数据库": "SQLite",
        "开发语言": "Python 3.8+",
        "界面框架": "Streamlit",
        "数据格式支持": "CSV, Excel",
        "安全特性": "用户权限管理、操作日志、数据备份",
        "开发者": "一中心一基地",
        "最后更新": "2026年1月",
        "技术支持": "support@example.com"
    }

    for key, value in info.items():
        st.text(f"• {key}: {value}")

    st.markdown("---")

    # 系统状态
    st.markdown("#### 📊 系统状态")

    # 检查数据库连接
    try:
        conn = sqlite3.connect(system.db_path)
        cursor = conn.cursor()

        # 检查表状态
        tables = ["users", "form_definitions", "form_data", "role_permissions", "login_logs", "operation_logs"]
        table_status = {}

        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                table_status[table] = {"status": "正常", "记录数": count}
            except:
                table_status[table] = {"status": "异常", "记录数": 0}

        conn.close()

        # 显示表状态
        for table, status in table_status.items():
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.text(table)
            with col2:
                if status["status"] == "正常":
                    st.success("正常")
                else:
                    st.error("异常")
            with col3:
                st.text(f"{status['记录数']} 条")

    except Exception as e:
        st.error(f"数据库连接异常: {str(e)}")


def show_system_settings():
    """显示系统设置页面"""
    st.markdown('<h2 class="sub-header">⚙️ 系统设置</h2>', unsafe_allow_html=True)

    # 检查权限
    if not check_permission("system_settings"):
        st.error("需要系统设置权限访问此页面")
        return

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["👥 用户管理", "🎭 角色管理", "📋 操作日志", "🗄️ 数据库管理", "ℹ️ 系统信息"])

    with tab1:
        show_user_management()

    with tab2:
        show_role_management()

    with tab3:
        show_operation_logs()

    with tab4:
        show_database_management()

    with tab5:
        show_system_info()


def main():
    """主函数 - 使用Streamlit运行"""

    # 初始化session state
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'current_user' not in st.session_state:
        st.session_state.current_user = None
    if 'current_form' not in st.session_state:
        st.session_state.current_form = None
    if 'user_permissions' not in st.session_state:
        st.session_state.user_permissions = []
    if 'selected_menu' not in st.session_state:
        st.session_state.selected_menu = "🏠 仪表盘"

    # 显示标题
    st.markdown('<h1 class="main-header">“一中心一基地”信息管理系统 </h1>',
                unsafe_allow_html=True)

    # 侧边栏
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/business.png", width=80)

        if not st.session_state.logged_in:
            # 登录表单
            st.markdown("## 🔐 用户登录")

            username = st.text_input("用户名", value="admin")
            password = st.text_input("密码", type="password", value="admin123")

            # 获取客户端信息（简化版本）
            user_agent = "Unknown"  # 简化处理

            col1, col2 = st.columns(2)
            with col1:
                if st.button("登录", type="primary", use_container_width=True):
                    if username and password:
                        user = system.login(username, password, user_agent=user_agent)
                        if user:
                            st.session_state.logged_in = True
                            st.session_state.current_user = user
                            st.session_state.user_permissions = system.get_user_permissions(user['id'])
                            st.success(f"登录成功！欢迎 {user['username']}")
                            st.rerun()
                        else:
                            st.error("用户名或密码错误")
                    else:
                        st.error("请输入用户名和密码")

            with col2:
                if st.button("注册", use_container_width=True):
                    st.info("请联系管理员注册新用户")

            # 显示系统功能
            st.markdown("---")
            st.markdown("### 📋 系统功能")
            st.write("""
                - 📊 数据管理
                - 📋 表单管理
                - 📈 统计分析
                - 📤 导入导出
                - 🖨️ 打印功能
                - 🔒 权限管理
                """)
        else:
            # 用户信息和导航
            current_user = st.session_state.current_user
            st.markdown(f"### 👤 欢迎, {current_user['username']}")
            st.markdown(f"角色: **{current_user['role']}**")

            # 显示用户权限（管理员可见）
            if check_permission("manage_users"):
                with st.expander("查看我的权限"):
                    for perm in st.session_state.user_permissions:
                        perm_name = system.PERMISSIONS.get(perm, perm)
                        st.text(f"• {perm_name}")

            st.markdown("---")

            # 根据权限显示导航菜单
            menu_options = ["🏠 仪表盘"]

            if check_permission("view_data"):
                menu_options.append("📊 数据管理")

            if check_permission("view_forms"):
                menu_options.append("📋 表单管理")

            if check_permission("view_reports"):
                menu_options.append("📈 统计分析")

            if check_permission("export_data"):
                menu_options.append("📤 导入导出")

            if check_permission("system_settings"):
                menu_options.append("⚙️ 系统设置")

            selected_menu = st.selectbox("功能菜单", menu_options, key="main_sidebar_menu")
            st.session_state.selected_menu = selected_menu

            st.markdown("---")

            if st.button("退出登录", type="secondary", use_container_width=True):
                # 记录登出操作
                system.log_operation(
                    user_id=current_user['id'],
                    username=current_user['username'],
                    operation="logout"
                )

                st.session_state.logged_in = False
                st.session_state.current_user = None
                st.session_state.current_form = None
                st.session_state.user_permissions = []
                st.session_state.selected_menu = "🏠 仪表盘"
                st.rerun()

            # 当前选择的状态
            if st.session_state.current_form:
                forms = system.get_forms()
                for form_id, form_name in forms:
                    if form_id == st.session_state.current_form:
                        st.info(f"当前表单: {form_name}")

    # 主内容区
    if not st.session_state.logged_in:
        # 未登录状态 - 显示欢迎信息
        show_welcome_page()
    else:
        # 已登录状态 - 根据菜单显示内容
        selected_menu = st.session_state.selected_menu

        if selected_menu == "🏠 仪表盘":
            show_dashboard()
        elif selected_menu == "📊 数据管理":
            show_data_management()
        elif selected_menu == "📋 表单管理":
            show_form_management()
        elif selected_menu == "📈 统计分析":
            show_statistical_analysis()
        elif selected_menu == "📤 导入导出":
            show_import_export()
        elif selected_menu == "⚙️ 系统设置":
            show_system_settings()

if __name__ == "__main__":
    main()