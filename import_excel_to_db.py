"""
导入 Excel 数据到 enterprise_support.db
用法：
    python import_excel_to_db.py <excel_file> [<db_path>]

参数：
    excel_file : 要导入的 Excel 文件路径
    db_path    : 数据库文件路径（可选，默认 ./enterprise_support.db）
"""

import sqlite3
import json
import sys
import os
import pandas as pd
from datetime import datetime

def create_form_if_not_exists(cursor, form_name, columns):
    """
    如果表单不存在，则创建表单定义，并返回 form_id。
    如果已存在，直接返回现有 form_id。
    """
    # 检查表单是否存在
    cursor.execute("SELECT id FROM form_definitions WHERE form_name = ?", (form_name,))
    row = cursor.fetchone()
    if row:
        return row[0]

    # 创建表单字段列表
    fields = []
    for col in columns:
        # 移除列名中可能存在的换行符和多余空格
        clean_name = col.replace('\n', ' ').strip()
        fields.append({
            "name": clean_name,
            "type": "text",      # 统一使用文本类型
            "required": False
        })

    form_config = {
        "form_name": form_name,
        "fields": fields
    }

    cursor.execute(
        "INSERT INTO form_definitions (form_name, form_config) VALUES (?, ?)",
        (form_name, json.dumps(form_config, ensure_ascii=False))
    )
    return cursor.lastrowid

def import_sheet_to_db(cursor, df, form_id):
    """将 DataFrame 数据插入到指定 form_id 的表单中"""
    # 获取列名列表（用于 JSON 键）
    columns = df.columns.tolist()
    # 清理列名中的换行符
    columns = [c.replace('\n', ' ').strip() for c in columns]

    inserted = 0
    for _, row in df.iterrows():
        # 如果整行全为空，跳过
        if row.isnull().all():
            continue

        # 构建 JSON 数据
        record = {}
        for col in columns:
            val = row[col]
            # 处理 NaN、NaT 等空值
            if pd.isna(val):
                record[col] = ""
            elif isinstance(val, (pd.Timestamp, datetime)):
                record[col] = val.strftime("%Y-%m-%d %H:%M:%S")
            else:
                record[col] = str(val)

        # 插入数据库
        cursor.execute(
            "INSERT INTO form_data (form_id, data_json) VALUES (?, ?)",
            (form_id, json.dumps(record, ensure_ascii=False))
        )
        inserted += 1

    return inserted

def main():
    # 解析命令行参数
    if len(sys.argv) < 2:
        print("用法: python import_excel_to_db.py <excel_file> [<db_path>]")
        sys.exit(1)

    excel_file = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else "enterprise_support.db"

    # 检查文件是否存在
    if not os.path.exists(excel_file):
        print(f"错误：Excel 文件 '{excel_file}' 不存在")
        sys.exit(1)

    # 连接数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 读取 Excel 文件
    try:
        excel_data = pd.read_excel(excel_file, sheet_name=None, header=0)
    except Exception as e:
        print(f"读取 Excel 文件失败：{e}")
        sys.exit(1)

    total_inserted = 0
    for sheet_name, df in excel_data.items():
        print(f"正在处理工作表：{sheet_name}")

        # 如果 DataFrame 为空，跳过
        if df.empty:
            print(f"工作表 {sheet_name} 为空，跳过")
            continue

        # 创建或获取表单 ID
        form_id = create_form_if_not_exists(cursor, sheet_name, df.columns)

        # 导入数据
        inserted = import_sheet_to_db(cursor, df, form_id)
        total_inserted += inserted
        print(f"  已导入 {inserted} 行数据")

    # 提交事务
    conn.commit()
    conn.close()

    print(f"\n导入完成！共导入 {total_inserted} 行数据到 {db_path}")

if __name__ == "__main__":
    main()