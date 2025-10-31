#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取Baostock信息工具
注意：这不是项目的主程序，而是被工作流调用的工具脚本
"""

# ================================
# 1. 导入模块和配置
# ================================

import baostock as bs
import inspect
import os
import logging
import time
from datetime import datetime
import traceback
import sys
import json
from pprint import pformat

# 配置日志
logging.basicConfig(level=logging.ERROR)

# 正确导入git_utils模块（只有一行，与项目其他文件完全一致）
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.git_utils import commit_files_in_batches

# ================================
# 2. 全局常量/参数定义
# ================================

# API测试参数
API_TEST_PARAMS = {
    # 标准测试代码 - 为不同类型的API使用合适的测试代码
    "TEST_CODES": {
        "stock": "sh.600000",   # 贵州茅台 - A股股票
        "index": "sh.000001",   # 上证指数
    },
    
    # API类型识别关键词
    "API_TYPE_KEYWORDS": {
        "stock": ["stock", "k_data", "dividend", "balance", "income", "cash_flow"],
        "index": ["index"]
    },
    
    # 输出参数
    "SHOW_DATA_SAMPLE": True,   # 是否显示数据示例
    "SAMPLE_ROWS": 100,           # 数据示例显示的行数
    "VERBOSE": True             # 是否显示详细日志
}

# 文件和目录参数
FILE_PARAMS = {
    "OUTPUT_DIR": "data/flags",
    "SAVE_API_DIR": "data/saveapi",  # 专门用于保存API数据的目录
    "FILE_PREFIX": "baostock_info",
    "DATE_FORMAT": "%Y%m%d"
}

# Baostock特定常量
BAOSTOCK = {
    "LOGIN_USER": "anonymous",
    "LOGIN_PASSWORD": "123456"
}

# ================================
# 3. 主要逻辑
# ================================

# 获取baostock版本
try:
    version = bs.__version__
except AttributeError:
    version = "Unknown"
print(f"🚀 开始获取Baostock信息...")
print(f"✅ Baostock版本: {version}")

# 尝试登录Baostock
print("🔄 尝试登录Baostock...")
login_result = bs.login(BAOSTOCK["LOGIN_USER"], BAOSTOCK["LOGIN_PASSWORD"])
if login_result.error_code != '0':
    print(f"❌ Baostock登录失败: {login_result.error_msg}")
    sys.exit(1)
else:
    print("✅ Baostock登录成功")

# 获取所有可用函数
print("🔍 正在扫描所有可用接口...")
start_time = time.time()

functions = []
# 只有没有指定接口时才扫描所有接口
if len(sys.argv) <= 1 or sys.argv[1].strip() == "":
    # Baostock的接口主要是bs模块的方法
    for name, obj in inspect.getmembers(bs):
        if inspect.isfunction(obj) and not name.startswith('_') and name != 'login' and name != 'logout':
            functions.append(name)

elapsed = time.time() - start_time
print(f"✅ 共找到 {len(functions)} 个可用接口 (耗时: {elapsed:.2f} 秒)")

# 按字母顺序排序
functions = sorted(functions)

# 如果没有指定接口，则创建并保存完整的接口列表文件
if len(sys.argv) <= 1 or sys.argv[1].strip() == "":
    # 准备输出内容
    output = f"Baostock Version: {version}\n"
    output += f"Total Functions: {len(functions)}\n\n"
    output += "=" * 50 + "\n"
    output += "Available Functions\n"
    output += "=" * 50 + "\n\n"

    # 添加所有函数到输出
    for func_name in functions:
        output += f"{func_name}\n"

    # 获取当前北京时间
    beijing_date = datetime.now().strftime(FILE_PARAMS["DATE_FORMAT"])

    # 添加时间戳
    output += "\n" + "=" * 50 + "\n"
    output += f"Generated on: {beijing_date} (Beijing Time)\n"
    output += "=" * 50 + "\n"

    # 保存到文件
    file_name = f"{beijing_date}{FILE_PARAMS['FILE_PREFIX']}.txt"
    output_dir = FILE_PARAMS["OUTPUT_DIR"]

    # 确保目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 写入文件
    file_path = os.path.join(output_dir, file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(output)

    print(f"📁 Baostock信息已保存到 {file_path}")
    
    # 【关键修复】确保文件真正提交到Git仓库
    try:
        # 直接使用"LAST_FILE"参数立即提交
        print(f"ℹ️ 正在将文件提交到Git仓库...")
        success = commit_files_in_batches(file_path, "LAST_FILE")
        
        if success:
            print(f"✅ 文件 {file_name} 已成功提交到Git仓库")
        else:
            print(f"⚠️ 提交文件到Git仓库失败，请检查Git配置")
    except Exception as e:
        print(f"❌ 提交文件到Git仓库失败: {str(e)}")
        print(f"💡 专业提示: 请检查项目结构，确保utils目录位于项目根目录")
    
    print(f"📌 提示: 完整接口列表已保存至: {file_path}")
else:
    # 如果指定了接口，不创建完整接口列表文件
    print("ℹ️ 检测到指定了接口名称，跳过完整接口列表的生成")

# 如果提供了接口名称参数，打印该接口的列名
if len(sys.argv) > 1 and sys.argv[1].strip() != "":
    interface_name = sys.argv[1].strip()
    print(f"\n🔍 开始查询接口: {interface_name}")
    
    # 获取所有函数列表用于检查
    all_functions = []
    for name, obj in inspect.getmembers(bs):
        if inspect.isfunction(obj) and not name.startswith('_') and name != 'login' and name != 'logout':
            all_functions.append(name)
    
    # 简化逻辑：接口不存在直接报告
    if interface_name not in all_functions:
        print(f"  ❌ 错误: 接口 '{interface_name}' 未在Baostock中找到")
        print(f"  📌 提示: 当前版本Baostock共有 {len(all_functions)} 个可用接口")
        
        # 提供可能的建议
        if "stock" in interface_name.lower():
            print("\n💡 专业提示：股票相关接口可能名称有误，常见股票接口包括：")
            print("   - query_stock_basic")
            print("   - query_history_k_data")
            print("   - query_daily")
            print("   - query_adj_data")
            print("   - query_dividend_data")
        elif "balance" in interface_name.lower() or "income" in interface_name.lower() or "cash" in interface_name.lower():
            print("\n💡 专业提示：财务相关接口可能名称有误，常见财务接口包括：")
            print("   - query_balance_data")
            print("   - query_income_data")
            print("   - query_cash_flow_data")
        elif "industry" in interface_name.lower():
            print("\n💡 专业提示：行业相关接口可能名称有误，常见行业接口包括：")
            print("   - query_stock_industry")
            
        print(f"\nℹ️ 提示: 运行不带参数的命令可查看所有可用接口")
        sys.exit(1)
    
    try:
        # 根据接口名称判断API类型
        api_type = None
        for type_name, keywords in API_TEST_PARAMS["API_TYPE_KEYWORDS"].items():
            if any(keyword in interface_name for keyword in keywords):
                api_type = type_name
                break
        
        # 获取测试代码
        test_code = API_TEST_PARAMS["TEST_CODES"].get(api_type, API_TEST_PARAMS["TEST_CODES"]["stock"])
        
        # 简化API调用策略
        result = None
        data = None
        
        # 简化调用逻辑：只尝试两种方式（无参数和带测试代码）
        print(f"  📡 尝试调用接口 {interface_name}...")
        
        # 尝试1：无参数调用
        try:
            print(f"  📡 尝试1：无参数调用 {interface_name}()")
            func = getattr(bs, interface_name)
            result = func()
            print(f"  ✅ 无参数调用成功")
        except Exception as e:
            print(f"  ⚠️ 无参数调用失败: {str(e)}")
            
            # 尝试2：使用适合该API类型的测试代码
            if api_type:
                try:
                    print(f"  📡 尝试2：使用{api_type}测试代码({test_code})调用 {interface_name}(code='{test_code}')")
                    func = getattr(bs, interface_name)
                    result = func(code=test_code)
                    print(f"  ✅ 使用测试代码调用成功")
                except Exception as e2:
                    print(f"  ⚠️ 使用测试代码调用失败: {str(e2)}")
        
        # 【关键修复】结果处理 - 全面分析各种返回类型
        print(f"  🔍 分析返回结果类型...")
        
        if result is None:
            print(f"  ❌ 接口调用返回None")
            print(f"  ℹ️ 提示: 该接口可能没有返回数据或发生了错误")
        else:
            # Baostock的接口通常返回包含error_code和rows的结构
            if hasattr(result, 'error_code'):
                if result.error_code != '0':
                    print(f"  ❌ Baostock API返回错误: {result.error_msg}")
                    print(f"  ℹ️ 错误代码: {result.error_code}")
                else:
                    # 获取数据
                    data = result
                    print(f"  ✅ Baostock API调用成功")
            else:
                data = result
            
            if data:
                # 获取返回结果的类型
                result_type = type(data).__name__
                print(f"  📦 返回类型: {result_type}")
                
                # 【关键修复】统一处理各种返回类型
                # 不再假设返回值是DataFrame，而是全面分析
                print(f"  📊 开始全面分析返回结果...")
                
                # 1. 尝试获取对象的基本信息
                try:
                    # 尝试获取对象的属性
                    attrs = dir(data)
                    if attrs:
                        print(f"  🧩 对象属性: {', '.join([attr for attr in attrs if not attr.startswith('__')][:10])}{'...' if len(attrs) > 10 else ''}")
                except Exception as e:
                    print(f"  ⚠️ 无法获取对象属性: {str(e)}")
                
                # 2. 尝试检查是否有错误信息
                error_indicators = ['error', 'message', 'status', 'code', 'msg', 'desc', 'reason']
                for indicator in error_indicators:
                    try:
                        if hasattr(data, indicator):
                            value = getattr(data, indicator)
                            print(f"  ❗ 检测到可能的错误信息 ({indicator}): {value}")
                        elif isinstance(data, dict) and indicator in data:
                            print(f"  ❗ 检测到可能的错误信息 ({indicator}): {data[indicator]}")
                    except Exception as e:
                        pass
                
                # 3. 尝试将结果转换为JSON
                try:
                    # 尝试将Baostock结果转换为字典
                    if hasattr(data, 'fields') and hasattr(data, 'rows'):
                        data_dict = {
                            "fields": data.fields,
                            "rows": data.rows
                        }
                        json_data = json.dumps(data_dict, default=str)
                    else:
                        json_data = json.dumps(data, default=str)
                    
                    print(f"  📦 尝试将结果转换为JSON: 成功 (长度: {len(json_data)})")
                    # 保存JSON数据到文件
                    save_dir = FILE_PARAMS["SAVE_API_DIR"]
                    os.makedirs(save_dir, exist_ok=True)
                    
                    # 生成文件名：api名+时间戳+_json
                    timestamp = datetime.now().strftime("%Y%m%d%H%M")
                    file_name = f"{interface_name}_{timestamp}_json.txt"
                    file_path = os.path.join(save_dir, file_name)
                    
                    # 保存JSON数据
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(json_data)
                    print(f"  💾 已保存JSON数据到: {file_path}")
                    
                    # 提交文件到Git仓库 - 使用"LAST_FILE"参数
                    print(f"  📤 正在提交JSON文件到Git仓库...")
                    success = commit_files_in_batches(file_path, "LAST_FILE")
                    
                    if success:
                        print(f"  ✅ JSON文件 {file_name} 已成功提交到Git仓库")
                    else:
                        print(f"  ❌ 提交JSON文件到Git仓库失败")
                except Exception as e:
                    print(f"  ⚠️ 无法将结果转换为JSON: {str(e)}")
                
                # 4. 检查是否是Baostock数据结构
                if hasattr(data, 'fields') and hasattr(data, 'rows'):
                    # Baostock标准返回结构
                    print(f"  📊 检测到Baostock标准数据结构")
                    
                    # 检查列数
                    if data.fields and len(data.fields) > 0:
                        columns = ", ".join(data.fields)
                        print(f"  🗂️ 成功获取列名: {columns}")
                        
                        # 显示列数据类型 - Baostock不直接提供类型信息
                        print(f"  📊 列数据类型:")
                        for col in data.fields:
                            print(f"    - {col}: str (Baostock默认返回字符串类型)")
                        
                        # 打印前100行数据示例（或实际行数，如果少于100）
                        if API_TEST_PARAMS["SHOW_DATA_SAMPLE"] and data.rows:
                            rows_to_show = min(API_TEST_PARAMS["SAMPLE_ROWS"], len(data.rows))
                            print(f"  📊 前{rows_to_show}行数据示例:")
                            
                            # 为每行数据添加索引和格式化
                            for i in range(rows_to_show):
                                row = data.rows[i]
                                row_dict = dict(zip(data.fields, row))
                                print(f"    [{i}] {row_dict}")
                            
                            # 保存前100条数据到文件
                            print(f"  💾 开始保存API数据到仓库...")
                            
                            # 创建保存目录
                            save_dir = FILE_PARAMS["SAVE_API_DIR"]
                            os.makedirs(save_dir, exist_ok=True)
                            
                            # 生成文件名：api名+时间戳
                            timestamp = datetime.now().strftime("%Y%m%d%H%M")
                            file_name = f"{interface_name}_{timestamp}.csv"
                            file_path = os.path.join(save_dir, file_name)
                            
                            # 保存前100条数据
                            rows_to_save = min(100, len(data.rows))
                            # 转换为DataFrame并保存
                            import pandas as pd
                            df = pd.DataFrame(data.rows[:rows_to_save], columns=data.fields)
                            df.to_csv(file_path, index=False, encoding="utf-8-sig")
                            print(f"  💾 已保存前{rows_to_save}条数据到: {file_path}")
                            
                            # 提交文件到Git仓库 - 使用"LAST_FILE"参数
                            print(f"  📤 正在提交文件到Git仓库...")
                            success = commit_files_in_batches(file_path, "LAST_FILE")
                            
                            if success:
                                print(f"  ✅ 文件 {file_name} 已成功提交到Git仓库")
                            else:
                                print(f"  ❌ 提交文件到Git仓库失败")
                        else:
                            print(f"  ℹ️ 返回的数据为空，但包含列名")
                    else:
                        print(f"  ❌ 返回的数据结构为空，无列名")
                # 检查是否是字典
                elif isinstance(data, dict):
                    print(f"  📂 返回的是字典，包含 {len(data)} 个键")
                    
                    # 显示字典结构
                    if data:
                        print("  📂 字典结构预览:")
                        
                        # 尝试提取第一个键的值来展示结构
                        first_key = next(iter(data))
                        first_value = data[first_key]
                        
                        if isinstance(first_value, dict):
                            print(f"    - 键值结构: {{'key': {{...}}}}")
                            print(f"    - 示例键: '{first_key}'")
                            print(f"    - 示例值结构: {list(first_value.keys())}")
                        elif isinstance(first_value, list):
                            print(f"    - 键值结构: {{'key': [...]}}")
                            print(f"    - 示例键: '{first_key}'")
                            if first_value:
                                print(f"    - 列表示例: {list(first_value[0].keys()) if isinstance(first_value[0], dict) else '元素类型: ' + type(first_value[0]).__name__}")
                        else:
                            print(f"    - 键值结构: {{'key': value}}")
                            print(f"    - 示例键: '{first_key}'")
                            print(f"    - 值类型: {type(first_value).__name__}")
                            
                        # 显示前3个键
                        sample_keys = list(data.keys())[:3]
                        print(f"    - 前{len(sample_keys)}个键示例: {', '.join(sample_keys)}")
                # 检查是否是列表
                elif isinstance(data, list):
                    print(f"  📋 返回的是列表，包含 {len(data)} 个元素")
                    
                    if data:
                        # 显示列表结构
                        first_item = data[0]
                        print(f"  📋 列表结构预览:")
                        
                        if isinstance(first_item, dict):
                            print(f"    - 列表元素是字典")
                            print(f"    - 字典键: {list(first_item.keys())}")
                            print(f"    - 示例数据: {pformat(first_item)[:200]}{'...' if len(pformat(first_item)) > 200 else ''}")
                        else:
                            print(f"    - 元素类型: {type(first_item).__name__}")
                            print(f"    - 示例数据: {str(first_item)[:200]}{'...' if len(str(first_item)) > 200 else ''}")
                # 检查是否是字符串
                elif isinstance(data, str):
                    print(f"  📝 返回的是字符串，长度: {len(data)}")
                    if len(data) > 200:
                        print(f"  📄 内容预览: {data[:200]}...")
                    else:
                        print(f"  📄 内容: {data}")
                    
                    # 尝试解析为JSON
                    try:
                        json_obj = json.loads(data)
                        print(f"  📦 检测到JSON字符串，成功解析")
                        # 分析JSON结构
                        if isinstance(json_obj, dict):
                            print(f"    - JSON结构: 字典，包含 {len(json_obj)} 个键")
                            sample_keys = list(json_obj.keys())[:3]
                            print(f"    - 前{len(sample_keys)}个键示例: {', '.join(sample_keys)}")
                        elif isinstance(json_obj, list):
                            print(f"    - JSON结构: 列表，包含 {len(json_obj)} 个元素")
                            if json_obj:
                                print(f"    - 第一个元素类型: {type(json_obj[0]).__name__}")
                    except Exception as e:
                        print(f"  ⚠️ 无法解析为JSON: {str(e)}")
                # 其他类型
                else:
                    print(f"  📄 返回的是 {result_type} 类型")
                    # 尝试转换为字符串并截断
                    str_repr = str(data)
                    if len(str_repr) > 500:
                        print(f"  📝 内容预览: {str_repr[:500]}...")
                    else:
                        print(f"  📝 内容: {str_repr}")
    
    except Exception as e:
        print(f"  ❌ 接口 {interface_name} 调用失败: {str(e)}")
        print(f"  📝 Traceback: {traceback.format_exc()}")
    finally:
        # 确保退出Baostock
        print("🔄 正在退出Baostock...")
        bs.logout()

else:
    # 确保退出Baostock
    print("🔄 正在退出Baostock...")
    bs.logout()
    
    print("\nℹ️ 提示: 如需查询特定接口的列名，请使用: python get_baostock_info.py 接口名称")
    print("   例如: python get_baostock_info.py query_history_k_data")
