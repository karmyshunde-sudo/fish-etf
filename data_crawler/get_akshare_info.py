#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取AkShare信息工具 - 专业级修复
注意：这不是项目的主程序，而是被工作流调用的工具脚本
"""

# ================================
# 1. 导入模块和配置
# ================================

import akshare as ak
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
        "stock": "600519",   # 贵州茅台 - A股股票
        "etf": "510300",     # 沪深300ETF
        "index": "000001",   # 上证指数
        "fund": "000001",    # 示例基金代码
        "futures": "IF2306", # 沪深300股指期货
        "bond": "sh010504",  # 示例债券代码
        "option": "10003040" # 示例期权代码
    },
    
    # API类型识别关键词
    "API_TYPE_KEYWORDS": {
        "stock": ["stock", "zh_a"],
        "etf": ["etf", "fund_etf"],
        "index": ["index", "sh_sz"],
        "fund": ["fund", "zh_fund"],
        "futures": ["futures", "stock_futures"],
        "bond": ["bond", "zh_bond"],
        "option": ["option", "stock_option"]
    },
    
    # 输出参数
    "SHOW_DATA_SAMPLE": True,   # 是否显示数据示例
    "SAMPLE_ROWS": 5,           # 数据示例显示的行数 - 已增加到5行
    "VERBOSE": True             # 是否显示详细日志
}

# 文件和目录参数
FILE_PARAMS = {
    "OUTPUT_DIR": "data/flags",
    "SAVE_API_DIR": "data/saveapi",  # 专门用于保存API数据的目录
    "FILE_PREFIX": "akshare_info",
    "DATE_FORMAT": "%Y%m%d"
}

# ================================
# 3. 主要逻辑
# ================================

# 获取akshare版本
version = ak.__version__
print(f"🚀 开始获取AkShare信息...")
print(f"✅ AkShare版本: {version}")

# 获取所有可用函数
print("🔍 正在扫描所有可用接口...")
start_time = time.time()

functions = []
# 只有没有指定接口时才扫描所有接口
if len(sys.argv) <= 1 or sys.argv[1].strip() == "":
    for name, obj in inspect.getmembers(ak):
        if inspect.isfunction(obj) and not name.startswith('_'):
            functions.append(name)

elapsed = time.time() - start_time
print(f"✅ 共找到 {len(functions)} 个可用接口 (耗时: {elapsed:.2f} 秒)")

# 按字母顺序排序
functions = sorted(functions)

# 如果没有指定接口，则创建并保存完整的接口列表文件
if len(sys.argv) <= 1 or sys.argv[1].strip() == "":
    # 准备输出内容
    output = f"AkShare Version: {version}\n"
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

    print(f"📁 AkShare信息已保存到 {file_path}")
    
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
    for name, obj in inspect.getmembers(ak):
        if inspect.isfunction(obj) and not name.startswith('_'):
            all_functions.append(name)
    
    # 简化逻辑：接口不存在直接报告
    if interface_name not in all_functions:
        print(f"  ❌ 错误: 接口 '{interface_name}' 未在AkShare中找到")
        print(f"  📌 提示: 当前版本AkShare共有 {len(all_functions)} 个可用接口")
        
        # 提供可能的建议
        if "financial" in interface_name.lower():
            print("\n💡 专业提示：财务相关接口可能名称有误，常见财务接口包括：")
            print("   - stock_financial_analysis_sina")
            print("   - stock_financial_abstract")
            print("   - stock_financial_report_sina")
        elif "stock" in interface_name.lower():
            print("\n💡 专业提示：股票相关接口可能名称有误，常见股票接口包括：")
            print("   - stock_zh_a_spot_em")
            print("   - stock_zh_a_hist")
            print("   - stock_zh_a_hist_hfq_em")
        elif "etf" in interface_name.lower():
            print("\n💡 专业提示：ETF相关接口可能名称有误，常见ETF接口包括：")
            print("   - fund_etf_hist_sina")
            print("   - fund_etf_spot_em")
            print("   - fund_etf_hist_em")
            
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
        
        # 简化调用逻辑：只尝试两种方式（无参数和带测试代码）
        print(f"  📡 尝试调用接口 {interface_name}...")
        
        # 尝试1：无参数调用
        try:
            print(f"  📡 尝试1：无参数调用 {interface_name}()")
            result = getattr(ak, interface_name)()
            print(f"  ✅ 无参数调用成功")
        except Exception as e:
            print(f"  ⚠️ 无参数调用失败: {str(e)}")
            
            # 尝试2：使用适合该API类型的测试代码
            if api_type:
                try:
                    print(f"  📡 尝试2：使用{api_type}测试代码({test_code})调用 {interface_name}(symbol='{test_code}')")
                    result = getattr(ak, interface_name)(symbol=test_code)
                    print(f"  ✅ 使用测试代码调用成功")
                except Exception as e2:
                    print(f"  ⚠️ 使用测试代码调用失败: {str(e2)}")
        
        # 【终极修复】结果处理 - 增强类型检测与展示
        print(f"  🔍 分析返回结果类型...")
        
        if result is None:
            print(f"  ❌ 接口调用返回None")
            print(f"  ℹ️ 提示: 该接口可能没有返回数据或发生了错误")
        else:
            # 获取返回结果的类型
            result_type = type(result).__name__
            print(f"  📦 返回类型: {result_type}")
            
            # 检查是否是DataFrame
            is_dataframe = hasattr(result, 'columns') and hasattr(result, 'empty')
            
            if is_dataframe:
                # 【关键修复】移除错误假设，客观描述空DataFrame
                if len(result.columns) > 0:
                    columns = ", ".join(result.columns)
                    print(f"  🗂️ 成功获取列名: {columns}")
                    
                    # 【关键修复】显示列数据类型
                    print(f"  📊 列数据类型:")
                    for col in result.columns:
                        # 获取该列非空值的数据类型
                        non_null_values = result[col].dropna()
                        if len(non_null_values) > 0:
                            sample_value = non_null_values.iloc[0]
                            col_type = type(sample_value).__name__
                        else:
                            col_type = "empty"
                        print(f"    - {col}: {col_type}")
                    
                    # 【关键修复】打印前5行数据示例（或实际行数，如果少于5）
                    if API_TEST_PARAMS["SHOW_DATA_SAMPLE"] and not result.empty:
                        rows_to_show = min(API_TEST_PARAMS["SAMPLE_ROWS"], len(result))
                        print(f"  📊 前{rows_to_show}行数据示例:")
                        
                        # 为每行数据添加索引和格式化
                        for i in range(rows_to_show):
                            row = result.iloc[i]
                            print(f"    [{i}] {row.to_dict()}")
                        
                        # 【关键修复】保存前5条数据到文件
                        print(f"  💾 开始保存API数据到仓库...")
                        
                        # 创建保存目录
                        save_dir = FILE_PARAMS["SAVE_API_DIR"]
                        os.makedirs(save_dir, exist_ok=True)
                        
                        # 生成文件名：api名+时间戳
                        timestamp = datetime.now().strftime("%Y%m%d%H%M")
                        file_name = f"{interface_name}_{timestamp}.csv"
                        file_path = os.path.join(save_dir, file_name)
                        
                        # 保存前5条数据
                        rows_to_save = min(5, len(result))
                        result.head(rows_to_save).to_csv(file_path, index=False, encoding="utf-8-sig")
                        print(f"  💾 已保存前{rows_to_save}条数据到: {file_path}")
                        
                        # 【关键修复】提交文件到Git仓库 - 使用"LAST_FILE"参数
                        print(f"  📤 正在提交文件到Git仓库...")
                        success = commit_files_in_batches(file_path, "LAST_FILE")
                        
                        if success:
                            print(f"  ✅ 文件 {file_name} 已成功提交到Git仓库")
                        else:
                            print(f"  ❌ 提交文件到Git仓库失败")
                    else:
                        print(f"  ℹ️ 返回的DataFrame为空，但包含列名")
                else:
                    print(f"  ❌ 返回的DataFrame为空，无列名")
                    
                    # 【关键修复】移除错误假设，提供客观建议
                    print(f"  🔍 分析空DataFrame原因:")
                    
                    # 【关键修复】客观描述可能原因
                    print(f"    - 无参数调用可能不返回有效数据，某些API需要参数才能获取完整数据")
                    print(f"    - 该API可能只在特定时间段返回数据（如财报季）")
                    print(f"    - 可能需要其他参数，建议参考AkShare文档")
                    
                    # 【关键修复】提供通用建议，不针对特定接口
                    if api_type:
                        print(f"    - 可尝试使用{api_type}类型的标准测试代码调用，例如: {interface_name}(symbol='{test_code}')")
                    else:
                        print(f"    - 可尝试添加symbol参数调用，例如: {interface_name}(symbol='600519')")
                    
                    # 检查是否有属性可以提供线索
                    try:
                        attrs = dir(result)
                        if attrs:
                            print(f"    - DataFrame属性: {', '.join([attr for attr in attrs if not attr.startswith('__')][:10])}{'...' if len(attrs) > 10 else ''}")
                            
                            # 检查是否有特殊属性
                            if 'status' in attrs:
                                print(f"    - 状态: {result.status}")
                            if 'message' in attrs:
                                print(f"    - 消息: {result.message}")
                            if 'error' in attrs:
                                print(f"    - 错误: {result.error}")
                    except Exception as e:
                        print(f"    - 无法获取DataFrame属性: {str(e)}")
                    
                    # 检查是否有索引
                    try:
                        if not result.index.empty:
                            print(f"    - 索引存在但为空: {len(result.index)}个索引项")
                        else:
                            print(f"    - 索引为空")
                    except Exception as e:
                        print(f"    - 无法获取索引信息: {str(e)}")
                    
                    # 检查是否有错误信息
                    error_indicators = ['error', 'message', 'status', 'code']
                    for indicator in error_indicators:
                        if hasattr(result, indicator):
                            value = getattr(result, indicator)
                            print(f"    - 检测到可能的错误信息 ({indicator}): {value}")
                    
                    # 检查是否有其他线索
                    try:
                        str_repr = str(result)
                        if str_repr and str_repr != "Empty DataFrame":
                            print(f"    - DataFrame字符串表示: {str_repr[:200]}{'...' if len(str_repr) > 200 else ''}")
                    except Exception as e:
                        pass
            # 检查是否是字典
            elif isinstance(result, dict):
                print(f"  📂 返回的是字典，包含 {len(result)} 个键")
                
                # 显示字典结构
                if result:
                    print("  📂 字典结构预览:")
                    
                    # 尝试提取第一个键的值来展示结构
                    first_key = next(iter(result))
                    first_value = result[first_key]
                    
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
                    sample_keys = list(result.keys())[:3]
                    print(f"    - 前{len(sample_keys)}个键示例: {', '.join(sample_keys)}")
            # 检查是否是列表
            elif isinstance(result, list):
                print(f"  📋 返回的是列表，包含 {len(result)} 个元素")
                
                if result:
                    # 显示列表结构
                    first_item = result[0]
                    print(f"  📋 列表结构预览:")
                    
                    if isinstance(first_item, dict):
                        print(f"    - 列表元素是字典")
                        print(f"    - 字典键: {list(first_item.keys())}")
                        print(f"    - 示例数据: {pformat(first_item)[:200]}{'...' if len(pformat(first_item)) > 200 else ''}")
                    else:
                        print(f"    - 元素类型: {type(first_item).__name__}")
                        print(f"    - 示例数据: {str(first_item)[:200]}{'...' if len(str(first_item)) > 200 else ''}")
            # 检查是否是字符串
            elif isinstance(result, str):
                print(f"  📝 返回的是字符串，长度: {len(result)}")
                if len(result) > 200:
                    print(f"  📄 内容预览: {result[:200]}...")
                else:
                    print(f"  📄 内容: {result}")
            # 其他类型
            else:
                print(f"  📄 返回的是 {result_type} 类型")
                # 尝试转换为字符串并截断
                str_repr = str(result)
                if len(str_repr) > 500:
                    print(f"  📝 内容预览: {str_repr[:500]}...")
                else:
                    print(f"  📝 内容: {str_repr}")
                    
                # 尝试检查是否有属性
                try:
                    attrs = dir(result)
                    if attrs:
                        print(f"  🧩 对象属性: {', '.join([attr for attr in attrs if not attr.startswith('__')][:5])}{'...' if len(attrs) > 5 else ''}")
                except:
                    pass
    
    except Exception as e:
        print(f"  ❌ 接口 {interface_name} 调用失败: {str(e)}")
        print(f"  📝 Traceback: {traceback.format_exc()}")

else:
    print("\nℹ️ 提示: 如需查询特定接口的列名，请使用: python get_akshare_info.py 接口名称")
    print("   例如: python get_akshare_info.py stock_zh_a_spot_em")
