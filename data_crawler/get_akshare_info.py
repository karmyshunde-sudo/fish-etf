#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取AkShare信息工具 - 专业级API测试
注意：这不是项目的主程序，而是被工作流调用的工具脚本
"""

# ================================
# 1. 专业级参数定义区 (所有可配置参数都在这里)
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
    
    # 重试策略参数
    "MAX_RETRIES": 3,           # 最大重试次数
    "RETRY_DELAY": 1.0,         # 重试前等待秒数
    "ALL_PARAM_RETRY": True,    # 是否尝试使用"all"参数重试
    
    # 输出参数
    "SHOW_DATA_SAMPLE": True,   # 是否显示数据示例
    "SAMPLE_ROWS": 2,           # 数据示例显示的行数
    "VERBOSE": True             # 是否显示详细日志
}

# 文件和目录参数
FILE_PARAMS = {
    "OUTPUT_DIR": "data/flags",
    "FILE_PREFIX": "akshare_info",
    "DATE_FORMAT": "%Y%m%d"
}

# ================================
# 2. 导入模块和配置
# ================================

import akshare as ak
import inspect
import os
import logging
import time
from datetime import datetime
import traceback
import sys

# 配置日志
logging.basicConfig(level=logging.ERROR)

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
    
    if interface_name in all_functions:
        try:
            # ================================
            # 4. API类型识别
            # ================================
            
            # 根据接口名称判断API类型
            api_type = None
            for type_name, keywords in API_TEST_PARAMS["API_TYPE_KEYWORDS"].items():
                if any(keyword in interface_name for keyword in keywords):
                    api_type = type_name
                    break
            
            # 获取测试代码
            test_code = API_TEST_PARAMS["TEST_CODES"].get(api_type, API_TEST_PARAMS["TEST_CODES"]["stock"])
            
            # ================================
            # 5. 专业级API调用策略
            # ================================
            
            result = None
            attempt = 0
            max_attempts = 4  # 无参数、特定测试代码、all、其他参数
            
            while result is None and attempt < max_attempts:
                attempt += 1
                
                if attempt == 1:
                    # 第1步：尝试无参数调用（最简单的方式）
                    if API_TEST_PARAMS["VERBOSE"]:
                        print(f"  📡 第{attempt}步：尝试无参数调用 {interface_name}()")
                    try:
                        result = getattr(ak, interface_name)()
                    except Exception as e:
                        if API_TEST_PARAMS["VERBOSE"]:
                            print(f"  ⚠️ 无参数调用失败: {str(e)}")
                
                elif attempt == 2 and api_type:
                    # 第2步：使用适合该API类型的测试代码
                    if API_TEST_PARAMS["VERBOSE"]:
                        print(f"  📡 第{attempt}步：尝试使用{api_type}测试代码({test_code})调用 {interface_name}(symbol='{test_code}')")
                    try:
                        result = getattr(ak, interface_name)(symbol=test_code)
                    except Exception as e:
                        if API_TEST_PARAMS["VERBOSE"]:
                            print(f"  ⚠️ 使用{api_type}测试代码调用失败: {str(e)}")
                
                elif attempt == 3 and API_TEST_PARAMS["ALL_PARAM_RETRY"]:
                    # 第3步：尝试使用"all"（数据量大但可能成功）
                    if API_TEST_PARAMS["VERBOSE"]:
                        print(f"  📡 第{attempt}步：尝试使用'all'调用 {interface_name}(symbol='all')")
                    try:
                        result = getattr(ak, interface_name)(symbol="all")
                    except Exception as e:
                        if API_TEST_PARAMS["VERBOSE"]:
                            print(f"  ⚠️ 使用'all'调用失败: {str(e)}")
                
                else:
                    # 第4步：尝试其他常见参数
                    if API_TEST_PARAMS["VERBOSE"]:
                        print(f"  📡 第{attempt}步：尝试其他常见参数")
                    try:
                        # 根据API类型尝试不同参数组合
                        if api_type == "stock":
                            try:
                                result = getattr(ak, interface_name)(symbol="sh600519")
                                if API_TEST_PARAMS["VERBOSE"]:
                                    print(f"  📡 尝试调用: {interface_name}(symbol='sh600519')")
                            except:
                                try:
                                    result = getattr(ak, interface_name)(symbol="sz000001")
                                    if API_TEST_PARAMS["VERBOSE"]:
                                        print(f"  📡 尝试调用: {interface_name}(symbol='sz000001')")
                                except:
                                    pass
                        elif api_type == "etf":
                            try:
                                result = getattr(ak, interface_name)(symbol="sh510300")
                                if API_TEST_PARAMS["VERBOSE"]:
                                    print(f"  📡 尝试调用: {interface_name}(symbol='sh510300')")
                            except:
                                try:
                                    result = getattr(ak, interface_name)(symbol="sh518880")
                                    if API_TEST_PARAMS["VERBOSE"]:
                                        print(f"  📡 尝试调用: {interface_name}(symbol='sh518880')")
                                except:
                                    pass
                        elif api_type == "index":
                            try:
                                result = getattr(ak, interface_name)(symbol="sh000001")
                                if API_TEST_PARAMS["VERBOSE"]:
                                    print(f"  📡 尝试调用: {interface_name}(symbol='sh000001')")
                            except:
                                try:
                                    result = getattr(ak, interface_name)(symbol="sz399001")
                                    if API_TEST_PARAMS["VERBOSE"]:
                                        print(f"  📡 尝试调用: {interface_name}(symbol='sz399001')")
                                except:
                                    pass
                        else:
                            # 尝试一些通用参数组合
                            try:
                                result = getattr(ak, interface_name)(period="daily")
                                if API_TEST_PARAMS["VERBOSE"]:
                                    print(f"  📡 尝试调用: {interface_name}(period='daily')")
                            except:
                                try:
                                    result = getattr(ak, interface_name)(date="20230101")
                                    if API_TEST_PARAMS["VERBOSE"]:
                                        print(f"  📡 尝试调用: {interface_name}(date='20230101')")
                                except:
                                    try:
                                        result = getattr(ak, interface_name)(market="sh")
                                        if API_TEST_PARAMS["VERBOSE"]:
                                            print(f"  📡 尝试调用: {interface_name}(market='sh')")
                                    except:
                                        pass
                
                # 检查是否成功获取列名
                if result is not None:
                    if hasattr(result, 'columns') and len(result.columns) > 0:
                        if API_TEST_PARAMS["VERBOSE"]:
                            print(f"  ✅ 第{attempt}步调用成功，成功获取列名")
                        break
                    else:
                        result = None
            
            # ================================
            # 6. 结果处理
            # ================================
            
            if result is not None and hasattr(result, 'columns') and len(result.columns) > 0:
                columns = ", ".join(result.columns)
                print(f"  🗂️ 成功获取列名: {columns}")
                
                # 打印前几行数据示例
                if API_TEST_PARAMS["SHOW_DATA_SAMPLE"] and hasattr(result, 'empty') and not result.empty:
                    print(f"  📊 前{API_TEST_PARAMS['SAMPLE_ROWS']}行数据示例:\n{result.head(API_TEST_PARAMS['SAMPLE_ROWS'])}")
            else:
                print(f"  ❌ 尝试了{attempt}种方式，仍无法获取有效的列名")
                
        except Exception as e:
            print(f"  ❌ 接口 {interface_name} 调用失败: {str(e)}")
            print(f"  📝 Traceback: {traceback.format_exc()}")
    else:
        print(f"  ❌ 错误: 接口 '{interface_name}' 未在AkShare中找到")
        print(f"  📌 提示: 当前版本AkShare共有 {len(all_functions)} 个可用接口，您可以使用不带参数的方式运行脚本查看完整列表")
else:
    print("\nℹ️ 提示: 如需查询特定接口的列名，请使用: python get_akshare_info.py 接口名称")
    print("   例如: python get_akshare_info.py stock_financial_analysis_indicator")
