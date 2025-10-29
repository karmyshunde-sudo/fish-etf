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

# 配置日志
logging.basicConfig(level=logging.ERROR)

# 【终极修复】只添加这一行，确保正确导入
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files

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
    
    # 确保文件真正提交到Git仓库
    try:
        # 提交文件
        success = commit_files_in_batches(file_path, "更新AkShare接口列表")
        
        # 立即强制提交剩余文件
        force_commit_remaining_files()
        
        if success:
            print(f"✅ 文件 {file_name} 已成功提交到Git仓库")
        else:
            print(f"⚠️ 提交文件到Git仓库失败，请检查Git配置")
    except Exception as e:
        print(f"❌ 提交文件到Git仓库失败: {str(e)}")
    
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
        
        # 结果处理
        if result is not None and hasattr(result, 'columns') and len(result.columns) > 0:
            columns = ", ".join(result.columns)
            print(f"  🗂️ 成功获取列名: {columns}")
            
            # 打印前几行数据示例
            if API_TEST_PARAMS["SHOW_DATA_SAMPLE"] and hasattr(result, 'empty') and not result.empty:
                print(f"  📊 前{API_TEST_PARAMS['SAMPLE_ROWS']}行数据示例:\n{result.head(API_TEST_PARAMS['SAMPLE_ROWS'])}")
        else:
            print(f"  ❌ 接口调用成功但返回空DataFrame，无法获取列名")
            print(f"  ℹ️ 提示: 可能需要其他参数或该接口返回非DataFrame类型")
            
    except Exception as e:
        print(f"  ❌ 接口 {interface_name} 调用失败: {str(e)}")
        print(f"  📝 Traceback: {traceback.format_exc()}")

else:
    print("\nℹ️ 提示: 如需查询特定接口的列名，请使用: python get_akshare_info.py 接口名称")
    print("   例如: python get_akshare_info.py stock_zh_a_spot_em")
