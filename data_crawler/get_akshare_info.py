#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取AkShare信息工具
注意：这不是项目的主程序，而是被工作流调用的工具脚本
"""

import akshare as ak
import inspect
import os
import logging
from datetime import datetime
import traceback
import sys
import time

# 配置日志
logging.basicConfig(level=logging.ERROR)

# 获取akshare版本
version = ak.__version__
print(f"🚀 开始获取AkShare信息...")
print(f"✅ AkShare版本: {version}")

# 获取所有可用函数 - 这是关键部分，通过inspect模块获取akshare中所有公共函数
print("🔍 正在扫描所有可用接口...")
start_time = time.time()

functions = []
for name, obj in inspect.getmembers(ak):
    if inspect.isfunction(obj) and not name.startswith('_'):
        functions.append(name)

elapsed = time.time() - start_time
print(f"✅ 共找到 {len(functions)} 个可用接口 (耗时: {elapsed:.2f} 秒)")

# 按字母顺序排序
functions = sorted(functions)

# 准备输出内容
output = f"AkShare Version: {version}\n"
output += f"Total Functions: {len(functions)}\n\n"
output += "=" * 50 + "\n"
output += "Available Functions\n"
output += "=" * 50 + "\n\n"

# 添加所有函数到输出
for func_name in functions:
    output += f"{func_name}\n"

# 获取当前北京时间（格式：YYYYMMDD）
beijing_date = datetime.now().strftime("%Y%m%d")

# 添加时间戳
output += "\n" + "=" * 50 + "\n"
output += f"Generated on: {beijing_date} (Beijing Time)\n"
output += "=" * 50 + "\n"

# 保存到文件
file_name = f"{beijing_date}akshare_info.txt"
output_dir = "data/flags"

# 确保目录存在
os.makedirs(output_dir, exist_ok=True)

# 写入文件
file_path = os.path.join(output_dir, file_name)
with open(file_path, "w", encoding="utf-8") as f:
    f.write(output)

print(f"📁 AkShare信息已保存到 {file_path}")
print(f"📌 提示: 完整接口列表已保存至: {file_path}")

# 如果提供了接口名称参数，打印该接口的列名
if len(sys.argv) > 1 and sys.argv[1].strip() != "":
    interface_name = sys.argv[1].strip()
    print(f"\n🔍 开始查询接口: {interface_name}")
    
    if interface_name in functions:
        try:
            # 尝试调用函数
            try:
                # 尝试无参数调用
                print(f"  📡 尝试无参数调用接口 {interface_name}...")
                result = getattr(ak, interface_name)()
                print(f"  ✅ 接口 {interface_name} 调用成功")
            except TypeError:
                # 如果函数需要参数，尝试一些常见参数
                print(f"  ⚠️ 接口 {interface_name} 需要参数，尝试常见参数...")
                
                if interface_name == 'fund_etf_hist_sina':
                    print("  📡 尝试调用: fund_etf_hist_sina(symbol='etf')")
                    result = ak.fund_etf_hist_sina(symbol="etf")
                elif interface_name == 'fund_etf_spot_em':
                    print("  📡 尝试调用: fund_etf_spot_em()")
                    result = ak.fund_etf_spot_em()
                elif interface_name == 'fund_aum_em':
                    print("  📡 尝试调用: fund_aum_em()")
                    result = ak.fund_aum_em()
                elif interface_name == 'stock_zh_a_hist':
                    print("  📡 尝试调用: stock_zh_a_hist(symbol='sh000001', period='daily', start_date='20200101', end_date='20200110')")
                    result = ak.stock_zh_a_hist(symbol="sh000001", period="daily", start_date="20200101", end_date="20200110")
                elif interface_name == 'stock_zh_a_hist_min':
                    print("  📡 尝试调用: stock_zh_a_hist_min(...)")
                    result = ak.stock_zh_a_hist_min(
                        symbol="sh000001", 
                        period="5", 
                        start_date="2020-01-01 09:30:00", 
                        end_date="2020-01-01 15:00:00"
                    )
                elif interface_name == 'stock_zh_a_hist_hfq':
                    print("  📡 尝试调用: stock_zh_a_hist_hfq(...)")
                    result = ak.stock_zh_a_hist_hfq(symbol="sh000001", period="daily", start_date="20200101", end_date="20200110")
                elif interface_name == 'stock_zh_a_hist_hfq_em':
                    print("  📡 尝试调用: stock_zh_a_hist_hfq_em(...)")
                    result = ak.stock_zh_a_hist_hfq_em(symbol="sh000001", period="daily", start_date="20200101", end_date="20200110")
                elif interface_name == 'stock_zh_a_minute':
                    print("  📡 尝试调用: stock_zh_a_minute(...)")
                    result = ak.stock_zh_a_minute(symbol="sh000001", period="5", adjust="qfq")
                elif interface_name == 'stock_zh_a_daily':
                    print("  📡 尝试调用: stock_zh_a_daily(...)")
                    result = ak.stock_zh_a_daily(symbol="sh000001", adjust="qfq")
                elif interface_name == 'stock_zh_a_spot_em':
                    print("  📡 尝试调用: stock_zh_a_spot_em()")
                    result = ak.stock_zh_a_spot_em()
                elif interface_name == 'stock_zh_a_hist':
                    print("  📡 尝试调用: stock_zh_a_hist(...)")
                    result = ak.stock_zh_a_hist(symbol="sh000001", period="daily", start_date="20200101", end_date="20200110")
                elif interface_name == 'fund_etf_hist_em':
                    print("  📡 尝试调用: fund_etf_hist_em()")
                    result = ak.fund_etf_hist_em()
                elif interface_name == 'fund_etf_iopv_em':
                    print("  📡 尝试调用: fund_etf_iopv_em()")
                    result = ak.fund_etf_iopv_em()
                else:
                    print(f"  ⚠️ 接口 {interface_name} 需要特定参数，但未在预定义列表中")
                    result = None
            
            # 如果结果是DataFrame，打印列名
            if result is not None and hasattr(result, 'columns'):
                columns = ", ".join(result.columns)
                print(f"  🗂️ 列名: {columns}")
            else:
                print("  📊 结果: 未返回DataFrame或需要特定参数")
        except Exception as e:
            print(f"  ❌ 接口 {interface_name} 调用失败: {str(e)}")
            print(f"  📝 Traceback: {traceback.format_exc()}")
    else:
        print(f"  ❌ 错误: 接口 '{interface_name}' 未在AkShare中找到")
        print(f"  📌 提示: 当前版本AkShare共有 {len(functions)} 个可用接口，您可以查看 {file_path} 获取完整列表")
else:
    print("\nℹ️ 提示: 如需查询特定接口的列名，请使用: python get_akshare_info.py 接口名称")
    print("   例如: python get_akshare_info.py fund_aum_em")
