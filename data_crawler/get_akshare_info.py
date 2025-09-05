#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取AkShare信息工具
输出AkShare版本、所有可用接口及其返回的列名
"""

import akshare as ak
import inspect
import os
import logging
from datetime import datetime
import traceback
import sys

# 配置日志
logging.basicConfig(level=logging.ERROR)

# 获取akshare版本
version = ak.__version__

# 获取所有可用函数
functions = []
for name, obj in inspect.getmembers(ak):
    if inspect.isfunction(obj) and not name.startswith('_'):
        functions.append(name)

# 准备输出内容
output = f"AkShare Version: {version}\n"
output += f"Total Functions: {len(functions)}\n\n"
output += "=" * 50 + "\n"
output += "Available Functions and Their Columns\n"
output += "=" * 50 + "\n\n"

# 获取每个函数返回的列名
for func_name in sorted(functions):
    try:
        func = getattr(ak, func_name)
        
        # 尝试调用函数
        try:
            # 尝试无参数调用
            result = func()
        except TypeError:
            # 如果函数需要参数，尝试一些常见参数
            if func_name == 'fund_etf_hist_sina':
                result = ak.fund_etf_hist_sina(symbol="etf")
            elif func_name == 'fund_etf_spot_em':
                result = ak.fund_etf_spot_em()
            elif func_name == 'fund_aum_em':
                result = ak.fund_aum_em()
            elif func_name == 'stock_zh_a_hist':
                result = ak.stock_zh_a_hist(symbol="sh000001", period="daily", start_date="20200101", end_date="20200110")
            elif func_name == 'stock_zh_a_hist_min':
                result = ak.stock_zh_a_hist_min(
                    symbol="sh000001", 
                    period="5", 
                    start_date="2020-01-01 09:30:00", 
                    end_date="2020-01-01 15:00:00"
                )
            elif func_name == 'stock_zh_a_hist_hfq':
                result = ak.stock_zh_a_hist_hfq(symbol="sh000001", period="daily", start_date="20200101", end_date="20200110")
            elif func_name == 'stock_zh_a_hist_hfq_em':
                result = ak.stock_zh_a_hist_hfq_em(symbol="sh000001", period="daily", start_date="20200101", end_date="20200110")
            elif func_name == 'stock_zh_a_minute':
                result = ak.stock_zh_a_minute(symbol="sh000001", period="5", adjust="qfq")
            elif func_name == 'stock_zh_a_daily':
                result = ak.stock_zh_a_daily(symbol="sh000001", adjust="qfq")
            elif func_name == 'stock_zh_a_spot_em':
                result = ak.stock_zh_a_spot_em()
            elif func_name == 'stock_zh_a_hist':
                result = ak.stock_zh_a_hist(symbol="sh000001", period="daily", start_date="20200101", end_date="20200110")
            elif func_name == 'fund_etf_hist_em':
                result = ak.fund_etf_hist_em()
            elif func_name == 'fund_etf_iopv_em':
                result = ak.fund_etf_iopv_em()
            else:
                result = None
        
        # 如果结果是DataFrame，获取列名
        if result is not None and hasattr(result, 'columns'):
            columns = ", ".join(result.columns)
            output += f"{func_name}:\n"
            output += f"  Columns: {columns}\n\n"
        else:
            output += f"{func_name}:\n"
            output += "  Result: DataFrame not returned or function requires specific parameters\n\n"
    except Exception as e:
        output += f"{func_name}:\n"
        output += f"  Error: {str(e)}\n"
        output += f"  Traceback: {traceback.format_exc()}\n\n"

# 获取当前北京时间（格式：YYYYMMDD）
beijing_date = datetime.now().strftime("%Y%m%d")

# 添加时间戳
output += "=" * 50 + "\n"
output += f"Generated on: {beijing_date} (Beijing Time)\n"
output += "=" * 50 + "\n"

# 保存到文件
file_name = f"{sys.argv[1]}akshare_info.txt" if len(sys.argv) > 1 else f"{beijing_date}akshare_info.txt"
output_dir = "data/flags"

# 确保目录存在
os.makedirs(output_dir, exist_ok=True)

# 写入文件
file_path = os.path.join(output_dir, file_name)
with open(file_path, "w", encoding="utf-8") as f:
    f.write(output)

print(f"AkShare信息已保存到 {file_path}")
