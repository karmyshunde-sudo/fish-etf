#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
恒生互联网科技业指数代码测试工具
专注于测试正确的代码组合
"""

import os
import logging
import pandas as pd
import yfinance as yf
from datetime import datetime
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def test_hang_seng_tech_index_code(start_date: str = "20240101", end_date: str = "20250929") -> str:
    """
    专门测试恒生互联网科技业指数的可能代码组合
    
    Args:
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）
        
    Returns:
        str: 成功获取数据的代码或错误信息
    """
    # 恒生互联网科技业指数的可能代码
    test_codes = [
        "800806",       # 恒生互联网科技业指数的正确代码
        "800806.HK",    # yfinance格式
        "02828.HK",     # 华夏恒生互联网ETF（跟踪该指数）
        "HSTECH.HK",    # 恒生科技指数（可能相关）
        "HSIII",        # 可能是内部代码
        "HSIII.HK"      # yfinance格式
    ]
    
    # 测试结果存储
    success_results = []
    error_results = []
    
    logger.info("=" * 50)
    logger.info("测试恒生互联网科技业指数的可能代码")
    logger.info("=" * 50)
    
    for code in test_codes:
        try:
            logger.info(f"尝试使用 yfinance.download 获取代码 {code}")
            
            # 转换日期格式
            start_dt = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")
            
            # 获取数据
            df = yf.download(code, start=start_dt, end=end_dt)
            
            if isinstance(df, pd.DataFrame) and not df.empty:
                logger.info(f"✅ 成功获取到 {len(df)} 条数据")
                logger.info(f"数据列名: {', '.join(df.columns)}")
                
                # 检查日期范围
                if 'Date' in df.columns:
                    first_date = df['Date'].min().strftime("%Y-%m-%d")
                    last_date = df['Date'].max().strftime("%Y-%m-%d")
                    logger.info(f"数据日期范围: {first_date} 至 {last_date}")
                
                success_results.append(f"✅ {code} - 获取到 {len(df)} 条数据")
            else:
                error_results.append(f"❌ {code} - 返回空数据")
        except Exception as e:
            error_msg = str(e)
            # 提取主要错误信息
            if "404" in error_msg:
                error_type = "404 error"
            elif "No data" in error_msg:
                error_type = "No data"
            elif "Symbol not found" in error_msg:
                error_type = "Symbol not found"
            else:
                error_type = "Unknown error"
            
            error_results.append(f"❌ {code} - {error_type}")
            logger.error(f"❌ 测试 {code} 失败: {error_type}")
    
    # 输出结果
    logger.info("\n" + "=" * 50)
    logger.info("测试结果总结")
    logger.info("=" * 50)
    
    if success_results:
        logger.info("✅ 成功获取数据的代码:")
        for result in success_results:
            logger.info(result)
        return success_results[0].split()[1]  # 返回第一个成功代码
    else:
        logger.info("❌ 没有成功获取到数据的代码")
        return "所有测试代码均失败"

# ================ 主流程 ================
if __name__ == "__main__":
    logger.info("===== 开始执行 恒生互联网科技业指数代码测试 =====")
    
    # 只执行测试函数
    successful_code = test_hang_seng_tech_index_code()
    
    if successful_code != "所有测试代码均失败":
        logger.info(f"✅ 找到有效代码: {successful_code}")
        send_wechat_message(f"✅ 恒生互联网科技业指数测试成功！有效代码: {successful_code}")
    else:
        logger.error("❌ 未找到有效代码")
        send_wechat_message("❌ 恒生互联网科技业指数测试失败：未找到有效代码")
    
    logger.info("=== 恒生互联网科技业指数代码测试完成 ===")
