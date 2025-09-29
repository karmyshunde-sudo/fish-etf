#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数 Yes/No 策略执行器 - 仅用于测试恒生互联网科技业指数代码
"""

import os
import logging
import pandas as pd
import akshare as ak
import time
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time
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
    测试恒生互联网科技业指数的可能代码
    
    Args:
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）
        
    Returns:
        str: 成功获取数据的代码或错误信息
    """
    # 恒生互联网科技业指数的可能代码
    test_codes = [
        "HSIII",       # 恒生互联网科技业指数代码
        "800806",      # 部分行情网站使用的代码
        "HSNDXIT",     # AKShare可能使用的代码（无.HI后缀）
        "HSNDXIT.HI",  # 原始代码
        "HSTECH.HK",   # yfinance中的代码
        "HSTECH"       # 简化代码
    ]
    
    # 测试结果存储
    success_results = []
    error_results = []
    
    # 1. 测试AKShare的stock_hk_index_daily_em
    logger.info("=" * 50)
    logger.info("测试恒生互联网科技业指数的可能代码 (AKShare)")
    logger.info("=" * 50)
    
    for code in test_codes:
        try:
            logger.info(f"尝试使用 ak.stock_hk_index_daily_em 获取代码 {code}")
            
            # 获取数据
            df = ak.stock_hk_index_daily_em(
                symbol=code,
                period="daily",
                start_date=start_date,
                end_date=end_date
            )
            
            if not df.empty:
                logger.info(f"✅ 成功获取到 {len(df)} 条数据")
                logger.info(f"数据列名: {', '.join(df.columns)}")
                
                # 检查日期范围
                if '日期' in df.columns:
                    first_date = df['日期'].min()
                    last_date = df['日期'].max()
                    logger.info(f"数据日期范围: {first_date} 至 {last_date}")
                
                success_results.append(f"✅ {code} - 获取到 {len(df)} 条数据")
            else:
                error_results.append(f"❌ {code} - 返回空数据")
        except Exception as e:
            error_msg = str(e)
            # 提取主要错误信息
            if "No data" in error_msg:
                error_type = "No data"
            elif "Invalid symbol" in error_msg:
                error_type = "Invalid symbol"
            elif "404" in error_msg:
                error_type = "404 error"
            else:
                error_type = "Unknown error"
            
            error_results.append(f"❌ {code} - {error_type}")
            logger.error(f"测试 {code} 失败: {error_type}")
    
    # 2. 测试yfinance
    logger.info("\n" + "=" * 50)
    logger.info("测试恒生互联网科技业指数的可能代码 (yfinance)")
    logger.info("=" * 50)
    
    yf_codes = [
        "02828.HK",    # 恒生互联网科技业指数的ETF代码
        "HSTECH.HK",   # 恒生科技指数代码
        "HSTECH",      # 简化代码
        "^HSI"         # 恒生指数
    ]
    
    for code in yf_codes:
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
                
                success_results.append(f"✅ yfinance {code} - 获取到 {len(df)} 条数据")
            else:
                error_results.append(f"❌ yfinance {code} - 返回空数据")
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
            
            error_results.append(f"❌ yfinance {code} - {error_type}")
            logger.error(f"测试 {code} 失败: {error_type}")
    
    # 输出结果
    logger.info("\n" + "=" * 50)
    logger.info("测试结果总结")
    logger.info("=" * 50)
    
    if success_results:
        logger.info("✅ 成功获取数据的代码:")
        for result in success_results:
            logger.info(result)
    else:
        logger.info("❌ 没有成功获取到数据的代码")
    
    if error_results:
        logger.info("\n❌ 失败的代码:")
        for result in error_results:
            logger.info(result)
    
    # 返回第一个成功代码
    if success_results:
        first_success = success_results[0]
        return first_success.split()[1]  # 返回代码部分
    else:
        return "所有测试代码均失败"

if __name__ == "__main__":
    logger.info("===== 开始执行 恒生互联网科技业指数代码测试 =====")
    
    # 测试恒生互联网科技业指数代码
    successful_code = test_hang_seng_tech_index_code()
    
    if successful_code != "所有测试代码均失败":
        logger.info(f"✅ 找到有效代码: {successful_code}")
        send_wechat_message(f"✅ 恒生互联网科技业指数测试成功！有效代码: {successful_code}")
    else:
        logger.error("❌ 未找到有效代码")
        send_wechat_message("❌ 恒生互联网科技业指数测试失败：未找到有效代码")
    
    logger.info("=== 恒生互联网科技业指数代码测试完成 ===")
