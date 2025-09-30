#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF API测试工具
使用固定ETF代码和日期测试所有可能的API接口
"""

import akshare as ak
import pandas as pd
import logging
import os
from datetime import datetime

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def test_etf_apis():
    """测试所有ETF相关API接口的返回结构"""
    # 固定测试参数
    etf_code = "159915"  # 创业板ETF
    start_date = "20250701"  # 测试日期前一天
    end_date = "20250702"  # 固定测试日期
    
    logger.info("=" * 50)
    logger.info(f"开始测试ETF {etf_code} API接口")
    logger.info(f"测试日期: {start_date} 至 {end_date}")
    logger.info("=" * 50)
    
    # 1. 测试fund_etf_hist_sina接口
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_hist_sina")
    logger.info(f"ak.fund_etf_hist_sina(symbol='{etf_code}')")
    logger.info("=" * 50)
    try:
        df = ak.fund_etf_hist_sina(symbol=etf_code)
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ 接口返回的列名: {list(df.columns)}")
            logger.info(f"数据示例 (前2条): {df.head(2).to_dict(orient='records')}")
        else:
            logger.info("❌ 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")
    
    # 2. 测试fund_etf_spot_em接口
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_spot_em")
    logger.info(f"ak.fund_etf_spot_em()")
    logger.info("=" * 50)
    try:
        df = ak.fund_etf_spot_em()
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ 接口返回的列名: {list(df.columns)}")
            # 筛选特定ETF
            if "代码" in df.columns:
                etf_data = df[df["代码"] == etf_code]
                if not etf_data.empty:
                    logger.info(f"ETF {etf_code} 数据示例: {etf_data.head(1).to_dict(orient='records')}")
                else:
                    logger.info(f"❌ ETF {etf_code} 无数据")
            else:
                logger.info("❌ 无'代码'列，无法筛选ETF")
        else:
            logger.info("❌ 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")
    
    # 3. 测试fund_etf_fund_daily_em接口
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_fund_daily_em")
    logger.info(f"ak.fund_etf_fund_daily_em()")
    logger.info("=" * 50)
    try:
        df = ak.fund_etf_fund_daily_em()
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ 接口返回的列名: {list(df.columns)}")
            # 筛选特定ETF
            if "基金代码" in df.columns:
                etf_data = df[df["基金代码"] == etf_code]
                if not etf_data.empty:
                    logger.info(f"ETF {etf_code} 数据示例: {etf_data.head(1).to_dict(orient='records')}")
                else:
                    logger.info(f"❌ ETF {etf_code} 无数据")
            else:
                logger.info("❌ 无'基金代码'列，无法筛选ETF")
        else:
            logger.info("❌ 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")
    
    # 4. 测试stock_zh_a_hist接口
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.stock_zh_a_hist")
    logger.info(f"ak.stock_zh_a_hist(symbol='{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 50)
    try:
        # 添加市场前缀
        symbol = etf_code
        if etf_code.startswith('5') or etf_code.startswith('6') or etf_code.startswith('9'):
            symbol = f"sh{etf_code}"
        else:
            symbol = f"sz{etf_code}"
        
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ 接口返回的列名: {list(df.columns)}")
            logger.info(f"数据示例: {df.head().to_dict(orient='records')}")
        else:
            logger.info("❌ 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

if __name__ == "__main__":
    logger.info("===== 开始执行ETF API测试 =====")
    test_etf_apis()
    logger.info("===== ETF API测试执行完成 =====")
