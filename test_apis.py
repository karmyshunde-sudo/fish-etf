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
    end_date = "202507011"  # 固定测试日期
    
    logger.info("=" * 30)
    logger.info(f"开始测试ETF {etf_code} API接口")
    logger.info(f"测试日期: {start_date} 至 {end_date}")
    logger.info("=" * 30)
    
    # 1. 测试fund_etf_hist_sina接口（带市场前缀）
    test_fund_etf_hist_sina(etf_code, start_date, end_date)
    
    # 2. 测试fund_etf_spot_em接口
    test_fund_etf_spot_em(etf_code, start_date, end_date)
    
    # 3. 测试fund_etf_fund_daily_em接口
    test_fund_etf_fund_daily_em(etf_code, start_date, end_date)
    
    # 4. 测试stock_zh_a_hist接口（带市场前缀）
    test_stock_zh_a_hist(etf_code, start_date, end_date)
    
    # 5. 测试stock_zh_a_hist_min接口（带市场前缀）
    test_stock_zh_a_hist_min(etf_code, start_date, end_date)
    
    # 6. 测试fund_etf_daily_em接口
    test_fund_etf_daily_em(etf_code, start_date, end_date)
    
    # 7. 测试fund_etf_hist_em接口
    test_fund_etf_hist_em(etf_code, start_date, end_date)
    
    logger.info("=" * 30)
    logger.info("ETF API测试完成")
    logger.info("请提供以上日志，将基于实际返回的列名编写匹配代码")
    logger.info("=" * 30)

def test_fund_etf_hist_sina(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_hist_sina接口"""
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.fund_etf_hist_sina (无市场前缀)")
    logger.info(f"ak.fund_etf_hist_sina(symbol='{etf_code}')")
    logger.info("=" * 30)
    try:
        df = ak.fund_etf_hist_sina(symbol=etf_code)
        log_api_result("ak.fund_etf_hist_sina", df, etf_code, "fund_etf_hist_sina")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")
    
    # 测试带市场前缀的情况
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.fund_etf_hist_sina (带市场前缀)")
    logger.info(f"ak.fund_etf_hist_sina(symbol='sh{etf_code}')")
    logger.info("=" * 30)
    try:
        df = ak.fund_etf_hist_sina(symbol=f"sh{etf_code}")
        log_api_result("ak.fund_etf_hist_sina", df, etf_code, "fund_etf_hist_sina")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")
    
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.fund_etf_hist_sina (带市场前缀)")
    logger.info(f"ak.fund_etf_hist_sina(symbol='sz{etf_code}')")
    logger.info("=" * 30)
    try:
        df = ak.fund_etf_hist_sina(symbol=f"sz{etf_code}")
        log_api_result("ak.fund_etf_hist_sina", df, etf_code, "fund_etf_hist_sina")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_spot_em(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_spot_em接口"""
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.fund_etf_spot_em")
    logger.info(f"ak.fund_etf_spot_em()")
    logger.info("=" * 30)
    try:
        df = ak.fund_etf_spot_em()
        log_api_result("ak.fund_etf_spot_em", df, etf_code, "fund_etf_spot_em")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_fund_daily_em(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_fund_daily_em接口"""
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.fund_etf_fund_daily_em")
    logger.info(f"ak.fund_etf_fund_daily_em()")
    logger.info("=" * 30)
    try:
        df = ak.fund_etf_fund_daily_em()
        log_api_result("ak.fund_etf_fund_daily_em", df, etf_code, "fund_etf_fund_daily_em")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_stock_zh_a_hist(etf_code: str, start_date: str, end_date: str):
    """测试stock_zh_a_hist接口"""
    # 测试上交所ETF
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.stock_zh_a_hist (上交所)")
    logger.info(f"ak.stock_zh_a_hist(symbol='sh{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 30)
    try:
        df = ak.stock_zh_a_hist(
            symbol=f"sh{etf_code}",
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        log_api_result("ak.stock_zh_a_hist", df, etf_code, "stock_zh_a_hist")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")
    
    # 测试深交所ETF
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.stock_zh_a_hist (深交所)")
    logger.info(f"ak.stock_zh_a_hist(symbol='sz{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 30)
    try:
        df = ak.stock_zh_a_hist(
            symbol=f"sz{etf_code}",
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        log_api_result("ak.stock_zh_a_hist", df, etf_code, "stock_zh_a_hist")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_stock_zh_a_hist_min(etf_code: str, start_date: str, end_date: str):
    """测试stock_zh_a_hist_min接口"""
    # 测试上交所ETF
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.stock_zh_a_hist_min (上交所)")
    logger.info(f"ak.stock_zh_a_hist_min(symbol='sh{etf_code}', period='5', start_date='{start_date} 09:30:00', end_date='{end_date} 15:00:00')")
    logger.info("=" * 30)
    try:
        df = ak.stock_zh_a_hist_min(
            symbol=f"sh{etf_code}",
            period="5",
            start_date=f"{start_date} 09:30:00",
            end_date=f"{end_date} 15:00:00"
        )
        log_api_result("ak.stock_zh_a_hist_min", df, etf_code, "stock_zh_a_hist_min")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")
    
    # 测试深交所ETF
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.stock_zh_a_hist_min (深交所)")
    logger.info(f"ak.stock_zh_a_hist_min(symbol='sz{etf_code}', period='5', start_date='{start_date} 09:30:00', end_date='{end_date} 15:00:00')")
    logger.info("=" * 30)
    try:
        df = ak.stock_zh_a_hist_min(
            symbol=f"sz{etf_code}",
            period="5",
            start_date=f"{start_date} 09:30:00",
            end_date=f"{end_date} 15:00:00"
        )
        log_api_result("ak.stock_zh_a_hist_min", df, etf_code, "stock_zh_a_hist_min")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_daily_em(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_daily_em接口"""
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.fund_etf_daily_em")
    logger.info(f"ak.fund_etf_daily_em(symbol='{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 30)
    try:
        df = ak.fund_etf_daily_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        log_api_result("ak.fund_etf_daily_em", df, etf_code, "fund_etf_daily_em")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_hist_em(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_hist_em接口"""
    logger.info("\n" + "=" * 30)
    logger.info(f"测试 ak.fund_etf_hist_em")
    logger.info(f"ak.fund_etf_hist_em(symbol='{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 30)
    try:
        df = ak.fund_etf_hist_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        log_api_result("ak.fund_etf_hist_em", df, etf_code, "fund_etf_hist_em")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def log_api_result(api_name: str, df: pd.DataFrame, etf_code: str, api_type: str):
    """记录API测试结果"""
    if isinstance(df, pd.DataFrame) and not df.empty:
        logger.info(f"✅ {api_name} 接口返回 {len(df)} 条数据")
        logger.info(f"📊 返回的列名: {list(df.columns)}")
        
        # 根据接口类型处理数据筛选
        if api_type == "fund_etf_spot_em":
            # fund_etf_spot_em返回的是所有ETF的实时数据
            if "代码" in df.columns:
                etf_data = df[df["代码"] == etf_code]
                if not etf_data.empty:
                    logger.info(f"📊 {etf_code} 数据示例:")
                    logger.info(f"{etf_data.head(1).to_dict(orient='records')}")
                else:
                    logger.info(f"❌ 未找到ETF {etf_code} 的数据")
        elif api_type == "fund_etf_fund_daily_em":
            # fund_etf_fund_daily_em返回的是所有ETF的历史数据
            if "基金代码" in df.columns:
                etf_data = df[df["基金代码"] == etf_code]
                if not etf_data.empty:
                    logger.info(f"📊 {etf_code} 数据示例:")
                    logger.info(f"{etf_data.head(1).to_dict(orient='records')}")
                else:
                    logger.info(f"❌ 未找到ETF {etf_code} 的数据")
        else:
            # 其他接口返回的是特定ETF的数据
            logger.info("📊 数据示例:")
            logger.info(f"{df.head(1).to_dict(orient='records')}")
    else:
        logger.info(f"❌ {api_name} 接口返回空数据")

if __name__ == "__main__":
    logger.info("===== 开始执行ETF API测试 =====")
    test_etf_apis()
    logger.info("===== ETF API测试执行完成 =====")
