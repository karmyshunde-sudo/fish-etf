#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF API测试工具
使用固定ETF代码和日期测试所有可能的API接口
"""

import akshare as ak
import pandas as pd
import logging
import time
import os
from datetime import datetime

# 初始化日志
logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)

def test_etf_apis():
    """测试所有ETF相关API接口的返回结构"""
    # 固定测试参数
    etf_code = "159915"  # 创业板ETF
    start_date = "20250701"  # 测试日期
    end_date = "202507011"  # 结束日期
    
    logger.info("=" * 50)
    logger.info(f"开始测试ETF {etf_code} API接口")
    logger.info(f"测试日期: {start_date} 至 {end_date}")
    logger.info("=" * 50)
    
    # 1. 测试fund_etf_hist_sina接口（无市场前缀）
    test_fund_etf_hist_sina(etf_code, start_date, end_date, with_prefix=False)
    
    # 2. 测试fund_etf_hist_sina接口（带sh市场前缀）
    test_fund_etf_hist_sina(etf_code, start_date, end_date, with_prefix="sh")
    
    # 3. 测试fund_etf_hist_sina接口（带sz市场前缀）
    test_fund_etf_hist_sina(etf_code, start_date, end_date, with_prefix="sz")
    
    # 4. 测试fund_etf_spot_em接口
    test_fund_etf_spot_em(etf_code)
    
    # 5. 测试fund_etf_fund_daily_em接口
    test_fund_etf_fund_daily_em(etf_code)
    
    # 6. 测试stock_zh_a_hist接口（sh前缀）
    test_stock_zh_a_hist(etf_code, start_date, end_date, market_prefix="sh")
    
    # 7. 测试stock_zh_a_hist接口（sz前缀）
    test_stock_zh_a_hist(etf_code, start_date, end_date, market_prefix="sz")
    
    # 8. 测试stock_zh_a_hist_min接口（sh前缀）
    test_stock_zh_a_hist_min(etf_code, start_date, end_date, market_prefix="sh")
    
    # 9. 测试stock_zh_a_hist_min接口（sz前缀）
    test_stock_zh_a_hist_min(etf_code, start_date, end_date, market_prefix="sz")
    
    # 10. 测试fund_etf_daily_em接口
    test_fund_etf_daily_em(etf_code, start_date, end_date)
    
    # 11. 测试fund_etf_hist_em接口
    test_fund_etf_hist_em(etf_code, start_date, end_date)
    
    logger.info("=" * 50)
    logger.info("ETF API测试完成")
    logger.info("请提供以上日志，将基于实际返回的列名编写匹配代码")
    logger.info("=" * 50)

def test_fund_etf_hist_sina(etf_code: str, start_date: str, end_date: str, with_prefix=False):
    """测试fund_etf_hist_sina接口"""
    prefix = ""
    symbol = etf_code
    
    if with_prefix:
        logger.info("\n" + "=" * 50)
        logger.info(f"测试 ak.fund_etf_hist_sina (带市场前缀)")
        logger.info(f"ak.fund_etf_hist_sina(symbol='{with_prefix}{etf_code}')")
        logger.info("=" * 50)
        
        prefix = with_prefix
        symbol = f"{with_prefix}{etf_code}"
    else:
        logger.info("\n" + "=" * 50)
        logger.info(f"测试 ak.fund_etf_hist_sina (无市场前缀)")
        logger.info(f"ak.fund_etf_hist_sina(symbol='{etf_code}')")
        logger.info("=" * 50)
    
    try:
        # 调用API
        df = ak.fund_etf_hist_sina(symbol=symbol)
        
        # 记录结果
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ ak.fund_etf_hist_sina 接口返回 {len(df)} 条数据")
            logger.info(f"📊 返回的列名: {list(df.columns)}")
            
            # 记录数据示例
            if not df.empty:
                logger.info("📊 数据示例:")
                logger.info(f"{df.head(1).to_dict(orient='records')}")
        else:
            logger.info(f"❌ ak.fund_etf_hist_sina 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_spot_em(etf_code: str):
    """测试fund_etf_spot_em接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_spot_em")
    logger.info(f"ak.fund_etf_spot_em()")
    logger.info("=" * 50)
    
    try:
        # 调用API
        df = ak.fund_etf_spot_em()
        
        # 记录结果
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ ak.fund_etf_spot_em 接口返回 {len(df)} 条数据")
            logger.info(f"📊 返回的列名: {list(df.columns)}")
            
            # 筛选特定ETF
            if "代码" in df.columns:
                etf_data = df[df["代码"] == etf_code]
                if not etf_data.empty:
                    logger.info(f"📊 {etf_code} 数据示例:")
                    logger.info(f"{etf_data.head(1).to_dict(orient='records')}")
                else:
                    logger.info(f"❌ {etf_code} 无数据")
            else:
                logger.info("❌ 无'代码'列，无法筛选ETF")
        else:
            logger.info("❌ ak.fund_etf_spot_em 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_fund_daily_em(etf_code: str):
    """测试fund_etf_fund_daily_em接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_fund_daily_em")
    logger.info(f"ak.fund_etf_fund_daily_em()")
    logger.info("=" * 50)
    
    try:
        # 调用API
        df = ak.fund_etf_fund_daily_em()
        
        # 记录结果
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ ak.fund_etf_fund_daily_em 接口返回 {len(df)} 条数据")
            logger.info(f"📊 返回的列名: {list(df.columns)}")
            
            # 筛选特定ETF
            if "基金代码" in df.columns:
                etf_data = df[df["基金代码"] == etf_code]
                if not etf_data.empty:
                    logger.info(f"📊 {etf_code} 数据示例:")
                    logger.info(f"{etf_data.head(1).to_dict(orient='records')}")
                else:
                    logger.info(f"❌ {etf_code} 无数据")
            else:
                logger.info("❌ 无'基金代码'列，无法筛选ETF")
        else:
            logger.info("❌ ak.fund_etf_fund_daily_em 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_stock_zh_a_hist(etf_code: str, start_date: str, end_date: str, market_prefix: str):
    """测试stock_zh_a_hist接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.stock_zh_a_hist ({market_prefix}前缀)")
    logger.info(f"ak.stock_zh_a_hist(symbol='{market_prefix}{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 50)
    
    try:
        # 添加市场前缀
        symbol = f"{market_prefix}{etf_code}"
        
        # 调用API
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        
        # 记录结果
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ ak.stock_zh_a_hist 接口返回 {len(df)} 条数据")
            logger.info(f"📊 返回的列名: {list(df.columns)}")
            
            # 记录数据示例
            logger.info("📊 数据示例:")
            logger.info(f"{df.head(1).to_dict(orient='records')}")
        else:
            logger.info("❌ ak.stock_zh_a_hist 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_stock_zh_a_hist_min(etf_code: str, start_date: str, end_date: str, market_prefix: str):
    """测试stock_zh_a_hist_min接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.stock_zh_a_hist_min ({market_prefix}前缀)")
    logger.info(f"ak.stock_zh_a_hist_min(symbol='{market_prefix}{etf_code}', period='5', start_date='{start_date} 09:30:00', end_date='{end_date} 15:00:00')")
    logger.info("=" * 50)
    
    try:
        # 添加市场前缀
        symbol = f"{market_prefix}{etf_code}"
        
        # 调用API
        df = ak.stock_zh_a_hist_min(
            symbol=symbol,
            period="5",
            start_date=f"{start_date} 09:30:00",
            end_date=f"{end_date} 15:00:00"
        )
        
        # 记录结果
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ ak.stock_zh_a_hist_min 接口返回 {len(df)} 条数据")
            logger.info(f"📊 返回的列名: {list(df.columns)}")
            
            # 记录数据示例
            logger.info("📊 数据示例:")
            logger.info(f"{df.head(1).to_dict(orient='records')}")
        else:
            logger.info("❌ ak.stock_zh_a_hist_min 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_daily_em(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_daily_em接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_daily_em")
    logger.info(f"ak.fund_etf_daily_em(symbol='{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 50)
    
    try:
        # 调用API
        df = ak.fund_etf_daily_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        
        # 记录结果
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ ak.fund_etf_daily_em 接口返回 {len(df)} 条数据")
            logger.info(f"📊 返回的列名: {list(df.columns)}")
            
            # 记录数据示例
            logger.info("📊 数据示例:")
            logger.info(f"{df.head(1).to_dict(orient='records')}")
        else:
            logger.info("❌ ak.fund_etf_daily_em 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_hist_em(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_hist_em接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_hist_em")
    logger.info(f"ak.fund_etf_hist_em(symbol='{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 50)
    
    try:
        # 调用API
        df = ak.fund_etf_hist_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        
        # 记录结果
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ ak.fund_etf_hist_em 接口返回 {len(df)} 条数据")
            logger.info(f"📊 返回的列名: {list(df.columns)}")
            
            # 记录数据示例
            logger.info("📊 数据示例:")
            logger.info(f"{df.head(1).to_dict(orient='records')}")
        else:
            logger.info("❌ ak.fund_etf_hist_em 接口返回空数据")
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_sina_data_source(etf_code: str, start_date: str, end_date: str):
    """测试新浪数据源接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 新浪数据源")
    logger.info(f"URL: https://finance.sina.com.cn/realstock/company/{etf_code}/hisdata/klc_kl.js")
    logger.info("=" * 50)
    
    try:
        import requests
        # 调用API
        url = f"https://finance.sina.com.cn/realstock/company/{etf_code}/hisdata/klc_kl.js"
        response = requests.get(url)
        
        # 记录结果
        if response.status_code == 200:
            logger.info(f"✅ 新浪接口请求成功")
            # 尝试解析数据
            # 这里可以添加数据解析逻辑
        else:
            logger.info(f"❌ 新浪接口请求失败: HTTP {response.status_code}")
    except Exception as e:
        logger.info(f"❌ 新浪接口请求失败: {str(e)}")

if __name__ == "__main__":
    logger.info("===== 开始执行ETF API测试 =====")
    test_etf_apis()
    logger.info("===== ETF API测试执行完成 =====")
