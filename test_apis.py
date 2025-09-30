#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF API测试工具
使用固定ETF代码和日期测试所有可能的API接口
包括AkShare和新浪数据源
"""

import akshare as ak
import pandas as pd
import logging
import os
import requests
import json
import re
from datetime import datetime
from config import Config

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
    
    # 1. 测试fund_etf_hist_sina接口（AkShare）
    test_fund_etf_hist_sina(etf_code, start_date, end_date)
    
    # 2. 测试fund_etf_spot_em接口（AkShare）
    test_fund_etf_spot_em(etf_code, start_date, end_date)
    
    # 3. 测试fund_etf_fund_daily_em接口（AkShare）
    test_fund_etf_fund_daily_em(etf_code, start_date, end_date)
    
    # 4. 测试stock_zh_a_hist接口（AkShare）
    test_stock_zh_a_hist(etf_code, start_date, end_date)
    
    # 5. 测试stock_zh_a_hist_min接口（AkShare）
    test_stock_zh_a_hist_min(etf_code, start_date, end_date)
    
    # 6. 测试fund_etf_daily_em接口（AkShare）
    test_fund_etf_daily_em(etf_code, start_date, end_date)
    
    # 7. 测试fund_etf_hist_em接口（AkShare）
    test_fund_etf_hist_em(etf_code, start_date, end_date)
    
    # 8. 测试新浪数据源接口
    test_sina_data_source(etf_code, start_date, end_date)
    
    logger.info("=" * 50)
    logger.info("ETF API测试完成")
    logger.info("请提供以上日志，将基于实际返回的列名编写匹配代码")
    logger.info("=" * 50)

def test_fund_etf_hist_sina(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_hist_sina接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_hist_sina (无市场前缀)")
    logger.info(f"ak.fund_etf_hist_sina(symbol='{etf_code}')")
    logger.info("=" * 50)
    try:
        df = ak.fund_etf_hist_sina(symbol=etf_code)
        log_api_result("ak.fund_etf_hist_sina", df, etf_code)
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_spot_em(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_spot_em接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_spot_em")
    logger.info(f"ak.fund_etf_spot_em()")
    logger.info("=" * 50)
    try:
        df = ak.fund_etf_spot_em()
        log_api_result("ak.fund_etf_spot_em", df, etf_code)
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_fund_daily_em(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_fund_daily_em接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_fund_daily_em")
    logger.info(f"ak.fund_etf_fund_daily_em()")
    logger.info("=" * 50)
    try:
        df = ak.fund_etf_fund_daily_em()
        log_api_result("ak.fund_etf_fund_daily_em", df, etf_code)
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_stock_zh_a_hist(etf_code: str, start_date: str, end_date: str):
    """测试stock_zh_a_hist接口"""
    # 测试上交所ETF
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.stock_zh_a_hist (上交所)")
    logger.info(f"ak.stock_zh_a_hist(symbol='sh{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 50)
    try:
        df = ak.stock_zh_a_hist(
            symbol=f"sh{etf_code}",
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        log_api_result("ak.stock_zh_a_hist", df, etf_code)
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")
    
    # 测试深交所ETF
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.stock_zh_a_hist (深交所)")
    logger.info(f"ak.stock_zh_a_hist(symbol='sz{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 50)
    try:
        df = ak.stock_zh_a_hist(
            symbol=f"sz{etf_code}",
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        log_api_result("ak.stock_zh_a_hist", df, etf_code)
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_stock_zh_a_hist_min(etf_code: str, start_date: str, end_date: str):
    """测试stock_zh_a_hist_min接口"""
    # 测试上交所ETF
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.stock_zh_a_hist_min (上交所)")
    logger.info(f"ak.stock_zh_a_hist_min(symbol='sh{etf_code}', period='5', start_date='{start_date} 09:30:00', end_date='{end_date} 15:00:00')")
    logger.info("=" * 50)
    try:
        df = ak.stock_zh_a_hist_min(
            symbol=f"sh{etf_code}",
            period="5",
            start_date=f"{start_date} 09:30:00",
            end_date=f"{end_date} 15:00:00"
        )
        log_api_result("ak.stock_zh_a_hist_min", df, etf_code)
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")
    
    # 测试深交所ETF
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.stock_zh_a_hist_min (深交所)")
    logger.info(f"ak.stock_zh_a_hist_min(symbol='sz{etf_code}', period='5', start_date='{start_date} 09:30:00', end_date='{end_date} 15:00:00')")
    logger.info("=" * 50)
    try:
        df = ak.stock_zh_a_hist_min(
            symbol=f"sz{etf_code}",
            period="5",
            start_date=f"{start_date} 09:30:00",
            end_date=f"{end_date} 15:00:00"
        )
        log_api_result("ak.stock_zh_a_hist_min", df, etf_code)
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_daily_em(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_daily_em接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_daily_em")
    logger.info(f"ak.fund_etf_daily_em(symbol='{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 50)
    try:
        df = ak.fund_etf_daily_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        log_api_result("ak.fund_etf_daily_em", df, etf_code)
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_fund_etf_hist_em(etf_code: str, start_date: str, end_date: str):
    """测试fund_etf_hist_em接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 ak.fund_etf_hist_em")
    logger.info(f"ak.fund_etf_hist_em(symbol='{etf_code}', period='daily', start_date='{start_date}', end_date='{end_date}')")
    logger.info("=" * 50)
    try:
        df = ak.fund_etf_hist_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        log_api_result("ak.fund_etf_hist_em", df, etf_code)
    except Exception as e:
        logger.info(f"❌ 接口调用失败: {str(e)}")

def test_sina_data_source(etf_code: str, start_date: str, end_date: str):
    """测试新浪数据源接口"""
    logger.info("\n" + "=" * 50)
    logger.info(f"测试 新浪数据源")
    logger.info(f"URL: https://finance.sina.com.cn/realstock/company/{etf_code}/hisdata/klc_kl.js")
    logger.info("=" * 50)
    
    try:
        # 尝试获取新浪数据
        url = f"https://finance.sina.com.cn/realstock/company/{etf_code}/hisdata/klc_kl.js"
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.info(f"❌ 新浪接口请求失败: HTTP {response.status_code}")
            return
        
        # 尝试提取JSON数据
        try:
            # 提取JSON数据
            json_data = extract_json_from_sina_response(response.text)
            
            if json_data:
                logger.info("✅ 新浪接口返回数据")
                logger.info(f"📊 数据结构: {json_data.keys()}")
                
                # 记录数据示例
                if "data" in json_data and "date" in json_data["data"]:
                    logger.info(f"📊 日期列示例: {json_data['data']['date'][:2]}")
                    logger.info(f"📊 开盘列示例: {json_data['data']['open'][:2]}")
                    logger.info(f"📊 收盘列示例: {json_data['data']['close'][:2]}")
                    logger.info(f"📊 最高列示例: {json_data['data']['high'][:2]}")
                    logger.info(f"📊 最低列示例: {json_data['data']['low'][:2]}")
                    logger.info(f"📊 成交量列示例: {json_data['data']['volume'][:2]}")
            else:
                logger.info("❌ 无法解析新浪接口返回的数据")
        except Exception as e:
            logger.info(f"❌ 数据解析失败: {str(e)}")
    except Exception as e:
        logger.info(f"❌ 新浪接口请求失败: {str(e)}")

def extract_json_from_sina_response(text: str) -> dict:
    """从新浪响应中提取JSON数据"""
    try:
        # 尝试查找JSON数据
        json_match = re.search(r'var\s+klc_kl\s*=\s*({.*?});', text, re.DOTALL)
        if json_match:
            # 提取JSON字符串
            json_str = json_match.group(1)
            return json.loads(json_str)
        
        # 如果没有找到标准JSON，尝试其他格式
        json_match = re.search(r'var\s+klc_kl\s*=\s*({.*?})', text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
            return json.loads(json_str)
        
        # 如果是直接返回JSON
        try:
            return json.loads(text)
        except:
            pass
            
        return {}
    except Exception as e:
        logger.error(f"解析新浪数据失败: {str(e)}")
        return {}

def log_api_result(api_name: str, df: pd.DataFrame, etf_code: str):
    """记录API测试结果"""
    if isinstance(df, pd.DataFrame) and not df.empty:
        logger.info(f"✅ {api_name} 接口返回 {len(df)} 条数据")
        logger.info(f"📊 返回的列名: {list(df.columns)}")
        
        # 根据接口类型处理数据筛选
        if "spot" in api_name or "fund_daily" in api_name:
            # 这些接口返回所有ETF数据，需要筛选
            if "代码" in df.columns:
                etf_data = df[df["代码"] == etf_code]
                if not etf_data.empty:
                    logger.info(f"📊 {etf_code} 数据示例:")
                    logger.info(f"{etf_data.head(1).to_dict(orient='records')}")
                else:
                    logger.info(f"❌ 未找到ETF {etf_code} 的数据")
            elif "基金代码" in df.columns:
                etf_data = df[df["基金代码"] == etf_code]
                if not etf_data.empty:
                    logger.info(f"📊 {etf_code} 数据示例:")
                    logger.info(f"{etf_data.head(1).to_dict(orient='records')}")
                else:
                    logger.info(f"❌ 未找到ETF {etf_code} 的数据")
            else:
                logger.info("📊 数据示例:")
                logger.info(f"{df.head(1).to_dict(orient='records')}")
        else:
            # 其他接口直接返回特定ETF数据
            logger.info("📊 数据示例:")
            logger.info(f"{df.head(1).to_dict(orient='records')}")
    else:
        logger.info(f"❌ {api_name} 接口返回空数据")

if __name__ == "__main__":
    logger.info("===== 开始执行ETF API测试 =====")
    test_etf_apis()
    logger.info("===== ETF API测试执行完成 =====")
