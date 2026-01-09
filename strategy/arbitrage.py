#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复版套利策略计算模块
解决消息格式化问题
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime
from typing import Tuple, List, Dict, Any
from config import Config
from utils.date_utils import get_beijing_time
from data_crawler.strategy_arbitrage_source import get_latest_arbitrage_opportunities
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

def validate_arbitrage_data(df: pd.DataFrame) -> bool:
    """极简数据验证"""
    if df.empty:
        logger.warning("实时套利数据为空")
        return False
    
    required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV", "折价率"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"实时套利数据缺少必要列: {', '.join(missing_columns)}")
        return False
    
    return True

def calculate_arbitrage_opportunity() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """修复版套利机会计算"""
    try:
        logger.info("开始计算套利机会")
        
        # 获取实时数据
        df = get_latest_arbitrage_opportunities()
        
        if not validate_arbitrage_data(df):
            logger.error("实时套利数据验证失败")
            return pd.DataFrame(), pd.DataFrame()
        
        # 数据基本清洗
        initial_count = len(df)
        
        # 1. 过滤无效价格
        df = df[(df["市场价格"] > 0.01) & (df["IOPV"] > 0.01)].copy()
        
        # 2. 过滤异常折价率
        df = df[(df["折价率"] >= -50) & (df["折价率"] <= 100)].copy()
        
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            logger.info(f"过滤掉 {filtered_count} 个无效数据，剩余 {len(df)} 个")
        
        if df.empty:
            logger.warning("过滤后无有效数据")
            return pd.DataFrame(), pd.DataFrame()
        
        logger.info(f"数据折价率范围: {df['折价率'].min():.2f}% ~ {df['折价率'].max():.2f}%")
        
        # 使用配置的阈值
        ARBITRAGE_THRESHOLD = getattr(Config, 'MIN_ARBITRAGE_DISPLAY_THRESHOLD', 1.0)
        
        # 区分折价和溢价机会
        discount_opportunities = df[df["折价率"] <= -ARBITRAGE_THRESHOLD].copy()
        premium_opportunities = df[df["折价率"] >= ARBITRAGE_THRESHOLD].copy()
        
        # 按折价率排序
        if not discount_opportunities.empty:
            discount_opportunities = discount_opportunities.sort_values("折价率", ascending=True)
        
        if not premium_opportunities.empty:
            premium_opportunities = premium_opportunities.sort_values("折价率", ascending=False)
        
        logger.info(f"发现 {len(discount_opportunities)} 个折价机会 (≤-{ARBITRAGE_THRESHOLD}%)")
        logger.info(f"发现 {len(premium_opportunities)} 个溢价机会 (≥{ARBITRAGE_THRESHOLD}%)")
        
        if not discount_opportunities.empty:
            logger.info(f"折价机会范围: {discount_opportunities['折价率'].min():.2f}% ~ {discount_opportunities['折价率'].max():.2f}%")
        
        if not premium_opportunities.empty:
            logger.info(f"溢价机会范围: {premium_opportunities['折价率'].min():.2f}% ~ {premium_opportunities['折价率'].max():.2f}%")
        
        return discount_opportunities, premium_opportunities
        
    except Exception as e:
        logger.error(f"计算套利机会失败: {str(e)}", exc_info=True)
        return pd.DataFrame(), pd.DataFrame()

def format_etf_data_for_push(df: pd.DataFrame, opportunity_type: str) -> List[Dict[str, Any]]:
    """
    格式化ETF数据用于推送
    返回字典列表，确保wechat_push模块能正确处理
    """
    if df.empty:
        return []
    
    formatted_data = []
    
    # 只取前10个机会
    display_df = df.head(10).copy()
    
    for _, row in display_df.iterrows():
        try:
            # 提取核心数据
            etf_code = str(row.get("ETF代码", "")).strip()
            etf_name = str(row.get("ETF名称", "")).strip()
            market_price = float(row.get("市场价格", 0))
            iopv = float(row.get("IOPV", 0))
            discount_rate = float(row.get("折价率", 0))
            
            if not etf_code or not etf_name:
                continue
            
            # 计算价差
            if discount_rate < 0:  # 折价
                price_diff = iopv - market_price
            else:  # 溢价
                price_diff = market_price - iopv
            
            # 创建格式化数据
            formatted_item = {
                "code": etf_code,
                "name": etf_name,
                "market_price": market_price,
                "iopv": iopv,
                "discount_rate": discount_rate,
                "price_diff": price_diff,
                "type": opportunity_type
            }
            
            formatted_data.append(formatted_item)
            
        except (ValueError, TypeError) as e:
            logger.debug(f"格式化ETF数据失败: {str(e)}")
            continue
    
    return formatted_data

def generate_arbitrage_message(discount_opportunities: pd.DataFrame, premium_opportunities: pd.DataFrame) -> List[str]:
    """
    生成套利消息 - 修复版
    返回字符串消息，兼容原有系统
    """
    messages = []
    
    # 获取配置阈值
    ARBITRAGE_THRESHOLD = getattr(Config, 'MIN_ARBITRAGE_DISPLAY_THRESHOLD', 1.0)
    
    # 生成折价消息
    if not discount_opportunities.empty:
        discount_msg = "【折价套利机会】\n"
        discount_msg += f"💰 操作建议：二级市场买入ETF，一级市场赎回\n"
        discount_msg += f"📊 筛选条件：折价率≥{ARBITRAGE_THRESHOLD}%\n"
        discount_msg += "==================\n"
        
        formatted_data = format_etf_data_for_push(discount_opportunities, "discount")
        
        for i, item in enumerate(formatted_data, 1):
            discount_rate = abs(item["discount_rate"])  # 取绝对值显示
            price_diff = item["price_diff"]
            
            discount_msg += f"{i}. {item['name']} ({item['code']})\n"
            discount_msg += f"   📉 折价率: {discount_rate:.2f}%\n"
            discount_msg += f"   💰 市场价格: {item['market_price']:.3f}元\n"
            discount_msg += f"   📊 IOPV净值: {item['iopv']:.3f}元\n"
            discount_msg += f"   💵 套利空间: {price_diff:.3f}元\n\n"
        
        discount_msg += f"📅 北京时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M')}\n"
        discount_msg += f"📊 环境：{os.getenv('ENVIRONMENT', 'Git-fish-etf')}"
        
        messages.append(discount_msg)
        logger.info(f"生成折价机会消息，包含 {min(len(discount_opportunities), 10)} 个机会")
    
    # 生成溢价消息
    if not premium_opportunities.empty:
        premium_msg = "【溢价套利机会】\n"
        premium_msg += f"💰 操作建议：一级市场申购ETF，二级市场卖出\n"
        premium_msg += f"📊 筛选条件：溢价率≥{ARBITRAGE_THRESHOLD}%\n"
        premium_msg += "==================\n"
        
        formatted_data = format_etf_data_for_push(premium_opportunities, "premium")
        
        for i, item in enumerate(formatted_data, 1):
            premium_rate = item["discount_rate"]
            price_diff = item["price_diff"]
            
            premium_msg += f"{i}. {item['name']} ({item['code']})\n"
            premium_msg += f"   📈 溢价率: {premium_rate:.2f}%\n"
            premium_msg += f"   💰 市场价格: {item['market_price']:.3f}元\n"
            premium_msg += f"   📊 IOPV净值: {item['iopv']:.3f}元\n"
            premium_msg += f"   💵 套利空间: {price_diff:.3f}元\n\n"
        
        premium_msg += f"📅 北京时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M')}\n"
        premium_msg += f"📊 环境：{os.getenv('ENVIRONMENT', 'Git-fish-etf')}"
        
        messages.append(premium_msg)
        logger.info(f"生成溢价机会消息，包含 {min(len(premium_opportunities), 10)} 个机会")
    
    return messages

# ===== 保持兼容性的空函数 =====
def add_etf_basic_info(df: pd.DataFrame) -> pd.DataFrame:
    return df

def calculate_arbitrage_scores(df: pd.DataFrame) -> pd.DataFrame:
    if not df.empty and "综合评分" not in df.columns:
        df["综合评分"] = 0.0
    return df

def filter_new_discount_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    return df

def filter_new_premium_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    return df

def mark_arbitrage_opportunities_pushed(discount_df: pd.DataFrame, premium_df: pd.DataFrame) -> bool:
    return True

def sort_opportunities_by_abs_premium(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["abs_premium_discount"] = df["折价率"].abs()
    df = df.sort_values("abs_premium_discount", ascending=False)
    df = df.drop(columns=["abs_premium_discount"])
    return df

def filter_valid_discount_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    ARBITRAGE_THRESHOLD = getattr(Config, 'MIN_ARBITRAGE_DISPLAY_THRESHOLD', 1.0)
    filtered_df = df[df["折价率"] <= -ARBITRAGE_THRESHOLD].copy()
    if not filtered_df.empty:
        filtered_df = filtered_df.sort_values("折价率", ascending=True)
    return filtered_df

def filter_valid_premium_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    ARBITRAGE_THRESHOLD = getattr(Config, 'MIN_ARBITRAGE_DISPLAY_THRESHOLD', 1.0)
    filtered_df = df[df["折价率"] >= ARBITRAGE_THRESHOLD].copy()
    if not filtered_df.empty:
        filtered_df = filtered_df.sort_values("折价率", ascending=False)
    return filtered_df

def calculate_premium_discount(market_price: float, iopv: float) -> float:
    if iopv <= 0:
        return 0.0
    return ((market_price - iopv) / iopv) * 100

def get_arbitrage_push_statistics() -> dict:
    return {
        "arbitrage": {"total_pushed": 0, "today_pushed": 0},
        "discount": {"total_pushed": 0, "today_pushed": 0},
        "premium": {"total_pushed": 0, "today_pushed": 0}
    }

# 模块初始化
try:
    logger.info("修复版套利策略模块初始化完成")
except Exception as e:
    error_msg = f"修复版套利策略模块初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
