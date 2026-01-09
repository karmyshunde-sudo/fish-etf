#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
极简版套利策略计算模块
核心原则：只基于实时折价率发现套利机会
移除所有复杂评分、历史数据分析等冗余逻辑
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime
from typing import Tuple, List
from config import Config
from utils.date_utils import get_beijing_time
from data_crawler.strategy_arbitrage_source import get_latest_arbitrage_opportunities
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

def validate_arbitrage_data(df: pd.DataFrame) -> bool:
    """
    极简数据验证
    """
    if df.empty:
        logger.warning("实时套利数据为空")
        return False
    
    required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV", "折价率"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"实时套利数据缺少必要列: {', '.join(missing_columns)}")
        return False
    
    # 基本数据质量检查
    valid_count = len(df[
        (df["市场价格"] > 0.01) & 
        (df["IOPV"] > 0.01) &
        (df["折价率"].between(-50, 100))  # 折价率在合理范围内
    ])
    
    if valid_count < len(df) * 0.8:  # 如果超过20%数据异常
        logger.warning(f"数据质量不佳: {valid_count}/{len(df)} 条数据有效")
    
    return True

def calculate_arbitrage_opportunity() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    极简版套利机会计算
    只基于实时折价率进行过滤和排序
    """
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
        
        # 2. 过滤异常折价率（放宽到±50%容忍度）
        df = df[(df["折价率"] >= -50) & (df["折价率"] <= 100)].copy()
        
        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            logger.info(f"过滤掉 {filtered_count} 个无效数据，剩余 {len(df)} 个")
        
        if df.empty:
            logger.warning("过滤后无有效数据")
            return pd.DataFrame(), pd.DataFrame()
        
        logger.info(f"数据折价率范围: {df['折价率'].min():.2f}% ~ {df['折价率'].max():.2f}%")
        
        # 使用配置的阈值（保持兼容性）
        ARBITRAGE_THRESHOLD = getattr(Config, 'MIN_ARBITRAGE_DISPLAY_THRESHOLD', 1.0)
        
        # 区分折价和溢价机会
        # 折价：市场价格 < IOPV（折价率为负）
        # 溢价：市场价格 > IOPV（折价率为正）
        
        discount_opportunities = df[df["折价率"] <= -ARBITRAGE_THRESHOLD].copy()
        premium_opportunities = df[df["折价率"] >= ARBITRAGE_THRESHOLD].copy()
        
        # 按折价率排序（折价：最负的在前；溢价：最正的在前）
        if not discount_opportunities.empty:
            discount_opportunities = discount_opportunities.sort_values("折价率", ascending=True)
        
        if not premium_opportunities.empty:
            premium_opportunities = premium_opportunities.sort_values("折价率", ascending=False)
        
        logger.info(f"发现 {len(discount_opportunities)} 个折价机会 (≤-{ARBITRAGE_THRESHOLD}%)")
        logger.info(f"发现 {len(premium_opportunities)} 个溢价机会 (≥{ARBITRAGE_THRESHOLD}%)")
        
        # 添加调试信息
        if not discount_opportunities.empty:
            logger.info(f"折价机会范围: {discount_opportunities['折价率'].min():.2f}% ~ {discount_opportunities['折价率'].max():.2f}%")
        
        if not premium_opportunities.empty:
            logger.info(f"溢价机会范围: {premium_opportunities['折价率'].min():.2f}% ~ {premium_opportunities['折价率'].max():.2f}%")
        
        return discount_opportunities, premium_opportunities
        
    except Exception as e:
        logger.error(f"计算套利机会失败: {str(e)}", exc_info=True)
        return pd.DataFrame(), pd.DataFrame()

def generate_arbitrage_message(discount_opportunities: pd.DataFrame, premium_opportunities: pd.DataFrame) -> List[str]:
    """
    极简版套利消息生成
    保持与原有函数名兼容性
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
        
        for i, (_, row) in enumerate(discount_opportunities.head(10).iterrows(), 1):
            discount_rate = abs(row["折价率"])  # 取绝对值显示
            price_diff = row["IOPV"] - row["市场价格"]
            
            discount_msg += f"{i}. {row['ETF名称']} ({row['ETF代码']})\n"
            discount_msg += f"   📉 折价率: {discount_rate:.2f}%\n"
            discount_msg += f"   💰 市场价格: {row['市场价格']:.3f}元\n"
            discount_msg += f"   📊 IOPV净值: {row['IOPV']:.3f}元\n"
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
        
        for i, (_, row) in enumerate(premium_opportunities.head(10).iterrows(), 1):
            premium_rate = row["折价率"]
            price_diff = row["市场价格"] - row["IOPV"]
            
            premium_msg += f"{i}. {row['ETF名称']} ({row['ETF代码']})\n"
            premium_msg += f"   📈 溢价率: {premium_rate:.2f}%\n"
            premium_msg += f"   💰 市场价格: {row['市场价格']:.3f}元\n"
            premium_msg += f"   📊 IOPV净值: {row['IOPV']:.3f}元\n"
            premium_msg += f"   💵 套利空间: {price_diff:.3f}元\n\n"
        
        premium_msg += f"📅 北京时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M')}\n"
        premium_msg += f"📊 环境：{os.getenv('ENVIRONMENT', 'Git-fish-etf')}"
        
        messages.append(premium_msg)
        logger.info(f"生成溢价机会消息，包含 {min(len(premium_opportunities), 10)} 个机会")
    
    return messages

# ===== 保持兼容性的空函数 =====
# 原系统调用这些函数，但我们简化版不需要它们，所以提供空实现

def add_etf_basic_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    简化版：只为保持兼容性而保留的空函数
    """
    return df

def calculate_arbitrage_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    简化版：只为保持兼容性而保留的空函数
    """
    if not df.empty and "综合评分" not in df.columns:
        df["综合评分"] = 0.0  # 添加一个空列保持兼容性
    return df

def filter_new_discount_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    简化版：直接返回所有机会，不过滤
    """
    return df

def filter_new_premium_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    简化版：直接返回所有机会，不过滤
    """
    return df

def mark_arbitrage_opportunities_pushed(discount_df: pd.DataFrame, premium_df: pd.DataFrame) -> bool:
    """
    简化版：只为保持兼容性而保留的空函数
    """
    return True

def sort_opportunities_by_abs_premium(df: pd.DataFrame) -> pd.DataFrame:
    """
    简化版：按折价率绝对值排序
    """
    if df.empty:
        return df
    
    df["abs_premium_discount"] = df["折价率"].abs()
    df = df.sort_values("abs_premium_discount", ascending=False)
    df = df.drop(columns=["abs_premium_discount"])
    return df

def filter_valid_discount_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    简化版：过滤有效折价机会
    """
    if df.empty:
        return df
    
    ARBITRAGE_THRESHOLD = getattr(Config, 'MIN_ARBITRAGE_DISPLAY_THRESHOLD', 1.0)
    filtered_df = df[df["折价率"] <= -ARBITRAGE_THRESHOLD].copy()
    
    if not filtered_df.empty:
        filtered_df = filtered_df.sort_values("折价率", ascending=True)
    
    return filtered_df

def filter_valid_premium_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    简化版：过滤有效溢价机会
    """
    if df.empty:
        return df
    
    ARBITRAGE_THRESHOLD = getattr(Config, 'MIN_ARBITRAGE_DISPLAY_THRESHOLD', 1.0)
    filtered_df = df[df["折价率"] >= ARBITRAGE_THRESHOLD].copy()
    
    if not filtered_df.empty:
        filtered_df = filtered_df.sort_values("折价率", ascending=False)
    
    return filtered_df

# ===== 其他保持兼容性的函数 =====

def calculate_premium_discount(market_price: float, iopv: float) -> float:
    """计算折溢价率"""
    if iopv <= 0:
        return 0.0
    return ((market_price - iopv) / iopv) * 100

def get_arbitrage_push_statistics() -> dict:
    """获取套利推送统计信息（简化版）"""
    return {
        "arbitrage": {"total_pushed": 0, "today_pushed": 0},
        "discount": {"total_pushed": 0, "today_pushed": 0},
        "premium": {"total_pushed": 0, "today_pushed": 0}
    }

# 模块初始化
try:
    logger.info("极简套利策略模块初始化完成")
    
except Exception as e:
    error_msg = f"极简套利策略模块初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
