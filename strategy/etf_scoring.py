#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF评分系统
基于多维度指标对ETF进行综合评分
特别优化了消息推送格式，确保使用统一的消息模板
"""

import pandas as pd
import numpy as np
import logging
import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    is_file_outdated
)
from utils.file_utils import load_etf_daily_data, load_etf_metadata, ensure_chinese_columns
from data_crawler.all_etfs import get_all_etf_codes, get_etf_name
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

# 常量定义
DATE_COL = "日期" if "日期" in Config.STANDARD_COLUMNS else "date"
CLOSE_COL = "收盘" if "收盘" in Config.STANDARD_COLUMNS else "close"
AMOUNT_COL = "成交额" if "成交额" in Config.STANDARD_COLUMNS else "amount"
ETF_CODE_COL = "ETF代码"
FUND_SIZE_COL = "基金规模"

def extract_scalar_value(value, default=0.0, log_prefix=""):
    """
    安全地从各种类型中提取标量值
    
    Args:
        value: 可能是标量、Series、DataFrame、字符串等
        default: 默认值，如果无法提取标量值
        log_prefix: 日志前缀，用于标识调用位置
    
    Returns:
        float: 标量值
    """
    try:
        # 如果已经是标量值，直接返回
        if isinstance(value, (int, float)):
            return float(value)
        
        # 如果是字符串，尝试转换为浮点数
        if isinstance(value, str):
            # 尝试移除非数字字符
            cleaned_str = ''.join(c for c in value if c.isdigit() or c in ['.', '-'])
            if cleaned_str:
                result = float(cleaned_str)
                logger.debug(f"{log_prefix}从字符串提取标量值: '{value}' -> {result}")
                return result
            logger.warning(f"{log_prefix}无法从字符串 '{value}' 提取有效数字，使用默认值{default}")
            return default
        
        # 如果是pandas对象，尝试提取标量值
        if isinstance(value, (pd.Series, pd.DataFrame)):
            # 尝试获取第一个值
            if value.size > 0:
                # 尝试使用.values.flatten()[0]（最可靠）
                try:
                    result = float(value.values.flatten()[0])
                    logger.debug(f"{log_prefix}通过.values.flatten()[0]提取标量值: {result}")
                    return result
                except Exception as e:
                    # 尝试使用.item()
                    try:
                        result = float(value.item())
                        logger.debug(f"{log_prefix}通过.item()提取标量值: {result}")
                        return result
                    except Exception as e2:
                        # 尝试使用.iloc[0]
                        try:
                            valid_values = value[~pd.isna(value)]
                            if not valid_values.empty:
                                result = float(valid_values.iloc[0])
                                logger.debug(f"{log_prefix}通过.iloc[0]提取标量值: {result}")
                                return result
                        except Exception as e3:
                            pass
            
            logger.error(f"{log_prefix}无法从pandas对象提取标量值(size={value.size})，使用默认值{default}")
            return default
        
        # 尝试直接转换为浮点数
        result = float(value)
        logger.debug(f"{log_prefix}直接转换为浮点数: {result}")
        return result
    
    except Exception as e:
        logger.error(f"{log_prefix}无法从类型 {type(value)} 中提取标量值: {str(e)}，使用默认值{default}")
        return default

def calculate_liquidity_score(df: pd.DataFrame) -> float:
    """
    计算流动性评分（0-100分，分数越高表示流动性越好）
    
    Args:
        df: ETF日线数据
    
    Returns:
        float: 流动性评分
    """
    try:
        # 创建DataFrame的深拷贝 - 这是关键修复点
        df = df.copy(deep=True)
        
        # 确保有足够数据
        if len(df) < 30:
            logger.warning(f"数据量不足({len(df)}天)，流动性评分可能不准确")
        
        # 确保使用中文列名
        df = ensure_chinese_columns(df)
        
        # 检查数据完整性
        required_columns = ["日期", "收盘", "成交额"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"流动性评分所需列缺失: {', '.join(missing_columns)}")
            return 50.0  # 返回默认值
        
        # 计算日均成交额（单位：万元）
        avg_volume = 0.0
        if AMOUNT_COL in df.columns:
            # 取最近30天数据
            recent_30d = df.tail(30)
            if len(recent_30d) > 0:
                # 确保成交额列是数值类型
                if not pd.api.types.is_numeric_dtype(recent_30d[AMOUNT_COL]):
                    # 使用.loc确保安全赋值
                    recent_30d = recent_30d.copy()
                    recent_30d.loc[:, AMOUNT_COL] = pd.to_numeric(recent_30d[AMOUNT_COL], errors='coerce')
                
                avg_volume = recent_30d[AMOUNT_COL].mean()
        
        # 流动性评分标准：
        # 1000万以下：30-50分
        # 1000-5000万：50-75分
        # 5000-10000万：75-90分
        # 10000万以上：90-100分
        if avg_volume <= 1000:
            score = 30 + (avg_volume / 1000) * 20
        elif avg_volume <= 5000:
            score = 50 + ((avg_volume - 1000) / 4000) * 25
        elif avg_volume <= 10000:
            score = 75 + ((avg_volume - 5000) / 5000) * 15
        else:
            score = 90 + min((avg_volume - 10000) / 10000, 10)
        
        logger.debug(f"ETF流动性评分: {score:.2f} (日均成交额: {avg_volume:.2f}万元)")
        return score
    
    except Exception as e:
        logger.error(f"计算流动性得分失败: {str(e)}", exc_info=True)
        return 50.0

def calculate_risk_score(df: pd.DataFrame) -> float:
    """
    计算风险评分（0-100分，分数越高风险越大）
    
    Args:
        df: ETF日线数据
    
    Returns:
        float: 风险评分（0-100分）
    """
    try:
        # 创建DataFrame的深拷贝
        df = df.copy(deep=True)
        
        # 确保使用中文列名
        df = ensure_chinese_columns(df)
        
        # 确保有足够数据
        if len(df) < 30:
            logger.warning(f"数据量不足({len(df)}天)，风险评分可能不准确")
        
        # 检查数据完整性
        required_columns = ["日期", "收盘"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"风险评分所需列缺失: {', '.join(missing_columns)}")
            return 50.0  # 返回默认值
        
        # 计算波动率
        if CLOSE_COL in df.columns and len(df) > 1:
            # 创建副本以避免SettingWithCopyWarning
            df = df.copy(deep=True)
            
            # 确保收盘价列是数值类型
            if not pd.api.types.is_numeric_dtype(df[CLOSE_COL]):
                df.loc[:, CLOSE_COL] = pd.to_numeric(df[CLOSE_COL], errors='coerce')
            
            # 计算日收益率
            df.loc[:, "daily_return"] = df[CLOSE_COL].pct_change()
            
            # 处理NaN值
            df = df.dropna(subset=["daily_return"])
            
            if not df.empty:
                volatility = df["daily_return"].std() * np.sqrt(252)  # 年化波动率
            else:
                volatility = 0.2  # 默认波动率
            
            # 波动率评分（越低越好）：
            # 波动率≤0.1=100分，0.2=75分，0.3=50分，0.4=25分，≥0.5=0分
            if volatility <= 0.1:
                score = 100.0
            elif volatility <= 0.2:
                score = 100 - (volatility - 0.1) * 250
            elif volatility <= 0.3:
                score = 75 - (volatility - 0.2) * 250
            elif volatility <= 0.4:
                score = 50 - (volatility - 0.3) * 250
            else:
                score = 25 - (volatility - 0.4) * 250
            
            # 确保评分在0-100范围内
            score = max(0, min(100, score))
        else:
            logger.warning("数据中缺少收盘价或数据量不足，使用默认风险评分")
            score = 50.0
        
        logger.debug(f"ETF风险评分: {score:.2f} (波动率: {volatility:.4f})")
        return score
    
    except Exception as e:
        logger.error(f"计算风险得分失败: {str(e)}", exc_info=True)
        return 50.0

def calculate_return_score(df: pd.DataFrame) -> float:
    """
    计算收益能力评分（0-100分，分数越高表示收益能力越强）
    
    Args:
        df: ETF日线数据
    
    Returns:
        float: 收益能力评分
    """
    try:
        # 创建DataFrame的深拷贝
        df = df.copy(deep=True)
        
        # 确保使用中文列名
        df = ensure_chinese_columns(df)
        
        # 确保有足够数据
        if len(df) < 30:
            logger.warning(f"数据量不足({len(df)}天)，收益评分可能不准确")
        
        # 检查数据完整性
        required_columns = ["日期", "收盘"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"收益评分所需列缺失: {', '.join(missing_columns)}")
            return 50.0  # 返回默认值
        
        # 检查是否包含必要列
        if CLOSE_COL not in df.columns:
            logger.error(f"数据中缺少必要列: {CLOSE_COL}")
            return 50.0
        
        # 确保价格列是数值类型
        if not pd.api.types.is_numeric_dtype(df[CLOSE_COL]):
            try:
                # 使用.loc确保安全赋值
                df.loc[:, CLOSE_COL] = pd.to_numeric(df[CLOSE_COL], errors='coerce')
                df = df.dropna(subset=[CLOSE_COL])
            except:
                logger.error(f"价格列 {CLOSE_COL} 无法转换为数值类型")
                return 50.0
        
        # 计算年化收益率
        if len(df) > 1:
            # 计算总收益率
            total_return = (df[CLOSE_COL].iloc[-1] / df[CLOSE_COL].iloc[0]) - 1
            
            # 计算年化收益率
            years = len(df) / 252
            if years > 0:
                annualized_return = (1 + total_return) ** (1 / years) - 1
            else:
                annualized_return = 0
            
            # 收益率评分标准
            if annualized_return <= 0:
                score = 0.0
            elif annualized_return <= 0.02:
                score = annualized_return * 1500
            elif annualized_return <= 0.05:
                score = 30 + (annualized_return - 0.02) * 1000
            elif annualized_return <= 0.08:
                score = 60 + (annualized_return - 0.05) * 833.3
            else:
                score = 85 + min((annualized_return - 0.08) * 1000, 15)
            
            # 确保评分在0-100范围内
            score = max(0, min(100, score))
        else:
            logger.warning("数据中缺少收盘价或数据量不足，使用默认收益评分")
            score = 50.0
        
        logger.debug(f"ETF收益评分: {score:.2f} (年化收益率: {annualized_return:.2%})")
        return score
    
    except Exception as e:
        logger.error(f"计算收益得分失败: {str(e)}", exc_info=True)
        return 50.0

def calculate_sentiment_score(df: pd.DataFrame) -> float:
    """
    计算市场情绪评分（0-100分，分数越高表示市场情绪越积极）
    
    Args:
        df: ETF日线数据
    
    Returns:
        float: 情绪评分
    """
    try:
        # 创建DataFrame的深拷贝
        df = df.copy(deep=True)
        
        # 确保使用中文列名
        df = ensure_chinese_columns(df)
        
        # 确保有足够数据
        if len(df) < 30:
            logger.warning(f"数据量不足({len(df)}天)，情绪评分可能不准确")
        
        # 检查数据完整性
        required_columns = ["日期", "成交额"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"情绪评分所需列缺失: {', '.join(missing_columns)}")
            return 50.0  # 返回默认值
        
        # 计算最近5天的成交额变化率
        volume_change = 0.0
        if AMOUNT_COL in df.columns and len(df) >= 5:
            # 确保成交额列是数值类型
            if not pd.api.types.is_numeric_dtype(df[AMOUNT_COL]):
                df.loc[:, AMOUNT_COL] = pd.to_numeric(df[AMOUNT_COL], errors='coerce')
            
            volume_5d = df[AMOUNT_COL].tail(5)
            volume_change = (volume_5d.iloc[-1] / volume_5d.iloc[0]) - 1
        
        # 计算最近5天的价格变化
        recent_price_change = 0.0
        if CLOSE_COL in df.columns and len(df) >= 5:
            # 确保收盘价列是数值类型
            if not pd.api.types.is_numeric_dtype(df[CLOSE_COL]):
                df.loc[:, CLOSE_COL] = pd.to_numeric(df[CLOSE_COL], errors='coerce')
            
            recent_price_change = (df[CLOSE_COL].iloc[-1] / df[CLOSE_COL].iloc[-5]) - 1
        
        # 综合情绪指标
        sentiment_score = 50 + (volume_change * 25) + (recent_price_change * 25)
        
        # 确保评分在0-100范围内
        sentiment_score = max(0, min(100, sentiment_score))
        
        logger.debug(f"ETF情绪评分: {sentiment_score:.2f} (成交额变化率: {volume_change:.2f}, 价格变化: {recent_price_change:.2f})")
        return sentiment_score
    
    except Exception as e:
        logger.error(f"计算情绪得分失败: {str(e)}", exc_info=True)
        return 50.0

def calculate_fundamental_score(etf_code: str) -> float:
    """
    计算基本面得分（规模）
    
    Args:
        etf_code: ETF代码
    
    Returns:
        float: 基本面得分
    """
    try:
        size = get_etf_basic_info(etf_code)
        
        # 规模得分（10亿=60分，100亿=100分）
        size_score = min(max(size * 0.4 + 50, 0), 100)
        
        # 移除上市时间得分，将权重全部给规模
        fundamental_score = size_score  # 100%权重给规模
        
        logger.debug(f"ETF {etf_code} 基本面评分: {fundamental_score:.2f} (规模: {size}亿元)")
        return fundamental_score
    
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 基本面评分失败: {str(e)}", exc_info=True)
        return 70.0  # 默认中等偏高评分

def calculate_component_stability_score(etf_code: str, df: pd.DataFrame) -> float:
    """
    计算成分股稳定性评分
    
    Args:
        etf_code: ETF代码
        df: ETF日线数据
    
    Returns:
        float: 成分股稳定性评分 (0-100)
    """
    try:
        if df is None or df.empty:
            logger.warning(f"ETF {etf_code} 无日线数据，无法计算成分股稳定性评分")
            return 70.0  # 默认中等偏高评分
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 计算波动率
        volatility = calculate_volatility(df)
        
        # 波动率评分（越低越好）：波动率≤0.1=100分，0.3=50分，≥0.5=0分
        component_score = max(0, 100 - (volatility * 200))
        
        # 考虑ETF规模（规模越大，成分股稳定性通常越高）
        size = get_etf_basic_info(etf_code)
        size_score = min(max(size * 0.5, 0), 100)
        
        # 综合评分（波动率占70%，规模占30%）
        total_score = component_score * 0.7 + size_score * 0.3
        
        logger.debug(f"ETF {etf_code} 成分股稳定性评分: {total_score:.2f} (波动率: {volatility:.4f}, 规模: {size}亿元)")
        return total_score
    
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 成分股稳定性评分失败: {str(e)}", exc_info=True)
        return 70.0  # 默认中等偏高评分

def calculate_volatility(df: pd.DataFrame) -> float:
    """
    计算ETF价格波动率
    
    Args:
        df: ETF日线数据
    
    Returns:
        float: 波动率
    """
    try:
        # 创建DataFrame的深拷贝
        df = df.copy(deep=True)
        
        # 确保使用中文列名
        df = ensure_chinese_columns(df)
        
        # 检查数据完整性
        required_columns = ["日期", "收盘"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"波动率计算所需列缺失: {', '.join(missing_columns)}")
            return 0.2  # 返回默认波动率
        
        # 检查是否包含必要列
        if CLOSE_COL not in df.columns:
            logger.error(f"数据中缺少必要列: {CLOSE_COL}")
            return 0.2
        
        # 检查收盘价列是否为数值类型
        if not pd.api.types.is_numeric_dtype(df[CLOSE_COL]):
            try:
                # 使用.loc确保安全赋值
                df.loc[:, CLOSE_COL] = pd.to_numeric(df[CLOSE_COL], errors='coerce')
            except Exception as e:
                logger.error(f"收盘价列转换失败: {str(e)}")
                return 0.2  # 默认波动率
        
        # 创建副本以避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 计算日收益率
        df.loc[:, "daily_return"] = df[CLOSE_COL].pct_change()
        
        # 处理NaN值
        if "daily_return" in df.columns:
            df = df.dropna(subset=["daily_return"])
        
        # 计算年化波动率
        if not df.empty:
            volatility = df["daily_return"].std() * np.sqrt(252)
        else:
            volatility = 0.2  # 默认波动率
        
        # 处理异常值
        volatility = min(max(volatility, 0), 1)
        
        return volatility
    
    except Exception as e:
        logger.error(f"计算波动率失败: {str(e)}", exc_info=True)
        return 0.2  # 默认中等波动率

def get_etf_basic_info(etf_code: str) -> float:
    """
    从ETF列表中获取ETF基本信息
    
    Args:
        etf_code: ETF代码 (6位数字)
    
    Returns:
        float: 基金规模(单位:亿元)
    """
    try:
        # 确保ETF代码格式一致（6位数字）
        etf_code = str(etf_code).strip().zfill(6)
        
        # 检查ETF列表是否有效
        etf_codes = get_all_etf_codes()
        
        # 创建DataFrame
        etf_list = pd.DataFrame(etf_codes, columns=['ETF代码'])
        
        if etf_list is None or etf_list.empty:
            logger.warning("ETF列表为空或无效，使用默认值")
            return 0.0
        
        # 确保ETF列表包含必要的列
        required_columns = [ETF_CODE_COL, FUND_SIZE_COL]
        for col in required_columns:
            if col not in etf_list.columns:
                logger.warning(f"ETF列表缺少必要列: {col}")
                return 0.0
        
        # 创建副本避免SettingWithCopyWarning
        etf_list = etf_list.copy(deep=True)
        
        # 修复：更安全的类型转换方法
        if ETF_CODE_COL in etf_list.columns:
            # 检查列是否包含非字符串值
            has_non_string = etf_list[ETF_CODE_COL].apply(lambda x: not isinstance(x, str)).any()
            
            # 如果列包含非字符串值，或者列是数值类型，则进行转换
            if has_non_string or pd.api.types.is_numeric_dtype(etf_list[ETF_CODE_COL]):
                etf_list.loc[:, ETF_CODE_COL] = etf_list[ETF_CODE_COL].astype(str)
        
        # 确保ETF列表中的ETF代码也是6位数字
        etf_list.loc[:, ETF_CODE_COL] = etf_list[ETF_CODE_COL].str.strip().str.zfill(6)
        
        etf_row = etf_list[etf_list[ETF_CODE_COL] == etf_code]
        if not etf_row.empty:
            # 处理规模
            size = 0.0
            if FUND_SIZE_COL in etf_row.columns:
                size = extract_scalar_value(
                    etf_row.iloc[0][FUND_SIZE_COL],
                    log_prefix=f"ETF {etf_code} 规模: "
                )
            return size
        
        logger.warning(f"ETF {etf_code} 未在ETF列表中找到，使用默认值")
        return 0.0
    
    except Exception as e:
        error_msg = f"获取ETF {etf_code} 基本信息失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return 0.0

def calculate_etf_score(etf_code: str, df: pd.DataFrame) -> float:
    """
    计算ETF综合评分（0-100分）
    
    Args:
        etf_code: ETF代码
        df: ETF日线数据
    
    Returns:
        float: ETF综合评分
    """
    try:
        # 获取当前双时区时间
        _, beijing_now = get_current_times()
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        if df is None or df.empty:
            logger.warning(f"ETF {etf_code} 无日线数据，评分设为0")
            return 0.0
        
        # 创建安全副本
        df = df.copy(deep=True)
        
        # 【日期datetime类型规则】确保日期列是datetime类型
        if DATE_COL in df.columns:
            try:
                # 仅在日期列存在时转换为datetime类型
                df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors='coerce')
                # 按日期排序
                df = df.sort_values(DATE_COL)
            except Exception as e:
                logger.error(f"日期列转换失败: {str(e)}")
                df = df.sort_values(DATE_COL)
        
        # 确保使用中文列名
        df = ensure_chinese_columns(df)
        
        # 检查数据量
        min_required_data = 30  # 默认需要30天数据
        if len(df) < min_required_data:
            if len(df) < 10:
                logger.warning(f"ETF {etf_code} 数据量严重不足({len(df)}天)，评分设为0")
                return 0.0
            else:
                logger.info(f"ETF {etf_code} 数据量不足({len(df)}天)，使用现有数据计算评分")
                min_required_data = len(df)
        
        # 取最近min_required_data天数据
        recent_data = df.tail(min_required_data)
        
        # 1. 流动性得分（日均成交额）
        liquidity_score = calculate_liquidity_score(recent_data)
        
        # 2. 风险控制得分
        risk_score = calculate_risk_score(recent_data)
        
        # 3. 收益能力得分
        return_score = calculate_return_score(recent_data)
        
        # 4. 情绪指标得分（成交量变化率）
        sentiment_score = calculate_sentiment_score(recent_data)
        
        # 5. 基本面得分（规模）
        fundamental_score = calculate_fundamental_score(etf_code)
        
        # 验证所有得分是否在有效范围内 [0, 100]
        scores = {
            "liquidity": max(0, min(100, liquidity_score)),
            "risk": max(0, min(100, risk_score)),
            "return": max(0, min(100, return_score)),
            "sentiment": max(0, min(100, sentiment_score)),
            "fundamental": max(0, min(100, fundamental_score))
        }
        
        # 获取权重
        weights = Config.SCORE_WEIGHTS.copy()
        
        # 确保权重字典包含所有必要的键
        required_keys = ['liquidity', 'risk', 'return', 'sentiment', 'fundamental']
        for key in required_keys:
            if key not in weights:
                logger.warning(f"权重字典缺少必要键: {key}, 使用默认值0.2")
                weights[key] = 0.2
        
        # 确保权重和为1
        total_weight = sum(weights.values())
        # 修复：使用容差比较浮点数
        if abs(total_weight - 1.0) > 1e-10:
            logger.warning(f"权重和不为1 ({total_weight:.2f})，自动调整")
            for key in weights:
                weights[key] = weights[key] / total_weight
        
        # 计算综合评分（加权平均）
        total_score = (
            scores["liquidity"] * weights['liquidity'] +
            scores["risk"] * weights['risk'] +
            scores["return"] * weights['return'] +
            scores["sentiment"] * weights['sentiment'] +
            scores["fundamental"] * weights['fundamental']
        )
        
        # 双重验证：确保评分在0-100范围内
        total_score = max(0, min(100, total_score))
        
        logger.debug(f"ETF {etf_code} 评分详情: " +
                     f"流动性={scores['liquidity']:.2f}({weights['liquidity']*100:.0f}%), " +
                     f"风险={scores['risk']:.2f}({weights['risk']*100:.0f}%), " +
                     f"收益={scores['return']:.2f}({weights['return']*100:.0f}%), " +
                     f"情绪={scores['sentiment']:.2f}({weights['sentiment']*100:.0f}%), " +
                     f"基本面={scores['fundamental']:.2f}({weights['fundamental']*100:.0f}%), " +
                     f"综合={total_score:.2f}")
        
        return round(total_score, 2)
    
    except Exception as e:
        error_msg = f"计算ETF {etf_code} 综合评分失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return 0.0

def get_etf_score_history(etf_code: str, days: int = 30) -> pd.DataFrame:
    """
    获取ETF历史评分数据
    
    Args:
        etf_code: ETF代码
        days: 查询天数
    
    Returns:
        pd.DataFrame: 评分历史数据
    """
    try:
        history = []
        beijing_now = get_beijing_time()
        
        for i in range(days):
            # 【日期datetime类型规则】确保日期是datetime类型
            date = (beijing_now - timedelta(days=i)).strftime("%Y-%m-%d")
            score_file = os.path.join(Config.SCORE_HISTORY_DIR, f"{etf_code}_{date}.json")
            
            if os.path.exists(score_file):
                try:
                    with open(score_file, 'r') as f:
                        score_data = json.load(f)
                    history.append({
                        "日期": date,
                        "评分": score_data.get("score", 0.0),
                        "排名": score_data.get("rank", 0)
                    })
                except Exception as e:
                    logger.error(f"读取评分历史文件 {score_file} 失败: {str(e)}")
        
        if not history:
            logger.info(f"未找到ETF {etf_code} 的评分历史数据")
            return pd.DataFrame()
        
        return pd.DataFrame(history)
    
    except Exception as e:
        error_msg = f"获取ETF {etf_code} 评分历史数据失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return pd.DataFrame()

def analyze_etf_score_trend(etf_code: str) -> str:
    """
    分析ETF评分趋势
    
    Args:
        etf_code: ETF代码
    
    Returns:
        str: 分析结果
    """
    try:
        # 获取评分历史
        history_df = get_etf_score_history(etf_code)
        if history_df.empty:
            return f"【{etf_code} 评分趋势】• 无历史评分数据"
        
        # 计算趋势
        latest_score = history_df.iloc[0]["评分"]
        avg_score = history_df["评分"].mean()
        trend = "上升" if latest_score > avg_score else "下降"
        
        # 生成分析报告
        report = f"【{etf_code} 评分趋势】"
        report += f"• 当前评分: {latest_score:.2f}"
        report += f"• 近期平均评分: {avg_score:.2f}"
        report += f"• 评分趋势: {trend}"
        
        # 添加建议
        if trend == "上升":
            report += "💡 建议：评分持续上升，可关注该ETF"
        else:
            report += "💡 建议：评分持续下降，需谨慎考虑"
        
        return report
    
    except Exception as e:
        error_msg = f"分析ETF {etf_code} 评分趋势失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return f"【{etf_code} 评分趋势】• 分析失败，请检查日志"

def calculate_arbitrage_score(etf_code: str,
                            etf_name: str,
                            premium_discount: float,  # 这已经是折溢价率，不需要再计算
                            market_price: float,
                            iopv: float,
                            fund_size: float,
                            avg_volume: float,
                            historical_data: Optional[pd.DataFrame] = None) -> float:
    """
    计算ETF套利综合评分
    
    Args:
        etf_code: ETF代码
        etf_name: ETF名称
        premium_discount: 折溢价率（标量值，不是Series）
        market_price: 市场价格
        iopv: IOPV净值
        fund_size: 基金规模
        avg_volume: 日均成交额
        historical_data: 历史数据（可选）
    
    Returns:
        float: 综合评分 (0-100)
    """
    try:
        # 限制在合理范围内
        MAX_DISCOUNT = -20.0  # 最大折价率（-20%）
        MAX_PREMIUM = 20.0    # 最大溢价率（20%）
        premium_discount = max(min(premium_discount, MAX_PREMIUM), MAX_DISCOUNT)
        
        # 记录实际使用的值
        logger.debug(f"ETF {etf_code} 实际使用的折溢价率: {premium_discount:.2f}%")
        
        # 计算基础ETF评分
        base_score = 70.0  # 默认值，实际应从历史数据计算
        if historical_data is not None and not historical_data.empty:
            base_score = calculate_etf_score(etf_code, historical_data)
        
        # 确保基础评分在0-100范围内
        if base_score < 0 or base_score > 100:
            logger.warning(f"ETF {etf_code} 基础评分超出范围({base_score:.2f})，强制限制在0-100")
            base_score = max(0, min(100, base_score))
        
        # 计算成分股稳定性评分
        component_score = calculate_component_stability_score(etf_code, historical_data)
        
        # 确保成分股稳定性评分在0-100范围内
        if component_score < 0 or component_score > 100:
            logger.warning(f"ETF {etf_code} 成分股稳定性评分超出范围({component_score:.2f})，强制限制在0-100")
            component_score = max(0, min(100, component_score))
        
        # 修复：折价情况 - 折价率越高（绝对值越大），评分应该越高
        if premium_discount < 0:
            abs_premium = abs(premium_discount)
            # 折价率越高（绝对值越大），评分应该越高
            if abs_premium >= Config.DISCOUNT_THRESHOLD * 3:
                premium_score = 100.0
            elif abs_premium >= Config.DISCOUNT_THRESHOLD * 2:
                premium_score = 90.0
            elif abs_premium >= Config.DISCOUNT_THRESHOLD * 1.5:
                premium_score = 80.0
            elif abs_premium >= Config.DISCOUNT_THRESHOLD:
                premium_score = 70.0
            else:
                premium_score = 0.0
        # 修复：溢价情况 - 溢价率越高，评分应该越高
        else:
            # 溢价率越高，评分应该越高
            if premium_discount >= Config.PREMIUM_THRESHOLD * 3:
                premium_score = 100.0
            elif premium_discount >= Config.PREMIUM_THRESHOLD * 2:
                premium_score = 90.0
            elif premium_discount >= Config.PREMIUM_THRESHOLD * 1.5:
                premium_score = 80.0
            elif premium_discount >= Config.PREMIUM_THRESHOLD:
                premium_score = 70.0
            else:
                premium_score = 0.0
        
        # 确保折溢价率评分在0-100范围内
        if premium_score < 0 or premium_score > 100:
            logger.warning(f"ETF {etf_code} 折溢价率评分超出范围({premium_score:.2f})，强制限制在0-100")
            premium_score = max(0, min(100, premium_score))
        
        # 获取评分权重
        weights = Config.ARBITRAGE_SCORE_WEIGHTS.copy()
        
        # 确保权重字典包含所有必要的键
        required_keys = ['premium_discount', 'liquidity', 'risk', 'return', 'market_sentiment', 'fundamental', 'component_stability']
        for key in required_keys:
            if key not in weights:
                logger.warning(f"权重字典缺少必要键: {key}, 使用默认值0.1")
                weights[key] = 0.1
        
        # 确保权重和为1
        total_weight = sum(weights.values())
        # 修复：使用容差比较浮点数
        if abs(total_weight - 1.0) > 1e-10:
            logger.warning(f"权重和不为1 ({total_weight:.2f})，自动调整")
            for key in weights:
                weights[key] = weights[key] / total_weight
        
        # 综合评分（加权平均）
        total_score = (
            base_score * (weights['liquidity'] + weights['risk'] + weights['return'] + weights['market_sentiment'] + weights['fundamental']) +
            component_score * weights['component_stability'] +
            premium_score * weights['premium_discount']
        )
        
        # 双重验证：确保评分在0-100范围内
        if total_score < 0 or total_score > 100:
            logger.error(f"ETF {etf_code} 套利综合评分超出范围({total_score:.2f})，强制限制在0-100")
            total_score = max(0, min(100, total_score))
        
        # 添加详细日志，便于问题排查
        logger.debug(f"ETF {etf_code} 套利综合评分详情: " +
                     f"基础评分={base_score:.2f}(权重{weights['liquidity'] + weights['risk'] + weights['return'] + weights['market_sentiment'] + weights['fundamental']:.2f}), " +
                     f"成分股稳定性={component_score:.2f}(权重{weights['component_stability']:.2f}), " +
                     f"折溢价率={premium_score:.2f}(权重{weights['premium_discount']:.2f}), " +
                     f"最终评分={total_score:.2f}")
        
        return total_score
    
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 套利综合评分失败: {str(e)}", exc_info=True)
        return 0.0

def get_top_rated_etfs(top_n=None,
                     min_score=60,
                     min_fund_size=10.0,
                     min_avg_volume=5000.0) -> pd.DataFrame:
    """
    从全市场ETF中筛选高分ETF
    
    Args:
        top_n: 返回前N名，为None则返回所有高于min_score的ETF
        min_score: 最低评分阈值
        min_fund_size: 最小基金规模(亿元)
        min_avg_volume: 最小日均成交额(万元)
    
    Returns:
        pd.DataFrame: 包含ETF代码、名称、评分等信息的DataFrame
    """
    try:
        metadata_df = load_etf_metadata()
        if metadata_df is None or metadata_df.empty:
            logger.warning("元数据为空，无法获取ETF列表")
            return pd.DataFrame()
        
        # 确保ETF代码唯一
        all_codes = metadata_df["ETF代码"].drop_duplicates().tolist()
        if not all_codes:
            logger.warning("元数据中无ETF代码")
            return pd.DataFrame()
        
        score_list = []
        logger.info(f"开始计算 {len(all_codes)} 只ETF的综合评分...")
        
        # 用于进度跟踪
        total = len(all_codes)
        processed = 0
        last_log_time = time.time()
        
        for idx, etf_code in enumerate(all_codes):
            try:
                df = load_etf_daily_data(etf_code)
                if df.empty:
                    logger.debug(f"ETF {etf_code} 无日线数据，跳过评分")
                    continue
                
                # 【日期datetime类型规则】确保日期列是datetime类型
                if not df.empty and DATE_COL in df.columns:
                    try:
                        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors='coerce')
                        df = df.sort_values(DATE_COL)
                    except Exception as e:
                        logger.error(f"日期列转换失败: {str(e)}")
                        df = df.sort_values(DATE_COL)
                
                # 确保ETF代码格式一致（6位数字）
                etf_code = str(etf_code).strip().zfill(6)
                
                # 计算ETF评分
                score = calculate_etf_score(etf_code, df)
                if score < min_score:
                    continue
                
                # 获取ETF基本信息（从本地元数据获取）
                size = 0.0
                size_row = metadata_df[metadata_df["ETF代码"] == etf_code]
                if not size_row.empty and "基金规模" in metadata_df.columns:
                    size = extract_scalar_value(
                        size_row.iloc[0]["基金规模"],
                        log_prefix=f"ETF {etf_code} 规模: "
                    )
                
                etf_name = get_etf_name(etf_code)
                
                # 计算日均成交额
                avg_volume = 0.0
                if AMOUNT_COL in df.columns:
                    recent_30d = df.tail(30)
                    if len(recent_30d) > 0:
                        avg_volume = recent_30d[AMOUNT_COL].mean()
                
                # 添加进度日志 - 简化输出
                processed += 1
                current_time = time.time()
                
                # 每处理100只ETF或每5秒记录一次进度
                if processed % 100 == 0 or (current_time - last_log_time) >= 5:
                    progress = (processed / total) * 100
                    logger.info(f"正在计算ETF评分: {processed}/{total} ({progress:.1f}%)")
                    last_log_time = current_time
                
                # 仅保留满足条件的ETF
                if size >= min_fund_size and avg_volume >= min_avg_volume:
                    score_list.append({
                        "ETF代码": etf_code,
                        "ETF名称": etf_name,
                        "评分": score,
                        "规模": size,
                        "日均成交额": avg_volume
                    })
            except Exception as e:
                logger.error(f"处理ETF {etf_code} 时发生错误: {str(e)}", exc_info=True)
                continue
        
        # 检查是否有符合条件的ETF
        if not score_list:
            warning_msg = (f"没有ETF达到最低评分阈值 {min_score}，" +
                          f"或未满足规模({min_fund_size}亿元)和日均成交额({min_avg_volume}万元)要求")
            logger.info(warning_msg)
            return pd.DataFrame()
        
        # 创建评分DataFrame
        score_df = pd.DataFrame(score_list).sort_values("评分", ascending=False)
        total_etfs = len(score_df)
        
        # 记录最终结果
        logger.info(f"评分完成。共{total_etfs}只ETF评分≥{min_score}")
        logger.info(f"应用筛选参数: 规模≥{min_fund_size}亿元, 日均成交额≥{min_avg_volume}万元")
        
        # 返回结果
        if top_n is not None and top_n > 0:
            return score_df.head(top_n)
        return score_df.head(max(10, int(total_etfs * Config.SCORE_TOP_PERCENT / 100)))
    
    except Exception as e:
        error_msg = f"获取高评分ETF列表失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return pd.DataFrame()

def calculate_position_strategy() -> str:
    """
    计算仓位策略
    
    Returns:
        str: 仓位策略建议
    """
    try:
        # 获取高评分ETF列表
        top_etfs = get_top_rated_etfs(top_n=10)
        
        if top_etfs.empty:
            return "未找到符合条件的高评分ETF，建议保持观望"
        
        # 生成仓位策略建议
        report = "【仓位策略建议】\n"
        report += f"推荐关注的ETF (前{len(top_etfs)}名):\n"
        
        for idx, row in top_etfs.iterrows():
            report += f"{idx+1}. {row['ETF代码']} {row['ETF名称']} (评分: {row['评分']:.2f}, 规模: {row['规模']:.2f}亿元)\n"
        
        # 根据ETF数量和评分确定仓位
        if len(top_etfs) >= 5:
            report += "\n建议仓位: 70%-90%\n"
            report += "理由: 市场机会较多，可适当提高仓位"
        elif len(top_etfs) >= 3:
            report += "\n建议仓位: 50%-70%\n"
            report += "理由: 市场存在机会，但需保持谨慎"
        else:
            report += "\n建议仓位: 30%-50%\n"
            report += "理由: 市场机会有限，建议降低仓位"
        
        return report
    
    except Exception as e:
        error_msg = f"计算仓位策略失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return "仓位策略计算失败，请检查日志"

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 检查ETF列表是否过期
    if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
        warning_msg = "ETF列表已过期，评分系统可能使用旧数据"
        logger.warning(warning_msg)
        # 发送警告通知
        send_wechat_message(
            message=warning_msg,
            message_type="warning"
        )
    
    logger.info("ETF评分系统模块初始化完成")
except Exception as e:
    error_msg = f"ETF评分系统模块初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
    
    try:
        # 退回到基础日志配置
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(error_msg)
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(error_msg)
    
    # 发送错误通知
    try:
        from wechat_push.push import send_wechat_message
        send_wechat_message(
            message=f"ETF评分系统模块初始化失败: {str(e)}",
            message_type="error"
        )
    except Exception as send_error:
        logger.error(f"发送错误通知失败: {str(send_error)}", exc_info=True)
