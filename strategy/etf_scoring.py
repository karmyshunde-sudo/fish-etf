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
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    is_file_outdated
)
from utils.file_utils import load_etf_daily_data, load_etf_metadata
from data_crawler.etf_list_manager import load_all_etf_list, get_etf_name
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

# 常量定义
DATE_COL = "日期" if "日期" in Config.STANDARD_COLUMNS else "date"
CLOSE_COL = "收盘" if "收盘" in Config.STANDARD_COLUMNS else "close"
AMOUNT_COL = "成交额" if "成交额" in Config.STANDARD_COLUMNS else "amount"
ETF_CODE_COL = "ETF代码"
FUND_SIZE_COL = "基金规模"
LISTING_DATE_COL = "上市日期"  # 统一使用"上市日期"

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

def calculate_arbitrage_score(
    etf_code: str,
    etf_name: str,
    premium_discount: float,
    market_price: float,
    iopv: float,
    fund_size: float,
    avg_volume: float,
    historical_data: Optional[pd.DataFrame] = None
) -> float:
    """
    针对小资金（2万）优化的ETF捡漏评分系统（0-100分）
    
    评分机制：
    1. 基础筛选（不满足则直接0分）：
       - ETF规模 ≥ 5亿元（避免迷你ETF风险）
       - 日均成交额 ≥ 100万元（小资金足够流动性）
    
    2. 有效折价率计算：
       - 有效折价率 = max(折价率 - 0.35%, 0)
       - 考虑小资金实际交易成本更高（佣金+印花税+滑点）
    
    3. 评分公式：
       - 得分 = min(100, 有效折价率 × 400)
       - 0.25%有效折价 = 100分（理想捡漏机会）
       - 0.1%有效折价 = 40分（一般机会）
    
    4. 附加条件（可选，根据风险偏好）：
       - 连续2天折价：额外+10分
       - 折价幅度大于行业平均：额外+5分
    """
    try:
        # 1. 基础筛选
        if fund_size < 5.0:  # 5亿元规模下限
            logger.debug(f"ETF {etf_code} 规模 {fund_size:.2f}亿元 < 5亿元，不满足规模要求")
            return 0.0
            
        if avg_volume < 100:  # 100万元日均成交额（适合小资金）
            logger.debug(f"ETF {etf_code} 日均成交额 {avg_volume:.2f}万元 < 100万元，不满足流动性要求")
            return 0.0
            
        # 2. 有效折价率计算（小资金交易成本更高）
        TRANSACTION_COST = 0.35  # 小资金实际交易成本约0.35%
        effective_discount = 0.0
        
        # 仅计算折价情况（溢价对捡漏无价值）
        if premium_discount < 0:
            effective_discount = max(-premium_discount - TRANSACTION_COST, 0)
        
        # 3. 评分公式
        score = min(100, effective_discount * 400)  # 更敏感的评分尺度
        
        # 4. 附加条件（如果提供了历史数据）
        if historical_data is not None and not historical_data.empty:
            # 检查是否连续2天折价
            if len(historical_data) >= 2:
                prev_premium = historical_data["折溢价率"].iloc[-2]
                if prev_premium < 0 and premium_discount < 0:
                    score = min(100, score + 10)  # 连续折价加分
            
            # 检查折价幅度是否大于行业平均（简化实现）
            industry_avg = -0.15  # 假设行业平均折价率为-0.15%
            if premium_discount < industry_avg:
                score = min(100, score + 5)
        
        # 记录评分详情
        logger.debug(f"ETF {etf_code} 捡漏评分详情: "
                     f"折溢价率={premium_discount:.2f}%, "
                     f"有效折价率={effective_discount:.2f}%, "
                     f"规模={fund_size:.2f}亿元, "
                     f"日均成交额={avg_volume:.2f}万元, "
                     f"最终评分={score:.2f}")
        
        return score
    
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 捡漏评分失败: {str(e)}", exc_info=True)
        return 0.0

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
        size, _ = get_etf_basic_info(etf_code)
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
        # 确保使用中文列名
        try:
            from utils.file_utils import ensure_chinese_columns
            df = ensure_chinese_columns(df)
        except ImportError:
            # 如果导入失败，尝试使用内置的列名映射
            logger.warning("无法导入ensure_chinese_columns，尝试使用内置列名映射")
            # 这里可以添加内置的列名映射逻辑
        
        # 检查是否包含必要列
        if "收盘" not in df.columns and "close" not in df.columns:
            logger.error("ETF日线数据缺少价格列，无法计算波动率")
            return 0.5  # 返回默认波动率
        
        # 选择合适的价格列
        price_col = "收盘" if "收盘" in df.columns else "close"
        logger.info(f"找到价格列: {price_col} (可用价格列: {list(df.columns)})")
        
        # 确保价格列是数值类型
        if not pd.api.types.is_numeric_dtype(df[price_col]):
            try:
                df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
                df = df.dropna(subset=[price_col])
            except:
                logger.error(f"价格列 {price_col} 无法转换为数值类型")
                return 0.5
        
        # 计算收益率
        df["daily_return"] = df[price_col].pct_change().dropna()
        
        # 确保收益率列是数值类型
        if not pd.api.types.is_numeric_dtype(df["daily_return"]):
            try:
                df["daily_return"] = pd.to_numeric(df["daily_return"], errors='coerce')
                df = df.dropna(subset=["daily_return"])
            except:
                logger.error("收益率列无法转换为数值类型")
                return 0.5
        
        # 计算波动率（年化波动率）
        if len(df["daily_return"]) < 2:
            logger.warning("收益率数据不足，无法计算波动率")
            return 0.5
        
        volatility = df["daily_return"].std() * np.sqrt(252)  # 年化波动率
        return volatility
    
    except Exception as e:
        logger.error(f"计算ETF波动率失败: {str(e)}", exc_info=True)
        return 0.5  # 默认波动率

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
        
        # 确保数据按日期排序
        if DATE_COL in df.columns:
            df = df.sort_values(DATE_COL)
        
        # 检查ETF是否为新上市
        size, listing_date = get_etf_basic_info(etf_code)
        is_new_etf = False
        days_since_listing = 0
        
        if listing_date:
            try:
                # 处理不同格式的日期字符串
                if isinstance(listing_date, str):
                    # 尝试多种日期格式
                    date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]
                    listing_date_obj = None
                    for fmt in date_formats:
                        try:
                            listing_date_obj = datetime.strptime(listing_date, fmt)
                            break
                        except:
                            continue
                    if listing_date_obj:
                        days_since_listing = (beijing_now - listing_date_obj).days
                        is_new_etf = days_since_listing < 90  # 上市90天内视为新ETF
                elif isinstance(listing_date, datetime):
                    days_since_listing = (beijing_now - listing_date).days
                    is_new_etf = days_since_listing < 90
            except Exception as e:
                logger.error(f"ETF {etf_code} 上市日期解析错误: {str(e)}")
        
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
        
        # 5. 基本面得分（规模、上市时间等）
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
        if abs(total_weight - 1.0) > 0.001:
            logger.warning(f"权重和不为1 ({total_weight}), 正在归一化")
            for key in weights:
                weights[key] /= total_weight
        
        # 计算综合评分
        total_score = (
            scores["liquidity"] * weights['liquidity'] +
            scores["risk"] * weights['risk'] +
            scores["return"] * weights['return'] +
            scores["sentiment"] * weights['sentiment'] +
            scores["fundamental"] * weights['fundamental']
        )
        
        # 双重验证：确保最终评分在0-100范围内
        total_score = max(0, min(100, total_score))
        
        # 对新上市ETF应用惩罚因子
        if is_new_etf and days_since_listing < 15:
            penalty_factor = 0.8 - (days_since_listing * 0.02)
            total_score = max(0, total_score * penalty_factor)
            logger.info(f"ETF {etf_code} 为新上市ETF，应用惩罚因子，最终评分: {total_score:.2f}")
        
        logger.debug(f"ETF {etf_code} 评分详情: "
                     f"流动性={scores['liquidity']:.2f}({weights['liquidity']*100:.0f}%), "
                     f"风险={scores['risk']:.2f}({weights['risk']*100:.0f}%), "
                     f"收益={scores['return']:.2f}({weights['return']*100:.0f}%), "
                     f"情绪={scores['sentiment']:.2f}({weights['sentiment']*100:.0f}%), "
                     f"基本面={scores['fundamental']:.2f}({weights['fundamental']*100:.0f}%), "
                     f"综合={total_score:.2f}")
        
        return round(total_score, 2)
    
    except Exception as e:
        error_msg = f"计算ETF {etf_code} 评分失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return 0.0

def calculate_liquidity_score(df: pd.DataFrame) -> float:
    """
    计算流动性得分（日均成交额）
    
    Args:
        df: ETF日线数据
    
    Returns:
        float: 流动性得分
    """
    try:
        if df is None or df.empty:
            logger.warning("传入的DataFrame为空，流动性得分设为0")
            return 0.0
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 检查是否包含成交额列
        if AMOUNT_COL not in df.columns:
            logger.warning(f"ETF日线数据缺少{AMOUNT_COL}列，无法计算流动性得分")
            return 50.0
        
        # 确保成交额列是数值类型
        if not pd.api.types.is_numeric_dtype(df[AMOUNT_COL]):
            try:
                df[AMOUNT_COL] = pd.to_numeric(df[AMOUNT_COL], errors="coerce").fillna(0)
            except Exception as e:
                logger.error(f"成交额列转换失败: {str(e)}")
                return 50.0
        
        # 计算日均成交额（单位：万元）
        avg_volume = df[AMOUNT_COL].mean() / 10000
        
        # 流动性评分（对数尺度，更符合实际感受）
        # 1000万元=50分，5000万元=75分，10000万元=90分
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
        # 确保有足够数据
        if len(df) < 30:
            logger.warning("ETF日线数据不足30天，无法准确计算风险评分")
            return 50.0  # 返回中性评分
        
        # 确保使用中文列名
        try:
            from utils.file_utils import ensure_chinese_columns
            df = ensure_chinese_columns(df)
        except ImportError:
            # 如果导入失败，尝试使用内置的列名映射
            logger.warning("无法导入ensure_chinese_columns，尝试使用内置列名映射")
            # 这里可以添加内置的列名映射逻辑
            pass
        
        # 检查是否包含必要列
        if "收盘" not in df.columns and "close" not in df.columns:
            logger.error("ETF日线数据缺少价格列，无法计算风险评分")
            return 50.0
        
        # 选择合适的价格列
        price_col = "收盘" if "收盘" in df.columns else "close"
        logger.info(f"找到价格列: {price_col} (可用价格列: {list(df.columns)})")
        
        # 确保价格列是数值类型
        if not pd.api.types.is_numeric_dtype(df[price_col]):
            try:
                df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
                df = df.dropna(subset=[price_col])
            except:
                logger.error(f"价格列 {price_col} 无法转换为数值类型")
                return 50.0
        
        # 计算收益率
        df["daily_return"] = df[price_col].pct_change().dropna()
        
        # 确保收益率列是数值类型
        if not pd.api.types.is_numeric_dtype(df["daily_return"]):
            try:
                df["daily_return"] = pd.to_numeric(df["daily_return"], errors='coerce')
                df = df.dropna(subset=["daily_return"])
            except:
                logger.error("收益率列无法转换为数值类型")
                return 50.0
        
        # 计算波动率（年化波动率）
        if len(df["daily_return"]) < 2:
            logger.warning("收益率数据不足，无法计算波动率")
            return 50.0
        
        volatility = df["daily_return"].std() * np.sqrt(252)  # 年化波动率
        
        # 计算折溢价率稳定性
        premium_discount_std = 0.5  # 默认值
        if "折溢价率" in df.columns:
            # 确保折溢价率列是数值类型
            if not pd.api.types.is_numeric_dtype(df["折溢价率"]):
                try:
                    # 使用辅助函数安全提取标量值
                    df["折溢价率"] = df["折溢价率"].apply(
                        lambda x: extract_scalar_value(x, log_prefix="折溢价率: ")
                    )
                    df = df.dropna(subset=["折溢价率"])
                except:
                    logger.warning("折溢价率列无法转换为数值类型")
            
            if not df["折溢价率"].empty:
                premium_discount_std = df["折溢价率"].std()
        
        # 综合风险指标（标准化到0-1）
        risk_factor = (volatility * 0.6 + premium_discount_std * 0.4)
        
        # 将风险指标转换为0-100分的评分（分数越高风险越大）
        # 使用S型曲线，使极端值变化更平滑
        risk_score = 100 / (1 + np.exp(-5 * (risk_factor - 0.2)))
        
        # 确保评分在0-100范围内
        risk_score = max(0, min(100, risk_score))
        
        logger.debug(f"ETF风险评分计算: 波动率={volatility:.4f}, 折溢价标准差={premium_discount_std:.4f}, 风险评分={risk_score:.2f}")
        return risk_score
    
    except Exception as e:
        logger.error(f"计算风险评分失败: {str(e)}", exc_info=True)
        return 50.0  # 出错时返回中性评分

def calculate_return_score(df: pd.DataFrame) -> float:
    """
    计算收益评分（0-100分，分数越高表示潜在收益越大）
    
    Args:
        df: ETF日线数据
    
    Returns:
        float: 收益评分（0-100分）
    """
    try:
        # 确保有足够数据
        if len(df) < 30:
            logger.warning("ETF日线数据不足30天，无法准确计算收益评分")
            return 50.0  # 返回中性评分
        
        # 确保使用中文列名
        try:
            from utils.file_utils import ensure_chinese_columns
            df = ensure_chinese_columns(df)
        except ImportError:
            # 如果导入失败，尝试使用内置的列名映射
            logger.warning("无法导入ensure_chinese_columns，尝试使用内置列名映射")
            # 这里可以添加内置的列名映射逻辑
            pass
        
        # 检查是否包含必要列
        if "收盘" not in df.columns and "close" not in df.columns:
            logger.error("ETF日线数据缺少价格列，无法计算收益评分")
            return 50.0
        
        # 选择合适的价格列
        price_col = "收盘" if "收盘" in df.columns else "close"
        logger.info(f"找到价格列: {price_col} (可用价格列: {list(df.columns)})")
        
        # 确保价格列是数值类型
        if not pd.api.types.is_numeric_dtype(df[price_col]):
            try:
                df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
                df = df.dropna(subset=[price_col])
            except:
                logger.error(f"价格列 {price_col} 无法转换为数值类型")
                return 50.0
        
        # 计算收益率
        df["daily_return"] = df[price_col].pct_change().dropna()
        
        # 确保收益率列是数值类型
        if not pd.api.types.is_numeric_dtype(df["daily_return"]):
            try:
                df["daily_return"] = pd.to_numeric(df["daily_return"], errors='coerce')
                df = df.dropna(subset=["daily_return"])
            except:
                logger.error("收益率列无法转换为数值类型")
                return 50.0
        
        # 计算年化收益率
        if len(df) < 2:
            return 50.0
        
        total_return = (df[price_col].iloc[-1] / df[price_col].iloc[0]) - 1
        annualized_return = total_return * (252 / len(df))
        
        # 计算夏普比率（无风险利率设为0.02）
        risk_free_rate = 0.02
        excess_return = annualized_return - risk_free_rate
        volatility = df["daily_return"].std() * np.sqrt(252)
        
        if volatility > 0:
            sharpe_ratio = excess_return / volatility
        else:
            sharpe_ratio = excess_return
        
        # 将夏普比率转换为0-100分的评分
        # 夏普比率≤0=0分，0.5=50分，1.0=100分
        if sharpe_ratio <= 0:
            return_score = 0
        elif sharpe_ratio <= 0.5:
            return_score = sharpe_ratio * 100
        elif sharpe_ratio <= 1.0:
            return_score = 50 + (sharpe_ratio - 0.5) * 100
        else:
            return_score = 100 + min(sharpe_ratio - 1.0, 1.0) * 50
        
        # 确保评分在0-100范围内
        return_score = max(0, min(100, return_score))
        
        logger.debug(f"ETF收益评分计算: 年化收益率={annualized_return:.4f}, 波动率={volatility:.4f}, 夏普比率={sharpe_ratio:.4f}, 收益评分={return_score:.2f}")
        return return_score
    
    except Exception as e:
        logger.error(f"计算收益评分失败: {str(e)}", exc_info=True)
        return 50.0  # 出错时返回中性评分

def calculate_sentiment_score(df: pd.DataFrame) -> float:
    """
    计算情绪指标得分（0-100分，分数越高表示情绪越积极）
    
    Args:
        df: ETF日线数据
    
    Returns:
        float: 情绪指标得分
    """
    try:
        if df is None or df.empty:
            logger.warning("传入的DataFrame为空，情绪得分设为50")
            return 50.0
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 检查是否包含必要列
        if AMOUNT_COL not in df.columns or CLOSE_COL not in df.columns:
            logger.warning("ETF日线数据缺少必要列，无法计算情绪得分")
            return 50.0
        
        # 确保成交额和收盘价是数值类型
        if not pd.api.types.is_numeric_dtype(df[AMOUNT_COL]):
            try:
                df[AMOUNT_COL] = pd.to_numeric(df[AMOUNT_COL], errors="coerce").fillna(0)
            except:
                logger.error(f"成交额列 {AMOUNT_COL} 无法转换为数值类型")
                return 50.0
        
        if not pd.api.types.is_numeric_dtype(df[CLOSE_COL]):
            try:
                df[CLOSE_COL] = pd.to_numeric(df[CLOSE_COL], errors="coerce").fillna(0)
            except:
                logger.error(f"收盘价列 {CLOSE_COL} 无法转换为数值类型")
                return 50.0
        
        # 计算最近5天的平均成交额
        recent_avg_volume = df[AMOUNT_COL].tail(5).mean()
        
        # 计算前5-10天的平均成交额
        prev_avg_volume = df[AMOUNT_COL].tail(10).head(5).mean() if len(df) >= 10 else df[AMOUNT_COL].mean()
        
        # 计算成交额变化率
        volume_change = (recent_avg_volume - prev_avg_volume) / max(prev_avg_volume, 1)
        
        # 计算最近5天的价格变化
        recent_price_change = (df[CLOSE_COL].iloc[-1] / df[CLOSE_COL].iloc[-5]) - 1 if len(df) >= 5 else 0
        
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
    计算基本面得分（规模、上市时间等）
    
    Args:
        etf_code: ETF代码
    
    Returns:
        float: 基本面得分
    """
    try:
        size, listing_date = get_etf_basic_info(etf_code)
        
        # 规模得分（10亿=60分，100亿=100分）
        size_score = min(max(size * 0.4 + 50, 0), 100)
        
        # 上市时间得分（1年=50分，5年=100分）
        if not listing_date:
            age_score = 50.0
        else:
            try:
                # 处理不同格式的日期字符串
                if isinstance(listing_date, str):
                    # 尝试多种日期格式
                    date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]
                    listing_date_obj = None
                    for fmt in date_formats:
                        try:
                            listing_date_obj = datetime.strptime(listing_date, fmt)
                            break
                        except:
                            continue
                    if listing_date_obj:
                        years_since_listing = (datetime.now() - listing_date_obj).days / 365
                        age_score = min(50 + years_since_listing * 10, 100)
                    else:
                        logger.warning(f"ETF {etf_code} 上市日期格式无法解析: {listing_date}")
                        age_score = 50.0
                elif isinstance(listing_date, datetime):
                    years_since_listing = (datetime.now() - listing_date).days / 365
                    age_score = min(50 + years_since_listing * 10, 100)
                else:
                    logger.warning(f"ETF {etf_code} 上市日期类型未知: {type(listing_date)}")
                    age_score = 50.0
            except Exception as e:
                logger.error(f"ETF {etf_code} 上市日期处理错误: {str(e)}")
                age_score = 50.0
        
        # 综合基本面评分（规模占70%，上市时间占30%）
        fundamental_score = size_score * 0.7 + age_score * 0.3
        
        logger.debug(f"ETF {etf_code} 基本面评分: {fundamental_score:.2f} (规模: {size}亿元, 上市日期: {listing_date})")
        return fundamental_score
    
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 基本面评分失败: {str(e)}", exc_info=True)
        return 70.0  # 默认中等偏高评分

def get_etf_basic_info(etf_code: str) -> Tuple[float, Optional[str]]:
    """
    从ETF列表中获取ETF基本信息
    
    Args:
        etf_code: ETF代码 (6位数字)
    
    Returns:
        Tuple[float, Optional[str]]: (基金规模(单位:亿元), 上市日期字符串)
    """
    try:
        # 确保ETF代码格式一致（6位数字）
        etf_code = str(etf_code).strip().zfill(6)
        
        # 检查ETF列表是否有效
        etf_list = load_all_etf_list()
        if etf_list is None or etf_list.empty:
            logger.warning("ETF列表为空或无效，使用默认值")
            return 0.0, ""
        
        # 确保ETF列表包含必要的列
        required_columns = [ETF_CODE_COL, FUND_SIZE_COL, LISTING_DATE_COL]
        for col in required_columns:
            if col not in etf_list.columns:
                logger.warning(f"ETF列表缺少必要列: {col}")
                return 0.0, ""
        
        # 确保ETF列表中的ETF代码也是6位数字
        etf_list[ETF_CODE_COL] = etf_list[ETF_CODE_COL].astype(str).str.strip().str.zfill(6)
        
        etf_row = etf_list[etf_list[ETF_CODE_COL] == etf_code]
        if not etf_row.empty:
            # 处理规模
            size = 0.0
            if FUND_SIZE_COL in etf_row.iloc[0]:
                size = extract_scalar_value(
                    etf_row.iloc[0][FUND_SIZE_COL], 
                    log_prefix=f"ETF {etf_code} 规模: "
                )
            
            # 处理上市日期
            listing_date = ""
            if LISTING_DATE_COL in etf_row.iloc[0]:
                listing_date = str(etf_row.iloc[0][LISTING_DATE_COL])
            
            return size, listing_date
        
        logger.warning(f"ETF {etf_code} 未在ETF列表中找到，使用默认值")
        return 0.0, ""
    
    except Exception as e:
        error_msg = f"获取ETF {etf_code} 基本信息失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return 0.0, ""

def get_top_rated_etfs(top_n=None, min_score=60, min_fund_size=10.0, min_avg_volume=5000.0) -> pd.DataFrame:
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
        
        all_codes = metadata_df["etf_code"].tolist()
        if not all_codes:
            logger.warning("元数据中无ETF代码")
            return pd.DataFrame()
        
        score_list = []
        logger.info(f"开始计算 {len(all_codes)} 只ETF的综合评分...")
        
        for etf_code in all_codes:
            try:
                df = load_etf_daily_data(etf_code)
                if df.empty:
                    logger.debug(f"ETF {etf_code} 无日线数据，跳过评分")
                    continue
                
                # 确保ETF代码格式一致（6位数字）
                etf_code = str(etf_code).strip().zfill(6)
                
                # 计算ETF评分
                score = calculate_etf_score(etf_code, df)
                if score < min_score:
                    continue
                
                # 获取ETF基本信息（从本地元数据获取）
                size = 0.0
                listing_date = ""
                if etf_code in metadata_df["etf_code"].values:
                    size = metadata_df[metadata_df["etf_code"] == etf_code]["size"].values[0]
                    listing_date = metadata_df[metadata_df["etf_code"] == etf_code]["listing_date"].values[0]
                
                etf_name = get_etf_name(etf_code)
                
                # 计算日均成交额（单位：万元）
                avg_volume = 0.0
                if AMOUNT_COL in df.columns:
                    recent_30d = df.tail(30)
                    if len(recent_30d) > 0:
                        avg_volume = recent_30d[AMOUNT_COL].mean() / 10000
                
                # 仅保留满足条件的ETF
                if size >= min_fund_size and avg_volume >= min_avg_volume:
                    score_list.append({
                        "ETF代码": etf_code,
                        "ETF名称": etf_name,
                        "评分": score,
                        "规模": size,
                        "日均成交额": avg_volume,
                        "上市日期": listing_date
                    })
                    logger.debug(f"ETF {etf_code} 评分: {score}, 规模: {size}亿元, 日均成交额: {avg_volume}万元")
            except Exception as e:
                logger.error(f"处理ETF {etf_code} 时发生错误: {str(e)}", exc_info=True)
                continue
        
        # 检查是否有符合条件的ETF
        if not score_list:
            warning_msg = (
                f"没有ETF达到最低评分阈值 {min_score}，"
                f"或未满足规模({min_fund_size}亿元)和日均成交额({min_avg_volume}万元)要求"
            )
            logger.info(warning_msg)
            return pd.DataFrame()
        
        # 创建评分DataFrame
        score_df = pd.DataFrame(score_list).sort_values("评分", ascending=False)
        total_etfs = len(score_df)
        
        # 计算前X%的ETF数量
        top_percent = Config.SCORE_TOP_PERCENT
        top_count = max(10, int(total_etfs * top_percent / 100))
        
        # 记录筛选结果
        logger.info(f"评分完成。共{total_etfs}只ETF评分≥{min_score}，取前{top_percent}%({top_count}只)")
        logger.info(f"应用筛选参数: 规模≥{min_fund_size}亿元, 日均成交额≥{min_avg_volume}万元")
        
        # 返回结果
        if top_n is not None and top_n > 0:
            return score_df.head(top_n)
        return score_df.head(top_count)
    
    except Exception as e:
        error_msg = f"获取高分ETF列表时发生错误: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return pd.DataFrame()

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
            date = (beijing_now - timedelta(days=i)).date().strftime("%Y-%m-%d")
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
            return f"【{etf_code} 评分趋势】\n• 无历史评分数据"
        
        # 计算趋势
        latest_score = history_df.iloc[0]["评分"]
        avg_score = history_df["评分"].mean()
        trend = "上升" if latest_score > avg_score else "下降"
        
        # 生成分析报告
        report = f"【{etf_code} 评分趋势】\n"
        report += f"• 当前评分: {latest_score:.2f}\n"
        report += f"• 近期平均评分: {avg_score:.2f}\n"
        report += f"• 评分趋势: {trend}\n"
        
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
        return f"【{etf_code} 评分趋势】\n• 分析失败，请检查日志"

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
            message_type="error"
        )
    
    # 初始化日志
    logger.info("ETF评分系统初始化完成")
    
except Exception as e:
    error_msg = f"ETF评分系统初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
    
    try:
        # 退回到基础日志配置
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
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
    except Exception as send_error:
        logger.error(f"发送错误通知失败: {str(send_error)}", exc_info=True)
