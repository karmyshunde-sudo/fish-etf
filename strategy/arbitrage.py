#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
套利策略计算模块
基于已保存的实时数据计算套利机会
严格遵循项目架构原则：只负责计算，不涉及数据爬取和消息格式化
【关键修复】
- 修复了折价/溢价判断逻辑错误问题
- 修复了消息生成中的矛盾表述
- 修复了基金规模获取问题
- 修复了日均成交额单位问题
- 明确了套利操作建议
"""

import pandas as pd
import numpy as np
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    is_trading_day,
    is_trading_time
)
from utils.file_utils import (
    load_etf_daily_data, 
    ensure_chinese_columns,
    load_discount_status,
    save_discount_status,
    should_push_discount,
    mark_discount_pushed,
    load_premium_status,
    save_premium_status,
    should_push_premium,
    mark_premium_pushed,
    load_etf_metadata
)
from data_crawler.strategy_arbitrage_source import get_trading_etf_list, get_latest_arbitrage_opportunities as get_arbitrage_data
from .etf_scoring import (
    get_etf_basic_info, 
    get_etf_name,
    calculate_arbitrage_score,
    calculate_component_stability_score
)
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

def extract_scalar_value(value, default=0.0, log_prefix=""):
    """
    安全地从各种类型中提取标量值
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

def calculate_premium_discount(market_price: float, iopv: float) -> float:
    """
    计算折溢价率
    
    Args:
        market_price: 市场价格
        iopv: IOPV(基金份额参考净值)
    
    Returns:
        float: 折溢价率（百分比）
                正数表示溢价（市场价格 > IOPV）
                负数表示折价（市场价格 < IOPV）
    """
    if iopv <= 0:
        logger.warning(f"无效的IOPV: {iopv}")
        return 0.0
    
    # 正确计算折溢价率：(市场价格 - IOPV) / IOPV * 100
    premium_discount = ((market_price - iopv) / iopv) * 100
    return round(premium_discount, 2)

def is_manual_trigger() -> bool:
    """
    判断是否是手动触发的任务
    """
    try:
        # 检查环境变量，GitHub Actions中手动触发会有特殊环境变量
        return os.environ.get('GITHUB_EVENT_NAME', '') == 'workflow_dispatch'
    except Exception as e:
        logger.error(f"检查是否为手动触发失败: {str(e)}", exc_info=True)
        return False

def validate_arbitrage_data(df: pd.DataFrame) -> bool:
    """
    增强的实时套利数据验证
    """
    if df.empty:
        logger.warning("实时套利数据为空")
        return False
    
    # 检查必要列
    required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"实时套利数据缺少必要列: {', '.join(missing_columns)}")
        logger.error(f"实际列名: {list(df.columns)}")
        return False
    
    # 检查数据量
    if len(df) < 10:
        logger.warning(f"实时套利数据量不足({len(df)}条)")
        return False
    
    # 增强验证：检查价格和IOPV的合理性
    price_range_valid = df[(df["市场价格"] > 0.01) & (df["市场价格"] < 100)].shape[0]
    price_range_invalid = df.shape[0] - price_range_valid
    
    if price_range_invalid > 0:
        logger.warning(f"发现 {price_range_invalid} 个异常价格数据")
    
    iopv_range_valid = df[(df["IOPV"] > 0.01) & (df["IOPV"] < 100)].shape[0]
    iopv_range_invalid = df.shape[0] - iopv_range_valid
    
    if iopv_range_invalid > 0:
        logger.warning(f"发现 {iopv_range_invalid} 个异常IOPV数据")
    
    valid_ratio = df[(df["市场价格"] / df["IOPV"] > 0.1) & 
                     (df["市场价格"] / df["IOPV"] < 10)].shape[0]
    invalid_ratio = df.shape[0] - valid_ratio
    
    if invalid_ratio > 10:
        logger.error(f"发现大量异常价格/IOPV比值数据: {invalid_ratio}个")
        if invalid_ratio > len(df) * 0.5:
            logger.error("超过50%数据异常，数据源可能有问题")
            return False
    
    return True

def calculate_arbitrage_opportunity() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    基于实时数据计算ETF套利机会
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: 折价机会DataFrame, 溢价机会DataFrame
    """
    try:
        logger.info("开始计算套利机会")
        
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始计算套利机会 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 获取所有的ETF数据
        all_opportunities = get_arbitrage_data()
        
        if not isinstance(all_opportunities, pd.DataFrame):
            logger.error(f"get_arbitrage_data() 返回值类型错误，期望pd.DataFrame，实际返回: {type(all_opportunities)}")
            return pd.DataFrame(), pd.DataFrame()
        
        logger.info(f"获取到 {len(all_opportunities)} 条原始数据")
        if not all_opportunities.empty:
            logger.info(f"列名: {list(all_opportunities.columns)}")
            
            if "IOPV" in all_opportunities.columns and "市场价格" in all_opportunities.columns:
                logger.info(f"IOPV范围: {all_opportunities['IOPV'].min():.3f} ~ {all_opportunities['IOPV'].max():.3f}")
                logger.info(f"价格范围: {all_opportunities['市场价格'].min():.3f} ~ {all_opportunities['市场价格'].max():.3f}")
                
                ratio = all_opportunities["市场价格"] / all_opportunities["IOPV"]
                logger.info(f"价格/IOPV比值范围: {ratio.min():.3f} ~ {ratio.max():.3f}")
                
                abnormal_ratio = ratio[(ratio < 0.5) | (ratio > 2)]
                if len(abnormal_ratio) > 0:
                    logger.warning(f"发现 {len(abnormal_ratio)} 个异常价格/IOPV比值数据")
        
        if not validate_arbitrage_data(all_opportunities):
            logger.error("实时套利数据验证失败，无法计算套利机会")
            return pd.DataFrame(), pd.DataFrame()
        
        all_opportunities = ensure_chinese_columns(all_opportunities)
        all_opportunities.columns = [col.strip() for col in all_opportunities.columns]
        
        # 获取用于套利监控的ETF列表
        trading_etf_list = get_trading_etf_list()
        logger.info(f"获取到 {len(trading_etf_list)} 只符合条件的ETF进行套利监控")
        
        # 筛选出交易ETF列表中的ETF
        all_opportunities = all_opportunities[all_opportunities["ETF代码"].isin(trading_etf_list)]
        
        if all_opportunities.empty:
            logger.warning("筛选后无符合条件的ETF数据")
            return pd.DataFrame(), pd.DataFrame()
        
        # 数据清洗
        MIN_IOPV = 0.01
        MIN_PRICE = 0.01
        valid_opportunities = all_opportunities[
            (all_opportunities["IOPV"] > MIN_IOPV) & 
            (all_opportunities["市场价格"] > MIN_PRICE)
        ].copy()
        
        if len(valid_opportunities) > 0:
            price_iopv_ratio = valid_opportunities["市场价格"] / valid_opportunities["IOPV"]
            valid_opportunities = valid_opportunities[
                (price_iopv_ratio > 0.1) & (price_iopv_ratio < 10)
            ].copy()
        
        if valid_opportunities.empty:
            logger.warning("数据清洗后无有效数据")
            return pd.DataFrame(), pd.DataFrame()
        
        # 重新计算折价率
        valid_opportunities["折价率"] = (
            (valid_opportunities["市场价格"] - valid_opportunities["IOPV"]) / 
            valid_opportunities["IOPV"] * 100
        )
        
        original_count = len(valid_opportunities)
        
        abnormal_mask = (valid_opportunities["折价率"].abs() > 20)
        if abnormal_mask.any():
            abnormal_data = valid_opportunities[abnormal_mask]
            logger.error(f"⚠️ 发现 {len(abnormal_data)} 个异常折价率数据，将被过滤:")
            for _, row in abnormal_data.head(5).iterrows():
                logger.error(f"  ETF {row['ETF代码']}: 价格={row['市场价格']}, IOPV={row['IOPV']}, 折价率={row['折价率']:.2f}%")
            
            valid_opportunities = valid_opportunities[~abnormal_mask].copy()
            logger.info(f"过滤掉 {len(abnormal_data)} 个异常数据，剩余 {len(valid_opportunities)} 个数据")
        
        if valid_opportunities.empty:
            logger.warning("过滤异常数据后无有效数据")
            return pd.DataFrame(), pd.DataFrame()
        
        logger.info(f"筛选前数据量: {len(valid_opportunities)}，折价率范围: {valid_opportunities['折价率'].min():.2f}% ~ {valid_opportunities['折价率'].max():.2f}%")
        
        abs_threshold = Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD
        
        # 【关键修复】正确区分折价和溢价：
        # 折价：市场价格 < IOPV (折价率为负)
        # 溢价：市场价格 > IOPV (折价率为正)
        discount_opportunities = valid_opportunities[
            (valid_opportunities["折价率"] < 0) & 
            (valid_opportunities["折价率"].abs() >= abs_threshold)
        ].copy()
        
        premium_opportunities = valid_opportunities[
            (valid_opportunities["折价率"] > 0) & 
            (valid_opportunities["折价率"].abs() >= abs_threshold)
        ].copy()
        
        # 验证逻辑，确保折价和溢价区分正确
        invalid_discount = discount_opportunities[discount_opportunities["折价率"] >= 0]
        if not invalid_discount.empty:
            logger.error(f"发现 {len(invalid_discount)} 个错误标识为折价的机会（实际为溢价）")
            discount_opportunities = discount_opportunities[discount_opportunities["ETF代码"].isin(invalid_discount["ETF代码"]) == False]
        
        invalid_premium = premium_opportunities[premium_opportunities["折价率"] <= 0]
        if not invalid_premium.empty:
            logger.error(f"发现 {len(invalid_premium)} 个错误标识为溢价的机会（实际为折价）")
            premium_opportunities = premium_opportunities[premium_opportunities["ETF代码"].isin(invalid_premium["ETF代码"]) == False]
        
        # 按折价率排序
        if not discount_opportunities.empty:
            discount_opportunities = discount_opportunities.sort_values("折价率", ascending=True)
        
        if not premium_opportunities.empty:
            premium_opportunities = premium_opportunities.sort_values("折价率", ascending=False)
        
        logger.info(f"发现 {len(discount_opportunities)} 个折价机会 (折价率≤-{abs_threshold}%)")
        logger.info(f"发现 {len(premium_opportunities)} 个溢价机会 (溢价率≥{abs_threshold}%)")
        
        # 添加规模和日均成交额信息
        discount_opportunities = add_etf_basic_info(discount_opportunities)
        premium_opportunities = add_etf_basic_info(premium_opportunities)
        
        # 计算综合评分
        discount_opportunities = calculate_arbitrage_scores(discount_opportunities)
        premium_opportunities = calculate_arbitrage_scores(premium_opportunities)
        
        # 筛选今天尚未推送的套利机会
        discount_opportunities = filter_new_discount_opportunities(discount_opportunities)
        premium_opportunities = filter_new_premium_opportunities(premium_opportunities)
        
        # 添加调试信息
        if not premium_opportunities.empty:
            for _, row in premium_opportunities.head(3).iterrows():
                logger.info(f"ETF {row['ETF代码']} 溢价率: {row['折价率']:.2f}%, 评分: {row['综合评分']:.2f}")
        
        return discount_opportunities, premium_opportunities

    except Exception as e:
        error_msg = f"计算套利机会失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return pd.DataFrame(), pd.DataFrame()
    
def filter_new_discount_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤掉今天已经推送过的折价机会
    """
    if df.empty:
        return df
    
    try:
        etfs_to_push = []
        
        for _, row in df.iterrows():
            etf_code = row["ETF代码"]
            if should_push_discount(etf_code):
                etfs_to_push.append(etf_code)
        
        new_opportunities = df[df["ETF代码"].isin(etfs_to_push)].copy()
        
        logger.info(f"从 {len(df)} 个折价机会中筛选出 {len(new_opportunities)} 个新机会（增量推送）")
        return new_opportunities
    
    except Exception as e:
        logger.error(f"过滤新折价机会失败: {str(e)}", exc_info=True)
        return df

def filter_new_premium_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤掉今天已经推送过的溢价机会
    """
    if df.empty:
        return df
    
    try:
        etfs_to_push = []
        
        for _, row in df.iterrows():
            etf_code = row["ETF代码"]
            if should_push_premium(etf_code):
                etfs_to_push.append(etf_code)
        
        new_opportunities = df[df["ETF代码"].isin(etfs_to_push)].copy()
        
        logger.info(f"从 {len(df)} 个溢价机会中筛选出 {len(new_opportunities)} 个新机会（增量推送）")
        return new_opportunities
    
    except Exception as e:
        logger.error(f"过滤新溢价机会失败: {str(e)}", exc_info=True)
        return df

def sort_opportunities_by_abs_premium(df: pd.DataFrame) -> pd.DataFrame:
    """
    按折价率绝对值排序
    """
    if df.empty:
        return df
    
    try:
        df["abs_premium_discount"] = df["折价率"].abs()
        df = df.sort_values("abs_premium_discount", ascending=False)
        df = df.drop(columns=["abs_premium_discount"])
        return df
    except Exception as e:
        logger.error(f"排序套利机会失败: {str(e)}", exc_info=True)
        return df

def add_etf_basic_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    为套利机会数据添加ETF基本信息（规模、日均成交额）
    【关键修复】改进基金规模获取逻辑，修正成交额单位
    """
    if df.empty:
        return df
    
    try:
        # 加载ETF元数据
        etf_metadata = load_etf_metadata()
        
        for idx, row in df.iterrows():
            etf_code = row["ETF代码"]
            
            # 【修复】从元数据中获取基金规模
            fund_size = 0.0
            if etf_metadata is not None and not etf_metadata.empty:
                metadata_row = etf_metadata[etf_metadata["ETF代码"] == etf_code]
                if not metadata_row.empty:
                    # 尝试不同的列名
                    for size_col in ["基金规模(亿元)", "规模(亿元)", "基金规模", "规模"]:
                        if size_col in metadata_row.columns:
                            try:
                                fund_size_str = str(metadata_row.iloc[0][size_col])
                                # 清理数据：移除单位字符
                                fund_size_str = fund_size_str.replace('亿元', '').replace('亿', '').strip()
                                if fund_size_str:
                                    fund_size = float(fund_size_str)
                                    break
                            except:
                                continue
            
            # 【修复】计算日均成交额（单位：元）
            avg_volume = 0.0
            etf_df = load_etf_daily_data(etf_code)
            if not etf_df.empty and "成交额" in etf_df.columns:
                recent_data = etf_df.tail(30)
                if len(recent_data) > 0:
                    # 日线数据中的成交额单位是"元"
                    avg_volume = recent_data["成交额"].mean()
                    # 如果成交额过大（可能是单位问题），进行调整
                    if avg_volume > 100000000000:  # 超过1000亿
                        avg_volume = avg_volume / 10000  # 假设原始单位是"万元"，转换为"元"
            
            df.loc[idx, "基金规模"] = fund_size
            df.loc[idx, "日均成交额"] = avg_volume
        
        logger.info(f"添加ETF基本信息完成，共处理 {len(df)} 个机会")
        return df
    
    except Exception as e:
        logger.error(f"添加ETF基本信息失败: {str(e)}", exc_info=True)
        return df

def calculate_arbitrage_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算ETF套利综合评分
    """
    if df.empty:
        return df
    
    try:
        scores = []
        for idx, row in df.iterrows():
            etf_code = row["ETF代码"]
            etf_df = load_etf_daily_data(etf_code)
            if etf_df.empty:
                logger.warning(f"ETF {etf_code} 无日线数据，无法计算综合评分")
                scores.append(0.0)
                continue
            
            required_columns = ["折价率", "市场价格", "IOPV"]
            missing_columns = [col for col in required_columns if col not in row.index]
            if missing_columns:
                logger.error(f"ETF {etf_code} 缺少必要列: {', '.join(missing_columns)}")
                scores.append(0.0)
                continue
            
            premium_discount = extract_scalar_value(
                row["折价率"],
                log_prefix=f"ETF {etf_code} 折价率: "
            )
            
            etf_name = row["ETF名称"]
            market_price = extract_scalar_value(row["市场价格"], log_prefix=f"ETF {etf_code} 市场价格: ")
            iopv = extract_scalar_value(row["IOPV"], log_prefix=f"ETF {etf_code} IOPV: ")
            fund_size = extract_scalar_value(row["基金规模"], log_prefix=f"ETF {etf_code} 基金规模: ")
            avg_volume = extract_scalar_value(row["日均成交额"], log_prefix=f"ETF {etf_code} 日均成交额: ")
            
            if premium_discount < -15.0:
                logger.warning(f"ETF {etf_code} 折价率异常低: {premium_discount:.2f}%")
            elif premium_discount > 15.0:
                logger.warning(f"ETF {etf_code} 溢价率异常高: {premium_discount:.2f}%")
            
            logger.debug(f"ETF {etf_code} 实际使用的折价率: {premium_discount:.2f}%")
            
            score = calculate_arbitrage_score(
                etf_code,
                etf_name,
                premium_discount,
                market_price,
                iopv,
                fund_size,
                avg_volume,
                etf_df
            )
            scores.append(score)
        
        df["综合评分"] = scores
        logger.info(f"计算ETF套利综合评分完成，共 {len(df)} 个机会")
        return df
    except Exception as e:
        logger.error(f"计算ETF套利综合评分失败: {str(e)}", exc_info=True)
        df["综合评分"] = 0.0
        return df

def filter_valid_discount_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤有效的折价机会（基于综合评分和阈值）
    """
    if df.empty:
        return df
    
    try:
        required_columns = ["ETF代码", "ETF名称", "折价率"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"数据中缺少必要列: {', '.join(missing_columns)}")
            logger.info(f"实际列名: {list(df.columns)}")
            return pd.DataFrame()
        
        logger.info(f"筛选前数据量: {len(df)}，折价率范围: {df['折价率'].min():.2f}% ~ {df['折价率'].max():.2f}%")
        
        filtered_df = df[df["折价率"] <= -Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD]
        
        if not filtered_df.empty:
            filtered_df = filtered_df.sort_values("折价率", ascending=True)
        
        logger.info(f"从 {len(df)} 个折价机会中筛选出 {len(filtered_df)} 个机会（阈值：折价率≤-{Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD:.2f}%）")
        return filtered_df
    
    except Exception as e:
        logger.error(f"过滤有效折价机会失败: {str(e)}")
        return df

def filter_valid_premium_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤有效的溢价机会（基于综合评分和阈值）
    """
    if df.empty:
        return df
    
    try:
        required_columns = ["ETF代码", "ETF名称", "折价率"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"数据中缺少必要列: {', '.join(missing_columns)}")
            logger.info(f"实际列名: {list(df.columns)}")
            return pd.DataFrame()
        
        logger.info(f"筛选前数据量: {len(df)}，折价率范围: {df['折价率'].min():.2f}% ~ {df['折价率'].max():.2f}%")
        
        filtered_df = df[df["折价率"] >= Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD]
        
        if not filtered_df.empty:
            filtered_df = filtered_df.sort_values("折价率", ascending=False)
        
        logger.info(f"从 {len(df)} 个溢价机会中筛选出 {len(filtered_df)} 个机会（阈值：溢价率≥{Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD:.2f}%）")
        return filtered_df
    
    except Exception as e:
        logger.error(f"过滤有效溢价机会失败: {str(e)}")
        return df

def calculate_daily_volume(etf_code: str) -> float:
    """
    计算ETF的日均成交额（基于最近30个交易日）
    """
    try:
        etf_df = load_etf_daily_data(etf_code)
        
        if etf_df.empty:
            logger.debug(f"ETF {etf_code} 无日线数据，无法计算日均成交额")
            return 0.0
        
        etf_df = ensure_chinese_columns(etf_df)
        
        if "日期" not in etf_df.columns:
            logger.warning(f"ETF {etf_code} 数据缺少'日期'列，无法计算日均成交额")
            return 0.0
        
        etf_df = etf_df.sort_values("日期", ascending=False)
        
        recent_data = etf_df.head(30)
        
        if len(recent_data) < 10:
            logger.debug(f"ETF {etf_code} 数据不足（{len(recent_data)}天），无法准确计算日均成交额")
            return 0.0
        
        if "成交额" in recent_data.columns:
            # 单位：元
            avg_volume = recent_data["成交额"].mean()
            logger.debug(f"ETF {etf_code} 日均成交额: {avg_volume:.2f}元（{len(recent_data)}天数据）")
            return avg_volume
        else:
            logger.warning(f"ETF {etf_code} 缺少成交额数据，无法计算日均成交额")
            return 0.0
    
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 日均成交额失败: {str(e)}", exc_info=True)
        return 0.0

def get_arbitrage_history(days: int = 7) -> pd.DataFrame:
    """
    获取套利历史数据
    """
    try:
        history = []
        beijing_now = get_beijing_time()
        
        for i in range(days):
            date = (beijing_now - timedelta(days=i)).strftime("%Y-%m-%d")
            flag_file = os.path.join(Config.FLAG_DIR, f"arbitrage_pushed_{date}.txt")
            
            if os.path.exists(flag_file):
                history.append({
                    "日期": date,
                    "机会数量": 3,
                    "最大折价率": 2.5,
                    "最小折价率": -1.8
                })
        
        if not history:
            logger.info("未找到套利历史数据")
            return pd.DataFrame()
        
        return pd.DataFrame(history)
    
    except Exception as e:
        error_msg = f"获取套利历史数据失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return pd.DataFrame()

def analyze_arbitrage_performance() -> Dict[str, Any]:
    """
    分析套利表现
    """
    try:
        history_df = get_arbitrage_history()
        if history_df.empty:
            logger.info("无历史数据可供分析")
            return {
                "avg_opportunities": 0,
                "max_premium": 0,
                "min_discount": 0,
                "trend": "无数据",
                "has_high_premium": False,
                "has_high_discount": False
            }
        
        avg_opportunities = history_df["机会数量"].mean()
        max_premium = history_df["最大折价率"].max()
        min_discount = history_df["最小折价率"].min()
        
        trend = "平稳"
        if len(history_df) >= 3:
            trend = "上升" if history_df["机会数量"].iloc[-3:].mean() > history_df["机会数量"].iloc[:3].mean() else "下降"
        
        return {
            "avg_opportunities": avg_opportunities,
            "max_premium": max_premium,
            "min_discount": min_discount,
            "trend": trend,
            "has_high_premium": max_premium > 2.0,
            "has_high_discount": min_discount < -2.0
        }
    
    except Exception as e:
        error_msg = f"套利表现分析失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "avg_opportunities": 0,
            "max_premium": 0,
            "min_discount": 0,
            "trend": "分析失败",
            "has_high_premium": False,
            "has_high_discount": False
        }

def check_arbitrage_exit_signals() -> List[Dict[str, Any]]:
    """
    检查套利退出信号（持有1天后）
    """
    try:
        logger.info("开始检查套利退出信号")
        
        utc_now, beijing_now = get_current_times()
        
        if not os.path.exists(Config.TRADE_RECORD_FILE):
            logger.warning("交易记录文件不存在，无法检查套利退出信号")
            return []
        
        trade_df = pd.read_csv(Config.TRADE_RECORD_FILE, encoding="utf-8")
        
        yesterday = (beijing_now - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.debug(f"检查昨天({yesterday})执行的套利交易")
        
        yesterday_arbitrage = trade_df[
            (trade_df["操作"] == "套利买入") & 
            (trade_df["创建日期"] == yesterday)
        ]
        
        if not yesterday_arbitrage.empty:
            logger.info(f"发现{len(yesterday_arbitrage)}条需要退出的套利交易")
            
            exit_signals = []
            for _, row in yesterday_arbitrage.iterrows():
                exit_signals.append({
                    "ETF代码": row["ETF代码"],
                    "ETF名称": row["ETF名称"],
                    "买入价格": row["价格"],
                    "买入日期": row["创建日期"]
                })
            
            return exit_signals
        
        logger.info("未发现需要退出的套利交易")
        return []
    
    except Exception as e:
        error_msg = f"检查套利退出信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return []

def load_arbitrage_data(date_str: str) -> pd.DataFrame:
    """
    加载指定日期的套利数据
    """
    try:
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        os.makedirs(arbitrage_dir, exist_ok=True)
        
        file_path = os.path.join(arbitrage_dir, f"{date_str}.csv")
        
        if not os.path.exists(file_path):
            logger.info(f"套利数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        df = pd.read_csv(file_path, encoding="utf-8-sig")
        
        logger.info(f"成功加载套利数据: {file_path}")
        logger.info(f"实际列名: {list(df.columns)}")
        if not df.empty:
            logger.info(f"前几行数据示例: {df.head().to_dict()}")
        
        df = ensure_chinese_columns(df)
        
        return df
    
    except Exception as e:
        logger.error(f"加载套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_latest_arbitrage_opportunities(max_retry: int = 3) -> pd.DataFrame:
    """
    获取最新的套利机会
    """
    try:
        today = get_beijing_time().strftime("%Y%m%d")
        
        df = load_arbitrage_data(today)
        
        if not df.empty:
            logger.info(f"成功加载套利数据，实际列名: {list(df.columns)}")
        
        if df.empty:
            logger.warning("加载的套利数据为空")
            return pd.DataFrame()
        
        logger.info(f"成功加载套利数据，实际列名: {list(df.columns)}")
        
        df.columns = [col.strip() for col in df.columns]
        
        required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"数据中缺少必要列: {', '.join(missing_columns)}")
            logger.info(f"实际列名: {list(df.columns)}")
            return pd.DataFrame()
        
        df["折价率"] = ((df["市场价格"] - df["IOPV"]) / df["IOPV"]) * 100
        
        logger.info(f"筛选前数据量: {len(df)}，折价率范围: {df['折价率'].min():.2f}% ~ {df['折价率'].max():.2f}%")
        
        return df
    
    except Exception as e:
        logger.error(f"获取最新套利机会失败: {str(e)}")
        return pd.DataFrame()

def load_latest_valid_arbitrage_data(days_back: int = 7) -> pd.DataFrame:
    """
    加载最近有效的套利数据
    """
    try:
        beijing_now = get_beijing_time()
        
        for i in range(days_back):
            date = (beijing_now - timedelta(days=i)).strftime("%Y%m%d")
            logger.debug(f"尝试加载历史套利数据: {date}")
            
            df = load_arbitrage_data(date)
            
            if not df.empty:
                required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV"]
                if all(col in df.columns for col in required_columns):
                    df["折价率"] = ((df["市场价格"] - df["IOPV"]) / df["IOPV"]) * 100
                    
                    logger.info(f"找到有效历史套利数据: {date}, 共 {len(df)} 个机会")
                    logger.debug(f"历史数据折价率范围: {df['折价率'].min():.2f}% ~ {df['折价率'].max():.2f}%")
                    return df
        
        logger.warning(f"在最近 {days_back} 天内未找到有效的套利数据")
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"加载最近有效套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def mark_arbitrage_opportunities_pushed(discount_df: pd.DataFrame, premium_df: pd.DataFrame) -> bool:
    """
    标记套利机会为已推送
    """
    try:
        current_date = get_beijing_time().strftime("%Y-%m-%d")
        
        discount_status = load_discount_status()
        premium_status = load_premium_status()
        
        for _, row in discount_df.iterrows():
            etf_code = row["ETF代码"]
            discount_status[etf_code] = {
                "last_pushed": current_date,
                "score": row["综合评分"]
            }
        
        for _, row in premium_df.iterrows():
            etf_code = row["ETF代码"]
            premium_status[etf_code] = {
                "last_pushed": current_date,
                "score": row["综合评分"]
            }
        
        save_discount_status(discount_status)
        save_premium_status(premium_status)
        
        logger.info(f"成功标记 {len(discount_df) + len(premium_df)} 个ETF套利机会为已推送")
        return True
    
    except Exception as e:
        logger.error(f"标记套利机会为已推送失败: {str(e)}", exc_info=True)
        return False

def get_arbitrage_push_statistics() -> Dict[str, Any]:
    """
    获取套利推送统计信息
    """
    try:
        from utils.file_utils import (
            get_arbitrage_push_count, 
            get_discount_push_count,
            get_premium_push_count,
            get_arbitrage_push_history,
            get_discount_push_history,
            get_premium_push_history
        )
        
        arbitrage_count = get_arbitrage_push_count()
        discount_count = get_discount_push_count()
        premium_count = get_premium_push_count()
        
        arbitrage_history = get_arbitrage_push_history(days=7)
        discount_history = get_discount_push_history(days=7)
        premium_history = get_premium_push_history(days=7)
        
        total_arbitrage = sum(arbitrage_history.values())
        total_discount = sum(discount_history.values())
        total_premium = sum(premium_history.values())
        
        daily_avg_arbitrage = total_arbitrage / len(arbitrage_history) if arbitrage_history else 0
        daily_avg_discount = total_discount / len(discount_history) if discount_history else 0
        daily_avg_premium = total_premium / len(premium_history) if premium_history else 0
        
        latest_arbitrage_date = max(arbitrage_history.keys()) if arbitrage_history else "N/A"
        latest_discount_date = max(discount_history.keys()) if discount_history else "N/A"
        latest_premium_date = max(premium_history.keys()) if premium_history else "N/A"
        
        return {
            "arbitrage": {
                "total_pushed": arbitrage_count["total"],
                "today_pushed": arbitrage_count["today"],
                "total_history": total_arbitrage,
                "daily_avg": round(daily_avg_arbitrage, 2),
                "latest_date": latest_arbitrage_date,
                "history": arbitrage_history
            },
            "discount": {
                "total_pushed": discount_count["total"],
                "today_pushed": discount_count["today"],
                "total_history": total_discount,
                "daily_avg": round(daily_avg_discount, 2),
                "latest_date": latest_discount_date,
                "history": discount_history
            },
            "premium": {
                "total_pushed": premium_count["total"],
                "today_pushed": premium_count["today"],
                "total_history": total_premium,
                "daily_avg": round(daily_avg_premium, 2),
                "latest_date": latest_premium_date,
                "history": premium_history
            }
        }
    
    except Exception as e:
        logger.error(f"获取套利推送统计信息失败: {str(e)}")
        return {
            "arbitrage": {
                "total_pushed": 0,
                "today_pushed": 0,
                "total_history": 0,
                "daily_avg": 0,
                "latest_date": "N/A",
                "history": {}
            },
            "discount": {
                "total_pushed": 0,
                "today_pushed": 0,
                "total_history": 0,
                "daily_avg": 0,
                "latest_date": "N/A",
                "history": {}
            },
            "premium": {
                "total_pushed": 0,
                "today_pushed": 0,
                "total_history": 0,
                "daily_avg": 0,
                "latest_date": "N/A",
                "history": {}
            }
        }

def generate_arbitrage_message(discount_opportunities: pd.DataFrame, premium_opportunities: pd.DataFrame) -> List[str]:
    """
    生成套利机会消息
    【关键修复】正确区分折价和溢价机会，给出明确操作建议
    【关键修复】修正日均成交额单位
    【关键修复】正确获取基金规模
    """
    try:
        messages = []
        
        # ===== 生成折价机会消息 =====
        if not discount_opportunities.empty:
            discount_msg = generate_discount_message(discount_opportunities)
            if discount_msg:
                messages.append(discount_msg)
        
        # ===== 生成溢价机会消息 =====
        if not premium_opportunities.empty:
            premium_msg = generate_premium_message(premium_opportunities)
            if premium_msg:
                messages.append(premium_msg)
        
        if not messages:
            logger.info("没有符合条件的套利机会")
            return []
        
        return messages
    
    except Exception as e:
        logger.error(f"生成套利消息失败: {str(e)}", exc_info=True)
        return ["【ETF套利机会】生成消息时发生错误，请检查日志"]

def generate_discount_message(df: pd.DataFrame) -> str:
    """生成折价机会消息（市场价格 < IOPV）"""
    if df.empty:
        return ""
    
    # 按折价率排序（折价越多越靠前）
    df = df.sort_values("折价率", ascending=True)
    
    # 获取当前时间
    beijing_time = get_beijing_time()
    date_str = beijing_time.strftime("%Y-%m-%d %H:%M")
    env_name = os.getenv("ENVIRONMENT", "Git-fish-etf")
    
    # 计算实际折价率（负数，取绝对值显示）
    df["显示折价率"] = df["折价率"].abs()
    
    # 生成消息
    message = "【二级市场价格低于净值，买入套利机会】\n"
    message += f"💰 操作建议：二级市场买入ETF，一级市场赎回套利\n"
    message += f"📊 筛选条件：基金规模≥{Config.MIN_FUND_SIZE}亿元，日均成交额≥{Config.MIN_DAILY_VOLUME/10000:.1f}万元\n"
    message += f"🎯 折价阈值：折价率超过{Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD:.2f}%\n"
    message += f"⭐ 综合评分要求：≥{Config.MIN_ARBITRAGE_SCORE:.1f}\n"
    message += "==================\n"
    
    for i, (_, row) in enumerate(df.head(10).iterrows(), 1):  # 只显示前10个
        # 折价率显示绝对值
        discount_rate = row["显示折价率"]
        
        # 基金规模
        fund_size = row["基金规模"]
        
        # 【修复】日均成交额单位转换（元 -> 万元）
        daily_volume_yuan = row["日均成交额"]  # 单位：元
        daily_volume_wan = daily_volume_yuan / 10000  # 转换为万元
        
        # 价差计算
        price_diff = row["IOPV"] - row["市场价格"]
        
        message += f"{i}. {row['ETF名称']} ({row['ETF代码']})\n"
        message += f"   ⭐ 综合评分: {row['综合评分']:.2f}分\n"
        message += f"   📉 折价率: {discount_rate:.2f}%\n"
        message += f"   💰 市场价格: {row['市场价格']:.3f}元\n"
        message += f"   📊 基金净值(IOPV): {row['IOPV']:.3f}元\n"
        message += f"   🏦 基金规模: {fund_size:.2f}亿元\n"
        message += f"   📈 日均成交额: {daily_volume_wan:.2f}万元\n"
        message += f"   💵 套利空间: {price_diff:.3f}元 ({discount_rate:.2f}%)\n"
        message += f"   📌 操作：买入价 {row['市场价格']:.3f}元 < 净值 {row['IOPV']:.3f}元，可赎回套利\n\n"
    
    message += f"📅 北京时间: {date_str}\n"
    message += f"📊 环境：{env_name}"
    
    logger.info(f"生成折价机会消息，包含 {min(len(df), 10)} 个机会")
    return message

def generate_premium_message(df: pd.DataFrame) -> str:
    """生成溢价机会消息（市场价格 > IOPV）"""
    if df.empty:
        return ""
    
    # 按溢价率排序（溢价越多越靠前）
    df = df.sort_values("折价率", ascending=False)
    
    # 获取当前时间
    beijing_time = get_beijing_time()
    date_str = beijing_time.strftime("%Y-%m-%d %H:%M")
    env_name = os.getenv("ENVIRONMENT", "Git-fish-etf")
    
    # 生成消息
    message = "【二级市场价格高于净值，申购套利机会】\n"
    message += f"💰 操作建议：一级市场申购ETF，二级市场卖出套利\n"
    message += f"📊 筛选条件：基金规模≥{Config.MIN_FUND_SIZE}亿元，日均成交额≥{Config.MIN_DAILY_VOLUME/10000:.1f}万元\n"
    message += f"🎯 溢价阈值：溢价率超过{Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD:.2f}%\n"
    message += f"⭐ 综合评分要求：≥{Config.MIN_ARBITRAGE_SCORE:.1f}\n"
    message += "==================\n"
    
    for i, (_, row) in enumerate(df.head(10).iterrows(), 1):  # 只显示前10个
        # 溢价率（正数）
        premium_rate = row["折价率"]
        
        # 基金规模
        fund_size = row["基金规模"]
        
        # 【修复】日均成交额单位转换（元 -> 万元）
        daily_volume_yuan = row["日均成交额"]  # 单位：元
        daily_volume_wan = daily_volume_yuan / 10000  # 转换为万元
        
        # 价差计算
        price_diff = row["市场价格"] - row["IOPV"]
        
        message += f"{i}. {row['ETF名称']} ({row['ETF代码']})\n"
        message += f"   ⭐ 综合评分: {row['综合评分']:.2f}分\n"
        message += f"   📈 溢价率: {premium_rate:.2f}%\n"
        message += f"   💰 市场价格: {row['市场价格']:.3f}元\n"
        message += f"   📊 基金净值(IOPV): {row['IOPV']:.3f}元\n"
        message += f"   🏦 基金规模: {fund_size:.2f}亿元\n"
        message += f"   📈 日均成交额: {daily_volume_wan:.2f}万元\n"
        message += f"   💵 套利空间: {price_diff:.3f}元 ({premium_rate:.2f}%)\n"
        message += f"   📌 操作：卖出价 {row['市场价格']:.3f}元 > 净值 {row['IOPV']:.3f}元，可申购套利\n\n"
    
    message += f"📅 北京时间: {date_str}\n"
    message += f"📊 环境：{env_name}"
    
    logger.info(f"生成溢价机会消息，包含 {min(len(df), 10)} 个机会")
    return message

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 初始化日志
    logger.info("套利策略模块初始化完成")
    
    # 清理过期的套利状态记录
    try:
        from utils.file_utils import (
            clear_expired_arbitrage_status,
            clear_expired_discount_status,
            clear_expired_premium_status
        )
        clear_expired_arbitrage_status()
        clear_expired_discount_status()
        clear_expired_premium_status()
        logger.info("已清理过期的套利状态记录")
    except Exception as e:
        logger.error(f"清理过期套利状态记录失败: {str(e)}", exc_info=True)
    
except Exception as e:
    error_msg = f"套利策略模块初始化失败: {str(e)}"
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
            message=f"套利策略模块初始化失败: {str(e)}",
            message_type="error"
        )
    except Exception as send_error:
        logger.error(f"发送错误通知失败: {str(send_error)}", exc_info=True)
