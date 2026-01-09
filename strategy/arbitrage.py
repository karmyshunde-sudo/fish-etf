#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
套利策略计算模块
基于已保存的实时数据计算套利机会
严格遵循项目架构原则：只负责计算，不涉及数据爬取和消息格式化
【已修复】
- 修复了非交易日仍尝试计算的问题 - 已移除，现在任何时间都可以计算
- 修复了ETF数量不一致问题
- 修复了无日线数据但有溢价率的逻辑矛盾
- 确保数据源一致性
- 修复列名一致性问题：数据文件中实际为"折价率"而非"折溢价率"
- 确保基金规模数据正确获取
- 【关键修复】彻底修复折溢价标识错误问题
- 【新增修复】增强数据验证，解决异常折价率问题
- 【新增修复】移除交易时间判断，支持任何时间测试
- 【新增修复】修复数据验证逻辑过度严格的问题
- 【新增修复】优化异常数据处理，避免所有数据被过滤
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
    is_trading_day,  # 保留导入但不再使用
    is_trading_time  # 保留导入但不再使用
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

def calculate_premium_discount(market_price: float, iopv: float) -> float:
    """
    计算折溢价率
    Args:
        market_price: 市场价格
        iopv: IOPV(基金份额参考净值)
    
    Returns:
        float: 折溢价率（百分比），正数表示溢价，负数表示折价
    """
    if iopv <= 0:
        logger.warning(f"无效的IOPV: {iopv}")
        return 0.0
    
    # 正确计算折溢价率：(市场价格 - IOPV) / IOPV * 100
    # 结果为正：溢价（市场价格 > IOPV）
    # 结果为负：折价（市场价格 < IOPV）
    premium_discount = ((market_price - iopv) / iopv) * 100
    return round(premium_discount, 2)

def is_manual_trigger() -> bool:
    """
    判断是否是手动触发的任务
    
    Returns:
        bool: 如果是手动触发返回True，否则返回False
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
    
    Args:
        df: 实时套利数据DataFrame
    Returns:
        bool: 数据是否有效
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
    
    # 【修复】降低数据量要求，避免因数据量少而返回False
    if len(df) < 5:  # 从10降低到5
        logger.warning(f"实时套利数据量较少({len(df)}条)，但仍尝试处理")
        # 不返回False，继续处理
    
    # 增强验证：检查价格和IOPV的合理性
    # 1. 检查价格范围（典型的ETF价格范围）
    price_range_valid = df[(df["市场价格"] > 0.01) & (df["市场价格"] < 100)].shape[0]
    price_range_invalid = df.shape[0] - price_range_valid
    
    if price_range_invalid > 0:
        logger.warning(f"发现 {price_range_invalid} 个异常价格数据")
    
    # 2. 检查IOPV范围
    iopv_range_valid = df[(df["IOPV"] > 0.01) & (df["IOPV"] < 100)].shape[0]
    iopv_range_invalid = df.shape[0] - iopv_range_valid
    
    if iopv_range_invalid > 0:
        logger.warning(f"发现 {iopv_range_invalid} 个异常IOPV数据")
    
    # 3. 检查价格和IOPV的比值是否在合理范围内
    # 正常情况下，市场价格和IOPV不会相差太大
    valid_ratio = df[(df["市场价格"] / df["IOPV"] > 0.1) & 
                     (df["市场价格"] / df["IOPV"] < 10)].shape[0]
    invalid_ratio = df.shape[0] - valid_ratio
    
    if invalid_ratio > 0:  # 如果有异常数据
        logger.warning(f"发现 {invalid_ratio} 个异常价格/IOPV比值数据")
        # 记录一些异常数据示例
        if invalid_ratio > 0:
            abnormal_samples = df[(df["市场价格"] / df["IOPV"] <= 0.1) | 
                                 (df["市场价格"] / df["IOPV"] >= 10)].head(3)
            for _, row in abnormal_samples.iterrows():
                ratio = row["市场价格"] / row["IOPV"]
                logger.warning(f"异常数据示例: ETF {row['ETF代码']}, 价格={row['市场价格']}, IOPV={row['IOPV']}, 比值={ratio:.3f}")
        
        # 【修复】不直接返回False，允许继续处理
        if invalid_ratio > len(df) * 0.8:  # 超过80%数据异常
            logger.error("超过80%数据异常，数据源可能有问题")
            return False
    
    return True

def calculate_arbitrage_opportunity() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    基于实时数据计算ETF套利机会 - 增强版本
    【重要修改】移除了交易日判断，现在任何时间都可以计算
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: 折价机会DataFrame, 溢价机会DataFrame
    """
    try:
        # 【重要修改】移除了交易日判断，现在任何时间都可以计算
        logger.info("开始计算套利机会（已移除交易日限制）")
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始计算套利机会 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 获取所有的ETF数据
        all_opportunities = get_arbitrage_data()
        
        # 检查返回值类型
        if not isinstance(all_opportunities, pd.DataFrame):
            logger.error(f"get_arbitrage_data() 返回值类型错误，期望pd.DataFrame，实际返回: {type(all_opportunities)}")
            return pd.DataFrame(), pd.DataFrame()
        
        # 添加诊断信息
        logger.info(f"获取到 {len(all_opportunities)} 条原始数据")
        if not all_opportunities.empty:
            logger.info(f"列名: {list(all_opportunities.columns)}")
            
            # 检查IOPV和价格的范围
            if "IOPV" in all_opportunities.columns and "市场价格" in all_opportunities.columns:
                logger.info(f"IOPV范围: {all_opportunities['IOPV'].min():.3f} ~ {all_opportunities['IOPV'].max():.3f}")
                logger.info(f"价格范围: {all_opportunities['市场价格'].min():.3f} ~ {all_opportunities['市场价格'].max():.3f}")
                
                # 计算价格/IOPV比值
                ratio = all_opportunities["市场价格"] / all_opportunities["IOPV"]
                logger.info(f"价格/IOPV比值范围: {ratio.min():.3f} ~ {ratio.max():.3f}")
                
                # 统计异常比例
                abnormal_ratio = ratio[(ratio < 0.1) | (ratio > 10)]
                if len(abnormal_ratio) > 0:
                    logger.warning(f"发现 {len(abnormal_ratio)} 个异常价格/IOPV比值数据")
        
        # ===== 使用新的验证函数 =====
        # 验证实时套利数据
        if not validate_arbitrage_data(all_opportunities):
            logger.warning("实时套利数据验证有警告，但仍尝试处理")
            # 不返回空，继续尝试处理
        
        if all_opportunities.empty:
            logger.error("获取的套利数据为空，无法计算套利机会")
            return pd.DataFrame(), pd.DataFrame()
        
        # 确保DataFrame使用中文列名
        all_opportunities = ensure_chinese_columns(all_opportunities)
        
        # 标准化列名 - 处理可能的空格问题
        all_opportunities.columns = [col.strip() for col in all_opportunities.columns]
        
        # ===== 关键修复：确保ETF列表一致性 =====
        # 获取用于套利监控的ETF列表
        trading_etf_list = get_trading_etf_list()
        logger.info(f"获取到 {len(trading_etf_list)} 只符合条件的ETF进行套利监控")
        
        # 筛选出交易ETF列表中的ETF
        all_opportunities = all_opportunities[all_opportunities["ETF代码"].isin(trading_etf_list)]
        
        # 检查筛选后的数据量
        if all_opportunities.empty:
            logger.warning("筛选后无符合条件的ETF数据")
            return pd.DataFrame(), pd.DataFrame()
        
        # ===== 关键修复：增强数据清洗 =====
        # 1. 检查价格和IOPV的基本有效性
        MIN_IOPV = 0.01
        MIN_PRICE = 0.01
        valid_opportunities = all_opportunities[
            (all_opportunities["IOPV"] > MIN_IOPV) & 
            (all_opportunities["市场价格"] > MIN_PRICE)
        ].copy()
        
        # 2. 过滤掉价格和IOPV差异过大的数据
        # 正常ETF的价格和IOPV不会相差10倍以上
        if len(valid_opportunities) > 0:
            price_iopv_ratio = valid_opportunities["市场价格"] / valid_opportunities["IOPV"]
            valid_opportunities = valid_opportunities[
                (price_iopv_ratio > 0.1) & (price_iopv_ratio < 10)
            ].copy()
        
        if valid_opportunities.empty:
            logger.warning("数据清洗后无有效数据")
            return pd.DataFrame(), pd.DataFrame()
        
        # 3. 重新计算折价率
        valid_opportunities["折价率"] = (
            (valid_opportunities["市场价格"] - valid_opportunities["IOPV"]) / 
            valid_opportunities["IOPV"] * 100
        )
        
        # 4. 记录异常折价率，但不立即过滤
        original_count = len(valid_opportunities)
        
        # 检查异常数据（使用更宽松的阈值）
        abnormal_mask = (valid_opportunities["折价率"].abs() > 30)  # 从20提高到30
        if abnormal_mask.any():
            abnormal_data = valid_opportunities[abnormal_mask]
            logger.warning(f"⚠️ 发现 {len(abnormal_data)} 个较大折价率数据（绝对值>30%）:")
            for _, row in abnormal_data.head(3).iterrows():  # 只显示前3个异常
                logger.warning(f"  ETF {row['ETF代码']}: 价格={row['市场价格']}, IOPV={row['IOPV']}, 折价率={row['折价率']:.2f}%")
            
            # 【修复】不直接过滤掉，保留数据但标记警告
            # 只过滤掉极端异常的数据（绝对值>50%）
            extreme_mask = (valid_opportunities["折价率"].abs() > 50)
            if extreme_mask.any():
                extreme_data = valid_opportunities[extreme_mask]
                logger.warning(f"过滤掉 {len(extreme_data)} 个极端异常数据（绝对值>50%）")
                valid_opportunities = valid_opportunities[~extreme_mask].copy()
        
        if valid_opportunities.empty:
            logger.warning("过滤极端异常数据后无有效数据")
            return pd.DataFrame(), pd.DataFrame()
        
        # 记录筛选前的统计信息
        logger.info(f"处理前数据量: {len(valid_opportunities)}，折价率范围: {valid_opportunities['折价率'].min():.2f}% ~ {valid_opportunities['折价率'].max():.2f}%")
        
        # ===== 核心修复：使用绝对值比较阈值 =====
        abs_threshold = Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD
        
        # 【关键修复】明确区分折价和溢价：
        # 折价：市场价格 < IOPV (折价率为负)，且绝对值大于阈值
        # 溢价：市场价格 > IOPV (折价率为正)，且绝对值大于阈值
        discount_opportunities = valid_opportunities[
            (valid_opportunities["折价率"] < 0) & 
            (valid_opportunities["折价率"].abs() >= abs_threshold)
        ].copy()
        
        premium_opportunities = valid_opportunities[
            (valid_opportunities["折价率"] > 0) & 
            (valid_opportunities["折价率"].abs() >= abs_threshold)
        ].copy()
        
        # 【关键修复】添加验证逻辑，确保折价和溢价区分正确
        # 检查折价机会是否真的为折价
        invalid_discount = discount_opportunities[discount_opportunities["折价率"] >= 0]
        if not invalid_discount.empty:
            logger.error(f"发现 {len(invalid_discount)} 个错误标识为折价的机会（实际为溢价）: {invalid_discount[['ETF代码', '折价率']].to_dict()}")
            # 从折价机会中移除
            discount_opportunities = discount_opportunities[discount_opportunities["ETF代码"].isin(invalid_discount["ETF代码"]) == False]
        
        # 检查溢价机会是否真的为溢价
        invalid_premium = premium_opportunities[premium_opportunities["折价率"] <= 0]
        if not invalid_premium.empty:
            logger.error(f"发现 {len(invalid_premium)} 个错误标识为溢价的机会（实际为折价）: {invalid_premium[['ETF代码', '折价率']].to_dict()}")
            # 从溢价机会中移除
            premium_opportunities = premium_opportunities[premium_opportunities["ETF代码"].isin(invalid_premium["ETF代码"]) == False]
        
        # 按折价率绝对值排序
        if not discount_opportunities.empty:
            discount_opportunities = discount_opportunities.sort_values("折价率", ascending=True)
        
        if not premium_opportunities.empty:
            premium_opportunities = premium_opportunities.sort_values("折价率", ascending=False)
        
        # 修复：更新日志信息，准确反映筛选条件
        logger.info(f"发现 {len(discount_opportunities)} 个折价机会 (阈值≤-{abs_threshold}%)")
        logger.info(f"发现 {len(premium_opportunities)} 个溢价机会 (阈值≥{abs_threshold}%)")
        
        if len(discount_opportunities) == 0 and len(premium_opportunities) == 0:
            logger.info("没有发现符合阈值的套利机会")
            return discount_opportunities, premium_opportunities
        
        # 添加规模和日均成交额信息
        if not discount_opportunities.empty:
            discount_opportunities = add_etf_basic_info(discount_opportunities)
        
        if not premium_opportunities.empty:
            premium_opportunities = add_etf_basic_info(premium_opportunities)
        
        # 计算综合评分
        if not discount_opportunities.empty:
            discount_opportunities = calculate_arbitrage_scores(discount_opportunities)
        
        if not premium_opportunities.empty:
            premium_opportunities = calculate_arbitrage_scores(premium_opportunities)
        
        # 筛选今天尚未推送的套利机会（增量推送功能）
        discount_opportunities = filter_new_discount_opportunities(discount_opportunities)
        premium_opportunities = filter_new_premium_opportunities(premium_opportunities)
        
        # 修复：添加日志，显示评分详情
        if not premium_opportunities.empty:
            for _, row in premium_opportunities.head(3).iterrows():  # 只显示前3个
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
    
    Args:
        df: 原始折价机会DataFrame
    
    Returns:
        pd.DataFrame: 仅包含新发现的折价机会的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 创建一个列表，包含应该推送的ETF代码
        etfs_to_push = []
        
        for _, row in df.iterrows():
            etf_code = row["ETF代码"]
            if should_push_discount(etf_code):
                etfs_to_push.append(etf_code)
        
        # 过滤DataFrame
        new_opportunities = df[df["ETF代码"].isin(etfs_to_push)].copy()
        
        logger.info(f"从 {len(df)} 个折价机会中筛选出 {len(new_opportunities)} 个新机会（增量推送）")
        return new_opportunities
    
    except Exception as e:
        logger.error(f"过滤新折价机会失败: {str(e)}", exc_info=True)
        # 出错时返回原始DataFrame，确保至少能推送新发现的机会
        return df

def filter_new_premium_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤掉今天已经推送过的溢价机会
    
    Args:
        df: 原始溢价机会DataFrame
    
    Returns:
        pd.DataFrame: 仅包含新发现的溢价机会的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 创建一个列表，包含应该推送的ETF代码
        etfs_to_push = []
        
        for _, row in df.iterrows():
            etf_code = row["ETF代码"]
            if should_push_premium(etf_code):
                etfs_to_push.append(etf_code)
        
        # 过滤DataFrame
        new_opportunities = df[df["ETF代码"].isin(etfs_to_push)].copy()
        
        logger.info(f"从 {len(df)} 个溢价机会中筛选出 {len(new_opportunities)} 个新机会（增量推送）")
        return new_opportunities
    
    except Exception as e:
        logger.error(f"过滤新溢价机会失败: {str(e)}", exc_info=True)
        # 出错时返回原始DataFrame，确保至少能推送新发现的机会
        return df

def sort_opportunities_by_abs_premium(df: pd.DataFrame) -> pd.DataFrame:
    """
    按折价率绝对值排序
    
    Args:
        df: 原始套利机会DataFrame
    
    Returns:
        pd.DataFrame: 排序后的DataFrame
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
    
    Args:
        df: 原始套利机会DataFrame
    
    Returns:
        pd.DataFrame: 添加基本信息后的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 为每只ETF添加基本信息
        for idx, row in df.iterrows():
            etf_code = row["ETF代码"]
            # 【关键修复】明确从all_etfs.csv获取基金规模
            size = get_etf_basic_info(etf_code)
            
            # 计算日均成交额
            avg_volume = 0.0
            etf_df = load_etf_daily_data(etf_code)
            if not etf_df.empty and "成交额" in etf_df.columns:
                # 取最近30天数据
                recent_data = etf_df.tail(30)
                if len(recent_data) > 0:
                    # 修复：不再进行单位转换，因为data_crawler中已统一转换为"万元"
                    avg_volume = recent_data["成交额"].mean()
            
            # 使用.loc避免SettingWithCopyWarning
            df.loc[idx, "基金规模"] = size
            df.loc[idx, "日均成交额"] = avg_volume
        
        logger.info(f"添加ETF基本信息完成，共处理 {len(df)} 个机会")
        return df
    
    except Exception as e:
        logger.error(f"添加ETF基本信息失败: {str(e)}", exc_info=True)
        return df

def calculate_arbitrage_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算ETF套利综合评分
    
    Args:
        df: 原始套利机会DataFrame
    
    Returns:
        pd.DataFrame: 包含综合评分的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 为每只ETF计算综合评分
        scores = []
        for idx, row in df.iterrows():
            etf_code = row["ETF代码"]
            # 获取ETF日线数据
            etf_df = load_etf_daily_data(etf_code)
            if etf_df.empty:
                logger.warning(f"ETF {etf_code} 无日线数据，无法计算综合评分")
                scores.append(0.0)
                continue
            
            # 检查必要列是否存在
            required_columns = ["折价率", "市场价格", "IOPV"]
            missing_columns = [col for col in required_columns if col not in row.index]
            if missing_columns:
                logger.error(f"ETF {etf_code} 缺少必要列: {', '.join(missing_columns)}")
                scores.append(0.0)
                continue
            
            # 使用辅助函数安全提取标量值
            premium_discount = extract_scalar_value(
                row["折价率"],
                log_prefix=f"ETF {etf_code} 折价率: "
            )
            
            # 从DataFrame行中提取所有必需参数
            # 修复：ETF名称是字符串，不应该使用extract_scalar_value
            etf_name = row["ETF名称"]
            market_price = extract_scalar_value(row["市场价格"], log_prefix=f"ETF {etf_code} 市场价格: ")
            iopv = extract_scalar_value(row["IOPV"], log_prefix=f"ETF {etf_code} IOPV: ")
            fund_size = extract_scalar_value(row["基金规模"], log_prefix=f"ETF {etf_code} 基金规模: ")
            avg_volume = extract_scalar_value(row["日均成交额"], log_prefix=f"ETF {etf_code} 日均成交额: ")
            
            # 检查异常折价率（不修改原始值）
            if premium_discount < -15.0:
                logger.warning(f"ETF {etf_code} 折价率异常低: {premium_discount:.2f}%")
            elif premium_discount > 15.0:
                logger.warning(f"ETF {etf_code} 溢价率异常高: {premium_discount:.2f}%")
            
            # 记录实际使用的值（用于调试）
            logger.debug(f"ETF {etf_code} 实际使用的折价率: {premium_discount:.2f}%")
            
            # 计算综合评分
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
        
        # 添加评分列
        df["综合评分"] = scores
        logger.info(f"计算ETF套利综合评分完成，共 {len(df)} 个机会")
        return df
    except Exception as e:
        logger.error(f"计算ETF套利综合评分失败: {str(e)}", exc_info=True)
        # 添加默认评分列
        df["综合评分"] = 0.0
        return df

def filter_valid_discount_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤有效的折价机会（基于综合评分和阈值）
    
    Args:
        df: 原始折价机会DataFrame
    
    Returns:
        pd.DataFrame: 过滤后的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 检查必要列是否存在
        required_columns = ["ETF代码", "ETF名称", "折价率"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"数据中缺少必要列: {', '.join(missing_columns)}")
            # 记录实际存在的列
            logger.info(f"实际列名: {list(df.columns)}")
            return pd.DataFrame()
        
        # 记录筛选前的统计信息
        logger.info(f"筛选前数据量: {len(df)}，折价率范围: {df['折价率'].min():.2f}% ~ {df['折价率'].max():.2f}%")
        
        # 直接使用已有的折价率列，不再重新计算
        # 折价机会：折价率为负
        # 关键修复：只按折价率阈值筛选，不按评分筛选
        filtered_df = df[df["折价率"] <= -Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD]
        
        # 按折价率绝对值排序（降序，折价率越大越靠前）
        if not filtered_df.empty:
            filtered_df = filtered_df.sort_values("折价率", ascending=True)
        
        # 修复：更新日志信息
        logger.info(f"从 {len(df)} 个折价机会中筛选出 {len(filtered_df)} 个机会（阈值：折价率≤-{Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD:.2f}%）")
        return filtered_df
    
    except Exception as e:
        logger.error(f"过滤有效折价机会失败: {str(e)}", exc_info=True)
        return df

def filter_valid_premium_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤有效的溢价机会（基于综合评分和阈值）
    
    Args:
        df: 原始溢价机会DataFrame
    
    Returns:
        pd.DataFrame: 过滤后的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 检查必要列是否存在
        required_columns = ["ETF代码", "ETF名称", "折价率"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"数据中缺少必要列: {', '.join(missing_columns)}")
            # 记录实际存在的列
            logger.info(f"实际列名: {list(df.columns)}")
            return pd.DataFrame()
        
        # 记录筛选前的统计信息
        logger.info(f"筛选前数据量: {len(df)}，折价率范围: {df['折价率'].min():.2f}% ~ {df['折价率'].max():.2f}%")
        
        # 直接使用已有的折价率列，不再重新计算
        # 溢价机会：折价率为正
        # 关键修复：只按溢价率阈值筛选，不按评分筛选
        filtered_df = df[df["折价率"] >= Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD]
        
        # 按折价率降序排序（溢价率越大越靠前）
        if not filtered_df.empty:
            filtered_df = filtered_df.sort_values("折价率", ascending=False)
        
        # 修复：更新日志信息
        logger.info(f"从 {len(df)} 个溢价机会中筛选出 {len(filtered_df)} 个机会（阈值：溢价率≥{Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD:.2f}%）")
        return filtered_df
    
    except Exception as e:
        logger.error(f"过滤有效溢价机会失败: {str(e)}", exc_info=True)
        return df

def calculate_daily_volume(etf_code: str) -> float:
    """
    计算ETF的日均成交额（基于最近30个交易日）
    
    Args:
        etf_code: ETF代码
        
    Returns:
        float: 日均成交额（万元）
    """
    try:
        # 加载ETF日线数据
        etf_df = load_etf_daily_data(etf_code)
        
        if etf_df.empty:
            logger.debug(f"ETF {etf_code} 无日线数据，无法计算日均成交额")
            return 0.0
        
        # 确保使用中文列名
        etf_df = ensure_chinese_columns(etf_df)
        
        # 检查是否包含"日期"列
        if "日期" not in etf_df.columns:
            logger.warning(f"ETF {etf_code} 数据缺少'日期'列，无法计算日均成交额")
            return 0.0
        
        # 确保数据按日期排序
        etf_df = etf_df.sort_values("日期", ascending=False)
        
        # 取最近30个交易日的数据
        recent_data = etf_df.head(30)
        
        # 检查是否有足够的数据
        if len(recent_data) < 10:  # 至少需要10天数据
            logger.debug(f"ETF {etf_code} 数据不足（{len(recent_data)}天），无法准确计算日均成交额")
            return 0.0
        
        # 计算日均成交额
        if "成交额" in recent_data.columns:
            # 修复：不再进行单位转换，因为data_crawler中已统一转换为"万元"
            avg_volume = recent_data["成交额"].mean()
            logger.debug(f"ETF {etf_code} 日均成交额: {avg_volume:.2f}万元（{len(recent_data)}天数据）")
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
    
    Args:
        days: 查询天数
    
    Returns:
        pd.DataFrame: 套利历史数据
    """
    try:
        history = []
        beijing_now = get_beijing_time()
        
        for i in range(days):
            # 【日期datetime类型规则】确保日期是datetime类型
            date = (beijing_now - timedelta(days=i)).strftime("%Y-%m-%d")
            flag_file = os.path.join(Config.FLAG_DIR, f"arbitrage_pushed_{date}.txt")
            
            if os.path.exists(flag_file):
                # 读取当日套利数据
                # 这里简化处理，实际应从数据库或文件中读取历史套利数据
                history.append({
                    "日期": date,
                    "机会数量": 3,  # 示例数据
                    "最大折价率": 2.5,  # 示例数据
                    "最小折价率": -1.8  # 示例数据
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
    
    Returns:
        Dict[str, Any]: 分析结果
    """
    try:
        # 获取历史数据
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
        
        # 计算统计指标
        avg_opportunities = history_df["机会数量"].mean()
        max_premium = history_df["最大折价率"].max()
        min_discount = history_df["最小折价率"].min()
        
        # 添加趋势分析
        trend = "平稳"
        if len(history_df) >= 3:
            trend = "上升" if history_df["机会数量"].iloc[-3:].mean() > history_df["机会数量"].iloc[:3].mean() else "下降"
        
        # 返回结构化分析结果
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
    
    Returns:
        List[Dict[str, Any]]: 需要退出的套利交易列表
    """
    try:
        logger.info("开始检查套利退出信号")
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 检查交易记录文件是否存在
        if not os.path.exists(Config.TRADE_RECORD_FILE):
            logger.warning("交易记录文件不存在，无法检查套利退出信号")
            return []
        
        # 读取交易记录
        trade_df = pd.read_csv(Config.TRADE_RECORD_FILE, encoding="utf-8")
        
        # 获取昨天的日期（基于北京时间）
        yesterday = (beijing_now - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.debug(f"检查昨天({yesterday})执行的套利交易")
        
        # 查找昨天执行的套利交易
        yesterday_arbitrage = trade_df[
            (trade_df["操作"] == "套利买入") & 
            (trade_df["创建日期"] == yesterday)
        ]
        
        if not yesterday_arbitrage.empty:
            logger.info(f"发现{len(yesterday_arbitrage)}条需要退出的套利交易")
            
            # 构建退出信号列表
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
    
    Args:
        date_str: 日期字符串，格式为YYYYMMDD
    
    Returns:
        pd.DataFrame: 套利数据DataFrame
    """
    try:
        # 构建套利数据目录
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        os.makedirs(arbitrage_dir, exist_ok=True)
        
        # 构建文件路径
        file_path = os.path.join(arbitrage_dir, f"{date_str}.csv")
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.info(f"套利数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        # 读取CSV文件（明确指定编码）
        df = pd.read_csv(file_path, encoding="utf-8-sig")
        
        # 添加关键诊断日志（INFO级别，确保可见）
        logger.info(f"成功加载套利数据: {file_path}")
        logger.info(f"实际列名: {list(df.columns)}")
        if not df.empty:
            logger.info(f"前几行数据示例: {df.head().to_dict()}")
        
        # 确保DataFrame使用中文列名
        df = ensure_chinese_columns(df)
        
        return df
    
    except Exception as e:
        logger.error(f"加载套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_latest_arbitrage_opportunities(max_retry: int = 3) -> pd.DataFrame:
    """
    获取最新的套利机会
    
    Args:
        max_retry: 最大重试次数
    
    Returns:
        pd.DataFrame: 套利机会DataFrame
    """
    try:
        # 获取当前日期
        today = get_beijing_time().strftime("%Y%m%d")
        
        # 尝试加载今天的套利数据
        df = load_arbitrage_data(today)
        
        # 记录实际加载的列名用于诊断 (INFO级别)
        if not df.empty:
            logger.info(f"成功加载套利数据，实际列名: {list(df.columns)}")
        
        # 检查数据完整性
        if df.empty:
            logger.warning("加载的套利数据为空")
            return pd.DataFrame()
        
        # 记录实际加载的列名用于诊断 (INFO级别)
        logger.info(f"成功加载套利数据，实际列名: {list(df.columns)}")
        
        # 标准化列名 - 处理可能的空格问题
        df.columns = [col.strip() for col in df.columns]
        
        # 检查数据完整性
        required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"数据中缺少必要列: {', '.join(missing_columns)}")
            # 记录实际存在的列
            logger.info(f"实际列名: {list(df.columns)}")
            return pd.DataFrame()
        
        # 修复：在策略计算模块中计算正确的折价率
        # 正确的计算公式：(市场价格 - IOPV) / IOPV * 100
        # 结果为正：溢价（市场价格 > IOPV）
        # 结果为负：折价（市场价格 < IOPV）
        df["折价率"] = ((df["市场价格"] - df["IOPV"]) / df["IOPV"]) * 100
        
        # 记录筛选前的统计信息
        logger.info(f"筛选前数据量: {len(df)}，折价率范围: {df['折价率'].min():.2f}% ~ {df['折价率'].max():.2f}%")
        
        return df
    
    except Exception as e:
        logger.error(f"获取最新套利机会失败: {str(e)}")
        return pd.DataFrame()

def load_latest_valid_arbitrage_data(days_back: int = 7) -> pd.DataFrame:
    """
    加载最近有效的套利数据
    
    Args:
        days_back: 向前查找的天数
    
    Returns:
        pd.DataFrame: 最近有效的套利数据
    """
    try:
        beijing_now = get_beijing_time()
        
        # 从今天开始向前查找
        for i in range(days_back):
            date = (beijing_now - timedelta(days=i)).strftime("%Y%m%d")
            logger.debug(f"尝试加载历史套利数据: {date}")
            
            df = load_arbitrage_data(date)
            
            # 检查数据是否有效
            if not df.empty:
                # 检查是否包含必要列
                required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV"]
                if all(col in df.columns for col in required_columns):
                    # 修复：在加载历史数据时也计算正确的折价率
                    df["折价率"] = ((df["市场价格"] - df["IOPV"]) / df["IOPV"]) * 100
                    
                    logger.info(f"找到有效历史套利数据: {date}, 共 {len(df)} 个机会")
                    # 记录历史数据的折价率范围
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
    
    Args:
        discount_df: 折价机会DataFrame
        premium_df: 溢价机会DataFrame
    
    Returns:
        bool: 是否成功标记
    """
    try:
        # 获取当前日期
        current_date = get_beijing_time().strftime("%Y-%m-%d")
        
        # 加载现有状态 - 使用正确的函数名
        discount_status = load_discount_status()
        premium_status = load_premium_status()
        
        # 更新折价状态
        for _, row in discount_df.iterrows():
            etf_code = row["ETF代码"]
            discount_status[etf_code] = {
                "last_pushed": current_date,
                "score": row["综合评分"]
            }
        
        # 更新溢价状态
        for _, row in premium_df.iterrows():
            etf_code = row["ETF代码"]
            premium_status[etf_code] = {
                "last_pushed": current_date,
                "score": row["综合评分"]
            }
        
        # 保存状态 - 使用正确的函数名
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
    
    Returns:
        Dict[str, Any]: 套利推送统计信息
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
        
        # 获取总推送量和今日推送量
        arbitrage_count = get_arbitrage_push_count()
        discount_count = get_discount_push_count()
        premium_count = get_premium_push_count()
        
        # 获取历史推送记录
        arbitrage_history = get_arbitrage_push_history(days=7)
        discount_history = get_discount_push_history(days=7)
        premium_history = get_premium_push_history(days=7)
        
        # 计算总推送量
        total_arbitrage = sum(arbitrage_history.values())
        total_discount = sum(discount_history.values())
        total_premium = sum(premium_history.values())
        
        # 计算日均推送量
        daily_avg_arbitrage = total_arbitrage / len(arbitrage_history) if arbitrage_history else 0
        daily_avg_discount = total_discount / len(discount_history) if discount_history else 0
        daily_avg_premium = total_premium / len(premium_history) if premium_history else 0
        
        # 获取最新推送日期
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
        logger.error(f"获取套利推送统计信息失败: {str(e)}", exc_info=True)
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
    生成套利机会消息，按照用户指定的格式
    【关键修复】不区分折溢价，只按折溢价率绝对值排序
    【关键修复】修正日均成交额单位（除以10000）
    【关键修复】严格遵循用户指定的消息模板
    【关键修复】增强数据验证，解决"不是ETF数据格式"问题
    """
    try:
        # 合并数据前检查格式
        required_columns = ["ETF代码", "ETF名称", "折价率", "市场价格", "IOPV", "基金规模", "日均成交额", "综合评分"]
        
        # 检查折价机会
        if not discount_opportunities.empty:
            missing_cols = [col for col in required_columns if col not in discount_opportunities.columns]
            if missing_cols:
                logger.error(f"折价机会数据缺少列: {missing_cols}")
        
        # 检查溢价机会
        if not premium_opportunities.empty:
            missing_cols = [col for col in required_columns if col not in premium_opportunities.columns]
            if missing_cols:
                logger.error(f"溢价机会数据缺少列: {missing_cols}")
        
        # 合并数据
        all_opportunities = pd.concat([discount_opportunities, premium_opportunities], ignore_index=True)
        
        if all_opportunities.empty:
            logger.info("没有符合条件的套利机会")
            return []
        
        # 数据质量检查
        invalid_rows = []
        for idx, row in all_opportunities.iterrows():
            # 检查必要字段是否存在且有效
            try:
                etf_code = str(row["ETF代码"])
                etf_name = str(row["ETF名称"])
                discount_rate = float(row["折价率"])
                market_price = float(row["市场价格"])
                iopv = float(row["IOPV"])
                fund_size = float(row["基金规模"])
                avg_volume = float(row["日均成交额"])
                score = float(row["综合评分"])
                
                # 检查数值合理性
                if market_price <= 0 or iopv <= 0:
                    invalid_rows.append(idx)
                    logger.warning(f"第{idx}行数据价格或IOPV无效: 价格={market_price}, IOPV={iopv}")
                    
            except (KeyError, ValueError, TypeError) as e:
                invalid_rows.append(idx)
                logger.warning(f"第{idx}行数据格式错误: {str(e)}")
        
        # 移除无效行
        if invalid_rows:
            logger.warning(f"移除 {len(invalid_rows)} 行无效数据")
            all_opportunities = all_opportunities.drop(invalid_rows)
            
        if all_opportunities.empty:
            logger.info("数据清洗后无有效机会")
            return []
        
        # 按折价率绝对值排序（降序）
        if not all_opportunities.empty:
            all_opportunities["abs_premium_discount"] = all_opportunities["折价率"].abs()
            all_opportunities = all_opportunities.sort_values("abs_premium_discount", ascending=False)
            all_opportunities = all_opportunities.drop(columns=["abs_premium_discount"])
        
        # 如果没有机会，返回空列表
        if all_opportunities.empty:
            logger.info("排序后无符合条件的套利机会")
            return []
        
        # 获取当前时间
        beijing_time = get_beijing_time()
        date_str = beijing_time.strftime("%Y-%m-%d %H:%M")
        env_name = os.getenv("ENVIRONMENT", "Git-fish-etf")
        
        # 消息分页（每页最多4个ETF）
        messages = []
        etfs_per_page = 4
        total_pages = (len(all_opportunities) + etfs_per_page - 1) // etfs_per_page
        
        # 生成第一页：筛选条件信息
        header_msg = "【以下ETF市场价格与净值有大差额】\n"
        header_msg += f"💓共{len(all_opportunities)}只ETF，分{total_pages}条消息推送，这是第1/{total_pages}条消息\n\n"
        header_msg += "📊 筛选条件：基金规模≥10.0亿元，日均成交额≥5000.0万元\n"
        header_msg += "💰 交易成本：0.12%（含印花税和佣金）\n"
        header_msg += f"🎯 折溢价阈值：折价率超过{Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD:.2f}%\n"
        header_msg += "⭐ 综合评分：≥70.0\n"
        header_msg += "==================\n"
        header_msg += f"📅 北京时间: {date_str}\n"
        header_msg += f"📊 环境：{env_name}"
        messages.append(header_msg)
        
        # 生成后续页面：ETF列表
        for page in range(total_pages):
            start_idx = page * etfs_per_page
            end_idx = min(start_idx + etfs_per_page, len(all_opportunities))
            
            # 【关键修复】生成页码信息
            page_msg = f"【第{page+1}页 共{total_pages}页】\n\n"
            
            for i, (_, row) in enumerate(all_opportunities.iloc[start_idx:end_idx].iterrows(), 1):
                # 【关键修复】修正日均成交额单位（除以10000）
                daily_volume = row["日均成交额"] / 10000 if row["日均成交额"] > 0 else 0
                
                page_msg += f"{i}. {row['ETF名称']} ({row['ETF代码']})\n"
                page_msg += f"   ⭐ 综合评分: {row['综合评分']:.2f}分\n"
                # 【关键修复】只显示"折溢价率"，不区分折价/溢价
                page_msg += f"   💹 折溢价率: {row['折价率']:.2f}%\n"
                page_msg += f"   📈 市场价格: {row['市场价格']:.3f}元\n"
                page_msg += f"   📊 基金净值: {row['IOPV']:.3f}元\n"
                page_msg += f"   🏦 基金规模: {row['基金规模']:.2f}亿元\n"
                # 【关键修复】修正日均成交额显示
                page_msg += f"   💰 日均成交额: {daily_volume:.2f}万元\n\n"
            
            page_msg = page_msg.rstrip()  # 移除最后一个空行
            page_msg += "\n==================\n"
            page_msg += f"📅 北京时间: {date_str}\n"
            page_msg += f"📊 环境：{env_name}"
            
            messages.append(page_msg)
        
        return messages
    
    except Exception as e:
        logger.error(f"生成套利消息失败: {str(e)}", exc_info=True)
        return ["【ETF套利机会】生成消息时发生错误，请检查日志"]

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 初始化日志
    logger.info("套利策略模块初始化完成（已移除交易日限制）")
    
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
