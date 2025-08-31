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

# 从Config中获取标准列名
ETF_CODE_COL = Config.ETF_STANDARD_COLUMNS[0]  # "ETF代码"
ETF_NAME_COL = Config.ETF_STANDARD_COLUMNS[1]  # "ETF名称"
FUND_SIZE_COL = Config.ETF_STANDARD_COLUMNS[3]  # "基金规模"
LISTING_DATE_COL = "成立日期"  # 成立日期列名（未在ETF_STANDARD_COLUMNS中定义）
DATE_COL = Config.COLUMN_NAME_MAPPING["date"]
CLOSE_COL = Config.COLUMN_NAME_MAPPING["close"]
AMOUNT_COL = Config.COLUMN_NAME_MAPPING["amount"]
VOLUME_COL = Config.COLUMN_NAME_MAPPING["volume"]

def get_top_rated_etfs(top_n: Optional[int] = None, min_score: float = 60, position_type: str = "稳健仓") -> pd.DataFrame:
    """
    从全市场ETF中筛选高分ETF
    
    Args:
        top_n: 返回前N名，为None则返回所有高于min_score的ETF
        min_score: 最低评分阈值
        position_type: 仓位类型（"稳健仓"或"激进仓"）
    
    Returns:
        pd.DataFrame: 包含ETF代码、名称、评分等信息的DataFrame
    """
    try:
        # 获取仓位类型对应的筛选参数
        params = Config.STRATEGY_PARAMETERS.get(position_type, Config.STRATEGY_PARAMETERS["稳健仓"])
        min_fund_size = params["min_fund_size"]
        min_avg_volume = params["min_avg_volume"]
        
        # 获取元数据
        metadata_df = load_etf_metadata()
        
        # 检查元数据是否有效
        if metadata_df is None or not isinstance(metadata_df, pd.DataFrame) or metadata_df.empty:
            # 检查元数据文件是否存在
            metadata_path = Config.METADATA_PATH
            if not os.path.exists(metadata_path):
                logger.warning("ETF元数据文件不存在，尝试从本地数据重建...")
                rebuild_etf_metadata()
            else:
                logger.warning("ETF元数据文件存在但格式错误，尝试修复...")
                if repair_etf_metadata(metadata_path):
                    metadata_df = load_etf_metadata()
                else:
                    logger.warning("ETF元数据修复失败，尝试重建...")
                    rebuild_etf_metadata()
                    metadata_df = load_etf_metadata()
            
            # 再次检查元数据是否有效
            if metadata_df is None or not isinstance(metadata_df, pd.DataFrame) or metadata_df.empty:
                # 最后一次尝试：使用基础ETF列表
                logger.warning("ETF元数据重建失败，尝试使用基础ETF列表...")
                metadata_df = create_basic_metadata_from_list()
                
                if metadata_df is None or metadata_df.empty:
                    error_msg = "ETF元数据重建失败，无法获取ETF列表"
                    logger.error(error_msg)
                    
                    # 发送错误通知
                    send_wechat_message(
                        message=error_msg,
                        message_type="error"
                    )
                    
                    return pd.DataFrame()
        
        # 确保列名正确（修复CSV文件列名问题）
        if "etf_code" not in metadata_df.columns:
            # 如果列名是中文，尝试映射
            if ETF_CODE_COL in metadata_df.columns:
                metadata_df = metadata_df.rename(columns={ETF_CODE_COL: "etf_code"})
            elif "etf_code" not in metadata_df.columns:
                error_msg = f"ETF元数据缺少必要列: {ETF_CODE_COL} (映射为 etf_code)"
                logger.warning(error_msg)
                send_wechat_message(
                    message=error_msg,
                    message_type="error"
                )
                return pd.DataFrame()
        
        # 获取所有ETF代码
        all_codes = metadata_df["etf_code"].tolist()
        if not all_codes:
            error_msg = "元数据中无ETF代码"
            logger.warning(error_msg)
            
            # 发送错误通知
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
            
            return pd.DataFrame()
        
        # 计算评分
        score_list = []
        logger.info(f"开始计算 {len(all_codes)} 只ETF的综合评分...")
        
        for etf_code in all_codes:
            try:
                # 获取ETF日线数据（从本地文件加载）
                df = load_etf_daily_data(etf_code)
                if df.empty:
                    logger.debug(f"ETF {etf_code} 无日线数据，跳过评分")
                    continue
                
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
                        avg_volume = recent_30d[AMOUNT_COL].mean() / 10000  # 转换为万元
                
                # 应用动态筛选参数
                if size >= min_fund_size and avg_volume >= min_avg_volume:
                    score_list.append({
                        "etf_code": etf_code,
                        "etf_name": etf_name,
                        "score": score,
                        "size": size,
                        "listing_date": listing_date,
                        "avg_volume": avg_volume
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
        score_df = pd.DataFrame(score_list).sort_values("score", ascending=False)
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

def rebuild_etf_metadata():
    """
    从本地数据重建ETF元数据
    """
    try:
        logger.info("开始从本地数据重建ETF元数据...")
        
        # 获取所有ETF代码
        etf_list = load_all_etf_list()
        if etf_list.empty:
            logger.warning("ETF列表为空，无法重建元数据")
            return False
        
        # 初始化元数据列表
        metadata_list = []
        
        # 遍历所有ETF，从本地日线数据计算元数据
        for _, etf in etf_list.iterrows():
            etf_code = etf[ETF_CODE_COL]
            
            # 获取ETF日线数据（从本地文件加载）
            df = load_etf_daily_data(etf_code)
            if df.empty:
                logger.debug(f"ETF {etf_code} 无日线数据，跳过元数据重建")
                continue
            
            # 计算波动率
            volatility = calculate_volatility(df)
            
            # 从ETF列表获取规模和成立日期
            size = etf[FUND_SIZE_COL] if FUND_SIZE_COL in etf else 0.0
            listing_date = etf[LISTING_DATE_COL] if LISTING_DATE_COL in etf else ""
            
            # 添加元数据
            metadata_list.append({
                "etf_code": etf_code,
                "etf_name": etf[ETF_NAME_COL],
                "volatility": volatility,
                "size": size,
                "listing_date": listing_date,
                "update_time": get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        if not metadata_list:
            logger.warning("没有有效的ETF元数据可重建")
            return False
        
        # 创建DataFrame
        metadata_df = pd.DataFrame(metadata_list)
        
        # 保存元数据
        metadata_path = Config.METADATA_PATH
        metadata_df.to_csv(metadata_path, index=False, encoding="utf-8-sig")
        logger.info(f"ETF元数据已重建，共{len(metadata_df)}条记录，保存至: {metadata_path}")
        return True
    
    except Exception as e:
        error_msg = f"重建ETF元数据失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return False

def repair_etf_metadata(file_path: str) -> bool:
    """
    尝试修复损坏的ETF元数据文件
    
    Args:
        file_path: 元数据文件路径
    
    Returns:
        bool: 修复成功返回True，否则返回False
    """
    try:
        logger.info(f"尝试修复ETF元数据文件: {file_path}")
        
        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 检查是否是有效的CSV
        if ',' not in content[:100]:  # 检查前100字符是否有逗号
            logger.warning("元数据文件格式异常，可能是JSON或损坏的CSV")
            return False
        
        # 检查列名是否正确
        metadata_df = pd.read_csv(file_path, encoding="utf-8")
        if "etf_code" not in metadata_df.columns and ETF_CODE_COL in metadata_df.columns:
            metadata_df = metadata_df.rename(columns={ETF_CODE_COL: "etf_code"})
            metadata_df.to_csv(file_path, index=False, encoding="utf-8-sig")
            logger.info("成功修复元数据文件列名")
            return True
        
        return False
    
    except Exception as e:
        logger.error(f"修复ETF元数据失败: {str(e)}", exc_info=True)
        return False

def create_basic_metadata_from_list() -> pd.DataFrame:
    """
    从ETF列表创建基础ETF元数据
    
    Returns:
        pd.DataFrame: 基础ETF元数据
    """
    try:
        logger.info("从ETF列表创建基础ETF元数据...")
        
        # 获取ETF列表
        etf_list = load_all_etf_list()
        if etf_list.empty:
            logger.warning("ETF列表为空，无法创建基础元数据")
            return pd.DataFrame()
        
        # 创建基础元数据
        metadata_list = []
        for _, etf in etf_list.iterrows():
            # 处理规模
            size = 0.0
            if FUND_SIZE_COL in etf:
                size_str = etf[FUND_SIZE_COL]
                if isinstance(size_str, str):
                    if "亿" in size_str:
                        size = float(size_str.replace("亿", ""))
                    elif "万" in size_str:
                        size = float(size_str.replace("万", "")) / 10000
                elif isinstance(size_str, (int, float)):
                    size = size_str
            
            metadata_list.append({
                "etf_code": etf[ETF_CODE_COL],
                "etf_name": etf[ETF_NAME_COL],
                "volatility": 0.1,  # 默认波动率
                "size": size,
                "listing_date": etf.get(LISTING_DATE_COL, "2020-01-01"),
                "update_time": get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        return pd.DataFrame(metadata_list)
    
    except Exception as e:
        logger.error(f"创建基础ETF元数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def calculate_etf_score(etf_code: str, df: pd.DataFrame) -> float:
    """
    计算ETF综合评分
    
    Args:
        etf_code: ETF代码
        df: ETF日线数据
    
    Returns:
        float: ETF综合评分
    """
    try:
        # 获取当前双时区时间
        _, beijing_now = get_current_times()
        
        # 确保数据按日期排序
        if DATE_COL in df.columns:
            df = df.sort_values(DATE_COL)
        
        # 检查数据量
        if len(df) < 30:
            logger.warning(f"ETF {etf_code} 数据量不足，评分设为0")
            return 0.0
        
        # 取最近30天数据
        recent_30d = df.tail(30)
        
        # 1. 流动性得分（日均成交额）
        liquidity_score = calculate_liquidity_score(recent_30d)
        
        # 2. 风险控制得分
        risk_score = calculate_risk_score(recent_30d)
        
        # 3. 收益能力得分
        return_score = calculate_return_score(recent_30d)
        
        # 4. 情绪指标得分（成交量变化率）
        sentiment_score = calculate_sentiment_score(recent_30d)
        
        # 5. 基本面得分（规模、成立时间等）
        fundamental_score = calculate_fundamental_score(etf_code)
        
        # 计算综合评分（加权平均）
        total_score = (
            liquidity_score * 0.2 +
            risk_score * 0.2 +
            return_score * 0.25 +
            sentiment_score * 0.15 +
            fundamental_score * 0.2
        )
        
        logger.debug(
            f"ETF {etf_code} 评分详情: "
            f"流动性={liquidity_score:.2f}, "
            f"风险={risk_score:.2f}, "
            f"收益={return_score:.2f}, "
            f"情绪={sentiment_score:.2f}, "
            f"基本面={fundamental_score:.2f}, "
            f"综合={total_score:.2f}"
        )
        
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
    """计算流动性得分（日均成交额）"""
    try:
        if AMOUNT_COL not in df.columns:
            error_msg = f"DataFrame中缺少'{AMOUNT_COL}'列，流动性得分设为0"
            logger.warning(error_msg)
            
            # 发送错误通知
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
            
            return 0.0
        
        avg_volume = df[AMOUNT_COL].mean() / 10000  # 转换为万元
        # 线性映射到0-100分，日均成交额1000万=60分，5000万=100分
        score = min(max(avg_volume * 0.01 + 50, 0), 100)
        return round(score, 2)
    
    except Exception as e:
        error_msg = f"计算流动性得分失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return 0.0

def calculate_risk_score(df: pd.DataFrame) -> float:
    """计算风险控制得分"""
    try:
        # 1. 波动率得分
        volatility = calculate_volatility(df)
        volatility_score = max(0, 100 - (volatility * 100))
        
        # 2. 夏普比率得分
        sharpe_ratio = calculate_sharpe_ratio(df)
        sharpe_score = min(max(sharpe_ratio * 50, 0), 100)
        
        # 3. 最大回撤得分
        max_drawdown = calculate_max_drawdown(df)
        drawdown_score = max(0, 100 - (max_drawdown * 500))
        
        # 综合风险得分
        risk_score = (volatility_score * 0.4 + sharpe_score * 0.4 + drawdown_score * 0.2)
        return round(risk_score, 2)
    
    except Exception as e:
        error_msg = f"计算风险得分失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return 0.0

def calculate_return_score(df: pd.DataFrame) -> float:
    """计算收益能力得分"""
    try:
        if CLOSE_COL in df.columns and DATE_COL in df.columns:
            return_30d = (df[CLOSE_COL].iloc[-1] / df[CLOSE_COL].iloc[0] - 1) * 100
            # 线性映射到0-100分，-5%=-50分，+5%=100分
            return_score = min(max(return_30d * 10 + 100, 0), 100)
            return round(return_score, 2)
        else:
            logger.warning(f"DataFrame缺少必要列: {CLOSE_COL} 或 {DATE_COL}")
            return 0.0
    
    except Exception as e:
        error_msg = f"计算收益得分失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return 0.0

def calculate_sentiment_score(df: pd.DataFrame) -> float:
    """计算情绪指标得分（成交量变化率）"""
    try:
        if VOLUME_COL in df.columns:
            if len(df) >= 5:
                volume_change = (df[VOLUME_COL].iloc[-1] / df[VOLUME_COL].iloc[-5] - 1) * 100
                sentiment_score = min(max(volume_change + 50, 0), 100)
            else:
                sentiment_score = 50
            
            return round(sentiment_score, 2)
        else:
            logger.warning(f"DataFrame缺少必要列: {VOLUME_COL}")
            return 50.0
    
    except Exception as e:
        error_msg = f"计算情绪得分失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return 50.0

def get_etf_basic_info(etf_code: str) -> Tuple[float, str]:
    """
    获取ETF基本信息（规模、成立日期等）
    
    Args:
        etf_code: ETF代码 (6位数字)
    
    Returns:
        Tuple[float, str]: (基金规模(单位:亿元), 上市日期字符串)
    """
    try:
        logger.debug(f"尝试获取ETF基本信息，代码: {etf_code}")
        
        # 从ETF列表获取规模和成立日期
        etf_list = load_all_etf_list()
        etf_row = etf_list[etf_list[ETF_CODE_COL] == etf_code]
        
        if not etf_row.empty:
            # 处理规模
            size = 0.0
            size_str = etf_row.iloc[0][FUND_SIZE_COL]
            if isinstance(size_str, str):
                if "亿" in size_str:
                    size = float(size_str.replace("亿", ""))
                elif "万" in size_str:
                    size = float(size_str.replace("万", "")) / 10000
            elif isinstance(size_str, (int, float)):
                size = size_str
            
            # 处理成立日期
            listing_date = etf_row.iloc[0].get(LISTING_DATE_COL, "")
            
            logger.debug(f"ETF {etf_code} 基本信息: 规模={size}亿元, 成立日期={listing_date}")
            return size, listing_date
        else:
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

def calculate_fundamental_score(etf_code: str) -> float:
    """计算基本面得分（规模、成立时间等）"""
    try:
        size, listing_date = get_etf_basic_info(etf_code)
        
        # 规模得分（10亿=60分，100亿=100分）
        size_score = min(max(size * 0.4 + 50, 0), 100)
        
        # 成立时间得分（1年=50分，5年=100分）
        if not listing_date:
            age_score = 50.0
        else:
            try:
                listing_date = datetime.strptime(listing_date, "%Y-%m-%d")
                age = (get_beijing_time() - listing_date).days / 365
                age_score = min(max(age * 10 + 40, 0), 100)
            except Exception as e:
                logger.error(f"解析成立日期失败: {str(e)}", exc_info=True)
                age_score = 50.0
        
        # 综合基本面得分
        fundamental_score = (size_score * 0.6 + age_score * 0.4)
        return round(fundamental_score, 2)
    
    except Exception as e:
        error_msg = f"计算基本面得分失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return 0.0

def calculate_volatility(df: pd.DataFrame) -> float:
    """计算波动率（年化）"""
    try:
        if CLOSE_COL not in df.columns:
            logger.warning(f"DataFrame缺少必要列: {CLOSE_COL}")
            return 0.0
        
        # 计算日收益率
        df["daily_return"] = df[CLOSE_COL].pct_change()
        
        # 计算年化波动率
        volatility = df["daily_return"].std() * np.sqrt(252)
        return round(volatility, 4)
    
    except Exception as e:
        error_msg = f"计算波动率失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return 0.0

def calculate_sharpe_ratio(df: pd.DataFrame) -> float:
    """计算夏普比率（年化）"""
    try:
        if CLOSE_COL not in df.columns:
            logger.warning(f"DataFrame缺少必要列: {CLOSE_COL}")
            return 0.0
        
        # 计算日收益率
        df["daily_return"] = df[CLOSE_COL].pct_change()
        
        # 年化收益率
        annual_return = (df[CLOSE_COL].iloc[-1] / df[CLOSE_COL].iloc[0]) ** (252 / len(df)) - 1
        
        # 年化波动率
        volatility = df["daily_return"].std() * np.sqrt(252)
        
        # 无风险利率（假设为2%）
        risk_free_rate = 0.02
        
        # 夏普比率
        if volatility > 0:
            sharpe_ratio = (annual_return - risk_free_rate) / volatility
        else:
            sharpe_ratio = 0.0
        
        return round(sharpe_ratio, 4)
    
    except Exception as e:
        error_msg = f"计算夏普比率失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return 0.0

def calculate_max_drawdown(df: pd.DataFrame) -> float:
    """计算最大回撤"""
    try:
        if CLOSE_COL not in df.columns:
            logger.warning(f"DataFrame缺少必要列: {CLOSE_COL}")
            return 0.0
        
        # 计算累计收益率
        df["cum_return"] = (1 + df[CLOSE_COL].pct_change()).cumprod()
        
        # 计算回撤
        df["drawdown"] = 1 - df["cum_return"] / df["cum_return"].cummax()
        
        # 最大回撤
        max_drawdown = df["drawdown"].max()
        return round(max_drawdown, 4)
    
    except Exception as e:
        error_msg = f"计算最大回撤失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return 0.0

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
    
    # 检查元数据文件是否存在
    if not os.path.exists(Config.METADATA_PATH):
        logger.warning("ETF元数据文件不存在，将在需要时重建")
    else:
        # 检查元数据是否需要更新
        if is_file_outdated(Config.METADATA_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
            logger.info("ETF元数据已过期，将在需要时重建")
    
    # 初始化日志
    logger.info("ETF评分系统初始化完成")
    
except Exception as e:
    error_msg = f"ETF评分系统初始化失败: {str(e)}"
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
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
    except Exception as send_error:
        logger.error(f"发送错误通知失败: {str(send_error)}", exc_info=True)
