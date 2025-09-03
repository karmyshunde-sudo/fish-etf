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
from utils.alert_utils import send_urgent_alert

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
                if df is None or df.empty:
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
        if etf_list is None or etf_list.empty:
            error_msg = "ETF列表为空，无法重建元数据"
            logger.error(error_msg)
            
            # 发送错误通知
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
            return False
        
        # 初始化元数据列表
        metadata_list = []
        
        # 遍历所有ETF，从本地日线数据计算元数据
        for _, etf in etf_list.iterrows():
            # 确保ETF代码格式一致（6位数字）
            etf_code = str(etf[ETF_CODE_COL]).strip().zfill(6)
            
            # 获取ETF日线数据（从本地文件加载）
            df = load_etf_daily_data(etf_code)
            if df is None or df.empty:
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
            error_msg = "没有有效的ETF元数据可重建"
            logger.error(error_msg)
            
            # 发送错误通知
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
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
        if etf_list is None or etf_list.empty:
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
            
            # 确保ETF代码格式一致（6位数字）
            etf_code = str(etf[ETF_CODE_COL]).strip().zfill(6)
            
            metadata_list.append({
                "etf_code": etf_code,
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
        min_required_data = 30  # 默认需要30天数据
        
        if listing_date:
            try:
                # 尝试解析成立日期
                date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]
                parsed_date = None
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(listing_date, fmt)
                        break
                    except:
                        continue
                
                if parsed_date:
                    # 计算ETF上市天数
                    days_since_listing = (beijing_now - parsed_date).days
                    if days_since_listing < min_required_data:
                        is_new_etf = True
                        # 根据上市天数调整所需数据量
                        min_required_data = max(5, days_since_listing)
                        logger.info(f"ETF {etf_code} 上市时间较短({days_since_listing}天)，使用{min_required_data}天数据计算评分")
            except Exception as e:
                logger.warning(f"解析ETF {etf_code} 成立日期失败: {str(e)}")
        
        # 检查数据量
        if len(df) < min_required_data:
            if len(df) < 5:
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
        
        # 5. 基本面得分（规模、成立时间等）
        fundamental_score = calculate_fundamental_score(etf_code)
        
        # 验证所有得分是否在有效范围内 [0, 100]
        scores = {
            "liquidity": liquidity_score,
            "risk": risk_score,
            "return": return_score,
            "sentiment": sentiment_score,
            "fundamental": fundamental_score
        }
        
        # 双重验证：确保所有得分在0-100范围内
        for name, score in scores.items():
            if score < 0 or score > 100:
                logger.error(f"ETF {etf_code} {name}得分超出范围({score})，强制限制在0-100")
                scores[name] = max(0, min(100, score))
        
        # 获取评分权重
        weights = Config.SCORE_WEIGHTS
        
        # 计算综合评分（加权平均）
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
        
        logger.debug(
            f"ETF {etf_code} 评分详情: "
            f"流动性={scores['liquidity']:.2f}({weights['liquidity']*100:.0f}%), "
            f"风险={scores['risk']:.2f}({weights['risk']*100:.0f}%), "
            f"收益={scores['return']:.2f}({weights['return']*100:.0f}%), "
            f"情绪={scores['sentiment']:.2f}({weights['sentiment']*100:.0f}%), "
            f"基本面={scores['fundamental']:.2f}({weights['fundamental']*100:.0f}%), "
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
        if df is None or df.empty:
            logger.warning("传入的DataFrame为空，流动性得分设为0")
            return 0.0
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
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
            # 尝试导入ensure_chinese_columns（如果尚未导入）
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
                    df["折溢价率"] = pd.to_numeric(df["折溢价率"], errors='coerce')
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

def calculate_return_score(premium_discount: Union[float, str, pd.Series, pd.DataFrame]) -> float:
    """
    计算收益评分（0-100分，分数越高表示潜在收益越大）
    
    Args:
        premium_discount: 折溢价率（百分比）
        
    Returns:
        float: 收益评分（0-100分）
    """
    try:
        # 确保premium_discount是标量值
        if isinstance(premium_discount, (pd.Series, pd.DataFrame)):
            # 如果是pandas对象，强制获取标量值
            if premium_discount.size == 1:
                # 单元素Series/DF，直接取值
                premium_discount = premium_discount.values.flatten()[0]
                logger.debug(f"从pandas对象获取标量值成功: {premium_discount}")
            else:
                # 多元素Series/DF，取第一个有效值
                valid_values = premium_discount[~pd.isna(premium_discount)]
                if not valid_values.empty:
                    premium_discount = valid_values.iloc[0]
                    logger.debug(f"从pandas对象获取第一个有效值成功: {premium_discount}")
                else:
                    logger.error("pandas对象中无有效值，使用默认值0.0")
                    premium_discount = 0.0
        
        # 处理字符串输入
        if isinstance(premium_discount, str):
            try:
                # 尝试移除百分号等非数字字符
                cleaned_str = ''.join(c for c in premium_discount if c.isdigit() or c in ['.', '-'])
                if cleaned_str:
                    premium_discount = float(cleaned_str)
                    logger.debug(f"将折溢价率字符串 '{premium_discount}' 转换为浮点数")
                else:
                    logger.error(f"无法从字符串 '{premium_discount}' 提取有效数字，使用默认值0.0")
                    premium_discount = 0.0
            except (ValueError, TypeError) as e:
                logger.error(f"无法将折溢价率 '{premium_discount}' 转换为浮点数: {str(e)}，使用默认值0.0")
                premium_discount = 0.0
        
        # 确保是数值类型
        if not isinstance(premium_discount, (int, float)):
            try:
                # 再次尝试转换为浮点数
                premium_discount = float(premium_discount)
                logger.debug(f"将非数值类型转换为浮点数: {premium_discount}")
            except (ValueError, TypeError) as e:
                logger.error(f"无法将类型 {type(premium_discount)} 转换为浮点数: {str(e)}，使用默认值0.0")
                premium_discount = 0.0
        
        # 记录实际使用的折溢价率值
        logger.debug(f"实际使用的折溢价率值: {premium_discount:.2f}")
        
        # 定义合理的折溢价率范围
        MAX_DISCOUNT = -5.0  # 最大折价率（-5%）
        MIN_PREMIUM = 0.5    # 最小溢价率（0.5%）
        MAX_PREMIUM = 3.0    # 最大溢价率（3.0%），超过此值风险急剧增加
        
        # 处理折价情况
        if premium_discount < 0:
            # 折价率越大（负值越小），评分越高，但有上限
            discount_rate = min(abs(premium_discount), abs(MAX_DISCOUNT))
            # 使用非线性函数，使评分增长更平缓
            return_score = 80 * (1 - np.exp(-2 * discount_rate))
        # 处理溢价情况
        else:
            # 溢价率在合理范围内时，评分随溢价率增加而增加
            if premium_discount <= MAX_PREMIUM:
                # 小幅溢价有正收益评分
                if premium_discount >= MIN_PREMIUM:
                    return_score = 50 * (premium_discount / MAX_PREMIUM)
                else:
                    return_score = 0
            else:
                # 过度溢价视为高风险，评分直接为0
                return_score = 0
        
        # 确保评分在0-100范围内
        return_score = max(0, min(100, return_score))
        
        logger.debug(f"折溢价率={premium_discount:.2f}%，收益评分={return_score:.2f}")
        return return_score
    
    except Exception as e:
        logger.error(f"计算收益评分失败: {str(e)}", exc_info=True)
        return 50.0  # 出错时返回中性评分

def calculate_sentiment_score(df: pd.DataFrame) -> float:
    """计算情绪指标得分（成交量变化率）"""
    try:
        if df is None or df.empty:
            logger.warning("传入的DataFrame为空，情绪得分设为50")
            return 50.0
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
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
        
        # 确保ETF代码格式一致（6位数字）
        etf_code = str(etf_code).strip().zfill(6)
        
        # 检查ETF列表是否有效
        if etf_list is None or etf_list.empty:
            logger.warning("ETF列表为空或无效，使用默认值")
            return 0.0, ""
        
        # 确保ETF列表包含必要的列
        required_columns = [ETF_CODE_COL, FUND_SIZE_COL]
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
                size_str = etf_row.iloc[0][FUND_SIZE_COL]
                if isinstance(size_str, str):
                    if "亿" in size_str:
                        size = float(size_str.replace("亿", ""))
                    elif "万" in size_str:
                        size = float(size_str.replace("万", "")) / 10000
                elif isinstance(size_str, (int, float)):
                    size = size_str
            else:
                logger.warning(f"ETF {etf_code} 缺少基金规模信息，使用默认值")
            
            # 处理成立日期
            listing_date = ""
            if LISTING_DATE_COL in etf_list.columns and LISTING_DATE_COL in etf_row.iloc[0]:
                listing_date = etf_row.iloc[0][LISTING_DATE_COL]
            
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
                # 处理不同格式的日期字符串
                if isinstance(listing_date, str):
                    # 尝试多种日期格式
                    date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]
                    parsed_date = None
                    
                    for fmt in date_formats:
                        try:
                            parsed_date = datetime.strptime(listing_date, fmt)
                            break
                        except:
                            continue
                    
                    if parsed_date is None:
                        logger.warning(f"无法解析ETF {etf_code} 的成立日期: {listing_date}")
                        age_score = 50.0
                    else:
                        age = (get_beijing_time() - parsed_date).days / 365
                        age_score = min(max(age * 10 + 40, 0), 100)
                else:
                    logger.warning(f"ETF {etf_code} 的成立日期格式不正确: {listing_date}")
                    age_score = 50.0
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

def get_price_column(df: pd.DataFrame) -> Optional[str]:
    """
    获取价格列名
    
    Returns:
        str: 价格列名，如果找不到返回None
    """
    # 记录DataFrame实际包含的列名，用于诊断问题
    logger.debug(f"DataFrame实际包含的列名: {list(df.columns)}")
    
    # 优先检查套利专用列
    if "市场价格" in df.columns:
        return "市场价格"
    
    # 定义可能的价格列名（按优先级排序）
    price_columns = [
        "收盘", "close", "市场价格", "最新价", "price", 
        "最新价格", "市场价格(元)", "IOPV", "昨收", "前收盘"
    ]
    
    # 检查哪些列存在于DataFrame中
    available_price_columns = [col for col in price_columns if col in df.columns]
    
    if available_price_columns:
        logger.info(f"找到价格列: {available_price_columns[0]} (可用价格列: {available_price_columns})")
        return available_price_columns[0]
    else:
        # 尝试模糊匹配
        for col in df.columns:
            if any(keyword in col for keyword in ["收", "价", "最新", "price"]):
                logger.info(f"通过模糊匹配找到价格列: {col}")
                return col
        
        logger.warning("未找到价格列，DataFrame实际列名: " + ", ".join(df.columns))
        return None

def calculate_volatility(df: pd.DataFrame) -> float:
    """计算波动率（年化）"""
    try:
        if df is None or df.empty:
            logger.warning("传入的DataFrame为空，波动率设为0")
            return 0.0
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 获取价格列
        price_col = get_price_column(df)
        if not price_col:
            logger.error("DataFrame缺少必要价格列，无法计算波动率。实际列名: " + ", ".join(df.columns))
            return 0.0
        
        # 确保价格列是数值类型
        if not pd.api.types.is_numeric_dtype(df[price_col]):
            try:
                df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
                # 移除NaN值
                df = df.dropna(subset=[price_col])
            except Exception as e:
                logger.error(f"转换价格列为数值类型失败: {str(e)}")
                return 0.0
        
        # 确保数据量足够
        if len(df) < 2:
            logger.warning("数据量不足，无法计算波动率")
            return 0.0
        
        # 计算日收益率
        df["daily_return"] = df[price_col].pct_change()
        
        # 移除NaN值
        df = df.dropna(subset=["daily_return"])
        
        # 确保数据量足够
        if len(df) < 2:
            logger.warning("计算收益率后数据量不足，无法计算波动率")
            return 0.0
        
        # 计算年化波动率
        volatility = df["daily_return"].std() * np.sqrt(252)
        return round(volatility, 4)
    
    except Exception as e:
        error_msg = f"计算波动率失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return 0.0

def calculate_premium_discount(market_price: float, iopv: float) -> float:
    """
    计算折溢价率
    
    Args:
        market_price: 市场价格
        iopv: IOPV(基金份额参考净值)
    
    Returns:
        float: 折溢价率（百分比），正数表示溢价，负数表示折价
    """
    try:
        if iopv <= 0:
            logger.warning(f"无效的IOPV: {iopv}")
            return 0.0
        
        # 双重验证：确保计算结果正确
        premium_discount = ((market_price - iopv) / iopv) * 100
        
        # 验证计算结果
        if market_price > iopv and premium_discount <= 0:
            error_msg = (f"严重错误：市场价格({market_price}) > IOPV({iopv})，"
                         f"但计算出的折溢价率({premium_discount})≤0")
            logger.error(error_msg)
            send_urgent_alert(error_msg, priority=1)
            # 修正计算结果
            premium_discount = abs((market_price - iopv) / iopv * 100)
        elif market_price < iopv and premium_discount >= 0:
            error_msg = (f"严重错误：市场价格({market_price}) < IOPV({iopv})，"
                         f"但计算出的折溢价率({premium_discount})≥0")
            logger.error(error_msg)
            send_urgent_alert(error_msg, priority=1)
            # 修正计算结果
            premium_discount = -abs((market_price - iopv) / iopv * 100)
        
        return round(premium_discount, 2)
    
    except Exception as e:
        error_msg = f"计算折溢价率失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return 0.0

def calculate_sharpe_ratio(df: pd.DataFrame) -> float:
    """计算夏普比率（年化）"""
    try:
        if df is None or df.empty:
            logger.warning("传入的DataFrame为空，夏普比率设为0")
            return 0.0
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 优先使用"收盘"列，如果没有则使用"市场价格"，再没有则使用"最新价"
        if CLOSE_COL in df.columns:
            price_col = CLOSE_COL
        elif "市场价格" in df.columns:
            price_col = "市场价格"
        elif "最新价" in df.columns:
            price_col = "最新价"
        else:
            logger.warning(f"DataFrame缺少必要列: {CLOSE_COL}, '市场价格'或'最新价'")
            return 0.0
        
        # 计算日收益率
        df["daily_return"] = df[price_col].pct_change()
        
        # 年化收益率
        if len(df) > 1:
            annual_return = (df[price_col].iloc[-1] / df[price_col].iloc[0]) ** (252 / len(df)) - 1
        else:
            annual_return = 0.0
        
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
        if df is None or df.empty:
            logger.warning("传入的DataFrame为空，最大回撤设为0")
            return 0.0
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 优先使用"收盘"列，如果没有则使用"市场价格"，再没有则使用"最新价"
        if CLOSE_COL in df.columns:
            price_col = CLOSE_COL
        elif "市场价格" in df.columns:
            price_col = "市场价格"
        elif "最新价" in df.columns:
            price_col = "最新价"
        else:
            logger.warning(f"DataFrame缺少必要列: {CLOSE_COL}, '市场价格'或'最新价'")
            return 0.0
        
        # 计算累计收益率
        df["cum_return"] = (1 + df[price_col].pct_change()).cumprod()
        
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

# ========================
# 新增：套利专用评分函数
# ========================

def calculate_arbitrage_score(etf_code: str, df: pd.DataFrame, premium_discount: float, meta: Optional[Dict] = None) -> float:
    """
    计算ETF套利综合评分
    
    Args:
        etf_code: ETF代码
        df: ETF日线数据
        premium_discount: 折溢价率
        meta: ETF元数据（可选）
    
    Returns:
        float: 综合评分 (0-100)
    """
    try:
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        if df is None or df.empty:
            logger.warning(f"ETF {etf_code} 无日线数据，无法计算套利综合评分")
            return 0.0
        
        df = df.copy(deep=True)
        
        # 计算基础ETF评分
        base_score = calculate_etf_score(etf_code, df)
        
        # 确保基础评分在0-100范围内
        if base_score < 0 or base_score > 100:
            logger.warning(f"ETF {etf_code} 基础评分超出范围({base_score:.2f})，强制限制在0-100")
            base_score = max(0, min(100, base_score))
        
        # 计算成分股稳定性评分
        component_score = calculate_component_stability_score(etf_code, df)
        
        # 确保成分股稳定性评分在0-100范围内
        if component_score < 0 or component_score > 100:
            logger.warning(f"ETF {etf_code} 成分股稳定性评分超出范围({component_score:.2f})，强制限制在0-100")
            component_score = max(0, min(100, component_score))
        
        # 记录折溢价率参数，用于调试
        logger.debug(f"ETF {etf_code} 套利评分参数: premium_discount={premium_discount}, "
                     f"DISCOUNT_THRESHOLD={Config.DISCOUNT_THRESHOLD}, "
                     f"PREMIUM_THRESHOLD={Config.PREMIUM_THRESHOLD}")
        
        # 计算折溢价率评分
        if premium_discount < 0:
            # 折价情况：折价率绝对值越大，评分越高
            abs_premium = abs(premium_discount)
            if abs_premium >= Config.DISCOUNT_THRESHOLD * 1.5:
                premium_score = 100.0
            elif abs_premium >= Config.DISCOUNT_THRESHOLD:
                # 确保分母不为0
                if Config.DISCOUNT_THRESHOLD * 0.5 > 0:
                    premium_score = 80.0 + (abs_premium - Config.DISCOUNT_THRESHOLD) * 20.0 / (Config.DISCOUNT_THRESHOLD * 0.5)
                else:
                    premium_score = 100.0
            else:
                # 确保分母不为0
                if Config.DISCOUNT_THRESHOLD > 0:
                    premium_score = 50.0 + (abs_premium * 30.0 / Config.DISCOUNT_THRESHOLD)
                else:
                    premium_score = 50.0 + (abs_premium * 30.0)
        else:
            # 溢价情况：溢价率越小，评分越高
            if premium_discount <= Config.PREMIUM_THRESHOLD * 0.5:
                premium_score = 100.0
            elif premium_discount <= Config.PREMIUM_THRESHOLD:
                # 确保分母不为0
                if Config.PREMIUM_THRESHOLD * 0.5 > 0:
                    premium_score = 80.0 - (premium_discount - Config.PREMIUM_THRESHOLD * 0.5) * 40.0 / (Config.PREMIUM_THRESHOLD * 0.5)
                else:
                    premium_score = 100.0
            else:
                # 确保分母不为0
                if Config.PREMIUM_THRESHOLD > 0:
                    premium_score = 50.0 - (premium_discount - Config.PREMIUM_THRESHOLD) * 20.0 / (Config.PREMIUM_THRESHOLD * 1.0)
                else:
                    premium_score = 50.0 - (premium_discount * 20.0)
        
        # 确保折溢价率评分在0-100范围内
        if premium_score < 0 or premium_score > 100:
            logger.warning(f"ETF {etf_code} 折溢价率评分超出范围({premium_score:.2f})，强制限制在0-100")
            premium_score = max(0, min(100, premium_score))
        
        # 获取评分权重
        weights = Config.ARBITRAGE_SCORE_WEIGHTS.copy()
        
        # 关键修复：确保权重字典包含所有必要的键
        required_keys = ['premium_discount', 'liquidity', 'risk', 'return', 'market_sentiment', 'fundamental', 'component_stability']
        for key in required_keys:
            if key not in weights:
                logger.warning(f"权重字典缺少必要键: {key}, 使用默认值0.1")
                weights[key] = 0.1
        
        # 确保权重和为1
        total_weight = sum(weights.values())
        if abs(total_weight - 1.0) > 0.001:
            logger.warning(f"权重和不为1 ({total_weight}), 正在归一化")
            for key in weights:
                weights[key] /= total_weight
        
        # 验证每个权重是否在合理范围内
        for key, weight in weights.items():
            if weight < 0 or weight > 1:
                logger.error(f"权重 {key} 超出范围({weight:.2f})，强制限制在0-1")
                weights[key] = max(0, min(1, weight))
        
        # 综合评分（加权平均）
        total_score = (
            base_score * (weights['liquidity'] + weights['risk'] + weights['return'] + weights['market_sentiment'] + weights['fundamental']) +
            component_score * weights['component_stability'] +
            premium_score * weights['premium_discount']
        )
        
        # 双重验证：确保评分在0-100范围内
        if total_score < 0 or total_score > 100:
            logger.error(f"ETF {etf_code} 套利综合评分超出范围({total_score})，强制限制在0-100")
            total_score = max(0, min(100, total_score))
        
        # 添加详细日志，便于问题排查
        logger.debug(f"ETF {etf_code} 套利综合评分详情: "
                     f"基础评分={base_score:.2f}(权重{weights['liquidity'] + weights['risk'] + weights['return'] + weights['market_sentiment'] + weights['fundamental']:.2f}), "
                     f"成分股稳定性={component_score:.2f}(权重{weights['component_stability']:.2f}), "
                     f"折溢价率={premium_score:.2f}(权重{weights['premium_discount']:.2f}), "
                     f"最终评分={total_score:.2f}")
        
        return total_score
    
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 套利综合评分失败: {str(e)}", exc_info=True)
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
        logger.error(f"计算成分股稳定性评分失败: {str(e)}", exc_info=True)
        return 70.0  # 默认中等偏高评分

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
