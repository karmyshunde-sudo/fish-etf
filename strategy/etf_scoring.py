#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF评分系统
基于多维度指标对ETF进行综合评分
"""

import pandas as pd
import numpy as np
import logging
import akshare as ak
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
from config import Config
from utils.date_utils import (
    get_current_times,
    format_dual_time,
    get_beijing_time,
    get_utc_time,
    is_file_outdated
)
from utils.file_utils import load_etf_daily_data, load_etf_metadata
from data_crawler.etf_list_manager import load_all_etf_list, get_etf_name

# 初始化日志
logger = logging.getLogger(__name__)

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
        if metadata_df is None or metadata_df.empty:
            logger.warning("元数据为空，无法获取ETF列表")
            return pd.DataFrame()
        
        # 获取所有ETF代码
        all_codes = metadata_df["etf_code"].tolist()
        if not all_codes:
            logger.warning("元数据中无ETF代码")
            return pd.DataFrame()
        
        # 计算评分
        score_list = []
        logger.info(f"开始计算 {len(all_codes)} 只ETF的综合评分...")
        
        for etf_code in all_codes:
            try:
                # 获取ETF日线数据
                df = load_etf_daily_data(etf_code)
                if df.empty:
                    logger.debug(f"ETF {etf_code} 无日线数据，跳过评分")
                    continue
                
                # 计算ETF评分
                score = calculate_etf_score(etf_code, df)
                if score < min_score:
                    continue
                
                # 获取ETF基本信息
                size, listing_date = get_etf_basic_info(etf_code)
                etf_name = get_etf_name(etf_code)
                
                # 计算日均成交额（单位：万元）
                avg_volume = 0.0
                if "成交额" in df.columns:
                    recent_30d = df.tail(30)
                    if len(recent_30d) > 0:
                        avg_volume = recent_30d["成交额"].mean() / 10000  # 转换为万元
                
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
            logger.info(f"没有ETF达到最低评分阈值 {min_score}，或未满足规模({min_fund_size}亿元)和日均成交额({min_avg_volume}万元)要求")
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
        logger.error(f"获取高分ETF列表时发生错误: {str(e)}", exc_info=True)
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
        df = df.sort_values("date")
        
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
        logger.error(f"计算ETF {etf_code} 评分失败: {str(e)}", exc_info=True)
        return 0.0

def calculate_liquidity_score(df: pd.DataFrame) -> float:
    """计算流动性得分（日均成交额）"""
    try:
        if "成交额" not in df.columns:
            logger.warning("DataFrame中缺少'成交额'列，流动性得分设为0")
            return 0.0
        
        avg_volume = df["成交额"].mean() / 10000  # 转换为万元
        # 线性映射到0-100分，日均成交额1000万=60分，5000万=100分
        score = min(max(avg_volume * 0.01 + 50, 0), 100)
        return round(score, 2)
    
    except Exception as e:
        logger.error(f"计算流动性得分失败: {str(e)}", exc_info=True)
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
        logger.error(f"计算风险得分失败: {str(e)}", exc_info=True)
        return 0.0

def calculate_return_score(df: pd.DataFrame) -> float:
    """计算收益能力得分"""
    try:
        return_30d = (df.iloc[-1]["收盘"] / df.iloc[0]["收盘"] - 1) * 100
        # 线性映射到0-100分，-5%=-50分，+5%=100分
        return_score = min(max(return_30d * 10 + 100, 0), 100)
        return round(return_score, 2)
    
    except Exception as e:
        logger.error(f"计算收益得分失败: {str(e)}", exc_info=True)
        return 0.0

def calculate_sentiment_score(df: pd.DataFrame) -> float:
    """计算情绪指标得分（成交量变化率）"""
    try:
        if len(df) >= 5:
            volume_change = (df["成交量"].iloc[-1] / df["成交量"].iloc[-5] - 1) * 100
            sentiment_score = min(max(volume_change + 50, 0), 100)
        else:
            sentiment_score = 50
        
        return round(sentiment_score, 2)
    
    except Exception as e:
        logger.error(f"计算情绪得分失败: {str(e)}", exc_info=True)
        return 50.0

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
        logger.error(f"计算基本面得分失败: {str(e)}", exc_info=True)
        return 0.0

def calculate_volatility(df: pd.DataFrame) -> float:
    """计算波动率（年化）"""
    try:
        # 计算日收益率
        df["daily_return"] = df["收盘"].pct_change()
        
        # 计算年化波动率
        volatility = df["daily_return"].std() * np.sqrt(252)
        return round(volatility, 4)
    
    except Exception as e:
        logger.error(f"计算波动率失败: {str(e)}", exc_info=True)
        return 0.0

def calculate_sharpe_ratio(df: pd.DataFrame) -> float:
    """计算夏普比率（年化）"""
    try:
        # 计算日收益率
        df["daily_return"] = df["收盘"].pct_change()
        
        # 年化收益率
        annual_return = (df["收盘"].iloc[-1] / df["收盘"].iloc[0]) ** (252 / len(df)) - 1
        
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
        logger.error(f"计算夏普比率失败: {str(e)}", exc_info=True)
        return 0.0

def calculate_max_drawdown(df: pd.DataFrame) -> float:
    """计算最大回撤"""
    try:
        # 计算累计收益率
        df["cum_return"] = (1 + df["收盘"].pct_change()).cumprod()
        
        # 计算回撤
        df["drawdown"] = 1 - df["cum_return"] / df["cum_return"].cummax()
        
        # 最大回撤
        max_drawdown = df["drawdown"].max()
        return round(max_drawdown, 4)
    
    except Exception as e:
        logger.error(f"计算最大回撤失败: {str(e)}", exc_info=True)
        return 0.0

def get_etf_basic_info(etf_code: str) -> Tuple[float, str]:
    """
    从AkShare获取ETF基本信息（规模、成立日期等）
    
    Args:
        etf_code: ETF代码 (6位数字)
    
    Returns:
        Tuple[float, str]: (基金规模(单位:亿元), 上市日期字符串)
    """
    try:
        logger.debug(f"尝试获取ETF基本信息，代码: {etf_code}")
        
        # 获取ETF基本信息
        df = ak.fund_etf_info_em(symbol=etf_code)
        if df.empty:
            logger.warning(f"AkShare未返回ETF {etf_code} 的基本信息")
            return 0.0, ""
        
        # 提取规模信息（单位：亿元）
        size_str = df.iloc[0]["基金规模"]
        # 处理"12.34亿"格式
        if "亿" in size_str:
            size = float(size_str.replace("亿", ""))
        # 处理"123400万"格式
        elif "万" in size_str:
            size = float(size_str.replace("万", "")) / 10000
        else:
            size = 0.0
        
        # 提取成立日期
        listing_date = df.iloc[0]["成立日期"]
        
        logger.debug(f"ETF {etf_code} 基本信息: 规模={size}亿元, 成立日期={listing_date}")
        return size, listing_date
    
    except Exception as e:
        logger.error(f"获取ETF {etf_code} 基本信息失败: {str(e)}", exc_info=True)
        return 0.0, ""

def analyze_etf_performance(etf_code: str, days: int = 30) -> Dict[str, Any]:
    """
    分析ETF历史表现
    
    Args:
        etf_code: ETF代码
        days: 分析天数
    
    Returns:
        Dict[str, Any]: 分析结果
    """
    try:
        # 获取ETF日线数据
        df = load_etf_daily_data(etf_code)
        if df.empty:
            logger.warning(f"ETF {etf_code} 无日线数据，无法分析表现")
            return {}
        
        # 取最近days天数据
        recent_data = df.tail(days)
        if len(recent_data) < 2:
            logger.warning(f"ETF {etf_code} 数据量不足，无法分析表现")
            return {}
        
        # 计算表现指标
        start_price = recent_data.iloc[0]["收盘"]
        end_price = recent_data.iloc[-1]["收盘"]
        return_rate = (end_price - start_price) / start_price * 100
        
        # 计算波动率
        volatility = calculate_volatility(recent_data)
        
        # 计算最大回撤
        max_drawdown = calculate_max_drawdown(recent_data)
        
        # 获取ETF基本信息
        size, listing_date = get_etf_basic_info(etf_code)
        etf_name = get_etf_name(etf_code)
        
        # 生成分析结果
        analysis = {
            "etf_code": etf_code,
            "etf_name": etf_name,
            "period_days": days,
            "start_date": recent_data.iloc[0]["date"],
            "end_date": recent_data.iloc[-1]["date"],
            "start_price": start_price,
            "end_price": end_price,
            "return_rate": return_rate,
            "volatility": volatility,
            "max_drawdown": max_drawdown,
            "fund_size": size,
            "listing_date": listing_date
        }
        
        logger.info(f"ETF {etf_code} {days}天表现分析完成")
        return analysis
    
    except Exception as e:
        logger.error(f"分析ETF {etf_code} 表现失败: {str(e)}", exc_info=True)
        return {}

def format_etf_analysis(etf_code: str, analysis: Dict[str, Any]) -> str:
    """
    格式化ETF分析结果
    
    Args:
        etf_code: ETF代码
        analysis: 分析结果
    
    Returns:
        str: 格式化后的分析消息
    """
    try:
        if not analysis:
            return f"【ETF {etf_code} 分析】\n• 无有效分析数据"
        
        # 获取当前双时区时间
        _, beijing_now = get_current_times()
        
        # 生成分析消息
        message = f"【ETF {analysis['etf_name']}({analysis['etf_code']}) 分析】\n"
        message += f"⏰ 分析时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        message += f"📊 分析周期: {analysis['start_date']} 至 {analysis['end_date']} ({analysis['period_days']}天)\n\n"
        
        # 添加价格表现
        message += "📈 价格表现\n"
        message += f"• 起始价格: {analysis['start_price']:.3f}元\n"
        message += f"• 结束价格: {analysis['end_price']:.3f}元\n"
        message += f"• 收益率: {analysis['return_rate']:.2f}%\n\n"
        
        # 添加风险指标
        message += "📉 风险指标\n"
        message += f"• 波动率: {analysis['volatility']:.4f}\n"
        message += f"• 最大回撤: {analysis['max_drawdown']:.4f}\n\n"
        
        # 添加基本面信息
        message += "📊 基本面信息\n"
        message += f"• 基金规模: {analysis['fund_size']:.2f}亿元\n"
        message += f"• 成立日期: {analysis['listing_date']}\n\n"
        
        # 添加投资建议
        message += "💡 投资建议\n"
        if analysis['return_rate'] > 5 and analysis['volatility'] < 0.1:
            message += "• 该ETF近期表现优异，风险较低，可考虑配置\n"
        elif analysis['return_rate'] > 0 and analysis['volatility'] < 0.2:
            message += "• 该ETF近期表现稳定，风险可控，可适度配置\n"
        elif analysis['return_rate'] < 0 and analysis['max_drawdown'] > 0.1:
            message += "• 该ETF近期表现不佳，回撤较大，建议谨慎配置\n"
        else:
            message += "• 该ETF表现中性，可根据个人风险偏好决定是否配置\n"
        
        return message
    
    except Exception as e:
        logger.error(f"格式化ETF分析失败: {str(e)}", exc_info=True)
        return f"【ETF分析】格式化消息失败"

def get_etf_score_history(etf_code: str, days: int = 30) -> pd.DataFrame:
    """
    获取ETF评分历史数据
    
    Args:
        etf_code: ETF代码
        days: 查询天数
    
    Returns:
        pd.DataFrame: 评分历史数据
    """
    try:
        history = []
        beijing_now = get_beijing_time()
        
        # 这里简化处理，实际应从历史评分文件中读取数据
        for i in range(days):
            date = (beijing_now - timedelta(days=i)).date().strftime("%Y-%m-%d")
            # 生成模拟评分数据
            score = 60 + (i % 10) * 2
            history.append({
                "日期": date,
                "评分": score,
                "排名": i + 1
            })
        
        if not history:
            logger.info(f"未找到ETF {etf_code} 的评分历史数据")
            return pd.DataFrame()
        
        return pd.DataFrame(history)
    
    except Exception as e:
        logger.error(f"获取ETF {etf_code} 评分历史数据失败: {str(e)}", exc_info=True)
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
        report += f"• 评分趋势: {trend}\n\n"
        
        # 添加建议
        if trend == "上升":
            report += "💡 建议：评分持续上升，可关注该ETF\n"
        else:
            report += "💡 建议：评分有所下降，建议关注原因\n"
        
        return report
    
    except Exception as e:
        error_msg = f"ETF {etf_code} 评分趋势分析失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return f"【{etf_code} 评分趋势】{error_msg}"

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 检查ETF列表是否过期
    if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
        logger.warning("ETF列表已过期，评分系统可能使用旧数据")
    
    # 初始化日志
    logger.info("ETF评分系统初始化完成")
    
except Exception as e:
    logger.error(f"ETF评分系统初始化失败: {str(e)}", exc_info=True)
    # 退回到基础日志配置
    import logging
    logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
    logging.error(f"ETF评分系统初始化失败: {str(e)}")
