#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股趋势跟踪策略（TickTen策略）
基于流动性、波动率、市值筛选优质个股，计算趋势信号并推送微信通知
"""

import os
import logging
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    is_file_outdated
)
from utils.file_utils import (
    load_etf_daily_data,
    init_dirs,
    load_stock_daily_data
)
from data_crawler.stock_list_manager import load_all_stock_list
from data_crawler.akshare_crawler import fetch_stock_data
from wechat_push.push import send_wechat_message
from strategy.etf_scoring import get_top_rated_etfs

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 策略参数（针对个股优化）
CRITICAL_VALUE_DAYS = 40  # 临界值计算周期（40日均线，原ETF策略为20日）
DEVIATION_THRESHOLD = 0.08  # 偏离阈值（8%，原ETF策略为2%）
VOLUME_CHANGE_THRESHOLD = 0.35  # 成交量变化阈值（35%，原ETF策略为20%）
MIN_CONSECUTIVE_DAYS = 3  # 最小连续站上/跌破天数（原ETF策略为1-2天）
PATTERN_CONFIDENCE_THRESHOLD = 0.7  # 形态确认阈值（70%置信度）
MAX_STOCK_POSITION = 0.15  # 单一个股最大仓位（15%，原ETF策略为30%-50%）
MIN_MARKET_CAP = 200  # 最小市值（200亿元）
MIN_DAILY_VOLUME = 100000000  # 最小日均成交额（1亿元）
MAX_ANNUAL_VOLATILITY = 0.4  # 最大年化波动率（40%）

def calculate_critical_value(df: pd.DataFrame, period: int = CRITICAL_VALUE_DAYS) -> float:
    """计算临界值（40日均线）"""
    if len(df) < period:
        logger.warning(f"数据不足{period}天，无法准确计算临界值")
        return df["收盘"].mean() if not df.empty else 0.0
    
    return df['收盘'].rolling(window=period).mean().iloc[-1]

def calculate_deviation(current: float, critical: float) -> float:
    """计算偏离率"""
    return (current - critical) / critical * 100

def calculate_consecutive_days_above(df: pd.DataFrame, critical_value: float, 
                                    period: int = CRITICAL_VALUE_DAYS) -> int:
    """计算连续站上均线的天数"""
    if len(df) < 2:
        return 0
    
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=period).mean().values
    
    # 从最新日期开始向前检查
    consecutive_days = 0
    for i in range(len(close_prices)-1, -1, -1):
        if i < period - 1:
            break
            
        if not np.isnan(ma_values[i]) and close_prices[i] >= ma_values[i]:
            consecutive_days += 1
        else:
            break
    
    return consecutive_days

def calculate_consecutive_days_below(df: pd.DataFrame, critical_value: float, 
                                    period: int = CRITICAL_VALUE_DAYS) -> int:
    """计算连续跌破均线的天数"""
    if len(df) < 2:
        return 0
    
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=period).mean().values
    
    # 从最新日期开始向前检查
    consecutive_days = 0
    for i in range(len(close_prices)-1, -1, -1):
        if i < period - 1:
            break
            
        if not np.isnan(ma_values[i]) and close_prices[i] < ma_values[i]:
            consecutive_days += 1
        else:
            break
    
    return consecutive_days

def calculate_volume_change(df: pd.DataFrame, days: int = 5) -> float:
    """计算成交量变化率"""
    if len(df) < days + 1:
        return 0.0
    
    recent_volume = df["成交量"].iloc[-days:].mean()
    previous_volume = df["成交量"].iloc[-(days*2):-days].mean()
    
    if previous_volume > 0:
        return (recent_volume - previous_volume) / previous_volume * 100
    return 0.0

def calculate_annual_volatility(df: pd.DataFrame) -> float:
    """计算年化波动率"""
    if len(df) < 30:
        return 0.0
    
    # 计算日收益率
    daily_returns = df["收盘"].pct_change().dropna()
    
    # 年化波动率 = 日波动率 * sqrt(252)
    if len(daily_returns) > 1:
        daily_vol = daily_returns.std()
        return daily_vol * np.sqrt(252)
    
    return 0.0

def calculate_market_cap(stock_code: str) -> float:
    """获取市值（亿元）"""
    try:
        # 这里简化处理，实际应从数据源获取
        # 可以使用akshare或其他数据源
        df = load_stock_daily_data(stock_code)
        if not df.empty:
            latest = df.iloc[-1]
            # 假设我们有总股本数据，这里简化为收盘价 * 总股本
            # 实际应用中需要从基本面数据获取
            return latest["收盘"] * 10  # 模拟值，单位：亿元
        return 0.0
    except Exception as e:
        logger.error(f"获取{stock_code}市值失败: {str(e)}", exc_info=True)
        return 0.0

def calculate_daily_volume(stock_code: str) -> float:
    """获取日均成交额（元）"""
    try:
        df = load_stock_daily_data(stock_code)
        if not df.empty and len(df) >= 20:
            return df["成交量"].iloc[-20:].mean() * df["收盘"].iloc[-20:].mean()
        return 0.0
    except Exception as e:
        logger.error(f"获取{stock_code}日均成交失败: {str(e)}", exc_info=True)
        return 0.0

def is_stock_suitable(stock_code: str, df: pd.DataFrame) -> bool:
    """
    判断个股是否适合策略（流动性、波动率、市值三重过滤）
    
    Args:
        stock_code: 股票代码
        df: 股票日线数据
    
    Returns:
        bool: 是否适合策略
    """
    try:
        # 1. 流动性过滤（日均成交>1亿）
        daily_volume = calculate_daily_volume(stock_code)
        if daily_volume < MIN_DAILY_VOLUME:
            return False
        
        # 2. 波动率过滤（年化波动率<40%）
        annual_volatility = calculate_annual_volatility(df)
        if annual_volatility > MAX_ANNUAL_VOLATILITY:
            return False
        
        # 3. 市值过滤（市值>200亿）
        market_cap = calculate_market_cap(stock_code)
        if market_cap < MIN_MARKET_CAP:
            return False
        
        return True
    
    except Exception as e:
        logger.error(f"筛选股票{stock_code}失败: {str(e)}", exc_info=True)
        return False

def calculate_stock_strategy_score(stock_code: str, df: pd.DataFrame) -> float:
    """
    计算个股策略得分（胜率评估）
    
    Args:
        stock_code: 股票代码
        df: 股票日线数据
    
    Returns:
        float: 策略得分（0-100）
    """
    try:
        if df.empty or len(df) < CRITICAL_VALUE_DAYS + 30:
            return 0.0
        
        # 1. 基础信号得分（40%权重）
        current = df["收盘"].iloc[-1]
        critical = calculate_critical_value(df)
        deviation = calculate_deviation(current, critical)
        
        base_score = 0.0
        # YES信号
        if current >= critical:
            # 偏离率越小，得分越高（最大40分）
            base_score = max(0, 40 - abs(deviation) * 2)
        # NO信号
        else:
            # 偏离率越负，得分越低（但超卖有反弹机会）
            base_score = max(0, 20 + deviation * 1.5)
        
        # 2. 信号确认得分（30%权重）
        volume_change = calculate_volume_change(df)
        consecutive_days = calculate_consecutive_days_above(df, critical) if current >= critical \
                          else calculate_consecutive_days_below(df, critical)
        
        confirmation_score = 0.0
        # 成交量确认（15分）
        if volume_change > VOLUME_CHANGE_THRESHOLD * 100:
            confirmation_score += 15
        # 连续天数确认（15分）
        if consecutive_days >= MIN_CONSECUTIVE_DAYS:
            confirmation_score += 15
        
        # 3. 历史回测得分（30%权重）
        # 这里简化处理，实际应进行历史回测
        historical_score = 25  # 默认值
        
        # 综合得分
        total_score = base_score * 0.4 + confirmation_score * 0.3 + historical_score * 0.3
        return min(total_score, 100.0)
    
    except Exception as e:
        logger.error(f"计算{stock_code}策略得分失败: {str(e)}", exc_info=True)
        return 0.0

def is_in_volatile_market(df: pd.DataFrame, period: int = CRITICAL_VALUE_DAYS) -> tuple:
    """判断是否处于震荡市
    
    Returns:
        tuple: (是否震荡市, 穿越次数, 最近10天偏离率范围)
    """
    if len(df) < 10:
        return False, 0, (0, 0)
    
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=period).mean().values
    
    # 检查是否连续10天在均线附近波动（-8%~+8%）
    last_10_days = df.tail(10)
    deviations = []
    for i in range(len(last_10_days)):
        # 确保有足够的数据计算均线
        if i < period - 1 or np.isnan(ma_values[-10 + i]):
            continue
            
        deviation = (close_prices[-10 + i] - ma_values[-10 + i]) / ma_values[-10 + i] * 100
        if abs(deviation) > 8.0:  # 个股波动更大，阈值提高到8%
            return False, 0, (0, 0)
        deviations.append(deviation)
    
    # 检查价格是否反复穿越均线
    cross_count = 0
    for i in range(len(close_prices)-10, len(close_prices)-1):
        # 确保有足够的数据计算均线
        if i < period - 1 or np.isnan(ma_values[i]) or np.isnan(ma_values[i+1]):
            continue
            
        if (close_prices[i] >= ma_values[i] and close_prices[i+1] < ma_values[i+1]) or \
           (close_prices[i] < ma_values[i] and close_prices[i+1] >= ma_values[i+1]):
            cross_count += 1
    
    # 至少需要5次穿越才认定为震荡市
    min_cross_count = 5
    is_volatile = cross_count >= min_cross_count
    
    # 计算最近10天偏离率范围
    if deviations:
        min_deviation = min(deviations)
        max_deviation = max(deviations)
    else:
        min_deviation = 0
        max_deviation = 0
    
    return is_volatile, cross_count, (min_deviation, max_deviation)

def detect_head_and_shoulders(df: pd.DataFrame, period: int = CRITICAL_VALUE_DAYS) -> dict:
    """检测M头和头肩顶形态
    
    Returns:
        dict: 形态检测结果
    """
    if len(df) < 20:  # 需要足够数据
        return {"pattern_type": "无", "detected": False, "confidence": 0, "peaks": []}
    
    # 获取收盘价
    close_prices = df["收盘"].values
    
    # 寻找局部高点
    peaks = []
    for i in range(5, len(close_prices)-5):
        if close_prices[i] > max(close_prices[i-5:i]) and close_prices[i] > max(close_prices[i+1:i+6]):
            peaks.append((i, close_prices[i]))
    
    # 如果找到的高点少于3个，无法形成头肩顶
    if len(peaks) < 3:
        return {"pattern_type": "无", "detected": False, "confidence": 0, "peaks": peaks}
    
    # 检测M头（两个高点）
    m_top_detected = False
    m_top_confidence = 0.0
    if len(peaks) >= 2:
        # 两个高点，第二个略低于第一个，中间有明显低点
        peak1_idx, peak1_price = peaks[-2]
        peak2_idx, peak2_price = peaks[-1]
        
        # 检查第二个高点是否低于第一个
        if peak2_price < peak1_price and peak2_price > peak1_price * 0.92:  # 个股波动大，阈值放宽
            # 检查中间是否有明显低点
            trough_idx = peak1_idx + np.argmin(close_prices[peak1_idx:peak2_idx])
            trough_price = close_prices[trough_idx]
            
            # 检查低点是否明显
            if trough_price < peak1_price * 0.95 and trough_price < peak2_price * 0.95:  # 阈值放宽
                m_top_detected = True
                # 计算置信度
                price_diff = (peak1_price - peak2_price) / peak1_price
                trough_depth = (peak1_price - trough_price) / peak1_price
                m_top_confidence = 0.5 + 0.5 * min(price_diff / 0.08, 1) + 0.5 * min(trough_depth / 0.08, 1)
                m_top_confidence = min(m_top_confidence, 1.0)
    
    # 检测头肩顶（三个高点）
    head_and_shoulders_detected = False
    head_and_shoulders_confidence = 0.0
    
    if len(peaks) >= 3:
        # 三个高点，中间最高，两侧较低
        shoulder1_idx, shoulder1_price = peaks[-3]
        head_idx, head_price = peaks[-2]
        shoulder2_idx, shoulder2_price = peaks[-1]
        
        # 检查中间是否为最高点
        if head_price > shoulder1_price and head_price > shoulder2_price:
            # 检查两侧肩膀是否大致对称
            shoulder_similarity = min(shoulder1_price, shoulder2_price) / max(shoulder1_price, shoulder2_price)
            
            # 检查中间低点
            trough1_idx = shoulder1_idx + np.argmin(close_prices[shoulder1_idx:head_idx])
            trough2_idx = head_idx + np.argmin(close_prices[head_idx:shoulder2_idx])
            neckline_price = (close_prices[trough1_idx] + close_prices[trough2_idx]) / 2
            
            # 检查头肩比例是否合理
            if shoulder_similarity > 0.8 and head_price > neckline_price * 1.1:  # 阈值放宽
                head_and_shoulders_detected = True
                # 计算置信度
                shoulder_diff = 1 - shoulder_similarity
                head_height = (head_price - neckline_price) / neckline_price
                head_and_shoulders_confidence = 0.5 + 0.3 * min(shoulder_diff / 0.2, 1) + 0.2 * min(head_height / 0.2, 1)
                head_and_shoulders_confidence = min(head_and_shoulders_confidence, 1.0)
    
    # 确定主要检测结果
    if head_and_shoulders_detected and head_and_shoulders_confidence > m_top_confidence:
        return {
            "pattern_type": "头肩顶",
            "detected": True,
            "confidence": head_and_shoulders_confidence,
            "peaks": peaks[-3:]
        }
    elif m_top_detected:
        return {
            "pattern_type": "M头",
            "detected": True,
            "confidence": m_top_confidence,
            "peaks": peaks[-2:]
        }
    else:
        return {
            "pattern_type": "无",
            "detected": False,
            "confidence": 0,
            "peaks": peaks[-3:] if len(peaks) >= 3 else peaks
        }

def calculate_stock_stop_loss(current_price: float, signal: str, deviation: float) -> float:
    """计算个股止损位"""
    if signal == "YES":
        # 上涨趋势中，止损设在5日均线下方
        return current_price * 0.92  # 8%止损
    else:
        # 下跌趋势中，止损设在前高上方
        return current_price * 1.05  # 5%止损

def calculate_stock_take_profit(current_price: float, signal: str, deviation: float) -> float:
    """计算个股止盈位"""
    if signal == "YES":
        # 上涨趋势中，止盈设在偏离率+15%处
        return current_price * 1.15
    else:
        # 下跌趋势中，止盈设在偏离率-5%处
        return current_price * 0.95

def generate_stock_signal_message(stock_info: dict, df: pd.DataFrame, 
                                 current: float, critical: float, deviation: float) -> str:
    """生成个股策略信号消息"""
    # 计算连续站上/跌破均线的天数
    consecutive = calculate_consecutive_days_above(df, critical) if current >= critical \
                 else calculate_consecutive_days_below(df, critical)
    
    # 计算成交量变化
    volume_change = calculate_volume_change(df)
    
    # 检测M头/头肩顶形态
    pattern_detection = detect_head_and_shoulders(df)
    
    # 3. 震荡市判断 - 优先级最高
    is_volatile, cross_count, (min_dev, max_dev) = is_in_volatile_market(df)
    if is_volatile:
        # 计算上轨和下轨价格
        upper_band = critical * (1 + max_dev/100)
        lower_band = critical * (1 + min_dev/100)
        
        message = (
            f"【震荡市】连续10日价格反复穿均线（穿越{cross_count}次），偏离率范围[{min_dev:.2f}%~{max_dev:.2f}%]\n"
            f"✅ 操作建议：\n"
            f"  • 上沿操作（价格≈{upper_band:.2f}）：小幅减仓10%-15%\n"
            f"  • 下沿操作（价格≈{lower_band:.2f}）：小幅加仓10%-15%\n"
            f"  • 总仓位严格控制在≤40%\n"
            f"⚠️ 避免频繁交易，等待趋势明朗\n"
        )
        return message
    
    # 1. YES信号：当前价格 ≥ 40日均线
    if current >= critical:
        # 子条件1：首次突破（价格刚站上均线，连续3-4日站稳+成交量放大35%+）
        if consecutive == 1 and volume_change > 35:
            message = (
                f"【首次突破】连续{consecutive}天站上40日均线，成交量放大{volume_change:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 建仓{int(MAX_STOCK_POSITION * 100)}%（单一个股上限{int(MAX_STOCK_POSITION * 100)}%）\n"
                f"  • 止损位：{calculate_stock_stop_loss(current, 'YES', deviation):.2f}（-8%）\n"
                f"  • 目标位：{calculate_stock_take_profit(current, 'YES', deviation):.2f}（+15%）\n"
                f"⚠️ 注意：若收盘跌破5日均线，立即减仓50%\n"
            )
        # 子条件1：首次突破（价格刚站上均线，连续3-4日站稳+成交量放大35%+）
        elif 2 <= consecutive <= 4 and volume_change > 35:
            message = (
                f"【首次突破确认】连续{consecutive}天站上40日均线，成交量放大{volume_change:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 可加仓至{int(MAX_STOCK_POSITION * 100)}%\n"
                f"  • 止损位上移至5日均线（约{current * 0.95:.2f}）\n"
                f"  • 若收盘跌破5日均线，减仓50%\n"
                f"⚠️ 注意：偏离率>10%时考虑部分止盈\n"
            )
        # 子条件2：持续站稳（价格维持在均线上）
        else:
            # 场景A：偏离率≤+8%（趋势稳健）
            if deviation <= 8.0:
                # 添加M头/头肩顶形态检测
                pattern_msg = ""
                if pattern_detection["detected"]:
                    pattern_name = pattern_detection["pattern_type"]
                    confidence = pattern_detection["confidence"]
                    if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                        pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），建议减仓10%-15%"
                    elif confidence >= 0.5:
                        pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓5%-10%"
                
                message = (
                    f"【趋势稳健】连续{consecutive}天站上40日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 持仓不动，不新增仓位\n"
                    f"  • 跟踪止损上移至5日均线（约{current * 0.95:.2f}）\n"
                    f"  • 若收盘跌破5日均线，减仓50%\n"
                    f"{pattern_msg}\n"
                )
            # 场景B：+8%＜偏离率≤+15%（趋势较强）
            elif 8.0 < deviation <= 15.0:
                # 添加M头/头肩顶形态检测
                pattern_msg = ""
                if pattern_detection["detected"]:
                    pattern_name = pattern_detection["pattern_type"]
                    confidence = pattern_detection["confidence"]
                    if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                        pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），立即减仓10%-15%"
                    elif confidence >= 0.5:
                        pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓5%-10%"
                
                message = (
                    f"【趋势较强】连续{consecutive}天站上40日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 观望，不新增仓位\n"
                    f"  • 逢高减仓10%-15%\n"
                    f"  • 若收盘跌破10日均线，减仓30%\n"
                    f"{pattern_msg}\n"
                )
            # 场景C：偏离率＞+15%（超买风险）
            else:
                # 添加M头/头肩顶形态检测
                pattern_msg = ""
                if pattern_detection["detected"]:
                    pattern_name = pattern_detection["pattern_type"]
                    confidence = pattern_detection["confidence"]
                    if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                        pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），立即减仓20%-30%"
                    elif confidence >= 0.5:
                        pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓15%-25%"
                
                message = (
                    f"【超买风险】连续{consecutive}天站上40日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 逢高减仓20%-30%\n"
                    f"  • 当前价格已处高位，避免新增仓位\n"
                    f"  • 等待偏离率回落至≤+8%（约{critical * 1.08:.2f}）时加回\n"
                    f"{pattern_msg}\n"
                )
    
    # 2. NO信号：当前价格 ＜ 40日均线
    else:
        # 子条件1：首次跌破（价格刚跌穿均线，连续1-2日未收回+成交量放大）
        if consecutive == 1 and volume_change > 35:
            message = (
                f"【首次跌破】连续{consecutive}天跌破40日均线，成交量放大{volume_change:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 立即减仓{int(MAX_STOCK_POSITION * 100 * 0.7)}%\n"
                f"  • 止损位：40日均线上方5%（约{critical * 1.05:.2f}）\n"
                f"⚠️ 若收盘未收回均线，明日继续减仓至{int(MAX_STOCK_POSITION * 100 * 0.3)}%\n"
            )
        # 子条件1：首次跌破（价格刚跌穿均线，连续2-3日未收回+成交量放大）
        elif 2 <= consecutive <= 3 and volume_change > 35:
            message = (
                f"【首次跌破确认】连续{consecutive}天跌破40日均线，成交量放大{volume_change:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 严格止损，仓位降至{int(MAX_STOCK_POSITION * 100 * 0.3)}%\n"
                f"  • 止损位：40日均线下方5%（约{critical * 0.95:.2f}）\n"
                f"⚠️ 信号确认，避免侥幸心理\n"
            )
        # 子条件2：持续跌破（价格维持在均线下）
        else:
            # 场景A：偏离率≥-8%（下跌初期）
            if deviation >= -8.0:
                message = (
                    f"【下跌初期】连续{consecutive}天跌破40日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 轻仓观望（仓位≤{int(MAX_STOCK_POSITION * 100 * 0.3)}%）\n"
                    f"  • 反弹至均线附近（约{critical:.2f}）减仓剩余仓位\n"
                    f"  • 暂不考虑新增仓位\n"
                    f"⚠️ 重点观察：收盘站上5日均线，可轻仓试多\n"
                )
            # 场景B：-15%≤偏离率＜-8%（下跌中期）
            elif -15.0 <= deviation < -8.0:
                message = (
                    f"【下跌中期】连续{consecutive}天跌破40日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 空仓为主，避免抄底\n"
                    f"  • 仅可试仓{int(MAX_STOCK_POSITION * 100 * 0.1)}%\n"
                    f"  • 严格止损：收盘跌破前低即离场\n"
                    f"⚠️ 重点观察：行业基本面是否有利空\n"
                )
            # 场景C：偏离率＜-15%（超卖机会）
            else:
                message = (
                    f"【超卖机会】连续{consecutive}天跌破40日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 小幅加仓{int(MAX_STOCK_POSITION * 100 * 0.1)}%\n"
                    f"  • 目标价：偏离率≥-8%（约{critical * 0.92:.2f}）\n"
                    f"  • 达到目标即卖出加仓部分\n"
                    f"⚠️ 重点观察：若跌破前低，立即止损\n"
                )
    
    return message

def get_top_stocks_for_strategy(n: int = 10) -> List[Dict]:
    """
    获取适合策略的前n只股票
    
    Args:
        n: 返回股票数量
    
    Returns:
        List[Dict]: 股票信息列表
    """
    try:
        # 1. 获取全市场股票列表
        all_stocks = load_all_stock_list()
        logger.info(f"获取全市场股票列表，共 {len(all_stocks)} 只股票")
        
        # 2. 筛选符合条件的股票
        suitable_stocks = []
        for _, stock in all_stocks.iterrows():
            stock_code = str(stock["股票代码"])
            stock_name = stock["股票名称"]
            
            # 加载日线数据
            df = load_stock_daily_data(stock_code)
            if df.empty or len(df) < CRITICAL_VALUE_DAYS + 30:
                continue
            
            # 检查是否适合策略
            if is_stock_suitable(stock_code, df):
                # 计算策略得分
                score = calculate_stock_strategy_score(stock_code, df)
                if score > 0:
                    suitable_stocks.append({
                        "code": stock_code,
                        "name": stock_name,
                        "score": score,
                        "df": df
                    })
            
            # 限制请求频率
            time.sleep(0.1)
        
        logger.info(f"筛选后符合条件的股票数量: {len(suitable_stocks)}")
        
        # 3. 按策略得分排序
        suitable_stocks.sort(key=lambda x: x["score"], reverse=True)
        
        # 4. 返回前n只
        return suitable_stocks[:n]
    
    except Exception as e:
        logger.error(f"获取优质股票列表失败: {str(e)}", exc_info=True)
        return []

def generate_report():
    """生成个股策略报告并推送微信"""
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始生成个股策略报告 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 1. 获取适合策略的前10只股票
        top_stocks = get_top_stocks_for_strategy(n=10)
        if not top_stocks:
            warning_msg = "无符合条件的个股，无法生成策略报告"
            logger.warning(warning_msg)
            send_wechat_message(warning_msg, message_type="error")
            return
        
        # 2. 生成每只股票的策略信号
        stock_reports = []
        for stock in top_stocks:
            stock_code = stock["code"]
            stock_name = stock["name"]
            df = stock["df"]
            
            # 计算最新数据
            latest_data = df.iloc[-1]
            close_price = latest_data["收盘"]
            critical_value = calculate_critical_value(df)
            deviation = calculate_deviation(close_price, critical_value)
            
            # 状态判断（收盘价在临界值之上为YES，否则为NO）
            status = "YES" if close_price >= critical_value else "NO"
            
            # 生成详细策略信号
            signal_message = generate_stock_signal_message(
                {"code": stock_code, "name": stock_name}, 
                df, 
                close_price, 
                critical_value, 
                deviation
            )
            
            # 构建消息
            message_lines = []
            message_lines.append(f"{stock_name}({stock_code})\n")
            message_lines.append(f"📊 当前：{close_price:.2f} | 40日均线：{critical_value:.2f} | 偏离率：{deviation:.2f}%\n")
            # 根据信号类型选择正确的符号
            signal_symbol = "✅" if status == "YES" else "❌"
            message_lines.append(f"{signal_symbol} 信号：{status}\n")
            message_lines.append("──────────────────\n")
            message_lines.append(signal_message)
            message_lines.append("──────────────────\n")
            
            message = "\n".join(message_lines)
            stock_reports.append({
                "stock": f"{stock_name}({stock_code})",
                "message": message,
                "status": status,
                "deviation": deviation
            })
            
            # 发送单只股票消息
            logger.info(f"推送 {stock_name}({stock_code}) 策略信号")
            send_wechat_message(message)
            time.sleep(1)
        
        # 3. 生成总结消息
        summary_lines = [
            "【今日个股趋势策略总结】\n",
            f"📅 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n",
            f"📊 策略筛选: 流动性>1亿 | 波动率<40% | 市值>200亿\n",
            "──────────────────\n"
        ]
        
        # 按信号类型分类
        yes_signals = [r for r in stock_reports if r["status"] == "YES"]
        no_signals = [r for r in stock_reports if r["status"] == "NO"]
        
        # 添加YES信号股票
        if yes_signals:
            summary_lines.append("✅ 上涨趋势 (YES信号):\n")
            for r in yes_signals:
                summary_lines.append(f"  • {r['stock']} | 偏离率: {r['deviation']:.2f}%\n")
            summary_lines.append("\n")
        
        # 添加NO信号股票
        if no_signals:
            summary_lines.append("❌ 下跌趋势 (NO信号):\n")
            for r in no_signals:
                summary_lines.append(f"  • {r['stock']} | 偏离率: {r['deviation']:.2f}%\n")
            summary_lines.append("\n")
        
        summary_lines.append("──────────────────\n")
        summary_lines.append("💡 操作指南:\n")
        summary_lines.append("1. YES信号: 可持仓或建仓，严格止损\n")
        summary_lines.append("2. NO信号: 减仓或观望，避免盲目抄底\n")
        summary_lines.append("3. 震荡市: 高抛低吸，控制总仓位≤40%\n")
        summary_lines.append("4. 单一个股仓位≤15%，分散投资5-8只\n")
        summary_lines.append("──────────────────\n")
        summary_lines.append("📊 数据来源: fish-etf (https://github.com/karmyshunde-sudo/fish-etf)\n")
        
        summary_message = "\n".join(summary_lines)
        
        # 4. 发送总结消息
        logger.info("推送个股策略总结消息")
        send_wechat_message(summary_message)
        
        logger.info(f"个股策略报告已成功发送至企业微信（共{len(top_stocks)}只股票）")
    
    except Exception as e:
        error_msg = f"个股策略执行失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(error_msg, message_type="error")

if __name__ == "__main__":
    logger.info("===== 开始执行个股趋势策略(TickTen) =====")
    
    # 添加延时，避免AkShare接口可能还未更新当日数据
    time.sleep(30)
    
    generate_report()
    logger.info("===== 个股趋势策略(TickTen)执行完成 =====")
