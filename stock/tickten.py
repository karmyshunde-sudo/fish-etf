#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【每版块8只股票MA20】趋势策略 (TickTen)
严格使用中文列名，与日线数据文件格式保持一致
直接使用仓库中已有的数据，不进行任何自动补全
"""
import os
import logging
import pandas as pd
import time
import random
import numpy as np
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time
from wechat_push.push import send_wechat_message
import sys
import traceback
import subprocess
# 【关键修复】导入Git工具函数
from utils.git_utils import commit_files_in_batches

# 配置日志
logger = logging.getLogger(__name__)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_stocks.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 策略参数
CRITICAL_VALUE_DAYS = 20  # 计算临界值的周期（20日均线）
DEVIATION_THRESHOLD = 0.02  # 偏离阈值（2%）
PATTERN_CONFIDENCE_THRESHOLD = 0.7  # 形态确认阈值（70%置信度）
MAX_AGE_DAYS = 7  # 基础信息文件最大有效天数

# 全局变量：市值过滤开关
ENABLE_MARKET_VALUE_FILTER = True

def ensure_directory_exists():
    """确保数据目录存在"""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(DAILY_DIR):
        os.makedirs(DAILY_DIR)
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def is_file_expired(file_path, max_age_days=MAX_AGE_DAYS):
    """检查文件是否过期（超过指定天数）"""
    if not os.path.exists(file_path):
        return True
    # 获取文件最后修改时间
    mtime = os.path.getmtime(file_path)
    mtime_date = datetime.fromtimestamp(mtime)
    # 检查是否超过指定天数
    return (datetime.now() - mtime_date).days > max_age_days

def get_stock_section(stock_code: str) -> str:
    """
    获取股票所属板块
    Args:
        stock_code: 股票代码（不带市场前缀）
    Returns:
        str: 板块名称
    """
    # 确保股票代码是字符串
    stock_code = str(stock_code).zfill(6)
    # 移除可能的市场前缀
    if stock_code.lower().startswith(('sh', 'sz')):
        stock_code = stock_code[2:]
    # 确保股票代码是6位数字
    stock_code = stock_code.zfill(6)
    # 根据股票代码前缀判断板块
    if stock_code.startswith('60'):
        return "沪市主板"
    elif stock_code.startswith('00'):
        return "深市主板"
    elif stock_code.startswith('30'):
        return "创业板"
    elif stock_code.startswith('688'):
        return "科创板"
    else:
        return "其他板块"

def load_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """从本地加载股票日线数据，严格使用中文列名"""
    try:
        # 确保股票代码是字符串，并且是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        # 日线数据目录
        daily_dir = DAILY_DIR
        # 检查本地是否有历史数据
        file_path = os.path.join(daily_dir, f"{stock_code}.csv")
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                # 严格检查中文列名
                required_columns = ["日期", "股票代码", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
                for col in required_columns:
                    if col not in df.columns:
                        logger.error(f"股票 {stock_code} 数据缺少必要列: {col}")
                        return pd.DataFrame()
                # 【日期datetime类型规则】确保日期列是datetime类型
                if "日期" in df.columns:
                    df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
                    # 移除可能存在的空格
                    df = df.sort_values("日期", ascending=True)
                # 确保数值列是数值类型
                numeric_columns = ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                # 移除NaN值
                df = df.dropna(subset=['收盘', '成交量'])
                logger.debug(f"成功加载股票 {stock_code} 的本地日线数据，共 {len(df)} 条有效记录")
                return df
            except Exception as e:
                logger.warning(f"读取股票 {stock_code} 数据失败: {str(e)}")
                logger.debug(traceback.format_exc())
        logger.warning(f"股票 {stock_code} 的日线数据不存在")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def calculate_critical_value(df: pd.DataFrame) -> float:
    """计算临界值（20日均线）"""
    if len(df) < CRITICAL_VALUE_DAYS:
        logger.warning(f"数据不足{CRITICAL_VALUE_DAYS}天，无法准确计算临界值")
        return df["收盘"].mean() if not df.empty else 0.0
    return df['收盘'].rolling(window=CRITICAL_VALUE_DAYS).mean().iloc[-1]

def calculate_deviation(current: float, critical: float) -> float:
    """计算偏离率"""
    return (current - critical) / critical * 100

def calculate_consecutive_days_above(df: pd.DataFrame, critical_value: float) -> int:
    """计算连续站上均线的天数"""
    if len(df) < 2:
        return 0
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS).mean().values
    # 从最新日期开始向前检查
    consecutive_days = 0
    for i in range(len(close_prices)-1, -1, -1):
        if i < CRITICAL_VALUE_DAYS - 1:
            break
        if close_prices[i] >= ma_values[i]:
            consecutive_days += 1
        else:
            break
    return consecutive_days

def calculate_consecutive_days_below(df: pd.DataFrame, critical_value: float) -> int:
    """计算连续跌破均线的天数"""
    if len(df) < 2:
        return 0
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS).mean().values
    # 从最新日期开始向前检查
    consecutive_days = 0
    for i in range(len(close_prices)-1, -1, -1):
        if i < CRITICAL_VALUE_DAYS - 1:
            break
        if close_prices[i] < ma_values[i]:
            consecutive_days += 1
        else:
            break
    return consecutive_days

def calculate_volume_change(df: pd.DataFrame) -> float:
    """
    计算成交量变化率
    Args:
        df: ETF日线数据
    Returns:
        float: 成交量变化率（当前成交量相比前一日的变化百分比）
    """
    try:
        if len(df) < 2:
            logger.warning("数据量不足，无法计算成交量变化")
            return 0.0
        # 获取最新两个交易日的成交量
        current_volume = df['成交量'].values[-1]
        previous_volume = df['成交量'].values[-2]
        # 确保是数值类型
        if not isinstance(current_volume, (int, float)) or not isinstance(previous_volume, (int, float)):
            # 尝试转换为浮点数
            try:
                current_volume = float(current_volume)
                previous_volume = float(previous_volume)
            except:
                logger.warning("成交量数据类型错误")
                return 0.0
        # 计算变化率
        if previous_volume > 0:
            volume_change = (current_volume - previous_volume) / previous_volume
            return volume_change
        else:
            return 0.0
    except Exception as e:
        logger.error(f"计算成交量变化失败: {str(e)}", exc_info=True)
        return 0.0

def calculate_loss_percentage(df: pd.DataFrame) -> float:
    """计算当前亏损比例（相对于最近一次买入点）"""
    if len(df) < 2:
        return 0.0
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS).mean().values
    # 从最新日期开始向前检查，找到最近一次站上均线的点
    buy_index = -1
    for i in range(1, len(close_prices)):
        if i < CRITICAL_VALUE_DAYS - 1:
            continue
        if close_prices[i] >= ma_values[i]:
            buy_index = i
            break
    # 如果找不到买入点，使用30天前作为参考
    if buy_index == -1:
        buy_index = max(0, len(close_prices) - 30)
    current_price = close_prices[-1]
    buy_price = close_prices[buy_index]
    loss_percentage = (current_price - buy_price) / buy_price * 100
    return loss_percentage

def is_in_volatile_market(df: pd.DataFrame) -> tuple:
    """判断是否处于震荡市
    Returns:
        tuple: (是否震荡市, 穿越次数, 最近10天偏离率范围)
    """
    if len(df) < 10:
        return False, 0, (0, 0)
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS).mean().values
    # 检查是否连续10天在均线附近波动（-5%~+5%）
    last_10_days = df.tail(10)
    deviations = []
    for i in range(len(last_10_days)):
        # 确保有足够的数据计算均线
        if i < CRITICAL_VALUE_DAYS - 1 or np.isnan(ma_values[-10 + i]):
            continue
        deviation = (close_prices[-10 + i] - ma_values[-10 + i]) / ma_values[-10 + i] * 100
        if abs(deviation) > 5.0:
            return False, 0, (0, 0)
        deviations.append(deviation)
    # 检查价格是否反复穿越均线
    cross_count = 0
    for i in range(len(close_prices)-10, len(close_prices)-1):
        # 确保有足够的数据计算均线
        if i < CRITICAL_VALUE_DAYS - 1 or np.isnan(ma_values[i]) or np.isnan(ma_values[i+1]):
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
        # 当没有有效数据时，使用0作为默认值
        min_deviation = 0
        max_deviation = 0
    return is_volatile, cross_count, (min_deviation, max_deviation)

def detect_head_and_shoulders(df: pd.DataFrame) -> dict:
    """检测M头和头肩顶形态"""
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
        if peak2_price < peak1_price and peak2_price > peak1_price * 0.95:
            # 检查中间是否有明显低点
            trough_idx = peak1_idx + np.argmin(close_prices[peak1_idx:peak2_idx])
            trough_price = close_prices[trough_idx]
            # 检查低点是否明显
            if trough_price < peak1_price * 0.97 and trough_price < peak2_price * 0.97:
                m_top_detected = True
                # 计算置信度
                price_diff = (peak1_price - peak2_price) / peak1_price
                trough_depth = (peak1_price - trough_price) / peak1_price
                m_top_confidence = 0.5 + 0.5 * min(price_diff / 0.05, 1) + 0.5 * min(trough_depth / 0.05, 1)
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
            if shoulder_similarity > 0.85 and head_price > neckline_price * 1.1:
                head_and_shoulders_detected = True
                # 计算置信度
                shoulder_diff = 1 - shoulder_similarity
                head_height = (head_price - neckline_price) / neckline_price
                head_and_shoulders_confidence = 0.5 + 0.3 * min(shoulder_diff / 0.15, 1) + 0.2 * min(head_height / 0.15, 1)
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

def generate_signal_message(index_info: dict, df: pd.DataFrame, current: float, critical: float, deviation: float) -> str:
    """生成策略信号消息"""
    # 计算连续站上/跌破均线的天数
    consecutive_above = calculate_consecutive_days_above(df, critical)
    consecutive_below = calculate_consecutive_days_below(df, critical)
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
            f"  • 上沿操作（价格≈{upper_band:.2f}）：小幅减仓10%-20%\n"
            f"  • 下沿操作（价格≈{lower_band:.2f}）：小幅加仓10%-20%\n"
            f"  ⚠️ 总仓位严格控制在≤50%\n"
        )
        return message
    # 1. YES信号：当前价格 ≥ 20日均线
    if current >= critical:
        # 子条件1：首次突破（价格刚站上均线，连续2-3日站稳+成交量放大20%+）
        if consecutive_above == 1 and volume_change > 0.2:  # 0.2 = 20%
            message = (
                f"【首次突破】连续{consecutive_above}天站上20日均线，成交量放大{volume_change*100:.1f}%\n"
                f"✅ 操作建议：立即建仓30%\n"
                f"  • 回调至5日均线（约{current * 0.99:.2f}）可加仓20%\n"
                f"⚠️ 止损：买入价下方5%\n"
            )
        # 子条件1：首次突破（价格刚站上均线，连续2-3日站稳+成交量放大20%+）
        elif 2 <= consecutive_above <= 3 and volume_change > 0.2:
            message = (
                f"【首次突破确认】连续{consecutive_above}天站上20日均线，成交量放大{volume_change*100:.1f}%\n"
                f"✅ 操作建议：可加仓至50%\n"
                f"  • 严格跟踪5日均线作为止损位（约{current * 0.99:.2f}）\n"
                f"⚠️ 注意：若收盘跌破5日均线，立即减仓50%\n"
            )
        # 子条件2：持续站稳（价格维持在均线上）
        else:
            # 场景A：偏离率≤+5%（MA20趋势稳健）
            if deviation <= 5.0:
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
                    f"【MA20趋势稳健】连续{consecutive_above}天站上20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：持仓不动，不新增仓位\n"
                    f"  • 跟踪止损上移至5日均线（约{current * 0.99:.2f}）\n"
                    f"  • 若收盘跌破5日均线，减仓50%\n"
                    f"{pattern_msg}\n"
                )
            # 场景B：+5%＜偏离率≤+10%（MA20趋势较强）
            elif 5.0 < deviation <= 10.0:
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
                    f"【MA20趋势较强】连续{consecutive_above}天站上20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：观望，不新增仓位\n"
                    f"  • 逢高减仓10%-15%\n"
                    f"  • 若收盘跌破10日均线，减仓30%\n"
                    f"{pattern_msg}\n"
                )
            # 场景C：偏离率＞+10%（超买风险）
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
                    f"【超买风险】连续{consecutive_above}天站上20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：逢高减仓20%-30%\n"
                    f"  • 当前价格已处高位，避免新增仓位\n"
                    f"  • 等待偏离率回落至≤+5%（约{critical * 1.05:.2f}）时加回\n"
                    f"{pattern_msg}\n"
                )
    # 2. NO信号：当前价格 ＜ 20日均线
    else:
        # 计算亏损比例
        loss_percentage = calculate_loss_percentage(df)
        # 子条件1：首次跌破（价格刚跌穿均线，连续1-2日未收回+成交量放大）
        if consecutive_below == 1 and volume_change > 0.2:
            if loss_percentage > -15.0:  # 亏损<15%
                message = (
                    f"【首次跌破】连续{consecutive_below}天跌破20日均线，成交量放大{volume_change*100:.1f}%\n"
                    f"✅ 操作建议：立即减仓50%\n"
                    f"  • 止损位：20日均线上方5%（约{critical * 1.05:.2f}）\n"
                    f"⚠️ 若收盘未收回均线，明日继续减仓至20%\n"
                )
            else:  # 亏损≥15%
                message = (
                    f"【首次跌破-严重亏损】连续{consecutive_below}天跌破20日均线，成交量放大{volume_change*100:.1f}%，亏损{loss_percentage:.2f}%\n"
                    f"✅ 操作建议：立即清仓\n"
                    f"  • 保留20%-30%底仓观察\n"
                    f"  • 严格止损：收盘价站上20日均线才考虑回补\n"
                    f"⚠️ 重大亏损信号，绝不抄底\n"
                )
        # 子条件1：首次跌破（价格刚跌穿均线，连续1-2日未收回+成交量放大）
        elif consecutive_below == 2 and volume_change > 0.2:
            message = (
                f"【首次跌破确认】连续{consecutive_below}天跌破20日均线，成交量放大{volume_change*100:.1f}%\n"
                f"✅ 操作建议：严格止损清仓\n"
                f"  • 仅保留20%-30%底仓\n"
                f"  • 严格止损：20日均线下方5%（约{critical * 0.95:.2f}）\n"
                f"⚠️ 信号已确认，立即止损清仓，严禁侥幸心理\n"
            )
        # 子条件2：持续跌破（价格维持在均线下）
        else:
            # 场景A：偏离率≥-5%（下跌初期）
            if deviation >= -5.0:
                message = (
                    f"【下跌初期】连续{consecutive_below}天跌破20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：轻仓观望（仓位≤20%）\n"
                    f"  • 反弹至均线附近（约{critical:.2f}）减仓剩余仓位\n"
                    f"  • 暂不考虑新增仓位\n"
                    f"⚠️ 重点观察：收盘站上5日均线，可轻仓试多\n"
                )
            # 场景B：-10%≤偏离率＜-5%（下跌中期）
            elif -10.0 <= deviation < -5.0:
                message = (
                    f"【下跌中期】连续{consecutive_below}天跌破20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：空仓为主，避免抄底\n"
                    f"  • 可试仓5%-10%\n"
                    f"  • 严格止损：收盘跌破前低即离场\n"
                    f"⚠️ 重点观察：行业基本面是否有利空，有利空则清仓\n"
                )
            # 场景C：偏离率＜-10%（超卖机会）
            else:
                message = (
                    f"【超卖机会】连续{consecutive_below}天跌破20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：小幅加仓10%-15%\n"
                    f"  • 目标价：偏离率≥-5%（约{critical * 0.95:.2f}）\n"
                    f"  • 达到目标即卖出加仓部分\n"
                    f"⚠️ 重点观察：若跌破前低，立即止损\n"
                )
    return message

def load_stock_basic_info() -> pd.DataFrame:
    """直接加载股票基础信息，不处理文件缺失或过期情况"""
    global ENABLE_MARKET_VALUE_FILTER
    
    try:
        # 直接加载基础信息文件，不进行任何自动补全
        if not os.path.exists(BASIC_INFO_FILE):
            logger.error(f"基础信息文件不存在: {BASIC_INFO_FILE}")
            return pd.DataFrame()
        # 直接加载现有文件
        df = pd.read_csv(BASIC_INFO_FILE)
        # 严格检查中文列名
        required_columns = ["代码", "名称", "所属板块", "流通市值", "总市值", "next_crawl_index"]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"基础信息文件缺少必要列: {col}")
                return pd.DataFrame()
        # 确保股票代码是6位字符串
        df["代码"] = df["代码"].astype(str).str.zfill(6)
        # 确保流通市值列是数值类型
        df["流通市值"] = pd.to_numeric(df["流通市值"], errors='coerce')
        # 【关键修复】确保总市值列是数值类型
        df["总市值"] = pd.to_numeric(df["总市值"], errors='coerce')
        
        # === 关键修改1：市值数据完整性检查逻辑 ===
        # 检查流通市值数据完整性
        valid_market_value_count = (~df["流通市值"].isna() & (df["流通市值"] > 0)).sum()
        market_value_completion = valid_market_value_count / len(df) if len(df) > 0 else 0
        
        # 检查总市值数据完整性（仅用于监控）
        valid_total_value_count = (~df["总市值"].isna() & (df["总市值"] > 0)).sum()
        total_value_completion = valid_total_value_count / len(df) if len(df) > 0 else 0
        
        # === 关键修改2：仅基于流通市值决定是否开启过滤 ===
        if market_value_completion >= 0.7:  # 流通市值完整性达到70%就开启过滤
            ENABLE_MARKET_VALUE_FILTER = True
            logger.info(f"流通市值数据完整性达到{market_value_completion:.1%}，开启市值过滤")
        else:
            ENABLE_MARKET_VALUE_FILTER = False
            logger.warning(f"流通市值数据完整性不足{market_value_completion:.1%}，临时关闭市值过滤")
        
        # 记录总市值完整性情况（仅用于监控）
        logger.info(f"总市值数据完整性：{total_value_completion:.1%}")
        
        # 保留无流通市值股票，但标记它们
        invalid_mask = (df["流通市值"] <= 0) | df["流通市值"].isna()
        invalid_count = invalid_mask.sum()
        if invalid_count > 0:
            # 为无市值股票添加状态标记
            if '数据状态' not in df.columns:
                df['数据状态'] = '正常'
            df.loc[invalid_mask, '数据状态'] = '流通市值缺失'
            logger.warning(f"检测到 {invalid_count} 条无流通市值数据的股票，已标记为'流通市值缺失'")
        
        # 【关键修复】保留无总市值股票，但标记它们
        invalid_mask = (df["总市值"] <= 0) | df["总市值"].isna()
        invalid_count = invalid_mask.sum()
        if invalid_count > 0:
            # 为无总市值股票添加状态标记
            if '数据状态' not in df.columns:
                df['数据状态'] = '正常'
            # 如果已有标记，添加新标记
            df.loc[invalid_mask, '数据状态'] = df.loc[invalid_mask, '数据状态'].apply(
                lambda x: f"{x}, 总市值缺失" if x != '正常' else "总市值缺失"
            )
            logger.warning(f"检测到 {invalid_count} 条无总市值数据的股票，已标记为'总市值缺失'")
        
        logger.info(f"成功加载基础信息文件，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"加载股票基础信息失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def calculate_stock_strategy_score(stock_code: str, df: pd.DataFrame) -> float:
    """计算股票策略评分（更精细化的评分机制）"""
    try:
        if df is None or df.empty or len(df) < 40:
            logger.debug(f"股票 {stock_code} 数据不足，无法计算策略评分")
            return 0.0
        # 检查必要列
        required_columns = ["开盘", "最高", "最低", "收盘", "成交量"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.debug(f"股票 {stock_code} 数据缺少必要列: {', '.join(missing_columns)}，无法计算策略评分")
            return 0.0
        # 获取最新数据
        current = df["收盘"].iloc[-1]
        if pd.isna(current) or current <= 0:
            logger.debug(f"股票 {stock_code} 无效的收盘价: {current}")
            return 0.0
        # 获取股票所属板块
        section = get_stock_section(stock_code)
        # 1. 【每版块8只股票MA20】趋势指标评分 (40%)
        trend_score = 0.0
        if len(df) >= 40:
            # 计算移动平均线
            df["ma5"] = df["收盘"].rolling(window=5).mean()
            df["ma10"] = df["收盘"].rolling(window=10).mean()
            df["ma20"] = df["收盘"].rolling(window=20).mean()
            df["ma40"] = df["收盘"].rolling(window=40).mean()
            # 1.1 多头排列评分 (20分) - 基于均线间距和角度
            ma5 = df["ma5"].iloc[-1] if "ma5" in df.columns else current
            ma10 = df["ma10"].iloc[-1] if "ma10" in df.columns else current
            ma20 = df["ma20"].iloc[-1] if "ma20" in df.columns else current
            ma40 = df["ma40"].iloc[-1] if "ma40" in df.columns else current
            # 检查是否多头排列
            if not pd.isna(ma5) and not pd.isna(ma10) and not pd.isna(ma20) and not pd.isna(ma40):
                # 计算均线间距比例
                spacing_ratio_5_10 = (ma5 - ma10) / ma10 if ma10 > 0 else 0
                spacing_ratio_10_20 = (ma10 - ma20) / ma20 if ma20 > 0 else 0
                spacing_ratio_20_40 = (ma20 - ma40) / ma40 if ma40 > 0 else 0
                # 计算均线斜率
                ma5_slope = (df["ma5"].iloc[-1] - df["ma5"].iloc[-5]) / 5 if len(df) >= 5 and "ma5" in df.columns else 0
                ma10_slope = (df["ma10"].iloc[-1] - df["ma10"].iloc[-5]) / 5 if len(df) >= 5 and "ma10" in df.columns else 0
                ma20_slope = (df["ma20"].iloc[-1] - df["ma20"].iloc[-5]) / 5 if len(df) >= 5 and "ma20" in df.columns else 0
                # 多头排列强度评分 (0-20分)
                spacing_score = min(10, max(0, (spacing_ratio_5_10 + spacing_ratio_10_20 + spacing_ratio_20_40) * 100))
                slope_score = min(10, max(0, (ma5_slope + ma10_slope + ma20_slope) * 100))
                trend_score += spacing_score + slope_score
        # 1.2 价格位置评分 (10分) - 基于在20日均线上方的天数和偏离率
        if "ma20" in df.columns and len(df) >= 20:
            ma20 = df["ma20"].iloc[-1]
            if not pd.isna(ma20) and ma20 > 0:
                # 计算价格偏离率
                deviation = (current - ma20) / ma20
                # 计算连续在均线上方的天数
                above_ma_days = 0
                for i in range(1, min(20, len(df))):
                    if df["收盘"].iloc[-i] > df["ma20"].iloc[-i]:
                        above_ma_days += 1
                    else:
                        break
                # 价格位置评分 (0-10分)
                deviation_score = max(0, min(5, 5 - abs(deviation) * 50))  # 理想偏离率在0-2%之间
                days_score = min(5, above_ma_days * 0.5)  # 每多一天加0.5分，最多5分
                trend_score += deviation_score + days_score
        # 1.3 【每版块8只股票MA20】趋势强度评分 (10分) - 基于20日涨幅和趋势稳定性
        if len(df) >= 20:
            price_change_20 = (current - df["收盘"].iloc[-20]) / df["收盘"].iloc[-20] * 100
            # 计算【每版块8只股票MA20】趋势稳定性 (价格在20日均线之上的比例)
            above_ma_ratio = 0
            if "ma20" in df.columns:
                above_ma_ratio = sum(1 for i in range(20) if df["收盘"].iloc[-i-1] > df["ma20"].iloc[-i-1]) / 20
            # 【每版块8只股票MA20】趋势强度评分 (0-10分)
            change_score = min(7, max(0, price_change_20 * 0.2))  # 每1%涨幅得0.2分，最高7分
            stability_score = min(3, above_ma_ratio * 3)  # 稳定性最高3分
            trend_score += change_score + stability_score
        # 2. 动量指标评分 (20%)
        momentum_score = 0.0
        # 计算MACD
        if "收盘" in df.columns:
            df["ema12"] = df["收盘"].ewm(span=12, adjust=False).mean()
            df["ema26"] = df["收盘"].ewm(span=26, adjust=False).mean()
            df["macd"] = df["ema12"] - df["ema26"]
            df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
            df["hist"] = df["macd"] - df["signal"]
        # 2.1 MACD评分 (10分) - 基于柱状体增长和正值大小
        if "hist" in df.columns and len(df) >= 2:
            macd_hist = df["hist"].iloc[-1]
            macd_hist_prev = df["hist"].iloc[-2]
            # MACD柱状体增长评分
            if not pd.isna(macd_hist) and not pd.isna(macd_hist_prev):
                growth_rate = (macd_hist - macd_hist_prev) / abs(macd_hist_prev) if macd_hist_prev != 0 else 1
                # 增长率评分 (0-5分)
                growth_score = min(5, max(0, growth_rate * 100))
                # 正值大小评分 (0-5分)
                positive_score = min(5, max(0, macd_hist * 10))
                momentum_score += growth_score + positive_score
        # 2.2 价格动量评分 (10分) - 基于短期价格动量
        if len(df) >= 5:
            price_momentum_5 = (current - df["收盘"].iloc[-5]) / df["收盘"].iloc[-5] * 100
            # 价格动量评分 (0-10分)
            momentum_score += min(10, max(0, price_momentum_5 * 0.5))
        # 3. 流动性指标评分 (20%)
        liquidity_score = 0.0
        if "成交量" in df.columns and len(df) >= 20:
            # 3.1 日均成交量评分 (10分)
            avg_volume = df["成交量"].rolling(window=20).mean().iloc[-1]
            # 换手率评分
            turnover_score = min(10, max(0, avg_volume * 0.0001))
            liquidity_score += turnover_score
        # 4. 波动率指标评分 (20%)
        volatility_score = 0.0
        if len(df) >= 20:
            # 计算20日波动率
            daily_returns = df["收盘"].pct_change().dropna()
            volatility = daily_returns.rolling(window=20).std().iloc[-1]
            # 波动率评分 (0-10分)
            volatility_score += min(10, max(0, (1 - volatility) * 100))
            # 波动率稳定性评分 (0-10分)
            volatility_std = daily_returns.rolling(window=20).std().std()
            volatility_stability_score = min(10, max(0, (1 - volatility_std) * 100))
            volatility_score += volatility_stability_score
        # 5. 综合评分 (100分制)
        total_score = trend_score + momentum_score + liquidity_score + volatility_score
        logger.debug(f"股票 {stock_code} 策略评分: {total_score:.2f}")
        return total_score
    except Exception as e:
        logger.error(f"计算股票 {stock_code} 策略评分失败: {str(e)}", exc_info=True)
        return 0.0

def filter_valid_stocks(basic_info_df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤出符合策略要求的有效股票
    这是tickten.py的核心筛选逻辑
    Args:
        basic_info_df: 原始股票基础信息DataFrame
    Returns:
        pd.DataFrame: 过滤后的有效股票DataFrame
    """
    global ENABLE_MARKET_VALUE_FILTER
    
    logger.info("=== 正在筛选符合策略要求的有效股票 ===")
    # 创建副本，避免修改原始数据
    filtered_df = basic_info_df.copy()
    
    # 1. 排除新上市股票（名称以"N"开头）
    initial_count = len(filtered_df)
    filtered_df = filtered_df[~filtered_df["名称"].str.startswith("N")]
    removed = initial_count - len(filtered_df)
    if removed > 0:
        logger.info(f"已排除 {removed} 只新上市股票（名称以'N'开头）")
    
    # 2. 排除ST股票
    initial_count = len(filtered_df)
    filtered_df = filtered_df[~filtered_df["名称"].str.contains("ST|退[市休]", na=False)]
    removed = initial_count - len(filtered_df)
    if removed > 0:
        logger.info(f"已排除 {removed} 只ST和退市股票")
    
    # === 关键修改3：市值过滤逻辑 ===
    # 3. 排除市值过小的股票（根据开关决定是否执行）
    if ENABLE_MARKET_VALUE_FILTER:
        initial_count = len(filtered_df)
        # 只检查流通市值，忽略总市值
        filtered_df = filtered_df[
            (~filtered_df["流通市值"].isna()) & 
            (filtered_df["流通市值"] >= 5e8)
        ]
        removed = initial_count - len(filtered_df)
        if removed > 0:
            logger.info(f"已排除 {removed} 只小市值股票（流通市值<5亿）")
    else:
        logger.info("⚠️ 流通市值过滤已关闭，跳过市值筛选")
    
    # 4. 排除非主板/科创板/创业板股票
    initial_count = len(filtered_df)
    filtered_df = filtered_df[filtered_df["代码"].str.startswith(("00", "30", "60", "688"))]
    removed = initial_count - len(filtered_df)
    if removed > 0:
        logger.info(f"已排除 {removed} 只非目标板块股票")
    
    logger.info(f"筛选完成，共 {len(filtered_df)} 只有效股票")
    return filtered_df

def get_top_stocks_for_strategy() -> dict:
    """获取各板块中适合策略的股票（直接使用本地已有数据）"""
    try:
        logger.info("=== 开始执行【每版块8只股票MA20】(TickTen) ===")
        # 1. 直接获取股票基础信息（不进行任何自动补全）
        basic_info_df = load_stock_basic_info()
        if basic_info_df.empty:
            logger.error("基础信息文件不存在或加载失败，无法继续")
            return {}
        logger.info(f"已加载股票基础信息，共 {len(basic_info_df)} 条记录")
        # 3. 按板块分组处理
        section_stocks = {
            "沪市主板": [],
            "深市主板": [],
            "创业板": [],
            "科创板": [],
            "其他板块": []
        }
        # 4. 处理每只股票
        stock_list = basic_info_df.to_dict('records')
        logger.info(f"开始处理 {len(stock_list)} 只股票...")
        # 确保所有股票代码是字符串格式（6位，前面补零）
        for stock in stock_list:
            stock["代码"] = str(stock["代码"]).zfill(6)
        logger.info(f"今天实际处理 {len(stock_list)} 只股票（完整处理）")
        def process_stock(stock):
            stock_code = stock["代码"]
            stock_name = stock["名称"]
            section = stock["所属板块"]
            # 检查板块是否有效
            if section not in section_stocks:
                section = "其他板块"
            # 2. 获取日线数据（从本地加载）
            df = load_stock_daily_data(stock_code)
            # 3. 检查数据完整性
            if df.empty or len(df) < 40:
                logger.debug(f"股票 {stock_code} 数据不完整，跳过")
                return None
            
            # === 关键修改4：股票处理逻辑 ===
            # 只有当市值过滤开启且流通市值确实缺失时才跳过
            if ENABLE_MARKET_VALUE_FILTER:
                # 检查流通市值是否有效
                circulating_market_cap = stock.get("流通市值", 0)
                if pd.isna(circulating_market_cap) or circulating_market_cap <= 0:
                    logger.debug(f"股票 {stock_code} 流通市值数据无效，跳过")
                    return None
            
            # 5. 计算策略评分
            score = calculate_stock_strategy_score(stock_code, df)
            if score > 0:
                return {
                    "code": stock_code,
                    "name": stock_name,
                    "score": score,
                    "df": df,
                    "section": section
                }
            return None
        # 5. 处理股票
        results = []
        for stock in stock_list:
            result = process_stock(stock)
            if result is not None:
                section_stocks[result["section"]].append(result)
        # 6. 对每个板块的股票按评分排序，并取前8只
        top_stocks_by_section = {}
        for section, stocks in section_stocks.items():
            if stocks:
                stocks.sort(key=lambda x: x["score"], reverse=True)
                top_stocks = stocks[:8]  # 每个板块最多8只
                top_stocks_by_section[section] = top_stocks
                logger.info(f"【最终结果】板块 {section} 筛选出 {len(top_stocks)} 只股票")
                # 记录筛选出的股票详情
                for i, stock in enumerate(top_stocks):
                    logger.info(f"  {i+1}. {stock['name']}({stock['code']}) - 评分: {stock['score']:.2f}")
            else:
                logger.info(f"【最终结果】板块 {section} 无符合条件的股票")
        return top_stocks_by_section
    except Exception as e:
        logger.error(f"获取优质股票列表失败: {str(e)}", exc_info=True)
        logger.error(traceback.format_exc())
        return {}

# 同时需要修改 save_and_commit_stock_codes 函数，让它返回文件路径
def save_and_commit_stock_codes(top_stocks):
    """保存股票代码到文件并提交到Git仓库，返回文件路径"""
    try:
        # 获取当前时间
        now = get_beijing_time()
        timestamp = now.strftime("%Y%m%d%H%M")
        filename = f"tick{timestamp}.txt"
        # 构建文件路径
        stock_dir = os.path.join(DATA_DIR, "stock")
        if not os.path.exists(stock_dir):
            os.makedirs(stock_dir, exist_ok=True)
        file_path = os.path.join(stock_dir, filename)
        # 收集所有股票代码
        all_stock_codes = set()
        # 从所有板块中收集股票代码
        for section, stocks in top_stocks.items():
            for stock in stocks:
                # 确保是6位股票代码
                code = str(stock['code']).zfill(6)
                all_stock_codes.add(code)
        # 保存到文件（ANSI编码，使用ASCII，因为股票代码是纯数字）
        with open(file_path, 'w', encoding='ascii') as f:
            for code in sorted(all_stock_codes):
                f.write(code + '\n')
        logger.info(f"✅ 已保存股票代码到 {file_path}")
        logger.info(f"文件内容预览: {list(all_stock_codes)[:5]}... (共{len(all_stock_codes)}个代码)")
        # 【关键修复】使用 git_utils 提交文件到Git仓库 - 标记为LAST_FILE
        logger.info("=== 开始Git提交流程 ===")
        # 标记为LAST_FILE确保立即提交（不等待批量阈值）
        success = commit_files_in_batches(file_path, "LAST_FILE")
        if success:
            logger.info(f"✅ 成功提交文件到Git仓库: {file_path}")
            # 添加提交后验证
            repo_root = os.getenv('GITHUB_WORKSPACE', '.')
            relative_path = os.path.relpath(file_path, repo_root)
            git_status = subprocess.run(
                ['git', 'status', '--porcelain', relative_path],
                cwd=repo_root,
                capture_output=True,
                text=True
            )
            if not git_status.stdout.strip():
                logger.info("✅ 文件已成功提交到远程仓库")
            else:
                logger.error("❌ 文件未完全提交到远程仓库")
                # 尝试强制提交剩余文件
                from utils.git_utils import force_commit_remaining_files
                force_commit_remaining_files()
        else:
            logger.error(f"❌ 提交文件到Git仓库失败: {file_path}")
            # 添加详细的诊断信息
            repo_root = os.getenv('GITHUB_WORKSPACE', '.')
            logger.info(f"当前工作目录: {os.getcwd()}")
            logger.info(f"仓库根目录: {repo_root}")
            # 检查Git状态
            git_status = subprocess.run(
                ['git', 'status'],
                cwd=repo_root,
                capture_output=True,
                text=True
            )
            logger.info(f"Git状态:\n{git_status.stdout}")
            # 检查暂存区
            git_diff = subprocess.run(
                ['git', 'diff', '--cached'],
                cwd=repo_root,
                capture_output=True,
                text=True
            )
            logger.info(f"暂存区差异:\n{git_diff.stdout}")
            # 发送错误通知
            error_msg = f"❌ 股票筛选结果文件提交失败: {filename}\n请立即检查系统"
            send_wechat_message(message=error_msg, message_type="error")
        
        # 【新增】返回文件路径
        return file_path
        
    except Exception as e:
        logger.error(f"❌ 保存股票代码文件失败: {str(e)}", exc_info=True)
        error_msg = f"❌ 股票筛选结果文件保存失败: {str(e)}"
        send_wechat_message(message=error_msg, message_type="error")
        return None

def generate_strategy_report():
    """生成策略报告并发送微信通知"""
    try:
        beijing_time = get_beijing_time()
        logger.info(f"北京时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
        # 获取优质股票列表
        top_stocks = get_top_stocks_for_strategy()
        if not top_stocks:
            logger.warning("没有找到符合条件的股票")
            status_message = (
                f"=== 【每版块8只股票MA20】状态报告 ===\n"
                f"时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"策略状态：未找到符合条件的股票\n"
                f"流通市值过滤：{'已开启' if ENABLE_MARKET_VALUE_FILTER else '已关闭'}\n"
                f"可能原因：\n"
                f"  • 市场整体技术信号不佳\n"
                f"  • 市值过滤条件过于严格\n"
                f"  • 数据完整性不足\n"
            )
            send_wechat_message(status_message)
            return
        
        # 【关键修改】在推送消息前，保存股票代码到txt文件
        file_path = save_and_commit_stock_codes(top_stocks)
        
        # 【新增】发送txt文件内容
        send_txt_file_content(file_path, beijing_time)
        
        # 【关键修改】按板块分组生成多个消息
        section_messages = []
        # 生成每个板块的消息
        for section, stocks in top_stocks.items():
            if stocks:
                report = []
                report.append(f"=== 【每版块8只股票MA20】报告 - {section} ===")
                report.append(f"策略依据：20日均线+成交量变化+形态识别")
                # 添加市值过滤状态
                market_value_status = "已启用（流通市值数据充足）" if ENABLE_MARKET_VALUE_FILTER else "已禁用（流通市值数据不足）"
                report.append(f"流通市值过滤：{market_value_status}")
                report.append(f"【{section}】")
                for i, stock in enumerate(stocks):
                    # 生成股票信号
                    current = stock["df"]["收盘"].iloc[-1]
                    critical = calculate_critical_value(stock["df"])
                    deviation = calculate_deviation(current, critical)
                    signal_msg = generate_signal_message(stock, stock["df"], current, critical, deviation)
                    report.append(f"{'='*20}")
                    report.append(f"{i+1}. {stock['name']}({stock['code']})")
                    report.append(f"评分: {stock['score']:.2f}")
                    report.append(f"当前价: {current:.2f}")
                    report.append(f"20日均线: {critical:.2f}")
                    report.append(f"偏离率: {deviation:.2f}%")
                    report.append(signal_msg)                
                section_messages.append("\n".join(report))
        
        # 【关键修改】分别发送每个板块的消息
        if section_messages:
            for i, message in enumerate(section_messages):
                logger.info(f"推送【每版块8只股票MA20】报告 - 板块 {i+1}/{len(section_messages)}")
                send_wechat_message(message)
                # 添加延时避免消息发送过快
                time.sleep(3)
        else:
            # 如果没有板块消息，发送默认消息
            default_message = (
                f"=== 【每版块8只股票MA20】报告 ===\n"
                f"今日无符合【每版块8只股票MA20】趋势条件的股票"
            )
            send_wechat_message(default_message)
        logger.info("【每版块8只股票MA20】执行完成")
    except Exception as e:
        logger.error(f"生成MA20策略报告失败: {str(e)}", exc_info=True)
        send_wechat_message(f"❌ 【每版块8只股票MA20】执行失败: {str(e)}")

def send_txt_file_content(file_path, beijing_time):
    """读取txt文件内容并通过微信发送"""
    try:
        if not os.path.exists(file_path):
            logger.error(f"【每版块8只股票MA20】股票代码文件不存在: {file_path}")
            return
        
        # 读取文件内容
        with open(file_path, 'r', encoding='ascii') as f:
            file_content = f.read().strip()
        
        if not file_content:
            logger.warning("【每版块8只股票MA20】股票代码文件为空")
            return
        
        # 统计股票数量
        stock_codes = file_content.split('\n')
        stock_count = len(stock_codes)
        
        # 构造文件内容消息
        file_message = (
            f"📊 【每版块8只股票MA20】股票数量: {stock_count} 只\n"
            f"══════════════════\n"
            f"{file_content}\n"
            f"══════════════════\n"
            f"💡 以上为【每版块8只股票MA20】筛选所有股票代码"
        )
        
        # 发送文件内容
        logger.info(f"发送【每版块8只股票MA20】股票代码文件内容，共 {stock_count} 只股票")
        send_wechat_message(file_message)
        
    except Exception as e:
        logger.error(f"发送【每版块8只股票MA20】txt文件内容失败: {str(e)}")
        # 发送错误通知但不要中断主流程
        error_msg = f"⚠️ 【每版块8只股票MA20】股票代码文件发送失败，但策略报告已正常生成"
        send_wechat_message(error_msg)

def main():
    """主函数：执行【每版块8只股票MA20】趋势策略"""
    global ENABLE_MARKET_VALUE_FILTER
    # 重置开关，确保每次执行都重新判断
    ENABLE_MARKET_VALUE_FILTER = True
    
    logger.info("=== 开始执行【每版块8只股票MA20】趋势策略(TickTen) ===")
    # 确保目录存在
    ensure_directory_exists()
    # 获取基础信息（直接加载，不进行任何自动补全）
    basic_info = load_stock_basic_info()
    if basic_info.empty:
        logger.error("基础信息文件不存在或加载失败，策略无法执行")
        return
    # 生成并发送策略报告
    generate_strategy_report()
    logger.info("=== 【每版块8只股票MA20】趋势策略执行完成 ===")

if __name__ == "__main__":
    main()
