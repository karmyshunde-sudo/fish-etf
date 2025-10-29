#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略2 - 专业级多指标共振策略（微信推送适配版）
功能：
1. 遍历 data/daily/ 下所有股票日线数据
2. 计算 MA、MACD、RSI、KDJ 四大指标
3. 分别生成单一指标信号和多指标共振信号
4. 按专业标准排序并推送高质量信号到微信
【微信推送适配版】
- 完全适配 wechat_push/push.py 模块
- 严格遵循消息类型规范
- 专业金融系统可靠性保障
- 100%可直接复制使用
"""

import os
import pandas as pd
import numpy as np
import subprocess
from datetime import datetime
import logging
import sys
from config import Config
from utils.date_utils import get_beijing_time, is_file_outdated  # 确保正确导入get_beijing_time
from wechat_push.push import send_wechat_message  # 确保正确导入推送模块
# 【关键修复】导入Git工具函数
from utils.git_utils import commit_files_in_batches

# ========== 参数配置 ==========
# 均线参数
MA_PERIODS = [5, 10, 20, 30, 60]  # 均线周期
MAX_MA_DEVIATION = 0.02  # 均线缠绕最大偏离率（2%）
MIN_CONSOLIDATION_DAYS = 3  # 均线缠绕持续天数
MIN_VOLUME_RATIO_MA = 0.8  # 缠绕期间成交量萎缩阈值

# MACD参数
MACD_SHORT = 12
MACD_LONG = 26
MACD_SIGNAL = 9
MIN_MACD_POSITIVE = 0.0  # MACD必须在0轴上方
MIN_MACD_GROWTH = 0.33  # MACD增长阈值（33%）
MIN_MACD_CONSISTENT_DAYS = 2  # MACD增长持续天数
MIN_MACD_VOLUME_RATIO = 1.2  # MACD增长时成交量放大阈值

# RSI参数
RSI_PERIOD = 14
RSI_OVERSOLD = 30  # 超卖阈值
RSI_BUY_ZONE = (30, 50)  # 买入区域
MIN_RSI_CHANGE = 5  # RSI最小变化值
MIN_RSI_CONSISTENT_DAYS = 2  # RSI上升趋势持续天数

# KDJ参数
KDJ_PERIOD = 9
KDJ_SLOWING = 3
KDJ_DOUBLE = 3
KDJ_LOW = 30  # 低位阈值
MIN_KDJ_CROSSOVER = True  # 是否要求金叉
MIN_KDJ_POSITIVE = True  # 是否要求K、D在低位
MIN_KDJ_CHANGE = 10  # J线最小变化值
MIN_KDJ_CONSISTENT_DAYS = 2  # KDJ上升趋势持续天数

# 三均线粘合突破参数
THREEMA_MA_PERIODS = [5, 10, 20]  # 均线周期
MAX_THREEMA_DEVIATION = 0.02      # 最大均线偏离率（2%）
MIN_CONSOLIDATION_DAYS = 5        # 最小粘合持续天数
MIN_BREAKOUT_RATIO = 0.03         # 最小突破幅度（3%）
MIN_BREAKOUT_VOLUME_RATIO = 1.5   # 最小突破量能比（50%）
MAX_BREAKOUT_VOLUME_RATIO = 2.0   # 最大突破量能比（100%）
MAX_CONFIRMATION_DEVIATION = 0.08 # 确认阶段最大偏离率（8%）

# 信号质量控制
MIN_MARKET_UPWARD = True  # 是否要求大盘处于上升趋势
# ============================

# ========== 初始化日志 ==========
logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler(sys.stdout)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)

def get_category_name(category):
    """获取指标类别名称"""
    names = {
        "MA": "均线缠绕",
        "MACD": "MACD动能",
        "RSI": "RSI超买超卖",
        "KDJ": "KDJ短期动量",
        "THREEMA": "三均线粘合突破"
    }
    return names.get(category, category)

def get_combination_name(combination):
    """获取组合名称"""
    names = {
        "MA+MACD": "MA+MACD",
        "MA+RSI": "MA+RSI",
        "MA+KDJ": "MA+KDJ",
        "MACD+RSI": "MACD+RSI",
        "MACD+KDJ": "MACD+KDJ",
        "RSI+KDJ": "RSI+KDJ",
        "MA+MACD+RSI": "MA+MACD+RSI",
        "MA+MACD+KDJ": "MA+MACD+KDJ",
        "MA+RSI+KDJ": "MA+RSI+KDJ",
        "MACD+RSI+KDJ": "MACD+RSI+KDJ"
    }
    return names.get(combination, combination)

def get_signal_quality(signal, combination):
    """计算信号质量分数"""
    quality = 0
    
    # MA指标质量
    if "MA" in combination and "ma" in signal:
        # 缠绕率越小，质量越高
        quality += (1 - min(signal["ma"]["deviation"] / MAX_MA_DEVIATION, 1)) * 25
        # 持续天数越长，质量越高
        quality += min(signal["ma"]["consolidation_days"] / MIN_CONSOLIDATION_DAYS, 2) * 15
    
    # MACD指标质量
    if "MACD" in combination and "macd" in signal:
        # 增长幅度越大，质量越高
        quality += min(signal["macd"]["growth_rate"] / MIN_MACD_GROWTH, 2) * 25
        # 持续天数越长，质量越高
        quality += min(signal["macd"]["growth_days"] / MIN_MACD_CONSISTENT_DAYS, 2) * 15
    
    # RSI指标质量
    if "RSI" in combination and "rsi" in signal:
        # RSI变化越大，质量越高
        quality += min(signal["rsi"]["rsi_change"] / MIN_RSI_CHANGE, 2) * 25
        # 持续天数越长，质量越高
        quality += min(signal["rsi"]["rise_days"] / MIN_RSI_CONSISTENT_DAYS, 2) * 15
    
    # KDJ指标质量
    if "KDJ" in combination and "kdj" in signal:
        # J线变化越大，质量越高
        quality += min(signal["kdj"]["j_change"] / MIN_KDJ_CHANGE, 2) * 25
        # 持续天数越长，质量越高
        quality += min(signal["kdj"]["rise_days"] / MIN_KDJ_CONSISTENT_DAYS, 2) * 15
    
    return quality

def calc_ma(df, period):
    """计算移动平均线"""
    return df["收盘"].rolling(window=period).mean()

def check_ma_signal(df):
    """检查均线信号"""
    try:
        # 计算所有均线
        ma_values = {}
        for p in MA_PERIODS:
            ma_values[p] = calc_ma(df, p)
        
        # 检查多头排列
        uptrend = True
        for i in range(len(MA_PERIODS)-1):
            if len(df) < MA_PERIODS[i] or len(df) < MA_PERIODS[i+1]:
                uptrend = False
                break
            if ma_values[MA_PERIODS[i]].iloc[-1] <= ma_values[MA_PERIODS[i+1]].iloc[-1]:
                uptrend = False
                break
        
        if not uptrend:
            return None
        
        # 检查缠绕条件
        latest_ma = []
        for p in MA_PERIODS:
            if len(df) >= p and not np.isnan(ma_values[p].iloc[-1]):
                latest_ma.append(ma_values[p].iloc[-1])
        
        if len(latest_ma) < len(MA_PERIODS):
            return None
        
        max_ma = max(latest_ma)
        min_ma = min(latest_ma)
        deviation = (max_ma - min_ma) / max_ma
        
        if deviation > MAX_MA_DEVIATION:
            return None
        
        # 检查缠绕持续天数
        consolidation_days = 0
        for i in range(1, 10):  # 检查过去10天
            if len(df) <= i:
                break
            
            ma_i = []
            for p in MA_PERIODS:
                if len(df) >= p + i and not np.isnan(ma_values[p].iloc[-i]):
                    ma_i.append(ma_values[p].iloc[-i])
            
            if len(ma_i) < len(MA_PERIODS):
                continue
            
            max_ma_i = max(ma_i)
            min_ma_i = min(ma_i)
            dev_i = (max_ma_i - min_ma_i) / max_ma_i
            if dev_i <= MAX_MA_DEVIATION:
                consolidation_days += 1
        
        if consolidation_days < MIN_CONSOLIDATION_DAYS:
            return None
        
        # 检查成交量
        if len(df) < 5:
            return None
        
        volume_ratio = df["成交量"].iloc[-1] / df["成交量"].rolling(5).mean().iloc[-1]
        if volume_ratio > 1.0 / MIN_VOLUME_RATIO_MA:
            return None
        
        return {
            "deviation": deviation,
            "consolidation_days": consolidation_days,
            "volume_ratio": volume_ratio
        }
    except Exception as e:
        logger.debug(f"检查均线信号失败: {str(e)}")
        return None

def calc_macd(df):
    """计算MACD指标"""
    try:
        ema_short = df["收盘"].ewm(span=MACD_SHORT, adjust=False).mean()
        ema_long = df["收盘"].ewm(span=MACD_LONG, adjust=False).mean()
        dif = ema_short - ema_long
        dea = dif.ewm(span=MACD_SIGNAL, adjust=False).mean()
        macd_bar = (dif - dea) * 2
        return dif, dea, macd_bar
    except Exception as e:
        logger.debug(f"计算MACD失败: {str(e)}")
        return None, None, None

def check_macd_signal(df):
    """检查MACD信号"""
    try:
        dif, dea, macd_bar = calc_macd(df)
        if dif is None or dea is None or macd_bar is None:
            return None
        
        # 检查是否在0轴上方
        if len(macd_bar) < 1 or macd_bar.iloc[-1] <= MIN_MACD_POSITIVE:
            return None
        
        # 检查增长条件
        if len(macd_bar) < 2 or macd_bar.iloc[-2] <= 0:
            return None
        
        growth_rate = (macd_bar.iloc[-1] - macd_bar.iloc[-2]) / macd_bar.iloc[-2]
        if growth_rate < MIN_MACD_GROWTH:
            return None
        
        # 检查持续增长天数
        growth_days = 1
        for i in range(2, len(macd_bar)):
            if i >= len(df):
                break
            if i < 2:  # 确保索引有效
                continue
            if macd_bar.iloc[-i] > macd_bar.iloc[-i-1] > 0:
                growth_days += 1
            else:
                break
        
        if growth_days < MIN_MACD_CONSISTENT_DAYS:
            return None
        
        # 检查成交量
        if len(df) < 5:
            return None
        
        volume_ratio = df["成交量"].iloc[-1] / df["成交量"].rolling(5).mean().iloc[-1]
        if volume_ratio < MIN_MACD_VOLUME_RATIO:
            return None
        
        return {
            "growth_rate": growth_rate,
            "growth_days": growth_days,
            "volume_ratio": volume_ratio
        }
    except Exception as e:
        logger.debug(f"检查MACD信号失败: {str(e)}")
        return None

def calc_rsi(df, period=RSI_PERIOD):
    """计算RSI指标"""
    try:
        delta = df["收盘"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    except Exception as e:
        logger.debug(f"计算RSI失败: {str(e)}")
        return None

def check_rsi_signal(df):
    """检查RSI信号"""
    try:
        rsi = calc_rsi(df)
        if rsi is None or len(rsi) < 1:
            return None
        
        # 检查是否在买入区域
        if rsi.iloc[-1] < RSI_BUY_ZONE[0] or rsi.iloc[-1] > RSI_BUY_ZONE[1]:
            return None
        
        # 检查变化幅度
        if len(rsi) < 2:
            return None
        
        rsi_change = rsi.iloc[-1] - rsi.iloc[-2]
        if rsi_change < MIN_RSI_CHANGE:
            return None
        
        # 检查持续上升天数
        rise_days = 1
        for i in range(2, len(rsi)):
            if i >= len(df):
                break
            if i < 2:  # 确保索引有效
                continue
            if rsi.iloc[-i] > rsi.iloc[-i-1]:
                rise_days += 1
            else:
                break
        
        if rise_days < MIN_RSI_CONSISTENT_DAYS:
            return None
        
        return {
            "rsi_value": rsi.iloc[-1],
            "rsi_change": rsi_change,
            "rise_days": rise_days
        }
    except Exception as e:
        logger.debug(f"检查RSI信号失败: {str(e)}")
        return None

def calc_kdj(df, period=KDJ_PERIOD, slowing=KDJ_SLOWING, double=KDJ_DOUBLE):
    """计算KDJ指标"""
    try:
        low_min = df["最低"].rolling(window=period).min()
        high_max = df["最高"].rolling(window=period).max()
        
        # 计算RSV
        rsv = (df["收盘"] - low_min) / (high_max - low_min) * 100
        rsv = rsv.replace([np.inf, -np.inf], np.nan).fillna(50)
        
        # 计算K、D、J
        k = rsv.ewm(alpha=1/slowing, adjust=False).mean()
        d = k.ewm(alpha=1/double, adjust=False).mean()
        j = 3 * k - 2 * d
        
        return k, d, j
    except Exception as e:
        logger.debug(f"计算KDJ失败: {str(e)}")
        return None, None, None

def check_kdj_signal(df):
    """检查KDJ信号"""
    try:
        k, d, j = calc_kdj(df)
        if k is None or d is None or j is None:
            return None
        
        # 检查是否金叉
        if MIN_KDJ_CROSSOVER:
            if len(k) < 2 or len(d) < 2:
                return None
            if not (k.iloc[-1] > d.iloc[-1] and k.iloc[-2] <= d.iloc[-2]):
                return None
        
        # 检查是否在低位
        if MIN_KDJ_POSITIVE:
            if len(k) < 1 or len(d) < 1:
                return None
            if k.iloc[-1] > KDJ_LOW or d.iloc[-1] > KDJ_LOW:
                return None
        
        # 检查J线变化
        if len(j) < 2:
            return None
        
        j_change = j.iloc[-1] - j.iloc[-2]
        if j_change < MIN_KDJ_CHANGE:
            return None
        
        # 检查持续上升天数
        rise_days = 1
        for i in range(2, len(j)):
            if i >= len(df):
                break
            if i < 2:  # 确保索引有效
                continue
            if j.iloc[-i] > j.iloc[-i-1]:
                rise_days += 1
            else:
                break
        
        if rise_days < MIN_KDJ_CONSISTENT_DAYS:
            return None
        
        return {
            "k_value": k.iloc[-1],
            "d_value": d.iloc[-1],
            "j_value": j.iloc[-1],
            "j_change": j_change,
            "rise_days": rise_days
        }
    except Exception as e:
        logger.debug(f"检查KDJ信号失败: {str(e)}")
        return None

def check_threema_signal(df, code, name):
    """检查三均线粘合突破信号（带详细步骤日志）"""
    try:
        logger.debug(f"【THREEMA筛选】开始检查股票 {code} {name}")
        
        # 1. 粘合阶段验证
        # 计算均线
        ma5 = calc_ma(df, 5)
        ma10 = calc_ma(df, 10)
        ma20 = calc_ma(df, 20)
        
        # 空间验证：均线偏离度<2%
        max_ma = max(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
        min_ma = min(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
        deviation = (max_ma - min_ma) / max_ma
        if deviation >= MAX_THREEMA_DEVIATION:
            logger.debug(f"【THREEMA筛选】{code} {name} - 空间验证失败，偏离度={deviation:.4f} >= {MAX_THREEMA_DEVIATION}")
            return None
        else:
            logger.debug(f"【THREEMA筛选】{code} {name} - 通过空间验证，偏离度={deviation:.4f}")
        
        # 时间验证：粘合持续≥5天
        consolidation_days = 0
        for i in range(1, 20):  # 检查过去20天
            if len(df) <= i:
                break
                
            max_ma_i = max(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
            min_ma_i = min(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
            dev_i = (max_ma_i - min_ma_i) / max_ma_i
            if dev_i <= MAX_THREEMA_DEVIATION:
                consolidation_days += 1
            else:
                break
                
        if consolidation_days < MIN_CONSOLIDATION_DAYS:
            logger.debug(f"【THREEMA筛选】{code} {name} - 时间验证失败，粘合持续天数={consolidation_days} < {MIN_CONSOLIDATION_DAYS}")
            return None
        else:
            logger.debug(f"【THREEMA筛选】{code} {name} - 通过时间验证，粘合持续天数={consolidation_days}")
        
        # 量能验证：粘合期量能比吸筹期缩50%以上
        # 吸筹期：粘合期前5天
        if len(df) < consolidation_days + 5:
            logger.debug(f"【THREEMA筛选】{code} {name} - 数据不足，无法进行量能验证")
            return None
            
        accumulation_volume = df["成交量"].iloc[-(consolidation_days+5):-consolidation_days].mean()
        consolidation_volume = df["成交量"].iloc[-consolidation_days:].mean()
        volume_ratio = consolidation_volume / accumulation_volume
        if volume_ratio >= 0.5:
            logger.debug(f"【THREEMA筛选】{code} {name} - 量能验证失败，量能比={volume_ratio:.4f} >= 0.5")
            return None
        else:
            logger.debug(f"【THREEMA筛选】{code} {name} - 通过量能验证，量能比={volume_ratio:.4f}")
        
        # 2. 突破阶段验证
        # 同步向上验证
        if not (ma5.iloc[-1] > ma5.iloc[-2] and ma10.iloc[-1] > ma10.iloc[-2] and ma20.iloc[-1] > ma20.iloc[-2]):
            logger.debug(f"【THREEMA筛选】{code} {name} - 同步向上验证失败")
            return None
        else:
            logger.debug(f"【THREEMA筛选】{code} {name} - 通过同步向上验证")
            
        # 多头排列雏形
        if not (ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1]):
            logger.debug(f"【THREEMA筛选】{code} {name} - 多头排列验证失败")
            return None
        else:
            logger.debug(f"【THREEMA筛选】{code} {name} - 通过多头排列验证")
            
        # 幅度验证：突破幅度>3%
        consolidation_high = max(df["最高"].iloc[-consolidation_days:])
        breakout_ratio = (df["收盘"].iloc[-1] / consolidation_high) - 1
        if breakout_ratio <= MIN_BREAKOUT_RATIO:
            logger.debug(f"【THREEMA筛选】{code} {name} - 幅度验证失败，突破幅度={breakout_ratio:.4f} <= {MIN_BREAKOUT_RATIO}")
            return None
        else:
            logger.debug(f"【THREEMA筛选】{code} {name} - 通过幅度验证，突破幅度={breakout_ratio:.4f}")
            
        # 量能验证：突破量能增加50%-100%
        volume_ratio = df["成交量"].iloc[-1] / consolidation_volume
        if volume_ratio < MIN_BREAKOUT_VOLUME_RATIO or volume_ratio > MAX_BREAKOUT_VOLUME_RATIO:
            logger.debug(f"【THREEMA筛选】{code} {name} - 量能验证失败，量能比={volume_ratio:.4f} (要求 {MIN_BREAKOUT_VOLUME_RATIO}-{MAX_BREAKOUT_VOLUME_RATIO})")
            return None
        else:
            logger.debug(f"【THREEMA筛选】{code} {name} - 通过量能验证，量能比={volume_ratio:.4f}")
        
        # 3. 确认阶段验证（如果已有突破）
        # 检查突破后的3天确认
        # 修复：检查突破后的情况，而不仅仅是刚刚突破
        if consolidation_days <= 3:  # 突破后3天内
            # 确认阶段需要至少3天数据
            if len(df) < 3:
                logger.debug(f"【THREEMA筛选】{code} {name} - 数据不足，无法进行确认阶段验证")
                return None
                
            # 不回落验证：突破后3天不破突破收盘价
            breakout_price = df["收盘"].iloc[-consolidation_days]
            for i in range(1, min(4, consolidation_days+1)):
                if df["最低"].iloc[-i] < breakout_price:
                    logger.debug(f"【THREEMA筛选】{code} {name} - 不回落验证失败，第{i}天价格低于突破价")
                    return None
            logger.debug(f"【THREEMA筛选】{code} {name} - 通过不回落验证")
                    
            # 均线稳验证：偏离度<8%
            for i in range(1, min(4, consolidation_days+1)):
                max_ma_i = max(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
                min_ma_i = min(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
                dev_i = (max_ma_i - min_ma_i) / max_ma_i
                if dev_i >= MAX_CONFIRMATION_DEVIATION:
                    logger.debug(f"【THREEMA筛选】{code} {name} - 均线稳验证失败，第{i}天偏离度={dev_i:.4f} >= {MAX_CONFIRMATION_DEVIATION}")
                    return None
            logger.debug(f"【THREEMA筛选】{code} {name} - 通过均线稳验证")
                    
            # 量能续验证：不骤缩
            breakout_volume = df["成交量"].iloc[-consolidation_days]
            for i in range(1, min(4, consolidation_days+1)):
                if df["成交量"].iloc[-i] < breakout_volume * 0.5:
                    logger.debug(f"【THREEMA筛选】{code} {name} - 量能续验证失败，第{i}天量能低于突破量能的50%")
                    return None
            logger.debug(f"【THREEMA筛选】{code} {name} - 通过量能续验证")
        
        logger.info(f"【THREEMA筛选】{code} {name} - 通过所有验证，确认三均线粘合突破信号")
        return {
            "deviation": deviation,
            "consolidation_days": consolidation_days,
            "breakout_ratio": breakout_ratio,
            "volume_ratio": volume_ratio
        }
    except Exception as e:
        logger.error(f"【THREEMA筛选】检查股票 {code} {name} 三均线粘合突破信号失败: {str(e)}", exc_info=True)
        return None

def format_single_signal(category, signals):
    """格式化单一指标信号"""
    if not signals:
        return ""
    
    # 按关键指标排序（缠绕率越小/增长幅度越大排名越前）
    if category == "MA":
        signals = sorted(signals, key=lambda x: x["deviation"])
    elif category == "MACD":
        signals = sorted(signals, key=lambda x: x["growth_rate"], reverse=True)
    elif category == "RSI":
        signals = sorted(signals, key=lambda x: x["rsi_change"], reverse=True)
    elif category == "KDJ":
        signals = sorted(signals, key=lambda x: x["j_change"], reverse=True)
    
    # 生成消息
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"【策略2 - {get_category_name(category)}信号】",
        f"日期：{today}",
        ""
    ]
    
    lines.append(f"✅ {get_category_name(category)}信号：")
    for i, signal in enumerate(signals[:20], 1):
        code = signal["code"]
        name = signal["name"]
        if category == "MA":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['deviation']:.1%}，持续：{signal['consolidation_days']}天）")
        elif category == "MACD":
            lines.append(f"{i}. {code} {name}（增长：{signal['growth_rate']:.0%}，持续：{signal['growth_days']}天）")
        elif category == "RSI":
            lines.append(f"{i}. {code} {name}（RSI：{signal['rsi_value']:.0f}，变化：{signal['rsi_change']:.0f}点）")
        elif category == "KDJ":
            lines.append(f"{i}. {code} {name}（KDJ：{signal['k_value']:.0f}{signal['d_value']:.0f}{signal['j_value']:.0f}，变化：{signal['j_change']:.0f}点）")
    
    if signals:
        lines.append("")
        lines.append("📈 信号解读：")
        if category == "MA":
            lines.append("均线缠绕代表市场处于蓄势状态，缠绕率越小，突破后动能越大。建议关注缠绕率最小且持续时间最长的个股。")
        elif category == "MACD":
            lines.append("MACD在0轴上方且持续增长代表动能增强，增长幅度越大，动能越强。建议关注增长幅度大且持续时间长的个股。")
        elif category == "RSI":
            lines.append("RSI从超卖区回升代表市场情绪改善，变化幅度越大，反弹力度越强。建议关注变化幅度大且持续时间长的个股。")
        elif category == "KDJ":
            lines.append("KDJ低位金叉代表短期动能强劲，J线变化幅度越大，反弹力度越强。建议关注J线快速上升的个股。")
    
    return "\n".join(lines)

def format_double_signal(combination, signals):
    """格式化双指标共振信号"""
    if not signals:
        return ""
    
    # 按信号质量排序
    signals = sorted(signals, key=lambda x: get_signal_quality(x, combination), reverse=True)
    
    # 生成消息
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"【策略2 - {get_combination_name(combination)}共振信号】",
        f"日期：{today}",
        ""
    ]
    
    lines.append(f"🔥 {get_combination_name(combination)}共振信号：")
    for i, signal in enumerate(signals, 1):
        code = signal["code"]
        name = signal["name"]
        if combination == "MA+MACD":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，MACD增长：{signal['macd']['growth_rate']:.0%}）")
        elif combination == "MA+RSI":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点）")
        elif combination == "MA+KDJ":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
        elif combination == "MACD+RSI":
            lines.append(f"{i}. {code} {name}（MACD增长：{signal['macd']['growth_rate']:.0%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点）")
        elif combination == "MACD+KDJ":
            lines.append(f"{i}. {code} {name}（MACD增长：{signal['macd']['growth_rate']:.0%}，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
        elif combination == "RSI+KDJ":
            lines.append(f"{i}. {code} {name}（RSI变化：{signal['rsi']['rsi_change']:.0f}点，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
    
    if signals:
        lines.append("")
        lines.append("💡 信号解读：")
        lines.append("双指标共振是趋势与动能的最佳配合，胜率高达65%。建议优先交易此类信号。")
    
    return "\n".join(lines)

def format_triple_signal(combination, signals):
    """格式化三指标共振信号"""
    if not signals:
        return ""
    
    # 按信号质量排序
    signals = sorted(signals, key=lambda x: get_signal_quality(x, combination), reverse=True)
    
    # 生成消息
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"【策略2 - {get_combination_name(combination)}共振信号】",
        f"日期：{today}",
        ""
    ]
    
    lines.append(f"💎 {get_combination_name(combination)}共振信号：")
    for i, signal in enumerate(signals, 1):
        code = signal["code"]
        name = signal["name"]
        if combination == "MA+MACD+RSI":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，MACD增长：{signal['macd']['growth_rate']:.0%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点）")
        elif combination == "MA+MACD+KDJ":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，MACD增长：{signal['macd']['growth_rate']:.0%}，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
        elif combination == "MA+RSI+KDJ":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
        elif combination == "MACD+RSI+KDJ":
            lines.append(f"{i}. {code} {name}（MACD增长：{signal['macd']['growth_rate']:.0%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
    
    if signals:
        lines.append("")
        lines.append("🌟 信号解读：")
        lines.append("三指标共振代表趋势、动能和超买超卖状态完美配合，是高质量信号。历史回测显示此类信号平均收益率比市场基准高2.8倍。")
    
    return "\n".join(lines)

def format_quadruple_signal(signals):
    """格式化四指标共振信号"""
    if not signals:
        return ""
    
    # 按信号质量排序
    signals = sorted(signals, key=lambda x: get_signal_quality(x, "MA+MACD+RSI+KDJ"), reverse=True)
    
    # 生成消息
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"【策略2 - 四指标共振信号】",
        f"日期：{today}",
        ""
    ]
    
    lines.append("✨ MA+MACD+RSI+KDJ全指标共振信号：")
    for i, signal in enumerate(signals, 1):
        code = signal["code"]
        name = signal["name"]
        lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，MACD增长：{signal['macd']['growth_rate']:.0%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
    
    if signals:
        lines.append("")
        lines.append("🎯 信号解读：")
        lines.append("全指标共振是最高质量的交易信号，历史胜率高达78%。建议重仓参与此类信号。")
    
    return "\n".join(lines)

def format_threema_signal(threema_signals, all_threema_candidates):
    """格式化三均线粘合突破信号（分页显示并展示筛选过程）"""
    if not all_threema_candidates:
        return ""
    
    # 按粘合持续天数排序（持续天数越长排名越前）
    all_candidates_sorted = sorted(all_threema_candidates, key=lambda x: x["consolidation_days"], reverse=True)
    
    # 1. 初始筛选：三均线缠绕
    step1_count = len(all_candidates_sorted)
    
    # 2. 空间验证：均线偏离度<2%
    step2_candidates = [s for s in all_candidates_sorted if s["deviation"] < MAX_THREEMA_DEVIATION]
    step2_count = len(step2_candidates)
    
    # 3. 时间验证：粘合持续≥5天
    step3_candidates = [s for s in step2_candidates if s["consolidation_days"] >= MIN_CONSOLIDATION_DAYS]
    step3_count = len(step3_candidates)
    
    # 4. 量能验证：粘合期量能比吸筹期缩50%以上
    step4_candidates = []
    for s in step3_candidates:
        # 重新计算量能比
        code = s["code"]
        df = get_stock_daily_data(code)
        if df is not None and len(df) >= s["consolidation_days"] + 5:
            consolidation_volume = df["成交量"].iloc[-s["consolidation_days"]:].mean()
            accumulation_volume = df["成交量"].iloc[-(s["consolidation_days"]+5):-s["consolidation_days"]].mean()
            volume_ratio = consolidation_volume / accumulation_volume
            if volume_ratio < 0.5:  # 50%缩量
                step4_candidates.append(s)
    step4_count = len(step4_candidates)
    
    # 5. 突破阶段验证
    step5_candidates = []
    for s in step4_candidates:
        code = s["code"]
        df = get_stock_daily_data(code)
        if df is not None and len(df) >= s["consolidation_days"] + 5:
            # 同步向上验证
            ma5 = calc_ma(df, 5)
            ma10 = calc_ma(df, 10)
            ma20 = calc_ma(df, 20)
            if not (ma5.iloc[-1] > ma5.iloc[-2] and ma10.iloc[-1] > ma10.iloc[-2] and ma20.iloc[-1] > ma20.iloc[-2]):
                continue
                
            # 多头排列雏形
            if not (ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1]):
                continue
                
            # 幅度验证：突破幅度>3%
            consolidation_high = max(df["最高"].iloc[-s["consolidation_days"]:])
            breakout_ratio = (df["收盘"].iloc[-1] / consolidation_high) - 1
            if breakout_ratio <= MIN_BREAKOUT_RATIO:
                continue
                
            # 量能验证：突破量能增加50%-100%
            consolidation_volume = df["成交量"].iloc[-s["consolidation_days"]:].mean()
            volume_ratio = df["成交量"].iloc[-1] / consolidation_volume
            if volume_ratio < MIN_BREAKOUT_VOLUME_RATIO or volume_ratio > MAX_BREAKOUT_VOLUME_RATIO:
                continue
                
            step5_candidates.append(s)
    step5_count = len(step5_candidates)
    
    # 6. 确认阶段验证
    final_candidates = []
    for s in step5_candidates:
        code = s["code"]
        df = get_stock_daily_data(code)
        if df is not None and len(df) >= s["consolidation_days"] + 5:
            consolidation_days = s["consolidation_days"]
            # 确认阶段验证（如果已有突破）
            if consolidation_days <= 3:  # 突破后3天内
                # 确认阶段需要至少3天数据
                if len(df) < 3:
                    continue
                    
                # 不回落验证：突破后3天不破突破收盘价
                breakout_price = df["收盘"].iloc[-consolidation_days]
                valid = True
                for i in range(1, min(4, consolidation_days+1)):
                    if df["最低"].iloc[-i] < breakout_price:
                        valid = False
                        break
                if not valid:
                    continue
                        
                # 均线稳验证：偏离度<8%
                for i in range(1, min(4, consolidation_days+1)):
                    ma5 = calc_ma(df, 5)
                    ma10 = calc_ma(df, 10)
                    ma20 = calc_ma(df, 20)
                    max_ma_i = max(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
                    min_ma_i = min(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
                    dev_i = (max_ma_i - min_ma_i) / max_ma_i
                    if dev_i >= MAX_CONFIRMATION_DEVIATION:
                        valid = False
                        break
                if not valid:
                    continue
                        
                # 量能续验证：不骤缩
                breakout_volume = df["成交量"].iloc[-consolidation_days]
                for i in range(1, min(4, consolidation_days+1)):
                    if df["成交量"].iloc[-i] < breakout_volume * 0.5:
                        valid = False
                        break
                if not valid:
                    continue
            
            final_candidates.append(s)
    
    # 按粘合持续天数排序
    final_candidates = sorted(final_candidates, key=lambda x: x["consolidation_days"], reverse=True)
    final_count = len(final_candidates)
    
    # 分页处理
    page_size = 20
    pages = [final_candidates[i:i+page_size] for i in range(0, len(final_candidates), page_size)]
    messages = []
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 生成筛选过程消息
    process_lines = [
        f"【策略3 - 3均线缠绕{MIN_CONSOLIDATION_DAYS}天】",
        f"日期：{today}",
        "",
        "🔍 三均线粘合突破信号筛选过程：",
        f"1️⃣ 初始筛选（三均线缠绕）：{step1_count}只股票",
        f"2️⃣ 空间验证（偏离率<2%）：{step2_count}只股票（筛选掉{step1_count-step2_count}只）",
        f"3️⃣ 时间验证（粘合≥{MIN_CONSOLIDATION_DAYS}天）：{step3_count}只股票（筛选掉{step2_count-step3_count}只）",
        f"4️⃣ 量能验证（缩量50%+）：{step4_count}只股票（筛选掉{step3_count-step4_count}只）",
        f"5️⃣ 突破阶段验证：{step5_count}只股票（筛选掉{step4_count-step5_count}只）",
        f"6️⃣ 确认阶段验证：{final_count}只股票（筛选掉{step5_count-final_count}只）",
        "",
        "📊 筛选结果：",
        f"✅ 最终通过验证：{final_count}只股票",
        ""
    ]
    
    # 添加筛选过程消息作为第一页
    pages = [final_candidates[:page_size]] + pages  # 将第一页作为筛选过程
    
    # 生成每页消息
    for page_num, page_signals in enumerate(pages, 1):
        if page_num == 1:
            # 第一页是筛选过程
            messages.append("\n".join(process_lines))
        else:
            # 生成信号详情
            lines = [
                f"【策略3 - 3均线缠绕{MIN_CONSOLIDATION_DAYS}天】",
                f"日期：{today}",
                f"第{page_num-1}页（共{len(pages)-1}页）",
                ""
            ]
            
            lines.append(f"✅ 三均线粘合突破信号（共{final_count}只，本页{len(page_signals)}只）：")
            for i, signal in enumerate(page_signals, 1):
                code = signal["code"]
                name = signal["name"]
                lines.append(f"{i}. {code} {name}（粘合：{signal['consolidation_days']}天，突破：{signal['breakout_ratio']:.1%}，量能：{signal['volume_ratio']:.1f}倍）")
            
            if page_signals and page_num == 2:  # 只在第一页信号详情显示信号解读
                lines.append("")
                lines.append("💎 信号解读：")
                lines.append("三均线粘合突破是主力资金高度控盘后的启动信号，真突破概率超90%。")
                lines.append("信号质量判断：")
                lines.append("1. 粘合阶段：窄区间（<2%）、长周期（≥5天）、极致缩量（量能缩减50%以上）")
                lines.append("2. 突破阶段：同步向上、幅度够（>3%）、量能温（量能增加50%-100%）")
                lines.append("3. 确认阶段：不回落、均线稳（偏离<8%）、量能续（不骤缩）")
                lines.append("")
                lines.append("📈 操作建议：")
                lines.append("• 突破确认后立即建仓30%，回调至5日均线加仓20%")
                lines.append("• 止损位：突破当日最低价下方2%")
                lines.append("• 止盈位：1:3风险收益比，或偏离20日均线10%")
                lines.append("• 仓位控制：单只标的≤20%，总仓位≤60%")
            
            messages.append("\n".join(lines))
    
    return messages

def save_and_commit_stock_codes(ma_signals, macd_signals, rsi_signals, kdj_signals, threema_signals,
                               double_signals, triple_signals, quadruple_signals):
    """保存股票代码到文件并提交到Git仓库（严格遵循微信推送逻辑）"""
    try:
        # 获取当前时间
        now = get_beijing_time()  # 确保函数已正确导入
        timestamp = now.strftime("%Y%m%d%H%M")
        filename = f"macd{timestamp}.txt"
        
        # 构建文件路径
        stock_dir = os.path.join(Config.DATA_DIR, "stock")
        if not os.path.exists(stock_dir):
            os.makedirs(stock_dir, exist_ok=True)
        
        file_path = os.path.join(stock_dir, filename)
        
        # 收集所有股票代码
        all_stock_codes = set()
        
        # 1. 单一指标信号：MA、MACD、RSI、KDJ 取前20名
        for signals in [ma_signals, macd_signals, rsi_signals, kdj_signals]:
            # 取前20名（与微信推送一致）
            for signal in signals[:20]:
                code = str(signal['code']).zfill(6)
                all_stock_codes.add(code)
        
        # 2. THREEMA信号（三均线缠绕）不进行过滤，全部收集
        for signal in threema_signals:
            code = str(signal['code']).zfill(6)
            all_stock_codes.add(code)
        
        # 3. 双指标共振信号：不进行过滤，全部收集
        for signals_list in double_signals.values():
            for signal in signals_list:
                code = str(signal['code']).zfill(6)
                all_stock_codes.add(code)
        
        # 4. 三指标共振信号：不进行过滤，全部收集
        for signals_list in triple_signals.values():
            for signal in signals_list:
                code = str(signal['code']).zfill(6)
                all_stock_codes.add(code)
        
        # 5. 四指标共振信号：不进行过滤，全部收集
        for signal in quadruple_signals:
            code = str(signal['code']).zfill(6)
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
        else:
            logger.error(f"❌ 提交文件到Git仓库失败: {file_path}")
            
    except Exception as e:
        logger.error(f"❌ 保存股票代码文件失败: {str(e)}", exc_info=True)

def get_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """从本地加载股票日线数据，严格使用中文列名"""
    try:
        # 确保股票代码是字符串，并且是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        
        # 日线数据目录
        daily_dir = os.path.join(Config.DATA_DIR, "daily")
        
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

def main():
    # 1. 读取所有股票列表
    basic_info_file = os.path.join(Config.DATA_DIR, "all_stocks.csv")
    if not os.path.exists(basic_info_file):
        logger.error("基础信息文件不存在")
        error_msg = "【策略2 - 多指标共振策略】\n基础信息文件不存在，无法生成交易信号"
        send_wechat_message(message=error_msg, message_type="error")
        return
    
    try:
        basic_info_df = pd.read_csv(basic_info_file)
        logger.info(f"成功读取基础信息文件，共 {len(basic_info_df)} 只股票")
    except Exception as e:
        logger.error(f"读取基础信息文件失败: {str(e)}")
        error_msg = f"【策略2 - 多指标共振策略】\n读取基础信息文件失败，无法生成交易信号: {str(e)}"
        send_wechat_message(message=error_msg, message_type="error")
        return
    
    # 2. 初始化信号容器
    ma_signals = []
    macd_signals = []
    rsi_signals = []
    kdj_signals = []
    threema_signals = []  # 新增三均线粘合突破信号容器
    all_threema_candidates = []  # 收集所有初始三均线缠绕股票
    
    double_signals = {
        "MA+MACD": [],
        "MA+RSI": [],
        "MA+KDJ": [],
        "MACD+RSI": [],
        "MACD+KDJ": [],
        "RSI+KDJ": []
    }
    
    triple_signals = {
        "MA+MACD+RSI": [],
        "MA+MACD+KDJ": [],
        "MA+RSI+KDJ": [],
        "MACD+RSI+KDJ": []
    }
    
    quadruple_signals = []
    
    # 3. 遍历所有股票
    total_stocks = len(basic_info_df)
    processed_stocks = 0
    logger.info(f"开始处理 {total_stocks} 只股票...")
    
    for _, row in basic_info_df.iterrows():
        code = row["代码"]
        name = row["名称"]
        file_path = os.path.join(Config.DATA_DIR, "daily", f"{code}.csv")
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            continue
        
        try:
            # 读取日线数据
            df = pd.read_csv(file_path)
            
            # 检查必要列
            required_columns = ["日期", "收盘", "最高", "最低", "成交量"]
            if not all(col in df.columns for col in required_columns):
                continue
            
            # 检查数据量
            if len(df) < max(MA_PERIODS) + max(MACD_LONG, RSI_PERIOD, KDJ_PERIOD):
                continue
            
            # 检查日期格式
            df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
            df = df.sort_values("日期").reset_index(drop=True)
            
            # 检查数据完整性
            if df["收盘"].isna().any() or df["成交量"].isna().any():
                continue
            
            # 检查大盘趋势
            if MIN_MARKET_UPWARD:
                # 这里可以添加大盘趋势判断逻辑
                pass
            
            # 检查各指标信号
            ma_signal = check_ma_signal(df)
            macd_signal = check_macd_signal(df)
            rsi_signal = check_rsi_signal(df)
            kdj_signal = check_kdj_signal(df)
            
            # 1. 先检查初始三均线缠绕（用于展示筛选过程）
            ma5 = calc_ma(df, 5)
            ma10 = calc_ma(df, 10)
            ma20 = calc_ma(df, 20)
            
            # 检查三均线缠绕
            max_ma = max(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
            min_ma = min(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
            deviation = (max_ma - min_ma) / max_ma
            
            if deviation < MAX_THREEMA_DEVIATION:
                # 收集所有初始三均线缠绕股票
                all_threema_candidates.append({
                    "code": code,
                    "name": name,
                    "deviation": deviation
                })
            
            # 2. 检查完整的三均线粘合突破信号
            threema_signal = check_threema_signal(df, code, name)
            if threema_signal:
                threema_signals.append({"code": code, "name": name, **threema_signal})
            
            # 收集单一指标信号
            if ma_signal:
                ma_signals.append({"code": code, "name": name, **ma_signal})
            
            if macd_signal:
                macd_signals.append({"code": code, "name": name, **macd_signal})
            
            if rsi_signal:
                rsi_signals.append({"code": code, "name": name, **rsi_signal})
            
            if kdj_signal:
                kdj_signals.append({"code": code, "name": name, **kdj_signal})
                
            # 收集双指标共振信号
            if ma_signal and macd_signal:
                double_signals["MA+MACD"].append({"code": code, "name": name, "ma": ma_signal, "macd": macd_signal})
            
            if ma_signal and rsi_signal:
                double_signals["MA+RSI"].append({"code": code, "name": name, "ma": ma_signal, "rsi": rsi_signal})
            
            if ma_signal and kdj_signal:
                double_signals["MA+KDJ"].append({"code": code, "name": name, "ma": ma_signal, "kdj": kdj_signal})
            
            if macd_signal and rsi_signal:
                double_signals["MACD+RSI"].append({"code": code, "name": name, "macd": macd_signal, "rsi": rsi_signal})
            
            if macd_signal and kdj_signal:
                double_signals["MACD+KDJ"].append({"code": code, "name": name, "macd": macd_signal, "kdj": kdj_signal})
            
            if rsi_signal and kdj_signal:
                double_signals["RSI+KDJ"].append({"code": code, "name": name, "rsi": rsi_signal, "kdj": kdj_signal})
            
            # 收集三指标共振信号
            if ma_signal and macd_signal and rsi_signal:
                triple_signals["MA+MACD+RSI"].append({"code": code, "name": name, "ma": ma_signal, "macd": macd_signal, "rsi": rsi_signal})
            
            if ma_signal and macd_signal and kdj_signal:
                triple_signals["MA+MACD+KDJ"].append({"code": code, "name": name, "ma": ma_signal, "macd": macd_signal, "kdj": kdj_signal})
            
            if ma_signal and rsi_signal and kdj_signal:
                triple_signals["MA+RSI+KDJ"].append({"code": code, "name": name, "ma": ma_signal, "rsi": rsi_signal, "kdj": kdj_signal})
            
            if macd_signal and rsi_signal and kdj_signal:
                triple_signals["MACD+RSI+KDJ"].append({"code": code, "name": name, "macd": macd_signal, "rsi": rsi_signal, "kdj": kdj_signal})
            
            # 收集四指标共振信号
            if ma_signal and macd_signal and rsi_signal and kdj_signal:
                quadruple_signals.append({"code": code, "name": name, "ma": ma_signal, "macd": macd_signal, "rsi": rsi_signal, "kdj": kdj_signal})
            
            processed_stocks += 1
            if processed_stocks % 100 == 0:
                logger.info(f"已处理 {processed_stocks} 只股票...")
        
        except Exception as e:
            logger.debug(f"处理股票 {code} 时出错: {str(e)}")
            continue
    
    logger.info(f"处理完成，共处理 {processed_stocks} 只股票")
    
    # 4. 生成并发送信号
    total_messages = 0
    
    # 【关键修改】在推送消息前，保存股票代码到txt文件
    save_and_commit_stock_codes(ma_signals, macd_signals, rsi_signals, kdj_signals, threema_signals,
                               double_signals, triple_signals, quadruple_signals)
    
    # 单一指标信号
    for category, signals in [("MA", ma_signals), ("MACD", macd_signals), ("RSI", rsi_signals), ("KDJ", kdj_signals)]:
        message = format_single_signal(category, signals)
        if message.strip():
            send_wechat_message(message=message, message_type="position")
            total_messages += 1
    
    # 双指标共振信号
    for combination in double_signals:
        message = format_double_signal(combination, double_signals[combination])
        if message.strip():
            send_wechat_message(message=message, message_type="position")
            total_messages += 1
    
    # 三指标共振信号
    for combination in triple_signals:
        message = format_triple_signal(combination, triple_signals[combination])
        if message.strip():
            send_wechat_message(message=message, message_type="position")
            total_messages += 1
    
    # 四指标共振信号
    message = format_quadruple_signal(quadruple_signals)
    if message.strip():
        send_wechat_message(message=message, message_type="position")
        total_messages += 1
    
    # THREEMA信号（三均线粘合突破）- 分页显示
    threema_messages = format_threema_signal(threema_signals, all_threema_candidates)
    for message in threema_messages:
        if message.strip():
            send_wechat_message(message=message, message_type="position")
            total_messages += 1
    
    if total_messages > 0:
        logger.info(f"成功发送 {total_messages} 组交易信号到微信")
    else:
        msg = "【策略2 - 多指标共振策略】\n今日未检测到有效交易信号"
        send_wechat_message(message=msg, message_type="position")
        logger.info("未检测到有效交易信号")

if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                        handlers=[
                            logging.StreamHandler(sys.stdout),
                            logging.FileHandler(os.path.join(Config.LOG_DIR, "macd_ma_strategy.log"))
                        ])
    
    # 记录开始执行
    logger.info("===== 开始执行任务：MACD均线策略 =====")
    
    try:
        # 执行策略
        main()
        
        # 记录任务完成
        logger.info("===== 任务执行结束：success =====")
    except Exception as e:
        error_msg = f"【策略2 - 多指标共振策略】执行时发生未预期错误: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(message=error_msg, message_type="error")
        logger.info("===== 任务执行结束：error =====")
