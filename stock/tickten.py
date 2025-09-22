#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股趋势跟踪策略（TickTen策略）
基于本地已保存的股票日线数据，应用流动性、波动率、市值三重过滤，筛选优质个股
按板块分类推送，每个板块最多10只，共40只
"""

import os
import logging
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
# ========== 以下是关键修改 ==========
from concurrent.futures import ThreadPoolExecutor
# ========== 以上是关键修改 ==========

# ===== 新增导入 =====
import sys
import traceback
# ===== 新增导入结束 =====

from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time
)
from wechat_push.push import send_wechat_message
from utils.git_utils import commit_and_push_file  # 确保导入这个函数

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# ========== 以下是关键修改 ==========
# 定义股票基础信息文件路径
BASIC_INFO_FILE = "data/all_stocks.csv"

# 数据更新间隔（天）
DATA_UPDATE_INTERVAL = 1
# ========== 以上是关键修改 ==========

# 股票板块配置
MARKET_SECTIONS = {
    "沪市主板": {
        "prefix": ["60"],
        "min_daily_volume": 5 * 10000,  # 日均成交额阈值(元)
        "min_volatility": 0.05,  # 最小波动率
        "max_volatility": 0.40,  # 最大波动率
        "min_market_cap": 5,  # 最小市值(亿元)
        "max_market_cap": 2000  # 最大市值(亿元)
    },
    "深市主板": {
        "prefix": ["00"],
        "min_daily_volume": 5 * 10000,
        "min_volatility": 0.05,
        "max_volatility": 0.40,
        "min_market_cap": 5,
        "max_market_cap": 2000
    },
    "创业板": {
        "prefix": ["30"],
        "min_daily_volume": 5 * 10000,  # 修复：统一单位为元
        "min_volatility": 0.05,
        "max_volatility": 0.40,
        "min_market_cap": 5,
        "max_market_cap": 2000
    },
    "科创板": {
        "prefix": ["688"],
        "min_daily_volume": 5 * 10000,  # 修复：统一单位为元
        "min_volatility": 0.05,
        "max_volatility": 0.40,
        "min_market_cap": 5,
        "max_market_cap": 2000
    }
}

# 其他参数
MIN_DATA_DAYS = 30  # 最小数据天数（用于计算指标）
MAX_STOCKS_TO_ANALYZE = 300  # 减少每次分析的最大股票数量（避免请求过多）
MAX_STOCKS_PER_SECTION = 8  # 每个板块最多报告的股票数量
CRITICAL_VALUE_DAYS = 40  # 临界值计算天数


def check_data_integrity(df: pd.DataFrame) -> Tuple[str, int]:
    """检查数据完整性并返回级别
    
    Returns:
        (str, int): (完整性级别, 数据天数)
    """
    if df is None or df.empty:
        return "none", 0
    
    # 计算数据天数
    data_days = len(df)
    
    # 检查必要列
    required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return "corrupted", data_days
    
    # 检查数据连续性
    df = df.copy()
    # 确保日期列是datetime类型
    try:
        df["日期"] = pd.to_datetime(df["日期"])
    except:
        return "corrupted", data_days
    
    df = df.sort_values("日期")
    
    # 检查日期间隔
    df["日期_diff"] = df["日期"].diff().dt.days
    gaps = df[df["日期_diff"] > 1]
    
    # 计算缺失率
    expected_days = (df["日期"].iloc[-1] - df["日期"].iloc[0]).days + 1
    missing_rate = 1 - (data_days / expected_days) if expected_days > 0 else 1
    
    # 数据完整性分级
    if data_days < MIN_DATA_DAYS:
        return "insufficient", data_days
    elif missing_rate > 0.2:  # 缺失率超过20%
        return "partial", data_days
    elif gaps.shape[0] > 5:  # 有5个以上大间隔
        return "gapped", data_days
    else:
        return "complete", data_days

# ========== 以下是关键修改 ==========
def load_stock_basic_info() -> pd.DataFrame:
    """加载股票基础信息"""
    try:
        if os.path.exists(BASIC_INFO_FILE):
            df = pd.read_csv(BASIC_INFO_FILE)
            
            # 确保所有必要列存在
            if "code" not in df.columns:
                logger.error(f"股票基础信息文件缺少 'code' 列")
                return pd.DataFrame()
            
            # 确保股票代码是字符串格式，并且是6位（前面补零）
            df["code"] = df["code"].astype(str).str.zfill(6)
            
            # 如果没有 section 列，添加并计算
            if "section" not in df.columns:
                df["section"] = df["code"].apply(get_stock_section)
            
            # 如果没有 market_cap 列，添加并初始化
            if "market_cap" not in df.columns:
                df["market_cap"] = 0.0
                
            # 如果没有 score 列，添加并初始化
            if "score" not in df.columns:
                df["score"] = 0.0
                
            # 如果没有 last_update 列，添加并初始化
            if "last_update" not in df.columns:
                df["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            logger.info(f"成功加载股票基础信息，共 {len(df)} 条记录")
            return df
        else:
            logger.error(f"股票基础信息文件 {BASIC_INFO_FILE} 不存在")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"加载股票基础信息失败: {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def get_last_update_time(df: pd.DataFrame, stock_code: str) -> Optional[datetime]:
    """获取股票最后更新时间"""
    if df.empty:
        return None
    
    stock_info = df[df["code"] == stock_code]
    if not stock_info.empty:
        last_update = stock_info["last_update"].values[0]
        try:
            # 尝试解析时间字符串
            return datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.debug(f"解析更新时间失败: {str(e)}")
            return None
    return None
# ========== 以上是关键修改 ==========

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
    for section, config in MARKET_SECTIONS.items():
        for prefix in config["prefix"]:
            if stock_code.startswith(prefix):
                return section
    
    return "其他板块"

def get_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """从本地加载股票日线数据"""
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
                
                # 确保必要列存在
                required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
                for col in required_columns:
                    if col not in df.columns:
                        logger.warning(f"股票 {stock_code} 数据缺少必要列: {col}")
                        return pd.DataFrame()
                
                # 确保日期列是字符串类型
                if "日期" in df.columns:
                    df["日期"] = df["日期"].astype(str)
                    # 确保日期格式为YYYY-MM-DD
                    df["日期"] = df["日期"].str.replace(r'(\d{4})/(\d{1,2})/(\d{1,2})', 
                                                      lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", 
                                                      regex=True)
                    # 处理其他可能的格式
                    df["日期"] = df["日期"].str.replace(r'(\d{4})-(\d{1,2}) (\d{1,2})', 
                                                      lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", 
                                                      regex=True)
                    # 移除可能存在的空格
                    df["日期"] = df["日期"].str.strip()
                    df = df.sort_values("日期", ascending=True)
                
                # 移除NaN值
                df = df.dropna(subset=['收盘', '成交量'])
                
                logger.info(f"成功加载股票 {stock_code} 的本地日线数据，共 {len(df)} 条有效记录")
                return df
            except Exception as e:
                logger.warning(f"读取股票 {stock_code} 数据失败: {str(e)}")
                logger.debug(traceback.format_exc())
        
        logger.warning(f"股票 {stock_code} 的日线数据不存在")
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def calculate_annual_volatility(df: pd.DataFrame) -> float:
    """计算年化波动率"""
    if len(df) < 20:
        logger.warning(f"数据不足20天，无法准确计算波动率")
        return 0.2  # 默认波动率
    
    # 直接使用"收盘"列计算日收益率（不进行任何列名映射）
    daily_returns = df["收盘"].pct_change().dropna()
    
    # 计算年化波动率
    if len(daily_returns) >= 20:
        volatility = daily_returns.std() * np.sqrt(252)
    else:
        volatility = 0.2  # 默认波动率
    
    # 限制波动率在合理范围内
    volatility = max(0.05, min(1.0, volatility))
    
    return volatility

# ========== 以下是关键修复 ==========
def calculate_market_cap(df: pd.DataFrame, stock_code: str) -> float:
    """计算股票市值（直接使用基础信息文件中的数据）
    
    Returns:
        float: 市值(亿元)
    """
    try:
        # 1. 优先使用基础信息文件中的市值数据
        basic_info_df = load_stock_basic_info()
        if not basic_info_df.empty:
            stock_info = basic_info_df[basic_info_df["code"] == stock_code]
            if not stock_info.empty:
                market_cap = stock_info["market_cap"].values[0]
                if not pd.isna(market_cap) and market_cap > 0:
                    logger.debug(f"使用基础信息文件中的市值数据: {market_cap:.2f}亿元")
                    return market_cap
        
        # 2. 如果基础信息中没有，尝试使用历史数据估算
        if df is not None and not df.empty and len(df) >= 250:
            if "成交量" in df.columns and "收盘" in df.columns:
                avg_volume = df["成交量"].iloc[-250:].mean()
                avg_price = df["收盘"].iloc[-250:].mean()
                if avg_volume > 0 and avg_price > 0:
                    # 估算日均成交额(万元)
                    daily_turnover = avg_volume * avg_price / 10000
                    # 假设换手率为2%，估算总市值
                    if daily_turnover > 0:
                        estimated_market_cap = daily_turnover / 0.02  # 换手率2%
                        logger.debug(f"使用历史数据估算市值: {estimated_market_cap:.2f}亿元")
                        return estimated_market_cap
        
        # 3. 如果无法获取市值，返回默认值
        logger.warning(f"⚠️ 无法获取股票 {stock_code} 的准确市值，使用默认市值 50亿元")
        return 50.0
    
    except Exception as e:
        logger.error(f"估算{stock_code}市值失败: {str(e)}", exc_info=True)
        return 50.0  # 返回默认市值
# ========== 以上是关键修复 ==========

def is_stock_suitable(stock_code: str, df: pd.DataFrame, data_level: str, data_days: int) -> bool:
    """判断个股是否适合策略（流动性、波动率、市值三重过滤）"""
    try:
        # 1. 数据完整性检查
        if data_level == "insufficient" or data_level == "corrupted":
            logger.debug(f"股票 {stock_code} 被过滤 - 数据量不足({data_days}天)")
            return False
            
        # 获取股票所属板块
        section = get_stock_section(stock_code)
        if section not in MARKET_SECTIONS:
            logger.debug(f"股票 {stock_code} 不属于任何板块，跳过")
            return False
            
        # 获取板块配置
        section_config = MARKET_SECTIONS[section]
        
        # 2. 市值过滤 - 使用板块特定的阈值
        market_cap = calculate_market_cap(df, stock_code)
        if market_cap < section_config["min_market_cap"]:
            logger.debug(f"股票 {stock_code}({section}) 被过滤 - 市值不足({market_cap:.2f}亿元 < {section_config['min_market_cap']}亿元)")
            return False
        if market_cap > section_config["max_market_cap"]:
            logger.debug(f"股票 {stock_code}({section}) 被过滤 - 市值过大({market_cap:.2f}亿元 > {section_config['max_market_cap']}亿元)")
            return False
            
        # 3. 波动率过滤 - 使用板块特定的阈值
        volatility = calculate_annual_volatility(df)
        if volatility < section_config["min_volatility"] or volatility > section_config["max_volatility"]:
            logger.debug(f"股票 {stock_code}({section}) 被过滤 - 波动率异常({volatility:.2%}不在{section_config['min_volatility']:.2%}-{section_config['max_volatility']:.2%}范围内)")
            return False
            
        # 4. 流动性过滤 - 使用板块特定的阈值
        avg_volume = calculate_avg_volume(df)
        if avg_volume < section_config["min_daily_volume"] / 10000:  # 转换为万元
            logger.debug(f"股票 {stock_code}({section}) 被过滤 - 流动性不足(日均成交额{avg_volume:.2f}万元 < {section_config['min_daily_volume']/10000:.2f}万元)")
            return False
            
        logger.debug(f"股票 {stock_code}({section}) 通过所有过滤条件")
        return True
        
    except Exception as e:
        logger.error(f"股票 {stock_code} 过滤检查失败: {str(e)}", exc_info=True)
        return False

def calculate_avg_volume(df: pd.DataFrame) -> float:
    """计算日均成交额（万元）"""
    if df is None or df.empty or "成交量" not in df.columns or "收盘" not in df.columns:
        return 0.0
    
    # 计算日均成交额（元）
    avg_volume = df["成交量"].iloc[-20:].mean() * 100 * df["收盘"].iloc[-20:].mean()
    # 转换为万元
    return avg_volume / 10000

# ========== 以下是关键修改：更弹性的评分机制 ==========
def calculate_stock_strategy_score(stock_code: str, df: pd.DataFrame) -> float:
    """计算股票策略评分（更精细化的评分机制）
    
    Args:
        stock_code: 股票代码
        df: 预处理后的股票数据
    
    Returns:
        float: 评分(0-100)
    """
    try:
        if df is None or df.empty or len(df) < 40:
            logger.debug(f"股票 {stock_code} 数据不足，无法计算策略评分")
            return 0.0
        
        # 检查必要列
        required_columns = ['开盘', '最高', '最低', '收盘', '成交量']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.debug(f"股票 {stock_code} 数据缺少必要列: {', '.join(missing_columns)}，无法计算策略评分")
            return 0.0
        
        # 获取最新数据
        current = df["收盘"].iloc[-1]
        if pd.isna(current) or current <= 0:
            logger.debug(f"股票 {stock_code} 无效的收盘价: {current}")
            return 0.0
        
        volume = df["成交量"].iloc[-1] if "成交量" in df.columns and len(df) >= 1 else 0
        
        # 获取股票所属板块
        section = get_stock_section(stock_code)
        
        # 1. 趋势指标评分 (40%)
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
                deviation_score = max(0, min(5, 5 - abs(deviation) * 50))  # 理想偏离率在0-2%
                days_score = min(5, above_ma_days * 0.5)  # 每多一天加0.5分，最多5分
                trend_score += deviation_score + days_score
        
        # 1.3 趋势强度评分 (10分) - 基于20日涨幅和趋势稳定性
        if len(df) >= 20:
            price_change_20 = (current - df["收盘"].iloc[-20]) / df["收盘"].iloc[-20] * 100
            
            # 计算趋势稳定性 (价格在20日均线之上的比例)
            above_ma_ratio = 0
            if "ma20" in df.columns:
                above_ma_ratio = sum(1 for i in range(20) if df["收盘"].iloc[-i-1] > df["ma20"].iloc[-i-1]) / 20
            
            # 趋势强度评分 (0-10分)
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
                growth_score = min(5, max(0, growth_rate * 10))
                
                # 正值大小评分 (0-5分)
                value_score = min(5, max(0, macd_hist * 10))
                
                momentum_score += growth_score + value_score
        
        # 2.2 RSI评分 (10分) - 基于与理想区域的距离
        if "收盘" in df.columns:
            delta = df["收盘"].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            avg_gain = gain.rolling(window=14).mean()
            avg_loss = loss.rolling(window=14).mean()
            rs = avg_gain / avg_loss.replace(0, np.nan)  # 避免除零错误
            df["rsi"] = 100 - (100 / (1 + rs))
        
        if "rsi" in df.columns:
            rsi = df["rsi"].iloc[-1]
            if not pd.isna(rsi):
                # RSI评分 (0-10分)，理想区域50-70
                if 50 <= rsi <= 70:
                    # 在理想区域内，越接近60分越高
                    distance = abs(rsi - 60)
                    rsi_score = max(0, 10 - distance * 0.2)
                else:
                    # 在理想区域外，根据距离扣分
                    distance = min(abs(rsi - 50), abs(rsi - 70))
                    rsi_score = max(0, 5 - distance * 0.1)
                
                momentum_score += rsi_score
        
        # 3. 量能指标评分 (20%)
        volume_score = 0.0
        if "成交量" in df.columns:
            df["volume_ma5"] = df["成交量"].rolling(window=5).mean()
        
        volume_ma5 = df["volume_ma5"].iloc[-1] if "volume_ma5" in df.columns and len(df) >= 1 else 0
        if volume_ma5 > 0 and volume > 0:
            volume_ratio = volume / volume_ma5
            
            # 3.1 量能放大评分 (10分) - 基于放大比例
            volume_score += min(10, volume_ratio * 5)  # 放大100%得满分
            
            # 3.2 量价配合评分 (10分) - 基于价格变化与量能变化的相关性
            if len(df) >= 2:
                price_change = (current - df["收盘"].iloc[-2]) / df["收盘"].iloc[-2]
                volume_change = (volume - volume_ma5) / volume_ma5
                
                # 计算近5天价格变化与量能变化的相关性
                price_changes = []
                volume_changes = []
                for i in range(1, min(5, len(df))):
                    price_changes.append((df["收盘"].iloc[-i] - df["收盘"].iloc[-i-1]) / df["收盘"].iloc[-i-1])
                    volume_changes.append((df["成交量"].iloc[-i] - df["成交量"].iloc[-i-1]) / df["成交量"].iloc[-i-1])
                
                # 计算相关系数
                if len(price_changes) > 1:
                    mean_price = sum(price_changes) / len(price_changes)
                    mean_volume = sum(volume_changes) / len(volume_changes)
                    
                    numerator = sum((p - mean_price) * (v - mean_volume) for p, v in zip(price_changes, volume_changes))
                    denominator = (sum((p - mean_price)**2 for p in price_changes) * sum((v - mean_volume)**2 for v in volume_changes)) ** 0.5
                    
                    if denominator != 0:
                        correlation = numerator / denominator
                        volume_score += max(0, min(10, correlation * 10))
        
        # 4. 波动率指标评分 (20%)
        volatility_score = 0.0
        # 计算波动率（20日年化波动率）
        if "收盘" in df.columns:
            df["pct_change"] = df["收盘"].pct_change() * 100
        
        if "pct_change" in df.columns:
            df["volatility"] = df["pct_change"].rolling(window=20).std() * np.sqrt(252)
        
        if "volatility" in df.columns and len(df) >= 20:
            volatility = df["volatility"].iloc[-1]
            
            if not pd.isna(volatility):
                # 4.1 波动率水平评分 (10分) - 基于与理想范围的距离
                section_config = MARKET_SECTIONS.get(section, MARKET_SECTIONS["沪市主板"])
                min_vol = section_config["min_volatility"]
                max_vol = section_config["max_volatility"]
                
                if min_vol <= volatility <= max_vol:
                    # 在理想范围内，越接近中间值分越高
                    mid_vol = (min_vol + max_vol) / 2
                    distance = abs(volatility - mid_vol)
                    vol_score = max(0, 10 - distance * 20)
                else:
                    # 在理想范围外，根据距离扣分
                    distance = min(abs(volatility - min_vol), abs(volatility - max_vol))
                    vol_score = max(0, 5 - distance * 10)
                
                volatility_score += vol_score
                
                # 4.2 波动率稳定性评分 (10分) - 基于波动率变化率
                if len(df) >= 21:
                    prev_volatility = df["volatility"].iloc[-21]
                    if not pd.isna(prev_volatility) and prev_volatility > 0:
                        volatility_change = (volatility - prev_volatility) / prev_volatility
                        
                        # 变化率越小，评分越高
                        stability_score = max(0, 10 - abs(volatility_change) * 100)
                        volatility_score += stability_score
        
        # 综合评分
        total_score = trend_score + momentum_score + volume_score + volatility_score
        total_score = max(0, min(100, total_score))  # 限制在0-100范围内
        
        logger.debug(f"股票 {stock_code}({section}) 策略评分: {total_score:.2f} "
                     f"(趋势={trend_score:.1f}, 动量={momentum_score:.1f}, "
                     f"量能={volume_score:.1f}, 波动率={volatility_score:.1f})")
        
        return total_score
    
    except Exception as e:
        logger.error(f"计算股票 {stock_code} 策略评分失败: {str(e)}", exc_info=True)
        return 0.0
# ========== 以上是关键修改：更弹性的评分机制 ==========

# ========== 以下是关键修改：为每个板块生成详细报告 ==========
def generate_section_report(section: str, stocks: List[Dict]) -> str:
    """生成单个板块的详细报告
    
    Args:
        section: 板块名称
        stocks: 该板块的股票列表
    
    Returns:
        str: 板块详细报告
    """
    report_lines = []
    
    # 添加标题
    beijing_time = get_beijing_time()
    report_lines.append(f"📊 {section}板块趋势策略报告 ({beijing_time.strftime('%Y-%m-%d %H:%M')})")
    report_lines.append("──────────────────")
    
    # 添加板块筛选条件
    section_config = MARKET_SECTIONS.get(section, MARKET_SECTIONS["沪市主板"])
    report_lines.append(f"🔍 筛选条件:")
    report_lines.append(f"  • 市值范围: {section_config['min_market_cap']}-{section_config['max_market_cap']}亿元")
    report_lines.append(f"  • 日均成交额: >{section_config['min_daily_volume']/10000:.2f}万元")
    report_lines.append(f"  • 年化波动率: {section_config['min_volatility']*100:.1f}%-{section_config['max_volatility']*100:.1f}%")
    report_lines.append("──────────────────")
    
    # 检查是否有符合条件的股票
    if not stocks:
        report_lines.append(f"⚠️ 未筛选出符合条件的{section}股票")
        report_lines.append("──────────────────")
        report_lines.append("💡 操作建议: 当前市场环境下，该板块暂无符合策略标准的标的")
        report_lines.append("──────────────────")
        report_lines.append("📊 数据来源: fish-etf (https://github.com/karmyshunde-sudo/fish-etf)")
        return "\n".join(report_lines)
    
    # 添加筛选出的股票详情
    report_lines.append(f"✅ 筛选出 {len(stocks)} 只优质股票 (按评分排序):")
    
    for i, stock in enumerate(stocks):
        stock_code = stock["code"]
        stock_name = stock["name"]
        score = stock["score"]
        df = stock["df"]
        
        # 获取最新数据
        current = df["收盘"].iloc[-1]
        volume = df["成交量"].iloc[-1] if "成交量" in df.columns and len(df) >= 1 else 0
        
        # 计算20日均线
        ma20 = df["收盘"].rolling(window=20).mean().iloc[-1] if len(df) >= 20 else current
        
        # 计算价格偏离率
        deviation = (current - ma20) / ma20 if ma20 > 0 else 0
        
        # 获取趋势指标
        trend_score = min(40, score * 0.4)  # 从总分中推算
        momentum_score = min(20, score * 0.2)
        volume_score = min(20, score * 0.2)
        volatility_score = min(20, score * 0.2)
        
        # 添加股票详情
        report_lines.append(f"{'='*30}")
        report_lines.append(f"{i+1}. {stock_name}({stock_code}) - {score:.1f}分")
        report_lines.append(f"📈 趋势: {trend_score:.1f}/40 | 动量: {momentum_score:.1f}/20")
        report_lines.append(f"📊 量能: {volume_score:.1f}/20 | 波动: {volatility_score:.1f}/20")
        report_lines.append(f"💰 价格: {current:.4f} | 20日均线: {ma20:.4f} | 偏离率: {deviation:.2%}")
        report_lines.append(f"🔄 量能: {volume:,.0f}手 | 5日均量: {calculate_avg_volume(df):,.2f}万元")
    
    report_lines.append("──────────────────")
    report_lines.append("💡 操作指南:")
    report_lines.append("1. 评分越高，趋势越强，可考虑适当增加仓位")
    report_lines.append("2. 每只个股仓位≤15%，分散投资5-8只")
    report_lines.append("3. 持续关注趋势变化，及时调整持仓")
    report_lines.append("4. 科创板/创业板波动较大，注意控制风险")
    report_lines.append("──────────────────")
    report_lines.append("📊 数据来源: fish-etf (https://github.com/karmyshunde-sudo/fish-etf)")
    
    return "\n".join(report_lines)
# ========== 以上是关键修改：为每个板块生成详细报告 ==========

# ========== 以下是关键修改 ==========
# 缓存字典
FILTER_CACHE = {}
SCORE_CACHE = {}
CACHE_EXPIRY = timedelta(hours=1)  # 缓存有效期
def get_cached_filter_result(stock_code: str, last_update: datetime) -> Optional[bool]:
    """获取缓存的筛选结果"""
    if stock_code in FILTER_CACHE:
        cached_result, cache_time = FILTER_CACHE[stock_code]
        if datetime.now() - cache_time < CACHE_EXPIRY:
            return cached_result
    return None

def cache_filter_result(stock_code: str, result: bool):
    """缓存筛选结果"""
    FILTER_CACHE[stock_code] = (result, datetime.now())

def get_cached_score(stock_code: str, last_update: datetime) -> Optional[float]:
    """获取缓存的评分结果"""
    if stock_code in SCORE_CACHE:
        cached_score, cache_time = SCORE_CACHE[stock_code]
        if datetime.now() - cache_time < CACHE_EXPIRY:
            return cached_score
    return None

def cache_score(stock_code: str, score: float):
    """缓存评分结果"""
    SCORE_CACHE[stock_code] = (score, datetime.now())
# ========== 以上是关键修改 ==========

def get_top_stocks_for_strategy() -> Dict[str, List[Dict]]:
    """按板块获取适合策略的股票（使用本地已保存数据）"""
    try:
        logger.info("===== 开始执行个股趋势策略(TickTen) =====")
        
        # 1. 获取股票基础信息
        basic_info_df = load_stock_basic_info()
        if basic_info_df.empty:
            logger.error("获取股票基础信息失败，无法继续")
            return {}
        
        logger.info(f"已加载股票基础信息，共 {len(basic_info_df)} 条记录")
        
        # 2. 按板块分组处理
        section_stocks = {section: [] for section in MARKET_SECTIONS.keys()}
        
        # 3. 初始化各板块计数器（添加详细过滤原因统计）
        section_counts = {section: {
            "total": 0, 
            "data_ok": 0, 
            "market_cap_filtered": 0,
            "volatility_filtered": 0,
            "liquidity_filtered": 0,
            "data_filtered": 0,
            "suitable": 0,
            "scored": 0
        } for section in MARKET_SECTIONS.keys()}
        
        # 4. 处理每只股票
        stock_list = basic_info_df.to_dict('records')
        logger.info(f"开始处理 {len(stock_list)} 只股票...")
        
        # 确保所有股票代码是字符串格式（6位，前面补零）
        for stock in stock_list:
            stock["code"] = str(stock["code"]).zfill(6)
        
        logger.info(f"今天实际处理 {len(stock_list)} 只股票（完整处理）")
        
        def process_stock(stock):
            # 确保股票代码是字符串，并且是6位（前面补零）
            stock_code = str(stock["code"]).zfill(6)
            stock_name = stock["name"]
            section = stock["section"]
            
            # 检查板块是否有效
            if section not in MARKET_SECTIONS:
                return None
            
            # 更新板块计数器
            section_counts[section]["total"] += 1
            
            # 1. 尝试从缓存获取结果
            last_update = get_last_update_time(basic_info_df, stock_code)
            cached_result = get_cached_filter_result(stock_code, last_update)
            if cached_result is not None:
                logger.debug(f"股票 {stock_code} 使用缓存筛选结果: {cached_result}")
                if not cached_result:
                    return None
                
                cached_score = get_cached_score(stock_code, last_update)
                if cached_score is not None and cached_score > 0:
                    # 从本地加载数据
                    df = get_stock_daily_data(stock_code)
                    if not df.empty:
                        return {
                            "code": stock_code,
                            "name": stock_name,
                            "score": cached_score,
                            "df": df,
                            "section": section
                        }
            
            # 2. 获取日线数据（从本地加载）
            df = get_stock_daily_data(stock_code)
            
            # 3. 检查数据完整性
            data_level, data_days = check_data_integrity(df)
            logger.debug(f"股票 {stock_code} 数据完整性: {data_level} ({data_days}天)")
            
            # 4. 根据数据完整性应用不同策略
            if data_level == "insufficient" or data_level == "corrupted":
                section_counts[section]["data_filtered"] += 1
                logger.debug(f"股票 {stock_code} 被过滤 - 数据量不足({data_days}天)")
                cache_filter_result(stock_code, False)
                return None
            
            # 检查市值数据是否可靠
            market_cap = calculate_market_cap(df, stock_code)
            if market_cap <= 0:
                logger.warning(f"⚠️ 股票 {stock_code} 市值数据不可靠，使用默认值")
                market_cap = 50.0  # 使用默认市值
            
            # 检查是否适合策略
            if not is_stock_suitable(stock_code, df, data_level, data_days):
                # 记录具体过滤原因
                if market_cap < MARKET_SECTIONS[section]["min_market_cap"]:
                    section_counts[section]["market_cap_filtered"] += 1
                    logger.debug(f"股票 {stock_code} 被过滤 - 市值不足")
                elif calculate_annual_volatility(df) < MARKET_SECTIONS[section]["min_volatility"] or \
                     calculate_annual_volatility(df) > MARKET_SECTIONS[section]["max_volatility"]:
                    section_counts[section]["volatility_filtered"] += 1
                    logger.debug(f"股票 {stock_code} 被过滤 - 波动率异常")
                elif calculate_avg_volume(df) < MARKET_SECTIONS[section]["min_daily_volume"] / 10000:
                    section_counts[section]["liquidity_filtered"] += 1
                    logger.debug(f"股票 {stock_code} 被过滤 - 流动性不足")
                else:
                    logger.debug(f"股票 {stock_code} 被过滤 - 未知原因")
                
                cache_filter_result(stock_code, False)
                return None
            
            section_counts[section]["suitable"] += 1
            
            # 5. 计算策略得分
            score = calculate_stock_strategy_score(stock_code, df)
            
            if score > 0:
                # 6. 缓存结果
                cache_filter_result(stock_code, True)
                cache_score(stock_code, score)
                
                # 7. 更新板块计数器
                section_counts[section]["scored"] += 1
                
                return {
                    "code": stock_code,
                    "name": stock_name,
                    "score": score,
                    "df": df,
                    "section": section
                }
            
            # 7. 缓存筛选失败结果
            cache_filter_result(stock_code, False)
            return None
        
        # 6. 并行处理股票（优化并发参数）
        results = []
        # 降低并发数，确保不会触发AkShare限制
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 增加每批处理的股票数量
            for i in range(0, len(stock_list), 15):
                batch = stock_list[i:i+15]
                batch_results = list(executor.map(process_stock, batch))
                results.extend(batch_results)
                # 减少等待时间
                time.sleep(0.8)
        
        # 7. 收集结果
        for result in results:
            if result is not None:
                section = result["section"]
                section_stocks[section].append(result)
        
        # 8. 记录各板块筛选结果（包含详细过滤原因）
        for section, counts in section_counts.items():
            if counts["total"] > 0:
                logger.info(f"【筛选详细统计】板块 {section}:")
                logger.info(f"  - 总股票数量: {counts['total']}")
                logger.info(f"  - 数据量不足: {counts['data_filtered']} ({counts['data_filtered']/counts['total']*100:.1f}%)")
                logger.info(f"  - 市值过滤: {counts['market_cap_filtered']} ({counts['market_cap_filtered']/counts['total']*100:.1f}%)")
                logger.info(f"  - 波动率过滤: {counts['volatility_filtered']} ({counts['volatility_filtered']/counts['total']*100:.1f}%)")
                logger.info(f"  - 流动性过滤: {counts['liquidity_filtered']} ({counts['liquidity_filtered']/counts['total']*100:.1f}%)")
                logger.info(f"  - 通过三重过滤: {counts['suitable']} ({counts['suitable']/counts['total']*100:.1f}%)")
                logger.info(f"  - 评分>0: {counts['scored']} ({counts['scored']/counts['total']*100:.1f}%)")
        
        # 9. 对每个板块的股票按得分排序，并取前N只
        top_stocks_by_section = {}
        for section, stocks in section_stocks.items():
            if stocks:
                stocks.sort(key=lambda x: x["score"], reverse=True)
                top_stocks = stocks[:MAX_STOCKS_PER_SECTION]
                top_stocks_by_section[section] = top_stocks
                logger.info(f"【最终结果】板块 {section} 筛选出 {len(top_stocks)} 只股票")
                # 记录筛选出的股票详情
                for i, stock in enumerate(top_stocks):
                    logger.info(f"  {i+1}. {stock['name']}({stock['code']}) - 评分: {stock['score']:.2f}")
            else:
                logger.info(f"【最终结果】板块 {section} 无符合条件的股票")
        
        # 10. 更新基础信息中的市值和评分
        updated_records = []
        for section, stocks in top_stocks_by_section.items():
            for stock in stocks:
                stock_code = str(stock["code"]).zfill(6)
                stock_name = stock["name"]
                section = stock["section"]
                market_cap = calculate_market_cap(stock["df"], stock_code)
                score = stock["score"]
                
                # 更新基础信息
                updated_records.append({
                    "code": stock_code,
                    "name": stock_name,
                    "section": section,
                    "market_cap": market_cap,
                    "score": score,
                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
        
        # 11. 保存更新后的基础信息
        if updated_records:
            # 创建临时DataFrame用于更新
            update_df = pd.DataFrame(updated_records)
            
            # 仅更新市值和评分，不改变基础信息结构
            for _, record in update_df.iterrows():
                mask = basic_info_df["code"] == record["code"]
                if mask.any():
                    # 更新现有记录的市值和评分
                    basic_info_df.loc[mask, "market_cap"] = record["market_cap"]
                    basic_info_df.loc[mask, "score"] = record["score"]
                    basic_info_df.loc[mask, "last_update"] = record["last_update"]
            
            # 12. 保存更新
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
            logger.info(f"股票基础信息已更新，共 {len(basic_info_df)} 条记录")
            
            # 使用 git_utils.py 中已有的工具函数
            try:
                logger.info("正在提交更新后的股票基础信息到GitHub仓库...")
                commit_message = "自动更新股票基础信息 [策略执行]"
                if commit_and_push_file(BASIC_INFO_FILE, commit_message):
                    logger.info("更新后的股票基础信息已成功提交并推送到GitHub仓库")
                else:
                    logger.warning("提交更新后的股票基础信息到GitHub仓库失败，但继续执行策略")
            except Exception as e:
                logger.warning(f"提交更新后的股票基础信息到GitHub仓库失败: {str(e)}")
                logger.warning(traceback.format_exc())
        
        return top_stocks_by_section
    
    except Exception as e:
        logger.error(f"获取优质股票列表失败: {str(e)}", exc_info=True)
        logger.error(traceback.format_exc())
        return {}

# ========== 以下是关键修改：移除旧的生成策略总结函数，改为生成每个板块的报告 ==========
def main():
    """主函数"""
    try:
        logger.info("===== 开始执行个股趋势策略(TickTen) =====")
        
        # 1. 获取适合策略的股票
        top_stocks_by_section = get_top_stocks_for_strategy()
        
        # 2. 为每个板块生成详细报告并推送
        total_stocks = 0
        for section, stocks in top_stocks_by_section.items():
            if stocks:
                total_stocks += len(stocks)
                # 生成板块报告
                section_report = generate_section_report(section, stocks)
                # 推送板块报告
                logger.info(f"推送 {section} 板块策略报告")
                send_wechat_message(section_report, message_type="stock_tickten")
                # 适当延时，避免消息推送过于频繁
                time.sleep(2)
        
        # 3. 生成并推送整体总结
        beijing_time = get_beijing_time()
        summary_lines = []
        summary_lines.append(f"📊 个股趋势策略执行总结 ({beijing_time.strftime('%Y-%m-%d %H:%M')})")
        summary_lines.append("──────────────────")
        summary_lines.append(f"✅ 共筛选出 {total_stocks} 只优质股票（按板块分布）:")
        
        for section, stocks in top_stocks_by_section.items():
            if stocks:
                summary_lines.append(f"  • {section}: {len(stocks)} 只")
        
        summary_lines.append("──────────────────")
        summary_lines.append("💡 操作指南:")
        summary_lines.append("1. 评分越高，趋势越强，可考虑适当增加仓位")
        summary_lines.append("2. 每只个股仓位≤15%，分散投资5-8只")
        summary_lines.append("3. 持续关注趋势变化，及时调整持仓")
        summary_lines.append("4. 科创板/创业板波动较大，注意控制风险")
        summary_lines.append("──────────────────")
        summary_lines.append("📊 数据来源: fish-etf (https://github.com/karmyshunde-sudo/fish-etf)")
        
        summary_message = "\n".join(summary_lines)
        logger.info("推送整体策略执行总结")
        send_wechat_message(summary_message, message_type="stock_tickten")
        
        logger.info("个股策略报告已成功发送至企业微信")
        logger.info("===== 个股趋势策略(TickTen)执行完成 =====")
    
    except Exception as e:
        error_msg = f"个股趋势策略执行失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        logger.error(traceback.format_exc())
        send_wechat_message(error_msg, message_type="error")

if __name__ == "__main__":
    main()
