#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股趋势跟踪策略（TickTen策略）
基于akshare实时爬取个股数据，应用流动性、波动率、市值三重过滤，筛选优质个股
按板块分类推送，每个板块最多10只，共40只
"""

import os
import logging
import pandas as pd
import numpy as np
import time
import akshare as ak
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
# ========== 以下是关键修改 ==========
from concurrent.futures import ThreadPoolExecutor
# ========== 以上是关键修改 ==========
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time
)
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

"""
==========================================
【参数详细说明】
以下参数可根据个人风险偏好调整
==========================================
"""

"""
板块定义参数说明：
- prefix: 股票代码前缀列表（用于识别属于该板块的股票）
  * 沪市主板：60开头（如600000）
  * 深市主板：00开头（如000001）
  * 创业板：30开头（如300001）
  * 科创板：688开头（如688001）

- min_market_cap: 最小市值（亿元）
  * 说明：低于此市值的股票将被过滤
  * 推荐值范围：
    - 沪市/深市主板：150-300亿
    - 创业板：80-150亿  
    - 科创板：50-100亿
  * 设置理由：过滤小市值股票，降低风险

- min_daily_volume: 最小日均成交额（元）
  * 说明：低于此成交额的股票将被过滤
  * 推荐值范围：
    - 沪市/深市主板：8000-15000万
    - 创业板：5000-10000万
    - 科创板：3000-8000万
  * 设置理由：确保足够流动性，避免无法交易

- max_volatility: 最大年化波动率（小数形式）
  * 说明：高于此波动率的股票将被过滤
  * 推荐值范围：
    - 沪市/深市主板：0.3-0.45（30%-45%）
    - 创业板：0.4-0.55（40%-55%）
    - 科创板：0.5-0.7（50%-70%）
  * 设置理由：过滤波动过大的股票，降低风险
"""
# 板块定义
MARKET_SECTIONS = {
    "沪市主板": {"prefix": ["60"], "min_market_cap": 50, "min_daily_volume": 50000000, "max_volatility": 0.4},
    "深市主板": {"prefix": ["00"], "min_market_cap": 50, "min_daily_volume": 50000000, "max_volatility": 0.4},
    "创业板": {"prefix": ["30"], "min_market_cap": 30, "min_daily_volume": 30000000, "max_volatility": 0.5},
    "科创板": {"prefix": ["688"], "min_market_cap": 20, "min_daily_volume": 20000000, "max_volatility": 0.6}
}

"""
策略核心参数说明：
- CRITICAL_VALUE_DAYS: 临界值计算周期（日）
  * 说明：用于计算均线的天数，值越大趋势越平滑但反应越慢
  * 推荐值：30-50（40是平衡点）
  * 默认值：40

- DEVIATION_THRESHOLD: 偏离阈值（小数形式）
  * 说明：用于判断是否超买/超卖的阈值
  * 推荐值：0.06-0.1（6%-10%）
  * 默认值：0.08（8%）

- VOLUME_CHANGE_THRESHOLD: 成交量变化阈值（小数形式）
  * 说明：用于确认信号的成交量变化要求
  * 推荐值：0.25-0.4（25%-40%）
  * 默认值：0.35（35%）

- MIN_CONSECUTIVE_DAYS: 最小连续站上/跌破天数
  * 说明：确认信号需要连续多少天在均线上方/下方
  * 推荐值：2-4
  * 默认值：3

- PATTERN_CONFIDENCE_THRESHOLD: 形态确认阈值（小数形式）
  * 说明：头肩顶/M头等形态的置信度阈值
  * 推荐值：0.6-0.8（60%-80%）
  * 默认值：0.7（70%）

- MAX_STOCK_POSITION: 单一个股最大仓位（小数形式）
  * 说明：单一个股在投资组合中的最大占比
  * 推荐值：0.05-0.2（5%-20%）
  * 默认值：0.15（15%）
"""
# 策略参数（针对个股优化）
CRITICAL_VALUE_DAYS = 40  # 临界值计算周期（40日均线）
DEVIATION_THRESHOLD = 0.08  # 偏离阈值（8%）
VOLUME_CHANGE_THRESHOLD = 0.35  # 成交量变化阈值（35%）
MIN_CONSECUTIVE_DAYS = 3  # 最小连续站上/跌破天数
PATTERN_CONFIDENCE_THRESHOLD = 0.7  # 形态确认阈值（70%置信度）
MAX_STOCK_POSITION = 0.15  # 单一个股最大仓位（15%）

"""
其他参数说明：
- MIN_DATA_DAYS: 最小数据天数
  * 说明：用于计算波动率等指标所需的最小历史数据天数
  * 推荐值：90-120
  * 默认值：100

- MAX_STOCKS_TO_ANALYZE: 每次分析的最大股票数量
  * 说明：避免请求过多导致被AkShare限制
  * 推荐值：400-600
  * 默认值：500

- MAX_STOCKS_PER_SECTION: 每个板块最多报告的股票数量
  * 说明：控制每个板块推送的股票数量上限，避免信息过载
  * 推荐值：5-15（太少可能错过机会，太多难以跟踪）
  * 默认值：10

- DATA_FETCH_DELAY: 数据请求间隔（秒）
  * 说明：避免被AkShare限制的请求间隔时间
  * 推荐值：0.4-0.6
  * 默认值：0.5
"""

# 其他参数
MIN_DATA_DAYS = 30  # 降低最小数据天数（用于计算指标）
MAX_STOCKS_TO_ANALYZE = 300  # 减少每次分析的最大股票数量（避免请求过多）
MAX_STOCKS_PER_SECTION = 8  # 每个板块最多报告的股票数量
DATA_FETCH_DELAY = 0.5  # 增加数据请求间隔（秒），避免被AkShare限制

"""
==========================================
【策略实现区】
以下为策略核心代码
==========================================
"""

def get_stock_section(stock_code: str) -> str:
    """
    判断股票所属板块
    
    Args:
        stock_code: 股票代码（不带市场前缀）
    
    Returns:
        str: 板块名称
    """
    for section, config in MARKET_SECTIONS.items():
        for prefix in config["prefix"]:
            if stock_code.startswith(prefix):
                return section
    return "其他板块"

def fetch_stock_list() -> pd.DataFrame:
    """
    从AkShare获取全市场股票列表
    
    Returns:
        pd.DataFrame: 股票列表（代码、名称、所属板块等）
    """
    try:
        logger.info("从AkShare获取全市场股票列表...")
        
        # 获取A股股票列表
        stock_list = ak.stock_info_a_code_name()
        
        if stock_list.empty:
            logger.error("获取股票列表失败：返回为空")
            return pd.DataFrame()
        
        # ========== 以下是关键修改 ==========
        # 记录初始股票数量
        initial_count = len(stock_list)
        logger.info(f"成功获取股票列表，共 {initial_count} 只股票（初始数量）")
        
        # 前置筛选条件：过滤ST股票和非主板/科创板/创业板股票
        stock_list = stock_list[~stock_list["name"].str.contains("ST")]
        stock_list = stock_list[
            stock_list["code"].str.startswith(("60", "00", "30", "688"))
        ]
        
        # 记录前置筛选后的股票数量
        filtered_count = len(stock_list)
        logger.info(f"【前置筛选】过滤ST股票和非主板/科创板/创业板股票后，剩余 {filtered_count} 只（过滤了 {initial_count - filtered_count} 只）")
        # ========== 以上是关键修改 ==========
        
        # 添加所属板块列
        stock_list["板块"] = stock_list["code"].apply(get_stock_section)
        
        logger.info(f"成功获取股票列表，共 {len(stock_list)} 只股票")
        return stock_list
    
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def fetch_stock_data(stock_code: str, days: int = 250) -> pd.DataFrame:
    """从AkShare获取个股历史数据
    Args:
        stock_code: 股票代码（不带市场前缀）
        days: 获取最近多少天的数据
    
    Returns:
        pd.DataFrame: 个股日线数据
    """
    try:
        # 确定市场前缀
        section = get_stock_section(stock_code)
        if section == "沪市主板" or section == "科创板":
            market_prefix = "sh"
        else:  # 深市主板、创业板
            market_prefix = "sz"
        
        # ========== 关键修复 ==========
        # 使用AkShare期望的格式（000001.SZ）
        full_code = f"{stock_code}.{'SZ' if market_prefix == 'sz' else 'SH'}"
        
        # 计算日期范围
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        logger.debug(f"从AkShare获取股票 {full_code} 数据，时间范围: {start_date} 至 {end_date}")
        
        # 尝试使用多个接口获取数据
        df = None
        for attempt in range(5):  # 增加重试次数到5次
            try:
                # 尝试使用stock_zh_a_hist接口
                df = ak.stock_zh_a_hist(symbol=full_code, period="daily", 
                                       start_date=start_date, end_date=end_date, 
                                       adjust="qfq")
                if not df.empty:
                    logger.debug(f"尝试{attempt+1}/5: 使用stock_zh_a_hist接口成功获取股票 {full_code} 数据")
                    break
            except Exception as e:
                logger.debug(f"尝试{attempt+1}/5: 使用stock_zh_a_hist接口获取股票 {full_code} 数据失败: {str(e)}")
            
            # 增加等待时间（指数退避）
            wait_time = 0.5 * (2 ** attempt)  # 0.5, 1.0, 2.0, 4.0, 8.0秒
            logger.debug(f"等待 {wait_time:.1f} 秒后重试...")
            time.sleep(wait_time)
        
        # 如果新接口失败，尝试旧接口
        if df is None or df.empty:
            logger.debug(f"使用stock_zh_a_hist接口获取股票 {full_code} 数据失败，尝试旧接口")
            for attempt in range(3):  # 尝试3次
                try:
                    df = ak.stock_zh_a_daily(symbol=full_code, 
                                           start_date=start_date, 
                                           end_date=end_date, 
                                           adjust="qfq")
                    if not df.empty:
                        logger.debug(f"尝试{attempt+1}/3: 使用旧接口成功获取股票 {full_code} 数据")
                        break
                except Exception as e:
                    logger.debug(f"尝试{attempt+1}/3: 使用旧接口获取股票 {full_code} 数据失败: {str(e)}")
                
                # 增加等待时间
                time.sleep(1.0 * (2 ** attempt))
        
        # 如果还是失败，返回空DataFrame
        if df is None or df.empty:
            logger.warning(f"获取股票 {stock_code} 数据失败，所有接口均无效")
            return pd.DataFrame()
        
        # 根据实际返回的列名进行映射
        if 'date' in df.columns:
            # 英文列名映射到标准列名
            column_mapping = {
                'date': 'date',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume',
                'amount': 'amount',
                'outstanding_share': 'outstanding_share',
                'turnover': 'turnover'
            }
        else:
            # 中文列名映射到标准列名
            column_mapping = {
                '日期': 'date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '换手率': 'turnover'
            }
        
        # 创建新的DataFrame，避免SettingWithCopyWarning
        new_df = pd.DataFrame()
        for col, std_col in column_mapping.items():
            if col in df.columns:
                new_df[std_col] = df[col]
        
        # 检查是否包含必要列
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in new_df.columns]
        
        if missing_columns:
            logger.warning(f"股票 {full_code} 数据缺少必要列: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # 确保日期列是datetime类型
        if 'date' in new_df.columns:
            try:
                new_df['date'] = pd.to_datetime(new_df['date'])
            except Exception as e:
                logger.error(f"股票 {full_code} 日期格式转换失败: {str(e)}")
                return pd.DataFrame()
        else:
            logger.error(f"股票 {full_code} 缺少日期列")
            return pd.DataFrame()
        
        # 确保数值列是数值类型
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
        for col in numeric_columns:
            if col in new_df.columns:
                try:
                    new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
                except Exception as e:
                    logger.warning(f"股票 {full_code} {col} 列转换为数值失败: {str(e)}")
        
        # 排序并重置索引
        new_df = new_df.sort_values('date').reset_index(drop=True)
        
        # 检查数据量
        logger.debug(f"股票 {full_code} 获取到 {len(new_df)} 天数据")
        
        # 确保数据不是全NaN
        if new_df[['open', 'high', 'low', 'close', 'volume']].isna().all().all():
            logger.warning(f"股票 {full_code} 数据全为NaN")
            return pd.DataFrame()
        
        return new_df
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def preprocess_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """预处理股票数据，计算必要指标
    Args:
        df: 股票日线数据
    
    Returns:
        pd.DataFrame: 预处理后的数据
    """
    try:
        if df is None or df.empty:
            logger.warning("股票数据为空，无法预处理")
            return df
        
        # 确保DataFrame是副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 确保日期列排序
        if 'date' in df.columns:
            df = df.sort_values('date')
        
        # 检查必要列
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"预处理失败：数据缺少必要列 {', '.join(missing_columns)}")
            return df
        
        # 检查close列是否为有效数值
        if 'close' not in df.columns or df['close'].isna().all():
            logger.error("预处理失败：close列不存在或全为NaN")
            return df
        
        # 计算移动平均线
        df["ma5"] = df["close"].rolling(window=5).mean()
        df["ma10"] = df["close"].rolling(window=10).mean()
        df["ma20"] = df["close"].rolling(window=20).mean()
        df["ma40"] = df["close"].rolling(window=CRITICAL_VALUE_DAYS).mean()
        
        # 计算成交量移动平均
        if 'volume' in df.columns:
            df["volume_ma5"] = df["volume"].rolling(window=5).mean()
        
        # 计算涨跌幅
        if 'close' in df.columns:
            df["pct_change"] = df["close"].pct_change() * 100
        
        # 计算波动率（20日年化波动率）
        if 'pct_change' in df.columns:
            df["volatility"] = df["pct_change"].rolling(window=20).std() * np.sqrt(252)
        
        # 计算MACD
        if 'close' in df.columns:
            df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
            df["ema26"] = df["close"].ewm(span=26, adjust=False).mean()
            df["macd"] = df["ema12"] - df["ema26"]
            df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
            df["hist"] = df["macd"] - df["signal"]
        
        # 计算RSI
        if 'close' in df.columns:
            delta = df["close"].diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            avg_gain = gain.rolling(window=14).mean()
            avg_loss = loss.rolling(window=14).mean()
            rs = avg_gain / avg_loss.replace(0, np.nan)  # 避免除零错误
            df["rsi"] = 100 - (100 / (1 + rs))
        
        # 计算布林带
        if 'close' in df.columns and 'ma20' in df.columns:
            df["std"] = df["close"].rolling(window=20).std()
            df["upper_band"] = df["ma20"] + (df["std"] * 2)
            df["lower_band"] = df["ma20"] - (df["std"] * 2)
        
        # 计算ATR (平均真实波幅)
        if all(col in df.columns for col in ['high', 'low', 'close']):
            high_low = df["high"] - df["low"]
            high_close = np.abs(df["high"] - df["close"].shift())
            low_close = np.abs(df["low"] - df["close"].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = np.max(ranges, axis=1)
            df["atr"] = pd.Series(true_range).rolling(window=14).mean()
        
        logger.debug("股票数据预处理完成")
        return df
    
    except Exception as e:
        logger.error(f"预处理股票数据失败: {str(e)}", exc_info=True)
        return df

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
    
    # 检查是否已经预计算了关键指标
    if "above_ma40" in df.columns:
        # 从最新日期开始向前检查
        consecutive_days = 0
        for i in range(len(df)-1, -1, -1):
            if i < period - 1:
                break
                
            if df["above_ma40"].iloc[i]:
                consecutive_days += 1
            else:
                break
        return consecutive_days
    
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
    
    # 检查是否已经预计算了关键指标
    if "below_ma40" in df.columns:
        # 从最新日期开始向前检查
        consecutive_days = 0
        for i in range(len(df)-1, -1, -1):
            if i < period - 1:
                break
                
            if df["below_ma40"].iloc[i]:
                consecutive_days += 1
            else:
                break
        return consecutive_days
    
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
    # 检查是否已经预计算了成交量变化率
    if "volume_change" in df.columns:
        return df["volume_change"].iloc[-1]
    
    if len(df) < days + 1:
        return 0.0
    
    recent_volume = df["成交量"].iloc[-days:].mean()
    previous_volume = df["成交量"].iloc[-(days*2):-days].mean()
    
    if previous_volume > 0:
        return (recent_volume - previous_volume) / previous_volume * 100
    return 0.0

def calculate_annual_volatility(df: pd.DataFrame) -> float:
    """计算年化波动率"""
    # 检查是否已经预计算了年化波动率
    if "annual_volatility" in df.columns:
        return df["annual_volatility"].iloc[-1]
    
    if len(df) < 30:
        return 0.0
    
    # 计算日收益率
    daily_returns = df["收盘"].pct_change().dropna()
    
    # 年化波动率 = 日波动率 * sqrt(252)
    if len(daily_returns) > 1:
        daily_vol = daily_returns.std()
        return daily_vol * np.sqrt(252)
    
    return 0.0

# ========== 以下是关键修改 ==========
def calculate_market_cap(df: pd.DataFrame, stock_code: str) -> float:
    """计算股票市值
    Args:
        df: 股票日线数据
        stock_code: 股票代码
    
    Returns:
        float: 市值(亿元)
    """
    try:
        if df is None or df.empty or len(df) < 1:
            logger.debug(f"股票 {stock_code} 数据不足，无法计算市值")
            return 0.0
        
        # 尝试从本地数据获取市值
        if "market_cap" in df.columns:
            market_cap = df["market_cap"].iloc[-1]
            if not pd.isna(market_cap) and market_cap > 0:
                return market_cap / 10000  # 转换为亿元
        
        # 获取最新收盘价
        if "close" in df.columns:
            current_price = df["close"].iloc[-1]
        else:
            logger.debug(f"股票 {stock_code} 缺少收盘价数据，无法计算市值")
            return 0.0
        
        # ========== 关键修复 ==========
        # 尝试使用akshare获取实时行情信息（添加重试机制）
        stock_info = None
        for attempt in range(3):
            try:
                stock_info = ak.stock_zh_a_spot_em()
                if not stock_info.empty:
                    logger.debug(f"成功获取实时行情数据，尝试匹配股票 {stock_code}")
                    break
            except Exception as e:
                logger.debug(f"尝试{attempt+1}/3: 获取实时行情数据失败: {str(e)}")
                time.sleep(1.5)  # 增加等待时间
        
        if stock_info is not None and not stock_info.empty:
            # 标准化股票代码格式（处理可能的前缀）
            stock_code_std = stock_code.zfill(6)
            if stock_code.startswith(('6', '9')):
                stock_code_std = f"sh{stock_code_std}"
            else:
                stock_code_std = f"sz{stock_code_std}"
            
            # 尝试匹配股票
            stock_info = stock_info[stock_info["代码"] == stock_code_std]
            
            # 如果没有找到，尝试只匹配数字部分
            if stock_info.empty:
                stock_info = stock_info[stock_info["代码"].str[-6:] == stock_code_std[-6:]]
            
            # 如果还是没有找到，尝试使用名称匹配
            if stock_info.empty and "name" in df.attrs:
                stock_info = stock_info[stock_info["名称"].str.contains(df.attrs["name"])]
            
            if not stock_info.empty:
                # 根据实际返回列名获取流通市值
                if "流通市值" in stock_info.columns:
                    market_cap = stock_info["流通市值"].iloc[0]
                    if not pd.isna(market_cap) and market_cap > 0:
                        logger.debug(f"使用stock_zh_a_spot_em获取流通市值: {market_cap/10000:.2f}亿元")
                        return market_cap / 10000  # 转换为亿元
        
        # 如果以上方法都失败，尝试使用历史数据估算
        if len(df) >= 250:  # 至少一年数据
            if "volume" in df.columns and "close" in df.columns:
                avg_volume = df["volume"].iloc[-250:].mean()
                avg_price = df["close"].iloc[-250:].mean()
                if avg_volume > 0 and avg_price > 0:
                    # 估算日均成交额(万元)
                    daily_turnover = avg_volume * avg_price / 10000
                    # 假设换手率为2%，估算总市值
                    if daily_turnover > 0:
                        estimated_market_cap = daily_turnover / 0.02  # 换手率2%
                        logger.debug(f"使用历史数据估算市值: {estimated_market_cap:.2f}亿元")
                        return estimated_market_cap
        
        logger.debug(f"股票 {stock_code} 市值计算失败，返回默认值0.0")
        return 0.0
    
    except Exception as e:
        logger.error(f"估算{stock_code}市值失败: {str(e)}", exc_info=True)
        return 0.0
# ========== 以上是关键修改 ==========

def is_stock_suitable(stock_code: str, df: pd.DataFrame) -> bool:
    """判断个股是否适合策略（流动性、波动率、市值三重过滤）
    Args:
        stock_code: 股票代码
        df: 股票日线数据
    
    Returns:
        bool: 是否适合策略
    """
    try:
        if df is None or df.empty or len(df) < MIN_DATA_DAYS:
            logger.debug(f"股票 {stock_code} 数据不足，跳过")
            return False
        
        # 获取股票所属板块
        section = get_stock_section(stock_code)
        if section == "其他板块" or section not in MARKET_SECTIONS:
            logger.debug(f"股票 {stock_code} 不属于任何板块，跳过")
            return False
        
        # 获取板块配置
        section_config = MARKET_SECTIONS[section]
        
        # 1. 流动性过滤（日均成交>设定阈值）
        # 修正：A股的成交量单位是"手"（1手=100股），需要乘以100
        if 'volume' in df.columns and 'close' in df.columns and len(df) >= 20:
            # 修正：成交量单位是"手"，需要乘以100转换为股
            daily_volume = df["volume"].iloc[-20:].mean() * 100 * df["close"].iloc[-20:].mean()
            logger.info(f"【流动性过滤】股票 {stock_code} - {section} - 日均成交额: {daily_volume/10000:.2f}万元, 要求: >{section_config['min_daily_volume']/10000:.2f}万元")
            
            if daily_volume < section_config["min_daily_volume"]:
                logger.info(f"【流动性过滤】股票 {stock_code} - {section} - 流动性过滤失败（日均成交额不足）")
                return False
            else:
                logger.info(f"【流动性过滤】股票 {stock_code} - {section} - 通过流动性过滤")
        else:
            logger.debug(f"股票 {stock_code} 缺少成交量或收盘价数据，无法进行流动性过滤")
            return False
        
        # 2. 波动率过滤（年化波动率<设定阈值）
        annual_volatility = calculate_annual_volatility(df)
        logger.info(f"【波动率过滤】股票 {stock_code} - {section} - 年化波动率: {annual_volatility:.2%}, 要求: <{section_config['max_volatility']:.0%}")
        
        if annual_volatility > section_config["max_volatility"]:
            logger.info(f"【波动率过滤】股票 {stock_code} - {section} - 波动率过滤失败（波动率过高）")
            return False
        else:
            logger.info(f"【波动率过滤】股票 {stock_code} - {section} - 通过波动率过滤")
        
        # 3. 市值过滤（市值>设定阈值）
        market_cap = calculate_market_cap(df, stock_code)
        logger.info(f"【市值过滤】股票 {stock_code} - {section} - 市值: {market_cap:.2f}亿元, 要求: >{section_config['min_market_cap']:.2f}亿元")
        
        if market_cap < section_config["min_market_cap"]:
            logger.info(f"【市值过滤】股票 {stock_code} - {section} - 市值过滤失败（市值不足）")
            return False
        else:
            logger.info(f"【市值过滤】股票 {stock_code} - {section} - 通过市值过滤")
        
        logger.info(f"【最终结果】股票 {stock_code} - {section} - 通过所有过滤条件")
        return True
    
    except Exception as e:
        logger.error(f"筛选股票{stock_code}失败: {str(e)}", exc_info=True)
        return False

def calculate_stock_strategy_score(stock_code: str, df: pd.DataFrame) -> float:
    """计算股票策略评分
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
        required_columns = ['close', 'volume', 'ma5', 'ma10', 'ma20', 'ma40']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.debug(f"股票 {stock_code} 数据缺少必要列: {', '.join(missing_columns)}，无法计算策略评分")
            return 0.0
        
        # 获取最新数据
        current = df["close"].iloc[-1]
        if pd.isna(current) or current <= 0:
            logger.debug(f"股票 {stock_code} 无效的收盘价: {current}")
            return 0.0
        
        volume = df["volume"].iloc[-1] if "volume" in df.columns and len(df) >= 1 else 0
        volume_ma5 = df["volume_ma5"].iloc[-1] if "volume_ma5" in df.columns and len(df) >= 1 else 0
        
        # 1. 趋势评分 (40%)
        trend_score = 0.0
        if len(df) >= 40:
            ma5 = df["ma5"].iloc[-1] if "ma5" in df.columns else current
            ma10 = df["ma10"].iloc[-1] if "ma10" in df.columns else current
            ma20 = df["ma20"].iloc[-1] if "ma20" in df.columns else current
            ma40 = df["ma40"].iloc[-1] if "ma40" in df.columns else current
            
            # 检查短期均线是否在长期均线上方（多头排列）
            if ma5 > ma10 > ma20 > ma40 and all(not pd.isna(x) for x in [ma5, ma10, ma20, ma40]):
                trend_score += 20  # 多头排列，加20分
            
            # 检查价格是否在均线上方
            if not pd.isna(ma20) and current > ma20:
                trend_score += 10  # 价格在20日均线上方，加10分
            
            # 检查趋势强度
            if len(df) >= 20:
                price_change_20 = (current - df["close"].iloc[-20]) / df["close"].iloc[-20] * 100
                if not pd.isna(price_change_20) and price_change_20 > 5:
                    trend_score += 10  # 20日涨幅大于5%，加10分
        
        # 2. 动量评分 (20%)
        momentum_score = 0.0
        if "hist" in df.columns and len(df) >= 2:
            macd_hist = df["hist"].iloc[-1]
            macd_hist_prev = df["hist"].iloc[-2]
            
            # MACD柱状体增加
            if not pd.isna(macd_hist) and not pd.isna(macd_hist_prev) and macd_hist > macd_hist_prev and macd_hist > 0:
                momentum_score += 10  # MACD柱状体增加且为正，加10分
            
            # RSI指标
            if "rsi" in df.columns:
                rsi = df["rsi"].iloc[-1]
                if not pd.isna(rsi):
                    if 50 < rsi < 70:
                        momentum_score += 10  # RSI在50-70之间，加10分
                    elif rsi >= 70:
                        momentum_score += 5  # RSI大于70，加5分
        
        # 3. 量能评分 (20%)
        volume_score = 0.0
        if volume_ma5 > 0 and volume > 0:
            volume_ratio = volume / volume_ma5
            
            # 量能放大
            if volume_ratio > 1.5:
                volume_score += 10  # 量能放大50%以上，加10分
            elif volume_ratio > 1.2:
                volume_score += 5  # 量能放大20%以上，加5分
            
            # 量价配合
            if len(df) >= 2:
                price_change = (current - df["close"].iloc[-2]) / df["close"].iloc[-2] * 100
                if price_change > 0 and volume_ratio > 1.0:
                    volume_score += 10  # 价格上涨且量能放大，加10分
        
        # 4. 波动率评分 (20%)
        volatility_score = 0.0
        if "volatility" in df.columns and len(df) >= 20:
            volatility = df["volatility"].iloc[-1]
            
            if not pd.isna(volatility):
                # 适中的波动率
                if 15 <= volatility <= 30:
                    volatility_score += 10  # 波动率在15%-30%之间，加10分
                elif volatility > 30:
                    volatility_score += 5  # 波动率大于30%，加5分
                
                # 波动率趋势
                if len(df) >= 21:
                    prev_volatility = df["volatility"].iloc[-21]
                    if not pd.isna(prev_volatility) and prev_volatility > 0:
                        volatility_change = (volatility - prev_volatility) / prev_volatility
                        
                        if -0.1 <= volatility_change <= 0.1:
                            volatility_score += 10  # 波动率稳定，加10分
        
        # 综合评分
        total_score = trend_score + momentum_score + volume_score + volatility_score
        total_score = max(0, min(100, total_score))  # 限制在0-100范围内
        
        logger.debug(f"股票 {stock_code} 策略评分: {total_score:.2f} "
                     f"(趋势={trend_score:.1f}, 动量={momentum_score:.1f}, "
                     f"量能={volume_score:.1f}, 波动率={volatility_score:.1f})")
        
        return total_score
    
    except Exception as e:
        logger.error(f"计算股票 {stock_code} 策略评分失败: {str(e)}", exc_info=True)
        return 0.0

def is_in_volatile_market(df: pd.DataFrame, period: int = CRITICAL_VALUE_DAYS) -> tuple:
    """判断是否处于震荡市
    
    Returns:
        tuple: (是否震荡市, 穿越次数, 最近10天偏离率范围)
    """
    if len(df) < 10:
        return False, 0, (0, 0)
    
    # 检查是否已经预计算了关键指标
    if "ma40" in df.columns:
        # 获取收盘价和均线序列
        close_prices = df["收盘"].values
        ma_values = df["ma40"].values
        
        # 检查是否连续10天在均线附近波动
        last_10_days = df.tail(10)
        deviations = []
        for i in range(len(last_10_days)):
            # 确保有足够的数据计算均线
            if i < period - 1 or np.isnan(ma_values[-10 + i]):
                continue
                
            deviation = (close_prices[-10 + i] - ma_values[-10 + i]) / ma_values[-10 + i] * 100
            # 根据板块不同，设置不同的震荡阈值
            section = get_stock_section(df.attrs.get("stock_code", ""))
            if section in ["科创板", "创业板"]:
                max_deviation = 10.0  # 科创板、创业板波动更大
            else:
                max_deviation = 8.0   # 主板波动较小
            
            if abs(deviation) > max_deviation:
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
    
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=period).mean().values
    
    # 检查是否连续10天在均线附近波动
    last_10_days = df.tail(10)
    deviations = []
    for i in range(len(last_10_days)):
        # 确保有足够的数据计算均线
        if i < period - 1 or np.isnan(ma_values[-10 + i]):
            continue
            
        deviation = (close_prices[-10 + i] - ma_values[-10 + i]) / ma_values[-10 + i] * 100
        # 根据板块不同，设置不同的震荡阈值
        section = get_stock_section(df.attrs.get("stock_code", ""))
        if section in ["科创板", "创业板"]:
            max_deviation = 10.0  # 科创板、创业板波动更大
        else:
            max_deviation = 8.0   # 主板波动较小
        
        if abs(deviation) > max_deviation:
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
    
    # 检查是否已经预计算了关键指标
    if "ma40" in df.columns:
        # 获取收盘价
        close_prices = df["收盘"].values
    else:
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
        if peak2_price < peak1_price and peak2_price > peak1_price * 0.92:
            # 检查中间是否有明显低点
            trough_idx = peak1_idx + np.argmin(close_prices[peak1_idx:peak2_idx])
            trough_price = close_prices[trough_idx]
            
            # 检查低点是否明显
            if trough_price < peak1_price * 0.95 and trough_price < peak2_price * 0.95:
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
            if shoulder_similarity > 0.8 and head_price > neckline_price * 1.1:
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

def calculate_stock_stop_loss(current_price: float, signal: str, deviation: float, section: str) -> float:
    """计算个股止损位"""
    # 根据板块不同，设置不同的止损幅度
    if section in ["科创板", "创业板"]:
        stop_loss_pct = 0.10  # 科创板、创业板止损10%
    else:
        stop_loss_pct = 0.08  # 主板止损8%
    
    if signal == "YES":
        # 上涨趋势中，止损设在5日均线下方
        return current_price * (1 - stop_loss_pct)
    else:
        # 下跌趋势中，止损设在前高上方
        return current_price * (1 + 0.05)

def calculate_stock_take_profit(current_price: float, signal: str, deviation: float, section: str) -> float:
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
    stock_code = stock_info["code"]
    stock_name = stock_info["name"]
    section = get_stock_section(stock_code)
    
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
        
        # 根据板块调整操作建议
        if section in ["科创板", "创业板"]:
            position_pct = 10
            max_position = 30
        else:
            position_pct = 15
            max_position = 40
        
        message = (
            f"【震荡市】{section} | 连续10日价格反复穿均线（穿越{cross_count}次），偏离率范围[{min_dev:.2f}%~{max_dev:.2f}%]\n"
            f"✅ 操作建议：\n"
            f"  • 上沿操作（价格≈{upper_band:.2f}）：小幅减仓{position_pct}%-{position_pct+5}%\n"
            f"  • 下沿操作（价格≈{lower_band:.2f}）：小幅加仓{position_pct}%-{position_pct+5}%\n"
            f"  • 总仓位严格控制在≤{max_position}%\n"
            f"⚠️ 避免频繁交易，等待趋势明朗\n"
        )
        return message
    
    # 1. YES信号：当前价格 ≥ 40日均线
    if current >= critical:
        # 子条件1：首次突破（价格刚站上均线，连续3-4日站稳+成交量放大35%+）
        if consecutive == 1 and volume_change > 35:
            # 根据板块调整仓位
            if section in ["科创板", "创业板"]:
                position_pct = 8
            else:
                position_pct = 12
            
            message = (
                f"【首次突破】{section} | 连续{consecutive}天站上40日均线，成交量放大{volume_change:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 建仓{position_pct}%（单一个股上限{int(MAX_STOCK_POSITION * 100)}%）\n"
                f"  • 止损位：{calculate_stock_stop_loss(current, 'YES', deviation, section):.2f}（-{int((1-calculate_stock_stop_loss(current, 'YES', deviation, section)/current)*100)}%）\n"
                f"  • 目标位：{calculate_stock_take_profit(current, 'YES', deviation, section):.2f}（+15%）\n"
                f"⚠️ 注意：若收盘跌破5日均线，立即减仓50%\n"
            )
        # 子条件1：首次突破（价格刚站上均线，连续3-4日站稳+成交量放大35%+）
        elif 2 <= consecutive <= 4 and volume_change > 35:
            # 根据板块调整仓位
            if section in ["科创板", "创业板"]:
                position_pct = 8
            else:
                position_pct = 12
            
            message = (
                f"【首次突破确认】{section} | 连续{consecutive}天站上40日均线，成交量放大{volume_change:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 可加仓至{position_pct}%\n"
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
                    f"【趋势稳健】{section} | 连续{consecutive}天站上40日均线，偏离率{deviation:.2f}%\n"
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
                    f"【趋势较强】{section} | 连续{consecutive}天站上40日均线，偏离率{deviation:.2f}%\n"
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
                    f"【超买风险】{section} | 连续{consecutive}天站上40日均线，偏离率{deviation:.2f}%\n"
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
            # 根据板块调整仓位
            if section in ["科创板", "创业板"]:
                reduce_pct = 80
                target_pct = 20
            else:
                reduce_pct = 70
                target_pct = 30
            
            message = (
                f"【首次跌破】{section} | 连续{consecutive}天跌破40日均线，成交量放大{volume_change:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 立即减仓{reduce_pct}%\n"
                f"  • 止损位：40日均线上方5%（约{critical * 1.05:.2f}）\n"
                f"⚠️ 若收盘未收回均线，明日继续减仓至{target_pct}%\n"
            )
        # 子条件1：首次跌破（价格刚跌穿均线，连续2-3日未收回+成交量放大）
        elif 2 <= consecutive <= 3 and volume_change > 35:
            # 根据板块调整仓位
            if section in ["科创板", "创业板"]:
                target_pct = 20
            else:
                target_pct = 30
            
            message = (
                f"【首次跌破确认】{section} | 连续{consecutive}天跌破40日均线，成交量放大{volume_change:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 严格止损，仓位降至{target_pct}%\n"
                f"  • 止损位：40日均线下方5%（约{critical * 0.95:.2f}）\n"
                f"⚠️ 信号确认，避免侥幸心理\n"
            )
        # 子条件2：持续跌破（价格维持在均线下）
        else:
            # 场景A：偏离率≥-8%（下跌初期）
            if deviation >= -8.0:
                # 根据板块调整仓位
                if section in ["科创板", "创业板"]:
                    max_position = 20
                else:
                    max_position = 30
                
                message = (
                    f"【下跌初期】{section} | 连续{consecutive}天跌破40日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 轻仓观望（仓位≤{max_position}%）\n"
                    f"  • 反弹至均线附近（约{critical:.2f}）减仓剩余仓位\n"
                    f"  • 暂不考虑新增仓位\n"
                    f"⚠️ 重点观察：收盘站上5日均线，可轻仓试多\n"
                )
            # 场景B：-15%≤偏离率＜-8%（下跌中期）
            elif -15.0 <= deviation < -8.0:
                # 根据板块调整仓位
                if section in ["科创板", "创业板"]:
                    test_pct = 5
                else:
                    test_pct = 10
                
                message = (
                    f"【下跌中期】{section} | 连续{consecutive}天跌破40日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 空仓为主，避免抄底\n"
                    f"  • 仅可试仓{test_pct}%\n"
                    f"  • 严格止损：收盘跌破前低即离场\n"
                    f"⚠️ 重点观察：行业基本面是否有利空\n"
                )
            # 场景C：偏离率＜-15%（超卖机会）
            else:
                # 根据板块调整仓位
                if section in ["科创板", "创业板"]:
                    add_pct = 8
                else:
                    add_pct = 10
                
                message = (
                    f"【超卖机会】{section} | 连续{consecutive}天跌破40日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 小幅加仓{add_pct}%\n"
                    f"  • 目标价：偏离率≥-8%（约{critical * 0.92:.2f}）\n"
                    f"  • 达到目标即卖出加仓部分\n"
                    f"⚠️ 重点观察：若跌破前低，立即止损\n"
                )
    
    return message

def get_top_stocks_for_strategy() -> Dict[str, List[Dict]]:
    """按板块获取适合策略的股票
    Returns:
        Dict[str, List[Dict]]: 按板块组织的股票信息
    """
    try:
        # 1. 获取全市场股票列表
        stock_list = fetch_stock_list()
        if stock_list.empty:
            logger.error("获取股票列表失败，无法继续")
            return {}
        
        # 记录初始股票数量
        total_initial = len(stock_list)
        logger.info(f"筛选前 {total_initial} 只股票（总数量）")
        
        # 2. 按板块分组处理
        section_stocks = {section: [] for section in MARKET_SECTIONS.keys()}
        
        # 使用并行化获取股票数据
        stock_codes = stock_list["code"].tolist()
        stock_names = stock_list["name"].tolist()
        
        # 初始化各板块计数器
        section_counts = {section: {"total": 0, "data_ok": 0, "suitable": 0, "scored": 0}
                         for section in MARKET_SECTIONS.keys()}
        
        def process_stock(i):
            stock_code = str(stock_codes[i])
            stock_name = stock_names[i]
            
            # 获取板块
            section = get_stock_section(stock_code)
            if section not in MARKET_SECTIONS:
                logger.debug(f"股票 {stock_name}({stock_code}) 不属于任何板块，跳过")
                return None
            
            # 更新板块计数器
            section_counts[section]["total"] += 1
            logger.debug(f"正在分析股票: {stock_name}({stock_code})| {section}")
            
            # 获取日线数据
            df = fetch_stock_data(stock_code)
            
            # ========== 关键修复 ==========
            # 降低最小数据天数要求（从100天降至30天）
            if df is None or df.empty:
                logger.debug(f"股票 {stock_name}({stock_code}) 数据为空，跳过")
                return None
            
            # 检查必要列
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.debug(f"股票 {stock_name}({stock_code}) 数据缺少必要列: {', '.join(missing_columns)}，跳过")
                return None
            
            # 降低最小数据天数要求（从100天降至30天）
            if len(df) < 30:
                logger.debug(f"股票 {stock_name}({stock_code}) 数据量不足({len(df)}天)，跳过")
                return None
            
            # 更新板块计数器
            section_counts[section]["data_ok"] += 1
            
            # 设置股票代码属性，便于后续识别
            df.attrs["stock_code"] = stock_code
            
            # 检查是否适合策略
            if is_stock_suitable(stock_code, df):
                # 更新板块计数器
                section_counts[section]["suitable"] += 1
                
                # 计算策略得分
                score = calculate_stock_strategy_score(stock_code, df)
                
                if score > 0:
                    # 更新板块计数器
                    section_counts[section]["scored"] += 1
                    return {
                        "code": stock_code,
                        "name": stock_name,
                        "score": score,
                        "df": df,
                        "section": section
                    }
            
            return None
        
        # ========== 关键修复 ==========
        # 调整并行参数，避免请求过于频繁
        # 原代码: with ThreadPoolExecutor(max_workers=20) as executor:
        # 修改为: 降低并发数量，增加任务延迟
        results = []
        # 分批处理股票，每批处理10只股票
        batch_size = 10
        for i in range(0, len(stock_list), batch_size):
            batch_indices = list(range(i, min(i + batch_size, len(stock_list))))
            with ThreadPoolExecutor(max_workers=5) as executor:  # 降低并发数量
                batch_results = list(executor.map(process_stock, batch_indices))
                results.extend(batch_results)
                # 每处理完一批，等待一段时间
                time.sleep(2.0)
        
        # 收集结果
        for result in results:
            if result is not None:
                section = result["section"]
                section_stocks[section].append(result)
        
        # 限制分析的股票数量
        for section in section_stocks:
            section_stocks[section] = section_stocks[section][:MAX_STOCKS_TO_ANALYZE]
        
        # 记录各板块筛选结果
        for section, counts in section_counts.items():
            if counts["total"] > 0:
                logger.info(f"【筛选统计】板块 {section}:")
                logger.info(f"  - 总股票数量: {counts['total']}")
                logger.info(f"  - 数据量足够: {counts['data_ok']} ({counts['data_ok']/counts['total']*100:.1f}%)")
                logger.info(f"  - 通过三重过滤: {counts['suitable']} ({counts['suitable']/counts['total']*100:.1f}%)")
                logger.info(f"  - 评分>0: {counts['scored']} ({counts['scored']/counts['total']*100:.1f}%)")
            else:
                logger.info(f"【筛选统计】板块 {section}: 无数据")
        
        # 3. 对每个板块的股票按得分排序，并取前N只
        top_stocks_by_section = {}
        for section, stocks in section_stocks.items():
            if stocks:
                stocks.sort(key=lambda x: x["score"], reverse=True)
                top_stocks = stocks[:MAX_STOCKS_PER_SECTION]
                top_stocks_by_section[section] = top_stocks
                logger.info(f"【最终结果】板块 {section} 筛选出 {len(top_stocks)} 只股票")
            else:
                logger.info(f"【最终结果】板块 {section} 无符合条件的股票")
        
        return top_stocks_by_section
    
    except Exception as e:
        logger.error(f"获取优质股票列表失败: {str(e)}", exc_info=True)
        return {}

def generate_section_report(section: str, stocks: List[Dict]):
    """
    生成单个板块的策略报告
    
    Args:
        section: 板块名称
        stocks: 该板块的股票列表
    """
    if not stocks:
        return
    
    logger.info(f"生成 {section} 板块策略报告")
    
    # 1. 生成板块筛选条件说明
    section_config = MARKET_SECTIONS[section]
    conditions = (
        f"【{section} 板块筛选条件】\n"
        f"• 市值 > {section_config['min_market_cap']}亿元\n"
        f"• 日均成交 > {section_config['min_daily_volume']/1000000:.0f}百万\n"
        f"• 年化波动率 < {section_config['max_volatility']*100:.0f}%\n"
        "──────────────────\n"
    )
    
    # 2. 生成每只股票的策略信号
    stock_reports = []
    for stock in stocks:
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
        logger.info(f"推送 {section} - {stock_name}({stock_code}) 策略信号")
        send_wechat_message(message)
        time.sleep(1)
    
    # 3. 生成板块总结消息
    summary_lines = [
        f"【{section} 板块策略总结】\n",
        conditions,
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
    summary_lines.append("3. 震荡市: 高抛低吸，控制总仓位\n")
    summary_lines.append(f"4. 单一个股仓位≤{int(MAX_STOCK_POSITION * 100)}%，分散投资\n")
    if section in ["科创板", "创业板"]:
        summary_lines.append("5. 科创板/创业板: 仓位和止损幅度适当放宽\n")
    summary_lines.append("──────────────────\n")
    summary_lines.append("📊 数据来源: fish-etf (https://github.com/karmyshunde-sudo/fish-etf      )\n")
    
    summary_message = "\n".join(summary_lines)
    
    # 4. 发送板块总结消息
    logger.info(f"推送 {section} 板块策略总结消息")
    send_wechat_message(summary_message)
    time.sleep(1)

def generate_overall_summary(top_stocks_by_section: Dict[str, List[Dict]]):
    """生成整体总结报告"""
    try:
        utc_now, beijing_now = get_current_times()
        
        summary_lines = [
            "【全市场个股趋势策略总结】\n",
            f"📅 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n",
            "📊 各板块筛选条件:\n"
        ]
        
        # 添加各板块筛选条件
        for section, config in MARKET_SECTIONS.items():
            summary_lines.append(
                f"  • {section}: 市值>{config['min_market_cap']}亿 | "
                f"日均成交>{config['min_daily_volume']/1000000:.0f}百万 | "
                f"波动率<{config['max_volatility']*100:.0f}%\n"
            )
        
        summary_lines.append("\n──────────────────\n")
        
        # 按板块统计
        total_stocks = 0
        for section, stocks in top_stocks_by_section.items():
            if stocks:
                yes_count = sum(1 for s in stocks if "YES" in s["message"])
                no_count = len(stocks) - yes_count
                summary_lines.append(f"📌 {section} ({len(stocks)}只):\n")
                summary_lines.append(f"  • 上涨趋势: {yes_count}只\n")
                summary_lines.append(f"  • 下跌趋势: {no_count}只\n\n")
                total_stocks += len(stocks)
        
        summary_lines.append(f"📊 总计: {total_stocks}只股票（每板块最多{MAX_STOCKS_PER_SECTION}只）\n")
        summary_lines.append("──────────────────\n")
        
        # 添加操作指南
        summary_lines.append("💡 操作指南:\n")
        summary_lines.append("1. YES信号: 可持仓或建仓，严格止损\n")
        summary_lines.append("2. NO信号: 减仓或观望，避免盲目抄底\n")
        summary_lines.append("3. 震荡市: 高抛低吸，控制总仓位≤40%\n")
        summary_lines.append("4. 单一个股仓位≤15%，分散投资5-8只\n")
        summary_lines.append("5. 科创板/创业板: 仓位和止损幅度适当放宽\n")
        summary_lines.append("──────────────────\n")
        summary_lines.append("📊 数据来源: fish-etf (https://github.com/karmyshunde-sudo/fish-etf      )\n")
        
        summary_message = "\n".join(summary_lines)
        
        # 发送整体总结消息
        logger.info("推送全市场策略总结消息")
        send_wechat_message(summary_message)
    
    except Exception as e:
        logger.error(f"生成整体总结失败: {str(e)}", exc_info=True)

def generate_report():
    """生成个股策略报告并推送微信"""
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始生成个股策略报告 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 1. 获取按板块分类的优质股票
        top_stocks_by_section = get_top_stocks_for_strategy()
        
        # 2. 生成每个板块的报告
        for section, stocks in top_stocks_by_section.items():
            if stocks:
                generate_section_report(section, stocks)
                time.sleep(2)
        
        # 3. 生成整体总结
        generate_overall_summary(top_stocks_by_section)
        
        logger.info(f"个股策略报告已成功发送至企业微信")
    
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
