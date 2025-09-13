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
from typing import Dict, List, Tuple, Optional, Any
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

# ========== 以下是关键修改 ==========
# 定义股票基础信息文件路径
BASIC_INFO_FILE = "data/all_stocks.csv"

# 市值过滤阈值（用于基础过滤）
MIN_MARKET_CAP_FOR_BASIC_FILTER = 50  # 亿元

# 数据更新间隔（天）
DATA_UPDATE_INTERVAL = 1
# ========== 以上是关键修改 ==========

# 股票板块配置
MARKET_SECTIONS = {
    "沪市主板": {
        "prefix": ["60"],
        "min_daily_volume": 5000 * 10000,  # 日均成交额阈值(元)
        "max_volatility": 0.40,  # 最大波动率
        "min_market_cap": 50,  # 最小市值(亿元)
        "max_market_cap": 2000  # 最大市值(亿元)
    },
    "深市主板": {
        "prefix": ["00"],
        "min_daily_volume": 5000 * 10000,
        "max_volatility": 0.40,
        "min_market_cap": 50,
        "min_market_cap": 50,
        "max_market_cap": 2000
    },
    "创业板": {
        "prefix": ["30"],
        "min_daily_volume": 5000 * 10000,
        "max_volatility": 0.40,
        "min_market_cap": 50,
        "max_market_cap": 2000
    },
    "科创板": {
        "prefix": ["688"],
        "min_daily_volume": 5000 * 10000,
        "max_volatility": 0.40,
        "min_market_cap": 50,
        "max_market_cap": 2000
    }
}

# 其他参数
MIN_DATA_DAYS = 30  # 最小数据天数（用于计算指标）
MAX_STOCKS_TO_ANALYZE = 300  # 减少每次分析的最大股票数量（避免请求过多）
MAX_STOCKS_PER_SECTION = 8  # 每个板块最多报告的股票数量
CRITICAL_VALUE_DAYS = 40  # 临界值计算天数

# ========== 以下是关键修改 ==========
def load_stock_basic_info() -> pd.DataFrame:
    """加载股票基础信息"""
    try:
        if os.path.exists(BASIC_INFO_FILE):
            df = pd.read_csv(BASIC_INFO_FILE)
            logger.info(f"成功加载股票基础信息，共 {len(df)} 条记录")
            return df
        else:
            logger.info("股票基础信息文件不存在，将创建新文件")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"加载股票基础信息失败: {str(e)}")
        return pd.DataFrame()

def save_stock_basic_info(df: pd.DataFrame) -> bool:
    """保存股票基础信息"""
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(BASIC_INFO_FILE), exist_ok=True)
        
        # 保存文件
        df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"股票基础信息已保存，共 {len(df)} 条记录")
        return True
    except Exception as e:
        logger.error(f"保存股票基础信息失败: {str(e)}")
        return False

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
        except:
            return None
    return None

def should_update_stock(df: pd.DataFrame, stock_code: str) -> bool:
    """判断是否需要更新股票数据"""
    last_update = get_last_update_time(df, stock_code)
    if last_update is None:
        return True
    
    # 如果最后更新时间超过DATA_UPDATE_INTERVAL天，则需要更新
    return (datetime.now() - last_update).days >= DATA_UPDATE_INTERVAL

def update_stock_basic_info(basic_info_df: pd.DataFrame, stock_code: str, stock_name: str, 
                           market_cap: float, section: str) -> pd.DataFrame:
    """更新股票基础信息"""
    # 准备新记录
    new_record = {
        "code": stock_code,
        "name": stock_name,
        "market_cap": market_cap,
        "section": section,
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # 检查是否已存在
    if not basic_info_df.empty:
        existing = basic_info_df[basic_info_df["code"] == stock_code]
        if not existing.empty:
            # 更新现有记录
            idx = basic_info_df[basic_info_df["code"] == stock_code].index[0]
            for key, value in new_record.items():
                basic_info_df.at[idx, key] = value
            return basic_info_df
    
    # 添加新记录
    new_df = pd.DataFrame([new_record])
    if basic_info_df.empty:
        return new_df
    else:
        return pd.concat([basic_info_df, new_df], ignore_index=True)
# ========== 以上是关键修改 ==========

def get_stock_section(stock_code: str) -> str:
    """根据股票代码判断所属板块
    
    Args:
        stock_code: 股票代码（6位数字）
    
    Returns:
        str: 板块名称
    """
    for section, config in MARKET_SECTIONS.items():
        for prefix in config["prefix"]:
            if stock_code.startswith(prefix):
                return section
    return "其他板块"

def fetch_stock_list() -> pd.DataFrame:
    """从仓库加载股票基础信息，必要时更新"""
    try:
        logger.info("正在加载股票基础信息...")
        
        # 1. 尝试加载现有基础信息
        if os.path.exists(BASIC_INFO_FILE):
            basic_info_df = pd.read_csv(BASIC_INFO_FILE)
            logger.info(f"成功加载现有股票基础信息，共 {len(basic_info_df)} 条记录")
            
            # 检查是否需要更新（基于最后更新时间）
            if "last_update" in basic_info_df.columns and not basic_info_df.empty:
                last_update_str = basic_info_df["last_update"].max()
                try:
                    last_update = datetime.strptime(last_update_str, "%Y-%m-%d %H:%M:%S")
                    if (datetime.now() - last_update).days < 1:
                        logger.info(f"股票基础信息未过期（最后更新: {last_update_str}），使用现有数据")
                        # 修复：移除重复记录，确保唯一性
                        basic_info_df = basic_info_df.drop_duplicates(subset=['code'], keep='last')
                        logger.info(f"去重后股票基础信息数量: {len(basic_info_df)} 条记录")
                        return basic_info_df
                except Exception as e:
                    logger.warning(f"解析最后更新时间失败: {str(e)}，将重新获取数据")
        else:
            logger.info("股票基础信息文件不存在，将创建新文件")
        
        # 2. 获取A股股票列表
        logger.info("正在从AkShare获取股票列表...")
        stock_list = ak.stock_info_a_code_name()
        if stock_list.empty:
            logger.error("获取股票列表失败：返回为空")
            # 如果无法获取新数据，尝试返回空DataFrame
            return pd.DataFrame(columns=["code", "name", "section", "market_cap", "last_update"])
        
        # 记录初始股票数量
        initial_count = len(stock_list)
        logger.info(f"成功获取股票列表，共 {initial_count} 只股票（初始数量）")
        
        # 前置筛选条件：过滤ST股票和非主板/科创板/创业板股票
        stock_list = stock_list[~stock_list["name"].str.contains("ST")]
        stock_list = stock_list[stock_list["code"].str.startswith(("60", "00", "30", "688"))]
        
        # 记录前置筛选后的股票数量
        filtered_count = len(stock_list)
        logger.info(f"【前置筛选】过滤ST股票和非主板/科创板/创业板股票后，剩余 {filtered_count} 只（过滤了 {initial_count - filtered_count} 只）")
        
        # 3. 创建基础信息DataFrame
        basic_info_data = []
        for _, row in stock_list.iterrows():
            stock_code = row["code"]
            stock_name = row["name"]
            section = get_stock_section(stock_code)
            
            # 检查是否已有记录
            existing_market_cap = 0
            existing_score = 0
            if os.path.exists(BASIC_INFO_FILE) and "code" in basic_info_df.columns:
                existing = basic_info_df[basic_info_df["code"] == stock_code]
                if not existing.empty:
                    # 保留现有市值和评分
                    existing_market_cap = existing["market_cap"].values[0]
                    if "score" in existing.columns:
                        existing_score = existing["score"].values[0]
            
            # 基础信息只包含必要字段
            basic_info_data.append({
                "code": stock_code,
                "name": stock_name,
                "section": section,
                "market_cap": existing_market_cap,
                "score": existing_score,
                "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        
        basic_info_df = pd.DataFrame(basic_info_data)
        
        # 4. 修复：确保股票代码唯一，移除重复记录
        if "code" in basic_info_df.columns:
            basic_info_df = basic_info_df.drop_duplicates(subset=['code'], keep='last')
            logger.info(f"去重后股票基础信息数量: {len(basic_info_df)} 条记录")
        
        # 5. 保存基础信息
        os.makedirs(os.path.dirname(BASIC_INFO_FILE), exist_ok=True)
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"股票基础信息已保存至 {BASIC_INFO_FILE}，共 {len(basic_info_df)} 条记录")
        
        return basic_info_df
    
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}", exc_info=True)
        # 如果失败，尝试返回现有数据
        if os.path.exists(BASIC_INFO_FILE):
            try:
                basic_info_df = pd.read_csv(BASIC_INFO_FILE)
                # 确保唯一性
                if "code" in basic_info_df.columns:
                    basic_info_df = basic_info_df.drop_duplicates(subset=['code'], keep='last')
                    logger.warning(f"使用现有数据并去重，共 {len(basic_info_df)} 条记录")
                return basic_info_df
            except:
                pass
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
        
        # 计算日期范围
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        
        # 尝试多种可能的股票代码格式
        possible_codes = [
            f"{market_prefix}{stock_code}",  # "sh000001"
            f"{stock_code}.{market_prefix.upper()}",  # "000001.SH"
            stock_code,  # "000001"
            f"{stock_code}.{'SZ' if market_prefix == 'sz' else 'SH'}",  # "000001.SZ"
            f"{market_prefix}{stock_code}.XSHG" if market_prefix == "sh" else f"{market_prefix}{stock_code}.XSHE",  # 交易所格式
        ]
        
        logger.debug(f"尝试获取股票 {stock_code} 数据，可能的代码格式: {possible_codes}")
        logger.debug(f"时间范围: {start_date} 至 {end_date}")
        
        # 尝试使用多种接口和代码格式获取数据
        df = None
        successful_code = None
        successful_interface = None
        
        # 先建议切换为stock_zh_a_hist 接口使用(该接口数据质量较好) [[2]]
        # 先尝试使用stock_zh_a_hist接口
        for code in possible_codes:
            for attempt in range(5):  # 增加重试次数
                try:
                    logger.debug(f"尝试{attempt+1}/5: 使用stock_zh_a_hist接口获取股票 {code}")
                    df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                           start_date=start_date, end_date=end_date, 
                                           adjust="qfq")
                    if not df.empty:
                        successful_code = code
                        successful_interface = "stock_zh_a_hist"
                        logger.debug(f"成功通过stock_zh_a_hist接口获取股票 {code} 数据")
                        break
                except Exception as e:
                    logger.debug(f"使用stock_zh_a_hist接口获取股票 {code} 失败: {str(e)}")
                
                # 指数退避等待，避免高并发获取数据导致IP被拉黑 [[5]]
                time.sleep(0.5 * (2 ** attempt))
            
            if df is not None and not df.empty:
                break
        
        # 如果stock_zh_a_hist接口失败，尝试stock_zh_a_daily接口
        if df is None or df.empty:
            for code in possible_codes:
                for attempt in range(3):
                    try:
                        logger.debug(f"尝试{attempt+1}/3: 使用stock_zh_a_daily接口获取股票 {code}")
                        df = ak.stock_zh_a_daily(symbol=code, 
                                               start_date=start_date, 
                                               end_date=end_date, 
                                               adjust="qfq")
                        if not df.empty:
                            successful_code = code
                            successful_interface = "stock_zh_a_daily"
                            logger.debug(f"成功通过stock_zh_a_daily接口获取股票 {code} 数据")
                            break
                    except Exception as e:
                        logger.debug(f"使用stock_zh_a_daily接口获取股票 {code} 失败: {str(e)}")
                    
                    time.sleep(1.0 * (2 ** attempt))
                
                if df is not None and not df.empty:
                    break
        
        # 如果还是失败，返回空DataFrame
        if df is None or df.empty:
            logger.warning(f"获取股票 {stock_code} 数据失败，所有接口和代码格式均无效")
            return pd.DataFrame()
        
        logger.info(f"✅ 成功通过 {successful_interface} 接口获取股票 {successful_code} 数据，共 {len(df)} 天")
        
        # 处理可能的列名差异
        if 'date' in df.columns:
            # 英文列名映射到标准列名
            column_mapping = {
                'date': '日期',
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量',
                'amount': '成交额',
                'amplitude': '振幅',
                'percent': '涨跌幅',
                'change': '涨跌额',
                'turnover': '换手率'
            }
        else:
            # 中文列名映射到标准列名
            column_mapping = {
                '日期': '日期',
                '开盘': '开盘',
                '最高': '最高',
                '最低': '最低',
                '收盘': '收盘',
                '成交量': '成交量',
                '成交额': '成交额',
                '振幅': '振幅',
                '涨跌幅': '涨跌幅',
                '涨跌额': '涨跌额',
                '换手率': '换手率'
            }
        
        # 重命名列
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        # 确保日期列存在
        if '日期' not in df.columns and 'date' in df.columns:
            df = df.rename(columns={'date': '日期'})
        
        # 检查是否有必要的列
        required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"股票 {stock_code} 数据缺少必要列: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # 确保日期列是字符串类型
        if "日期" in df.columns:
            df["日期"] = df["日期"].astype(str)
            # 确保日期格式为YYYY-MM-DD
            df["日期"] = df["日期"].str.replace(r'(\d{4})/(\d{1,2})/(\d{1,2})', 
                                              lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", 
                                              regex=True)
            df = df.sort_values("日期", ascending=True)
        
        # 检查数据量
        if len(df) < 10:
            logger.warning(f"股票 {stock_code} 数据量不足({len(df)}天)，可能影响分析结果")
        
        logger.debug(f"成功获取股票 {stock_code} 数据，共 {len(df)} 条记录")
        return df
    
    except Exception as e:
        logger.debug(f"获取股票 {stock_code} 数据失败: {str(e)}")
        return pd.DataFrame()

def calculate_annual_volatility(df: pd.DataFrame) -> float:
    """计算年化波动率"""
    if len(df) < 20:
        logger.warning(f"数据不足20天，无法准确计算波动率")
        return 0.2  # 默认波动率
    
    # 计算日收益率
    # ========== 以下是关键修改 ==========
    # 原始代码: daily_returns = df["收盘"].pct_change().dropna()
    # 修改为: 使用标准列名 '收盘'
    daily_returns = df["收盘"].pct_change().dropna()
    # ========== 以上是关键修改 ==========
    
    # 计算年化波动率
    if len(daily_returns) >= 20:
        volatility = daily_returns.std() * np.sqrt(252)
    else:
        volatility = 0.2  # 默认波动率
    
    # 限制波动率在合理范围内
    volatility = max(0.05, min(1.0, volatility))
    
    return volatility

def calculate_market_cap(df: pd.DataFrame, stock_code: str) -> float:
    """直接从接口获取市值（无需计算）"""
    try:
        # 1. 直接获取实时行情数据
        stock_info = ak.stock_zh_a_spot_em()
        if stock_info.empty:
            logger.warning(f"无法获取股票 {stock_code} 的实时行情数据")
            return 0.0
        
        # 2. 标准化股票代码匹配（纯6位数字）
        stock_code_std = stock_code.zfill(6)
        
        # 3. 尝试匹配股票
        matched = stock_info[stock_info["代码"] == stock_code_std]
        
        # 4. 如果匹配成功，直接获取流通市值
        if not matched.empty:
            if "流通市值" in matched.columns:
                market_cap = float(matched["流通市值"].iloc[0])
                if market_cap > 0:
                    # 正确转换：元 → 亿元（除以100,000,000）
                    market_cap_in_billion = market_cap / 100000000
                    logger.debug(f"✅ 使用实时数据获取流通市值: {market_cap_in_billion:.2f}亿元")
                    return market_cap_in_billion
        
        # 5. 如果流通市值不可用，尝试总市值
        if not matched.empty and "总市值" in matched.columns:
            market_cap = float(matched["总市值"].iloc[0])
            if market_cap > 0:
                market_cap_in_billion = market_cap / 100000000
                logger.debug(f"✅ 使用实时数据获取总市值: {market_cap_in_billion:.2f}亿元")
                return market_cap_in_billion
        
        logger.warning(f"❌ 无法获取股票 {stock_code} 的有效市值数据")
        return 0.0
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 市值失败: {str(e)}", exc_info=True)
        return 0.0

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
        if '成交量' in df.columns and '收盘' in df.columns and len(df) >= 20:
            daily_volume = df["成交量"].iloc[-20:].mean() * 100 * df["收盘"].iloc[-20:].mean()
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
        
        # 1. 趋势评分 (40%)
        trend_score = 0.0
        if len(df) >= 40:
            # 计算移动平均线
            df["ma5"] = df["收盘"].rolling(window=5).mean()
            df["ma10"] = df["收盘"].rolling(window=10).mean()
            df["ma20"] = df["收盘"].rolling(window=20).mean()
            df["ma40"] = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS).mean()
            
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
                price_change_20 = (current - df["收盘"].iloc[-20]) / df["收盘"].iloc[-20] * 100
                if not pd.isna(price_change_20) and price_change_20 > 5:
                    trend_score += 10  # 20日涨幅大于5%，加10分
        
        # 2. 动量评分 (20%)
        momentum_score = 0.0
        # 计算MACD
        if "收盘" in df.columns:
            df["ema12"] = df["收盘"].ewm(span=12, adjust=False).mean()
            df["ema26"] = df["收盘"].ewm(span=26, adjust=False).mean()
            df["macd"] = df["ema12"] - df["ema26"]
            df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
            df["hist"] = df["macd"] - df["signal"]
        
        if "hist" in df.columns and len(df) >= 2:
            macd_hist = df["hist"].iloc[-1]
            macd_hist_prev = df["hist"].iloc[-2]
            
            # MACD柱状体增加
            if not pd.isna(macd_hist) and not pd.isna(macd_hist_prev) and macd_hist > macd_hist_prev and macd_hist > 0:
                momentum_score += 10  # MACD柱状体增加且为正，加10分
            
            # RSI指标
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
                    if 50 < rsi < 70:
                        momentum_score += 10  # RSI在50-70之间，加10分
                    elif rsi >= 70:
                        momentum_score += 5  # RSI大于70，加5分
        
        # 3. 量能评分 (20%)
        volume_score = 0.0
        if "成交量" in df.columns:
            df["volume_ma5"] = df["成交量"].rolling(window=5).mean()
        
        volume_ma5 = df["volume_ma5"].iloc[-1] if "volume_ma5" in df.columns and len(df) >= 1 else 0
        if volume_ma5 > 0 and volume > 0:
            volume_ratio = volume / volume_ma5
            
            # 量能放大
            if volume_ratio > 1.5:
                volume_score += 10  # 量能放大50%以上，加10分
            elif volume_ratio > 1.2:
                volume_score += 5  # 量能放大20%以上，加5分
            
            # 量价配合
            if len(df) >= 2:
                price_change = (current - df["收盘"].iloc[-2]) / df["收盘"].iloc[-2] * 100
                if price_change > 0 and volume_ratio > 1.0:
                    volume_score += 10  # 价格上涨且量能放大，加10分
        
        # 4. 波动率评分 (20%)
        volatility_score = 0.0
        # 计算波动率（20日年化波动率）
        if "收盘" in df.columns:
            df["pct_change"] = df["收盘"].pct_change() * 100
        
        if "pct_change" in df.columns:
            df["volatility"] = df["pct_change"].rolling(window=20).std() * np.sqrt(252)
        
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

def get_top_stocks_for_strategy() -> Dict[str, List[Dict]]:
    """按板块获取适合策略的股票"""
    try:
        # 1. 获取股票基础信息
        basic_info_df = fetch_stock_list()
        if basic_info_df.empty:
            logger.error("获取股票基础信息失败，无法继续")
            return {}
        
        logger.info(f"已加载股票基础信息，共 {len(basic_info_df)} 条记录")
        
        # 2. 按板块分组处理
        section_stocks = {section: [] for section in MARKET_SECTIONS.keys()}
        
        # 3. 初始化各板块计数器
        section_counts = {section: {"total": 0, "data_ok": 0, "suitable": 0, "scored": 0}
                         for section in MARKET_SECTIONS.keys()}
        
        # 4. 处理每只股票
        stock_list = basic_info_df.to_dict('records')
        
        def process_stock(stock):
            stock_code = stock["code"]
            stock_name = stock["name"]
            section = stock["section"]
            
            # 检查板块是否有效
            if section not in MARKET_SECTIONS:
                return None
            
            # 更新板块计数器
            section_counts[section]["total"] += 1
            
            # 获取日线数据
            df = fetch_stock_data(stock_code)
            
            # 检查数据量
            if df is None or df.empty or len(df) < MIN_DATA_DAYS:
                return None
            
            # 更新板块计数器
            section_counts[section]["data_ok"] += 1
            
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
        
        # 5. 并行处理股票（限制并发数量，避免被AkShare限制）
        results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 分批处理，避免请求过于频繁
            for i in range(0, len(stock_list), 10):
                batch = stock_list[i:i+10]
                batch_results = list(executor.map(process_stock, batch))
                results.extend(batch_results)
                time.sleep(1.0)  # 批次间等待
        
        # 6. 收集结果
        for result in results:
            if result is not None:
                section = result["section"]
                section_stocks[section].append(result)
        
        # 7. 记录各板块筛选结果
        for section, counts in section_counts.items():
            if counts["total"] > 0:
                logger.info(f"【筛选统计】板块 {section}:")
                logger.info(f"  - 总股票数量: {counts['total']}")
                logger.info(f"  - 数据量足够: {counts['data_ok']} ({counts['data_ok']/counts['total']*100:.1f}%)")
                logger.info(f"  - 通过三重过滤: {counts['suitable']} ({counts['suitable']/counts['total']*100:.1f}%)")
                logger.info(f"  - 评分>0: {counts['scored']} ({counts['scored']/counts['total']*100:.1f}%)")
        
        # 8. 对每个板块的股票按得分排序，并取前N只
        top_stocks_by_section = {}
        for section, stocks in section_stocks.items():
            if stocks:
                stocks.sort(key=lambda x: x["score"], reverse=True)
                top_stocks = stocks[:MAX_STOCKS_PER_SECTION]
                top_stocks_by_section[section] = top_stocks
                logger.info(f"【最终结果】板块 {section} 筛选出 {len(top_stocks)} 只股票")
        
        # 9. 更新基础信息中的市值和评分
        updated_records = []
        for section, stocks in top_stocks_by_section.items():
            for stock in stocks:
                stock_code = stock["code"]
                stock_name = stock["name"]
                section = stock["section"]
                market_cap = calculate_market_cap(stock["df"], stock_code)
                score = stock["score"]
                
                # 更新记录
                updated_records.append({
                    "code": stock_code,
                    "name": stock_name,
                    "section": section,
                    "market_cap": market_cap,
                    "score": score,
                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
        
        # 10. 保存更新后的基础信息
        if updated_records:
            updated_df = pd.DataFrame(updated_records)
            # 合并到基础信息
            for _, record in updated_df.iterrows():
                mask = basic_info_df["code"] == record["code"]
                if mask.any():
                    # 更新现有记录
                    basic_info_df.loc[mask, "market_cap"] = record["market_cap"]
                    basic_info_df.loc[mask, "score"] = record["score"]
                    basic_info_df.loc[mask, "last_update"] = record["last_update"]
                else:
                    # 添加新记录
                    basic_info_df = pd.concat([basic_info_df, pd.DataFrame([record])], ignore_index=True)
            
            # 保存更新
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
            logger.info(f"股票基础信息已更新，共 {len(basic_info_df)} 条记录")
        
        return top_stocks_by_section
    
    except Exception as e:
        logger.error(f"获取优质股票列表失败: {str(e)}", exc_info=True)
        return {}

def generate_stock_signal_message(stock: Dict, df: pd.DataFrame, 
                                close_price: float, critical_value: float, 
                                deviation: float) -> str:
    """生成股票信号详细消息
    
    Args:
        stock: 股票信息
        df: 股票数据
        close_price: 当前收盘价
        critical_value: 临界值
        deviation: 偏离度
    
    Returns:
        str: 信号详细消息
    """
    stock_code = stock["code"]
    stock_name = stock["name"]
    
    # 获取最新数据
    latest_data = df.iloc[-1]
    
    # 检查是否包含必要列
    required_columns = ['开盘', '最高', '最低', '收盘', '成交量']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return f"{stock_name}({stock_code}) 数据不完整，无法生成信号"
    
    # 计算指标
    ma5 = latest_data["收盘"] if len(df) < 5 else df["收盘"].rolling(5).mean().iloc[-1]
    ma10 = latest_data["收盘"] if len(df) < 10 else df["收盘"].rolling(10).mean().iloc[-1]
    ma20 = latest_data["收盘"] if len(df) < 20 else df["收盘"].rolling(20).mean().iloc[-1]
    volume = latest_data["成交量"]
    volume_ma5 = volume if len(df) < 5 else df["成交量"].rolling(5).mean().iloc[-1]
    
    # 量能分析
    volume_ratio = volume / volume_ma5 if volume_ma5 > 0 else 1.0
    volume_analysis = "量能放大" if volume_ratio > 1.2 else "量能平稳" if volume_ratio > 0.8 else "量能萎缩"
    
    # 趋势分析
    trend_analysis = "多头排列" if ma5 > ma10 > ma20 else "空头排列" if ma5 < ma10 < ma20 else "震荡走势"
    
    # 生成消息
    message = []
    message.append(f"{stock_name}({stock_code})")
    message.append(f"📊 价格: {close_price:.4f} | 临界值: {critical_value:.4f} | 偏离度: {deviation:.2%}")
    message.append(f"📈 趋势: {trend_analysis} | {volume_analysis}")
    message.append(f"⏰ 量能: {volume:,.0f}手 | 5日均量: {volume_ma5:,.0f}手 | 比例: {volume_ratio:.2f}")
    
    return "\n".join(message)

def generate_strategy_summary(top_stocks_by_section: Dict[str, List[Dict]]) -> str:
    """生成策略总结消息
    
    Args:
        top_stocks_by_section: 按板块组织的股票信息
    
    Returns:
        str: 策略总结消息
    """
    summary_lines = []
    
    # 添加标题
    beijing_time = get_beijing_time()
    summary_lines.append(f"📊 个股趋势策略报告 ({beijing_time.strftime('%Y-%m-%d %H:%M')})")
    summary_lines.append("──────────────────")
    
    # 添加各板块结果
    total_stocks = 0
    for section, stocks in top_stocks_by_section.items():
        if stocks:
            summary_lines.append(f"📌 {section}板块 ({len(stocks)}只):")
            for stock in stocks:
                stock_code = stock["code"]
                stock_name = stock["name"]
                score = stock["score"]
                summary_lines.append(f"   • {stock_name}({stock_code}) {score:.1f}分")
            total_stocks += len(stocks)
    
    summary_lines.append(f"📊 总计: {total_stocks}只股票（每板块最多{MAX_STOCKS_PER_SECTION}只）")
    summary_lines.append("──────────────────")
    
    # 添加操作指南
    summary_lines.append("💡 操作指南:")
    summary_lines.append("1. YES信号: 可持仓或建仓，严格止损")
    summary_lines.append("2. NO信号: 减仓或观望，避免盲目抄底")
    summary_lines.append("3. 震荡市: 高抛低吸，控制总仓位≤40%")
    summary_lines.append("4. 单一个股仓位≤15%，分散投资5-8只")
    summary_lines.append("5. 科创板/创业板: 仓位和止损幅度适当放宽")
    summary_lines.append("──────────────────")
    summary_lines.append("📊 数据来源: fish-etf (https://github.com/karmyshunde-sudo/fish-etf )")
    
    summary_message = "\n".join(summary_lines)
    return summary_message

def main():
    """主函数"""
    try:
        logger.info("===== 开始执行个股趋势策略(TickTen) =====")
        
        # 1. 获取适合策略的股票
        top_stocks_by_section = get_top_stocks_for_strategy()
        
        # 2. 生成策略总结消息
        summary_message = generate_strategy_summary(top_stocks_by_section)
        
        # 3. 推送全市场策略总结消息
        logger.info("推送全市场策略总结消息")
        send_wechat_message(summary_message, message_type="stock_tickten")
        
        logger.info("个股策略报告已成功发送至企业微信")
        logger.info("===== 个股趋势策略(TickTen)执行完成 =====")
    
    except Exception as e:
        error_msg = f"个股趋势策略执行失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(error_msg, message_type="error")

if __name__ == "__main__":
    main()
