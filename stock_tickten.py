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

from utils.git_utils import commit_and_push_file
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
    df["日期"] = pd.to_datetime(df["日期"])
    df = df.sort_values("日期")
    
    # 检查日期间隔
    df["日期_diff"] = df["日期"].diff().dt.days
    gaps = df[df["日期_diff"] > 1]
    
    # 计算缺失率
    expected_days = (df["日期"].iloc[-1] - df["日期"].iloc[0]).days + 1
    missing_rate = 1 - (data_days / expected_days) if expected_days > 0 else 1
    
    # 数据完整性分级
    if data_days < 30:
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
                        
                        # 关键修复：检查市值数据是否有效
                        valid_market_cap_count = len(basic_info_df[basic_info_df["market_cap"] > 10])
                        if valid_market_cap_count < len(basic_info_df) * 0.2:
                            logger.warning(f"市值数据有效性不足 ({valid_market_cap_count}/{len(basic_info_df)}), 将重新获取市值数据")
                            basic_info_df = basic_info_df.drop(columns=["market_cap"], errors='ignore')
                        else:
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
        
        # ========== 关键修复 ==========
        # 一次性获取所有股票的实时行情数据（包含市值信息）
        logger.info("正在获取所有股票的实时行情数据（包含市值信息）...")
        stock_info = None
        for attempt in range(3):
            try:
                stock_info = ak.stock_zh_a_spot_em()
                if not stock_info.empty:
                    logger.info(f"成功获取所有股票的实时行情数据，共 {len(stock_info)} 条记录")
                    break
            except Exception as e:
                logger.warning(f"尝试{attempt+1}/3: 获取实时行情数据失败: {str(e)}")
                time.sleep(1.5 * (2 ** attempt))  # 指数退避
        
        # 3. 创建基础信息DataFrame
        basic_info_data = []
        for _, row in stock_list.iterrows():
            # 确保股票代码是字符串，并且是6位（前面补零）
            stock_code = str(row["code"]).zfill(6)
            stock_name = row["name"]
            section = get_stock_section(stock_code)
            
            # 检查是否已有记录
            existing_market_cap = 0
            existing_score = 0
            if os.path.exists(BASIC_INFO_FILE) and "code" in basic_info_df.columns:
                existing = basic_info_df[basic_info_df["code"] == stock_code]
                if not existing.empty:
                    # 保留现有评分
                    if "score" in existing.columns:
                        existing_score = existing["score"].values[0]
            
            # 从实时行情数据获取市值
            market_cap = 0
            if stock_info is not None:
                # 标准化股票代码匹配
                stock_code_std = stock_code.zfill(6)
                matched = stock_info[stock_info["代码"] == stock_code_std]
                
                if not matched.empty:
                    # 尝试获取流通市值
                    if "流通市值" in matched.columns:
                        market_cap = float(matched["流通市值"].values[0]) / 100000000  # 元 → 亿元
                        if market_cap > 0:
                            logger.debug(f"获取股票 {stock_code} {stock_name} 的流通市值: {market_cap:.2f}亿元")
                    
                    # 尝试获取总市值
                    if market_cap == 0 and "总市值" in matched.columns:
                        market_cap = float(matched["总市值"].values[0]) / 100000000  # 元 → 亿元
                        if market_cap > 0:
                            logger.debug(f"获取股票 {stock_code} {stock_name} 的总市值: {market_cap:.2f}亿元")
            
            # 基础信息只包含必要字段
            basic_info_data.append({
                "code": stock_code,
                "name": stock_name,
                "section": section,
                "market_cap": market_cap,
                "score": existing_score,
                "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        # ========== 关键修复 ==========
        
        basic_info_df = pd.DataFrame(basic_info_data)
        
        # 4. 确保股票代码唯一
        if "code" in basic_info_df.columns:
            basic_info_df = basic_info_df.drop_duplicates(subset=['code'], keep='last')
            logger.info(f"去重后股票基础信息数量: {len(basic_info_df)} 条记录")
        
        # 5. 保存基础信息
        os.makedirs(os.path.dirname(BASIC_INFO_FILE), exist_ok=True)
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"股票基础信息已保存至 {BASIC_INFO_FILE}，共 {len(basic_info_df)} 条记录")
        
        # ========== 正确修复 ==========
        # 使用 git_utils.py 中已有的工具函数
        try:
            logger.info("正在提交股票基础信息到GitHub仓库...")
            commit_message = "自动更新股票基础信息 [初始化]"
            if commit_and_push_file(BASIC_INFO_FILE, commit_message):
                logger.info("股票基础信息已成功提交并推送到GitHub仓库")
            else:
                logger.warning("提交股票基础信息到GitHub仓库失败，但继续执行策略")
        except Exception as e:
            logger.warning(f"提交股票基础信息到GitHub仓库失败: {str(e)}")
        # ========== 正确修复 ==========
        
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

def fetch_stock_data(stock_code: str, days: int = None) -> pd.DataFrame:
    """从AkShare获取个股历史数据（真正的增量爬取）
    
    Args:
        stock_code: 股票代码（不带市场前缀）
        days: 获取最近多少天的数据，None表示增量爬取
    
    Returns:
        pd.DataFrame: 个股日线数据
    """
    try:
        # 确保股票代码是字符串，并且是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        
        # 确定市场前缀
        section = get_stock_section(stock_code)
        if section == "沪市主板" or section == "科创板":
            market_prefix = "sh"
        else:  # 深市主板、创业板
            market_prefix = "sz"
        
        # 检查本地是否已有数据
        file_path = os.path.join(Config.DATA_DIR, "daily", f"{stock_code}.csv")
        start_date = None
        
        if os.path.exists(file_path) and days is None:
            # 读取现有数据文件
            try:
                existing_df = pd.read_csv(file_path)
                if not existing_df.empty and "日期" in existing_df.columns:
                    # 确保日期列是datetime类型
                    existing_df["日期"] = pd.to_datetime(existing_df["日期"])
                    # 获取现有数据的最新日期
                    latest_date = existing_df["日期"].max().strftime("%Y%m%d")
                    # 从最新日期的下一天开始获取
                    start_date = (datetime.strptime(latest_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
                    logger.info(f"股票 {stock_code} 检测到现有数据，最新日期: {latest_date}, 将从 {start_date} 开始增量获取")
            except Exception as e:
                logger.warning(f"股票 {stock_code} 读取现有数据失败，将重新获取1年数据: {str(e)}")
        
        # 计算日期范围
        end_date = datetime.now().strftime("%Y%m%d")
        if start_date is None:
            if days is not None:
                # 如果指定了天数，获取指定天数的数据
                start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
            else:
                # 默认获取1年数据（仅当没有现有数据时）
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        
        # 如果开始日期晚于结束日期，无需获取
        if start_date > end_date:
            logger.info(f"股票 {stock_code} 无需更新数据")
            return pd.DataFrame()
        
        # 尝试多种可能的股票代码格式
        possible_codes = [
            f"{market_prefix}{stock_code}",  # "sh000001"
            f"{stock_code}.{market_prefix.upper()}",  # "000001.SH"
            stock_code,  # "000001"
            f"{stock_code}.{'SZ' if market_prefix == 'sz' else 'SH'}",  # "000001.SZ"
            f"{market_prefix}{stock_code}.XSHG" if market_prefix == "sh" else f"{market_prefix}{stock_code}.XSHE",  # 交易所格式
        ]
        
        logger.debug(f"股票 {stock_code} 尝试获取数据，可能的代码格式: {possible_codes}")
        logger.debug(f"股票 {stock_code} 时间范围: {start_date} 至 {end_date}")
        
        # 尝试使用多种接口和代码格式获取数据
        df = None
        successful_code = None
        successful_interface = None
        
        # 先尝试使用stock_zh_a_hist接口
        for code in possible_codes:
            for attempt in range(3):  # 减少重试次数
                try:
                    logger.debug(f"股票 {stock_code} 尝试{attempt+1}/3: 使用stock_zh_a_hist接口获取股票 {code}")
                    df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                           start_date=start_date, end_date=end_date, 
                                           adjust="qfq")
                    if not df.empty:
                        successful_code = code
                        successful_interface = "stock_zh_a_hist"
                        logger.debug(f"股票 {stock_code} 成功通过stock_zh_a_hist接口获取股票 {code} 数据")
                        break
                except Exception as e:
                    logger.debug(f"股票 {stock_code} 使用stock_zh_a_hist接口获取股票 {code} 失败: {str(e)}")
                
                # 优化等待时间，避免被限流
                if attempt == 0:
                    time.sleep(0.3)  # 第一次失败等待0.3秒
                elif attempt == 1:
                    time.sleep(0.6)  # 第二次失败等待0.6秒
                else:
                    time.sleep(1.0)  # 第三次失败等待1.0秒
            
            if df is not None and not df.empty:
                break
        
        # 如果stock_zh_a_hist接口失败，尝试其他接口（仅当增量获取失败时）
        if df is None or df.empty and (days is None or days > 30):
            for code in possible_codes:
                for attempt in range(2):  # 更少的重试次数
                    try:
                        logger.debug(f"股票 {stock_code} 尝试{attempt+1}/2: 使用stock_zh_a_daily接口获取股票 {code}")
                        df = ak.stock_zh_a_daily(symbol=code, 
                                               start_date=start_date, 
                                               end_date=end_date, 
                                               adjust="qfq")
                        if not df.empty:
                            successful_code = code
                            successful_interface = "stock_zh_a_daily"
                            logger.debug(f"股票 {stock_code} 成功通过stock_zh_a_daily接口获取股票 {code} 数据")
                            break
                    except Exception as e:
                        logger.debug(f"股票 {stock_code} 使用stock_zh_a_daily接口获取股票 {code} 失败: {str(e)}")
                    
                    time.sleep(0.5 * (2 ** attempt))
                
                if df is not None and not df.empty:
                    break
        
        # 如果还是失败，返回空DataFrame
        if df is None or df.empty:
            logger.debug(f"股票 {stock_code} 获取数据失败，所有接口和代码格式均无效")
            return pd.DataFrame()
        
        # 确保日期列存在
        if "日期" not in df.columns:
            logger.debug(f"股票 {stock_code} 数据缺少'日期'列")
            return pd.DataFrame()
        
        # 检查是否有必要的列
        required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.debug(f"股票 {stock_code} 数据缺少必要列: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # 确保日期列是字符串类型
        if "日期" in df.columns:
            df["日期"] = df["日期"].astype(str)
            # 确保日期格式为YYYY-MM-DD
            df["日期"] = df["日期"].str.replace(r'(\d{4})/(\d{1,2})/(\d{1,2})', 
                                              lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", 
                                              regex=True)
            # 移除可能存在的空格
            df["日期"] = df["日期"].str.strip()
            df = df.sort_values("日期", ascending=True)
        
        # 检查数据量
        if len(df) < 5:
            logger.debug(f"股票 {stock_code} 数据量不足({len(df)}天)，可能影响分析结果")
        
        # 记录实际获取的数据量
        logger.info(f"股票 {stock_code} ✅ 成功通过 {successful_interface} 接口获取数据，共 {len(df)} 天（{start_date} 至 {end_date}）")
        
        return df
    
    except Exception as e:
        # 关键修复：在日志中包含股票代码
        logger.error(f"股票 {stock_code} 获取数据失败: {str(e)}", exc_info=True)
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

def calculate_market_cap(df: pd.DataFrame, stock_code: str) -> Optional[float]:
    """计算股票市值（优先使用缓存数据）
    
    Returns:
        Optional[float]: 市值(亿元)，None表示市值数据不可靠
    """
    try:
        # 尝试从本地数据获取市值（如果已经计算过）
        if df is not None and not df.empty and "market_cap" in df.columns:
            market_cap = df["market_cap"].iloc[-1]
            if not pd.isna(market_cap) and market_cap > 0:
                return market_cap
        
        # 检查是否需要缓存市值数据
        cache_file = os.path.join(os.path.dirname(BASIC_INFO_FILE), "market_cap_cache.csv")
        cache_days = 3  # 市值数据缓存3天
        
        if os.path.exists(cache_file):
            cache_df = pd.read_csv(cache_file)
            record = cache_df[cache_df["code"] == stock_code]
            if not record.empty:
                last_update = record["last_update"].values[0]
                try:
                    last_update_time = datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S")
                    if (datetime.now() - last_update_time).days <= cache_days:
                        market_cap = record["market_cap"].values[0]
                        if not pd.isna(market_cap) and market_cap > 0:
                            logger.debug(f"使用缓存的市值数据: {market_cap:.2f}亿元 (最后更新: {last_update})")
                            return market_cap
                except Exception as e:
                    logger.warning(f"解析市值缓存更新时间失败: {str(e)}")
        
        # 尝试从实时行情数据获取
        stock_info = None
        for attempt in range(3):
            try:
                stock_info = ak.stock_zh_a_spot_em()
                if not stock_info.empty:
                    break
            except Exception as e:
                logger.debug(f"尝试{attempt+1}/3: 获取实时行情数据失败: {str(e)}")
                time.sleep(1.5 * (2 ** attempt))  # 指数退避
        
        if stock_info is not None and not stock_info.empty:
            # 标准化股票代码匹配
            stock_code_std = stock_code.zfill(6)
            matched = stock_info[stock_info["代码"] == stock_code_std]
            
            if not matched.empty:
                # 直接使用中文列名获取流通市值
                if "流通市值" in matched.columns:
                    market_cap = float(matched["流通市值"].values[0]) / 100000000  # 元 → 亿元
                    if market_cap > 0:
                        logger.debug(f"✅ 获取到流通市值: {market_cap:.2f}亿元")
                        # 更新缓存
                        if os.path.exists(cache_file):
                            cache_df = pd.read_csv(cache_file)
                            if stock_code in cache_df["code"].values:
                                cache_df.loc[cache_df["code"] == stock_code, "market_cap"] = market_cap
                                cache_df.loc[cache_df["code"] == stock_code, "last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            else:
                                new_record = pd.DataFrame([{
                                    "code": stock_code,
                                    "market_cap": market_cap,
                                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }])
                                cache_df = pd.concat([cache_df, new_record], ignore_index=True)
                            cache_df.to_csv(cache_file, index=False)
                        else:
                            cache_df = pd.DataFrame([{
                                "code": stock_code,
                                "market_cap": market_cap,
                                "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }])
                            cache_df.to_csv(cache_file, index=False)
                        return market_cap
                
                # 直接使用中文列名获取总市值
                if "总市值" in matched.columns:
                    market_cap = float(matched["总市值"].values[0]) / 100000000  # 元 → 亿元
                    if market_cap > 0:
                        logger.debug(f"✅ 获取到总市值: {market_cap:.2f}亿元")
                        # 更新缓存
                        if os.path.exists(cache_file):
                            cache_df = pd.read_csv(cache_file)
                            if stock_code in cache_df["code"].values:
                                cache_df.loc[cache_df["code"] == stock_code, "market_cap"] = market_cap
                                cache_df.loc[cache_df["code"] == stock_code, "last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            else:
                                new_record = pd.DataFrame([{
                                    "code": stock_code,
                                    "market_cap": market_cap,
                                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }])
                                cache_df = pd.concat([cache_df, new_record], ignore_index=True)
                            cache_df.to_csv(cache_file, index=False)
                        else:
                            cache_df = pd.DataFrame([{
                                "code": stock_code,
                                "market_cap": market_cap,
                                "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }])
                            cache_df.to_csv(cache_file, index=False)
                        return market_cap
        
        # 如果无法获取准确市值，返回None表示数据不可靠
        logger.warning(f"⚠️ 无法获取股票 {stock_code} 的准确市值，市值数据不可靠")
        return None
    
    except Exception as e:
        logger.error(f"估算{stock_code}市值失败: {str(e)}", exc_info=True)
        return None

def is_stock_suitable(stock_code: str, df: pd.DataFrame, data_level: str, data_days: int) -> bool:
    """根据数据完整性级别应用不同的筛选策略"""
    try:
        if df is None or df.empty or data_days < 10:
            logger.debug(f"股票 {stock_code} 数据不足，跳过")
            return False
        
        # 获取股票所属板块
        section = get_stock_section(stock_code)
        if section == "其他板块" or section not in MARKET_SECTIONS:
            logger.debug(f"股票 {stock_code} 不属于任何板块，跳过")
            return False
        
        # 获取板块配置
        section_config = MARKET_SECTIONS[section]
        
        # 根据数据完整性应用不同筛选策略
        if data_level == "complete":
            # 完整数据：应用全部三重过滤
            return _full_filter_strategy(stock_code, df, section, section_config)
        elif data_level in ["gapped", "partial"]:
            # 部分数据：应用简化版过滤
            return _simplified_filter_strategy(stock_code, df, section, section_config, data_days)
        elif data_level == "insufficient":
            # 数据严重不足：只应用基础过滤
            return _basic_filter_strategy(stock_code, df, section, section_config, data_days)
        else:  # corrupted or unknown
            logger.debug(f"股票 {stock_code} 数据损坏，跳过")
            return False
    
    except Exception as e:
        logger.error(f"筛选股票{stock_code}失败: {str(e)}", exc_info=True)
        return False

def _full_filter_strategy(stock_code: str, df: pd.DataFrame, section: str, section_config: dict) -> bool:
    """完整数据筛选策略（全部三重过滤）"""
    # 1. 流动性过滤
    if '成交量' in df.columns and '收盘' in df.columns and len(df) >= 20:
        daily_volume = df["成交量"].iloc[-20:].mean() * 100 * df["收盘"].iloc[-20:].mean()
        if daily_volume < section_config["min_daily_volume"]:
            return False
    
    # 2. 波动率过滤
    annual_volatility = calculate_annual_volatility(df)
    if annual_volatility > section_config["max_volatility"]:
        return False
    
    # 3. 市值过滤
    market_cap = calculate_market_cap(df, stock_code)
    if market_cap is None or market_cap < section_config["min_market_cap"]:
        return False
    
    return True

def _simplified_filter_strategy(stock_code: str, df: pd.DataFrame, section: str, 
                              section_config: dict, data_days: int) -> bool:
    """简化版筛选策略（跳过部分计算）"""
    # 只应用关键过滤
    # 1. 流动性过滤（使用可用数据）
    if '成交量' in df.columns and '收盘' in df.columns:
        # 使用可用的最近数据
        available_days = min(20, data_days)
        daily_volume = df["成交量"].iloc[-available_days:].mean() * 100 * df["收盘"].iloc[-available_days:].mean()
        if daily_volume < section_config["min_daily_volume"] * 0.8:  # 适当放宽阈值
            return False
    
    # 2. 市值过滤（必须）
    market_cap = calculate_market_cap(df, stock_code)
    if market_cap is None or market_cap < section_config["min_market_cap"]:
        return False
    
    # 跳过波动率过滤（数据不完整时波动率计算不可靠）
    return True

def _basic_filter_strategy(stock_code: str, df: pd.DataFrame, section: str, 
                         section_config: dict, data_days: int) -> bool:
    """基础筛选策略（仅应用关键过滤）"""
    # 只应用最基础的市值过滤
    market_cap = calculate_market_cap(df, stock_code)
    if market_cap is None or market_cap < section_config["min_market_cap"] * 0.9:  # 进一步放宽
        return False
    
    return True

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

def get_stock_group(stock_code: str) -> int:
    """
    根据股票代码确定股票所属的组（1-5）
    
    Args:
        stock_code: 股票代码（可能带市场前缀）
    
    Returns:
        int: 股票所属的组（1-5）
    """
    # 确保股票代码是字符串
    stock_code = str(stock_code)
    
    # 移除可能的市场前缀（sh/sz）
    if stock_code.lower().startswith(('sh', 'sz')):
        stock_code = stock_code[2:]
    
    # 确保股票代码是6位数字
    stock_code = stock_code.zfill(6)
    
    # 计算组号：基于股票代码最后一位数字
    last_digit = int(stock_code[-1])
    
    # 将0-9的数字映射到1-5的组
    group = (last_digit % 5) + 1
    
    return group

def get_top_stocks_for_strategy() -> Dict[str, List[Dict]]:
    """按板块获取适合策略的股票（使用增量数据）"""
    try:
        logger.info("===== 开始执行个股趋势策略(TickTen) =====")
        
        # 1. 获取股票基础信息
        basic_info_df = fetch_stock_list()
        if basic_info_df.empty:
            logger.error("获取股票基础信息失败，无法继续")
            return {}
        
        logger.info(f"已加载股票基础信息，共 {len(basic_info_df)} 条记录")
        
        # 2. 首先应用基础过滤（市值过滤）
        if not basic_info_df.empty:
            # 过滤市值不足的股票
            initial_count = len(basic_info_df)
            basic_info_df = basic_info_df[basic_info_df["market_cap"] >= MIN_MARKET_CAP_FOR_BASIC_FILTER]
            filtered_count = len(basic_info_df)
            logger.info(f"【基础过滤】市值过滤后剩余 {filtered_count} 只股票（市值≥{MIN_MARKET_CAP_FOR_BASIC_FILTER}亿元）（过滤了 {initial_count - filtered_count} 只）")
        
        # 如果没有通过市值过滤的股票，直接返回
        if basic_info_df.empty:
            logger.warning("没有通过市值过滤的股票，无法继续筛选")
            return {}
        
        # 3. 按板块分组处理
        section_stocks = {section: [] for section in MARKET_SECTIONS.keys()}
        
        # 4. 初始化各板块计数器
        section_counts = {section: {"total": 0, "data_ok": 0, "suitable": 0, "scored": 0}
                         for section in MARKET_SECTIONS.keys()}
        
        # 5. 处理每只股票 - 仅处理通过市值过滤的股票
        stock_list = basic_info_df.to_dict('records')
        logger.info(f"开始处理 {len(stock_list)} 只通过市值过滤的股票...")
        
        # 分阶段执行：只处理今天的分组
        today = datetime.now().date()
        # 使用日期的天数对5取模，确保每天处理不同的组
        today_group = (today.day % 5) + 1
        logger.info(f"今天处理第 {today_group} 组股票（共5组）")
        
        # 确保所有股票代码是字符串格式（6位，前面补零）
        for stock in stock_list:
            stock["code"] = str(stock["code"]).zfill(6)
        
        # 直接根据股票代码最后一位计算分组
        # 将股票代码最后一位数字映射到1-5的组
        stock_list = [stock for stock in stock_list if (int(stock["code"][-1]) % 5) + 1 == today_group]
        logger.info(f"今天实际处理 {len(stock_list)} 只股票（分组过滤后）")
        
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
                    # 从缓存加载数据（避免重复读取文件）
                    df = get_cached_stock_data(stock_code)
                    if df is not None:
                        return {
                            "code": stock_code,
                            "name": stock_name,
                            "score": cached_score,
                            "df": df,
                            "section": section
                        }
            
            # 2. 获取日线数据（增量更新）
            df = fetch_stock_data(stock_code)
            
            # 3. 检查数据完整性
            data_level, data_days = check_data_integrity(df)
            logger.debug(f"股票 {stock_code} 数据完整性: {data_level} ({data_days}天)")
            
            # 4. 根据数据完整性应用不同策略
            if is_stock_suitable(stock_code, df, data_level, data_days):
                # 5. 计算策略得分
                score = calculate_stock_strategy_score(stock_code, df)
                
                if score > 0:
                    # 6. 缓存结果
                    cache_filter_result(stock_code, True)
                    cache_score(stock_code, score)
                    
                    # 7. 更新板块计数器
                    section_counts[section]["suitable"] += 1
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
        # 提高并发数，但确保不会触发AkShare限制
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
        
        # 8. 记录各板块筛选结果
        for section, counts in section_counts.items():
            if counts["total"] > 0:
                logger.info(f"【筛选统计】板块 {section}:")
                logger.info(f"  - 总股票数量: {counts['total']}")
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
        
        # 10. 更新基础信息中的市值和评分
        updated_records = []
        for section, stocks in top_stocks_by_section.items():
            for stock in stocks:
                stock_code = str(stock["code"]).zfill(6)
                stock_name = stock["name"]
                section = stock["section"]
                market_cap = calculate_market_cap(stock["df"], stock_code)
                score = stock["score"]
                
                # 只更新市值数据，不覆盖基础信息
                if market_cap is not None:
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
        
        return top_stocks_by_section
    
    except Exception as e:
        logger.error(f"获取优质股票列表失败: {str(e)}", exc_info=True)
        return {}

# ========== 以下是关键修改 ==========
STOCK_DATA_CACHE = {}
CACHE_MAX_SIZE = 1000  # 最大缓存股票数量

def get_cached_stock_data(stock_code: str) -> Optional[pd.DataFrame]:
    """获取缓存的股票数据"""
    if stock_code in STOCK_DATA_CACHE:
        return STOCK_DATA_CACHE[stock_code]
    return None

def cache_stock_data(stock_code: str, df: pd.DataFrame):
    """缓存股票数据"""
    # 实现LRU缓存策略
    if len(STOCK_DATA_CACHE) >= CACHE_MAX_SIZE:
        # 移除最旧的缓存项
        oldest_key = min(STOCK_DATA_CACHE.keys(), key=lambda k: STOCK_DATA_CACHE[k]["timestamp"])
        del STOCK_DATA_CACHE[oldest_key]
    
    STOCK_DATA_CACHE[stock_code] = {
        "data": df,
        "timestamp": datetime.now()
    }

def get_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """获取股票日线数据（带缓存）"""
    # 1. 检查缓存
    cached_df = get_cached_stock_data(stock_code)
    if cached_df is not None:
        return cached_df
    
    # 2. 从文件加载
    daily_dir = os.path.join(os.path.dirname(BASIC_INFO_FILE), "daily")
    file_path = os.path.join(daily_dir, f"{stock_code}.csv")
    
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            # 缓存数据
            cache_stock_data(stock_code, df)
            return df
        except Exception as e:
            logger.warning(f"读取股票 {stock_code} 数据失败: {str(e)}")
    
    # 3. 如果文件不存在或读取失败，获取新数据
    logger.info(f"股票 {stock_code} 首次爬取或历史数据处理失败，获取1年历史数据")
    df = fetch_stock_data(stock_code, days=365)
    
    if not df.empty:
        # 限制数据为最近1年
        df = limit_to_one_year_data(df)
        
        # 保存数据
        os.makedirs(daily_dir, exist_ok=True)
        df.to_csv(file_path, index=False)
        
        # 缓存数据
        cache_stock_data(stock_code, df)
        
        # 提交到Git
        try:
            logger.info(f"正在提交股票 {stock_code} 数据到GitHub仓库...")
            commit_message = f"自动更新股票 {stock_code} 数据 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
            if commit_and_push_file(file_path, commit_message):
                logger.info(f"股票 {stock_code} 数据已成功提交并推送到GitHub仓库")
        except Exception as e:
            logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败: {str(e)}")
    
    return df
# ========== 以上是关键修改 ==========


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

# ========== 以下是关键修改 ==========
def get_last_crawl_date(stock_code: str) -> str:
    """获取股票最后爬取日期"""
    # 检查是否有最后爬取日期记录
    last_crawl_file = os.path.join(os.path.dirname(BASIC_INFO_FILE), "last_crawl_date.csv")
    if os.path.exists(last_crawl_file):
        try:
            last_crawl_df = pd.read_csv(last_crawl_file)
            record = last_crawl_df[last_crawl_df["code"] == stock_code]
            if not record.empty:
                return record["last_date"].values[0]
        except Exception as e:
            logger.warning(f"读取最后爬取日期失败: {str(e)}")
    
    # 如果没有记录，返回1年之前的日期（首次爬取）
    return (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

def save_last_crawl_date(stock_code: str, date: str):
    """保存股票最后爬取日期"""
    last_crawl_file = os.path.join(os.path.dirname(BASIC_INFO_FILE), "last_crawl_date.csv")
    
    try:
        if os.path.exists(last_crawl_file):
            df = pd.read_csv(last_crawl_file)
            # 更新或添加记录
            if stock_code in df["code"].values:
                df.loc[df["code"] == stock_code, "last_date"] = date
            else:
                df = pd.concat([df, pd.DataFrame([{"code": stock_code, "last_date": date}])], ignore_index=True)
        else:
            df = pd.DataFrame([{"code": stock_code, "last_date": date}])
        
        df.to_csv(last_crawl_file, index=False)
    except Exception as e:
        logger.error(f"保存最后爬取日期失败: {str(e)}", exc_info=True)

def get_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """获取股票日线数据（增量更新，并限制为最近1年数据）"""
    try:
        # 确保股票代码是字符串，并且是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        
        daily_dir = os.path.join(os.path.dirname(BASIC_INFO_FILE), "daily")
        os.makedirs(daily_dir, exist_ok=True)
        
        # 检查本地是否有历史数据
        file_path = os.path.join(daily_dir, f"{stock_code}.csv")
        if os.path.exists(file_path):
            try:
                # 读取历史数据
                historical_df = pd.read_csv(file_path)
                
                # 获取最后一条记录的日期
                last_date = historical_df["日期"].max()
                logger.debug(f"股票 {stock_code} 最后数据日期: {last_date}")
                
                # 计算需要爬取的日期范围
                start_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y%m%d")
                end_date = datetime.now().strftime("%Y%m%d")
                
                # 如果没有新数据需要爬取
                if start_date > end_date:
                    logger.info(f"股票 {stock_code} 数据已最新，无需爬取")
                    
                    # ========== 关键修改 ==========
                    # 限制数据为最近1年
                    limited_df = limit_to_one_year_data(historical_df)
                    if len(limited_df) < len(historical_df):
                        limited_df.to_csv(file_path, index=False)
                        logger.info(f"股票 {stock_code} 数据已限制为最近1年，保存 {len(limited_df)} 条记录")
                    return limited_df
                    # ========== 关键修改 ==========
                
                # 获取增量数据
                logger.info(f"股票 {stock_code} 增量爬取: {start_date} 至 {end_date}")
                incremental_df = fetch_stock_data_incremental(stock_code, start_date, end_date)
                
                # 如果获取到新数据，合并并保存
                if not incremental_df.empty:
                    # 确保增量数据的日期格式一致
                    incremental_df["日期"] = incremental_df["日期"].astype(str)
                    
                    # 合并数据
                    combined_df = pd.concat([historical_df, incremental_df], ignore_index=True)
                    # 去重并按日期排序
                    combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                    combined_df = combined_df.sort_values("日期", ascending=True)
                    
                    # 限制数据为最近1年
                    # ========== 关键修改 ==========
                    limited_df = limit_to_one_year_data(combined_df)
                    # ========== 关键修改 ==========
                    
                    # 保存更新后的数据
                    limited_df.to_csv(file_path, index=False)
                    
                    # 更新最后爬取日期
                    latest_date = incremental_df["日期"].max()
                    save_last_crawl_date(stock_code, latest_date)
                    
                    logger.info(f"股票 {stock_code} 数据已更新，最新日期: {latest_date}")
                    
                    # ========== 关键修改 ==========
                    # 立即提交到Git仓库（每股票单独提交）
                    try:
                        logger.info(f"正在提交股票 {stock_code} 数据到GitHub仓库...")
                        commit_message = f"自动更新股票 {stock_code} 数据 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
                        if commit_and_push_file(file_path, commit_message):
                            logger.info(f"股票 {stock_code} 数据已成功提交并推送到GitHub仓库")
                        else:
                            logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败，但继续执行策略")
                    except Exception as e:
                        logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败: {str(e)}，但继续执行策略")
                    # ========== 关键修改 ==========
                    
                    return limited_df
            except Exception as e:
                logger.warning(f"处理股票 {stock_code} 历史数据时出错: {str(e)}，将重新获取完整数据")
        
        # 首次爬取或历史数据处理失败
        logger.info(f"股票 {stock_code} 首次爬取或历史数据处理失败，获取1年历史数据")
        full_df = fetch_stock_data(stock_code, days=365)
        
        if not full_df.empty:
            # 限制数据为最近1年（虽然这里已经是1年，但确保格式正确）
            # ========== 关键修改 ==========
            limited_df = limit_to_one_year_data(full_df)
            # ========== 关键修改 ==========
            
            # 保存数据
            limited_df.to_csv(file_path, index=False)
            
            # 更新最后爬取日期
            latest_date = limited_df["日期"].max()
            save_last_crawl_date(stock_code, latest_date)
            
            # ========== 关键修改 ==========
            # 立即提交到Git仓库（每股票单独提交）
            try:
                logger.info(f"正在提交股票 {stock_code} 数据到GitHub仓库...")
                commit_message = f"自动更新股票 {stock_code} 数据 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
                if commit_and_push_file(file_path, commit_message):
                    logger.info(f"股票 {stock_code} 数据已成功提交并推送到GitHub仓库")
                else:
                    logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败，但继续执行策略")
            except Exception as e:
                logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败: {str(e)}，但继续执行策略")
            # ========== 关键修改 ==========
            
            return limited_df
        
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

# ========== 关键新增 ==========
def limit_to_one_year_data(df: pd.DataFrame) -> pd.DataFrame:
    """限制数据为最近1年的数据
    
    Args:
        df: 原始DataFrame
    
    Returns:
        pd.DataFrame: 限制为1年数据后的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 计算1年前的日期
        one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        
        # 确保日期列存在
        if "日期" not in df.columns:
            logger.warning("数据中缺少日期列，无法限制为1年数据")
            return df
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 转换日期列
        df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
        
        # 过滤数据
        mask = df["日期"] >= pd.to_datetime(one_year_ago)
        df = df.loc[mask]
        
        # 转换回字符串格式
        df["日期"] = df["日期"].dt.strftime("%Y-%m-%d")
        
        logger.info(f"数据已限制为最近1年（从 {one_year_ago} 至 {datetime.now().strftime('%Y-%m-%d')}），剩余 {len(df)} 条记录")
        return df
    
    except Exception as e:
        logger.error(f"限制数据为1年时发生错误: {str(e)}", exc_info=True)
        return df
# ========== 关键新增 ==========

def fetch_stock_data_incremental(stock_code: str) -> pd.DataFrame:
    """增量获取股票历史数据"""
    try:
        # 确保股票代码是字符串，并且是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        
        # 确定市场前缀
        section = get_stock_section(stock_code)
        if section == "沪市主板" or section == "科创板":
            market_prefix = "sh"
        else:  # 深市主板、创业板
            market_prefix = "sz"
        
        # 检查本地是否已有数据
        file_path = os.path.join(Config.DATA_DIR, "daily", f"{stock_code}.csv")
        start_date = None
        
        if os.path.exists(file_path):
            # 读取现有数据文件
            existing_df = pd.read_csv(file_path)
            if not existing_df.empty and "日期" in existing_df.columns:
                # 获取现有数据的最新日期
                latest_date = pd.to_datetime(existing_df["日期"]).max().strftime("%Y%m%d")
                # 从最新日期的下一天开始获取
                start_date = (datetime.strptime(latest_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
                logger.info(f"检测到现有数据，最新日期: {latest_date}, 将从 {start_date} 开始增量获取")
        
        # 如果没有找到有效起始日期，获取1年数据
        end_date = datetime.now().strftime("%Y%m%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        
        # 如果开始日期晚于结束日期，无需获取
        if start_date > end_date:
            logger.info(f"股票 {stock_code} 无需更新数据")
            return pd.DataFrame()
        
        # 尝试多种可能的股票代码格式
        possible_codes = [
            f"{market_prefix}{stock_code}",  # "sh000001"
            f"{stock_code}.{market_prefix.upper()}",  # "000001.SH"
            stock_code,  # "000001"
            f"{stock_code}.{'SZ' if market_prefix == 'sz' else 'SH'}",  # "000001.SZ"
            f"{market_prefix}{stock_code}.XSHG" if market_prefix == "sh" else f"{market_prefix}{stock_code}.XSHE",  # 交易所格式
        ]
        
        # 尝试使用多种接口和代码格式获取数据
        df = None
        for code in possible_codes:
            for attempt in range(3):  # 重试3次
                try:
                    logger.debug(f"尝试{attempt+1}/3: 使用stock_zh_a_hist接口获取股票 {code}")
                    df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                          start_date=start_date, end_date=end_date, 
                                          adjust="qfq")
                    if not df.empty:
                        break
                except Exception as e:
                    logger.debug(f"使用stock_zh_a_hist接口获取股票 {code} 失败: {str(e)}")
                
                # 增加重试等待时间
                time.sleep(1.5 * (2 ** attempt))
            
            if df is not None and not df.empty:
                break
        
        if df is None or df.empty:
            return pd.DataFrame()
        
        # 确保日期列存在
        if "日期" not in df.columns:
            return pd.DataFrame()
        
        # 检查是否有必要的列
        required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return pd.DataFrame()
        
        # 确保日期列是字符串类型
        if "日期" in df.columns:
            df["日期"] = df["日期"].astype(str)
            # 确保日期格式为YYYY-MM-DD
            df["日期"] = df["日期"].str.replace(r'(\d{4})/(\d{1,2})/(\d{1,2})', 
                                              lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", 
                                              regex=True)
        
        return df
    
    except Exception as e:
        logger.debug(f"获取股票 {stock_code} 增量数据失败: {str(e)}")
        return pd.DataFrame()
# ========== 以上是关键修改 ==========

def main():
    """主函数"""
    try:
        logger.info("===== 开始执行个股趋势策略(TickTen) =====")
        
        # 清理过期的市值缓存
        cache_file = os.path.join(os.path.dirname(BASIC_INFO_FILE), "market_cap_cache.csv")
        if os.path.exists(cache_file):
            try:
                cache_df = pd.read_csv(cache_file)
                if not cache_df.empty and "last_update" in cache_df.columns:
                    # 保留最近3天的缓存
                    cache_df["last_update"] = pd.to_datetime(cache_df["last_update"])
                    cutoff_date = datetime.now() - timedelta(days=3)
                    cache_df = cache_df[cache_df["last_update"] >= cutoff_date]
                    cache_df.to_csv(cache_file, index=False)
                    logger.info(f"清理市值缓存，保留 {len(cache_df)} 条有效记录")
            except Exception as e:
                logger.warning(f"清理市值缓存失败: {str(e)}")
                # 如果清理失败，删除缓存文件重新开始
                try:
                    os.remove(cache_file)
                    logger.info("已删除损坏的市值缓存文件")
                except:
                    pass
        
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
