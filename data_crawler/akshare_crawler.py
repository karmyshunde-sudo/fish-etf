#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块
使用AkShare接口获取ETF日线数据
特别优化了列名映射和数据完整性检查
"""

import akshare as ak
import pandas as pd
import logging
import time
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timedelta, date
from config import Config
from retrying import retry

# 修复：正确导入函数
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    get_last_trading_day,
    is_trading_day
)
# 从正确的模块导入数据处理函数
from utils.file_utils import (
    ensure_chinese_columns, internal_ensure_chinese_columns
)
from utils.data_processor import (
    ensure_required_columns,
    clean_and_format_data,
    limit_to_one_year_data
)
# 仅添加必要的git工具导入（不添加任何新函数）
from utils.git_utils import commit_files_in_batches

# 初始化日志
logger = logging.getLogger(__name__)

# 重试配置
MAX_RETRY_ATTEMPTS = 5
RETRY_WAIT_FIXED = 3000
RETRY_WAIT_EXPONENTIAL_MAX = 15000

# 打印AkShare版本
logger.info(f"AkShare版本: {ak.__version__}")

def empty_result_check(result: pd.DataFrame) -> bool:
    """
    检查AkShare返回结果是否为空
    
    Args:
        result: AkShare返回的DataFrame
        
    Returns:
        bool: 如果结果为空返回True，否则返回False
    """
    return result is None or result.empty

def retry_if_akshare_error(exception: Exception) -> bool:
    """
    重试条件：AkShare相关错误
    
    Args:
        exception: 异常对象
        
    Returns:
        bool: 如果是AkShare错误返回True，否则返回False
    """
    from requests.exceptions import ConnectionError, Timeout
    return isinstance(exception, (ValueError, ConnectionError, Timeout, OSError))

@retry(
    stop_max_attempt_number=MAX_RETRY_ATTEMPTS,
    wait_fixed=RETRY_WAIT_FIXED,
    retry_on_result=empty_result_check,
    retry_on_exception=retry_if_akshare_error
)
def crawl_etf_daily_akshare(etf_code: str, start_date: str, end_date: str, is_first_crawl: bool = False) -> pd.DataFrame:
    """用AkShare爬取ETF日线数据
    
    Args:
        etf_code: ETF代码 (6位数字)
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        is_first_crawl: 是否是首次爬取
    
    Returns:
        pd.DataFrame: 包含ETF日线数据的DataFrame
    """
    try:
        # 修复：将字符串日期转换为date对象
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
        # 获取最近交易日
        last_trading_day = get_last_trading_day(end_date_obj)
        # 转换回字符串格式
        end_date = last_trading_day.strftime("%Y-%m-%d")
        
        # 关键修复：处理单日请求问题
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
        if start_date_obj == last_trading_day:
            # 如果是单日请求，扩展为至少3天的范围
            start_date_obj = start_date_obj - timedelta(days=2)
            start_date = start_date_obj.strftime("%Y-%m-%d")
            logger.info(f"单日请求扩展为 {start_date} 至 {end_date}")
        
        logger.info(f"开始爬取ETF {etf_code} 的数据，时间范围：{start_date} 至 {end_date}")
        
        # 优先使用stock_zh_a_hist获取完整数据（最完整接口）
        df = try_stock_zh_a_hist(etf_code, start_date, end_date)
        
        # 如果stock_zh_a_hist失败，尝试fund_etf_hist_sina
        if df.empty:
            logger.info(f"stock_zh_a_hist获取失败，尝试fund_etf_hist_sina")
            df = try_fund_etf_hist_sina(etf_code, start_date, end_date)
        
        # 如果主要接口都失败，尝试fund_etf_spot_em获取实时数据
        if df.empty:
            logger.info(f"主要接口获取失败，尝试fund_etf_spot_em获取实时数据")
            df = try_fund_etf_spot_em(etf_code, start_date, end_date)
        
        # 检查数据是否成功获取
        if df.empty:
            logger.warning(f"所有数据源均未获取到{etf_code}数据（{start_date}至{end_date}）")
            return pd.DataFrame()
        
        # 标准化列名
        df = ensure_chinese_columns(df)
        
        # 确保所有必需列都存在（不进行计算，只检查）
        df = ensure_required_columns(df)
        
        # 数据清洗：去重、格式转换
        df = clean_and_format_data(df)
        
        # 首次爬取时限制数据量为1年
        if is_first_crawl:
            df = limit_to_one_year_data(df, end_date)
        
        logger.info(f"成功获取ETF {etf_code} 数据，共{len(df)}条记录")
        return df
    except Exception as e:
        logger.error(f"爬取ETF {etf_code} 失败: {str(e)}", exc_info=True)
        raise

def try_stock_zh_a_hist(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """使用stock_zh_a_hist接口获取ETF数据（优先使用）"""
    try:
        logger.info(f"尝试使用stock_zh_a_hist接口获取ETF {etf_code} 数据")
        # 关键修复：为510300等上交所ETF添加"sh"前缀
        symbol = f"sh{etf_code}" if etf_code.startswith('5') else f"sz{etf_code}"
        df = ak.index_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        
        if not df.empty:
            logger.info(f"stock_zh_a_hist 接口成功获取ETF {etf_code} 数据")
            logger.info(f"📊 stock_zh_a_hist 接口返回的原始列名: {list(df.columns)}")
            
            # 标准化列名
            df = df.rename(columns={
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
            })
            
            # 确保日期格式
            if '日期' in df.columns:
                df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
            
            # 添加ETF代码和名称（但不在此处计算折溢价率）
            if 'ETF代码' not in df.columns:
                df['ETF代码'] = etf_code
            if 'ETF名称' not in df.columns:
                # 名称需要在外部获取，这里留空
                df['ETF名称'] = ""
                
            return df
    except Exception as e:
        logger.debug(f"stock_zh_a_hist接口失败: {str(e)}")
    
    return pd.DataFrame()

def try_fund_etf_hist_sina(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """使用fund_etf_hist_sina接口获取ETF数据"""
    try:
        logger.info(f"尝试使用fund_etf_hist_sina接口获取ETF {etf_code} 数据")
        # 关键修复：使用正确的市场前缀
        symbol = get_symbol_with_market_prefix(etf_code)
        df = ak.fund_etf_hist_sina(symbol=symbol)
        
        if not df.empty:
            logger.info(f"fund_etf_hist_sina 接口成功获取ETF {etf_code} 数据")
            logger.info(f"📊 fund_etf_hist_sina 接口返回的原始列名: {list(df.columns)}")
            
            # 标准化列名
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
                'turnover_rate': '换手率',
                'trade_date': '日期',
                'open_price': '开盘',
                'high_price': '最高',
                'low_price': '最低',
                'close_price': '收盘',
                'vol': '成交量',
                'amount_volume': '成交额',
                'amplitude_percent': '振幅',
                'pct_chg': '涨跌幅',
                'price_change': '涨跌额',
                'turnover_ratio': '换手率',
                'net_value': '净值',
                'iopv': 'IOPV'
            }
            # 重命名列
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            # 确保日期列存在
            if '日期' not in df.columns and 'date' in df.columns:
                df = df.rename(columns={'date': '日期'})
            
            # 日期过滤
            if '日期' in df.columns:
                df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
                mask = (df['日期'] >= start_date) & (df['日期'] <= end_date)
                df = df.loc[mask]
            
            # 添加ETF代码和名称
            if 'ETF代码' not in df.columns:
                df['ETF代码'] = etf_code
            if 'ETF名称' not in df.columns:
                df['ETF名称'] = ""
                
            return df
    except Exception as e:
        logger.debug(f"fund_etf_hist_sina接口失败: {str(e)}")
    
    return pd.DataFrame()

def try_fund_etf_spot_em(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """使用fund_etf_spot_em接口获取ETF实时数据（仅最新数据）"""
    try:
        logger.info(f"尝试使用fund_etf_spot_em接口获取ETF {etf_code} 实时数据")
        df = ak.fund_etf_spot_em()
        
        if not df.empty:
            logger.info(f"fund_etf_spot_em 接口返回的原始列名: {list(df.columns)}")
            
            # 过滤指定ETF
            df = df[df["代码"] == etf_code]
            
            if not df.empty:
                # 标准化列名
                column_mapping = {
                    "代码": "ETF代码",
                    "名称": "ETF名称",
                    "最新价": "收盘",
                    "IOPV实时估值": "IOPV",
                    "基金折价率": "折溢价率",
                    "涨跌额": "涨跌额",
                    "涨跌幅": "涨跌幅",
                    "成交量": "成交量",
                    "成交额": "成交额",
                    "开盘价": "开盘",
                    "最高价": "最高",
                    "最低价": "最低",
                    "昨收": "前收盘",
                    "振幅": "振幅",
                    "换手率": "换手率",
                    "数据日期": "日期"
                }
                df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
                
                # 确保日期格式
                if "日期" in df.columns:
                    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
                
                # 设置成交量单位为"手"（1手=100股）
                if "成交量" in df.columns:
                    df["成交量"] = df["成交量"] / 100
                
                # 仅保留在指定日期范围内的数据
                if "日期" in df.columns:
                    mask = (df["日期"] >= start_date) & (df["日期"] <= end_date)
                    df = df.loc[mask]
                
                return df
    except Exception as e:
        logger.debug(f"fund_etf_spot_em接口失败: {str(e)}")
    
    return pd.DataFrame()

def get_symbol_with_market_prefix(etf_code: str) -> str:
    """
    根据ETF代码获取带市场前缀的代码
    
    Args:
        etf_code: ETF代码
        
    Returns:
        str: 带市场前缀的代码
    """
    if etf_code.startswith(('5', '6', '9')):
        return f"sh{etf_code}"
    else:
        return f"sz{etf_code}"

def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    确保DataFrame包含所有必需的交易数据列
    注意：此函数仅检查列是否存在，不进行任何计算
    
    Args:
        df: 原始DataFrame
        
    Returns:
        pd.DataFrame: 包含所有必需列的DataFrame
    """
    if df.empty:
        return df
    
    # 必需列列表
    required_columns = [
        "日期", "开盘", "最高", "最低", "收盘", "成交量", 
        "成交额", "振幅", "涨跌幅", "涨跌额", "换手率", 
        "ETF代码", "ETF名称", "折溢价率"
    ]
    
    # 检查缺失列
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"❌ 数据源缺少必需列：{', '.join(missing_columns)}")
        # 不尝试修复，只记录错误
        return pd.DataFrame()
    
    return df

def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    清洗并格式化数据
    """
    try:
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 处理日期列
        if "日期" in df.columns:
            # 尝试将日期列转换为datetime类型
            try:
                # 先确保是字符串类型，便于处理各种可能的日期格式
                df["日期"] = df["日期"].astype(str)
                # 尝试转换为datetime
                df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
                # 格式化为字符串
                df["日期"] = df["日期"].dt.strftime("%Y-%m-%d")
            except Exception as e:
                logger.error(f"日期列处理失败: {str(e)}", exc_info=True)
        
        # 保持原始列顺序
        required_columns = [
            "日期", "开盘", "最高", "最低", "收盘", "成交量", 
            "成交额", "振幅", "涨跌幅", "涨跌额", "换手率", 
            "ETF代码", "ETF名称", "折溢价率"
        ]
        
        # 确保列顺序一致
        final_columns = [col for col in required_columns if col in df.columns]
        if final_columns:
            df = df[final_columns]
        
        # 移除重复行
        df = df.drop_duplicates(subset=["日期"], keep="last")
        
        # 按日期排序
        df = df.sort_values("日期", ascending=False)
        
        return df
    except Exception as e:
        logger.error(f"数据清洗过程中发生错误: {str(e)}", exc_info=True)
        return pd.DataFrame()

def limit_to_one_year_data(df: pd.DataFrame, end_date: str) -> pd.DataFrame:
    """
    限制数据为最近1年的数据
    
    Args:
        df: 原始DataFrame
        end_date: 结束日期
        
    Returns:
        pd.DataFrame: 限制为1年数据后的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 计算1年前的日期
        one_year_ago = (pd.to_datetime(end_date) - timedelta(days=365)).strftime("%Y-%m-%d")
        
        # 确保日期列存在
        if "日期" not in df.columns:
            logger.warning("数据中缺少日期列，无法限制为1年数据")
            return df
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 转换日期列
        df.loc[:, "日期"] = pd.to_datetime(df["日期"], errors='coerce')
        
        # 过滤数据
        mask = df["日期"] >= pd.to_datetime(one_year_ago)
        df = df.loc[mask]
        
        logger.info(f"数据已限制为最近1年（从 {one_year_ago} 至 {end_date}），剩余 {len(df)} 条数据")
        return df
    except Exception as e:
        logger.error(f"限制数据为1年时发生错误: {str(e)}", exc_info=True)
        return df
