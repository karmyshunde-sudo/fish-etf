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
from utils.date_utils import get_beijing_time  # 导入北京时间工具

# 初始化日志
logger = logging.getLogger(__name__)

# 重试配置
MAX_RETRY_ATTEMPTS = 3
RETRY_WAIT_FIXED = 2000  # 毫秒
RETRY_WAIT_EXPONENTIAL_MAX = 10000  # 毫秒

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
    return isinstance(exception, (ValueError, ConnectionError, TimeoutError))

@retry(stop_max_attempt_number=MAX_RETRY_ATTEMPTS,
       wait_fixed=RETRY_WAIT_FIXED,
       retry_on_result=empty_result_check,
       retry_on_exception=retry_if_akshare_error)
def crawl_etf_daily_akshare(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """用AkShare爬取ETF日线数据
    Args:
        etf_code: ETF代码 (6位数字)
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
    Returns:
        pd.DataFrame: 包含ETF日线数据的DataFrame
    """
    try:
        # 确保结束日期是交易日
        end_date = get_last_trading_day(end_date).strftime("%Y-%m-%d")
        
        logger.info(f"开始爬取ETF {etf_code} 的数据，时间范围：{start_date} 至 {end_date}")
        
        # 尝试多种AkShare接口
        df = try_multiple_akshare_interfaces(etf_code, start_date, end_date)
        
        if df.empty:
            logger.warning(f"AkShare未获取到{etf_code}数据（{start_date}至{end_date}）")
            return pd.DataFrame()
        
        # 记录返回的列名，用于调试
        logger.info(f"📊 AkShare数据源返回的原始列名: {list(df.columns)}")
        
        # 标准化列名
        df = standardize_column_names(df)
        
        # 确保所有必需列都存在
        df = ensure_required_columns(df)
        
        # 数据清洗：去重、格式转换
        df = clean_and_format_data(df)
        
        # 限制数据量为1年（365天）
        df = limit_to_one_year_data(df, end_date)
        
        logger.info(f"AkShare成功获取{etf_code}数据，共{len(df)}条（已限制为1年数据）")
        return df
    except Exception as e:
        logger.error(f"AkShare爬取{etf_code}失败：{str(e)}", exc_info=True)
        # 等待一段时间后重试
        time.sleep(2)
        raise  # 触发重试

def try_multiple_akshare_interfaces(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    尝试多种AkShare接口获取ETF数据
    
    Args:
        etf_code: ETF代码
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        pd.DataFrame: 获取到的DataFrame
    """
    interfaces = [
        lambda: try_fund_etf_hist_em(etf_code, start_date, end_date),
        lambda: try_fund_etf_hist_sina(etf_code)
    ]
    
    for i, interface in enumerate(interfaces):
        try:
            logger.debug(f"尝试第{i+1}种接口获取ETF {etf_code} 数据")
            df = interface()
            
            if not df.empty:
                logger.info(f"第{i+1}种接口成功获取ETF {etf_code} 数据")
                
                # 记录返回的列名，用于调试
                logger.info(f"📊 第{i+1}种接口返回的原始列名: {list(df.columns)}")
                
                # 对返回的数据进行日期过滤
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    mask = (df['date'] >= pd.to_datetime(start_date)) & (df['date'] <= pd.to_datetime(end_date))
                    df = df.loc[mask]
                elif '日期' in df.columns:
                    df['日期'] = pd.to_datetime(df['日期'])
                    mask = (df['日期'] >= pd.to_datetime(start_date)) & (df['日期'] <= pd.to_datetime(end_date))
                    df = df.loc[mask]
                
                if not df.empty:
                    logger.info(f"第{i+1}种接口成功获取ETF {etf_code} 数据（过滤后）")
                    return df
        except Exception as e:
            logger.warning(f"第{i+1}种接口调用失败: {str(e)}", exc_info=True)
            continue
    
    logger.warning(f"所有AkShare接口均无法获取ETF {etf_code} 数据")
    return pd.DataFrame()

def try_fund_etf_hist_em(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    尝试使用 fund_etf_hist_em 接口
    
    Args:
        etf_code: ETF代码
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        pd.DataFrame: 获取到的DataFrame
    """
    try:
        logger.debug(f"尝试使用 fund_etf_hist_em 接口获取ETF {etf_code} 数据")
        df = ak.fund_etf_hist_em(symbol=etf_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        
        if not df.empty:
            # 记录返回的列名，用于调试
            logger.info(f"📊 fund_etf_hist_em 接口返回的原始列名: {list(df.columns)}")
        return df
    except Exception as e:
        logger.warning(f"fund_etf_hist_em 接口调用失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def try_fund_etf_hist_sina(etf_code: str) -> pd.DataFrame:
    """
    尝试使用 fund_etf_hist_sina 接口
    
    Args:
        etf_code: ETF代码
        
    Returns:
        pd.DataFrame: 获取到的DataFrame
    """
    try:
        logger.debug(f"尝试使用 fund_etf_hist_sina 接口获取ETF {etf_code} 数据")
        symbol = get_symbol_with_market_prefix(etf_code)
        df = ak.fund_etf_hist_sina(symbol=symbol)
        
        # 新浪接口返回的数据可能需要特殊处理
        if not df.empty:
            # 记录返回的列名，用于调试
            logger.info(f"📊 fund_etf_hist_sina 接口返回的原始列名: {list(df.columns)}")
            
            # 新浪接口返回的列名可能是英文，需要转换为中文
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
                'turnover_ratio': '换手率'
            }
            
            # 重命名列
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            # 确保日期列存在
            if '日期' not in df.columns and 'date' in df.columns:
                df = df.rename(columns={'date': '日期'})
                
        return df
    except Exception as e:
        logger.warning(f"fund_etf_hist_sina 接口调用失败: {str(e)}", exc_info=True)
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

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化列名，将不同数据源的列名转换为统一的中文列名
    
    Args:
        df: 原始DataFrame
        
    Returns:
        pd.DataFrame: 标准化列名后的DataFrame
    """
    if df.empty:
        return df
    
    # 定义可能的列名变体
    column_variants = {
        '日期': ['date', '日期', 'trade_date', 'dt', 'datetime', '交易日期', 'time'],
        '开盘': ['open', '开盘价', '开', 'open_price', 'openprice', 'openprice_'],
        '最高': ['high', '最高价', '高', 'high_price', 'highprice', 'highprice_'],
        '最低': ['low', '最低价', '低', 'low_price', 'lowprice', 'lowprice_'],
        '收盘': ['close', '收盘价', '收', 'close_price', 'closeprice', 'closeprice_', 'price'],
        '成交量': ['volume', '成交量', 'vol', '成交数量', 'amount_volume', 'vol_', 'volume_'],
        '成交额': ['amount', '成交额', '成交金额', 'turnover', '成交总价', 'amount_', 'turnover_'],
        '振幅': ['amplitude', '振幅%', '振幅百分比', 'amplitude_percent', 'amplitude_', 'amp_'],
        '涨跌幅': ['percent', '涨跌幅', '涨跌%', 'change_percent', 'pct_chg', 'changepercent', 'chg_pct', 'pctchange'],
        '涨跌额': ['change', '涨跌额', '价格变动', 'price_change', 'change_', 'chg_', 'pricechg'],
        '换手率': ['turnover_rate', '换手率', 'turnover_ratio', 'turnover', 'turnoverrate', 'turnover_rate_']
    }
    
    # 创建新的列名映射
    new_columns = {}
    for standard_name, variants in column_variants.items():
        for variant in variants:
            if variant in df.columns and variant not in new_columns:
                new_columns[variant] = standard_name
    
    # 重命名列
    df = df.rename(columns=new_columns)
    
    logger.info(f"✅ 标准化后的列名: {list(df.columns)}")
    return df

def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    确保DataFrame包含所有必需的交易数据列，缺失的列用默认值填充
    
    Args:
        df: 原始DataFrame
        
    Returns:
        pd.DataFrame: 包含所有必需列的DataFrame
    """
    if df.empty:
        return df
    
    # 定义数据源必需列（基础交易数据）
    data_source_required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
    
    # 检查必需列是否存在
    missing_columns = [col for col in data_source_required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"❌ 数据源缺少必需列：{', '.join(missing_columns)}，无法继续")
        return pd.DataFrame()  # 必需列缺失，返回空DataFrame
    
    # 定义可计算的衍生列
    derived_columns = ["成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
    
    # 检查衍生列是否存在
    missing_derived_columns = [col for col in derived_columns if col not in df.columns]
    
    if missing_derived_columns:
        logger.warning(f"⚠️ 数据源缺少可计算列：{', '.join(missing_derived_columns)}，将尝试计算")
        
        # 为缺失的衍生列计算值
        for col in missing_derived_columns:
            try:
                if col == '成交额':
                    # 如果有成交量，尝试估算成交额（简单估算：成交量 * 收盘价 * 100）
                    if '成交量' in df.columns and '收盘' in df.columns:
                        # 计算出的成交额单位是元，转换为万元
                        df['成交额'] = (df['成交量'] * df['收盘'] * 100 / 10000).round(2)
                    else:
                        df['成交额'] = 0.0
                elif col == '振幅':
                    # 振幅 = (最高 - 最低) / 前收盘 * 100%
                    if '最高' in df.columns and '最低' in df.columns and '收盘' in df.columns:
                        # 假设前收盘价是前一天的收盘价
                        df['前收盘'] = df['收盘'].shift(1)
                        df['振幅'] = ((df['最高'] - df['最低']) / df['前收盘'] * 100).round(2).fillna(0)
                        df = df.drop(columns=['前收盘'])
                    else:
                        df['振幅'] = 0.0
                elif col == '涨跌幅':
                    # 涨跌幅 = (收盘 - 前收盘) / 前收盘 * 100%
                    if '收盘' in df.columns:
                        df['前收盘'] = df['收盘'].shift(1)
                        df['涨跌幅'] = ((df['收盘'] - df['前收盘']) / df['前收盘'] * 100).round(2).fillna(0)
                        df = df.drop(columns=['前收盘'])
                    else:
                        df['涨跌幅'] = 0.0
                elif col == '涨跌额':
                    # 涨跌额 = 收盘 - 前收盘
                    if '收盘' in df.columns:
                        df['前收盘'] = df['收盘'].shift(1)
                        df['涨跌额'] = (df['收盘'] - df['前收盘']).round(4).fillna(0)
                        df = df.drop(columns=['前收盘'])
                    else:
                        df['涨跌额'] = 0.0
                elif col == '换手率':
                    # 换手率 = 成交量 / 流通股本 * 100%
                    # 由于不知道流通股本，暂时用0填充
                    df['换手率'] = 0.0
            except Exception as e:
                logger.error(f"计算列 {col} 时发生错误: {str(e)}", exc_info=True)
                df[col] = 0.0
    
    return df

def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    数据清洗和格式化
    
    Args:
        df: 原始DataFrame
        
    Returns:
        pd.DataFrame: 清洗后的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 日期格式转换
        if "日期" in df.columns:
            # 严格使用北京时间
            df.loc[:, "日期"] = pd.to_datetime(df["日期"], errors='coerce').dt.tz_localize(Config.UTC_TIMEZONE, errors='ignore')
            df.loc[:, "日期"] = df["日期"].dt.tz_convert(Config.BEIJING_TIMEZONE)
            df.loc[:, "日期"] = df["日期"].dt.date
            
            # 确保日期列是字符串格式（YYYY-MM-DD）
            df.loc[:, "日期"] = df["日期"].apply(lambda x: x.strftime("%Y-%m-%d") if not pd.isna(x) else "")
        
        # 数值列转换
        numeric_cols = ["开盘", "最高", "最低", "收盘", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
        for col in numeric_cols:
            if col in df.columns:
                try:
                    # 创建副本以避免SettingWithCopyWarning
                    df = df.copy(deep=True)
                    
                    # 处理可能的字符串值（如"-"）
                    if df[col].dtype == 'object':
                        df.loc[:, col] = df[col].replace('-', '0')
                    
                    # 尝试转换为数值类型
                    df.loc[:, col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
                    
                    # 特殊处理：涨跌幅和振幅保留2位小数
                    if col in ["涨跌幅", "振幅"]:
                        df.loc[:, col] = df[col].round(2)
                    # 其他数值列根据需要保留小数位
                    elif col in ["开盘", "最高", "最低", "收盘"]:
                        df.loc[:, col] = df[col].round(4)
                    elif col in ["成交额"]:
                        # 修复：将成交额从元转换为万元
                        df.loc[:, col] = df[col] / 10000
                        df.loc[:, col] = df[col].round(2)  # 保留2位小数
                except Exception as e:
                    logger.error(f"转换列 {col} 为数值类型时出错: {str(e)}", exc_info=True)
                    df.loc[:, col] = 0.0
        
        # 处理重复数据
        if "日期" in df.columns:
            df = df.drop_duplicates(subset=["日期"], keep="last")
            df = df.sort_values("日期", ascending=False)
        
        logger.debug("数据清洗和格式化完成")
        return df
    except Exception as e:
        logger.error(f"数据清洗过程中发生错误: {str(e)}", exc_info=True)
        return df

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
