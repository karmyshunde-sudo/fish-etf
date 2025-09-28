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
from datetime import datetime, timedelta, date  # 修复：添加date导入
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
MAX_RETRY_ATTEMPTS = 5  # 增加重试次数，从3增加到5
RETRY_WAIT_FIXED = 3000  # 增加等待时间，从2000毫秒增加到3000毫秒
RETRY_WAIT_EXPONENTIAL_MAX = 15000  # 增加最大等待时间，从10000毫秒增加到15000毫秒

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
    # 【关键修复】空结果不触发重试，避免RetryError
    # 空数据是正常情况（如周末无交易），不应重试
    if result is None or result.empty:
        return False  # 不重试空结果
    
    return False

def retry_if_akshare_error(exception: Exception) -> bool:
    """
    重试条件：AkShare相关错误
    
    Args:
        exception: 异常对象
        
    Returns:
        bool: 如果是AkShare错误返回True，否则返回False
    """
    # 扩展异常类型，包括requests库的网络错误
    from requests.exceptions import ConnectionError, Timeout
    return isinstance(exception, (ValueError, ConnectionError, Timeout, OSError))
        
@retry(
    stop_max_attempt_number=MAX_RETRY_ATTEMPTS,
    wait_fixed=RETRY_WAIT_FIXED,
    retry_on_result=empty_result_check,
    retry_on_exception=retry_if_akshare_error
)
def crawl_etf_daily_akshare(etf_code: str, start_date: str, end_date: str, is_first_crawl: bool = False) -> pd.DataFrame:
    """
    使用AkShare爬取ETF日线数据
    
    Args:
        etf_code: ETF代码
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        is_first_crawl: 是否首次爬取
    
    Returns:
        pd.DataFrame: ETF日线数据
    """
    try:
        logger.debug(f"开始爬取ETF {etf_code} 日线数据: {start_date} ~ {end_date}")
        
        # 爬取ETF日线数据
        df = ak.fund_etf_hist_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq" if is_first_crawl else ""
        )
        
        # 【关键修复】添加类型检查，确保 df 是 DataFrame
        if df is None:
            logger.warning(f"ETF {etf_code} API返回None，跳过")
            return pd.DataFrame()
        
        # 【关键修复】检查 df 是否为 DataFrame 类型
        if not isinstance(df, pd.DataFrame):
            logger.error(f"ETF {etf_code} 返回的数据类型错误: {type(df)}，跳过")
            return pd.DataFrame()
        
        # 【关键修复】检查 DataFrame 是否为空
        if df.empty:
            logger.warning(f"ETF {etf_code} 无有效数据（API返回空DataFrame）")
            return pd.DataFrame()
        
        # 【关键修复】记录实际获取的数据条数
        data_count = len(df)
        logger.info(f"ETF {etf_code} 获取到 {data_count} 条有效数据")
        
        # 【关键修复】检查数据量是否足够
        if data_count < 5 and is_first_crawl:
            logger.warning(f"ETF {etf_code} 数据量较少 ({data_count}条)，可能是新上市ETF")
        
        # 【关键修复】检查日期范围是否合理
        if not df.empty and "日期" in df.columns:
            first_date = df["日期"].min()
            last_date = df["日期"].max()
            logger.debug(f"ETF {etf_code} 数据日期范围: {first_date} 至 {last_date}")
        
        # 【关键修复】使用正确的数据清洗函数
        df = clean_and_format_data(df)
        
        return df
        
    except Exception as e:
        logger.error(f"爬取ETF {etf_code} 失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def try_fund_etf_spot_em(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """使用fund_etf_spot_em接口获取ETF实时数据（优先使用）"""
    try:
        logger.info(f"尝试使用fund_etf_spot_em接口获取ETF {etf_code} 数据")
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

def try_fund_etf_fund_daily_em(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """使用fund_etf_fund_daily_em接口获取ETF历史数据（备用）"""
    try:
        logger.info(f"尝试使用fund_etf_fund_daily_em接口获取ETF {etf_code} 数据")
        df = ak.fund_etf_fund_daily_em()
        
        if not df.empty:
            logger.info(f"fund_etf_fund_daily_em 接口返回的原始列名: {list(df.columns)}")
            
            # 过滤指定ETF
            df = df[df["基金代码"] == etf_code]
            
            if not df.empty:
                # 处理包含日期的列名
                date_columns = [col for col in df.columns if col.startswith(('20', '21'))]
                if date_columns:
                    # 创建新的DataFrame用于存储结果
                    result_df = pd.DataFrame()
                    
                    for date_col in date_columns:
                        # 提取日期部分
                        date_str = date_col.split("-")[0]
                        date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                        
                        # 提取相关数据
                        daily_data = {
                            "日期": date,
                            "单位净值": df[f"{date_col}-单位净值"].values[0] if f"{date_col}-单位净值" in df else None,
                            "累计净值": df[f"{date_col}-累计净值"].values[0] if f"{date_col}-累计净值" in df else None,
                            "增长率": df["增长率"].values[0] if "增长率" in df else None,
                            "市价": df["市价"].values[0] if "市价" in df else None,
                            "折价率": df["折价率"].values[0] if "折价率" in df else None
                        }
                        
                        # 添加到结果DataFrame
                        result_df = pd.concat([result_df, pd.DataFrame([daily_data])], ignore_index=True)
                    
                    # 标准化列名
                    result_df = result_df.rename(columns={
                        "单位净值": "收盘",
                        "折价率": "折溢价率"
                    })
                    
                    # 确保日期格式
                    if "日期" in result_df.columns:
                        result_df["日期"] = pd.to_datetime(result_df["日期"]).dt.strftime("%Y-%m-%d")
                    
                    # 添加基础列
                    result_df["ETF代码"] = etf_code
                    result_df["ETF名称"] = get_etf_name(etf_code)
                    
                    # 仅保留在指定日期范围内的数据
                    if "日期" in result_df.columns:
                        mask = (result_df["日期"] >= start_date) & (result_df["日期"] <= end_date)
                        result_df = result_df.loc[mask]
                    
                    # 添加必要衍生字段
                    if "收盘" in result_df.columns and "增长率" in result_df.columns:
                        result_df["涨跌幅"] = result_df["增长率"]
                    
                    return result_df
    except Exception as e:
        logger.debug(f"fund_etf_fund_daily_em接口失败: {str(e)}")
    
    return pd.DataFrame()

def get_etf_name(etf_code: str) -> str:
    """获取ETF名称"""
    try:
        # 尝试从all_etfs.csv获取ETF名称
        all_etfs_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
        if os.path.exists(all_etfs_file):
            all_etfs = pd.read_csv(all_etfs_file)
            etf_row = all_etfs[all_etfs["ETF代码"] == etf_code]
            if not etf_row.empty:
                return etf_row["ETF名称"].values[0]
        
        # 如果从文件获取失败，尝试通过API获取
        df = ak.fund_etf_spot_em()
        if not df.empty and "代码" in df.columns and "名称" in df.columns:
            etf_data = df[df["代码"] == etf_code]
            if not etf_data.empty:
                return etf_data["名称"].values[0]
        
        return "未知ETF"
    except Exception as e:
        logger.error(f"获取ETF名称失败: {str(e)}", exc_info=True)
        return "未知ETF"

def get_symbol_with_market_prefix(etf_code: str) -> str:
    """
    根据ETF代码获取带市场前缀的代码（保留但不再使用）
    
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
    确保DataFrame包含所有必需的交易数据列，缺失的列用默认值填充
    """
    if df.empty:
        return df
    
    # 定义基础必需列
    required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"❌ 数据源缺少必需列：{', '.join(missing_columns)}")
        return df
    
    # 检查关键字段
    if "折溢价率" not in df.columns:
        logger.warning("⚠️ 数据源不提供折溢价率列，将尝试通过净值或IOPV计算")
    
    # 检查衍生列
    derived_columns = ["成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
    missing_derived_columns = [col for col in derived_columns if col not in df.columns]
    
    if missing_derived_columns:
        logger.info(f"ℹ️ 数据源缺少可计算列：{', '.join(missing_derived_columns)}，将尝试计算")
    
    return df

def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    清洗并格式化数据
    """
    try:
        if df.empty:
            return df
            
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 处理日期列
        if "日期" in df.columns and not df.empty:
            # 确保日期列是datetime类型
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
            # 删除无效日期
            df = df.dropna(subset=["日期"])
        
        # 【关键修复】正确返回DataFrame，而不是date对象
        return df
        
    except Exception as e:
        logger.error(f"清洗数据失败: {str(e)}", exc_info=True)
        return df  # 即使出错也返回DataFrame
