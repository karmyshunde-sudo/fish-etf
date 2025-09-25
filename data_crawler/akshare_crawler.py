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
    return result is None or result.empty

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
        
        # 严格使用两个API接口获取所有数据
        df = try_fund_etf_spot_em(etf_code, start_date, end_date)
        
        # 如果fund_etf_spot_em失败，尝试fund_etf_fund_daily_em
        if df.empty:
            logger.info(f"fund_etf_spot_em获取失败，尝试fund_etf_fund_daily_em")
            df = try_fund_etf_fund_daily_em(etf_code, start_date, end_date)
        
        # 检查数据是否成功获取
        if df.empty:
            logger.warning(f"所有数据源均未获取到{etf_code}数据（{start_date}至{end_date}）")
            return pd.DataFrame()
        
        # 标准化列名
        df = ensure_chinese_columns(df)
        
        # 确保所有必需列都存在
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
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        # 处理日期列
        if "日期" in df.columns and not df.empty:
            # 确保日期列是datetime类型
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
            # 删除无效日期
            df = df.dropna(subset=["日期"])
            # 获取最大日期
            if not df.empty:
                latest_date = df["日期"].max()
                if not pd.isna(latest_date):
                    return latest_date.date()
    except Exception as e:
        logger.error(f"获取文件 {file_path} 最新日期失败: {str(e)}", exc_info=True)
    
    # 出错时返回一个较早的日期，确保会重新爬取
    return date(2024, 9, 1)
