# sina_crawler.py
import akshare as ak
import pandas as pd
import logging
import time
import re
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
from config import Config
from retrying import retry

# 初始化日志
logger = logging.getLogger(__name__)

# 重试配置
MAX_RETRY_ATTEMPTS = 3
RETRY_WAIT_FIXED = 2000  # 毫秒
RETRY_WAIT_EXPONENTIAL_MAX = 10000  # 毫秒

def empty_result_check(result: pd.DataFrame) -> bool:
    """
    检查新浪接口返回结果是否为空
    :param result: 新浪接口返回的DataFrame
    :return: 如果结果为空返回True，否则返回False
    """
    return result is None or result.empty

def retry_if_sina_error(exception: Exception) -> bool:
    """
    重试条件：新浪接口相关错误
    :param exception: 异常对象
    :return: 如果是新浪接口错误返回True，否则返回False
    """
    return isinstance(exception, (ValueError, ConnectionError, TimeoutError))

@retry(
    stop_max_attempt_number=MAX_RETRY_ATTEMPTS,
    wait_fixed=RETRY_WAIT_FIXED,
    retry_on_result=empty_result_check,
    retry_on_exception=retry_if_sina_error
)
def crawl_etf_daily_sina(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    用新浪接口爬取ETF日线数据
    :param etf_code: ETF代码 (6位数字)
    :param start_date: 开始日期 (YYYY-MM-DD)
    :param end_date: 结束日期 (YYYY-MM-DD)
    :return: 原始DataFrame
    """
    try:
        # 验证输入参数
        if not validate_etf_code(etf_code):
            return pd.DataFrame()
            
        if not validate_date_range(start_date, end_date):
            return pd.DataFrame()
        
        # 添加市场前缀
        symbol = get_symbol_with_market_prefix(etf_code)
        
        # 使用新浪接口
        df = ak.fund_etf_hist_sina(symbol=symbol)
        
        # 检查结果
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()
        
        # 记录API返回的列名
        logger.info(f"ETF {etf_code} 新浪接口返回的列名: {list(df.columns)}")
        
        # 按日期过滤
        df = filter_by_date_range(df, start_date, end_date)
        
        return df
    
    except Exception as e:
        logger.error(f"新浪接口爬取{etf_code}失败：{str(e)}")
        time.sleep(2)
        raise

def get_symbol_with_market_prefix(etf_code: str) -> str:
    """
    根据ETF代码获取带市场前缀的代码
    :param etf_code: ETF代码
    :return: 带市场前缀的代码
    """
    if etf_code.startswith('5') or etf_code.startswith('6') or etf_code.startswith('9'):
        return f"sh{etf_code}"
    else:
        return f"sz{etf_code}"

def filter_by_date_range(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """
    按日期范围过滤数据
    :param df: 原始DataFrame
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 过滤后的DataFrame
    """
    if df.empty or "日期" not in df.columns:
        return df
    
    # 确保日期列为字符串格式
    df["日期"] = df["日期"].astype(str)
    
    # 过滤日期范围
    df = df[(df["日期"] >= start_date) & (df["日期"] <= end_date)]
    return df

def validate_date_range(start_date: str, end_date: str) -> bool:
    """
    验证日期范围是否有效
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 如果日期范围有效返回True，否则返回False
    """
    try:
        # 检查日期格式
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
        
        # 检查日期顺序
        return start_date <= end_date
    except ValueError:
        return False

def validate_etf_code(etf_code: str) -> bool:
    """
    验证ETF代码是否有效
    :param etf_code: ETF代码
    :return: 如果ETF代码有效返回True，否则返回False
    """
    if not etf_code or not isinstance(etf_code, str):
        return False
    
    # 移除可能的前缀
    clean_code = re.sub(r"^(sh|sz)?", "", etf_code)
    
    # 检查是否为6位数字
    return re.match(r"^\d{6}$", clean_code) is not None

# 模块初始化
logger.info("新浪爬虫模块初始化完成")
