#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块
使用AkShare接口获取ETF日线数据
简洁高效，只关注数据获取本身
"""

import akshare as ak
import pandas as pd
import logging
import time
from datetime import datetime, date
from config import Config
from retrying import retry

# 初始化日志
logger = logging.getLogger(__name__)

# 重试配置
MAX_RETRY_ATTEMPTS = 3
RETRY_WAIT_FIXED = 2000

def retry_if_akshare_error(exception: Exception) -> bool:
    """
    重试条件：AkShare相关错误
    """
    from requests.exceptions import ConnectionError, Timeout
    return isinstance(exception, (ConnectionError, Timeout, OSError))

@retry(
    stop_max_attempt_number=MAX_RETRY_ATTEMPTS,
    wait_fixed=RETRY_WAIT_FIXED,
    retry_on_exception=retry_if_akshare_error
)
def crawl_etf_daily_akshare(etf_code: str, start_date: str, end_date: str, is_first_crawl: bool = False) -> pd.DataFrame:
    """
    使用AkShare爬取ETF日线数据
    成功则返回DataFrame，失败则返回空DataFrame
    """
    try:
        # 直接爬取ETF日线数据
        df = ak.fund_etf_hist_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )
        
        # 只检查基本有效性
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()
        
        # 确保返回数据
        return df
        
    except Exception as e:
        logger.debug(f"ETF {etf_code} 通过AkShare获取失败: {str(e)}")
        return pd.DataFrame()

def get_etf_name(etf_code: str) -> str:
    """
    获取ETF名称
    """
    try:
        # 尝试从all_etfs.csv获取ETF名称
        all_etfs_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
        if os.path.exists(all_etfs_file):
            all_etfs = pd.read_csv(all_etfs_file)
            etf_row = all_etfs[all_etfs["ETF代码"] == etf_code]
            if not etf_row.empty:
                return etf_row["ETF名称"].values[0]
        
        # 尝试通过API获取
        df = ak.fund_etf_spot_em()
        if not df.empty and "代码" in df.columns and "名称" in df.columns:
            etf_data = df[df["代码"] == etf_code]
            if not etf_data.empty:
                return etf_data["名称"].values[0]
        
        return "未知ETF"
    except Exception:
        return "未知ETF"

def try_fund_etf_spot_em(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    使用fund_etf_spot_em接口获取ETF实时数据
    """
    try:
        df = ak.fund_etf_spot_em()
        if not df.empty and "代码" in df.columns:
            df = df[df["代码"] == etf_code]
            if not df.empty:
                return df
    except Exception:
        pass
    return pd.DataFrame()

def try_fund_etf_fund_daily_em(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    使用fund_etf_fund_daily_em接口获取ETF历史数据
    """
    try:
        df = ak.fund_etf_fund_daily_em()
        if not df.empty and "基金代码" in df.columns:
            df = df[df["基金代码"] == etf_code]
            if not df.empty:
                return df
    except Exception:
        pass
    return pd.DataFrame()

def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    确保DataFrame包含必需列
    由init.py处理，这里只返回原始数据
    """
    return df
