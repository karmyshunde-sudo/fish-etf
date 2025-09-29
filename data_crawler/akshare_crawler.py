#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块
使用AkShare接口获取ETF日线数据
确保与仓库中的数据结构完全匹配
"""

import akshare as ak
import pandas as pd
import logging
import time
import os
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
MAX_RETRY_ATTEMPTS = 5  # 增加重试次数
RETRY_WAIT_FIXED = 3000  # 增加等待时间
RETRY_WAIT_EXPONENTIAL_MAX = 15000  # 增加最大等待时间

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
    if result is None or result.empty:
        return False
    return False

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
    """
    使用AkShare爬取ETF日线数据
    确保与仓库中的数据结构完全匹配
    """
    try:
        logger.debug(f"开始爬取ETF {etf_code} 日线数据: {start_date} ~ {end_date}")
        
        # 爬取ETF日线数据
        df = ak.fund_etf_hist_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )
        
        # 【关键添加】记录API返回的实际列名
        if isinstance(df, pd.DataFrame):
            logger.info(f"ETF {etf_code} API返回的列名: {list(df.columns)}")
            logger.info(f"ETF {etf_code} API返回的数据示例:")
            if not df.empty:
                logger.info(f"第一条数据: {df.iloc[0].to_dict()}")
                if len(df) > 1:
                    logger.info(f"第二条数据: {df.iloc[1].to_dict()}")
        else:
            logger.warning(f"ETF {etf_code} API返回的数据类型不是DataFrame，类型: {type(df)}")
            return pd.DataFrame()
        
        # 检查结果是否为空
        if df is None:
            logger.warning(f"ETF {etf_code} API返回None，跳过")
            return pd.DataFrame()
        
        if not isinstance(df, pd.DataFrame):
            logger.error(f"ETF {etf_code} 返回的数据类型错误: {type(df)}，跳过")
            return pd.DataFrame()
        
        if df.empty:
            logger.warning(f"ETF {etf_code} 无有效数据（API返回空DataFrame）")
            return pd.DataFrame()
        
        # 记录实际获取的数据条数
        data_count = len(df)
        logger.info(f"ETF {etf_code} 获取到 {data_count} 条原始数据")
        
        # 确保所有必需列都存在
        required_columns = [
            '日期', '开盘', '最高', '最低', '收盘', '成交量', 
            '成交额', '振幅', '涨跌幅', '涨跌额', '换手率', 
            'ETF代码', 'ETF名称', '爬取时间', '折溢价率'
        ]
        
        # 添加缺失的列
        for col in required_columns:
            if col not in df.columns:
                if col == 'ETF代码':
                    df[col] = etf_code
                elif col == 'ETF名称':
                    df[col] = get_etf_name(etf_code)
                elif col == '爬取时间':
                    df[col] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                else:
                    # 对于其他列，用NaN填充
                    df[col] = pd.NA
        
        # 确保日期格式正确
        if '日期' in df.columns:
            # 如果日期列是字符串类型
            if df['日期'].dtype == object:
                try:
                    # 尝试将日期转换为标准格式
                    df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.strftime("%Y-%m-%d")
                except Exception as e:
                    logger.error(f"日期格式转换失败: {str(e)}")
        
        # 确保数值列是数值类型
        numeric_columns = [
            '开盘', '最高', '最低', '收盘', '成交量', '成交额',
            '振幅', '涨跌幅', '涨跌额', '换手率', '折溢价率'
        ]
        for col in numeric_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except Exception as e:
                    logger.error(f"{col} 列转换为数值类型失败: {str(e)}")
        
        # 添加数据有效性检查
        if '收盘' in df.columns:
            valid_data = df[df['收盘'].notna()]
            if len(valid_data) == 0:
                logger.warning(f"ETF {etf_code} 没有有效价格数据")
                return df
        else:
            valid_data = df
            logger.warning(f"ETF {etf_code} 数据中没有'收盘'列，无法过滤无效数据")
        
        logger.info(f"ETF {etf_code} 有效数据: {len(valid_data)} 条")
        return valid_data
        
    except Exception as e:
        logger.error(f"爬取ETF {etf_code} 失败: {str(e)}", exc_info=True)
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

def try_fund_etf_spot_em(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """使用fund_etf_spot_em接口获取ETF实时数据（备用）"""
    try:
        logger.info(f"尝试使用fund_etf_spot_em接口获取ETF {etf_code} 数据")
        df = ak.fund_etf_spot_em()
        
        if not df.empty:
            # 【关键添加】记录API返回的实际列名
            logger.info(f"fund_etf_spot_em API返回的列名: {list(df.columns)}")
            if not df.empty:
                logger.info(f"数据示例: {df.iloc[0].to_dict()}")
            
            # 过滤指定ETF
            df = df[df["代码"] == etf_code]
            
            if not df.empty:
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
            # 【关键添加】记录API返回的实际列名
            logger.info(f"fund_etf_fund_daily_em API返回的列名: {list(df.columns)}")
            if not df.empty:
                logger.info(f"数据示例: {df.iloc[0].to_dict()}")
            
            # 过滤指定ETF
            df = df[df["基金代码"] == etf_code]
            
            if not df.empty:
                # 处理包含日期的列名
                date_columns = [col for col in df.columns if col.startswith(('20', '21'))]
                if date_columns:
                    result_df = pd.DataFrame()
                    
                    for date_col in date_columns:
                        date_str = date_col.split("-")[0]
                        date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                        
                        daily_data = {
                            "日期": date,
                            "单位净值": df[f"{date_col}-单位净值"].values[0] if f"{date_col}-单位净值" in df else None,
                            "累计净值": df[f"{date_col}-累计净值"].values[0] if f"{date_col}-累计净值" in df else None,
                            "增长率": df["增长率"].values[0] if "增长率" in df else None,
                            "市价": df["市价"].values[0] if "市价" in df else None,
                            "折价率": df["折价率"].values[0] if "折价率" in df else None
                        }
                        
                        result_df = pd.concat([result_df, pd.DataFrame([daily_data])], ignore_index=True)
                    
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
                    if "单位净值" in result_df.columns and "增长率" in result_df.columns:
                        result_df["收盘"] = result_df["单位净值"]
                        result_df["涨跌幅"] = result_df["增长率"]
                    
                    return result_df
    except Exception as e:
        logger.debug(f"fund_etf_fund_daily_em接口失败: {str(e)}")
    
    return pd.DataFrame()

def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    确保DataFrame包含所有必需的交易数据列，缺失的列用默认值填充
    """
    if df.empty:
        return df
    
    required_columns = [
        '日期', '开盘', '最高', '最低', '收盘', '成交量', 
        '成交额', '振幅', '涨跌幅', '涨跌额', '换手率', 
        'ETF代码', 'ETF名称', '爬取时间', '折溢价率'
    ]
    
    # 检查必需列
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"❌ 数据源缺少必需列：{', '.join(missing_columns)}")
    
    # 检查关键字段
    if "折溢价率" not in df.columns:
        logger.warning("⚠️ 数据源不提供折溢价率列，将尝试通过净值或IOPV计算")
    
    return df
