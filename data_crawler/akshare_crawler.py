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
        
        # ========== 以下是关键修复 ==========
        # 1. 尝试多种AkShare接口（优先使用旧代码的详细逻辑）
        df = try_multiple_akshare_interfaces(etf_code, start_date, end_date)
        
        # 2. 如果旧代码逻辑失败，尝试新代码中的特殊处理逻辑
        if df.empty:
            logger.info(f"旧代码逻辑获取ETF {etf_code} 数据失败，尝试新代码逻辑")
            df = try_fund_etf_hist_em_with_net_value(etf_code, start_date, end_date)
            
            # 如果仍然为空，尝试恒生指数特殊处理
            if df.empty and etf_code == "513750":
                logger.info(f"尝试特殊处理ETF {etf_code}")
                df = try_fund_etf_spot_em_with_premium(etf_code)
        
        # 3. 如果AkShare接口全部失败，尝试yfinance作为备选（仅限美股指数）
        if df.empty and etf_code.startswith('^'):
            logger.info(f"尝试通过yfinance获取美股指数 {etf_code} 数据")
            df = fetch_us_index_from_yfinance(etf_code, start_date, end_date)
        
        # 4. 如果是A股ETF，尝试使用指数数据作为最后备选
        if df.empty and etf_code.startswith(("51", "159", "50", "510", "512", "513", "515", "518")):
            logger.info(f"尝试通过指数数据获取ETF {etf_code} 数据作为最后备选")
            df = try_index_data_as_etf_backup(etf_code, start_date, end_date)
        
        if df.empty:
            logger.warning(f"所有数据源均未获取到{etf_code}数据（{start_date}至{end_date}）")
            return pd.DataFrame()
        
        # 记录返回的列名，用于调试
        logger.info(f"📊 数据源返回的原始列名: {list(df.columns)}")
        
        # 标准化列名 - 优先使用旧代码中的详细列名映射
        df = ensure_chinese_columns(df)
        
        # 确保所有必需列都存在 - 使用旧代码中的完整验证逻辑
        df = ensure_required_columns(df)
        
        # 数据清洗：去重、格式转换
        df = clean_and_format_data(df)
        
        # 首次爬取时限制数据量为1年（365天）
        if is_first_crawl:
            df = limit_to_one_year_data(df, end_date)
        
        logger.info(f"成功获取ETF {etf_code} 数据，共{len(df)}条记录")
        return df
    except Exception as e:
        logger.error(f"爬取ETF {etf_code} 失败: {str(e)}", exc_info=True)
        raise  # 触发重试

def try_multiple_akshare_interfaces(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """尝试多种AkShare接口获取ETF数据"""
    logger.info(f"尝试获取ETF {etf_code} 数据，最多 3 种接口")
    
    # 接口1: fund_etf_hist_em (提供IOPV和折溢价率)
    try:
        logger.info(f"尝试使用fund_etf_hist_em接口获取ETF {etf_code} 数据")
        df = ak.fund_etf_hist_em(symbol=etf_code, period="daily", 
                                start_date=start_date, end_date=end_date, adjust="")
        
        if not df.empty:
            logger.info(f"第1种接口（fund_etf_hist_em）成功获取ETF {etf_code} 数据")
            logger.info(f"📊 fund_etf_hist_em 接口返回的原始列名: {list(df.columns)}")
            
            # 标准化列名
            if '净值日期' in df.columns:
                df = df.rename(columns={
                    '净值日期': '日期',
                    '单位净值': 'IOPV',
                    '折价率': '折溢价率'
                })
            elif '净值估算日期' in df.columns:
                df = df.rename(columns={
                    '净值估算日期': '日期',
                    '单位净值估算': 'IOPV',
                    '折价率估算': '折溢价率'
                })
            
            # 确保日期格式
            if '日期' in df.columns:
                df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
            
            return df
    except Exception as e:
        logger.debug(f"fund_etf_hist_em接口失败: {str(e)}")
    
    # 接口2: stock_zh_index_daily_js (提供基础数据)
    try:
        logger.info(f"尝试使用stock_zh_index_daily_js接口获取ETF {etf_code} 数据")
        # 关键修复：为510300等上交所ETF添加"sh"前缀
        symbol = f"sh{etf_code}" if etf_code.startswith('5') else f"sz{etf_code}"
        df = ak.stock_zh_index_daily_js(symbol=symbol)
        
        if not df.empty:
            logger.info(f"第2种接口（stock_zh_index_daily_js）成功获取ETF {etf_code} 数据")
            logger.info(f"📊 stock_zh_index_daily_js 接口返回的原始列名: {list(df.columns)}")
            
            # 标准化列名
            df = df.rename(columns={
                'date': '日期',
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量'
            })
            
            # 尝试计算折溢价率（如果可能）
            if 'IOPV' in df.columns and '收盘' in df.columns:
                df['折溢价率'] = (df['收盘'] - df['IOPV']) / df['IOPV'] * 100
            
            return df
    except Exception as e:
        logger.debug(f"stock_zh_index_daily_js接口失败: {str(e)}")
    
    # 接口3: fund_etf_hist_sina (基础数据)
    try:
        logger.info(f"尝试使用fund_etf_hist_sina接口获取ETF {etf_code} 数据")
        # 关键修复：使用正确的市场前缀
        symbol = get_symbol_with_market_prefix(etf_code)
        df = ak.fund_etf_hist_sina(symbol=symbol)
        
        if not df.empty:
            logger.info(f"第3种接口（fund_etf_hist_sina）成功获取ETF {etf_code} 数据")
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
            
            # 日期过滤（关键修复）
            if '日期' in df.columns:
                df['日期'] = pd.to_datetime(df['日期'])
                mask = (df['日期'] >= pd.to_datetime(start_date)) & (df['日期'] <= pd.to_datetime(end_date))
                df = df.loc[mask]
        
        return df
    except Exception as e:
        logger.warning(f"fund_etf_hist_sina 接口调用失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

    logger.warning(f"所有AkShare接口均未获取到ETF {etf_code} 数据")
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

def fetch_us_index_from_yfinance(index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    使用YFinance获取美股指数数据（最可靠的替代方案）
    
    Args:
        index_code: 指数代码（如"^NDX"）
        start_date: 开始日期（YYYYMMDD）
        end_date: 结束日期（YYYYMMDD）
        
    Returns:
        pd.DataFrame: 指数日线数据
    """
    try:
        # 转换日期格式
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        
        # 指数代码映射
        symbol_map = {
            '^NDX': '^NDX',  # 纳斯达克100
            '^DJI': '^DJI',  # 道琼斯工业指数
            '^GSPC': '^GSPC' # 标准普尔500
        }
        
        symbol = symbol_map.get(index_code, index_code)
        
        # 检查是否已安装yfinance
        try:
            import yfinance as yf
        except ImportError:
            logger.error("需要安装yfinance: pip install yfinance")
            return pd.DataFrame()
        
        # 获取数据
        df = yf.download(symbol, start=start_dt, end=end_dt)
        
        if df.empty:
            logger.warning(f"通过yfinance获取{index_code}数据为空")
            return pd.DataFrame()
        
        # 标准化列名
        df = df.reset_index()
        df = df.rename(columns={
            'Date': '日期',
            'Open': '开盘',
            'High': '最高',
            'Low': '最低',
            'Close': '收盘',
            'Volume': '成交量',
            'Adj Close': '复权收盘'
        })
        
        # 确保日期格式正确
        df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        
        logger.info(f"成功通过yfinance获取{index_code}数据，共{len(df)}条记录")
        return df
    
    except Exception as e:
        logger.error(f"通过yfinance获取{index_code}失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def try_fund_etf_hist_em_with_net_value(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    尝试使用 fund_etf_hist_em 接口获取包含净值的数据
    Args:
        etf_code: ETF代码
        start_date: 开始日期
        end_date: 结束日期
    Returns:
        pd.DataFrame: 获取到的DataFrame
    """
    try:
        logger.debug(f"尝试使用 fund_etf_hist_em 接口获取ETF {etf_code} 数据（包含净值）")
        df = ak.fund_etf_hist_em(symbol=etf_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        
        if not df.empty:
            # 记录返回的列名，用于调试
            logger.info(f"📊 fund_etf_hist_em 接口返回的原始列名: {list(df.columns)}")
            
            # 检查是否有净值数据（fund_etf_hist_em 可能返回的净值列）
            net_value_columns = [col for col in df.columns if "净值" in col or "net" in col.lower()]
            if net_value_columns:
                # 选择第一个净值列
                net_value_col = net_value_columns[0]
                df["净值"] = df[net_value_col]
                logger.info(f"✅ fund_etf_hist_em 接口成功获取净值数据（列名: {net_value_col}）")
        
        return df
    except Exception as e:
        logger.warning(f"fund_etf_hist_em 接口调用失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def try_fund_etf_spot_em_with_premium(etf_code: str) -> pd.DataFrame:
    """
    尝试使用 fund_etf_spot_em 接口获取包含折价率的数据（仅最新数据）
    Args:
        etf_code: ETF代码
    Returns:
        pd.DataFrame: 获取到的DataFrame
    """
    try:
        logger.debug(f"尝试使用 fund_etf_spot_em 接口获取ETF {etf_code} 数据（包含折价率）")
        df = ak.fund_etf_spot_em()
        
        if not df.empty:
            # 记录返回的列名，用于调试
            logger.info(f"📊 fund_etf_spot_em 接口返回的原始列名: {list(df.columns)}")
            
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
                
                logger.info("✅ fund_etf_spot_em 接口成功获取折价率数据")
        
        return df
    except Exception as e:
        logger.warning(f"fund_etf_spot_em 接口调用失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def try_index_data_as_etf_backup(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    尝试使用指数数据作为ETF数据的最后备选方案
    Args:
        etf_code: ETF代码
        start_date: 开始日期
        end_date: 结束日期
    Returns:
        pd.DataFrame: 获取到的DataFrame
    """
    try:
        # 宽基ETF与指数的映射关系
        index_mapping = {
            "510300": "000300",  # 沪深300ETF -> 沪深300指数
            "510500": "000905",  # 中证500ETF -> 中证500指数
            "510050": "000016",  # 上证50ETF -> 上证50指数
            "588000": "000688",  # 科创50ETF -> 科创50指数
            "159915": "399006",  # 创业板ETF -> 创业板指数
            "512880": "399975",  # 证券ETF -> 证券公司指数
            "512660": "399967",  # 军工ETF -> 军工指数
            "512400": "399395",  # 有色金属ETF -> 有色金属指数
            "515070": "930713",  # AI产业ETF -> AI产业指数
            "512800": "399965",  # 银行ETF -> 银行指数
            "512890": "399986",  # 环保ETF -> 环保产业指数
            "515220": "930606",  # 红利低波ETF -> 红利低波指数
            "515790": "930972",  # 光伏ETF -> 光伏产业指数
            "159855": "931151",  # 新能源车ETF -> 新能源车指数
            "159995": "399812",  # 通信ETF -> 通信设备指数
            "159928": "399007",  # 消费ETF -> 主要消费指数
            "512690": "930917",  # 港股通50ETF -> 港股通50指数
            "513050": "H30533.CSI",  # 中概互联ETF -> 中证海外中国互联网指数
            "513100": "^NDX",  # 纳指100ETF -> 纳斯达克100指数
            "513500": "H30533.CSI",  # 中概互联ETF -> 中证海外中国互联网指数
            "513400": "HSNDXIT.HI"  # 恒生互联网ETF -> 恒生互联网科技业指数
        }
        
        index_code = index_mapping.get(etf_code)
        if not index_code:
            logger.info(f"ETF {etf_code} 没有对应的指数映射，无法使用指数数据作为备选")
            return pd.DataFrame()
        
        logger.info(f"尝试使用指数 {index_code} 数据作为ETF {etf_code} 的备选数据")
        
        # 根据指数类型使用不同的数据接口
        if index_code.startswith('^'):
            # 美股指数
            return fetch_us_index_from_yfinance(index_code, start_date, end_date)
        
        elif index_code.endswith('.CSI'):
            # 中证系列指数
            index_name = index_code.replace('.CSI', '')
            return ak.index_zh_a_hist(
                symbol=index_name,
                period="daily",
                start_date=start_date,
                end_date=end_date
            )
        
        elif index_code.endswith('.HI'):
            # 恒生系列指数
            index_name = index_code.replace('.HI', '')
            
            # 尝试使用 index_hk_hist 方法
            try:
                df = ak.index_hk_hist(symbol=index_name, period="daily", 
                                     start_date=start_date, end_date=end_date)
                if not df.empty:
                    logger.info(f"📊 index_hk_hist 接口返回的原始列名: {list(df.columns)}")
                    logger.info(f"✅ 通过 index_hk_hist 方法成功获取恒生指数 {index_code} 数据")
                    return df
            except Exception as e:
                logger.warning(f"index_hk_hist 方法失败: {str(e)}")
            
            # 尝试使用 stock_hk_index_hist 方法
            try:
                df = ak.stock_hk_index_hist(symbol=index_name, period="daily", 
                                          start_date=start_date, end_date=end_date)
                if not df.empty:
                    logger.info(f"📊 stock_hk_index_hist 接口返回的原始列名: {list(df.columns)}")
                    logger.info(f"✅ 通过 stock_hk_index_hist 方法成功获取恒生指数 {index_code} 数据")
                    return df
            except Exception as e:
                logger.warning(f"stock_hk_index_hist 方法失败: {str(e)}")
            
            logger.warning(f"无法获取恒生指数 {index_code} 数据")
            return pd.DataFrame()
        
        else:
            # A股指数
            return ak.index_zh_a_hist(
                symbol=index_code,
                period="daily",
                start_date=start_date,
                end_date=end_date
            )
    
    except Exception as e:
        logger.error(f"通过指数数据获取ETF {etf_code} 失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

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
    
    # 定义基础必需列（包含"折溢价率"）
    required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "折溢价率"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"❌ 数据源缺少必需列：{', '.join(missing_columns)}，将尝试修复")
    
    # 1. 优先使用数据源提供的原始折溢价率数据
    if "折溢价率" not in df.columns:
        logger.warning("⚠️ 数据源不提供折溢价率列，将尝试通过净值或IOPV计算")
        
        # 尝试从fund_etf_hist_em获取的净值数据计算
        if "净值" in df.columns and "收盘" in df.columns:
            df["折溢价率"] = ((df["收盘"] - df["净值"]) / df["净值"] * 100).round(2)
            logger.info("✅ 通过净值成功计算折溢价率")
        # 尝试从fund_etf_hist_sina获取的IOPV数据计算
        elif "IOPV" in df.columns and "收盘" in df.columns:
            df["折溢价率"] = ((df["收盘"] - df["IOPV"]) / df["IOPV"] * 100).round(2)
            logger.info("✅ 通过IOPV成功计算折溢价率")
        else:
            logger.error("❌ 无法计算折溢价率，数据不可用")
            # 仍然创建折溢价率列，但用NaN填充
            df["折溢价率"] = float('nan')
    else:
        # 2. 检查原始折溢价率数据是否有效
        if df["折溢价率"].isna().all() or (df["折溢价率"] == 0).all():
            logger.warning("⚠️ 原始折溢价率数据全为0或空值，将尝试重新计算")
            # 尝试从净值重新计算
            if "净值" in df.columns and "收盘" in df.columns:
                df["折溢价率"] = ((df["收盘"] - df["净值"]) / df["净值"] * 100).round(2)
                logger.info("✅ 通过净值重新计算折溢价率")
            # 尝试从IOPV重新计算
            elif "IOPV" in df.columns and "收盘" in df.columns:
                df["折溢价率"] = ((df["收盘"] - df["IOPV"]) / df["IOPV"] * 100).round(2)
                logger.info("✅ 通过IOPV重新计算折溢价率")
            else:
                logger.warning("ℹ️ 无法重新计算折溢价率，保留原始数据（可能全为0）")
    
    # 3. 处理其他衍生列
    derived_columns = ["成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
    missing_derived_columns = [col for col in derived_columns if col not in df.columns]
    
    if missing_derived_columns:
        logger.info(f"ℹ️ 数据源缺少可计算列：{', '.join(missing_derived_columns)}，将尝试计算")
        
        for col in missing_derived_columns:
            try:
                if col == '成交额':
                    # 如果有成交量和收盘价，可以计算成交额
                    if '成交量' in df.columns and '收盘' in df.columns:
                        # 注意：A股成交量单位是"手"（1手=100股）
                        df['成交额'] = (df['成交量'] * df['收盘'] * 100 / 10000).round(2)
                        logger.info("✅ 成功计算成交额")
                    else:
                        logger.warning("⚠️ 无法计算成交额，缺少必要数据")
                
                elif col == '振幅':
                    # 振幅 = (最高 - 最低) / 前收盘 * 100%
                    if '最高' in df.columns and '最低' in df.columns and '收盘' in df.columns:
                        # 使用前一天收盘价作为前收盘
                        df['前收盘'] = df['收盘'].shift(1)
                        # 处理第一天（没有前收盘）的情况
                        df['前收盘'] = df['前收盘'].fillna(df['开盘'])
                        df['振幅'] = ((df['最高'] - df['最低']) / df['前收盘'] * 100).round(2)
                        df = df.drop(columns=['前收盘'])
                        logger.info("✅ 成功计算振幅")
                    else:
                        logger.warning("⚠️ 无法计算振幅，缺少必要数据")
                
                elif col == '涨跌幅':
                    # 涨跌幅 = (收盘 - 前收盘) / 前收盘 * 100%
                    if '收盘' in df.columns:
                        df['前收盘'] = df['收盘'].shift(1)
                        # 处理第一天（没有前收盘）的情况
                        df['前收盘'] = df['前收盘'].fillna(df['开盘'])
                        df['涨跌幅'] = ((df['收盘'] - df['前收盘']) / df['前收盘'] * 100).round(2)
                        df = df.drop(columns=['前收盘'])
                        logger.info("✅ 成功计算涨跌幅")
                    else:
                        logger.warning("⚠️ 无法计算涨跌幅，缺少必要数据")
                
                elif col == '涨跌额':
                    # 涨跌额 = 收盘 - 前收盘
                    if '收盘' in df.columns:
                        df['前收盘'] = df['收盘'].shift(1)
                        # 处理第一天（没有前收盘）的情况
                        df['前收盘'] = df['前收盘'].fillna(df['开盘'])
                        df['涨跌额'] = (df['收盘'] - df['前收盘']).round(4)
                        df = df.drop(columns=['前收盘'])
                        logger.info("✅ 成功计算涨跌额")
                    else:
                        logger.warning("⚠️ 无法计算涨跌额，缺少必要数据")
                
                elif col == '换手率':
                    # 换手率 = 成交量 / 流通股本 * 100%
                    # 由于不知道流通股本，无法准确计算
                    logger.warning("⚠️ 无法准确计算换手率，缺少流通股本数据")
                    # 不填充换手率，因为不准确
            
            except Exception as e:
                logger.error(f"计算列 {col} 时发生错误: {str(e)}", exc_info=True)
    
    # 4. 再次检查必需列是否存在
    final_missing_columns = [col for col in required_columns if col not in df.columns]
    if final_missing_columns:
        logger.error(f"❌ 修复后仍缺少必需列：{', '.join(final_missing_columns)}")
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
                # 检查是否成功转换
                if pd.api.types.is_datetime64_any_dtype(df["日期"]):
                    # 格式化为字符串
                    df["日期"] = df["日期"].dt.strftime("%Y-%m-%d")
                else:
                    logger.warning("日期列转换为datetime失败，保留原始值")
            except Exception as e:
                logger.error(f"日期列处理失败: {str(e)}", exc_info=True)
        # 确保所有必需列都存在
        df = ensure_required_columns(df)
        # 处理数值列
        numeric_cols = ["开盘", "最高", "最低", "收盘", "成交量", "折溢价率"]
        for col in numeric_cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                except Exception as e:
                    logger.error(f"列 {col} 转换为数值失败: {str(e)}", exc_info=True)
        # 计算缺失列
        if "成交量" in df.columns and "收盘" in df.columns:
            # 如果有成交量和收盘价，可以计算成交额
            if "成交额" not in df.columns:
                df["成交额"] = df["成交量"] * df["收盘"]
        # 计算涨跌幅等
        if "收盘" in df.columns:
            if "涨跌幅" not in df.columns:
                df["涨跌幅"] = df["收盘"].pct_change() * 100
            if "涨跌额" not in df.columns:
                df["涨跌额"] = df["收盘"].diff()
        # 处理NaN值
        if "日期" in df.columns and "收盘" in df.columns:
            df = df.dropna(subset=["日期", "收盘"])
        return df
    except Exception as e:
        logger.error(f"数据清洗过程中发生错误: {str(e)}", exc_info=True)
        raise

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
