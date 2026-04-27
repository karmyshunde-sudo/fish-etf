#!/usr/bin/env python3 
# -*- coding: utf-8 -*-
"""
ETF 日线数据多数据源爬取模块
【核心功能】多数据源轮换，解决 yfinance 单点故障问题
【数据源】
  1. yfinance (Yahoo Finance) - 原有数据源
  2. EasyMoney (emfinance) - 新增国内稳定数据源
  3. AkShare - 新增东方财富数据源
  4. Sina Finance - 新增新浪财经数据源
"""

import pandas as pd
import logging
import os
import time
import random
import tempfile
import shutil
from datetime import datetime, timedelta
from typing import Tuple, Optional, List, Dict
from config import Config
from utils.date_utils import get_beijing_time, get_last_trading_day, is_trading_day

# 初始化日志
logger = logging.getLogger(__name__)

# 数据源配置
DATA_SOURCES = [
    {
        "name": "yfinance",
        "func": "_fetch_yfinance_daily",
        "priority": 1,
        "timeout": 15,
        "delay_range": (1.0, 2.0)
    },
    {
        "name": "EasyMoney",
        "func": "_fetch_easymoney_daily",
        "priority": 2,
        "timeout": 12,
        "delay_range": (0.8, 1.5)
    },
    {
        "name": "AkShare",
        "func": "_fetch_akshare_daily",
        "priority": 3,
        "timeout": 10,
        "delay_range": (1.0, 2.0)
    },
    {
        "name": "Sina",
        "func": "_fetch_sina_daily",
        "priority": 4,
        "timeout": 10,
        "delay_range": (0.5, 1.0)
    }
]

# 全局状态
_current_source_index = 0
_failed_sources = set()

def _fetch_yfinance_daily(etf_code: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
    """从 Yahoo Finance 获取 ETF 日线数据"""
    try:
        import yfinance as yf
        
        symbol = etf_code
        if etf_code.startswith(('51', '56', '57', '58')):
            symbol = f"{etf_code}.SS"
        elif etf_code.startswith('15'):
            symbol = f"{etf_code}.SZ"
        else:
            symbol = f"{etf_code}.SZ"
        
        logger.debug(f"[yfinance] 获取 ETF {etf_code} 数据，符号：{symbol}")
        
        etf_ticker = yf.Ticker(symbol)
        df = etf_ticker.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            auto_adjust=True
        )
        
        if df is None or df.empty:
            return None
        
        df = df.reset_index()
        
        # 列名映射
        column_mapping = {
            'Date': '日期',
            'Open': '开盘',
            'High': '最高',
            'Low': '最低',
            'Close': '收盘',
            'Volume': '成交量'
        }
        
        actual_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=actual_mapping)
        
        # 计算其他列
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.dropna(subset=['日期'])
        
        if '收盘' in df.columns:
            df['涨跌额'] = df['收盘'].diff()
            prev_close = df['收盘'].shift(1)
            df['涨跌幅'] = (df['涨跌额'] / prev_close.replace(0, float('nan')) * 100).round(2)
            df['涨跌幅'] = df['涨跌幅'].fillna(0)
            df['振幅'] = ((df['最高'] - df['最低']) / prev_close.replace(0, float('nan')) * 100).round(2)
            df['振幅'] = df['振幅'].fillna(0)
        
        if '成交额' not in df.columns and '收盘' in df.columns and '成交量' in df.columns:
            df['成交额'] = (df['收盘'] * df['成交量']).round(2)
        
        df['换手率'] = 0.0
        df['折价率'] = 0.0
        df['IOPV'] = df.get('收盘', 0)
        
        return df
        
    except Exception as e:
        logger.error(f"[yfinance] ETF {etf_code} 数据获取失败：{str(e)}")
        return None

def _fetch_easymoney_daily(etf_code: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
    """从 EasyMoney (emfinance) 获取 ETF 日线数据"""
    try:
        import emfinance as em
        
        # 确定市场
        if etf_code.startswith(('51', '56', '57', '58')):
            market = "SH"
        else:
            market = "SZ"
        
        logger.debug(f"[EasyMoney] 获取 ETF {etf_code} 数据，市场：{market}")
        
        # 获取历史数据
        df = em.fetch_history_data(
            stock_code=etf_code,
            market=market,
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            data_type="ETF"
        )
        
        if df is None or df.empty:
            logger.debug(f"[EasyMoney] ETF {etf_code} 返回空数据")
            return None
        
        # 列名标准化
        column_mapping = {
            'date': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'volume': '成交量',
            'amount': '成交额'
        }
        
        # 尝试多种可能的列名
        possible_columns = {
            '日期': ['date', 'Date', 'datetime', 'time', '交易日'],
            '开盘': ['open', 'Open', '开盘价'],
            '最高': ['high', 'High', '最高价'],
            '最低': ['low', 'Low', '最低价'],
            '收盘': ['close', 'Close', '收盘价'],
            '成交量': ['volume', 'Volume', 'vol', '成交量'],
            '成交额': ['amount', 'Amount', 'turnover', '成交额', '成交金额']
        }
        
        for cn_col, possible_names in possible_columns.items():
            for name in possible_names:
                if name in df.columns and cn_col not in df.columns:
                    df[cn_col] = df[name]
                    break
        
        # 日期格式化
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.dropna(subset=['日期'])
        
        # 计算涨跌幅
        if '收盘' in df.columns:
            df['涨跌额'] = df['收盘'].diff()
            prev_close = df['收盘'].shift(1)
            df['涨跌幅'] = (df['涨跌额'] / prev_close.replace(0, float('nan')) * 100).round(2)
            df['涨跌幅'] = df['涨跌幅'].fillna(0)
        
        if '最高' in df.columns and '最低' in df.columns:
            prev_close = df['收盘'].shift(1)
            df['振幅'] = ((df['最高'] - df['最低']) / prev_close.replace(0, float('nan')) * 100).round(2)
            df['振幅'] = df['振幅'].fillna(0)
        
        # 补充默认列
        df['换手率'] = 0.0
        df['折价率'] = 0.0
        df['IOPV'] = df.get('收盘', 0)
        
        return df
        
    except ImportError:
        logger.warning("[EasyMoney] emfinance 未安装，跳过此数据源")
        return None
    except Exception as e:
        logger.error(f"[EasyMoney] ETF {etf_code} 数据获取失败：{str(e)}")
        return None

def _fetch_akshare_daily(etf_code: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
    """从 AkShare (东方财富) 获取 ETF 日线数据"""
    try:
        import akshare as ak
        
        logger.debug(f"[AkShare] 获取 ETF {etf_code} 数据")
        
        # 确定市场代码
        if etf_code.startswith(('51', '56', '57', '58')):
            symbol = f"sh{etf_code}"
        else:
            symbol = f"sz{etf_code}"
        
        # 获取历史数据
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            adjust="qfq"
        )
        
        if df is None or df.empty:
            logger.debug(f"[AkShare] ETF {etf_code} 返回空数据")
            return None
        
        # 列名映射（AkShare 返回中文列名）
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
        
        available_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=available_columns)
        
        # 确保日期格式
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.dropna(subset=['日期'])
        
        # 补充默认列
        if '换手率' not in df.columns:
            df['换手率'] = 0.0
        if '折价率' not in df.columns:
            df['折价率'] = 0.0
        if 'IOPV' not in df.columns:
            df['IOPV'] = df.get('收盘', 0)
        
        return df
        
    except ImportError:
        logger.warning("[AkShare] akshare 未安装，跳过此数据源")
        return None
    except Exception as e:
        logger.error(f"[AkShare] ETF {etf_code} 数据获取失败：{str(e)}")
        return None

def _fetch_sina_daily(etf_code: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
    """从 Sina Finance 获取 ETF 日线数据"""
    try:
        import requests
        
        logger.debug(f"[Sina] 获取 ETF {etf_code} 数据")
        
        # 确定市场代码
        if etf_code.startswith(('51', '56', '57', '58')):
            sina_code = f"sh{etf_code}"
        else:
            sina_code = f"sz{etf_code}"
        
        # 新浪财经日线数据 API
        url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {
            "symbol": sina_code,
            "scale": 240,  # 日线
            "ma": "no",
            "datalen": 1024
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code != 200:
            logger.debug(f"[Sina] HTTP {response.status_code}")
            return None
        
        data = response.json()
        
        if not data or len(data) == 0:
            logger.debug(f"[Sina] 返回空数据")
            return None
        
        # 转换为 DataFrame
        df = pd.DataFrame(data)
        
        # 列名映射
        # Sina 返回：day, open, high, low, close, volume
        column_mapping = {
            'day': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'volume': '成交量'
        }
        
        available_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=available_columns)
        
        # 日期格式化
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.dropna(subset=['日期'])
        
        # 计算其他列
        if '收盘' in df.columns:
            df['涨跌额'] = df['收盘'].diff()
            prev_close = df['收盘'].shift(1)
            df['涨跌幅'] = (df['涨跌额'] / prev_close.replace(0, float('nan')) * 100).round(2)
            df['涨跌幅'] = df['涨跌幅'].fillna(0)
        
        if '最高' in df.columns and '最低' in df.columns:
            prev_close = df['收盘'].shift(1)
            df['振幅'] = ((df['最高'] - df['最低']) / prev_close.replace(0, float('nan')) * 100).round(2)
            df['振幅'] = df['振幅'].fillna(0)
        
        # 成交额估算
        if '成交额' not in df.columns and '收盘' in df.columns and '成交量' in df.columns:
            df['成交额'] = (df['收盘'] * df['成交量']).round(2)
        
        df['换手率'] = 0.0
        df['折价率'] = 0.0
        df['IOPV'] = df.get('收盘', 0)
        
        return df
        
    except Exception as e:
        logger.error(f"[Sina] ETF {etf_code} 数据获取失败：{str(e)}")
        return None

def fetch_etf_daily_data(etf_code: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
    """
    多数据源轮换获取 ETF 日线数据
    
    Args:
        etf_code: ETF 代码
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        pd.DataFrame: ETF 日线数据
    """
    global _current_source_index, _failed_sources
    
    total_sources = len(DATA_SOURCES)
    
    for offset in range(total_sources):
        source_idx = (_current_source_index + offset) % total_sources
        source = DATA_SOURCES[source_idx]
        
        # 跳过临时失败的数据源
        if source["name"] in _failed_sources:
            logger.debug(f"跳过临时失败的数据源：{source['name']}")
            continue
        
        try:
            logger.info(f"尝试数据源 [{source['name']}] (优先级：{source['priority']})")
            
            # 动态延时
            delay_min, delay_max = source["delay_range"]
            time.sleep(random.uniform(delay_min, delay_max))
            
            # 调用对应的获取函数
            func_name = source["func"]
            func = globals().get(func_name)
            
            if not func:
                logger.error(f"找不到数据源函数：{func_name}")
                continue
            
            df = func(etf_code, start_date, end_date)
            
            # 验证数据有效性
            if df is None or df.empty:
                raise ValueError("返回空数据")
            
            # 检查必要列
            required_columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"缺少必要列：{', '.join(missing_columns)}")
            
            # 成功获取数据
            logger.info(f"✅ [{source['name']}] 成功获取 ETF {etf_code} {len(df)} 条数据")
            
            # 锁定当前数据源
            _current_source_index = source_idx
            
            # 从失败列表中移除
            if source["name"] in _failed_sources:
                _failed_sources.remove(source["name"])
                logger.info(f"数据源 {source['name']} 恢复使用")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ [{source['name']}] 失败：{str(e)}")
            continue
    
    # 所有数据源都失败
    logger.error(f"所有数据源均无法获取 ETF {etf_code} 数据")
    
    # 轮换到下一个数据源
    _current_source_index = (_current_source_index + 1) % total_sources
    
    return None

def normalize_etf_dataframe(df: pd.DataFrame, etf_code: str, etf_name: str) -> pd.DataFrame:
    """
    规范化 ETF 日线数据结构与精度
    
    Args:
        df: 原始 DataFrame
        etf_code: ETF 代码
        etf_name: ETF 名称
    
    Returns:
        pd.DataFrame: 规范化后的 DataFrame
    """
    expected_columns = [
        "日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额",
        "振幅", "涨跌幅", "涨跌额", "换手率", "IOPV", "折价率",
        "ETF 代码", "ETF 名称", "爬取时间"
    ]
    
    # 缺少列自动补 0
    for col in expected_columns:
        if col not in df.columns:
            df[col] = 0
    
    # 精度处理
    four_decimals = ["开盘", "最高", "最低", "收盘", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率", "IOPV", "折价率"]
    for col in four_decimals:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)
    
    if "成交量" in df.columns:
        df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce").fillna(0).astype(int)
    
    df["ETF 代码"] = etf_code
    df["ETF 名称"] = etf_name
    df["爬取时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    df = df[expected_columns]
    df = df.sort_values(by="日期", ascending=True).reset_index(drop=True)
    
    return df

# 测试函数
if __name__ == "__main__":
    # 测试数据源
    test_etf = "510050"
    end_date = get_beijing_time()
    start_date = end_date - timedelta(days=30)
    
    logger.info(f"测试多数据源 ETF 日线爬取：{test_etf}")
    
    df = fetch_etf_daily_data(test_etf, start_date, end_date)
    
    if df is not None and not df.empty:
        logger.info(f"成功获取 {len(df)} 条数据")
        logger.info(f"列名：{list(df.columns)}")
        logger.info(f"数据样本:\n{df.head()}")
    else:
        logger.error("获取数据失败")
