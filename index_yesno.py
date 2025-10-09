#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数 Yes/No 策略执行器
每天计算指定指数的策略信号并推送微信通知
"""

import os
import logging
import pandas as pd
import akshare as ak
import time
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 指定计算的指数列表（硬编码，包含完整策略信息）
INDICES = [
    # 新增的4个ETF放在最前面
    {
        "code": "^NDX",
        "name": "纳斯达克100",
        "akshare_code": "^NDX",
        "etf_code": "159892",
        "etf_name": "华夏纳斯达克100ETF",
        "description": "跟踪纳斯达克100指数，美股科技龙头"
    },
    {
        "code": "^NDX",
        "name": "纳斯达克100",
        "akshare_code": "^NDX",
        "etf_code": "513100",
        "etf_name": "国泰纳斯达克100ETF",
        "description": "跟踪纳斯达克100指数，美股科技龙头"
    },
    {
        "code": "H30533.CSI",
        "name": "中证海外中国互联网",
        "akshare_code": "H30533.CSI",
        "etf_code": "513500",
        "etf_name": "易方达中概互联网ETF",
        "description": "跟踪中证海外中国互联网指数，涵盖海外上市中概股"
    },
    {
        "code": "^HSI",  # 已修改为恒生指数代码
        "name": "恒生指数",
        "akshare_code": "^HSI",
        "etf_code": "513400",
        "etf_name": "华夏恒生互联网ETF",
        "description": "跟踪恒生指数，港股龙头"  # 已修改描述
    },
    
    # 原有ETF列表，保持完全不变
    {
        "code": "000300",
        "name": "沪深300",
        "akshare_code": "sh000300",
        "etf_code": "510300",
        "etf_name": "华泰柏瑞沪深300ETF",
        "description": "宽基核心，日均成交额超10亿"
    },
    {
        "code": "000905",
        "name": "中证500",
        "akshare_code": "sh000905",
        "etf_code": "510500",
        "etf_name": "南方中证500ETF",
        "description": "中证500流动性标杆ETF"
    },
    {
        "code": "000688",
        "name": "科创50",
        "akshare_code": "sh000688",
        "etf_code": "588000",
        "etf_name": "华夏科创50ETF",
        "description": "科创板核心宽基ETF"
    },
    {
        "code": "399006",
        "name": "创业板指数",
        "akshare_code": "sz399006",
        "etf_code": "159915",
        "etf_name": "易方达创业板ETF",
        "description": "创业板规模最大ETF之一"
    },
    {
        "code": "399005",
        "name": "中小板指数",
        "akshare_code": "sz399005",
        "etf_code": "159902",
        "etf_name": "华夏中小板ETF",
        "description": "跟踪中小板全指"
    },
    {
        "code": "399395",
        "name": "国证有色金属",
        "akshare_code": "sz399395",
        "etf_code": "512400",
        "etf_name": "南方有色金属ETF",
        "description": "覆盖有色全产业链"
    },
    {
        "code": "399967",
        "name": "中证军工指数",
        "akshare_code": "sz399967",
        "etf_code": "512660",
        "etf_name": "富国中证军工ETF",
        "description": "军工行业规模领先ETF"
    },
    {
        "code": "399975",
        "name": "中证证券指数",
        "akshare_code": "sz399975",
        "etf_code": "512880",
        "etf_name": "国泰中证全指证券公司ETF",
        "description": "证券行业流动性首选"
    },
    {
        "code": "930713",
        "name": "中证AI产业",
        "akshare_code": "sh930713",
        "etf_code": "515070",
        "etf_name": "华夏中证AI产业ETF",
        "description": "AI全产业链覆盖"
    },
    {
        "code": "990001",
        "name": "中证全指半导体",
        "akshare_code": "sh990001",
        "etf_code": "159813",
        "etf_name": "国泰CES半导体ETF",
        "description": "半导体行业主流标的"
    },
    {
        "code": "000821",
        "name": "中证红利低波动指数",
        "akshare_code": "sh000821",
        "etf_code": "515450",
        "etf_name": "华泰柏瑞中证红利低波动ETF",
        "description": "稳健型红利类ETF"
    },
    {
        "code": "000829",
        "name": "上海金ETF指数",
        "akshare_code": "sh000829",
        "etf_code": "518850",
        "etf_name": "华安黄金ETF",
        "description": "国内规模最大黄金ETF"
    },
    {
        "code": "000012",
        "name": "上证国债指数",
        "akshare_code": "sh000012",
        "etf_code": "511260",
        "etf_name": "博时上证国债ETF",
        "description": "跟踪上证国债指数，低波动"
    }
]

# 策略参数
CRITICAL_VALUE_DAYS = 20  # 计算临界值的周期（20日均线）
DEVIATION_THRESHOLD = 0.02  # 偏离阈值（2%）
PATTERN_CONFIDENCE_THRESHOLD = 0.7  # 形态确认阈值（70%置信度）

def check_network_connection():
    """检查网络连接是否正常"""
    try:
        import requests
        response = requests.get('https://www.baidu.com  ', timeout=5)
        return response.status_code == 200
    except Exception:
        return False

def fetch_index_data(index_code: str, days: int = 250) -> pd.DataFrame:
    """
    从可靠数据源获取指数历史数据
    
    Args:
        index_code: 指数代码（如"000300"）
        days: 获取最近多少天的数据
        
    Returns:
        pd.DataFrame: 指数日线数据
    """
    try:
        # 计算日期范围 - 保持为datetime对象
        end_date_dt = datetime.now()
        start_date_dt = end_date_dt - timedelta(days=days)
        
        # 仅在需要字符串时转换
        end_date = end_date_dt.strftime("%Y%m%d")
        start_date = start_date_dt.strftime("%Y%m%d")
        
        logger.info(f"获取指数 {index_code} 数据，时间范围: {start_date} 至 {end_date}")
        
        # 特殊处理恒生指数
        if index_code == "^HSI":
            logger.info("特殊处理恒生指数: ^HSI")
            logger.info("使用 yfinance 获取恒生指数 (^HSI) 数据")
            
            try:
                # 使用datetime对象进行转换
                start_dt = start_date_dt.strftime("%Y-%m-%d")
                end_dt = end_date_dt.strftime("%Y-%m-%d")
                
                # 获取数据
                df = yf.download('^HSI', start=start_dt, end=end_dt)
                
                if isinstance(df, pd.DataFrame) and not df.empty:
                    logger.info(f"✅ 成功获取到 {len(df)} 条恒生指数数据")
                    logger.info(f"数据列名: {', '.join(df.columns)}")
                    
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
                    
                    # 【日期datetime类型规则】确保日期列为datetime类型
                    df['日期'] = pd.to_datetime(df['日期'])
                    
                    # 排序
                    df = df.sort_values('日期').reset_index(drop=True)
                    
                    # 检查数据量
                    if len(df) <= 1:
                        logger.warning(f"⚠️ 只获取到{len(df)}条数据，可能是当天数据，无法用于历史分析")
                        return pd.DataFrame()
                    
                    logger.info(f"✅ 获取到恒生指数历史数据，日期范围: {df['日期'].min()} 至 {df['日期'].max()}，共{len(df)}条记录")
                    return df
                else:
                    logger.warning("⚠️ yfinance 返回空数据")
                    return pd.DataFrame()
            except Exception as e:
                logger.error(f"❌ yfinance.download 方法获取恒生指数历史数据失败: {str(e)}")
                return pd.DataFrame()
        
        # 根据指数类型使用不同的数据接口
        if index_code.startswith('^'):
            # 美股指数处理 - 使用YFinance
            return fetch_us_index_from_yfinance(index_code, start_date_dt, end_date_dt)
        
        elif index_code.endswith('.CSI'):
            # 中证系列指数
            index_name = index_code.replace('.CSI', '')
            # 传递datetime对象
            return ak.index_zh_a_hist(
                symbol=index_name,
                period="daily",
                start_date=start_date,
                end_date=end_date
            )
        
        elif index_code.endswith('.HI'):
            # 恒生系列指数 - 使用专门的函数处理
            # 传递datetime对象
            return fetch_hang_seng_index_data(index_code, start_date_dt, end_date_dt)
        
        else:
            # A股指数
            # 传递datetime对象
            return ak.index_zh_a_hist(
                symbol=index_code,
                period="daily",
                start_date=start_date,
                end_date=end_date
            )
    
    except Exception as e:
        logger.error(f"获取指数 {index_code} 数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def fetch_hang_seng_index_data(index_code: str, start_date_dt: datetime, end_date_dt: datetime) -> pd.DataFrame:
    """
    专门处理恒生指数数据获取
    仅提供详细的日志输出，不保存任何文件
    
    Args:
        index_code: 指数代码（如"HSNDXIT.HI"）
        start_date_dt: 开始日期（datetime对象）
        end_date_dt: 结束日期（datetime对象）
        
    Returns:
        pd.DataFrame: 指数日线数据
    """
    index_name = index_code.replace('.HI', '')
    logger.info(f"获取恒生指数数据: {index_code} ({index_name})")
    
    # 1. 尝试使用AKShare的stock_hk_index_daily_em获取恒生科技指数历史数据
    try:
        logger.info("尝试使用 ak.stock_hk_index_daily_em 获取恒生科技指数历史数据")
        
        # 恒生科技指数的正确代码是 "HSNDXIT" 而不是 "HSNDXIT.HI"
        symbol = index_code.replace('.HI', '')
        logger.info(f"使用指数代码: {symbol} (已移除'.HI'后缀)")
        
        # 转换为字符串格式
        start_date = start_date_dt.strftime("%Y%m%d")
        end_date = end_date_dt.strftime("%Y%m%d")
        
        # 获取数据
        df = ak.stock_hk_index_daily_em(symbol=symbol, period="daily", 
                                      start_date=start_date, end_date=end_date)
        
        if not df.empty:
            logger.info(f"✅ 成功获取到 {len(df)} 条数据")
            
            # 检查数据列
            logger.info(f"数据列名: {', '.join(df.columns)}")
            
            # 【日期datetime类型规则】确保日期列为datetime类型
            if '日期' in df.columns:
                try:
                    df['日期'] = pd.to_datetime(df['日期'])
                except Exception as e:
                    logger.error(f"日期格式转换失败: {str(e)}")
                    return pd.DataFrame()
            
            # 检查日期范围
            if '日期' in df.columns:
                first_date = df['日期'].min()
                last_date = df['日期'].max()
                logger.info(f"数据日期范围: {first_date} 至 {last_date}")
            
            # 检查数据量
            if len(df) <= 1:
                logger.warning(f"⚠️ 只获取到{len(df)}条数据，可能是当天数据，无法用于历史分析")
                return pd.DataFrame()
            
            return df
        else:
            logger.warning("⚠️ ak.stock_hk_index_daily_em 返回空数据")
            logger.warning("可能原因:")
            logger.warning("  - 指数代码不正确（应为 'HSNDXIT' 而不是 'HSNDXIT.HI'）")
            logger.warning("  - 数据源无历史数据")
            logger.warning("  - 网络连接问题")
            logger.warning("  - 日期范围不正确")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"❌ ak.stock_hk_index_daily_em 方法获取恒生科技指数历史数据失败: {str(e)}")
        logger.error("详细错误信息:")
        logger.error(str(e))
        
        # 检查akshare版本
        import akshare as ak
        logger.error(f"当前akshare版本: {ak.__version__}")
        logger.error("可能解决方案:")
        logger.error("  - 确认指数代码为 'HSNDXIT'（无.HI后缀）")
        logger.error("  - 更新akshare: pip install --upgrade akshare")
        logger.error("  - 检查akshare文档: https://www.akshare.xyz/  ")
        logger.error("  - 查看akshare issue: https://github.com/akshare/akshare/issues  ")
        
        return pd.DataFrame()
    
    # 2. 如果第一步失败，尝试使用AKShare的stock_hk_index_spot_em获取实时数据
    try:
        logger.info("尝试使用 ak.stock_hk_index_spot_em 获取恒生科技指数实时数据")
        
        # 获取数据
        df = ak.stock_hk_index_spot_em()
        
        if not df.empty:
            # 过滤指定的指数
            filtered = df[df["代码"] == index_code.replace('.HI', '')]
            
            if not filtered.empty:
                logger.info(f"✅ 成功获取到 {len(filtered)} 条实时数据")
                logger.info(f"数据列名: {', '.join(filtered.columns)}")
                
                # 仅用于诊断，不返回实时数据
                logger.info("⚠️ 注意: 实时数据无法用于历史分析")
                logger.info(f"实时数据: {filtered.iloc[0].to_dict()}")
                return pd.DataFrame()
            else:
                logger.warning("⚠️ 未找到指定指数的实时数据")
        else:
            logger.warning("⚠️ ak.stock_hk_index_spot_em 返回空数据")
    except Exception as e:
        logger.error(f"❌ ak.stock_hk_index_spot_em 方法获取数据失败: {str(e)}")
    
    # 3. 尝试使用yfinance获取恒生科技指数
    try:
        import yfinance as yf
        # 使用datetime对象
        start_dt = start_date_dt.strftime("%Y-%m-%d")
        end_dt = end_date_dt.strftime("%Y-%m-%d")
        
        logger.info(f"尝试使用 yfinance.download 获取恒生科技指数历史数据 (HSTECH.HK)")
        df = yf.download('HSTECH.HK', start=start_dt, end=end_dt)
        
        if isinstance(df, pd.DataFrame) and not df.empty:
            logger.info(f"✅ 成功获取到 {len(df)} 条数据")
            logger.info(f"数据列名: {', '.join(df.columns)}")
            
            # 检查日期范围
            if 'Date' in df.columns:
                first_date = df.index.min().strftime("%Y-%m-%d")
                last_date = df.index.max().strftime("%Y-%m-%d")
                logger.info(f"数据日期范围: {first_date} 至 {last_date}")
            
            # 检查数据量
            if len(df) <= 1:
                logger.warning(f"⚠️ 只获取到{len(df)}条数据，可能是当天数据，无法用于历史分析")
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
            
            # 【日期datetime类型规则】确保日期列为datetime类型
            df['日期'] = pd.to_datetime(df['日期'])
            
            return df
        else:
            logger.warning("⚠️ yfinance 返回空数据或非DataFrame")
            logger.warning("可能原因:")
            logger.warning("  - 指数代码不正确（应为 'HSTECH.HK'）")
            logger.warning("  - 数据源无历史数据")
            logger.warning("  - 网络连接问题")
            logger.warning("  - 日期范围不正确")
    except Exception as e:
        logger.error(f"❌ yfinance.download 方法获取恒生科技指数历史数据失败: {str(e)}")
        logger.error("详细错误信息:")
        logger.error(str(e))
    
    # 没有获取到任何有效数据
    logger.error(f"❌ 无法获取恒生科技指数历史数据: {index_code}")
    logger.error("❌ 可能原因：")
    logger.error("  1. 指数代码不正确：")
    logger.error("     - 应使用 'HSNDXIT' 而不是 'HSNDXIT.HI'")
    logger.error("     - 恒生科技指数代码应为 'HSNDXIT'")
    logger.error("  2. 日期范围问题：")
    logger.error("     - 开始日期: " + start_date_dt.strftime("%Y%m%d"))
    logger.error("     - 结束日期: " + end_date_dt.strftime("%Y%m%d"))
    logger.error("  3. AKShare接口问题：")
    logger.error("     - 确认akshare版本: " + ak.__version__)
    logger.error("     - 查看文档: https://www.akshare.xyz/  ")
    logger.error("     - 检查issue: https://github.com/akshare/akshare/issues  ")
    
    return pd.DataFrame()

def fetch_us_index_from_yfinance(index_code: str, start_date_dt: datetime, end_date_dt: datetime) -> pd.DataFrame:
    """
    使用YFinance获取美股指数数据
    
    Args:
        index_code: 指数代码（如"^NDX"）
        start_date_dt: 开始日期（datetime对象）
        end_date_dt: 结束日期（datetime对象）
        
    Returns:
        pd.DataFrame: 指数日线数据
    """
    try:
        # 转换日期格式 - 仅在需要时转换
        start_dt = start_date_dt.strftime("%Y-%m-%d")
        end_dt = end_date_dt.strftime("%Y-%m-%d")
        
        # 指数代码映射
        symbol_map = {
            '^NDX': '^NDX',  # 纳斯达克100
            '^DJI': '^DJI',  # 道琼斯工业指数
            '^GSPC': '^GSPC', # 标准普尔500
            '^HSTECH': '^HSTECH' # 恒生科技指数
        }
        
        symbol = symbol_map.get(index_code, index_code)
        
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
        
        # 【日期datetime类型规则】确保日期列为datetime类型
        df['日期'] = pd.to_datetime(df['日期'])
        
        logger.info(f"成功通过yfinance获取{index_code}数据，共{len(df)}条记录")
        return df
    
    except Exception as e:
        logger.error(f"通过yfinance获取{index_code}失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def calculate_critical_value(df: pd.DataFrame) -> float:
    """计算临界值（20日均线）"""
    if len(df) < CRITICAL_VALUE_DAYS:
        logger.warning(f"数据不足{CRITICAL_VALUE_DAYS}天，无法准确计算临界值")
        return df["收盘"].mean() if not df.empty else 0.0
    
    return df['收盘'].rolling(window=CRITICAL_VALUE_DAYS).mean().iloc[-1]

def calculate_deviation(current: float, critical: float) -> float:
    """计算偏离率"""
    return (current - critical) / critical * 100

def calculate_consecutive_days_above(df: pd.DataFrame, critical_value: float) -> int:
    """计算连续站上均线的天数"""
    if len(df) < 2:
        return 0
    
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS).mean().values
    
    # 从最新日期开始向前检查
    consecutive_days = 0
    for i in range(len(close_prices)-1, -1, -1):
        if i < CRITICAL_VALUE_DAYS - 1:
            break
            
        if close_prices[i] >= ma_values[i]:
            consecutive_days += 1
        else:
            break
    
    return consecutive_days

def calculate_consecutive_days_below(df: pd.DataFrame, critical_value: float) -> int:
    """计算连续跌破均线的天数"""
    if len(df) < 2:
        return 0
    
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS).mean().values
    
    # 从最新日期开始向前检查
    consecutive_days = 0
    for i in range(len(close_prices)-1, -1, -1):
        if i < CRITICAL_VALUE_DAYS - 1:
            break
            
        if close_prices[i] < ma_values[i]:
            consecutive_days += 1
        else:
            break
    
    return consecutive_days

def calculate_volume_change(df: pd.DataFrame) -> float:
    """
    计算成交量变化率
    
    Args:
        df: ETF日线数据
    
    Returns:
        float: 成交量变化率（当前成交量相比前一日的变化百分比）
    """
    try:
        if len(df) < 2:
            logger.warning("数据量不足，无法计算成交量变化")
            return 0.0
        
        # 获取最新两个交易日的成交量
        current_volume = df['成交量'].values[-1]
        previous_volume = df['成交量'].values[-2]
        
        # 确保是数值类型
        if not isinstance(current_volume, (int, float)) or not isinstance(previous_volume, (int, float)):
            # 尝试转换为浮点数
            try:
                current_volume = float(current_volume)
                previous_volume = float(previous_volume)
            except:
                logger.warning("成交量数据无法转换为数值类型")
                return 0.0
        
        # 计算变化率
        if previous_volume > 0:
            volume_change = (current_volume - previous_volume) / previous_volume
            return volume_change
        else:
            return 0.0
    
    except Exception as e:
        logger.error(f"计算成交量变化失败: {str(e)}", exc_info=True)
        return 0.0

def calculate_loss_percentage(df: pd.DataFrame) -> float:
    """计算当前亏损比例（相对于最近一次买入点）"""
    if len(df) < 2:
        return 0.0
    
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS).mean().values
    
    # 从最新日期开始向前检查，找到最近一次站上均线的点
    buy_index = -1
    for i in range(len(close_prices)-1, -1, -1):
        if i < CRITICAL_VALUE_DAYS - 1:
            continue
            
        if close_prices[i] >= ma_values[i]:
            buy_index = i
            break
    
    # 如果找不到买入点，使用30天前作为参考
    if buy_index == -1:
        buy_index = max(0, len(close_prices) - 30)
    
    current_price = close_prices[-1]
    buy_price = close_prices[buy_index]
    
    loss_percentage = (current_price - buy_price) / buy_price * 100
    return loss_percentage

def is_in_volatile_market(df: pd.DataFrame) -> tuple:
    """判断是否处于震荡市
    
    Returns:
        tuple: (是否震荡市, 穿越次数, 最近10天偏离率范围)
    """
    if len(df) < 10:
        return False, 0, (0, 0)
    
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS).mean().values
    
    # 检查是否连续10天在均线附近波动（-5%~+5%）
    last_10_days = df.tail(10)
    deviations = []
    for i in range(len(last_10_days)):
        # 确保有足够的数据计算均线
        if i < CRITICAL_VALUE_DAYS - 1 or np.isnan(ma_values[-10 + i]):
            continue
            
        deviation = (close_prices[-10 + i] - ma_values[-10 + i]) / ma_values[-10 + i] * 100
        if abs(deviation) > 5.0:
            return False, 0, (0, 0)
        deviations.append(deviation)
    
    # 检查价格是否反复穿越均线
    cross_count = 0
    for i in range(len(close_prices)-10, len(close_prices)-1):
        # 确保有足够的数据计算均线
        if i < CRITICAL_VALUE_DAYS - 1 or np.isnan(ma_values[i]) or np.isnan(ma_values[i+1]):
            continue
            
        if (close_prices[i] >= ma_values[i] and close_prices[i+1] < ma_values[i+1]) or \
           (close_prices[i] < ma_values[i] and close_prices[i+1] >= ma_values[i+1]):
            cross_count += 1
    
    # 至少需要5次穿越才认定为震荡市
    min_cross_count = 5
    is_volatile = cross_count >= min_cross_count
    
    # 计算最近10天偏离率范围
    if deviations:
        min_deviation = min(deviations)
        max_deviation = max(deviations)
    else:
        # 当没有有效数据时，使用0作为默认值
        min_deviation = 0
        max_deviation = 0
    
    return is_volatile, cross_count, (min_deviation, max_deviation)

def detect_head_and_shoulders(df: pd.DataFrame) -> dict:
    """检测M头和头肩顶形态
    
    Returns:
        dict: 形态检测结果
    """
    if len(df) < 20:  # 需要足够数据
        return {"pattern_type": "无", "detected": False, "confidence": 0, "peaks": []}
    
    # 获取收盘价
    close_prices = df["收盘"].values
    
    # 寻找局部高点
    peaks = []
    for i in range(5, len(close_prices)-5):
        if close_prices[i] > max(close_prices[i-5:i]) and close_prices[i] > max(close_prices[i+1:i+6]):
            peaks.append((i, close_prices[i]))
    
    # 如果找到的高点少于3个，无法形成头肩顶
    if len(peaks) < 3:
        return {"pattern_type": "无", "detected": False, "confidence": 0, "peaks": peaks}
    
    # 检测M头（两个高点）
    m_top_detected = False
    m_top_confidence = 0.0
    if len(peaks) >= 2:
        # 两个高点，第二个略低于第一个，中间有明显低点
        peak1_idx, peak1_price = peaks[-2]
        peak2_idx, peak2_price = peaks[-1]
        
        # 检查第二个高点是否低于第一个
        if peak2_price < peak1_price and peak2_price > peak1_price * 0.95:
            # 检查中间是否有明显低点
            trough_idx = peak1_idx + np.argmin(close_prices[peak1_idx:peak2_idx])
            trough_price = close_prices[trough_idx]
            
            # 检查低点是否明显
            if trough_price < peak1_price * 0.97 and trough_price < peak2_price * 0.97:
                m_top_detected = True
                # 计算置信度
                price_diff = (peak1_price - peak2_price) / peak1_price
                trough_depth = (peak1_price - trough_price) / peak1_price
                m_top_confidence = 0.5 + 0.5 * min(price_diff / 0.05, 1) + 0.5 * min(trough_depth / 0.05, 1)
                m_top_confidence = min(m_top_confidence, 1.0)
    
    # 检测头肩顶（三个高点）
    head_and_shoulders_detected = False
    head_and_shoulders_confidence = 0.0
    
    if len(peaks) >= 3:
        # 三个高点，中间最高，两侧较低
        shoulder1_idx, shoulder1_price = peaks[-3]
        head_idx, head_price = peaks[-2]
        shoulder2_idx, shoulder2_price = peaks[-1]
        
        # 检查中间是否为最高点
        if head_price > shoulder1_price and head_price > shoulder2_price:
            # 检查两侧肩膀是否大致对称
            shoulder_similarity = min(shoulder1_price, shoulder2_price) / max(shoulder1_price, shoulder2_price)
            
            # 检查中间低点
            trough1_idx = shoulder1_idx + np.argmin(close_prices[shoulder1_idx:head_idx])
            trough2_idx = head_idx + np.argmin(close_prices[head_idx:shoulder2_idx])
            neckline_price = (close_prices[trough1_idx] + close_prices[trough2_idx]) / 2
            
            # 检查头肩比例是否合理
            if shoulder_similarity > 0.85 and head_price > neckline_price * 1.1:
                head_and_shoulders_detected = True
                # 计算置信度
                shoulder_diff = 1 - shoulder_similarity
                head_height = (head_price - neckline_price) / neckline_price
                head_and_shoulders_confidence = 0.5 + 0.3 * min(shoulder_diff / 0.15, 1) + 0.2 * min(head_height / 0.15, 1)
                head_and_shoulders_confidence = min(head_and_shoulders_confidence, 1.0)
    
    # 确定主要检测结果
    if head_and_shoulders_detected and head_and_shoulders_confidence > m_top_confidence:
        return {
            "pattern_type": "头肩顶",
            "detected": True,
            "confidence": head_and_shoulders_confidence,
            "peaks": peaks[-3:]
        }
    elif m_top_detected:
        return {
            "pattern_type": "M头",
            "detected": True,
            "confidence": m_top_confidence,
            "peaks": peaks[-2:]
        }
    else:
        return {
            "pattern_type": "无",
            "detected": False,
            "confidence": 0,
            "peaks": peaks[-3:] if len(peaks) >= 3 else peaks
        }

def generate_signal_message(index_info: dict, df: pd.DataFrame, current: float, critical: float, deviation: float) -> str:
    """生成策略信号消息"""
    # 计算连续站上/跌破均线的天数
    consecutive_above = calculate_consecutive_days_above(df, critical)
    consecutive_below = calculate_consecutive_days_below(df, critical)
    
    # 计算成交量变化
    volume_change = calculate_volume_change(df)
    
    # 检测M头/头肩顶形态
    pattern_detection = detect_head_and_shoulders(df)
    
    # 3. 震荡市判断 - 优先级最高
    is_volatile, cross_count, (min_dev, max_dev) = is_in_volatile_market(df)
    if is_volatile:
        # 计算上轨和下轨价格
        upper_band = critical * (1 + max_dev/100)
        lower_band = critical * (1 + min_dev/100)
        
        message = (
            f"【震荡市】连续10日价格反复穿均线（穿越{cross_count}次），偏离率范围[{min_dev:.2f}%~{max_dev:.2f}%]\n"
            f"✅ 操作建议：\n"
            f"  • 上沿操作（价格≈{upper_band:.2f}）：小幅减仓10%-20%（如{index_info['etf_code']}）\n"
            f"  • 下沿操作（价格≈{lower_band:.2f}）：小幅加仓10%-20%（如{index_info['etf_code']}）\n"
            f"  • 总仓位严格控制在≤50%\n"
            f"⚠️ 避免频繁交易，等待趋势明朗\n"
        )
        return message
    
    # 1. YES信号：当前价格 ≥ 20日均线
    if current >= critical:
        # 子条件1：首次突破（价格刚站上均线，连续2-3日站稳+成交量放大20%+）
        if consecutive_above == 1 and volume_change > 0.2:
            message = (
                f"【首次突破】连续{consecutive_above}天站上20日均线，成交量放大{volume_change*100:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 核心宽基ETF（{index_info['etf_code']}）立即建仓30%\n"
                f"  • 卫星行业ETF立即建仓20%\n"
                f"  • 回调至5日均线（约{current * 0.99:.2f}）可加仓20%\n"
                f"⚠️ 止损：买入价下方5%（宽基ETF）或3%（高波动ETF）\n"
            )
        # 子条件1：首次突破（价格刚站上均线，连续2-3日站稳+成交量放大20%+）
        elif 2 <= consecutive_above <= 3 and volume_change > 0.2:
            message = (
                f"【首次突破确认】连续{consecutive_above}天站上20日均线，成交量放大{volume_change*100:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 核心宽基ETF（{index_info['etf_code']}）可加仓至50%\n"
                f"  • 卫星行业ETF可加仓至35%\n"
                f"  • 严格跟踪5日均线作为止损位（约{current * 0.99:.2f}）\n"
                f"⚠️ 注意：若收盘跌破5日均线，立即减仓50%\n"
            )
        # 子条件2：持续站稳（价格维持在均线上）
        else:
            # 场景A：偏离率≤+5%（趋势稳健）
            if deviation <= 5.0:
                # 添加M头/头肩顶形态检测
                pattern_msg = ""
                if pattern_detection["detected"]:
                    pattern_name = pattern_detection["pattern_type"]
                    confidence = pattern_detection["confidence"]
                    if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                        pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），建议减仓10%-15%"
                    elif confidence >= 0.5:
                        pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓5%-10%"
                
                message = (
                    f"【趋势稳健】连续{consecutive_above}天站上20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 持仓不动，不新增仓位\n"
                    f"  • 跟踪止损上移至5日均线（约{current * 0.99:.2f}）\n"
                    f"  • 若收盘跌破5日均线，减仓50%\n"
                    f"{pattern_msg}\n"
                )
            # 场景B：+5%＜偏离率≤+10%（趋势较强）
            elif 5.0 < deviation <= 10.0:
                # 添加M头/头肩顶形态检测
                pattern_msg = ""
                if pattern_detection["detected"]:
                    pattern_name = pattern_detection["pattern_type"]
                    confidence = pattern_detection["confidence"]
                    if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                        pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），立即减仓10%-15%"
                    elif confidence >= 0.5:
                        pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓5%-10%"
                
                message = (
                    f"【趋势较强】连续{consecutive_above}天站上20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 观望，不新增仓位\n"
                    f"  • 逢高减仓10%-15%（{index_info['etf_code']}）\n"
                    f"  • 若收盘跌破10日均线，减仓30%\n"
                    f"{pattern_msg}\n"
                )
            # 场景C：偏离率＞+10%（超买风险）
            else:
                # 添加M头/头肩顶形态检测
                pattern_msg = ""
                if pattern_detection["detected"]:
                    pattern_name = pattern_detection["pattern_type"]
                    confidence = pattern_detection["confidence"]
                    if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                        pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），立即减仓20%-30%"
                    elif confidence >= 0.5:
                        pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓15%-25%"
                
                message = (
                    f"【超买风险】连续{consecutive_above}天站上20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 逢高减仓20%-30%（仅卫星ETF）\n"
                    f"  • 当前价格已处高位，避免新增仓位\n"
                    f"  • 等待偏离率回落至≤+5%（约{critical * 1.05:.2f}）时加回\n"
                    f"{pattern_msg}\n"
                )
    
    # 2. NO信号：当前价格 ＜ 20日均线
    else:
        # 计算亏损比例
        loss_percentage = calculate_loss_percentage(df)
        
        # 子条件1：首次跌破（价格刚跌穿均线，连续1-2日未收回+成交量放大）
        if consecutive_below == 1 and volume_change > 0.2:
            if loss_percentage > -15.0:  # 亏损<15%
                message = (
                    f"【首次跌破】连续{consecutive_below}天跌破20日均线，成交量放大{volume_change*100:.1f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 核心宽基ETF（{index_info['etf_code']}）立即减仓50%\n"
                    f"  • 卫星行业ETF立即减仓70%-80%\n"
                    f"  • 止损位：20日均线上方5%（约{critical * 1.05:.2f}）\n"
                    f"⚠️ 若收盘未收回均线，明日继续减仓至20%\n"
                )
            else:  # 亏损≥15%
                message = (
                    f"【首次跌破-严重亏损】连续{consecutive_below}天跌破20日均线，成交量放大{volume_change*100:.1f}%，亏损{loss_percentage:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 核心宽基ETF（{index_info['etf_code']}）立即清仓\n"
                    f"  • 卫星行业ETF保留20%-30%底仓观察\n"
                    f"  • 严格止损：收盘价站上20日均线才考虑回补\n"
                    f"⚠️ 重大亏损信号，避免盲目抄底\n"
                )
        # 子条件1：首次跌破（价格刚跌穿均线，连续1-2日未收回+成交量放大）
        elif consecutive_below == 2 and volume_change > 0.2:
            message = (
                f"【首次跌破确认】连续{consecutive_below}天跌破20日均线，成交量放大{volume_change*100:.1f}%\n"
                f"✅ 操作建议：\n"
                f"  • 核心宽基ETF（{index_info['etf_code']}）严格止损清仓\n"
                f"  • 卫星行业ETF仅保留20%-30%底仓\n"
                f"  • 严格止损：20日均线下方5%（约{critical * 0.95:.2f}）\n"
                f"⚠️ 信号确认，避免侥幸心理\n"
            )
        # 子条件2：持续跌破（价格维持在均线下）
        else:
            # 场景A：偏离率≥-5%（下跌初期）
            if deviation >= -5.0:
                message = (
                    f"【下跌初期】连续{consecutive_below}天跌破20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 轻仓观望（仓位≤20%）\n"
                    f"  • 反弹至均线附近（约{critical:.2f}）减仓剩余仓位\n"
                    f"  • 暂不考虑新增仓位\n"
                    f"⚠️ 重点观察：收盘站上5日均线，可轻仓试多\n"
                )
            # 场景B：-10%≤偏离率＜-5%（下跌中期）
            elif -10.0 <= deviation < -5.0:
                message = (
                    f"【下跌中期】连续{consecutive_below}天跌破20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 空仓为主，避免抄底\n"
                    f"  • 仅核心宽基ETF（{index_info['etf_code']}）可试仓5%-10%\n"
                    f"  • 严格止损：收盘跌破前低即离场\n"
                    f"⚠️ 重点观察：行业基本面是否有利空，有利空则清仓\n"
                )
            # 场景C：偏离率＜-10%（超卖机会）
            else:
                message = (
                    f"【超卖机会】连续{consecutive_below}天跌破20日均线，偏离率{deviation:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 核心宽基ETF（{index_info['etf_code']}）小幅加仓10%-15%\n"
                    f"  • 目标价：偏离率≥-5%（约{critical * 0.95:.2f}）\n"
                    f"  • 达到目标即卖出加仓部分\n"
                    f"⚠️ 重点观察：若跌破前低，立即止损\n"
                )
    
    return message

def generate_report():
    """生成策略报告并推送微信"""
    try:
        beijing_time = get_beijing_time()
        
        # 用于存储所有指数的简要信息，用于总结消息
        summary_lines = []
        valid_indices_count = 0
        
        # 直接按INDICES顺序遍历
        for idx in INDICES:
            code = idx["code"]
            name = idx["name"]
            
            # 直接从AkShare获取指数数据（不使用本地文件）
            df = fetch_index_data(code)
            if df.empty:
                logger.warning(f"无数据: {name}({code})")
                # 即使没有数据，也发送一条消息通知
                message_lines = []
                message_lines.append(f"{name} 【{code}；ETF：{idx['etf_code']}，{idx['description']}】\n")
                message_lines.append(f"📊 当前：数据获取失败 | 临界值：N/A | 偏离率：N/A\n")
                # 修正：错误信号类型显示问题
                message_lines.append(f"❌ 信号：数据获取失败\n")
                message_lines.append("──────────────────\n")
                message_lines.append("⚠️ 获取指数数据失败，请检查数据源\n")
                message_lines.append("──────────────────\n")
                message_lines.append(f"📅 计算时间: {beijing_time.strftime('%Y-%m-%d %H:%M')}\n")
                message_lines.append("📊 数据来源：GIT：fish-etf\n")
                
                message = "\n".join(message_lines)
                logger.info(f"推送 {name} 策略信号（数据获取失败）\n")
                send_wechat_message(message)
                time.sleep(1)
                continue
            
            # 确保有足够数据
            if len(df) < CRITICAL_VALUE_DAYS:
                logger.warning(f"指数 {name}({code}) 数据不足{CRITICAL_VALUE_DAYS}天，跳过计算\n")
                # 发送数据不足的消息
                message_lines = []
                message_lines.append(f"{name} 【{code}；ETF：{idx['etf_code']}，{idx['description']}】\n")
                message_lines.append(f"📊 当前：数据不足 | 临界值：N/A | 偏离率：N/A\n")
                # 修正：错误信号类型显示问题
                message_lines.append(f"⚠️ 信号：数据不足\n")
                message_lines.append("──────────────────\n")
                message_lines.append(f"⚠️ 需要至少{CRITICAL_VALUE_DAYS}天数据进行计算，当前只有{len(df)}天\n")
                message_lines.append("──────────────────\n")
                message_lines.append(f"📅 计算时间: {beijing_time.strftime('%Y-%m-%d %H:%M')}\n")
                message_lines.append("📊 数据来源：GIT：fish-etf\n")
                
                message = "\n".join(message_lines)
                logger.info(f"\n推送 {name} 策略信号（数据不足）\n")
                send_wechat_message(message)
                time.sleep(2)
                continue
            
            # 修复：确保获取标量值而不是Series
            # 使用.values[-1]确保获取标量值
            close_price = df['收盘'].values[-1]
            
            # 修复：确保critical_value是标量值
            critical_value = calculate_critical_value(df)
            # 如果返回的是Series，获取最后一个值
            if isinstance(critical_value, pd.Series):
                critical_value = critical_value.values[-1]
            # 如果返回的是DataFrame，获取最后一个值
            elif isinstance(critical_value, pd.DataFrame):
                critical_value = critical_value.iloc[-1, 0]
            
            # 修复：确保close_price和critical_value都是数值类型
            try:
                close_price = float(close_price)
                critical_value = float(critical_value)
            except (TypeError, ValueError) as e:
                logger.error(f"转换价格值失败: {str(e)}")
                continue
            
            # 计算偏离率
            deviation = calculate_deviation(close_price, critical_value)
            
            # 状态判断（收盘价在临界值之上为YES，否则为NO）
            # 修复：现在close_price和critical_value都是标量值，可以安全比较
            status = "YES" if close_price >= critical_value else "NO"
            
            # 生成详细策略信号
            signal_message = generate_signal_message(idx, df, close_price, critical_value, deviation)
            
            # 构建消息
            message_lines = []
            message_lines.append(f"{name} 【{code}；ETF：{idx['etf_code']}，{idx['description']}】\n")
            message_lines.append(f"📊 当前：{close_price:.2f} | 临界值：{critical_value:.2f} | 偏离率：{deviation:.2f}%\n")
            # 修正：根据信号类型选择正确的符号
            signal_symbol = "✅" if status == "YES" else "❌"
            message_lines.append(f"{signal_symbol} 信号：{status}\n")
            message_lines.append(signal_message)            
            message = "\n".join(message_lines)
            
            # 发送消息
            logger.info(f"推送 {name} 策略信号")
            send_wechat_message(message)
            
            # 添加到总结消息
            # 确保名称对齐 - 使用固定宽度
            name_padding = 10 if len(name) <= 4 else 8  # 中文名称通常2-4个字
            name_with_padding = f"{name}{' ' * (name_padding - len(name))}"
            
            # 修正：根据信号类型选择正确的符号
            signal_symbol = "✅" if status == "YES" else "❌"
            summary_line = f"{name_with_padding}【{code}；ETF：{idx['etf_code']}】{signal_symbol} 信号：{status} 📊 当前：{close_price:.2f} | 临界值：{critical_value:.2f} | 偏离率：{deviation:.2f}%\n"
            summary_lines.append(summary_line)
            
            valid_indices_count += 1
            time.sleep(1)
        
        # 如果有有效的指数数据，发送总结消息
        if valid_indices_count > 0:
            # 构建总结消息
            summary_message = "\n".join(summary_lines) 
            
            logger.info("推送总结消息")
            send_wechat_message(summary_message)
            time.sleep(1)
        
        logger.info(f"所有指数策略报告已成功发送至企业微信（共{valid_indices_count}个有效指数）")
    
    except Exception as e:
        logger.error(f"策略执行失败: {str(e)}", exc_info=True)
        # 修正：错误消息与正常信号消息分离
        send_wechat_message(f"🚨 【错误通知】策略执行异常: {str(e)}", message_type="error")

if __name__ == "__main__":
    logger.info("===== 开始执行 指数Yes/No策略 =====")
    
    # 添加延时，避免在每天23:00整点时AkShare接口可能还未更新当日数据
    time.sleep(30)
    
    generate_report()
    logger.info("=== 指数Yes/No策略执行完成 ===")
