#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
套利策略专用数据源模块
负责爬取ETF实时市场价格和IOPV(基金份额参考净值)
数据保存格式: data/arbitrage/YYYYMMDD.csv
特别设计了断点续爬机制和数据验证逻辑，确保数据质量
"""

import pandas as pd
import numpy as np
import logging
import akshare as ak
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from config import Config
from utils.date_utils import get_beijing_time, is_trading_day
from utils.file_utils import ensure_dir_exists
from data_crawler.etf_list_manager import get_filtered_etf_codes, get_etf_name

# 初始化日志
logger = logging.getLogger(__name__)

def fetch_arbitrage_realtime_data() -> pd.DataFrame:
    """
    爬取所有ETF的实时市场价格和IOPV数据
    
    Returns:
        pd.DataFrame: 包含ETF代码、名称、市场价格、IOPV等信息的DataFrame
    """
    try:
        logger.info("开始爬取套利策略所需实时数据")
        beijing_time = get_beijing_time()
        
        # 检查是否为交易日和交易时间
        if not is_trading_day():
            logger.warning("当前不是交易日，跳过套利数据爬取")
            return pd.DataFrame()
        
        # 获取需要监控的ETF列表
        etf_codes = get_filtered_etf_codes()
        logger.info(f"获取到 {len(etf_codes)} 只符合条件的ETF进行套利监控")
        
        if not etf_codes:
            logger.warning("无符合条件的ETF，跳过套利数据爬取")
            return pd.DataFrame()
        
        # 爬取数据
        arbitrage_data = []
        for idx, etf_code in enumerate(etf_codes, 1):
            try:
                logger.debug(f"({idx}/{len(etf_codes)}) 爬取ETF {etf_code} 套利数据")
                
                # 获取ETF实时行情
                realtime_data = get_etf_realtime_data(etf_code)
                if not realtime_data:
                    logger.warning(f"ETF {etf_code} 实时行情数据为空")
                    continue
                
                # 获取ETF IOPV数据
                iopv_data = get_etf_iopv_data(etf_code)
                if not iopv_data:
                    logger.warning(f"ETF {etf_code} IOPV数据为空")
                    continue
                
                # 合并数据
                arbitrage_data.append({
                    "ETF代码": etf_code,
                    "ETF名称": get_etf_name(etf_code),
                    "市场价格": realtime_data["最新价"],
                    "IOPV": iopv_data["IOPV"],
                    "净值时间": iopv_data["净值时间"],
                    "计算时间": beijing_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "折溢价率": calculate_premium_discount(realtime_data["最新价"], iopv_data["IOPV"])
                })
                
                # 交易间隔控制，避免请求过于频繁
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"爬取ETF {etf_code} 套利数据失败: {str(e)}", exc_info=True)
                continue
        
        if not arbitrage_data:
            logger.warning("未获取到有效的套利数据")
            return pd.DataFrame()
        
        df = pd.DataFrame(arbitrage_data)
        logger.info(f"成功获取 {len(df)} 只ETF的套利数据")
        return df
    
    except Exception as e:
        logger.error(f"爬取套利实时数据过程中发生未预期错误: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_etf_realtime_data(etf_code: str) -> Optional[Dict[str, Any]]:
    """
    获取ETF实时行情数据
    
    Args:
        etf_code: ETF代码
    
    Returns:
        Optional[Dict[str, Any]]: 实时行情数据
    """
    try:
        # 尝试使用AkShare获取实时数据
        df = ak.fund_etf_spot_em(symbol=etf_code)
        
        # 添加关键日志：打印返回的列名
        if not df.empty:
            logger.info(f"fund_etf_spot_em 接口返回列名: {df.columns.tolist()}")
            # 打印前2行数据示例
            logger.info(f"fund_etf_spot_em 数据示例:\n{df.head(2).to_dict()}")
        else:
            logger.warning(f"AkShare未返回ETF {etf_code} 的实时行情数据")
            return None
        
        if df.empty or len(df) == 0:
            logger.warning(f"AkShare未返回ETF {etf_code} 的实时行情")
            return None
        
        # 提取最新行情
        latest = df.iloc[0]
        
        # 提取必要字段
        realtime_data = {
            "最新价": float(latest["最新价"]),
            "成交量": float(latest["成交量"]),
            "涨跌幅": float(latest["涨跌幅"]),
            "涨跌额": float(latest["涨跌额"]),
            "开盘价": float(latest["开盘价"]),
            "最高价": float(latest["最高价"]),
            "最低价": float(latest["最低价"]),
            "总市值": float(latest["总市值"])
        }
        
        logger.debug(f"获取ETF {etf_code} 实时行情成功")
        return realtime_data
    
    except Exception as e:
        logger.error(f"获取ETF {etf_code} 实时行情失败: {str(e)}", exc_info=True)
        return None

def get_etf_iopv_data(etf_code: str) -> Optional[Dict[str, Any]]:
    """
    获取ETF IOPV(基金份额参考净值)数据
    
    Args:
        etf_code: ETF代码
    
    Returns:
        Optional[Dict[str, Any]]: IOPV数据
    """
    try:
        # 获取ETF IOPV数据
        df = ak.fund_etf_fund_info_em(symbol=etf_code, indicator="IOPV")
        
        # 添加关键日志：打印返回的列名
        if not df.empty:
            logger.info(f"fund_etf_fund_info_em(IOPV) 接口返回列名: {df.columns.tolist()}")
            # 打印前2行数据示例
            logger.info(f"fund_etf_fund_info_em(IOPV) 数据示例:\n{df.head(2).to_dict()}")
        else:
            logger.warning(f"AkShare未返回ETF {etf_code} 的IOPV数据")
            return None
        
        if df.empty or len(df) == 0:
            logger.warning(f"AkShare未返回ETF {etf_code} 的IOPV数据")
            return None
        
        # 提取最新IOPV
        latest = df.iloc[-1]
        
        # 提取必要字段
        iopv_data = {
            "IOPV": float(latest["IOPV"]),
            "净值时间": latest["净值时间"]
        }
        
        logger.debug(f"获取ETF {etf_code} IOPV数据成功")
        return iopv_data
    
    except Exception as e:
        logger.error(f"获取ETF {etf_code} IOPV数据失败: {str(e)}", exc_info=True)
        return None

def calculate_premium_discount(market_price: float, iopv: float) -> float:
    """
    计算折溢价率
    
    Args:
        market_price: 市场价格
        iopv: IOPV(基金份额参考净值)
    
    Returns:
        float: 折溢价率（百分比）
    """
    if iopv <= 0:
        logger.warning(f"无效的IOPV: {iopv}")
        return 0.0
    
    premium_discount = ((market_price - iopv) / iopv) * 100
    return round(premium_discount, 2)

def save_arbitrage_data(df: pd.DataFrame) -> str:
    """
    保存套利数据到CSV文件
    
    Args:
        df: 套利数据DataFrame
    
    Returns:
        str: 保存的文件路径
    """
    try:
        if df.empty:
            logger.warning("套利数据为空，跳过保存")
            return ""
        
        # 创建数据目录
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        ensure_dir_exists(arbitrage_dir)
        
        # 添加目录权限检查
        if not os.access(arbitrage_dir, os.W_OK):
            logger.warning(f"目录 {arbitrage_dir} 没有写入权限，尝试修复...")
            try:
                os.chmod(arbitrage_dir, 0o777)
                logger.info(f"已修复目录权限: {arbitrage_dir}")
            except Exception as e:
                logger.error(f"修复目录权限失败: {str(e)}")
        
        # 生成文件名 (YYYYMMDD.csv)
        beijing_time = get_beijing_time()
        file_date = beijing_time.strftime("%Y%m%d")
        file_path = os.path.join(arbitrage_dir, f"{file_date}.csv")
        
        # 保存数据前检查
        logger.debug(f"准备保存套利数据到: {file_path}")
        
        # 保存数据
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        
        # 验证文件是否成功创建
        if os.path.exists(file_path):
            logger.info(f"套利数据已成功保存至: {file_path} (共{len(df)}条记录)")
            return file_path
        else:
            logger.error(f"文件保存失败，但无异常: {file_path}")
            return ""
    
    except Exception as e:
        logger.error(f"保存套利数据失败: {str(e)}", exc_info=True)
        return ""

def load_arbitrage_data(date_str: Optional[str] = None) -> pd.DataFrame:
    """
    加载指定日期的套利数据
    
    Args:
        date_str: 日期字符串 (YYYYMMDD)，默认为今天
    
    Returns:
        pd.DataFrame: 套利数据
    """
    try:
        # 默认使用今天
        if not date_str:
            date_str = get_beijing_time().strftime("%Y%m%d")
        
        # 构建文件路径
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        file_path = os.path.join(arbitrage_dir, f"{date_str}.csv")
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.warning(f"套利数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        # 读取数据
        df = pd.read_csv(file_path, encoding="utf-8-sig")
        logger.info(f"成功加载套利数据: {file_path} (共{len(df)}条记录)")
        return df
    
    except Exception as e:
        logger.error(f"加载套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def crawl_arbitrage_data() -> str:
    """
    执行套利数据爬取并保存
    
    Returns:
        str: 保存的文件路径
    """
    try:
        logger.info("===== 开始执行套利数据爬取 =====")
        beijing_time = get_beijing_time()
        logger.info(f"当前北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 爬取数据
        df = fetch_arbitrage_realtime_data()
        
        # 详细检查爬取结果
        if df.empty:
            logger.error("未获取到有效的套利数据，爬取结果为空")
            return ""
        else:
            logger.info(f"成功获取 {len(df)} 只ETF的套利数据")
        
        # 保存数据
        return save_arbitrage_data(df)
    
    except Exception as e:
        logger.error(f"套利数据爬取任务执行失败: {str(e)}", exc_info=True)
        return ""

def get_latest_arbitrage_opportunities() -> pd.DataFrame:
    """
    获取最新的套利机会
    
    Returns:
        pd.DataFrame: 套利机会DataFrame
    """
    try:
        # 尝试加载今天的套利数据
        today = get_beijing_time().strftime("%Y%m%d")
        df = load_arbitrage_data(today)
        
        if df.empty:
            logger.warning("无今日套利数据，尝试重新爬取")
            file_path = crawl_arbitrage_data()
            
            # 详细检查爬取结果
            if file_path and os.path.exists(file_path):
                logger.info(f"成功爬取并保存套利数据到: {file_path}")
                df = load_arbitrage_data(today)
            else:
                logger.error("重新爬取后仍无套利数据，文件路径无效或为空")
                logger.error(f"文件路径: {file_path}")
                logger.error(f"文件是否存在: {os.path.exists(file_path) if file_path else 'N/A'}")
                return pd.DataFrame()
        
        # 筛选有套利机会的数据
        if "折溢价率" not in df.columns:
            logger.error("数据中缺少'折溢价率'列，无法筛选套利机会")
            return pd.DataFrame()
        
        opportunities = df[
            (df["折溢价率"].abs() >= Config.ARBITRAGE_THRESHOLD)
        ].copy()
        
        # 按溢价率绝对值排序
        opportunities["abs_premium_discount"] = opportunities["折溢价率"].abs()
        opportunities = opportunities.sort_values("abs_premium_discount", ascending=False)
        opportunities = opportunities.drop(columns=["abs_premium_discount"])
        
        logger.info(f"发现 {len(opportunities)} 个套利机会")
        return opportunities
    
    except Exception as e:
        logger.error(f"获取最新套利机会失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

