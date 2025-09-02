#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
套利策略专用数据源模块
负责爬取ETF实时市场价格和IOPV(基金份额参考净值)
数据保存格式: data/arbitrage/YYYYMMDD.csv
特别设计了断点续爬机制和数据验证逻辑，确保数据质量
增强功能：支持手动测试模式，使用最近有效数据作为测试数据
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
from utils.date_utils import get_beijing_time, is_trading_day, is_trading_time
from utils.file_utils import ensure_dir_exists
from data_crawler.etf_list_manager import get_filtered_etf_codes, get_etf_name

# 初始化日志
logger = logging.getLogger(__name__)

def is_manual_trigger() -> bool:
    """
    判断是否是手动触发的任务
    
    Returns:
        bool: 如果是手动触发返回True，否则返回False
    """
    try:
        # 检查环境变量，GitHub Actions中手动触发会有特殊环境变量
        return os.environ.get('GITHUB_EVENT_NAME', '') == 'workflow_dispatch'
    except Exception as e:
        logger.error(f"检查是否为手动触发失败: {str(e)}", exc_info=True)
        return False


def fetch_arbitrage_realtime_data() -> pd.DataFrame:
    """
    爬取所有ETF的实时市场价格和IOPV数据
    
    Returns:
        pd.DataFrame: 包含ETF代码、名称、市场价格、IOPV等信息的DataFrame
    """
    try:
        logger.info("===== 开始执行套利数据爬取 =====")
        beijing_time = get_beijing_time()
        logger.info(f"当前北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 检查是否为交易日和交易时间
        if not is_trading_day():
            logger.warning("当前不是交易日，跳过套利数据爬取")
            return pd.DataFrame()
        
        # 检查是否为交易时间
        if not is_trading_time():
            logger.warning(f"当前不是交易时间 ({Config.TRADING_START_TIME} - {Config.TRADING_END_TIME})，跳过套利数据爬取")
            return pd.DataFrame()
        
        # 获取需要监控的ETF列表
        etf_codes = get_filtered_etf_codes()
        logger.info(f"获取到 {len(etf_codes)} 只符合条件的ETF进行套利监控")
        
        if not etf_codes:
            logger.warning("无符合条件的ETF，跳过套利数据爬取")
            return pd.DataFrame()
        
        # 爬取数据 - 使用单个API调用获取所有数据
        df = ak.fund_etf_spot_em()
        
        # 记录返回的列名
        logger.info(f"fund_etf_spot_em 接口返回列名: {df.columns.tolist()}")
        
        if df.empty:
            logger.error("AkShare未返回ETF实时行情数据")
            return pd.DataFrame()
        
        # 过滤出需要的ETF
        df = df[df['代码'].isin(etf_codes)]
        
        if df.empty:
            logger.warning("筛选后无符合条件的ETF数据")
            return pd.DataFrame()
        
        # 重命名列名以匹配我们的需求
        column_mapping = {
            '代码': 'ETF代码',
            '名称': 'ETF名称',
            '最新价': '市场价格',
            'IOPV实时估值': 'IOPV',
            '基金折价率': '折溢价率',
            '更新时间': '净值时间'
        }
        
        # 只保留我们需要的列
        available_columns = [col for col in column_mapping.keys() if col in df.columns]
        df = df[available_columns].rename(columns=column_mapping)
        
        # 添加计算时间
        df['计算时间'] = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # 如果没有直接提供折溢价率，我们自己计算
        if '折溢价率' not in df.columns and 'IOPV' in df.columns and '市场价格' in df.columns:
            df['折溢价率'] = df.apply(
                lambda row: calculate_premium_discount(row['市场价格'], row['IOPV']), 
                axis=1
            )
        
        logger.info(f"成功获取 {len(df)} 只ETF的套利数据")
        return df
    
    except Exception as e:
        logger.error(f"爬取套利实时数据过程中发生未预期错误: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_etf_realtime_data(etf_code: str) -> Optional[Dict[str, Any]]:
    """
    获取ETF实时行情数据（已不再需要，但保留以防其他模块调用）
    
    Args:
        etf_code: ETF代码
    
    Returns:
        Optional[Dict[str, Any]]: 实时行情数据
    """
    try:
        # 尝试使用AkShare获取实时数据
        df = ak.fund_etf_spot_em()
        
        # 过滤出特定ETF
        df = df[df['代码'] == etf_code]
        
        if df.empty:
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
            "总市值": float(latest["总市值"]) if "总市值" in latest else 0.0
        }
        
        logger.debug(f"获取ETF {etf_code} 实时行情成功")
        return realtime_data
    
    except Exception as e:
        logger.error(f"获取ETF {etf_code} 实时行情失败: {str(e)}", exc_info=True)
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

def crawl_arbitrage_data(is_manual: bool = False) -> str:
    """
    执行套利数据爬取并保存
    
    Args:
        is_manual: 是否是手动测试
    
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
            logger.warning("未获取到套利数据")
            # 如果是手动测试，尝试加载最近有效数据
            if is_manual:
                logger.info("【测试模式】爬取失败，尝试加载最近有效套利数据")
                df = load_latest_valid_arbitrage_data()
                if not df.empty:
                    # 添加测试标记
                    if "备注" not in df.columns:
                        df["备注"] = "【测试数据】使用历史数据"
                    else:
                        df["备注"] = df["备注"].fillna("【测试数据】使用历史数据")
                    logger.info(f"【测试模式】成功加载最近有效套利数据，共 {len(df)} 个机会")
        
        # 检查数据完整性
        if df.empty:
            logger.error("未获取到有效的套利数据，爬取结果为空")
            return ""
        
        # 确保包含必要列
        required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV", "折溢价率"]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"数据中缺少必要列: {col}")
                return ""
        
        # 保存数据
        return save_arbitrage_data(df)
    
    except Exception as e:
        logger.error(f"套利数据爬取任务执行失败: {str(e)}", exc_info=True)
        return ""

def get_latest_arbitrage_opportunities(is_manual: Optional[bool] = None) -> pd.DataFrame:
    """
    获取最新的套利机会
    
    Args:
        is_manual: 是否是手动测试模式（可选，如果不提供则自动检测）
    
    Returns:
        pd.DataFrame: 套利机会DataFrame
    """
    try:
        # 自动检测是否是手动触发
        if is_manual is None:
            is_manual = is_manual_trigger()
        
        # 获取当前日期
        today = get_beijing_time().strftime("%Y%m%d")
        
        # 尝试加载今天的套利数据
        df = load_arbitrage_data(today)
        
        # 检查是否在交易时间
        if not is_trading_time():
            if is_manual:
                logger.warning("【测试模式】当前不是交易时间，尝试使用最近有效数据")
                # 尝试加载最近一天的有效数据
                df = load_latest_valid_arbitrage_data()
                
                if not df.empty:
                    # 添加测试标记
                    if "备注" not in df.columns:
                        df["备注"] = "【测试数据】使用" + df["日期"].max() + "的数据"
                    else:
                        df["备注"] = df["备注"].fillna("【测试数据】使用" + df["日期"].max() + "的数据")
                    logger.info(f"【测试模式】成功加载最近有效套利数据（{df['日期'].max()}），共 {len(df)} 个机会")
                else:
                    logger.error("【测试模式】未找到有效的历史套利数据")
            else:
                logger.warning(f"当前不是交易时间 ({Config.TRADING_START_TIME} - {Config.TRADING_END_TIME})，跳过套利数据爬取")
                return pd.DataFrame()
        
        # 如果数据为空，尝试重新爬取（仅在交易时间或手动测试模式下）
        if df.empty and (is_trading_time() or is_manual):
            logger.warning("无今日套利数据，尝试重新爬取")
            file_path = crawl_arbitrage_data(is_manual=is_manual)
            
            # 详细检查爬取结果
            if file_path and os.path.exists(file_path):
                logger.info(f"成功爬取并保存套利数据到: {file_path}")
                df = load_arbitrage_data(today)
            else:
                logger.warning("重新爬取后仍无套利数据")
                # 如果是手动测试，尝试加载最近有效数据
                if is_manual:
                    logger.info("【测试模式】尝试加载最近有效套利数据")
                    df = load_latest_valid_arbitrage_data()
                    if not df.empty:
                        # 添加测试标记
                        if "备注" not in df.columns:
                            df["备注"] = "【测试数据】使用历史数据"
                        else:
                            df["备注"] = df["备注"].fillna("【测试数据】使用历史数据")
                        logger.info(f"【测试模式】成功加载最近有效套利数据，共 {len(df)} 个机会")
        
        # 检查数据完整性
        if df.empty:
            logger.error("加载的套利数据为空")
            return pd.DataFrame()
        
        # 确保包含必要列
        required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV", "折溢价率"]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"数据中缺少必要列: {col}")
                return pd.DataFrame()
        
        # 添加数据来源标记
        if is_manual:
            if "数据来源" not in df.columns:
                df["数据来源"] = "【测试数据】历史数据"
            else:
                df["数据来源"] = df["数据来源"].fillna("【测试数据】历史数据")
        else:
            if "数据来源" not in df.columns:
                df["数据来源"] = "实时数据"
            else:
                df["数据来源"] = df["数据来源"].fillna("实时数据")
        
        # 筛选有套利机会的数据
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
