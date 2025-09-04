#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
套利策略专用数据源模块
负责爬取ETF实时市场价格和IOPV(基金份额参考净值)
数据保存格式: data/arbitrage/YYYYMMDD.csv
增强功能：增量保存数据、自动清理过期数据、支持新系统无历史数据场景
"""

import pandas as pd
import numpy as np
import logging
import akshare as ak
import os
import time
import datetime
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from config import Config
from utils.date_utils import get_beijing_time, is_trading_day, is_trading_time
from utils.file_utils import ensure_dir_exists
from data_crawler.etf_list_manager import get_filtered_etf_codes, get_etf_name, load_all_etf_list  # 修改：添加load_all_etf_list导入

# 初始化日志
logger = logging.getLogger(__name__)

def clean_old_arbitrage_data(days_to_keep: int = 7) -> None:
    """
    清理超过指定天数的套利数据文件（仅清理实时行情数据，不清理交易流水）
    
    Args:
        days_to_keep: 保留天数
    """
    try:
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        if not os.path.exists(arbitrage_dir):
            logger.info("套利数据目录不存在，无需清理")
            return
        
        # 获取当前日期（仅日期部分，不包含时间）
        current_date = get_beijing_time().date()
        logger.info(f"清理旧套利数据：保留最近 {days_to_keep} 天的数据")
        logger.info(f"当前日期: {current_date}")
        
        # 遍历目录中的所有文件
        files_to_keep = []
        files_to_delete = []
        
        for file_name in os.listdir(arbitrage_dir):
            if not file_name.endswith(".csv"):
                continue
                
            # 提取文件日期
            try:
                file_date_str = file_name.split(".")[0]
                file_date = datetime.strptime(file_date_str, "%Y%m%d").date()
                
                # 计算日期差（仅比较日期，不考虑时间）
                days_diff = (current_date - file_date).days
                
                # 记录详细信息
                logger.debug(f"检查文件: {file_name}, 文件日期: {file_date}, 日期差: {days_diff}天")
                
                # 判断是否删除
                if days_diff > days_to_keep:
                    files_to_delete.append((file_name, file_date, days_diff))
                else:
                    files_to_keep.append((file_name, file_date, days_diff))
            except (ValueError, TypeError) as e:
                logger.warning(f"解析文件日期失败: {file_name}, 错误: {str(e)}")
                continue
        
        # 删除超过保留天数的文件
        for file_name, file_date, days_diff in files_to_delete:
            file_path = os.path.join(arbitrage_dir, file_name)
            try:
                os.remove(file_path)
                logger.info(f"已删除旧套利数据文件: {file_name} (文件日期: {file_date}, 超期: {days_diff - days_to_keep}天)")
            except Exception as e:
                logger.error(f"删除文件失败: {file_path}, 错误: {str(e)}")
        
        # 记录保留的文件
        logger.info(f"保留套利数据文件: {len(files_to_keep)} 个")
        if files_to_keep:
            logger.debug("保留的文件列表:")
            for file_name, file_date, days_diff in files_to_keep:
                logger.debug(f"  - {file_name} (文件日期: {file_date}, 剩余保留天数: {days_to_keep - days_diff}天)")
        
        # 记录删除的文件
        logger.info(f"已删除套利数据文件: {len(files_to_delete)} 个")
        if files_to_delete:
            logger.debug("已删除的文件列表:")
            for file_name, file_date, days_diff in files_to_delete:
                logger.debug(f"  - {file_name} (文件日期: {file_date}, 超期: {days_diff - days_to_keep}天)")
    
    except Exception as e:
        logger.error(f"清理旧套利数据失败: {str(e)}", exc_info=True)

def append_arbitrage_data(df: pd.DataFrame) -> str:
    """
    增量保存套利数据到CSV文件
    
    Args:
        df: 套利数据DataFrame
    
    Returns:
        str: 保存的文件路径
    """
    try:
        if df.empty:
            logger.warning("套利数据为空，跳过保存")
            return ""
        
        # 添加时间戳列
        if "timestamp" not in df.columns:
            df["timestamp"] = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        
        # 创建数据目录
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        ensure_dir_exists(arbitrage_dir)
        
        # 生成文件名 (YYYYMMDD.csv)
        beijing_time = get_beijing_time()
        file_date = beijing_time.strftime("%Y%m%d")
        file_path = os.path.join(arbitrage_dir, f"{file_date}.csv")
        
        # 保存数据 - 增量追加模式
        if os.path.exists(file_path):
            # 读取现有数据
            existing_df = pd.read_csv(file_path, encoding="utf-8-sig")
            # 合并数据
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            # 去重（基于ETF代码和时间戳）
            combined_df = combined_df.drop_duplicates(
                subset=["ETF代码", "timestamp"], 
                keep="last"
            )
            # 保存合并后的数据
            combined_df.to_csv(file_path, index=False, encoding="utf-8-sig")
        else:
            # 创建新文件
            df.to_csv(file_path, index=False, encoding="utf-8-sig")
        
        logger.info(f"套利数据已增量保存至: {file_path} (新增{len(df)}条记录)")
        return file_path
    
    except Exception as e:
        logger.error(f"增量保存套利数据失败: {str(e)}", exc_info=True)
        return ""

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
        
        # 检查是否为交易日
        if not is_trading_day():
            logger.warning("当前不是交易日")
        
        # 检查是否为交易时间
        current_time = beijing_time.time()
        trading_start = datetime.strptime(Config.TRADING_START_TIME, "%H:%M").time()
        trading_end = datetime.strptime(Config.TRADING_END_TIME, "%H:%M").time()
        
        if not (trading_start <= current_time <= trading_end):
            logger.warning(f"当前不是交易时间 ({trading_start} - {trading_end})")
        
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
        
        # 修复：移除所有计算逻辑，只返回原始数据
        # 不再计算折溢价率，这部分逻辑应该在策略层处理
        
        logger.info(f"成功获取 {len(df)} 只ETF的实时数据")
        return df
    
    except Exception as e:
        logger.error(f"爬取套利实时数据过程中发生未预期错误: {str(e)}", exc_info=True)
        return pd.DataFrame()

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
            logger.debug(f"套利数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        # 读取数据
        df = pd.read_csv(file_path, encoding="utf-8-sig")
        logger.debug(f"成功加载套利数据: {file_path} (共{len(df)}条记录)")
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
            logger.info(f"成功获取 {len(df)} 只ETF的实时数据")
        
        # 增量保存数据
        return append_arbitrage_data(df)
    
    except Exception as e:
        logger.error(f"套利数据爬取任务执行失败: {str(e)}", exc_info=True)
        return ""

def get_latest_arbitrage_opportunities() -> pd.DataFrame:
    """
    获取最新的套利机会数据（原始数据）
    
    Returns:
        pd.DataFrame: 原始套利数据，不做任何筛选和排序
    """
    try:
        # 尝试加载今天的套利数据
        today = get_beijing_time().strftime("%Y%m%d")
        df = load_arbitrage_data(today)
        
        # 如果数据为空，尝试重新爬取
        if df.empty:
            logger.warning("无今日套利数据，尝试重新爬取")
            file_path = crawl_arbitrage_data()
            
            # 详细检查爬取结果
            if file_path and os.path.exists(file_path):
                logger.info(f"成功爬取并保存套利数据到: {file_path}")
                df = load_arbitrage_data(today)
            else:
                logger.warning("重新爬取后仍无套利数据")
        
        # 检查数据完整性
        if df.empty:
            logger.warning("加载的套利数据为空，将尝试加载最近有效数据")
            df = load_latest_valid_arbitrage_data()
        
        # 检查数据完整性
        if df.empty:
            logger.error("无法获取任何有效的套利数据")
            return pd.DataFrame()
        
        # 修复：不再检查"折溢价率"列，因为数据源可能不提供该列
        # 只检查必要列是否存在
        required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"数据中缺少必要列: {', '.join(missing_columns)}")
            # 记录实际存在的列
            logger.debug(f"实际列名: {list(df.columns)}")
            return pd.DataFrame()
        
        # 新增：从all_etfs.csv获取上市日期
        try:
            etf_list = load_all_etf_list()
            if not etf_list.empty and "上市日期" in etf_list.columns:
                # 创建ETF代码到上市日期的映射
                listing_date_map = dict(zip(etf_list["ETF代码"], etf_list["上市日期"]))
                
                # 添加上市日期列
                df["上市日期"] = df["ETF代码"].map(listing_date_map)
                
                # 处理缺失值
                if "上市日期" in df.columns:
                    df["上市日期"] = df["上市日期"].fillna("")
                    
                logger.debug(f"已从all_etfs.csv添加上市日期信息，共{len(df)}条记录")
            else:
                logger.warning("无法从all_etfs.csv获取上市日期信息")
                # 确保上市日期列存在，即使为空
                df["上市日期"] = ""
        except Exception as e:
            logger.error(f"获取上市日期信息失败: {str(e)}，将添加空列", exc_info=True)
            df["上市日期"] = ""
        
        # 修复：不再进行筛选和排序，只返回原始数据
        logger.info(f"成功加载 {len(df)} 条原始套利数据")
        return df
    
    except Exception as e:
        logger.error(f"获取最新套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def load_latest_valid_arbitrage_data(days_back: int = 7) -> pd.DataFrame:
    """
    加载最近有效的套利数据
    
    Args:
        days_back: 向前查找的天数
    
    Returns:
        pd.DataFrame: 最近有效的套利数据
    """
    try:
        beijing_now = get_beijing_time()
        
        # 从今天开始向前查找
        for i in range(days_back):
            date = (beijing_now - timedelta(days=i)).strftime("%Y%m%d")
            df = load_arbitrage_data(date)
            
            # 检查数据是否有效
            if not df.empty:
                # 检查是否包含必要列
                required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV"]
                if all(col in df.columns for col in required_columns) and len(df) > 0:
                    logger.info(f"找到有效历史套利数据: {date}, 共 {len(df)} 个机会")
                    return df
        
        logger.warning(f"在最近 {days_back} 天内未找到有效的套利数据")
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"加载最近有效套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 初始化日志
    logger.info("套利数据源模块初始化完成")
    
    # 清理过期的套利数据
    try:
        clean_old_arbitrage_data(days_to_keep=7)
        logger.info("已清理超过7天的套利数据文件")
    except Exception as e:
        logger.error(f"清理旧套利数据文件失败: {str(e)}", exc_info=True)
    
except Exception as e:
    error_msg = f"套利数据源模块初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
    
    try:
        # 退回到基础日志配置
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(error_msg)
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(error_msg)
