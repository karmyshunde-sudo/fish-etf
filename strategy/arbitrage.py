#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
套利策略计算模块
基于已保存的实时数据计算套利机会
严格遵循项目架构原则：只负责计算，不涉及数据爬取和消息格式化
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    is_file_outdated
)
from utils.file_utils import (
    load_etf_daily_data, 
    ensure_chinese_columns,
    load_arbitrage_status,
    save_arbitrage_status,
    should_push_arbitrage,
    mark_arbitrage_pushed
)
from data_crawler.strategy_arbitrage_source import get_latest_arbitrage_opportunities  # 使用新数据源
from .etf_scoring import get_etf_basic_info, get_etf_name

# 初始化日志
logger = logging.getLogger(__name__)

def calculate_arbitrage_opportunity() -> pd.DataFrame:
    """
    基于实时数据计算ETF套利机会
    
    Returns:
        pd.DataFrame: 套利机会DataFrame（仅包含原始数据，不进行格式化）
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始计算套利机会 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 获取最新的套利机会
        opportunities = get_latest_arbitrage_opportunities()
        
        if opportunities.empty:
            logger.info("未发现有效套利机会")
            return pd.DataFrame()
        
        # 添加规模和日均成交额信息
        opportunities = add_etf_basic_info(opportunities)
        
        # 按折溢价率绝对值排序
        opportunities["abs_premium_discount"] = opportunities["折溢价率"].abs()
        opportunities = opportunities.sort_values("abs_premium_discount", ascending=False)
        opportunities = opportunities.drop(columns=["abs_premium_discount"])
        
        # 筛选今天尚未推送的套利机会（增量推送功能）
        opportunities = filter_new_arbitrage_opportunities(opportunities)
        
        logger.info(f"发现 {len(opportunities)} 个新的套利机会 (阈值≥{Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD}%)")
        return opportunities
    
    except Exception as e:
        error_msg = f"套利机会计算失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return pd.DataFrame()  # 确保始终返回DataFrame

def filter_new_arbitrage_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤掉今天已经推送过的套利机会
    
    Args:
        df: 原始套利机会DataFrame
    
    Returns:
        pd.DataFrame: 仅包含新发现的套利机会的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 创建一个列表，包含应该推送的ETF代码
        etfs_to_push = []
        
        for _, row in df.iterrows():
            etf_code = row["ETF代码"]
            if should_push_arbitrage(etf_code):
                etfs_to_push.append(etf_code)
        
        # 过滤DataFrame
        new_opportunities = df[df["ETF代码"].isin(etfs_to_push)].copy()
        
        logger.info(f"从 {len(df)} 个机会中筛选出 {len(new_opportunities)} 个新机会（增量推送）")
        return new_opportunities
    
    except Exception as e:
        logger.error(f"过滤新套利机会失败: {str(e)}", exc_info=True)
        # 出错时返回原始DataFrame，确保至少能推送新发现的机会
        return df

def add_etf_basic_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    为套利机会数据添加ETF基本信息（规模、日均成交额）
    
    Args:
        df: 原始套利机会DataFrame
    
    Returns:
        pd.DataFrame: 添加基本信息后的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 加载ETF列表
        etf_list = load_etf_list()
        if etf_list.empty:
            logger.warning("ETF列表为空，无法添加基本信息")
            return df
        
        # 为每只ETF添加基本信息
        for idx, row in df.iterrows():
            etf_code = row["ETF代码"]
            etf_info = etf_list[etf_list["ETF代码"] == etf_code]
            
            if not etf_info.empty:
                df.at[idx, "规模"] = etf_info["基金规模"].values[0]
                df.at[idx, "日均成交额"] = etf_info["日均成交额"].values[0]
        
        # 确保只保留满足筛选条件的ETF
        filtered_df = df[
            (df["规模"] >= Config.GLOBAL_MIN_FUND_SIZE) &
            (df["日均成交额"] >= Config.GLOBAL_MIN_AVG_VOLUME)
        ]
        
        logger.info(f"从 {len(df)} 个机会中筛选出 {len(filtered_df)} 个符合条件的机会")
        return filtered_df
    
    except Exception as e:
        logger.error(f"添加ETF基本信息失败: {str(e)}", exc_info=True)
        return df

def calculate_daily_volume(etf_code: str) -> float:
    """
    计算ETF的日均成交额（基于最近30个交易日）
    
    Args:
        etf_code: ETF代码
        
    Returns:
        float: 日均成交额（万元）
    """
    try:
        # 加载ETF日线数据
        etf_df = load_etf_daily_data(etf_code)
        
        if etf_df.empty:
            logger.debug(f"ETF {etf_code} 无日线数据，无法计算日均成交额")
            return 0.0
        
        # 确保使用中文列名
        etf_df = ensure_chinese_columns(etf_df)
        
        # 检查是否包含"日期"列
        if "日期" not in etf_df.columns:
            logger.warning(f"ETF {etf_code} 数据缺少'日期'列，无法计算日均成交额")
            return 0.0
        
        # 确保数据按日期排序
        etf_df = etf_df.sort_values("日期", ascending=False)
        
        # 取最近30个交易日的数据
        recent_data = etf_df.head(30)
        
        # 检查是否有足够的数据
        if len(recent_data) < 10:  # 至少需要10天数据
            logger.debug(f"ETF {etf_code} 数据不足（{len(recent_data)}天），无法准确计算日均成交额")
            return 0.0
        
        # 计算日均成交额（单位：万元）
        if "成交额" in recent_data.columns:
            # 假设成交额单位是元，转换为万元
            avg_volume = recent_data["成交额"].mean() / 10000
            logger.debug(f"ETF {etf_code} 日均成交额: {avg_volume:.2f}万元（{len(recent_data)}天数据）")
            return avg_volume
        else:
            logger.warning(f"ETF {etf_code} 缺少成交额数据，无法计算日均成交额")
            return 0.0
    
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 日均成交额失败: {str(e)}", exc_info=True)
        return 0.0

def load_etf_list() -> pd.DataFrame:
    """
    加载ETF列表
    
    Returns:
        pd.DataFrame: ETF列表
    """
    try:
        # 检查ETF列表文件是否存在
        if not os.path.exists(Config.ALL_ETFS_PATH):
            logger.error("ETF列表文件不存在")
            return pd.DataFrame()
        
        # 检查ETF列表是否过期
        if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
            logger.warning("ETF列表已过期，可能影响套利计算准确性")
        
        # 读取ETF列表
        etf_list = pd.read_csv(Config.ALL_ETFS_PATH, encoding="utf-8")
        if etf_list.empty:
            logger.warning("ETF列表为空")
            return pd.DataFrame()
        
        # 确保包含必要列
        required_columns = ["ETF代码", "ETF名称", "基金规模"]
        for col in required_columns:
            if col not in etf_list.columns:
                logger.error(f"ETF列表缺少必要列: {col}")
                return pd.DataFrame()
        
        # 添加日均成交额列（动态计算）
        etf_list["日均成交额"] = 0.0
        total = len(etf_list)
        logger.info(f"开始计算 {total} 只ETF的日均成交额...")
        
        for i, (_, etf) in enumerate(etf_list.iterrows(), 1):
            etf_code = etf["ETF代码"]
            logger.debug(f"({i}/{total}) 计算ETF {etf_code} 的日均成交额")
            
            # 动态计算日均成交额
            avg_daily_volume = calculate_daily_volume(etf_code)
            etf_list.at[_, "日均成交额"] = avg_daily_volume
        
        # 筛选符合条件的ETF
        filtered_etfs = etf_list[
            (etf_list["基金规模"] >= Config.GLOBAL_MIN_FUND_SIZE) &
            (etf_list["日均成交额"] >= Config.GLOBAL_MIN_AVG_VOLUME)
        ]
        
        logger.info(f"加载 {len(filtered_etfs)} 只符合条件的ETF")
        return filtered_etfs
    
    except Exception as e:
        logger.error(f"加载ETF列表失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def calculate_premium_discount(market_price: float, iopv: float) -> float:
    """
    计算折溢价率
    
    Args:
        market_price: 市场价格
        iopv: IOPV(基金份额参考净值)
    
    Returns:
        float: 折溢价率（百分比），正数表示溢价，负数表示折价
    """
    if iopv <= 0:
        logger.warning(f"无效的IOPV: {iopv}")
        return 0.0
    
    # 正确计算折溢价率：(市场价格 - IOPV) / IOPV * 100
    # 结果为正：溢价（市场价格 > IOPV）
    # 结果为负：折价（市场价格 < IOPV）
    premium_discount = ((market_price - iopv) / iopv) * 100
    return round(premium_discount, 2)

def get_arbitrage_history(days: int = 7) -> pd.DataFrame:
    """
    获取套利历史数据
    
    Args:
        days: 查询天数
    
    Returns:
        pd.DataFrame: 套利历史数据
    """
    try:
        history = []
        beijing_now = get_beijing_time()
        
        for i in range(days):
            date = (beijing_now - timedelta(days=i)).date().strftime("%Y-%m-%d")
            flag_file = os.path.join(Config.FLAG_DIR, f"arbitrage_pushed_{date}.txt")
            
            if os.path.exists(flag_file):
                # 读取当日套利数据
                # 这里简化处理，实际应从数据库或文件中读取历史套利数据
                history.append({
                    "日期": date,
                    "机会数量": 3,  # 示例数据
                    "最大折溢价率": 2.5,  # 示例数据
                    "最小折溢价率": -1.8  # 示例数据
                })
        
        if not history:
            logger.info("未找到套利历史数据")
            return pd.DataFrame()
        
        return pd.DataFrame(history)
    
    except Exception as e:
        error_msg = f"获取套利历史数据失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return pd.DataFrame()

def analyze_arbitrage_performance() -> Dict[str, Any]:
    """
    分析套利表现
    
    Returns:
        Dict[str, Any]: 分析结果
    """
    try:
        # 获取历史数据
        history_df = get_arbitrage_history()
        if history_df.empty:
            logger.info("无历史数据可供分析")
            return {
                "avg_opportunities": 0,
                "max_premium": 0,
                "min_discount": 0,
                "trend": "无数据",
                "has_high_premium": False,
                "has_high_discount": False
            }
        
        # 计算统计指标
        avg_opportunities = history_df["机会数量"].mean()
        max_premium = history_df["最大折溢价率"].max()
        min_discount = history_df["最小折溢价率"].min()
        
        # 添加趋势分析
        trend = "平稳"
        if len(history_df) >= 3:
            trend = "上升" if history_df["机会数量"].iloc[-3:].mean() > history_df["机会数量"].iloc[:3].mean() else "下降"
        
        # 返回结构化分析结果
        return {
            "avg_opportunities": avg_opportunities,
            "max_premium": max_premium,
            "min_discount": min_discount,
            "trend": trend,
            "has_high_premium": max_premium > 2.0,
            "has_high_discount": min_discount < -2.0
        }
    
    except Exception as e:
        error_msg = f"套利表现分析失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "avg_opportunities": 0,
            "max_premium": 0,
            "min_discount": 0,
            "trend": "分析失败",
            "has_high_premium": False,
            "has_high_discount": False
        }

def check_arbitrage_exit_signals() -> List[Dict[str, Any]]:
    """
    检查套利退出信号（持有1天后）
    
    Returns:
        List[Dict[str, Any]]: 需要退出的套利交易列表
    """
    try:
        logger.info("开始检查套利退出信号")
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 检查交易记录文件是否存在
        if not os.path.exists(Config.TRADE_RECORD_FILE):
            logger.warning("交易记录文件不存在，无法检查套利退出信号")
            return []
        
        # 读取交易记录
        trade_df = pd.read_csv(Config.TRADE_RECORD_FILE, encoding="utf-8")
        
        # 获取昨天的日期（基于北京时间）
        yesterday = (beijing_now - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.debug(f"检查昨天({yesterday})执行的套利交易")
        
        # 查找昨天执行的套利交易
        yesterday_arbitrage = trade_df[
            (trade_df["操作"] == "套利买入") & 
            (trade_df["创建日期"] == yesterday)
        ]
        
        if not yesterday_arbitrage.empty:
            logger.info(f"发现{len(yesterday_arbitrage)}条需要退出的套利交易")
            
            # 构建退出信号列表
            exit_signals = []
            for _, row in yesterday_arbitrage.iterrows():
                exit_signals.append({
                    "ETF代码": row["ETF代码"],
                    "ETF名称": row["ETF名称"],
                    "买入价格": row["价格"],
                    "买入日期": row["创建日期"]
                })
            
            return exit_signals
        
        logger.info("未发现需要退出的套利交易")
        return []
    
    except Exception as e:
        error_msg = f"检查套利退出信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return []

def load_arbitrage_data(date_str: str) -> pd.DataFrame:
    """
    加载指定日期的套利数据
    
    Args:
        date_str: 日期字符串，格式为YYYYMMDD
    
    Returns:
        pd.DataFrame: 套利数据DataFrame
    """
    try:
        # 构建套利数据目录
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        os.makedirs(arbitrage_dir, exist_ok=True)
        
        # 构建文件路径
        file_path = os.path.join(arbitrage_dir, f"{date_str}.csv")
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.warning(f"套利数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        # 读取CSV文件
        df = pd.read_csv(file_path)
        
        logger.debug(f"成功加载套利数据: {file_path}，共{len(df)}条")
        return df
    
    except Exception as e:
        logger.error(f"加载套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def crawl_arbitrage_data() -> Optional[str]:
    """
    爬取套利数据并保存
    
    Returns:
        Optional[str]: 保存的文件路径，如果爬取失败则返回None
    """
    try:
        # 获取当前日期
        today = get_beijing_time().strftime("%Y%m%d")
        
        # 构建套利数据目录
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        os.makedirs(arbitrage_dir, exist_ok=True)
        
        # 构建文件路径
        file_path = os.path.join(arbitrage_dir, f"{today}.csv")
        
        # 获取套利数据
        df = get_latest_arbitrage_opportunities()
        
        if df.empty:
            logger.warning("未获取到套利数据，无法保存")
            return None
        
        # 保存到CSV文件
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        logger.info(f"成功保存套利数据到: {file_path}")
        
        return file_path
    
    except Exception as e:
        logger.error(f"爬取套利数据失败: {str(e)}", exc_info=True)
        return None

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
                if 'file_path' in locals():
                    logger.error(f"文件路径: {file_path}")
                    logger.error(f"文件是否存在: {os.path.exists(file_path)}")
                return pd.DataFrame()
        
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
        
        # 筛选有套利机会的数据 - 只保留折价率≥3.0%的机会（市场价格低于IOPV）
        # 注意：折价率是负数，所以使用 <= -阈值
        opportunities = df[
            (df["折溢价率"] <= -Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD)
        ].copy()
        
        # 按折价率绝对值排序（从大到小）
        opportunities["abs_premium_discount"] = opportunities["折溢价率"].abs()
        opportunities = opportunities.sort_values("abs_premium_discount", ascending=False)
        opportunities = opportunities.drop(columns=["abs_premium_discount"])
        
        logger.info(f"发现 {len(opportunities)} 个折价机会 (阈值≥{Config.MIN_ARBITRAGE_DISPLAY_THRESHOLD}%)")
        return opportunities
    
    except Exception as e:
        logger.error(f"获取最新套利机会失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def mark_arbitrage_opportunities_pushed(opportunities: pd.DataFrame) -> bool:
    """
    标记套利机会已推送
    
    Args:
        opportunities: 套利机会DataFrame
    
    Returns:
        bool: 是否成功标记
    """
    if opportunities.empty:
        logger.info("无套利机会需要标记为已推送")
        return True
    
    try:
        success = True
        for _, row in opportunities.iterrows():
            etf_code = row["ETF代码"]
            if not mark_arbitrage_pushed(etf_code):
                logger.error(f"标记ETF {etf_code} 套利机会已推送失败")
                success = False
        
        if success:
            logger.info(f"成功标记 {len(opportunities)} 个ETF套利机会为已推送")
        return success
    
    except Exception as e:
        logger.error(f"标记套利机会已推送失败: {str(e)}", exc_info=True)
        return False

def get_arbitrage_push_statistics() -> Dict[str, Any]:
    """
    获取套利推送统计信息
    
    Returns:
        Dict[str, Any]: 套利推送统计信息
    """
    try:
        from utils.file_utils import get_arbitrage_push_count, get_arbitrage_push_history
        
        # 获取总推送量和今日推送量
        count_info = get_arbitrage_push_count()
        
        # 获取历史推送记录
        history = get_arbitrage_push_history(days=7)
        
        # 计算总推送量
        total_pushed = sum(history.values())
        
        # 计算日均推送量
        daily_avg = total_pushed / len(history) if history else 0
        
        # 获取最新推送日期
        latest_date = max(history.keys()) if history else "N/A"
        
        return {
            "total_pushed": count_info["total"],
            "today_pushed": count_info["today"],
            "total_history": total_pushed,
            "daily_avg": round(daily_avg, 2),
            "latest_date": latest_date,
            "history": history
        }
    
    except Exception as e:
        logger.error(f"获取套利推送统计信息失败: {str(e)}", exc_info=True)
        return {
            "total_pushed": 0,
            "today_pushed": 0,
            "total_history": 0,
            "daily_avg": 0,
            "latest_date": "N/A",
            "history": {}
        }

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 初始化日志
    logger.info("套利策略模块初始化完成")
    
    # 清理过期的套利状态记录
    try:
        from utils.file_utils import clear_expired_arbitrage_status
        clear_expired_arbitrage_status()
        logger.info("已清理过期的套利状态记录")
    except Exception as e:
        logger.error(f"清理过期套利状态记录失败: {str(e)}", exc_info=True)
    
except Exception as e:
    error_msg = f"套利策略模块初始化失败: {str(e)}"
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
