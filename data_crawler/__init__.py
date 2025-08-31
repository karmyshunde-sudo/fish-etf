#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF数据爬取模块
提供ETF日线数据爬取、ETF列表管理等功能
特别优化了增量保存和断点续爬机制
"""

import os
import time
import pandas as pd
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
from retrying import retry
import akshare as ak
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday
from config import Config
from .etf_list_manager import update_all_etf_list, get_filtered_etf_codes, load_all_etf_list
from .akshare_crawler import crawl_etf_daily_akshare
from .sina_crawler import crawl_etf_daily_sina
from utils.date_utils import get_beijing_time, is_trading_day
from utils.file_utils import ensure_chinese_columns

# 初始化日志
logger = logging.getLogger(__name__)

# 定义中国股市节假日日历（2025年）
class ChinaStockHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday("元旦", month=1, day=1),
        Holiday("春节", month=1, day=29, observance=lambda d: d + pd.DateOffset(days=+5)),
        Holiday("清明节", month=4, day=4),
        Holiday("劳动节", month=5, day=1, observance=lambda d: d + pd.DateOffset(days=+2)),
        Holiday("端午节", month=6, day=2),
        Holiday("中秋节", month=9, day=8),
        Holiday("国庆节", month=10, day=1, observance=lambda d: d + pd.DateOffset(days=+6)),
    ]

# 重试装饰器配置
def retry_if_exception(exception: Exception) -> bool:
    """重试条件：网络或数据相关错误"""
    return isinstance(exception, (ConnectionError, TimeoutError, ValueError, pd.errors.EmptyDataError))

@retry(
    stop_max_attempt_number=3,
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    retry_on_exception=retry_if_exception
)
def akshare_retry(func, *args, **kwargs):
    """带重试机制的函数调用封装"""
    return func(*args, **kwargs)

def get_etf_name(etf_code: str) -> str:
    """
    根据ETF代码获取名称
    :param etf_code: ETF代码
    :return: ETF名称
    """
    try:
        etf_list = load_all_etf_list()
        if etf_list.empty:
            logger.warning("全市场ETF列表为空")
            return f"ETF-{etf_code}"
        
        target_code = str(etf_code).strip().zfill(6)
        name_row = etf_list[
            etf_list["ETF代码"].astype(str).str.strip().str.zfill(6) == target_code
        ]
        
        if not name_row.empty:
            return name_row.iloc[0]["ETF名称"]
        else:
            logger.debug(f"未在全市场列表中找到ETF代码: {target_code}")
            return f"ETF-{etf_code}"
    except Exception as e:
        logger.error(f"获取ETF名称失败: {str(e)}", exc_info=True)
        return f"ETF-{etf_code}"

def get_last_crawl_date(etf_code: str, etf_daily_dir: str) -> str:
    """
    获取ETF最后爬取日期
    :param etf_code: ETF代码
    :param etf_daily_dir: ETF日线数据目录
    :return: 最后爬取日期（格式：YYYY-MM-DD）
    """
    try:
        file_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
        if not os.path.exists(file_path):
            # 文件不存在，返回初始爬取日期
            current_date = get_beijing_time().date()
            start_date = (current_date - timedelta(days=Config.INITIAL_CRAWL_DAYS)).strftime("%Y-%m-%d")
            logger.debug(f"ETF {etf_code} 无历史数据，使用初始日期: {start_date}")
            return start_date
        
        df = pd.read_csv(file_path, encoding="utf-8")
        if df.empty or "date" not in df.columns:
            # 文件为空或没有date列，返回初始爬取日期
            current_date = get_beijing_time().date()
            start_date = (current_date - timedelta(days=Config.INITIAL_CRAWL_DAYS)).strftime("%Y-%m-%d")
            logger.debug(f"ETF {etf_code} 数据文件异常，使用初始日期: {start_date}")
            return start_date
        
        # 确保日期列是datetime类型
        df["date"] = pd.to_datetime(df["date"])
        last_date = df["date"].max().date()
        
        # 计算下一个交易日作为开始日期
        china_bd = CustomBusinessDay(calendar=ChinaStockHolidayCalendar())
        next_trading_day = pd.Timestamp(last_date) + china_bd
        return next_trading_day.date().strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"获取{etf_code}最后爬取日期失败: {str(e)}", exc_info=True)
        # 出错时返回初始爬取日期
        current_date = get_beijing_time().date()
        start_date = (current_date - timedelta(days=Config.INITIAL_CRAWL_DAYS)).strftime("%Y-%m-%d")
        logger.debug(f"ETF {etf_code} 获取最后爬取日期失败，使用初始日期: {start_date}")
        return start_date

def record_failed_etf(etf_daily_dir: str, etf_code: str, etf_name: str, error_message: Optional[str] = None) -> None:
    """
    记录失败的ETF信息
    :param etf_daily_dir: ETF日线数据目录
    :param etf_code: ETF代码
    :param etf_name: ETF名称
    :param error_message: 错误信息
    """
    try:
        failed_file = os.path.join(etf_daily_dir, "failed_etfs.txt")
        timestamp = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(failed_file, "a", encoding="utf-8") as f:
            if error_message:
                f.write(f"{etf_code}|{etf_name}|{timestamp}|{error_message}\n")
            else:
                f.write(f"{etf_code}|{etf_name}|{timestamp}\n")
        
        logger.debug(f"记录失败ETF: {etf_code} - {etf_name}")
    except Exception as e:
        logger.error(f"记录失败ETF信息失败: {str(e)}", exc_info=True)

def crawl_etf_daily_incremental() -> None:
    """
    增量爬取ETF日线数据（单只保存+断点续爬逻辑）
    
    注意：此函数不再包含是否执行的判断逻辑，由调用方决定是否执行
    """
    try:
        logger.info("===== 开始执行任务：crawl_etf_daily =====")
        beijing_time = get_beijing_time()
        logger.info(f"当前北京时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}（UTC+8）")
        
        # 初始化目录
        Config.init_dirs()
        etf_daily_dir = Config.ETFS_DAILY_DIR
        logger.info(f"✅ 确保目录存在: {etf_daily_dir}")
        
        # 已完成列表路径
        completed_file = os.path.join(etf_daily_dir, "etf_daily_completed.txt")
        
        # 加载已完成列表
        completed_codes = set()
        if os.path.exists(completed_file):
            try:
                with open(completed_file, "r", encoding="utf-8") as f:
                    completed_codes = set(line.strip() for line in f if line.strip())
                logger.info(f"已完成爬取的ETF数量：{len(completed_codes)}")
            except Exception as e:
                logger.error(f"读取已完成列表失败: {str(e)}", exc_info=True)
                completed_codes = set()
        
        # 获取待爬取ETF列表
        all_codes = get_filtered_etf_codes()
        to_crawl_codes = [code for code in all_codes if code not in completed_codes]
        total = len(to_crawl_codes)
        
        if total == 0:
            logger.info("所有ETF日线数据均已爬取完成，无需继续")
            return
        
        logger.info(f"待爬取ETF总数：{total}只")
        
        # 分批爬取（每批50只）
        batch_size = Config.CRAWL_BATCH_SIZE
        batches = [to_crawl_codes[i:i+batch_size] for i in range(0, total, batch_size)]
        logger.info(f"共分为 {len(batches)} 个批次，每批 {batch_size} 只ETF")
        
        # 逐批、逐只爬取
        for batch_idx, batch in enumerate(batches, 1):
            batch_num = len(batch)
            logger.info(f"==============================")
            logger.info(f"正在处理批次 {batch_idx}/{len(batches)}")
            logger.info(f"ETF范围：{batch_idx*batch_size - batch_size + 1}-{min(batch_idx*batch_size, total)}只（共{batch_num}只）")
            logger.info(f"==============================")
            
            for idx, etf_code in enumerate(batch, 1):
                try:
                    # 打印当前进度
                    logger.info(f"--- 批次{batch_idx} - 第{idx}只 / 共{batch_num}只 ---")
                    etf_name = get_etf_name(etf_code)
                    logger.info(f"ETF代码：{etf_code} | 名称：{etf_name}")
                    
                    # 确定爬取时间范围（增量爬取）
                    start_date = get_last_crawl_date(etf_code, etf_daily_dir)
                    end_date = beijing_time.date().strftime("%Y-%m-%d")
                    
                    if start_date > end_date:
                        logger.info(f"📅 无新数据需要爬取（上次爬取至{start_date}）")
                        # 标记为已完成
                        with open(completed_file, "a", encoding="utf-8") as f:
                            f.write(f"{etf_code}\n")
                        continue
                    
                    logger.info(f"📅 爬取时间范围：{start_date} 至 {end_date}")
                    
                    # 先尝试AkShare爬取
                    df = crawl_etf_daily_akshare(etf_code, start_date, end_date)
                    
                    # AkShare失败则尝试新浪爬取
                    if df.empty:
                        logger.warning("⚠️ AkShare未获取到数据，尝试使用新浪接口")
                        df = crawl_etf_daily_sina(etf_code, start_date, end_date)
                    
                    # 数据校验
                    if df.empty:
                        logger.warning(f"⚠️ 所有接口均未获取到数据，跳过保存")
                        # 记录失败日志，但不标记为已完成，以便下次重试
                        record_failed_etf(etf_daily_dir, etf_code, etf_name)
                        continue
                    
                    # 确保使用中文列名
                    df = ensure_chinese_columns(df)
                    
                    # 补充ETF基本信息
                    df["ETF代码"] = etf_code
                    df["ETF名称"] = etf_name
                    df["爬取时间"] = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
                    
                    # 处理已有数据的追加逻辑
                    save_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
                    if os.path.exists(save_path):
                        try:
                            existing_df = pd.read_csv(save_path)
                            # 确保现有数据也是中文列名
                            existing_df = ensure_chinese_columns(existing_df)
                            
                            # 合并数据并去重
                            combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=["日期"], keep="last")
                            # 按日期排序
                            combined_df = combined_df.sort_values("日期", ascending=False)
                            df = combined_df
                        except Exception as e:
                            logger.error(f"合并现有数据失败: {str(e)}，将覆盖原文件", exc_info=True)
                    
                    # 保存数据
                    df.to_csv(save_path, index=False, encoding="utf-8-sig")
                    logger.info(f"✅ 保存成功：{save_path}（共{len(df)}条数据）")
                    
                    # 记录已完成
                    with open(completed_file, "a", encoding="utf-8") as f:
                        f.write(f"{etf_code}\n")
                    
                    # 单只爬取后短休眠
                    time.sleep(1)
                    
                except Exception as e:
                    # 单只失败不中断，记录日志后继续
                    logger.error(f"❌ 爬取失败：{str(e)}", exc_info=True)
                    # 记录失败日志
                    record_failed_etf(etf_daily_dir, etf_code, etf_name, str(e))
                    time.sleep(3)  # 失败后延长休眠
                    continue
            
            # 批次间长休眠（减轻服务器压力）
            if batch_idx < len(batches):
                logger.info(f"批次{batch_idx}处理完成，休眠10秒后继续...")
                time.sleep(10)
        
        logger.info("===== 所有待爬取ETF处理完毕 =====")
        
    except Exception as e:
        logger.error(f"增量爬取任务执行失败: {str(e)}", exc_info=True)
        raise

def update_etf_list() -> bool:
    """
    更新ETF列表
    :return: 是否成功更新
    """
    try:
        logger.info("开始更新ETF列表")
        etf_list = update_all_etf_list()
        if etf_list.empty:
            logger.warning("ETF列表更新后为空")
            return False
        
        logger.info(f"ETF列表更新成功，共{len(etf_list)}只ETF")
        return True
    except Exception as e:
        logger.error(f"更新ETF列表失败: {str(e)}", exc_info=True)
        return False

def get_crawl_status() -> Dict[str, Any]:
    """
    获取爬取状态信息
    :return: 包含爬取状态信息的字典
    """
    try:
        etf_daily_dir = Config.ETFS_DAILY_DIR
        
        # 获取已完成列表
        completed_file = os.path.join(etf_daily_dir, "etf_daily_completed.txt")
        completed_codes = set()
        if os.path.exists(completed_file):
            with open(completed_file, "r", encoding="utf-8") as f:
                completed_codes = set(line.strip() for line in f if line.strip())
        
        # 获取失败列表
        failed_file = os.path.join(etf_daily_dir, "failed_etfs.txt")
        failed_count = 0
        if os.path.exists(failed_file):
            with open(failed_file, "r", encoding="utf-8") as f:
                failed_count = len(f.readlines())
        
        # 获取所有ETF列表
        all_codes = get_filtered_etf_codes()
        
        return {
            "total_etfs": len(all_codes),
            "completed_etfs": len(completed_codes),
            "failed_etfs": failed_count,
            "progress": f"{len(completed_codes)}/{len(all_codes)}",
            "percentage": round(len(completed_codes) / len(all_codes) * 100, 2) if all_codes else 0
        }
    except Exception as e:
        logger.error(f"获取爬取状态失败: {str(e)}", exc_info=True)
        return {
            "total_etfs": 0,
            "completed_etfs": 0,
            "failed_etfs": 0,
            "progress": "0/0",
            "percentage": 0
        }

# 模块初始化
try:
    # 确保必要的目录存在
    if Config.init_dirs():
        logger.info("数据爬取模块初始化完成")
    else:
        logger.warning("数据爬取模块初始化完成，但存在警告")
except Exception as e:
    logger.error(f"数据爬取模块初始化失败: {str(e)}", exc_info=True)
    # 退回到基础日志配置
    import logging
    logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
    logging.error(f"数据爬取模块初始化失败: {str(e)}")
