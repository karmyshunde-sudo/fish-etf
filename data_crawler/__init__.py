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
import tempfile  # 修复：添加tempfile导入
import shutil    # 修复：添加shutil导入
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple, Optional
import akshare as ak
# 修复：添加 retrying 导入 - 这是关键修复
from retrying import retry

# 添加必要的导入
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    is_file_outdated,
    is_trading_day,
    get_last_trading_day
)
from utils.file_utils import (
    ensure_dir_exists,
    get_last_crawl_date,
    record_failed_etf,
    ensure_chinese_columns,
    standardize_column_names
)
# 从正确的模块导入数据处理函数
from utils.data_processor import (
    ensure_required_columns,
    clean_and_format_data,
    limit_to_one_year_data
)
from data_crawler.akshare_crawler import crawl_etf_daily_akshare
from data_crawler.sina_crawler import crawl_etf_daily_sina
from data_crawler.etf_list_manager import (
    get_filtered_etf_codes,
    get_etf_name,
    update_all_etf_list
)

# 初始化日志
logger = logging.getLogger(__name__)

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

def crawl_etf_daily_incremental() -> None:
    """增量爬取ETF日线数据（单只保存+断点续爬逻辑）
    注意：此函数不再包含是否执行的判断逻辑，由调用方决定是否执行"""
    try:
        logger.info("===== 开始执行任务：crawl_etf_daily =====")
        beijing_time = get_beijing_time()
        logger.info(f"当前北京时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}（UTC+8）")
        
        # 初始化目录
        Config.init_dirs()
        etf_daily_dir = Config.ETFS_DAILY_DIR
        logger.info(f"✅ 确保目录存在: {etf_daily_dir}")
        
        # 获取最近一个交易日作为结束日期
        last_trading_day = get_last_trading_day()
        end_date = last_trading_day.strftime("%Y-%m-%d")
        
        # 获取待爬取ETF列表
        all_codes = get_filtered_etf_codes()
        to_crawl_codes = []
        
        # 精确判断哪些ETF需要爬取
        for code in all_codes:
            save_path = os.path.join(etf_daily_dir, f"{code}.csv")
            is_first_crawl = not os.path.exists(save_path)
            
            if is_first_crawl:
                # 首次爬取，需要获取数据
                to_crawl_codes.append(code)
                continue
            
            # 检查现有数据的最新日期
            latest_data_date = get_latest_data_date(save_path)
            if latest_data_date < last_trading_day:
                # 数据不是最新的，需要增量爬取
                to_crawl_codes.append(code)
        
        total = len(to_crawl_codes)
        if total == 0:
            logger.info("所有ETF日线数据均已最新，无需继续")
            return
        logger.info(f"待爬取ETF总数：{total}只（基于实际数据状态判断）")
        
        # 已完成列表路径（仅用于记录进度，不用于判断是否需要爬取）
        completed_file = os.path.join(etf_daily_dir, "etf_daily_completed.txt")
        
        # 加载已完成列表（仅用于进度显示）
        completed_codes = set()
        if os.path.exists(completed_file):
            try:
                with open(completed_file, "r", encoding="utf-8") as f:
                    completed_codes = set(line.strip() for line in f if line.strip())
                logger.info(f"进度记录中已完成爬取的ETF数量：{len(completed_codes)}")
            except Exception as e:
                logger.error(f"读取进度记录失败: {str(e)}", exc_info=True)
                completed_codes = set()
        
        # 分批爬取（每批50只）
        batch_size = Config.CRAWL_BATCH_SIZE
        num_batches = (total + batch_size - 1) // batch_size
        
        # 初始化一个列表来跟踪需要提交的文件
        files_to_commit = []
        
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total)
            batch_codes = to_crawl_codes[start_idx:end_idx]
            
            logger.info(f"处理第 {batch_idx+1}/{num_batches} 批 ETF ({len(batch_codes)}只)")
            
            for etf_code in batch_codes:
                etf_name = get_etf_name(etf_code)
                logger.info(f"ETF代码：{etf_code}| 名称：{etf_name}")
                
                # 确定爬取时间范围（增量爬取）
                save_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
                is_first_crawl = not os.path.exists(save_path)
                
                # 首次爬取获取一年数据，增量爬取只获取新数据
                if is_first_crawl:
                    # 首次爬取：获取1年历史数据
                    start_date = (last_trading_day - timedelta(days=365)).strftime("%Y-%m-%d")
                    logger.info(f"📅 首次爬取，获取1年历史数据：{start_date} 至 {end_date}")
                else:
                    # 增量爬取：获取上次爬取后的数据
                    start_date = get_last_crawl_date(etf_code, etf_daily_dir)
                    # 如果上次爬取日期已经是今天，无需再爬
                    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
                    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
                    if start_date_obj > end_date_obj:
                        logger.info(f"📅 无新数据需要爬取（上次爬取至{start_date}）")
                        # 标记为已完成（仅用于进度显示）
                        with open(completed_file, "a", encoding="utf-8") as f:
                            f.write(f"{etf_code}\n")
                        continue
                    logger.info(f"📅 增量爬取，获取新数据：{start_date} 至 {end_date}")
                
                # 先尝试AkShare爬取
                df = crawl_etf_daily_akshare(etf_code, start_date, end_date, is_first_crawl=is_first_crawl)
                
                # AkShare失败则尝试新浪爬取
                if df.empty:
                    logger.warning("⚠️ AkShare未获取到数据，尝试使用新浪接口")
                    df = crawl_etf_daily_sina(etf_code, start_date, end_date, is_first_crawl=is_first_crawl)
                
                # 数据校验
                if df.empty:
                    logger.warning(f"⚠️ 所有接口均未获取到数据，跳过保存")
                    # 记录失败日志，但不标记为已完成，以便下次重试
                    record_failed_etf(etf_daily_dir, etf_code, etf_name)
                    continue
                
                # 确保使用中文列名
                df = ensure_chinese_columns(df)
                
                # 确保所有必需列都存在
                df = ensure_required_columns(df)
                
                # 补充ETF基本信息
                df["ETF代码"] = etf_code
                df["ETF名称"] = etf_name
                df["爬取时间"] = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
                
                # 处理已有数据的追加逻辑
                if os.path.exists(save_path):
                    try:
                        existing_df = pd.read_csv(save_path)
                        # 确保现有数据也是中文列名
                        existing_df = ensure_chinese_columns(existing_df)
                        
                        # 确保必需列
                        existing_df = ensure_required_columns(existing_df)
                        
                        # 合并数据并去重
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                        combined_df = combined_df.sort_values("日期", ascending=False)
                        
                        # 使用临时文件进行原子操作，确保数据安全
                        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig')
                        try:
                            combined_df.to_csv(temp_file.name, index=False)
                            # 原子替换：先写入临时文件，再替换原文件
                            shutil.move(temp_file.name, save_path)
                            logger.info(f"✅ 数据已追加至: {save_path} (合并后共{len(combined_df)}条)")
                        finally:
                            if os.path.exists(temp_file.name):
                                os.unlink(temp_file.name)
                    except Exception as e:
                        logger.error(f"合并数据失败: {str(e)}，尝试覆盖保存", exc_info=True)
                        # 使用临时文件进行原子操作
                        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig')
                        try:
                            df.to_csv(temp_file.name, index=False)
                            # 原子替换
                            shutil.move(temp_file.name, save_path)
                            logger.info(f"✅ 数据已覆盖保存至: {save_path} ({len(df)}条)")
                        finally:
                            if os.path.exists(temp_file.name):
                                os.unlink(temp_file.name)
                else:
                    # 使用临时文件进行原子操作
                    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig')
                    try:
                        df.to_csv(temp_file.name, index=False)
                        # 原子替换
                        shutil.move(temp_file.name, save_path)
                        logger.info(f"✅ 数据已保存至: {save_path} ({len(df)}条)")
                    finally:
                        if os.path.exists(temp_file.name):
                            os.unlink(temp_file.name)
                
                # ===== 关键修改：使用新的git_utils函数 =====
                # 调用commit_files_in_batches，它会自动处理每10个文件提交一次
                try:
                    from utils.git_utils import commit_files_in_batches
                    commit_files_in_batches(save_path)
                    logger.info(f"✅ ETF {etf_code} 数据已标记用于批量提交")
                except ImportError:
                    logger.warning("未找到git_utils模块，跳过Git提交")
                except Exception as e:
                    logger.error(f"标记ETF {etf_code} 数据用于批量提交时出错: {str(e)}", exc_info=True)
                
                # 标记为已完成（仅用于进度显示）
                with open(completed_file, "a", encoding="utf-8") as f:
                    f.write(f"{etf_code}\n")
                
                # 限制请求频率
                time.sleep(1)  # 使用硬编码值代替Config.CRAWL_INTERVAL
            
            # 批次间暂停
            if batch_idx < num_batches - 1:
                batch_pause_seconds = 2  # 硬编码值，10秒
                logger.info(f"批次处理完成，暂停 {batch_pause_seconds} 秒...")
                time.sleep(batch_pause_seconds)
    
    except Exception as e:
        logger.error(f"ETF日线数据增量爬取任务执行失败: {str(e)}", exc_info=True)
        raise

def save_all_etf_data(etf_data_cache: Dict[str, pd.DataFrame], etf_daily_dir: str) -> None:
    """
    一次性保存所有ETF数据到文件
    Args:
        etf_data_cache: 内存中的ETF数据缓存
        etf_daily_dir: ETF日线数据目录
    """
    logger.info("开始批量保存ETF数据到文件...")
    try:
        # 初始化一个列表来跟踪需要提交的文件
        files_to_commit = []
        
        for etf_code, df in etf_data_cache.items():
            save_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
            try:
                if os.path.exists(save_path):
                    existing_df = pd.read_csv(save_path)
                    # 确保现有数据也是中文列名
                    existing_df = ensure_chinese_columns(existing_df)
                    
                    # 确保必需列
                    existing_df = ensure_required_columns(existing_df)
                    
                    # 合并数据并去重
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                    combined_df = combined_df.sort_values("日期", ascending=False)
                    
                    # 保存合并后的数据
                    combined_df.to_csv(save_path, index=False, encoding="utf-8-sig")
                    
                    logger.info(f"✅ 数据已追加至: {save_path} (合并后共{len(combined_df)}条)")
                else:
                    df.to_csv(save_path, index=False, encoding="utf-8-sig")
                    logger.info(f"✅ 数据已保存至: {save_path} ({len(df)}条)")
                
                # ===== 关键修改：使用新的git_utils函数 =====
                # 调用commit_files_in_batches，它会自动处理每10个文件提交一次
                try:
                    from utils.git_utils import commit_files_in_batches
                    commit_files_in_batches(save_path)
                    logger.info(f"✅ ETF {etf_code} 数据已标记用于批量提交")
                except ImportError:
                    logger.warning("未找到git_utils模块，跳过Git提交")
                except Exception as e:
                    logger.error(f"标记ETF {etf_code} 数据用于批量提交时出错: {str(e)}", exc_info=True)
            except Exception as e:
                logger.error(f"保存ETF {etf_code} 数据失败: {str(e)}", exc_info=True)
        logger.info(f"批量保存完成，共处理 {len(etf_data_cache)} 个ETF")
    except Exception as e:
        logger.error(f"批量保存ETF数据失败: {str(e)}", exc_info=True)
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
    Config.init_dirs()
    
    # 初始化日志
    logger.info("数据爬取模块初始化完成")
    
except Exception as e:
    error_msg = f"数据爬取模块初始化失败: {str(e)}"
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
        print(f"数据爬取模块初始化失败: {str(e)}")

def get_latest_data_date(file_path: str) -> date:
    """
    获取数据文件中的最新日期
    
    Args:
        file_path: 数据文件路径
        
    Returns:
        date: 最新日期
    """
    try:
        df = pd.read_csv(file_path)
        if "日期" in df.columns and not df.empty:
            # 确保日期列是datetime类型
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
            # 删除无效日期
            df = df.dropna(subset=["日期"])
            # 获取最大日期
            if not df.empty:
                latest_date = df["日期"].max()
                if not pd.isna(latest_date):
                    return latest_date.date()
    except Exception as e:
        logger.error(f"获取文件 {file_path} 最新日期失败: {str(e)}", exc_info=True)
    
    # 出错时返回一个较早的日期，确保会重新爬取
    return date(2024, 9, 1)
