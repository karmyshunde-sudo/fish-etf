#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票日线数据增量爬取器
每5分钟运行一次，每次只爬取100只股票
用于预加载股票数据，避免策略执行时的网络请求延迟
"""
import os
import logging
import pandas as pd
import time
import akshare as ak
import subprocess  # 添加这个导入，用于执行Git命令
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from config import Config
from utils.date_utils import get_beijing_time, get_utc_time
from utils.git_utils import commit_and_push_file
from concurrent.futures import ThreadPoolExecutor
import random

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 股票基础信息文件路径
BASIC_INFO_FILE = "data/all_stocks.csv"
# 日线数据存储目录
DAILY_DATA_DIR = os.path.join(Config.DATA_DIR, "daily")

# 每次爬取的股票数量
STOCKS_PER_RUN = 100
# 请求延迟参数
REQUEST_DELAY_BASE = 1.5
REQUEST_DELAY_RANDOM_FACTOR = 0.5
MAX_RETRIES = 3
EXPONENTIAL_BACKOFF_BASE = 2.0

def ensure_daily_data_dir():
    """确保日线数据目录存在"""
    os.makedirs(DAILY_DATA_DIR, exist_ok=True)
    logger.info(f"已确保日线数据目录存在: {DAILY_DATA_DIR}")

def apply_request_delay():
    """应用请求延迟，避免被AkShare限流"""
    delay = REQUEST_DELAY_BASE + random.uniform(0, REQUEST_DELAY_RANDOM_FACTOR)
    time.sleep(delay)

def fetch_stock_data_with_retry(stock_code: str, max_retries: int = MAX_RETRIES) -> Optional[pd.DataFrame]:
    """带重试机制的股票数据获取"""
    # 关键修复：确保股票代码是字符串并格式化为6位
    original_stock_code = str(stock_code)  # 先转换为字符串
    stock_code = original_stock_code.zfill(6)  # 然后格式化为6位
    
    for attempt in range(max_retries):
        try:
            # 应用请求延迟
            apply_request_delay()
            
            # 获取股票所属板块
            section = get_stock_section(stock_code)
            
            # 确定市场前缀
            if section == "沪市主板" or section == "科创板":
                market_prefix = "sh"
            else:  # 深市主板、创业板
                market_prefix = "sz"
            
            # 检查本地是否已有数据
            file_path = os.path.join(DAILY_DATA_DIR, f"{stock_code}.csv")
            start_date = None
            
            if os.path.exists(file_path):
                # 读取现有数据文件
                try:
                    existing_df = pd.read_csv(file_path)
                    if not existing_df.empty and "日期" in existing_df.columns:
                        # 确保日期列是datetime类型
                        existing_df["日期"] = pd.to_datetime(existing_df["日期"])
                        # 获取现有数据的最新日期
                        latest_date = existing_df["日期"].max().strftime("%Y%m%d")
                        # 从最新日期的下一天开始获取
                        start_date = (datetime.strptime(latest_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
                        logger.debug(f"股票 {stock_code} (原始: {original_stock_code}) 检测到现有数据，最新日期: {latest_date}, 将从 {start_date} 开始增量获取")
                except Exception as e:
                    logger.warning(f"读取股票 {stock_code} (原始: {original_stock_code}) 现有数据失败: {str(e)}")
                    # 如果无法读取现有数据，从一年前开始获取
                    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
            
            # 计算日期范围
            end_date = datetime.now().strftime("%Y%m%d")
            if start_date is None:
                # 默认获取1年数据
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
            
            # 如果开始日期晚于结束日期，无需获取
            if start_date > end_date:
                logger.info(f"股票 {stock_code} (原始: {original_stock_code}) 数据已最新，无需爬取")
                return None
            
            # 尝试多种可能的股票代码格式（优化顺序）
            possible_codes = [
                f"{market_prefix}{stock_code}",  # "sh000001"
                f"{stock_code}.{'SZ' if market_prefix == 'sz' else 'SH'}",  # "000001.SZ" - 优先尝试这种格式
                f"{stock_code}.{market_prefix.upper()}",  # "000001.SH"
                stock_code,  # "000001"
                f"{market_prefix}{stock_code}.XSHG" if market_prefix == "sh" else f"{market_prefix}{stock_code}.XSHE",  # 交易所格式
            ]
            
            # 尝试使用多种接口和代码格式获取数据
            df = None
            successful_code = None
            successful_interface = None
            
            # 先尝试使用stock_zh_a_hist接口
            for code in possible_codes:
                for inner_attempt in range(2):  # 减少重试次数
                    try:
                        logger.debug(f"股票 {stock_code} (原始: {original_stock_code}) 尝试{inner_attempt+1}/2: 使用stock_zh_a_hist接口获取股票 {code}")
                        df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                              start_date=start_date, end_date=end_date, 
                                              adjust="qfq")
                        if not df.empty:
                            successful_code = code
                            successful_interface = "stock_zh_a_hist"
                            logger.debug(f"股票 {stock_code} (原始: {original_stock_code}) 成功通过 {successful_interface} 接口获取股票 {code} 数据")
                            break
                    except Exception as e:
                        logger.debug(f"股票 {stock_code} (原始: {original_stock_code}) 使用stock_zh_a_hist接口获取股票 {code} 失败: {str(e)}")
                    
                    # 优化等待时间
                    time.sleep(0.3 * (2 ** inner_attempt))
                
                if df is not None and not df.empty:
                    break
            
            # 如果stock_zh_a_hist接口失败，尝试其他接口
            if df is None or df.empty:
                for code in possible_codes:
                    for inner_attempt in range(2):
                        try:
                            logger.debug(f"股票 {stock_code} (原始: {original_stock_code}) 尝试{inner_attempt+1}/2: 使用stock_zh_a_daily接口获取股票 {code}")
                            df = ak.stock_zh_a_daily(symbol=code, 
                                                   start_date=start_date, 
                                                   end_date=end_date, 
                                                   adjust="qfq")
                            if not df.empty:
                                successful_code = code
                                successful_interface = "stock_zh_a_daily"
                                logger.debug(f"股票 {stock_code} (原始: {original_stock_code}) 成功通过 {successful_interface} 接口获取股票 {code} 数据")
                                break
                        except Exception as e:
                            logger.debug(f"股票 {stock_code} (原始: {original_stock_code}) 使用stock_zh_a_daily接口获取股票 {code} 失败: {str(e)}")
                        
                        time.sleep(0.5 * (2 ** inner_attempt))
                    
                    if df is not None and not df.empty:
                        break
            
            # 如果还是失败，返回None
            if df is None or df.empty:
                logger.warning(f"股票 {stock_code} (原始: {original_stock_code}) 获取数据失败，所有接口和代码格式均无效")
                return None
            
            # 确保日期列存在
            if "日期" not in df.columns:
                logger.warning(f"股票 {stock_code} (原始: {original_stock_code}) 数据缺少'日期'列")
                return None
            
            # 檢查是否有必要的列
            required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"股票 {stock_code} (原始: {original_stock_code}) 数据缺少必要列: {', '.join(missing_columns)}")
                return None
            
            # 确保日期列格式正确
            if "日期" in df.columns:
                # 处理不同的日期格式
                if df["日期"].dtype == 'object':
                    # 尝试多种日期格式
                    try:
                        df["日期"] = pd.to_datetime(df["日期"], format='%Y-%m-%d')
                    except:
                        try:
                            df["日期"] = pd.to_datetime(df["日期"], format='%Y%m%d')
                        except:
                            df["日期"] = pd.to_datetime(df["日期"])
                
                # 转换为字符串格式
                df["日期"] = df["日期"].astype(str)
                # 确保日期格式为YYYY-MM-DD
                df["日期"] = df["日期"].str.replace(r'(\d{4})/(\d{1,2})/(\d{1,2})', 
                                                  lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", 
                                                  regex=True)
                # 处理其他可能的格式
                df["日期"] = df["日期"].str.replace(r'(\d{4})-(\d{1,2}) (\d{1,2})', 
                                                  lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", 
                                                  regex=True)
                # 移除可能存在的空格
                df["日期"] = df["日期"].str.strip()
                df = df.sort_values("日期", ascending=True)
            
            # 限制为最近1年的数据
            one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            df = df[df["日期"] >= one_year_ago]
            
            # 记录实际获取的数据量
            logger.info(f"股票 {stock_code} (原始: {original_stock_code}) ✅ 成功通过 {successful_interface} 接口获取数据，共 {len(df)} 天（{start_date} 至 {end_date}）")
            
            return df
        
        except Exception as e:
            # 檢测是否是限流错误
            if "429" in str(e) or "Too Many Requests" in str(e) or "请求过于频繁" in str(e):
                # 指数退避重试
                wait_time = REQUEST_DELAY_BASE * (EXPONENTIAL_BACKOFF_BASE ** attempt)
                logger.warning(f"股票 {stock_code} (原始: {original_stock_code}) 请求被限流，等待 {wait_time:.1f} 秒后重试 ({attempt+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"股票 {stock_code} (原始: {original_stock_code}) 获取数据失败: {str(e)}", exc_info=True)
                break
    
    logger.warning(f"股票 {stock_code} (原始: {original_stock_code}) 获取数据失败，超过最大重试次数")
    return None

def get_stock_section(stock_code: str) -> str:
    """
    获取股票所属板块
    
    Args:
        stock_code: 股票代码（不带市场前缀）
    
    Returns:
        str: 板块名称
    """
    # 确保股票代码是字符串
    stock_code = str(stock_code).zfill(6)
    
    # 移除可能的市场前缀
    if stock_code.lower().startswith(('sh', 'sz')):
        stock_code = stock_code[2:]
    
    # 确保股票代码是6位数字
    stock_code = stock_code.zfill(6)
    
    # 股票板块配置
    MARKET_SECTIONS = {
        "沪市主板": {"prefix": ["60"]},
        "深市主板": {"prefix": ["00"]},
        "创业板": {"prefix": ["30"]},
        "科创板": {"prefix": ["688"]}
    }
    
    # 根据股票代码前缀判断板块
    for section, config in MARKET_SECTIONS.items():
        for prefix in config["prefix"]:
            if stock_code.startswith(prefix):
                return section
    
    return "其他板块"

def process_stock_for_crawl(stock_code: str) -> bool:
    """处理单只股票的爬取任务"""
    try:
        # 获取日线数据
        df = fetch_stock_data_with_retry(stock_code)
        
        if df is not None and not df.empty:
            # 保存数据
            file_path = os.path.join(DAILY_DATA_DIR, f"{stock_code}.csv")
            
            # 如果文件已存在，先读取并合并
            if os.path.exists(file_path):
                existing_df = pd.read_csv(file_path)
                # 合并数据
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                # 去重并按日期排序
                combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                combined_df = combined_df.sort_values("日期", ascending=True)
                # 限制为最近1年数据
                one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
                combined_df = combined_df[combined_df["日期"] >= one_year_ago]
                # 保存更新后的数据
                combined_df.to_csv(file_path, index=False)
            else:
                # 新建文件
                df.to_csv(file_path, index=False)
            
            # 提交到Git仓库
            try:
                logger.info(f"正在提交股票 {stock_code} 数据到GitHub仓库...")
                commit_message = f"自动更新股票 {stock_code} 数据 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
                
                # ===== 关键修复：添加重试机制和冲突解决 =====
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        if commit_and_push_file(file_path, commit_message):
                            logger.info(f"股票 {stock_code} 数据已成功提交并推送到GitHub仓库")
                            break
                        else:
                            if attempt < max_retries - 1:
                                # 尝试拉取最新更改解决冲突
                                logger.info(f"Git提交失败，尝试拉取最新更改...")
                                repo_root = os.path.dirname(os.path.abspath(file_path))
                                subprocess.run(["git", "pull", "--rebase", "origin", "main"], 
                                              check=True, cwd=repo_root)
                                
                                wait_time = 2 ** attempt  # 指数退避
                                logger.warning(f"股票 {stock_code} 提交失败，{wait_time}秒后重试 ({attempt+1}/{max_retries})")
                                time.sleep(wait_time)
                            else:
                                logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败，超过最大重试次数，但继续执行爬取")
                    except Exception as e:
                        logger.warning(f"Git操作过程中出错: {str(e)}")
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.warning(f"股票 {stock_code} 提交失败，{wait_time}秒后重试 ({attempt+1}/{max_retries})")
                            time.sleep(wait_time)
                        else:
                            logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败，超过最大重试次数，但继续执行爬取")
                # ===== 修复结束 =====
            
            except Exception as e:
                logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败: {str(e)}，但继续执行爬取")
            
            return True
        return False
    except Exception as e:
        logger.error(f"处理股票 {stock_code} 时出错: {str(e)}", exc_info=True)
        return False

def main():
    """主函数：股票日线数据增量爬取"""
    try:
        logger.info("===== 开始执行股票日线数据增量爬取 =====")
        
        # 确保日线数据目录存在
        ensure_daily_data_dir()
        
        # 1. 加载股票基础信息
        if not os.path.exists(BASIC_INFO_FILE):
            logger.error(f"股票基础信息文件 {BASIC_INFO_FILE} 不存在，请先运行策略获取基础信息")
            return
        
        basic_info_df = pd.read_csv(BASIC_INFO_FILE)
        if basic_info_df.empty:
            logger.error("股票基础信息为空，无法继续")
            return
        
        logger.info(f"已加载股票基础信息，共 {len(basic_info_df)} 条记录")
        
        # 2. 确保 next_crawl_index 列存在
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
            # 保存修改
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
            logger.info("已添加 next_crawl_index 列到股票基础信息")
        
        # 获取当前爬取索引
        current_index = int(basic_info_df["next_crawl_index"].iloc[0])
        total_stocks = len(basic_info_df)
        
        logger.info(f"当前爬取索引: {current_index} (共 {total_stocks} 只股票)")
        
        # 3. 确定要爬取的股票范围
        start_index = current_index
        end_index = min(current_index + STOCKS_PER_RUN, total_stocks)
        
        # 如果到达末尾，从头开始
        if start_index >= total_stocks:
            start_index = 0
            end_index = min(STOCKS_PER_RUN, total_stocks)
            current_index = 0
            logger.info("已到达股票列表末尾，从头开始爬取")
        
        # 4. 爬取指定范围的股票
        stock_codes = basic_info_df["code"].tolist()
        stocks_to_crawl = stock_codes[start_index:end_index]
        
        logger.info(f"本次将爬取 {len(stocks_to_crawl)} 只股票 (索引 {start_index} 到 {end_index-1})")
        
        # 并行处理股票
        success_count = 0
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = executor.map(process_stock_for_crawl, stocks_to_crawl)
            for i, result in enumerate(results):
                if result:
                    success_count += 1
                logger.info(f"已处理 {i+1}/{len(stocks_to_crawl)} 只股票，成功: {success_count}")
        
        # 5. 更新 next_crawl_index
        next_index = end_index if end_index < total_stocks else 0
        basic_info_df["next_crawl_index"] = next_index
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        
        logger.info(f"股票基础信息已更新，next_crawl_index 设置为 {next_index}")
        
        # 提交更新后的股票基础信息
        try:
            logger.info("正在提交更新后的股票基础信息到GitHub仓库...")
            commit_message = f"自动更新 next_crawl_index 为 {next_index} [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
            if commit_and_push_file(BASIC_INFO_FILE, commit_message):
                logger.info("股票基础信息已成功提交并推送到GitHub仓库")
            else:
                logger.warning("提交股票基础信息到GitHub仓库失败，但爬取任务已完成")
        except Exception as e:
            logger.warning(f"提交股票基础信息到GitHub仓库失败: {str(e)}，但爬取任务已完成")
        
        logger.info("===== 股票日线数据增量爬取完成 =====")
    
    except Exception as e:
        logger.error(f"股票日线数据增量爬取失败: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
