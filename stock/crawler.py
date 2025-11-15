#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据爬取模块 - 严格确保股票代码为6位格式，日期处理逻辑完善
【2025-10-14-0836：循环索引，保证每次都是爬取150只股票】
【2025-11-14：爬取数据源额外新编写代码】
- 彻底解决Git提交问题
- 简单直接的Git提交机制
- 100%可直接复制使用
"""

import os
import logging
import pandas as pd
import akshare as ak
import time
import random
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import is_trading_day, get_last_trading_day, get_beijing_time
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files
# 导入股票列表更新模块
from stock.all_stocks import update_stock_list
# 新增：导入数据源模块
from stock.stock_source import get_stock_daily_data_from_sources

# 配置日志
logger = logging.getLogger(__name__)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_stocks.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# 【关键参数】
BATCH_SIZE = 8  # 单次处理的股票数量
MINOR_BATCH_SIZE = 10  # 每10只股票提交一次
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

def ensure_directory_exists():
    """确保数据目录存在"""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(DAILY_DIR):
        os.makedirs(DAILY_DIR)
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def format_stock_code(code):
    """
    规范化股票代码为6位字符串格式
    Args:
        code: 股票代码（可能包含前缀或非6位）
    Returns:
        str: 规范化的6位股票代码
    """
    # 转换为字符串
    code_str = str(code).strip().lower()
    
    # 移除可能的市场前缀
    if code_str.startswith(('sh', 'sz', 'hk', 'bj')):
        code_str = code_str[2:]
    
    # 移除可能的点号（如"0.600022"）
    if '.' in code_str:
        code_str = code_str.split('.')[1] if code_str.startswith('0.') else code_str
    
    # 确保是6位数字
    code_str = code_str.zfill(6)
    
    # 验证格式
    if not code_str.isdigit() or len(code_str) != 6:
        logger.warning(f"股票代码格式化失败: {code_str}")
        return None
    
    return code_str

def save_stock_daily_data(stock_code, df):
    """保存股票日线数据到CSV文件"""
    if df.empty:
        return None
    
    try:
        # 确保股票代码是6位
        stock_code = format_stock_code(stock_code)
        if not stock_code:
            logger.error(f"无法保存：股票代码格式化失败")
            return None
        
        file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        
        # 确保数据中的"股票代码"列是6位格式
        if '股票代码' in df.columns:
            df['股票代码'] = df['股票代码'].apply(lambda x: format_stock_code(str(x)))
            df = df[df['股票代码'].notna()]
        else:
            df['股票代码'] = stock_code
        
        # 保存数据
        df.to_csv(file_path, index=False)
        
        # 【关键修复】直接添加到Git暂存区
        try:
            os.system(f'git add "{file_path}"')
            logger.debug(f"✅ 文件已添加到Git暂存区: {file_path}")
        except Exception as e:
            logger.error(f"❌ 添加文件到Git暂存区失败: {str(e)}")
        
        return file_path
    except Exception as e:
        logger.error(f"保存股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)
        return None

def update_all_stocks_daily_data():
    """更新所有股票的日线数据"""
    ensure_directory_exists()
    
    # 确保基础信息文件存在
    if not os.path.exists(BASIC_INFO_FILE):
        logger.info("基础信息文件不存在，正在创建...")
        if not update_stock_list():
            logger.error("基础信息文件创建失败，无法更新日线数据")
            return False
    
    # 获取基础信息
    try:
        basic_info_df = pd.read_csv(BASIC_INFO_FILE)
        if basic_info_df.empty:
            logger.error("基础信息文件为空，无法更新日线数据")
            return False
        
        # 确保"代码"列是6位格式
        basic_info_df["代码"] = basic_info_df["代码"].apply(format_stock_code)
        basic_info_df = basic_info_df[basic_info_df["代码"].notna()]
        basic_info_df = basic_info_df[basic_info_df["代码"].str.len() == 6]
        basic_info_df = basic_info_df.reset_index(drop=True)
        
        # 保存更新后的基础信息文件
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        commit_files_in_batches(BASIC_INFO_FILE, "更新股票基础信息")
        logger.info(f"已更新基础信息文件，共 {len(basic_info_df)} 条记录")
        
    except Exception as e:
        logger.error(f"读取基础信息文件失败: {str(e)}", exc_info=True)
        return False
    
    # 获取 next_crawl_index
    next_index = int(basic_info_df["next_crawl_index"].iloc[0])
    total_stocks = len(basic_info_df)
    
    logger.info(f"当前爬取状态: next_crawl_index = {next_index} (共 {total_stocks} 只股票)")
    
    # 确定处理范围
    start_idx = next_index % total_stocks
    end_idx = start_idx + BATCH_SIZE
    actual_end_idx = end_idx % total_stocks
    
    # 处理循环情况
    if end_idx <= total_stocks:
        batch_df = basic_info_df.iloc[start_idx:end_idx]
        logger.info(f"处理本批次 股票 ({BATCH_SIZE}只)，从索引 {start_idx} 开始")
    else:
        batch_df = pd.concat([
            basic_info_df.iloc[start_idx:total_stocks],
            basic_info_df.iloc[0:end_idx-total_stocks]
        ])
        logger.info(f"处理本批次 股票 ({BATCH_SIZE}只)，从索引 {start_idx} 开始（循环处理）")
    
    # 处理这批股票
    batch_codes = batch_df["代码"].tolist()
    file_paths = []
    
    for i, stock_code in enumerate(batch_codes):
        # 确保股票代码是6位
        stock_code = format_stock_code(stock_code)
        if not stock_code:
            continue
            
        # 添加随机延时
        time.sleep(random.uniform(1.5, 2.5))
        df = get_stock_daily_data_from_sources(stock_code)
        
        if not df.empty:
            file_path = save_stock_daily_data(stock_code, df)
            if file_path:
                file_paths.append(file_path)
        
        # 【关键修复】每处理10个股票就提交一次
        if (i + 1) % MINOR_BATCH_SIZE == 0 and file_paths:
            logger.info(f"批量提交 {len(file_paths)} 只股票日线数据...")
            # 构建提交消息
            commit_msg = f"feat: 批量提交{len(file_paths)}只股票日线数据 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
            # 直接使用文件路径提交
            commit_files_in_batches(file_paths, commit_msg)
            logger.info(f"✅ 小批次数据文件提交成功：{len(file_paths)}只")
            file_paths = []
    
    # 【关键修复】处理完本批次后，提交任何剩余文件
    if file_paths:
        logger.info(f"提交剩余的 {len(file_paths)} 只股票日线数据...")
        commit_msg = f"feat: 批量提交{len(file_paths)}只股票日线数据 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        commit_files_in_batches(file_paths, commit_msg)
    
    # 【关键修复】确保所有文件都被提交
    logger.info("执行最终兜底提交...")
    force_commit_remaining_files()
    
    # 更新 next_crawl_index
    new_index = actual_end_idx
    logger.info(f"更新 next_crawl_index = {new_index}")
    basic_info_df["next_crawl_index"] = new_index
    basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
    
    # 提交基础信息文件
    commit_files_in_batches(BASIC_INFO_FILE, "更新股票基础信息")
    logger.info(f"已提交更新后的基础信息文件到仓库")
    
    # 检查剩余股票数量
    remaining_stocks = (total_stocks - new_index) % total_stocks
    logger.info(f"已完成 {BATCH_SIZE} 只股票爬取，还有 {remaining_stocks} 只股票待爬取")
    
    return True

def main():
    """主函数：更新所有股票数据"""
    logger.info("===== 开始更新股票数据 =====")
    
    # 添加初始延时
    time.sleep(random.uniform(1.0, 2.0))
    
    # 确保基础信息文件存在
    if not os.path.exists(BASIC_INFO_FILE) or os.path.getsize(BASIC_INFO_FILE) == 0:
        logger.info("基础信息文件不存在或为空，正在创建...")
        if not update_stock_list():
            logger.error("基础信息文件创建失败，无法继续")
            return
    
    # 更新股票数据
    if update_all_stocks_daily_data():
        logger.info("已成功处理一批股票数据")
    else:
        logger.error("处理股票数据失败")
    
    logger.info("===== 股票数据更新完成 =====")

if __name__ == "__main__":
    main()
