#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据爬取模块 - 仅调用一次API获取所有必要数据
严格使用API返回的原始列名，确保高效获取股票数据
"""

import os
import logging
import pandas as pd
import akshare as ak
import time
import random
from datetime import datetime, timedelta
from config import Config
# 只需导入Git工具模块
from utils.git_utils import commit_files_in_batches

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_stocks.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

def ensure_directory_exists():
    """确保数据目录存在"""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(DAILY_DIR):
        os.makedirs(DAILY_DIR)
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

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
    
    # 根据股票代码前缀判断板块
    if stock_code.startswith('60'):
        return "沪市主板"
    elif stock_code.startswith('00'):
        return "深市主板"
    elif stock_code.startswith('30'):
        return "创业板"
    elif stock_code.startswith('688'):
        return "科创板"
    else:
        return "其他板块"

def create_or_update_basic_info():
    """创建或更新股票基础信息文件，仅调用一次API"""
    ensure_directory_exists()
    
    # 添加随机延时，避免请求过于频繁
    time.sleep(random.uniform(1.0, 2.0))
    
    try:
        # 正确调用 stock_zh_a_spot_em 获取所有必要数据
        df = ak.stock_zh_a_spot_em()
        
        # 检查返回结果
        if df.empty:
            logger.error("获取股票列表失败：返回为空")
            return False
        
        # 打印API返回的列名，用于调试
        logger.info(f"API返回的列名: {df.columns.tolist()}")
        
        # 确保必要列存在
        required_columns = ['代码', '名称', '流通市值']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"获取股票列表失败: 缺少必要列 {col}")
                return False
        
        # 过滤ST股票和非主板/科创板/创业板股票
        stock_list = df.copy()
        
        # 过滤ST股票
        stock_list = stock_list[~stock_list['名称'].str.contains('ST', na=False)]
        
        # 过滤非主板/科创板/创业板股票
        stock_list = stock_list[stock_list['代码'].str.startswith(('0', '3', '6'))]
        
        logger.info(f"成功获取股票列表，共 {len(stock_list)} 只股票")
        
        # 准备基础信息DataFrame - 严格使用API返回的原始列名
        basic_info_df = pd.DataFrame({
            "代码": stock_list['代码'],
            "名称": stock_list['名称'],
            "所属板块": stock_list['代码'].apply(get_stock_section),
            "流通市值": stock_list['流通市值']
        })
        
        # 确保流通市值列是数值类型
        basic_info_df["流通市值"] = pd.to_numeric(basic_info_df["流通市值"], errors='coerce').fillna(0)
        
        # 保留无市值股票，但标记它们
        invalid_mask = (basic_info_df["流通市值"] <= 0) | basic_info_df["流通市值"].isna()
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            basic_info_df["数据状态"] = '正常'
            basic_info_df.loc[invalid_mask, "数据状态"] = '流通市值缺失'
            logger.warning(f"检测到 {invalid_count} 条无市值数据的股票，已标记为'流通市值缺失'")
        
        # 添加 next_crawl_index 列（如果不存在）
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = True
        
        # 保存基础信息
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"已创建/更新股票基础信息文件，共 {len(basic_info_df)} 条记录")
        
        # 确认文件已保存
        if os.path.exists(BASIC_INFO_FILE) and os.path.getsize(BASIC_INFO_FILE) > 0:
            logger.info(f"基础信息文件已成功保存到: {BASIC_INFO_FILE}")
            
            # 【关键修改】只需调用，无需处理返回值
            commit_files_in_batches(BASIC_INFO_FILE)
            return True
        else:
            logger.error(f"基础信息文件保存失败: {BASIC_INFO_FILE} 不存在或为空")
            return False
            
    except Exception as e:
        logger.error(f"创建基础信息文件失败: {str(e)}", exc_info=True)
        return False

def fetch_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """获取单只股票的日线数据，使用中文列名"""
    try:
        # 确保股票代码是字符串，并且是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        
        # 确定市场前缀
        market_prefix = 'sh' if stock_code.startswith('6') else 'sz'
        ak_code = f"{market_prefix}{stock_code}"
        
        # 获取日线数据 - 修复adjust参数并调整为1年数据
        df = ak.stock_zh_a_hist(
            symbol=ak_code,
            period="daily",
            start_date=(datetime.now() - timedelta(days=365)).strftime("%Y%m%d"),
            end_date=datetime.now().strftime("%Y%m%d"),
            adjust="qfq"  # 关键修复：使用有效的复权参数
        )
        
        if df.empty:
            logger.warning(f"股票 {stock_code} 的日线数据为空")
            return pd.DataFrame()
        
        # 确保必要列存在
        required_columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"股票 {stock_code} 数据缺少必要列: {col}")
                return pd.DataFrame()
        
        # 确保日期格式正确
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.sort_values('日期').reset_index(drop=True)
        
        # 确保数值列是数值类型
        numeric_columns = ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 移除NaN值
        df = df.dropna(subset=['收盘', '成交量'])
        
        logger.info(f"成功获取股票 {stock_code} 的日线数据，共 {len(df)} 条记录")
        return df
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def save_stock_daily_data(stock_code: str, df: pd.DataFrame):
    """保存股票日线数据到CSV文件，使用中文列名"""
    if df.empty:
        return
    
    try:
        file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        df.to_csv(file_path, index=False)
        logger.debug(f"已保存股票 {stock_code} 的日线数据到 {file_path}")
        
        # 【关键修改】只需简单调用，无需任何额外逻辑
        commit_files_in_batches(file_path)
    except Exception as e:
        logger.error(f"保存股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)

def update_all_stocks_daily_data():
    """更新所有股票的日线数据，使用中文列名"""
    ensure_directory_exists()
    
    # 确保基础信息文件存在
    if not os.path.exists(BASIC_INFO_FILE):
        logger.info("基础信息文件不存在，正在创建...")
        if not create_or_update_basic_info():
            logger.error("基础信息文件创建失败，无法更新日线数据")
            return False
    
    # 获取基础信息文件
    try:
        basic_info_df = pd.read_csv(BASIC_INFO_FILE)
        if basic_info_df.empty:
            logger.error("基础信息文件为空，无法更新日线数据")
            return False
    except Exception as e:
        logger.error(f"读取基础信息文件失败: {str(e)}", exc_info=True)
        return False
    
    # 获取需要更新的股票列表（next_crawl_index 为 True 的股票）
    stock_codes = basic_info_df[basic_info_df["next_crawl_index"]]["代码"].tolist()
    if not stock_codes:
        logger.info("没有需要爬取的股票，next_crawl_index 均为 False")
        
        # 重置 next_crawl_index，标记所有股票为需要爬取
        basic_info_df["next_crawl_index"] = True
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        
        # 【关键修改】只需调用，无需处理返回值
        commit_files_in_batches(BASIC_INFO_FILE)
        
        # 尝试获取新的股票列表
        stock_codes = basic_info_df["代码"].tolist()
        logger.info(f"重置 next_crawl_index，将爬取所有 {len(stock_codes)} 只股票")
    
    logger.info(f"开始更新 {len(stock_codes)} 只股票的日线数据（基于 next_crawl_index 标记）")
    
    # 每100只股票处理一次
    batch_size = 100
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i+batch_size]
        logger.info(f"正在处理第 {i//batch_size + 1} 批，共 {len(batch)} 只股票")
        
        for stock_code in batch:
            # 添加随机延时，避免请求过于频繁
            time.sleep(random.uniform(0.5, 1.5))
            df = fetch_stock_daily_data(stock_code)
            if not df.empty:
                save_stock_daily_data(stock_code, df)
        
        # 更新 next_crawl_index
        basic_info_df.loc[basic_info_df["代码"].isin(batch), "next_crawl_index"] = False
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"已更新基础信息文件的 next_crawl_index 列")
        
        # 【关键修改】只需调用，无需处理返回值
        commit_files_in_batches(BASIC_INFO_FILE)
    
    logger.info("所有股票日线数据更新完成")
    return True

def main():
    """主函数：更新所有股票数据"""
    logger.info("===== 开始更新股票数据 =====")
    
    # 添加初始延时，避免立即请求
    time.sleep(random.uniform(1.0, 2.0))
    
    # 1. 确保基础信息文件存在
    if not os.path.exists(BASIC_INFO_FILE) or os.path.getsize(BASIC_INFO_FILE) == 0:
        logger.info("基础信息文件不存在或为空，正在创建...")
        if not create_or_update_basic_info():
            logger.error("基础信息文件创建失败，无法继续")
            return
    
    # 2. 更新所有股票日线数据
    if update_all_stocks_daily_data():
        logger.info("日线数据更新成功")
    else:
        logger.error("日线数据更新失败")
    
    logger.info("===== 股票数据更新完成 =====")

if __name__ == "__main__":
    main()
