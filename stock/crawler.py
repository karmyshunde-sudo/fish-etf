#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据爬取模块
严格使用中文列名，确保与日线数据文件格式一致
包含 next_crawl_index 逻辑，用于分批爬取股票日线数据
"""

import os
import logging
import pandas as pd
import akshare as ak
import time
import random
import numpy as np
from datetime import datetime, timedelta
from config import Config
import traceback
from concurrent.futures import ThreadPoolExecutor

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

# 文件过期检查
MAX_AGE_DAYS = 7  # 基础信息文件最大有效天数
BATCH_SIZE = 100  # 每批处理的股票数量

def ensure_directory_exists():
    """确保数据目录存在"""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(DAILY_DIR):
        os.makedirs(DAILY_DIR)
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def is_file_expired(file_path, max_age_days=MAX_AGE_DAYS):
    """检查文件是否过期（超过指定天数）"""
    if not os.path.exists(file_path):
        return True
    
    # 获取文件最后修改时间
    mtime = os.path.getmtime(file_path)
    mtime_date = datetime.fromtimestamp(mtime)
    
    # 检查是否超过指定天数
    return (datetime.now() - mtime_date).days > max_age_days

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

def fetch_market_cap_data():
    """获取股票流通市值数据，严格使用中文列名"""
    try:
        # 添加随机延时，避免请求过于频繁
        time.sleep(random.uniform(0.5, 1.5))
        
        # 正确调用 stock_zh_a_spot_em 不带参数
        df = ak.stock_zh_a_spot_em()
        
        # 检查返回结果
        if df.empty:
            logger.error("获取流通市值数据失败：返回为空")
            return {}
        
        # 检查必要列
        required_columns = ["代码", "名称", "流通市值"]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"获取流通市值数据失败: 缺少必要列 {col}")
                return {}
        
        # 创建市值字典
        market_cap_dict = {}
        
        for _, row in df.iterrows():
            stock_code = str(row['代码']).zfill(6)
            try:
                # 流通市值单位是万元，转换为亿元
                market_cap = float(row['流通市值']) / 10000
                if not np.isnan(market_cap) and market_cap > 0:
                    market_cap_dict[stock_code] = market_cap
            except (TypeError, ValueError, KeyError) as e:
                logger.warning(f"处理股票 {stock_code} 流通市值时出错: {str(e)}")
        
        logger.info(f"成功获取 {len(market_cap_dict)} 只股票的流通市值数据")
        return market_cap_dict
    except Exception as e:
        logger.error(f"获取流通市值数据失败: {str(e)}", exc_info=True)
        return {}

def fetch_single_stock_market_cap(stock_code):
    """获取单只股票的市值数据（通过正确方式）"""
    try:
        # 确保股票代码是字符串，并且是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        
        # 添加随机延时，避免请求过于频繁
        time.sleep(random.uniform(0.5, 1.5))
        
        # 尝试获取流通市值数据
        market_cap_dict = fetch_market_cap_data()
        
        if market_cap_dict and stock_code in market_cap_dict:
            market_cap = market_cap_dict[stock_code]
            logger.info(f"成功获取股票 {stock_code} 的市值数据: {market_cap:.2f}亿元")
            return market_cap
        else:
            logger.warning(f"股票 {stock_code} 的市值数据在批量获取中未找到")
            return None
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 市值数据失败: {str(e)}", exc_info=True)
        return None

def create_or_update_basic_info():
    """创建或更新股票基础信息文件，使用中文列名，包含 next_crawl_index 逻辑"""
    ensure_directory_exists()
    
    # 添加随机延时，避免请求过于频繁
    time.sleep(random.uniform(1.0, 2.0))
    
    # 获取股票列表
    try:
        stock_list = ak.stock_info_a_code_name()
        if stock_list.empty:
            logger.error("获取股票列表失败：返回为空")
            return False
        
        # 过滤ST股票和非主板/科创板/创业板股票
        stock_list = stock_list[~stock_list['name'].str.contains('ST', na=False)]
        stock_list = stock_list[stock_list['code'].str.startswith(('0', '3', '6'))]
        
        logger.info(f"成功获取股票列表，共 {len(stock_list)} 只股票")
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}", exc_info=True)
        return False
    
    # 获取市值数据
    market_cap_dict = fetch_market_cap_data()
    
    # 准备基础信息DataFrame - 严格使用中文列名
    basic_info_df = pd.DataFrame({
        "股票代码": stock_list['code'],
        "股票名称": stock_list['name'],
        "所属板块": stock_list['code'].apply(get_stock_section),
        "流通市值": stock_list['code'].apply(lambda x: market_cap_dict.get(str(x).zfill(6), 0.0))
    })
    
    # 确保流通市值列是数值类型
    basic_info_df["流通市值"] = pd.to_numeric(basic_info_df["流通市值"], errors='coerce').fillna(0)
    
    # 检查是否有无市值数据
    invalid_mask = (basic_info_df["流通市值"] <= 0) | basic_info_df["流通市值"].isna()
    invalid_count = invalid_mask.sum()
    
    # 如果有无市值数据，尝试补充
    if invalid_count > 0:
        logger.warning(f"检测到 {invalid_count} 条无有效市值数据的股票，正在尝试补充...")
        
        # 获取需要补充市值的股票列表
        invalid_stocks = basic_info_df[invalid_mask]["股票代码"].tolist()
        
        # 逐只股票尝试获取市值数据
        for stock_code in invalid_stocks:
            stock_code = str(stock_code).zfill(6)
            logger.info(f"尝试补充股票 {stock_code} 的市值数据")
            
            # 使用正确方式获取单只股票市值
            market_cap = fetch_single_stock_market_cap(stock_code)
            
            # 如果获取成功，更新数据
            if market_cap is not None:
                idx = basic_info_df[basic_info_df["股票代码"] == stock_code].index[0]
                basic_info_df.at[idx, "流通市值"] = market_cap
    
    # 再次检查
    invalid_mask = (basic_info_df["流通市值"] <= 0) | basic_info_df["流通市值"].isna()
    invalid_count = invalid_mask.sum()
    
    if invalid_count > 0:
        logger.warning(f"仍有 {invalid_count} 条无法获取市值数据的股票")
        # 标记为数据异常，但不设置默认值
        basic_info_df.loc[invalid_mask, "流通市值"] = 0.0
        basic_info_df["数据状态"] = '正常'
        basic_info_df.loc[invalid_mask, "数据状态"] = '流通市值缺失'
    
    # 添加 next_crawl_index 列（如果不存在）
    if "next_crawl_index" not in basic_info_df.columns:
        # 初始时，所有股票都标记为需要爬取
        basic_info_df["next_crawl_index"] = True
    
    # 保存基础信息
    basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
    logger.info(f"已创建/更新股票基础信息文件，共 {len(basic_info_df)} 条记录，其中 {len(basic_info_df[basic_info_df['流通市值'] > 0])} 条有有效市值数据")
    
    return True

def fetch_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """获取单只股票的日线数据，使用中文列名"""
    try:
        # 添加随机延时，避免请求过于频繁
        time.sleep(random.uniform(0.5, 1.5))
        
        # 确保股票代码是字符串，并且是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        
        # 确定市场前缀
        market_prefix = 'sh' if stock_code.startswith('6') else 'sz'
        ak_code = f"{market_prefix}{stock_code}"
        
        # 获取日线数据
        df = ak.stock_zh_a_hist(
            symbol=ak_code,
            period="daily",
            start_date=(datetime.now() - timedelta(days=730)).strftime("%Y%m%d"),
            end_date=datetime.now().strftime("%Y%m%d"),
            adjust=""
        )
        
        if df.empty:
            logger.warning(f"股票 {stock_code} 的日线数据为空")
            return pd.DataFrame()
        
        # 确保必要列存在
        required_columns = ["日期", "股票代码", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"股票 {stock_code} 数据缺少必要列: {col}")
                return pd.DataFrame()
        
        # 确保日期格式正确
        if '日期' in df.columns:
            # 处理多种日期格式
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
    except Exception as e:
        logger.error(f"保存股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)

def update_all_stocks_daily_data():
    """更新所有股票的日线数据，使用中文列名，包含 next_crawl_index 逻辑"""
    ensure_directory_exists()
    
    # 确保基础信息文件存在且不过期
    if not os.path.exists(BASIC_INFO_FILE) or is_file_expired(BASIC_INFO_FILE):
        logger.info("基础信息文件不存在或已过期，正在创建...")
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
    stock_codes = basic_info_df[basic_info_df["next_crawl_index"]]["股票代码"].tolist()
    if not stock_codes:
        logger.info("没有需要爬取的股票，next_crawl_index 均为 False")
        
        # 重置 next_crawl_index，标记所有股票为需要爬取
        basic_info_df["next_crawl_index"] = True
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        
        # 尝试获取新的股票列表
        stock_codes = basic_info_df["股票代码"].tolist()
        logger.info(f"重置 next_crawl_index，将爬取所有 {len(stock_codes)} 只股票")
    
    logger.info(f"开始更新 {len(stock_codes)} 只股票的日线数据（基于 next_crawl_index 标记）")
    
    # 处理股票
    def process_stock(stock_code):
        # 添加随机延时，避免请求过于频繁
        time.sleep(random.uniform(0.5, 1.5))
        df = fetch_stock_daily_data(stock_code)
        if not df.empty:
            save_stock_daily_data(stock_code, df)
    
    # 使用线程池并发处理
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(process_stock, stock_codes)
    
    # 更新 next_crawl_index
    try:
        # 读取最新的基础信息文件
        basic_info_df = pd.read_csv(BASIC_INFO_FILE)
        
        # 更新 next_crawl_index
        basic_info_df.loc[basic_info_df["股票代码"].isin(stock_codes), "next_crawl_index"] = False
        
        # 如果有超过 BATCH_SIZE 的股票未爬取，设置下一批股票的 next_crawl_index 为 True
        not_crawled_mask = ~basic_info_df["next_crawl_index"]
        not_crawled_count = not_crawled_mask.sum()
        
        if not_crawled_count > 0:
            # 获取未爬取的股票索引
            not_crawled_indices = basic_info_df[not_crawled_mask].index
            
            # 设置下一批股票的 next_crawl_index 为 True
            next_batch_indices = not_crawled_indices[:BATCH_SIZE]
            basic_info_df.loc[next_batch_indices, "next_crawl_index"] = True
            
            logger.info(f"设置下一批 {min(BATCH_SIZE, len(next_batch_indices))} 只股票的 next_crawl_index 为 True")
        
        # 保存更新
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"已更新基础信息文件的 next_crawl_index 列")
        
    except Exception as e:
        logger.error(f"更新 next_crawl_index 失败: {str(e)}", exc_info=True)
    
    logger.info("所有股票日线数据更新完成")
    return True

def get_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """获取股票日线数据（从本地），使用中文列名"""
    try:
        # 确保股票代码是字符串，并且是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        
        # 日线数据目录
        daily_dir = DAILY_DIR
        
        # 检查本地是否有历史数据
        file_path = os.path.join(daily_dir, f"{stock_code}.csv")
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                
                # 确保必要列存在
                required_columns = ["日期", "股票代码", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
                for col in required_columns:
                    if col not in df.columns:
                        logger.warning(f"股票 {stock_code} 数据缺少必要列: {col}")
                        return pd.DataFrame()
                
                # 确保日期列是字符串类型
                if "日期" in df.columns:
                    df["日期"] = df["日期"].astype(str)
                    # 移除可能存在的空格
                    df["日期"] = df["日期"].str.strip()
                    df = df.sort_values("日期", ascending=True)
                
                # 确保数值列是数值类型
                numeric_columns = ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 移除NaN值
                df = df.dropna(subset=['收盘', '成交量'])
                
                logger.debug(f"成功加载股票 {stock_code} 的本地日线数据，共 {len(df)} 条有效记录")
                return df
            except Exception as e:
                logger.warning(f"读取股票 {stock_code} 数据失败: {str(e)}")
        
        logger.warning(f"股票 {stock_code} 的日线数据不存在")
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def ensure_market_cap_data():
    """确保所有股票都有有效的市值数据，使用中文列名"""
    if not os.path.exists(BASIC_INFO_FILE):
        logger.info("基础信息文件不存在，正在创建...")
        create_or_update_basic_info()
        return
    
    try:
        basic_info_df = pd.read_csv(BASIC_INFO_FILE)
        if '流通市值' not in basic_info_df.columns:
            logger.warning("基础信息文件缺少流通市值列，重新获取基础信息")
            create_or_update_basic_info()
            return
        
        # 检查是否有无市值数据
        invalid_mask = (basic_info_df['流通市值'] <= 0) | basic_info_df['流通市值'].isna()
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            logger.info(f"检测到 {invalid_count} 条无效市值数据，正在修复...")
            
            # 添加随机延时，避免请求过于频繁
            time.sleep(random.uniform(1.0, 2.0))
            
            # 获取市值数据
            market_cap_dict = fetch_market_cap_data()
            
            # 修复无效数据
            for idx, row in basic_info_df[invalid_mask].iterrows():
                stock_code = row["股票代码"]
                if stock_code in market_cap_dict:
                    basic_info_df.at[idx, "流通市值"] = market_cap_dict[stock_code]
                    logger.info(f"成功修复股票 {stock_code} 的市值数据: {market_cap_dict[stock_code]:.2f}亿元")
            
            # 检查是否还有无效数据
            invalid_mask = (basic_info_df['流通市值'] <= 0) | basic_info_df['流通市值'].isna()
            invalid_count = invalid_mask.sum()
            
            if invalid_count > 0:
                logger.warning(f"仍然有 {invalid_count} 条无效市值数据")
                # 标记数据异常
                basic_info_df['数据状态'] = '正常'
                basic_info_df.loc[invalid_mask, '数据状态'] = '流通市值缺失'
            
            # 保存更新
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
            logger.info(f"已更新基础信息文件，共 {len(basic_info_df)} 条记录")
        
    except Exception as e:
        logger.error(f"确保市值数据完整时出错: {str(e)}", exc_info=True)
        logger.error(traceback.format_exc())

def main():
    """主函数：更新所有股票数据"""
    logger.info("===== 开始更新股票数据 =====")
    
    # 添加初始延时，避免立即请求
    time.sleep(random.uniform(1.0, 2.0))
    
    # 1. 确保基础信息文件存在且不过期
    if not os.path.exists(BASIC_INFO_FILE) or is_file_expired(BASIC_INFO_FILE):
        logger.info("基础信息文件不存在或已过期，正在更新...")
        if not create_or_update_basic_info():
            logger.error("基础信息文件更新失败，无法继续")
            return
    
    # 2. 确保市值数据完整
    ensure_market_cap_data()
    
    # 3. 更新所有股票日线数据（基于 next_crawl_index 逻辑）
    if update_all_stocks_daily_data():
        logger.info("日线数据更新成功")
    else:
        logger.error("日线数据更新失败")
    
    logger.info("===== 股票数据更新完成 =====")

if __name__ == "__main__":
    main()
