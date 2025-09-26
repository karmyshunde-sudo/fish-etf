#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据爬取模块
负责爬取股票基础信息和日线数据，确保数据完整性
"""

import os
import logging
import pandas as pd
import akshare as ak
import time
import numpy as np
from datetime import datetime, timedelta
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
from config import Config

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

def get_stock_list():
    """获取股票列表"""
    try:
        # 获取A股股票列表
        stock_list = ak.stock_info_a_code_name()
        if stock_list.empty:
            logger.error("获取股票列表失败：返回为空")
            return pd.DataFrame()
        
        # 过滤ST股票和非主板/科创板/创业板股票
        stock_list = stock_list[~stock_list['name'].str.contains('ST', na=False)]
        stock_list = stock_list[stock_list['code'].str.startswith(('0', '3', '6'))]
        
        logger.info(f"成功获取股票列表，共 {len(stock_list)} 只股票")
        return stock_list
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def fetch_market_cap_data():
    """获取股票流通市值数据"""
    try:
        # 获取流通市值数据
        df = ak.stock_zh_a_spot_em()
        
        # 创建市值字典
        market_cap_dict = {}
        
        # 确保列名正确
        required_columns = ["代码", "名称", "流通市值"]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"获取流通市值数据失败: 缺少必要列 {col}")
                return {}
        
        for _, row in df.iterrows():
            stock_code = str(row['代码']).zfill(6)
            # 流通市值单位是万元，转换为亿元
            try:
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

def create_or_update_basic_info():
    """创建或更新股票基础信息文件"""
    ensure_directory_exists()
    
    # 获取股票列表
    stock_list = get_stock_list()
    if stock_list.empty:
        logger.error("获取股票列表失败，基础信息文件更新失败")
        return False
    
    # 获取市值数据
    market_cap_dict = fetch_market_cap_data()
    
    # 准备基础信息DataFrame
    basic_info_df = pd.DataFrame({
        'code': stock_list['code'],
        'name': stock_list['name'],
        'section': stock_list['code'].apply(get_stock_section),
        'next_crawl_index': 0
    })
    
    # 添加市值数据
    basic_info_df['market_cap'] = basic_info_df['code'].apply(
        lambda x: market_cap_dict.get(str(x).zfill(6), 0.0)
    )
    
    # 确保市值列是数值类型
    basic_info_df['market_cap'] = pd.to_numeric(basic_info_df['market_cap'], errors='coerce').fillna(0)
    
    # 关键修复：不再移除无市值股票，而是记录并尝试修复
    initial_count = len(basic_info_df)
    invalid_mask = (basic_info_df['market_cap'].isna()) | (basic_info_df['market_cap'] <= 0)
    invalid_count = invalid_mask.sum()
    
    if invalid_count > 0:
        logger.warning(f"检测到 {invalid_count} 条无有效市值数据的股票")
        # 尝试修复无市值股票
        for idx, row in basic_info_df[invalid_mask].iterrows():
            stock_code = row['code']
            logger.info(f"尝试修复股票 {stock_code} 的市值数据")
            # 尝试重新获取单只股票数据
            stock_data = ak.stock_zh_a_spot_em(symbol=stock_code)
            if not stock_data.empty and '流通市值' in stock_data.columns:
                try:
                    market_cap = float(stock_data['流通市值'].values[0]) / 10000
                    if not np.isnan(market_cap) and market_cap > 0:
                        basic_info_df.at[idx, 'market_cap'] = market_cap
                        logger.info(f"成功修复股票 {stock_code} 的市值数据: {market_cap:.2f}亿元")
                except:
                    pass
    
    # 再次检查
    invalid_mask = (basic_info_df['market_cap'].isna()) | (basic_info_df['market_cap'] <= 0)
    invalid_count = invalid_mask.sum()
    
    if invalid_count > 0:
        logger.warning(f"仍然有 {invalid_count} 条无有效市值数据的股票")
        # 为无市值股票设置默认值，而不是移除
        basic_info_df.loc[invalid_mask, 'market_cap'] = 50.0  # 设置为50亿元默认值
    
    # 保存基础信息
    basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
    logger.info(f"已创建/更新股票基础信息文件，共 {len(basic_info_df)} 条记录，所有股票都有有效市值数据")
    
    return True

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

def fetch_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """获取单只股票的日线数据"""
    try:
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
        
        # 关键修复：正确映射中文列名到英文列名
        # stock_zh_a_hist 返回的是中文列名，我们需要映射为英文列名
        if "日期" in df.columns:
            df = df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'turnover',
                '振幅': 'amplitude',
                '涨跌幅': 'change_percent',
                '换手率': 'turnover_rate'
            })
        elif "date" in df.columns:
            # 已经是英文列名，无需处理
            pass
        else:
            logger.warning(f"股票 {stock_code} 数据列名不匹配")
            return pd.DataFrame()
        
        # 确保必要列存在
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"股票 {stock_code} 数据缺少必要列: {col}")
                return pd.DataFrame()
        
        # 确保日期格式正确
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.sort_values('date').reset_index(drop=True)
        
        # 确保数值列是数值类型
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'turnover']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 移除NaN值
        df = df.dropna(subset=['close', 'volume'])
        
        logger.info(f"成功获取股票 {stock_code} 的日线数据，共 {len(df)} 条记录")
        return df
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def save_stock_daily_data(stock_code: str, df: pd.DataFrame):
    """保存股票日线数据到CSV文件"""
    if df.empty:
        return
    
    try:
        file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        df.to_csv(file_path, index=False)
        logger.debug(f"已保存股票 {stock_code} 的日线数据到 {file_path}")
    except Exception as e:
        logger.error(f"保存股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)

def update_all_stocks_daily_data():
    """更新所有股票的日线数据"""
    ensure_directory_exists()
    
    # 获取基础信息文件
    if not os.path.exists(BASIC_INFO_FILE):
        logger.error(f"基础信息文件 {BASIC_INFO_FILE} 不存在，无法更新日线数据")
        return False
    
    basic_info_df = pd.read_csv(BASIC_INFO_FILE)
    if basic_info_df.empty:
        logger.error("基础信息文件为空，无法更新日线数据")
        return False
    
    # 获取需要更新的股票列表
    stock_codes = basic_info_df['code'].tolist()
    logger.info(f"开始更新 {len(stock_codes)} 只股票的日线数据")
    
    def process_stock(stock_code):
        df = fetch_stock_daily_data(stock_code)
        if not df.empty:
            save_stock_daily_data(stock_code, df)
    
    # 使用线程池并发处理
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_stock, stock_codes)
    
    logger.info("所有股票日线数据更新完成")
    return True

def get_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """获取股票日线数据（从本地）"""
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
                required_columns = ["date", "open", "high", "low", "close", "volume"]
                for col in required_columns:
                    if col not in df.columns:
                        logger.warning(f"股票 {stock_code} 数据缺少必要列: {col}")
                        return pd.DataFrame()
                
                # 确保日期列是字符串类型
                if "date" in df.columns:
                    df["date"] = df["date"].astype(str)
                    # 移除可能存在的空格
                    df["date"] = df["date"].str.strip()
                    df = df.sort_values("date", ascending=True)
                
                # 确保数值列是数值类型
                numeric_columns = ["open", "high", "low", "close", "volume"]
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 移除NaN值
                df = df.dropna(subset=['close', 'volume'])
                
                logger.debug(f"成功加载股票 {stock_code} 的本地日线数据，共 {len(df)} 条有效记录")
                return df
            except Exception as e:
                logger.warning(f"读取股票 {stock_code} 数据失败: {str(e)}")
                logger.debug(traceback.format_exc())
        
        logger.warning(f"股票 {stock_code} 的日线数据不存在")
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def ensure_market_cap_data():
    """确保所有股票都有有效的市值数据"""
    if not os.path.exists(BASIC_INFO_FILE):
        logger.info("基础信息文件不存在，正在创建...")
        create_or_update_basic_info()
        return
    
    try:
        basic_info_df = pd.read_csv(BASIC_INFO_FILE)
        if 'market_cap' not in basic_info_df.columns:
            logger.warning("基础信息文件缺少市值列，重新获取基础信息")
            create_or_update_basic_info()
            return
        
        # 检查是否有无市值数据
        invalid_mask = (basic_info_df['market_cap'].isna()) | (basic_info_df['market_cap'] <= 0)
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            logger.info(f"检测到 {invalid_count} 条无效市值数据，正在修复...")
            
            # 获取市值数据
            market_cap_dict = fetch_market_cap_data()
            
            # 修复无效数据
            for idx, row in basic_info_df[invalid_mask].iterrows():
                stock_code = row['code']
                if stock_code in market_cap_dict:
                    basic_info_df.at[idx, 'market_cap'] = market_cap_dict[stock_code]
                    logger.info(f"成功修复股票 {stock_code} 的市值数据: {market_cap_dict[stock_code]:.2f}亿元")
            
            # 检查是否还有无效数据
            invalid_mask = (basic_info_df['market_cap'].isna()) | (basic_info_df['market_cap'] <= 0)
            invalid_count = invalid_mask.sum()
            
            if invalid_count > 0:
                logger.warning(f"仍然有 {invalid_count} 条无效市值数据，使用默认值50亿元修复")
                # 为无市值股票设置默认值
                basic_info_df.loc[invalid_mask, 'market_cap'] = 50.0
            
            # 保存更新
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
            logger.info(f"已更新基础信息文件，所有股票都有有效市值数据")
        
    except Exception as e:
        logger.error(f"确保市值数据完整时出错: {str(e)}", exc_info=True)
        logger.error(traceback.format_exc())

def main():
    """主函数：更新所有股票数据"""
    logger.info("===== 开始更新股票数据 =====")
    
    # 1. 创建/更新基础信息文件
    if create_or_update_basic_info():
        logger.info("基础信息文件更新成功")
    else:
        logger.error("基础信息文件更新失败")
    
    # 2. 确保市值数据完整
    ensure_market_cap_data()
    
    # 3. 更新所有股票日线数据
    if update_all_stocks_daily_data():
        logger.info("日线数据更新成功")
    else:
        logger.error("日线数据更新失败")
    
    logger.info("===== 股票数据更新完成 =====")

if __name__ == "__main__":
    main()
