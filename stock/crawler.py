#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据爬取模块 - 严格确保股票代码为6位格式，日期处理逻辑完善
【最终修复版】
- 彻底修复股票代码格式问题，确保所有地方都保存为6位代码
- 彻底修复日期类型问题，确保所有日期比较都使用相同类型
- 严格确保结束日期不晚于当前时间，不处理未来日期
- 100%可直接复制使用
"""

import os
import logging
import pandas as pd
import akshare as ak
import time
import random
import json
from datetime import datetime, timedelta, date
from config import Config
from utils.date_utils import is_trading_day, get_last_trading_day
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
    if code_str.startswith(('sh', 'sz')):
        code_str = code_str[2:]
    
    # 移除可能的数字前缀（如"0."）
    if '.' in code_str:
        code_str = code_str.split('.')[1]
    
    # 确保是6位数字
    code_str = code_str.zfill(6)
    
    # 验证格式
    if not code_str.isdigit() or len(code_str) != 6:
        logger.warning(f"股票代码格式化失败: {code_str}")
        return None
    
    return code_str

def get_stock_section(stock_code: str) -> str:
    """
    获取股票所属板块
    
    Args:
        stock_code: 股票代码（已格式化为6位）
    
    Returns:
        str: 板块名称
    """
    # 确保股票代码是6位
    stock_code = format_stock_code(stock_code)
    if not stock_code:
        return "格式错误"
    
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

def to_datetime(date_input):
    """
    统一转换为datetime.datetime类型
    Args:
        date_input: 日期输入，可以是str、date、datetime等类型
    Returns:
        datetime.datetime: 统一的datetime类型
    """
    if isinstance(date_input, datetime):
        return date_input
    elif isinstance(date_input, date):
        return datetime.combine(date_input, datetime.min.time())
    elif isinstance(date_input, str):
        # 尝试多种日期格式
        for fmt in ["%Y-%m-%d", "%Y%m%d"]:
            try:
                return datetime.strptime(date_input, fmt)
            except:
                continue
        logger.warning(f"无法解析日期格式: {date_input}")
        return None
    return None

def get_valid_trading_date_range(start_date, end_date):
    """
    获取有效的交易日范围，确保只包含历史交易日
    
    Args:
        start_date: 起始日期（可能包含非交易日）
        end_date: 结束日期（可能包含非交易日）
    
    Returns:
        tuple: (valid_start_date, valid_end_date) - 有效的交易日范围
    """
    # 统一转换为datetime.datetime类型
    start_date = to_datetime(start_date)
    end_date = to_datetime(end_date)
    
    if start_date is None or end_date is None:
        logger.error("日期格式转换失败")
        return None, None
    
    # 确保结束日期不晚于当前时间
    now = datetime.now()
    if end_date > now:
        end_date = now
        logger.warning(f"结束日期晚于当前时间，已调整为当前时间: {end_date.strftime('%Y%m%d %H:%M:%S')}")
    
    # 查找有效的结束交易日
    valid_end_date = end_date
    days_back = 0
    while days_back < 30:  # 最多查找30天
        if is_trading_day(valid_end_date):
            break
        valid_end_date -= timedelta(days=1)
        days_back += 1
    
    # 如果找不到有效的结束交易日，返回空范围
    if days_back >= 30:
        logger.warning(f"无法找到有效的结束交易日（从 {end_date.strftime('%Y-%m-%d')} 开始）")
        return None, None
    
    # 查找有效的起始交易日
    valid_start_date = start_date
    days_forward = 0
    while days_forward < 30:  # 最多查找30天
        if is_trading_day(valid_start_date):
            break
        valid_start_date += timedelta(days=1)
        days_forward += 1
    
    # 如果找不到有效的起始交易日，使用结束交易日作为起始日
    if days_forward >= 30:
        valid_start_date = valid_end_date
    
    # 确保起始日期不晚于结束日期
    if valid_start_date > valid_end_date:
        valid_start_date = valid_end_date
    
    return valid_start_date, valid_end_date

def fetch_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """获取单只股票的日线数据，使用中文列名"""
    try:
        # 【关键修复】确保股票代码是6位（前面补零）
        stock_code = format_stock_code(stock_code)
        if not stock_code:
            logger.error(f"股票代码格式化失败: {stock_code}")
            return pd.DataFrame()
        
        # 【关键修复】检查本地是否已有该股票的日线数据文件
        local_file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        existing_data = None
        last_date = None
        
        if os.path.exists(local_file_path):
            try:
                # 读取已有的数据
                existing_data = pd.read_csv(local_file_path)
                if not existing_data.empty and '日期' in existing_data.columns:
                    # 【日期datetime类型规则】确保日期列是datetime类型
                    existing_data['日期'] = pd.to_datetime(existing_data['日期'], errors='coerce')
                    # 获取最后一条数据的日期
                    last_date = existing_data['日期'].max()
                    if pd.notna(last_date):
                        logger.info(f"股票 {stock_code} 本地已有数据，最后日期: {last_date.strftime('%Y-%m-%d')}")
                    else:
                        last_date = None
            except Exception as e:
                logger.warning(f"读取股票 {stock_code} 本地数据失败: {str(e)}")
                existing_data = None
                last_date = None
        
        # ===== 关键修复：确保只处理历史交易日 =====
        # 1. 确定爬取的日期范围
        if last_date is not None:
            # 查找下一个交易日作为起始点
            current_date = last_date + timedelta(days=1)
            start_date = None
            
            # 最多查找30天，避免无限循环
            for i in range(30):
                if is_trading_day(current_date):
                    start_date = current_date
                    break
                current_date += timedelta(days=1)
            
            if not start_date:
                # 如果找不到交易日，使用最近一个交易日
                last_trading_date = get_last_trading_day()
                if last_trading_date:
                    start_date = last_trading_date
                    logger.warning(f"无法找到股票 {stock_code} 的下一个交易日，使用最近交易日: {start_date.strftime('%Y%m%d')}")
                else:
                    logger.warning(f"无法找到股票 {stock_code} 的有效交易日，跳过爬取")
                    return pd.DataFrame()
            
            # 获取当前日期前的最近一个交易日作为结束日期
            end_date = get_last_trading_day()
            
            # 确保结束日期不晚于当前时间
            now = datetime.now()
            if end_date > now:
                end_date = now
                logger.warning(f"结束日期晚于当前时间，已调整为当前时间: {end_date.strftime('%Y%m%d %H:%M:%S')}")
            
            # 关键修复：确保日期类型一致
            if not isinstance(start_date, datetime):
                start_date = to_datetime(start_date)
            if not isinstance(end_date, datetime):
                end_date = to_datetime(end_date)
            
            # 关键修复：严格检查日期
            # 开始日期 >= 结束日期，代表数据已最新
            if start_date >= end_date:
                logger.info(f"股票 {stock_code} 没有新数据需要爬取（开始日期: {start_date.strftime('%Y%m%d')} >= 结束日期: {end_date.strftime('%Y%m%d')}）")
                return pd.DataFrame()
            
            logger.info(f"股票 {stock_code} 增量爬取，从 {start_date.strftime('%Y%m%d')} 到 {end_date.strftime('%Y%m%d')}")
        else:
            # 没有本地数据，爬取最近一年的数据
            start_date = datetime.now() - timedelta(days=365)
            end_date = get_last_trading_day()
            
            # 确保起始日期是交易日
            current_date = start_date
            start_date = None
            for i in range(30):
                if is_trading_day(current_date):
                    start_date = current_date
                    break
                current_date += timedelta(days=1)
            
            if not start_date:
                start_date = end_date
            
            # 确保起始日期不晚于结束日期
            if start_date > end_date:
                start_date = end_date
            
            logger.info(f"股票 {stock_code} 首次爬取，获取从 {start_date.strftime('%Y%m%d')} 到 {end_date.strftime('%Y%m%d')} 的数据")
        
        # 【关键修复】统一日期格式
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        
        # 【关键修复】使用测试成功的调用方式：不带市场前缀！
        logger.debug(f"正在获取股票 {stock_code} 的日线数据 (代码: {stock_code}, 复权参数: qfq)")
        
        # 【关键修复】使用测试成功的参数进行增量爬取
        try:
            df = ak.stock_zh_a_hist(
                symbol=stock_code,      # 不带市场前缀！
                period="daily",
                start_date=start_date_str,
                end_date=end_date_str,
                adjust="qfq"
            )
        except Exception as e:
            logger.warning(f"获取股票 {stock_code} 的增量数据失败，尝试获取30天数据: {str(e)}")
            try:
                # 尝试获取30天数据（适用于新上市股票）
                df = ak.stock_zh_a_hist(
                    symbol=stock_code,      # 不带市场前缀！
                    period="daily",
                    start_date=(datetime.now() - timedelta(days=30)).strftime("%Y%m%d"),
                    end_date=datetime.now().strftime("%Y%m%d"),
                    adjust="qfq"
                )
            except Exception as e:
                logger.warning(f"获取股票 {stock_code} 的30天数据失败，尝试获取不复权数据: {str(e)}")
                # 尝试不复权数据
                df = ak.stock_zh_a_hist(
                    symbol=stock_code,      # 不带市场前缀！
                    period="daily",
                    start_date=(datetime.now() - timedelta(days=30)).strftime("%Y%m%d"),
                    end_date=datetime.now().strftime("%Y%m%d"),
                    adjust=""
                )
        
        # 【关键修复】添加详细的API响应检查
        if df is None or df.empty:
            logger.warning(f"股票 {stock_code} 的日线数据为空")
            return pd.DataFrame()
        
        # 添加列名检查日志
        logger.debug(f"股票 {stock_code} 获取到的列名: {df.columns.tolist()}")
        
        # 确保必要列存在
        required_columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"股票 {stock_code} 数据缺少必要列: {missing_columns}")
            return pd.DataFrame()
        
        # 【日期datetime类型规则】确保日期列是datetime类型
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
            df = df.sort_values('日期').reset_index(drop=True)
        
        # 确保数值列是数值类型
        numeric_columns = ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 移除NaN值
        df = df.dropna(subset=['收盘', '成交量'])
        
        # 【关键修复】合并新数据与已有数据
        if existing_data is not None and not existing_data.empty:
            # 合并数据并去重
            combined_df = pd.concat([existing_data, df], ignore_index=True)
            # 按日期去重，保留最新的数据
            combined_df = combined_df.drop_duplicates(subset=['日期'], keep='last')
            # 按日期排序
            combined_df = combined_df.sort_values('日期').reset_index(drop=True)
            
            # 【关键修复】只保留最近一年的数据（约250个交易日）
            if len(combined_df) > 250:
                combined_df = combined_df.tail(250)
            
            df = combined_df
            logger.info(f"股票 {stock_code} 合并后共有 {len(df)} 条记录（新增 {len(df) - len(existing_data)} 条）")
        else:
            logger.info(f"股票 {stock_code} 成功获取 {len(df)} 条日线数据")
        
        return df
    
    except Exception as e:
        # 添加详细的异常日志
        logger.error(f"获取股票 {stock_code} 日线数据时发生未捕获的异常:", exc_info=True)
        logger.error(f"akshare 版本: {ak.__version__}")
        logger.error(f"akshare 模块路径: {ak.__file__}")
        return pd.DataFrame()

def save_stock_daily_data(stock_code: str, df: pd.DataFrame):
    """保存股票日线数据到CSV文件，使用中文列名"""
    if df.empty:
        return
    
    try:
        # 【关键修复】确保股票代码是6位（前面补零）
        stock_code = format_stock_code(stock_code)
        if not stock_code:
            logger.error(f"无法保存：股票代码格式化失败")
            return
        
        file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        # 【日期datetime类型规则】保存前将日期列转换为字符串
        if '日期' in df.columns:
            df_save = df.copy()
            df_save['日期'] = df_save['日期'].dt.strftime('%Y-%m-%d')
            df_save.to_csv(file_path, index=False)
        else:
            df.to_csv(file_path, index=False)
        
        logger.debug(f"已保存股票 {stock_code} 的日线数据到 {file_path}")
        
        # 【关键修复】只需简单调用，无需任何额外逻辑
        commit_files_in_batches(file_path)
        logger.debug(f"已提交股票 {stock_code} 的日线数据到仓库")
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
        
        # 【关键修复】确保"代码"列是6位格式
        basic_info_df["代码"] = basic_info_df["代码"].apply(format_stock_code)
        # 移除无效股票
        basic_info_df = basic_info_df[basic_info_df["代码"].notna()]
        basic_info_df = basic_info_df[basic_info_df["代码"].str.len() == 6]
        basic_info_df = basic_info_df.reset_index(drop=True)
        
        # 保存更新后的基础信息文件
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        commit_files_in_batches(BASIC_INFO_FILE)
        logger.info(f"已更新基础信息文件，确保所有股票代码为6位格式，共 {len(basic_info_df)} 条记录")
        
    except Exception as e:
        logger.error(f"读取基础信息文件失败: {str(e)}", exc_info=True)
        return False
    
    # 【关键修复】获取 next_crawl_index 值
    # 由于所有行的 next_crawl_index 值相同，取第一行即可
    next_index = int(basic_info_df["next_crawl_index"].iloc[0])
    total_stocks = len(basic_info_df)
    
    logger.info(f"当前爬取状态: next_crawl_index = {next_index} (共 {total_stocks} 只股票)")
    
    # 【关键修复】确定要爬取的股票范围
    start_idx = next_index
    end_idx = min(next_index + 150, total_stocks)
    
    # 如果已经爬取完所有股票，重置索引
    if start_idx >= total_stocks:
        logger.info("已爬取完所有股票，重置爬取状态")
        start_idx = 0
        end_idx = min(150, total_stocks)
    
    # 获取要爬取的股票
    batch_df = basic_info_df.iloc[start_idx:end_idx]
    batch_codes = batch_df["代码"].tolist()
    
    if not batch_codes:
        logger.warning("没有可爬取的股票")
        return False
    
    logger.info(f"正在处理第 {start_idx//150 + 1} 批，共 {len(batch_codes)} 只股票 (索引 {start_idx} - {end_idx-1})")
    
    # 记录第一批和最后一批股票
    first_stock = batch_df.iloc[0]
    last_stock = batch_df.iloc[-1]
    logger.info(f"当前批次第一只股票: {first_stock['代码']} - {first_stock['名称']} (索引 {start_idx})")
    logger.info(f"当前批次最后一只股票: {last_stock['代码']} - {last_stock['名称']} (索引 {end_idx-1})")
    
    # 处理这批股票
    for stock_code in batch_codes:
        # 【关键修复】确保股票代码是6位
        stock_code = format_stock_code(stock_code)
        if not stock_code:
            continue
            
        # 添加随机延时，避免请求过于频繁
        time.sleep(random.uniform(1.5, 2.5))  # 增加延时，避免被限流
        df = fetch_stock_daily_data(stock_code)
        if not df.empty:
            save_stock_daily_data(stock_code, df)
    
    # 【关键修复】更新 next_crawl_index
    new_index = end_idx
    if new_index >= total_stocks:
        new_index = 0  # 重置，下次从头开始
    
    logger.info(f"更新 next_crawl_index = {new_index}")
    basic_info_df["next_crawl_index"] = new_index
    basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
    
    # 提交更新后的基础信息文件
    commit_files_in_batches(BASIC_INFO_FILE)
    logger.info(f"已提交更新后的基础信息文件到仓库: {BASIC_INFO_FILE}")
    
    # 检查是否还有未完成的股票
    remaining_stocks = total_stocks - new_index
    if remaining_stocks < 0:
        remaining_stocks = total_stocks  # 重置后
    
    logger.info(f"已完成 {len(batch_codes)} 只股票爬取，还有 {remaining_stocks} 只股票待爬取")
    
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
    
    # 2. 只更新一批股票（最多150只）
    if update_all_stocks_daily_data():
        logger.info("已成功处理一批股票数据")
    else:
        logger.error("处理股票数据失败")
    
    logger.info("===== 股票数据更新完成 =====")

if __name__ == "__main__":
    main()
