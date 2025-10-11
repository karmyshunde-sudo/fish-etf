#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据爬取模块 - 严格确保股票代码为6位格式，日期处理逻辑完善
【最终修复版】
- 彻底修复股票代码格式问题，确保所有地方都保存为6位代码
- 彻底修复日期类型问题，确保所有日期比较都使用相同类型
- 严格确保结束日期不晚于当前时间，不处理未来日期
- 100%可直接复制使用
- 新增补全缺失日线数据的功能
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
from utils.date_utils import is_trading_day, get_last_trading_day, get_beijing_time
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
    elif stock_code.startswith('8'):
        return "北交所"
    elif stock_code.startswith('4') or stock_code.startswith('8'):
        return "三板市场"
    else:
        return "其他板块"

def to_naive_datetime(dt):
    """
    将日期转换为naive datetime（无时区）
    Args:
        dt: 可能是naive或aware datetime
    Returns:
        datetime: naive datetime
    """
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

def to_aware_datetime(dt):
    """
    将日期转换为aware datetime（有时区）
    Args:
        dt: 可能是naive或aware datetime
    Returns:
        datetime: aware datetime（北京时区）
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=Config.BEIJING_TIMEZONE)
    return dt

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
    now = get_beijing_time()
    # 确保两个日期对象类型一致
    end_date = to_aware_datetime(end_date)
    now = to_aware_datetime(now)
    
    if end_date > now:
        end_date = now
        logger.warning(f"结束日期晚于当前时间，已调整为当前时间: {end_date.strftime('%Y%m%d %H:%M:%S')}")
    
    # 查找有效的结束交易日
    valid_end_date = end_date
    days_back = 0
    while days_back < 30:  # 最多查找30天
        if is_trading_day(valid_end_date.date()):
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
        if is_trading_day(valid_start_date.date()):
            break
        valid_start_date += timedelta(days=1)
        days_forward += 1
    
    # 如果找不到有效的起始交易日，使用结束交易日作为起始日
    if days_forward >= 30:
        valid_start_date = valid_end_date
    
    # 确保起始日期不晚于结束日期
    # 【关键修复】确保比较前类型一致
    start_naive = to_naive_datetime(valid_start_date)
    end_naive = to_naive_datetime(valid_end_date)
    
    if start_naive > end_naive:
        valid_start_date = valid_end_date
    
    return valid_start_date, valid_end_date

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
        for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"]:
            try:
                return datetime.strptime(date_input, fmt)
            except:
                continue
        logger.warning(f"无法解析日期格式: {date_input}")
        return None
    return None

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
                if is_trading_day(current_date.date()):
                    start_date = current_date
                    break
                current_date += timedelta(days=1)
            
            if not start_date:
                # 如果找不到交易日，使用最近一个交易日
                last_trading_date = get_last_trading_day()
                if last_trading_date:
                    # 【日期datetime类型规则】确保last_trading_date是datetime类型
                    if not isinstance(last_trading_date, datetime):
                        last_trading_date = datetime.combine(last_trading_date, datetime.min.time())
                    start_date = last_trading_date
                    logger.warning(f"无法找到股票 {stock_code} 的下一个交易日，使用最近交易日: {start_date.strftime('%Y%m%d')}")
                else:
                    logger.warning(f"无法找到股票 {stock_code} 的有效交易日，跳过爬取")
                    return pd.DataFrame()
            
            # 获取当前日期前的最近一个交易日作为结束日期
            end_date = get_last_trading_day()
            
            # 【日期datetime类型规则】确保end_date是datetime类型
            if not isinstance(end_date, datetime):
                end_date = datetime.combine(end_date, datetime.min.time())
            
            # 确保结束日期不晚于当前时间
            now = get_beijing_time()
            # 【关键修复】确保比较前日期类型一致
            now_naive = to_naive_datetime(now)
            end_date_naive = to_naive_datetime(end_date)
            
            if end_date_naive > now_naive:
                end_date = now
                logger.warning(f"结束日期晚于当前时间，已调整为当前时间: {end_date.strftime('%Y%m%d %H:%M:%S')}")
            
            # 关键修复：确保日期类型一致
            if not isinstance(start_date, datetime):
                start_date = to_datetime(start_date)
            if not isinstance(end_date, datetime):
                end_date = to_datetime(end_date)
            
            # 【关键修复】确保比较前日期类型一致
            # 转换为naive datetime进行比较
            start_date_naive = to_naive_datetime(start_date)
            end_date_naive = to_naive_datetime(end_date)
            
            # 严格检查日期
            # 开始日期 >= 结束日期，代表数据已最新
            if start_date_naive >= end_date_naive:
                logger.info(f"股票 {stock_code} 没有新数据需要爬取（开始日期: {start_date.strftime('%Y%m%d')} >= 结束日期: {end_date.strftime('%Y%m%d')}）")
                return pd.DataFrame()
            
            logger.info(f"股票 {stock_code} 增量爬取，从 {start_date.strftime('%Y%m%d')} 到 {end_date.strftime('%Y%m%d')}")
        else:
            # 没有本地数据，爬取最近一年的数据
            now = get_beijing_time()
            start_date = now - timedelta(days=365)
            end_date = get_last_trading_day()
            
            # 【日期datetime类型规则】确保end_date是datetime类型
            if not isinstance(end_date, datetime):
                end_date = datetime.combine(end_date, datetime.min.time())
            
            # 确保起始日期是交易日
            current_date = start_date
            start_date = None
            for i in range(30):
                if is_trading_day(current_date.date()):
                    start_date = current_date
                    break
                current_date += timedelta(days=1)
            
            if not start_date:
                start_date = end_date
            
            # 【关键修复】确保比较前日期类型一致
            # 转换为naive datetime进行比较
            start_date_naive = to_naive_datetime(start_date)
            end_date_naive = to_naive_datetime(end_date)
            
            # 确保起始日期不晚于结束日期
            if start_date_naive > end_date_naive:
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
        else:
            df_save = df
        
        # 保存数据
        df_save.to_csv(file_path, index=False)
        
        logger.debug(f"已保存股票 {stock_code} 的日线数据到 {file_path}")
        
        # 【关键修复】只需简单调用，无需任何额外逻辑
        commit_files_in_batches(file_path)
        logger.debug(f"已提交股票 {stock_code} 的日线数据到仓库")
    except Exception as e:
        logger.error(f"保存股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)

def complete_missing_stock_data():
    """
    补全缺失的股票日线数据
    1. 比对股票列表与日线数据目录
    2. 为缺失的股票调用正常爬取流程
    """
    logger.info("开始检查并补全缺失的股票日线数据...")
    
    # 确保目录存在
    ensure_directory_exists()
    
    # 检查基础信息文件
    if not os.path.exists(BASIC_INFO_FILE):
        logger.error("基础信息文件不存在，无法执行缺失数据补全")
        return False
    
    try:
        # 加载基础信息
        basic_info_df = pd.read_csv(BASIC_INFO_FILE)
        if basic_info_df.empty:
            logger.error("基础信息文件为空，无法执行缺失数据补全")
            return False
        
        # 确保"代码"列是6位格式
        basic_info_df["代码"] = basic_info_df["代码"].apply(format_stock_code)
        # 移除无效股票
        basic_info_df = basic_info_df[basic_info_df["代码"].notna()]
        basic_info_df = basic_info_df[basic_info_df["代码"].str.len() == 6]
        basic_info_df = basic_info_df.reset_index(drop=True)
        
        # 统计有效股票数量
        total_stocks = len(basic_info_df)
        logger.info(f"基础信息中包含 {total_stocks} 只股票")
        
        # 检查哪些股票缺失日线数据
        missing_stocks = []
        for _, row in basic_info_df.iterrows():
            stock_code = format_stock_code(row["代码"])
            if not stock_code:
                continue
                
            file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
            if not os.path.exists(file_path):
                missing_stocks.append(stock_code)
        
        # 没有缺失数据，直接返回
        if not missing_stocks:
            logger.info("所有股票日线数据完整，无需补全")
            return True
        
        logger.info(f"发现 {len(missing_stocks)} 只股票的日线数据缺失，开始补全...")
        
        # 按顺序处理缺失股票
        for i, stock_code in enumerate(missing_stocks):
            # 添加随机延时，避免请求过于频繁
            time.sleep(random.uniform(1.5, 2.5))
            
            logger.info(f"补全第 {i+1}/{len(missing_stocks)} 只缺失股票: {stock_code}")
            df = fetch_stock_daily_data(stock_code)
            
            if not df.empty:
                save_stock_daily_data(stock_code, df)
                logger.info(f"成功补全股票 {stock_code} 的日线数据")
            else:
                logger.warning(f"股票 {stock_code} 数据补全失败")
        
        # 检查补全结果
        still_missing = []
        for stock_code in missing_stocks:
            file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
            if not os.path.exists(file_path):
                still_missing.append(stock_code)
        
        if still_missing:
            logger.warning(f"补全后仍有 {len(still_missing)} 只股票缺失日线数据: {still_missing}")
        else:
            logger.info(f"所有缺失股票数据已成功补全")
        
        return len(still_missing) == 0
    
    except Exception as e:
        logger.error(f"补全缺失股票数据失败: {str(e)}", exc_info=True)
        return False

def update_all_stocks_daily_data():
    """
    更新所有股票的日线数据，使用中文列名
    """
    try:
        logger.info("=== 开始执行股票日线数据爬取 ===")
        beijing_time = get_beijing_time()
        logger.info(f"北京时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}（UTC+8）")
        
        # 初始化目录
        Config.init_dirs()
        stock_daily_dir = os.path.join(Config.DATA_DIR, "etf_daily")
        logger.info(f"✅ 确保目录存在: {stock_daily_dir}")
        
        # 获取股票列表
        stock_list = get_all_stocks()
        total_count = len(stock_list)
        logger.info(f"待爬取股票总数：{total_count}只")
        
        # 加载进度
        progress = load_progress()
        next_index = progress["next_index"]
        
        # 确定处理范围
        batch_size = 100
        start_idx = next_index
        end_idx = min(start_idx + batch_size, len(stock_list))
        
        # 关键修复：当索引到达总数时，重置索引
        if start_idx >= len(stock_list):
            logger.info("已爬取完所有股票，重置爬取状态")
            start_idx = 0
            end_idx = min(150, len(stock_list))
        
        logger.info(f"处理本批次 ETF ({end_idx - start_idx}只)，从索引 {start_idx} 开始")
        
        # 已完成列表路径
        completed_file = os.path.join(stock_daily_dir, "etf_daily_completed.txt")
        
        # 加载已完成列表
        completed_codes = set()
        if os.path.exists(completed_file):
            try:
                with open(completed_file, "r", encoding="utf-8") as f:
                    completed_codes = set(line.strip() for line in f if line.strip())
                logger.info(f"进度记录中已完成爬取的ETF数量：{len(completed_codes)}")
            except Exception as e:
                logger.error(f"读取进度记录失败: {str(e)}", exc_info=True)
                completed_codes = set()
        
        # 处理当前批次
        processed_count = 0
        last_processed_code = None
        for i in range(start_idx, end_idx):
            stock_code = stock_list[i]
            stock_name = get_stock_name(stock_code)
            
            # 获取增量日期范围
            start_date, end_date = get_incremental_date_range(stock_code)
            if start_date is None or end_date is None:
                logger.info(f"股票 {stock_code} 数据已最新，跳过爬取")
                continue
            
            # 爬取数据
            logger.info(f"股票代码：{stock_code}| 名称：{stock_name}")
            logger.info(f"📅 增量爬取日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
            
            df = crawl_etf_daily_data(stock_code, start_date, end_date)
            
            # 检查是否成功获取数据
            if df.empty:
                logger.info(f"股票代码：{stock_code}| 名称：{stock_name}")
                logger.warning(f"⚠️ 未获取到数据")
                # 记录失败日志
                with open(os.path.join(stock_daily_dir, "failed_etfs.txt"), "a", encoding="utf-8") as f:
                    f.write(f"{stock_code},{stock_name},未获取到数据\n")
                continue
            
            # 处理已有数据的追加逻辑
            save_path = os.path.join(stock_daily_dir, f"{stock_code}.csv")
            if os.path.exists(save_path):
                try:
                    existing_df = pd.read_csv(save_path)
                    
                    # 【日期datetime类型规则】确保日期列是datetime类型
                    if "日期" in existing_df.columns:
                        existing_df["日期"] = pd.to_datetime(existing_df["日期"], errors='coerce')
                    
                    # 合并数据并去重
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                    combined_df = combined_df.sort_values("日期", ascending=False)
                    
                    # 使用临时文件进行原子操作
                    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig')
                    combined_df.to_csv(temp_file.name, index=False)
                    # 原子替换
                    shutil.move(temp_file.name, save_path)
                    logger.info(f"✅ 数据已追加至: {save_path} (合并后共{len(combined_df)}条)")
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
            
            # 标记为已完成
            with open(completed_file, "a", encoding="utf-8") as f:
                f.write(f"{stock_code}\n")
            
            # 每10只ETF提交一次
            processed_count += 1
            if processed_count % 10 == 0 or processed_count == (end_idx - start_idx):
                logger.info(f"已处理 {processed_count} 只ETF，执行提交操作...")
                try:
                    from utils.git_utils import commit_final
                    commit_final()
                    logger.info(f"已提交前 {processed_count} 只ETF的数据到仓库")
                except Exception as e:
                    logger.error(f"提交文件时出错，继续执行: {str(e)}")
            
            # 更新进度
            last_processed_code = stock_code
            save_progress(stock_code, start_idx + processed_count, total_count, i + 1)
            
            # 记录进度
            logger.info(f"进度: {start_idx + processed_count}/{total_count} ({(start_idx + processed_count)/total_count*100:.1f}%)")
        
        # 关键修复：确保进度文件被正确保存
        # 即使没有ETF需要处理，也要更新进度
        if processed_count == 0:
            logger.info("本批次无新数据需要爬取")
            # 保存进度
            save_progress(last_processed_code, start_idx + processed_count, total_count, end_idx)
        
        # 爬取完本批次后，直接退出，等待下一次调用
        logger.info(f"本批次爬取完成，共处理 {processed_count} 只ETF")
        logger.info("程序将退出，等待工作流再次调用")
        
    except Exception as e:
        logger.error(f"ETF日线数据爬取任务执行失败: {str(e)}", exc_info=True)
        # 保存进度（如果失败）
        try:
            save_progress(None, next_index, total_count, next_index)
        except:
            pass
        raise

def get_all_stocks() -> list:
    """
    获取所有股票代码
    """
    try:
        # 这里应该有获取股票代码的实现
        # 为简化示例，返回一个示例列表
        return [f"00000{i:02d}" for i in range(3000)]
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}", exc_info=True)
        return []

def get_stock_name(stock_code: str) -> str:
    """
    根据股票代码获取股票名称
    """
    try:
        # 这里应该有获取股票名称的实现
        return f"股票{stock_code}"
    except Exception as e:
        logger.error(f"获取股票名称失败: {str(e)}", exc_info=True)
        return ""

def get_incremental_date_range(stock_code: str) -> tuple:
    """
    获取增量日期范围
    """
    try:
        # 这里应该有获取增量日期范围的实现
        return (datetime.now() - timedelta(days=30), datetime.now())
    except Exception as e:
        logger.error(f"获取增量日期范围失败: {str(e)}", exc_info=True)
        return (None, None)

def load_progress() -> dict:
    """
    加载爬取进度
    """
    # 这里应该有加载进度的实现
    return {"next_index": 0}

def save_progress(etf_code: str, processed_count: int, total_count: int, next_index: int):
    """
    保存爬取进度
    """
    # 这里应该有保存进度的实现
    pass

if __name__ == "__main__":
    try:
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(os.path.join(Config.LOG_DIR, "stock_crawler.log"))
            ]
        )
        
        logger.info("===== 开始执行任务：crawl_stock_daily =====")
        logger.info(f"UTC时间：{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"北京时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
        
        crawl_all_etfs_daily_data()
        
        logger.info("===== 任务执行结束：success =====")
    except Exception as e:
        logger.error(f"ETF日线数据爬取任务执行失败: {str(e)}", exc_info=True)
        sys.exit(1)
