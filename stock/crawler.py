#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据爬取模块 - 严格确保股票代码为6位格式，日期处理逻辑完善
【2025-10-14-0836：循环索引，保证每次都是爬取150只股票】
- 彻底解决Git提交问题
- 循环批处理机制（可配置批次大小）
- 专业金融系统可靠性保障
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
from utils.date_utils import is_trading_day, get_last_trading_day, get_beijing_time
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files

# 配置日志
logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_stocks.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# 【关键参数】可在此处修改每次处理的股票数量
# 专业修复：批次大小作为可配置参数
BATCH_SIZE = 100  # 可根据需要调整为100、150、200等
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
    将換日期转换为naive datetime（无时区）
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
    将換日期转换为aware datetime（有时区）
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
        
        # 【关键修复】传递提交消息，确保commit_files_in_batches能正确工作
        commit_message = f"自动更新股票 {stock_code} 日线数据"
        commit_files_in_batches(file_path, commit_message)
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
        commit_files_in_batches(BASIC_INFO_FILE, "更新股票基础信息")
        logger.info(f"已更新基础信息文件，确保所有股票代码为6位格式，共 {len(basic_info_df)} 条记录")
        
    except Exception as e:
        logger.error(f"读取基础信息文件失败: {str(e)}", exc_info=True)
        return False
    
    # 【关键修复】获取 next_crawl_index 值
    # 由于所有行的 next_crawl_index 值相同，取第一行即可
    next_index = int(basic_info_df["next_crawl_index"].iloc[0])
    total_stocks = len(basic_info_df)
    
    logger.info(f"当前爬取状态: next_crawl_index = {next_index} (共 {total_stocks} 只股票)")
    
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # 专业修复：循环批处理机制
    # 1. 确定处理范围（使用循环处理）
    start_idx = next_index % total_stocks
    end_idx = start_idx + BATCH_SIZE
    
    # 2. 计算实际的end_idx（用于进度更新）
    actual_end_idx = end_idx % total_stocks
    
    # 3. 记录第一批和最后一批股票（使用实际索引）
    first_stock_idx = start_idx % total_stocks
    last_stock_idx = (end_idx - 1) % total_stocks
    
    # 4. 处理循环情况
    if end_idx <= total_stocks:
        batch_df = basic_info_df.iloc[start_idx:end_idx]
        logger.info(f"处理本批次 股票 ({BATCH_SIZE}只)，从索引 {start_idx} 开始")
    else:
        # 循环处理：第一部分（start_idx到total_stocks）+ 第二部分（0到end_idx-total_stocks）
        batch_df = pd.concat([basic_info_df.iloc[start_idx:total_stocks], basic_info_df.iloc[0:end_idx-total_stocks]])
        logger.info(f"处理本批次 股票 ({BATCH_SIZE}只)，从索引 {start_idx} 开始（循环处理）")
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    # 记录第一批和最后一批股票
    first_stock = basic_info_df.iloc[first_stock_idx]
    last_stock = basic_info_df.iloc[last_stock_idx]
    logger.info(f"当前批次第一只股票: {first_stock['代码']} - {first_stock['名称']} (索引 {first_stock_idx})")
    logger.info(f"当前批次最后一只股票: {last_stock['代码']} - {last_stock['名称']} (索引 {last_stock_idx})")
    
    # 处理这批股票
    batch_codes = batch_df["代码"].tolist()
    
    if not batch_codes:
        logger.warning("没有可爬取的股票")
        return False
    
    # 【关键修复】跟踪已处理股票数量，确保每10个提交一次
    processed_count = 0
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
            processed_count += 1
            
            # 【关键修复】每处理10个股票就检查一次提交状态
            if processed_count % 10 == 0:
                logger.info(f"已处理 {processed_count} 只股票，执行提交操作...")
    
    # 【关键修复】处理完本批次后，确保提交任何剩余文件
    logger.info(f"处理完本批次后，检查并提交任何剩余文件...")
    if not force_commit_remaining_files():
        logger.error("强制提交剩余文件失败，可能导致数据丢失")
    
    # 【关键修复】更新 next_crawl_index
    new_index = actual_end_idx
    logger.info(f"更新 next_crawl_index = {new_index}")
    basic_info_df["next_crawl_index"] = new_index
    basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
    
    # 提交更新后的基础信息文件
    commit_files_in_batches(BASIC_INFO_FILE, "更新股票基础信息")
    logger.info(f"已提交更新后的基础信息文件到仓库: {BASIC_INFO_FILE}")
    
    # 检查是否还有未完成的股票
    remaining_stocks = (total_stocks - new_index) % total_stocks
    logger.info(f"已完成 {BATCH_SIZE} 只股票爬取，还有 {remaining_stocks} 只股票待爬取")
    
    return True

def create_or_update_basic_info():
    """创建或更新股票基础信息，增加总市值列"""
    try:
        # 获取股票基础信息
        logger.info("正在获取股票基础信息...")
        stock_info = ak.stock_info_a_code_name()
        
        if stock_info.empty:
            logger.error("获取股票基础信息失败：返回空数据")
            return False
        
        # 确保代码列是6位格式
        stock_info["代码"] = stock_info["代码"].apply(format_stock_code)
        # 移除无效股票
        stock_info = stock_info[stock_info["代码"].notna()]
        stock_info = stock_info[stock_info["代码"].str.len() == 6]
        stock_info = stock_info.reset_index(drop=True)
        
        # 【关键修复】获取实时行情数据以添加总市值
        logger.info("正在获取股票实时行情数据以获取总市值...")
        try:
            # 获取实时行情数据
            spot_em_df = ak.stock_zh_a_spot_em()
            
            if not spot_em_df.empty:
                # 确保行情数据中的代码格式一致
                spot_em_df["代码"] = spot_em_df["代码"].apply(format_stock_code)
                
                # 检查总市值列是否存在
                if "总市值" in spot_em_df.columns:
                    # 创建市值映射字典
                    market_cap_map = {}
                    for _, row in spot_em_df.iterrows():
                        stock_code = row["代码"]
                        market_cap = row["总市值"]
                        
                        # 处理可能的NaN值和无效值
                        if pd.isna(market_cap) or market_cap in ["--", "-", ""]:
                            market_cap_map[stock_code] = 0.0
                        else:
                            # 确保是数值类型
                            try:
                                # 尝试直接转换为浮点数
                                market_cap_value = float(market_cap)
                                
                                # 假设单位是亿元，转换为元
                                market_cap_map[stock_code] = market_cap_value * 100000000
                            except (TypeError, ValueError):
                                # 尝试处理带单位的字符串
                                try:
                                    if isinstance(market_cap, str):
                                        if "亿" in market_cap:
                                            value = float(market_cap.replace("亿", ""))
                                            market_cap_map[stock_code] = value * 100000000
                                        elif "万" in market_cap:
                                            value = float(market_cap.replace("万", ""))
                                            market_cap_map[stock_code] = value * 10000
                                        else:
                                            # 尝试直接转换为浮点数
                                            market_cap_map[stock_code] = float(market_cap)
                                except:
                                    market_cap_map[stock_code] = 0.0
                else:
                    logger.warning("实时行情数据中缺少'总市值'列，无法添加该列")
                    # 添加默认列
                    stock_info["总市值"] = 0.0
                    # 继续执行，稍后会添加总市值列
            else:
                logger.warning("获取股票实时行情数据失败：返回空数据，跳过总市值列添加")
                # 添加默认列
                stock_info["总市值"] = 0.0
        except Exception as e:
            logger.error(f"获取实时行情数据失败，跳过总市值列添加: {str(e)}")
            # 添加默认列
            stock_info["总市值"] = 0.0
        
        # 【关键修复】添加总市值列到基础信息（如果之前没有添加）
        if "总市值" not in stock_info.columns:
            # 创建市值映射字典
            market_cap_map = {}
            if "总市值" in locals():
                market_cap_map = market_cap_map
            else:
                market_cap_map = {row["代码"]: 0.0 for _, row in stock_info.iterrows()}
            
            # 添加总市值列
            stock_info["总市值"] = stock_info["代码"].map(
                lambda x: market_cap_map.get(x, 0.0)
            )
        
        # 添加 next_crawl_index 列
        stock_info["next_crawl_index"] = 0
        
        # 保存基础信息
        stock_info.to_csv(BASIC_INFO_FILE, index=False)
        commit_files_in_batches(BASIC_INFO_FILE, "创建股票基础信息")
        logger.info(f"股票基础信息已保存至: {BASIC_INFO_FILE}，共{len(stock_info)}条记录")
        
        return True
    except Exception as e:
        logger.error(f"获取股票基础信息失败: {str(e)}", exc_info=True)
        return False

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
    
    # 2. 只更新一批股票（最多BATCH_SIZE只）
    if update_all_stocks_daily_data():
        logger.info("已成功处理一批股票数据")
    else:
        logger.error("处理股票数据失败")
    
    logger.info("===== 股票数据更新完成 =====")

if __name__ == "__main__":
    main()
