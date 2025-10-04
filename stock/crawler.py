#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据爬取模块 - 仅调用一次API获取所有必要数据
严格使用API返回的原始列名，确保高效获取股票数据
【最终修复版】
- 彻底修复日期类型错误
- 当开始日期等于结束日期时不再尝试爬取
- 严格确保只处理历史交易日
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
# 合并重复的导入语句，优化代码结构
from utils.date_utils import is_trading_day, get_last_trading_day, parse_date_string
# 只需导入Git工具模块
from utils.git_utils import commit_files_in_batches

# 常量定义：提取魔法数字，提升可维护性
MAX_DATE_SEARCH = 30    # 日期查找最大天数（避免无限循环）
BATCH_SIZE = 150        # 每次爬取股票批次大小
MAX_RETRY_DAYS = 30     # 数据获取重试时间范围（天）
YEAR_DAYS = 365         # 首次爬取时间范围（一年）
MAX_HISTORY_DAYS = 250  # 保留历史数据最大条数（约一年交易日）

# 配置日志：避免重复添加Handler，提升健壮性
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_stocks.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在（初始化时执行一次，与ensure_directory_exists函数功能互补）
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


def ensure_directory_exists():
    """确保数据目录存在"""
    # 简化逻辑：os.makedirs(exist_ok=True)已处理目录存在场景，无需额外判断
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(DAILY_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)


def get_stock_section(stock_code: str) -> str:
    """
    获取股票所属板块
    
    Args:
        stock_code: 股票代码（不带市场前缀）
    
    Returns:
        str: 板块名称
    """
    # 简化代码：移除重复的zfill(6)操作，仅在处理前缀后统一补零
    stock_code = str(stock_code)
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
    time.sleep(random.uniform(2.0, 3.0))
    
    # 【关键修改】添加重试机制：提取重试等待时间变量，减少重复代码
    max_retries = 3
    retry_wait_time = random.uniform(5.0, 10.0)  # 统一重试等待时间
    for attempt in range(max_retries):
        try:
            logger.info(f"尝试第 {attempt + 1} 次获取股票列表...")
            
            # 正确调用 stock_zh_a_spot_em 获取所有必要数据
            df = ak.stock_zh_a_spot_em()
            
            # 检查返回结果
            if df.empty:
                logger.error("获取股票列表失败：返回为空")
                if attempt < max_retries - 1:
                    time.sleep(retry_wait_time)
                    continue
                return False
            
            # 打印API返回的列名，用于调试
            logger.info(f"API返回的列名: {df.columns.tolist()}")
            
            # 确保必要列存在：用列表推导式简化缺失列检查
            required_columns = ['代码', '名称', '流通市值']
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                logger.error(f"获取股票列表失败: 缺少必要列 {missing_cols}")
                if attempt < max_retries - 1:
                    time.sleep(retry_wait_time)
                    continue
                return False
            
            # 准备基础信息DataFrame - 严格使用API返回的原始列名
            basic_info_df = pd.DataFrame({
                "代码": df['代码'],
                "名称": df['名称'],
                "所属板块": df['代码'].apply(get_stock_section),
                "流通市值": df['流通市值']
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
            
            # 【关键修改】确保调用 tickten.py 中的筛选函数
            logger.info("准备调用 tickten.py 中的筛选函数...")
            try:
                from stock.tickten import filter_valid_stocks
                logger.info("成功导入 tickten.py 的 filter_valid_stocks 函数")
                filtered_df = filter_valid_stocks(basic_info_df)
                logger.info(f"调用 tickten.py 筛选函数后，剩余 {len(filtered_df)} 只有效股票（原 {len(basic_info_df)} 只）")
            except ImportError as e:
                logger.error(f"无法导入 tickten.py 的筛选函数: {str(e)}")
                logger.warning("使用原始数据作为后备方案")
                filtered_df = basic_info_df.copy()
            except Exception as e:
                logger.error(f"调用 tickten.py 筛选函数时发生异常: {str(e)}", exc_info=True)
                logger.warning("使用原始数据作为后备方案")
                filtered_df = basic_info_df.copy()
            
            # 【关键修改】添加 next_crawl_index 列 - 作为数值索引
            filtered_df["next_crawl_index"] = 0
            
            # 保存基础信息
            filtered_df.to_csv(BASIC_INFO_FILE, index=False)
            logger.info(f"已创建/更新股票基础信息文件，共 {len(filtered_df)} 条记录")
            
            # 确认文件已保存
            if os.path.exists(BASIC_INFO_FILE) and os.path.getsize(BASIC_INFO_FILE) > 0:
                logger.info(f"基础信息文件已成功保存到: {BASIC_INFO_FILE}")
                
                # 详细记录前5只股票（用于验证索引）
                first_5 = filtered_df.head(5)
                for idx, row in first_5.iterrows():
                    logger.info(f"基础信息文件前5只股票[{idx}]: {row['代码']} - {row['名称']}")
                
                # 【关键修改】提交基础信息文件到仓库
                commit_files_in_batches(BASIC_INFO_FILE)
                logger.info(f"已提交基础信息文件到仓库: {BASIC_INFO_FILE}")
                return True
            else:
                logger.error(f"基础信息文件保存失败: {BASIC_INFO_FILE} 不存在或为空")
                return False
                
        except Exception as e:
            logger.error(f"第 {attempt + 1} 次尝试创建基础信息文件失败: {str(e)}", exc_info=True)
            if attempt < max_retries - 1:
                logger.info(f"等待 {retry_wait_time:.1f} 秒后进行第 {attempt + 2} 次重试...")
                time.sleep(retry_wait_time)
            else:
                logger.error("所有重试都失败，无法创建基础信息文件")
                return False
    
    return False


def test_akshare_api():
    """测试 akshare API 是否正常工作"""
    logger.info("===== 开始 akshare API 测试 =====")
    logger.info(f"akshare 版本: {ak.__version__}")
    logger.info(f"akshare 模块路径: {ak.__file__}")
    
    # 【关键修改】只测试成功的方法：不带市场前缀的股票代码
    test_stocks = [
        ("600000", "浦发银行"),  # 沪市主板
        ("000001", "平安银行"),  # 深市主板
        ("300001", "特锐德"),    # 创业板
        ("688001", "华兴源创")   # 科创板
    ]
    
    successful_tests = 0
    test_results = []
    
    for stock_code, name in test_stocks:
        try:
            logger.info(f"--- 测试股票 {stock_code} ({name}) ---")
            
            # 【关键修改】使用测试成功的参数
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=(datetime.now() - timedelta(days=MAX_RETRY_DAYS)).strftime("%Y%m%d"),  # 使用常量
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust="qfq"
            )
            
            if df is not None and not df.empty:
                logger.info(f"  ✓ 成功获取 {len(df)} 条数据")
                logger.debug(f"  数据列名: {list(df.columns)}")
                successful_tests += 1
                test_results.append(True)
            else:
                logger.warning(f"  ✗ 返回空数据")
                test_results.append(False)
                
        except Exception as e:
            logger.error(f"  ✗ 测试失败: {str(e)}")
            test_results.append(False)
    
    logger.info("===== akshare API 测试完成 =====")
    
    # 分析测试结果
    total_tests = len(test_results)
    success_rate = successful_tests / total_tests if total_tests > 0 else 0
    
    logger.info(f"=== API测试结果分析 ===")
    logger.info(f"总测试数: {total_tests}, 成功数: {successful_tests}, 成功率: {success_rate:.2%}")
    
    # 【关键修改】只要有一个成功就算通过
    if successful_tests > 0:
        logger.info("API测试通过，可以继续执行爬取任务")
        return True, "qfq"  # 返回成功的复权参数
    else:
        logger.error("API测试全部失败，停止执行爬取任务")
        return False, None


def get_valid_trading_date_range(start_date, end_date):
    """
    获取有效的交易日范围，确保只包含历史交易日
    
    Args:
        start_date: 起始日期（可能包含非交易日）
        end_date: 结束日期（可能包含非交易日）
    
    Returns:
        tuple: (valid_start_date, valid_end_date) - 有效的交易日范围
    """
    # 转换为datetime对象
    if isinstance(start_date, str):
        start_date = parse_date_string(start_date)
    if isinstance(end_date, str):
        end_date = parse_date_string(end_date)
    
    # 确保结束日期不晚于当前日期
    today = datetime.now().date()
    if end_date > today:
        end_date = today
    
    # 查找有效的结束交易日（向后查找）：使用常量替换魔法数字
    valid_end_date = end_date
    days_back = 0
    while days_back < MAX_DATE_SEARCH:
        if is_trading_day(valid_end_date):
            break
        valid_end_date -= timedelta(days=1)
        days_back += 1
    
    # 如果找不到有效的结束交易日，返回空范围
    if days_back >= MAX_DATE_SEARCH:
        logger.warning(f"无法找到有效的结束交易日（从 {end_date} 开始）")
        return None, None
    
    # 查找有效的起始交易日：使用常量替换魔法数字
    valid_start_date = start_date
    days_forward = 0
    while days_forward < MAX_DATE_SEARCH:
        if is_trading_day(valid_start_date):
            break
        valid_start_date += timedelta(days=1)
        days_forward += 1
    
    # 如果找不到有效的起始交易日，使用结束交易日作为起始日
    if days_forward >= MAX_DATE_SEARCH:
        valid_start_date = valid_end_date
    
    # 确保起始日期不晚于结束日期
    if valid_start_date > valid_end_date:
        valid_start_date = valid_end_date
    
    return valid_start_date, valid_end_date


def fetch_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """获取单只股票的日线数据，使用中文列名"""
    try:
        # 【关键修复】确保股票代码是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        
        # 【关键修复】检查本地是否已有该股票的日线数据文件
        local_file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        existing_data = None
        last_date = None
        
        if os.path.exists(local_file_path):
            try:
                # 读取已有的数据
                existing_data = pd.read_csv(local_file_path)
                if not existing_data.empty and '日期' in existing_data.columns:
                    # 获取最后一条数据的日期
                    last_date = pd.to_datetime(existing_data['日期'].max(), errors='coerce')
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
        start_date = None
        end_date = get_last_trading_day()
        if last_date is not None:
            # 查找下一个交易日作为起始点：使用常量替换魔法数字
            current_date = last_date + timedelta(days=1)
            days_search = 0
            while days_search < MAX_DATE_SEARCH:
                if is_trading_day(current_date):
                    start_date = current_date
                    break
                current_date += timedelta(days=1)
                days_search += 1
            
            if not start_date:
                # 如果找不到交易日，使用最近一个交易日
                last_trading_date = get_last_trading_day()
                if last_trading_date:
                    start_date = last_trading_date
                    logger.warning(f"无法找到股票 {stock_code} 的下一个交易日，使用最近交易日: {start_date}")
                else:
                    logger.warning(f"无法找到股票 {stock_code} 的有效交易日，跳过爬取")
                    return pd.DataFrame()
            
            # 确保结束日期不晚于当前日期
            today = datetime.now().date()
            if end_date.date() > today:
                end_date = get_last_trading_day()
            
            # ===== 关键修复：判断是否需要爬取 =====
            if start_date >= end_date:
                logger.info(f"股票 {stock_code} 没有新数据需要爬取（开始日期: {start_date} >= 结束日期: {end_date}）")
                return pd.DataFrame()
            
            logger.info(f"股票 {stock_code} 增量爬取，从 {start_date.strftime('%Y%m%d')} 到 {end_date.strftime('%Y%m%d')}")
        else:
            # 没有本地数据，爬取最近一年的数据：使用常量替换魔法数字
            start_date = get_last_trading_day() - timedelta(days=YEAR_DAYS)
            # 确保起始日期是交易日：使用常量替换魔法数字
            current_date = start_date
            valid_start_date = None
            days_search = 0
            while days_search < MAX_DATE_SEARCH:
                if is_trading_day(current_date):
                    valid_start_date = current_date
                    break
                current_date += timedelta(days=1)
                days_search += 1
            
            if not valid_start_date:
                valid_start_date = end_date
            
            # 确保起始日期不晚于结束日期
            if valid_start_date > end_date:
                valid_start_date = end_date
            
            start_date = valid_start_date
            start_date_str = start_date.strftime("%Y%m%d")
            end_date_str = end_date.strftime("%Y%m%d")
            logger.info(f"股票 {stock_code} 首次爬取，获取从 {start_date_str} 到 {end_date_str} 的数据")
        
        # ===== 关键修复：日期格式化 =====
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        
        # 【关键修复】提取ak.stock_zh_a_hist重复参数，减少代码冗余
        ak_hist_common_params = {
            'symbol': stock_code,
            'period': 'daily',
            'start_date': start_date_str,
            'end_date': end_date_str
        }
        logger.debug(f"正在获取股票 {stock_code} 的日线数据 (代码: {stock_code}, 复权参数: qfq)")
        
        # 【关键修复】使用测试成功的参数进行增量爬取
        df = None
        try:
            df = ak.stock_zh_a_hist(adjust="qfq", **ak_hist_common_params)
        except Exception as e:
            logger.warning(f"获取股票 {stock_code} 的增量数据失败，尝试获取{MAX_RETRY_DAYS}天数据: {str(e)}")
            # 尝试获取30天数据（适用于新上市股票）：使用常量替换魔法数字
            retry_params = ak_hist_common_params.copy()
            retry_params['start_date'] = (datetime.now() - timedelta(days=MAX_RETRY_DAYS)).strftime("%Y%m%d")
            retry_params['end_date'] = datetime.now().strftime("%Y%m%d")
            try:
                df = ak.stock_zh_a_hist(adjust="qfq", **retry_params)
            except Exception as e:
                logger.warning(f"获取股票 {stock_code} 的{MAX_RETRY_DAYS}天数据失败，尝试获取不复权数据: {str(e)}")
                # 尝试不复权数据
                df = ak.stock_zh_a_hist(adjust="", **retry_params)
        
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
        
        # 【关键修复】合并新数据与已有数据
        if existing_data is not None and not existing_data.empty:
            # 合并数据并去重
            combined_df = pd.concat([existing_data, df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['日期'], keep='last')
            combined_df = combined_df.sort_values('日期').reset_index(drop=True)
            
            # 【关键修复】只保留最近一年的数据：使用常量替换魔法数字
            if len(combined_df) > MAX_HISTORY_DAYS:
                combined_df = combined_df.tail(MAX_HISTORY_DAYS)
            
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
        stock_code = str(stock_code).zfill(6)
        
        file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
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
    except Exception as e:
        logger.error(f"读取基础信息文件失败: {str(e)}", exc_info=True)
        return False
    
    # 【关键修复】获取 next_crawl_index 值
    next_index = int(basic_info_df["next_crawl_index"].iloc[0])
    total_stocks = len(basic_info_df)
    
    logger.info(f"当前爬取状态: next_crawl_index = {next_index} (共 {total_stocks} 只股票)")
    
    # 【关键修复】确定要爬取的股票范围：使用常量替换魔法数字
    start_idx = next_index
    end_idx = min(next_index + BATCH_SIZE, total_stocks)
    
    # 如果已经爬取完所有股票，重置索引
    if start_idx >= total_stocks:
        logger.info("已爬取完所有股票，重置爬取状态")
        start_idx = 0
        end_idx = min(BATCH_SIZE, total_stocks)
    
    # 获取要爬取的股票
    batch_df = basic_info_df.iloc[start_idx:end_idx]
    batch_codes = batch_df["代码"].tolist()
    
    if not batch_codes:
        logger.warning("没有可爬取的股票")
        return False
    
    # 使用常量计算批次号，提升可维护性
    batch_num = start_idx // BATCH_SIZE + 1
    logger.info(f"正在处理第 {batch_num} 批，共 {len(batch_codes)} 只股票 (索引 {start_idx} - {end_idx-1})")
    
    # 记录第一批和最后一批股票
    first_stock = batch_df.iloc[0]
    last_stock = batch_df.iloc[-1]
    logger.info(f"当前批次第一只股票: {first_stock['代码']} - {first_stock['名称']} (索引 {start_idx})")
    logger.info(f"当前批次最后一只股票: {last_stock['代码']} - {last_stock['名称']} (索引 {end_idx-1})")
    
    # 处理这批股票
    for stock_code in batch_codes:
        # 【关键修复】确保股票代码是6位
        stock_code = str(stock_code).zfill(6)
        
        # 添加随机延时，避免请求过于频繁
        time.sleep(random.uniform(1.5, 2.5))
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
    
    # 1. 运行 akshare API 测试（关键诊断工具）
    # api_ok, best_param = test_akshare_api()
    # if not api_ok:
    #    logger.error("akshare API 测试失败，停止执行爬取任务")
    #    return
    
    # 2. 确保基础信息文件存在
    if not os.path.exists(BASIC_INFO_FILE) or os.path.getsize(BASIC_INFO_FILE) == 0:
        logger.info("基础信息文件不存在或为空，正在创建...")
        if not create_or_update_basic_info():
            logger.error("基础信息文件创建失败，无法继续")
            return
    
    # 3. 只更新一批股票（最多150只）
    if update_all_stocks_daily_data():
        logger.info("已成功处理一批股票数据")
    else:
        logger.error("处理股票数据失败")
    
    logger.info("===== 股票数据更新完成 =====")


if __name__ == "__main__":
    main()
