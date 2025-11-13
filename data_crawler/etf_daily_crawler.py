#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块
使用指定接口爬取ETF日线数据
【yFinance数据DS-etf_daily_crawler-3.py】
【生产级实现】
- 严格遵循"各司其职"原则
- 与股票爬取系统完全一致的进度管理逻辑
- 专业金融系统可靠性保障
- 100%可直接复制使用
"""

import yfinance as yf
import pandas as pd
import logging
import os
import time
import random
import tempfile
import shutil
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time, get_last_trading_day, is_trading_day
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files, _verify_git_file_content

# 初始化日志
logger = logging.getLogger(__name__)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "etf", "daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_etfs.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# 【关键参数】可在此处修改每次处理的ETF数量
BATCH_SIZE = 40  # 可根据需要调整为60、100、150、200等
COMMIT_BATCH_SIZE = 10  # 每COMMIT_BATCH_SIZE个文件提交一次
BASE_DELAY = 0.8
MAX_RETRIES = 3
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

class RequestThrottler:
    """请求限流器 - 动态调整请求间隔"""
    def __init__(self, base_delay=0.8, max_delay=3.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.current_delay = base_delay
        self.success_count = 0
        self.failure_count = 0
        self.last_request_time = None
    
    def wait(self):
        """等待适当时间再请求"""
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.current_delay:
                time.sleep(self.current_delay - elapsed)
        
        self.last_request_time = time.time()
    
    def record_success(self):
        """记录成功请求"""
        self.success_count += 1
        self.failure_count = 0
        
        if self.success_count % 10 == 0 and self.current_delay > self.base_delay:
            self.current_delay = max(self.base_delay, self.current_delay - 0.1)
    
    def record_failure(self):
        """记录失败请求"""
        self.failure_count += 1
        self.success_count = 0
        
        if self.failure_count >= 3:
            self.current_delay = min(self.max_delay, self.current_delay + 0.5)
            self.failure_count = 0

throttler = RequestThrottler(base_delay=BASE_DELAY)

def get_etf_name(etf_code):
    """
    获取ETF名称
    """
    try:
        # 确保ETF列表文件存在
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"ETF列表文件不存在: {BASIC_INFO_FILE}")
            return etf_code
        
        # 读取时指定ETF代码列为字符串类型
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"ETF代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("ETF列表文件为空")
            return etf_code
        
        # 确保必要列存在
        if "ETF代码" not in basic_info_df.columns or "ETF名称" not in basic_info_df.columns:
            logger.error("ETF列表文件缺少必要列")
            return etf_code
        
        # 确保比较时数据类型一致（都转为字符串）
        etf_code_str = str(etf_code).strip()
        etf_row = basic_info_df[basic_info_df["ETF代码"] == etf_code_str]
        
        if not etf_row.empty:
            return etf_row["ETF名称"].values[0]
        
        logger.warning(f"ETF {etf_code_str} 不在列表中")
        return etf_code
    except Exception as e:
        logger.error(f"获取ETF名称失败: {str(e)}", exc_info=True)
        return etf_code

def get_etf_fund_size(etf_code: str) -> float:
    """
    从ETF列表中获取基金规模（只读）
    """
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"ETF列表文件不存在: {BASIC_INFO_FILE}")
            return 0.0
        
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        if "ETF代码" not in basic_info_df.columns or "基金规模" not in basic_info_df.columns:
            logger.warning(f"ETF列表缺少必要列（ETF代码/基金规模）")
            return 0.0
        
        etf_row = basic_info_df[basic_info_df["ETF代码"] == str(etf_code).strip()]
        if etf_row.empty:
            logger.warning(f"ETF {etf_code} 在列表中不存在")
            return 0.0
        
        fund_size = float(etf_row["基金规模"].values[0])
        return fund_size * 100000000  # 亿元转股
    
    except Exception as e:
        logger.error(f"获取ETF {etf_code} 基金规模失败: {str(e)}", exc_info=True)
        return 0.0

def get_next_crawl_index() -> int:
    """
    获取下一个要处理的ETF索引
    Returns:
        int: 下一个要处理的ETF索引
    """
    try:
        # 确保ETF列表文件存在
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"ETF列表文件不存在: {BASIC_INFO_FILE}")
            return 0
        
        # 使用正确的函数名（添加下划线）
        if not _verify_git_file_content(BASIC_INFO_FILE):
            logger.warning("ETF列表文件内容与Git仓库不一致，可能需要重新加载")
        
        # 读取时指定ETF代码列为字符串类型
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"ETF代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("ETF列表文件为空，无法获取进度")
            return 0
        
        # 确保"next_crawl_index"列存在
        if "next_crawl_index" not in basic_info_df.columns:
            # 添加列并初始化
            basic_info_df["next_crawl_index"] = 0
            # 保存更新后的文件
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
            if not _verify_git_file_content(BASIC_INFO_FILE):
                logger.warning("ETF列表文件内容与Git仓库不一致，可能需要重新提交")
            logger.info("已添加next_crawl_index列并初始化为0")
        
        # 获取第一个ETF的next_crawl_index值
        next_index = int(basic_info_df["next_crawl_index"].iloc[0])
        logger.info(f"当前进度：下一个索引位置: {next_index}/{len(basic_info_df)}")
        return next_index
    except Exception as e:
        logger.error(f"获取ETF进度索引失败: {str(e)}", exc_info=True)
        return 0

def save_crawl_progress(next_index: int):
    """
    保存ETF爬取进度
    Args:
        next_index: 下一个要处理的ETF索引
    """
    try:
        # 确保ETF列表文件存在
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"ETF列表文件不存在: {BASIC_INFO_FILE}")
            return
        
        # 读取时指定ETF代码列为字符串类型
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"ETF代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("ETF列表文件为空，无法更新进度")
            return
        
        # 确保"next_crawl_index"列存在
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
        
        # 更新所有行的next_crawl_index值
        basic_info_df["next_crawl_index"] = next_index
        # 保存更新后的文件
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        if not _verify_git_file_content(BASIC_INFO_FILE):
            logger.warning("文件内容验证失败，可能需要重试提交")
        
        logger.info(f"✅ 进度已保存：下一个索引位置: {next_index}/{len(basic_info_df)}")
        # 【关键修复】进度文件不单独提交，等待批量提交
    except Exception as e:
        logger.error(f"❌ 保存ETF进度失败: {str(e)}", exc_info=True)

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

def load_etf_daily_data(etf_code: str) -> pd.DataFrame:
    """
    加载ETF日线数据
    """
    try:
        # 构建文件路径
        file_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.warning(f"ETF {etf_code} 日线数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        # 标准列定义
        standard_columns = [
            '日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额',
            '振幅', '涨跌幅', '涨跌额', '换手率',
            'IOPV', '折价率', '溢价率',
            'ETF代码', 'ETF名称', '爬取时间'
        ]
        
        # 读取CSV文件，明确指定数据类型
        df = pd.read_csv(
            file_path,
            encoding="utf-8",
            dtype={
                "日期": str,
                "开盘": float,
                "最高": float,
                "最低": float,
                "收盘": float,
                "成交量": float,
                "成交额": float,
                "振幅": float,
                "涨跌幅": float,
                "涨跌额": float,
                "换手率": float,
                "IOPV": float,
                "折价率": float,
                "溢价率": float
            }
        )
        
        # 检查必需列
        required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"ETF {etf_code} 数据缺少必要列: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # 确保日期列为字符串格式
        df["日期"] = df["日期"].astype(str)
        # 按日期排序并去重
        df = df.sort_values("日期").drop_duplicates(subset=["日期"], keep="last")
        # 移除未来日期的数据
        today = datetime.now().strftime("%Y-%m-%d")
        df = df[df["日期"] <= today]
        
        # 只返回标准列中存在的列
        return df[[col for col in standard_columns if col in df.columns]]
    except Exception as e:
        logger.error(f"加载ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def apply_precision_control(df: pd.DataFrame) -> pd.DataFrame:
    """
    应用数据精度控制（保留4位小数）
    """
    # 需要保留4位小数的字段
    precision_fields = [
        '开盘', '最高', '最低', '收盘', '成交额',
        '振幅', '涨跌幅', '涨跌额', '换手率',
        'IOPV', '折价率', '溢价率'
    ]
    
    for field in precision_fields:
        if field in df.columns:
            # 保留4位小数
            df[field] = df[field].round(4)
    
    # 成交量通常为整数
    if '成交量' in df.columns:
        df['成交量'] = df['成交量'].round(0).astype(int)
    
    return df

def process_yfinance_data(df: pd.DataFrame, etf_code: str) -> pd.DataFrame:
    """
    处理Yahoo Finance返回的DataFrame
    """
    # 1. 确保DataFrame是扁平结构
    if isinstance(df.columns, pd.MultiIndex):
        # 提取第一级列名
        columns = []
        for col in df.columns:
            if isinstance(col, tuple) and len(col) > 0:
                columns.append(col[0])
            else:
                columns.append(col)
        df.columns = columns
    
    # 2. 确保日期列存在
    if 'Date' in df.columns:
        df = df.reset_index(drop=True)
    elif df.index.name == 'Date':
        df = df.reset_index()
    elif 'date' in df.columns:
        df = df.rename(columns={'date': 'Date'})
    else:
        return pd.DataFrame()  # 无有效日期列，返回空DataFrame
    
    # 3. 检查必要列
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"ETF {etf_code} 缺少必要列: {col}")
            return pd.DataFrame()  # 关键修复：缺失必要列，直接返回空DataFrame
    
    # 4. 创建临时单列DataFrame
    result_df = pd.DataFrame()
    result_df['日期'] = df['Date'].dt.strftime('%Y-%m-%d')
    result_df['开盘'] = df['Open'].astype(float)
    result_df['最高'] = df['High'].astype(float)
    result_df['最低'] = df['Low'].astype(float)
    result_df['收盘'] = df['Close'].astype(float)
    result_df['成交量'] = df['Volume'].astype(float)
    
    # 5. 计算衍生字段
    # 振幅 = (最高 - 最低) / 最低 * 100%
    result_df['振幅'] = ((result_df['最高'] - result_df['最低']) / result_df['最低'] * 100).round(2)
    
    # 涨跌额 = 收盘 - 前一日收盘
    result_df['涨跌额'] = result_df['收盘'].diff().fillna(0)
    
    # 涨跌幅 = 涨跌额 / 前一日收盘 * 100%
    prev_close = result_df['收盘'].shift(1)
    # 避免除以0
    valid_prev_close = prev_close.replace(0, float('nan'))
    result_df['涨跌幅'] = (result_df['涨跌额'] / valid_prev_close * 100).round(2)
    result_df['涨跌幅'] = result_df['涨跌幅'].fillna(0)
    
    # 换手率 = 成交量 / 基金规模
    fund_size = get_etf_fund_size(etf_code)
    if fund_size > 0:
        result_df['换手率'] = (result_df['成交量'] / fund_size * 100).round(2)
    else:
        result_df['换手率'] = 0.0
    
    # 6. IOPV/折价率/溢价率（Yahoo Finance不提供）
    result_df['IOPV'] = 0.0
    result_df['折价率'] = 0.0
    result_df['溢价率'] = 0.0
    
    # 7. 成交额 = 收盘 * 成交量
    result_df['成交额'] = (result_df['收盘'] * result_df['成交量']).round(2)
    
    # 关键修复：应用数据精度控制
    result_df = apply_precision_control(result_df)
    
    return result_df

def crawl_etf_daily_data(etf_code: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    使用Yahoo Finance爬取ETF日线数据
    """
    try:
        # 确保日期参数是datetime类型
        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            logger.error(f"ETF {etf_code} 日期参数类型错误，应为datetime类型")
            return pd.DataFrame()
        
        # 确保日期对象有正确的时区信息
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 转换为Yahoo Finance所需的格式
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        # 1. 执行请求（带重试机制）
        max_retries = MAX_RETRIES
        for retry in range(max_retries):
            try:
                throttler.wait()
                
                # Yahoo Finance API
                symbol = etf_code
                if etf_code.startswith(('51', '56', '57', '58')):
                    symbol = f"{etf_code}.SS"
                elif etf_code.startswith('15'):
                    symbol = f"{etf_code}.SZ"
                
                # 获取数据
                df = yf.download(
                    symbol,
                    start=start_str,
                    end=end_str,
                    progress=False,
                    auto_adjust=True,
                    timeout=15
                )
                
                # 检查是否获取到数据
                if df is None or df.empty:
                    raise ValueError("No data returned")
                
                throttler.record_success()
                break
                
            except Exception as e:
                throttler.record_failure()
                if retry == max_retries - 1:
                    logger.error(f"ETF {etf_code} 接口请求失败 (重试 {max_retries} 次): {str(e)}")
                    return pd.DataFrame()
                
                wait_time = BASE_DELAY * (2 ** retry) + random.uniform(0.1, 0.5)
                logger.warning(f"ETF {etf_code} 请求失败，{wait_time:.1f}秒后重试: {str(e)}")
                time.sleep(wait_time)
        
        # 2. 处理数据
        df = process_yfinance_data(df, etf_code)
        
        # 3. 严格数据验证
        required_columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量']
        if any(col not in df.columns for col in required_columns) or df.empty:
            logger.error(f"ETF {etf_code} 数据验证失败 - 无法保存")
            return pd.DataFrame()
        
        # 4. 补充必要字段
        df['ETF代码'] = etf_code
        df['ETF名称'] = get_etf_name(etf_code)
        df['爬取时间'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 5. 确保字段顺序
        standard_columns = [
            '日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额',
            '振幅', '涨跌幅', '涨跌额', '换手率',
            'IOPV', '折价率', '溢价率',
            'ETF代码', 'ETF名称', '爬取时间'
        ]
        
        return df[[col for col in standard_columns if col in df.columns]]
    
    except Exception as e:
        logger.error(f"ETF {etf_code} 数据爬取失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_incremental_date_range(etf_code: str) -> (datetime, datetime):
    """
    获取增量爬取的日期范围
    专业修复：解决ETF全部跳过问题
    """
    try:
        # 获取最近交易日
        last_trading_day = get_last_trading_day()
        if not isinstance(last_trading_day, datetime):
            last_trading_day = datetime.now()
        
        # 确保时区一致
        if last_trading_day.tzinfo is None:
            last_trading_day = last_trading_day.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 设置结束日期为最近交易日（确保是交易日）
        end_date = last_trading_day
        
        # 获取当前北京时间
        current_time = get_beijing_time()
        
        # 如果结束日期晚于当前时间，调整为当前时间
        if end_date > current_time:
            end_date = current_time
        
        # 专业修复：确保结束日期是交易日
        while not is_trading_day(end_date.date()):
            end_date -= timedelta(days=1)
            if (last_trading_day - end_date).days > 30:
                logger.error("无法找到有效的结束交易日")
                return None, None
        
        # 专业修复：设置结束时间为当天23:59:59
        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
        
        # 构建ETF数据文件路径
        save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
        
        # 检查历史数据文件是否存在
        if os.path.exists(save_path):
            try:
                df = pd.read_csv(save_path)
                
                # 确保日期列存在
                if "日期" not in df.columns:
                    logger.warning(f"ETF {etf_code} 数据文件缺少'日期'列")
                    # 使用默认回退策略：获取一年数据
                    start_date = last_trading_day - timedelta(days=365)
                    if start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                    return start_date, end_date
                
                # 确保日期列是datetime类型
                df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
                
                # 获取最新有效日期
                valid_dates = df["日期"].dropna()
                if valid_dates.empty:
                    logger.warning(f"ETF {etf_code} 数据文件中日期列全为NaN")
                    start_date = last_trading_day - timedelta(days=365)
                    if start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                    return start_date, end_date
                
                latest_date = valid_dates.max()
                
                # 确保latest_date是datetime类型并带有时区
                if not isinstance(latest_date, datetime):
                    latest_date = pd.to_datetime(latest_date)
                
                if latest_date.tzinfo is None:
                    latest_date = latest_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                
                # 专业修复：比较日期部分（忽略时间部分）
                latest_date_date = latest_date.date()
                end_date_date = end_date.date()
                
                logger.debug(f"ETF {etf_code} 日期比较: 最新日期={latest_date_date}, 结束日期={end_date_date}")
                
                # 专业修复：如果最新日期小于结束日期，则需要爬取
                if latest_date_date < end_date_date:
                    # 专业修复：从最新日期的下一个交易日开始
                    start_date = latest_date + timedelta(days=1)
                    
                    # 确保start_date是交易日
                    while not is_trading_day(start_date.date()):
                        start_date += timedelta(days=1)
                    
                    # 确保start_date有时区信息
                    if start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                    
                    # 专业修复：确保start_date不超过end_date
                    if start_date > end_date:
                        logger.info(f"ETF {etf_code} 数据已最新（最新日期={latest_date_date}，结束日期={end_date_date}）")
                        return None, None
                    
                    logger.info(f"ETF {etf_code} 需要更新数据: 最新日期 {latest_date_date} < 结束日期 {end_date_date}")
                    logger.info(f"ETF {etf_code} 增量爬取日期范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
                    return start_date, end_date
                else:
                    logger.info(f"ETF {etf_code} 数据已最新: 最新日期 {latest_date_date} >= 结束日期 {end_date_date}")
                    return None, None
            
            except Exception as e:
                logger.error(f"读取ETF {etf_code} 数据文件失败: {str(e)}", exc_info=True)
                # 出错时尝试获取一年数据
                start_date = last_trading_day - timedelta(days=365)
                if start_date.tzinfo is None:
                    start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                return start_date, end_date
        else:
            logger.info(f"ETF {etf_code} 无历史数据，将获取一年历史数据")
            start_date = last_trading_day - timedelta(days=365)
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
            return start_date, end_date
    
    except Exception as e:
        logger.error(f"获取增量日期范围失败: {str(e)}", exc_info=True)
        last_trading_day = get_last_trading_day()
        start_date = last_trading_day - timedelta(days=365)
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        end_date = last_trading_day.replace(hour=23, minute=59, second=59, microsecond=0)
        return start_date, end_date

def save_etf_daily_data(etf_code: str, df: pd.DataFrame) -> None:
    """
    保存ETF日线数据 - 【关键修复】只保存到本地，不进行任何Git提交操作
    """
    if df.empty:
        return
    
    # 确保目录存在
    os.makedirs(DAILY_DIR, exist_ok=True)
    
    # 保存到CSV - 日期列已经是字符串格式，不需要转换
    save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
    
    # 使用临时文件进行原子操作
    temp_file = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
            df.to_csv(temp_file.name, index=False)
        shutil.move(temp_file.name, save_path)
        logger.info(f"✅ 数据已保存至: {save_path} ({len(df)}条)")
        
        # 【关键修复】移除所有Git提交操作，只在批量处理时提交
        logger.debug(f"ETF {etf_code} 数据已保存到本地，等待批量提交")
        
    except Exception as e:
        logger.error(f"保存ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
        # 删除临时文件
        if temp_file and os.path.exists(temp_file.name):
            os.unlink(temp_file.name)

def crawl_all_etfs_daily_data() -> None:
    """
    爬取所有ETF日线数据
    """
    try:
        logger.info("=== 开始执行ETF日线数据爬取 ===")
        beijing_time = get_beijing_time()
        logger.info(f"北京时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}（UTC+8）")
        
        # 初始化目录
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(DAILY_DIR, exist_ok=True)
        logger.info(f"✅ 确保目录存在: {DATA_DIR}")
        
        # 获取所有ETF代码
        etf_codes = get_all_etf_codes()
        total_count = len(etf_codes)
        
        if total_count == 0:
            logger.error("ETF列表为空，无法进行爬取")
            return
        
        logger.info(f"待爬取ETF总数：{total_count}只（全市场ETF）")
        
        # 获取当前进度
        next_index = get_next_crawl_index()
        
        # 专业修复：循环批处理机制
        start_idx = next_index % total_count
        end_idx = start_idx + BATCH_SIZE
        actual_end_idx = end_idx % total_count
        
        # 处理循环情况
        if end_idx <= total_count:
            batch_codes = etf_codes[start_idx:end_idx]
            logger.info(f"处理本批次 ETF ({BATCH_SIZE}只)，从索引 {start_idx} 开始")
        else:
            batch_codes = etf_codes[start_idx:total_count] + etf_codes[0:end_idx-total_count]
            logger.info(f"处理本批次 ETF ({BATCH_SIZE}只)，从索引 {start_idx} 开始（循环处理）")

        # 记录第一批和最后一批ETF
        first_stock_idx = start_idx % total_count
        last_stock_idx = (end_idx - 1) % total_count
        first_stock = f"{etf_codes[first_stock_idx]} - {get_etf_name(etf_codes[first_stock_idx])}" if first_stock_idx < len(etf_codes) else "N/A"
        last_stock = f"{etf_codes[last_stock_idx]} - {get_etf_name(etf_codes[last_stock_idx])}" if last_stock_idx < len(etf_codes) else "N/A"
        logger.info(f"当前批次第一只ETF: {first_stock} (索引 {first_stock_idx})")
        logger.info(f"当前批次最后一只ETF: {last_stock} (索引 {last_stock_idx})")
        
        # 【关键修复】初始化计数器在循环外部
        processed_count = 0
        successful_count = 0
        skipped_count = 0
        failed_count = 0
        
        for i, etf_code in enumerate(batch_codes):
            # 添加随机延时，避免请求过于频繁
            time.sleep(random.uniform(5, 11))
            etf_name = get_etf_name(etf_code)
            logger.info(f"ETF代码：{etf_code}| 名称：{etf_name}")
            
            # 获取增量日期范围
            start_date, end_date = get_incremental_date_range(etf_code)
            if start_date is None or end_date is None:
                logger.info(f"ETF {etf_code} 数据已最新，跳过爬取")
                # 【关键修复】即使跳过也要计数
                processed_count += 1
                skipped_count += 1
                current_index = (start_idx + i) % total_count
                logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
                
                # 检查是否需要提交
                if processed_count % COMMIT_BATCH_SIZE == 0:
                    logger.info(f"【批量提交】已处理 {processed_count} 只ETF（成功 {successful_count}，跳过 {skipped_count}，失败 {failed_count}），提交批量文件到远程仓库...")
                    commit_result = force_commit_remaining_files()
                    if commit_result:
                        logger.info(f"✅ 批量提交成功！已提交 {processed_count} 个文件到远程仓库")
                    else:
                        logger.error("❌ 批量提交失败！")
                continue
            
            # 爬取数据
            logger.info(f"📅 增量爬取日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
            df = crawl_etf_daily_data(etf_code, start_date, end_date)
            
            # 检查是否成功获取数据
            if df.empty:
                logger.warning(f"⚠️ 未获取到数据")
                # 记录失败日志
                with open(os.path.join(DAILY_DIR, "failed_etfs.txt"), "a", encoding="utf-8") as f:
                    f.write(f"{etf_code},{etf_name},未获取到数据\n")
                
                # 【关键修复】即使失败也要计数
                processed_count += 1
                failed_count += 1
                current_index = (start_idx + i) % total_count
                logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
                
                # 检查是否需要提交
                if processed_count % COMMIT_BATCH_SIZE == 0:
                    logger.info(f"【批量提交】已处理 {processed_count} 只ETF（成功 {successful_count}，跳过 {skipped_count}，失败 {failed_count}），提交批量文件到远程仓库...")
                    commit_result = force_commit_remaining_files()
                    if commit_result:
                        logger.info(f"✅ 批量提交成功！已提交 {processed_count} 个文件到远程仓库")
                    else:
                        logger.error("❌ 批量提交失败！")
                continue
            
            # 处理已有数据
            save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
            if os.path.exists(save_path):
                try:
                    existing_df = pd.read_csv(save_path)
                    if "日期" in existing_df.columns:
                        existing_df["日期"] = pd.to_datetime(existing_df["日期"], errors='coerce')
                    
                    # 确保新数据的日期列也是datetime类型以便合并
                    df_temp = df.copy()
                    df_temp["日期"] = pd.to_datetime(df_temp["日期"], errors='coerce')
                    
                    combined_df = pd.concat([existing_df, df_temp], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                    combined_df = combined_df.sort_values("日期", ascending=False)
                    
                    # 转换回字符串格式保存
                    combined_df["日期"] = combined_df["日期"].dt.strftime('%Y-%m-%d')
                    
                    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                        combined_df.to_csv(temp_file.name, index=False)
                    shutil.move(temp_file.name, save_path)
                    logger.info(f"✅ 数据已追加至: {save_path} (合并后共{len(combined_df)}条)")
                except Exception as e:
                    logger.error(f"合并数据失败: {str(e)}")
                    continue
                finally:
                    if 'temp_file' in locals() and os.path.exists(temp_file.name):
                        os.unlink(temp_file.name)
            else:
                save_etf_daily_data(etf_code, df)
            
            # 【关键修复】成功处理计数
            processed_count += 1
            successful_count += 1
            current_index = (start_idx + i) % total_count
            logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
            
            # 【关键修复】每处理10只ETF就调用git_utils提交
            if processed_count % COMMIT_BATCH_SIZE == 0:
                logger.info(f"【批量提交】已处理 {processed_count} 只ETF（成功 {successful_count}，跳过 {skipped_count}，失败 {failed_count}），提交批量文件到远程仓库...")
                commit_result = force_commit_remaining_files()
                if commit_result:
                    logger.info(f"✅ 批量提交成功！已提交 {processed_count} 个文件到远程仓库")
                else:
                    logger.error("❌ 批量提交失败！")
        
        # 专业修复：整批处理完成后才更新进度
        new_index = actual_end_idx
        save_crawl_progress(new_index)
        logger.info(f"进度已更新为 {new_index}/{total_count}")
        
        # 检查是否还有未完成的ETF
        remaining_stocks = total_count - new_index
        if remaining_stocks < 0:
            remaining_stocks = total_count  # 重置后
        
        logger.info(f"本批次爬取完成，共处理 {processed_count} 只ETF（成功 {successful_count}，跳过 {skipped_count}，失败 {failed_count}），还有 {remaining_stocks} 只ETF待爬取")
        
        # 关键修复：确保所有剩余文件都被提交
        logger.info("【最终提交】处理完成后，确保提交所有剩余文件到远程仓库...")
        final_commit_result = force_commit_remaining_files()
        if final_commit_result:
            logger.info("✅ 最终提交成功！所有文件已成功提交到远程仓库")
        else:
            logger.error("❌ 最终提交失败！可能有文件未提交到远程仓库")
        
    except Exception as e:
        logger.error(f"ETF日线数据爬取任务执行失败: {str(e)}", exc_info=True)
        # 尝试保存进度以恢复状态
        try:
            if 'next_index' in locals() and 'total_count' in locals():
                logger.error("尝试保存进度以恢复状态...")
                save_crawl_progress(next_index)
                # 强制提交剩余文件
                emergency_commit_result = force_commit_remaining_files()
                if emergency_commit_result:
                    logger.info("✅ 紧急提交成功！已保存当前进度")
                else:
                    logger.error("❌ 紧急提交失败！")
        except Exception as save_error:
            logger.error(f"异常情况下保存进度失败: {str(save_error)}", exc_info=True)
        raise

def get_all_etf_codes() -> list:
    """
    获取所有ETF代码
    """
    try:
        # 确保ETF列表文件存在
        if not os.path.exists(BASIC_INFO_FILE):
            logger.info("ETF列表文件不存在，正在创建...")
            from data_crawler.all_etfs import update_all_etf_list
            update_all_etf_list()
        
        # 读取时指定ETF代码列为字符串类型
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"ETF代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("ETF列表文件为空")
            return []
        
        # 确保"ETF代码"列存在
        if "ETF代码" not in basic_info_df.columns:
            logger.error("ETF列表文件缺少'ETF代码'列")
            return []
        
        # 直接获取ETF代码（已确保是字符串）
        etf_codes = basic_info_df["ETF代码"].tolist()
        
        logger.info(f"获取到 {len(etf_codes)} 只ETF代码")
        return etf_codes
    except Exception as e:
        logger.error(f"获取ETF代码列表失败: {str(e)}", exc_info=True)
        return []

if __name__ == "__main__":
    try:
        # 首次运行时确保安装依赖
        try:
            import yfinance
        except ImportError:
            logger.error("缺少yfinance依赖，请先安装: pip install yfinance")
            raise SystemExit(1)
            
        crawl_all_etfs_daily_data()
    except Exception as e:
        logger.error(f"ETF日线数据爬取失败: {str(e)}", exc_info=True)
        # 发送错误通知
        try:
            from wechat_push.push import send_wechat_message
            send_wechat_message(
                message=f"ETF日线数据爬取失败: {str(e)}",
                message_type="error"
            )
        except:
            pass
        # 确保进度文件已保存
        try:
            next_index = get_next_crawl_index()
            total_count = len(get_all_etf_codes())
            logger.info(f"当前进度: {next_index}/{total_count}")
        except Exception as e:
            logger.error(f"读取进度文件失败: {str(e)}")
