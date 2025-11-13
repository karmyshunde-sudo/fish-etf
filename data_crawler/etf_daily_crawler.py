#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块 - 严格匹配股票爬取机制
【关键修复】
- 严格匹配股票爬取代码的提交机制
- 每处理一个ETF调用 commit_files_in_batches() 但不立即提交
- 每10个ETF调用 force_commit_remaining_files() 确保提交
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
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files

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
# 【关键参数】与股票爬取代码完全一致
BATCH_SIZE = 30  # 一个批次处理的ETF数量
COMMIT_BATCH_SIZE = 10  # 每COMMIT_BATCH_SIZE个文件提交一次
BASE_DELAY = 2.5  # 基础延迟增加到2.5秒
MAX_RETRIES = 1  # 增加重试次数
CONNECTION_TIMEOUT = 30  # 增加超时时间
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

def get_etf_name(etf_code):
    """获取ETF名称（只读）"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"ETF列表文件不存在: {BASIC_INFO_FILE}")
            return etf_code
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        if basic_info_df.empty or "ETF代码" not in basic_info_df.columns or "ETF名称" not in basic_info_df.columns:
            return etf_code
        etf_row = basic_info_df[basic_info_df["ETF代码"] == str(etf_code).strip()]
        return etf_row["ETF名称"].values[0] if not etf_row.empty else etf_code
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
    """获取进度索引（只读）"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            return 0
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        return int(basic_info_df["next_crawl_index"].iloc[0])
    except Exception as e:
        logger.error(f"获取ETF进度索引失败: {str(e)}", exc_info=True)
        return 0

def save_crawl_progress(next_index: int):
    """保存进度（仅更新进度字段）"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            return
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
        basic_info_df["next_crawl_index"] = next_index
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        
        # 通过 commit_files_in_batches 提交
        commit_message = f"feat: 更新ETF爬取进度 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        commit_files_in_batches(BASIC_INFO_FILE, commit_message)
    except Exception as e:
        logger.error(f"保存ETF进度失败: {str(e)}", exc_info=True)

def to_naive_datetime(dt):
    """转换为无时区时间"""
    if dt is None: return None
    return dt.replace(tzinfo=None) if dt.tzinfo else dt

def to_aware_datetime(dt):
    """转换为北京时区时间"""
    if dt is None: return None
    return dt.replace(tzinfo=Config.BEIJING_TIMEZONE) if dt.tzinfo is None else dt

def to_datetime(date_input):
    """统一日期转换"""
    if isinstance(date_input, datetime): return date_input
    if isinstance(date_input, str):
        for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"]:
            try: return datetime.strptime(date_input, fmt)
            except: continue
    return None

def get_valid_trading_date_range(start_date, end_date):
    """获取有效交易日范围"""
    start_date = to_datetime(start_date)
    end_date = to_datetime(end_date)
    if not start_date or not end_date: return None, None
    
    end_date = to_aware_datetime(end_date)
    now = to_aware_datetime(get_beijing_time())
    if end_date > now: end_date = now
    
    # 查找有效结束交易日
    valid_end_date = end_date
    for _ in range(30):
        if is_trading_day(valid_end_date.date()): break
        valid_end_date -= timedelta(days=1)
    else:
        return None, None
    
    # 查找有效起始交易日
    valid_start_date = start_date
    for _ in range(30):
        if is_trading_day(valid_start_date.date()): break
        valid_start_date += timedelta(days=1)
    else:
        valid_start_date = valid_end_date
    
    if to_naive_datetime(valid_start_date) > to_naive_datetime(valid_end_date):
        valid_start_date = valid_end_date
    
    return valid_start_date, valid_end_date

def load_etf_daily_data(etf_code: str) -> pd.DataFrame:
    """加载本地数据"""
    try:
        file_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
        if not os.path.exists(file_path):
            return pd.DataFrame()
        
        standard_columns = [
            '日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额',
            '振幅', '涨跌幅', '涨跌额', '换手率',
            'IOPV', '折价率', '溢价率',
            'ETF代码', 'ETF名称', '爬取时间'
        ]
        
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
        
        required_columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量']
        if any(col not in df.columns for col in required_columns):
            return pd.DataFrame()
        
        df = df[[col for col in standard_columns if col in df.columns]]
        df["日期"] = df["日期"].astype(str)
        df = df.sort_values("日期").drop_duplicates(subset=["日期"], keep="last")
        today = datetime.now().strftime("%Y-%m-%d")
        return df[df["日期"] <= today]
    except Exception as e:
        logger.error(f"加载ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# 【关键修复】严格匹配股票爬取代码的提交机制
# 1. 每处理一个ETF调用 commit_files_in_batches() 
# 2. 每10个ETF调用 force_commit_remaining_files() 确保提交
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
class RequestThrottler:
    """请求限流器 - 修复连接问题"""
    def __init__(self, base_delay=2.5, max_delay=15.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.current_delay = base_delay
        self.last_request_time = None
        self.failure_count = 0
    
    def wait(self):
        """等待适当时间再请求"""
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.current_delay:
                time.sleep(self.current_delay - elapsed)
        
        self.last_request_time = time.time()
    
    def record_failure(self):
        """记录失败请求 - 实现指数退避策略"""
        self.failure_count += 1
        # 指数退避：2^failure_count * base_delay
        self.current_delay = min(
            self.max_delay, 
            self.base_delay * (2 ** self.failure_count) + random.uniform(0.5, 1.5)
        )
        logger.warning(f"请求失败，指数退避延时: {self.current_delay:.2f}秒")
    
    def record_success(self):
        """记录成功请求 - 重置延时"""
        self.failure_count = 0
        self.current_delay = self.base_delay

throttler = RequestThrottler(base_delay=BASE_DELAY)

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
        # 确保日期格式正确
        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            logger.error(f"ETF {etf_code} 日期参数类型错误")
            return pd.DataFrame()
        
        # 统一时区处理
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 转换为Yahoo Finance所需的格式
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        # 1. 执行请求（带增强重试机制）
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
                    timeout=CONNECTION_TIMEOUT
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
                
                wait_time = BASE_DELAY * (2 ** retry) + random.uniform(1.5, 3.0)
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
    """增量日期范围（保持原有逻辑）"""
    try:
        last_trading_day = get_last_trading_day()
        if not isinstance(last_trading_day, datetime):
            last_trading_day = datetime.now()
        
        if last_trading_day.tzinfo is None:
            last_trading_day = last_trading_day.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        end_date = last_trading_day
        current_time = get_beijing_time()
        if end_date > current_time:
            end_date = current_time
        
        while not is_trading_day(end_date.date()):
            end_date -= timedelta(days=1)
        
        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
        
        save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
        if os.path.exists(save_path):
            try:
                df = pd.read_csv(save_path)
                if "日期" not in df.columns:
                    return last_trading_day - timedelta(days=365), end_date
                
                df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
                valid_dates = df["日期"].dropna()
                if valid_dates.empty:
                    return last_trading_day - timedelta(days=365), end_date
                
                latest_date = valid_dates.max()
                if latest_date.tzinfo is None:
                    latest_date = latest_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                
                latest_date_date = latest_date.date()
                end_date_date = end_date.date()
                
                if latest_date_date < end_date_date:
                    start_date = latest_date + timedelta(days=1)
                    while not is_trading_day(start_date.date()):
                        start_date += timedelta(days=1)
                    if start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                    if start_date > end_date:
                        return None, None
                    return start_date, end_date
                return None, None
            except Exception as e:
                logger.error(f"读取ETF {etf_code} 数据文件失败: {str(e)}", exc_info=True)
                return last_trading_day - timedelta(days=365), end_date
        else:
            return last_trading_day - timedelta(days=365), end_date
    except Exception as e:
        logger.error(f"获取增量日期范围失败: {str(e)}", exc_info=True)
        last_trading_day = get_last_trading_day()
        return last_trading_day - timedelta(days=365), last_trading_day

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# 【关键修复】严格匹配股票爬取代码的提交机制
# 1. 每处理一个ETF调用 commit_files_in_batches() 但不立即提交
# 2. 每10个ETF调用 force_commit_remaining_files() 确保提交
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
def save_etf_daily_data(etf_code: str, df: pd.DataFrame) -> None:
    """保存数据（关键修复：与股票爬取代码相同）"""
    if df.empty: 
        logger.error(f"ETF {etf_code} 数据为空，无法保存")
        return
    
    os.makedirs(DAILY_DIR, exist_ok=True)
    save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
    
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
            # 保存数据
            df.to_csv(temp_file.name, index=False)
        
        # 移动文件
        shutil.move(temp_file.name, save_path)
        logger.info(f"ETF {etf_code} 日线数据已保存至 {save_path}，共{len(df)}条数据")
        
        # 关键修复：调用 commit_files_in_batches，但不检查返回值
        commit_message = f"自动更新ETF {etf_code} 日线数据"
        commit_files_in_batches(save_path, commit_message)
        logger.debug(f"已添加ETF {etf_code} 日线数据到提交队列")
        
    except Exception as e:
        logger.error(f"保存ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
        # 删除临时文件
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)

def crawl_all_etfs_daily_data() -> None:
    """主爬取逻辑"""
    try:
        logger.info("=== 开始执行ETF日线数据爬取 ===")
        beijing_time = get_beijing_time()
        logger.info(f"北京时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}（UTC+8）")
        
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(DAILY_DIR, exist_ok=True)
        logger.info(f"✅ 确保目录存在: {DATA_DIR}")
        
        etf_codes = get_all_etf_codes()
        total_count = len(etf_codes)
        
        if total_count == 0:
            logger.error("ETF列表为空，无法进行爬取")
            return
        
        logger.info(f"待爬取ETF总数：{total_count}只（全市场ETF）")
        next_index = get_next_crawl_index()
        
        start_idx = next_index % total_count
        end_idx = start_idx + BATCH_SIZE
        actual_end_idx = end_idx % total_count
        
        if end_idx <= total_count:
            batch_codes = etf_codes[start_idx:end_idx]
            logger.info(f"处理本批次 ETF ({BATCH_SIZE}只)，从索引 {start_idx} 开始")
        else:
            batch_codes = etf_codes[start_idx:total_count] + etf_codes[0:end_idx-total_count]
            logger.info(f"处理本批次 ETF ({BATCH_SIZE}只)，从索引 {start_idx} 开始（循环处理）")
        
        first_stock_idx = start_idx % total_count
        last_stock_idx = (end_idx - 1) % total_count
        first_stock = f"{etf_codes[first_stock_idx]} - {get_etf_name(etf_codes[first_stock_idx])}" if first_stock_idx < len(etf_codes) else "N/A"
        last_stock = f"{etf_codes[last_stock_idx]} - {get_etf_name(etf_codes[last_stock_idx])}" if last_stock_idx < len(etf_codes) else "N/A"
        logger.info(f"当前批次第一只ETF: {first_stock} (索引 {first_stock_idx})")
        logger.info(f"当前批次最后一只ETF: {last_stock} (索引 {last_stock_idx})")
        
        # 关键修复：使用本地计数器，不再依赖全局变量
        processed_count = 0
        for i, etf_code in enumerate(batch_codes):
            etf_name = get_etf_name(etf_code)
            logger.info(f"ETF代码：{etf_code}| 名称：{etf_name}")
            
            start_date, end_date = get_incremental_date_range(etf_code)
            if start_date is None or end_date is None:
                logger.info(f"ETF {etf_code} 数据已最新，跳过爬取")
                continue
            
            logger.info(f"📅 增量爬取日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
            df = crawl_etf_daily_data(etf_code, start_date, end_date)
            
            if df.empty:
                logger.error(f"❌ ETF {etf_code} 数据获取失败 - 无法保存")
                with open(os.path.join(DAILY_DIR, "failed_etfs.txt"), "a", encoding="utf-8") as f:
                    f.write(f"{etf_code},{etf_name},数据验证失败\n")
                continue
            
            # 保存数据（但不提交）
            save_etf_daily_data(etf_code, df)
            
            processed_count += 1
            current_index = (start_idx + i) % total_count
            logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
            
            # 关键修复：每处理10个ETF，手动触发提交
            if processed_count % COMMIT_BATCH_SIZE == 0:
                logger.info(f"已处理 {processed_count} 只ETF，执行批量提交...")
                if not force_commit_remaining_files():
                    logger.error("提交批量文件失败")
        
        # 关键修复：处理完本批次后，确保提交所有剩余文件
        logger.info("处理完成后，确保提交所有剩余文件...")
        if not force_commit_remaining_files():
            logger.error("强制提交剩余文件失败，可能导致数据丢失")
        
        # 更新进度
        new_index = actual_end_idx
        save_crawl_progress(new_index)
        logger.info(f"进度已更新为 {new_index}/{total_count}")
        
        remaining_stocks = total_count - new_index
        if remaining_stocks < 0:
            remaining_stocks = total_count
        logger.info(f"本批次爬取完成，共处理 {processed_count} 只ETF，还有 {remaining_stocks} 只ETF待爬取")
        
    except Exception as e:
        logger.error(f"ETF日线数据爬取任务执行失败: {str(e)}", exc_info=True)
        try:
            if 'next_index' in locals() and 'total_count' in locals():
                logger.error("尝试保存进度以恢复状态...")
                save_crawl_progress(next_index)
                if not force_commit_remaining_files():
                    logger.error("強制提交剩余文件失败")
        except Exception as save_error:
            logger.error(f"异常情况下保存进度失败: {str(save_error)}", exc_info=True)
        raise

def get_all_etf_codes() -> list:
    """获取ETF代码列表（只读）"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.error("ETF列表文件不存在，无法进行爬取")
            return []
        
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        
        if basic_info_df.empty or "ETF代码" not in basic_info_df.columns:
            logger.error("ETF列表文件格式错误")
            return []
        
        return basic_info_df["ETF代码"].tolist()
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
        try:
            from wechat_push.push import send_wechat_message
            send_wechat_message(
                message=f"ETF日线数据爬取失败: {str(e)}",
                message_type="error"
            )
        except:
            pass
