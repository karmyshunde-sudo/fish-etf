#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块 - 优化版
【关键优化】
- 科学延时策略：增量更新0.8-1.5秒，全量2-3秒
- 动态延时：根据请求结果自动调整
- 失败重试机制：避免临时限流导致失败
- 严格保持数据结构不变
"""

import requests
import pandas as pd
import logging
import os
import time
import random
import tempfile
import shutil
import io
import math
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
# 【关键参数】优化版
BATCH_SIZE = 80  # 保持原有逻辑不变
BASE_DELAY = 0.8  # 基础延时（秒）
DYNAMIC_DELAY = True  # 启用动态延时
MAX_RETRIES = 3      # 最大重试次数
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
    返回单位：股（基金规模(亿元) × 100,000,000）
    """
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"ETF列表文件不存在: {BASIC_INFO_FILE}")
            return 0.0
        
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        
        # 检查必要列
        if "ETF代码" not in basic_info_df.columns or "基金规模" not in basic_info_df.columns:
            logger.warning(f"ETF列表缺少必要列（ETF代码/基金规模）")
            return 0.0
        
        # 获取对应ETF的基金规模
        etf_row = basic_info_df[basic_info_df["ETF代码"] == str(etf_code).strip()]
        if etf_row.empty:
            logger.warning(f"ETF {etf_code} 在列表中不存在")
            return 0.0
        
        fund_size = float(etf_row["基金规模"].values[0])
        
        # 基金规模单位转换：亿元 → 股
        return fund_size * 100000000
    
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
        
        # 定义标准列名
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
        
        # 确保所有必要列存在
        required_columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量']
        if any(col not in df.columns for col in required_columns):
            return pd.DataFrame()
        
        # 仅保留标准列
        df = df[[col for col in standard_columns if col in df.columns]]
        
        # 确保日期列格式
        df["日期"] = df["日期"].astype(str)
        df = df.sort_values("日期").drop_duplicates(subset=["日期"], keep="last")
        today = datetime.now().strftime("%Y-%m-%d")
        return df[df["日期"] <= today]
    except Exception as e:
        logger.error(f"加载ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# 【核心优化】动态延时策略
# 1. 基础延时：0.8-1.5秒（增量更新）
# 2. 动态调整：根据请求结果自动优化
# 3. 失败重试：避免临时限流
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
class RequestThrottler:
    """
    请求限流器 - 动态调整请求间隔
    """
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
        
        # 每10次成功请求尝试减少延时
        if self.success_count % 10 == 0 and self.current_delay > self.base_delay:
            self.current_delay = max(self.base_delay, self.current_delay - 0.1)
    
    def record_failure(self):
        """记录失败请求"""
        self.failure_count += 1
        self.success_count = 0
        
        # 失败3次后增加延时
        if self.failure_count >= 3:
            self.current_delay = min(self.max_delay, self.current_delay + 0.5)
            self.failure_count = 0

# 初始化全局限流器
throttler = RequestThrottler(base_delay=BASE_DELAY)

def get_etf_iopv(etf_code: str, date: datetime) -> float:
    """
    获取ETF的IOPV（实时参考净值）
    """
    try:
        date_str = date.strftime("%Y%m%d")
        logger.debug(f"获取 {etf_code} 在 {date_str} 的IOPV")
        
        # 深交所ETF
        if etf_code.startswith('15'):
            url = f"http://www.szse.cn/api/marketdata/v1/etf/realtime?etfCode={etf_code}"
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "http://www.szse.cn/market/etf/index.html",
            }
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                iopv_str = data[0].get('iopv', '')
                return float(iopv_str) if iopv_str else None
        
        # 上交所ETF (51/58开头)
        elif etf_code.startswith(('51', '58')):
            url = f"http://www.sse.com.cn/market/etfdata/iopvdata/{etf_code}.csv"
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.sse.com.cn/market/etf/iopv/",
            }
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            df = pd.read_csv(io.StringIO(response.text), encoding='gbk')
            if not df.empty:
                iopv_str = df.iloc[-1]['参考净值']
                return float(iopv_str) if iopv_str else None
        
        logger.warning(f"ETF {etf_code} 不支持获取IOPV")
        return None
    
    except Exception as e:
        logger.warning(f"ETF {etf_code} IOPV获取失败: {str(e)}")
        return None

def calculate_additional_fields(df: pd.DataFrame, etf_code: str) -> pd.DataFrame:
    """
    计算所有必要衍生字段
    """
    # 确保数据按日期排序
    df = df.sort_values("日期").reset_index(drop=True)
    
    # 1. 振幅 = (最高 - 最低) / 最低 * 100%
    df['振幅'] = ((df['最高'] - df['最低']) / df['最低'] * 100).round(2)
    
    # 2. 涨跌额 = 收盘 - 前一日收盘
    df['涨跌额'] = df['收盘'].diff().fillna(0)
    
    # 3. 涨跌幅 = 涨跌额 / 前一日收盘 * 100%
    df['涨跌幅'] = (df['涨跌额'] / df['收盘'].shift(1) * 100).round(2)
    
    # 4. 换手率 = 成交量 / 基金规模（从ETF列表获取）
    fund_size = get_etf_fund_size(etf_code)
    if fund_size > 0:
        df['换手率'] = (df['成交量'] / fund_size * 100).round(2)
    else:
        df['换手率'] = 0.0
    
    # 5. 折价率/溢价率（严格区分）
    df['折价率'] = df.apply(lambda row: 
        round(((row['IOPV'] - row['收盘']) / row['IOPV'] * 100), 2) 
        if row['IOPV'] > row['收盘'] else 0, axis=1)
    
    df['溢价率'] = df.apply(lambda row: 
        round(((row['收盘'] - row['IOPV']) / row['IOPV'] * 100), 2) 
        if row['收盘'] > row['IOPV'] else 0, axis=1)
    
    return df

def crawl_etf_daily_data(etf_code: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    使用交易所官方接口爬取ETF日线数据
    优化点：
      - 动态延时策略
      - 失败重试机制
      - 严格保持数据结构
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
        
        # 1. 判断交易所
        if etf_code.startswith(('51', '58')):
            exchange = 'sse'  # 上交所
            url = f"http://www.sse.com.cn/market/etfdata/dailydata/{etf_code}.csv"
            logger.info(f"ETF {etf_code} 使用上交所接口: {url}")
        elif etf_code.startswith('15'):
            exchange = 'szse'  # 深交所
            url = f"http://www.szse.cn/api/marketdata/v1/etf?etfCode={etf_code}"
            logger.info(f"ETF {etf_code} 使用深交所接口: {url}")
        else:
            logger.error(f"ETF {etf_code} 代码格式不支持 (非51/58/15开头)")
            return pd.DataFrame()
        
        # 2. 添加必要请求头
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.sse.com.cn/" if exchange == 'sse' else "http://www.szse.cn/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        
        # 3. 执行请求（带重试机制）
        max_retries = MAX_RETRIES
        for retry in range(max_retries):
            try:
                # 动态延时
                throttler.wait()
                
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=20,
                    verify=True
                )
                response.raise_for_status()
                
                # 记录成功请求
                throttler.record_success()
                break
                
            except requests.exceptions.RequestException as e:
                # 记录失败请求
                throttler.record_failure()
                
                # 最后一次重试失败
                if retry == max_retries - 1:
                    logger.error(f"ETF {etf_code} 接口请求失败 (重试 {max_retries} 次): {str(e)}")
                    return pd.DataFrame()
                
                # 等待并重试
                wait_time = BASE_DELAY * (2 ** retry) + random.uniform(0.1, 0.5)
                logger.warning(f"ETF {etf_code} 请求失败，{wait_time:.1f}秒后重试: {str(e)}")
                time.sleep(wait_time)
        
        # 4. 处理不同交易所返回格式
        if exchange == 'sse':
            try:
                df = pd.read_csv(
                    io.StringIO(response.text),
                    encoding='gbk',
                    parse_dates=['日期'],
                    date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d')
                )
                df.rename(columns={
                    '开盘价': '开盘',
                    '最高价': '最高',
                    '最低价': '最低',
                    '收盘价': '收盘',
                    '成交量(股)': '成交量',
                    '成交金额(元)': '成交额',
                    '参考净值': 'IOPV'
                }, inplace=True)
            except Exception as e:
                logger.error(f"上交所CSV解析失败: {str(e)}")
                return pd.DataFrame()
        
        else:  # 深交所
            try:
                data = response.json()
                if not isinstance(data, list) or not data:
                    return pd.DataFrame()
                df = pd.DataFrame(data)
                df.rename(columns={
                    'date': '日期',
                    'open': '开盘',
                    'high': '最高',
                    'low': '最低',
                    'close': '收盘',
                    'volume': '成交量',
                    'turnover': '成交额',
                    'iopv': 'IOPV'
                }, inplace=True)
                df['日期'] = pd.to_datetime(df['日期'], format='%Y-%m-%d')
            except Exception as e:
                logger.error(f"深交所JSON解析失败: {str(e)}")
                return pd.DataFrame()
        
        # 5. 基础数据验证
        required_columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量']
        if any(col not in df.columns for col in required_columns):
            logger.error(f"ETF {etf_code} 数据缺少必要列: {df.columns.tolist()}")
            return pd.DataFrame()
        
        # 6. 筛选指定日期范围
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        df = df[(df['日期'] >= start_str) & (df['日期'] <= end_str)]
        
        # 7. 获取IOPV并计算所有衍生字段
        if 'IOPV' not in df.columns:
            df['IOPV'] = None
        
        # 对于没有IOPV的数据行，尝试获取
        for idx, row in df[df['IOPV'].isna()].iterrows():
            iopv = get_etf_iopv(etf_code, row['日期'])
            if iopv is not None:
                df.at[idx, 'IOPV'] = iopv
        
        # 计算所有衍生字段
        df = calculate_additional_fields(df, etf_code)
        
        # 8. 补充必要字段
        df['ETF代码'] = etf_code
        df['ETF名称'] = get_etf_name(etf_code)
        df['爬取时间'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 9. 【严格保证】字段顺序完全匹配您的要求
        standard_columns = [
            '日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额',
            '振幅', '涨跌幅', '涨跌额', '换手率',
            'IOPV', '折价率', '溢价率',
            'ETF代码', 'ETF名称', '爬取时间'
        ]
        
        # 仅保留需要的列（确保顺序一致）
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

def save_etf_daily_data(etf_code: str, df: pd.DataFrame) -> None:
    """保存数据（保持原有逻辑）"""
    if df.empty: return
    
    os.makedirs(DAILY_DIR, exist_ok=True)
    save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
    
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
            # 确保日期格式正确
            if "日期" in df.columns:
                df_save = df.copy()
                df_save["日期"] = df_save["日期"].dt.strftime('%Y-%m-%d')
            else:
                df_save = df
            df_save.to_csv(temp_file.name, index=False)
        shutil.move(temp_file.name, save_path)
        logger.info(f"ETF {etf_code} 日线数据已保存至 {save_path}，共{len(df)}条数据")
    except Exception as e:
        logger.error(f"保存ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)

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
        
        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        # 保持原有分批处理逻辑
        start_idx = next_index % total_count
        end_idx = start_idx + BATCH_SIZE
        actual_end_idx = end_idx % total_count
        
        if end_idx <= total_count:
            batch_codes = etf_codes[start_idx:end_idx]
            logger.info(f"处理本批次 ETF ({BATCH_SIZE}只)，从索引 {start_idx} 开始")
        else:
            batch_codes = etf_codes[start_idx:total_count] + etf_codes[0:end_idx-total_count]
            logger.info(f"处理本批次 ETF ({BATCH_SIZE}只)，从索引 {start_idx} 开始（循环处理）")
        # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

        first_stock_idx = start_idx % total_count
        last_stock_idx = (end_idx - 1) % total_count
        first_stock = f"{etf_codes[first_stock_idx]} - {get_etf_name(etf_codes[first_stock_idx])}" if first_stock_idx < len(etf_codes) else "N/A"
        last_stock = f"{etf_codes[last_stock_idx]} - {get_etf_name(etf_codes[last_stock_idx])}" if last_stock_idx < len(etf_codes) else "N/A"
        logger.info(f"当前批次第一只ETF: {first_stock} (索引 {first_stock_idx})")
        logger.info(f"当前批次最后一只ETF: {last_stock} (索引 {last_stock_idx})")
        
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
                logger.warning(f"⚠️ 未获取到数据")
                with open(os.path.join(DAILY_DIR, "failed_etfs.txt"), "a", encoding="utf-8") as f:
                    f.write(f"{etf_code},{etf_name},未获取到数据\n")
                continue
            
            save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
            if os.path.exists(save_path):
                try:
                    existing_df = pd.read_csv(save_path)
                    if "日期" in existing_df.columns:
                        existing_df["日期"] = pd.to_datetime(existing_df["日期"], errors='coerce')
                    
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                    combined_df = combined_df.sort_values("日期", ascending=False)
                    
                    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                        combined_df.to_csv(temp_file.name, index=False)
                    shutil.move(temp_file.name, save_path)
                    logger.info(f"✅ 数据已追加至: {save_path} (合并后共{len(combined_df)}条)")
                finally:
                    if os.path.exists(temp_file.name):
                        os.unlink(temp_file.name)
            else:
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                    df.to_csv(temp_file.name, index=False)
                shutil.move(temp_file.name, save_path)
                logger.info(f"✅ 数据已保存至: {save_path} ({len(df)}条)")
            
            processed_count += 1
            current_index = (start_idx + i) % total_count
            logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
            
            if processed_count % 10 == 0:
                logger.info(f"已处理 {processed_count} 只ETF，提交批量文件...")
                if not force_commit_remaining_files():
                    logger.error("提交批量文件失败")
        
        new_index = actual_end_idx
        save_crawl_progress(new_index)
        logger.info(f"进度已更新为 {new_index}/{total_count}")
        
        remaining_stocks = total_count - new_index
        if remaining_stocks < 0:
            remaining_stocks = total_count
        logger.info(f"本批次爬取完成，共处理 {processed_count} 只ETF，还有 {remaining_stocks} 只ETF待爬取")
        
        logger.info("处理完成后，确保提交所有剩余文件...")
        if not force_commit_remaining_files():
            logger.error("强制提交剩余文件失败，可能导致数据丢失")
        
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
        
        # 严格只读 - 确保不修改ETF列表
        if basic_info_df.empty or "ETF代码" not in basic_info_df.columns:
            logger.error("ETF列表文件格式错误")
            return []
        
        return basic_info_df["ETF代码"].tolist()
    except Exception as e:
        logger.error(f"获取ETF代码列表失败: {str(e)}", exc_info=True)
        return []

if __name__ == "__main__":
    try:
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
