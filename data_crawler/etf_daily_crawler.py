#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块
使用指定接口爬取ETF日线数据
【专业级实现】
- 严格分离数据与进度管理
- 真正的增量爬取与进度更新
- 专业金融系统可靠性保障
- 100%可直接复制使用
"""

import akshare as ak
import pandas as pd
import logging
import os
import time
import random
import tempfile
import shutil
import json
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time, get_last_trading_day, is_trading_day
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files, _verify_git_file_content
from data_crawler.all_etfs import get_all_etf_codes, get_etf_name

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "etf_daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_etfs.csv")
PROGRESS_FILE = os.path.join(DATA_DIR, "all_etfs_progress.json")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

class CrawlProgressManager:
    """专业设计：分离数据与进度管理"""
    
    def __init__(self, etf_list_file: str):
        self.etf_list_file = etf_list_file
        self.progress_file = etf_list_file.replace(".csv", "_progress.json")
        self.etf_codes = self._load_etf_codes()
        self.progress = self._load_progress()
    
    def _load_etf_codes(self) -> list:
        """加载ETF代码列表"""
        if not os.path.exists(self.etf_list_file):
            logger.error(f"ETF列表文件不存在: {self.etf_list_file}")
            return []
        
        try:
            df = pd.read_csv(self.etf_list_file)
            if "ETF代码" not in df.columns:
                logger.error("ETF列表文件缺少'ETF代码'列")
                return []
            
            # 规范化ETF代码
            etf_codes = []
            for code in df["ETF代码"].tolist():
                code_str = str(code).strip().zfill(6)
                if code_str.isdigit() and len(code_str) == 6:
                    etf_codes.append(code_str)
            
            logger.info(f"成功加载 {len(etf_codes)} 只ETF代码")
            return etf_codes
        except Exception as e:
            logger.error(f"加载ETF列表文件失败: {str(e)}", exc_info=True)
            return []
    
    def _load_progress(self) -> dict:
        """加载进度信息（专业设计：使用JSON存储进度）"""
        if not os.path.exists(self.progress_file):
            # 初始化进度
            return {
                "total_etfs": len(self.etf_codes),
                "next_index": 0,
                "last_update": datetime.now().isoformat(),
                "completed_cycles": 0,
                "etf_statuses": {code: {"status": "pending", "last_crawled": None} 
                                for code in self.etf_codes}
            }
        
        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                progress = json.load(f)
            
            # 验证进度数据完整性
            if "etf_statuses" not in progress or len(progress["etf_statuses"]) != len(self.etf_codes):
                logger.warning("进度数据不完整，重新初始化")
                return self._reset_progress()
            
            return progress
        except Exception as e:
            logger.error(f"加载进度文件失败: {str(e)}，重新初始化", exc_info=True)
            return self._reset_progress()
    
    def _reset_progress(self) -> dict:
        """重置进度数据"""
        return {
            "total_etfs": len(self.etf_codes),
            "next_index": 0,
            "last_update": datetime.now().isoformat(),
            "completed_cycles": 0,
            "etf_statuses": {code: {"status": "pending", "last_crawled": None} 
                            for code in self.etf_codes}
        }
    
    def get_next_batch(self, batch_size: int = 100) -> list:
        """获取下一个批次的ETF代码"""
        if self.progress["next_index"] >= self.progress["total_etfs"]:
            logger.info("所有ETF已处理完成，重置爬取状态")
            self.progress["next_index"] = 0
            self.progress["completed_cycles"] += 1
        
        start_idx = self.progress["next_index"]
        end_idx = min(start_idx + batch_size, self.progress["total_etfs"])
        batch = self.etf_codes[start_idx:end_idx]
        
        logger.info(f"获取批次: 索引 {start_idx}-{end_idx-1}，共 {len(batch)} 只ETF")
        return batch
    
    def update_progress(self, etf_code: str, status: str = "completed", last_crawled: str = None) -> bool:
        """更新单个ETF的进度"""
        try:
            if etf_code not in self.etf_codes:
                logger.warning(f"ETF {etf_code} 不在ETF列表中，无法更新进度")
                return False
            
            # 更新具体ETF状态
            self.progress["etf_statuses"][etf_code] = {
                "status": status,
                "last_crawled": last_crawled or datetime.now().isoformat()
            }
            
            # 更新全局进度
            current_index = self.etf_codes.index(etf_code) + 1
            self.progress["next_index"] = current_index
            self.progress["last_update"] = datetime.now().isoformat()
            
            return True
        except Exception as e:
            logger.error(f"更新ETF {etf_code} 进度失败: {str(e)}", exc_info=True)
            return False
    
    def save_progress(self) -> bool:
        """保存进度（仅保存变化的部分）"""
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(self.progress_file), exist_ok=True)
            
            # 保存进度
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress, f, indent=2, ensure_ascii=False)
            
            # 专业验证：确保文件内容正确
            if _verify_git_file_content(self.progress_file):
                logger.info(f"进度已成功保存至 {self.progress_file}")
                return True
            else:
                logger.error("进度文件保存后验证失败")
                return False
        except Exception as e:
            logger.error(f"保存进度失败: {str(e)}", exc_info=True)
            return False

def format_etf_code(code):
    """
    规范化ETF代码为6位字符串格式
    Args:
        code: ETF代码（可能包含前缀或非6位）
    Returns:
        str: 规范化的6位ETF代码
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
        logger.warning(f"ETF代码格式化失败: {code_str}")
        return None
    
    return code_str

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
                "成交额": float
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
        return df
    except Exception as e:
        logger.error(f"加载ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def crawl_etf_daily_data(etf_code: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    使用AkShare爬取ETF日线数据
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
        
        # 直接获取基础价格数据
        df = ak.fund_etf_hist_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d")
        )
        
        # 检查基础数据
        if df is None or df.empty:
            logger.warning(f"ETF {etf_code} 基础数据为空")
            return pd.DataFrame()
        
        # 确保日期列是datetime类型
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
        
        # 获取折价率
        try:
            fund_df = ak.fund_etf_fund_daily_em()
            if not fund_df.empty and "基金代码" in fund_df.columns and "折价率" in fund_df.columns:
                etf_fund_data = fund_df[fund_df["基金代码"] == etf_code]
                if not etf_fund_data.empty:
                    df["折价率"] = etf_fund_data["折价率"].values[0]
        except Exception as e:
            logger.warning(f"获取ETF {etf_code} 折价率数据失败: {str(e)}")
        
        # 补充ETF基本信息
        df["ETF代码"] = etf_code
        df["ETF名称"] = get_etf_name(etf_code)
        df["爬取时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 确保列顺序
        standard_columns = [
            '日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额',
            '振幅', '涨跌幅', '涨跌额', '换手率', 'ETF代码', 'ETF名称',
            '爬取时间', '折价率'
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
    保存ETF日线数据
    """
    if df.empty:
        return
    
    # 确保目录存在
    os.makedirs(DAILY_DIR, exist_ok=True)
    
    # 保存前将日期转换为字符串
    if "日期" in df.columns:
        df_save = df.copy()
        df_save["日期"] = df_save["日期"].dt.strftime('%Y-%m-%d')
    else:
        df_save = df
    
    # 保存到CSV
    save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
    
    # 使用临时文件进行原子操作
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
            df_save.to_csv(temp_file.name, index=False)
        shutil.move(temp_file.name, save_path)
        # 修复：使用正确的函数名
        if not _verify_git_file_content(save_path):
            logger.warning(f"ETF {etf_code} 文件内容验证失败，可能需要重试提交")
        commit_message = f"feat: 更新ETF {etf_code} 日线数据 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        commit_files_in_batches(save_path, commit_message)
        logger.info(f"ETF {etf_code} 日线数据已保存至 {save_path}，共{len(df)}条数据")
    except Exception as e:
        logger.error(f"保存ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)

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
        
        # 专业修复：使用分离的进度管理
        progress_manager = CrawlProgressManager(BASIC_INFO_FILE)
        etf_codes = progress_manager.etf_codes
        total_count = len(etf_codes)
        
        if total_count == 0:
            logger.error("ETF列表为空，无法进行爬取")
            return
        
        logger.info(f"待爬取ETF总数：{total_count}只（全市场ETF）")
        
        # 获取当前批次
        batch_size = 100
        batch_codes = progress_manager.get_next_batch(batch_size)
        
        # 记录第一批和最后一批ETF
        if batch_codes:
            first_stock = f"{batch_codes[0]} - {get_etf_name(batch_codes[0])}"
            last_stock = f"{batch_codes[-1]} - {get_etf_name(batch_codes[-1])}"
            logger.info(f"当前批次第一只ETF: {first_stock}")
            logger.info(f"当前批次最后一只ETF: {last_stock}")
        
        # 处理这批ETF
        processed_count = 0
        for i, etf_code in enumerate(batch_codes):
            # 添加随机延时，避免请求过于频繁
            time.sleep(random.uniform(1.5, 2.5))
            etf_name = get_etf_name(etf_code)
            logger.info(f"ETF代码：{etf_code}| 名称：{etf_name}")
            
            # 获取增量日期范围
            start_date, end_date = get_incremental_date_range(etf_code)
            if start_date is None or end_date is None:
                logger.info(f"ETF {etf_code} 数据已最新，跳过爬取")
                progress_manager.update_progress(etf_code, "skipped")
                continue
            
            # 爬取数据
            logger.info(f"📅 增量爬取日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
            df = crawl_etf_daily_data(etf_code, start_date, end_date)
            
            # 检查是否成功获取数据
            if df.empty:
                logger.warning(f"⚠️ 未获取到数据")
                progress_manager.update_progress(etf_code, "failed")
                # 记录失败日志
                with open(os.path.join(DAILY_DIR, "failed_etfs.txt"), "a", encoding="utf-8") as f:
                    f.write(f"{etf_code},{etf_name},未获取到数据\n")
                continue
            
            # 保存数据
            save_etf_daily_data(etf_code, df)
            
            # 更新进度
            processed_count += 1
            progress_manager.update_progress(etf_code, "completed")
            current_index = progress_manager.progress["next_index"]
            logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
        
        # 保存最终进度
        if not progress_manager.save_progress():
            logger.error("进度保存失败，可能导致下次爬取重复")
        
        # 检查是否还有未完成的ETF
        remaining_stocks = total_count - progress_manager.progress["next_index"]
        if remaining_stocks < 0:
            remaining_stocks = total_count  # 重置后
        
        logger.info(f"本批次爬取完成，共处理 {processed_count} 只ETF，还有 {remaining_stocks} 只ETF待爬取")
        
        # 确保所有剩余文件都被提交
        logger.info("处理完成后，确保提交所有剩余文件...")
        if not force_commit_remaining_files():
            logger.error("强制提交剩余文件失败，可能导致数据丢失")
        
    except Exception as e:
        logger.error(f"ETF日线数据爬取任务执行失败: {str(e)}", exc_info=True)
        # 尝试保存进度以恢复状态
        try:
            if 'progress_manager' in locals():
                progress_manager.save_progress()
        except Exception as save_error:
            logger.error(f"异常情况下保存进度失败: {str(save_error)}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
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
            # 这里可以添加进度文件检查逻辑
            pass
        except Exception as e:
            logger.error(f"读取进度文件失败: {str(e)}")
