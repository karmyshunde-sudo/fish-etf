#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件操作工具模块
提供文件读写、标志文件管理、目录操作等常用功能
特别优化了增量数据保存功能
"""
import os
import json
import csv
import logging
import shutil
import tempfile
import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Union, TextIO, Tuple
from datetime import datetime, timedelta, timezone  # 确保timedelta已正确导入
# 添加缺失的导入
from utils.date_utils import get_beijing_time, get_utc_time
from pathlib import Path
# 导入init_dirs函数
from config import Config
# 配置日志
logger = logging.getLogger(__name__)
# 重新导出init_dirs函数，使其可以从file_utils模块导入
init_dirs = Config.init_dirs
def ensure_dir_exists(dir_path: str) -> bool:
    """
    确保目录存在，如果不存在则创建
    Args:
        dir_path: 目录路径
    Returns:
        bool: 是否成功创建或目录已存在
    """
    try:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            logger.debug(f"已创建目录: {dir_path}")
        return True
    except Exception as e:
        logger.error(f"创建目录失败: {dir_path} - {str(e)}", exc_info=True)
        return False
def check_flag(flag_file: str) -> bool:
    """
    检查标志文件是否存在
    Args:
        flag_file: 标志文件路径
    Returns:
        bool: 标志文件是否存在
    """
    try:
        return os.path.exists(flag_file)
    except Exception as e:
        logger.error(f"检查标志文件失败: {str(e)}", exc_info=True)
        return False
def set_flag(flag_file: str) -> bool:
    """
    设置标志文件
    Args:
        flag_file: 标志文件路径
    Returns:
        bool: 是否成功设置
    """
    try:
        # 确保目录存在
        dir_path = os.path.dirname(flag_file)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        # 创建标志文件
        with open(flag_file, 'w', encoding='utf-8') as f:
            f.write(f"标记于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.debug(f"已设置标志文件: {flag_file}")
        return True
    except Exception as e:
        logger.error(f"设置标志文件失败: {str(e)}", exc_info=True)
        return False
def get_file_mtime(file_path: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    获取文件最后修改时间（UTC与北京时间）
    Args:
        file_path: 文件路径
    Returns:
        Tuple[Optional[datetime], Optional[datetime]]: (UTC时间, 北京时间)，如果获取失败返回(None, None)
    """
    try:
        if not os.path.exists(file_path):
            return None, None
        # 获取文件最后修改时间戳
        mtime = os.path.getmtime(file_path)
        # 转换为datetime对象（带时区信息）
        utc_time = datetime.fromtimestamp(mtime, tz=Config.UTC_TIMEZONE)
        beijing_time = datetime.fromtimestamp(mtime, tz=Config.BEIJING_TIMEZONE)
        return utc_time, beijing_time
    except Exception as e:
        logger.error(f"获取文件修改时间失败: {str(e)}", exc_info=True)
        return None, None
def ensure_chinese_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    确保DataFrame使用中文列名，完全依赖config.py中定义的映射
    Args:
        df: 原始DataFrame
    Returns:
        pd.DataFrame: 使用中文列名的DataFrame
    """
    try:
        if df.empty:
            return df
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        # 检查是否已经是中文列名 - 使用config中定义的STANDARD_COLUMNS
        if all(col in Config.STANDARD_COLUMNS for col in df.columns):
            logger.debug("DataFrame已使用标准中文列名，无需映射")
            return df
        # 记录原始列名用于诊断
        logger.info(f"原始列名: {list(df.columns)}")
        # 使用config中定义的列名映射
        col_mapping = {}
        for eng_col, chn_col in Config.COLUMN_NAME_MAPPING.items():
            # 模糊匹配，处理大小写和空格差异
            for col in df.columns:
                if eng_col.lower() in str(col).lower().replace(' ', ''):
                    col_mapping[col] = chn_col
                    logger.debug(f"映射列名: {col} -> {chn_col}")
                    break
        # 重命名列
        df = df.rename(columns=col_mapping)
        # 确保关键列存在
        required_columns = ["日期", "收盘", "成交额"]
        for col in required_columns:
            if col not in df.columns:
                # 尝试使用替代列
                if col == "日期" and "交易日期" in df.columns:
                    df["日期"] = df["交易日期"]
                elif col == "收盘" and "最新价" in df.columns:
                    df["收盘"] = df["最新价"]
                elif col == "成交额" and "成交金额" in df.columns:
                    df["成交额"] = df["成交金额"]
                else:
                    logger.warning(f"DataFrame缺少必要列: {col}")
                    # 创建空列，避免后续处理失败
                    df[col] = np.nan
        # 记录映射后的列名
        logger.info(f"映射后列名: {list(df.columns)}")
        return df
    except Exception as e:
        logger.error(f"确保中文列名失败: {str(e)}", exc_info=True)
        return df
def internal_ensure_chinese_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    内部实现的列名标准化函数（不依赖utils.file_utils）
    Args:
        df: 原始DataFrame
    Returns:
        pd.DataFrame: 使用中文列名的DataFrame
    """
    if df.empty:
        return df
    # 列名映射字典（移除了所有与"折溢价率"相关的映射）
    column_mapping = {
        # 日期列
        'date': '日期',
        'trade_date': '日期',
        'dt': '日期',
        'date_time': '日期',
        '日期': '日期',  # 防止重复映射
        # 价格列
        'open': '开盘',
        'open_price': '开盘',
        'openprice': '开盘',
        'openprice_': '开盘',
        '开盘': '开盘',
        'high': '最高',
        'high_price': '最高',
        'highprice': '最高',
        '最高': '最高',
        'low': '最低',
        'low_price': '最低',
        'lowprice': '最低',
        '最低': '最低',
        'close': '收盘',
        'close_price': '收盘',
        'closeprice': '收盘',
        '收盘价': '收盘',
        '最新价': '收盘',
        '收盘': '收盘',
        # 成交量列
        'volume': '成交量',
        'vol': '成交量',
        '成交数量': '成交量',
        '成交量': '成交量',
        # 成交额列
        'amount': '成交额',
        '成交金额': '成交额',
        '成交额': '成交额',
        # 涨跌幅列
        'pct_chg': '涨跌幅',
        'change_pct': '涨跌幅',
        '涨跌幅': '涨跌幅',
        '涨跌幅度': '涨跌幅',
        # 涨跌额列
        'change': '涨跌额',
        'price_change': '涨跌额',
        '涨跌额': '涨跌额',
        '涨跌': '涨跌额',
        # 振幅列
        'amplitude': '振幅',
        '振幅': '振幅',
        # 换手率列
        'turnover': '换手率',
        'turnover_rate': '换手率',
        '换手率': '换手率',
        # IOPV列
        'iopv': 'IOPV',
        'iopv_value': 'IOPV',
        'IOPV实时估值': 'IOPV',
        '实时估值': 'IOPV',
        'IOPV': 'IOPV',
        # 净值列
        'net_value': '净值',
        'nav': '净值',
        '净值': '净值',
        '单位净值': '净值',
        # ETF代码列
        'code': 'ETF代码',
        'symbol': 'ETF代码',
        'ETF代码': 'ETF代码',
        # ETF名称列
        'name': 'ETF名称',
        '基金名称': 'ETF名称',
        'ETF名称': 'ETF名称'
    }
    # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
    df = df.copy(deep=True)
    # 记录原始列名用于诊断
    logger.debug(f"internal_ensure_chinese_columns: 原始列名: {list(df.columns)}")
    # 尝试模糊匹配列名（处理大小写和空格差异）
    final_mapping = {}
    for col in df.columns:
        # 标准化列名（小写、移除空格）
        temp_col = str(col).strip().lower().replace(' ', '')
        normalized_col = temp_col.replace('\n', '')
        # normalized_col = str(col).strip().lower().replace(' ', '').replace('\n', '')
        # 尝试匹配映射字典
        for src, target in column_mapping.items():
            normalized_src = src.strip().lower().replace(' ', '')
            # 完全匹配或部分匹配
            if normalized_src == normalized_col or normalized_col.startswith(normalized_src) or normalized_src.startswith(normalized_col):
                final_mapping[col] = target
                logger.debug(f"internal_ensure_chinese_columns: 映射列名 {col} -> {target}")
                break
    # 重命名列
    df = df.rename(columns=final_mapping)
    # 确保关键列存在
    required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
    for col in required_columns:
        if col not in df.columns:
            logger.warning(f"internal_ensure_chinese_columns: DataFrame缺少必要列: {col}")
            # 尝试使用替代列
            if col == "日期" and "交易日期" in df.columns:
                df["日期"] = df["交易日期"]
            elif col == "收盘" and "最新价" in df.columns:
                df["收盘"] = df["最新价"]
            elif col == "成交量" and "成交数量" in df.columns:
                df["成交量"] = df["成交数量"]
            elif col == "成交额" and "成交金额" in df.columns:
                df["成交额"] = df["成交金额"]
    # 记录映射后的列名
    logger.debug(f"internal_ensure_chinese_columns: 映射后列名: {list(df.columns)}")
    return df
def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化列名（英文转中文）
    这是ensure_chinese_columns的别名，用于明确表示在数据爬取上下文中的用途
    Args:
        df: 输入DataFrame
    Returns:
        pd.DataFrame: 标准化列名后的DataFrame
    """
    try:
        return ensure_chinese_columns(df)
    except Exception as e:
        logger.error(f"标准化列名失败: {str(e)}", exc_info=True)
        return df
def get_last_crawl_date(etf_code: str, etf_daily_dir: str) -> str:
    """
    获取ETF最后爬取日期
    Args:
        etf_code: ETF代码
        etf_daily_dir: ETF日线数据目录
    Returns:
        str: 最后爬取日期（格式：YYYY-MM-DD）
    """
    try:
        file_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
        # 如果文件不存在，返回初始爬取日期
        if not os.path.exists(file_path):
            # 首次爬取：获取1年历史数据
            last_trading_day = get_last_trading_day()
            start_date = (last_trading_day - timedelta(days=365)).strftime("%Y-%m-%d")
            logger.debug(f"ETF {etf_code} 无历史数据，首次爬取使用日期: {start_date}")
            return start_date
        # 读取CSV文件
        df = pd.read_csv(file_path)
        # 如果DataFrame为空或没有日期列，返回初始爬取日期
        if df.empty or "日期" not in df.columns:
            # 首次爬取：获取1年历史数据
            last_trading_day = get_last_trading_day()
            start_date = (last_trading_day - timedelta(days=365)).strftime("%Y-%m-%d")
            logger.debug(f"ETF {etf_code} 数据文件异常，首次爬取使用日期: {start_date}")
            return start_date
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        # 确保日期列是datetime类型
        if "日期" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["日期"]):
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
        # 删除无效日期
        df = df.dropna(subset=["日期"])
        # 如果处理后DataFrame为空
        if df.empty:
            # 首次爬取：获取1年历史数据
            last_trading_day = get_last_trading_day()
            start_date = (last_trading_day - timedelta(days=365)).strftime("%Y-%m-%d")
            logger.debug(f"ETF {etf_code} 日期列无效，首次爬取使用日期: {start_date}")
            return start_date
        # 获取最新日期
        latest_date = df["日期"].max()
        if pd.isna(latest_date):
            # 首次爬取：获取1年历史数据
            last_trading_day = get_last_trading_day()
            start_date = (last_trading_day - timedelta(days=365)).strftime("%Y-%m-%d")
            logger.debug(f"ETF {etf_code} 日期列包含无效值，首次爬取使用日期: {start_date}")
            return start_date
        # 转换为字符串
        latest_date_str = latest_date.strftime("%Y-%m-%d")
        # 获取下一个交易日作为开始日期
        # 局部导入，避免循环导入
        from utils.date_utils import get_next_trading_day
        next_trading_day = get_next_trading_day(latest_date)
        next_date = next_trading_day.strftime("%Y-%m-%d")
        logger.debug(f"ETF {etf_code} 最后爬取日期: {latest_date_str}，增量爬取从 {next_date} 开始")
        return next_date
    except Exception as e:
        logger.error(f"获取ETF {etf_code} 最后爬取日期失败: {str(e)}", exc_info=True)
        # 出错时返回初始爬取日期
        last_trading_day = get_last_trading_day()
        start_date = (last_trading_day - timedelta(days=365)).strftime("%Y-%m-%d")
        return start_date
def load_etf_daily_data(etf_code: str, data_dir: Optional[Union[str, Path]] = None) -> pd.DataFrame:
    """
    加载ETF日线数据
    Args:
        etf_code: ETF代码
        data_dir: 数据存储目录（可选）
    Returns:
        pd.DataFrame: ETF日线数据
    """
    try:
        # 确保目录存在
        if data_dir is None:
            data_dir = Config.ETFS_DAILY_DIR
        # 确保ETF代码格式一致（6位数字）
        etf_code = str(etf_code).strip().zfill(6)
        # 构建文件路径
        file_path = os.path.join(data_dir, f"{etf_code}.csv")
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.debug(f"ETF {etf_code} 日线数据文件不存在: {file_path}")
            return pd.DataFrame()
        # 读取CSV文件
        df = pd.read_csv(file_path)
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        # 记录加载的列名用于诊断
        logger.debug(f"成功加载ETF {etf_code} 日线数据: {file_path}，列名: {list(df.columns)}，共{len(df)}条")
        # 确保DataFrame使用中文列名
        df = ensure_chinese_columns(df)
        # 检查数据完整性
        required_columns = ["日期", "收盘", "成交额"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"ETF {etf_code} 日线数据缺少必要列: {', '.join(missing_columns)}")
            # 记录实际存在的列
            logger.debug(f"实际列名: {list(df.columns)}")
        # 确保数据按日期排序
        if "日期" in df.columns:
            df = df.sort_values("日期", ascending=False)
        # 不再重新计算折溢价率，直接使用已有数据
        # 如果确实需要折溢价率但数据中没有，应该在数据爬取阶段确保获取
        if "折溢价率" not in df.columns:
            logger.warning(f"ETF {etf_code} 日线数据缺少'折溢价率'列，应检查数据爬取过程")
        return df
    except Exception as e:
        logger.error(f"加载ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()
def load_arbitrage_data(date_str: str) -> pd.DataFrame:
    """
    加载指定日期的套利数据
    Args:
        date_str: 日期字符串，格式为YYYYMMDD
    Returns:
        pd.DataFrame: 套利数据DataFrame
    """
    try:
        # 构建套利数据目录
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        os.makedirs(arbitrage_dir, exist_ok=True)
        # 构建文件路径
        file_path = os.path.join(arbitrage_dir, f"{date_str}.csv")
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.info(f"套利数据文件不存在: {file_path}")
            return pd.DataFrame()
        # 读取CSV文件（明确指定编码）
        df = pd.read_csv(file_path, encoding="utf-8-sig")
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        # 添加关键诊断日志
        logger.info(f"成功加载套利数据: {file_path}")
        logger.info(f"实际列名: {list(df.columns)}")
        if not df.empty:
            logger.info(f"前几行数据示例: {df.head().to_dict()}")
        # 确保DataFrame使用中文列名
        df = ensure_chinese_columns(df)
        # 二次验证列名
        if "ETF代码" not in df.columns or "ETF名称" not in df.columns:
            logger.error(f"加载的套利数据缺少必要列，实际列名: {list(df.columns)}")
            return pd.DataFrame()
        return df
    except Exception as e:
        logger.error(f"加载套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()
def load_arbitrage_status() -> Dict[str, Dict[str, Any]]:
    """
    加载套利状态记录
    Returns:
        Dict[str, Dict[str, Any]]: 套利状态字典
    """
    try:
        # 确保目录存在
        status_dir = Path(Config.ARBITRAGE_STATUS_FILE).parent
        if not status_dir.exists():
            os.makedirs(status_dir, exist_ok=True)
        # 检查文件是否存在
        if not os.path.exists(Config.ARBITRAGE_STATUS_FILE):
            logger.debug(f"套利状态文件不存在: {Config.ARBITRAGE_STATUS_FILE}")
            return {}
        # 读取状态文件
        with open(Config.ARBITRAGE_STATUS_FILE, 'r', encoding='utf-8') as f:
            status = json.load(f)
        logger.debug(f"成功加载套利状态，共 {len(status)} 条记录")
        return status
    except Exception as e:
        logger.error(f"加载套利状态失败: {str(e)}", exc_info=True)
        return {}
def save_arbitrage_status(status: Dict[str, Dict[str, Any]]) -> bool:
    """
    保存套利状态记录
    Args:
        status: 套利状态字典
    Returns:
        bool: 是否保存成功
    """
    try:
        # 确保目录存在
        status_dir = Path(Config.ARBITRAGE_STATUS_FILE).parent
        if not status_dir.exists():
            os.makedirs(status_dir, exist_ok=True)
        # 保存状态
        with open(Config.ARBITRAGE_STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status, f, indent=2, ensure_ascii=False)
        logger.debug(f"套利状态保存成功，共 {len(status)} 条记录")
        return True
    except Exception as e:
        logger.error(f"保存套利状态失败: {str(e)}", exc_info=True)
        return False
def should_push_arbitrage(etf_code: str) -> bool:
    """
    检查是否应该推送该ETF的套利机会
    Args:
        etf_code: ETF代码
    Returns:
        bool: 是否应该推送
    """
    try:
        # 获取当前北京时间
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        # 加载套利状态
        status = load_arbitrage_status()
        # 如果ETF从未推送过，或者上次推送不是今天，则应该推送
        return etf_code not in status or status.get(etf_code, {}).get("last_pushed") != today
    except Exception as e:
        logger.error(f"检查是否应该推送套利机会失败: {str(e)}", exc_info=True)
        # 出错时保守策略：允许推送
        return True
def mark_arbitrage_pushed(etf_code: str, score: float) -> bool:
    """
    标记ETF套利机会已推送
    Args:
        etf_code: ETF代码
        score: 套利评分
    Returns:
        bool: 是否成功标记
    """
    try:
        # 获取当前北京时间
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        # 加载套利状态
        status = load_arbitrage_status()
        # 更新状态
        status[etf_code] = {
            "last_pushed": today,
            "score": score
        }
        # 保存更新后的状态
        return save_arbitrage_status(status)
    except Exception as e:
        logger.error(f"标记ETF套利机会已推送失败: {str(e)}", exc_info=True)
        return False
def load_discount_status() -> Dict[str, Dict[str, Any]]:
    """
    加载折价状态记录
    Returns:
        Dict[str, Dict[str, Any]]: 折价状态字典
    """
    try:
        # 确保目录存在
        status_dir = Path(Config.DISCOUNT_STATUS_FILE).parent
        if not status_dir.exists():
            os.makedirs(status_dir, exist_ok=True)
        # 检查文件是否存在
        if not os.path.exists(Config.DISCOUNT_STATUS_FILE):
            logger.debug(f"折价状态文件不存在: {Config.DISCOUNT_STATUS_FILE}")
            return {}
        # 读取状态文件
        with open(Config.DISCOUNT_STATUS_FILE, 'r', encoding='utf-8') as f:
            status = json.load(f)
        logger.debug(f"成功加载折价状态，共 {len(status)} 条记录")
        return status
    except Exception as e:
        logger.error(f"加载折价状态失败: {str(e)}", exc_info=True)
        return {}
def save_discount_status(status: Dict[str, Dict[str, Any]]) -> bool:
    """
    保存折价状态记录
    Args:
        status: 折价状态字典
    Returns:
        bool: 是否保存成功
    """
    try:
        # 确保目录存在
        status_dir = Path(Config.DISCOUNT_STATUS_FILE).parent
        if not status_dir.exists():
            os.makedirs(status_dir, exist_ok=True)
        # 保存状态
        with open(Config.DISCOUNT_STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status, f, indent=2, ensure_ascii=False)
        logger.debug(f"折价状态保存成功，共 {len(status)} 条记录")
        return True
    except Exception as e:
        logger.error(f"保存折价状态失败: {str(e)}", exc_info=True)
        return False
def should_push_discount(etf_code: str) -> bool:
    """
    检查是否应该推送该ETF的折价机会
    Args:
        etf_code: ETF代码
    Returns:
        bool: 是否应该推送
    """
    try:
        # 获取当前北京时间
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        # 加载折价状态
        status = load_discount_status()
        # 如果ETF从未推送过，或者上次推送不是今天，则应该推送
        return etf_code not in status or status.get(etf_code, {}).get("last_pushed") != today
    except Exception as e:
        logger.error(f"检查是否应该推送折价机会失败: {str(e)}", exc_info=True)
        # 出错时保守策略：允许推送
        return True
def mark_discount_pushed(etf_code: str, score: float) -> bool:
    """
    标记ETF折价机会已推送
    Args:
        etf_code: ETF代码
        score: 折价评分
    Returns:
        bool: 是否成功标记
    """
    try:
        # 获取当前北京时间
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        # 加载折价状态
        status = load_discount_status()
        # 更新状态
        status[etf_code] = {
            "last_pushed": today,
            "score": score
        }
        # 保存更新后的状态
        return save_discount_status(status)
    except Exception as e:
        logger.error(f"标记ETF折价机会已推送失败: {str(e)}", exc_info=True)
        return False
def load_premium_status() -> Dict[str, Dict[str, Any]]:
    """
    加载溢价状态记录
    Returns:
        Dict[str, Dict[str, Any]]: 溢价状态字典
    """
    try:
        # 确保目录存在
        status_dir = Path(Config.PREMIUM_STATUS_FILE).parent
        if not status_dir.exists():
            os.makedirs(status_dir, exist_ok=True)
        # 检查文件是否存在
        if not os.path.exists(Config.PREMIUM_STATUS_FILE):
            logger.debug(f"溢价状态文件不存在: {Config.PREMIUM_STATUS_FILE}")
            return {}
        # 读取状态文件
        with open(Config.PREMIUM_STATUS_FILE, 'r', encoding='utf-8') as f:
            status = json.load(f)
        logger.debug(f"成功加载溢价状态，共 {len(status)} 条记录")
        return status
    except Exception as e:
        logger.error(f"加载溢价状态失败: {str(e)}", exc_info=True)
        return {}
def save_premium_status(status: Dict[str, Dict[str, Any]]) -> bool:
    """
    保存溢价状态记录
    Args:
        status: 溢价状态字典
    Returns:
        bool: 是否保存成功
    """
    try:
        # 确保目录存在
        status_dir = Path(Config.PREMIUM_STATUS_FILE).parent
        if not status_dir.exists():
            os.makedirs(status_dir, exist_ok=True)
        # 保存状态
        with open(Config.PREMIUM_STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status, f, indent=2, ensure_ascii=False)
        logger.debug(f"溢价状态保存成功，共 {len(status)} 条记录")
        return True
    except Exception as e:
        logger.error(f"保存溢价状态失败: {str(e)}", exc_info=True)
        return False
def should_push_premium(etf_code: str) -> bool:
    """
    检查是否应该推送该ETF的溢价机会
    Args:
        etf_code: ETF代码
    Returns:
        bool: 是否应该推送
    """
    try:
        # 获取当前北京时间
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        # 加载溢价状态
        status = load_premium_status()
        # 如果ETF从未推送过，或者上次推送不是今天，则应该推送
        return etf_code not in status or status.get(etf_code, {}).get("last_pushed") != today
    except Exception as e:
        logger.error(f"检查是否应该推送溢价机会失败: {str(e)}", exc_info=True)
        # 出错时保守策略：允许推送
        return True
def mark_premium_pushed(etf_code: str, score: float) -> bool:
    """
    标记ETF溢价机会已推送
    Args:
        etf_code: ETF代码
        score: 溢价评分
    Returns:
        bool: 是否成功标记
    """
    try:
        # 获取当前北京时间
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        # 加载溢价状态
        status = load_premium_status()
        # 更新状态
        status[etf_code] = {
            "last_pushed": today,
            "score": score
        }
        # 保存更新后的状态
        return save_premium_status(status)
    except Exception as e:
        logger.error(f"标记ETF溢价机会已推送失败: {str(e)}", exc_info=True)
        return False
def clear_expired_arbitrage_status() -> bool:
    """
    【已修复】不再清理套利状态记录
    套利状态记录是交易流水，必须永久保存
    Returns:
        bool: 始终返回True（不再执行清理操作）
    """
    logger.info("跳过套利状态记录清理 - 交易流水必须永久保存")
    return True
def clear_expired_discount_status() -> bool:
    """
    【已修复】不再清理折价状态记录
    折价状态记录是交易流水，必须永久保存
    Returns:
        bool: 始终返回True（不再执行清理操作）
    """
    logger.info("跳过折价状态记录清理 - 交易流水必须永久保存")
    return True
def clear_expired_premium_status() -> bool:
    """
    【已修复】不再清理溢价状态记录
    溢价状态记录是交易流水，必须永久保存
    Returns:
        bool: 始终返回True（不再执行清理操作）
    """
    logger.info("跳过溢价状态记录清理 - 交易流水必须永久保存")
    return True
def get_arbitrage_push_count() -> Dict[str, int]:
    """
    获取套利推送统计
    Returns:
        Dict[str, int]: 套利推送统计
    """
    try:
        # 获取当前日期
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        # 加载套利状态
        status = load_arbitrage_status()
        # 计算总推送量和今日推送量
        total_pushed = len(status)
        today_pushed = sum(1 for s in status.values() if s.get("last_pushed") == today)
        return {
            "total": total_pushed,
            "today": today_pushed
        }
    except Exception as e:
        logger.error(f"获取套利推送统计失败: {str(e)}", exc_info=True)
        return {
            "total": 0,
            "today": 0
        }
def get_discount_push_count() -> Dict[str, int]:
    """
    获取折价推送统计
    Returns:
        Dict[str, int]: 折价推送统计
    """
    try:
        # 获取当前日期
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        # 加载折价状态
        status = load_discount_status()
        # 计算总推送量和今日推送量
        total_pushed = len(status)
        today_pushed = sum(1 for s in status.values() if s.get("last_pushed") == today)
        return {
            "total": total_pushed,
            "today": today_pushed
        }
    except Exception as e:
        logger.error(f"获取折价推送统计失败: {str(e)}", exc_info=True)
        return {
            "total": 0,
            "today": 0
        }
def get_premium_push_count() -> Dict[str, int]:
    """
    获取溢价推送统计
    Returns:
        Dict[str, int]: 溢价推送统计
    """
    try:
        # 获取当前日期
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        # 加载溢价状态
        status = load_premium_status()
        # 计算总推送量和今日推送量
        total_pushed = len(status)
        today_pushed = sum(1 for s in status.values() if s.get("last_pushed") == today)
        return {
            "total": total_pushed,
            "today": today_pushed
        }
    except Exception as e:
        logger.error(f"获取溢价推送统计失败: {str(e)}", exc_info=True)
        return {
            "total": 0,
            "today": 0
        }
def get_arbitrage_push_history(days: int = 7) -> Dict[str, int]:
    """
    获取套利推送历史记录
    Args:
        days: 查询天数
    Returns:
        Dict[str, int]: 套利推送历史记录
    """
    try:
        # 获取当前日期
        from utils.date_utils import get_beijing_time
        today = get_beijing_time()
        # 加载套利状态
        status = load_arbitrage_status()
        # 初始化历史记录
        history = {}
        # 按日期统计推送量
        for i in range(days):
            date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            history[date] = 0
            for s in status.values():
                if s.get("last_pushed") == date:
                    history[date] += 1
        return history
    except Exception as e:
        logger.error(f"获取套利推送历史记录失败: {str(e)}", exc_info=True)
        return {}
def get_discount_push_history(days: int = 7) -> Dict[str, int]:
    """
    获取折价推送历史记录
    Args:
        days: 查询天数
    Returns:
        Dict[str, int]: 折价推送历史记录
    """
    try:
        # 获取当前日期
        from utils.date_utils import get_beijing_time
        today = get_beijing_time()
        # 加载折价状态
        status = load_discount_status()
        # 初始化历史记录
        history = {}
        # 按日期统计推送量
        for i in range(days):
            date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            history[date] = 0
            for s in status.values():
                if s.get("last_pushed") == date:
                    history[date] += 1
        return history
    except Exception as e:
        logger.error(f"获取折价推送历史记录失败: {str(e)}", exc_info=True)
        return {}
def get_premium_push_history(days: int = 7) -> Dict[str, int]:
    """
    获取溢价推送历史记录
    Args:
        days: 查询天数
    Returns:
        Dict[str, int]: 溢价推送历史记录
    """
    try:
        # 获取当前日期
        from utils.date_utils import get_beijing_time
        today = get_beijing_time()
        # 加载溢价状态
        status = load_premium_status()
        # 初始化历史记录
        history = {}
        # 按日期统计推送量
        for i in range(days):
            date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            history[date] = 0
            for s in status.values():
                if s.get("last_pushed") == date:
                    history[date] += 1
        return history
    except Exception as e:
        logger.error(f"获取溢价推送历史记录失败: {str(e)}", exc_info=True)
        return {}
def load_etf_metadata() -> pd.DataFrame:
    """
    加载ETF元数据
    Returns:
        pd.DataFrame: ETF元数据
    """
    try:
        # 检查元数据文件是否存在
        if not os.path.exists(Config.METADATA_PATH):
            logger.warning("ETF元数据文件不存在，将尝试创建")
            # 尝试创建基础元数据
            create_base_etf_metadata()
            if not os.path.exists(Config.METADATA_PATH):
                logger.error("ETF元数据文件创建失败")
                return pd.DataFrame()
        # 加载元数据
        df = pd.read_csv(Config.METADATA_PATH)
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        # 确保使用中文列名
        df = ensure_chinese_columns(df)
        # 确保包含必要列
        required_columns = ["ETF代码", "ETF名称", "基金规模"]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"ETF元数据缺少必要列: {col}")
                return pd.DataFrame()
        # 修复：先确保ETF代码列是字符串
        # 先转换为字符串，再进行格式化
        df["ETF代码"] = df["ETF代码"].astype(str)
        df["ETF代码"] = df["ETF代码"].str.strip().str.zfill(6)
        return df
    except Exception as e:
        logger.error(f"加载ETF元数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()
def create_base_etf_metadata() -> None:
    """
    创建基础ETF元数据文件
    """
    try:
        # 确保目录存在
        metadata_dir = Path(Config.METADATA_PATH).parent
        if not metadata_dir.exists():
            os.makedirs(metadata_dir, exist_ok=True)
        # 获取ETF列表
        etf_list = load_all_etf_list()
        if etf_list.empty:
            logger.warning("ETF列表为空，无法创建基础元数据")
            return
        # 创建元数据列表
        metadata_list = []
        for _, etf in etf_list.iterrows():
            # 修复：确保ETF代码是字符串
            etf_code = str(etf.get("ETF代码", "")).strip().zfill(6)
            metadata_list.append({
                "ETF代码": etf_code,
                "ETF名称": etf.get("ETF名称", ""),
                "基金规模": etf.get("基金规模", 0.0),
                "update_time": get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
            })
        # 创建DataFrame
        df = pd.DataFrame(metadata_list)
        # 保存到CSV文件
        df.to_csv(Config.METADATA_PATH, index=False, encoding="utf-8-sig")
        logger.info(f"成功创建基础ETF元数据文件: {Config.METADATA_PATH} (共{len(df)}条记录)")
    except Exception as e:
        logger.error(f"创建基础ETF元数据失败: {str(e)}", exc_info=True)
def load_all_etf_list() -> pd.DataFrame:
    """
    加载全市场ETF列表
    Returns:
        pd.DataFrame: ETF列表
    """
    try:
        # 检查ETF列表文件是否存在
        if not os.path.exists(Config.ALL_ETFS_PATH):
            logger.error("ETF列表文件不存在")
            return pd.DataFrame()
        # 检查ETF列表是否过期
        if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
            logger.warning("ETF列表已过期，可能影响评分准确性")
        # 读取ETF列表
        etf_list = pd.read_csv(Config.ALL_ETFS_PATH, encoding="utf-8")
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        etf_list = etf_list.copy(deep=True)
        # 确保使用中文列名
        etf_list = ensure_chinese_columns(etf_list)
        # 确保包含必要列
        required_columns = ["ETF代码", "ETF名称", "基金规模"]
        for col in required_columns:
            if col not in etf_list.columns:
                logger.error(f"ETF列表缺少必要列: {col}")
                return pd.DataFrame()
        # 修复：先确保ETF代码列是字符串
        # 先转换为字符串，再进行格式化
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str)
        etf_list["ETF代码"] = etf_list["ETF代码"].str.strip().str.zfill(6)
        return etf_list
    except Exception as e:
        logger.error(f"加载ETF列表失败: {str(e)}", exc_info=True)
        return pd.DataFrame()
def is_file_outdated(file_path: str, days: int) -> bool:
    """
    检查文件是否过期
    Args:
        file_path: 文件路径
        days: 有效期（天）
    Returns:
        bool: 文件是否过期
    """
    try:
        # 检查文件是否存在
        if not os.path.exists(file_path):
            return True
        # 获取文件最后修改时间
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
        # 将文件修改时间转换为时区感知对象（使用北京时间时区）
        file_mtime = file_mtime.replace(tzinfo=Config.BEIJING_TIMEZONE)
        # 获取当前时间
        from utils.date_utils import get_beijing_time
        current_time = get_beijing_time()
        # 检查是否过期
        time_diff = current_time - file_mtime
        is_outdated = time_diff.days > days
        if is_outdated:
            logger.info(f"文件 {file_path} 已过期（{time_diff.days}天 > {days}天）")
        else:
            logger.debug(f"文件 {file_path} 未过期（{time_diff.days}天 ≤ {days}天）")
        return is_outdated
    except Exception as e:
        logger.error(f"检查文件是否过期失败: {str(e)}", exc_info=True)
        # 出错时视为过期，触发更新
        return True
def record_failed_etf(etf_daily_dir: str, etf_code: str, etf_name: str) -> None:
    """
    记录失败的ETF
    Args:
        etf_daily_dir: ETF日线数据目录
        etf_code: ETF代码
        etf_name: ETF名称
    """
    try:
        # 创建失败记录文件路径
        failed_file = os.path.join(etf_daily_dir, "failed_etfs.csv")
        # 获取当前时间
        from utils.date_utils import get_beijing_time
        current_time = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        # 检查文件是否存在
        file_exists = os.path.exists(failed_file)
        # 写入失败记录
        with open(failed_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # 如果文件不存在，先写入标题
            if not file_exists:
                writer.writerow(["ETF代码", "ETF名称", "失败时间", "重试次数"])
            # 写入失败记录
            writer.writerow([etf_code, etf_name, current_time, 1])
        logger.debug(f"已记录失败ETF: {etf_code} - {etf_name}")
    except Exception as e:
        logger.error(f"记录失败ETF失败: {str(e)}", exc_info=True)
def clean_old_arbitrage_data(days_to_keep: int = 7) -> None:
    """
    清理旧的套利数据文件
    Args:
        days_to_keep: 保留天数
    """
    try:
        # 构建套利数据目录
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        # 检查目录是否存在
        if not os.path.exists(arbitrage_dir):
            return
        # 获取当前时间
        from utils.date_utils import get_beijing_time
        current_time = get_beijing_time()
        # 遍历目录中的文件
        for filename in os.listdir(arbitrage_dir):
            if filename.endswith(".csv"):
                file_path = os.path.join(arbitrage_dir, filename)
                # 获取文件最后修改时间
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                # 确保时区信息
                file_mtime = file_mtime.replace(tzinfo=Config.BEIJING_TIMEZONE)
                # 检查是否过期
                if (current_time - file_mtime).days > days_to_keep:
                    # 删除过期文件
                    os.remove(file_path)
                    logger.info(f"已删除过期套利数据文件: {filename}")
    except Exception as e:
        logger.error(f"清理旧套利数据文件失败: {str(e)}", exc_info=True)
def get_etf_score_history(etf_code: str, days: int = 30) -> pd.DataFrame:
    """
    获取ETF评分历史数据
    Args:
        etf_code: ETF代码
        days: 查询天数
    Returns:
        pd.DataFrame: 评分历史数据
    """
    try:
        history = []
        from utils.date_utils import get_beijing_time
        beijing_now = get_beijing_time()
        for i in range(days):
            # 确保日期是datetime类型
            date_obj = beijing_now - timedelta(days=i)
            date_str = date_obj.strftime("%Y-%m-%d")
            score_file = os.path.join(Config.SCORE_HISTORY_DIR, f"{etf_code}_{date_str}.json")
            if os.path.exists(score_file):
                try:
                    with open(score_file, 'r') as f:
                        score_data = json.load(f)
                        history.append({
                            "日期": date_str,
                            "评分": score_data.get("score", 0.0),
                            "排名": score_data.get("rank", 0)
                        })
                except Exception as e:
                    logger.error(f"读取评分历史文件 {score_file} 失败: {str(e)}")
        if not history:
            logger.info(f"未找到ETF {etf_code} 的评分历史数据")
            return pd.DataFrame()
        return pd.DataFrame(history)
    except Exception as e:
        error_msg = f"获取ETF {etf_code} 评分历史数据失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return pd.DataFrame()
def save_etf_score_history(etf_code: str, score: float, rank: int) -> None:
    """
    保存ETF评分历史数据
    Args:
        etf_code: ETF代码
        score: 评分
        rank: 排名
    """
    try:
        # 确保评分历史目录存在
        if not os.path.exists(Config.SCORE_HISTORY_DIR):
            os.makedirs(Config.SCORE_HISTORY_DIR, exist_ok=True)
        # 获取当前日期
        from utils.date_utils import get_beijing_time
        date = get_beijing_time().strftime("%Y-%m-%d")
        # 构建文件路径
        score_file = os.path.join(Config.SCORE_HISTORY_DIR, f"{etf_code}_{date}.json")
        # 保存评分数据
        score_data = {
            "score": score,
            "rank": rank,
            "timestamp": get_beijing_time().isoformat()
        }
        with open(score_file, 'w', encoding='utf-8') as f:
            json.dump(score_data, f, ensure_ascii=False, indent=2)
        logger.debug(f"成功保存ETF {etf_code} 评分历史: {score_file}")
    except Exception as e:
        logger.error(f"保存ETF {etf_code} 评分历史失败: {str(e)}", exc_info=True)
# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    # 检查元数据文件是否存在
    if not os.path.exists(Config.METADATA_PATH):
        logger.warning("ETF元数据文件不存在，将在需要时重建")
    else:
        # 检查元数据是否需要更新
        if is_file_outdated(Config.METADATA_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
            logger.info("ETF元数据已过期，将在需要时重建")
    # 初始化日志
    logger.info("文件操作工具模块初始化完成")
except Exception as e:
    error_msg = f"文件操作工具模块初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
    try:
        # 退回到基础日志配置
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(error_msg)
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(error_msg)
