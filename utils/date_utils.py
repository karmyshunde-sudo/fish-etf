#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日期时间工具模块
提供时间处理、时区转换、日期计算等常用功能
特别优化了与ETF数据爬取和策略计算相关的日期处理
"""

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Union, Any
import pytz
import pandas as pd
from dateutil import parser, relativedelta
from pathlib import Path

# 配置日志
logger = logging.getLogger(__name__)

# 常量定义
BEIJING_TIMEZONE = pytz.timezone('Asia/Shanghai')
UTC_TIMEZONE = pytz.utc
MARKET_OPEN_TIME = (9, 30)  # 市场开盘时间 (小时, 分钟)
MARKET_CLOSE_TIME = (15, 0)  # 市场收盘时间 (小时, 分钟)

def get_current_times() -> Tuple[datetime, datetime]:
    """
    获取当前双时区时间（UTC和北京时间）
    
    Returns:
        Tuple[datetime, datetime]: (UTC时间, 北京时间)
    """
    try:
        # 获取UTC时间
        utc_now = datetime.now(timezone.utc)
        
        # 转换为北京时间
        beijing_now = utc_now.astimezone(BEIJING_TIMEZONE)
        
        logger.debug(f"获取当前时间: UTC={utc_now}, 北京={beijing_now}")
        return utc_now, beijing_now
    except Exception as e:
        logger.error(f"获取当前时间失败: {str(e)}", exc_info=True)
        # 回退机制
        now = datetime.now()
        return now.replace(tzinfo=UTC_TIMEZONE), now.replace(tzinfo=BEIJING_TIMEZONE)

def format_dual_time(dt: datetime, source_tz: str = 'UTC', target_tz: str = 'Asia/Shanghai') -> str:
    """
    格式化双时区时间字符串
    :param dt: 原始时间（假设为source_tz时区）
    :return: "YYYY-MM-DD HH:MM:SS (UTC) / YYYY-MM-DD HH:MM:SS (CST)"
    """
    try:
        # 确保时间有时区信息
        if dt.tzinfo is None:
            source_tzinfo = pytz.timezone(source_tz)
            dt = source_tzinfo.localize(dt)
        
        # 转换为目标时区
        target_tzinfo = pytz.timezone(target_tz)
        target_time = dt.astimezone(target_tzinfo)
        
        return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} ({source_tz}) / {target_time.strftime('%Y-%m-%d %H:%M:%S')} ({target_tz})"
    except Exception as e:
        logger.error(f"时间格式化失败: {str(e)}", exc_info=True)
        return str(dt)

def get_beijing_time() -> datetime:
    """
    获取当前北京时间（带时区信息）
    
    Returns:
        datetime: 当前北京时间
    """
    try:
        # 从UTC时间转换为北京时间
        utc_now = datetime.now(timezone.utc)
        beijing_time = utc_now.astimezone(BEIJING_TIMEZONE)
        logger.debug(f"获取北京时间: {beijing_time}")
        return beijing_time
    except Exception as e:
        logger.error(f"获取北京时间失败: {str(e)}", exc_info=True)
        # 回退到本地时间（假设服务器在北京时区）
        return datetime.now().replace(tzinfo=BEIJING_TIMEZONE)

def get_utc_time() -> datetime:
    """
    获取当前UTC时间（带时区信息）
    
    Returns:
        datetime: 当前UTC时间
    """
    try:
        utc_time = datetime.now(timezone.utc)
        logger.debug(f"获取UTC时间: {utc_time}")
        return utc_time
    except Exception as e:
        logger.error(f"获取UTC时间失败: {str(e)}", exc_info=True)
        return datetime.utcnow().replace(tzinfo=timezone.utc)

def convert_to_beijing_time(dt: Union[datetime, str]) -> Optional[datetime]:
    """
    将时间转换为北京时间
    
    Args:
        dt: 要转换的时间（datetime对象或字符串）
        
    Returns:
        Optional[datetime]: 转换后的北京时间，失败返回None
    """
    try:
        # 如果是字符串，先解析
        if isinstance(dt, str):
            dt = parser.parse(dt)
        
        # 如果时间没有时区信息，假设为UTC时间
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        # 转换为北京时间
        beijing_time = dt.astimezone(BEIJING_TIMEZONE)
        logger.debug(f"时间转换为北京时间: {dt} -> {beijing_time}")
        return beijing_time
        
    except Exception as e:
        logger.error(f"时间转换失败 {dt}: {str(e)}", exc_info=True)
        return None

def convert_to_utc_time(dt: Union[datetime, str]) -> Optional[datetime]:
    """
    将时间转换为UTC时间
    
    Args:
        dt: 要转换的时间（datetime对象或字符串）
        
    Returns:
        Optional[datetime]: 转换后的UTC时间，失败返回None
    """
    try:
        # 如果是字符串，先解析
        if isinstance(dt, str):
            dt = parser.parse(dt)
        
        # 如果时间没有时区信息，假设为北京时间
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BEIJING_TIMEZONE)
        
        # 转换为UTC时间
        utc_time = dt.astimezone(UTC_TIMEZONE)
        logger.debug(f"时间转换为UTC时间: {dt} -> {utc_time}")
        return utc_time
        
    except Exception as e:
        logger.error(f"时间转换失败 {dt}: {str(e)}", exc_info=True)
        return None

def is_trading_day(date: Optional[datetime] = None) -> bool:
    """
    判断指定日期是否为交易日（A股市场）
    简化版实现，实际应用中应接入交易所日历API
    
    Args:
        date: 要判断的日期，如果为None则使用当前日期
        
    Returns:
        bool: 如果是交易日返回True，否则返回False
    """
    try:
        if date is None:
            date = get_beijing_time()
        
        # 转换为日期对象
        if isinstance(date, datetime):
            date = date.date()
        
        # 周末不是交易日
        if date.weekday() >= 5:  # 5=周六, 6=周日
            logger.debug(f"{date} 是周末，非交易日")
            return False
        
        # 这里可以添加更多判断逻辑，如节假日等
        # 实际应用中应使用交易所官方日历或第三方API
        
        logger.debug(f"{date} 是交易日")
        return True
        
    except Exception as e:
        logger.error(f"判断交易日失败 {date}: {str(e)}", exc_info=True)
        return False

def is_market_open(time_to_check: Optional[datetime] = None) -> bool:
    """
    判断当前是否在交易时间内（A股市场）
    
    Args:
        time_to_check: 要检查的时间，如果为None则使用当前时间
        
    Returns:
        bool: 如果在交易时间内返回True，否则返回False
    """
    try:
        if time_to_check is None:
            time_to_check = get_beijing_time()
        
        # 检查是否为交易日
        if not is_trading_day(time_to_check):
            return False
        
        # 检查时间是否在交易时段内
        current_time = time_to_check.time()
        market_open = datetime.strptime(f"{MARKET_OPEN_TIME[0]}:{MARKET_OPEN_TIME[1]}", "%H:%M").time()
        market_close = datetime.strptime(f"{MARKET_CLOSE_TIME[0]}:{MARKET_CLOSE_TIME[1]}", "%H:%M").time()
        
        is_open = market_open <= current_time <= market_close
        logger.debug(f"市场状态检查: {time_to_check} -> {'开市' if is_open else '闭市'}")
        return is_open
        
    except Exception as e:
        logger.error(f"判断市场状态失败 {time_to_check}: {str(e)}", exc_info=True)
        return False

def format_date(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    格式化日期时间
    
    Args:
        dt: 要格式化的日期时间
        fmt: 格式字符串
        
    Returns:
        str: 格式化后的日期字符串
    """
    try:
        formatted = dt.strftime(fmt)
        return formatted
    except Exception as e:
        logger.error(f"日期格式化失败 {dt}: {str(e)}", exc_info=True)
        return str(dt)

def parse_date(date_str: str, fmt: Optional[str] = None) -> Optional[datetime]:
    """
    解析日期字符串
    
    Args:
        date_str: 日期字符串
        fmt: 格式字符串，如果为None则自动解析
        
    Returns:
        Optional[datetime]: 解析后的日期时间，失败返回None
    """
    try:
        if fmt:
            dt = datetime.strptime(date_str, fmt)
        else:
            dt = parser.parse(date_str)
        
        logger.debug(f"解析日期字符串: {date_str} -> {dt}")
        return dt
    except Exception as e:
        logger.error(f"解析日期字符串失败 {date_str}: {str(e)}", exc_info=True)
        return None

def get_next_trading_day(date: Optional[datetime] = None) -> datetime:
    """
    获取下一个交易日
    
    Args:
        date: 起始日期，如果为None则使用当前日期
        
    Returns:
        datetime: 下一个交易日
    """
    try:
        if date is None:
            date = get_beijing_time()
        
        # 转换为日期对象
        if isinstance(date, datetime):
            date = date.date()
        
        # 循环查找下一个交易日
        next_day = date + timedelta(days=1)
        while not is_trading_day(next_day):
            next_day += timedelta(days=1)
        
        logger.debug(f"下一个交易日: {date} -> {next_day}")
        return datetime.combine(next_day, datetime.min.time()).replace(tzinfo=BEIJING_TIMEZONE)
        
    except Exception as e:
        logger.error(f"获取下一个交易日失败 {date}: {str(e)}", exc_info=True)
        # 回退：简单加一天
        return (date if date else get_beijing_time()) + timedelta(days=1)

def get_previous_trading_day(date: Optional[datetime] = None) -> datetime:
    """
    获取上一个交易日
    
    Args:
        date: 起始日期，如果为None则使用当前日期
        
    Returns:
        datetime: 上一个交易日
    """
    try:
        if date is None:
            date = get_beijing_time()
        
        # 转换为日期对象
        if isinstance(date, datetime):
            date = date.date()
        
        # 循环查找上一个交易日
        prev_day = date - timedelta(days=1)
        while not is_trading_day(prev_day):
            prev_day -= timedelta(days=1)
        
        logger.debug(f"上一个交易日: {date} -> {prev_day}")
        return datetime.combine(prev_day, datetime.min.time()).replace(tzinfo=BEIJING_TIMEZONE)
        
    except Exception as e:
        logger.error(f"获取上一个交易日失败 {date}: {str(e)}", exc_info=True)
        # 回退：简单减一天
        return (date if date else get_beijing_time()) - timedelta(days=1)

def is_same_day(dt1: datetime, dt2: datetime) -> bool:
    """
    判断两个日期时间是否在同一天（考虑时区）
    
    Args:
        dt1: 第一个日期时间
        dt2: 第二个日期时间
        
    Returns:
        bool: 如果在同一天返回True，否则返回False
    """
    try:
        # 转换为同一时区（北京时间）进行比较
        dt1_beijing = convert_to_beijing_time(dt1)
        dt2_beijing = convert_to_beijing_time(dt2)
        
        if dt1_beijing is None or dt2_beijing is None:
            return False
        
        same_day = dt1_beijing.date() == dt2_beijing.date()
        logger.debug(f"日期比较: {dt1} 和 {dt2} -> {'同一天' if same_day else '不同天'}")
        return same_day
        
    except Exception as e:
        logger.error(f"日期比较失败 {dt1} vs {dt2}: {str(e)}", exc_info=True)
        return False

def get_trading_days(start_date: datetime, end_date: datetime) -> List[datetime]:
    """
    获取指定日期范围内的所有交易日
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        List[datetime]: 交易日列表
    """
    try:
        # 确保日期范围正确
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        
        # 确保日期带有时区信息
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=BEIJING_TIMEZONE)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=BEIJING_TIMEZONE)
        
        trading_days = []
        current_date = start_date
        
        # 遍历日期范围，收集所有交易日
        while current_date <= end_date:
            if is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        logger.debug(f"获取交易日范围: {start_date} 到 {end_date} -> 共{len(trading_days)}个交易日")
        return trading_days
        
    except Exception as e:
        logger.error(f"获取交易日范围失败 {start_date} 到 {end_date}: {str(e)}", exc_info=True)
        return []

def get_date_range(days: int, end_date: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """
    获取指定天数的日期范围
    
    Args:
        days: 天数
        end_date: 结束日期，如果为None则使用当前日期
        
    Returns:
        Tuple[datetime, datetime]: (开始日期, 结束日期)
    """
    try:
        if end_date is None:
            end_date = get_beijing_time()
        
        # 确保结束日期带有时区信息
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=BEIJING_TIMEZONE)
        
        start_date = end_date - timedelta(days=days)
        
        logger.debug(f"获取日期范围: {days}天 -> {start_date} 到 {end_date}")
        return start_date, end_date
        
    except Exception as e:
        logger.error(f"获取日期范围失败 {days}天: {str(e)}", exc_info=True)
        # 回退：默认返回最近30天
        default_end = get_beijing_time()
        default_start = default_end - timedelta(days=30)
        return default_start, default_end

def get_cron_next_run(cron_expression: str) -> Optional[datetime]:
    """
    获取cron表达式的下一次运行时间
    
    Args:
        cron_expression: cron表达式
        
    Returns:
        Optional[datetime]: 下一次运行时间，失败返回None
    """
    try:
        from croniter import croniter
        from datetime import datetime
        
        base_time = get_beijing_time()
        iter = croniter(cron_expression, base_time)
        next_run = iter.get_next(datetime)
        
        logger.debug(f"CRON下一次运行: {cron_expression} -> {next_run}")
        return next_run
        
    except ImportError:
        logger.warning("croniter模块未安装，无法计算CRON下一次运行时间")
        return None
    except Exception as e:
        logger.error(f"计算CRON下一次运行时间失败 {cron_expression}: {str(e)}", exc_info=True)
        return None

def get_timestamp() -> int:
    """
    获取当前时间戳（秒级）
    
    Returns:
        int: 当前时间戳
    """
    return int(time.time())

def get_millisecond_timestamp() -> int:
    """
    获取当前时间戳（毫秒级）
    
    Returns:
        int: 当前时间戳（毫秒）
    """
    return int(time.time() * 1000)

def sleep_until(target_time: datetime) -> None:
    """
    休眠直到指定时间
    
    Args:
        target_time: 目标时间
    """
    try:
        now = get_beijing_time()
        if target_time <= now:
            logger.warning(f"目标时间已过: {target_time}")
            return
        
        sleep_seconds = (target_time - now).total_seconds()
        logger.info(f"休眠 {sleep_seconds:.2f} 秒直到 {target_time}")
        time.sleep(sleep_seconds)
        
    except Exception as e:
        logger.error(f"休眠直到指定时间失败 {target_time}: {str(e)}", exc_info=True)

def validate_date_range(start_date: datetime, end_date: datetime) -> bool:
    """
    验证日期范围是否有效
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        bool: 如果日期范围有效返回True，否则返回False
    """
    try:
        # 确保日期带有时区信息
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=BEIJING_TIMEZONE)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=BEIJING_TIMEZONE)
        
        if start_date > end_date:
            logger.warning(f"日期范围无效: {start_date} > {end_date}")
            return False
        
        # 检查日期是否在未来（不允许未来日期）
        now = get_beijing_time()
        if start_date > now or end_date > now:
            logger.warning(f"日期范围包含未来日期: {start_date} 到 {end_date}")
            return False
        
        logger.debug(f"日期范围验证通过: {start_date} 到 {end_date}")
        return True
        
    except Exception as e:
        logger.error(f"日期范围验证失败 {start_date} 到 {end_date}: {str(e)}", exc_info=True)
        return False

def is_file_outdated(file_path: Union[str, Path], max_age_days: int) -> bool:
    """
    判断文件是否过期
    :param file_path: 文件路径
    :param max_age_days: 最大年龄（天）
    :return: 如果文件过期返回True，否则返回False
    """
    if not os.path.exists(file_path):
        logger.debug(f"文件不存在: {file_path}")
        return True
    
    try:
        # 获取文件最后修改时间（以UTC时间处理）
        timestamp = os.path.getmtime(file_path)
        # 正确处理：将文件修改时间视为UTC时间
        last_modify_time = datetime.fromtimestamp(timestamp, tz=UTC_TIMEZONE)
        
        # 转换为北京时间用于比较
        last_modify_time_beijing = last_modify_time.astimezone(BEIJING_TIMEZONE)
        
        # 获取当前北京时间
        current_time = get_beijing_time()
        
        # 计算距离上次更新的天数
        time_diff = current_time - last_modify_time_beijing
        days_since_update = time_diff.days
        
        need_update = days_since_update >= max_age_days
        
        if need_update:
            logger.info(f"文件已过期({days_since_update}天)，需要更新")
        else:
            logger.debug(f"文件未过期({days_since_update}天)，无需更新")
            
        return need_update
    except Exception as e:
        logger.error(f"检查文件更新状态失败: {str(e)}", exc_info=True)
        # 出错时保守策略是要求更新
        return True

def get_file_mtime(file_path: Union[str, Path]) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    获取文件修改时间（UTC与北京时间）
    :param file_path: 文件路径
    :return: (UTC时间, 北京时间)
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists() or not file_path.is_file():
            logger.warning(f"文件不存在或不是文件: {file_path}")
            return None, None
        
        # 获取文件修改时间（本地时间）
        timestamp = file_path.stat().st_mtime
        mtime = datetime.fromtimestamp(timestamp, tz=UTC_TIMEZONE)
        
        # 转换为北京时间
        beijing_time = mtime.astimezone(BEIJING_TIMEZONE)
        
        logger.debug(f"获取文件修改时间: {file_path} -> UTC: {mtime}, CST: {beijing_time}")
        return mtime, beijing_time
    except Exception as e:
        logger.error(f"获取文件修改时间失败: {str(e)}", exc_info=True)
        return None, None

def get_date_n_days_ago(n: int) -> datetime:
    """
    获取n天前的日期（北京时间）
    
    Args:
        n: 天数
        
    Returns:
        datetime: n天前的日期
    """
    try:
        current_time = get_beijing_time()
        date_n_days_ago = current_time - timedelta(days=n)
        logger.debug(f"获取{n}天前的日期: {date_n_days_ago}")
        return date_n_days_ago
    except Exception as e:
        logger.error(f"获取{n}天前的日期失败: {str(e)}", exc_info=True)
        return get_beijing_time()

def is_weekend(date: Optional[datetime] = None) -> bool:
    """
    判断是否为周末
    
    Args:
        date: 要判断的日期，如果为None则使用当前日期
        
    Returns:
        bool: 如果是周末返回True，否则返回False
    """
    try:
        if date is None:
            date = get_beijing_time()
        
        # 转换为日期对象
        if isinstance(date, datetime):
            date = date.date()
        
        # 周末是周六和周日
        if date.weekday() >= 5:  # 5=周六, 6=周日
            logger.debug(f"{date} 是周末")
            return True
        
        logger.debug(f"{date} 不是周末")
        return False
    except Exception as e:
        logger.error(f"判断是否为周末失败: {str(e)}", exc_info=True)
        return False

def get_business_days(start_date: datetime, end_date: datetime) -> List[datetime]:
    """
    获取指定日期范围内的所有工作日
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        List[datetime]: 工作日列表
    """
    try:
        # 确保日期范围正确
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        
        # 确保日期带有时区信息
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=BEIJING_TIMEZONE)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=BEIJING_TIMEZONE)
        
        business_days = []
        current_date = start_date
        
        # 遍历日期范围，收集所有工作日
        while current_date <= end_date:
            if not is_weekend(current_date):
                business_days.append(current_date)
            current_date += timedelta(days=1)
        
        logger.debug(f"获取工作日范围: {start_date} 到 {end_date} -> 共{len(business_days)}个工作日")
        return business_days
    except Exception as e:
        logger.error(f"获取工作日范围失败 {start_date} 到 {end_date}: {str(e)}", exc_info=True)
        return []

def validate_time_range(start_time: str, end_time: str) -> bool:
    """
    验证时间范围是否有效（24小时制）
    
    Args:
        start_time: 开始时间（格式：HH:MM）
        end_time: 结束时间（格式：HH:MM）
        
    Returns:
        bool: 如果时间范围有效返回True，否则返回False
    """
    try:
        import re
        # 检查格式
        if not re.match(r'^\d{2}:\d{2}$', start_time) or not re.match(r'^\d{2}:\d{2}$', end_time):
            logger.error(f"时间格式无效，应为 HH:MM: {start_time} 或 {end_time}")
            return False
        
        # 转换为时间对象
        start = datetime.strptime(start_time, "%H:%M").time()
        end = datetime.strptime(end_time, "%H:%M").time()
        
        # 检查顺序
        if start >= end:
            logger.error(f"开始时间 {start_time} 不能晚于或等于结束时间 {end_time}")
            return False
            
        return True
    except Exception as e:
        logger.error(f"时间范围验证失败 {start_time} 到 {end_time}: {str(e)}", exc_info=True)
        return False

def get_time_difference(dt1: datetime, dt2: datetime) -> timedelta:
    """
    计算两个时间点之间的差值（考虑时区）
    
    Args:
        dt1: 第一个时间点
        dt2: 第二个时间点
        
    Returns:
        timedelta: 两个时间点之间的差值
    """
    try:
        # 确保时间带有时区信息
        if dt1.tzinfo is None:
            dt1 = dt1.replace(tzinfo=BEIJING_TIMEZONE)
        if dt2.tzinfo is None:
            dt2 = dt2.replace(tzinfo=BEIJING_TIMEZONE)
        
        # 统一转换为UTC时间
        dt1_utc = dt1.astimezone(UTC_TIMEZONE)
        dt2_utc = dt2.astimezone(UTC_TIMEZONE)
        
        return dt2_utc - dt1_utc
    except Exception as e:
        logger.error(f"计算时间差失败 {dt1} 和 {dt2}: {str(e)}", exc_info=True)
        return timedelta()
