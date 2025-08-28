#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日期时间工具模块
提供时间处理、时区转换、日期计算等常用功能
特别优化了与ETF数据爬取和策略计算相关的日期处理
"""

import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Union
import pytz
import pandas as pd
from dateutil import parser, relativedelta

# 配置日志
logger = logging.getLogger(__name__)

# 常量定义
BEIJING_TIMEZONE = pytz.timezone('Asia/Shanghai')
UTC_TIMEZONE = pytz.utc
MARKET_OPEN_TIME = (9, 30)  # 市场开盘时间 (小时, 分钟)
MARKET_CLOSE_TIME = (15, 0)  # 市场收盘时间 (小时, 分钟)

def get_beijing_time() -> datetime:
    """
    获取当前北京时间（带时区信息）
    
    Returns:
        datetime: 当前北京时间
    """
    try:
        utc_now = datetime.now(timezone.utc)
        beijing_time = utc_now.astimezone(BEIJING_TIMEZONE)
        logger.debug(f"获取北京时间: {beijing_time}")
        return beijing_time
    except Exception as e:
        logger.error(f"获取北京时间失败: {str(e)}")
        # 回退到本地时间（假设服务器在北京时区）
        return datetime.now()

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
        logger.error(f"获取UTC时间失败: {str(e)}")
        return datetime.utcnow()

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
            dt = pytz.utc.localize(dt)
        
        # 转换为北京时间
        beijing_time = dt.astimezone(BEIJING_TIMEZONE)
        logger.debug(f"时间转换为北京时间: {dt} -> {beijing_time}")
        return beijing_time
        
    except Exception as e:
        logger.error(f"时间转换失败 {dt}: {str(e)}")
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
            dt = BEIJING_TIMEZONE.localize(dt)
        
        # 转换为UTC时间
        utc_time = dt.astimezone(UTC_TIMEZONE)
        logger.debug(f"时间转换为UTC时间: {dt} -> {utc_time}")
        return utc_time
        
    except Exception as e:
        logger.error(f"时间转换失败 {dt}: {str(e)}")
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
        logger.error(f"判断交易日失败 {date}: {str(e)}")
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
        logger.error(f"判断市场状态失败 {time_to_check}: {str(e)}")
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
        logger.error(f"日期格式化失败 {dt}: {str(e)}")
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
        logger.error(f"解析日期字符串失败 {date_str}: {str(e)}")
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
        return datetime.combine(next_day, datetime.min.time())
        
    except Exception as e:
        logger.error(f"获取下一个交易日失败 {date}: {str(e)}")
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
        return datetime.combine(prev_day, datetime.min.time())
        
    except Exception as e:
        logger.error(f"获取上一个交易日失败 {date}: {str(e)}")
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
        logger.error(f"日期比较失败 {dt1} vs {dt2}: {str(e)}")
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
        logger.error(f"获取交易日范围失败 {start_date} 到 {end_date}: {str(e)}")
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
        
        start_date = end_date - timedelta(days=days)
        
        logger.debug(f"获取日期范围: {days}天 -> {start_date} 到 {end_date}")
        return start_date, end_date
        
    except Exception as e:
        logger.error(f"获取日期范围失败 {days}天: {str(e)}")
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
        logger.error(f"计算CRON下一次运行时间失败 {cron_expression}: {str(e)}")
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
        logger.error(f"休眠直到指定时间失败 {target_time}: {str(e)}")

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
        logger.error(f"日期范围验证失败 {start_date} 到 {end_date}: {str(e)}")
        return False
