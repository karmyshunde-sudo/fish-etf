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
from datetime import datetime, date, timedelta  # 修复：添加对date的导入
from typing import Optional, Tuple, List, Union, Any
import pandas as pd
from dateutil import parser, relativedelta
from pathlib import Path
import numpy as np
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday
from pandas.tseries.offsets import CustomBusinessDay

# 配置日志
logger = logging.getLogger(__name__)

# 交易日历定义
class ChinaStockHolidayCalendar(AbstractHolidayCalendar):
    """
    中国股市交易日历（基于中国法定节假日）
    """
    rules = [
        Holiday('New Year', month=1, day=1, observance=nearest_workday),
        Holiday('Spring Festival', month=1, day=1, observance=nearest_workday),
        Holiday('Qingming Festival', month=4, day=4, observance=nearest_workday),
        Holiday('Labor Day', month=5, day=1, observance=nearest_workday),
        Holiday('Dragon Boat Festival', month=6, day=1, observance=nearest_workday),
        Holiday('Mid-Autumn Festival', month=8, day=15, observance=nearest_workday),
        Holiday('National Day', month=10, day=1, observance=nearest_workday),
    ]

def get_current_times() -> Tuple[datetime, datetime]:
    """
    获取当前双时区时间（UTC和北京时间）
    
    Returns:
        Tuple[datetime, datetime]: (UTC时间, 北京时间)
    """
    try:
        # 直接使用config.py中定义的时区变量
        from config import Config
        
        utc_now = datetime.now(Config.UTC_TIMEZONE)
        beijing_now = datetime.now(Config.BEIJING_TIMEZONE)
        logger.debug(f"获取当前时间: UTC={utc_now}, 北京={beijing_now}")
        return utc_now, beijing_now
    except Exception as e:
        logger.error(f"获取当前时间失败: {str(e)}", exc_info=True)
        # 回退：直接使用config定义的时区
        from config import Config
        return datetime.now().replace(tzinfo=Config.UTC_TIMEZONE), datetime.now().replace(tzinfo=Config.BEIJING_TIMEZONE)

def get_beijing_time() -> datetime:
    """
    获取当前北京时间（带时区信息）
    
    Returns:
        datetime: 当前北京时间
    """
    try:
        # 直接使用config.py中定义的时区
        from config import Config
        return datetime.now(Config.BEIJING_TIMEZONE)
    except Exception as e:
        logger.error(f"获取北京时间失败: {str(e)}", exc_info=True)
        # 回退：直接使用config定义的时区
        from config import Config
        return datetime.now().replace(tzinfo=Config.BEIJING_TIMEZONE)

def get_utc_time() -> datetime:
    """
    获取当前UTC时间（带时区信息）
    
    Returns:
        datetime: 当前UTC时间
    """
    try:
        # 直接使用config.py中定义的时区
        from config import Config
        return datetime.now(Config.UTC_TIMEZONE)
    except Exception as e:
        logger.error(f"获取UTC时间失败: {str(e)}", exc_info=True)
        # 回退：直接使用config定义的时区
        from config import Config
        return datetime.utcnow().replace(tzinfo=Config.UTC_TIMEZONE)

def is_trading_day(date_param: Optional[Union[datetime, str]] = None) -> bool:
    """
    检查是否为交易日
    
    Args:
        date_param: 日期 (datetime对象或YYYY-MM-DD字符串)
    
    Returns:
        bool: 是否为交易日
    """
    try:
        # 【日期datetime类型规则】确保日期在内存中保持为datetime类型
        if date_param is None:
            date_obj = get_beijing_time()
        elif isinstance(date_param, str):
            # 直接解析为datetime对象，而不是date
            date_obj = datetime.strptime(date_param, "%Y-%m-%d")
            # 确保日期在内存中是datetime类型
            if date_obj.tzinfo is None:
                from config import Config
                date_obj = date_obj.replace(tzinfo=Config.BEIJING_TIMEZONE)
        else:
            date_obj = date_param
        
        # 检查是否为周末
        if date_obj.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            return False
        
        # 检查是否为法定节假日（这里简化处理，实际应查询节假日数据库）
        # 可以根据需要添加更多节假日检查逻辑
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        
        # 简单检查中国主要节假日
        if month == 1 and day == 1:  # 元旦
            return False
        if month == 10 and 1 <= day <= 7:  # 国庆节
            return False
        if month == 5 and 1 <= day <= 3:  # 劳动节
            return False
        
        # 特殊调休日处理（简化版）
        # 实际应用中应该有一个节假日数据库
        special_holidays = [
            # 2025年节假日（示例）
            "2025-01-01", "2025-01-28", "2025-01-29", "2025-01-30", 
            "2025-01-31", "2025-02-01", "2025-02-02", "2025-02-03",
            "2025-04-04", "2025-04-05", "2025-04-06",
            "2025-05-01", "2025-05-02", "2025-05-03",
            "2025-06-08", "2025-06-09", "2025-06-10",
            "2025-09-15", "2025-09-16", "2025-09-17",
            "2025-10-01", "2025-10-02", "2025-10-03", 
            "2025-10-04", "2025-10-05", "2025-10-06", "2025-10-07"
        ]
        
        if date_obj.strftime("%Y-%m-%d") in special_holidays:
            return False
        
        # 工作日调休为假期的情况（简化版）
        # 实际应用中应该有一个节假日数据库
        special_workdays = [
            # 2025年调休（示例）
            "2025-01-26", "2025-01-27", "2025-02-08",
            "2025-04-27", "2025-05-10",
            "2025-09-28", "2025-10-11"
        ]
        
        if date_obj.strftime("%Y-%m-%d") in special_workdays:
            return True
        
        return True
    
    except Exception as e:
        logger.error(f"检查交易日失败 {date_param}: {str(e)}", exc_info=True)
        # 出错时保守策略：假设是交易日
        return True
    
    except Exception as e:
        logger.error(f"判断交易日失败: {str(e)}", exc_info=True)
        # 回退：简单判断（仅检查是否为周末）
        if date_param is None:
            date_param = datetime.now()
        return date_param.weekday() < 5

def is_trading_time() -> bool:
    """
    检查当前是否在交易时间（不依赖其他可能引起循环导入的模块）
    
    Returns:
        bool: 是否在交易时间
    """
    try:
        from datetime import datetime
        from config import Config
        
        # 获取当前系统时间（假设系统时区已设置为北京时间）
        current_time = datetime.now()
        
        # 将配置中的交易时间字符串转换为time对象
        trading_start = datetime.strptime(Config.TRADING_START_TIME, "%H:%M").time()
        trading_end = datetime.strptime(Config.TRADING_END_TIME, "%H:%M").time()
        
        # 检查是否在交易时间内
        return trading_start <= current_time.time() <= trading_end
    
    except Exception as e:
        logger.error(f"判断交易时间失败: {str(e)}", exc_info=True)
        # 回退：使用默认交易时间
        current_time = datetime.now().time()
        return datetime.strptime("09:30", "%H:%M").time() <= current_time <= datetime.strptime("15:00", "%H:%M").time()

def is_same_day(date1: datetime, date2: datetime) -> bool:
    """
    判断两个时间是否是同一天
    
    Args:
        date1: 第一个日期时间
        date2: 第二个日期时间
    
    Returns:
        bool: 如果是同一天返回True，否则返回False
    """
    try:
        # 【日期datetime类型规则】确保比较时使用datetime对象
        return date1.date() == date2.date()
    except Exception as e:
        logger.error(f"比较日期失败: {str(e)}", exc_info=True)
        return False

def get_next_trading_day(current_date: datetime) -> datetime:
    """
    获取下一个交易日
    
    Args:
        current_date: 当前日期时间
    
    Returns:
        datetime: 下一个交易日（作为datetime对象）
    """
    try:
        # 【日期datetime类型规则】确保输入是datetime类型
        if not isinstance(current_date, datetime):
            if isinstance(current_date, date):
                # 将date转换为datetime
                current_date = datetime.combine(current_date, datetime.min.time())
            else:
                current_date = datetime.now()
        
        # 统一处理时区
        if current_date.tzinfo is None:
            from config import Config
            current_date = current_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 获取下一个日期（作为datetime对象）
        next_day = current_date + timedelta(days=1)
        # 确保是00:00:00
        next_day = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
        
        while not is_trading_day(next_day):
            next_day += timedelta(days=1)
            # 防止无限循环
            if (next_day - current_date).days > 30:
                logger.warning(f"在30天内找不到交易日，使用 {next_day} 作为下一个交易日")
                break
        
        # 确保返回的日期是datetime类型
        return next_day
    
    except Exception as e:
        logger.error(f"获取下一个交易日失败: {str(e)}", exc_info=True)
        # 出错时返回明天
        return current_date + timedelta(days=1)

def get_previous_trading_day(date_param: Optional[datetime] = None) -> datetime:
    """
    获取上一个交易日
    
    Args:
        date_param: 起始日期，如果为None则使用当前日期
    
    Returns:
        datetime: 上一个交易日
    """
    try:
        from config import Config
        
        if date_param is None:
            date_param = get_beijing_time()
        
        # 【日期datetime类型规则】确保是datetime对象
        if not isinstance(date_param, datetime):
            if isinstance(date_param, date):
                date_param = datetime.combine(date_param, datetime.min.time())
            else:
                date_param = get_beijing_time()
        
        # 确保时区信息
        if date_param.tzinfo is None:
            date_param = date_param.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 循环查找上一个交易日
        prev_day = date_param - timedelta(days=1)
        # 确保是00:00:00
        prev_day = prev_day.replace(hour=0, minute=0, second=0, microsecond=0)
        
        while not is_trading_day(prev_day):
            prev_day -= timedelta(days=1)
            # 确保是00:00:00
            prev_day = prev_day.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.debug(f"上一个交易日: {date_param} -> {prev_day}")
        return prev_day
    
    except Exception as e:
        logger.error(f"获取上一个交易日失败 {date_param}: {str(e)}", exc_info=True)
        # 回退：简单减一天
        return (date_param if date_param else get_beijing_time()) - timedelta(days=1)

def get_last_trading_day(date_param: Optional[datetime] = None) -> datetime:
    """
    获取最近一个交易日（包括今天，如果今天是交易日）
    
    Args:
        date_param: 日期，如果为None则使用当前日期
    
    Returns:
        datetime: 最近一个交易日
    """
    try:
        from config import Config
        
        if date_param is None:
            date_param = get_beijing_time()
        elif not isinstance(date_param, datetime):
            if isinstance(date_param, date):
                date_param = datetime.combine(date_param, datetime.min.time())
            else:
                date_param = get_beijing_time()
        
        # 【日期datetime类型规则】确保是datetime对象
        if date_param.tzinfo is None:
            date_param = date_param.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 如果是交易日，返回当天
        if is_trading_day(date_param):
            # 确保是00:00:00
            return date_param.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # 如果不是交易日，向前查找最近的交易日
        prev_day = date_param.replace(hour=0, minute=0, second=0, microsecond=0)
        while not is_trading_day(prev_day):
            prev_day -= timedelta(days=1)
            # 确保是00:00:00
            prev_day = prev_day.replace(hour=0, minute=0, second=0, microsecond=0)
        
        return prev_day
    
    except Exception as e:
        logger.error(f"获取最近交易日失败 {date_param}: {str(e)}", exc_info=True)
        # 回退：返回昨天
        return (date_param if date_param else get_beijing_time()) - timedelta(days=1)

def is_file_outdated(file_path: str, max_age_days: int) -> bool:
    """
    判断文件是否过期
    
    Args:
        file_path: 文件路径
        max_age_days: 最大年龄（天）
    
    Returns:
        bool: 文件是否过期
    """
    try:
        from config import Config
        
        if not os.path.exists(file_path):
            logger.info(f"文件不存在: {file_path}")
            return True
        
        # 获取文件最后修改时间戳
        timestamp = os.path.getmtime(file_path)
        
        # 使用config定义的时区
        # 【日期datetime类型规则】确保时间是datetime类型
        utc_time = datetime.fromtimestamp(timestamp, tz=Config.UTC_TIMEZONE)
        beijing_time = datetime.fromtimestamp(timestamp, tz=Config.BEIJING_TIMEZONE)
        
        # 当前北京时间
        current_beijing_time = get_beijing_time()
        
        # 计算文件年龄（天）
        file_age = (current_beijing_time - beijing_time).days
        
        logger.debug(f"文件 {file_path} 年龄: {file_age} 天 (最大允许年龄: {max_age_days} 天)")
        
        return file_age > max_age_days
    
    except Exception as e:
        logger.error(f"检查文件过期状态失败 {file_path}: {str(e)}", exc_info=True)
        # 出错时保守策略：认为文件未过期
        return False

def get_file_mtime(file_path: str) -> Tuple[datetime, datetime]:
    """
    获取文件最后修改时间（UTC和北京时间）
    
    Args:
        file_path: 文件路径
    
    Returns:
        Tuple[datetime, datetime]: (UTC时间, 北京时间)
    """
    try:
        from config import Config
        
        if not os.path.exists(file_path):
            return get_utc_time(), get_beijing_time()
        
        # 获取文件最后修改时间戳
        timestamp = os.path.getmtime(file_path)
        
        # 【日期datetime类型规则】确保时间是datetime类型
        utc_time = datetime.fromtimestamp(timestamp, tz=Config.UTC_TIMEZONE)
        beijing_time = datetime.fromtimestamp(timestamp, tz=Config.BEIJING_TIMEZONE)
        
        logger.debug(f"获取文件修改时间: {file_path} -> UTC: {utc_time}, CST: {beijing_time}")
        return utc_time, beijing_time
    
    except Exception as e:
        logger.error(f"获取文件修改时间失败 {file_path}: {str(e)}", exc_info=True)
        return get_utc_time(), get_beijing_time()

def format_time_for_display(dt: datetime) -> str:
    """
    格式化时间用于显示
    
    Args:
        dt: datetime对象
    
    Returns:
        str: 格式化后的时间字符串
    """
    try:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(f"格式化时间失败: {str(e)}", exc_info=True)
        return str(dt)

def is_market_open(time_to_check: Optional[datetime] = None) -> bool:
    """
    判断指定时间是否在市场开市时间内
    
    Args:
        time_to_check: 要检查的时间，默认为当前时间
    
    Returns:
        bool: 如果在开市时间内返回True，否则返回False
    """
    try:
        from config import Config
        
        if time_to_check is None:
            time_to_check = get_beijing_time()
        
        # 【日期datetime类型规则】确保是datetime对象
        if not isinstance(time_to_check, datetime):
            time_to_check = get_beijing_time()
        
        # 将时间转换为北京时间
        if time_to_check.tzinfo is None:
            time_to_check = time_to_check.replace(tzinfo=Config.UTC_TIMEZONE)
        beijing_time = time_to_check.astimezone(Config.BEIJING_TIMEZONE)
        
        # 获取市场开市和闭市时间
        market_open = datetime.strptime(f"{Config.MARKET_OPEN_TIME[0]}:{Config.MARKET_OPEN_TIME[1]}", "%H:%M").time()
        market_close = datetime.strptime(f"{Config.MARKET_CLOSE_TIME[0]}:{Config.MARKET_CLOSE_TIME[1]}", "%H:%M").time()
        
        # 检查是否在交易时间内
        current_time = beijing_time.time()
        is_open = market_open <= current_time <= market_close
        
        logger.debug(f"市场状态检查: {time_to_check} -> {'开市' if is_open else '闭市'}")
        return is_open
    
    except Exception as e:
        logger.error(f"判断市场状态失败 {time_to_check}: {str(e)}", exc_info=True)
        return False

# 模块初始化
try:
    # 确保必要的目录存在
    from config import Config
    Config.init_dirs()
    
    # 验证时区设置
    utc_now, beijing_now = get_current_times()
    
    # 验证时区设置
    if beijing_now.tzinfo is None or utc_now.tzinfo is None:
        logging.warning("时区信息不完整，可能存在时区问题")
    else:
        logging.info(f"北京时间时区: {beijing_now.tzname()}")
        logging.info(f"UTC时间时区: {utc_now.tzname()}")
    
    # 简化验证：直接检查时区偏移
    beijing_offset = beijing_now.utcoffset().total_seconds() / 3600
    utc_offset = utc_now.utcoffset().total_seconds() / 3600
    time_diff = beijing_offset - utc_offset
    
    if abs(time_diff - 8) > 0.01:  # 允许0.01小时的误差
        logging.warning(f"时区偏移不正确: 北京时间比UTC时间快 {time_diff:.2f} 小时")
    else:
        logging.info("时区设置验证通过")
    
    # 初始化日志
    logger.info("日期时间工具模块初始化完成")
    
except ImportError:
    logging.warning("无法导入date_utils模块，时区检查跳过")
except Exception as e:
    logger.error(f"日期时间工具模块初始化失败: {str(e)}", exc_info=True)
    
    try:
        # 退回到基础日志配置
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(f"日期时间工具模块初始化失败: {str(e)}")
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(f"日期时间工具模块初始化失败: {str(e)}")
