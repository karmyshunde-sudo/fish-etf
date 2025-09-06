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

# 配置日志
logger = logging.getLogger(__name__)

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

def is_trading_day(date_param: Optional[Union[datetime, date]] = None) -> bool:
    """
    判断指定日期是否为交易日（A股市场）
    
    Args:
        date_param: 要判断的日期，如果为None则使用当前北京时间
    
    Returns:
        bool: 如果是交易日返回True，否则返回False
    """
    try:
        from config import Config
        
        if date_param is None:
            # 直接使用config定义的北京时间
            date_param = get_beijing_time().date()
        elif isinstance(date_param, datetime):
            # 如果传入的是datetime，确保使用config定义的时区
            if date_param.tzinfo is None:
                date_param = date_param.replace(tzinfo=Config.UTC_TIMEZONE)
            date_param = date_param.astimezone(Config.BEIJING_TIMEZONE).date()
        elif isinstance(date_param, date):
            # 如果传入的是date，直接使用
            pass
        else:
            logger.error(f"无效的日期类型: {type(date_param)}")
            return False
        
        # 周末不是交易日
        if date_param.weekday() >= 5:  # 5=周六, 6=周日
            logger.debug(f"{date_param} 是周末，非交易日")
            return False
        
        logger.debug(f"{date_param} 是交易日")
        return True
    
    except Exception as e:
        logger.error(f"判断交易日失败: {str(e)}", exc_info=True)
        # 回退：简单判断（仅检查是否为周末）
        if date_param is None:
            date_param = datetime.now().date()
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
        current_time = datetime.now().time()
        
        # 将配置中的交易时间字符串转换为time对象
        trading_start = datetime.strptime(Config.TRADING_START_TIME, "%H:%M").time()
        trading_end = datetime.strptime(Config.TRADING_END_TIME, "%H:%M").time()
        
        # 检查是否在交易时间内
        return trading_start <= current_time <= trading_end
    
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
        # 直接比较日期部分
        return date1.date() == date2.date()
    except Exception as e:
        logger.error(f"比较日期失败: {str(e)}", exc_info=True)
        return False

def get_next_trading_day(date_param: Optional[Union[datetime, date]] = None) -> datetime:
    """
    获取下一个交易日
    
    Args:
        date_param: 起始日期，如果为None则使用当前日期
    
    Returns:
        datetime: 下一个交易日
    """
    try:
        from config import Config
        
        if date_param is None:
            date_param = get_beijing_time()
        
        # 转换为日期对象
        if isinstance(date_param, datetime):
            date_param = date_param.date()
        
        # 循环查找下一个交易日
        next_day = date_param + timedelta(days=1)
        while not is_trading_day(next_day):
            next_day += timedelta(days=1)
        
        logger.debug(f"下一个交易日: {date_param} -> {next_day}")
        return datetime.combine(next_day, datetime.min.time()).replace(tzinfo=Config.BEIJING_TIMEZONE)
    
    except Exception as e:
        logger.error(f"获取下一个交易日失败 {date_param}: {str(e)}", exc_info=True)
        # 回退：简单加一天
        return (date_param if date_param else get_beijing_time()) + timedelta(days=1)

def get_previous_trading_day(date_param: Optional[Union[datetime, date]] = None) -> datetime:
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
        
        # 转换为日期对象
        if isinstance(date_param, datetime):
            date_param = date_param.date()
        
        # 循环查找上一个交易日
        prev_day = date_param - timedelta(days=1)
        while not is_trading_day(prev_day):
            prev_day -= timedelta(days=1)
        
        logger.debug(f"上一个交易日: {date_param} -> {prev_day}")
        return datetime.combine(prev_day, datetime.min.time()).replace(tzinfo=Config.BEIJING_TIMEZONE)
    
    except Exception as e:
        logger.error(f"获取上一个交易日失败 {date_param}: {str(e)}", exc_info=True)
        # 回退：简单减一天
        return (date_param if date_param else get_beijing_time()) - timedelta(days=1)

def get_last_trading_day(date_param: Optional[Union[datetime, date]] = None) -> date:
    """
    获取最近一个交易日（包括今天，如果今天是交易日）
    
    Args:
        date_param: 日期，如果为None则使用当前日期
    
    Returns:
        date: 最近一个交易日
    """
    try:
        from config import Config
        
        if date_param is None:
            date_param = get_beijing_time().date()
        elif isinstance(date_param, datetime):
            date_param = date_param.date()
        
        # 如果是交易日，返回当天
        if is_trading_day(date_param):
            return date_param
        
        # 如果不是交易日，向前查找最近的交易日
        while not is_trading_day(date_param):
            date_param = date_param - timedelta(days=1)
        
        return date_param
    
    except Exception as e:
        logger.error(f"获取最近交易日失败 {date_param}: {str(e)}", exc_info=True)
        # 回退：返回昨天
        return (date_param if date_param else get_beijing_time().date()) - timedelta(days=1)

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
        
        # 使用config定义的时区
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
