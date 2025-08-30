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
        logger.error(f"日期比较失败: {str(e)}", exc_info=True)
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
        # 简单方法：直接计算时间差（秒）
        current_timestamp = time.time()
        file_timestamp = os.path.getmtime(file_path)
        days_since_update = (current_timestamp - file_timestamp) / (24 * 3600)
        
        # 直接使用时间戳计算，不涉及时区转换
        need_update = days_since_update >= max_age_days
        
        if need_update:
            logger.info(f"文件已过期({days_since_update:.1f}天)，需要更新")
        else:
            logger.debug(f"文件未过期({days_since_update:.1f}天)，无需更新")
            
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
        from config import Config
        file_path = Path(file_path)
        if not file_path.exists() or not file_path.is_file():
            logger.warning(f"文件不存在或不是文件: {file_path}")
            return None, None
        
        # 获取文件修改时间戳
        timestamp = file_path.stat().st_mtime
        
        # 使用config定义的时区
        utc_time = datetime.fromtimestamp(timestamp, tz=Config.UTC_TIMEZONE)
        beijing_time = datetime.fromtimestamp(timestamp, tz=Config.BEIJING_TIMEZONE)
        
        logger.debug(f"获取文件修改时间: {file_path} -> UTC: {utc_time}, CST: {beijing_time}")
        return utc_time, beijing_time
    
    except Exception as e:
        logger.error(f"获取文件修改时间失败 {file_path}: {str(e)}", exc_info=True)
        return None, None

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
        logger.error(f"判断交易日失败 {date_param}: {str(e)}", exc_info=True)
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
        from config import Config
        
        if time_to_check is None:
            time_to_check = get_beijing_time()
        
        # 检查是否为交易日
        if not is_trading_day(time_to_check):
            return False
        
        # 检查时间是否在交易时段内
        current_time = time_to_check.time()
        market_open = datetime.strptime(f"{Config.MARKET_OPEN_TIME[0]}:{Config.MARKET_OPEN_TIME[1]}", "%H:%M").time()
        market_close = datetime.strptime(f"{Config.MARKET_CLOSE_TIME[0]}:{Config.MARKET_CLOSE_TIME[1]}", "%H:%M").time()
        
        is_open = market_open <= current_time <= market_close
        logger.debug(f"市场状态检查: {time_to_check} -> {'开市' if is_open else '闭市'}")
        return is_open
        
    except Exception as e:
        logger.error(f"判断市场状态失败 {time_to_check}: {str(e)}", exc_info=True)
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
        return
