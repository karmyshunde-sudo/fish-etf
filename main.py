#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF、指数、股票系统 - 主入口文件
负责调度不同任务类型，包括数据爬取、套利计算和消息推送
特别优化了时区处理，确保所有时间显示为北京时间
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Tuple
import time

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config
# 修改1：从新文件导入函数
from data_crawler.etf_daily_crawler import crawl_all_etfs_daily_data
from data_crawler.all_etfs import update_all_etf_list
from strategy import (
    calculate_arbitrage_opportunity,
    calculate_position_strategy,
    send_daily_report_via_wechat,
    check_arbitrage_exit_signals,
    mark_arbitrage_opportunities_pushed  # 新增：导入增量推送标记函数
)
from wechat_push.push import send_wechat_message, send_task_completion_notification
from utils.file_utils import check_flag, set_flag, get_file_mtime
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    is_file_outdated,
    is_trading_day,
    get_last_trading_day  # 新增：导入获取最近交易日的函数
)

# 初始化日志配置
Config.setup_logging(log_file=Config.LOG_FILE)
logger = logging.getLogger(__name__)

def is_manual_trigger() -> bool:
    """
    检查是否为手动触发任务
    
    Returns:
        bool: 如果是手动触发返回True，否则返回False
    """
    try:
        # GitHub Actions手动触发事件名称
        return os.getenv("GITHUB_EVENT_NAME", "") == "workflow_dispatch"
    except Exception as e:
        logger.error(f"检查触发方式失败: {str(e)}", exc_info=True)
        # 出错时保守策略：认为不是手动触发
        return False

def should_execute_crawl_etf_daily() -> bool:
    """
    判断是否应该执行ETF日线数据爬取任务
    
    Returns:
        bool: 如果应该执行返回True，否则返回False
    """
    # 手动触发的任务总是执行
    if is_manual_trigger():
        logger.info("手动触发的任务，总是执行ETF日线数据爬取")
        return True
    
    # 定时触发的任务：检查是否是交易日或是否已过18点
    beijing_time: datetime = get_beijing_time()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
    beijing_date = beijing_time.date()
    
    # 检查是否为交易日
    is_trading = is_trading_day(beijing_date)
    
    # 非交易日且未到补爬时间（18点后允许补爬）
    if not is_trading and beijing_time.hour < 18:
        logger.info(f"今日{beijing_date}非交易日且未到补爬时间（{beijing_time.hour}点），跳过爬取日线数据（定时任务）")
        return False
    
    logger.info(f"今日{beijing_date}{'是' if is_trading else '不是'}交易日，当前时间{beijing_time.hour}点，{'执行' if is_trading or beijing_time.hour >= 18 else '跳过'}爬取日线数据")
    return True

def should_execute_calculate_arbitrage() -> bool:
    """
    判断是否应该执行套利机会计算任务
    
    Returns:
        bool: 如果应该执行返回True，否则返回False
    """
    # 手动触发的任务总是执行
    if is_manual_trigger():
        logger.info("手动触发的任务，总是执行套利机会计算")
        return True
    
    # 交易时间检查：9:30-15:00之间才执行
    beijing_time: datetime = get_beijing_time()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
    current_time_str = beijing_time.strftime("%H:%M")
    
    # 检查是否在交易时间内
    if not (Config.TRADING_START_TIME <= current_time_str <= Config.TRADING_END_TIME):
        logger.info(f"当前时间 {current_time_str} 不在交易时间范围内 ({Config.TRADING_START_TIME}-{Config.TRADING_END_TIME})，跳过套利计算")
        return False
    
    logger.info("交易时间内，执行套利机会计算任务")
    return True

def should_execute_calculate_position() -> bool:
    """
    判断是否应该执行仓位策略计算任务
    
    Returns:
        bool: 如果应该执行返回True，否则返回False
    """
    # 手动触发的任务总是执行
    if is_manual_trigger():
        logger.info("手动触发的任务，总是执行仓位策略计算")
        return True
    
    # 定时触发的任务：检查当天是否已推送
    if check_flag(Config.get_position_flag_file()):
        logger.info("今日已推送仓位策略，跳过本次计算（定时任务）")
        return False
    
    return True

def handle_update_etf_list() -> Dict[str, Any]:
    """
    处理ETF列表更新任务 - 直接执行，不进行任何条件判断
    因为定时器已经确保只在周日触发此任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        logger.info("开始更新全市场ETF列表（周日强制更新）")
        # 修改2：直接调用新函数
        etf_list = update_all_etf_list()
        
        if etf_list.empty:
            error_msg = "ETF列表更新失败：获取到空的ETF列表"
            logger.error(error_msg)
            result = {"status": "error", "message": error_msg}
            send_task_completion_notification("update_etf_list", result)
            return result
        
        # 确定数据来源
        source = "兜底文件"
        if hasattr(etf_list, 'source'):
            source = etf_list.source
        elif len(etf_list) > 500:  # 假设兜底文件约520只
            source = "网络数据源"
        
        success_msg = f"全市场ETF列表更新完成，共{len(etf_list)}只"
        logger.info(success_msg)
        
        # 获取文件修改时间（UTC与北京时间）
        utc_mtime: datetime  # 【日期datetime类型规则】明确类型为datetime
        beijing_mtime: datetime  # 【日期datetime类型规则】明确类型为datetime
        utc_mtime, beijing_mtime = get_file_mtime(Config.ALL_ETFS_PATH)
        
        # 计算过期时间
        expiration_utc: datetime = utc_mtime + timedelta(days=Config.ETF_LIST_UPDATE_INTERVAL)
        expiration_beijing: datetime = beijing_mtime + timedelta(days=Config.ETF_LIST_UPDATE_INTERVAL)
        
        # 构建结果字典（包含双时区信息）
        result = {
            "status": "success", 
            "message": success_msg, 
            "count": len(etf_list),
            "source": source,
            "last_modified_utc": utc_mtime.strftime("%Y-%m-%d %H:%M"),
            "last_modified_beijing": beijing_mtime.strftime("%Y-%m-%d %H:%M"),
            "expiration_utc": expiration_utc.strftime("%Y-%m-%d %H:%M"),
            "expiration_beijing": expiration_beijing.strftime("%Y-%m-%d %H:%M")
        }
        
        # 发送任务完成通知
        send_task_completion_notification("update_etf_list", result)
        
        return result
    
    except Exception as e:
        error_msg = f"ETF列表更新失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result = {"status": "error", "message": error_msg}
        send_task_completion_notification("update_etf_list", result)
        return result

def handle_crawl_etf_daily() -> Dict[str, Any]:
    """
    处理ETF日线数据爬取任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 检查是否应该执行任务（仅对定时任务有效）
        if not is_manual_trigger() and not should_execute_crawl_etf_daily():
            logger.info("根据定时任务规则，跳过ETF日线数据爬取任务")
            return {"status": "skipped", "message": "非交易日且未到补爬时间"}
        
        # 获取当前双时区时间
        utc_now: datetime, beijing_now: datetime = get_current_times()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
        logger.info(f"开始执行ETF日线数据爬取 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 修改3：执行新的爬取函数
        crawl_all_etfs_daily_data()
        
        success_msg = "ETF日线数据爬取完成"
        logger.info(success_msg)
        
        # 构建结果字典
        result = {
            "status": "success", 
            "message": success_msg,
            "crawl_time_utc": utc_now.strftime("%Y-%m-%d %H:%M"),
            "crawl_time_beijing": beijing_now.strftime("%Y-%m-%d %H:%M")
        }
        
        # 发送任务完成通知
        send_task_completion_notification("crawl_etf_daily", result)
        
        return result
    
    except Exception as e:
        error_msg = f"ETF日线数据爬取失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result = {"status": "error", "message": error_msg}
        send_task_completion_notification("crawl_etf_daily", result)
        return result

def handle_calculate_arbitrage() -> Dict[str, Any]:
    """
    处理套利机会计算任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 检查是否应该执行任务（仅对定时任务有效）
        if not is_manual_trigger() and not should_execute_calculate_arbitrage():
            logger.info("根据定时任务规则，跳过套利机会计算任务")
            return {"status": "skipped", "message": "Not in trading hours"}
        
        # 获取当前双时区时间
        utc_now: datetime, beijing_now: datetime = get_current_times()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
        
        # 计算套利机会
        logger.info("开始计算套利机会")
        discount_df, premium_df = calculate_arbitrage_opportunity()
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        if not discount_df.empty:
            discount_df = discount_df.copy(deep=True)
        if not premium_df.empty:
            premium_df = premium_df.copy(deep=True)
        
        # 检查是否有新的套利机会
        new_opportunities = False
        discount_success = False
        if not discount_df.empty:
            logger.info(f"发现 {len(discount_df)} 个新的折价机会")
            # 推送折价消息
            discount_success = send_wechat_message(discount_df, message_type="discount")
            if discount_success:
                new_opportunities = True
                logger.info("折价机会消息发送成功")
            else:
                logger.error("折价机会消息发送失败")
        
        premium_success = False
        if not premium_df.empty:
            logger.info(f"发现 {len(premium_df)} 个新的溢价机会")
            # 推送溢价消息
            premium_success = send_wechat_message(premium_df, message_type="premium")
            if premium_success:
                new_opportunities = True
                logger.info("溢价机会消息发送成功")
            else:
                logger.error("溢价机会消息发送失败")
        
        # 只有在消息成功发送后才标记为已推送
        if discount_success or premium_success:
            if mark_arbitrage_opportunities_pushed(discount_df, premium_df):
                logger.info("成功标记所有推送的ETF为已推送")
        else:
            logger.warning("消息发送失败，未标记为已推送")
        
        if new_opportunities:
            return {
                "status": "success", 
                "message": f"Arbitrage strategy pushed successfully (Discount: {len(discount_df)}, Premium: {len(premium_df)})",
                "calculation_time_utc": utc_now.strftime("%Y-%m-%d %H:%M"),
                "calculation_time_beijing": beijing_now.strftime("%Y-%m-%d %H:%M")
            }
        else:
            logger.info("未发现需要推送的新套利机会")
            return {
                "status": "skipped", 
                "message": "No new arbitrage opportunities to push"
            }
            
    except Exception as e:
        error_msg = f"套利机会计算失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return {"status": "error", "message": error_msg}

def handle_calculate_position() -> Dict[str, Any]:
    """
    处理仓位策略计算任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 检查是否应该执行任务（仅对定时任务有效）
        if not is_manual_trigger() and not should_execute_calculate_position():
            logger.info("根据定时任务规则，跳过仓位策略计算任务")
            return {"status": "skipped", "message": "Position strategy already pushed today"}
        
        # 获取当前双时区时间
        utc_now: datetime, beijing_now: datetime = get_current_times()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
        
        # 计算仓位策略
        logger.info("开始计算仓位策略")
        message = calculate_position_strategy()
        
        # 检查是否是错误消息
        if "失败" in message or "无法计算" in message:
            logger.error(f"仓位策略计算失败: {message}")
            send_wechat_message(
                message=f"仓位策略计算失败: {message}",
                message_type="error"
            )
            return {"status": "error", "message": message}
        
        # 推送消息
        send_success = send_wechat_message(message, message_type="position")
        
        if send_success:
            set_flag(Config.get_position_flag_file())  # 标记已推送
            return {
                "status": "success", 
                "message": "Position strategy pushed successfully",
                "calculation_time_utc": utc_now.strftime("%Y-%m-%d %H:%M"),
                "calculation_time_beijing": beijing_now.strftime("%Y-%m-%d %H:%M")
            }
        else:
            error_msg = "仓位策略推送失败"
            logger.error(error_msg)
            return {"status": "failed", "message": error_msg}
            
    except Exception as e:
        error_msg = f"仓位策略计算失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return {"status": "error", "message": error_msg}

def handle_send_daily_report() -> Dict[str, Any]:
    """
    处理每日报告发送任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 获取当前双时区时间
        utc_now: datetime, beijing_now: datetime = get_current_times()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
        logger.info(f"开始生成并发送每日报告 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 生成并发送报告
        success = send_daily_report_via_wechat()
        
        if success:
            return {
                "status": "success",
                "message": "Daily report sent successfully",
                "report_time_utc": utc_now.strftime("%Y-%m-%d %H:%M"),
                "report_time_beijing": beijing_now.strftime("%Y-%m-%d %H:%M")
            }
        else:
            error_msg = "每日报告发送失败"
            logger.error(error_msg)
            return {"status": "failed", "message": error_msg}
            
    except Exception as e:
        error_msg = f"每日报告处理失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return {"status": "error", "message": error_msg}

def handle_check_arbitrage_exit() -> Dict[str, Any]:
    """
    处理套利退出信号检查任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 获取当前双时区时间
        utc_now: datetime, beijing_now: datetime = get_current_times()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
        logger.info(f"开始检查套利退出信号 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 检查退出信号
        success = check_arbitrage_exit_signals()
        
        if success:
            return {
                "status": "success",
                "message": "Arbitrage exit signals checked successfully",
                "check_time_utc": utc_now.strftime("%Y-%m-%d %H:%M"),
                "check_time_beijing": beijing_now.strftime("%Y-%m-%d %H:%M")
            }
        else:
            logger.info("未发现需要退出的套利交易")
            return {
                "status": "skipped",
                "message": "No arbitrage positions need to exit"
            }
            
    except Exception as e:
        error_msg = f"套利退出信号检查失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return {"status": "error", "message": error_msg}

def handle_index_yesno() -> Dict[str, Any]:
    """
    处理ETF Yes/No策略任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 获取当前双时区时间
        utc_now: datetime, beijing_now: datetime = get_current_times()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
        logger.info(f"开始执行ETF Yes/No策略 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 导入index_YesNo模块
        from index_yesno import generate_report
        
        # 执行策略
        generate_report()
        
        success_msg = "ETF Yes/No策略执行完成"
        logger.info(success_msg)
        
        # 构建结果字典
        result = {
            "status": "success",
            "message": success_msg,
            "execution_time_utc": utc_now.strftime("%Y-%m-%d %H:%M"),
            "execution_time_beijing": beijing_now.strftime("%Y-%m-%d %H:%M")
        }
        
        # 发送任务完成通知
        send_task_completion_notification("index_yesno", result)
        
        return result
    except Exception as e:
        error_msg = f"ETF Yes/No策略执行失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result = {"status": "error", "message": error_msg}
        send_task_completion_notification("index_yesno", result, message_type="error")
        return result

def main() -> Dict[str, Any]:
    """
    主函数：根据环境变量执行对应任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 获取当前双时区时间
        utc_now: datetime, beijing_now: datetime = get_current_times()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
        
        # 从环境变量获取任务类型（由GitHub Actions传递）
        task = os.getenv("TASK", "unknown")
        
        logger.info(f"===== 开始执行任务：{task} =====")
        logger.info(f"UTC时间：{utc_now.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"北京时间：{beijing_now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 设置环境
        if not setup_environment():
            error_msg = "环境设置失败，任务终止"
            logger.error(error_msg)
            return {"status": "error", "task": task, "message": error_msg}
        
        # 根据任务类型执行对应操作
        task_handlers = {
            "crawl_etf_daily": handle_crawl_etf_daily,
            "calculate_arbitrage": handle_calculate_arbitrage,
            "calculate_position": handle_calculate_position,
            "update_etf_list": handle_update_etf_list,
            "send_daily_report": handle_send_daily_report,
            "check_arbitrage_exit": handle_check_arbitrage_exit,
            "index_yesno": handle_index_yesno  # 新增：指数 Yes/No策略任务
        }
        
        if task in task_handlers:
            result = task_handlers[task]()
            response = {
                "status": result["status"], 
                "task": task, 
                "message": result["message"],
                "timestamp": beijing_now.isoformat()
            }
        else:
            # 未知任务
            error_msg = (
                f"未知任务类型：{task}（支持的任务："
                f"{', '.join(task_handlers.keys())}）"
            )
            logger.error(error_msg)
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
            response = {"status": "error", "task": task, "message": error_msg}
        
        logger.info(f"===== 任务执行结束：{response['status']} =====")
        
        # 输出JSON格式的结果（供GitHub Actions等调用方使用）
        print(json.dumps(response, indent=2, ensure_ascii=False))
        
        return response
        
    except Exception as e:
        error_msg = f"主程序执行失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 尝试发送错误消息
        try:
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        
        # 返回错误响应
        response = {
            "status": "error",
            "task": os.getenv("TASK", "unknown"),
            "message": error_msg,
            "timestamp": get_beijing_time().isoformat()
        }
        
        print(json.dumps(response, indent=2, ensure_ascii=False))
        return response

def setup_environment() -> bool:
    """
    设置运行环境，检查必要的目录和文件
    
    Returns:
        bool: 环境设置是否成功
    """
    try:
        # 获取当前双时区时间
        utc_now: datetime, beijing_now: datetime = get_current_times()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
        
        logger.info(f"开始设置运行环境 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 确保必要的目录存在
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(Config.LOG_DIR, exist_ok=True)
        os.makedirs(os.path.dirname(Config.get_arbitrage_flag_file()), exist_ok=True)
        os.makedirs(os.path.dirname(Config.get_position_flag_file()), exist_ok=True)
        
        # 检查ETF列表是否过期
        if os.path.exists(Config.ALL_ETFS_PATH):
            if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
                logger.warning("ETF列表已过期，建议更新")
            else:
                logger.info("ETF列表有效")
        else:
            logger.warning("ETF列表文件不存在")
        
        # 检查企业微信配置
        if not Config.WECOM_WEBHOOK:
            logger.warning("企业微信Webhook未配置，消息推送将不可用")
        
        # 记录环境信息
        logger.info(f"当前北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("环境设置完成")
        return True
    except Exception as e:
        error_msg = f"环境设置失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False

def run_scheduled_tasks():
    """
    运行定时任务（用于本地测试）
    """
    try:
        logger.info("开始运行定时任务")
        
        # 获取当前双时区时间
        _, beijing_now: datetime = get_current_times()  # 【日期datetime类型规则】确保日期在内存中是datetime类型
        
        # 每5分钟检查一次套利机会（交易时间内）
        if beijing_now.minute % 5 == 0 and Config.TRADING_START_TIME <= beijing_now.strftime("%H:%M") <= Config.TRADING_END_TIME:
            logger.info("执行套利机会计算任务")
            handle_calculate_arbitrage()
        
        # 每小时检查一次仓位策略
        if beijing_now.minute == 0:
            logger.info("执行仓位策略计算任务")
            handle_calculate_position()
        
        # 检查套利退出信号
        if beijing_now.hour >= 14 and beijing_now.minute >= 55:
            logger.info("临近收盘，检查套利退出信号")
            handle_check_arbitrage_exit()
        
        # 每日报告
        if beijing_now.hour == 15 and beijing_now.minute == 30:
            logger.info("执行每日报告发送任务")
            handle_send_daily_report()
        
        # 更新ETF列表（每周日20:00）
        if beijing_now.weekday() == 6 and beijing_now.hour == 20 and beijing_now.minute == 0:
            if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
                logger.info("ETF列表已过期，执行更新任务")
                handle_update_etf_list()
        
        # 爬取日线数据
        if beijing_now.hour == 18 and beijing_now.minute == 0:
            logger.info("执行ETF日线数据爬取任务")
            handle_crawl_etf_daily()
        
        # 指数 Yes/No策略（每天晚上11点）
        if beijing_now.hour == 23 and beijing_now.minute == 0:
            logger.info("执行ETF Yes/No策略任务")
            handle_index_yesno()
        
        logger.info("定时任务执行完成")
        
    except Exception as e:
        logger.error(f"定时任务执行失败: {str(e)}", exc_info=True)

def test_all_modules():
    """
    测试所有模块功能
    """
    try:
        logger.info("开始测试所有模块")
        
        # 测试环境设置
        logger.info("测试环境设置...")
        setup_environment()
        
        # 测试ETF列表更新
        logger.info("测试ETF列表更新...")
        handle_update_etf_list()
        
        # 测试日线数据爬取
        logger.info("测试日线数据爬取...")
        handle_crawl_etf_daily()
        
        # 测试套利机会计算
        logger.info("测试套利机会计算...")
        handle_calculate_arbitrage()
        
        # 测试仓位策略
        logger.info("测试仓位策略...")
        handle_calculate_position()
        
        # 测试指数 Yes/No策略
        logger.info("测试ETF Yes/No策略...")
        handle_index_yesno()
        
        # 测试每日报告
        logger.info("测试每日报告...")
        handle_send_daily_report()
        
        # 测试套利退出信号
        logger.info("测试套利退出信号...")
        handle_check_arbitrage_exit()
        
        logger.info("所有模块测试完成")
        
    except Exception as e:
        logger.error(f"模块测试失败: {str(e)}", exc_info=True)

if __name__ == "__main__":
    # 检查是否为测试模式
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        logger.info("运行测试模式")
        test_all_modules()
    elif len(sys.argv) > 1 and sys.argv[1] == "schedule":
        logger.info("运行定时任务模式")
        run_scheduled_tasks()
    else:
        # 正常执行
        main()
