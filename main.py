#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF套利策略系统 - 主入口文件
负责调度不同任务类型，包括数据爬取、套利计算和消息推送
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config
from data_crawler import crawl_etf_daily_incremental
from data_crawler.etf_list_manager import update_all_etf_list
from strategy import calculate_arbitrage_opportunity, format_arbitrage_message, calculate_position_strategy
from wechat_push.push import send_wechat_message
from utils.file_utils import check_flag, set_flag
from utils.date_utils import get_beijing_time

# 初始化日志配置
Config.setup_logging(log_file=Config.LOG_FILE)
logger = logging.getLogger(__name__)

def setup_environment() -> bool:
    """设置运行环境，检查必要的目录和文件"""
    try:
        # 确保必要的目录存在
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(Config.LOG_DIR, exist_ok=True)
        os.makedirs(os.path.dirname(Config.get_arbitrage_flag_file()), exist_ok=True)
        os.makedirs(os.path.dirname(Config.get_position_flag_file()), exist_ok=True)
        
        logger.info("环境设置完成")
        return True
    except Exception as e:
        logger.error(f"环境设置失败: {str(e)}")
        return False

def send_task_completion_notification(task: str, result: Dict[str, Any]):
    """
    发送任务完成通知到企业微信
    
    Args:
        task: 任务名称
        result: 任务执行结果
    """
    try:
        if result["status"] == "success":
            status_emoji = "✅"
            status_msg = "成功"
        elif result["status"] == "skipped":
            status_emoji = "⏭️"
            status_msg = "已跳过"
        else:
            status_emoji = "❌"
            status_msg = "失败"
        
        # 构建任务总结消息
        summary_msg = (
            f"【任务执行】{task}\n\n"
            f"{status_emoji} 状态: {status_msg}\n"
            f"📝 详情: {result.get('message', '无详细信息')}\n"
        )
        
        # 添加任务特定信息
        if task == "update_etf_list" and result["status"] == "success":
            # 从消息中提取ETF数量（格式："全市场ETF列表更新完成，共XXX只"）
            count = 0
            message = result.get('message', '')
            if "共" in message and "只" in message:
                try:
                    count = int(message.split("共")[1].split("只")[0])
                except:
                    pass
            summary_msg += f"📊 ETF数量: {count}只\n"
            
            # 添加数据来源信息
            source = result.get('source', '未知')
            summary_msg += f"来源: {source}\n"
            
            # 添加列表有效期信息
            try:
                file_path = Config.ALL_ETFS_PATH
                if os.path.exists(file_path):
                    last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
                    expiration = last_modified + timedelta(days=Config.ETF_LIST_UPDATE_INTERVAL)
                    summary_msg += f"📅 生成时间: {last_modified.strftime('%Y-%m-%d %H:%M')}\n"
                    summary_msg += f"⏳ 过期时间: {expiration.strftime('%Y-%m-%d %H:%M')}\n"
            except Exception as e:
                logger.error(f"获取ETF列表文件信息失败: {str(e)}")
                summary_msg += "📅 列表有效期信息: 获取失败\n"
        
        elif task == "crawl_etf_daily" and result["status"] == "success":
            summary_msg += "📈 数据爬取: 完成\n"
            
        elif task == "calculate_arbitrage" and result["status"] == "success":
            summary_msg += "🔍 套利机会: 已推送\n"
            
        elif task == "calculate_position" and result["status"] == "success":
            summary_msg += "💼 仓位策略: 已推送\n"
        
        # 发送任务总结通知
        send_wechat_message(summary_msg)
        logger.info(f"已发送任务完成通知: {task} - {status_msg}")
        
    except Exception as e:
        logger.error(f"发送任务完成通知失败: {str(e)}")
        logger.error(traceback.format_exc())

def handle_crawl_etf_daily() -> Dict[str, Any]:
    """处理ETF日线数据爬取任务"""
    try:
        logger.info("开始执行ETF日线数据增量爬取")
        crawl_etf_daily_incremental()
        
        result = {
            "status": "success", 
            "message": "ETF日线数据增量爬取完成"
        }
        
        # 发送任务完成通知
        send_task_completion_notification("crawl_etf_daily", result)
        
        return result
    except Exception as e:
        error_msg = f"ETF日线数据爬取失败: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        
        result = {"status": "error", "message": error_msg}
        
        # 发送任务完成通知
        send_task_completion_notification("crawl_etf_daily", result)
        
        return result

def handle_calculate_arbitrage() -> Dict[str, Any]:
    """处理套利机会计算任务"""
    try:
        # 检查当天是否已推送套利结果
        if check_flag(Config.get_arbitrage_flag_file()):
            logger.info("今日已推送套利机会，跳过本次计算")
            result = {
                "status": "skipped", 
                "message": "今日已推送套利机会，跳过本次计算"
            }
            # 发送任务完成通知
            send_task_completion_notification("calculate_arbitrage", result)
            return result
        
        # 计算套利机会
        logger.info("开始计算套利机会")
        arbitrage_df = calculate_arbitrage_opportunity()
        
        # 格式化并推送消息
        message = format_arbitrage_message(arbitrage_df)
        send_success = send_wechat_message(message)
        
        if send_success:
            set_flag(Config.get_arbitrage_flag_file())  # 标记已推送
            result = {"status": "success", "message": "套利策略已成功推送"}
            # 发送任务完成通知
            send_task_completion_notification("calculate_arbitrage", result)
            return result
        else:
            error_msg = "套利策略推送失败"
            logger.error(error_msg)
            result = {"status": "failed", "message": error_msg}
            # 发送任务完成通知
            send_task_completion_notification("calculate_arbitrage", result)
            return result
            
    except Exception as e:
        error_msg = f"套利机会计算失败: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        
        result = {"status": "error", "message": error_msg}
        
        # 发送任务完成通知
        send_task_completion_notification("calculate_arbitrage", result)
        
        return result

def handle_calculate_position() -> Dict[str, Any]:
    """处理仓位策略计算任务"""
    try:
        # 检查当天是否已推送仓位策略
        if check_flag(Config.get_position_flag_file()):
            logger.info("今日已推送仓位策略，跳过本次计算")
            result = {
                "status": "skipped", 
                "message": "今日已推送仓位策略，跳过本次计算"
            }
            # 发送任务完成通知
            send_task_completion_notification("calculate_position", result)
            return result
        
        # 计算仓位策略
        logger.info("开始计算仓位策略")
        message = calculate_position_strategy()
        
        # 推送消息
        send_success = send_wechat_message(message)
        
        if send_success:
            set_flag(Config.get_position_flag_file())  # 标记已推送
            result = {"status": "success", "message": "仓位策略已成功推送"}
            # 发送任务完成通知
            send_task_completion_notification("calculate_position", result)
            return result
        else:
            error_msg = "仓位策略推送失败"
            logger.error(error_msg)
            result = {"status": "failed", "message": error_msg}
            # 发送任务完成通知
            send_task_completion_notification("calculate_position", result)
            return result
            
    except Exception as e:
        error_msg = f"仓位策略计算失败: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        
        result = {"status": "error", "message": error_msg}
        
        # 发送任务完成通知
        send_task_completion_notification("calculate_position", result)
        
        return result

def handle_update_etf_list() -> Dict[str, Any]:
    """处理ETF列表更新任务"""
    try:
        logger.info("开始更新全市场ETF列表")
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
        
        # 记录文件最后修改时间
        file_path = Config.ALL_ETFS_PATH
        last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
        expiration = last_modified + timedelta(days=7)
        
        # 返回结果包含数据来源和有效期
        result = {
            "status": "success", 
            "message": success_msg, 
            "count": len(etf_list),
            "source": source,
            "last_modified": last_modified.strftime("%Y-%m-%d %H:%M:%S"),
            "expiration": expiration.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 发送任务完成通知
        send_task_completion_notification("update_etf_list", result)
        
        return result
    
    except Exception as e:
        error_msg = f"ETF列表更新失败: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        result = {"status": "error", "message": error_msg}
        send_task_completion_notification("update_etf_list", result)
        return result

def main() -> Dict[str, Any]:
    """主函数：根据环境变量执行对应任务"""
    # 从环境变量获取任务类型（由GitHub Actions传递）
    task = os.getenv("TASK", "unknown")
    now = get_beijing_time()
    
    logger.info(f"===== 开始执行任务：{task} =====")
    logger.info(f"当前时间：{now.strftime('%Y-%m-%d %H:%M:%S')}（北京时间）")
    
    # 设置环境
    if not setup_environment():
        error_msg = "环境设置失败，任务终止"
        logger.error(error_msg)
        result = {"status": "error", "task": task, "message": error_msg}
        # 发送任务完成通知
        send_task_completion_notification(task, result)
        return result
    
    try:
        # 根据任务类型执行不同操作
        task_handlers = {
            "crawl_etf_daily": handle_crawl_etf_daily,
            "calculate_arbitrage": handle_calculate_arbitrage,
            "calculate_position": handle_calculate_position,
            "update_etf_list": handle_update_etf_list
        }
        
        if task in task_handlers:
            result = task_handlers[task]()
        else:
            error_msg = f"未知任务类型：{task}（支持的任务：crawl_etf_daily, calculate_arbitrage, calculate_position, update_etf_list）"
            logger.error(error_msg)
            result = {"status": "error", "task": task, "message": error_msg}
            # 发送任务完成通知
            send_task_completion_notification(task, result)
        
        # 构建最终响应
        response = {
            "status": result["status"],
            "task": task,
            "message": result["message"],
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        logger.info(f"===== 任务执行结束：{response['status']} =====")
        
        # 输出JSON格式的结果（供GitHub Actions等调用方使用）
        print(json.dumps(response, indent=2, ensure_ascii=False))
        
        return response
    
    except Exception as e:
        error_msg = f"任务执行过程中发生未预期错误: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        
        response = {
            "status": "critical_error",
            "task": task,
            "message": error_msg,
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 发送任务完成通知
        send_task_completion_notification(task, response)
        
        # 输出JSON格式的结果
        print(json.dumps(response, indent=2, ensure_ascii=False))
        
        return response

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"主程序发生未捕获异常: {str(e)}")
        logger.critical(traceback.format_exc())
        
        # 尝试获取当前任务
        task = os.getenv("TASK", "unknown")
        
        # 发送紧急通知
        send_wechat_message(f"【系统崩溃】主程序发生未捕获异常: {str(e)}\n任务类型: {task}")
        
        # 返回错误响应
        error_response = {
            "status": "critical_error", 
            "task": task, 
            "message": f"主程序崩溃: {str(e)}",
            "timestamp": get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # 输出JSON格式的结果
        print(json.dumps(error_response, indent=2, ensure_ascii=False))
        
        sys.exit(1)
