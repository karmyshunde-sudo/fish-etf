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
from datetime import datetime
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
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
    format=Config.LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Config.LOG_FILE_PATH)
    ]
)
logger = logging.getLogger(__name__)

def setup_environment() -> bool:
    """设置运行环境，检查必要的目录和文件"""
    try:
        # 确保必要的目录存在
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(Config.LOG_DIR, exist_ok=True)
        os.makedirs(os.path.dirname(Config.ARBITRAGE_FLAG_FILE), exist_ok=True)
        os.makedirs(os.path.dirname(Config.POSITION_FLAG_FILE), exist_ok=True)
        
        logger.info("环境设置完成")
        return True
    except Exception as e:
        logger.error(f"环境设置失败: {str(e)}")
        return False

def handle_crawl_etf_daily() -> Dict[str, Any]:
    """处理ETF日线数据爬取任务"""
    try:
        logger.info("开始执行ETF日线数据增量爬取")
        crawl_etf_daily_incremental()
        return {"status": "success", "message": "ETF日线数据增量爬取完成"}
    except Exception as e:
        error_msg = f"ETF日线数据爬取失败: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        send_wechat_message(f"【系统错误】ETF日线数据爬取失败: {str(e)}")
        return {"status": "error", "message": error_msg}

def handle_calculate_arbitrage() -> Dict[str, Any]:
    """处理套利机会计算任务"""
    try:
        # 检查当天是否已推送套利结果
        if check_flag(Config.ARBITRAGE_FLAG_FILE):
            logger.info("今日已推送套利机会，跳过本次计算")
            return {"status": "skipped", "message": "Arbitrage message already pushed today"}
        
        # 计算套利机会
        logger.info("开始计算套利机会")
        arbitrage_df = calculate_arbitrage_opportunity()
        
        # 格式化并推送消息
        message = format_arbitrage_message(arbitrage_df)
        send_success = send_wechat_message(message)
        
        if send_success:
            set_flag(Config.ARBITRAGE_FLAG_FILE)  # 标记已推送
            return {"status": "success", "message": "Arbitrage strategy pushed successfully"}
        else:
            error_msg = "套利策略推送失败"
            logger.error(error_msg)
            return {"status": "failed", "message": error_msg}
            
    except Exception as e:
        error_msg = f"套利机会计算失败: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        send_wechat_message(f"【系统错误】套利机会计算失败: {str(e)}")
        return {"status": "error", "message": error_msg}

def handle_calculate_position() -> Dict[str, Any]:
    """处理仓位策略计算任务"""
    try:
        # 检查当天是否已推送仓位策略
        if check_flag(Config.POSITION_FLAG_FILE):
            logger.info("今日已推送仓位策略，跳过本次计算")
            return {"status": "skipped", "message": "Position strategy already pushed today"}
        
        # 计算仓位策略
        logger.info("开始计算仓位策略")
        message = calculate_position_strategy()
        
        # 推送消息
        send_success = send_wechat_message(message)
        
        if send_success:
            set_flag(Config.POSITION_FLAG_FILE)  # 标记已推送
            return {"status": "success", "message": "Position strategy pushed successfully"}
        else:
            error_msg = "仓位策略推送失败"
            logger.error(error_msg)
            return {"status": "failed", "message": error_msg}
            
    except Exception as e:
        error_msg = f"仓位策略计算失败: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        send_wechat_message(f"【系统错误】仓位策略计算失败: {str(e)}")
        return {"status": "error", "message": error_msg}

def handle_update_etf_list() -> Dict[str, Any]:
    """处理ETF列表更新任务"""
    try:
        logger.info("开始更新全市场ETF列表")
        etf_list = update_all_etf_list()
        return {
            "status": "success", 
            "message": f"全市场ETF列表更新完成，共{len(etf_list)}只"
        }
    except Exception as e:
        error_msg = f"ETF列表更新失败: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        send_wechat_message(f"【系统错误】ETF列表更新失败: {str(e)}")
        return {"status": "error", "message": error_msg}

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
        return {"status": "error", "task": task, "message": error_msg}
    
    # 根据任务类型执行对应操作
    task_handlers = {
        "crawl_etf_daily": handle_crawl_etf_daily,
        "calculate_arbitrage": handle_calculate_arbitrage,
        "calculate_position": handle_calculate_position,
        "update_etf_list": handle_update_etf_list
    }
    
    if task in task_handlers:
        result = task_handlers[task]()
        response = {"status": result["status"], "task": task, "message": result["message"]}
    else:
        # 未知任务
        error_msg = f"未知任务类型：{task}（支持的任务：crawl_etf_daily, calculate_arbitrage, calculate_position, update_etf_list）"
        logger.error(error_msg)
        send_wechat_message(f"【系统错误】{error_msg}")
        response = {"status": "error", "task": task, "message": error_msg}
    
    logger.info(f"===== 任务执行结束：{response['status']} =====")
    
    # 输出JSON格式的结果（供GitHub Actions等调用方使用）
    print(json.dumps(response, indent=2, ensure_ascii=False))
    
    return response

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"主程序发生未捕获异常: {str(e)}")
        logger.critical(traceback.format_exc())
        
        # 发送紧急通知
        send_wechat_message(f"【系统崩溃】主程序发生未捕获异常: {str(e)}")
        
        # 返回错误响应
        error_response = {
            "status": "critical_error", 
            "task": "unknown", 
            "message": f"主程序崩溃: {str(e)}"
        }
        print(json.dumps(error_response, indent=2, ensure_ascii=False))
        
        sys.exit(1)
