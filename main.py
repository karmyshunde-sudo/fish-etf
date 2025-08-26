import os
import sys
import json
import logging
from config import Config
from data_crawler import crawl_etf_daily_incremental
from data_crawler.etf_list_manager import update_all_etf_list
from strategy import calculate_arbitrage_opportunity, format_arbitrage_message, calculate_position_strategy
from wechat_push.push import send_wechat_message
from utils.file_utils import check_flag, set_flag
from utils.date_utils import get_beijing_time

# 初始化日志
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL, logging.INFO),
    format=Config.LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def main():
    """主函数：根据环境变量执行对应任务"""
    # 从环境变量获取任务类型（由GitHub Actions传递）
    task = os.getenv("TASK", "unknown")
    now = get_beijing_time()
    logger.info(f"===== 开始执行任务：{task} =====")
    logger.info(f"当前时间：{now.strftime('%Y-%m-%d %H:%M:%S')}（北京时间）")
    
    # 1. 任务1：下午4点增量爬取ETF日线数据
    if task == "crawl_etf_daily":
        crawl_etf_daily_incremental()
        response = {"status": "success", "task": task, "message": "ETF日线数据增量爬取完成"}
    
    # 2. 任务2：多个时间点计算套利机会（单日仅推一次）
    elif task == "calculate_arbitrage":
        # 检查当天是否已推送套利结果
        if check_flag(Config.ARBITRAGE_FLAG_FILE):
            logger.info("今日已推送套利机会，跳过本次计算")
            response = {"status": "skipped", "task": task, "message": "Arbitrage message already pushed today"}
        else:
            # 计算套利机会
            arbitrage_df = calculate_arbitrage_opportunity()
            # 格式化并推送消息
            message = format_arbitrage_message(arbitrage_df)
            send_success = send_wechat_message(message)
            if send_success:
                set_flag(Config.ARBITRAGE_FLAG_FILE)  # 标记已推送
                response = {"status": "success", "task": task, "message": "Arbitrage strategy pushed successfully"}
            else:
                response = {"status": "failed", "task": task, "message": "Failed to push arbitrage strategy"}
    
    # 3. 任务3：下午2点计算仓位策略
    elif task == "calculate_position":
        # 检查当天是否已推送仓位策略
        if check_flag(Config.POSITION_FLAG_FILE):
            logger.info("今日已推送仓位策略，跳过本次计算")
            response = {"status": "skipped", "task": task, "message": "Position strategy already pushed today"}
        else:
            # 计算仓位策略
            message = calculate_position_strategy()
            # 推送消息
            send_success = send_wechat_message(message)
            if send_success:
                set_flag(Config.POSITION_FLAG_FILE)  # 标记已推送
                response = {"status": "success", "task": task, "message": "Position strategy pushed successfully"}
            else:
                response = {"status": "failed", "task": task, "message": "Failed to push position strategy"}
    
    # 4. 任务4：更新全市场ETF列表
    elif task == "update_etf_list":
        etf_list = update_all_etf_list()
        response = {
            "status": "success", 
            "task": task, 
            "message": f"全市场ETF列表更新完成，共{len(etf_list)}只"
        }
    
    # 未知任务
    else:
        error_msg = f"未知任务类型：{task}（支持的任务：crawl_etf_daily, calculate_arbitrage, calculate_position, update_etf_list）"
        logger.error(error_msg)
        send_wechat_message(f"【系统错误】{error_msg}")
        response = {"status": "error", "task": task, "message": error_msg}
    
    logger.info(f"===== 任务执行结束：{response['status']} =====")
    print(json.dumps(response, indent=2, ensure_ascii=False))
    return response

if __name__ == "__main__":
    main()
