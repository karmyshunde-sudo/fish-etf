#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略模块主入口
负责协调各个策略组件，提供统一的策略执行接口
"""

import os
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
from config import Config
from utils.date_utils import (
    get_current_times,
    format_dual_time,
    get_beijing_time,
    get_utc_time,
    is_market_open,
    is_trading_day
)
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

# 直接导出策略函数，以便 main.py 可以导入
from .arbitrage import calculate_arbitrage_opportunity, format_arbitrage_message
from .position import calculate_position_strategy
from .etf_scoring import get_top_rated_etfs

def run_all_strategies() -> Dict[str, Any]:
    """
    运行所有策略并返回结果
    
    Returns:
        Dict[str, Any]: 包含所有策略结果的字典
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始运行所有ETF策略 (UTC: {utc_now}, CST: {beijing_now})")
        
        results = {
            "arbitrage": "",
            "position": "",
            "arbitrage_df": pd.DataFrame(),
            "success": False,
            "error": None
        }
        
        # 1. 运行套利策略
        arbitrage_result = calculate_arbitrage_opportunity()
        if isinstance(arbitrage_result, pd.DataFrame):
            results["arbitrage_df"] = arbitrage_result
            results["arbitrage"] = "✅ 套利机会已识别"
        else:
            results["arbitrage"] = f"❌ 套利机会计算失败: {arbitrage_result}"
            results["error"] = results["arbitrage"]
        
        # 2. 运行仓位策略
        position_result = calculate_position_strategy()
        results["position"] = position_result
        
        # 标记执行成功
        if not results["error"]:
            results["success"] = True
            logger.info("所有策略执行成功")
        else:
            logger.warning("策略执行完成，但存在错误")
            
        return results
    
    except Exception as e:
        error_msg = f"运行所有策略时发生未预期错误: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "arbitrage": f"【策略错误】{error_msg}",
            "position": f"【策略错误】{error_msg}",
            "arbitrage_df": pd.DataFrame(),
            "success": False,
            "error": error_msg
        }

def get_daily_report() -> str:
    """
    生成每日策略报告
    
    Returns:
        str: 格式化的策略报告
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始生成每日策略报告 (UTC: {utc_now}, CST: {beijing_now})")
        
        strategies = run_all_strategies()
        
        # 生成报告标题
        report = (
            "【ETF量化策略每日报告】\n"
            f"📅 生成时间: {format_dual_time(beijing_now)}\n\n"
        )
        
        # 添加套利机会分析
        report += "🔍 套利机会分析\n"
        if not strategies["arbitrage_df"].empty:
            top_opportunities = strategies["arbitrage_df"].head(3)
            for _, row in top_opportunities.iterrows():
                report += (
                    f"• {row['ETF名称']}({row['ETF代码']}): "
                    f"折溢价率 {row['折溢价率']:.2f}% | "
                    f"当前价格 {row['最新价']:.3f}元\n"
                )
        else:
            report += "• 未发现有效套利机会\n"
        
        # 添加仓位操作建议
        report += "\n💼 仓位操作建议\n"
        report += f"{strategies['position']}\n"
        
        # 添加市场状态信息
        market_status = "开市" if is_market_open() else "闭市"
        trading_status = "交易日" if is_trading_day() else "非交易日"
        
        report += (
            "\n📊 市场状态\n"
            f"• 当前状态: {market_status}\n"
            f"• 今日是否交易日: {trading_status}\n"
        )
        
        # 添加风险提示
        report += (
            "\n⚠️ 风险提示\n"
            "• 本策略基于历史数据和统计模型，不构成投资建议\n"
            "• 市场有风险，投资需谨慎\n"
            "• 请结合个人风险承受能力做出投资决策\n"
        )
        
        logger.info("每日策略报告生成完成")
        return report
    except Exception as e:
        error_msg = f"生成每日报告失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return f"【报告生成错误】{error_msg}"

def send_daily_report_via_wechat() -> bool:
    """
    生成并发送每日策略报告到微信
    
    Returns:
        bool: 发送是否成功
    """
    try:
        # 获取当前北京时间用于文件命名
        beijing_now = get_beijing_time()
        today = beijing_now.date().strftime("%Y-%m-%d")
        
        # 检查是否已经发送过今日报告
        report_sent_flag = os.path.join(Config.FLAG_DIR, f"report_sent_{today}.txt")
        if os.path.exists(report_sent_flag):
            logger.info("今日报告已发送，跳过重复发送")
            return True
        
        # 生成报告
        report = get_daily_report()
        
        # 发送到微信
        success = send_wechat_message(report)
        
        if success:
            # 标记已发送
            os.makedirs(os.path.dirname(report_sent_flag), exist_ok=True)
            with open(report_sent_flag, "w", encoding="utf-8") as f:
                f.write(beijing_now.strftime("%Y-%m-%d %H:%M:%S"))
            logger.info("每日策略报告已成功发送到微信")
        else:
            logger.error("微信消息发送失败")
        
        return success
    except Exception as e:
        error_msg = f"发送每日报告失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False

def check_arbitrage_exit_signals() -> bool:
    """
    检查套利退出信号（持有1天后）
    
    Returns:
        bool: 是否成功检查退出信号
    """
    try:
        from .position import init_trade_record
        logger.info("开始检查套利退出信号")
        
        # 初始化交易记录
        init_trade_record()
        
        # 检查交易记录文件是否存在
        if not os.path.exists(Config.TRADE_RECORD_FILE):
            logger.warning("交易记录文件不存在，无法检查套利退出信号")
            return False
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 读取交易记录
        trade_df = pd.read_csv(Config.TRADE_RECORD_FILE, encoding="utf-8")
        
        # 获取昨天的日期（基于北京时间）
        yesterday = (beijing_now - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.debug(f"检查昨天({yesterday})执行的套利交易")
        
        # 查找昨天执行的套利交易
        yesterday_arbitrage = trade_df[
            (trade_df["操作"] == "套利买入") & 
            (trade_df["创建日期"] == yesterday)
        ]
        
        if not yesterday_arbitrage.empty:
            logger.info(f"发现{len(yesterday_arbitrage)}条需要退出的套利交易")
            
            # 生成退出信号消息
            exit_message = "【套利退出信号】\n"
            for _, row in yesterday_arbitrage.iterrows():
                exit_message += (
                    f"• {row['ETF名称']}({row['ETF代码']})："
                    f"已持有1天，建议退出\n"
                )
            
            # 发送退出信号
            send_wechat_message(exit_message)
            return True
        
        logger.info("未发现需要退出的套利交易")
        return False
    
    except Exception as e:
        error_msg = f"检查套利退出信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False

def run_strategy_with_retry(strategy_func, max_retries: int = 3, delay: int = 5) -> Any:
    """
    带重试的策略执行函数
    
    Args:
        strategy_func: 策略函数
        max_retries: 最大重试次数
        delay: 重试延迟（秒）
        
    Returns:
        Any: 策略执行结果
    """
    from functools import wraps
    import time

    @wraps(strategy_func)
    def wrapper(*args, **kwargs):
        last_exception = None
        for attempt in range(max_retries):
            try:
                # 获取当前双时区时间
                utc_now, beijing_now = get_current_times()
                logger.info(f"尝试执行策略 ({attempt + 1}/{max_retries}) (UTC: {utc_now}, CST: {beijing_now})")
                return strategy_func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                logger.warning(f"策略执行失败 ({attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"{delay}秒后重试...")
                    time.sleep(delay)
        
        logger.error(f"策略执行失败，已达最大重试次数")
        raise last_exception
    
    return wrapper

def check_strategy_execution_conditions() -> Tuple[bool, str]:
    """
    检查策略执行条件
    
    Returns:
        Tuple[bool, str]: (是否可以执行, 原因)
    """
    try:
        # 检查是否为交易日
        if not is_trading_day():
            return False, "今日非交易日"
        
        # 检查市场是否开市
        if not is_market_open():
            return False, "当前非交易时间"
        
        # 检查ETF列表是否更新
        if not os.path.exists(Config.ALL_ETFS_PATH):
            return False, "ETF列表未更新"
        
        # 检查ETF列表是否过期
        if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
            return False, "ETF列表已过期"
        
        return True, "所有条件满足"
    
    except Exception as e:
        logger.error(f"检查策略执行条件失败: {str(e)}", exc_info=True)
        return False, f"条件检查异常: {str(e)}"

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 创建策略标志目录
    os.makedirs(Config.FLAG_DIR, exist_ok=True)
    
    # 初始化日志
    logger.info("策略模块初始化完成")
    
    # 记录当前市场状态
    market_status = "开市" if is_market_open() else "闭市"
    trading_status = "交易日" if is_trading_day() else "非交易日"
    logger.info(f"当前市场状态: {trading_status}，{market_status}")
    
except Exception as e:
    logger.error(f"策略模块初始化失败: {str(e)}", exc_info=True)
    # 退回到基础日志配置
    import logging
    logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
    logging.error(f"策略模块初始化失败: {str(e)}")
