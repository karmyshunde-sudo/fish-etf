# strategy/__init__.py
import os
import pandas as pd
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from config import Config
# 修复：更新导入语句，使用新的格式化函数
from wechat_push.push import _format_discount_message, _format_premium_message, send_wechat_message
from utils.date_utils import get_current_times, get_beijing_time, get_utc_time

# 初始化日志
logger = logging.getLogger(__name__)

# 直接导出策略函数，以便 main.py 可以导入
from .arbitrage import (
    calculate_arbitrage_opportunity,
    mark_arbitrage_opportunities_pushed,  # 修复：添加增量推送标记函数的导出
    calculate_arbitrage_scores
)
from .position import calculate_position_strategy
from .etf_scoring import (
    get_etf_basic_info, 
    get_etf_name,
    calculate_arbitrage_score,
    calculate_component_stability_score
)

def run_all_strategies() -> Dict[str, Any]:
    """运行所有策略并返回结果
    :return: 包含所有策略结果的字典
    """
    try:
        logger.info("开始运行所有ETF策略...")
        results = {
            "discount_df": pd.DataFrame(),
            "premium_df": pd.DataFrame(),
            "position_msg": "",
            "success": False,
            "error": None
        }

        # 1. 运行套利策略
        logger.info("\n" + "="*50)
        logger.info("运行套利策略")
        logger.info("="*50)
        try:
            discount_df, premium_df = calculate_arbitrage_opportunity()
            results["discount_df"] = discount_df
            results["premium_df"] = premium_df
            logger.info(f"套利策略执行完成，发现 {len(discount_df)} 个折价机会和 {len(premium_df)} 个溢价机会")
        except Exception as e:
            error_msg = f"套利策略执行失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results["error"] = error_msg

        # 2. 运行仓位策略
        logger.info("\n" + "="*50)
        logger.info("运行仓位策略")
        logger.info("="*50)
        try:
            position_msg = calculate_position_strategy()
            results["position_msg"] = position_msg
            logger.info("仓位策略执行完成")
        except Exception as e:
            error_msg = f"仓位策略执行失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results["error"] = error_msg if not results["error"] else f"{results['error']}; {error_msg}"

        # 标记执行成功
        if not results["error"]:
            results["success"] = True
        logger.info("所有策略执行完成")
        return results
    except Exception as e:
        error_msg = f"运行所有策略时发生未预期错误: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "discount_df": pd.DataFrame(),
            "premium_df": pd.DataFrame(),
            "position_msg": "",
            "success": False,
            "error": error_msg
        }

def get_daily_report() -> str:
    """生成每日策略报告
    :return: 格式化后的每日报告字符串
    """
    try:
        logger.info("开始生成每日策略报告")
        strategies = run_all_strategies()
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 【日期datetime类型规则】确保时间是datetime类型
        if not isinstance(utc_now, datetime):
            logger.warning("UTC时间不是datetime类型，已转换")
            utc_now = datetime.now()
        if not isinstance(beijing_now, datetime):
            logger.warning("北京时间不是datetime类型，已转换")
            beijing_now = datetime.now()
        
        # 构建报告
        report = f"【ETF量化策略每日报告】\n"
        report += f"📅 报告时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        report += f"🌍 UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        # 格式化折价消息
        report += "📊 折价机会分析：\n"
        if not strategies["discount_df"].empty:
            # 修复：这里直接使用导入的_format_discount_message函数
            report += _format_discount_message(strategies["discount_df"]) + "\n"
        else:
            report += "【折价机会】\n未发现有效折价套利机会\n\n"
        
        # 格式化溢价消息
        report += "📈 溢价机会分析：\n"
        if not strategies["premium_df"].empty:
            # 修复：这里直接使用导入的_format_premium_message函数
            report += _format_premium_message(strategies["premium_df"]) + "\n"
        else:
            report += "【溢价机会】\n未发现有效溢价套利机会\n\n"
        
        report += "\n📉 仓位操作建议：\n"
        report += strategies["position_msg"] + "\n"
        
        if strategies["error"]:
            report += "\n⚠️ 执行警告：\n"
            report += f"部分策略执行过程中出现错误: {strategies['error']}"
        
        report += "\n💡 温馨提示：以上建议仅供参考，请结合市场情况谨慎决策！"
        logger.info("每日策略报告生成完成")
        return report
    except Exception as e:
        error_msg = f"生成每日报告失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return f"【报告生成错误】{error_msg}"

def send_daily_report_via_wechat() -> bool:
    """生成并发送每日策略报告到微信
    :return: 是否成功发送报告
    """
    try:
        report = get_daily_report()
        return send_wechat_message(report, message_type="daily_report")
    except Exception as e:
        error_msg = f"发送微信报告失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False

def check_arbitrage_exit_signals() -> bool:
    """检查套利退出信号（持有1天后）
    :return: 是否成功检查退出信号
    """
    try:
        from .arbitrage import check_arbitrage_exit_signals as check_arbitrage
        return check_arbitrage()
    except Exception as e:
        error_msg = f"检查套利退出信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False

def analyze_arbitrage_performance() -> Dict[str, Any]:
    """分析套利表现
    :return: 分析结果
    """
    try:
        logger.info("开始分析套利表现")
        
        # 获取历史数据
        from .arbitrage import get_arbitrage_history
        history_df = get_arbitrage_history()
        
        if history_df.empty:
            logger.info("无历史数据可供分析")
            return {
                "avg_opportunities": 0,
                "max_premium": 0,
                "min_discount": 0,
                "trend": "无数据",
                "has_high_premium": False,
                "has_high_discount": False
            }
        
        # 计算统计指标
        avg_opportunities = history_df["机会数量"].mean()
        max_premium = history_df["最大折溢价率"].max()
        min_discount = history_df["最小折溢价率"].min()
        
        # 添加趋势分析
        trend = "平稳"
        if len(history_df) >= 3:
            trend = "上升" if history_df["机会数量"].iloc[-3:].mean() > history_df["机会数量"].iloc[:3].mean() else "下降"
        
        # 返回结构化分析结果
        result = {
            "avg_opportunities": avg_opportunities,
            "max_premium": max_premium,
            "min_discount": min_discount,
            "trend": trend,
            "has_high_premium": max_premium > 2.0,
            "has_high_discount": min_discount < -2.0
        }
        
        logger.info("套利表现分析完成")
        return result
    except Exception as e:
        error_msg = f"套利表现分析失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "avg_opportunities": 0,
            "max_premium": 0,
            "min_discount": 0,
            "trend": "分析失败",
            "has_high_premium": False,
            "has_high_discount": False
        }

def run_strategy_with_retry(strategy_func, max_retries: int = 3, delay: int = 5) -> Any:
    """带重试的策略执行函数
    :param strategy_func: 策略函数
    :param max_retries: 最大重试次数
    :param delay: 重试延迟（秒）
    :return: 策略执行结果
    """
    import time
    from functools import wraps
    
    @wraps(strategy_func)
    def wrapper(*args, **kwargs):
        last_exception = None
        for attempt in range(max_retries):
            try:
                logger.info(f"尝试执行策略 ({attempt + 1}/{max_retries})")
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

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 创建策略标志目录
    os.makedirs(Config.FLAG_DIR, exist_ok=True)
    
    logger.info("策略模块初始化完成")
except Exception as e:
    logger.error(f"策略模块初始化失败: {str(e)}", exc_info=True)
    
    # 退回到基础日志配置
    try:
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(f"策略模块初始化失败: {str(e)}")
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(f"策略模块初始化失败: {str(e)}")
    
    # 发送错误通知
    try:
        from wechat_push.push import send_wechat_message
        send_wechat_message(
            message=f"策略模块初始化失败: {str(e)}",
            message_type="error"
        )
    except Exception as send_error:
        logger.error(f"发送错误通知失败: {str(send_error)}", exc_info=True)
