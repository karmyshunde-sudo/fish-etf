# strategy/__init__.py
import os
import pandas as pd
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from config import Config
from wechat_push.push import _format_arbitrage_message, send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

# 直接导出策略函数，以便 main.py 可以导入
from .arbitrage import calculate_arbitrage_opportunity
from .position import calculate_position_strategy, check_arbitrage_exit_signals as check_position_exit_signals
from .etf_scoring import get_etf_basic_info, get_etf_name

def run_all_strategies() -> Dict[str, Any]:
    """运行所有策略并返回结果
    :return: 包含所有策略结果的字典
    """
    try:
        logger.info("开始运行所有ETF策略...")
        results = {
            "arbitrage_df": pd.DataFrame(),
            "position_msg": "",
            "success": False,
            "error": None
        }

        # 1. 运行套利策略
        logger.info("\n" + "="*50)
        logger.info("运行套利策略")
        logger.info("="*50)
        try:
            arbitrage_df = calculate_arbitrage_opportunity()
            results["arbitrage_df"] = arbitrage_df
            logger.info(f"套利策略执行完成，发现 {len(arbitrage_df)} 个机会")
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
            "arbitrage_df": pd.DataFrame(),
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
        
        # 格式化套利消息
        arbitrage_msg = ""
        if not strategies["arbitrage_df"].empty:
            arbitrage_msg = _format_arbitrage_message(strategies["arbitrage_df"])
        else:
            arbitrage_msg = "【套利机会】\n未发现有效套利机会"
        
        # 获取当前双时区时间
        from utils.date_utils import get_current_times
        utc_now, beijing_now = get_current_times()
        
        # 构建报告
        report = f"【ETF量化策略每日报告】\n"
        report += f"📅 报告时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        report += f"🌍 UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        report += "📊 套利机会分析：\n"
        report += arbitrage_msg + "\n"
        
        report += "\n📈 仓位操作建议：\n"
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
        from utils.date_utils import get_current_times, get_beijing_time
        from wechat_push.push import send_wechat_message
        logger.info("开始检查套利退出信号")
        
        # 检查交易记录文件是否存在
        if not os.path.exists(Config.TRADE_RECORD_FILE):
            logger.warning("交易记录文件不存在，无法检查套利退出信号")
            return False
            
        # 读取交易记录
        trade_df = pd.read_csv(Config.TRADE_RECORD_FILE, encoding="utf-8")
        
        # 获取昨天的日期（基于北京时间）
        utc_now, beijing_now = get_current_times()
        yesterday = (beijing_now - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # 查找昨天执行的套利交易
        yesterday_arbitrage = trade_df[
            (trade_df["操作"] == "套利买入") & 
            (trade_df["创建日期"] == yesterday)
        ]
        
        if not yesterday_arbitrage.empty:
            logger.info(f"发现{len(yesterday_arbitrage)}条需要退出的套利交易")
            
            # 生成退出信号消息内容
            exit_content = "【套利退出信号】\n"
            exit_content += f"发现 {len(yesterday_arbitrage)} 条需要退出的套利交易\n\n"
            
            for _, row in yesterday_arbitrage.iterrows():
                exit_content += (
                    f"• {row['ETF名称']}({row['ETF代码']})："
                    f"已持有1天，建议退出\n"
                )
            
            # 发送退出信号
            send_wechat_message(exit_content, message_type="arbitrage")
            return True
        
        logger.info("未发现需要退出的套利交易")
        return False
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
