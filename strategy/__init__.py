# strategy/__init__.py
import os
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from config import Config

# 初始化日志
logger = logging.getLogger(__name__)

def run_all_strategies() -> Dict[str, Any]:
    """
    运行所有策略并返回结果
    :return: 包含所有策略结果的字典
    """
    try:
        logger.info("开始运行所有ETF策略...")
        
        results = {
            "arbitrage": "",
            "position": "",
            "arbitrage_df": pd.DataFrame(),
            "success": False,
            "error": None
        }
        
        # 1. 运行套利策略
        logger.info("\n" + "="*50)
        logger.info("运行套利策略")
        logger.info("="*50)
        try:
            from .arbitrage import calculate_arbitrage_opportunity, format_arbitrage_message
            arbitrage_df = calculate_arbitrage_opportunity()
            arbitrage_msg = format_arbitrage_message(arbitrage_df)
            results["arbitrage"] = arbitrage_msg
            results["arbitrage_df"] = arbitrage_df
            logger.info("套利策略执行完成")
        except Exception as e:
            error_msg = f"套利策略执行失败: {str(e)}"
            logger.error(error_msg)
            results["arbitrage"] = f"【套利策略错误】\n{error_msg}"
            results["error"] = error_msg
        
        # 2. 运行仓位策略
        logger.info("\n" + "="*50)
        logger.info("运行仓位策略")
        logger.info("="*50)
        try:
            from .position import calculate_position_strategy
            position_msg = calculate_position_strategy()
            results["position"] = position_msg
            logger.info("仓位策略执行完成")
        except Exception as e:
            error_msg = f"仓位策略执行失败: {str(e)}"
            logger.error(error_msg)
            results["position"] = f"【仓位策略错误】\n{error_msg}"
            results["error"] = error_msg if not results["error"] else f"{results['error']}; {error_msg}"
        
        # 标记执行成功
        if not results["error"]:
            results["success"] = True
            logger.info("所有策略执行完成")
        else:
            logger.warning("策略执行完成，但存在错误")
        
        return results
        
    except Exception as e:
        error_msg = f"运行所有策略时发生未预期错误: {str(e)}"
        logger.error(error_msg)
        return {
            "arbitrage": f"【策略错误】\n{error_msg}",
            "position": f"【策略错误】\n{error_msg}",
            "arbitrage_df": pd.DataFrame(),
            "success": False,
            "error": error_msg
        }

def get_daily_report() -> str:
    """
    生成每日策略报告
    :return: 格式化后的每日报告字符串
    """
    try:
        logger.info("开始生成每日策略报告")
        
        strategies = run_all_strategies()
        
        report = "【ETF量化策略每日报告】\n\n"
        report += "📊 套利机会分析：\n"
        report += strategies["arbitrage"] + "\n\n"
        report += "📈 仓位操作建议：\n"
        report += strategies["position"] + "\n\n"
        
        if strategies["error"]:
            report += "⚠️ 执行警告：\n"
            report += f"部分策略执行过程中出现错误: {strategies['error']}\n\n"
        
        report += "💡 温馨提示：以上建议仅供参考，请结合市场情况谨慎决策！"
        
        logger.info("每日策略报告生成完成")
        return report
        
    except Exception as e:
        error_msg = f"生成每日报告失败: {str(e)}"
        logger.error(error_msg)
        return f"【报告生成错误】\n{error_msg}"

def send_daily_report_via_wechat() -> bool:
    """
    生成并发送每日策略报告到微信
    :return: 是否成功发送报告
    """
    try:
        from wechat_push import send_wechat_message
        
        # 检查是否已经发送过今日报告
        today = datetime.now().strftime("%Y-%m-%d")
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
                f.write(today)
            logger.info("每日策略报告已成功发送到微信")
        else:
            logger.error("微信消息发送失败")
            
        return success
        
    except Exception as e:
        error_msg = f"发送微信报告失败: {str(e)}"
        logger.error(error_msg)
        return False

def check_arbitrage_exit_signals() -> bool:
    """
    检查套利退出信号（持有1天后）
    :return: 是否成功检查退出信号
    """
    try:
        from position import init_trade_record
        from wechat_push import send_wechat_message
        
        logger.info("开始检查套利退出信号")
        
        init_trade_record()
        
        # 检查交易记录文件是否存在
        if not os.path.exists(Config.TRADE_RECORD_FILE):
            logger.warning("交易记录文件不存在，无法检查套利退出信号")
            return False
        
        trade_df = pd.read_csv(Config.TRADE_RECORD_FILE, encoding="utf-8")
        
        # 获取昨天的日期
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # 查找昨天执行的套利交易
        yesterday_arbitrage = trade_df[
            (trade_df["交易日期"] == yesterday) & 
            (trade_df["仓位类型"] == "套利仓") &
            (trade_df["操作类型"] == "买入")
        ]
        
        if not yesterday_arbitrage.empty:
            exit_messages = []
            for _, trade in yesterday_arbitrage.iterrows():
                try:
                    # 建议卖出套利持仓
                    exit_messages.append(
                        f"套利持仓退出建议: 卖出 {trade['ETF名称']} ({trade['ETF代码']})，"
                        f"买入价: {trade['价格']}元，建议获利了结"
                    )
                except Exception as e:
                    logger.error(f"处理套利退出交易时发生错误: {str(e)}")
                    continue
            
            if exit_messages:
                message = "【套利持仓退出提示】\n\n" + "\n".join(exit_messages)
                message += "\n\n💡 套利持仓建议持有不超过1天，请及时了结！"
                
                # 发送微信消息
                send_wechat_message(message)
                logger.info("套利退出提示已发送")
                
        logger.info("套利退出信号检查完成")
        return True
        
    except Exception as e:
        error_msg = f"检查套利退出信号失败: {str(e)}"
        logger.error(error_msg)
        return False

def run_strategy_with_retry(strategy_func, max_retries: int = 3, delay: int = 5) -> Any:
    """
    带重试的策略执行函数
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
    logger.error(f"策略模块初始化失败: {str(e)}")
    # 退回到基础日志配置
    import logging
    logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
    logging.error(f"策略模块初始化失败: {str(e)}")

# 0828-1256【strategy/__init__.py代码】一共175行代码
