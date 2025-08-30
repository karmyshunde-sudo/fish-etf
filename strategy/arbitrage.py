#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
套利策略计算模块
基于ETF净值与市场价格的差异计算套利机会
特别优化了消息推送格式，确保使用统一的消息模板
"""

import pandas as pd
import numpy as np
import logging
import akshare as ak
import os
from datetime import datetime, timedelta
from typing import Union, Optional, Tuple, Dict, Any
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    is_file_outdated
)
from .etf_scoring import get_etf_basic_info, get_etf_name
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

def calculate_arbitrage_opportunity() -> Union[pd.DataFrame, str]:
    """
    计算ETF套利机会
    
    Returns:
        Union[pd.DataFrame, str]: 套利机会DataFrame或错误消息
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始计算套利机会 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 获取ETF列表
        etf_list = load_etf_list()
        if etf_list.empty:
            logger.warning("ETF列表为空，无法计算套利机会")
            return "ETF列表为空，无法计算套利机会"
        
        # 计算套利机会
        opportunities = []
        for _, etf in etf_list.iterrows():
            try:
                # 获取ETF实时数据
                etf_code = etf["ETF代码"]
                etf_name = etf["ETF名称"]
                
                # 获取ETF实时行情
                etf_realtime = get_etf_realtime_data(etf_code)
                if etf_realtime is None:
                    continue
                
                # 获取ETF净值数据
                etf_nav = get_etf_nav_data(etf_code)
                if etf_nav is None:
                    continue
                
                # 计算折溢价率
                premium_discount = calculate_premium_discount(
                    etf_realtime["最新价"], 
                    etf_nav["单位净值"]
                )
                
                # 仅保留有套利机会的ETF（折溢价率绝对值大于阈值）
                if abs(premium_discount) >= Config.ARBITRAGE_THRESHOLD:
                    opportunities.append({
                        "ETF代码": etf_code,
                        "ETF名称": etf_name,
                        "最新价": etf_realtime["最新价"],
                        "单位净值": etf_nav["单位净值"],
                        "折溢价率": premium_discount,
                        "规模": etf["基金规模"],
                        "成交量": etf_realtime["成交量"]
                    })
            except Exception as e:
                logger.error(f"计算ETF {etf['ETF代码']} 套利机会失败: {str(e)}", exc_info=True)
        
        # 创建DataFrame
        if not opportunities:
            logger.info("未发现有效套利机会")
            return pd.DataFrame()
        
        df = pd.DataFrame(opportunities)
        # 按折溢价率绝对值排序
        df["abs_premium_discount"] = df["折溢价率"].abs()
        df = df.sort_values("abs_premium_discount", ascending=False)
        df = df.drop(columns=["abs_premium_discount"])
        
        logger.info(f"发现 {len(df)} 个套利机会")
        return df
    
    except Exception as e:
        error_msg = f"套利机会计算失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return error_msg

def load_etf_list() -> pd.DataFrame:
    """
    加载ETF列表
    
    Returns:
        pd.DataFrame: ETF列表
    """
    try:
        # 检查ETF列表文件是否存在
        if not os.path.exists(Config.ALL_ETFS_PATH):
            error_msg = "ETF列表文件不存在"
            logger.error(error_msg)
            
            # 发送错误通知
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
            
            return pd.DataFrame()
        
        # 检查ETF列表是否过期
        if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
            logger.warning("ETF列表已过期，可能影响套利计算准确性")
        
        # 读取ETF列表
        etf_list = pd.read_csv(Config.ALL_ETFS_PATH, encoding="utf-8")
        if etf_list.empty:
            logger.warning("ETF列表为空")
            return pd.DataFrame()
        
        # 确保包含必要列
        required_columns = ["ETF代码", "ETF名称", "基金规模", "日均成交额"]
        for col in required_columns:
            if col not in etf_list.columns:
                error_msg = f"ETF列表缺少必要列: {col}"
                logger.error(error_msg)
                
                # 发送错误通知
                send_wechat_message(
                    message=error_msg,
                    message_type="error"
                )
                
                return pd.DataFrame()
        
        # 筛选符合条件的ETF
        filtered_etfs = etf_list[
            (etf_list["基金规模"] >= Config.MIN_FUND_SIZE) &
            (etf_list["日均成交额"] >= Config.MIN_AVG_VOLUME)
        ]
        
        logger.info(f"加载 {len(filtered_etfs)} 只符合条件的ETF")
        return filtered_etfs
    
    except Exception as e:
        error_msg = f"加载ETF列表失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return pd.DataFrame()

def get_etf_realtime_data(etf_code: str) -> Optional[Dict[str, Any]]:
    """
    获取ETF实时行情数据
    
    Args:
        etf_code: ETF代码
    
    Returns:
        Optional[Dict[str, Any]]: 实时行情数据
    """
    try:
        # 尝试使用AkShare获取实时数据
        df = ak.fund_etf_spot_em(symbol=etf_code)
        if df.empty or len(df) == 0:
            logger.warning(f"AkShare未返回ETF {etf_code} 的实时行情")
            return None
        
        # 提取最新行情
        latest = df.iloc[0]
        
        # 提取必要字段
        realtime_data = {
            "最新价": float(latest["最新价"]),
            "成交量": float(latest["成交量"]),
            "涨跌幅": float(latest["涨跌幅"]),
            "涨跌额": float(latest["涨跌额"]),
            "开盘价": float(latest["开盘价"]),
            "最高价": float(latest["最高价"]),
            "最低价": float(latest["最低价"]),
            "总市值": float(latest["总市值"])
        }
        
        logger.debug(f"获取ETF {etf_code} 实时行情成功")
        return realtime_data
    
    except Exception as e:
        logger.error(f"获取ETF {etf_code} 实时行情失败: {str(e)}", exc_info=True)
        return None

def get_etf_nav_data(etf_code: str) -> Optional[Dict[str, Any]]:
    """
    获取ETF净值数据
    
    Args:
        etf_code: ETF代码
    
    Returns:
        Optional[Dict[str, Any]]: 净值数据
    """
    try:
        # 获取ETF净值数据
        df = ak.fund_etf_fund_info_em(symbol=etf_code, indicator="单位净值走势")
        if df.empty or len(df) == 0:
            logger.warning(f"AkShare未返回ETF {etf_code} 的净值数据")
            return None
        
        # 提取最新净值
        latest = df.iloc[-1]
        
        # 提取必要字段
        nav_data = {
            "单位净值": float(latest["单位净值"]),
            "累计净值": float(latest["累计净值"]),
            "净值日期": latest["净值日期"]
        }
        
        logger.debug(f"获取ETF {etf_code} 净值数据成功")
        return nav_data
    
    except Exception as e:
        logger.error(f"获取ETF {etf_code} 净值数据失败: {str(e)}", exc_info=True)
        return None

def calculate_premium_discount(market_price: float, nav: float) -> float:
    """
    计算折溢价率
    
    Args:
        market_price: 市场价格
        nav: 单位净值
    
    Returns:
        float: 折溢价率（百分比）
    """
    if nav <= 0:
        logger.warning(f"无效的净值: {nav}")
        return 0.0
    
    premium_discount = ((market_price - nav) / nav) * 100
    return round(premium_discount, 2)

def generate_arbitrage_message_content(df: pd.DataFrame) -> str:
    """
    生成套利机会消息内容（不包含格式）
    
    Args:
        df: 套利机会DataFrame
    
    Returns:
        str: 纯业务内容
    """
    try:
        if df.empty:
            return "【套利机会】\n未发现有效套利机会"
        
        # 生成消息内容
        content = "【套利机会】\n"
        
        # 添加前3个最佳机会
        top_opportunities = df.head(3)
        content += "今日最佳套利机会:\n"
        for i, (_, row) in enumerate(top_opportunities.iterrows(), 1):
            direction = "溢价" if row["折溢价率"] > 0 else "折价"
            content += (
                f"{i}. {row['ETF名称']}({row['ETF代码']})\n"
                f"• {direction}: {abs(row['折溢价率']):.2f}%\n"
                f"• 价格: {row['最新价']:.3f}元 | 净值: {row['单位净值']:.3f}元\n"
                f"• 规模: {row['规模']:.2f}亿元 | 成交量: {row['成交量']:.0f}\n"
            )
        
        # 添加其他机会数量
        if len(df) > 3:
            content += f"• 还有 {len(df) - 3} 个套利机会...\n"
        
        # 添加风险提示
        content += (
            "\n风险提示\n"
            "• 套利机会转瞬即逝，请及时操作\n"
            "• 交易成本可能影响套利收益\n"
            "• 市场波动可能导致策略失效"
        )
        
        return content
    
    except Exception as e:
        error_msg = f"生成套利消息内容失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return f"【套利机会】生成套利消息内容失败: {str(e)}"

def send_arbitrage_opportunity() -> bool:
    """
    计算并发送套利机会
    
    Returns:
        bool: 发送是否成功
    """
    try:
        # 获取当前北京时间用于文件命名
        beijing_now = get_beijing_time()
        today = beijing_now.date().strftime("%Y-%m-%d")
        
        # 检查是否已经发送过今日套利机会
        arbitrage_flag = os.path.join(Config.FLAG_DIR, f"arbitrage_sent_{today}.txt")
        if os.path.exists(arbitrage_flag):
            logger.info("今日套利机会已发送，跳过重复发送")
            return True
        
        # 计算套利机会
        arbitrage_df = calculate_arbitrage_opportunity()
        if isinstance(arbitrage_df, str):
            logger.warning(f"套利机会计算失败: {arbitrage_df}")
            return False
        
        # 生成消息内容（纯业务内容）
        content = generate_arbitrage_message_content(arbitrage_df)
        
        # 发送到微信（使用arbitrage类型）
        success = send_wechat_message(content, message_type="arbitrage")
        
        if success:
            # 标记已发送
            os.makedirs(os.path.dirname(arbitrage_flag), exist_ok=True)
            with open(arbitrage_flag, "w", encoding="utf-8") as f:
                f.write(beijing_now.strftime("%Y-%m-%d %H:%M:%S"))
            logger.info("套利机会已成功发送到微信")
        else:
            logger.error("微信消息发送失败")
            
            # 发送错误通知
            send_wechat_message(
                message="套利机会计算成功，但微信消息发送失败",
                message_type="error"
            )
        
        return success
    
    except Exception as e:
        error_msg = f"发送套利机会失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return False

def get_arbitrage_history(days: int = 7) -> pd.DataFrame:
    """
    获取套利历史数据
    
    Args:
        days: 查询天数
    
    Returns:
        pd.DataFrame: 套利历史数据
    """
    try:
        history = []
        beijing_now = get_beijing_time()
        
        for i in range(days):
            date = (beijing_now - timedelta(days=i)).date().strftime("%Y-%m-%d")
            flag_file = os.path.join(Config.FLAG_DIR, f"arbitrage_sent_{date}.txt")
            
            if os.path.exists(flag_file):
                # 读取当日套利数据
                # 这里简化处理，实际应从数据库或文件中读取历史套利数据
                history.append({
                    "日期": date,
                    "机会数量": 3,  # 示例数据
                    "最大折溢价率": 2.5,  # 示例数据
                    "最小折溢价率": -1.8  # 示例数据
                })
        
        if not history:
            logger.info("未找到套利历史数据")
            return pd.DataFrame()
        
        return pd.DataFrame(history)
    
    except Exception as e:
        error_msg = f"获取套利历史数据失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return pd.DataFrame()

def analyze_arbitrage_performance() -> str:
    """
    分析套利表现
    
    Returns:
        str: 分析结果
    """
    try:
        # 获取历史数据
        history_df = get_arbitrage_history()
        if history_df.empty:
            return "【套利表现分析】\n• 无历史数据可供分析"
        
        # 计算统计指标
        avg_opportunities = history_df["机会数量"].mean()
        max_premium = history_df["最大折溢价率"].max()
        min_discount = history_df["最小折溢价率"].min()
        
        # 生成分析报告
        report = "【套利表现分析】\n"
        report += f"• 近期平均每天发现 {avg_opportunities:.1f} 个套利机会\n"
        report += f"• 最大溢价率: {max_premium:.2f}%\n"
        report += f"• 最大折价率: {min_discount:.2f}%\n\n"
        
        # 添加趋势分析
        if len(history_df) >= 3:
            trend = "上升" if history_df["机会数量"].iloc[-3:].mean() > history_df["机会数量"].iloc[:3].mean() else "下降"
            report += f"• 套利机会数量呈{trend}趋势\n"
        
        # 添加建议
        if max_premium > 2.0:
            report += "\n💡 建议：溢价率较高时，可考虑卖出ETF\n"
        if min_discount < -2.0:
            report += "💡 建议：折价率较高时，可考虑买入ETF\n"
        
        return report
    
    except Exception as e:
        error_msg = f"套利表现分析失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return f"【套利表现分析】{error_msg}"

def check_arbitrage_exit_signals() -> bool:
    """
    检查套利退出信号（持有1天后）
    
    Returns:
        bool: 是否发现需要退出的套利交易
    """
    try:
        logger.info("开始检查套利退出信号")
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 检查交易记录文件是否存在
        if not os.path.exists(Config.TRADE_RECORD_FILE):
            logger.warning("交易记录文件不存在，无法检查套利退出信号")
            return False
        
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
            
            # 生成退出信号消息内容
            exit_content = "【套利退出信号】\n"
            exit_content += f"发现 {len(yesterday_arbitrage)} 条需要退出的套利交易\n\n"
            
            for _, row in yesterday_arbitrage.iterrows():
                exit_content += (
                    f"• {row['ETF名称']}({row['ETF代码']})："
                    f"已持有1天，建议退出\n"
                )
            
            # 发送退出信号
            send_wechat_message(exit_content)
            return True
        
        logger.info("未发现需要退出的套利交易")
        return False
    
    except Exception as e:
        error_msg = f"检查套利退出信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return False

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 初始化日志
    logger.info("套利策略模块初始化完成")
    
except Exception as e:
    logger.error(f"套利策略模块初始化失败: {str(e)}", exc_info=True)
    
    try:
        # 退回到基础日志配置
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(f"套利策略模块初始化失败: {str(e)}")
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(f"套利策略模块初始化失败: {str(e)}")
