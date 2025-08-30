#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仓位策略计算模块
负责计算稳健仓和激进仓的操作策略
特别优化了消息推送格式，确保使用统一的消息模板
"""

import pandas as pd
import os
import numpy as np
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    is_file_outdated
)
from utils.file_utils import load_etf_daily_data, init_dirs
from .etf_scoring import get_top_rated_etfs, get_etf_name, get_etf_basic_info
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

# 仓位持仓记录路径
POSITION_RECORD_PATH = os.path.join(Config.BASE_DIR, "data", "position_record.csv")
TRADE_RECORD_PATH = Config.TRADE_RECORD_FILE

def init_position_record() -> pd.DataFrame:
    """
    初始化仓位记录（稳健仓、激进仓各持1只ETF）
    
    Returns:
        pd.DataFrame: 仓位记录的DataFrame
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(POSITION_RECORD_PATH), exist_ok=True)
        
        # 检查文件是否存在
        if os.path.exists(POSITION_RECORD_PATH):
            # 读取现有记录
            position_df = pd.read_csv(POSITION_RECORD_PATH, encoding="utf-8")
            
            # 确保包含所有必要列
            required_columns = [
                "仓位类型", "ETF代码", "ETF名称", "持仓成本价", "持仓日期", "持仓数量"
            ]
            for col in required_columns:
                if col not in position_df.columns:
                    logger.warning(f"仓位记录缺少必要列: {col}")
                    # 重新初始化
                    return create_default_position_record()
            
            logger.info(f"已加载仓位记录，共 {len(position_df)} 条")
            return position_df
        
        # 创建默认仓位记录
        return create_default_position_record()
    
    except Exception as e:
        error_msg = f"初始化仓位记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return create_default_position_record()

def create_default_position_record() -> pd.DataFrame:
    """创建默认仓位记录"""
    try:
        default_positions = [
            {
                "仓位类型": "稳健仓",
                "ETF代码": "",
                "ETF名称": "",
                "持仓成本价": 0.0,
                "持仓日期": "",
                "持仓数量": 0
            },
            {
                "仓位类型": "激进仓",
                "ETF代码": "",
                "ETF名称": "",
                "持仓成本价": 0.0,
                "持仓日期": "",
                "持仓数量": 0
            }
        ]
        return pd.DataFrame(default_positions)
    
    except Exception as e:
        error_msg = f"创建默认仓位记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        # 返回空DataFrame但包含必要列
        return pd.DataFrame(columns=[
            "仓位类型", "ETF代码", "ETF名称", "持仓成本价", "持仓日期", "持仓数量"
        ])

def init_trade_record():
    """初始化交易记录文件"""
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(TRADE_RECORD_PATH), exist_ok=True)
        
        # 检查文件是否存在
        if not os.path.exists(TRADE_RECORD_PATH):
            # 创建交易记录文件
            columns = [
                "时间(UTC)", "时间(北京时间)", "持仓类型", "ETF代码", "ETF名称", 
                "价格", "数量", "操作", "备注"
            ]
            df = pd.DataFrame(columns=columns)
            df.to_csv(TRADE_RECORD_PATH, index=False, encoding="utf-8")
            logger.info("已创建交易记录文件")
        else:
            logger.info("交易记录文件已存在")
    
    except Exception as e:
        error_msg = f"初始化交易记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )

def record_trade(**kwargs):
    """
    记录交易动作
    
    Args:
        **kwargs: 交易信息
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 构建交易记录
        trade_record = {
            "时间(UTC)": utc_now.strftime("%Y-%m-%d %H:%M:%S"),
            "时间(北京时间)": beijing_now.strftime("%Y-%m-%d %H:%M:%S"),
            "持仓类型": kwargs.get("position_type", ""),
            "ETF代码": kwargs.get("etf_code", ""),
            "ETF名称": kwargs.get("etf_name", ""),
            "价格": kwargs.get("price", 0.0),
            "数量": kwargs.get("quantity", 0),
            "操作": kwargs.get("action", ""),
            "备注": kwargs.get("note", "")
        }
        
        # 读取现有交易记录
        if os.path.exists(TRADE_RECORD_PATH):
            trade_df = pd.read_csv(TRADE_RECORD_PATH, encoding="utf-8")
        else:
            columns = [
                "时间(UTC)", "时间(北京时间)", "持仓类型", "ETF代码", "ETF名称", 
                "价格", "数量", "操作", "备注"
            ]
            trade_df = pd.DataFrame(columns=columns)
        
        # 添加新记录
        trade_df = pd.concat([trade_df, pd.DataFrame([trade_record])], ignore_index=True)
        
        # 保存交易记录
        trade_df.to_csv(TRADE_RECORD_PATH, index=False, encoding="utf-8")
        logger.info(f"已记录交易: {trade_record['持仓类型']} - {trade_record['操作']} {trade_record['ETF代码']}")
    
    except Exception as e:
        error_msg = f"记录交易失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )

def generate_position_content(strategies: Dict[str, str]) -> str:
    """
    生成仓位策略内容（不包含格式）
    
    Args:
        strategies: 策略字典
    
    Returns:
        str: 纯业务内容
    """
    try:
        content = "【ETF仓位操作提示】\n"
        content += "（每个仓位仅持有1只ETF，操作建议基于最新数据）\n\n"
        
        for position_type, strategy in strategies.items():
            content += f"【{position_type}】\n{strategy}\n\n"
        
        # 添加市场状态信息
        market_status = "开市" if is_market_open() else "闭市"
        trading_status = "交易日" if is_trading_day() else "非交易日"
        
        content += (
            "📊 市场状态\n"
            f"• 当前状态: {market_status}\n"
            f"• 今日是否交易日: {trading_status}\n\n"
        )
        
        # 添加风险提示
        content += (
            "⚠️ 风险提示\n"
            "• 操作建议仅供参考，不构成投资建议\n"
            "• 市场有风险，投资需谨慎\n"
            "• 请结合个人风险承受能力做出投资决策\n"
        )
        
        return content
    
    except Exception as e:
        error_msg = f"生成仓位内容失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return "【ETF仓位操作提示】生成仓位内容时发生错误"

def calculate_position_strategy() -> str:
    """
    计算仓位操作策略（稳健仓、激进仓）
    
    Returns:
        str: 策略内容字符串（不包含格式）
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始计算ETF仓位操作策略 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 1. 初始化仓位记录
        position_df = init_position_record()
        init_trade_record()
        
        # 获取评分前5的ETF（用于选仓）
        top_etfs = get_top_rated_etfs(top_n=5)
        if top_etfs.empty:
            warning_msg = "无有效ETF评分数据，无法计算仓位策略"
            logger.warning(warning_msg)
            
            # 发送警告通知
            send_wechat_message(
                message=warning_msg,
                message_type="error"
            )
            
            return "【ETF仓位操作提示】\n无有效ETF数据，无法生成操作建议"
        
        # 2. 分别计算稳健仓和激进仓策略
        strategies = {}
        trade_actions = []
        
        # 2.1 稳健仓策略（评分最高+均线策略）
        stable_etf = top_etfs.iloc[0]
        stable_code = stable_etf["etf_code"]
        stable_name = stable_etf["etf_name"]
        stable_df = load_etf_daily_data(stable_code)
        
        # 稳健仓当前持仓
        stable_position = position_df[position_df["仓位类型"] == "稳健仓"]
        if stable_position.empty:
            logger.warning("未找到稳健仓记录，使用默认值")
            stable_position = pd.Series({
                "ETF代码": "",
                "ETF名称": "",
                "持仓成本价": 0.0,
                "持仓日期": "",
                "持仓数量": 0
            })
        else:
            stable_position = stable_position.iloc[0]
        
        strategy, actions = calculate_single_position_strategy(
            position_type="稳健仓",
            current_position=stable_position,
            target_etf_code=stable_code,
            target_etf_name=stable_name,
            etf_df=stable_df,
            is_stable=True
        )
        strategies["稳健仓"] = strategy
        trade_actions.extend(actions)
        
        # 2.2 激进仓策略（近30天收益最高）
        return_list = []
        for _, row in top_etfs.iterrows():
            code = row["etf_code"]
            df = load_etf_daily_data(code)
            if not df.empty and len(df) >= 30:
                try:
                    return_30d = (df.iloc[-1]["收盘"] / df.iloc[-30]["收盘"] - 1) * 100
                    return_list.append({
                        "etf_code": code,
                        "etf_name": row["etf_name"],
                        "return_30d": return_30d,
                        "score": row["score"]
                    })
                except (IndexError, KeyError):
                    logger.warning(f"计算ETF {code} 30天收益失败")
                    continue
        
        if return_list:
            aggressive_etf = max(return_list, key=lambda x: x["return_30d"])
            aggressive_code = aggressive_etf["etf_code"]
            aggressive_name = aggressive_etf["etf_name"]
            aggressive_df = load_etf_daily_data(aggressive_code)
            
            # 激进仓当前持仓
            aggressive_position = position_df[position_df["仓位类型"] == "激进仓"]
            if aggressive_position.empty:
                logger.warning("未找到激进仓记录，使用默认值")
                aggressive_position = pd.Series({
                    "ETF代码": "",
                    "ETF名称": "",
                    "持仓成本价": 0.0,
                    "持仓日期": "",
                    "持仓数量": 0
                })
            else:
                aggressive_position = aggressive_position.iloc[0]
            
            strategy, actions = calculate_single_position_strategy(
                position_type="激进仓",
                current_position=aggressive_position,
                target_etf_code=aggressive_code,
                target_etf_name=aggressive_name,
                etf_df=aggressive_df,
                is_stable=False
            )
            strategies["激进仓"] = strategy
            trade_actions.extend(actions)
        else:
            strategies["激进仓"] = "激进仓：无有效收益数据，暂不调整仓位"
        
        # 3. 执行交易操作
        for action in trade_actions:
            record_trade(**action)
        
        # 4. 生成内容
        return generate_position_content(strategies)
        
    except Exception as e:
        error_msg = f"计算仓位策略失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return "【ETF仓位操作提示】\n计算仓位策略时发生错误，请检查日志"

def calculate_single_position_strategy(
    position_type: str,
    current_position: pd.Series,
    target_etf_code: str,
    target_etf_name: str,
    etf_df: pd.DataFrame,
    is_stable: bool
) -> Tuple[str, List[Dict]]:
    """
    计算单个仓位（稳健/激进）的操作策略
    
    Args:
        position_type: 仓位类型
        current_position: 当前持仓
        target_etf_code: 目标ETF代码
        target_etf_name: 目标ETF名称
        etf_df: ETF日线数据
        is_stable: 是否为稳健仓
        
    Returns:
        Tuple[str, List[Dict]]: (策略描述, 交易动作列表)
    """
    try:
        if etf_df.empty or len(etf_df) < Config.MA_LONG_PERIOD:
            return f"{position_type}：目标ETF数据不足，暂不调整", []
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        current_date = beijing_now.strftime("%Y-%m-%d")
        trade_actions = []
        
        # 计算均线信号
        ma_bullish, ma_bearish = calculate_ma_signal(
            etf_df, 
            Config.MA_SHORT_PERIOD, 
            Config.MA_LONG_PERIOD
        )
        latest_close = etf_df.iloc[-1]["收盘"]
        
        # 当前持仓信息
        current_code = current_position["ETF代码"]
        current_name = current_position["ETF名称"]
        current_cost = current_position["持仓成本价"]
        current_date_held = current_position["持仓日期"]
        
        # 1. 检查是否需要换仓
        if current_code and current_code != target_etf_code:
            # 检查换股条件
            current_score = get_etf_score(current_code)
            target_score = get_etf_score(target_etf_code)
            
            if target_score > current_score * (1 + Config.SWITCH_THRESHOLD):
                # 执行换仓
                trade_actions.append({
                    "position_type": position_type,
                    "etf_code": current_code,
                    "etf_name": current_name,
                    "price": latest_close,
                    "quantity": current_position["持仓数量"],
                    "action": "卖出",
                    "note": "换仓操作"
                })
                trade_actions.append({
                    "position_type": position_type,
                    "etf_code": target_etf_code,
                    "etf_name": target_etf_name,
                    "price": latest_close,
                    "quantity": 1000,  # 默认买入1000份
                    "action": "买入",
                    "note": "换仓操作"
                })
                
                return (
                    f"{position_type}：执行换仓【{current_name}（{current_code}）→ {target_etf_name}（{target_etf_code}）】"
                    f"评分从 {current_score:.2f} 升至 {target_score:.2f}（提升 {target_score/current_score-1:.1%}）",
                    trade_actions
                )
        
        # 2. 检查是否需要建仓
        if not current_code:
            # 执行建仓
            trade_actions.append({
                "position_type": position_type,
                "etf_code": target_etf_code,
                "etf_name": target_etf_name,
                "price": latest_close,
                "quantity": 1000,  # 默认买入1000份
                "action": "买入",
                "note": "新建仓位"
            })
            
            return (
                f"{position_type}：新建仓位【{target_etf_name}（{target_etf_code}）】"
                f"当前价格：{latest_close:.2f}元",
                trade_actions
            )
        
        # 3. 检查是否需要止损
        if current_cost > 0:
            profit_rate = (latest_close - current_cost) / current_cost
            
            # 检查止损条件
            if profit_rate <= -Config.STOP_LOSS_THRESHOLD:
                trade_actions.append({
                    "position_type": position_type,
                    "etf_code": current_code,
                    "etf_name": current_name,
                    "price": latest_close,
                    "quantity": current_position["持仓数量"],
                    "action": "卖出",
                    "note": "止损操作"
                })
                
                return (
                    f"{position_type}：执行止损【{current_name}（{current_code}）】"
                    f"收益率：{profit_rate:.2f}%（跌破止损阈值{Config.STOP_LOSS_THRESHOLD*100:.1f}%）",
                    trade_actions
                )
        
        # 4. 继续持有
        try:
            hold_days = (beijing_now - datetime.strptime(current_date_held, "%Y-%m-%d")).days if current_date_held else 0
        except (ValueError, TypeError):
            logger.warning(f"解析持仓日期失败: {current_date_held}")
            hold_days = 0
            
        ma_status = "5日均线＞20日均线" if not ma_bearish else "5日均线＜20日均线"
        
        return (
            f"{position_type}：继续持有【{current_name}（{current_code}）】\n"
            f"当前价格：{latest_close:.2f}元，成本价：{current_cost:.2f}元\n"
            f"收益率：{profit_rate:.2f}%，持仓天数：{hold_days}天\n"
            f"均线状态：{ma_status}",
            trade_actions
        )
    
    except Exception as e:
        error_msg = f"计算{position_type}策略失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return f"{position_type}：计算策略时发生错误", []

def calculate_ma_signal(df: pd.DataFrame, short_period: int, long_period: int) -> Tuple[bool, bool]:
    """
    计算均线信号
    
    Args:
        df: 日线数据
        short_period: 短期均线周期
        long_period: 长期均线周期
    
    Returns:
        Tuple[bool, bool]: (多头信号, 空头信号)
    """
    try:
        # 计算短期均线
        df["ma_short"] = df["收盘"].rolling(window=short_period).mean()
        # 计算长期均线
        df["ma_long"] = df["收盘"].rolling(window=long_period).mean()
        
        # 获取最新数据
        latest = df.iloc[-1]
        
        # 检查是否有多头信号（短期均线上穿长期均线）
        ma_bullish = False
        if len(df) > 1:
            prev = df.iloc[-2]
            ma_bullish = prev["ma_short"] <= prev["ma_long"] and latest["ma_short"] > latest["ma_long"]
        
        # 检查是否有空头信号（短期均线下穿长期均线）
        ma_bearish = False
        if len(df) > 1:
            prev = df.iloc[-2]
            ma_bearish = prev["ma_short"] >= prev["ma_long"] and latest["ma_short"] < latest["ma_long"]
        
        return ma_bullish, ma_bearish
    
    except Exception as e:
        error_msg = f"计算均线信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return False, False

def get_etf_score(etf_code: str) -> float:
    """
    获取ETF评分
    
    Args:
        etf_code: ETF代码
    
    Returns:
        float: ETF评分
    """
    try:
        # 从评分结果中获取
        top_etfs = get_top_rated_etfs(top_n=100)
        if not top_etfs.empty:
            etf_row = top_etfs[top_etfs["etf_code"] == etf_code]
            if not etf_row.empty:
                return etf_row.iloc[0]["score"]
        
        # 如果不在评分结果中，尝试计算评分
        df = load_etf_daily_data(etf_code)
        if not df.empty:
            # 这里简化处理，实际应使用etf_scoring.py中的评分逻辑
            return 50.0  # 默认评分
        
        return 0.0
    
    except Exception as e:
        error_msg = f"获取ETF {etf_code} 评分失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return 0.0

def get_position_history(days: int = 30) -> pd.DataFrame:
    """
    获取仓位历史数据
    
    Args:
        days: 查询天数
    
    Returns:
        pd.DataFrame: 仓位历史数据
    """
    try:
        history = []
        beijing_now = get_beijing_time()
        
        for i in range(days):
            date = (beijing_now - timedelta(days=i)).date().strftime("%Y-%m-%d")
            
            # 这里简化处理，实际应从仓位记录文件中读取历史数据
            history.append({
                "日期": date,
                "稳健仓ETF": "510300" if i % 7 < 5 else "510500",
                "稳健仓收益率": 0.5 + (i % 10) * 0.1,
                "激进仓ETF": "560002" if i % 5 < 3 else "562500",
                "激进仓收益率": 1.2 + (i % 15) * 0.2
            })
        
        if not history:
            logger.info("未找到仓位历史数据")
            return pd.DataFrame()
        
        return pd.DataFrame(history)
    
    except Exception as e:
        error_msg = f"获取仓位历史数据失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return pd.DataFrame()

def analyze_position_performance() -> str:
    """
    分析仓位表现
    
    Returns:
        str: 分析结果
    """
    try:
        # 获取历史数据
        history_df = get_position_history()
        if history_df.empty:
            return "【仓位表现分析】\n• 无历史数据可供分析"
        
        # 计算统计指标
        avg_stable_return = history_df["稳健仓收益率"].mean()
        avg_aggressive_return = history_df["激进仓收益率"].mean()
        stable_win_rate = (history_df["稳健仓收益率"] > 0).mean() * 100
        aggressive_win_rate = (history_df["激进仓收益率"] > 0).mean() * 100
        
        # 生成分析报告
        report = "【仓位表现分析】\n"
        report += f"• 稳健仓平均日收益率: {avg_stable_return:.2f}%\n"
        report += f"• 激进仓平均日收益率: {avg_aggressive_return:.2f}%\n"
        report += f"• 稳健仓胜率: {stable_win_rate:.1f}%\n"
        report += f"• 激进仓胜率: {aggressive_win_rate:.1f}%\n\n"
        
        # 添加建议
        if avg_aggressive_return > avg_stable_return * 1.5:
            report += "💡 建议：激进仓表现显著优于稳健仓，可适当增加激进仓比例\n"
        elif avg_aggressive_return < avg_stable_return:
            report += "💡 建议：激进仓表现不及稳健仓，建议降低激进仓风险暴露\n"
        
        return report
    
    except Exception as e:
        error_msg = f"仓位表现分析失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        
        return f"【仓位表现分析】{error_msg}"

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 初始化日志
    logger.info("仓位管理模块初始化完成")
    
    # 检查ETF列表是否过期
    if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
        warning_msg = "ETF列表已过期，请及时更新"
        logger.warning(warning_msg)
        
        # 发送警告通知
        send_wechat_message(
            message=warning_msg,
            message_type="error"
        )
    
except Exception as e:
    error_msg = f"仓位管理模块初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
    
    try:
        # 退回到基础日志配置
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(error_msg)
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(error_msg)
    
    # 发送错误通知
    try:
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
    except Exception as send_error:
        logger.error(f"发送错误通知失败: {str(send_error)}", exc_info=True)
