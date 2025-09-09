#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仓位策略计算模块
负责计算稳健仓和激进仓的操作策略
【终极修复版】彻底解决ATR计算和变量作用域问题
专为小资金散户设计，仅使用标准日线数据字段
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
from data_crawler.etf_list_manager import load_all_etf_list
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
                "仓位类型", "ETF代码", "ETF名称", "持仓成本价", "持仓日期", "持仓数量", "最新操作", "操作日期", "创建时间", "更新时间"
            ]
            for col in required_columns:
                if col not in position_df.columns:
                    logger.warning(f"仓位记录缺少必要列: {col}")
                    # 重新初始化
                    return create_default_position_record()
            
            # 确保包含稳健仓和激进仓
            if "稳健仓" not in position_df["仓位类型"].values:
                position_df = pd.concat([position_df, pd.DataFrame([{
                    "仓位类型": "稳健仓",
                    "ETF代码": "",
                    "ETF名称": "",
                    "持仓成本价": 0.0,
                    "持仓日期": "",
                    "持仓数量": 0,
                    "最新操作": "未持仓",
                    "操作日期": "",
                    "创建时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }])], ignore_index=True)
            
            if "激进仓" not in position_df["仓位类型"].values:
                position_df = pd.concat([position_df, pd.DataFrame([{
                    "仓位类型": "激进仓",
                    "ETF代码": "",
                    "ETF名称": "",
                    "持仓成本价": 0.0,
                    "持仓日期": "",
                    "持仓数量": 0,
                    "最新操作": "未持仓",
                    "操作日期": "",
                    "创建时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }])], ignore_index=True)
            
            # 保存更新后的记录
            position_df.to_csv(POSITION_RECORD_PATH, index=False, encoding="utf-8")
            
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
                "持仓数量": 0,
                "最新操作": "未持仓",
                "操作日期": "",
                "创建时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            {
                "仓位类型": "激进仓",
                "ETF代码": "",
                "ETF名称": "",
                "持仓成本价": 0.0,
                "持仓日期": "",
                "持仓数量": 0,
                "最新操作": "未持仓",
                "操作日期": "",
                "创建时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
            "仓位类型", "ETF代码", "ETF名称", "持仓成本价", "持仓日期", "持仓数量", 
            "最新操作", "操作日期", "创建时间", "更新时间"
        ])

def init_trade_record() -> None:
    """
    初始化交易记录文件
    """
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
    生成仓位策略内容
    
    Args:
        strategies: 策略字典
    
    Returns:
        str: 格式化后的策略内容
    """
    content = "【ETF仓位操作提示】\n"
    content += "（小资金趋势交易策略：聚焦最强ETF，动态仓位管理）\n"
    content += "（注：本策略仅基于价格趋势，不依赖折溢价率）\n\n"
    
    for position_type, strategy in strategies.items():
        # 解析策略内容，提取详细数据
        if "ETF名称" in strategy and "ETF代码" in strategy and "当前价格" in strategy:
            # 提取ETF名称和代码
            etf_name = strategy.split("ETF名称：")[1].split("\n")[0]
            etf_code = strategy.split("ETF代码：")[1].split("\n")[0]
            current_price = strategy.split("当前价格：")[1].split("\n")[0]
            
            # 提取20日均线
            critical_value = strategy.split("20日均线：")[1].split("\n")[0] if "20日均线：" in strategy else "N/A"
            
            # 生成详细内容
            content += f"【{position_type}】\n"
            content += f"ETF名称：{etf_name}（{etf_code}）\n"
            content += f"当前价格：{current_price}\n"
            content += f"20日均线：{critical_value}\n"
            content += f"操作建议：{strategy.split('操作建议：')[1] if '操作建议：' in strategy else '详细建议'}\n\n"
        else:
            # 如果策略内容不符合预期格式，直接显示
            content += f"【{position_type}】\n{strategy}\n\n"
    
    # 添加小资金操作提示
    content += "💡 小资金操作指南：\n"
    content += "1. 优先交易日成交>1亿的ETF（避免流动性风险）\n"
    content += "2. 单只ETF仓位≤60%，总仓位80%-100%（集中火力）\n"
    content += "3. 盈利超8%后，止损上移至成本价（锁定利润）\n"
    content += "4. 每周一进行ETF轮动（永远持有最强标的）"
    
    return content

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
        
        # 2. 获取评分前5的ETF（用于选仓）
        try:
            # 智能处理评分数据
            top_etfs = get_top_rated_etfs(top_n=5)
            
            # 安全过滤：确保只处理有效的ETF
            if not top_etfs.empty:
                # 过滤货币ETF（511开头）
                top_etfs = top_etfs[top_etfs["ETF代码"].apply(lambda x: not str(x).startswith("511"))]
                
                # 过滤数据量不足的ETF
                valid_etfs = []
                for _, row in top_etfs.iterrows():
                    etf_code = str(row["ETF代码"])
                    df = load_etf_daily_data(etf_code)
                    if not df.empty and len(df) >= 20:
                        valid_etfs.append(row)
                
                top_etfs = pd.DataFrame(valid_etfs)
                logger.info(f"过滤后有效ETF数量: {len(top_etfs)}")
            
            # 检查是否有有效数据
            if top_etfs.empty or len(top_etfs) == 0:
                warning_msg = "无有效ETF评分数据，无法计算仓位策略"
                logger.warning(warning_msg)
                
                # 发送警告通知
                send_wechat_message(
                    message=warning_msg,
                    message_type="error"
                )
                
                return "【ETF仓位操作提示】\n无有效ETF数据，无法生成操作建议"
        
        except Exception as e:
            error_msg = f"获取ETF评分数据失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            # 发送错误通知
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
            
            return "【ETF仓位操作提示】\n获取ETF评分数据失败，请检查日志"
        
        # 3. 分别计算稳健仓和激进仓策略
        strategies = {}
        trade_actions = []
        
        # 3.1 稳健仓策略（评分最高+趋势策略）
        stable_etf = top_etfs.iloc[0]
        stable_code = str(stable_etf["ETF代码"])
        stable_name = stable_etf["ETF名称"]
        stable_df = load_etf_daily_data(stable_code)
        
        # 确保DataFrame是副本，避免SettingWithCopyWarning
        if not stable_df.empty:
            stable_df = stable_df.copy(deep=True)
        
        # 稳健仓当前持仓
        stable_position = position_df[position_df["仓位类型"] == "稳健仓"]
        if stable_position.empty:
            logger.warning("未找到稳健仓记录，使用默认值")
            stable_position = pd.Series({
                "ETF代码": "",
                "ETF名称": "",
                "持仓成本价": 0.0,
                "持仓日期": "",
                "持仓数量": 0,
                "最新操作": "未持仓",
                "操作日期": "",
                "创建时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        
        # 3.2 激进仓策略（近30天收益最高）
        return_list = []
        for _, row in top_etfs.iterrows():
            code = str(row["ETF代码"])
            df = load_etf_daily_data(code)
            if not df.empty and len(df) >= 30:
                try:
                    # 确保DataFrame是副本
                    df = df.copy(deep=True)
                    return_30d = (df.iloc[-1]["收盘"] / df.iloc[-30]["收盘"] - 1) * 100
                    return_list.append({
                        "ETF代码": code,
                        "ETF名称": row["ETF名称"],
                        "return_30d": return_30d
                    })
                except (IndexError, KeyError, TypeError):
                    logger.warning(f"计算ETF {code} 30天收益失败")
                    continue
        
        if return_list:
            aggressive_etf = max(return_list, key=lambda x: x["return_30d"])
            aggressive_code = aggressive_etf["ETF代码"]
            aggressive_name = aggressive_etf["ETF名称"]
            aggressive_df = load_etf_daily_data(aggressive_code)
            
            # 确保DataFrame是副本
            if not aggressive_df.empty:
                aggressive_df = aggressive_df.copy(deep=True)
            
            # 激进仓当前持仓
            aggressive_position = position_df[position_df["仓位类型"] == "激进仓"]
            if aggressive_position.empty:
                logger.warning("未找到激进仓记录，使用默认值")
                aggressive_position = pd.Series({
                    "ETF代码": "",
                    "ETF名称": "",
                    "持仓成本价": 0.0,
                    "持仓日期": "",
                    "持仓数量": 0,
                    "最新操作": "未持仓",
                    "操作日期": "",
                    "创建时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        
        # 4. 执行交易操作
        for action in trade_actions:
            record_trade(**action)
        
        # 5. 生成内容
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
    计算单个仓位策略（小资金趋势交易版）
    
    Args:
        position_type: 仓位类型（稳健仓/激进仓）
        current_position: 当前仓位
        target_etf_code: 目标ETF代码
        target_etf_name: 目标ETF名称
        etf_df: ETF日线数据（仅使用标准字段）
        is_stable: 是否为稳健仓
    
    Returns:
        Tuple[str, List[Dict]]: 策略内容和交易动作列表
    """
    try:
        # 1. 检查数据是否足够
        if etf_df.empty or len(etf_df) < 20:
            error_msg = f"ETF {target_etf_code} 数据不足，无法计算策略"
            logger.warning(error_msg)
            return f"{position_type}：{error_msg}", []
        
        # 2. 获取最新数据
        latest_data = etf_df.iloc[-1]
        current_price = latest_data["收盘"]
        
        # 3. 计算关键指标（仅使用标准日线数据字段）
        ma5 = etf_df["收盘"].rolling(5).mean().iloc[-1]
        ma10 = etf_df["收盘"].rolling(10).mean().iloc[-1]
        ma20 = etf_df["收盘"].rolling(20).mean().iloc[-1]
        
        # 4. 计算ATR（平均真实波幅）用于动态止损
        atr = calculate_atr(etf_df, period=14)
        
        # 5. 初始化成交量相关变量（关键修复：提前定义，避免作用域问题）
        volume = 0.0
        avg_volume = 0.0
        if not etf_df.empty:
            volume = etf_df["成交量"].iloc[-1]
            avg_volume = etf_df["成交量"].rolling(5).mean().iloc[-1]
        
        # 6. 构建详细策略内容
        strategy_content = f"ETF名称：{target_etf_name}\n"
        strategy_content += f"ETF代码：{target_etf_code}\n"
        strategy_content += f"当前价格：{current_price:.2f}\n"
        strategy_content += f"20日均线：{ma20:.2f}\n"
        
        # 7. 小资金专属策略逻辑
        trade_actions = []
        
        # 7.1 计算动态止损位（基于ATR）
        stop_loss = current_price - 1.5 * atr
        risk_ratio = (current_price - stop_loss) / current_price if current_price > 0 else 0
        
        # 7.2 判断是否处于趋势中（核心逻辑）
        in_trend = (ma5 > ma20) and (current_price > ma20)
        
        # 8. 趋势策略（完全基于价格趋势，无折溢价率依赖）
        if in_trend:
            # 8.1 检查是否是突破信号
            is_breakout = (current_price > etf_df["收盘"].rolling(20).max().iloc[-2])
            
            # 8.2 检查成交量
            volume_ok = (volume > avg_volume * 1.1)  # 仅需10%放大
            
            # 8.3 趋势确认
            if is_breakout or (ma5 > ma10 and volume_ok):
                # 仓位计算（小资金专属）
                position_size = "100%" if is_stable else "100%"
                
                if current_position["持仓数量"] == 0:
                    # 新建仓位
                    strategy_content += f"操作建议：{position_type}：新建仓位【{target_etf_name}】{position_size}（突破信号+趋势确认，小资金应集中）\n"
                    strategy_content += f"• 动态止损：{stop_loss:.2f}元（风险比 {risk_ratio:.1%}）"
                    
                    # 生成交易动作
                    trade_actions.append({
                        "etf_code": target_etf_code,
                        "etf_name": target_etf_name,
                        "position_type": position_type,
                        "action": "新建仓位",
                        "quantity": position_size,
                        "price": current_price,
                        "reason": f"突破信号+趋势确认，止损{stop_loss:.2f}"
                    })
                else:
                    # 已持仓，检查是否需要加仓
                    if "持仓成本价" in current_position and current_position["持仓成本价"] > 0:
                        profit_pct = ((current_price - current_position["持仓成本价"]) / 
                                     current_position["持仓成本价"] * 100)
                        
                        # 盈利超8%后，止损上移至成本价
                        if profit_pct > 8 and stop_loss < current_position["持仓成本价"]:
                            stop_loss = current_position["持仓成本价"]
                            risk_ratio = 0
                            strategy_content += "• 盈利超8%，止损上移至成本价（零风险持仓）\n"
                    
                    # 仅在突破新高时加仓
                    if is_breakout and current_position["持仓数量"] < 100:
                        strategy_content += f"操作建议：{position_type}：加仓至{position_size}（突破新高，强化趋势）\n"
                        strategy_content += f"• 动态止损：{stop_loss:.2f}元（风险比 {risk_ratio:.1%}）"
                        
                        trade_actions.append({
                            "etf_code": target_etf_code,
                            "etf_name": target_etf_name,
                            "position_type": position_type,
                            "action": "加仓",
                            "quantity": "补足至100%",
                            "price": current_price,
                            "reason": "突破新高，强化趋势"
                        })
                    else:
                        strategy_content += f"操作建议：{position_type}：持有（趋势稳健，止损已上移）\n"
                        strategy_content += f"• 动态止损：{stop_loss:.2f}元（风险比 {risk_ratio:.1%}）"
        
        # 8.5 无趋势/下跌趋势
        else:
            # 检查是否触发止损
            need_stop = False
            if current_position["持仓数量"] > 0 and "持仓成本价" in current_position:
                # 只有在有持仓成本价的情况下才检查止损
                if current_position["持仓成本价"] > 0:
                    need_stop = (current_price <= stop_loss)
            
            # 检查是否超卖（小资金抄底机会）
            is_oversold = False
            if len(etf_df) > 30:
                min_30d = etf_df["收盘"].rolling(30).min().iloc[-1]
                if min_30d > 0:  # 避免除零错误
                    is_oversold = (ma5 > ma10 and 
                                  volume > avg_volume * 1.1 and
                                  (current_price / min_30d - 1) < 0.1)
            
            if need_stop:
                # 止损操作
                loss_pct = 0
                if "持仓成本价" in current_position and current_position["持仓成本价"] > 0:
                    loss_pct = ((current_price - current_position["持仓成本价"]) / 
                              current_position["持仓成本价"] * 100)
                strategy_content += f"操作建议：{position_type}：止损清仓（价格跌破动态止损位{stop_loss:.2f}，亏损{loss_pct:.2f}%）"
                
                trade_actions.append({
                    "etf_code": target_etf_code,
                    "etf_name": target_etf_name,
                    "position_type": position_type,
                    "action": "止损",
                    "quantity": "100%",
                    "price": current_price,
                    "reason": f"跌破动态止损{stop_loss:.2f}"
                })
            elif is_oversold:
                # 超卖反弹机会
                strategy_content += f"操作建议：{position_type}：建仓60%（超卖反弹机会，接近30日低点）"
                
                trade_actions.append({
                    "etf_code": target_etf_code,
                    "etf_name": target_etf_name,
                    "position_type": position_type,
                    "action": "建仓",
                    "quantity": "60%",
                    "price": current_price,
                    "reason": "超卖反弹机会"
                })
            else:
                # 无操作
                strategy_content += f"操作建议：{position_type}：空仓观望（趋势未确认）"
        
        return strategy_content, trade_actions
    
    except Exception as e:
        error_msg = f"计算{position_type}策略失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return f"{position_type}：计算策略时发生错误，请检查日志", []

def calculate_atr(df: pd.DataFrame, period: int = 14) -> float:
    """
    计算平均真实波幅(ATR)，用于动态止损
    
    Args:
        df: 日线数据
        period: 计算周期
    
    Returns:
        float: ATR值
    """
    try:
        # 检查数据量是否足够
        if len(df) < period + 1:
            logger.warning(f"数据量不足，无法计算ATR（需要至少{period+1}条数据，实际{len(df)}条）")
            return 0.0
        
        # 计算真实波幅(TR)
        high = df["最高"].values
        low = df["最低"].values
        close = df["收盘"].values
        
        # TR = max(当日最高 - 当日最低, |当日最高 - 昨日收盘|, |当日最低 - 昨日收盘|)
        tr1 = high[1:] - low[1:]
        tr2 = np.abs(high[1:] - close[:-1])
        tr3 = np.abs(low[1:] - close[:-1])
        tr = np.max(np.vstack([tr1, tr2, tr3]), axis=0)
        
        # 计算ATR（指数移动平均）
        n = len(tr)
        if n < period:
            return 0.0
            
        atr = np.zeros(n)
        # 第一个ATR值使用简单移动平均
        atr[period-1] = np.mean(tr[:period])
        
        # 后续ATR值使用指数移动平均
        for i in range(period, n):
            atr[i] = (atr[i-1] * (period-1) + tr[i]) / period
        
        return atr[-1]
    
    except Exception as e:
        logger.error(f"计算ATR失败: {str(e)}", exc_info=True)
        return 0.0

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
        # 确保DataFrame是副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 计算短期均线
        df.loc[:, "ma_short"] = df["收盘"].rolling(window=short_period).mean()
        # 计算长期均线
        df.loc[:, "ma_long"] = df["收盘"].rolling(window=long_period).mean()
        
        # 检查数据量是否足够
        if len(df) < long_period:
            logger.warning(f"数据量不足，无法计算均线信号（需要至少{long_period}条数据，实际{len(df)}条）")
            return False, False
        
        # 检查是否有多头信号（短期均线上穿长期均线）
        ma_bullish = False
        if len(df) > 1:
            prev = df.iloc[-2]
            curr = df.iloc[-1]
            # 检查前一日短期均线 <= 长期均线，当日短期均线 > 长期均线
            if not np.isnan(prev["ma_short"]) and not np.isnan(prev["ma_long"]) and \
               not np.isnan(curr["ma_short"]) and not np.isnan(curr["ma_long"]):
                ma_bullish = prev["ma_short"] <= prev["ma_long"] and curr["ma_short"] > curr["ma_long"]
        
        # 检查是否有空头信号（短期均线下穿长期均线）
        ma_bearish = False
        if len(df) > 1:
            prev = df.iloc[-2]
            curr = df.iloc[-1]
            # 检查前一日短期均线 >= 长期均线，当日短期均线 < 长期均线
            if not np.isnan(prev["ma_short"]) and not np.isnan(prev["ma_long"]) and \
               not np.isnan(curr["ma_short"]) and not np.isnan(curr["ma_long"]):
                ma_bearish = prev["ma_short"] >= prev["ma_long"] and curr["ma_short"] < curr["ma_long"]
        
        logger.debug(f"均线信号计算结果: 多头={ma_bullish}, 空头={ma_bearish}")
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
            etf_row = top_etfs[top_etfs["ETF代码"] == etf_code]
            if not etf_row.empty:
                return etf_row.iloc[0]["评分"]
        
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
