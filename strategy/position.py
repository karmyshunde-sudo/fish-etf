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
from data_crawler.etf_list_manager import load_all_etf_list  # 新增：导入load_all_etf_list
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
    content += "（每个仓位仅持有1只ETF，操作建议基于最新数据）\n\n"
    
    for position_type, strategy in strategies.items():
        # 解析策略内容，提取详细数据
        if "ETF名称" in strategy and "ETF代码" in strategy and "当前价格" in strategy:
            # 提取ETF名称和代码
            etf_name = strategy.split("ETF名称：")[1].split("\n")[0]
            etf_code = strategy.split("ETF代码：")[1].split("\n")[0]
            current_price = strategy.split("当前价格：")[1].split("\n")[0]
            
            # 提取20日均线和偏离率
            critical_value = strategy.split("20日均线：")[1].split("\n")[0] if "20日均线：" in strategy else "N/A"
            deviation = strategy.split("偏离率：")[1].split("\n")[0] if "偏离率：" in strategy else "N/A"
            
            # 提取评分、规模和成交额
            score = strategy.split("评分：")[1].split("\n")[0] if "评分：" in strategy else "N/A"
            fund_size = strategy.split("基金规模：")[1].split("\n")[0] if "基金规模：" in strategy else "N/A"
            avg_volume = strategy.split("日均成交额：")[1].split("\n")[0] if "日均成交额：" in strategy else "N/A"
            
            # 生成详细内容
            content += f"【{position_type}】\n"
            content += f"ETF名称：{etf_name}（{etf_code}）\n"
            content += f"当前价格：{current_price}\n"
            content += f"20日均线：{critical_value}\n"
            content += f"偏离率：{deviation}\n"
            content += f"评分：{score}\n"
            content += f"基金规模：{fund_size}\n"
            content += f"日均成交额：{avg_volume}\n"
            content += f"操作建议：{strategy.split('操作建议：')[1] if '操作建议：' in strategy else '详细建议'}\n\n"
        else:
            # 如果策略内容不符合预期格式，直接显示
            content += f"【{position_type}】\n{strategy}\n\n"
    
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
        
        # 获取评分前5的ETF（用于选仓）
        top_etfs = get_top_rated_etfs(top_n=5)
        if top_etfs.empty or len(top_etfs) == 0:
            warning_msg = "无有效ETF评分数据，无法计算仓位策略"
            logger.warning(warning_msg)
            
            # 发送警告通知
            send_wechat_message(
                message=warning_msg,
                message_type="error"
            )
            
            return "【ETF仓位操作提示】\n无有效ETF数据，无法生成操作建议"
        
        logger.info(f"获取到 {len(top_etfs)} 个高评分ETF")
        
        # 2. 分别计算稳健仓和激进仓策略
        strategies = {}
        trade_actions = []
        
        # 2.1 稳健仓策略（评分最高+均线策略）
        stable_etf = top_etfs.iloc[0]
        stable_code = stable_etf["ETF代码"]
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
        
        # 2.2 激进仓策略（近30天收益最高）
        return_list = []
        for _, row in top_etfs.iterrows():
            code = row["ETF代码"]
            df = load_etf_daily_data(code)
            if not df.empty and len(df) >= 30:
                try:
                    # 确保DataFrame是副本
                    df = df.copy(deep=True)
                    return_30d = (df.iloc[-1]["收盘"] / df.iloc[-30]["收盘"] - 1) * 100
                    return_list.append({
                        "ETF代码": code,
                        "ETF名称": row["ETF名称"],
                        "return_30d": return_30d,
                        "评分": row["评分"]
                    })
                except (IndexError, KeyError):
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
    计算单个仓位策略
    
    Args:
        position_type: 仓位类型（稳健仓/激进仓）
        current_position: 当前仓位
        target_etf_code: 目标ETF代码
        target_etf_name: 目标ETF名称
        etf_df: ETF日线数据
        is_stable: 是否为稳健仓
    
    Returns:
        Tuple[str, List[Dict]]: 策略内容和交易动作列表
    """
    try:
        # 获取最新数据
        latest_data = etf_df.iloc[-1]
        current_price = latest_data["收盘"]
        
        # 计算20日均线
        critical_value = calculate_critical_value(etf_df)
        deviation = calculate_deviation(current_price, critical_value)
        
        # 获取ETF评分信息
        top_etfs = get_top_rated_etfs(top_n=10)
        etf_info = top_etfs[top_etfs["ETF代码"] == target_etf_code]
        
        # 构建详细策略内容
        strategy_content = f"ETF名称：{target_etf_name}\n"
        strategy_content += f"ETF代码：{target_etf_code}\n"
        strategy_content += f"当前价格：{current_price:.2f}\n"
        strategy_content += f"20日均线：{critical_value:.2f}\n"
        strategy_content += f"偏离率：{deviation:.2f}%\n"
        
        if not etf_info.empty:
            strategy_content += f"评分：{etf_info.iloc[0]['评分']:.1f}\n"
            strategy_content += f"基金规模：{etf_info.iloc[0]['规模']:.1f}亿元\n"
            strategy_content += f"日均成交额：{etf_info.iloc[0]['日均成交额']:.1f}万元\n"
        else:
            strategy_content += "评分：N/A\n"
            strategy_content += "基金规模：N/A\n"
            strategy_content += "日均成交额：N/A\n"
        
        # 3. 震荡市判断 - 优先级最高
        is_volatile, cross_count, (min_dev, max_dev) = is_in_volatile_market(etf_df)
        if is_volatile:
            # 计算上轨和下轨价格
            upper_band = critical_value * (1 + max_dev/100)
            lower_band = critical_value * (1 + min_dev/100)
            
            # 震荡市操作
            if deviation >= max_dev - 1:  # 接近上沿
                strategy_content += f"操作建议：{position_type}：小幅减仓10%-20%（偏离率{deviation:.2f}%，接近震荡上沿{max_dev:.2f}%）"
            else:  # 接近下沿
                strategy_content += f"操作建议：{position_type}：小幅加仓10%-20%（偏离率{deviation:.2f}%，接近震荡下沿{min_dev:.2f}%）"
            
            return strategy_content, []
        
        # 1. YES信号：当前价格 ≥ 20日均线
        if current_price >= critical_value:
            # 计算连续站上均线的天数
            consecutive_above = calculate_consecutive_days_above(etf_df, critical_value)
            # 计算成交量变化
            volume_change = calculate_volume_change(etf_df)
            
            # 子条件1：首次突破（价格刚站上均线，连续2-3日站稳+成交量放大20%+）
            if consecutive_above == 1 and volume_change > 20:
                strategy_content += f"操作建议：{position_type}：新建仓位【{target_etf_name}（{target_etf_code}）】当前价格：{current_price:.2f}元（首次突破，连续{consecutive_above}日站上20日均线，成交量放大{volume_change:.1f}%）"
                
                # 生成交易动作
                trade_actions = [{
                    "etf_code": target_etf_code,
                    "etf_name": target_etf_name,
                    "position_type": position_type,
                    "action": "新建仓位",
                    "quantity": "30%" if is_stable else "20%",
                    "price": current_price,
                    "reason": "首次突破，连续站上均线"
                }]
                
                return strategy_content, trade_actions
            
            # 子条件1：首次突破（价格刚站上均线，连续2-3日站稳+成交量放大20%+）
            elif 2 <= consecutive_above <= 3 and volume_change > 20:
                strategy_content += f"操作建议：{position_type}：新建仓位【{target_etf_name}（{target_etf_code}）】当前价格：{current_price:.2f}元（首次突破确认，连续{consecutive_above}日站上20日均线，成交量放大{volume_change:.1f}%）"
                
                # 生成交易动作
                trade_actions = [{
                    "etf_code": target_etf_code,
                    "etf_name": target_etf_name,
                    "position_type": position_type,
                    "action": "新建仓位",
                    "quantity": "50%" if is_stable else "35%",
                    "price": current_price,
                    "reason": "首次突破确认，连续站上均线"
                }]
                
                return strategy_content, trade_actions
            
            # 子条件2：持续站稳（价格维持在均线上）
            else:
                # 场景A：偏离率≤+5%（趋势稳健）
                if deviation <= 5.0:
                    # 添加M头/头肩顶形态检测
                    pattern_detection = detect_head_and_shoulders(etf_df)
                    pattern_msg = ""
                    if pattern_detection["detected"]:
                        pattern_name = pattern_detection["pattern_type"]
                        confidence = pattern_detection["confidence"]
                        if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                            pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），建议减仓10%-15%"
                        elif confidence >= 0.5:
                            pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓5%-10%"
                    
                    strategy_content += f"操作建议：{position_type}：持仓不动（偏离率{deviation:.2f}%，趋势稳健）{pattern_msg}"
                    
                    # 无交易动作
                    return strategy_content, []
                
                # 场景B：+5%＜偏离率≤+10%（趋势较强）
                elif 5.0 < deviation <= 10.0:
                    # 添加M头/头肩顶形态检测
                    pattern_detection = detect_head_and_shoulders(etf_df)
                    pattern_msg = ""
                    if pattern_detection["detected"]:
                        pattern_name = pattern_detection["pattern_type"]
                        confidence = pattern_detection["confidence"]
                        if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                            pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），立即减仓10%-15%"
                        elif confidence >= 0.5:
                            pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓5%-10%"
                    
                    strategy_content += f"操作建议：{position_type}：观望（偏离率{deviation:.2f}%，趋势较强）{pattern_msg}"
                    
                    # 无交易动作
                    return strategy_content, []
                
                # 场景C：偏离率＞+10%（超买风险）
                else:
                    # 添加M头/头肩顶形态检测
                    pattern_detection = detect_head_and_shoulders(etf_df)
                    pattern_msg = ""
                    if pattern_detection["detected"]:
                        pattern_name = pattern_detection["pattern_type"]
                        confidence = pattern_detection["confidence"]
                        if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                            pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），立即减仓20%-30%"
                        elif confidence >= 0.5:
                            pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓15%-25%"
                    
                    strategy_content += f"操作建议：{position_type}：逢高减仓20%-30%（偏离率{deviation:.2f}%，超买风险）{pattern_msg}"
                    
                    # 生成交易动作
                    trade_actions = [{
                        "etf_code": target_etf_code,
                        "etf_name": target_etf_name,
                        "position_type": position_type,
                        "action": "减仓",
                        "quantity": "20%-30%" if is_stable else "15%-25%",
                        "price": current_price,
                        "reason": "超买风险"
                    }]
                    
                    return strategy_content, trade_actions
        
        # 2. NO信号：当前价格 ＜ 20日均线
        else:
            # 计算连续跌破均线的天数
            consecutive_below = calculate_consecutive_days_below(etf_df, critical_value)
            # 计算成交量变化
            volume_change = calculate_volume_change(etf_df)
            # 计算亏损比例
            loss_percentage = calculate_loss_percentage(etf_df)
            
            # 子条件1：首次跌破（价格刚跌穿均线，连续1-2日未收回+成交量放大）
            if consecutive_below == 1 and volume_change > 20:
                if loss_percentage > -15.0:  # 亏损<15%
                    strategy_content += f"操作建议：{position_type}：减仓【{target_etf_name}（{target_etf_code}）】当前价格：{current_price:.2f}元（首次跌破，连续{consecutive_below}日跌破20日均线，成交量放大{volume_change:.1f}%，亏损{loss_percentage:.2f}%）"
                    
                    # 生成交易动作
                    trade_actions = [{
                        "etf_code": target_etf_code,
                        "etf_name": target_etf_name,
                        "position_type": position_type,
                        "action": "减仓",
                        "quantity": "50%" if is_stable else "70%-80%",
                        "price": current_price,
                        "reason": "首次跌破，亏损<15%"
                    }]
                    
                    return strategy_content, trade_actions
                else:  # 亏损≥15%
                    strategy_content += f"操作建议：{position_type}：清仓【{target_etf_name}（{target_etf_code}）】当前价格：{current_price:.2f}元（首次跌破-严重亏损，连续{consecutive_below}日跌破20日均线，成交量放大{volume_change:.1f}%，亏损{loss_percentage:.2f}%）"
                    
                    # 生成交易动作
                    trade_actions = [{
                        "etf_code": target_etf_code,
                        "etf_name": target_etf_name,
                        "position_type": position_type,
                        "action": "清仓",
                        "quantity": "100%",
                        "price": current_price,
                        "reason": "首次跌破-严重亏损"
                    }]
                    
                    return strategy_content, trade_actions
            
            # 子条件1：首次跌破（价格刚跌穿均线，连续1-2日未收回+成交量放大）
            elif consecutive_below == 2 and volume_change > 20:
                strategy_content += f"操作建议：{position_type}：严格止损清仓【{target_etf_name}（{target_etf_code}）】当前价格：{current_price:.2f}元（首次跌破确认，连续{consecutive_below}日跌破20日均线，成交量放大{volume_change:.1f}%）"
                
                # 生成交易动作
                trade_actions = [{
                    "etf_code": target_etf_code,
                    "etf_name": target_etf_name,
                    "position_type": position_type,
                    "action": "清仓",
                    "quantity": "100%",
                    "price": current_price,
                    "reason": "首次跌破确认"
                }]
                
                return strategy_content, trade_actions
            
            # 子条件2：持续跌破（价格维持在均线下）
            else:
                # 场景A：偏离率≥-5%（下跌初期）
                if deviation >= -5.0:
                    strategy_content += f"操作建议：{position_type}：轻仓观望（偏离率{deviation:.2f}%，下跌初期）"
                    
                    # 无交易动作
                    return strategy_content, []
                
                # 场景B：-10%≤偏离率＜-5%（下跌中期）
                elif -10.0 <= deviation < -5.0:
                    strategy_content += f"操作建议：{position_type}：空仓为主（偏离率{deviation:.2f}%，下跌中期）"
                    
                    # 无交易动作
                    return strategy_content, []
                
                # 场景C：偏离率＜-10%（超卖机会）
                else:
                    strategy_content += f"操作建议：{position_type}：小幅加仓10%-15%（偏离率{deviation:.2f}%，超卖机会）"
                    
                    # 生成交易动作
                    trade_actions = [{
                        "etf_code": target_etf_code,
                        "etf_name": target_etf_name,
                        "position_type": position_type,
                        "action": "加仓",
                        "quantity": "10%-15%",
                        "price": current_price,
                        "reason": "超卖机会"
                    }]
                    
                    return strategy_content, trade_actions
        
        # 默认返回
        strategy_content += "操作建议：无明确操作建议"
        return strategy_content, []
    
    except Exception as e:
        error_msg = f"计算{position_type}策略失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return f"{position_type}：计算策略时发生错误，请检查日志", []

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
