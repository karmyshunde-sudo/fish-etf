#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仓位策略计算模块
负责计算稳健仓和激进仓的操作策略
【终极自包含版】所有计算在position.py内部完成，无外部依赖
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
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

# 仓位持仓记录路径
POSITION_RECORD_PATH = os.path.join(Config.BASE_DIR, "data", "position_record.csv")
TRADE_RECORD_PATH = os.path.join(Config.BASE_DIR, "data", "trade_records.csv")
PERFORMANCE_RECORD_PATH = os.path.join(Config.BASE_DIR, "data", "strategy_performance.json")

def internal_load_etf_daily_data(etf_code: str) -> pd.DataFrame:
    """
    内部实现的ETF日线数据加载函数（不依赖utils.file_utils）
    
    Args:
        etf_code: ETF代码
    
    Returns:
        pd.DataFrame: ETF日线数据
    """
    try:
        # 构建文件路径
        file_path = os.path.join(Config.DATA_DIR, "etf_daily", f"{etf_code}.csv")
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.warning(f"ETF {etf_code} 日线数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        # 读取CSV文件 - 关键修复：指定数据类型
        df = pd.read_csv(file_path, encoding="utf-8", dtype={
            "日期": str,
            "开盘": float,
            "最高": float,
            "最低": float,
            "收盘": float,
            "成交量": float,
            "成交额": float
        })
        
        # 内部列名标准化
        df = internal_ensure_chinese_columns(df)
        
        # 检查必需列（不再检查"折溢价率"）
        required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"ETF {etf_code} 数据缺少必要列: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # 确保日期列为datetime类型
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        
        # 按日期排序
        df = df.sort_values("日期")
        
        return df
    
    except Exception as e:
        logger.error(f"加载ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def internal_ensure_chinese_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    内部实现的列名标准化函数（不依赖utils.file_utils）
    
    Args:
        df: 原始DataFrame
    
    Returns:
        pd.DataFrame: 使用中文列名的DataFrame
    """
    if df.empty:
        return df
    
    # 列名映射字典（移除了所有与"折溢价率"相关的映射）
    column_mapping = {
        # 日期列
        'date': '日期',
        'trade_date': '日期',
        'dt': '日期',
        'date_time': '日期',
        
        # 价格列
        'open': '开盘',
        'open_price': '开盘',
        'openprice': '开盘',
        'openprice_': '开盘',
        
        'high': '最高',
        'high_price': '最高',
        'highprice': '最高',
        
        'low': '最低',
        'low_price': '最低',
        'lowprice': '最低',
        
        'close': '收盘',
        'close_price': '收盘',
        'closeprice': '收盘',
        'last_price': '收盘',
        
        # 成交量列
        'volume': '成交量',
        'vol': '成交量',
        'amount': '成交量',
        'volume_': '成交量',
        'vol_': '成交量',
        
        # 成交额列
        'amount': '成交额',
        'turnover': '成交额',
        'money': '成交额',
        'amount_': '成交额',
        
        # 其他技术指标
        'amplitude': '振幅',
        'amplitude_percent': '振幅',
        'amplitude%': '振幅',
        
        'percent': '涨跌幅',
        'change_rate': '涨跌幅',
        'pct_chg': '涨跌幅',
        'percent_change': '涨跌幅',
        
        'change': '涨跌额',
        'price_change': '涨跌额',
        'change_amount': '涨跌额',
        
        'turnover_rate': '换手率',
        'turnoverratio': '换手率',
        'turnover_rate_': '换手率',
        
        # 净值列（仅用于内部计算，不作为输出列）
        'net_value': '净值',
        'iopv': 'IOPV',
        'estimate_value': '净值'
    }
    
    # 重命名列
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # 确保日期列存在
    if '日期' not in df.columns and 'date' in df.columns:
        df = df.rename(columns={'date': '日期'})
    
    return df

def internal_validate_etf_data(df: pd.DataFrame, etf_code: str = "Unknown") -> bool:
    """
    内部数据验证函数，不依赖utils.file_utils
    Args:
        df: ETF日线数据DataFrame
        etf_code: ETF代码，用于日志记录
    Returns:
        bool: 数据是否完整有效
    """
    if df.empty:
        logger.warning(f"ETF {etf_code} 数据为空")
        return False
    
    # 仅检查真正必需的列
    required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f"ETF {etf_code} 数据缺少必要列: {', '.join(missing_columns)}")
        return False
    
    # 检查数据量
    if len(df) < 20:
        file_path = os.path.join(Config.DATA_DIR, "etf_daily", f"{etf_code}.csv")
        logger.warning(f"ETF {etf_code} 数据量不足({len(df)}天)，需要至少20天数据。数据文件: {file_path}")
        return False
    
    # 检查数据连续性
    df = df.sort_values("日期")
    date_diff = (pd.to_datetime(df["日期"]).diff().dt.days.fillna(0))
    max_gap = date_diff.max()
    
    if max_gap > 7:
        file_path = os.path.join(Config.DATA_DIR, "etf_daily", f"{etf_code}.csv")
        logger.warning(f"ETF {etf_code} 数据存在较大间隔({max_gap}天)，可能影响分析结果。数据文件: {file_path}")
    elif max_gap > 3:
        logger.debug(f"ETF {etf_code} 数据存在间隔({max_gap}天)，但不影响核心计算")
    else:
        logger.debug(f"ETF {etf_code} 数据间隔正常，最大间隔{max_gap}天")
    
    return True

def check_data_health(etf_code: str, required_days: int = 20) -> Tuple[bool, str]:
    """
    检查单个ETF数据的健康状况（完整性、连续性）
    
    Args:
        etf_code: ETF代码
        required_days: 所需的最少交易日数
    
    Returns:
        Tuple[bool, str]: (是否健康, 原因)
    """
    df = internal_load_etf_daily_data(etf_code)
    
    if df.empty:
        return False, "数据为空"
    
    if len(df) < required_days:
        return False, f"数据量不足({len(df)}天)，需要至少{required_days}天"
    
    # 检查日期连续性
    df = df.sort_values("日期")
    dates = pd.to_datetime(df["日期"]).dt.date
    date_diff = dates.diff().dt.days.fillna(0).iloc[1:]  # 跳过第一个NaN
    max_gap = date_diff.max()
    
    # 中国股市正常交易日间隔一般为1天（周末）或3-7天（小长假）
    # 9天的间隔极不正常，可能是数据缺失
    if max_gap > 7: 
        return False, f"数据存在过大间隔({max_gap}天)，可能数据缺失"
    
    return True, "数据健康"


def init_position_record() -> pd.DataFrame:
    """
    初始化仓位记录（稳健仓、激进仓各持1只ETF）
    
    Returns:
        pd.DataFrame: 仓位记录的DataFrame
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(POSITION_RECORD_PATH), exist_ok=True)
        
        # 定义明确的数据类型映射
        dtype_mapping = {
            "仓位类型": "string",
            "ETF代码": "string",
            "ETF名称": "string",
            "持仓成本价": "float64",
            "持仓日期": "string",
            "持仓数量": "int64",
            "最新操作": "string",
            "操作日期": "string",
            "持仓天数": "int64",
            "创建时间": "string",
            "更新时间": "string"
        }
        
        # 检查文件是否存在
        if os.path.exists(POSITION_RECORD_PATH):
            try:
                # 关键修复：读取时明确指定数据类型
                position_df = pd.read_csv(
                    POSITION_RECORD_PATH, 
                    encoding="utf-8",
                    dtype=dtype_mapping
                )
                
                # 确保所有列都有正确的数据类型
                for col, dtype in dtype_mapping.items():
                    if col in position_df.columns:
                        if dtype == "string":
                            position_df[col] = position_df[col].astype(str).fillna("")
                        elif dtype == "int64":
                            position_df[col] = pd.to_numeric(position_df[col], errors='coerce').fillna(0).astype(int)
                        elif dtype == "float64":
                            position_df[col] = pd.to_numeric(position_df[col], errors='coerce').fillna(0.0)
                
                # 确保包含所有必要列
                required_columns = list(dtype_mapping.keys())
                for col in required_columns:
                    if col not in position_df.columns:
                        logger.warning(f"仓位记录缺少必要列: {col}，正在添加")
                        if dtype_mapping[col] == "string":
                            position_df[col] = ""
                        elif dtype_mapping[col] == "int64":
                            position_df[col] = 0
                        elif dtype_mapping[col] == "float64":
                            position_df[col] = 0.0
                
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
                        "持仓天数": 0,
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
                        "持仓天数": 0,
                        "创建时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }])], ignore_index=True)
                
                # 保存更新后的记录
                position_df.to_csv(POSITION_RECORD_PATH, index=False, encoding="utf-8")
                
                logger.info(f"已加载仓位记录，共 {len(position_df)} 条")
                return position_df
                
            except Exception as e:
                logger.warning(f"读取仓位记录文件失败: {str(e)}，将创建新文件")
        
        # 如果文件不存在或读取失败，创建默认仓位记录
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
                "持仓天数": 0,
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
                "持仓天数": 0,
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
            "最新操作", "操作日期", "持仓天数", "创建时间", "更新时间"
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
                "交易日期", "交易时间", "UTC时间", "持仓类型", "操作类型", 
                "ETF代码", "ETF名称", "价格", "数量", "金额", 
                "持仓天数", "收益率", "持仓成本价", "当前价格", 
                "止损位", "止盈位", "原因", "操作状态"
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

def init_performance_record() -> None:
    """
    初始化策略表现记录文件
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(PERFORMANCE_RECORD_PATH), exist_ok=True)
        
        # 检查文件是否存在
        if not os.path.exists(PERFORMANCE_RECORD_PATH):
            # 创建策略表现记录文件
            performance_data = {
                "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "avg_holding_days": 0.0,
                "profit_loss_ratio": 0.0,
                "max_drawdown": 0.0,
                "annualized_return": 0.0,
                "hs300_return": 0.0,
                "sharpe_ratio": 0.0,
                "calmar_ratio": 0.0
            }
            with open(PERFORMANCE_RECORD_PATH, 'w', encoding='utf-8') as f:
                import json  # 确保在此处导入json
                json.dump(performance_data, f, ensure_ascii=False, indent=4)
            logger.info("已创建策略表现记录文件")
        else:
            logger.info("策略表现记录文件已存在")
    
    except Exception as e:
        error_msg = f"初始化策略表现记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )

def get_strategy_performance() -> Dict[str, float]:
    """
    获取策略表现（优先从记录文件读取）
    
    Returns:
        Dict[str, float]: 策略表现指标
    """
    try:
        if os.path.exists(PERFORMANCE_RECORD_PATH):
            with open(PERFORMANCE_RECORD_PATH, 'r', encoding='utf-8') as f:
                import json  # 确保在此处导入json
                performance_data = json.load(f)
                # 检查是否需要更新（超过1天）
                last_update = datetime.strptime(performance_data["last_update"], "%Y-%m-%d %H:%M:%S")
                if (datetime.now() - last_update).days > 0:
                    return calculate_strategy_performance()
                return performance_data
        else:
            return calculate_strategy_performance()
    
    except Exception as e:
        logger.error(f"获取策略表现失败，尝试重新计算: {str(e)}", exc_info=True)
        return calculate_strategy_performance()

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
            "交易日期": beijing_now.strftime("%Y-%m-%d"),
            "交易时间": beijing_now.strftime("%H:%M:%S"),
            "UTC时间": utc_now.strftime("%Y-%m-%d %H:%M:%S"),
            "持仓类型": str(kwargs.get("position_type", "")),
            "操作类型": str(kwargs.get("action", "")),
            "ETF代码": str(kwargs.get("etf_code", "")),
            "ETF名称": str(kwargs.get("etf_name", "")),
            "价格": float(kwargs.get("price", 0.0)),
            # 关键修复：将数量保持为字符串，不尝试转换为int
            "数量": str(kwargs.get("quantity", "0")),
            "持仓天数": int(kwargs.get("holding_days", 0)),
            "收益率": float(kwargs.get("return_rate", 0.0)),
            "持仓成本价": float(kwargs.get("cost_price", 0.0)),
            "当前价格": float(kwargs.get("current_price", 0.0)),
            "止损位": float(kwargs.get("stop_loss", 0.0)),
            "止盈位": float(kwargs.get("take_profit", 0.0)),
            "原因": str(kwargs.get("reason", "")),
            "操作状态": str(kwargs.get("status", "已完成"))
        }
        
        # 计算金额
        try:
            quantity = kwargs.get("quantity", "0")
            if isinstance(quantity, str) and quantity.endswith("%"):
                quantity_value = float(quantity.replace("%", "")) / 100
            else:
                quantity_value = float(quantity)
            trade_record["金额"] = trade_record["价格"] * quantity_value
        except (ValueError, TypeError):
            trade_record["金额"] = 0.0
        
        # 读取现有交易记录，明确指定数据类型
        if os.path.exists(TRADE_RECORD_PATH):
            trade_df = pd.read_csv(TRADE_RECORD_PATH, encoding="utf-8")
        else:
            columns = [
                "交易日期", "交易时间", "UTC时间", "持仓类型", "操作类型", 
                "ETF代码", "ETF名称", "价格", "数量", "金额", 
                "持仓天数", "收益率", "持仓成本价", "当前价格", 
                "止损位", "止盈位", "原因", "操作状态"
            ]
            trade_df = pd.DataFrame(columns=columns)
        
        # 添加新记录
        trade_df = pd.concat([trade_df, pd.DataFrame([trade_record])], ignore_index=True)
        
        # 保存交易记录
        trade_df.to_csv(TRADE_RECORD_PATH, index=False, encoding="utf-8")
        logger.info(f"已记录交易: {trade_record['持仓类型']} - {trade_record['操作类型']} {trade_record['ETF代码']}")
    
    except Exception as e:
        error_msg = f"记录交易失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )

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

def calculate_volatility(df: pd.DataFrame, window: int = 20) -> float:
    """计算波动率(年化)"""
    try:
        if len(df) < window + 1:
            return 0.0
            
        # 计算日收益率
        returns = df["收盘"].pct_change().dropna()
        
        # 计算标准差(年化)
        daily_std = returns[-window:].std()
        annualized_vol = daily_std * (252 ** 0.5)  # 年化波动率
        
        return annualized_vol
    
    except Exception as e:
        logger.error(f"计算波动率失败: {str(e)}")
        return 0.0

def calculate_adx(df: pd.DataFrame, period=14) -> float:
    """计算ADX指标（真实实现）"""
    try:
        # 确保有足够的数据
        if len(df) < period + 1:
            logger.warning(f"ADX计算失败：数据量不足（需要{period+1}条，实际{len(df)}条）")
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
        
        # 计算+DM和-DM
        plus_dm = high[1:] - high[:-1]
        minus_dm = low[:-1] - low[1:]
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        plus_dm[plus_dm < minus_dm] = 0
        minus_dm[minus_dm <= plus_dm] = 0
        
        # 计算平滑后的TR、+DM和-DM
        tr_smooth = np.zeros(len(tr))
        plus_dm_smooth = np.zeros(len(plus_dm))
        minus_dm_smooth = np.zeros(len(minus_dm))
        
        # 检查初始数据是否有效
        valid_initial = np.sum(tr[:period] > 0)
        if valid_initial < period * 0.7:  # 如果70%以上的初始数据无效
            logger.warning(f"ADX计算失败：初始数据质量差（有效数据{valid_initial}/{period}）")
            return 0.0
            
        tr_smooth[period-1] = np.sum(tr[:period])
        plus_dm_smooth[period-1] = np.sum(plus_dm[:period])
        minus_dm_smooth[period-1] = np.sum(minus_dm[:period])
        
        # 检查初始值是否为零
        if tr_smooth[period-1] == 0:
            logger.warning("ADX计算失败：初始TR值为零")
            return 0.0
            
        for i in range(period, len(tr)):
            # 添加边界检查
            if tr_smooth[i-1] == 0:
                tr_smooth[i] = tr[i]
            else:
                tr_smooth[i] = tr_smooth[i-1] - (tr_smooth[i-1]/period) + tr[i]
                
            if plus_dm_smooth[i-1] == 0:
                plus_dm_smooth[i] = plus_dm[i]
            else:
                plus_dm_smooth[i] = plus_dm_smooth[i-1] - (plus_dm_smooth[i-1]/period) + plus_dm[i]
                
            if minus_dm_smooth[i-1] == 0:
                minus_dm_smooth[i] = minus_dm[i]
            else:
                minus_dm_smooth[i] = minus_dm_smooth[i-1] - (minus_dm_smooth[i-1]/period) + minus_dm[i]
        
        # 计算+DI和-DI
        plus_di = np.zeros(len(tr_smooth))
        minus_di = np.zeros(len(tr_smooth))
        
        # 避免除零错误
        for i in range(period-1, len(tr_smooth)):
            if tr_smooth[i] > 0:
                plus_di[i] = 100 * (plus_dm_smooth[i] / tr_smooth[i])
                minus_di[i] = 100 * (minus_dm_smooth[i] / tr_smooth[i])
            else:
                plus_di[i] = 0
                minus_di[i] = 0
        
        # 计算DX
        dx = np.zeros(len(plus_di))
        for i in range(period-1, len(plus_di)):
            sum_di = plus_di[i] + minus_di[i]
            if sum_di > 0:
                dx[i] = 100 * np.abs(plus_di[i] - minus_di[i]) / sum_di
            else:
                dx[i] = 0
        
        # 计算ADX
        adx = np.zeros(len(dx))
        valid_adx_start = period * 2 - 1
        
        if valid_adx_start < len(dx) and np.sum(dx[period-1:valid_adx_start] > 0) > 0:
            adx[valid_adx_start] = np.mean(dx[period-1:valid_adx_start])
            
            for i in range(valid_adx_start+1, len(dx)):
                if adx[i-1] > 0:
                    adx[i] = ((period-1) * adx[i-1] + dx[i]) / period
                else:
                    adx[i] = dx[i]
            
            return adx[-1] if len(adx) > 0 else 0.0
        else:
            logger.warning("ADX计算失败：无法计算有效ADX值")
            return 0.0
            
    except Exception as e:
        logger.error(f"计算ADX失败: {str(e)}")
        return 0.0

def calculate_ma_signal(df: pd.DataFrame) -> Tuple[bool, bool]:
    """
    计算均线信号
    
    Args:
        df: 日线数据
    
    Returns:
        Tuple[bool, bool]: (多头信号, 空头信号)
    """
    try:
        # 确保DataFrame是副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 计算短期均线
        df.loc[:, "ma_short"] = df["收盘"].rolling(window=5).mean()
        # 计算长期均线
        df.loc[:, "ma_long"] = df["收盘"].rolling(window=20).mean()
        
        # 检查数据量是否足够
        if len(df) < 20:
            logger.warning(f"数据量不足，无法计算均线信号（需要至少20条数据，实际{len(df)}条）")
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

def calculate_volume_signal(df: pd.DataFrame) -> bool:
    """计算成交量信号（当前量>5日均量*1.2）"""
    try:
        if len(df) < 6:
            return False
        
        current_vol = df.iloc[-1]["成交量"]
        avg_vol = df["成交量"].rolling(5).mean().iloc[-1]
        
        return current_vol > avg_vol * 1.2
    except Exception as e:
        logger.error(f"计算成交量信号失败: {str(e)}")
        return False

def calculate_strategy_performance() -> Dict[str, float]:
    """
    分析策略历史表现（基于真实交易记录）
    
    Returns:
        Dict[str, float]: 策略表现指标
    """
    try:
        # 检查交易记录文件是否存在
        if not os.path.exists(TRADE_RECORD_PATH):
            logger.warning("交易记录文件不存在，无法分析策略表现")
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "avg_holding_days": 0.0,
                "profit_loss_ratio": 0.0,
                "max_drawdown": 0.0,
                "annualized_return": 0.0,
                "hs300_return": 0.0,
                "sharpe_ratio": 0.0,
                "calmar_ratio": 0.0
            }
        
        # 读取交易记录
        trade_df = pd.read_csv(TRADE_RECORD_PATH, encoding="utf-8")
        
        # 检查是否有足够的交易记录
        if len(trade_df) < 5:
            logger.warning(f"交易记录不足({len(trade_df)}条)，无法准确分析策略表现")
            return {
                "total_trades": len(trade_df),
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "avg_holding_days": 0.0,
                "profit_loss_ratio": 0.0,
                "max_drawdown": 0.0,
                "annualized_return": 0.0,
                "hs300_return": 0.0,
                "sharpe_ratio": 0.0,
                "calmar_ratio": 0.0
            }
        
        # 计算胜率
        winning_trades = trade_df[trade_df["收益率"] > 0]
        losing_trades = trade_df[trade_df["收益率"] <= 0]
        total_trades = len(trade_df)
        winning_count = len(winning_trades)
        losing_count = len(losing_trades)
        
        win_rate = (winning_count / total_trades * 100) if total_trades > 0 else 0.0
        avg_holding_days = trade_df["持仓天数"].mean() if not trade_df.empty else 0.0
        
        # 计算盈亏比
        total_profit = winning_trades["收益率"].sum() if not winning_trades.empty else 0.0
        total_loss = abs(losing_trades["收益率"].sum()) if not losing_trades.empty else 0.0
        profit_loss_ratio = (total_profit / total_loss) if total_loss > 0 else 0.0
        
        # 计算最大回撤
        cumulative_returns = trade_df["收益率"].cumsum()
        peak = cumulative_returns.cummax()
        drawdown = peak - cumulative_returns
        max_drawdown = drawdown.max() * 100  # 转换为百分比
        
        # 计算年化收益率
        start_date = pd.to_datetime(trade_df["交易日期"].min())
        end_date = pd.to_datetime(trade_df["交易日期"].max())
        days = (end_date - start_date).days
        annualized_return = (1 + cumulative_returns.iloc[-1]) ** (252 / days) - 1 if days > 0 else 0.0
        annualized_return *= 100  # 转换为百分比
        
        # 计算沪深300收益
        hs300_df = internal_load_etf_daily_data("510300")
        hs300_return = 0.0
        if not hs300_df.empty and len(hs300_df) >= 20:
            # 计算最近6个月收益
            six_month_ago = hs300_df.iloc[-126] if len(hs300_df) >= 126 else hs300_df.iloc[0]
            hs300_return = (hs300_df.iloc[-1]["收盘"] / six_month_ago["收盘"] - 1) * 100
        
        # 计算夏普比率 (简化版)
        daily_returns = trade_df["收益率"]
        if not daily_returns.empty:
            excess_returns = daily_returns - 0.02 / 252  # 无风险利率假设为2%
            sharpe_ratio = np.sqrt(252) * excess_returns.mean() / excess_returns.std() if excess_returns.std() > 0 else 0.0
        else:
            sharpe_ratio = 0.0
        
        # 计算卡玛比率
        calmar_ratio = annualized_return / max_drawdown if max_drawdown > 0 else 0.0
        
        # 更新策略表现记录
        performance_data = {
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_trades": int(total_trades),
            "winning_trades": int(winning_count),
            "losing_trades": int(losing_count),
            "win_rate": float(win_rate),
            "avg_holding_days": float(avg_holding_days),
            "profit_loss_ratio": float(profit_loss_ratio),
            "max_drawdown": float(max_drawdown),
            "annualized_return": float(annualized_return),
            "hs300_return": float(hs300_return),
            "sharpe_ratio": float(sharpe_ratio),
            "calmar_ratio": float(calmar_ratio)
        }
        
        with open(PERFORMANCE_RECORD_PATH, 'w', encoding='utf-8') as f:
            json.dump(performance_data, f, ensure_ascii=False, indent=4)
        
        return performance_data
    
    except Exception as e:
        logger.error(f"分析策略表现失败: {str(e)}", exc_info=True)
        return {
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "win_rate": 0.0,
            "avg_holding_days": 0.0,
            "profit_loss_ratio": 0.0,
            "max_drawdown": 0.0,
            "annualized_return": 0.0,
            "hs300_return": 0.0,
            "sharpe_ratio": 0.0,
            "calmar_ratio": 0.0
        }


def get_top_rated_etfs(top_n: int = 5) -> pd.DataFrame:
    """
    获取评分前N的ETF列表（使用已加载的ETF列表）
    Args:
        top_n: 获取前N名
    Returns:
        pd.DataFrame: 评分前N的ETF列表
    """
    try:
        from data_crawler.etf_list_manager import load_all_etf_list
        logger.info("正在从内存中获取ETF列表...")
        etf_list = load_all_etf_list()
        
        if etf_list.empty:
            logger.error("ETF列表为空，无法获取评分前N的ETF")
            return pd.DataFrame()
        
        required_columns = ["ETF代码", "ETF名称", "基金规模"]
        for col in required_columns:
            if col not in etf_list.columns:
                logger.error(f"ETF列表缺少必要列: {col}，无法进行有效评分")
                return pd.DataFrame() 
        
        etf_list = etf_list[
            (etf_list["基金规模"] >= 10.0) & 
            (~etf_list["ETF代码"].astype(str).str.startswith("511"))
        ].copy()
        
        if etf_list.empty:
            logger.warning("筛选后无符合条件的ETF")
            return pd.DataFrame()
        
        scored_etfs = []
        for _, row in etf_list.iterrows():
            etf_code = str(row["ETF代码"])
            df = internal_load_etf_daily_data(etf_code)
            
            if not internal_validate_etf_data(df, etf_code):
                logger.debug(f"ETF {etf_code} 数据验证失败，跳过评分")
                continue
                
            if len(df) < 20:
                logger.debug(f"ETF {etf_code} 数据量不足，跳过评分")
                continue
                
            atr = calculate_atr(df, 14)
            if atr <= 0:
                logger.debug(f"ETF {etf_code} ATR计算无效({atr})，跳过评分")
                continue
            
            ma_bullish, _ = calculate_ma_signal(df)
            volume_ok = calculate_volume_signal(df)
            adx = calculate_adx(df, 14)
            
            score = 0.0
            if ma_bullish:
                score += 40.0
            if volume_ok and df["成交量"].iloc[-1] > 50000000:
                score += 30.0
            if adx > 25:
                score += 30.0
                
            volatility = calculate_volatility(df)
            if volatility < 0.01:
                logger.debug(f"ETF {etf_code} 波动率过低({volatility:.4f})，跳过评分")
                continue
                
            scored_etfs.append({
                "ETF代码": etf_code,
                "ETF名称": row["ETF名称"],
                "基金规模": row["基金规模"],
                "评分": score,
                "ADX": adx,
                "ATR": atr,
                "Volatility": volatility,
                "ETF数据": df
            })
        
        if not scored_etfs:
            logger.warning("无任何ETF通过严格评分筛选")
            return pd.DataFrame()
            
        scored_df = pd.DataFrame(scored_etfs).sort_values("评分", ascending=False)
        logger.info(f"成功获取评分前{top_n}的ETF列表，共 {len(scored_df)} 条记录")
        return scored_df.head(top_n)
        
    except Exception as e:
        logger.error(f"获取评分前N的ETF失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def filter_valid_etfs(top_etfs: pd.DataFrame) -> List[Dict]:
    """
    筛选有效的ETF（数据完整、符合交易条件）
    Args:
        top_etfs: 评分前N的ETF列表
    Returns:
        List[Dict]: 有效的ETF列表
    """
    valid_etfs = []
    
    for _, row in top_etfs.iterrows():
        etf_code = str(row["ETF代码"])
        df = internal_load_etf_daily_data(etf_code)
        
        if not internal_validate_etf_data(df, etf_code):
            logger.debug(f"ETF {etf_code} 数据不完整，跳过")
            continue
        
        if len(df) < 30:
            file_path = os.path.join(Config.DATA_DIR, "etf_daily", f"{etf_code}.csv")
            logger.warning(f"ETF {etf_code} 数据量不足(仅{len(df)}天)，跳过。数据文件: {file_path}")
            continue
            
        ma_bullish, _ = calculate_ma_signal(df)
        volume_ok = calculate_volume_signal(df)
        adx = calculate_adx(df, 14)
        
        if ma_bullish and volume_ok and adx > 18:
            volume = df["成交量"].iloc[-1]
            volatility = calculate_volatility(df)
            liquidity_score = 1.0 if volume > 100000000 else 0.5
            trend_score = 1.0 if adx > 25 else 0.7
            quality_score = liquidity_score * 0.6 + trend_score * 0.4
            
            valid_etfs.append({
                "ETF代码": etf_code,
                "ETF名称": row["ETF名称"],
                "评分": row["评分"],
                "质量评分": quality_score,
                "ETF数据": df,
                "ADX": adx
            })
    
    valid_etfs.sort(key=lambda x: x["质量评分"], reverse=True)
    logger.info(f"筛选后有效ETF数量: {len(valid_etfs)}")
    return valid_etfs

def calculate_dynamic_stop_loss(current_price: float, etf_df: pd.DataFrame, 
                              position_type: str) -> Tuple[float, float]:
    """计算动态止损位"""
    try:
        # 计算ATR
        atr = calculate_atr(etf_df, 14)
        
        # 根据仓位类型确定基础止损系数
        base_stop_factor = 1.5 if position_type == "稳健仓" else 2.0
        
        # 计算最终止损位
        stop_loss = current_price - base_stop_factor * atr
        risk_ratio = (current_price - stop_loss) / current_price if current_price > 0 else 0
        
        return stop_loss, risk_ratio
    
    except Exception as e:
        logger.error(f"计算动态止损失败: {str(e)}")
        return 0.0, 0.0

def calculate_strategy_score(etf_df: pd.DataFrame, position_type: str) -> int:
    """计算策略评分"""
    try:
        # 获取最新数据
        latest_data = etf_df.iloc[-1]
        current_price = latest_data["收盘"]
        
        # 计算20日均线
        ma20 = etf_df["收盘"].rolling(20).mean().iloc[-1]
        
        # 计算价格偏离度
        price_deviation = 0.0
        if ma20 > 0:
            price_deviation = (current_price - ma20) / ma20
        
        # 计算ADX
        adx = calculate_adx(etf_df, 14)
        
        # 计算60日均线斜率
        ma60 = etf_df["收盘"].rolling(60).mean()
        if len(ma60) >= 62:
            ma60_slope = ((ma60.iloc[-1] - ma60.iloc[-3]) / ma60.iloc[-3]) * 100
        else:
            ma60_slope = 0.0
        
        # 计算量能指标
        volume = etf_df["成交量"].iloc[-1]
        avg_volume = etf_df["成交量"].rolling(5).mean().iloc[-1]
        volume_ratio = volume / avg_volume if avg_volume > 0 else 0
        
        # 计算RSI
        delta = etf_df["收盘"].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = -delta.where(delta < 0, 0).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        rsi_value = rsi.iloc[-1] if len(rsi) > 0 else 50.0
        
        # 计算MACD
        exp12 = etf_df["收盘"].ewm(span=12, adjust=False).mean()
        exp26 = etf_df["收盘"].ewm(span=26, adjust=False).mean()
        macd = exp12 - exp26
        signal = macd.ewm(span=9, adjust=False).mean()
        macd_hist = macd - signal
        macd_bar = macd_hist.iloc[-1] if len(macd_hist) > 0 else 0.0
        
        # 计算布林带宽度
        sma20 = etf_df["收盘"].rolling(20).mean()
        std20 = etf_df["收盘"].rolling(20).std()
        upper_band = sma20 + (std20 * 2)
        lower_band = sma20 - (std20 * 2)
        bollinger_width = (upper_band.iloc[-1] - lower_band.iloc[-1]) / sma20.iloc[-1] if sma20.iloc[-1] > 0 else 0
        
        # 初始化评分
        score = 0
        
        # 1. 价格与均线关系 (30分)
        if price_deviation > -0.05:  # 小于5%偏离
            score += 25
        elif price_deviation > -0.10:  # 5%-10%偏离
            score += 15
        else:  # 大于10%偏离
            score += 5
            
        # 2. 趋势强度 (20分)
        if adx > 25:
            score += 20
        elif adx > 20:
            score += 15
        elif adx > 15:
            score += 10
        else:
            score += 5
            
        # 3. 均线斜率 (15分)
        if ma60_slope > 0:
            score += 15
        elif ma60_slope > -0.3:
            score += 10
        elif ma60_slope > -0.6:
            score += 5
        else:
            score += 0
            
        # 4. 量能分析 (15分)
        if volume_ratio > 1.2:
            score += 15
        elif volume_ratio > 1.0:
            score += 10
        elif volume_ratio > 0.8:
            score += 5
        else:
            score += 0
            
        # 5. 技术形态 (20分)
        # RSI部分 (10分)
        if 30 <= rsi_value <= 70:
            rsi_score = 10
        elif rsi_value < 30 or rsi_value > 70:
            rsi_score = 5
        else:
            rsi_score = 0
        score += rsi_score
        
        # MACD部分 (10分)
        if macd_bar > 0:
            macd_score = 10
        elif macd_bar > -0.005:
            macd_score = 5
        else:
            macd_score = 0
        score += macd_score
        
        # 布林带宽度变化 (额外加分)
        if bollinger_width > 0.05:  # 宽度扩张5%以上
            score += 5
            
        return min(max(score, 0), 100)  # 限制在0-100范围内
    
    except Exception as e:
        logger.error(f"计算策略评分失败: {str(e)}")
        return 50  # 默认评分

def update_position_record(position_type: str, etf_code: str, etf_name: str, 
                          cost_price: float, current_price: float, 
                          quantity: int, action: str) -> None:
    """
    更新仓位记录
    
    Args:
        position_type: 仓位类型
        etf_code: ETF代码
        etf_name: ETF名称
        cost_price: 持仓成本价
        current_price: 当前价格
        quantity: 持仓数量
        action: 操作类型
    """
    try:
        # 读取现有记录 - 关键修复：指定数据类型
        position_df = pd.read_csv(POSITION_RECORD_PATH, encoding="utf-8", 
                                 dtype={
                                     "ETF代码": str,
                                     "ETF名称": str,
                                     "持仓成本价": float,
                                     "持仓数量": int,
                                     "持仓天数": int
                                 })
        
        # 确保正确的数据类型
        position_df["ETF代码"] = position_df["ETF代码"].astype(str)
        position_df["ETF名称"] = position_df["ETF名称"].astype(str)
        position_df["持仓成本价"] = position_df["持仓成本价"].astype(float)
        position_df["持仓数量"] = position_df["持仓数量"].astype(int)
        position_df["持仓天数"] = position_df["持仓天数"].astype(int)
        
        # 更新指定仓位类型的数据
        mask = position_df['仓位类型'] == position_type
        position_df.loc[mask, 'ETF代码'] = str(etf_code)
        position_df.loc[mask, 'ETF名称'] = str(etf_name)
        position_df.loc[mask, '持仓成本价'] = float(cost_price)
        position_df.loc[mask, '持仓日期'] = datetime.now().strftime("%Y-%m-%d")
        position_df.loc[mask, '持仓数量'] = int(quantity)
        position_df.loc[mask, '最新操作'] = str(action)
        position_df.loc[mask, '操作日期'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 更新持仓天数
        if quantity > 0:
            # 如果有持仓，天数+1
            current_days = position_df.loc[mask, '持仓天数'].values[0]
            if current_days > 0:
                position_df.loc[mask, '持仓天数'] = int(current_days) + 1
            else:
                position_df.loc[mask, '持仓天数'] = 1
        else:
            position_df.loc[mask, '持仓天数'] = 0
            
        position_df.loc[mask, '更新时间'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 保存更新后的记录
        position_df.to_csv(POSITION_RECORD_PATH, index=False, encoding="utf-8")
        logger.info(f"已更新{position_type}仓位记录: {etf_code} {action}")
    
    except Exception as e:
        error_msg = f"更新{position_type}仓位记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )

def generate_position_content(strategies: Dict[str, str]) -> str:
    """
    生成仓位策略内容（基于真实计算指标）
    
    Args:
        strategies: 策略字典
    
    Returns:
        str: 格式化后的策略内容
    """
    content = "【ETF趋势策略深度分析报告】\n"
    content += "（小资金趋势交易策略：基于多指标量化分析的动态仓位管理）\n\n"
    
    # 获取策略表现
    performance = get_strategy_performance()
    
    # 为每个仓位类型生成详细分析
    for position_type, strategy in strategies.items():
        # 解析策略内容，提取详细数据
        if "ETF名称：" in strategy and "ETF代码：" in strategy and "当前价格：" in strategy:
            # 提取ETF名称和代码
            etf_name = strategy.split("ETF名称：")[1].split("\n")[0]
            etf_code = strategy.split("ETF代码：")[1].split("\n")[0]
            
            # 加载ETF日线数据
            etf_df = internal_load_etf_daily_data(etf_code)
            if etf_df.empty or len(etf_df) < 20:
                content += f"【{position_type}】\n{etf_name}({etf_code}) 数据不足，无法生成详细分析\n\n"
                continue
            
            # 确保DataFrame是副本
            etf_df = etf_df.copy(deep=True)
            
            # 获取最新数据
            latest_data = etf_df.iloc[-1]
            current_price = latest_data["收盘"]
            
            # 计算20日均线
            ma20 = etf_df["收盘"].rolling(20).mean().iloc[-1]
            
            # 计算价格偏离度
            price_deviation = 0.0
            if ma20 > 0:
                price_deviation = (current_price - ma20) / ma20
            
            # 计算ADX
            adx = calculate_adx(etf_df, 14)
            
            # 计算60日均线斜率
            ma60_slope = 0.0
            if len(etf_df) >= 62:
                ma60 = etf_df["收盘"].rolling(60).mean()
                ma60_slope = ((ma60.iloc[-1] - ma60.iloc[-3]) / ma60.iloc[-3]) * 100
            
            # 计算RSI
            delta = etf_df["收盘"].diff()
            gain = delta.where(delta > 0, 0).rolling(14).mean()
            loss = -delta.where(delta < 0, 0).rolling(14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            rsi_value = rsi.iloc[-1] if len(rsi) > 0 else 50.0
            
            # 计算MACD
            exp12 = etf_df["收盘"].ewm(span=12, adjust=False).mean()
            exp26 = etf_df["收盘"].ewm(span=26, adjust=False).mean()
            macd = exp12 - exp26
            signal = macd.ewm(span=9, adjust=False).mean()
            macd_hist = macd - signal
            macd_bar = macd_hist.iloc[-1] if len(macd_hist) > 0 else 0.0
            
            # 计算布林带
            sma20 = etf_df["收盘"].rolling(20).mean()
            std20 = etf_df["收盘"].rolling(20).std()
            upper_band = sma20 + (std20 * 2)
            lower_band = sma20 - (std20 * 2)
            bollinger_width = (upper_band.iloc[-1] - lower_band.iloc[-1]) / sma20.iloc[-1] if sma20.iloc[-1] > 0 else 0
            
            # 计算量能指标
            volume = etf_df["成交量"].iloc[-1]
            avg_volume = etf_df["成交量"].rolling(5).mean().iloc[-1]
            volume_ratio = volume / avg_volume if avg_volume > 0 else 0
            
            # 计算策略评分
            strategy_score = calculate_strategy_score(etf_df, position_type)
            
            # 生成详细内容
            content += f"📊 {etf_name}({etf_code}) - 详细分析\n"
            content += f"• 价格状态：{current_price:.2f} ({price_deviation*100:.1f}% 低于20日均线)\n"
            
            # 趋势强度分析
            trend_strength = "弱趋势"
            if adx > 25:
                trend_strength = "强趋势"
            elif adx > 20:
                trend_strength = "中等趋势"
            content += f"• 趋势强度：ADX={adx:.1f} ({trend_strength}) | 60日均线斜率={ma60_slope:.1f}%/日\n"
            
            # 量能分析
            volume_status = "健康" if volume > 10000000 else "不足"
            volume_str = f"{volume/10000:.1f}万" if volume > 10000000 else f"{volume:.0f}手"
            volume_ratio_status = "放大" if volume_ratio > 1.0 else "萎缩"
            content += f"• 量能分析：{volume_str} ({volume_status}) | 量比={volume_ratio:.2f} ({volume_ratio_status})\n"
            
            # 技术形态分析
            rsi_status = "超卖" if rsi_value < 30 else "中性" if rsi_value < 70 else "超买"
            macd_status = "正值扩大" if macd_bar > 0 and macd_bar > macd_hist.iloc[-2] else "负值扩大"
            content += f"• 技术形态：RSI={rsi_value:.1f} ({rsi_status}) | MACD柱={macd_bar:.4f} ({macd_status})\n"
            
            # 关键信号
            bollinger_status = "扩张" if bollinger_width > 0 else "收窄"
            content += f"• 关键信号：布林带宽度{abs(bollinger_width)*100:.1%} {bollinger_status}，波动率可能{ '上升' if bollinger_width > 0 else '下降' }\n"
            
            # 策略评分
            score_status = "低于" if strategy_score < 40 else "高于"
            entry_status = "不建议" if strategy_score < 40 else "可考虑"
            content += f"• 策略评分：{strategy_score:.0f}/100 ({score_status}40分{entry_status}入场)\n"
            
            # 操作建议
            if "操作建议：" in strategy:
                content += f"• 操作建议：{strategy.split('操作建议：')[1]}\n\n"
            else:
                content += f"• 操作建议：{strategy}\n\n"
        else:
            # 如果策略内容不符合预期格式，直接显示
            content += f"【{position_type}】\n{strategy}\n\n"
    
    # 添加小资金操作提示
    content += "💡 策略执行指南：\n"
    content += "1. 入场条件：趋势评分≥40分 + 价格突破20日均线\n"
    content += "2. 仓位管理：单ETF≤60%，总仓位80%-100%\n"
    content += "3. 止损规则：入场后设置ATR(14)×2的动态止损\n"
    content += "4. 止盈策略：盈利超8%后，止损上移至成本价\n"
    content += "5. ETF轮动：每周一评估并切换至最强标的\n\n"
    
    # 添加策略历史表现（基于真实计算）
    content += "📊 策略历史表现(近6个月)：\n"
    content += f"• 胜率：{performance['win_rate']:.1f}% | 平均持仓周期：{performance['avg_holding_days']:.1f}天\n"
    content += f"• 盈亏比：{performance['profit_loss_ratio']:.1f}:1 | 最大回撤：{performance['max_drawdown']:.1f}%\n"
    content += f"• 年化收益率：{performance['annualized_return']:.1f}% (同期沪深300: {performance['hs300_return']:.1f}%)\n"
    content += f"• 夏普比率：{performance['sharpe_ratio']:.2f} | 卡玛比率：{performance['calmar_ratio']:.2f}\n\n"
    
    # 添加市场分析
    content += "🔍 数据验证：基于真实交易记录计算，策略表现指标每交易日更新\n"
    
    # 添加时间戳和数据来源
    content += "==================\n"
    content += f"📅 UTC时间: {get_utc_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
    content += f"📅 北京时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
    content += "📊 策略版本: TrendStrategy v4.0.0\n"
    content += "🔗 详细分析: https://github.com/karmyshunde-sudo/fish-etf/actions/runs/17605215706\n"
    content += "📊 环境：生产\n"
    
    return content

def calculate_position_strategy() -> str:
    """
    计算仓位操作策略（稳健仓、激进仓）
    
    Returns:
        str: 策略内容字符串
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始计算ETF仓位操作策略 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 1. 初始化仓位记录
        position_df = init_position_record()
        init_trade_record()
        init_performance_record()
        
        # 2. 确保ETF列表存在 - 修复：使用正确的ETF列表路径
        etf_list_path = Config.ALL_ETFS_PATH  # 使用Config.ALL_ETFS_PATH，不是"etf_list.csv"
        if not os.path.exists(etf_list_path):
            logger.warning(f"ETF列表文件不存在: {etf_list_path}")
            # 尝试重新加载ETF列表
            try:
                from data_crawler.etf_list_manager import update_all_etf_list
                logger.info("正在尝试重新加载ETF列表...")
                etf_list = update_all_etf_list()
                if etf_list.empty:
                    logger.error("ETF列表加载失败，无法计算仓位策略")
                    return "【ETF仓位操作提示】ETF列表加载失败，请检查数据源"
                logger.info(f"成功重新加载ETF列表，共 {len(etf_list)} 条记录")
            except Exception as e:
                error_msg = f"重新加载ETF列表失败: {str(e)}"
                logger.error(error_msg, exc_info=True)
                return "【ETF仓位操作提示】ETF列表文件不存在，无法计算仓位策略"
        
        # 3. 获取评分前5的ETF（用于选仓）
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
                    df = internal_load_etf_daily_data(etf_code)
                    if not df.empty and len(df) >= 30:  # 要求至少30天数据
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
        
        # 3. 筛选有效的ETF
        valid_etfs = filter_valid_etfs(top_etfs)
        
        # 4. 分别计算稳健仓和激进仓策略
        strategies = {}
        trade_actions = []
        
        # 4.1 稳健仓策略（评分最高+趋势策略）
        if valid_etfs:
            stable_etf = valid_etfs[0]
            stable_code = stable_etf["ETF代码"]
            stable_name = stable_etf["ETF名称"]
            stable_df = stable_etf["ETF数据"]
            
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
                    "持仓天数": 0,
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
        else:
            strategies["稳健仓"] = "稳健仓：无符合条件的ETF，保持空仓"
        
        # 4.2 激进仓策略（质量评分第二的ETF）
        if len(valid_etfs) > 1:
            aggressive_etf = valid_etfs[1]
            aggressive_code = aggressive_etf["ETF代码"]
            aggressive_name = aggressive_etf["ETF名称"]
            aggressive_df = aggressive_etf["ETF数据"]
            
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
                    "持仓天数": 0,
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
            strategies["激进仓"] = "激进仓：无符合条件的ETF，保持空仓"
        
        # 5. 执行交易操作
        for action in trade_actions:
            record_trade(**action)
        
        # 6. 生成内容
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
        etf_df: ETF日线数据
        is_stable: 是否为稳健仓
    Returns:
        Tuple[str, List[Dict]]: 策略内容和交易动作列表
    """
    try:
        if not internal_validate_etf_data(etf_df, target_etf_code):
            error_msg = f"ETF {target_etf_code} 数据验证失败，无法计算策略"
            logger.warning(error_msg)
            return f"{position_type}：{error_msg}", []
        
        latest_data = etf_df.iloc[-1]
        current_price = latest_data["收盘"]
        
        ma5 = etf_df["收盘"].rolling(5).mean().iloc[-1]
        ma10 = etf_df["收盘"].rolling(10).mean().iloc[-1]
        ma20 = etf_df["收盘"].rolling(20).mean().iloc[-1]
        ma60 = etf_df["收盘"].rolling(60).mean().iloc[-1]
        
        in_long_term_trend = current_price > ma60 and ma60 > ma60.iloc[-2]
        in_short_term_trend = ma5 > ma10 and ma10 > ma20
        
        volume = etf_df["成交量"].iloc[-1]
        avg_volume = etf_df["成交量"].rolling(5).mean().iloc[-1]
        volume_ok = volume > avg_volume * 1.5
        
        adx = calculate_adx(etf_df, 14)
        atr = calculate_atr(etf_df, 14)
        
        if atr <= 0:
            error_msg = f"ETF {target_etf_code} ATR计算无效，无法设置有效止损，放弃交易"
            logger.error(error_msg)
            return f"{position_type}：{error_msg}", []
            
        stop_loss_factor = 1.5 if is_stable else 2.0
        stop_loss = current_price - stop_loss_factor * atr
        risk_ratio = (current_price - stop_loss) / current_price
        
        is_breakout = (current_price > etf_df["收盘"].rolling(20).max().iloc[-2]) and volume_ok
        
        rsi = calculate_rsi(etf_df, 14)
        is_oversold = rsi < 30 and volume > avg_volume * 1.2
        
        strategy_content = f"ETF名称：{target_etf_name}\n"
        strategy_content += f"ETF代码：{target_etf_code}\n"
        strategy_content += f"当前价格：{current_price:.2f}\n"
        strategy_content += f"技术状态：{'多头' if in_short_term_trend else '空头'} | ADX={adx:.1f} | ATR={atr:.4f}\n"
        
        trade_actions = []
        
        if in_long_term_trend and in_short_term_trend and adx > 25:
            if is_breakout:
                if current_position["持仓数量"] == 0:
                    position_size = 100
                    strategy_content += f"操作建议：{position_type}：新建仓位【{target_etf_name}】{position_size}%（确认突破，趋势强劲）\n"
                    strategy_content += f"• 动态止损：{stop_loss:.2f}元（风险比 {risk_ratio:.1%}）\n"
                    
                    trade_actions.append({
                        "position_type": position_type,
                        "action": "新建仓位",
                        "etf_code": target_etf_code,
                        "etf_name": target_etf_name,
                        "price": current_price,
                        "quantity": position_size,
                        "amount": current_price * position_size,
                        "holding_days": 0,
                        "return_rate": 0.0,
                        "cost_price": current_price,
                        "current_price": current_price,
                        "stop_loss": stop_loss,
                        "take_profit": current_price * 1.2,
                        "reason": f"突破信号+强趋势确认",
                        "status": "已完成"
                    })
                    
                    update_position_record(position_type, target_etf_code, target_etf_name, 
                                        current_price, current_price, position_size, "新建仓位")
                else:
                    if current_price > current_position["持仓成本价"] * 1.05:
                        strategy_content += f"操作建议：{position_type}：加仓（趋势加速，浮盈扩大）\n"
                    else:
                        strategy_content += f"操作建议：{position_type}：持有（趋势稳健）\n"
            else:
                strategy_content += f"操作建议：{position_type}：持有（趋势中，等待突破）\n"
        else:
            if current_position["持仓数量"] > 0 and "持仓成本价" in current_position:
                cost_price = current_position["持仓成本价"]
                if cost_price > 0 and current_price <= stop_loss:
                    loss_pct = ((current_price - cost_price) / cost_price) * 100
                    strategy_content += f"操作建议：{position_type}：止损清仓（跌破动态止损{stop_loss:.2f}，亏损{loss_pct:.2f}%）"
                else:
                    strategy_content += f"操作建议：{position_type}：持有观望（趋势未确认）"
            else:
                strategy_content += f"操作建议：{position_type}：空仓观望（趋势未确认）"
        
        return strategy_content, trade_actions
        
    except Exception as e:
        error_msg = f"计算{position_type}策略失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return f"{position_type}：计算策略时发生致命错误，请检查日志", []

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 初始化日志
    logger.info("仓位管理模块初始化完成")
    
    # 检查ETF列表是否过期
    etf_list_path = os.path.join(Config.DATA_DIR, "etf_list.csv")
    if os.path.exists(etf_list_path) and is_file_outdated(etf_list_path, Config.ETF_LIST_UPDATE_INTERVAL):
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
