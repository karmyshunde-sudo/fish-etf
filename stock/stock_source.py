# stock/stock_source.py
import time
import random
import numpy as np
import pandas as pd
import akshare as ak
import yfinance as yf
import logging
from datetime import datetime

# ===== 优先级配置（硬编码，按稳定性排序）=====
# 格式: (数据源索引, 接口索引, 优先级分数)
# 分数越低越优先（1=最稳定，5=最不稳定）
SOURCE_PRIORITY = [
    (0, 1, 1),  # AKShare - 同花顺日线（最稳定）
    (0, 0, 2),  # AKShare - 东方财富日线
    (2, 0, 3),  # Tencent Finance - A股日线
    (0, 2, 4),  # AKShare - 新浪财经日线
    (1, 0, 5),  # Yahoo Finance（最不稳定）
]

# ===== 模块级全局状态 =====
_current_priority_index = 0  # 记录当前优先级位置（同时用于数据源定位）

def get_stock_daily_data_from_sources(stock_code: str, 
                                    start_date: datetime, 
                                    end_date: datetime,
                                    existing_data: pd.DataFrame = None) -> pd.DataFrame:
    """
    获取单只股票的日线数据，使用智能多数据源轮换策略
    
    Args:
        stock_code: 6位股票代码
        start_date: 数据起始日期
        end_date: 数据结束日期
        existing_data: 已有数据（用于增量更新）
    
    Returns:
        pd.DataFrame: 标准化后的日线数据（包含所有必要列）
    """
    global _current_priority_index
    logger = logging.getLogger("StockCrawler")
    
    try:
        # ===== 1. 参数预处理 =====
        if not stock_code or len(stock_code) != 6 or not stock_code.isdigit():
            logger.error(f"股票代码 {stock_code} 格式无效")
            return pd.DataFrame()
        
        # 日期格式化
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        
        # ===== 2. 定义真正的多数据源配置 =====
        DATA_SOURCES = [
            # 数据源1：AKShare（多个接口）
            {
                "name": "AKShare",
                "interfaces": [
                    {
                        "name": "东方财富日线",
                        "func": ak.stock_zh_a_hist_min_em,
                        "params": {
                            "period": "daily",
                            "adjust": ""
                        },
                        "delay_range": (3.0, 4.0),
                        "source_type": "akshare"
                    },
                    {
                        "name": "同花顺日线",
                        "func": ak.stock_zh_a_hist_ths,
                        "params": {
                            "period": "daily",
                            "adjust": "qfq"
                        },
                        "delay_range": (2.5, 3.5),
                        "source_type": "akshare"
                    },
                    {
                        "name": "新浪财经日线",
                        "func": ak.stock_zh_a_hist_sina,
                        "params": {
                            "period": "daily",
                            "adjust": ""
                        },
                        "delay_range": (3.5, 4.5),
                        "source_type": "akshare"
                    }
                ]
            },
            # 数据源2：Yahoo Finance（真正独立数据源）
            {
                "name": "Yahoo Finance",
                "interfaces": [
                    {
                        "name": "全球市场数据",
                        "func": _fetch_yfinance_data,
                        "params": {
                            "period": "1d",
                            "auto_adjust": True
                        },
                        "delay_range": (2.0, 2.5),
                        "source_type": "yfinance"
                    }
                ]
            },
            # 数据源3：腾讯财经（真正独立数据源）
            {
                "name": "Tencent Finance",
                "interfaces": [
                    {
                        "name": "A股日线数据",
                        "func": ak.stock_zh_a_hist_qq,
                        "params": {
                            "period": "daily",
                            "adjust": "qfq"
                        },
                        "delay_range": (2.0, 2.5),
                        "source_type": "akshare"
                    }
                ]
            }
        ]

        # ===== 3. 智能轮换逻辑（基于优先级索引）=====
        success = False
        result_df = pd.DataFrame()
        last_error = None
        total_priority = len(SOURCE_PRIORITY)
        
        # 从当前优先级开始尝试
        for offset in range(total_priority):
            # 计算当前尝试的优先级索引
            priority_idx = (_current_priority_index + offset) % total_priority
            ds_idx, if_idx, _ = SOURCE_PRIORITY[priority_idx]
            
            # 确保接口索引有效
            if ds_idx >= len(DATA_SOURCES) or if_idx >= len(DATA_SOURCES[ds_idx]["interfaces"]):
                continue
                
            source = DATA_SOURCES[ds_idx]
            interface = source["interfaces"][if_idx]
            
            try:
                # 动态延时（基于优先级优化）
                delay_min, delay_max = interface["delay_range"]
                # 优先级高的接口延时更短（稳定性高）
                if priority_idx < 2:  # 前两个优先级
                    delay_factor = 0.8
                elif priority_idx < 4:  # 中间两个优先级
                    delay_factor = 1.0
                else:  # 最后一个优先级
                    delay_factor = 1.2
                
                time.sleep(random.uniform(delay_min * delay_factor, delay_max * delay_factor))
                
                logger.debug(f"尝试 [{source['name']}->{interface['name']}] 获取 {stock_code} 数据 "
                            f"(优先级: {priority_idx+1}/{total_priority})")
                
                # 调用接口
                if interface["source_type"] == "yfinance":
                    # Yahoo Finance特殊处理
                    df = interface["func"](
                        symbol=stock_code,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        **interface["params"]
                    )
                else:
                    # AKShare常规处理
                    df = interface["func"](
                        symbol=stock_code,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        **interface["params"]
                    )
                
                # 验证数据有效性
                if df is None or df.empty:
                    raise ValueError("返回空数据")
                
                # 数据标准化
                df = _standardize_data(df, interface["source_type"], stock_code, logger)
                
                # 检查标准化后数据
                standard_cols = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
                if not all(col in df.columns for col in standard_cols):
                    missing = [col for col in standard_cols if col not in df.columns]
                    raise ValueError(f"标准化后仍缺失必要列: {', '.join(missing)}")
                
                # 保存成功状态
                result_df = df
                success = True
                _current_priority_index = priority_idx  # 锁定当前优先级
                logger.info(f"✅ 【{source['name']}->{interface['name']}] 成功获取 {len(result_df)} 条数据 (锁定优先级: {priority_idx+1})")
                break
                
            except Exception as e:
                last_error = e
                logger.debug(f"❌ [{source['name']}->{interface['name']}] 失败: {str(e)}")
                continue
        
        # 所有数据源都失败
        if not success:
            logger.error(f"所有数据源均无法获取 {stock_code} 数据: {str(last_error)}")
            # 失败后递增优先级索引
            _current_priority_index = (_current_priority_index + 1) % total_priority
            return pd.DataFrame()
        
        # ===== 4. 数据完整性保障 =====
        required_columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", 
                           "振幅", "涨跌幅", "涨跌额", "换手率"]
        
        # 补充缺失列
        for col in required_columns:
            if col not in result_df.columns:
                result_df[col] = np.nan
                logger.warning(f"数据列 {col} 缺失，已用NaN填充")
        
        # 确保日期格式
        result_df["日期"] = pd.to_datetime(result_df["日期"], errors='coerce')
        result_df = result_df.sort_values('日期').reset_index(drop=True)
        
        # 确保数值列
        numeric_columns = ["开盘", "最高", "最低", "收盘", "成交量", "成交额", "振幅", 
                          "涨跌幅", "涨跌额", "换手率"]
        for col in numeric_columns:
            if col in result_df.columns:
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
        
        # ===== 5. 增量更新处理 =====
        if existing_data is not None and not existing_data.empty:
            # 标准化已有数据
            existing_data = _standardize_existing_data(existing_data, logger)
            
            # 合并数据
            combined_df = pd.concat([existing_data, result_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['日期'], keep='last')
            combined_df = combined_df.sort_values('日期').reset_index(drop=True)
            
            # 保留最近250条
            if len(combined_df) > 250:
                combined_df = combined_df.tail(250)
                
            logger.info(f"股票 {stock_code} 合并后共有 {len(combined_df)} 条记录")
            return combined_df
        
        logger.info(f"股票 {stock_code} 成功获取 {len(result_df)} 条日线数据")
        return result_df
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 日线数据时发生异常: {str(e)}", exc_info=True)
        return pd.DataFrame()

def _fetch_yfinance_data(symbol: str, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
    """封装Yahoo Finance的API调用"""
    # 转换A股代码格式
    yf_symbol = symbol
    if symbol.startswith('6'):
        yf_symbol = f"{symbol}.SS"
    elif symbol.startswith(('00', '30')):
        yf_symbol = f"{symbol}.SZ"
    elif symbol.startswith('8'):
        yf_symbol = f"{symbol}.BJ"
    
    try:
        df = yf.download(
            yf_symbol,
            start=start_date,
            end=end_date,
            **kwargs
        )
        if df.empty:
            raise ValueError("返回空数据")
        df.reset_index(inplace=True)
        return df
    except Exception as e:
        raise ValueError(f"Yahoo Finance请求失败: {str(e)}")

def _standardize_data(df: pd.DataFrame, source_type: str, stock_code: str, logger) -> pd.DataFrame:
    """标准化为统一数据格式"""
    # 根据数据源类型处理
    if source_type == "akshare":
        # AKShare标准处理逻辑
        if "日期" in df.columns:
            df = df.rename(columns={
                "开盘": "open", "收盘": "close", "最高": "high", "最低": "low",
                "成交量": "volume", "成交额": "amount"
            })
        elif "date" in df.columns:
            df = df.rename(columns={
                "open": "open", "close": "close", "high": "high", "low": "low",
                "volume": "volume", "amount": "amount"
            })
    
    elif source_type == "yfinance":
        # Yahoo Finance处理
        df = df.rename(columns={
            "Open": "open", "Close": "close", "High": "high", 
            "Low": "low", "Volume": "volume", "Adj Close": "amount"
        })
        # 添加缺失列
        if "amount" not in df.columns:
            df["amount"] = df["close"] * df["volume"]
    
    # 统一列名处理
    standard_cols = {
        "date": "日期",
        "open": "开盘",
        "high": "最高",
        "low": "最低",
        "close": "收盘",
        "volume": "成交量",
        "amount": "成交额"
    }
    
    # 标准化日期格式
    if "Date" in df.columns:
        df["日期"] = df["Date"].dt.strftime("%Y-%m-%d")
    elif "date" in df.columns:
        df["日期"] = df["date"].dt.strftime("%Y-%m-%d")
    else:
        if 'index' in df.columns:
            df["日期"] = df["index"].dt.strftime("%Y-%m-%d")
        else:
            df["日期"] = pd.to_datetime(df.index).strftime("%Y-%m-%d")
    
    # 重命名列
    for src, dst in standard_cols.items():
        if src in df.columns:
            df[dst] = df[src]
    
    # 补充必要列
    required_cols = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan
    
    # 添加股票代码
    df['股票代码'] = stock_code
    
    # 确保所有必要列存在
    return df[[col for col in required_cols if col in df.columns] + ["股票代码"]]

def _standardize_existing_data(df: pd.DataFrame, logger) -> pd.DataFrame:
    """标准化已有数据格式"""
    # 确保日期列
    if '日期' not in df.columns:
        if 'date' in df.columns:
            df['日期'] = pd.to_datetime(df['date']).dt.strftime("%Y-%m-%d")
        elif 'Date' in df.columns:
            df['日期'] = df['Date'].dt.strftime("%Y-%m-%d")
        else:
            logger.warning("已有数据缺少日期列，跳过增量更新")
            return df
    
    # 确保数值列
    numeric_columns = ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 保留必要列
    required_cols = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan
    
    return df
