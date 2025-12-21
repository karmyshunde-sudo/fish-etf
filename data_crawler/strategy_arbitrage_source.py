# ======= 251117-1500 多数据源-strategy_arbitrage_source-DS2.py ======

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
套利策略专用数据源模块 - 多数据源轮换机制
负责爬取ETF实时市场价格和IOPV(基金份额参考净值)
数据保存格式: data/arbitrage/YYYYMMDD.csv
增强功能：增量保存数据、自动清理过期数据、支持新系统无历史数据场景
【关键修复】使用多数据源轮换机制，降低对akshare的依赖
【问题修复】修复数据列缺失、异常折溢价率、增强日志记录
"""

import pandas as pd
import numpy as np
import logging
import akshare as ak
import yfinance as yf
import requests
import json
import os
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from config import Config
from utils.date_utils import get_beijing_time, is_trading_day, is_trading_time
from utils.file_utils import ensure_dir_exists
from data_crawler.all_etfs import get_all_etf_codes, get_etf_name

# 初始化日志
logger = logging.getLogger(__name__)

# ===== 多数据源配置 =====
# 优先级配置（按稳定性排序）
SOURCE_PRIORITY = [
    (0, 0, 1),  # 数据源0-接口0：腾讯财经（最稳定）
    (1, 0, 2),  # 数据源1-接口0：新浪财经
    (2, 0, 3),  # 数据源2-接口0：东方财富（akshare）- 降级到第三位
    (3, 0, 4),  # 数据源3-接口0：Yahoo Finance（最不稳定）
]

# 模块级全局状态
_current_priority_index = 0  # 记录当前优先级位置

def clean_old_arbitrage_data(days_to_keep: int = 7) -> None:
    """
    清理超过指定天数的套利数据文件（仅清理实时行情数据，不清理交易流水）
    
    Args:
        days_to_keep: 保留天数
    """
    try:
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        if not os.path.exists(arbitrage_dir):
            logger.info("套利数据目录不存在，无需清理")
            return
        
        current_date = get_beijing_time()
        logger.info(f"清理旧套利数据：保留最近 {days_to_keep} 天的数据")
        logger.info(f"当前日期: {current_date}")
        
        files_to_keep = []
        files_to_delete = []
        
        for file_name in os.listdir(arbitrage_dir):
            if not file_name.endswith(".csv"):
                continue
                
            try:
                file_date_str = file_name.split(".")[0]
                file_date = datetime.strptime(file_date_str, "%Y%m%d")
                
                if file_date.tzinfo is None:
                    file_date = file_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                
                days_diff = (current_date - file_date).days
                
                logger.debug(f"检查文件: {file_name}, 文件日期: {file_date}, 日期差: {days_diff}天")
                
                if days_diff > days_to_keep:
                    files_to_delete.append((file_name, file_date, days_diff))
                else:
                    files_to_keep.append((file_name, file_date, days_diff))
            except (ValueError, TypeError) as e:
                logger.warning(f"解析文件日期失败: {file_name}, 错误: {str(e)}")
                continue
        
        for file_name, file_date, days_diff in files_to_delete:
            file_path = os.path.join(arbitrage_dir, file_name)
            try:
                os.remove(file_path)
                logger.info(f"已删除旧套利数据文件: {file_name} (文件日期: {file_date}, 超期: {days_diff - days_to_keep}天)")
            except Exception as e:
                logger.error(f"删除文件失败: {file_path}, 错误: {str(e)}")
        
        logger.info(f"保留套利数据文件: {len(files_to_keep)} 个")
        if files_to_keep:
            logger.debug("保留的文件列表:")
            for file_name, file_date, days_diff in files_to_keep:
                logger.debug(f"  - {file_name} (文件日期: {file_date}, 剩余保留天数: {days_to_keep - days_diff}天)")
        
        logger.info(f"已删除套利数据文件: {len(files_to_delete)} 个")
    
    except Exception as e:
        logger.error(f"清理旧套利数据失败: {str(e)}", exc_info=True)

def append_arbitrage_data(df: pd.DataFrame) -> str:
    """
    增量保存套利数据到CSV文件
    
    Args:
        df: 套利数据DataFrame
    
    Returns:
        str: 保存的文件路径
    """
    try:
        if df.empty:
            logger.warning("套利数据为空，跳过保存")
            return ""
        
        df = df.copy(deep=True)
        
        if "timestamp" not in df.columns:
            timestamp = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
            df.loc[:, "timestamp"] = timestamp
        
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        ensure_dir_exists(arbitrage_dir)
        
        beijing_time = get_beijing_time()
        file_date = beijing_time.strftime("%Y%m%d")
        file_path = os.path.join(arbitrage_dir, f"{file_date}.csv")
        
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path, encoding="utf-8-sig").copy(deep=True)
            if "timestamp" in existing_df.columns:
                existing_df["timestamp"] = pd.to_datetime(existing_df["timestamp"], errors='coerce')
            
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=["ETF代码", "timestamp"], 
                keep="last"
            )
            combined_df.to_csv(file_path, index=False, encoding="utf-8-sig")
        else:
            df.to_csv(file_path, index=False, encoding="utf-8-sig")
        
        logger.info(f"套利数据已增量保存至: {file_path} (新增{len(df)}条记录)")
        return file_path
    
    except Exception as e:
        logger.error(f"增量保存套利数据失败: {str(e)}", exc_info=True)
        return ""

def get_trading_etf_list() -> List[str]:
    """
    获取用于套利监控的ETF列表（统一数据源）
    
    Returns:
        List[str]: ETF代码列表
    """
    try:
        etf_codes = get_all_etf_codes()
        if not etf_codes:
            logger.error("无法获取ETF代码列表")
            return []
        
        etf_list = pd.DataFrame({
            "ETF代码": etf_codes,
            "ETF名称": [get_etf_name(code) for code in etf_codes]
        })
        
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str)
        
        etf_list = etf_list[
            (~etf_list["ETF代码"].str.startswith("511")) &  # 排除货币ETF
            (etf_list["ETF代码"].str.len() == 6)  # 确保代码长度为6位
        ].copy()
        
        etf_list = etf_list.drop_duplicates(subset=["ETF代码"])
        
        logger.info(f"筛选后用于套利监控的ETF数量: {len(etf_list)}")
        return etf_list["ETF代码"].tolist()
    except Exception as e:
        logger.error(f"获取交易ETF列表失败: {str(e)}", exc_info=True)
        return []

def fetch_arbitrage_realtime_data() -> pd.DataFrame:
    """
    爬取所有ETF的实时市场价格和IOPV数据 - 多数据源版本
    
    Returns:
        pd.DataFrame: 包含ETF代码、名称、市场价格、IOPV等信息的DataFrame
    """
    global _current_priority_index
    
    try:
        logger.info("=== 开始执行套利数据爬取（多数据源轮换）===")
        beijing_time = get_beijing_time()
        logger.info(f"当前北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 检查是否为交易日
        #if not is_trading_day():
        #    logger.warning("当前不是交易日，跳过套利数据爬取")
        #    return pd.DataFrame()
        
        # 检查是否为交易时间
        current_time = beijing_time.time()
        trading_start = datetime.strptime(Config.TRADING_START_TIME, "%H:%M").time()
        trading_end = datetime.strptime(Config.TRADING_END_TIME, "%H:%M").time()
        
        if not (trading_start <= current_time <= trading_end):
            logger.warning(f"当前不是交易时间 ({trading_start} - {trading_end})，跳过套利数据爬取")
            return pd.DataFrame()
        
        # 获取需要监控的ETF列表
        etf_codes = get_trading_etf_list()
        if not etf_codes:
            logger.error("无法获取有效的ETF代码列表")
            return pd.DataFrame()
        
        logger.info(f"获取到 {len(etf_codes)} 只符合条件的ETF进行套利监控")
        
        # ===== 多数据源轮换逻辑 =====
        DATA_SOURCES = [
            # 数据源0：腾讯财经（最高优先级）
            {
                "name": "腾讯财经",
                "interfaces": [
                    {
                        "name": "ETF实时行情",
                        "func": _fetch_tencent_etf_data,
                        "delay_range": (1.0, 1.5),
                        "source_type": "tencent"
                    }
                ]
            },
            # 数据源1：新浪财经
            {
                "name": "新浪财经",
                "interfaces": [
                    {
                        "name": "ETF实时行情",
                        "func": _fetch_sina_etf_data,
                        "delay_range": (1.0, 1.5),
                        "source_type": "sina"
                    }
                ]
            },
            # 数据源2：东方财富（akshare）- 降级到第三位
            {
                "name": "东方财富",
                "interfaces": [
                    {
                        "name": "ETF实时行情",
                        "func": _fetch_akshare_etf_data,
                        "delay_range": (3.0, 4.0),
                        "source_type": "akshare"
                    }
                ]
            },
            # 数据源3：Yahoo Finance
            {
                "name": "Yahoo Finance",
                "interfaces": [
                    {
                        "name": "ETF实时行情",
                        "func": _fetch_yfinance_etf_data,
                        "delay_range": (2.0, 2.5),
                        "source_type": "yfinance"
                    }
                ]
            }
        ]
        
        # 智能轮换逻辑
        success = False
        result_df = pd.DataFrame()
        last_error = None
        total_priority = len(SOURCE_PRIORITY)
        
        for offset in range(total_priority):
            priority_idx = (_current_priority_index + offset) % total_priority
            ds_idx, if_idx, _ = SOURCE_PRIORITY[priority_idx]
            
            if ds_idx >= len(DATA_SOURCES) or if_idx >= len(DATA_SOURCES[ds_idx]["interfaces"]):
                continue
                
            source = DATA_SOURCES[ds_idx]
            interface = source["interfaces"][if_idx]
            
            try:
                func = interface["func"]
                
                # 动态延时
                delay_min, delay_max = interface["delay_range"]
                if priority_idx < 2:  # 前两个优先级
                    delay_factor = 0.8
                elif priority_idx < 4:  # 中间两个优先级
                    delay_factor = 1.0
                else:
                    delay_factor = 1.2
                
                time.sleep(random.uniform(delay_min * delay_factor, delay_max * delay_factor))
                
                logger.debug(f"尝试 [{source['name']}->{interface['name']}] 获取ETF实时数据 "
                            f"(优先级: {priority_idx+1}/{total_priority})")
                
                # 调用接口
                df = func(etf_codes)
                
                # 验证数据有效性
                if df is None or df.empty:
                    raise ValueError("返回空数据")
                
                # 数据标准化
                df = _standardize_etf_data(df, interface["source_type"], logger)
                
                # 检查标准化后数据
                required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV"]
                if not all(col in df.columns for col in required_columns):
                    missing = [col for col in required_columns if col not in df.columns]
                    raise ValueError(f"标准化后仍缺失必要列: {', '.join(missing)}")
                
                # 保存成功状态
                result_df = df
                success = True
                _current_priority_index = priority_idx  # 锁定当前优先级
                logger.info(f"✅ 【{source['name']}->{interface['name']}] 成功获取 {len(result_df)} 条ETF实时数据 (锁定优先级: {priority_idx+1})")
                break
                
            except Exception as e:
                last_error = e
                logger.error(f"❌ [{source['name']}->{interface['name']}] 失败: {str(e)}", exc_info=True)
                continue
        
        # 所有数据源都失败
        if not success:
            logger.error(f"所有数据源均无法获取ETF实时数据: {str(last_error)}")
            _current_priority_index = (_current_priority_index + 1) % total_priority
            return pd.DataFrame()
        
        return result_df
    
    except Exception as e:
        logger.error(f"爬取套利实时数据过程中发生未预期错误: {str(e)}", exc_info=True)
        return pd.DataFrame()

def _fetch_tencent_etf_data(etf_codes: List[str]) -> pd.DataFrame:
    """从腾讯财经获取ETF实时数据"""
    try:
        logger.info("尝试从腾讯财经获取ETF实时数据")
        
        # 腾讯财经ETF实时数据API
        base_url = "http://qt.gtimg.cn/q="
        
        all_data = []
        for code in etf_codes:
            try:
                # 构建代码格式
                if code.startswith('5'):
                    tencent_code = f"sh{code}"
                else:
                    tencent_code = f"sz{code}"
                
                url = f"{base_url}{tencent_code}"
                response = requests.get(url, timeout=10)
                
                if response.status_code != 200:
                    continue
                
                content = response.text
                logger.debug(f"腾讯财经原始返回数据: {content}")
                
                if not content or "pv_none_match" in content:
                    continue
                
                # 解析数据格式: v_sh510050="1~华夏上证50ETF~510050~2.345~2.350~2.340..."
                parts = content.split('~')
                if len(parts) < 40:
                    continue
                
                # 提取关键数据
                etf_name = parts[1] if len(parts) > 1 else ""
                current_price = float(parts[3]) if len(parts) > 3 and parts[3] else 0
                iopv = float(parts[38]) if len(parts) > 38 and parts[38] else current_price  # IOPV在腾讯数据中的位置可能不同
                
                if current_price > 0:
                    all_data.append({
                        "ETF代码": code,
                        "ETF名称": etf_name,
                        "市场价格": current_price,
                        "IOPV": iopv,
                        "收盘": current_price,  # 添加收盘价列
                        "日期": get_beijing_time().strftime("%Y-%m-%d")  # 添加日期列
                    })
                
                # 避免请求过快
                time.sleep(0.1)
                
            except Exception as e:
                logger.debug(f"获取ETF {code} 数据失败: {str(e)}")
                continue
        
        df = pd.DataFrame(all_data)
        if not df.empty:
            logger.info(f"腾讯财经获取到 {len(df)} 只ETF的实时数据")
            logger.info(f"腾讯财经数据样本: {df.iloc[0].to_dict() if len(df) > 0 else '无数据'}")
        return df
        
    except Exception as e:
        logger.error(f"腾讯财经ETF数据获取失败: {str(e)}")
        raise ValueError(f"腾讯财经ETF数据获取失败: {str(e)}")

def _fetch_sina_etf_data(etf_codes: List[str]) -> pd.DataFrame:
    """从新浪财经获取ETF实时数据"""
    try:
        logger.info("尝试从新浪财经获取ETF实时数据")
        
        # 新浪财经ETF实时数据API
        base_url = "http://hq.sinajs.cn/list="
        
        all_data = []
        batch_size = 50  # 分批处理，避免URL过长
        
        for i in range(0, len(etf_codes), batch_size):
            batch_codes = etf_codes[i:i + batch_size]
            
            # 构建代码列表
            code_list = []
            for code in batch_codes:
                if code.startswith('5'):
                    sina_code = f"sh{code}"
                else:
                    sina_code = f"sz{code}"
                code_list.append(sina_code)
            
            url = f"{base_url}{','.join(code_list)}"
            response = requests.get(url, timeout=15)
            
            if response.status_code != 200:
                continue
            
            content = response.text
            logger.debug(f"新浪财经原始返回数据: {content}")
            
            lines = content.split(';')
            
            for line in lines:
                if not line.strip():
                    continue
                
                try:
                    # 解析数据格式: var hq_str_sh510050="华夏上证50ETF,2.345,2.350,2.340,...";
                    parts = line.split('="')
                    if len(parts) < 2:
                        continue
                    
                    code_part = parts[0].split('_')[-1]
                    data_part = parts[1].rstrip('";')
                    
                    data_items = data_part.split(',')
                    if len(data_items) < 30:
                        continue
                    
                    # 提取股票代码
                    etf_code = code_part[2:]  # 去掉市场前缀
                    
                    # 提取关键数据
                    etf_name = data_items[0] if data_items[0] else ""
                    current_price = float(data_items[3]) if len(data_items) > 3 and data_items[3] else 0
                    
                    if current_price > 0:
                        all_data.append({
                            "ETF代码": etf_code,
                            "ETF名称": etf_name,
                            "市场价格": current_price,
                            "IOPV": current_price,  # 新浪数据中IOPV可能需要其他方式获取
                            "收盘": current_price,  # 添加收盘价列
                            "日期": get_beijing_time().strftime("%Y-%m-%d")  # 添加日期列
                        })
                        
                except Exception as e:
                    logger.debug(f"解析ETF数据失败: {str(e)}")
                    continue
            
            # 批次间延时
            time.sleep(0.5)
        
        df = pd.DataFrame(all_data)
        if not df.empty:
            logger.info(f"新浪财经获取到 {len(df)} 只ETF的实时数据")
            logger.info(f"新浪财经数据样本: {df.iloc[0].to_dict() if len(df) > 0 else '无数据'}")
        return df
        
    except Exception as e:
        logger.error(f"新浪财经ETF数据获取失败: {str(e)}")
        raise ValueError(f"新浪财经ETF数据获取失败: {str(e)}")

def _fetch_akshare_etf_data(etf_codes: List[str]) -> pd.DataFrame:
    """从东方财富（akshare）获取ETF实时数据"""
    try:
        logger.info("尝试从东方财富获取ETF实时数据")
        
        # 使用akshare获取所有ETF实时数据
        df = ak.fund_etf_spot_em()
        
        logger.info(f"fund_etf_spot_em 接口返回列名: {df.columns.tolist()}")
        if not df.empty:
            logger.info(f"akshare原始数据样本: {df.iloc[0].to_dict() if len(df) > 0 else '无数据'}")
        
        if df.empty:
            logger.error("AkShare未返回ETF实时行情数据")
            return pd.DataFrame()
        
        df = df.copy(deep=True)
        
        # 过滤出需要的ETF
        df = df[df['代码'].isin(etf_codes)].copy(deep=True)
        
        if df.empty:
            logger.warning("筛选后无符合条件的ETF数据")
            return pd.DataFrame()
        
        # 重命名列名
        column_mapping = {
            '代码': 'ETF代码',
            '名称': 'ETF名称',
            '最新价': '市场价格',
            'IOPV实时估值': 'IOPV',
            '基金折价率': '折溢价率',
            '更新时间': '净值时间'
        }
        
        available_columns = [col for col in column_mapping.keys() if col in df.columns]
        df = df[available_columns].rename(columns=column_mapping).copy(deep=True)
        
        df["ETF代码"] = df["ETF代码"].astype(str)
        
        beijing_time = get_beijing_time()
        df.loc[:, '计算时间'] = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
        df.loc[:, '收盘'] = df['市场价格']  # 添加收盘价列
        df.loc[:, '日期'] = beijing_time.strftime("%Y-%m-%d")  # 添加日期列
        
        logger.info(f"东方财富获取成功: {len(df)} 只ETF的实时数据")
        if not df.empty:
            logger.info(f"东方财富处理后的数据样本: {df.iloc[0].to_dict()}")
        return df
        
    except Exception as e:
        logger.error(f"东方财富ETF数据获取失败: {str(e)}")
        raise ValueError(f"东方财富ETF数据获取失败: {str(e)}")

def _fetch_yfinance_etf_data(etf_codes: List[str]) -> pd.DataFrame:
    """从Yahoo Finance获取ETF实时数据"""
    try:
        logger.info("尝试从Yahoo Finance获取ETF实时数据")
        
        all_data = []
        
        for code in etf_codes:
            try:
                # 转换代码格式
                if code.startswith('5'):
                    yf_symbol = f"{code}.SS"
                else:
                    yf_symbol = f"{code}.SZ"
                
                # 获取实时数据
                etf = yf.Ticker(yf_symbol)
                info = etf.info
                history = etf.history(period="1d")
                
                if history.empty:
                    continue
                
                current_price = history['Close'].iloc[-1]
                etf_name = info.get('longName', '') or info.get('shortName', '')
                
                # Yahoo Finance可能不提供IOPV，使用当前价格作为近似值
                iopv = current_price
                
                if current_price > 0:
                    all_data.append({
                        "ETF代码": code,
                        "ETF名称": etf_name,
                        "市场价格": current_price,
                        "IOPV": iopv,
                        "收盘": current_price,  # 添加收盘价列
                        "日期": get_beijing_time().strftime("%Y-%m-%d")  # 添加日期列
                    })
                
                # 避免请求过快
                time.sleep(0.2)
                
            except Exception as e:
                logger.debug(f"获取ETF {code} 数据失败: {str(e)}")
                continue
        
        df = pd.DataFrame(all_data)
        if not df.empty:
            logger.info(f"Yahoo Finance获取到 {len(df)} 只ETF的实时数据")
            logger.info(f"Yahoo Finance数据样本: {df.iloc[0].to_dict() if len(df) > 0 else '无数据'}")
        return df
        
    except Exception as e:
        logger.error(f"Yahoo Finance ETF数据获取失败: {str(e)}")
        raise ValueError(f"Yahoo Finance ETF数据获取失败: {str(e)}")

def _standardize_etf_data(df: pd.DataFrame, source_type: str, logger) -> pd.DataFrame:
    """标准化ETF实时数据格式"""
    
    if df.empty:
        return df
    
    # 确保必要列存在
    required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV", "收盘", "日期"]
    
    # 根据数据源类型处理
    if source_type == "akshare":
        # akshare数据已经过初步处理，只需确保格式
        df = df.rename(columns={
            "代码": "ETF代码",
            "名称": "ETF名称",
            "最新价": "市场价格",
            "IOPV实时估值": "IOPV"
        })
    
    # 确保数值列是数值类型
    numeric_columns = ["市场价格", "IOPV", "收盘"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 过滤无效数据
    df = df[
        (df["市场价格"] > 0) & 
        (df["IOPV"] > 0) &
        (df["收盘"] > 0) &
        (df["ETF代码"].notna()) &
        (df["ETF名称"].notna())
    ].copy()
    
    # 计算折价率
    if "市场价格" in df.columns and "IOPV" in df.columns:
        df["折价率"] = ((df["市场价格"] - df["IOPV"]) / df["IOPV"]) * 100
        
        # 检查异常折溢价率 - 正常范围应该在 -20% 到 +20% 之间
        abnormal_discount = df[df["折价率"] < -20]
        abnormal_premium = df[df["折价率"] > 20]
        
        if len(abnormal_discount) > 0:
            logger.error(f"⚠️ 发现 {len(abnormal_discount)} 个异常折价率 (<-20%): {abnormal_discount[['ETF代码', '折价率']].to_dict()}")
            logger.error("这可能表明数据源或计算逻辑有问题，请检查！")
        
        if len(abnormal_premium) > 0:
            logger.error(f"⚠️ 发现 {len(abnormal_premium)} 个异常溢价率 (>20%): {abnormal_premium[['ETF代码', '折价率']].to_dict()}")
            logger.error("这可能表明数据源或计算逻辑有问题，请检查！")
        
        # 记录折价率统计信息
        if not df.empty:
            min_discount = df["折价率"].min()
            max_discount = df["折价率"].max()
            avg_discount = df["折价率"].mean()
            logger.info(f"折价率统计 - 最小值: {min_discount:.2f}%, 最大值: {max_discount:.2f}%, 平均值: {avg_discount:.2f}%")
            
            # 如果出现极端值，发出严重警告
            if min_discount < -50 or max_discount > 50:
                logger.critical("🚨 发现极端折溢价率！这几乎肯定是数据错误，请立即检查数据源和计算逻辑！")
    
    # 确保所有必要列存在
    for col in required_columns:
        if col not in df.columns:
            if col == "收盘" and "市场价格" in df.columns:
                df[col] = df["市场价格"]
            elif col == "日期":
                df[col] = get_beijing_time().strftime("%Y-%m-%d")
            else:
                df[col] = np.nan
    
    # 添加成交额列（如果缺失）
    if "成交额" not in df.columns:
        df["成交额"] = 0  # 默认值
    
    # 添加振幅列（如果缺失）
    if "振幅" not in df.columns:
        df["振幅"] = 0  # 默认值
    
    # 移除完全无效的行
    df = df.dropna(subset=["ETF代码", "ETF名称", "市场价格", "IOPV"])
    
    logger.info(f"标准化后数据: {len(df)} 条有效记录")
    
    return df

def load_arbitrage_data(date_str: Optional[str] = None) -> pd.DataFrame:
    """
    加载指定日期的套利数据
    
    Args:
        date_str: 日期字符串 (YYYYMMDD)，默认为今天
    
    Returns:
        pd.DataFrame: 套利数据
    """
    try:
        beijing_time = get_beijing_time()
        
        if not date_str:
            date_str = beijing_time.strftime("%Y%m%d")
        
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        file_path = os.path.join(arbitrage_dir, f"{date_str}.csv")
        
        if not os.path.exists(file_path):
            logger.debug(f"套利数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        df = pd.read_csv(file_path, encoding="utf-8-sig").copy(deep=True)
        
        if "ETF代码" in df.columns:
            df["ETF代码"] = df["ETF代码"].astype(str)
        
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')
        
        logger.debug(f"成功加载套利数据: {file_path} (共{len(df)}条记录)")
        return df
    
    except Exception as e:
        logger.error(f"加载套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def crawl_arbitrage_data() -> str:
    """
    执行套利数据爬取并保存
    
    Returns:
        str: 保存的文件路径
    """
    try:
        logger.info("=== 开始执行套利数据爬取（多数据源）===")
        beijing_time = get_beijing_time()
        logger.info(f"当前北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 检查是否为交易日和交易时间
        #if not is_trading_day():
        #    logger.warning("当前不是交易日，跳过套利数据爬取")
        #    return ""
        
        current_time = beijing_time.time()
        trading_start = datetime.strptime(Config.TRADING_START_TIME, "%H:%M").time()
        trading_end = datetime.strptime(Config.TRADING_END_TIME, "%H:%M").time()
        
        if not (trading_start <= current_time <= trading_end):
            logger.warning(f"当前不是交易时间 ({trading_start} - {trading_end})，跳过套利数据爬取")
            return ""
        
        # 使用多数据源爬取数据
        df = fetch_arbitrage_realtime_data()
        
        if df.empty:
            logger.warning("未获取到有效的套利数据，爬取结果为空")
            return ""
        else:
            logger.info(f"成功获取 {len(df)} 只ETF的实时数据")
        
        return append_arbitrage_data(df)
    
    except Exception as e:
        logger.error(f"套利数据爬取任务执行失败: {str(e)}", exc_info=True)
        return ""

def get_latest_arbitrage_opportunities() -> pd.DataFrame:
    """
    获取最新的套利机会数据（原始数据）
    
    Returns:
        pd.DataFrame: 原始套利数据，不做任何筛选和排序
    """
    try:
        # 检查是否为交易日
        #if not is_trading_day():
        #    logger.warning("当前不是交易日，跳过获取套利机会")
        #    return pd.DataFrame()
        
        beijing_time = get_beijing_time()
        today = beijing_time.strftime("%Y%m%d")
        
        # 尝试加载今天的套利数据
        df = load_arbitrage_data(today)
        
        # 如果数据为空，尝试重新爬取
        if df.empty:
            logger.warning("无今日套利数据，尝试重新爬取")
            file_path = crawl_arbitrage_data()
            
            if file_path and os.path.exists(file_path):
                logger.info(f"成功爬取并保存套利数据到: {file_path}")
                df = load_arbitrage_data(today)
            else:
                logger.warning("重新爬取后仍无套利数据")
        
        # 检查数据完整性
        if df.empty:
            logger.warning("加载的套利数据为空，将尝试加载最近有效数据")
            df = load_latest_valid_arbitrage_data()
        
        # 检查数据完整性
        if df.empty:
            logger.error("无法获取任何有效的套利数据")
            return pd.DataFrame()
        
        df = df.copy(deep=True)
        
        if "ETF代码" in df.columns:
            df["ETF代码"] = df["ETF代码"].astype(str)
        
        # 检查必要列
        required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV", "收盘", "日期"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"数据中缺少必要列: {', '.join(missing_columns)}")
            logger.debug(f"实际列名: {list(df.columns)}")
            return pd.DataFrame()
        
        # 确保数据质量
        df = df.dropna(subset=["ETF代码", "ETF名称", "市场价格", "IOPV"])
        
        # 记录最终数据量
        logger.info(f"成功加载 {len(df)} 条原始套利数据")
        return df
    
    except Exception as e:
        logger.error(f"获取最新套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def load_latest_valid_arbitrage_data(days_back: int = 7) -> pd.DataFrame:
    """
    加载最近有效的套利数据
    
    Args:
        days_back: 向前查找的天数
    
    Returns:
        pd.DataFrame: 最近有效的套利数据
    """
    try:
        beijing_now = get_beijing_time()
        
        for i in range(days_back):
            date = (beijing_now - timedelta(days=i)).strftime("%Y%m%d")
            df = load_arbitrage_data(date)
            
            if not df.empty:
                df = df.copy(deep=True)
                
                required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV", "收盘", "日期"]
                if all(col in df.columns for col in required_columns) and len(df) > 0:
                    logger.info(f"找到有效历史套利数据: {date}, 共 {len(df)} 个机会")
                    return df
        
        logger.warning(f"在最近 {days_back} 天内未找到有效的套利数据")
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"加载最近有效套利数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 初始化日志
    logger.info("套利数据源模块初始化完成（多数据源版本）")
    
    # 清理过期的套利数据
    try:
        clean_old_arbitrage_data(days_to_keep=7)
        logger.info("已清理超过7天的套利数据文件")
    except Exception as e:
        logger.error(f"清理旧套利数据文件失败: {str(e)}", exc_info=True)
    
except Exception as e:
    error_msg = f"套利数据源模块初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
    
    try:
        # 退回到基础日志配置
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(error_msg)
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(error_msg)
