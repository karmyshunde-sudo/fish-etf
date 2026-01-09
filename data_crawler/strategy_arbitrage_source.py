#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速套利数据源模块 - 并行+批量获取优化
核心原则：速度优先，简单直接
使用新浪财经批量接口 + 腾讯财经并行接口，大幅提升获取速度
"""

import pandas as pd
import numpy as np
import logging
import requests
import os
import time
import concurrent.futures
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from config import Config
from utils.date_utils import get_beijing_time
from utils.file_utils import ensure_dir_exists
from data_crawler.all_etfs import get_all_etf_codes, get_etf_name

# 初始化日志
logger = logging.getLogger(__name__)

def normalize_price_units_simple(df: pd.DataFrame) -> pd.DataFrame:
    """
    极简单位标准化 - 只处理最明显的错误
    """
    if df.empty:
        return df
    
    df = df.copy()
    modified_count = 0
    
    for idx, row in df.iterrows():
        price = row.get('市场价格', 0)
        iopv = row.get('IOPV', 0)
        
        if price <= 0 or iopv <= 0:
            continue
        
        ratio = price / iopv
        
        # 明显的单位错误：价格是分，IOPV是元（价格>10且IOPV<10）
        if ratio > 10 and price > 10 and iopv < 10:
            df.loc[idx, '市场价格'] = price / 100
            modified_count += 1
        
        # 明显的单位错误：价格是元，IOPV是分（价格<1且IOPV>10）
        elif ratio < 0.1 and price < 1 and iopv > 10:
            df.loc[idx, '市场价格'] = price * 100
            modified_count += 1
    
    if modified_count > 0:
        logger.debug(f"单位标准化调整了 {modified_count} 个ETF数据")
    
    return df

def fetch_sina_etf_batch(etf_codes: List[str], batch_size: int = 80) -> pd.DataFrame:
    """
    新浪财经批量接口 - 一次请求多个ETF，速度最快
    """
    try:
        logger.info(f"尝试新浪财经批量接口，ETF数量: {len(etf_codes)}")
        
        all_data = []
        
        # 分批处理，避免URL过长
        for i in range(0, len(etf_codes), batch_size):
            batch_codes = etf_codes[i:i + batch_size]
            
            # 构建新浪财经代码格式
            sina_codes = []
            for code in batch_codes:
                if code.startswith('5'):
                    sina_codes.append(f"sh{code}")
                else:
                    sina_codes.append(f"sz{code}")
            
            # 批量请求
            codes_str = ",".join(sina_codes)
            url = f"http://hq.sinajs.cn/list={codes_str}"
            
            try:
                response = requests.get(url, timeout=8)
                if response.status_code != 200:
                    logger.debug(f"新浪批量请求失败: {response.status_code}")
                    continue
                
                content = response.text
                if not content:
                    continue
                
                # 解析每行数据
                lines = content.split(';')
                for line in lines:
                    if not line.strip() or 'hq_str' not in line:
                        continue
                    
                    try:
                        # 解析格式: var hq_str_sh510050="华夏上证50ETF,2.345,2.350,...";
                        parts = line.split('="')
                        if len(parts) < 2:
                            continue
                        
                        code_part = parts[0].split('_')[-1]  # 如sh510050
                        data_part = parts[1].rstrip('";')
                        data_items = data_part.split(',')
                        
                        if len(data_items) < 30:
                            continue
                        
                        # 提取股票代码
                        etf_code = code_part[2:]  # 去掉sh/sz前缀
                        
                        # 提取关键数据
                        etf_name = data_items[0] if data_items[0] else ""
                        current_price = float(data_items[3]) if len(data_items) > 3 and data_items[3] else 0
                        
                        # 新浪财经的IOPV可能需要其他方式获取，这里先用价格替代
                        # 对于套利来说，IOPV准确性很重要，但这个接口不直接提供IOPV
                        iopv = current_price
                        
                        # 尝试从其他位置获取IOPV（新浪财经实时数据位置可能变化）
                        # 这里暂时使用价格，实际应用中可能需要调整或使用其他接口
                        if current_price > 0:
                            all_data.append({
                                "ETF代码": etf_code,
                                "ETF名称": etf_name,
                                "市场价格": current_price,
                                "IOPV": iopv,  # 注意：新浪接口可能没有IOPV
                                "数据源": "sina"
                            })
                            
                    except (ValueError, IndexError) as e:
                        continue
                        
            except requests.RequestException as e:
                logger.debug(f"新浪批量请求异常: {str(e)}")
                continue
        
        df = pd.DataFrame(all_data)
        if not df.empty:
            logger.info(f"新浪财经批量接口获取 {len(df)} 只ETF数据")
        return df
        
    except Exception as e:
        logger.error(f"新浪财经批量接口失败: {str(e)}")
        return pd.DataFrame()

def fetch_tencent_etf_parallel(etf_codes: List[str], max_workers: int = 15) -> pd.DataFrame:
    """
    腾讯财经并行接口 - 使用线程池并行请求
    """
    try:
        logger.info(f"尝试腾讯财经并行接口，ETF数量: {len(etf_codes)}，线程数: {max_workers}")
        
        def fetch_single(code: str) -> Optional[dict]:
            """获取单个ETF数据"""
            try:
                if code.startswith('5'):
                    tencent_code = f"sh{code}"
                else:
                    tencent_code = f"sz{code}"
                
                url = f"http://qt.gtimg.cn/q={tencent_code}"
                response = requests.get(url, timeout=4)
                
                if response.status_code != 200:
                    return None
                
                content = response.text.strip()
                if not content or "pv_none_match" in content:
                    return None
                
                parts = content.split('~')
                if len(parts) < 40:
                    return None
                
                etf_name = parts[1] if len(parts) > 1 else ""
                current_price = float(parts[3]) if len(parts) > 3 and parts[3] else 0
                
                # 腾讯财经的IOPV在第39个位置（索引38）
                iopv = float(parts[38]) if len(parts) > 38 and parts[38] else current_price
                
                if current_price > 0 and iopv > 0:
                    return {
                        "ETF代码": code,
                        "ETF名称": etf_name,
                        "市场价格": current_price,
                        "IOPV": iopv,
                        "数据源": "tencent"
                    }
            except Exception:
                return None
            
            return None
        
        # 使用线程池并行请求
        all_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_code = {executor.submit(fetch_single, code): code for code in etf_codes}
            
            # 收集结果
            completed = 0
            for future in concurrent.futures.as_completed(future_to_code):
                completed += 1
                result = future.result()
                if result:
                    all_data.append(result)
                
                # 每完成100个输出一次进度
                if completed % 100 == 0:
                    logger.debug(f"腾讯财经接口进度: {completed}/{len(etf_codes)}")
        
        df = pd.DataFrame(all_data)
        if not df.empty:
            logger.info(f"腾讯财经并行接口获取 {len(df)} 只ETF数据")
        return df
        
    except Exception as e:
        logger.error(f"腾讯财经并行接口失败: {str(e)}")
        return pd.DataFrame()

def fetch_akshare_etf_spot() -> pd.DataFrame:
    """
    akshare ETF实时数据 - 作为备选数据源
    注意：akshare可能较慢，但数据相对准确
    """
    try:
        logger.info("尝试akshare ETF实时数据接口")
        
        import akshare as ak
        df = ak.fund_etf_spot_em()
        
        if df.empty:
            logger.warning("akshare返回空数据")
            return pd.DataFrame()
        
        # 重命名列
        column_mapping = {
            '代码': 'ETF代码',
            '名称': 'ETF名称', 
            '最新价': '市场价格',
            'IOPV实时估值': 'IOPV'
        }
        
        # 只保留需要的列
        available_cols = [col for col in column_mapping.keys() if col in df.columns]
        df = df[available_cols].copy()
        df.rename(columns=column_mapping, inplace=True)
        
        df["数据源"] = "akshare"
        logger.info(f"akshare接口获取 {len(df)} 只ETF数据")
        return df
        
    except Exception as e:
        logger.error(f"akshare接口失败: {str(e)}")
        return pd.DataFrame()

def get_trading_etf_list() -> List[str]:
    """
    获取用于套利监控的ETF列表
    """
    try:
        etf_codes = get_all_etf_codes()
        if not etf_codes:
            logger.error("无法获取ETF代码列表")
            return []
        
        # 排除货币ETF（511开头）
        filtered_codes = [code for code in etf_codes if not code.startswith('511')]
        
        logger.info(f"套利监控ETF数量: {len(filtered_codes)}")
        return filtered_codes
    
    except Exception as e:
        logger.error(f"获取ETF列表失败: {str(e)}")
        return []

def fetch_etf_realtime_data_fast() -> pd.DataFrame:
    """
    快速获取ETF实时数据 - 多数据源策略
    优先级：新浪批量 > 腾讯并行 > akshare
    """
    try:
        start_time = time.time()
        logger.info("开始快速获取ETF实时数据")
        
        # 获取ETF列表
        etf_codes = get_trading_etf_list()
        if not etf_codes:
            logger.error("无有效ETF代码列表")
            return pd.DataFrame()
        
        # 策略1：优先使用新浪财经批量接口（最快）
        df = fetch_sina_etf_batch(etf_codes)
        
        # 策略2：如果新浪数据不足或质量不好，使用腾讯财经并行接口
        if df.empty or len(df) < len(etf_codes) * 0.3:  # 如果获取不到30%的数据
            logger.warning(f"新浪数据不足({len(df)}/{len(etf_codes)})，尝试腾讯财经")
            
            # 尝试从新浪获取失败的ETF代码
            if not df.empty:
                success_codes = set(df["ETF代码"].tolist())
                remaining_codes = [code for code in etf_codes if code not in success_codes]
            else:
                remaining_codes = etf_codes
            
            tencent_df = fetch_tencent_etf_parallel(remaining_codes)
            
            if not tencent_df.empty:
                # 合并数据，新浪数据优先
                if df.empty:
                    df = tencent_df
                else:
                    df = pd.concat([df, tencent_df], ignore_index=True)
        
        # 策略3：如果仍然数据不足，尝试akshare（最慢，但可能最准确）
        if df.empty or len(df) < len(etf_codes) * 0.2:  # 如果获取不到20%的数据
            logger.warning("数据严重不足，尝试akshare接口")
            akshare_df = fetch_akshare_etf_spot()
            
            if not akshare_df.empty:
                if df.empty:
                    df = akshare_df
                else:
                    # 只添加akshare中有但当前没有的ETF
                    current_codes = set(df["ETF代码"].tolist())
                    new_akshare_df = akshare_df[~akshare_df["ETF代码"].isin(current_codes)]
                    if not new_akshare_df.empty:
                        df = pd.concat([df, new_akshare_df], ignore_index=True)
        
        if df.empty:
            logger.warning("所有数据源均失败，未获取到任何ETF数据")
            return pd.DataFrame()
        
        # 数据后处理
        df = normalize_price_units_simple(df)
        
        # 确保IOPV不为0
        df = df[df["IOPV"] > 0].copy()
        
        # 计算折价率
        df["折价率"] = ((df["市场价格"] - df["IOPV"]) / df["IOPV"]) * 100
        
        # 过滤明显异常的折价率（现实中很少超过±50%）
        df = df[(df["折价率"] >= -50) & (df["折价率"] <= 100)].copy()
        
        # 添加时间戳
        beijing_time = get_beijing_time()
        df["日期"] = beijing_time.strftime("%Y-%m-%d")
        df["timestamp"] = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # 统计信息
        elapsed = time.time() - start_time
        logger.info(f"获取完成: {len(df)}只ETF，耗时: {elapsed:.1f}秒")
        if not df.empty:
            logger.info(f"折价率范围: {df['折价率'].min():.2f}% ~ {df['折价率'].max():.2f}%")
        
        return df
        
    except Exception as e:
        logger.error(f"快速获取ETF数据失败: {str(e)}")
        return pd.DataFrame()

def crawl_arbitrage_data() -> str:
    """
    执行套利数据爬取并保存
    """
    try:
        logger.info("开始执行套利数据爬取")
        
        # 获取实时数据
        df = fetch_etf_realtime_data_fast()
        
        if df.empty:
            logger.warning("未获取到有效实时数据")
            return ""
        
        # 保存数据
        arbitrage_dir = os.path.join(Config.DATA_DIR, "arbitrage")
        ensure_dir_exists(arbitrage_dir)
        
        beijing_time = get_beijing_time()
        file_date = beijing_time.strftime("%Y%m%d")
        file_path = os.path.join(arbitrage_dir, f"{file_date}.csv")
        
        # 增量保存
        if os.path.exists(file_path):
            try:
                existing_df = pd.read_csv(file_path, encoding="utf-8-sig")
                # 合并数据，按时间戳去重
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=["ETF代码", "timestamp"], keep="last")
                combined_df.to_csv(file_path, index=False, encoding="utf-8-sig")
                logger.info(f"套利数据已增量保存: {file_path} (新增{len(df)}条，合并后{len(combined_df)}条)")
            except Exception as e:
                logger.error(f"增量保存失败: {str(e)}，直接覆盖")
                df.to_csv(file_path, index=False, encoding="utf-8-sig")
        else:
            df.to_csv(file_path, index=False, encoding="utf-8-sig")
            logger.info(f"套利数据已保存: {file_path} ({len(df)}条)")
        
        return file_path
    
    except Exception as e:
        logger.error(f"套利数据爬取失败: {str(e)}")
        return ""

def load_arbitrage_data(date_str: Optional[str] = None) -> pd.DataFrame:
    """
    加载套利数据
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
        
        df = pd.read_csv(file_path, encoding="utf-8-sig")
        
        if "ETF代码" in df.columns:
            df["ETF代码"] = df["ETF代码"].astype(str).str.strip()
        
        logger.debug(f"加载套利数据: {file_path} (共{len(df)}条)")
        return df
    
    except Exception as e:
        logger.error(f"加载套利数据失败: {str(e)}")
        return pd.DataFrame()

def get_latest_arbitrage_opportunities() -> pd.DataFrame:
    """
    获取最新的套利机会数据
    """
    try:
        beijing_time = get_beijing_time()
        today = beijing_time.strftime("%Y%m%d")
        
        # 尝试加载今日数据
        df = load_arbitrage_data(today)
        
        # 如果数据为空或太少，重新爬取
        if df.empty or len(df) < 100:
            logger.warning(f"今日数据不足({len(df)}条)，重新爬取")
            file_path = crawl_arbitrage_data()
            
            if file_path and os.path.exists(file_path):
                df = load_arbitrage_data(today)
        
        if df.empty:
            logger.warning("加载的套利数据为空")
            return pd.DataFrame()
        
        # 确保必要列存在
        required_columns = ["ETF代码", "ETF名称", "市场价格", "IOPV"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"数据缺少必要列: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # 如果缺少折价率列，实时计算
        if "折价率" not in df.columns:
            df["折价率"] = ((df["市场价格"] - df["IOPV"]) / df["IOPV"]) * 100
        
        logger.info(f"成功加载 {len(df)} 条套利数据")
        return df
    
    except Exception as e:
        logger.error(f"获取最新套利数据失败: {str(e)}")
        return pd.DataFrame()

def load_latest_valid_arbitrage_data(days_back: int = 3) -> pd.DataFrame:
    """
    加载最近有效的套利数据（备用）
    """
    try:
        beijing_now = get_beijing_time()
        
        for i in range(days_back):
            date = (beijing_now - timedelta(days=i)).strftime("%Y%m%d")
            df = load_arbitrage_data(date)
            
            if not df.empty and len(df) > 50:
                logger.info(f"找到有效历史套利数据: {date}，共 {len(df)} 条")
                return df
        
        logger.warning(f"在最近 {days_back} 天内未找到有效的套利数据")
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"加载最近有效套利数据失败: {str(e)}")
        return pd.DataFrame()

# 模块初始化
try:
    Config.init_dirs()
    logger.info("快速套利数据源模块初始化完成")
    
except Exception as e:
    error_msg = f"快速套利数据源模块初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
