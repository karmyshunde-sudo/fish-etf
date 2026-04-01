#!/usr/bin/env python3 
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块 - 真正批量保存版本
YF数据旧目录10只提交-etf_daily_crawler-QW1.py
"""

import yfinance as yf
import pandas as pd
import logging
import os
import time
import random
import tempfile
import shutil
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time, get_last_trading_day, is_trading_day
from utils.new_git import commit_single_file, commit_batch_files, commit_remaining_files, verify_git_file_content

# 初始化日志
logger = logging.getLogger(__name__)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
# DAILY_DIR = os.path.join(DATA_DIR, "etf", "daily")
DAILY_DIR = os.path.join(DATA_DIR, "etf_daily")  # ✅ 改回旧的路径
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_etfs.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# 新增：每10只ETF提交一次
MINOR_BATCH_SIZE = 10
# 一次运行处理300只ETF
MAJOR_BATCH_SIZE = 450

def get_etf_name(etf_code):
    """获取ETF名称"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"ETF列表文件不存在: {BASIC_INFO_FILE}")
            return etf_code
        
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"ETF代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("ETF列表文件为空")
            return etf_code
        
        if "ETF代码" not in basic_info_df.columns or "ETF名称" not in basic_info_df.columns:
            logger.error("ETF列表文件缺少必要列")
            return etf_code
        
        etf_code_str = str(etf_code).strip()
        etf_row = basic_info_df[basic_info_df["ETF代码"] == etf_code_str]
        
        if not etf_row.empty:
            return etf_row["ETF名称"].values[0]
        
        logger.warning(f"ETF {etf_code_str} 不在列表中")
        return etf_code
    except Exception as e:
        logger.error(f"获取ETF名称失败: {str(e)}", exc_info=True)
        return etf_code


def get_next_crawl_index() -> int:
    """获取下一个要处理的ETF索引"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"ETF列表文件不存在: {BASIC_INFO_FILE}")
            return 0
        
        if not verify_git_file_content(BASIC_INFO_FILE):
            logger.warning("ETF列表文件内容与Git仓库不一致，可能需要重新加载")
        
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"ETF代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("ETF列表文件为空，无法获取进度")
            return 0
        
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
            if not verify_git_file_content(BASIC_INFO_FILE):
                logger.warning("ETF列表文件内容与Git仓库不一致，可能需要重新提交")
            logger.info("已添加next_crawl_index列并初始化为0")
        
        next_index = int(basic_info_df["next_crawl_index"].iloc[0])
        logger.info(f"当前进度：下一个索引位置: {next_index}/{len(basic_info_df)}")
        return next_index
    except Exception as e:
        logger.error(f"获取ETF进度索引失败: {str(e)}", exc_info=True)
        return 0


def save_crawl_progress(next_index: int):
    """保存ETF爬取进度 - 仅保存到文件，不提交"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"ETF列表文件不存在: {BASIC_INFO_FILE}")
            return
        
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"ETF代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("ETF列表文件为空，无法更新进度")
            return
        
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
        
        basic_info_df["next_crawl_index"] = next_index
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"✅ 进度已保存：下一个索引位置: {next_index}/{len(basic_info_df)}")
    except Exception as e:
        logger.error(f"❌ 保存ETF进度失败: {str(e)}", exc_info=True)


def commit_crawl_progress():
    """提交进度文件到Git仓库"""
    try:
        commit_message = f"feat: 更新ETF爬取进度 - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        success = commit_single_file(BASIC_INFO_FILE, commit_message)
        if success:
            logger.info("✅ 进度文件已提交到Git仓库")
        else:
            logger.error("❌ 进度文件提交失败")
        return success
    except Exception as e:
        logger.error(f"❌ 提交进度文件失败: {str(e)}", exc_info=True)
        return False


def get_all_etf_codes() -> list:
    """获取所有ETF代码"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.info("ETF列表文件不存在，正在创建...")
            from data_crawler.all_etfs import update_all_etf_list
            update_all_etf_list()
        
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"ETF代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("ETF列表文件为空")
            return []
        
        if "ETF代码" not in basic_info_df.columns:
            logger.error("ETF列表文件缺少'ETF代码'列")
            return []
        
        etf_codes = basic_info_df["ETF代码"].tolist()
        logger.info(f"获取到 {len(etf_codes)} 只ETF代码")
        return etf_codes
    except Exception as e:
        logger.error(f"获取ETF代码列表失败: {str(e)}", exc_info=True)
        return []


# ✅ 新增：统一规范ETF日线数据结构与精度
def normalize_etf_df(df: pd.DataFrame, etf_code: str, etf_name: str) -> pd.DataFrame:
    """
    规范ETF日线数据结构与精度，使其与data/etf//159222.csv一致
    """
    import datetime

    expected_columns = [
        "日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额",
        "振幅", "涨跌幅", "涨跌额", "换手率", "IOPV", "折价率", "溢价率",
        "ETF代码", "ETF名称", "爬取时间"
    ]

    # 缺少列自动补0
    for col in expected_columns:
        if col not in df.columns:
            df[col] = 0

    # 精度处理
    four_decimals = ["开盘", "最高", "最低", "收盘", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率", "IOPV", "折价率", "溢价率"]
    for col in four_decimals:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

    if "成交量" in df.columns:
        df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce").fillna(0).astype(int)

    df["ETF代码"] = etf_code
    df["ETF名称"] = etf_name
    df["爬取时间"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df = df[expected_columns]
    df = df.sort_values(by="日期", ascending=True)
    return df


def crawl_etf_data(etf_code: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """使用多数据源爬取 ETF 日线数据（优先使用多数据源，失败后降级到 yfinance）"""
    try:
        # 尝试使用多数据源方案
        try:
            from data_crawler.multi_source_daily_crawler import fetch_etf_daily_data, normalize_etf_dataframe
            
            logger.info(f"[多数据源] 尝试获取 ETF {etf_code} 数据")
            
            # 使用多数据源获取数据
            df = fetch_etf_daily_data(etf_code, start_date, end_date)
            
            if df is not None and not df.empty:
                # 规范化数据结构
                etf_name = get_etf_name(etf_code)
                df = normalize_etf_dataframe(df, etf_code, etf_name)
                logger.info(f"[多数据源] ✅ ETF {etf_code} 成功获取 {len(df)} 条数据")
                return df
            else:
                logger.warning(f"[多数据源] ❌ ETF {etf_code} 所有数据源均失败，降级到 yfinance")
        except ImportError as e:
            logger.warning(f"[多数据源] 模块导入失败：{str(e)}，降级到 yfinance")
        except Exception as e:
            logger.error(f"[多数据源] ETF {etf_code} 获取失败：{str(e)}，降级到 yfinance")
        
        # 降级到 yfinance（向后兼容）
        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            logger.error(f"ETF {etf_code} 日期参数类型错误，应为datetime类型")
            return pd.DataFrame()
        
        symbol = etf_code
        if etf_code.startswith(('51', '56', '57', '58')):
            symbol = f"{etf_code}.SS"
        elif etf_code.startswith('15'):
            symbol = f"{etf_code}.SZ"
        else:
            symbol = f"{etf_code}.SZ"
        
        logger.info(f"[yfinance] 尝试获取 ETF {etf_code} 数据，符号：{symbol}")
        
        etf_ticker = yf.Ticker(symbol)
        df = etf_ticker.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            auto_adjust=True
        )
        
        if df is None:
            logger.warning(f"ETF {etf_code} 返回数据为None")
            return pd.DataFrame()
        
        if df.empty:
            logger.warning(f"ETF {etf_code} 返回数据为空")
            alternative_symbols = []
            if symbol.endswith('.SS'):
                alternative_symbols.append(symbol.replace('.SS', '.SZ'))
            elif symbol.endswith('.SZ'):
                alternative_symbols.append(symbol.replace('.SZ', '.SS'))
            
            for alt_symbol in alternative_symbols:
                logger.info(f"尝试替代符号: {alt_symbol}")
                try:
                    alt_ticker = yf.Ticker(alt_symbol)
                    df = alt_ticker.history(
                        start=start_date.strftime("%Y-%m-%d"),
                        end=end_date.strftime("%Y-%m-%d"),
                        auto_adjust=True
                    )
                    if not df.empty:
                        symbol = alt_symbol
                        logger.info(f"使用替代符号 {alt_symbol} 成功获取数据")
                        break
                except Exception as alt_e:
                    logger.warning(f"替代符号 {alt_symbol} 也失败: {str(alt_e)}")
            
            if df.empty:
                logger.warning(f"ETF {etf_code} 所有符号尝试均失败")
                return pd.DataFrame()
        
        df = df.reset_index()
        
        # logger.info(f"ETF {etf_code} 实际列名: {df.columns.tolist()}")
        # logger.info(f"ETF {etf_code} 数据形状: {df.shape}")
        
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"ETF {etf_code} 缺少基础列: {', '.join(missing_columns)}")
            logger.error(f"现有列: {', '.join(df.columns.tolist())}")
            return pd.DataFrame()
        
        column_mapping = {
            'Date': '日期',
            'Open': '开盘',
            'High': '最高', 
            'Low': '最低',
            'Close': '收盘',
            'Volume': '成交量'
        }
        
        actual_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=actual_mapping)
        
        # 确保日期列是字符串格式
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.strftime('%Y-%m-%d')
            if df['日期'].isnull().any():
                logger.warning(f"ETF {etf_code} 日期列包含无效日期，已过滤")
                df = df.dropna(subset=['日期'])
        else:
            logger.error(f"ETF {etf_code} 重命名后缺少日期列")
            return pd.DataFrame()
        
        chinese_required = ['日期', '开盘', '最高', '最低', '收盘', '成交量']
        chinese_missing = [col for col in chinese_required if col not in df.columns]
        
        if chinese_missing:
            logger.error(f"ETF {etf_code} 重命名后缺少列: {', '.join(chinese_missing)}")
            return pd.DataFrame()
        
        df = df.sort_values('日期').reset_index(drop=True)
        df['涨跌额'] = df['收盘'].diff()
        
        prev_close = df['收盘'].shift(1)
        df['涨跌幅'] = (df['涨跌额'] / prev_close.replace(0, float('nan')) * 100).round(2)
        df['涨跌幅'] = df['涨跌幅'].fillna(0)
        
        df['振幅'] = ((df['最高'] - df['最低']) / prev_close.replace(0, float('nan')) * 100).round(2)
        df['振幅'] = df['振幅'].fillna(0)
        
        if '成交额' not in df.columns:
            df['成交额'] = (df['收盘'] * df['成交量']).round(2)
        
        df['换手率'] = 0.0
        df['ETF代码'] = etf_code
        df['ETF名称'] = get_etf_name(etf_code)
        df['爬取时间'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['折价率'] = 0.0
        
        standard_columns = [
            '日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额',
            '振幅', '涨跌幅', '涨跌额', '换手率', 'ETF代码', 'ETF名称',
            '爬取时间', '折价率'
        ]
        
        final_columns = [col for col in standard_columns if col in df.columns]
        df = df[final_columns]
        
        logger.info(f"ETF {etf_code} 成功处理 {len(df)} 条数据")
        return df
        
    except Exception as e:
        logger.error(f"ETF {etf_code} 数据爬取失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_incremental_date_range(etf_code: str) -> (datetime, datetime):
    """获取增量爬取的日期范围"""
    try:
        last_trading_day = get_last_trading_day()
        if not isinstance(last_trading_day, datetime):
            last_trading_day = datetime.now()
        
        if last_trading_day.tzinfo is None:
            last_trading_day = last_trading_day.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        end_date = last_trading_day
        current_time = get_beijing_time()
        
        if end_date > current_time:
            end_date = current_time
        
        while not is_trading_day(end_date.date()):
            end_date -= timedelta(days=1)
            if (last_trading_day - end_date).days > 30:
                logger.error("无法找到有效的结束交易日")
                return None, None
        
        end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
        
        save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
        
        if os.path.exists(save_path):
            try:
                df = pd.read_csv(save_path)
                
                if "日期" not in df.columns:
                    logger.warning(f"ETF {etf_code} 数据文件缺少'日期'列")
                    start_date = last_trading_day - timedelta(days=365)
                    if start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                    return start_date, end_date
                
                df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
                valid_dates = df["日期"].dropna()
                if valid_dates.empty:
                    logger.warning(f"ETF {etf_code} 数据文件中日期列全为NaN")
                    start_date = last_trading_day - timedelta(days=365)
                    if start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                    return start_date, end_date
                
                latest_date = valid_dates.max()
                if not isinstance(latest_date, datetime):
                    latest_date = pd.to_datetime(latest_date)
                
                if latest_date.tzinfo is None:
                    latest_date = latest_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                
                latest_date_date = latest_date.date()
                end_date_date = end_date.date()
                
                logger.debug(f"ETF {etf_code} 日期比较: 最新日期={latest_date_date}, 结束日期={end_date_date}")
                
                if latest_date_date < end_date_date:
                    start_date = latest_date + timedelta(days=1)
                    
                    while not is_trading_day(start_date.date()):
                        start_date += timedelta(days=1)
                    
                    if start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                    
                    if start_date > end_date:
                        logger.info(f"ETF {etf_code} 数据已最新（最新日期={latest_date_date}，结束日期={end_date_date}）")
                        return None, None
                    
                    logger.info(f"ETF {etf_code} 需要更新数据: 最新日期 {latest_date_date} < 结束日期 {end_date_date}")
                    logger.info(f"ETF {etf_code} 增量爬取日期范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
                    return start_date, end_date
                else:
                    logger.info(f"ETF {etf_code} 数据已最新: 最新日期 {latest_date_date} >= 结束日期 {end_date_date}")
                    return None, None
            
            except Exception as e:
                logger.error(f"读取ETF {etf_code} 数据文件失败: {str(e)}", exc_info=True)
                start_date = last_trading_day - timedelta(days=365)
                if start_date.tzinfo is None:
                    start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                return start_date, end_date
        else:
            logger.info(f"ETF {etf_code} 无历史数据，将获取一年历史数据")
            start_date = last_trading_day - timedelta(days=365)
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
            return start_date, end_date
    
    except Exception as e:
        logger.error(f"获取增量日期范围失败: {str(e)}", exc_info=True)
        last_trading_day = get_last_trading_day()
        start_date = last_trading_day - timedelta(days=365)
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        end_date = last_trading_day.replace(hour=23, minute=59, second=59, microsecond=0)
        return start_date, end_date


def save_etf_data_batch(etf_data_dict: dict) -> int:
    """
    批量保存ETF日线数据 - 真正批量保存版本（修复日期类型混合排序错误）
    """
    if not etf_data_dict:
        return 0

    os.makedirs(DAILY_DIR, exist_ok=True)
    saved_count = 0

    for etf_code, df in etf_data_dict.items():
        if df.empty:
            continue

        save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")

        # ✅ 保存前规范化数据结构与精度
        etf_name = df["ETF名称"].iloc[0] if "ETF名称" in df.columns else get_etf_name(etf_code)
        df = normalize_etf_df(df, etf_code, etf_name)

        try:
            # =============================
            # ✅ Step 1: 读取已有数据（如存在）
            # =============================
            if os.path.exists(save_path):
                existing_df = pd.read_csv(save_path)

                # 确保旧数据的"日期"列统一为datetime格式
                if "日期" in existing_df.columns:
                    existing_df["日期"] = pd.to_datetime(existing_df["日期"], errors="coerce")

                # 确保新数据的"日期"列也是datetime格式
                if "日期" in df.columns:
                    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")

                # =============================
                # ✅ Step 2: 合并数据
                # =============================
                combined_df = pd.concat([existing_df, df], ignore_index=True)

                # 再次统一日期列类型
                combined_df["日期"] = pd.to_datetime(combined_df["日期"], errors="coerce")

                # 丢弃无效日期
                invalid_dates = combined_df["日期"].isna().sum()
                if invalid_dates > 0:
                    logger.warning(f"⚠️ ETF {etf_code} 合并后发现 {invalid_dates} 条无效日期记录，已过滤")
                    combined_df = combined_df.dropna(subset=["日期"])

                # =============================
                # ✅ Step 3: 去重 + 排序
                # =============================
                combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                combined_df = combined_df.sort_values("日期", ascending=True).reset_index(drop=True)

                # =============================
                # ✅ Step 4: 格式化日期列为字符串保存
                # =============================
                combined_df["日期"] = combined_df["日期"].dt.strftime("%Y-%m-%d")

                # =============================
                # ✅ Step 5: 临时文件安全写入
                # =============================
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                    combined_df.to_csv(temp_file.name, index=False)

                shutil.move(temp_file.name, save_path)
                logger.info(f"✅ 数据已合并至: {save_path} (共{len(combined_df)}条)")

            else:
                # =============================
                # ✅ 无旧数据，直接保存新数据
                # =============================
                if "日期" in df.columns:
                    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
                    df = df.dropna(subset=["日期"])
                    df["日期"] = df["日期"].dt.strftime("%Y-%m-%d")

                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                    df.to_csv(temp_file.name, index=False)

                shutil.move(temp_file.name, save_path)
                logger.info(f"✅ 数据已保存至: {save_path} ({len(df)}条)")

            saved_count += 1

        except Exception as e:
            logger.error(f"保存ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)

    return saved_count


def crawl_all_etfs_daily_data() -> None:
    """爬取所有ETF日线数据 - 真正批量保存版本"""
    try:
        logger.info("=== 开始执行ETF日线数据爬取 ===")
        beijing_time = get_beijing_time()
        logger.info(f"北京时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}（UTC+8）")
        
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(DAILY_DIR, exist_ok=True)
        logger.info(f"✅ 确保目录存在: {DATA_DIR}")
        
        etf_codes = get_all_etf_codes()
        total_count = len(etf_codes)
        
        if total_count == 0:
            logger.error("ETF列表为空，无法进行爬取")
            return
        
        logger.info(f"待爬取ETF总数：{total_count}只（全市场ETF）")
        
        next_index = get_next_crawl_index()
        
        # 修改：始终处理MAJOR_BATCH_SIZE只ETF，不足时循环补充
        total_to_process = MAJOR_BATCH_SIZE
        logger.info(f"本次将处理 {total_to_process} 只ETF（目标：{MAJOR_BATCH_SIZE}只）")
        logger.info(f"当前进度索引: {next_index}, 总ETF数量: {total_count}")
        
        processed_count = 0
        successful_count = 0
        failed_etfs = []
        
        # 修改：处理固定数量的ETF，不足时从索引0开始循环
        for i in range(total_to_process):
            # 计算当前ETF在列表中的实际索引（循环处理）
            current_index = (next_index + i) % total_count
            etf_code = etf_codes[current_index]
            etf_name = get_etf_name(etf_code)
            logger.info(f"正在处理第 {i+1}/{total_to_process} 只ETF: {etf_code} | {etf_name}")
            
            # 获取增量日期范围
            start_date, end_date = get_incremental_date_range(etf_code)
            if start_date is None or end_date is None:
                logger.info(f"ETF {etf_code} 数据已最新，跳过爬取")
                processed_count += 1
                successful_count += 1  # 跳过的也算成功处理
            else:
                # 爬取数据
                logger.info(f"📅 增量爬取日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
                df = crawl_etf_data(etf_code, start_date, end_date)
                
                if df.empty:
                    logger.warning(f"⚠️ 未获取到数据")
                    failed_etfs.append(f"{etf_code},{etf_name},未获取到数据")
                    processed_count += 1
                else:
                    # 保存数据
                    save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
                    
                    # ✅ 保存前规范化数据结构与精度
                    df = normalize_etf_df(df, etf_code, etf_name)
                    
                    try:
                        # 读取已有数据（如存在）
                        if os.path.exists(save_path):
                            existing_df = pd.read_csv(save_path)
                            
                            # 确保旧数据的"日期"列统一为datetime格式
                            if "日期" in existing_df.columns:
                                existing_df["日期"] = pd.to_datetime(existing_df["日期"], errors="coerce")
                            
                            # 确保新数据的"日期"列也是datetime格式
                            if "日期" in df.columns:
                                df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
                            
                            # 合并数据
                            combined_df = pd.concat([existing_df, df], ignore_index=True)
                            
                            # 再次统一日期列类型
                            combined_df["日期"] = pd.to_datetime(combined_df["日期"], errors="coerce")
                            
                            # 丢弃无效日期
                            invalid_dates = combined_df["日期"].isna().sum()
                            if invalid_dates > 0:
                                logger.warning(f"⚠️ ETF {etf_code} 合并后发现 {invalid_dates} 条无效日期记录，已过滤")
                                combined_df = combined_df.dropna(subset=["日期"])
                            
                            # 去重 + 排序
                            combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                            combined_df = combined_df.sort_values("日期", ascending=True).reset_index(drop=True)
                            
                            # 格式化日期列为字符串保存
                            combined_df["日期"] = combined_df["日期"].dt.strftime("%Y-%m-%d")
                            
                            # 临时文件安全写入
                            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                                combined_df.to_csv(temp_file.name, index=False)
                            
                            shutil.move(temp_file.name, save_path)
                            logger.info(f"✅ 数据已合并至: {save_path} (共{len(combined_df)}条)")
                        else:
                            # 无旧数据，直接保存新数据
                            if "日期" in df.columns:
                                df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
                                df = df.dropna(subset=["日期"])
                                df["日期"] = df["日期"].dt.strftime("%Y-%m-%d")
                            
                            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                                df.to_csv(temp_file.name, index=False)
                            
                            shutil.move(temp_file.name, save_path)
                            logger.info(f"✅ 数据已保存至: {save_path} ({len(df)}条)")
                        
                        successful_count += 1
                        processed_count += 1
                        logger.info(f"✅ ETF {etf_code} 数据获取成功，共 {len(df)} 条记录")
                        
                    except Exception as e:
                        logger.error(f"保存ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
                        failed_etfs.append(f"{etf_code},{etf_name},保存失败: {str(e)}")
                        processed_count += 1
            
            current_progress = f"{(next_index + i) % total_count}/{total_count} ({(next_index + i) % total_count/total_count*100:.1f}%)"
            logger.info(f"进度: {current_progress}")
            
            # 每10只ETF提交一次（包括最后一只）
            if (i + 1) % MINOR_BATCH_SIZE == 0 or i == total_to_process - 1:
                # 修改：无论是否有数据都要更新进度
                # 更新进度（当前已处理数量）
                new_index = (next_index + i + 1) % total_count
                logger.info(f"🔄 正在更新进度: {next_index} -> {new_index}")
                save_crawl_progress(new_index)
                logger.info(f"✅ 进度已更新为 {new_index}/{total_count}")
                
                # 提交进度文件
                logger.info("🔄 正在提交进度文件到Git...")
                progress_commit_success = commit_crawl_progress()
                if progress_commit_success:
                    logger.info(f"✅ 进度文件提交成功，进度更新为 {new_index}/{total_count}")
                else:
                    logger.error("❌ 进度文件提交失败")
                
                # 检查是否有成功的ETF需要提交数据文件
                batch_start = (i // MINOR_BATCH_SIZE) * MINOR_BATCH_SIZE
                batch_end = min((i // MINOR_BATCH_SIZE + 1) * MINOR_BATCH_SIZE, total_to_process)
                
                batch_etf_codes = []
                for j in range(batch_start, batch_end):
                    batch_index = (next_index + j) % total_count
                    batch_etf_codes.append(etf_codes[batch_index])
                
                # 检查这些ETF是否有数据文件
                batch_files = []
                for etf_code in batch_etf_codes:
                    file_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
                    if os.path.exists(file_path):
                        batch_files.append(file_path)
                
                if batch_files:
                    # 提交数据文件 - 使用新的Git工具
                    if len(batch_files) == MINOR_BATCH_SIZE:
                        # 完整批次使用commit_batch_files
                        commit_msg = f"feat: 批量提交{len(batch_files)}只ETF日线数据 - {datetime.now().strftime('%Y%m%d%H%M%S')}"
                        logger.info(f"提交完整批次数据文件: {commit_msg}")
                        commit_success = commit_batch_files(batch_files, commit_msg)
                    else:
                        # 剩余文件使用commit_remaining_files
                        commit_msg = f"feat: 提交剩余{len(batch_files)}只ETF日线数据 - {datetime.now().strftime('%Y%m%d%H%M%S')}"
                        logger.info(f"提交剩余数据文件: {commit_msg}")
                        commit_success = commit_remaining_files(batch_files, commit_msg)
                    
                    if commit_success:
                        logger.info(f"✅ 数据文件提交成功：{len(batch_files)}只")
                    else:
                        logger.error("❌ 数据文件提交失败")
                else:
                    logger.info("当前小批次没有数据文件需要提交")
                
                logger.info(f"🔄 已处理完批次，准备处理下一批次")
            
            # 每只ETF之间随机等待
            time.sleep(random.uniform(1.2, 4.6))
        
        # 处理结束后记录失败ETF
        if failed_etfs:
            failed_file = os.path.join(DAILY_DIR, "failed_etfs.txt")
            with open(failed_file, "w", encoding="utf-8") as f:
                f.write("\n".join(failed_etfs))
            logger.info(f"记录了 {len(failed_etfs)} 只失败的ETF")
        
        # 计算剩余ETF数量（相对于当前进度）
        final_index = (next_index + total_to_process) % total_count
        remaining_stocks = total_count - final_index
        if remaining_stocks < 0:
            remaining_stocks = total_count + remaining_stocks
            
        logger.info(f"本次爬取完成，共处理 {processed_count} 只ETF，成功 {successful_count} 只，失败 {len(failed_etfs)} 只")
        logger.info(f"进度已更新至: {final_index}/{total_count}")
        logger.info(f"还有 {remaining_stocks} 只ETF待爬取")
        
        # 最终确认进度更新
        final_check_index = get_next_crawl_index()
        logger.info(f"最终进度确认: {final_check_index}/{total_count}")
        
    except Exception as e:
        logger.error(f"ETF日线数据爬取任务执行失败: {str(e)}", exc_info=True)
        # 异常情况下尝试保存进度
        try:
            if 'next_index' in locals() and 'total_count' in locals() and 'i' in locals():
                new_index = (next_index + i + 1) % total_count
                logger.error(f"尝试保存进度以恢复状态... 新进度: {new_index}")
                save_crawl_progress(new_index)
                commit_crawl_progress()
                logger.info(f"进度已保存为 {new_index}/{total_count}")
        except Exception as save_error:
            logger.error(f"异常情况下保存进度失败: {str(save_error)}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        crawl_all_etfs_daily_data()
    except Exception as e:
        logger.error(f"ETF日线数据爬取失败: {str(e)}", exc_info=True)
        # 发送错误通知
        try:
            from wechat_push.push import send_wechat_message
            send_wechat_message(
                message=f"ETF日线数据爬取失败: {str(e)}",
                message_type="error"
            )
        except:
            pass
        # 确保进度文件已保存
        try:
            next_index = get_next_crawl_index()
            total_count = len(get_all_etf_codes())
            logger.info(f"当前进度: {next_index}/{total_count}")
        except Exception as e:
            logger.error(f"读取进度文件失败: {str(e)}")
