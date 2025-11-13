#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块 - 批量提交修复版
yFinance数据-etf_daily_crawler-DS10.py
【确保每10个成功文件一起提交，而不是只提交最后一个】
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
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files

# 初始化日志
logger = logging.getLogger(__name__)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "etf", "daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_etfs.csv")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)

# 关键参数
BATCH_SIZE = 40
COMMIT_BATCH_SIZE = 10

def get_etf_name(etf_code):
    """获取ETF名称"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            return etf_code
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        if basic_info_df.empty or "ETF代码" not in basic_info_df.columns:
            return etf_code
        etf_row = basic_info_df[basic_info_df["ETF代码"] == str(etf_code).strip()]
        return etf_row["ETF名称"].values[0] if not etf_row.empty else etf_code
    except Exception as e:
        logger.error(f"获取ETF名称失败: {str(e)}")
        return etf_code

def get_yfinance_symbol(etf_code):
    """获取yfinance对应的symbol"""
    if etf_code.startswith(('51', '56', '57', '58')):
        return f"{etf_code}.SS"
    elif etf_code.startswith('15'):
        return f"{etf_code}.SZ"
    else:
        return etf_code

def crawl_etf_data(etf_code, start_date, end_date):
    """爬取ETF数据"""
    try:
        symbol = get_yfinance_symbol(etf_code)
        df = yf.download(
            symbol,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
            timeout=30
        )
        
        if df is None or df.empty:
            return None
        
        # 处理数据
        result_df = pd.DataFrame()
        result_df['日期'] = df.index.strftime('%Y-%m-%d')
        result_df['开盘'] = df['Open'].astype(float)
        result_df['最高'] = df['High'].astype(float)
        result_df['最低'] = df['Low'].astype(float)
        result_df['收盘'] = df['Close'].astype(float)
        result_df['成交量'] = df['Volume'].astype(float)
        
        # 计算衍生字段
        result_df['振幅'] = ((result_df['最高'] - result_df['最低']) / result_df['最低'] * 100).round(2)
        result_df['涨跌额'] = result_df['收盘'].diff().fillna(0)
        
        prev_close = result_df['收盘'].shift(1)
        valid_prev_close = prev_close.replace(0, float('nan'))
        result_df['涨跌幅'] = (result_df['涨跌额'] / valid_prev_close * 100).round(2)
        result_df['涨跌幅'] = result_df['涨跌幅'].fillna(0)
        
        result_df['成交额'] = (result_df['收盘'] * result_df['成交量']).round(2)
        result_df['换手率'] = 0.0
        result_df['IOPV'] = 0.0
        result_df['折价率'] = 0.0
        result_df['溢价率'] = 0.0
        
        result_df['ETF代码'] = etf_code
        result_df['ETF名称'] = get_etf_name(etf_code)
        result_df['爬取时间'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return result_df
    except Exception as e:
        logger.error(f"yfinance爬取 {etf_code} 失败: {str(e)}")
        return None

def save_etf_data(etf_code, df):
    """保存ETF数据到本地"""
    if df is None or df.empty:
        return None
    
    try:
        save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
        
        # 如果文件已存在，合并数据
        if os.path.exists(save_path):
            try:
                existing_df = pd.read_csv(save_path)
                if "日期" in existing_df.columns:
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                    combined_df = combined_df.sort_values("日期")
                    df = combined_df
            except Exception as e:
                logger.warning(f"合并数据失败: {str(e)}")
        
        # 保存文件
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
            df.to_csv(temp_file.name, index=False)
        shutil.move(temp_file.name, save_path)
        
        logger.info(f"✅ 数据已保存到本地: {save_path} ({len(df)}条)")
        return save_path
    except Exception as e:
        logger.error(f"保存ETF {etf_code} 数据失败: {str(e)}")
        return None

def get_incremental_date_range(etf_code):
    """获取增量日期范围"""
    try:
        last_trading_day = get_last_trading_day()
        if not isinstance(last_trading_day, datetime):
            last_trading_day = datetime.now()
        
        end_date = last_trading_day.replace(hour=23, minute=59, second=59, microsecond=0)
        save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
        
        if os.path.exists(save_path):
            try:
                df = pd.read_csv(save_path)
                if "日期" not in df.columns or df.empty:
                    start_date = last_trading_day - timedelta(days=365)
                    return start_date, end_date
                
                df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
                valid_dates = df["日期"].dropna()
                
                if valid_dates.empty:
                    start_date = last_trading_day - timedelta(days=365)
                    return start_date, end_date
                
                latest_date = valid_dates.max()
                latest_date_date = latest_date.date()
                end_date_date = end_date.date()
                
                if latest_date_date < end_date_date:
                    start_date = latest_date + timedelta(days=1)
                    while not is_trading_day(start_date.date()):
                        start_date += timedelta(days=1)
                    
                    if start_date > end_date:
                        return None, None
                    
                    return start_date, end_date
                else:
                    return None, None
            except Exception as e:
                logger.error(f"读取历史文件失败: {str(e)}")
                start_date = last_trading_day - timedelta(days=365)
                return start_date, end_date
        else:
            start_date = last_trading_day - timedelta(days=365)
            return start_date, end_date
    except Exception as e:
        logger.error(f"获取日期范围失败: {str(e)}")
        last_trading_day = get_last_trading_day()
        start_date = last_trading_day - timedelta(days=365)
        end_date = last_trading_day.replace(hour=23, minute=59, second=59, microsecond=0)
        return start_date, end_date

def get_all_etf_codes():
    """获取所有ETF代码"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            return []
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        if basic_info_df.empty or "ETF代码" not in basic_info_df.columns:
            return []
        etf_codes = basic_info_df["ETF代码"].tolist()
        logger.info(f"获取到 {len(etf_codes)} 只ETF代码")
        return etf_codes
    except Exception as e:
        logger.error(f"获取ETF代码失败: {str(e)}")
        return []

def get_next_crawl_index():
    """获取下一个爬取索引"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            return 0
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        if basic_info_df.empty:
            return 0
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
            return 0
        return int(basic_info_df["next_crawl_index"].iloc[0])
    except Exception as e:
        logger.error(f"获取进度失败: {str(e)}")
        return 0

def save_crawl_progress(next_index):
    """保存爬取进度"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            return
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        if basic_info_df.empty:
            return
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
        basic_info_df["next_crawl_index"] = next_index
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"✅ 进度已保存: {next_index}/{len(basic_info_df)}")
    except Exception as e:
        logger.error(f"保存进度失败: {str(e)}")

def crawl_all_etfs_daily_data():
    """主爬取函数 - 批量提交修复版"""
    try:
        logger.info("=== 开始执行ETF日线数据爬取（批量提交修复版）===")
        
        # 获取ETF代码列表
        etf_codes = get_all_etf_codes()
        total_count = len(etf_codes)
        if total_count == 0:
            logger.error("ETF列表为空")
            return
        
        logger.info(f"待爬取ETF总数：{total_count}只")
        
        # 获取进度
        next_index = get_next_crawl_index()
        logger.info(f"当前进度：{next_index}/{total_count}")
        
        # 处理当前批次
        start_idx = next_index % total_count
        end_idx = start_idx + BATCH_SIZE
        
        if end_idx <= total_count:
            batch_codes = etf_codes[start_idx:end_idx]
        else:
            batch_codes = etf_codes[start_idx:total_count] + etf_codes[0:end_idx-total_count]
        
        logger.info(f"处理本批次 ETF ({len(batch_codes)}只)，从索引 {start_idx} 开始")
        
        # 【关键修复】使用列表累积成功文件，确保批量提交
        success_count = 0
        fail_count = 0
        skip_count = 0
        batch_files = []  # 累积成功文件的列表
        batch_number = 1  # 批次编号
        
        for i, etf_code in enumerate(batch_codes):
            time.sleep(random.uniform(3, 8))
            etf_name = get_etf_name(etf_code)
            current_index = (start_idx + i) % total_count
            logger.info(f"ETF代码：{etf_code}| 名称：{etf_name}")
            
            # 获取日期范围
            date_range = get_incremental_date_range(etf_code)
            if date_range[0] is None:
                logger.info(f"ETF {etf_code} 数据已最新，跳过")
                skip_count += 1
                logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
                continue
            
            start_date, end_date = date_range
            logger.info(f"📅 爬取日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
            
            # 爬取数据
            df = crawl_etf_data(etf_code, start_date, end_date)
            
            if df is None or df.empty:
                logger.warning(f"⚠️ ETF {etf_code} 未获取到数据")
                fail_count += 1
                logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
                continue
            
            # 保存数据到本地
            file_path = save_etf_data(etf_code, df)
            if file_path:
                # 【关键修复】添加到批量文件列表，而不是立即提交
                batch_files.append(file_path)
                success_count += 1
                logger.info(f"🎯 成功计数器: {success_count}/{COMMIT_BATCH_SIZE}")
                
                # 检查是否达到提交条件
                if success_count >= COMMIT_BATCH_SIZE:
                    logger.info(f"🚀 达到提交条件! 开始提交批次{batch_number}，包含 {len(batch_files)} 个文件")
                    
                    # 【关键修复】使用LAST_FILE参数确保批量提交
                    commit_message = f"自动更新ETF日线数据 批次{batch_number} [skip ci]"
                    commit_result = commit_files_in_batches("LAST_FILE", commit_message)
                    
                    if commit_result:
                        logger.info(f"✅ 批次{batch_number}提交成功! 提交了 {len(batch_files)} 个文件")
                        # 重置计数器和文件列表
                        success_count = 0
                        batch_files = []
                        batch_number += 1
                    else:
                        logger.error(f"❌ 批次{batch_number}提交失败!")
            else:
                fail_count += 1
            
            logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
        
        # 【关键修复】提交剩余文件
        if batch_files:
            logger.info(f"🚀 提交剩余 {len(batch_files)} 个文件")
            commit_message = f"自动更新ETF日线数据 最终批次 [skip ci]"
            commit_result = commit_files_in_batches("LAST_FILE", commit_message)
            if commit_result:
                logger.info("✅ 剩余文件提交成功!")
            else:
                logger.error("❌ 剩余文件提交失败!")
        
        # 更新进度
        new_index = end_idx % total_count
        save_crawl_progress(new_index)
        
        # 统计结果
        total_processed = success_count + fail_count + skip_count
        logger.info("=" * 60)
        logger.info("📊 统计结果:")
        logger.info(f"✅ 成功: {success_count}")
        logger.info(f"❌ 失败: {fail_count}") 
        logger.info(f"⏭️  跳过: {skip_count}")
        logger.info(f"📦 总计: {total_processed}/{len(batch_codes)}")
        logger.info(f"💾 提交批次: {batch_number - 1}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"ETF爬取任务失败: {str(e)}", exc_info=True)
        # 尝试提交剩余文件
        try:
            if 'batch_files' in locals() and batch_files:
                commit_files_in_batches("LAST_FILE", "紧急提交剩余文件 [skip ci]")
        except:
            pass
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
