#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块
使用指定接口爬取ETF日线数据
"""

import akshare as ak
import pandas as pd
import logging
import os
import time
import tempfile
import shutil
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time, get_last_trading_day
from utils.file_utils import ensure_dir_exists, get_last_crawl_date
from data_crawler.all_etfs import get_all_etf_codes, get_etf_name
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def crawl_etf_daily_data(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    使用AkShare爬取ETF日线数据
    """
    try:
        # 1. 获取基础价格数据
        df = ak.fund_etf_hist_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        
        # 2. 检查基础数据
        if df.empty:
            logger.warning(f"ETF {etf_code} 基础数据为空")
            return pd.DataFrame()
        
        # 3. 获取折价率
        fund_df = ak.fund_etf_fund_daily_em()
        if not fund_df.empty and "基金代码" in fund_df.columns and "折价率" in fund_df.columns:
            etf_fund_data = fund_df[fund_df["基金代码"] == etf_code]
            if not etf_fund_data.empty:
                # 从fund_df提取折价率
                df["折价率"] = etf_fund_data["折价率"].values[0]
        
        # 4. 补充ETF基本信息
        df["ETF代码"] = etf_code
        df["ETF名称"] = get_etf_name(etf_code)
        df["爬取时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 5. 确保列顺序与目标结构一致
        standard_columns = [
            '日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额',
            '振幅', '涨跌幅', '涨跌额', '换手率', 'ETF代码', 'ETF名称',
            '爬取时间', '折价率'
        ]
        
        # 只保留目标列
        df = df[[col for col in standard_columns if col in df.columns]]
        
        return df
    
    except Exception as e:
        logger.error(f"ETF {etf_code} 数据爬取失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def save_etf_daily_data(etf_code: str, df: pd.DataFrame) -> None:
    """
    保存ETF日线数据
    """
    if df.empty:
        return
    
    # 确保目录存在
    etf_daily_dir = Config.ETFS_DAILY_DIR
    ensure_dir_exists(etf_daily_dir)
    
    # 保存到CSV
    save_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
    
    # 使用临时文件进行原子操作
    try:
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig')
        df.to_csv(temp_file.name, index=False)
        # 原子替换
        shutil.move(temp_file.name, save_path)
        logger.info(f"ETF {etf_code} 日线数据已保存至 {save_path}，共{len(df)}条数据")
    except Exception as e:
        logger.error(f"保存ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
    finally:
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)

def crawl_all_etfs_daily_data() -> None:
    """
    爬取所有ETF日线数据
    """
    try:
        logger.info("=== 开始执行ETF日线数据爬取 ===")
        beijing_time = get_beijing_time()
        logger.info(f"北京时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}（UTC+8）")
        
        # 初始化目录
        Config.init_dirs()
        etf_daily_dir = Config.ETFS_DAILY_DIR
        logger.info(f"✅ 确保目录存在: {etf_daily_dir}")
        
        # 获取最近一个交易日作为结束日期
        last_trading_day = get_last_trading_day()
        end_date = last_trading_day.strftime("%Y%m%d")
        
        # 获取所有ETF代码
        etf_codes = get_all_etf_codes()
        logger.info(f"待爬取ETF总数：{len(etf_codes)}只（全市场ETF）")
        
        # 已完成列表路径
        completed_file = os.path.join(etf_daily_dir, "etf_daily_completed.txt")
        
        # 加载已完成列表
        completed_codes = set()
        if os.path.exists(completed_file):
            try:
                with open(completed_file, "r", encoding="utf-8") as f:
                    completed_codes = set(line.strip() for line in f if line.strip())
                logger.info(f"进度记录中已完成爬取的ETF数量：{len(completed_codes)}")
            except Exception as e:
                logger.error(f"读取进度记录失败: {str(e)}", exc_info=True)
                completed_codes = set()
        
        # 分批爬取
        batch_size = Config.CRAWL_BATCH_SIZE
        num_batches = (len(etf_codes) + batch_size - 1) // batch_size
        
        # 初始化一个列表来跟踪需要提交的文件
        files_to_commit = []
        
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(etf_codes))
            batch_codes = etf_codes[start_idx:end_idx]
            
            logger.info(f"处理第 {batch_idx+1}/{num_batches} 批 ETF ({len(batch_codes)}只)")
            
            for etf_code in batch_codes:
                etf_name = get_etf_name(etf_code)
                
                # 确定爬取时间范围（一年）
                start_date = (last_trading_day - timedelta(days=365)).strftime("%Y%m%d")
                
                # 爬取数据
                logger.info(f"ETF代码：{etf_code}| 名称：{etf_name}")
                logger.info(f"📅 爬取一年历史数据：{start_date} 至 {end_date}")
                
                df = crawl_etf_daily_data(etf_code, start_date, end_date)
                
                # 检查是否成功获取数据
                if df.empty:
                    logger.info(f"ETF代码：{etf_code}| 名称：{etf_name}")
                    logger.warning(f"⚠️ 未获取到数据")
                    # 记录失败日志
                    with open(os.path.join(etf_daily_dir, "failed_etfs.txt"), "a", encoding="utf-8") as f:
                        f.write(f"{etf_code},{etf_name},未获取到数据\n")
                    continue
                
                # 处理已有数据的追加逻辑
                save_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
                if os.path.exists(save_path):
                    try:
                        existing_df = pd.read_csv(save_path)
                        
                        # 合并数据并去重
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                        combined_df = combined_df.sort_values("日期", ascending=False)
                        
                        # 使用临时文件进行原子操作
                        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig')
                        combined_df.to_csv(temp_file.name, index=False)
                        # 原子替换
                        shutil.move(temp_file.name, save_path)
                        logger.info(f"✅ 数据已追加至: {save_path} (合并后共{len(combined_df)}条)")
                    finally:
                        if os.path.exists(temp_file.name):
                            os.unlink(temp_file.name)
                else:
                    # 使用临时文件进行原子操作
                    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig')
                    try:
                        df.to_csv(temp_file.name, index=False)
                        # 原子替换
                        shutil.move(temp_file.name, save_path)
                        logger.info(f"✅ 数据已保存至: {save_path} ({len(df)}条)")
                    finally:
                        if os.path.exists(temp_file.name):
                            os.unlink(temp_file.name)
                
                # 标记为已完成
                with open(completed_file, "a", encoding="utf-8") as f:
                    f.write(f"{etf_code}\n")
                
                # 限制请求频率
                time.sleep(1)
            
            # 批次间暂停
            if batch_idx < num_batches - 1:
                batch_pause_seconds = 2
                logger.info(f"批次处理完成，暂停 {batch_pause_seconds} 秒...")
                time.sleep(batch_pause_seconds)
    
    except Exception as e:
        logger.error(f"ETF日线数据爬取任务执行失败: {str(e)}", exc_info=True)
        raise

def get_all_etf_codes() -> list:
    """
    获取所有ETF代码
    """
    # 从all_etfs模块获取
    from all_etfs import get_all_etf_codes
    return get_all_etf_codes()
