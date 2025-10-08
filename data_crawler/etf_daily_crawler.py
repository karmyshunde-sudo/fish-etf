#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块
使用指定接口爬取ETF日线数据
【最终修复版】
- 与股票日线爬取完全一致的逻辑
- 每次只爬取100只ETF
- 每10只ETF提交到Git仓库
- 从进度文件读取继续爬取的位置
- 无任何延时或防封机制
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
from utils.date_utils import get_beijing_time, get_last_trading_day, is_trading_day
from utils.file_utils import ensure_dir_exists, get_last_crawl_date
from data_crawler.all_etfs import get_all_etf_codes, get_etf_name
from wechat_push.push import send_wechat_message
from utils.git_utils import commit_files_in_batches

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 进度文件路径 - 与股票日线爬取相同
PROGRESS_FILE = os.path.join(Config.ETFS_DAILY_DIR, "etf_daily_crawl_progress.txt")

def save_progress(etf_code: str, processed_count: int, total_count: int, next_index: int):
    """
    保存爬取进度
    Args:
        etf_code: 最后成功爬取的ETF代码
        processed_count: 已处理ETF数量
        total_count: ETF总数
        next_index: 下次应处理的索引位置
    """
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            f.write(f"last_etf={etf_code}\n")
            f.write(f"processed={processed_count}\n")
            f.write(f"total={total_count}\n")
            f.write(f"next_index={next_index}\n")
            f.write(f"timestamp={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        logger.info(f"进度已保存：处理了 {processed_count}/{total_count} 只ETF，下一个索引位置: {next_index}")
    except Exception as e:
        logger.error(f"保存进度失败: {str(e)}", exc_info=True)

def load_progress() -> dict:
    """
    加载爬取进度
    Returns:
        dict: 进度信息
    """
    progress = {
        "last_etf": None,
        "processed": 0,
        "total": 0,
        "next_index": 0,
        "timestamp": None
    }
    
    if not os.path.exists(PROGRESS_FILE):
        return progress
    
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    if key in progress:
                        if key == "processed" or key == "total" or key == "next_index":
                            try:
                                progress[key] = int(value)
                            except:
                                pass
                        elif key == "timestamp":
                            try:
                                progress[key] = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                            except:
                                pass
                        else:
                            progress[key] = value
        logger.info(f"加载进度：已处理 {progress['processed']}/{progress['total']} 只ETF，下一个索引位置: {progress['next_index']}")
        return progress
    except Exception as e:
        logger.error(f"加载进度失败: {str(e)}", exc_info=True)
        return progress

def crawl_etf_daily_data(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    使用AkShare爬取ETF日线数据
    """
    df = None
    
    try:
        # 直接获取基础价格数据（无重试机制，简化逻辑）
        df = ak.fund_etf_hist_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date,
            end_date=end_date
        )
        
        # 检查基础数据
        if df is None or df.empty:
            logger.warning(f"ETF {etf_code} 基础数据为空")
            return pd.DataFrame()
        
        # 获取折价率
        try:
            fund_df = ak.fund_etf_fund_daily_em()
            if not fund_df.empty and "基金代码" in fund_df.columns and "折价率" in fund_df.columns:
                etf_fund_data = fund_df[fund_df["基金代码"] == etf_code]
                if not etf_fund_data.empty:
                    # 从fund_df提取折价率
                    df["折价率"] = etf_fund_data["折价率"].values[0]
        except Exception as e:
            logger.warning(f"获取ETF {etf_code} 折价率数据失败: {str(e)}")
        
        # 补充ETF基本信息
        df["ETF代码"] = etf_code
        df["ETF名称"] = get_etf_name(etf_code)
        df["爬取时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 确保列顺序与目标结构一致
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

def get_incremental_date_range(etf_code: str) -> (str, str):
    """
    获取增量爬取的日期范围
    返回：(start_date, end_date)
    
    重点：从数据文件的"日期"列获取最新日期，而不是最后爬取日期
    """
    try:
        # 获取最近交易日作为结束日期
        last_trading_day = get_last_trading_day()
        end_date = last_trading_day.strftime("%Y%m%d")
        
        # 确保结束日期不晚于当前日期
        current_date = datetime.now().date()
        if datetime.strptime(end_date, "%Y%m%d").date() > current_date:
            logger.warning(f"结束日期 {end_date} 晚于当前日期，已调整为当前日期")
            end_date = current_date.strftime("%Y%m%d")
        
        save_path = os.path.join(Config.ETFS_DAILY_DIR, f"{etf_code}.csv")
        
        # 如果数据文件存在，获取数据文件中的最新日期
        if os.path.exists(save_path):
            try:
                # 读取数据文件
                df = pd.read_csv(save_path)
                
                # 确保"日期"列存在
                if "日期" not in df.columns:
                    logger.warning(f"ETF {etf_code} 数据文件缺少'日期'列")
                    return None, None
                
                # 确保日期列是字符串类型
                df["日期"] = df["日期"].astype(str)
                
                # 获取最新日期
                latest_date = df["日期"].max()
                
                # 转换为日期对象
                latest_date_obj = datetime.strptime(latest_date, "%Y-%m-%d").date()
                
                # 从最新日期的下一个交易日开始
                next_trading_day = latest_date_obj + timedelta(days=1)
                
                # 确保是交易日
                while not is_trading_day(next_trading_day):
                    next_trading_day += timedelta(days=1)
                
                start_date = next_trading_day.strftime("%Y%m%d")
                
                # 确保日期比较基于相同类型
                start_date_obj = datetime.strptime(start_date, "%Y%m%d").date()
                end_date_obj = datetime.strptime(end_date, "%Y%m%d").date()
                
                # 如果起始日期晚于结束日期，说明数据已经是最新
                if start_date_obj > end_date_obj:
                    logger.info(f"ETF {etf_code} 数据已最新，无需爬取")
                    return None, None
                
                # 确保不超过一年
                one_year_ago = last_trading_day - timedelta(days=365)
                if start_date_obj < one_year_ago:
                    logger.info(f"ETF {etf_code} 爬取日期已超过一年，从{one_year_ago.strftime('%Y%m%d')}开始")
                    start_date = one_year_ago.strftime("%Y%m%d")
            except Exception as e:
                logger.error(f"读取ETF {etf_code} 数据文件失败: {str(e)}", exc_info=True)
                # 出错时使用全量爬取一年数据
                start_date = (last_trading_day - timedelta(days=365)).strftime("%Y%m%d")
        else:
            # 首次爬取，获取一年数据
            start_date = (last_trading_day - timedelta(days=365)).strftime("%Y%m%d")
        
        logger.info(f"ETF {etf_code} 增量爬取日期范围：{start_date} 至 {end_date}")
        return start_date, end_date
    
    except Exception as e:
        logger.error(f"获取增量日期范围失败: {str(e)}", exc_info=True)
        # 出错时使用全量爬取一年数据
        last_trading_day = get_last_trading_day()
        end_date = last_trading_day.strftime("%Y%m%d")
        start_date = (last_trading_day - timedelta(days=365)).strftime("%Y%m%d")
        return start_date, end_date

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
        
        # 【关键修改】使用git工具模块提交变更
        commit_files_in_batches(save_path)
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
        
        # 获取所有ETF代码
        etf_codes = get_all_etf_codes()
        total_count = len(etf_codes)
        logger.info(f"待爬取ETF总数：{total_count}只（全市场ETF）")
        
        # 加载进度
        progress = load_progress()
        next_index = progress["next_index"]
        
        # 确定处理范围
        batch_size = 100
        start_idx = next_index
        end_idx = min(start_idx + batch_size, len(etf_codes))
        
        logger.info(f"处理本批次 ETF ({end_idx - start_idx}只)，从索引 {start_idx} 开始")
        
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
        
        # 处理当前批次
        processed_count = 0
        for i in range(start_idx, end_idx):
            etf_code = etf_codes[i]
            etf_name = get_etf_name(etf_code)
            
            # 获取增量日期范围
            start_date, end_date = get_incremental_date_range(etf_code)
            if start_date is None or end_date is None:
                logger.info(f"ETF {etf_code} 数据已最新，跳过爬取")
                continue
            
            # 爬取数据
            logger.info(f"ETF代码：{etf_code}| 名称：{etf_name}")
            logger.info(f"📅 增量爬取日期范围：{start_date} 至 {end_date}")
            
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
            
            # 每10只ETF提交一次
            processed_count += 1
            if processed_count % 10 == 0 or processed_count == (end_idx - start_idx):
                logger.info(f"已处理 {processed_count} 只ETF，执行提交操作...")
                try:
                    from utils.git_utils import commit_final
                    commit_final()
                    logger.info(f"已提交前 {processed_count} 只ETF的数据到仓库")
                except Exception as e:
                    logger.error(f"提交文件时出错，继续执行: {str(e)}")
            
            # 更新进度
            save_progress(etf_code, start_idx + processed_count, total_count, i + 1)
            
            # 记录进度
            logger.info(f"进度: {start_idx + processed_count}/{total_count} ({(start_idx + processed_count)/total_count*100:.1f}%)")
        
        # 爬取完本批次后，直接退出，等待下一次调用
        logger.info(f"本批次爬取完成，共处理 {processed_count} 只ETF")
        logger.info("程序将退出，等待工作流再次调用")
        
    except Exception as e:
        logger.error(f"ETF日线数据爬取任务执行失败: {str(e)}", exc_info=True)
        # 保存进度（如果失败）
        try:
            save_progress(None, next_index, total_count, next_index)
        except:
            pass
        raise

def get_all_etf_codes() -> list:
    """
    获取所有ETF代码
    """
    try:
        etf_list_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
        if not os.path.exists(etf_list_file):
            logger.info("ETF列表文件不存在，正在创建...")
            from data_crawler.all_etfs import update_all_etf_list
            update_all_etf_list()
        
        etf_list = pd.read_csv(etf_list_file)
        return etf_list["ETF代码"].tolist()
    
    except Exception as e:
        logger.error(f"获取ETF代码列表失败: {str(e)}", exc_info=True)
        return []

if __name__ == "__main__":
    try:
        crawl_all_etfs_daily_data()
    finally:
        # 确保进度文件已保存
        try:
            progress = load_progress()
            logger.info(f"当前进度: {progress['next_index']}/{progress['total']}")
        except Exception as e:
            logger.error(f"读取进度文件失败: {str(e)}")
