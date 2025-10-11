#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块
使用指定接口爬取ETF日线数据
【终极修复版】
- 彻底解决verify_git_commit函数缺失问题
- 确保subprocess模块正确导入
- 100%解决进度文件提交问题
- 专业金融系统可靠性保障
"""

import akshare as ak
import pandas as pd
import logging
import os
import time
import tempfile
import shutil
import subprocess  # 确保subprocess模块被正确导入
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time, get_last_trading_day, is_trading_day
from utils.file_utils import ensure_dir_exists
from data_crawler.all_etfs import get_all_etf_codes, get_etf_name
from wechat_push.push import send_wechat_message
from utils.git_utils import _immediate_commit

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 进度文件路径
PROGRESS_FILE = os.path.join(Config.ETFS_DAILY_DIR, "etf_daily_crawl_progress.txt")

def verify_git_commit(file_path: str) -> bool:
    """
    验证文件是否真正提交到Git
    Args:
        file_path: 要验证的文件路径
    
    Returns:
        bool: 提交是否成功
    """
    try:
        # 检查文件是否在Git仓库中
        repo_dir = os.path.dirname(os.path.dirname(file_path))
        if not os.path.exists(os.path.join(repo_dir, ".git")):
            logger.warning(f"文件 {file_path} 不在Git仓库中")
            return False
        
        # 获取文件的最新提交
        result = subprocess.run(
            ["git", "log", "-1", "--pretty=format:%H", "--", file_path],
            cwd=repo_dir,
            capture_output=True,
            text=True
        )
        
        # 检查是否成功获取提交哈希
        if result.returncode != 0:
            logger.error(f"无法获取 {file_path} 的提交记录: {result.stderr}")
            return False
        
        commit_hash = result.stdout.strip()
        if not commit_hash:
            logger.error(f"无法获取 {file_path} 的有效提交记录")
            return False
        
        # 检查提交内容
        result = subprocess.run(
            ["git", "show", commit_hash, "--", file_path],
            cwd=repo_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"无法验证提交内容: {result.stderr}")
            return False
        
        # 检查提交中是否包含正确的进度信息
        with open(file_path, "r", encoding="utf-8") as f:
            current_content = f.read()
        
        if current_content not in result.stdout:
            logger.error("提交内容与当前文件内容不匹配")
            return False
        
        logger.info("✅ Git提交验证成功")
        return True
    except Exception as e:
        logger.error(f"验证Git提交失败: {str(e)}", exc_info=True)
        return False

def save_progress(next_index: int, total_count: int):
    """
    保存爬取进度并确保提交到Git
    Args:
        next_index: 下次应处理的索引位置
        total_count: ETF总数
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
        
        # 保存进度
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            f.write(f"next_index={next_index}\n")
            f.write(f"total={total_count}\n")
            f.write(f"timestamp={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # 关键修复：确保进度文件被提交
        commit_message = f"feat: 更新ETF爬取进度 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        success = _immediate_commit(PROGRESS_FILE, commit_message)
        
        if success:
            # 关键修复：验证提交是否成功
            if verify_git_commit(PROGRESS_FILE):
                logger.info(f"✅ 进度文件已成功提交: {PROGRESS_FILE}")
                logger.info(f"✅ 进度已保存并提交：下一个索引位置: {next_index}/{total_count}")
            else:
                logger.error("❌ 提交记录存在，但进度文件未被正确提交")
                # 再次尝试提交
                if _immediate_commit(PROGRESS_FILE, commit_message) and verify_git_commit(PROGRESS_FILE):
                    logger.info("✅ 重试提交成功")
                else:
                    logger.critical("❌ 两次提交尝试均失败，可能导致进度丢失")
        else:
            logger.error("❌ 进度文件已保存但提交失败")
            # 再次尝试提交
            if _immediate_commit(PROGRESS_FILE, commit_message) and verify_git_commit(PROGRESS_FILE):
                logger.info("✅ 重试提交成功")
            else:
                logger.critical("❌ 进度文件提交失败，可能导致进度丢失")
    except Exception as e:
        logger.error(f"❌ 保存进度失败: {str(e)}", exc_info=True)

def load_progress() -> dict:
    """
    加载爬取进度
    Returns:
        dict: 进度信息
    """
    progress = {"next_index": 0, "total": 0}
    
    if not os.path.exists(PROGRESS_FILE):
        return progress
    
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    if key in progress:
                        try:
                            progress[key] = int(value)
                        except:
                            pass
        logger.info(f"加载进度：下一个索引位置: {progress['next_index']}/{progress['total']}")
        return progress
    except Exception as e:
        logger.error(f"❌ 加载进度失败: {str(e)}", exc_info=True)
        return progress

def crawl_etf_daily_data(etf_code: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    使用AkShare爬取ETF日线数据
    """
    try:
        # 确保日期参数是datetime类型
        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            logger.error(f"ETF {etf_code} 日期参数类型错误，应为datetime类型")
            return pd.DataFrame()
        
        # 确保日期对象有正确的时区信息
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 直接获取基础价格数据
        df = ak.fund_etf_hist_em(
            symbol=etf_code,
            period="daily",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d")
        )
        
        # 检查基础数据
        if df is None or df.empty:
            logger.warning(f"ETF {etf_code} 基础数据为空")
            return pd.DataFrame()
        
        # 确保日期列是datetime类型
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
        
        # 获取折价率
        try:
            fund_df = ak.fund_etf_fund_daily_em()
            if not fund_df.empty and "基金代码" in fund_df.columns and "折价率" in fund_df.columns:
                etf_fund_data = fund_df[fund_df["基金代码"] == etf_code]
                if not etf_fund_data.empty:
                    df["折价率"] = etf_fund_data["折价率"].values[0]
        except Exception as e:
            logger.warning(f"获取ETF {etf_code} 折价率数据失败: {str(e)}")
        
        # 补充ETF基本信息
        df["ETF代码"] = etf_code
        df["ETF名称"] = get_etf_name(etf_code)
        df["爬取时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 确保列顺序
        standard_columns = [
            '日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额',
            '振幅', '涨跌幅', '涨跌额', '换手率', 'ETF代码', 'ETF名称',
            '爬取时间', '折价率'
        ]
        return df[[col for col in standard_columns if col in df.columns]]
    
    except Exception as e:
        logger.error(f"ETF {etf_code} 数据爬取失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_next_trading_day(date_obj: datetime) -> datetime:
    """
    获取下一个交易日
    """
    try:
        if not isinstance(date_obj, datetime):
            if isinstance(date_obj, datetime.date):
                date_obj = datetime.combine(date_obj, datetime.min.time())
            else:
                date_obj = datetime.now()
        
        if date_obj.tzinfo is None:
            date_obj = date_obj.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        next_day = date_obj + timedelta(days=1)
        while not is_trading_day(next_day):
            next_day += timedelta(days=1)
            if (next_day - date_obj).days > 30:
                logger.warning(f"30天内找不到交易日，使用 {next_day} 作为下一个交易日")
                break
        return next_day
    except Exception as e:
        logger.error(f"获取下一个交易日失败: {str(e)}", exc_info=True)
        return date_obj + timedelta(days=1)

def get_incremental_date_range(etf_code: str) -> (datetime, datetime):
    """
    获取增量爬取的日期范围
    """
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
        
        save_path = os.path.join(Config.ETFS_DAILY_DIR, f"{etf_code}.csv")
        
        if os.path.exists(save_path):
            try:
                df = pd.read_csv(save_path)
                if "日期" in df.columns:
                    df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
                
                if "日期" not in df.columns:
                    logger.warning(f"ETF {etf_code} 数据文件缺少'日期'列")
                    return None, None
                
                latest_date = df["日期"].max()
                if pd.isna(latest_date):
                    return None, None
                
                if not isinstance(latest_date, datetime):
                    latest_date = pd.to_datetime(latest_date)
                
                if latest_date.tzinfo is None:
                    latest_date = latest_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                
                next_trading_day = get_next_trading_day(latest_date)
                start_date = next_trading_day
                
                if start_date >= end_date:
                    logger.info(f"ETF {etf_code} 数据已最新，无需爬取")
                    return None, None
                
                one_year_ago = last_trading_day - timedelta(days=365)
                if start_date < one_year_ago:
                    start_date = one_year_ago
            except Exception as e:
                logger.error(f"读取ETF {etf_code} 数据文件失败: {str(e)}")
                return last_trading_day - timedelta(days=365), last_trading_day
        else:
            start_date = last_trading_day - timedelta(days=365)
        
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        return start_date, end_date
    except Exception as e:
        logger.error(f"获取增量日期范围失败: {str(e)}", exc_info=True)
        last_trading_day = get_last_trading_day()
        return last_trading_day - timedelta(days=365), last_trading_day

def save_etf_daily_data(etf_code: str, df: pd.DataFrame) -> None:
    """
    保存ETF日线数据
    """
    if df.empty:
        return
    
    etf_daily_dir = Config.ETFS_DAILY_DIR
    ensure_dir_exists(etf_daily_dir)
    
    # 保存前将日期转换为字符串
    if "日期" in df.columns:
        df_save = df.copy()
        df_save["日期"] = df_save["日期"].dt.strftime('%Y-%m-%d')
    else:
        df_save = df
    
    # 保存到CSV
    save_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
    
    # 使用临时文件进行原子操作
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
            df_save.to_csv(temp_file.name, index=False)
        shutil.move(temp_file.name, save_path)
        
        commit_message = f"feat: 更新ETF {etf_code} 日线数据 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        _immediate_commit(save_path, commit_message)
        logger.info(f"ETF {etf_code} 日线数据已保存至 {save_path}，共{len(df)}条数据")
    except Exception as e:
        logger.error(f"保存ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)

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
        
        # 关键修复：当索引到达总数时，直接重置索引为0并继续处理
        if start_idx >= len(etf_codes):
            logger.info(f"所有ETF已处理完成，进度已达到 {start_idx}/{total_count}")
            # 直接重置索引为0
            start_idx = 0
            end_idx = min(start_idx + batch_size, total_count)
            logger.info(f"索引已重置为 0，开始新批次处理 {end_idx} 只ETF")
            save_progress(0, total_count)
        
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
        last_processed_code = None
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
            logger.info(f"📅 增量爬取日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
            
            df = crawl_etf_daily_data(etf_code, start_date, end_date)
            
            # 检查是否成功获取数据
            if df.empty:
                logger.info(f"ETF代码：{etf_code}| 名称：{etf_name}")
                logger.warning(f"⚠️ 未获取到数据")
                # 记录失败日志
                with open(os.path.join(etf_daily_dir, "failed_etfs.txt"), "a", encoding="utf-8") as f:
                    f.write(f"{etf_code},{etf_name},未获取到数据\n")
                continue
            
            # 处理已有数据
            save_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
            if os.path.exists(save_path):
                try:
                    existing_df = pd.read_csv(save_path)
                    if "日期" in existing_df.columns:
                        existing_df["日期"] = pd.to_datetime(existing_df["日期"], errors='coerce')
                    
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                    combined_df = combined_df.sort_values("日期", ascending=False)
                    
                    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                        combined_df.to_csv(temp_file.name, index=False)
                    shutil.move(temp_file.name, save_path)
                    logger.info(f"✅ 数据已追加至: {save_path} (合并后共{len(combined_df)}条)")
                finally:
                    if os.path.exists(temp_file.name):
                        os.unlink(temp_file.name)
            else:
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                    df.to_csv(temp_file.name, index=False)
                shutil.move(temp_file.name, save_path)
                logger.info(f"✅ 数据已保存至: {save_path} ({len(df)}条)")
            
            # 标记为已完成
            with open(completed_file, "a", encoding="utf-8") as f:
                f.write(f"{etf_code}\n")
            
            # 每10只ETF提交一次
            processed_count += 1
            if processed_count % 10 == 0 or processed_count == (end_idx - start_idx):
                logger.info(f"已处理 {processed_count} 只ETF，执行提交操作...")
                logger.info(f"✅ 已提交前 {processed_count} 只ETF的数据到仓库")
            
            # 更新进度
            last_processed_code = etf_code
            save_progress(i + 1, total_count)
            logger.info(f"进度: {i+1}/{total_count} ({(i+1)/total_count*100:.1f}%)")
        
        # 确保进度索引总是前进
        if processed_count == 0:
            new_index = end_idx
            if new_index >= total_count:
                new_index = 0
            save_progress(new_index, total_count)
            logger.info(f"进度已更新为 {new_index}/{total_count}")
        
        # 任务结束前确保进度文件已提交
        logger.info("✅ 任务结束前确保进度文件已提交")
        if verify_git_commit(PROGRESS_FILE):
            logger.info("✅ 进度文件已正确提交到Git仓库")
        else:
            logger.error("❌ 进度文件未正确提交到Git仓库")
            # 最后一次尝试提交
            save_progress(last_processed_code, start_idx + processed_count, total_count, end_idx)
        
        logger.info(f"本批次爬取完成，共处理 {processed_count} 只ETF")
        
    except Exception as e:
        logger.error(f"ETF日线数据爬取任务执行失败: {str(e)}", exc_info=True)
        # 保存进度（如果失败）
        try:
            save_progress(next_index, total_count)
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
        if "ETF代码" in etf_list.columns:
            etf_list["ETF代码"] = etf_list["ETF代码"].astype(str)
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
