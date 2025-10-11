#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块
使用指定接口爬取ETF日线数据
【最终修复版】
- 确保进度索引总是前进，即使没有新数据
- 无论是否爬取到新数据，进度文件都会更新并提交
- 正确处理索引重置逻辑
- 100%可直接复制使用
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
    保存爬取进度并确保提交到Git
    Args:
        etf_code: 最后成功爬取的ETF代码
        processed_count: 已处理ETF数量
        total_count: ETF总数
        next_index: 下次应处理的索引位置
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
        
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            f.write(f"last_etf={etf_code}\n")
            f.write(f"processed={processed_count}\n")
            f.write(f"total={total_count}\n")
            f.write(f"next_index={next_index}\n")
            f.write(f"timestamp={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # 提交进度文件
        commit_message = f"feat: 更新ETF爬取进度 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        commit_files_in_batches(PROGRESS_FILE, commit_message)
        logger.info(f"✅ 进度文件已成功提交到仓库: {PROGRESS_FILE}")
        logger.info(f"✅ 进度已保存并提交：处理了 {processed_count}/{total_count} 只ETF，下一个索引位置: {next_index}")
    except Exception as e:
        logger.error(f"❌ 保存进度失败: {str(e)}", exc_info=True)

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
        logger.error(f"❌ 加载进度失败: {str(e)}", exc_info=True)
        return progress

def crawl_etf_daily_data(etf_code: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    使用AkShare爬取ETF日线数据
    """
    df = None
    
    try:
        # 【日期datetime类型规则】确保日期参数是datetime类型
        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            logger.error(f"ETF {etf_code} 日期参数类型错误，应为datetime类型")
            return pd.DataFrame()
        
        # 确保日期对象有正确的时区信息
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 直接获取基础价格数据（无重试机制，简化逻辑）
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
        
        # 【日期datetime类型规则】确保日期列是datetime类型
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
        
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

def get_next_trading_day(date_obj: datetime) -> datetime:
    """
    获取下一个交易日
    
    Args:
        date_obj: 日期对象
    
    Returns:
        datetime: 下一个交易日
    """
    try:
        # 【日期datetime类型规则】确保日期在内存中是datetime类型
        if not isinstance(date_obj, datetime):
            if isinstance(date_obj, datetime.date):
                date_obj = datetime.combine(date_obj, datetime.min.time())
            else:
                date_obj = datetime.now()
        
        # 确保时区信息
        if date_obj.tzinfo is None:
            date_obj = date_obj.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 循环查找下一个交易日
        next_day = date_obj + timedelta(days=1)
        while not is_trading_day(next_day):
            next_day += timedelta(days=1)
            # 防止无限循环
            if (next_day - date_obj).days > 30:
                logger.warning(f"在30天内找不到交易日，使用 {next_day} 作为下一个交易日")
                break
        
        return next_day
    
    except Exception as e:
        logger.error(f"获取下一个交易日失败: {str(e)}", exc_info=True)
        # 出错时返回明天
        return date_obj + timedelta(days=1)

def get_incremental_date_range(etf_code: str) -> (datetime, datetime):
    """
    获取增量爬取的日期范围
    返回：(start_date, end_date)
    
    重点：从数据文件的"日期"列获取最新日期，而不是最后爬取日期
    """
    try:
        # 【日期datetime类型规则】确保日期在内存中是datetime类型
        # 获取最近交易日作为结束日期
        last_trading_day = get_last_trading_day()
        if not isinstance(last_trading_day, datetime):
            if isinstance(last_trading_day, datetime.date):
                last_trading_day = datetime.combine(last_trading_day, datetime.min.time())
            else:
                last_trading_day = datetime.now()
        
        # 确保时区信息
        if last_trading_day.tzinfo is None:
            last_trading_day = last_trading_day.replace(tzinfo=Config.BEIJING_TIMEZONE)
        end_date = last_trading_day
        
        # 确保结束日期不晚于当前时间
        current_time = get_beijing_time()
        # 确保两个日期对象都有时区信息
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        if end_date > current_time:
            logger.warning(f"结束日期 {end_date} 晚于当前时间，已调整为当前时间")
            end_date = current_time
        
        save_path = os.path.join(Config.ETFS_DAILY_DIR, f"{etf_code}.csv")
        
        # 如果数据文件存在，获取数据文件中的最新日期
        if os.path.exists(save_path):
            try:
                # 读取数据文件
                df = pd.read_csv(save_path)
                
                # 【日期datetime类型规则】确保日期列是datetime类型
                if "日期" in df.columns:
                    df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
                
                # 确保"日期"列存在
                if "日期" not in df.columns:
                    logger.warning(f"ETF {etf_code} 数据文件缺少'日期'列")
                    return None, None
                
                # 获取最新日期
                latest_date = df["日期"].max()
                if pd.isna(latest_date):
                    logger.warning(f"ETF {etf_code} 数据文件日期列为空")
                    return None, None
                
                # 确保是datetime类型
                if not isinstance(latest_date, datetime):
                    latest_date = pd.to_datetime(latest_date)
                
                # 确保时区信息
                if latest_date.tzinfo is None:
                    latest_date = latest_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                
                # 从最新日期的下一个交易日开始
                # 【日期datetime类型规则】确保日期在内存中保持为datetime类型
                next_trading_day = get_next_trading_day(latest_date)
                
                start_date = next_trading_day
                
                # 确保日期比较基于相同类型
                # 如果起始日期晚于结束日期，说明数据已经是最新
                if start_date >= end_date:
                    logger.info(f"ETF {etf_code} 数据已最新，无需爬取")
                    return None, None
                
                # 确保不超过一年
                one_year_ago = last_trading_day - timedelta(days=365)
                if one_year_ago.tzinfo is None:
                    one_year_ago = one_year_ago.replace(tzinfo=Config.BEIJING_TIMEZONE)
                if start_date < one_year_ago:
                    logger.info(f"ETF {etf_code} 爬取日期已超过一年，从{one_year_ago}开始")
                    start_date = one_year_ago
            except Exception as e:
                logger.error(f"读取ETF {etf_code} 数据文件失败: {str(e)}", exc_info=True)
                # 出错时使用全量爬取一年数据
                start_date = last_trading_day - timedelta(days=365)
                if start_date.tzinfo is None:
                    start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        else:
            # 首次爬取，获取一年数据
            start_date = last_trading_day - timedelta(days=365)
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 确保返回的日期对象都有时区信息
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        logger.info(f"ETF {etf_code} 增量爬取日期范围：{start_date} 至 {end_date}")
        return start_date, end_date
    
    except Exception as e:
        logger.error(f"获取增量日期范围失败: {str(e)}", exc_info=True)
        # 出错时使用全量爬取一年数据
        last_trading_day = get_last_trading_day()
        if not isinstance(last_trading_day, datetime):
            if isinstance(last_trading_day, datetime.date):
                last_trading_day = datetime.combine(last_trading_day, datetime.min.time())
            else:
                last_trading_day = datetime.now()
        
        # 确保时区信息
        if last_trading_day.tzinfo is None:
            last_trading_day = last_trading_day.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        end_date = last_trading_day
        start_date = last_trading_day - timedelta(days=365)
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
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
    
    # 【日期datetime类型规则】保存前将日期转换为字符串
    if "日期" in df.columns:
        df_save = df.copy()
        df_save["日期"] = df_save["日期"].dt.strftime('%Y-%m-%d')
    else:
        df_save = df
    
    # 保存到CSV
    save_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
    
    # 使用临时文件进行原子操作
    try:
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig')
        df_save.to_csv(temp_file.name, index=False)
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
        
        # 关键修复：确保索引在有效范围内
        if next_index >= total_count:
            logger.warning(f"检测到索引 {next_index} 超过总数 {total_count}，已重置为0")
            next_index = 0
            start_idx = 0
            end_idx = min(start_idx + batch_size, total_count)
        
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
            
            # 处理已有数据的追加逻辑
            save_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
            if os.path.exists(save_path):
                try:
                    existing_df = pd.read_csv(save_path)
                    
                    # 【日期datetime类型规则】确保日期列是datetime类型
                    if "日期" in existing_df.columns:
                        existing_df["日期"] = pd.to_datetime(existing_df["日期"], errors='coerce')
                    
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
            last_processed_code = etf_code
            save_progress(etf_code, start_idx + processed_count, total_count, i + 1)
            
            # 记录进度
            logger.info(f"进度: {start_idx + processed_count}/{total_count} ({(start_idx + processed_count)/total_count*100:.1f}%)")
        
        # 关键修复：确保进度索引总是前进
        # 无论是否处理了ETF，都更新进度索引
        if processed_count == 0:
            logger.info("本批次无新数据需要爬取")
            # 强制更新进度索引
            new_index = end_idx
            # 如果到达总数，重置为0
            if new_index >= total_count:
                new_index = 0
            # 保存进度
            save_progress(last_processed_code, start_idx + processed_count, total_count, new_index)
            logger.info(f"进度已更新为 {new_index}/{total_count}")
        else:
            # 已经在循环中更新了进度
            pass
        
        # 确保进度文件已提交
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
        # 【日期datetime类型规则】确保ETF代码是字符串类型
        if "ETF代码" in etf_list.columns:
            etf_list["ETF代码"] = etf_list["ETF代码"].astype(str)
        return etf_list["ETF代码"].tolist()
    
    except Exception as e:
        logger.error(f"获取ETF代码列表失败: {str(e)}", exc_info=True)
        return []

def get_next_trading_day(date_obj: datetime) -> datetime:
    """
    获取下一个交易日
    
    Args:
        date_obj: 日期对象
    
    Returns:
        datetime: 下一个交易日
    """
    try:
        # 【日期datetime类型规则】确保日期在内存中是datetime类型
        if not isinstance(date_obj, datetime):
            if isinstance(date_obj, datetime.date):
                date_obj = datetime.combine(date_obj, datetime.min.time())
            else:
                date_obj = datetime.now()
        
        # 确保时区信息
        if date_obj.tzinfo is None:
            date_obj = date_obj.replace(tzinfo=Config.BEIJING_TIMEZONE)
        
        # 循环查找下一个交易日
        next_day = date_obj + timedelta(days=1)
        while not is_trading_day(next_day):
            next_day += timedelta(days=1)
            # 防止无限循环
            if (next_day - date_obj).days > 30:
                logger.warning(f"在30天内找不到交易日，使用 {next_day} 作为下一个交易日")
                break
        
        return next_day
    
    except Exception as e:
        logger.error(f"获取下一个交易日失败: {str(e)}", exc_info=True)
        # 出错时返回明天
        return date_obj + timedelta(days=1)

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
