#=====5数据源crawler-QW11.py=====
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票日线数据爬取模块 - 完全遵循ETF爬取逻辑
【2025-11-15：彻底解决Git提交问题】
- 每10只股票数据缓存并提交
- 严格遵循ETF爬取逻辑：数据缓存 -> 小批次提交 -> 兜底提交 -> 最后更新进度
- 确保数据文件先提交，进度文件后更新
"""

import os
import logging
import pandas as pd
import akshare as ak
import time
import random
import tempfile
import shutil
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import is_trading_day, get_last_trading_day, get_beijing_time
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files, _verify_git_file_content
from stock.all_stocks import update_stock_list
from stock.stock_source import get_stock_daily_data_from_sources

# 配置日志
logger = logging.getLogger(__name__)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_stocks.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# 批次参数
MINOR_BATCH_SIZE = 10  # 每10只股票提交一次
BATCH_SIZE = 1         # 单次运行处理8只股票

def format_stock_code(code):
    """
    规范化股票代码为6位字符串格式
    Args:
        code: 股票代码（可能包含前缀或非6位）
    Returns:
        str: 规范化的6位股票代码
    """
    # 转换为字符串
    code_str = str(code).strip().lower()
    
    # 移除可能的市场前缀
    if code_str.startswith(('sh', 'sz', 'hk', 'bj')):
        code_str = code_str[2:]
    
    # 移除可能的点号（如"0.600022"）
    if '.' in code_str:
        code_str = code_str.split('.')[1] if code_str.startswith('0.') else code_str
    
    # 确保是6位数字
    code_str = code_str.zfill(6)
    
    # 验证格式
    if not code_str.isdigit() or len(code_str) != 6:
        logger.warning(f"股票代码格式化失败: {code_str}")
        return None
    
    return code_str

def get_next_crawl_index() -> int:
    """获取下一个要处理的股票索引"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"股票列表文件不存在: {BASIC_INFO_FILE}")
            return 0
        
        if not _verify_git_file_content(BASIC_INFO_FILE):
            logger.warning("股票列表文件内容与Git仓库不一致，可能需要重新加载")
        
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("股票列表文件为空，无法获取进度")
            return 0
        
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
            if not _verify_git_file_content(BASIC_INFO_FILE):
                logger.warning("股票列表文件内容与Git仓库不一致，可能需要重新提交")
            logger.info("已添加next_crawl_index列并初始化为0")
        
        next_index = int(basic_info_df["next_crawl_index"].iloc[0])
        logger.info(f"当前进度：下一个索引位置: {next_index}/{len(basic_info_df)}")
        return next_index
    except Exception as e:
        logger.error(f"获取股票进度索引失败: {str(e)}", exc_info=True)
        return 0

def save_crawl_progress(next_index: int):
    """保存股票爬取进度 - 仅保存到文件，不提交"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"股票列表文件不存在: {BASIC_INFO_FILE}")
            return
        
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("股票列表文件为空，无法更新进度")
            return
        
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
        
        basic_info_df["next_crawl_index"] = next_index
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"✅ 进度已保存：下一个索引位置: {next_index}/{len(basic_info_df)}")
    except Exception as e:
        logger.error(f"❌ 保存股票进度失败: {str(e)}", exc_info=True)

def commit_crawl_progress():
    """提交进度文件到Git仓库"""
    try:
        commit_message = f"feat: 更新股票爬取进度 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        success = commit_files_in_batches(BASIC_INFO_FILE, commit_message)
        if success:
            logger.info("✅ 进度文件已提交到Git仓库")
        else:
            logger.error("❌ 进度文件提交失败")
        return success
    except Exception as e:
        logger.error(f"❌ 提交进度文件失败: {str(e)}", exc_info=True)
        return False

def get_all_stock_codes() -> list:
    """获取所有股票代码"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.info("股票列表文件不存在，正在创建...")
            if not update_stock_list():
                logger.error("股票列表文件创建失败")
                return []
        
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("股票列表文件为空")
            return []
        
        if "代码" not in basic_info_df.columns:
            logger.error("股票列表文件缺少'代码'列")
            return []
        
        stock_codes = basic_info_df["代码"].tolist()
        logger.info(f"获取到 {len(stock_codes)} 只股票代码")
        return stock_codes
    except Exception as e:
        logger.error(f"获取股票代码列表失败: {str(e)}", exc_info=True)
        return []

def normalize_stock_df(df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
    """
    规范股票日线数据结构与精度
    """
    expected_columns = [
        "日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额",
        "振幅", "涨跌幅", "涨跌额", "换手率", "股票代码", "股票名称"
    ]

    # 缺少列自动补0
    for col in expected_columns:
        if col not in df.columns:
            df[col] = 0

    # 精度处理
    four_decimals = ["开盘", "最高", "最低", "收盘", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
    for col in four_decimals:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

    if "成交量" in df.columns:
        df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce").fillna(0).astype(int)

    df["股票代码"] = stock_code
    df["股票名称"] = get_stock_name(stock_code)
    
    df = df[expected_columns]
    df = df.sort_values(by="日期", ascending=True)
    return df

def get_stock_name(stock_code):
    """获取股票名称"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.warning(f"股票列表文件不存在: {BASIC_INFO_FILE}")
            return stock_code
        
        basic_info_df = pd.read_csv(
            BASIC_INFO_FILE,
            dtype={"代码": str}
        )
        
        if basic_info_df.empty:
            logger.error("股票列表文件为空")
            return stock_code
        
        if "代码" not in basic_info_df.columns or "名称" not in basic_info_df.columns:
            logger.error("股票列表文件缺少必要列")
            return stock_code
        
        stock_code_str = str(stock_code).strip()
        stock_row = basic_info_df[basic_info_df["代码"] == stock_code_str]
        
        if not stock_row.empty:
            return stock_row["名称"].values[0]
        
        logger.warning(f"股票 {stock_code_str} 不在列表中")
        return stock_code
    except Exception as e:
        logger.error(f"获取股票名称失败: {str(e)}", exc_info=True)
        return stock_code

def get_incremental_date_range(stock_code: str) -> (datetime, datetime):
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
        
        save_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        
        if os.path.exists(save_path):
            try:
                df = pd.read_csv(save_path)
                
                if "日期" not in df.columns:
                    logger.warning(f"股票 {stock_code} 数据文件缺少'日期'列")
                    start_date = last_trading_day - timedelta(days=365)
                    if start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                    return start_date, end_date
                
                df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
                valid_dates = df["日期"].dropna()
                if valid_dates.empty:
                    logger.warning(f"股票 {stock_code} 数据文件中日期列全为NaN")
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
                
                logger.debug(f"股票 {stock_code} 日期比较: 最新日期={latest_date_date}, 结束日期={end_date_date}")
                
                if latest_date_date < end_date_date:
                    start_date = latest_date + timedelta(days=1)
                    
                    while not is_trading_day(start_date.date()):
                        start_date += timedelta(days=1)
                    
                    if start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                    
                    if start_date > end_date:
                        logger.info(f"股票 {stock_code} 数据已最新（最新日期={latest_date_date}，结束日期={end_date_date}）")
                        return None, None
                    
                    logger.info(f"股票 {stock_code} 需要更新数据: 最新日期 {latest_date_date} < 结束日期 {end_date_date}")
                    logger.info(f"股票 {stock_code} 增量爬取日期范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
                    return start_date, end_date
                else:
                    logger.info(f"股票 {stock_code} 数据已最新: 最新日期 {latest_date_date} >= 结束日期 {end_date_date}")
                    return None, None
            
            except Exception as e:
                logger.error(f"读取股票 {stock_code} 数据文件失败: {str(e)}", exc_info=True)
                start_date = last_trading_day - timedelta(days=365)
                if start_date.tzinfo is None:
                    start_date = start_date.replace(tzinfo=Config.BEIJING_TIMEZONE)
                return start_date, end_date
        else:
            logger.info(f"股票 {stock_code} 无历史数据，将获取一年历史数据")
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

def save_stock_data_batch(stock_data_dict: dict) -> int:
    """
    批量保存股票日线数据
    """
    if not stock_data_dict:
        return 0

    os.makedirs(DAILY_DIR, exist_ok=True)
    saved_count = 0

    for stock_code, df in stock_data_dict.items():
        if df.empty:
            continue

        save_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")

        # 保存前规范化数据结构与精度
        stock_name = df["股票名称"].iloc[0] if "股票名称" in df.columns else get_stock_name(stock_code)
        df = normalize_stock_df(df, stock_code)

        try:
            # =============================
            # Step 1: 读取已有数据（如存在）
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
                # Step 2: 合并数据
                # =============================
                combined_df = pd.concat([existing_df, df], ignore_index=True)

                # 再次统一日期列类型
                combined_df["日期"] = pd.to_datetime(combined_df["日期"], errors="coerce")

                # 丢弃无效日期
                invalid_dates = combined_df["日期"].isna().sum()
                if invalid_dates > 0:
                    logger.warning(f"⚠️ 股票 {stock_code} 合并后发现 {invalid_dates} 条无效日期记录，已过滤")
                    combined_df = combined_df.dropna(subset=["日期"])

                # =============================
                # Step 3: 去重 + 排序
                # =============================
                combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                combined_df = combined_df.sort_values("日期", ascending=True).reset_index(drop=True)

                # =============================
                # Step 4: 格式化日期列为字符串保存
                # =============================
                combined_df["日期"] = combined_df["日期"].dt.strftime("%Y-%m-%d")

                # =============================
                # Step 5: 临时文件安全写入
                # =============================
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                    combined_df.to_csv(temp_file.name, index=False)

                shutil.move(temp_file.name, save_path)
                logger.info(f"✅ 数据已合并至: {save_path} (共{len(combined_df)}条)")

            else:
                # =============================
                # 无旧数据，直接保存新数据
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
            logger.error(f"保存股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)

    return saved_count

def crawl_all_stocks_daily_data():
    """爬取所有股票日线数据"""
    try:
        logger.info("=== 开始执行股票日线数据爬取 ===")
        beijing_time = get_beijing_time()
        logger.info(f"北京时间：{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}（UTC+8）")
        
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(DAILY_DIR, exist_ok=True)
        logger.info(f"✅ 确保目录存在: {DATA_DIR}")
        
        stock_codes = get_all_stock_codes()
        total_count = len(stock_codes)
        
        if total_count == 0:
            logger.error("股票列表为空，无法进行爬取")
            return
        
        logger.info(f"待爬取股票总数：{total_count}只（全市场股票）")
        
        next_index = get_next_crawl_index()
        total_to_process = min(BATCH_SIZE, total_count - next_index)
        
        if total_to_process <= 0:
            logger.info("没有股票需要处理")
            return
            
        logger.info(f"本次将处理 {total_to_process} 只股票（目标：{BATCH_SIZE}只）")
        
        start_idx = next_index % total_count
        stock_data_dict = {}  # 小批次数据缓存
        processed_count = 0
        successful_count = 0
        failed_stocks = []
        
        # 处理所有股票
        for i in range(total_to_process):
            current_index = (start_idx + i) % total_count
            stock_code = stock_codes[current_index]
            stock_name = get_stock_name(stock_code)
            logger.info(f"股票代码：{stock_code}| 名称：{stock_name}")
            
            # 获取增量日期范围
            start_date, end_date = get_incremental_date_range(stock_code)
            if start_date is None or end_date is None:
                logger.info(f"股票 {stock_code} 数据已最新，跳过爬取")
                processed_count += 1
                continue
            
            # 爬取数据
            logger.info(f"📅 增量爬取日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
            df = get_stock_daily_data_from_sources(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                logger.warning(f"⚠️ 未获取到数据")
                failed_stocks.append(f"{stock_code},{stock_name},未获取到数据")
                processed_count += 1
                continue
            
            # 缓存到小批次
            stock_data_dict[stock_code] = df
            successful_count += 1
            processed_count += 1
            
            current_progress = f"{current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)"
            logger.info(f"进度: {current_progress} - 数据已缓存")
            
            # 每10只股票提交一次（包括最后一只）
            if (i + 1) % MINOR_BATCH_SIZE == 0 or i == total_to_process - 1:
                # 保存当前小批次数据
                if stock_data_dict:
                    logger.info(f"开始保存小批次数据（{len(stock_data_dict)}只股票）...")
                    saved_count = save_stock_data_batch(stock_data_dict)
                    logger.info(f"✅ 小批次数据保存完成，成功保存 {saved_count} 个股票数据文件")
                    
                    # 重新添加：确保数据文件被添加到Git暂存区
                    os.system("git add data/daily/*.csv")
                    
                    # 构建要提交的文件列表
                    file_list = [os.path.join(DAILY_DIR, f"{code}.csv") for code in stock_data_dict.keys()]
                    # 提交数据文件
                    commit_msg = f"feat: 批量提交{len(stock_data_dict)}只股票日线数据 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
                    logger.info(f"提交数据文件: {commit_msg}")
                    commit_success = commit_files_in_batches(file_list, commit_msg)
                    
                    if commit_success:
                        logger.info(f"✅ 小批次数据文件提交成功：{len(stock_data_dict)}只")
                    else:
                        logger.error("❌ 小批次数据文件提交失败")
                    
                    # 更新进度（当前已处理数量）
                    new_index = start_idx + i + 1
                    new_index = new_index % total_count
                    save_crawl_progress(new_index)
                    logger.info(f"✅ 进度已更新为 {new_index}/{total_count}")
                    
                    # 提交进度文件
                    progress_commit_success = commit_crawl_progress()
                    if progress_commit_success:
                        logger.info(f"✅ 进度文件提交成功，进度更新为 {new_index}/{total_count}")
                    else:
                        logger.error("❌ 进度文件提交失败")
                    
                    # 清空小批次缓存
                    stock_data_dict = {}
                else:
                    logger.info("当前小批次没有新数据，跳过提交")
            
            # 每只股票之间随机等待
            time.sleep(random.uniform(1.2, 4.6))
        
        # 处理结束后记录失败股票
        if failed_stocks:
            failed_file = os.path.join(DAILY_DIR, "failed_stocks.txt")
            with open(failed_file, "w", encoding="utf-8") as f:
                f.write("\n".join(failed_stocks))
            logger.info(f"记录了 {len(failed_stocks)} 只失败的股票")
        
        # 计算剩余股票数量
        remaining_stocks = total_count - (start_idx + total_to_process)
        if remaining_stocks < 0:
            remaining_stocks = total_count + remaining_stocks
            
        logger.info(f"本次爬取完成，共处理 {processed_count} 只股票，成功 {successful_count} 只，失败 {len(failed_stocks)} 只")
        logger.info(f"还有 {remaining_stocks} 只股票待爬取")
        
    except Exception as e:
        logger.error(f"股票日线数据爬取任务执行失败: {str(e)}", exc_info=True)
        # 异常情况下尝试保存进度
        try:
            if 'next_index' in locals() and 'total_count' in locals():
                new_index = start_idx + i + 1 if 'i' in locals() else next_index
                new_index = new_index % total_count
                logger.error("尝试保存进度以恢复状态...")
                save_crawl_progress(new_index)
                commit_crawl_progress()
                logger.info(f"进度已保存为 {new_index}/{total_count}")
        except Exception as save_error:
            logger.error(f"异常情况下保存进度失败: {str(save_error)}", exc_info=True)
        raise

def main():
    """主函数"""
    logger.info("===== 开始更新股票数据 =====")
    
    # 确保基础信息文件存在
    if not os.path.exists(BASIC_INFO_FILE) or os.path.getsize(BASIC_INFO_FILE) == 0:
        logger.info("基础信息文件不存在或为空，正在创建...")
        if not update_stock_list():
            logger.error("基础信息文件创建失败，无法继续")
            return
    
    # 执行爬取
    try:
        crawl_all_stocks_daily_data()
        logger.info("✅ 股票日线数据爬取完成")
    except Exception as e:
        logger.error(f"❌ 股票日线数据爬取失败: {str(e)}", exc_info=True)
        # 发送错误通知
        try:
            from wechat_push.push import send_wechat_message
            send_wechat_message(
                message=f"股票日线数据爬取失败: {str(e)}",
                message_type="error"
            )
        except:
            pass
    
    logger.info("===== 股票数据更新完成 =====")

if __name__ == "__main__":
    main()
