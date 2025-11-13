#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块 - 严格计数器版本
yFinance数据DS-etf_daily_crawler-6.py
【严格按照成功计数器=10时提交，确保每10个成功文件提交一次】
"""

import yfinance as yf
import pandas as pd
import logging
import os
import time
import random
import tempfile
import shutil
import subprocess
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time, get_last_trading_day, is_trading_day

# 初始化日志
logger = logging.getLogger(__name__)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "etf", "daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_etfs.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# 【关键参数】严格遵循每10个成功文件提交一次
BATCH_SIZE = 40  # 每批处理40个ETF
COMMIT_BATCH_SIZE = 10  # 每10个成功文件提交一次
BASE_DELAY = 1.0
MAX_RETRIES = 2

class StrictCounterETFCrawler:
    def __init__(self):
        # 【关键】严格的计数器系统
        self.success_count = 0  # 成功获取数据的计数器
        self.fail_count = 0     # 失败获取数据的计数器
        self.skip_count = 0     # 跳过计数（数据已最新）
        self.staged_files = []  # 暂存区：成功文件的路径列表
        self.batch_commit_number = 1  # 批次提交编号
        
    def get_etf_name(self, etf_code):
        """获取ETF名称"""
        try:
            if not os.path.exists(BASIC_INFO_FILE):
                return etf_code
            
            basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
            if basic_info_df.empty or "ETF代码" not in basic_info_df.columns or "ETF名称" not in basic_info_df.columns:
                return etf_code
            
            etf_row = basic_info_df[basic_info_df["ETF代码"] == str(etf_code).strip()]
            if not etf_row.empty:
                return etf_row["ETF名称"].values[0]
            
            return etf_code
        except Exception as e:
            logger.error(f"获取ETF名称失败: {str(e)}")
            return etf_code

    def get_yfinance_symbol(self, etf_code):
        """获取yfinance对应的symbol"""
        if etf_code.startswith(('51', '56', '57', '58')):
            return f"{etf_code}.SS"
        elif etf_code.startswith('15'):
            return f"{etf_code}.SZ"
        else:
            return etf_code

    def crawl_with_yfinance(self, etf_code, start_date, end_date):
        """使用yfinance爬取数据"""
        try:
            symbol = self.get_yfinance_symbol(etf_code)
            
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
            result_df['ETF名称'] = self.get_etf_name(etf_code)
            result_df['爬取时间'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return result_df
            
        except Exception as e:
            logger.error(f"yfinance爬取 {etf_code} 失败: {str(e)}")
            return None

    def crawl_etf_data(self, etf_code, start_date, end_date):
        """爬取ETF数据"""
        max_retries = MAX_RETRIES
        
        for retry in range(max_retries):
            try:
                df = self.crawl_with_yfinance(etf_code, start_date, end_date)
                
                if df is not None and not df.empty:
                    return df
                
                # 等待后重试
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 3
                    logger.info(f"数据获取失败，{wait_time}秒后重试...")
                    time.sleep(wait_time)
                    
            except Exception as e:
                logger.error(f"爬取 {etf_code} 失败 (尝试 {retry + 1}): {str(e)}")
                if retry < max_retries - 1:
                    time.sleep((retry + 1) * 3)
        
        return None

    def save_etf_data(self, etf_code, df):
        """保存ETF数据到本地"""
        if df is None or df.empty:
            return False
        
        try:
            save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
            
            # 如果文件已存在，合并数据
            if os.path.exists(save_path):
                try:
                    existing_df = pd.read_csv(save_path)
                    if "日期" in existing_df.columns:
                        # 合并数据并去重
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                        combined_df = combined_df.sort_values("日期")
                        df = combined_df
                except Exception as e:
                    logger.warning(f"合并数据失败，将覆盖文件: {str(e)}")
            
            # 保存文件
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                df.to_csv(temp_file.name, index=False)
            shutil.move(temp_file.name, save_path)
            
            logger.info(f"✅ 数据已保存到本地: {save_path} ({len(df)}条)")
            return save_path  # 返回文件路径
            
        except Exception as e:
            logger.error(f"保存ETF {etf_code} 数据失败: {str(e)}")
            return None

    def git_add_file(self, file_path):
        """Git添加单个文件"""
        try:
            if not os.path.exists(file_path):
                logger.warning(f"文件不存在，无法添加: {file_path}")
                return False
            
            result = subprocess.run(
                ['git', 'add', file_path],
                capture_output=True, text=True, timeout=30
            )
            
            if result.returncode == 0:
                logger.info(f"✅ Git添加成功: {file_path}")
                return True
            else:
                logger.error(f"❌ Git添加失败 {file_path}: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Git添加异常 {file_path}: {str(e)}")
            return False

    def git_commit_batch(self, batch_files, batch_number):
        """提交批次文件到Git"""
        try:
            if not batch_files:
                logger.warning("没有文件需要提交")
                return True
            
            # 检查是否有变更
            status_result = subprocess.run(
                ['git', 'status', '--porcelain'],
                capture_output=True, text=True, timeout=30
            )
            
            if not status_result.stdout.strip():
                logger.info("没有文件变更，跳过提交")
                return True
            
            # 执行提交
            commit_message = f"自动更新ETF日线数据 批次{batch_number} [skip ci]"
            commit_result = subprocess.run(
                ['git', 'commit', '-m', commit_message],
                capture_output=True, text=True, timeout=30
            )
            
            if commit_result.returncode == 0:
                logger.info(f"✅ Git提交成功: 批次{batch_number}")
                
                # 执行推送
                push_result = subprocess.run(
                    ['git', 'push'],
                    capture_output=True, text=True, timeout=60
                )
                
                if push_result.returncode == 0:
                    logger.info(f"✅ Git推送成功: 批次{batch_number}")
                    return True
                else:
                    logger.error(f"❌ Git推送失败: {push_result.stderr}")
                    return False
            else:
                logger.error(f"❌ Git提交失败: {commit_result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Git提交异常: {str(e)}")
            return False

    def process_successful_etf(self, etf_code, file_path):
        """
        【核心逻辑】处理成功的ETF
        - 添加到暂存区
        - 成功计数器+1
        - 检查是否需要提交
        """
        # 1. Git添加文件到暂存区
        if self.git_add_file(file_path):
            # 2. 添加到暂存区列表
            self.staged_files.append(file_path)
            
            # 3. 成功计数器+1
            self.success_count += 1
            logger.info(f"🎯 成功计数器: {self.success_count}/{COMMIT_BATCH_SIZE}")
            
            # 4. 检查是否达到提交条件
            if self.success_count >= COMMIT_BATCH_SIZE:
                logger.info(f"🚀 达到提交条件! 成功计数器={self.success_count}，开始提交批次{self.batch_commit_number}")
                
                # 执行批次提交
                if self.git_commit_batch(self.staged_files, self.batch_commit_number):
                    logger.info(f"✅ 批次{self.batch_commit_number}提交成功!")
                    
                    # 5. 重置计数器和暂存区
                    self.success_count = 0
                    self.staged_files = []
                    self.batch_commit_number += 1
                    
                    return True
                else:
                    logger.error(f"❌ 批次{self.batch_commit_number}提交失败!")
                    return False
            return True
        else:
            logger.error(f"❌ ETF {etf_code} Git添加失败")
            return False

    def get_incremental_date_range(self, etf_code):
        """获取增量日期范围"""
        try:
            last_trading_day = get_last_trading_day()
            if not isinstance(last_trading_day, datetime):
                last_trading_day = datetime.now()
            
            # 设置结束日期
            end_date = last_trading_day.replace(hour=23, minute=59, second=59, microsecond=0)
            
            # 检查历史文件
            save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
            
            if os.path.exists(save_path):
                try:
                    df = pd.read_csv(save_path)
                    if "日期" not in df.columns or df.empty:
                        # 获取一年数据
                        start_date = last_trading_day - timedelta(days=365)
                        return start_date, end_date
                    
                    # 获取最新日期
                    df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
                    valid_dates = df["日期"].dropna()
                    
                    if valid_dates.empty:
                        start_date = last_trading_day - timedelta(days=365)
                        return start_date, end_date
                    
                    latest_date = valid_dates.max()
                    latest_date_date = latest_date.date()
                    end_date_date = end_date.date()
                    
                    if latest_date_date < end_date_date:
                        # 需要更新数据
                        start_date = latest_date + timedelta(days=1)
                        # 确保是交易日
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
                # 新ETF，获取一年数据
                start_date = last_trading_day - timedelta(days=365)
                return start_date, end_date
                
        except Exception as e:
            logger.error(f"获取日期范围失败: {str(e)}")
            last_trading_day = get_last_trading_day()
            start_date = last_trading_day - timedelta(days=365)
            end_date = last_trading_day.replace(hour=23, minute=59, second=59, microsecond=0)
            return start_date, end_date

    def get_all_etf_codes(self):
        """获取所有ETF代码"""
        try:
            if not os.path.exists(BASIC_INFO_FILE):
                logger.error("ETF列表文件不存在")
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

    def get_next_crawl_index(self):
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
            
            next_index = int(basic_info_df["next_crawl_index"].iloc[0])
            return next_index
            
        except Exception as e:
            logger.error(f"获取进度失败: {str(e)}")
            return 0

    def save_crawl_progress(self, next_index):
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

    def commit_remaining_files(self):
        """提交剩余未提交的文件"""
        if self.staged_files and self.success_count > 0:
            logger.info(f"🚀 提交剩余 {len(self.staged_files)} 个文件 (成功计数器={self.success_count})")
            
            if self.git_commit_batch(self.staged_files, f"最终批次_{self.batch_commit_number}"):
                logger.info("✅ 剩余文件提交成功!")
                self.success_count = 0
                self.staged_files = []
                return True
            else:
                logger.error("❌ 剩余文件提交失败!")
                return False
        else:
            logger.info("没有剩余文件需要提交")
            return True

    def crawl_all_etfs_daily_data(self):
        """主爬取函数 - 严格计数器版本"""
        try:
            logger.info("=== 开始执行ETF日线数据爬取（严格计数器版本）===")
            logger.info(f"北京时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 确保目录存在
            os.makedirs(DAILY_DIR, exist_ok=True)
            
            # 获取ETF代码列表
            etf_codes = self.get_all_etf_codes()
            total_count = len(etf_codes)
            
            if total_count == 0:
                logger.error("ETF列表为空")
                return
            
            logger.info(f"待爬取ETF总数：{total_count}只")
            
            # 获取进度
            next_index = self.get_next_crawl_index()
            logger.info(f"当前进度：{next_index}/{total_count}")
            
            # 处理当前批次
            start_idx = next_index % total_count
            end_idx = start_idx + BATCH_SIZE
            
            if end_idx <= total_count:
                batch_codes = etf_codes[start_idx:end_idx]
            else:
                batch_codes = etf_codes[start_idx:total_count] + etf_codes[0:end_idx-total_count]
            
            logger.info(f"处理本批次 ETF ({len(batch_codes)}只)，从索引 {start_idx} 开始")
            
            # 【核心处理循环】
            processed_count = 0
            for i, etf_code in enumerate(batch_codes):
                # 随机延时
                time.sleep(random.uniform(3, 8))
                
                etf_name = self.get_etf_name(etf_code)
                current_index = (start_idx + i) % total_count
                logger.info(f"ETF代码：{etf_code}| 名称：{etf_name}")
                
                # 获取日期范围
                date_range = self.get_incremental_date_range(etf_code)
                if date_range[0] is None:
                    logger.info(f"ETF {etf_code} 数据已最新，跳过")
                    self.skip_count += 1
                    processed_count += 1
                    logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
                    continue
                
                start_date, end_date = date_range
                logger.info(f"📅 爬取日期范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
                
                # 爬取数据
                df = self.crawl_etf_data(etf_code, start_date, end_date)
                
                if df is None or df.empty:
                    logger.warning(f"⚠️ ETF {etf_code} 未获取到数据")
                    # 记录失败
                    with open(os.path.join(DAILY_DIR, "failed_etfs.txt"), "a", encoding="utf-8") as f:
                        f.write(f"{etf_code},{etf_name},未获取到数据\n")
                    self.fail_count += 1
                    processed_count += 1
                    logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
                    continue
                
                # 保存数据到本地
                file_path = self.save_etf_data(etf_code, df)
                if file_path:
                    # 【关键步骤】处理成功的ETF（添加到暂存区并检查提交条件）
                    if self.process_successful_etf(etf_code, file_path):
                        processed_count += 1
                    else:
                        self.fail_count += 1  # Git操作失败也算失败
                        processed_count += 1
                else:
                    self.fail_count += 1
                    processed_count += 1
                
                logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
            
            # 【关键步骤】提交剩余未提交的文件
            logger.info("开始提交剩余未提交的文件...")
            self.commit_remaining_files()
            
            # 更新进度
            new_index = end_idx % total_count
            self.save_crawl_progress(new_index)
            
            # 【严格的计数器验证】
            total_processed = self.success_count + self.fail_count + self.skip_count
            logger.info("=" * 60)
            logger.info("📊 严格的计数器统计结果:")
            logger.info(f"✅ 成功计数: {self.success_count}")
            logger.info(f"❌ 失败计数: {self.fail_count}") 
            logger.info(f"⏭️  跳过计数: {self.skip_count}")
            logger.info(f"📦 总处理: {total_processed}/{len(batch_codes)}")
            logger.info(f"💾 提交批次: {self.batch_commit_number - 1}")
            logger.info("=" * 60)
            
            # 验证计数器是否正确
            if total_processed == len(batch_codes):
                logger.info("✅ 计数器验证通过!")
            else:
                logger.error(f"❌ 计数器验证失败! 期望: {len(batch_codes)}, 实际: {total_processed}")
            
        except Exception as e:
            logger.error(f"ETF爬取任务失败: {str(e)}", exc_info=True)
            # 尝试提交剩余文件
            try:
                self.commit_remaining_files()
            except:
                pass
            raise

def crawl_all_etfs_daily_data():
    """主入口函数"""
    crawler = StrictCounterETFCrawler()
    crawler.crawl_all_etfs_daily_data()

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
