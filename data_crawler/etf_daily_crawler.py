#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线数据爬取模块 - 生产级修复版本

yFinance数据-etf_daily_crawler-DS13.py

修复问题：
1. 提交逻辑错误（只提交第一个文件）
2. 数据获取问题（yfinance返回空数据）
3. 时间处理问题（时区错误）
4. 文件保存验证
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
    etf_code = str(etf_code).strip()
    if etf_code.startswith(('51', '56', '57', '58')):
        return f"{etf_code}.SS"
    elif etf_code.startswith('15'):
        return f"{etf_code}.SZ"
    else:
        return etf_code

def crawl_etf_data(etf_code, start_date, end_date):
    """爬取ETF数据 - 修复版本"""
    try:
        symbol = get_yfinance_symbol(etf_code)
        logger.info(f"尝试获取 {etf_code} 数据，symbol: {symbol}")
        
        # 使用Ticker对象获取更详细的信息
        ticker = yf.Ticker(symbol)
        
        # 获取历史数据
        df = ticker.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            auto_adjust=False  # 不自动调整价格
        )
        
        if df is None or df.empty:
            logger.warning(f"yfinance返回空数据: {etf_code}")
            return None
        
        logger.info(f"获取到 {len(df)} 条数据，列名: {df.columns.tolist()}")
        
        # 检查数据是否有效
        if df['Close'].isna().all() or (df['Close'] == 0).all():
            logger.warning(f"数据全为0或空: {etf_code}")
            return None
        
        # 创建结果DataFrame
        result_df = pd.DataFrame()
        result_df['日期'] = df.index.strftime('%Y-%m-%d')
        result_df['开盘'] = df['Open'].round(4)
        result_df['最高'] = df['High'].round(4)
        result_df['最低'] = df['Low'].round(4)
        result_df['收盘'] = df['Close'].round(4)
        result_df['成交量'] = df['Volume'].astype(int)
        
        # 计算其他字段
        result_df['振幅'] = ((result_df['最高'] - result_df['最低']) / result_df['最低'].replace(0, 1) * 100).round(2)
        result_df['涨跌额'] = result_df['收盘'].diff().fillna(0).round(4)
        
        # 计算涨跌幅
        prev_close = result_df['收盘'].shift(1)
        result_df['涨跌幅'] = (result_df['涨跌额'] / prev_close.replace(0, 1) * 100).round(2)
        result_df['涨跌幅'] = result_df['涨跌幅'].fillna(0)
        
        # 计算成交额
        if 'Dividends' in df.columns:
            result_df['成交额'] = (df['Close'] * df['Volume']).round(2)
        else:
            result_df['成交额'] = (result_df['收盘'] * result_df['成交量']).round(2)
        
        # 填充其他字段
        result_df['换手率'] = 0.0
        result_df['IOPV'] = 0.0
        result_df['折价率'] = 0.0
        result_df['溢价率'] = 0.0
        
        result_df['ETF代码'] = etf_code
        result_df['ETF名称'] = get_etf_name(etf_code)
        result_df['爬取时间'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 验证数据
        sample_close = result_df['收盘'].iloc[0] if len(result_df) > 0 else 'N/A'
        logger.info(f"数据验证 - 首条收盘价: {sample_close}, 数据量: {len(result_df)}")
        return result_df
        
    except Exception as e:
        logger.error(f"yfinance爬取 {etf_code} 失败: {str(e)}", exc_info=True)
        return None

def save_etf_data(etf_code, df):
    """保存ETF数据到本地 - 修复版本"""
    if df is None or df.empty:
        logger.warning(f"ETF {etf_code} 无数据可保存")
        return None
    
    try:
        save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
        
        # 如果文件已存在，合并数据
        if os.path.exists(save_path):
            try:
                existing_df = pd.read_csv(save_path)
                if not existing_df.empty and "日期" in existing_df.columns:
                    # 合并数据，去重
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                    combined_df = combined_df.sort_values("日期")
                    original_count = len(existing_df)
                    new_count = len(combined_df)
                    df = combined_df
                    logger.info(f"数据合并: 原有 {original_count} 条，合并后 {new_count} 条，新增 {new_count - original_count} 条")
                else:
                    logger.info(f"现有文件格式错误，覆盖写入")
            except Exception as e:
                logger.warning(f"合并数据失败，覆盖写入: {str(e)}")
        
        # 确保目录存在
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        # 使用临时文件确保数据完整性
        temp_file = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8-sig') as temp_file:
                df.to_csv(temp_file.name, index=False)
            
            # 移动临时文件到目标位置
            shutil.move(temp_file.name, save_path)
            temp_file = None  # 防止删除
            
            # 验证文件是否真的保存了
            if os.path.exists(save_path):
                file_size = os.path.getsize(save_path)
                # 读取验证文件内容
                verify_df = pd.read_csv(save_path)
                actual_count = len(verify_df)
                
                logger.info(f"✅ 数据保存成功: {save_path} ({actual_count}条, {file_size} bytes)")
                
                # 数据完整性检查
                if actual_count != len(df):
                    logger.warning(f"数据完整性警告: 预期 {len(df)} 条，实际保存 {actual_count} 条")
                
                return save_path
            else:
                logger.error(f"❌ 文件保存验证失败: {save_path}")
                return None
                
        finally:
            # 清理临时文件
            if temp_file and os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            
    except Exception as e:
        logger.error(f"保存ETF {etf_code} 数据失败: {str(e)}", exc_info=True)
        return None

def get_incremental_date_range(etf_code):
    """获取增量日期范围 - 修复版本"""
    try:
        # 使用naive datetime避免时区问题
        end_date = datetime.now()
        save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")
        
        if os.path.exists(save_path):
            try:
                df = pd.read_csv(save_path)
                if "日期" not in df.columns or df.empty:
                    start_date = end_date - timedelta(days=365)
                    logger.info(f"文件为空或格式错误，重新爬取全年数据")
                    return start_date, end_date
                
                # 转换日期列
                df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
                valid_dates = df["日期"].dropna()
                
                if valid_dates.empty:
                    start_date = end_date - timedelta(days=365)
                    logger.info(f"无有效日期数据，重新爬取全年数据")
                    return start_date, end_date
                
                latest_date = valid_dates.max()
                if hasattr(latest_date, 'tzinfo') and latest_date.tzinfo is not None:
                    latest_date = latest_date.replace(tzinfo=None)
                
                start_date = latest_date + timedelta(days=1)
                
                if start_date > end_date:
                    logger.info(f"数据已是最新，无需更新")
                    return None, None
                
                logger.info(f"增量更新: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
                return start_date, end_date
                
            except Exception as e:
                logger.warning(f"读取历史文件失败，重新爬取: {str(e)}")
                start_date = end_date - timedelta(days=365)
                return start_date, end_date
        else:
            start_date = end_date - timedelta(days=365)
            logger.info(f"新ETF，爬取全年数据")
            return start_date, end_date
            
    except Exception as e:
        logger.error(f"获取日期范围失败: {str(e)}")
        start_date = datetime.now() - timedelta(days=365)
        end_date = datetime.now()
        return start_date, end_date

def get_all_etf_codes():
    """获取所有ETF代码"""
    try:
        if not os.path.exists(BASIC_INFO_FILE):
            logger.error("ETF基础信息文件不存在")
            return []
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, dtype={"ETF代码": str})
        if basic_info_df.empty or "ETF代码" not in basic_info_df.columns:
            logger.error("ETF基础信息文件格式错误")
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

def commit_files_in_batches(file_paths, commit_message):
    """批量提交文件到Git - 修复版本"""
    if not file_paths:
        logger.warning("没有文件需要提交")
        return False
    
    # 确保file_paths是列表
    if isinstance(file_paths, str):
        file_paths = [file_paths]
    
    try:
        logger.info(f"开始提交 {len(file_paths)} 个文件")
        
        # 验证文件存在
        existing_files = []
        for file_path in file_paths:
            if os.path.exists(file_path):
                existing_files.append(file_path)
            else:
                logger.warning(f"文件不存在: {file_path}")
        
        if not existing_files:
            logger.error("没有有效文件可提交")
            return False
        
        # 添加文件到git
        for file_path in existing_files:
            try:
                subprocess.run(['git', 'add', file_path], check=True, capture_output=True, text=True)
                logger.debug(f"已添加: {file_path}")
            except subprocess.CalledProcessError as e:
                logger.error(f"添加文件失败 {file_path}: {e.stderr}")
                return False
        
        # 检查是否有变更
        try:
            result = subprocess.run(['git', 'status', '--porcelain'], check=True, capture_output=True, text=True)
            if not result.stdout.strip():
                logger.info("没有变更需要提交")
                return True
        except subprocess.CalledProcessError as e:
            logger.error(f"检查git状态失败: {e.stderr}")
            return False
        
        # 提交变更
        try:
            subprocess.run(['git', 'commit', '-m', commit_message], check=True, capture_output=True, text=True)
            logger.info(f"提交成功: {commit_message}")
        except subprocess.CalledProcessError as e:
            logger.error(f"提交失败: {e.stderr}")
            return False
        
        # 推送变更
        try:
            # 先拉取最新更改
            try:
                subprocess.run(['git', 'pull', '--rebase'], check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                logger.warning(f"拉取失败，继续推送: {e.stderr}")
            
            # 推送更改
            subprocess.run(['git', 'push'], check=True, capture_output=True, text=True)
            logger.info("推送成功")
        except subprocess.CalledProcessError as e:
            logger.error(f"推送失败: {e.stderr}")
            return False
        
        logger.info(f"✅ 成功提交 {len(existing_files)} 个文件")
        return True
        
    except Exception as e:
        logger.error(f"提交过程失败: {str(e)}")
        return False

def crawl_all_etfs_daily_data():
    """主爬取函数 - 生产级修复版本"""
    try:
        logger.info("=== 开始执行ETF日线数据爬取（生产级修复版本）===")
        
        # 获取ETF代码列表
        etf_codes = get_all_etf_codes()
        total_count = len(etf_codes)
        if total_count == 0:
            logger.error("ETF列表为空，终止执行")
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
        
        success_count = 0
        fail_count = 0
        skip_count = 0
        batch_files = []  # 累积成功文件的列表
        batch_number = 1  # 批次编号
        
        for i, etf_code in enumerate(batch_codes):
            # 随机延迟避免被封
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
                batch_files.append(file_path)
                success_count += 1
                logger.info(f"🎯 成功计数器: {len(batch_files)}/{COMMIT_BATCH_SIZE}")
                
                # 检查是否达到提交条件
                if len(batch_files) >= COMMIT_BATCH_SIZE:
                    logger.info(f"🚀 达到提交条件! 开始提交批次{batch_number}，包含 {len(batch_files)} 个文件")
                    
                    # 使用正确的提交逻辑
                    commit_message = f"feat: 批量提交ETF数据 批次{batch_number} [包含 {len(batch_files)} 个文件]"
                    commit_result = commit_files_in_batches(batch_files, commit_message)
                    
                    if commit_result:
                        logger.info(f"✅ 批次{batch_number}提交成功! 提交了 {len(batch_files)} 个文件")
                        batch_files = []  # 清空列表
                        batch_number += 1
                    else:
                        logger.error(f"❌ 批次{batch_number}提交失败!")
            else:
                fail_count += 1
            
            logger.info(f"进度: {current_index}/{total_count} ({(current_index)/total_count*100:.1f}%)")
        
        # 提交剩余文件
        if batch_files:
            logger.info(f"🚀 提交剩余 {len(batch_files)} 个文件")
            commit_message = f"feat: 批量提交ETF数据 最终批次 [包含 {len(batch_files)} 个文件]"
            commit_result = commit_files_in_batches(batch_files, commit_message)
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
        logger.info(f"📈 进度更新: {next_index} -> {new_index}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"ETF爬取任务失败: {str(e)}", exc_info=True)
        # 尝试提交剩余文件
        try:
            if 'batch_files' in locals() and batch_files:
                logger.info("尝试紧急提交剩余文件...")
                commit_files_in_batches(batch_files, "feat: 紧急提交ETF数据 [异常恢复]")
        except Exception as commit_error:
            logger.error(f"紧急提交失败: {commit_error}")
        raise

if __name__ == "__main__":
    try:
        crawl_all_etfs_daily_data()
        logger.info("🎉 ETF日线数据爬取任务完成!")
    except Exception as e:
        logger.error(f"❌ ETF日线数据爬取失败: {str(e)}", exc_info=True)
        # 发送错误通知
        try:
            from wechat_push.push import send_wechat_message
            send_wechat_message(
                message=f"ETF日线数据爬取失败: {str(e)}",
                message_type="error"
            )
        except ImportError:
            logger.warning("微信推送模块未安装")
        except Exception as notify_error:
            logger.error(f"发送通知失败: {notify_error}")
        exit(1)
