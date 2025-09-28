#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票数据爬取模块 - 仅调用一次API获取所有必要数据
严格使用API返回的原始列名，确保高效获取股票数据
"""

import os
import logging
import pandas as pd
import akshare as ak
import time
import random
import json
from datetime import datetime, timedelta
from config import Config
# 只需导入Git工具模块
from utils.git_utils import commit_files_in_batches

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "daily")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_stocks.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

def ensure_directory_exists():
    """确保数据目录存在"""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    if not os.path.exists(DAILY_DIR):
        os.makedirs(DAILY_DIR)
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def get_stock_section(stock_code: str) -> str:
    """
    获取股票所属板块
    
    Args:
        stock_code: 股票代码（不带市场前缀）
    
    Returns:
        str: 板块名称
    """
    # 确保股票代码是字符串
    stock_code = str(stock_code).zfill(6)
    
    # 移除可能的市场前缀
    if stock_code.lower().startswith(('sh', 'sz')):
        stock_code = stock_code[2:]
    
    # 确保股票代码是6位数字
    stock_code = stock_code.zfill(6)
    
    # 根据股票代码前缀判断板块
    if stock_code.startswith('60'):
        return "沪市主板"
    elif stock_code.startswith('00'):
        return "深市主板"
    elif stock_code.startswith('30'):
        return "创业板"
    elif stock_code.startswith('688'):
        return "科创板"
    else:
        return "其他板块"

def create_or_update_basic_info():
    """创建或更新股票基础信息文件，仅调用一次API"""
    ensure_directory_exists()
    
    # 添加随机延时，避免请求过于频繁
    time.sleep(random.uniform(1.0, 2.0))
    
    try:
        # 正确调用 stock_zh_a_spot_em 获取所有必要数据
        df = ak.stock_zh_a_spot_em()
        
        # 检查返回结果
        if df.empty:
            logger.error("获取股票列表失败：返回为空")
            return False
        
        # 打印API返回的列名，用于调试
        logger.info(f"API返回的列名: {df.columns.tolist()}")
        
        # 确保必要列存在
        required_columns = ['代码', '名称', '流通市值']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"获取股票列表失败: 缺少必要列 {col}")
                return False
        
        # 准备基础信息DataFrame - 严格使用API返回的原始列名
        basic_info_df = pd.DataFrame({
            "代码": df['代码'],
            "名称": df['名称'],
            "所属板块": df['代码'].apply(get_stock_section),
            "流通市值": df['流通市值']
        })
        
        # 确保流通市值列是数值类型
        basic_info_df["流通市值"] = pd.to_numeric(basic_info_df["流通市值"], errors='coerce').fillna(0)
        
        # 保留无市值股票，但标记它们
        invalid_mask = (basic_info_df["流通市值"] <= 0) | basic_info_df["流通市值"].isna()
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            basic_info_df["数据状态"] = '正常'
            basic_info_df.loc[invalid_mask, "数据状态"] = '流通市值缺失'
            logger.warning(f"检测到 {invalid_count} 条无市值数据的股票，已标记为'流通市值缺失'")
        
        # 【关键修改】调用 tickten.py 中的筛选函数
        try:
            from stock.tickten import filter_valid_stocks
            filtered_df = filter_valid_stocks(basic_info_df)
            logger.info(f"调用 tickten.py 筛选函数后，剩余 {len(filtered_df)} 只有效股票（原 {len(basic_info_df)} 只）")
        except Exception as e:
            logger.error(f"调用 tickten.py 筛选函数失败: {str(e)}", exc_info=True)
            return False
        
        # 【关键修改】添加 next_crawl_index 列 - 作为数值索引
        # 所有行的 next_crawl_index 值都相同，表示下一个要爬取的起始索引
        filtered_df["next_crawl_index"] = 0
        
        # 保存基础信息
        filtered_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"已创建/更新股票基础信息文件，共 {len(filtered_df)} 条记录")
        
        # 确认文件已保存
        if os.path.exists(BASIC_INFO_FILE) and os.path.getsize(BASIC_INFO_FILE) > 0:
            logger.info(f"基础信息文件已成功保存到: {BASIC_INFO_FILE}")
            
            # 详细记录前5只股票（用于验证索引）
            first_5 = filtered_df.head(5)
            for idx, row in first_5.iterrows():
                logger.info(f"基础信息文件前5只股票[{idx}]: {row['代码']} - {row['名称']}")
            
            # 【关键修改】提交基础信息文件到仓库
            commit_files_in_batches(BASIC_INFO_FILE)
            logger.info(f"已提交基础信息文件到仓库: {BASIC_INFO_FILE}")
            
            return True
        else:
            logger.error(f"基础信息文件保存失败: {BASIC_INFO_FILE} 不存在或为空")
            return False
            
    except Exception as e:
        logger.error(f"创建基础信息文件失败: {str(e)}", exc_info=True)
        return False

def test_akshare_api():
    """测试 akshare API 是否正常工作"""
    logger.info("===== 开始 akshare API 测试 =====")
    logger.info(f"akshare 版本: {ak.__version__}")
    logger.info(f"akshare 模块路径: {ak.__file__}")
    
    # 【关键修改】使用正确的测试方法
    test_results = []
    
    # 测试不同的股票代码格式和参数
    test_cases = [
        # (symbol, period, adjust) - 尝试多种组合
        ("sh600000", "daily", "qfq"),    # 沪市
        ("sz000001", "daily", "qfq"),   # 深市
        ("sz300001", "daily", "qfq"),   # 创业板
        ("sh688001", "daily", "qfq"),   # 科创板
        ("600000", "daily", "qfq"),     # 不带前缀
        ("000001", "daily", "qfq"),     # 不带前缀
        ("sh600000", "daily", ""),      # 无复权
        ("sh600000", "weekly", "qfq"),  # 周线
    ]
    
    for i, (symbol, period, adjust) in enumerate(test_cases):
        try:
            logger.info(f"--- 测试案例 {i+1}: symbol={symbol}, period={period}, adjust={adjust or '无复权'} ---")
            
            # 尝试获取少量数据（最近5天）
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
            
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            if df is not None and not df.empty:
                logger.info(f"  ✓ 成功获取 {len(df)} 条数据")
                logger.debug(f"  数据列名: {list(df.columns)}")
                test_results.append(True)
            else:
                logger.warning(f"  ✗ 返回空数据")
                test_results.append(False)
                
        except Exception as e:
            logger.error(f"  ✗ 测试失败: {str(e)}")
            test_results.append(False)
    
    # 如果上面的方法都不行，尝试最基本的调用
    if not any(test_results):
        logger.info("--- 尝试最基本的API调用 ---")
        try:
            # 测试获取所有股票列表
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                logger.info(f"✓ 基本股票列表API调用成功，获取到 {len(df)} 只股票")
                logger.debug(f"数据列名: {list(df.columns)}")
                test_results.append(True)
            else:
                logger.error("✗ 基本股票列表API调用失败")
                test_results.append(False)
        except Exception as e:
            logger.error(f"✗ 基本股票列表API调用失败: {str(e)}")
            test_results.append(False)
    
    logger.info("===== akshare API 测试完成 =====")
    
    # 分析测试结果
    successful_tests = sum(test_results)
    total_tests = len(test_results)
    success_rate = successful_tests / total_tests if total_tests > 0 else 0
    
    logger.info(f"=== API测试结果分析 ===")
    logger.info(f"总测试数: {total_tests}, 成功数: {successful_tests}, 成功率: {success_rate:.2%}")
    
    if successful_tests == 0:
        logger.error("所有API测试都失败，akshare可能存在问题")
        return False, None
    
    # 确定最佳参数（这里简单处理，实际可以根据成功率选择）
    best_param = "qfq"  # 默认使用前复权
    
    if success_rate >= 0.5:
        logger.info("API测试通过，可以继续执行爬取任务")
        return True, best_param
    else:
        logger.error("API测试失败率过高，停止执行爬取任务")
        return False, None

def fetch_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """获取单只股票的日线数据，使用中文列名"""
    try:
        # 确保股票代码是字符串，并且是6位（前面补零）
        stock_code = str(stock_code).zfill(6)
        
        # 确定市场前缀
        market_prefix = 'sh' if stock_code.startswith('6') else 'sz'
        ak_code = f"{market_prefix}{stock_code}"
        
        # 重要：使用正确的复权参数
        adjust_param = "qfq"  # 前复权
        
        logger.debug(f"正在获取股票 {stock_code} 的日线数据 (代码: {ak_code}, 复权参数: {adjust_param})")
        
        # 【关键修改】使用正确的参数组合
        try:
            df = ak.stock_zh_a_hist(
                symbol=ak_code,
                period="daily",
                start_date=(datetime.now() - timedelta(days=365)).strftime("%Y%m%d"),
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust=adjust_param
            )
        except Exception as e:
            logger.warning(f"使用标准参数失败，尝试简化参数: {str(e)}")
            # 尝试不带日期参数
            try:
                df = ak.stock_zh_a_hist(
                    symbol=ak_code,
                    period="daily",
                    adjust=adjust_param
                )
            except Exception as e2:
                logger.warning(f"使用简化参数也失败，尝试无复权: {str(e2)}")
                # 最后尝试无复权
                df = ak.stock_zh_a_hist(
                    symbol=ak_code,
                    period="daily",
                    adjust=""
                )
        
        # 【关键修改】添加详细的API响应检查
        if df is None or df.empty:
            logger.warning(f"股票 {stock_code} 的日线数据为空")
            return pd.DataFrame()
        
        # 添加列名检查日志
        logger.debug(f"股票 {stock_code} 获取到的列名: {df.columns.tolist()}")
        
        # 确保必要列存在
        required_columns = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"股票 {stock_code} 数据缺少必要列: {missing_columns}")
            return pd.DataFrame()
        
        # 确保日期格式正确
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.strftime('%Y-%m-%d')
            df = df.sort_values('日期').reset_index(drop=True)
        
        # 确保数值列是数值类型
        numeric_columns = ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 移除NaN值
        df = df.dropna(subset=['收盘', '成交量'])
        
        logger.info(f"成功获取股票 {stock_code} 的日线数据，共 {len(df)} 条记录")
        return df
    
    except Exception as e:
        # 添加详细的异常日志
        logger.error(f"获取股票 {stock_code} 日线数据时发生未捕获的异常:", exc_info=True)
        logger.error(f"akshare 版本: {ak.__version__}")
        logger.error(f"akshare 模块路径: {ak.__file__}")
        return pd.DataFrame()

def save_stock_daily_data(stock_code: str, df: pd.DataFrame):
    """保存股票日线数据到CSV文件，使用中文列名"""
    if df.empty:
        return
    
    try:
        file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        df.to_csv(file_path, index=False)
        logger.debug(f"已保存股票 {stock_code} 的日线数据到 {file_path}")
        
        # 【关键修改】只需简单调用，无需任何额外逻辑
        commit_files_in_batches(file_path)
        logger.debug(f"已提交股票 {stock_code} 的日线数据到仓库")
    except Exception as e:
        logger.error(f"保存股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)

def update_all_stocks_daily_data():
    """更新所有股票的日线数据，使用中文列名"""
    ensure_directory_exists()
    
    # 确保基础信息文件存在
    if not os.path.exists(BASIC_INFO_FILE):
        logger.info("基础信息文件不存在，正在创建...")
        if not create_or_update_basic_info():
            logger.error("基础信息文件创建失败，无法更新日线数据")
            return False
    
    # 获取基础信息文件
    try:
        basic_info_df = pd.read_csv(BASIC_INFO_FILE)
        if basic_info_df.empty:
            logger.error("基础信息文件为空，无法更新日线数据")
            return False
    except Exception as e:
        logger.error(f"读取基础信息文件失败: {str(e)}", exc_info=True)
        return False
    
    # 【关键修复】获取 next_crawl_index 值
    # 由于所有行的 next_crawl_index 值相同，取第一行即可
    next_index = int(basic_info_df["next_crawl_index"].iloc[0])
    total_stocks = len(basic_info_df)
    
    logger.info(f"当前爬取状态: next_crawl_index = {next_index} (共 {total_stocks} 只股票)")
    
    # 【关键修复】确定要爬取的股票范围
    start_idx = next_index
    end_idx = min(next_index + 100, total_stocks)
    
    # 如果已经爬取完所有股票，重置索引
    if start_idx >= total_stocks:
        logger.info("已爬取完所有股票，重置爬取状态")
        start_idx = 0
        end_idx = min(100, total_stocks)
    
    # 获取要爬取的股票
    batch_df = basic_info_df.iloc[start_idx:end_idx]
    batch_codes = batch_df["代码"].tolist()
    
    if not batch_codes:
        logger.warning("没有可爬取的股票")
        return False
    
    logger.info(f"正在处理第 {start_idx//100 + 1} 批，共 {len(batch_codes)} 只股票 (索引 {start_idx} - {end_idx-1})")
    
    # 记录第一批和最后一批股票
    first_stock = batch_df.iloc[0]
    last_stock = batch_df.iloc[-1]
    logger.info(f"当前批次第一只股票: {first_stock['代码']} - {first_stock['名称']} (索引 {start_idx})")
    logger.info(f"当前批次最后一只股票: {last_stock['代码']} - {last_stock['名称']} (索引 {end_idx-1})")
    
    # 处理这批股票
    for stock_code in batch_codes:
        # 添加随机延时，避免请求过于频繁
        time.sleep(random.uniform(1.5, 2.5))  # 增加延时，避免被限流
        df = fetch_stock_daily_data(stock_code)
        if not df.empty:
            save_stock_daily_data(stock_code, df)
    
    # 【关键修复】更新 next_crawl_index
    new_index = end_idx
    if new_index >= total_stocks:
        new_index = 0  # 重置，下次从头开始
    
    logger.info(f"更新 next_crawl_index = {new_index}")
    basic_info_df["next_crawl_index"] = new_index
    basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
    
    # 提交更新后的基础信息文件
    commit_files_in_batches(BASIC_INFO_FILE)
    logger.info(f"已提交更新后的基础信息文件到仓库: {BASIC_INFO_FILE}")
    
    # 检查是否还有未完成的股票
    remaining_stocks = total_stocks - new_index
    if remaining_stocks < 0:
        remaining_stocks = total_stocks  # 重置后
    
    logger.info(f"已完成 {len(batch_codes)} 只股票爬取，还有 {remaining_stocks} 只股票待爬取")
    
    return True

def main():
    """主函数：更新所有股票数据"""
    logger.info("===== 开始更新股票数据 =====")
    
    # 添加初始延时，避免立即请求
    time.sleep(random.uniform(1.0, 2.0))
    
    # 1. 运行 akshare API 测试（关键诊断工具）
    api_ok, best_param = test_akshare_api()
    if not api_ok:
        logger.error("akshare API 测试失败，停止执行爬取任务")
        return
    
    # 2. 确保基础信息文件存在
    if not os.path.exists(BASIC_INFO_FILE) or os.path.getsize(BASIC_INFO_FILE) == 0:
        logger.info("基础信息文件不存在或为空，正在创建...")
        if not create_or_update_basic_info():
            logger.error("基础信息文件创建失败，无法继续")
            return
    
    # 3. 只更新一批股票（最多100只）
    if update_all_stocks_daily_data():
        logger.info("已成功处理一批股票数据")
    else:
        logger.error("处理股票数据失败")
    
    logger.info("===== 股票数据更新完成 =====")

if __name__ == "__main__":
    main()
