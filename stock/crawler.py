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
import json  # 确保导入 json 模块
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
VALID_STOCKS_FILE = os.path.join(DATA_DIR, "valid_stocks.csv")  # 新增有效股票列表文件
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
        
        # 过滤ST股票和非主板/科创板/创业板股票
        stock_list = df.copy()
        
        # 过滤ST股票
        stock_list = stock_list[~stock_list['名称'].str.contains('ST', na=False)]
        
        # 过滤非主板/科创板/创业板股票
        stock_list = stock_list[stock_list['代码'].str.startswith(('0', '3', '6'))]
        
        logger.info(f"成功获取股票列表，共 {len(stock_list)} 只股票")
        
        # 准备基础信息DataFrame - 严格使用API返回的原始列名
        basic_info_df = pd.DataFrame({
            "代码": stock_list['代码'],
            "名称": stock_list['名称'],
            "所属板块": stock_list['代码'].apply(get_stock_section),
            "流通市值": stock_list['流通市值']
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
        
        # 保存基础信息
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"已创建/更新股票基础信息文件，共 {len(basic_info_df)} 条记录")
        
        # 确认文件已保存
        if os.path.exists(BASIC_INFO_FILE) and os.path.getsize(BASIC_INFO_FILE) > 0:
            logger.info(f"基础信息文件已成功保存到: {BASIC_INFO_FILE}")
            
            # 【关键修改】提交基础信息文件
            commit_files_in_batches(BASIC_INFO_FILE)
            
            return True
        else:
            logger.error(f"基础信息文件保存失败: {BASIC_INFO_FILE} 不存在或为空")
            return False
            
    except Exception as e:
        logger.error(f"创建基础信息文件失败: {str(e)}", exc_info=True)
        return False

def get_valid_stock_codes() -> list:
    """
    获取所有符合策略要求的有效股票代码列表
    从 tickten.py 生成的有效股票列表文件中读取
    
    Returns:
        list: 有效股票代码列表
    """
    if not os.path.exists(VALID_STOCKS_FILE):
        logger.warning(f"有效股票列表文件 {VALID_STOCKS_FILE} 不存在，尝试调用 tickten.py 生成...")
        try:
            # 尝试导入 tickten.py 并生成有效股票列表
            from stock.tickten import get_valid_stock_codes as tickten_get_valid_stock_codes
            valid_stock_codes = tickten_get_valid_stock_codes()
            if not valid_stock_codes:
                logger.error("无法获取有效股票列表")
                return []
            return valid_stock_codes
        except Exception as e:
            logger.error(f"调用 tickten.py 生成有效股票列表失败: {str(e)}", exc_info=True)
            return []
    
    try:
        # 从已存在的有效股票列表文件中读取
        valid_df = pd.read_csv(VALID_STOCKS_FILE)
        valid_stock_codes = valid_df["代码"].tolist()
        logger.info(f"已加载有效股票列表，共 {len(valid_stock_codes)} 只股票")
        return valid_stock_codes
    except Exception as e:
        logger.error(f"加载有效股票列表失败: {str(e)}", exc_info=True)
        return []

def test_akshare_api():
    """测试 akshare API 是否正常工作"""
    logger.info("===== 开始 akshare API 测试 =====")
    logger.info(f"akshare 版本: {ak.__version__}")
    logger.info(f"akshare 模块路径: {ak.__file__}")
    
    test_stocks = [
        ("600000", "浦发银行"),  # 沪市主板
        ("000001", "平安银行"),  # 深市主板
        ("300001", "特锐德"),    # 创业板
        ("688001", "华兴源创")   # 科创板
    ]
    
    # 修正：akshare 中有效的 adjust 参数是 "qfq", "hfq", ""（空字符串）
    for adjust_param in ["qfq", "hfq", ""]:
        for stock_code, name in test_stocks:
            try:
                logger.info(f"--- 测试股票 {stock_code} ({name}) ---")
                logger.info(f"  测试复权参数: {adjust_param or '无复权'}")
                
                # 确定市场前缀
                market_prefix = 'sh' if stock_code.startswith('6') else 'sz'
                ak_code = f"{market_prefix}{stock_code}"
                
                # 获取日线数据（只获取最近30天）
                df = ak.stock_zh_a_hist(
                    symbol=ak_code,
                    period="daily",
                    start_date=(datetime.now() - timedelta(days=30)).strftime("%Y%m%d"),
                    end_date=datetime.now().strftime("%Y%m%d"),
                    adjust=adjust_param
                )
                
                if df.empty:
                    logger.warning(f"  股票 {stock_code} 使用 {adjust_param or '无复权'} 参数返回空数据")
                else:
                    logger.info(f"  股票 {stock_code} 使用 {adjust_param or '无复权'} 参数成功获取 {len(df)} 条数据")
                    logger.debug(f"  数据列名: {df.columns.tolist()}")
                    if not df.empty:
                        logger.debug(f"  首条数据: {df.iloc[0].to_dict()}")
            except Exception as e:
                logger.error(f"  测试 {stock_code} 使用 {adjust_param or '无复权'} 参数时出错: {str(e)}", exc_info=True)
    
    logger.info("===== akshare API 测试完成 =====")

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
        
        # 【关键修改】对于新上市股票，减少请求的历史数据天数
        try:
            # 尝试获取1年数据
            df = ak.stock_zh_a_hist(
                symbol=ak_code,
                period="daily",
                start_date=(datetime.now() - timedelta(days=365)).strftime("%Y%m%d"),
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust=adjust_param
            )
        except Exception as e:
            logger.warning(f"获取股票 {stock_code} 的1年数据失败，尝试获取30天数据: {str(e)}")
            try:
                # 尝试获取30天数据（适用于新上市股票）
                df = ak.stock_zh_a_hist(
                    symbol=ak_code,
                    period="daily",
                    start_date=(datetime.now() - timedelta(days=30)).strftime("%Y%m%d"),
                    end_date=datetime.now().strftime("%Y%m%d"),
                    adjust=adjust_param
                )
            except Exception as e:
                logger.warning(f"获取股票 {stock_code} 的30天数据失败，尝试获取不复权数据: {str(e)}")
                # 尝试不复权数据
                df = ak.stock_zh_a_hist(
                    symbol=ak_code,
                    period="daily",
                    start_date=(datetime.now() - timedelta(days=30)).strftime("%Y%m%d"),
                    end_date=datetime.now().strftime("%Y%m%d"),
                    adjust=""
                )
        
        # 【关键修改】添加详细的API响应检查
        if df.empty:
            logger.warning(f"股票 {stock_code} 的日线数据为空 - 可能原因: 1) 无效的复权参数 2) API限制 3) 股票代码格式问题")
            # 尝试用其他复权参数再试一次（用于诊断）
            for test_adjust in ["hfq", ""]:
                try:
                    test_df = ak.stock_zh_a_hist(
                        symbol=ak_code,
                        period="daily",
                        start_date=(datetime.now() - timedelta(days=30)).strftime("%Y%m%d"),
                        end_date=datetime.now().strftime("%Y%m%d"),
                        adjust=test_adjust
                    )
                    if not test_df.empty:
                        logger.error(f"!!! 诊断发现: 股票 {stock_code} 使用 {test_adjust or '无复权'} 复权参数可以获取数据，但 {adjust_param} 不行 !!!")
                        return test_df
                except Exception as e:
                    logger.debug(f"测试复权参数 {test_adjust or '无复权'} 失败: {str(e)}")
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
    
    # 【关键修改】获取有效股票列表（由 tickten.py 定义）
    valid_stock_codes = get_valid_stock_codes()
    if not valid_stock_codes:
        logger.error("无法获取有效股票列表，无法更新日线数据")
        return False
    
    # 【关键修改】获取需要爬取的股票列表
    # 只处理有效股票
    need_crawl_df = basic_info_df[basic_info_df["代码"].isin(valid_stock_codes)]
    stock_codes = need_crawl_df["代码"].tolist()
    
    # 【关键修改】检查 next_crawl_index 状态
    # 我们使用一个单独的状态文件来跟踪进度
    state_file = os.path.join(DATA_DIR, "crawler_state.json")
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                next_index = state.get("next_crawl_index", 0)
        except Exception as e:
            logger.warning(f"加载爬取状态失败: {str(e)}，将重置为0")
            next_index = 0
    else:
        next_index = 0
    
    total_valid_stocks = len(stock_codes)
    logger.info(f"当前爬取状态: next_crawl_index = {next_index} (共 {total_valid_stocks} 只有效股票)")
    
    # 【关键修改】确定要爬取的股票范围
    start_idx = next_index
    end_idx = min(next_index + 100, total_valid_stocks)
    
    # 如果已经爬取完所有股票，重置索引
    if start_idx >= total_valid_stocks:
        logger.info("已爬取完所有有效股票，重置爬取状态")
        start_idx = 0
        end_idx = min(100, total_valid_stocks)
    
    # 获取要爬取的股票
    batch_codes = stock_codes[start_idx:end_idx]
    
    if not batch_codes:
        logger.warning("没有可爬取的股票")
        return False
    
    logger.info(f"正在处理第 {start_idx//100 + 1} 批，共 {len(batch_codes)} 只股票 (索引 {start_idx} - {end_idx-1})")
    
    # 记录第一批和最后一批股票
    first_stock = need_crawl_df.iloc[stock_codes.index(batch_codes[0])]
    last_stock = need_crawl_df.iloc[stock_codes.index(batch_codes[-1])]
    logger.info(f"当前批次第一只股票: {first_stock['代码']} - {first_stock['名称']} (索引 {start_idx})")
    logger.info(f"当前批次最后一只股票: {last_stock['代码']} - {last_stock['名称']} (索引 {end_idx-1})")
    
    # 处理这批股票
    for stock_code in batch_codes:
        # 添加随机延时，避免请求过于频繁
        time.sleep(random.uniform(1.5, 2.5))  # 增加延时，避免被限流
        df = fetch_stock_daily_data(stock_code)
        if not df.empty:
            save_stock_daily_data(stock_code, df)
    
    # 【关键修改】更新爬取状态
    new_index = end_idx
    if new_index >= total_valid_stocks:
        new_index = 0  # 重置，下次从头开始
    
    logger.info(f"更新 next_crawl_index = {new_index}")
    
    # 保存爬取状态
    try:
        with open(state_file, 'w') as f:
            json.dump({"next_crawl_index": new_index}, f)
        logger.debug(f"已更新爬取状态: next_crawl_index = {new_index}")
        
        # 提交状态文件
        commit_files_in_batches(state_file)
    except Exception as e:
        logger.error(f"保存爬取状态失败: {str(e)}")
    
    # 检查是否还有未完成的股票
    remaining_stocks = total_valid_stocks - new_index
    if remaining_stocks < 0:
        remaining_stocks = total_valid_stocks  # 重置后
    
    logger.info(f"已完成 {len(batch_codes)} 只股票爬取，还有 {remaining_stocks} 只股票待爬取")
    
    return True

def main():
    """主函数：更新所有股票数据"""
    logger.info("===== 开始更新股票数据 =====")
    
    # 添加初始延时，避免立即请求
    time.sleep(random.uniform(1.0, 2.0))
    
    # 1. 运行 akshare API 测试（关键诊断工具）
    test_akshare_api()
    
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
