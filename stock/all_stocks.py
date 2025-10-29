#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票列表更新模块

【详细过滤条件】
1. 基础过滤：
   - 移除ST和*ST股票
   - 移除名称以"N"开头的新上市股票
   - 移除名称包含"退市"的股票

2. 财务数据过滤：
   - 市盈率(动态)：排除亏损股票（PE_TTM ≤ 0）
   - 每股收益：排除负数股票（EPS < 0）
   - 市盈率(静态)：排除亏损股票（PE_STATIC ≤ 0）
   - 营业总收入：排除同比下降的股票（营业收入同比增长率 < 0）
   - 总质押股份数量：排除有质押的股票（质押数量 > 0）
   - 净利润：排除净利润同比下降的股票
   - ROE：排除低于5%的股票
"""

import os
import logging
import pandas as pd
import akshare as ak
import time
import random
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import is_trading_day, get_last_trading_day, get_beijing_time
from utils.git_utils import commit_files_in_batches

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

# 重试配置
MAX_RETRIES = 3
RETRY_DELAY = 5  # 重试前等待时间（秒）

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

def get_stock_section(stock_code: str) -> str:
    """
    获取股票所属板块
    
    Args:
        stock_code: 股票代码（已格式化为6位）
    
    Returns:
        str: 板块名称
    """
    # 确保股票代码是6位
    stock_code = format_stock_code(stock_code)
    if not stock_code:
        return "格式错误"
    
    # 根据股票代码前缀判断板块
    if stock_code.startswith('60'):
        return "沪市主板"
    elif stock_code.startswith('00'):
        return "深市主板"
    elif stock_code.startswith('30'):
        return "创业板"
    elif stock_code.startswith('688'):
        return "科创板"
    elif stock_code.startswith('8'):
        return "北交所"
    elif stock_code.startswith('4') or stock_code.startswith('8'):
        return "三板市场"
    else:
        return "其他板块"

def get_stock_financial_data():
    """
    获取股票财务数据
    
    Returns:
        pd.DataFrame: 股票财务数据
    """
    for retry in range(MAX_RETRIES):
        try:
            # 【关键修复】添加随机延时避免被封（5.0-8.0秒）
            delay = random.uniform(5.0, 8.0)
            logger.info(f"获取财务数据前等待 {delay:.2f} 秒...")
            time.sleep(delay)
            
            logger.info("正在获取股票财务数据...")
            
            # 获取财务数据
            financial_data = ak.stock_financial_analysis_indicator(symbol="all")
            
            if financial_data.empty:
                logger.error("获取股票财务数据失败：返回空数据")
                if retry < MAX_RETRIES - 1:
                    logger.warning(f"将在 {RETRY_DELAY} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
                    continue
                return pd.DataFrame()
            
            # 重命名列，确保一致性
            financial_data = financial_data.rename(columns={
                '代码': '股票代码',
                '名称': '股票名称',
                '市盈率-动态': 'PE_TTM',
                '每股收益': 'EPS',
                '市盈率-静态': 'PE_STATIC',
                '营业收入-同比增长': 'Revenue_Growth',
                '净利润-同比增长': 'NetProfit_Growth',
                '净资产收益率': 'ROE'
            })
            
            # 转换数据类型
            financial_data['PE_TTM'] = pd.to_numeric(financial_data['PE_TTM'], errors='coerce')
            financial_data['EPS'] = pd.to_numeric(financial_data['EPS'], errors='coerce')
            financial_data['PE_STATIC'] = pd.to_numeric(financial_data['PE_STATIC'], errors='coerce')
            financial_data['Revenue_Growth'] = pd.to_numeric(financial_data['Revenue_Growth'], errors='coerce')
            financial_data['NetProfit_Growth'] = pd.to_numeric(financial_data['NetProfit_Growth'], errors='coerce')
            financial_data['ROE'] = pd.to_numeric(financial_data['ROE'], errors='coerce')
            
            logger.info(f"成功获取 {len(financial_data)} 条股票财务数据")
            return financial_data
        
        except Exception as e:
            logger.error(f"获取股票财务数据失败 (尝试 {retry+1}/{MAX_RETRIES}): {str(e)}", exc_info=True)
            if retry < MAX_RETRIES - 1:
                logger.warning(f"将在 {RETRY_DELAY} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
    
    logger.error("获取股票财务数据失败，已达到最大重试次数")
    return pd.DataFrame()

def get_stock_pledge_data():
    """
    获取股票质押数据
    
    Returns:
        pd.DataFrame: 股票质押数据
    """
    for retry in range(MAX_RETRIES):
        try:
            # 【关键修复】添加随机延时避免被封（5.0-8.0秒）
            delay = random.uniform(5.0, 8.0)
            logger.info(f"获取质押数据前等待 {delay:.2f} 秒...")
            time.sleep(delay)
            
            logger.info("正在获取股票质押数据...")
            
            # 获取质押数据
            pledge_data = ak.stock_a_pledge_ratio()
            
            if pledge_data.empty:
                logger.error("获取股票质押数据失败：返回空数据")
                if retry < MAX_RETRIES - 1:
                    logger.warning(f"将在 {RETRY_DELAY} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
                    continue
                return pd.DataFrame()
            
            # 重命名列，确保一致性
            pledge_data = pledge_data.rename(columns={
                'code': '股票代码',
                'name': '股票名称',
                'pledge_ratio': 'Pledge_Ratio',
                'pledge_total': 'Pledge_Total'
            })
            
            # 确保股票代码是6位
            pledge_data['股票代码'] = pledge_data['股票代码'].apply(format_stock_code)
            
            logger.info(f"成功获取 {len(pledge_data)} 条股票质押数据")
            return pledge_data
        
        except Exception as e:
            logger.error(f"获取股票质押数据失败 (尝试 {retry+1}/{MAX_RETRIES}): {str(e)}", exc_info=True)
            if retry < MAX_RETRIES - 1:
                logger.warning(f"将在 {RETRY_DELAY} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
    
    logger.error("获取股票质押数据失败，已达到最大重试次数")
    return pd.DataFrame()

def get_stock_basic_info():
    """
    获取股票基础信息，应用所有过滤条件
    
    Returns:
        pd.DataFrame: 过滤后的股票基础信息
    """
    for retry in range(MAX_RETRIES):
        try:
            # 【关键修复】添加随机延时避免被封（5.0-8.0秒）
            delay = random.uniform(5.0, 8.0)
            logger.info(f"获取基础信息前等待 {delay:.2f} 秒...")
            time.sleep(delay)
            
            logger.info("正在获取股票基础信息...")
            
            # 获取股票基础信息
            stock_info = ak.stock_zh_a_spot_em()
            
            if stock_info.empty:
                logger.error("获取股票基础信息失败：返回空数据")
                if retry < MAX_RETRIES - 1:
                    logger.warning(f"将在 {RETRY_DELAY} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
                    continue
                return pd.DataFrame()
            
            # 【关键修复】只保留必需列
            required_columns = ["代码", "名称", "流通市值", "总市值"]
            available_columns = [col for col in required_columns if col in stock_info.columns]
            
            if not available_columns:
                logger.error("接口返回数据缺少所有必要列")
                if retry < MAX_RETRIES - 1:
                    logger.warning(f"将在 {RETRY_DELAY} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY)
                    continue
                return pd.DataFrame()
            
            stock_info = stock_info[available_columns].copy()
            
            # 确保代码列是6位格式
            stock_info["代码"] = stock_info["代码"].apply(lambda x: str(x).zfill(6))
            
            # 移除无效股票
            stock_info = stock_info[stock_info["代码"].notna()]
            stock_info = stock_info[stock_info["代码"].str.len() == 6]
            stock_info = stock_info.reset_index(drop=True)
            
            # 【关键修复】应用基础过滤条件
            # 1. 移除ST和*ST股票
            stock_info = stock_info[~stock_info["名称"].str.contains("ST", na=False)].copy()
            stock_info = stock_info[~stock_info["名称"].str.contains("*ST", na=False)].copy()
            
            # 2. 移除名称以"N"开头的新上市股票
            stock_info = stock_info[~stock_info["名称"].str.startswith("N")].copy()
            
            # 3. 移除名称包含"退市"的股票
            stock_info = stock_info[~stock_info["名称"].str.contains("退市", na=False)].copy()
            
            logger.info(f"成功获取 {len(stock_info)} 条股票基础信息（已应用基础过滤条件）")
            
            # 【专业修复】处理市值单位 - 确保单位统一为"元"
            def process_market_cap(value):
                if pd.isna(value) or value in ["--", "-", ""]:
                    return 0.0
                    
                if isinstance(value, str):
                    value = value.strip().replace(",", "")
                    if "亿" in value:
                        num_part = value.replace("亿", "")
                        try:
                            return float(num_part) * 100000000
                        except:
                            return 0.0
                    elif "万" in value:
                        num_part = value.replace("万", "")
                        try:
                            return float(num_part) * 10000
                        except:
                            return 0.0
                    else:
                        try:
                            return float(value)
                        except:
                            return 0.0
                else:
                    # akshare的stock_zh_a_spot_em通常返回亿元单位
                    if value < 1000000:  # 小于100万，不太可能是元单位
                        return value * 100000000  # 亿元转元
                    else:
                        return value  # 已经是元单位
            
            # 处理总市值和流通市值
            if "总市值" in stock_info.columns:
                stock_info["总市值"] = stock_info["总市值"].apply(process_market_cap)
            else:
                stock_info["总市值"] = 0.0
                
            if "流通市值" in stock_info.columns:
                stock_info["流通市值"] = stock_info["流通市值"].apply(process_market_cap)
            else:
                stock_info["流通市值"] = 0.0
            
            # 【关键修复】添加所属板块
            stock_info["所属板块"] = stock_info["代码"].apply(get_stock_section)
            
            # 获取财务数据
            financial_data = get_stock_financial_data()
            
            # 获取质押数据
            pledge_data = get_stock_pledge_data()
            
            # 合并财务数据
            if not financial_data.empty:
                stock_info = pd.merge(stock_info, financial_data, left_on="代码", right_on="股票代码", how="left")
            
            # 合并质押数据
            if not pledge_data.empty:
                stock_info = pd.merge(stock_info, pledge_data, left_on="代码", right_on="股票代码", how="left")
            
            # 【关键修复】应用财务数据过滤条件
            initial_count = len(stock_info)
            
            # 1. 市盈率(动态)：排除亏损股票（PE_TTM ≤ 0）
            if 'PE_TTM' in stock_info.columns:
                stock_info = stock_info[(stock_info['PE_TTM'] > 0) | (stock_info['PE_TTM'].isna())]
            
            # 2. 每股收益：排除负数股票（EPS < 0）
            if 'EPS' in stock_info.columns:
                stock_info = stock_info[(stock_info['EPS'] >= 0) | (stock_info['EPS'].isna())]
            
            # 3. 市盈率(静态)：排除亏损股票（PE_STATIC ≤ 0）
            if 'PE_STATIC' in stock_info.columns:
                stock_info = stock_info[(stock_info['PE_STATIC'] > 0) | (stock_info['PE_STATIC'].isna())]
            
            # 4. 营业总收入：排除同比下降的股票（营业收入同比增长率 < 0）
            if 'Revenue_Growth' in stock_info.columns:
                stock_info = stock_info[(stock_info['Revenue_Growth'] >= 0) | (stock_info['Revenue_Growth'].isna())]
            
            # 5. 总质押股份数量：排除有质押的股票（质押数量 > 0）
            if 'Pledge_Total' in stock_info.columns:
                stock_info = stock_info[(stock_info['Pledge_Total'] <= 0) | (stock_info['Pledge_Total'].isna())]
            
            # 6. 净利润：排除净利润同比下降的股票
            if 'NetProfit_Growth' in stock_info.columns:
                stock_info = stock_info[(stock_info['NetProfit_Growth'] >= 0) | (stock_info['NetProfit_Growth'].isna())]
            
            # 7. ROE：排除低于5%的股票
            if 'ROE' in stock_info.columns:
                stock_info = stock_info[(stock_info['ROE'] >= 5) | (stock_info['ROE'].isna())]
            
            logger.info(f"应用财务数据过滤条件后，剩余 {len(stock_info)} 条股票记录")
            
            # 【关键修复】添加必需列
            stock_info["数据状态"] = "正常"
            stock_info["next_crawl_index"] = 0
            
            # 【关键修复】确保列顺序正确
            final_columns = ["代码", "名称", "所属板块", "流通市值", "总市值", "数据状态", "next_crawl_index"]
            stock_info = stock_info[final_columns]
            
            return stock_info
        
        except Exception as e:
            logger.error(f"获取股票基础信息失败 (尝试 {retry+1}/{MAX_RETRIES}): {str(e)}", exc_info=True)
            if retry < MAX_RETRIES - 1:
                logger.warning(f"将在 {RETRY_DELAY} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
    
    logger.error("获取股票基础信息失败，已达到最大重试次数")
    return pd.DataFrame()

def update_stock_list():
    """
    更新股票列表，保存到all_stocks.csv
    """
    try:
        logger.info("开始更新股票列表...")
        
        # 获取过滤后的股票基础信息
        stock_info = get_stock_basic_info()
        
        if stock_info.empty:
            logger.error("获取的股票基础信息为空")
            return False
        
        # 保存到CSV文件
        stock_info.to_csv(BASIC_INFO_FILE, index=False, float_format='%.0f')
        
        # 提交到Git仓库
        commit_files_in_batches(BASIC_INFO_FILE, "更新股票列表")
        
        logger.info(f"股票列表已成功更新，共 {len(stock_info)} 条记录")
        return True
    
    except Exception as e:
        logger.error(f"更新股票列表失败: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                        handlers=[
                            logging.StreamHandler(),
                            logging.FileHandler(os.path.join(LOG_DIR, "all_stocks.log"))
                        ])
    
    # 更新股票列表
    update_stock_list()
