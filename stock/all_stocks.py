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
import traceback
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

# 专业级重试配置
MAX_RETRIES = 6  # 增加重试次数
BASE_RETRY_DELAY = 10  # 基础重试延迟（秒）
MAX_RANDOM_DELAY = 20  # 最大随机延时（秒）

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
            # 【终极修复】大幅增加随机延时（10.0-20.0秒）- 避免被封
            delay = random.uniform(10.0, 20.0)
            logger.info(f"获取财务数据前等待 {delay:.2f} 秒（尝试 {retry+1}/{MAX_RETRIES}）...")
            time.sleep(delay)
            
            logger.info("正在获取股票财务数据...")
            
            # 获取财务数据
            financial_data = ak.stock_financial_analysis_indicator(symbol="all")
            
            if financial_data.empty:
                logger.error("获取股票财务数据失败：返回空数据")
                if retry < MAX_RETRIES - 1:
                    # 【智能退避】每次重试增加额外延迟
                    extra_delay = retry * 5
                    total_delay = BASE_RETRY_DELAY + extra_delay
                    logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                    time.sleep(total_delay)
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
            logger.error(f"异常堆栈: {traceback.format_exc()}")
            if retry < MAX_RETRIES - 1:
                # 【智能退避】每次重试增加额外延迟
                extra_delay = retry * 5
                total_delay = BASE_RETRY_DELAY + extra_delay
                logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                time.sleep(total_delay)
    
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
            # 【终极修复】大幅增加随机延时（10.0-20.0秒）- 避免被封
            delay = random.uniform(10.0, 20.0)
            logger.info(f"获取质押数据前等待 {delay:.2f} 秒（尝试 {retry+1}/{MAX_RETRIES}）...")
            time.sleep(delay)
            
            logger.info("正在获取股票质押数据...")
            
            # 获取质押数据
            pledge_data = ak.stock_a_pledge_ratio()
            
            if pledge_data.empty:
                logger.error("获取股票质押数据失败：返回空数据")
                if retry < MAX_RETRIES - 1:
                    # 【智能退避】每次重试增加额外延迟
                    extra_delay = retry * 5
                    total_delay = BASE_RETRY_DELAY + extra_delay
                    logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                    time.sleep(total_delay)
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
            logger.error(f"异常堆栈: {traceback.format_exc()}")
            if retry < MAX_RETRIES - 1:
                # 【智能退避】每次重试增加额外延迟
                extra_delay = retry * 5
                total_delay = BASE_RETRY_DELAY + extra_delay
                logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                time.sleep(total_delay)
    
    logger.error("获取股票质押数据失败，已达到最大重试次数")
    return pd.DataFrame()

def get_stock_basic_info():
    """
    获取股票基础信息，应用所有过滤条件
    
    Returns:
        pd.DataFrame: 过滤后的股票基础信息
    """
    # 【终极修复】使用更可靠的方法获取股票列表
    # 不再使用可能分页的 stock_zh_a_spot_em，而是组合多个接口
    all_stocks = []
    
    # 1. 获取主板A股
    for retry in range(MAX_RETRIES):
        try:
            # 【终极修复】大幅增加随机延时（10.0-20.0秒）- 避免被封
            delay = random.uniform(10.0, 20.0)
            logger.info(f"获取主板A股前等待 {delay:.2f} 秒（尝试 {retry+1}/{MAX_RETRIES}）...")
            time.sleep(delay)
            
            logger.info("正在获取主板A股数据...")
            df = ak.stock_info_a_code_name(symbol="主板A股")
            if not df.empty:
                df['板块'] = '主板A股'
                all_stocks.append(df)
                logger.info(f"成功获取 {len(df)} 条主板A股数据")
                break
        except Exception as e:
            logger.error(f"获取主板A股失败 (尝试 {retry+1}/{MAX_RETRIES}): {str(e)}", exc_info=True)
            logger.error(f"异常堆栈: {traceback.format_exc()}")
            if retry < MAX_RETRIES - 1:
                # 【智能退避】每次重试增加额外延迟
                extra_delay = retry * 5
                total_delay = BASE_RETRY_DELAY + extra_delay
                logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                time.sleep(total_delay)
    
    # 2. 获取科创板
    for retry in range(MAX_RETRIES):
        try:
            # 【终极修复】大幅增加随机延时（10.0-20.0秒）- 避免被封
            delay = random.uniform(10.0, 20.0)
            logger.info(f"获取科创板前等待 {delay:.2f} 秒（尝试 {retry+1}/{MAX_RETRIES}）...")
            time.sleep(delay)
            
            logger.info("正在获取科创板数据...")
            df = ak.stock_info_a_code_name(symbol="科创板")
            if not df.empty:
                df['板块'] = '科创板'
                all_stocks.append(df)
                logger.info(f"成功获取 {len(df)} 条科创板数据")
                break
        except Exception as e:
            logger.error(f"获取科创板失败 (尝试 {retry+1}/{MAX_RETRIES}): {str(e)}", exc_info=True)
            logger.error(f"异常堆栈: {traceback.format_exc()}")
            if retry < MAX_RETRIES - 1:
                # 【智能退避】每次重试增加额外延迟
                extra_delay = retry * 5
                total_delay = BASE_RETRY_DELAY + extra_delay
                logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                time.sleep(total_delay)
    
    # 3. 获取创业板
    for retry in range(MAX_RETRIES):
        try:
            # 【终极修复】大幅增加随机延时（10.0-20.0秒）- 避免被封
            delay = random.uniform(10.0, 20.0)
            logger.info(f"获取创业板前等待 {delay:.2f} 秒（尝试 {retry+1}/{MAX_RETRIES}）...")
            time.sleep(delay)
            
            logger.info("正在获取创业板数据...")
            df = ak.stock_info_a_code_name(symbol="创业板")
            if not df.empty:
                df['板块'] = '创业板'
                all_stocks.append(df)
                logger.info(f"成功获取 {len(df)} 条创业板数据")
                break
        except Exception as e:
            logger.error(f"获取创业板失败 (尝试 {retry+1}/{MAX_RETRIES}): {str(e)}", exc_info=True)
            logger.error(f"异常堆栈: {traceback.format_exc()}")
            if retry < MAX_RETRIES - 1:
                # 【智能退避】每次重试增加额外延迟
                extra_delay = retry * 5
                total_delay = BASE_RETRY_DELAY + extra_delay
                logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                time.sleep(total_delay)
    
    # 4. 获取北交所
    for retry in range(MAX_RETRIES):
        try:
            # 【终极修复】大幅增加随机延时（10.0-20.0秒）- 避免被封
            delay = random.uniform(10.0, 20.0)
            logger.info(f"获取北交所前等待 {delay:.2f} 秒（尝试 {retry+1}/{MAX_RETRIES}）...")
            time.sleep(delay)
            
            logger.info("正在获取北交所数据...")
            df = ak.stock_info_bj_a_code_name()
            if not df.empty:
                df['板块'] = '北交所'
                all_stocks.append(df)
                logger.info(f"成功获取 {len(df)} 条北交所数据")
                break
        except Exception as e:
            logger.error(f"获取北交所失败 (尝试 {retry+1}/{MAX_RETRIES}): {str(e)}", exc_info=True)
            logger.error(f"异常堆栈: {traceback.format_exc()}")
            if retry < MAX_RETRIES - 1:
                # 【智能退避】每次重试增加额外延迟
                extra_delay = retry * 5
                total_delay = BASE_RETRY_DELAY + extra_delay
                logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                time.sleep(total_delay)
    
    if not all_stocks:
        logger.error("无法获取任何股票列表数据")
        return pd.DataFrame()
    
    # 合并所有数据
    stock_info = pd.concat(all_stocks, ignore_index=True)
    
    # 确保列名一致
    if 'code' in stock_info.columns:
        stock_info = stock_info.rename(columns={'code': '代码'})
    if 'name' in stock_info.columns:
        stock_info = stock_info.rename(columns={'name': '名称'})
    
    # 【关键修复】应用基础过滤条件
    # 1. 移除ST和*ST股票（关键修复：设置regex=False避免正则表达式错误）
    if '名称' in stock_info.columns:
        # 修复正则表达式问题：使用regex=False
        stock_info = stock_info[~stock_info["名称"].str.contains("ST", na=False, regex=False)].copy()
        stock_info = stock_info[~stock_info["名称"].str.contains("*ST", na=False, regex=False)].copy()
    
    # 2. 移除名称以"N"开头的新上市股票
    if '名称' in stock_info.columns:
        stock_info = stock_info[~stock_info["名称"].str.startswith("N")].copy()
    
    # 3. 移除名称包含"退市"的股票
    if '名称' in stock_info.columns:
        stock_info = stock_info[~stock_info["名称"].str.contains("退市", na=False, regex=False)].copy()
    
    # 确保代码列是6位格式
    if '代码' in stock_info.columns:
        stock_info["代码"] = stock_info["代码"].apply(lambda x: str(x).zfill(6))
        # 移除无效股票
        stock_info = stock_info[stock_info["代码"].notna()]
        stock_info = stock_info[stock_info["代码"].str.len() == 6]
        stock_info = stock_info.reset_index(drop=True)
    
    # 如果没有代码列，尝试从其他列提取
    if '代码' not in stock_info.columns and 'symbol' in stock_info.columns:
        stock_info["代码"] = stock_info["symbol"].apply(lambda x: str(x).zfill(6))
    
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
            # 如果数值小于100万，不太可能是元单位
            if value < 1000000:
                return value * 100000000  # 亿元转元
            else:
                return value  # 已经是元单位
    
    # 添加市值列（如果不存在）
    if "流通市值" not in stock_info.columns:
        stock_info["流通市值"] = 0.0
    if "总市值" not in stock_info.columns:
        stock_info["总市值"] = 0.0
    
    # 处理总市值和流通市值
    stock_info["总市值"] = stock_info["总市值"].apply(process_market_cap)
    stock_info["流通市值"] = stock_info["流通市值"].apply(process_market_cap)
    
    # 【关键修复】添加所属板块
    stock_info["所属板块"] = stock_info["代码"].apply(get_stock_section)
    
    # 获取财务数据
    financial_data = get_stock_financial_data()
    
    # 获取质押数据
    pledge_data = get_stock_pledge_data()
    
    # 合并财务数据
    if not financial_data.empty and '股票代码' in financial_data.columns:
        stock_info = pd.merge(stock_info, financial_data, left_on="代码", right_on="股票代码", how="left")
    
    # 合并质押数据
    if not pledge_data.empty and '股票代码' in pledge_data.columns:
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
    # 只保留存在的列
    existing_columns = [col for col in final_columns if col in stock_info.columns]
    stock_info = stock_info[existing_columns]
    
    return stock_info

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
        logger.error(f"异常堆栈: {traceback.format_exc()}")
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
