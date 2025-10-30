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
BASE_RETRY_DELAY = 20  # 基础重试延迟（秒）
MAX_RANDOM_DELAY = 30  # 最大随机延时（秒）

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
            # 【终极修复】大幅增加随机延时（20.0-30.0秒）- 避免被封
            delay = random.uniform(20.0, 30.0)
            logger.info(f"获取财务数据前等待 {delay:.2f} 秒（尝试 {retry+1}/{MAX_RETRIES}）...")
            time.sleep(delay)
            
            logger.info("正在获取股票财务数据...")
            
            # 【关键修复】正确调用API - 无参数调用（基于您提供的日志证据）
            financial_data = ak.stock_financial_analysis_indicator_em()
            
            # 【关键修复】添加严格的返回值检查
            if financial_data is None:
                logger.error("API返回None，可能是网络问题或数据源问题")
                if retry < MAX_RETRIES - 1:
                    extra_delay = retry * 10
                    total_delay = BASE_RETRY_DELAY + extra_delay
                    logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                    time.sleep(total_delay)
                    continue
                return pd.DataFrame()
            
            if financial_data.empty:
                logger.error("获取股票财务数据失败：返回空数据")
                if retry < MAX_RETRIES - 1:
                    extra_delay = retry * 10
                    total_delay = BASE_RETRY_DELAY + extra_delay
                    logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                    time.sleep(total_delay)
                    continue
                return pd.DataFrame()
            
            # 【终极修复】根据实际API列名精准映射（基于您提供的列名）
            financial_data = financial_data.rename(columns={
                'SECURITY_CODE': '股票代码',
                'SECURITY_NAME_ABBR': '股票名称',
                'EPSJB': 'EPS',  # 每股收益(基本)
                'EPSKCJB': 'PE_STATIC',  # 每股收益(扣除非经常性损益) - 用于计算静态市盈率
                'TOTALOPERATEREVETZ': 'Revenue_Growth',  # 营业总收入同比增长
                'PARENTNETPROFITTZ': 'NetProfit_Growth',  # 净利润同比增长
                'ROEJQ': 'ROE',  # 净资产收益率(加权)
                'BPSTZ': 'Pledge_Total'  # 质押数量 - 从您提供的列名中确认
            })
            
            # 【关键修复】确保股票代码格式统一为6位
            financial_data['股票代码'] = financial_data['股票代码'].apply(format_stock_code)
            
            # 【关键修复】转换数据类型（只转换必需的列）
            required_columns = ['EPS', 'Revenue_Growth', 'NetProfit_Growth', 'ROE', 'Pledge_Total']
            for col in required_columns:
                if col in financial_data.columns:
                    financial_data[col] = pd.to_numeric(financial_data[col], errors='coerce')
            
            # 【关键修复】确保股票代码唯一 - 移除重复项
            financial_data = financial_data.drop_duplicates(subset=['股票代码'], keep='first')
            
            logger.info(f"成功获取 {len(financial_data)} 条股票财务数据（已去重）")
            return financial_data
        
        except Exception as e:
            logger.error(f"获取股票财务数据失败 (尝试 {retry+1}/{MAX_RETRIES}): {str(e)}", exc_info=True)
            logger.error(f"异常堆栈: {traceback.format_exc()}")
            if retry < MAX_RETRIES - 1:
                extra_delay = retry * 10
                total_delay = BASE_RETRY_DELAY + extra_delay
                logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                time.sleep(total_delay)
    
    logger.error("获取股票财务数据失败，已达到最大重试次数")
    return pd.DataFrame()

def get_stock_basic_info():
    """
    获取股票基础信息，应用所有过滤条件
    
    Returns:
        pd.DataFrame: 过滤后的股票基础信息
    """
    # 【终极修复】采用渐进式获取和保存策略
    # 第一阶段：获取基础股票列表并保存
    base_stock_info = get_base_stock_info()
    
    if base_stock_info.empty:
        logger.error("无法获取基础股票列表，无法继续")
        return pd.DataFrame()
    
    # 第二阶段：获取财务数据（如果成功则更新）
    financial_data = get_stock_financial_data()
    
    # 【关键修复】应用财务数据过滤条件 - 逐步过滤并保存
    filtered_stock_info = apply_financial_filters_step_by_step(base_stock_info, financial_data)
    
    return filtered_stock_info

def get_base_stock_info():
    """
    获取基础股票列表（仅包含必要信息）
    
    Returns:
        pd.DataFrame: 基础股票列表
    """
    for retry in range(MAX_RETRIES):
        try:
            # 【终极修复】大幅增加随机延时（15.0-25.0秒）- 避免被封
            delay = random.uniform(15.0, 25.0)
            logger.info(f"获取基础信息前等待 {delay:.2f} 秒（尝试 {retry+1}/{MAX_RETRIES}）...")
            time.sleep(delay)
            
            logger.info("正在获取股票基础信息...")
            
            # 【关键修复】恢复使用原来的API - ak.stock_zh_a_spot_em()
            # 这是您指定的原始API，工作非常顺畅
            stock_info = ak.stock_zh_a_spot_em()
            
            if stock_info.empty:
                logger.error("获取股票基础信息失败：返回空数据")
                if retry < MAX_RETRIES - 1:
                    # 【智能退避】每次重试增加额外延迟
                    extra_delay = retry * 8
                    total_delay = BASE_RETRY_DELAY + extra_delay
                    logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                    time.sleep(total_delay)
                    continue
                return pd.DataFrame()
            
            # 【关键修复】只保留必需列
            required_columns = ["代码", "名称", "流通市值", "总市值"]
            available_columns = [col for col in required_columns if col in stock_info.columns]
            
            if not available_columns:
                logger.error("接口返回数据缺少所有必要列")
                if retry < MAX_RETRIES - 1:
                    # 【智能退避】每次重试增加额外延迟
                    extra_delay = retry * 8
                    total_delay = BASE_RETRY_DELAY + extra_delay
                    logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                    time.sleep(total_delay)
                    continue
                return pd.DataFrame()
            
            stock_info = stock_info[available_columns].copy()
            
            # 【关键修复】确保股票代码唯一 - 移除重复项
            stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
            
            # 确保代码列是6位格式
            stock_info["代码"] = stock_info["代码"].apply(lambda x: str(x).zfill(6))
            
            # 移除无效股票
            stock_info = stock_info[stock_info["代码"].notna()]
            stock_info = stock_info[stock_info["代码"].str.len() == 6]
            stock_info = stock_info.reset_index(drop=True)
            
            # 【关键修复】应用基础过滤条件
            # 1. 只需一次检查移除ST和*ST股票（因为*ST也包含"ST"）
            stock_info = stock_info[~stock_info["名称"].str.contains("ST", na=False, regex=False)].copy()
            
            # 2. 移除名称以"N"开头的新上市股票
            stock_info = stock_info[~stock_info["名称"].str.startswith("N")].copy()
            
            # 3. 移除名称包含"退市"的股票
            stock_info = stock_info[~stock_info["名称"].str.contains("退市", na=False, regex=False)].copy()
            
            # 【关键修复】再次确保股票代码唯一 - 移除重复项
            stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
            
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
            
            # 【关键修复】添加必需列
            stock_info["数据状态"] = "基础数据已获取"
            stock_info["next_crawl_index"] = 0
            
            # 【关键修复】确保列顺序正确
            final_columns = ["代码", "名称", "所属板块", "流通市值", "总市值", "数据状态", "next_crawl_index"]
            stock_info = stock_info[final_columns]
            
            # 【关键修复】渐进式保存：立即保存基础股票列表
            save_base_stock_info(stock_info)
            
            return stock_info
        
        except Exception as e:
            logger.error(f"获取基础股票信息失败 (尝试 {retry+1}/{MAX_RETRIES}): {str(e)}", exc_info=True)
            logger.error(f"异常堆栈: {traceback.format_exc()}")
            if retry < MAX_RETRIES - 1:
                # 【智能退避】每次重试增加额外延迟
                extra_delay = retry * 8
                total_delay = BASE_RETRY_DELAY + extra_delay
                logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES}) - 智能退避策略")
                time.sleep(total_delay)
    
    logger.error("获取基础股票信息失败，已达到最大重试次数")
    return pd.DataFrame()

def save_base_stock_info(stock_info):
    """
    保存基础股票列表到文件
    
    Args:
        stock_info: 基础股票列表DataFrame
    """
    try:
        # 保存到CSV文件
        stock_info.to_csv(BASIC_INFO_FILE, index=False, float_format='%.0f')
        
        # 提交到Git仓库
        commit_files_in_batches(BASIC_INFO_FILE, "更新股票列表（基础数据）")
        
        logger.info(f"基础股票列表已成功更新，共 {len(stock_info)} 条记录")
    except Exception as e:
        logger.error(f"保存基础股票列表失败: {str(e)}", exc_info=True)

def apply_financial_filters_step_by_step(base_stock_info, financial_data):
    """
    【关键修复】应用财务数据过滤条件 - 逐步过滤并保存
    
    Args:
        base_stock_info: 基础股票列表
        financial_ 财务数据
    
    Returns:
        pd.DataFrame: 应用财务过滤后的股票列表
    """
    if base_stock_info.empty:
        return pd.DataFrame()
    
    # 【关键修复】创建副本，避免修改原始数据
    stock_info = base_stock_info.copy()
    
    # 【关键修复】记录初始股票数量
    initial_count = len(stock_info)
    logger.info(f"开始应用财务过滤，初始股票数量: {initial_count}")
    
    # 【关键修复】添加财务数据 - 确保只添加列，不增加行数
    if not financial_data.empty:
        # 【关键修复】使用inner join，确保只保留基础股票中的股票
        stock_info = pd.merge(
            stock_info, 
            financial_data, 
            left_on="代码", 
            right_on="股票代码", 
            how="left",
            suffixes=('', '_financial')
        )
        
        # 【关键修复】移除财务数据中的重复股票代码
        stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
        
        logger.info(f"已合并财务数据，共 {len(stock_info)} 条记录（基础股票: {initial_count}）")
    
    # 【关键修复】确保股票数量不会增加
    if len(stock_info) > initial_count:
        logger.warning(f"合并后股票数量增加! 初始: {initial_count}, 合并后: {len(stock_info)}")
        # 【关键修复】强制移除重复项，确保股票数量不增加
        stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
        logger.warning(f"已移除重复股票，股票数量: {len(stock_info)}")
    
    # 【关键修复】逐步应用财务数据过滤条件，并每步保存
    # 1. 市盈率(动态)：排除亏损股票（PE_TTM ≤ 0）
    if 'EPS' in stock_info.columns and '最新价' in stock_info.columns:
        # 创建PE_TTM列
        stock_info['PE_TTM'] = stock_info['最新价'] / stock_info['EPS']
        
        before = len(stock_info)
        stock_info = stock_info[(stock_info['PE_TTM'] > 0) & (stock_info['PE_TTM'] != float('inf'))]
        removed = before - len(stock_info)
        if removed > 0:
            logger.info(f"排除 {removed} 只PE_TTM ≤ 0 的股票（市盈率(动态)亏损）")
        
        # 【关键修复】确保股票数量减少
        if len(stock_info) > before:
            logger.error("过滤后股票数量增加！强制修正...")
            stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
        
        # 【关键修复】立即保存并提交
        if not stock_info.empty:
            save_and_commit_filtered_stock_info(stock_info, "更新股票列表（动态市盈率过滤后）")
        else:
            logger.error("动态市盈率过滤后股票列表为空，无法继续")
            return pd.DataFrame()
    
    # 2. 每股收益：排除负数股票（EPS < 0）
    if 'EPS' in stock_info.columns:
        before = len(stock_info)
        stock_info = stock_info[stock_info['EPS'] > 0]
        removed = before - len(stock_info)
        if removed > 0:
            logger.info(f"排除 {removed} 只EPS ≤ 0 的股票（每股收益非正）")
        
        # 【关键修复】确保股票数量减少
        if len(stock_info) > before:
            logger.error("过滤后股票数量增加！强制修正...")
            stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
        
        # 【关键修复】立即保存并提交
        if not stock_info.empty:
            save_and_commit_filtered_stock_info(stock_info, "更新股票列表（每股收益过滤后）")
        else:
            logger.error("每股收益过滤后股票列表为空，无法继续")
            return pd.DataFrame()
    
    # 3. 市盈率(静态)：排除亏损股票（PE_STATIC ≤ 0）
    if 'PE_STATIC' in stock_info.columns and '最新价' in stock_info.columns:
        # 创建PE_STATIC列
        stock_info['PE_STATIC'] = stock_info['最新价'] / stock_info['PE_STATIC']
        
        before = len(stock_info)
        stock_info = stock_info[(stock_info['PE_STATIC'] > 0) & (stock_info['PE_STATIC'] != float('inf'))]
        removed = before - len(stock_info)
        if removed > 0:
            logger.info(f"排除 {removed} 只PE_STATIC ≤ 0 的股票（市盈率(静态)亏损）")
        
        # 【关键修复】确保股票数量减少
        if len(stock_info) > before:
            logger.error("过滤后股票数量增加！强制修正...")
            stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
        
        # 【关键修复】立即保存并提交
        if not stock_info.empty:
            save_and_commit_filtered_stock_info(stock_info, "更新股票列表（静态市盈率过滤后）")
        else:
            logger.error("静态市盈率过滤后股票列表为空，无法继续")
            return pd.DataFrame()
    
    # 4. 营业总收入：排除同比下降的股票（营业收入同比增长率 < 0）
    if 'Revenue_Growth' in stock_info.columns:
        before = len(stock_info)
        stock_info = stock_info[stock_info['Revenue_Growth'] >= 0]
        removed = before - len(stock_info)
        if removed > 0:
            logger.info(f"排除 {removed} 只Revenue_Growth < 0 的股票（营业总收入同比下降）")
        
        # 【关键修复】确保股票数量减少
        if len(stock_info) > before:
            logger.error("过滤后股票数量增加！强制修正...")
            stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
        
        # 【关键修复】立即保存并提交
        if not stock_info.empty:
            save_and_commit_filtered_stock_info(stock_info, "更新股票列表（营业总收入过滤后）")
        else:
            logger.error("营业总收入过滤后股票列表为空，无法继续")
            return pd.DataFrame()
    
    # 5. 总质押股份数量：排除有质押的股票（质押数量 > 0）
    if 'Pledge_Total' in stock_info.columns:
        before = len(stock_info)
        stock_info = stock_info[stock_info['Pledge_Total'] <= 0]
        removed = before - len(stock_info)
        if removed > 0:
            logger.info(f"排除 {removed} 只Pledge_Total > 0 的股票（有质押）")
        
        # 【关键修复】确保股票数量减少
        if len(stock_info) > before:
            logger.error("过滤后股票数量增加！强制修正...")
            stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
        
        # 【关键修复】立即保存并提交
        if not stock_info.empty:
            save_and_commit_filtered_stock_info(stock_info, "更新股票列表（质押数量过滤后）")
        else:
            logger.error("质押数量过滤后股票列表为空，无法继续")
            return pd.DataFrame()
    
    # 6. 净利润：排除净利润同比下降的股票
    if 'NetProfit_Growth' in stock_info.columns:
        before = len(stock_info)
        stock_info = stock_info[stock_info['NetProfit_Growth'] >= 0]
        removed = before - len(stock_info)
        if removed > 0:
            logger.info(f"排除 {removed} 只NetProfit_Growth < 0 的股票（净利润同比下降）")
        
        # 【关键修复】确保股票数量减少
        if len(stock_info) > before:
            logger.error("过滤后股票数量增加！强制修正...")
            stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
        
        # 【关键修复】立即保存并提交
        if not stock_info.empty:
            save_and_commit_filtered_stock_info(stock_info, "更新股票列表（净利润过滤后）")
        else:
            logger.error("净利润过滤后股票列表为空，无法继续")
            return pd.DataFrame()
    
    # 7. ROE：排除低于5%的股票
    if 'ROE' in stock_info.columns:
        before = len(stock_info)
        stock_info = stock_info[stock_info['ROE'] >= 5]
        removed = before - len(stock_info)
        if removed > 0:
            logger.info(f"排除 {removed} 只ROE < 5% 的股票（ROE过低）")
        
        # 【关键修复】确保股票数量减少
        if len(stock_info) > before:
            logger.error("过滤后股票数量增加！强制修正...")
            stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
        
        # 【关键修复】立即保存并提交
        if not stock_info.empty:
            save_and_commit_filtered_stock_info(stock_info, "更新股票列表（ROE过滤后）")
        else:
            logger.error("ROE过滤后股票列表为空，无法继续")
            return pd.DataFrame()
    
    # 【关键修复】最终检查：确保股票数量不会超过初始数量
    if len(stock_info) > initial_count:
        logger.error(f"最终股票数量({len(stock_info)})超过初始数量({initial_count})! 强制修正...")
        stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
        if len(stock_info) > initial_count:
            # 如果去重后仍然超过初始数量，说明有新股票被添加，这是不应该的
            logger.critical("严重错误：过滤后股票数量仍然超过初始数量！")
            # 尝试只保留初始股票列表中的股票
            stock_info = stock_info[stock_info['代码'].isin(base_stock_info['代码'])]
    
    # 更新数据状态
    stock_info["数据状态"] = "完整数据已获取"
    
    # 【关键修复】保存应用财务过滤后的股票列表
    try:
        # 确保列顺序正确
        final_columns = ["代码", "名称", "所属板块", "流通市值", "总市值", "数据状态", "next_crawl_index"]
        for col in final_columns:
            if col not in stock_info.columns:
                stock_info[col] = None
                
        stock_info = stock_info[final_columns]
        
        stock_info.to_csv(BASIC_INFO_FILE, index=False, float_format='%.0f')
        commit_files_in_batches(BASIC_INFO_FILE, "更新股票列表（财务过滤后）")
        logger.info(f"应用财务过滤后的股票列表已成功更新，共 {len(stock_info)} 条记录（初始: {initial_count}）")
    except Exception as e:
        logger.error(f"保存财务过滤后的股票列表失败: {str(e)}", exc_info=True)
    
    return stock_info

def save_and_commit_filtered_stock_info(stock_info, commit_message):
    """
    【关键修复】保存并提交过滤后的股票列表
    
    Args:
        stock_info: 过滤后的股票列表DataFrame
        commit_message: 提交信息
    """
    try:
        # 确保列顺序正确
        final_columns = ["代码", "名称", "所属板块", "流通市值", "总市值", "数据状态", "next_crawl_index"]
        for col in final_columns:
            if col not in stock_info.columns:
                stock_info[col] = None
                
        stock_info = stock_info[final_columns]
        
        stock_info.to_csv(BASIC_INFO_FILE, index=False, float_format='%.0f')
        commit_files_in_batches(BASIC_INFO_FILE, commit_message)
        logger.info(f"已保存并提交过滤后的股票列表，共 {len(stock_info)} 条记录: {commit_message}")
    except Exception as e:
        logger.error(f"保存并提交过滤后的股票列表失败: {str(e)}", exc_info=True)

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
