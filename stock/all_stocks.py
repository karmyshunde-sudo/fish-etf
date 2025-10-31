#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票列表更新模块 - Baostock 数据源（已修正列名问题）

【详细过滤条件】
1. 基础过滤：
   - 移除ST和*ST股票
   - 移除名称以"N"开头的新上市股票
   - 移除名称包含"退市"的股票

注意：不再包含市盈率过滤，因为新CSV结构已移除该字段
"""

import os
import logging
import pandas as pd
import baostock as bs
import time
import random
import traceback
from datetime import datetime
from config import Config
from utils.date_utils import get_beijing_time
from utils.git_utils import commit_files_in_batches

# 配置日志
logger = logging.getLogger(__name__)

# 数据目录配置
DATA_DIR = Config.DATA_DIR
DAILY_DIR = os.path.join(DATA_DIR, "daily")
STOCK_DIR = os.path.join(DATA_DIR, "stock")
BASIC_INFO_FILE = os.path.join(DATA_DIR, "all_stocks.csv")
TOP_500_FILE = os.path.join(STOCK_DIR, "top500stock.csv")
LOG_DIR = os.path.join(DATA_DIR, "logs")

# 确保目录存在
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(STOCK_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# 专业级重试配置
MAX_RETRIES = 3  # 增加重试次数
BASE_RETRY_DELAY = 2  # 基础重试延迟（秒）
MAX_RANDOM_DELAY = 8  # 最大随机延时（秒）

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

def save_top_500_stock_data(stock_data):
    """
    保存前500条股票数据用于验证
    Args:
        stock_data: 股票数据DataFrame
    """
    try:
        # 【关键修复】确保只保存前500条数据
        top_500 = stock_data.head(500).copy()
        
        # 【关键修复】保存为CSV文件
        top_500.to_csv(TOP_500_FILE, index=False)
        
        # 【关键修复】提交到Git仓库
        commit_files_in_batches(TOP_500_FILE, "保存前500条股票数据用于验证")
        
        logger.info(f"已成功保存前500条股票数据到 {TOP_500_FILE}")
        
        # 【关键修复】检查"名称"列是否存在
        if "名称" in top_500.columns:
            logger.info(f"前500条数据中包含的列: {', '.join(top_500.columns)}")
            logger.info(f"前500条数据中ST股票数量: {top_500['名称'].str.contains('ST', na=False).sum()}")
            logger.info(f"前500条数据中退市股票数量: {top_500['名称'].str.contains('退市', na=False).sum()}")
            logger.info(f"前500条数据中N开头股票数量: {top_500['名称'].str.startswith('N').sum()}")
        else:
            logger.warning("列'名称'不存在，无法进行ST/退市/N开头股票统计")
            logger.info(f"实际可用列: {', '.join(top_500.columns)}")
    except Exception as e:
        logger.error(f"保存前500条股票数据失败: {str(e)}", exc_info=True)

def get_stock_list_data():
    """
    获取股票列表数据（使用baostock接口）
    
    Returns:
        pd.DataFrame: 股票列表数据
    """
    # 登录Baostock
    login_result = bs.login()
    if login_result.error_code != '0':
        logger.error(f"Baostock登录失败: {login_result.error_msg}")
        return pd.DataFrame()
    
    try:
        for retry in range(MAX_RETRIES):
            try:
                # 【关键修复】大幅增加随机延时（2.0-8.0秒）- 避免被封
                delay = random.uniform(2.0, 8.0)
                logger.info(f"获取股票列表前等待 {delay:.2f} 秒（尝试 {retry+1}/{MAX_RETRIES}）...")
                time.sleep(delay)
                
                logger.info("正在获取股票列表数据...")
                
                # 【关键修复】使用query_stock_basic接口获取数据
                rs = bs.query_stock_basic()
                
                # 检查返回结果
                if rs.error_code != '0':
                    logger.error(f"API返回错误: {rs.error_msg}")
                    if retry < MAX_RETRIES - 1:
                        extra_delay = retry * 10
                        total_delay = BASE_RETRY_DELAY + extra_delay
                        logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                        time.sleep(total_delay)
                        continue
                    return pd.DataFrame()
                
                # 收集数据
                data_list = []
                while rs.next():
                    data_list.append(rs.get_row_data())
                
                if not data_list:
                    logger.error("获取股票列表数据失败：返回空数据")
                    if retry < MAX_RETRIES - 1:
                        extra_delay = retry * 10
                        total_delay = BASE_RETRY_DELAY + extra_delay
                        logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                        time.sleep(total_delay)
                        continue
                    return pd.DataFrame()
                
                # 转换为DataFrame
                df = pd.DataFrame(data_list, columns=rs.fields)
                
                # 【关键修复】打印实际返回的字段，用于调试
                logger.info(f"Baostock query_stock_basic 返回的字段: {', '.join(rs.fields)}")
                
                # 【关键修复】重命名列名
                # Baostock返回的列名与代码期望的列名不同
                column_mapping = {
                    'code': '代码',
                    'code_name': '名称',
                    'ipoDate': '上市日期',
                    'outDate': '退市日期',
                    'type': '证券类型',
                    'status': '上市状态'
                }
                
                # 仅保留存在的列
                existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
                df = df.rename(columns=existing_columns)
                
                # 确保有"名称"列
                if "名称" not in df.columns:
                    logger.error("返回数据中缺少'名称'列，无法继续处理")
                    return pd.DataFrame()
                
                # 【关键修复】确保股票代码格式统一为6位
                # 处理可能的格式: sh.600000, sz.000001
                if "代码" in df.columns:
                    df['代码'] = df['代码'].apply(lambda x: x[3:] if x.startswith(('sh.', 'sz.')) else x)
                    df['代码'] = df['代码'].apply(format_stock_code)
                    df = df[df['代码'].notna()]
                
                # 【关键修复】添加所属板块列
                if "代码" in df.columns:
                    df['所属板块'] = df['代码'].apply(get_stock_section)
                else:
                    df['所属板块'] = "未知板块"
                    logger.warning("代码列不存在，所属板块列已设为'未知板块'")
                
                # 【关键修复】添加流通股本和总股本（Baostock不直接提供）
                df['流通股本'] = 0.0
                df['总股本'] = 0.0
                logger.warning("Baostock不提供流通股本和总股本数据，已设为0.0")
                
                # 【关键修复】确保有必要的列
                required_columns = ["代码", "名称", "所属板块", "流通股本", "总股本", "上市状态"]
                for col in required_columns:
                    if col not in df.columns:
                        if col in ["流通股本", "总股本"]:
                            df[col] = 0.0
                        else:
                            df[col] = ""
                        logger.warning(f"列 '{col}' 不存在，已添加默认值")
                
                logger.info(f"成功获取 {len(df)} 条股票列表数据")
                
                # 【关键修复】保存前500条数据用于验证
                save_top_500_stock_data(df)
                
                return df
            
            except Exception as e:
                logger.error(f"获取股票列表数据失败 (尝试 {retry+1}/{MAX_RETRIES}): {str(e)}", exc_info=True)
                logger.error(f"异常堆栈: {traceback.format_exc()}")
                if retry < MAX_RETRIES - 1:
                    extra_delay = retry * 10
                    total_delay = BASE_RETRY_DELAY + extra_delay
                    logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                    time.sleep(total_delay)
    
        logger.error("获取股票列表数据失败，已达到最大重试次数")
        return pd.DataFrame()
    
    finally:
        # 确保登出
        bs.logout()

def apply_basic_filters(stock_data):
    """
    应用基础过滤条件
    
    Args:
        stock_data: 股票列表DataFrame
    
    Returns:
        pd.DataFrame: 应用基础过滤后的股票数据
    """
    # 创建副本，避免修改原始数据
    stock_info = stock_data.copy()
    
    # 【关键修复】记录初始股票数量
    initial_count = len(stock_info)
    logger.info(f"开始应用基础过滤，初始股票数量: {initial_count}")
    
    # 【关键修复】检查是否包含必要列
    if "名称" not in stock_info.columns:
        logger.error("数据中缺少'名称'列，无法应用过滤条件")
        return stock_info
    
    # 【关键修复】应用基础过滤条件
    # 1. 移除ST和*ST股票
    before = len(stock_info)
    stock_info = stock_info[~stock_info["名称"].str.contains("ST", na=False, regex=False)]
    removed = before - len(stock_info)
    if removed > 0:
        logger.info(f"排除 {removed} 只ST股票（基础过滤）")
    
    # 2. 移除名称以"N"开头的新上市股票
    before = len(stock_info)
    stock_info = stock_info[~stock_info["名称"].str.startswith("N")]
    removed = before - len(stock_info)
    if removed > 0:
        logger.info(f"排除 {removed} 只新上市股票（基础过滤）")
    
    # 3. 移除名称包含"退市"的股票
    before = len(stock_info)
    stock_info = stock_info[~stock_info["名称"].str.contains("退市", na=False, regex=False)]
    removed = before - len(stock_info)
    if removed > 0:
        logger.info(f"排除 {removed} 只退市股票（基础过滤）")
    
    # 4. 移除已退市股票
    if "上市状态" in stock_info.columns:
        before = len(stock_info)
        stock_info = stock_info[stock_info["上市状态"] == "1"]
        removed = before - len(stock_info)
        if removed > 0:
            logger.info(f"排除 {removed} 只已退市股票（基础过滤）")
    
    # 【关键修复】确保股票代码唯一 - 移除重复项
    if "代码" in stock_info.columns:
        stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
    
    # 【关键修复】记录基础过滤后股票数量
    logger.info(f"基础过滤完成，剩余 {len(stock_info)} 条记录（初始: {initial_count}）")
    
    return stock_info

def save_base_stock_info(stock_info):
    """
    【关键修复】保存基础股票列表到文件
    确保文件结构: 代码,名称,所属板块,流通股本,总股本,数据状态,filter,next_crawl_index
    
    Args:
        stock_info: 基础股票列表DataFrame
    """
    try:
        # 【关键修复】确保列名正确
        # 确保流通股本和总股本是数值类型
        if "流通股本" in stock_info.columns:
            stock_info["流通股本"] = pd.to_numeric(stock_info["流通股本"], errors='coerce')
        else:
            stock_info["流通股本"] = 0.0
            logger.warning("流通股本列不存在，已添加默认值0.0")
            
        if "总股本" in stock_info.columns:
            stock_info["总股本"] = pd.to_numeric(stock_info["总股本"], errors='coerce')
        else:
            stock_info["总股本"] = 0.0
            logger.warning("总股本列不存在，已添加默认值0.0")
        
        # 【关键修复】添加必需列
        stock_info["数据状态"] = "基础数据已获取"
        stock_info["filter"] = False  # 添加filter列并设置默认值为False
        stock_info["next_crawl_index"] = 0
        
        # 【关键修复】确保列顺序正确
        final_columns = ["代码", "名称", "所属板块", "流通股本", "总股本", "数据状态", "filter", "next_crawl_index"]
        
        # 检查并添加缺失的列
        for col in final_columns:
            if col not in stock_info.columns:
                if col == "filter":
                    stock_info[col] = False
                    logger.warning(f"列 {col} 不存在，已添加默认值 False")
                elif col == "next_crawl_index":
                    stock_info[col] = 0
                    logger.warning(f"列 {col} 不存在，已添加默认值 0")
                else:
                    stock_info[col] = ""
                    logger.warning(f"列 {col} 不存在，已添加默认值空字符串")
        
        # 选择正确的列并排序
        stock_info = stock_info[final_columns]
        
        # 保存到CSV文件
        stock_info.to_csv(BASIC_INFO_FILE, index=False, float_format='%.2f')
        
        # 提交到Git仓库
        commit_files_in_batches(BASIC_INFO_FILE, "更新股票列表（基础过滤后）")
        
        logger.info(f"基础股票列表已成功更新，共 {len(stock_info)} 条记录")
    except Exception as e:
        logger.error(f"保存基础股票列表失败: {str(e)}", exc_info=True)

def update_stock_list():
    """
    更新股票列表，保存到all_stocks.csv
    """
    try:
        logger.info("开始更新股票列表...")
        
        # 【关键修复】直接获取股票列表数据
        stock_data = get_stock_list_data()
        
        if stock_data.empty:
            logger.error("获取的股票列表数据为空")
            return False
        
        # 【关键修复】应用基础过滤条件
        filtered_data = apply_basic_filters(stock_data)
        
        if filtered_data.empty:
            logger.error("基础过滤后股票列表为空")
            return False
        
        # 【关键修复】保存基础股票列表
        save_base_stock_info(filtered_data)
        
        logger.info(f"股票列表已成功更新")
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
