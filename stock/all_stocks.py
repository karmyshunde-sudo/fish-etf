#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票列表更新模块 - 严格单API实现

【详细过滤条件】
1. 基础过滤：
   - 移除ST和*ST股票
   - 移除名称以"N"开头的新上市股票
   - 移除名称包含"退市"的股票
   - 市盈率(动态)：排除亏损股票（PE_TTM ≤ 0）

注意：仅使用stock_zh_a_spot_em接口返回的列进行过滤，不存在的列不过滤
"""

import os
import logging
import pandas as pd
import akshare as ak
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
        logger.info(f"前500条数据中包含的列: {', '.join(top_500.columns)}")
        logger.info(f"前500条数据中ST股票数量: {top_500['名称'].str.contains('ST', na=False).sum()}")
        logger.info(f"前500条数据中退市股票数量: {top_500['名称'].str.contains('退市', na=False).sum()}")
        logger.info(f"前500条数据中N开头股票数量: {top_500['名称'].str.startswith('N').sum()}")
    except Exception as e:
        logger.error(f"保存前500条股票数据失败: {str(e)}", exc_info=True)

def get_stock_list_data():
    """
    获取股票列表数据（使用stock_zh_a_spot_em接口）
    
    Returns:
        pd.DataFrame: 股票列表数据
    """
    for retry in range(MAX_RETRIES):
        try:
            # 【关键修复】大幅增加随机延时（20.0-30.0秒）- 避免被封
            delay = random.uniform(20.0, 30.0)
            logger.info(f"获取股票列表前等待 {delay:.2f} 秒（尝试 {retry+1}/{MAX_RETRIES}）...")
            time.sleep(delay)
            
            logger.info("正在获取股票列表数据...")
            
            # 【关键修复】使用stock_zh_a_spot_em接口获取数据
            stock_data = ak.stock_zh_a_spot_em()
            
            # 【关键修复】添加严格的返回值检查
            if stock_data is None:
                logger.error("API返回None，可能是网络问题或数据源问题")
                if retry < MAX_RETRIES - 1:
                    extra_delay = retry * 10
                    total_delay = BASE_RETRY_DELAY + extra_delay
                    logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                    time.sleep(total_delay)
                    continue
                return pd.DataFrame()
            
            if stock_data.empty:
                logger.error("获取股票列表数据失败：返回空数据")
                if retry < MAX_RETRIES - 1:
                    extra_delay = retry * 10
                    total_delay = BASE_RETRY_DELAY + extra_delay
                    logger.warning(f"将在 {total_delay:.1f} 秒后重试 ({retry+1}/{MAX_RETRIES})")
                    time.sleep(total_delay)
                    continue
                return pd.DataFrame()
            
            # 【关键修复】确保列名与期望一致
            expected_columns = ["序号", "代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", 
                              "振幅", "最高", "最低", "今开", "昨收", "量比", "换手率", "市盈率-动态", 
                              "市净率", "总市值", "流通市值", "涨速", "5分钟涨跌", "60日涨跌幅", "年初至今涨跌幅"]
            
            # 检查是否存在关键列
            for col in ["代码", "名称", "流通市值", "市盈率-动态"]:
                if col not in stock_data.columns:
                    logger.error(f"接口返回数据缺少必要列: {col}")
                    return pd.DataFrame()
            
            # 【关键修复】确保股票代码格式统一为6位
            stock_data['代码'] = stock_data['代码'].apply(format_stock_code)
            
            # 【关键修复】转换数据类型（只转换必需的列）
            numeric_columns = ["流通市值", "市盈率-动态"]
            for col in numeric_columns:
                if col in stock_data.columns:
                    stock_data[col] = pd.to_numeric(stock_data[col], errors='coerce')
            
            logger.info(f"成功获取 {len(stock_data)} 条股票列表数据")
            
            # 【关键修复】保存前500条数据用于验证
            save_top_500_stock_data(stock_data)
            
            return stock_data
        
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

def apply_basic_filters(stock_data):
    """
    应用基础过滤条件（仅使用stock_zh_a_spot_em接口返回的列）
    
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
    
    # 4. 市盈率(动态)：排除亏损股票（PE_TTM ≤ 0）
    if "市盈率-动态" in stock_info.columns:
        before = len(stock_info)
        # 注意：市盈率-动态为0或NaN可能表示数据缺失，这里只排除明确小于0的
        stock_info = stock_info[(stock_info["市盈率-动态"] > 0) | (stock_info["市盈率-动态"].isna())]
        removed = before - len(stock_info)
        if removed > 0:
            logger.info(f"排除 {removed} 只市盈率(动态) ≤ 0 的股票（基础过滤）")
    
        
    # 【关键修复】确保股票代码唯一 - 移除重复项
    stock_info = stock_info.drop_duplicates(subset=['代码'], keep='first')
    
    # 【关键修复】记录基础过滤后股票数量
    logger.info(f"基础过滤完成，剩余 {len(stock_info)} 条记录（初始: {initial_count}）")
    
    return stock_info

def save_base_stock_info(stock_info):
    """
    【关键修复】保存基础股票列表到文件
    确保文件结构: 代码,名称,所属板块,流通市值,总市值,数据状态,动态市盈率,filter,next_crawl_index
    
    Args:
        stock_info: 基础股票列表DataFrame
    """
    try:
        # 【关键修复】确保列名正确
        # 重命名"总市值"列（如果存在）
        if "总市值" in stock_info.columns:
            stock_info = stock_info.rename(columns={"总市值": "总市值"})
        else:
            stock_info["总市值"] = 0.0
        
        # 【关键修复】添加动态市盈率列
        if "市盈率-动态" in stock_info.columns:
            stock_info["动态市盈率"] = stock_info["市盈率-动态"]
        else:
            stock_info["动态市盈率"] = None
        
        # 【关键修复】添加必需列
        stock_info["所属板块"] = stock_info["代码"].apply(get_stock_section)
        
        # 确保流通市值和总市值是数值类型
        if "流通市值" in stock_info.columns:
            stock_info["流通市值"] = pd.to_numeric(stock_info["流通市值"], errors='coerce')
        else:
            stock_info["流通市值"] = 0.0
            
        if "总市值" in stock_info.columns:
            stock_info["总市值"] = pd.to_numeric(stock_info["总市值"], errors='coerce')
        else:
            stock_info["总市值"] = 0.0
        
        stock_info["数据状态"] = "基础数据已获取"
        stock_info["next_crawl_index"] = 0
        
        # 【新增】添加filter列，设置默认值为False
        stock_info["filter"] = False
        
        # 【关键修复】确保列顺序正确，按要求添加"动态市盈率"和"filter"
        final_columns = ["代码", "名称", "所属板块", "流通市值", "总市值", "数据状态", "动态市盈率", "filter", "next_crawl_index"]
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
