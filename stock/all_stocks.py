#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票列表更新模块 - Baostock 数据源（严格单接口实现）

【详细过滤条件】
1. 基础过滤：
   - 移除ST和*ST股票
   - 移除名称以"N"开头的新上市股票
   - 移除名称包含"退市"的股票
   - 移除指数股票（在数据获取阶段完成）
2. 质押数据过滤：
   - 移除质押股数超过阈值的股票
   - 增加净利润，并删掉为负的股票(移到filterstock.py当中）

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
from utils.new_git import commit_single_file, commit_batch_files, commit_remaining_files
import akshare as ak  # 新增：用于获取质押数据

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

# 质押过滤参数配置
PLEDGE_FILTER = {
    "enabled": True,
    "threshold": 100,  # 默认为一百（万股—），表示移除所有质押股数>100万的股票
    "column": "质押股数",
    "condition": "<= {threshold}（排除质押股数超过阈值的股票）"
}

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

def is_index_stock(row):
    """
    检查股票是否为指数
    Args:
        row: 股票数据行
    Returns:
        bool: 是否是指数
    """
    # 方法1: 检查名称是否包含指数相关关键词
    index_keywords = ["指数", "ETF", "LOF", "基金", "债券", "国债", "信用债", "可转债", "期货", "期权", "理财", "票据"]
    if "名称" in row and any(keyword in str(row["名称"]) for keyword in index_keywords):
        return True
    
    # 方法2: 检查股票类型
    if "证券类型" in row:
        # 只保留普通股（A股），其他类型（如指数、基金等）都过滤掉
        # 根据Baostock文档，普通股的type为1
        return str(row["证券类型"]) != "1"
    
    # 方法3: 检查市场代码
    if "代码" in row:
        code = str(row["代码"])
        # 排除以000开头的指数代码（除了000001上证指数外，其他000开头的代码可能是股票）
        if code.startswith("000") and code != "000001":
            # 000开头且不是000001的可能是指数
            return True
    
    return False

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
        
        # 【关键修复】提交到Git仓库 - 使用新的Git工具
        commit_message = f"保存前500条股票数据用于验证 - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        commit_success = commit_single_file(TOP_500_FILE, commit_message)
        
        if commit_success:
            logger.info(f"已成功保存前500条股票数据到 {TOP_500_FILE}")
        else:
            logger.error(f"前500条股票数据提交失败")
        
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
                
                # 【关键修改】打印实际返回的字段，用于调试
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
                
                # 【关键修改】确保股票代码格式统一为6位
                if "代码" in df.columns:
                    # 从 Baostock 格式转换为纯数字代码
                    df['代码'] = df['代码'].apply(lambda x: x[3:] if x.startswith(('sh.', 'sz.')) else x)
                    df['代码'] = df['代码'].apply(format_stock_code)
                    df = df[df['代码'].notna()]
                
                # 【关键修改】添加所属板块列
                if "代码" in df.columns:
                    df['所属板块'] = df['代码'].apply(get_stock_section)
                else:
                    df['所属板块'] = "未知板块"
                    logger.warning("代码列不存在，所属板块列已设为'未知板块'")
                
                # 【关键修改】获取流通市值数据并合并
                market_value_data = get_market_value_data()
                if not market_value_data.empty and "代码" in df.columns:
                    # 从Baostock格式转换为标准6位代码
                    df['代码'] = df['代码'].apply(lambda x: str(x).zfill(6))
                    # 合并流通市值和总市值数据
                    df = pd.merge(df, market_value_data, on='代码', how='left')
                    
                    # 填充缺失值
                    df['流通市值'] = df['流通市值'].fillna(0.0)
                    df['总市值'] = df['总市值'].fillna(0.0)
                    
                    logger.info("已成功合并流通市值数据")
                else:
                    # 如果获取失败，保留原有列但不设为0.0（让后续代码处理）
                    if '流通市值' not in df.columns:
                        df['流通市值'] = 0.0
                        logger.warning("流通市值列不存在，已添加默认值0.0")
                    if '总市值' not in df.columns:
                        df['总市值'] = 0.0
                        logger.warning("总市值列不存在，已添加默认值0.0")
                
                # 【关键修改】添加缺失的列（根据要求设为0或默认值）
                # 注意：这些列在Baostock接口中不存在，根据要求设为0
                df['动态市盈率'] = 0.0
                
                # 【关键修改】确保有必要的列
                required_columns = ["代码", "名称", "所属板块", "流通市值", "总市值", "上市状态", "动态市盈率"]
                for col in required_columns:
                    if col not in df.columns:
                        if col in ["流通市值", "总市值", "动态市盈率"]:
                            df[col] = 0.0
                            logger.warning(f"列 '{col}' 不存在，已添加默认值 0.0")
                        else:
                            df[col] = ""
                            logger.warning(f"列 '{col}' 不存在，已添加默认值空字符串")
                
                # 【关键修改】移除所有指数股票（在基础过滤前）
                original_count = len(df)
                df = df[~df.apply(is_index_stock, axis=1)]
                logger.info(f"指数过滤：从 {original_count} 条中移除了 {original_count - len(df)} 条指数/ETF/基金/债券股票")
                
                logger.info(f"成功获取 {len(df)} 条股票列表数据（已过滤指数）")
                
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
    # 确保名称列是字符串类型并去除前导/尾随空格
    stock_info["名称"] = stock_info["名称"].astype(str).str.strip()
    # 现在再检查是否以"N"开头
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

def get_pledge_data():
    """
    获取股票质押数据
    Returns:
        pd.DataFrame: 包含质押数据的DataFrame
    """
    try:
        logger.info("正在获取股票质押数据...")
        df = ak.stock_gpzy_pledge_ratio_em()
        
        if df.empty:
            logger.error("获取股票质押数据失败：返回空数据")
            return pd.DataFrame()
        
        # 打印实际返回的列名
        logger.info(f"质押数据实际列名: {', '.join(df.columns)}")
        
        # 确保列名正确
        required_columns = ['股票代码', '质押股数', '无限售股质押数']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"质押数据缺少必要列: {col}")
                return pd.DataFrame()
        
        # 确保股票代码格式正确
        df['股票代码'] = df['股票代码'].apply(lambda x: str(x).zfill(6))
        
        # 筛选有效数据
        df = df[df['股票代码'].apply(lambda x: len(x) == 6)]
        
        # 重命名列，确保与主数据匹配
        df = df.rename(columns={
            '股票代码': '代码',
            '质押股数': '质押股数',
            '无限售股质押数': '无限售股质押数'
        })
        
        # 选择需要的列
        df = df[['代码', '质押股数', '无限售股质押数']]
        
        # 填充缺失值
        df['质押股数'] = df['质押股数'].fillna(0)
        df['无限售股质押数'] = df['无限售股质押数'].fillna(0)
        
        logger.info(f"成功获取 {len(df)} 条股票质押数据")
        return df
    
    except Exception as e:
        logger.error(f"获取股票质押数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_market_value_data():
    """
    获取股票流通市值和总市值数据
    Returns:
        pd.DataFrame: 包含代码、流通市值、总市值的DataFrame
    """
    try:
        logger.info("正在获取流通市值数据...")
        # 使用akshare获取股票实时行情数据
        df = ak.stock_zh_a_spot_em()
        
        if df.empty:
            logger.error("获取流通市值数据失败：返回空数据")
            return pd.DataFrame()
        
        # 重命名列以匹配系统需求
        df = df.rename(columns={
            '代码': '代码',
            '流通市值': '流通市值',
            '总市值': '总市值',
            'market_cap': '总市值',  # 新增：添加英文列名映射
            'total_market_value': '总市值',  # 新增：添加英文列名映射
            'total_value': '总市值'  # 新增：添加英文列名映射
        })
        
        # 选择需要的列
        df = df[['代码', '流通市值', '总市值']]
        
        # 确保股票代码格式正确
        df['代码'] = df['代码'].apply(lambda x: str(x).zfill(6))
        
        logger.info(f"成功获取 {len(df)} 条流通市值数据")
        return df
    except Exception as e:
        logger.error(f"获取流通市值数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def apply_pledge_filter(stock_data):
    """
    应用质押数据过滤条件
    
    Args:
        stock_data: 基础股票列表DataFrame
    
    Returns:
        pd.DataFrame: 应用质押过滤后的股票数据
    """
    # 获取质押数据
    pledge_data = get_pledge_data()
    if pledge_data.empty:
        logger.warning("质押数据获取失败，跳过质押过滤")
        return stock_data
    
    # 创建副本避免SettingWithCopyWarning
    stock_info = stock_data.copy()
    
    # 确保流通市值列存在
    if '流通市值' not in stock_info.columns:
        stock_info['流通市值'] = 0.0
        logger.warning("流通市值列不存在，添加默认值0.0")
    
    # 确保总市值列存在
    if '总市值' not in stock_info.columns:
        stock_info['总市值'] = 0.0
        logger.warning("总市值列不存在，添加默认值0.0")
    
    # 仅在有质押数据时添加质押股数列
    if '质押股数' not in stock_info.columns:
        # 添加质押股数列，初始值为0
        stock_info['质押股数'] = 0
    
    # 仅在有无限售股质押数据时添加无限售股质押数列
    if '无限售股质押数' not in stock_info.columns:
        # 添加无限售股质押数列，初始值为0
        stock_info['无限售股质押数'] = 0
    
    # 合并质押数据
    merged_data = pd.merge(stock_info, pledge_data, on='代码', how='left', suffixes=('', '_new'))
    
    # 更新质押股数列
    if '质押股数_new' in merged_data.columns:
        # 用新数据替换旧数据
        merged_data['质押股数'] = merged_data['质押股数_new'].fillna(0)
        # 移除临时列
        merged_data = merged_data.drop(columns=['质押股数_new'])
    else:
        # 如果新数据中没有质押股数列，保持原值
        logger.warning("质押数据中没有'质押股数'列，使用默认值0")
        merged_data['质押股数'] = 0
    
    # 更新无限售股质押数列
    if '无限售股质押数_new' in merged_data.columns:
        # 用新数据替换旧数据
        merged_data['无限售股质押数'] = merged_data['无限售股质押数_new'].fillna(0)
        # 移除临时列
        merged_data = merged_data.drop(columns=['无限售股质押数_new'])
    else:
        # 如果新数据中没有无限售股质押数列，保持原值
        logger.warning("质押数据中没有'无限售股质押数'列，使用默认值0")
        merged_data['无限售股质押数'] = 0
    
    # 记录过滤前的股票数量
    initial_count = len(merged_data)
    logger.info(f"开始应用质押过滤，初始股票数量: {initial_count}")
    
    # 添加详细的质押数据统计
    logger.info(f"质押数据统计: 最小值={merged_data['质押股数'].min()}, 最大值={merged_data['质押股数'].max()}, 平均值={merged_data['质押股数'].mean():.2f}")
    logger.info(f"无限售股质押数统计: 最小值={merged_data['无限售股质押数'].min()}, 最大值={merged_data['无限售股质押数'].max()}, 平均值={merged_data['无限售股质押数'].mean():.2f}")
    
    # 按质押股数排序，打印最大的前两行数据
    top_pledge = merged_data.sort_values('质押股数', ascending=False).head(2)
    logger.info("质押股数最大的前两行数据:")
    for i, row in top_pledge.iterrows():
        logger.info(f"{row['代码']}: {row['名称']} - 质押股数: {row['质押股数']}")
    
    # 按无限售股质押数排序，打印最大的前两行数据
    top_unrestricted = merged_data.sort_values('无限售股质押数', ascending=False).head(2)
    logger.info("无限售股质押数最大的前两行数据:")
    for i, row in top_unrestricted.iterrows():
        logger.info(f"{row['代码']}: {row['名称']} - 无限售股质押数: {row['无限售股质押数']}")
    
    # 应用质押过滤条件
    if PLEDGE_FILTER["enabled"]:
        threshold = PLEDGE_FILTER["threshold"]
        before = len(merged_data)
        # 记录被过滤的股票代码
        filtered_stocks = merged_data[merged_data['质押股数'] > threshold]
        filtered_codes = filtered_stocks['代码'].head(50).tolist()
        
        # 应用过滤
        merged_data = merged_data[merged_data['质押股数'] <= threshold]
        removed = before - len(merged_data)
        
        if removed > 0:
            logger.info(f"排除 {removed} 只质押股数超过阈值({threshold})的股票（质押过滤）")
            # 记录前50个被过滤的股票代码
            if filtered_codes:
                logger.info(f"前50个被过滤的股票代码: {', '.join(filtered_codes)}")
        else:
            logger.info(f"所有股票质押股数均未超过阈值({threshold})")
    
    # 记录质押过滤后股票数量
    logger.info(f"质押过滤完成，剩余 {len(merged_data)} 条记录（初始: {initial_count}）")
    
    return merged_data

def save_base_stock_info(stock_info, include_pledge=False):
    """
    保存基础股票列表到文件
    确保文件结构: 代码,名称,所属板块,流通市值,总市值,数据状态,动态市盈率,filter,next_crawl_index[,质押股数]
    
    Args:
        stock_info: 基础股票列表DataFrame
        include_pledge: 是否包含质押股数列
    """
    try:
        # 创建副本避免SettingWithCopyWarning
        stock_info = stock_info.copy()
        
        # 【关键修复】确保列名正确
        # 确保流通市值和总市值是数值类型
        if "流通市值" in stock_info.columns:
            stock_info["流通市值"] = pd.to_numeric(stock_info["流通市值"], errors='coerce')
        else:
            stock_info["流通市值"] = 0.0
            logger.warning("流通市值列不存在，已添加默认值0.0")
            
        if "总市值" in stock_info.columns:
            stock_info["总市值"] = pd.to_numeric(stock_info["总市值"], errors='coerce')
        else:
            stock_info["总市值"] = 0.0
            logger.warning("总市值列不存在，已添加默认值0.0")
        
        if "动态市盈率" in stock_info.columns:
            stock_info["动态市盈率"] = pd.to_numeric(stock_info["动态市盈率"], errors='coerce')
        else:
            stock_info["动态市盈率"] = 0.0
            logger.warning("动态市盈率列不存在，已添加默认值0.0")
        
        # 【关键修复】添加必需列
        stock_info["数据状态"] = "基础数据已获取"
        stock_info["filter"] = False  # 添加filter列并设置默认值为False
        stock_info["next_crawl_index"] = 0
        
        # 定义基础列（不包含质押股数）
        basic_columns = ["代码", "名称", "所属板块", "流通市值", "总市值", "数据状态", "动态市盈率", "filter", "next_crawl_index"]
        
        # 如果需要包含质押股数列
        if include_pledge:
            # 确保"质押股数"列存在
            if '质押股数' not in stock_info.columns:
                logger.warning("质押股数列不存在，添加默认值0")
                stock_info['质押股数'] = 0
            else:
                # 确保"质押股数"是数值类型
                stock_info['质押股数'] = pd.to_numeric(stock_info['质押股数'], errors='coerce').fillna(0)
            
            # 定义完整列
            final_columns = basic_columns + ["质押股数"]
        else:
            # 只使用基础列
            final_columns = basic_columns
        
        # 检查并添加缺失的列
        for col in final_columns:
            if col not in stock_info.columns:
                if col == "filter":
                    stock_info[col] = False
                    logger.warning(f"列 {col} 不存在，已添加默认值 False")
                elif col == "next_crawl_index":
                    stock_info[col] = 0
                    logger.warning(f"列 {col} 不存在，已添加默认值 0")
                elif col == "质押股数":
                    stock_info[col] = 0
                    logger.warning(f"列 {col} 不存在，已添加默认值 0")
                elif col in ["流通市值", "总市值", "动态市盈率"]:
                    stock_info[col] = 0.0
                    logger.warning(f"列 {col} 不存在，已添加默认值 0.0")
                else:
                    stock_info[col] = ""
                    logger.warning(f"列 {col} 不存在，已添加默认值空字符串")
        
        # 选择正确的列并排序
        stock_info = stock_info[final_columns]
        
        # 保存到CSV文件
        stock_info.to_csv(BASIC_INFO_FILE, index=False, float_format='%.2f')
        
        # 提交到Git仓库 - 使用新的Git工具
        commit_message = f"更新股票列表（基础过滤后） - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        commit_success = commit_single_file(BASIC_INFO_FILE, commit_message)
        
        if commit_success:
            logger.info(f"基础股票列表已成功更新，共 {len(stock_info)} 条记录")
        else:
            logger.error("基础股票列表提交失败")
            
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
        
        # 【关键修复】初次保存时，不包含质押股数列
        save_base_stock_info(filtered_data, include_pledge=False)
        
        # 【新增】应用质押数据过滤
        logger.info("开始应用质押数据过滤...")
        pledge_filtered_data = apply_pledge_filter(filtered_data)
        
        # 【修复】保存过滤后的股票列表
        if not pledge_filtered_data.empty:
            # 直接保存质押过滤后的数据（已经包含质押信息）
            save_base_stock_info(pledge_filtered_data, include_pledge=True)
            logger.info(f"股票列表已成功应用质押过滤并更新")
        else:
            logger.warning("质押过滤后无股票数据，跳过保存")
            return False

        logger.info("股票列表更新流程全部完成 ✅")
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
