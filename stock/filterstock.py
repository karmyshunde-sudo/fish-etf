#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票列表财务过滤器
功能：
1. 读取all_stocks.csv文件
2. 逐个股票获取实时行情数据（仅更新【流通市值、总市值、动态市盈率】三个字段）
3. 应用财务条件过滤
4. 将过滤后的股票列表保存回all_stocks.csv

财务过滤条件：
- 动态市盈率 >= 参数值
- 流通市值 / 总市值 > 参数值

使用说明：
1. 该脚本应在每周固定时间运行（例如周末）
2. 运行前确保已安装必要依赖：pip install baostock pandas akshare
3. 脚本会更新all_stocks.csv文件
"""

import os
import pandas as pd
import baostock as bs
import time
import logging
import sys
from datetime import datetime
from config import Config
from utils.date_utils import get_beijing_time
from utils.git_utils import commit_files_in_batches
import akshare as ak  # 新增：用于获取实时行情数据

# 配置日志
logger = logging.getLogger(__name__)

# 添加BATCH_SIZE参数，方便灵活调整每次处理的股票数量
BATCH_SIZE = 100  # 每次处理的股票数量

# 🚫 删除所有财务指标配置，只保留两个参数
FINANCIAL_FILTER_PARAMS = {
    "dynamic_pe_ratio": {
        "enabled": True,
        "threshold": 0.0,  # 动态市盈率阈值
        "column": "动态市盈率",
        "condition": ">= {threshold}（排除动态市盈率低于阈值的股票）"
    },
    "circulation_market_cap_ratio": {
        "enabled": True,
        "threshold": 0.9,  # 流通市值/总市值比值阈值
        "column": "流通市值/总市值",
        "condition": "> {threshold}（排除流通市值/总市值比值低于阈值的股票）"
    }
}

def get_stock_quote(code):
    """
    使用 ak.stock_zh_a_daily 接口获取单只股票的最新行情数据
    参数：
    - code: 股票代码（6位字符串）
    返回：
    - dict: 包含流通市值、总市值、动态市盈率的字典
    - None: 获取失败
    """
    try:
        # 构造 akstock 的参数
        df = ak.stock_zh_a_daily(symbol=code, adjust="qfq")
        
        if df.empty:
            logger.warning(f"股票 {code} 行情数据为空")
            return None
        
        # 取最新一条数据
        latest_row = df.iloc[-1]
        
        # 提取需要的字段
        quote_data = {
            '总市值': latest_row.get('总市值', 0.0),
            '流通市值': latest_row.get('流通市值', 0.0),
            '动态市盈率': latest_row.get('市盈率-动态', 0.0)
        }
        
        # 转换为数值型
        for key in quote_data:
            try:
                quote_data[key] = float(quote_data[key])
            except (ValueError, TypeError):
                quote_data[key] = 0.0
        
        return quote_data
    
    except Exception as e:
        logger.error(f"获取股票 {code} 行情数据失败: {str(e)}")
        return None

def filter_and_update_stocks():
    """
    主函数：过滤股票并更新all_stocks.csv
    """
    # 获取all_stocks.csv文件路径
    basic_info_file = os.path.join(Config.DATA_DIR, "all_stocks.csv")
    
    if not os.path.exists(basic_info_file):
        logger.error("基础信息文件不存在")
        return
    
    try:
        # 读取所有股票
        basic_info_df = pd.read_csv(basic_info_file)
        logger.info(f"成功读取基础信息文件，共 {len(basic_info_df)} 只股票")
        
        # 确保有filter列，如果没有则添加
        if 'filter' not in basic_info_df.columns:
            basic_info_df['filter'] = False
            logger.info("添加filter列到all_stocks.csv文件")
        
        # 找出需要处理的股票（filter为False）
        to_process = basic_info_df[basic_info_df['filter'] == False]
        logger.info(f"需要处理的股票数量: {len(to_process)}")
        
        # 如果没有需要处理的股票，重置所有filter为False并退出
        if len(to_process) == 0:
            logger.info("所有股票都已处理，重置filter列")
            basic_info_df['filter'] = False
            basic_info_df.to_csv(basic_info_file, index=False)
            logger.info("filter列已重置，退出执行")
            return
        
        # 只处理前BATCH_SIZE只股票
        process_batch = to_process.head(BATCH_SIZE)
        logger.info(f"本次处理股票数量: {len(process_batch)}")

        # 🚫 删除原财务数据获取逻辑，改为逐只股票获取实时行情数据
        for _, row in process_batch.iterrows():
            code = row['代码']
            logger.info(f"正在处理股票 {code}...")

            try:
                # 获取单只股票的实时行情数据
                quote_data = get_stock_quote(code)
                if quote_data is None:
                    logger.warning(f"股票 {code} 实时行情数据为空")
                    continue

                # 更新 basic_info_df 中对应的三列
                basic_info_df.loc[basic_info_df['代码'] == code, '总市值'] = quote_data['总市值']
                basic_info_df.loc[basic_info_df['代码'] == code, '流通市值'] = quote_data['流通市值']
                basic_info_df.loc[basic_info_df['代码'] == code, '动态市盈率'] = quote_data['动态市盈率']

                logger.info(f"✅ 股票 {code} 实时行情数据更新成功")

            except Exception as e:
                logger.error(f"处理股票 {code} 实时行情数据时出错: {str(e)}")
                continue  # 跳过当前股票，继续下一个

            # 每处理完一只股票，暂停 0.5 秒，避免系统负载过高
            time.sleep(0.5)

        # 记录补充前状态
        initial_count = len(basic_info_df)
        logger.info(f"补充指标前股票数量: {initial_count}")

        # 应用新过滤条件
        # 条件1：动态市盈率 >= 0
        before_pe = len(basic_info_df)
        basic_info_df = basic_info_df.dropna(subset=['动态市盈率'])  # 先排除NaN
        basic_info_df = basic_info_df[basic_info_df['动态市盈率'] >= FINANCIAL_FILTER_PARAMS["dynamic_pe_ratio"]["threshold"]]
        removed_pe = before_pe - len(basic_info_df)
        logger.info(f"排除 {removed_pe} 只动态市盈率 < {FINANCIAL_FILTER_PARAMS['dynamic_pe_ratio']['threshold']} 的股票（PE过滤）")

        # 条件2：流通市值 / 总市值 > 90%
        before_ratio = len(basic_info_df)
        basic_info_df = basic_info_df.dropna(subset=['总市值', '流通市值'])
        basic_info_df = basic_info_df[basic_info_df['总市值'] > 0]
        basic_info_df['流通市值占比'] = basic_info_df['流通市值'] / basic_info_df['总市值']
        basic_info_df = basic_info_df[basic_info_df['流通市值占比'] > FINANCIAL_FILTER_PARAMS["circulation_market_cap_ratio"]["threshold"]]
        removed_ratio = before_ratio - len(basic_info_df)
        logger.info(f"排除 {removed_ratio} 只流通市值占比 <= {FINANCIAL_FILTER_PARAMS['circulation_market_cap_ratio']['threshold']} 的股票（市值结构过滤）")

        # 清理临时列
        if '流通市值占比' in basic_info_df.columns:
            basic_info_df = basic_info_df.drop(columns=['流通市值占比'])

        # 更新 filter 列：通过过滤的设置为 True
        basic_info_df['filter'] = True  # 所有通过过滤的股票标记为 True

        # 重新整理列顺序（确保与原结构一致）
        target_columns = [
            "代码", "名称", "所属板块", "流通市值", "总市值", "数据状态", 
            "动态市盈率", "filter", "next_crawl_index", "质押股数"
        ]
        # 补充缺失列（如果有的话）
        for col in target_columns:
            if col not in basic_info_df.columns:
                if col == "filter":
                    basic_info_df[col] = False
                elif col == "next_crawl_index":
                    basic_info_df[col] = 0
                elif col in ["流通市值", "总市值", "动态市盈率"]:
                    basic_info_df[col] = 0.0
                elif col == "质押股数":
                    basic_info_df[col] = 0
                else:
                    basic_info_df[col] = ""

        # 选择目标列并排序
        basic_info_df = basic_info_df[target_columns]

        # 保存最终结果
        basic_info_df.to_csv(basic_info_file, index=False, float_format='%.2f')
        commit_files_in_batches(basic_info_file, "更新股票列表（补充流通市值/总市值/动态市盈率并过滤）")
        logger.info(f"✅ 股票列表已成功补充财务指标并完成最终过滤，共 {len(basic_info_df)} 条记录")

    except Exception as e:
        logger.error(f"处理股票列表时发生错误: {str(e)}", exc_info=True)

if __name__ == "__main__":
    start_time = datetime.now()
    logger.info("开始执行股票财务过滤器")
    
    filter_and_update_stocks()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"股票财务过滤器执行完成，耗时 {duration:.2f} 秒")
