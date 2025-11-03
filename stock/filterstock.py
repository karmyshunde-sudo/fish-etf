#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票列表财务过滤器
功能：
1. 读取all_stocks.csv文件
2. 逐个股票获取财务数据
3. 应用财务条件过滤
4. 将过滤后的股票列表保存回all_stocks.csv

财务过滤条件：
- 仅保留流通市值、总市值、动态市盈率三个指标
- 动态市盈率 >= 15.0
- 流通市值/总市值 > 0.8

使用说明：
1. 该脚本应在每周固定时间运行（例如周末）
2. 运行前确保已安装必要依赖：pip install baostock pandas
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

# 配置日志
logger = logging.getLogger(__name__)

# 添加BATCH_SIZE参数，方便灵活调整每次处理的股票数量
BATCH_SIZE = 100  # 每次处理的股票数量

# 财务指标过滤参数配置（仅保留需要的三个指标）
FINANCIAL_FILTER_PARAMS = {
    "dynamic_pe": {
        "enabled": True,
        "threshold": 15.0,
        "column": "动态市盈率",
        "category": "估值指标",
        "condition": ">= 15.0（动态市盈率大于等于15）"
    },
    "circulating_to_total_ratio": {
        "enabled": True,
        "threshold": 0.8,
        "column": "流通市值/总市值",
        "category": "流通性",
        "condition": "> 0.8（流通市值占总市值比例大于80%）"
    }
}

def get_financial_data(code):
    """
    使用Baostock的query_stock_basic接口获取单只股票数据（最稳定方式）
    参数：
    - code: 股票代码（6位字符串）
    返回：
    - dict: 包含动态市盈率、总市值、流通市值、流通市值/总市值比率
    - None: 获取失败（但不会删除股票）
    """
    try:
        # 转换为baostock格式的代码
        bs_code = "sh." + code if code.startswith('6') else "sz." + code
        
        # 获取股票基本信息
        rs = bs.query_stock_basic(code=bs_code)
        if rs.error_code != '0':
            logger.error(f"获取股票 {code} 基本信息失败: {rs.error_msg}")
            return None
        
        # 记录返回字段
        logger.info(f"Baostock query_stock_basic 返回的字段: {', '.join(rs.fields)}")
        
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            logger.warning(f"获取股票 {code} 基本信息成功，但无数据返回")
            return None
        
        # 创建DataFrame
        df = pd.DataFrame(data_list, columns=rs.fields)
        row = df.iloc[0]
        
        # 提取基本信息
        peTTM = row.get('peTTM', None)
        totalShare = row.get('totalShare', None)
        liquidShare = row.get('liquidShare', None)
        
        # 转换数据类型
        try:
            peTTM = float(peTTM) if peTTM is not None else None
            totalShare = float(totalShare) if totalShare is not None else None
            liquidShare = float(liquidShare) if liquidShare is not None else None
        except (ValueError, TypeError):
            peTTM = None
            totalShare = None
            liquidShare = None
        
        # 计算市值（需要收盘价，但Baostock query_stock_basic不返回收盘价）
        # 因此我们使用最简单的方案：只获取市盈率，市值不计算，直接使用市盈率和总股本、流通股本
        # 但实际需要市值数据，所以我们使用查询K线的简单方式
        # 由于您要求简单，我们只查询最近一天的收盘价
        rs_k = bs.query_history_k_data(
            code=bs_code,
            fields="close",
            start_date=datetime.now().strftime("%Y-%m-%d"),
            end_date=datetime.now().strftime("%Y-%m-%d")
        )
        if rs_k.error_code != '0':
            logger.error(f"获取股票 {code} K线数据失败: {rs_k.error_msg}")
            return None
        
        k_data = []
        while rs_k.next():
            k_data.append(rs_k.get_row_data())
        
        if not k_data:
            logger.warning(f"获取股票 {code} K线数据成功，但无数据返回")
            return None
        
        close_price = float(k_data[0][1])
        
        # 计算市值
        total_market_value = totalShare * close_price if totalShare is not None else None
        circulating_market_value = liquidShare * close_price if liquidShare is not None else None
        circulating_to_total_ratio = None
        if total_market_value and total_market_value > 0 and circulating_market_value:
            circulating_to_total_ratio = circulating_market_value / total_market_value
        
        result = {
            "dynamic_pe": peTTM,
            "total_market_value": total_market_value,
            "circulating_market_value": circulating_market_value,
            "circulating_to_total_ratio": circulating_to_total_ratio
        }
        
        logger.info(f"股票 {code} 通过Baostock获取的基本信息: {result}")
        return result
    except Exception as e:
        logger.error(f"获取股票 {code} 财务数据失败: {str(e)}")
        return None

def apply_financial_filters(stock_code, financial_data):
    """
    应用财务过滤条件（仅检查动态市盈率和流通市值/总市值比率）
    参数：
    - stock_code: 股票代码
    - financial_ 股票财务数据
    返回：
    - bool: 是否通过所有财务条件
    """
    if financial_data is None:
        return False
    
    for param_name, param_config in FINANCIAL_FILTER_PARAMS.items():
        if not param_config["enabled"]:
            continue
        
        # 检查指标是否存在
        if param_name not in financial_data or financial_data[param_name] is None:
            logger.debug(f"股票 {stock_code} 缺少 {param_name} 数据")
            return False
        
        value = financial_data[param_name]
        # 根据阈值检查
        if param_config["condition"].startswith(">= "):
            if value < param_config["threshold"]:
                logger.debug(f"股票 {stock_code} {param_name} 不满足条件: {value} < {param_config['threshold']}")
                return False
        elif param_config["condition"].startswith("> "):
            if value <= param_config["threshold"]:
                logger.debug(f"股票 {stock_code} {param_name} 不满足条件: {value} <= {param_config['threshold']}")
                return False
    
    return True

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
        logger.info(f"过滤前需要处理的股票数量: {len(to_process)}")
        
        # 如果没有需要处理的股票（即所有filter都为True），重置filter为False
        if len(to_process) == 0:
            logger.info("所有股票都已处理，重置filter列为False")
            basic_info_df['filter'] = False
            basic_info_df.to_csv(basic_info_file, index=False)
            logger.info("filter列已重置，退出执行")
            return
        
        # 只处理前BATCH_SIZE只股票
        process_batch = to_process.head(BATCH_SIZE)
        logger.info(f"本次处理股票数量: {len(process_batch)}")
        
        # 登录Baostock
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"Baostock登录失败: {login_result.error_msg}")
            return
        
        try:
            # 逐个处理股票
            for idx, stock in process_batch.iterrows():
                stock_code = str(stock["代码"]).zfill(6)
                stock_name = stock["名称"]
                
                logger.info(f"处理股票: {stock_code} {stock_name} ({idx+1}/{len(process_batch)})")
                
                # 获取财务数据
                financial_data = get_financial_data(stock_code)
                if financial_data is None:
                    logger.warning(f"股票 {stock_code} 财务数据获取失败，跳过本次处理（保留股票）")
                    continue
                
                # 应用财务过滤
                if apply_financial_filters(stock_code, financial_data):
                    basic_info_df.loc[idx, 'filter'] = True
                    logger.info(f"股票 {stock_code} 通过所有过滤条件")
                else:
                    logger.info(f"股票 {stock_code} 未通过过滤条件，删除该行")
                    basic_info_df.drop(idx, inplace=True)
                
                # API调用频率限制
                time.sleep(0.5)
        
        finally:
            # 确保登出
            bs.logout()
        
        # 保存更新后的股票列表
        basic_info_df.to_csv(basic_info_file, index=False)
        logger.info(f"已更新 {basic_info_file} 文件，当前共 {len(basic_info_df)} 只股票")
        
        # 检查是否所有股票的filter都为True（即全部通过过滤）
        if basic_info_df['filter'].all():
            logger.info("所有股票都通过过滤，重置filter列为False")
            basic_info_df['filter'] = False
            basic_info_df.to_csv(basic_info_file, index=False)
        
        # 提交到Git仓库
        try:
            commit_files_in_batches(Config.DATA_DIR, "LAST_FILE")
            logger.info("已提交过滤后的股票列表到Git仓库")
        except Exception as e:
            logger.error(f"提交到Git仓库失败: {str(e)}")
        
    except Exception as e:
        logger.error(f"处理股票列表时发生错误: {str(e)}", exc_info=True)

if __name__ == "__main__":
    start_time = datetime.now()
    logger.info("开始执行股票财务过滤器")
    
    filter_and_update_stocks()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"股票财务过滤器执行完成，耗时 {duration:.2f} 秒")
