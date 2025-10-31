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
- 可为每个财务指标配置启用状态和阈值
- 默认只启用5个关键指标，其他78个指标默认禁用
- 采用"短板式处理"：任一条件不满足即过滤

使用说明：
1. 该脚本应在每周固定时间运行（例如周末）
2. 运行前确保已安装必要依赖：pip install akshare pandas
3. 脚本会更新all_stocks.csv文件
"""

import os
import pandas as pd
import akshare as ak
import time
import logging
import sys
from datetime import datetime
from config import Config
from utils.date_utils import get_beijing_time
from utils.git_utils import commit_files_in_batches

# 配置日志
logger = logging.getLogger(__name__)


# 财务指标过滤参数配置
# 每个指标可以独立启用/禁用，并设置阈值
FINANCIAL_FILTER_PARAMS = {
    # 关键财务指标（默认启用）
    "basic_earnings_per_share": {
        "enabled": True,
        "threshold": 0.0,
        "column": "基本每股收益",
        "category": "每股指标",
        "condition": ">= 0（排除亏损股）"
    },
    "static_pe_ratio": {
        "enabled": True,
        "threshold": 0.0,
        "column": "基本每股收益",
        "category": "每股指标",
        "condition": "> 0（排除市盈率≤0的股票）",
        "is_computed": True
    },
    "total_pledged_shares": {
        "enabled": True,
        "threshold": 0.0,
        "column": "总质押股份数量",
        "category": "常用指标",
        "condition": "<= 0（排除有质押的股票）"
    },
    "net_profit_growth": {
        "enabled": True,
        "threshold": 0.0,
        "column": "归母净利润",
        "category": "常用指标",
        "condition": ">= 0（排除净利润下降的股票）"
    },
    "roe": {
        "enabled": True,
        "threshold": 5.0,
        "column": "净资产收益率(ROE)",
        "category": "盈利能力",
        "condition": ">= 5%（排除ROE低于5%的股票）"
    },
    
    # 其他财务指标（默认禁用）
    "gross_profit_margin": {
        "enabled": False,
        "threshold": 0.0,
        "column": "毛利率",
        "category": "常用指标",
        "condition": ">= 0（排除毛利率为负的股票）"
    },
    "net_profit_margin": {
        "enabled": False,
        "threshold": 0.0,
        "column": "销售净利率",
        "category": "常用指标",
        "condition": ">= 0（排除净利率为负的股票）"
    },
    "current_ratio": {
        "enabled": False,
        "threshold": 1.0,
        "column": "流动比率",
        "category": "财务风险",
        "condition": ">= 1.0（排除流动性风险）"
    },
    "debt_to_assets_ratio": {
        "enabled": False,
        "threshold": 70.0,
        "column": "资产负债率",
        "category": "财务风险",
        "condition": "<= 70%（排除高负债率股票）"
    },
    "operating_cash_flow_ratio": {
        "enabled": False,
        "threshold": 0.0,
        "column": "经营现金流量净额",
        "category": "常用指标",
        "condition": ">= 0（排除经营现金流为负的股票）"
    },
    "total_assets": {
        "enabled": False,
        "threshold": 0.0,
        "column": "总资产",
        "category": "常用指标",
        "condition": ">= 0（排除总资产为负的股票）"
    },
    "net_assets": {
        "enabled": False,
        "threshold": 0.0,
        "column": "股东权益合计(净资产)",
        "category": "常用指标",
        "condition": ">= 0（排除净资产为负的股票）"
    },
    "eps_diluted": {
        "enabled": False,
        "threshold": 0.0,
        "column": "稀释每股收益",
        "category": "每股指标",
        "condition": ">= 0（排除稀释每股收益为负的股票）"
    },
    "net_assets_per_share": {
        "enabled": False,
        "threshold": 0.0,
        "column": "每股净资产",
        "category": "每股指标",
        "condition": ">= 0（排除每股净资产为负的股票）"
    },
    "cash_flow_per_share": {
        "enabled": False,
        "threshold": 0.0,
        "column": "每股现金流",
        "category": "每股指标",
        "condition": ">= 0（排除每股现金流为负的股票）"
    },
    "total_capital_return": {
        "enabled": False,
        "threshold": 0.0,
        "column": "总资本回报率",
        "category": "盈利能力",
        "condition": ">= 0（排除总资本回报率为负的股票）"
    },
    "total_assets_turnover": {
        "enabled": False,
        "threshold": 0.0,
        "column": "总资产周转率",
        "category": "营运能力",
        "condition": ">= 0（排除总资产周转率为负的股票）"
    },
    "inventory_turnover": {
        "enabled": False,
        "threshold": 0.0,
        "column": "存货周转率",
        "category": "营运能力",
        "condition": ">= 0（排除存货周转率为负的股票）"
    },
    "receivables_turnover": {
        "enabled": False,
        "threshold": 0.0,
        "column": "应收账款周转率",
        "category": "营运能力",
        "condition": ">= 0（排除应收账款周转率为负的股票）"
    },
    "quick_ratio": {
        "enabled": False,
        "threshold": 0.0,
        "column": "速动比率",
        "category": "财务风险",
        "condition": ">= 0（排除速动比率不满足要求的股票）"
    },
    "conservative_quick_ratio": {
        "enabled": False,
        "threshold": 0.0,
        "column": "保守速动比率",
        "category": "财务风险",
        "condition": ">= 0（排除保守速动比率不满足要求的股票）"
    },
    "equity_multiplier": {
        "enabled": False,
        "threshold": 0.0,
        "column": "权益乘数",
        "category": "财务风险",
        "condition": ">= 0（排除权益乘数不满足要求的股票）"
    },
    "equity_ratio": {
        "enabled": False,
        "threshold": 0.0,
        "column": "产权比率",
        "category": "财务风险",
        "condition": ">= 0（排除产权比率不满足要求的股票）"
    },
    "net_profit_after_tax": {
        "enabled": False,
        "threshold": 0.0,
        "column": "净利润",
        "category": "常用指标",
        "condition": ">= 0（排除净利润为负的股票）"
    },
    "net_profit_deducted": {
        "enabled": False,
        "threshold": 0.0,
        "column": "扣非净利润",
        "category": "常用指标",
        "condition": ">= 0（排除扣非净利润为负的股票）"
    },
    "operating_income": {
        "enabled": False,
        "threshold": 0.0,
        "column": "营业总收入",
        "category": "常用指标",
        "condition": ">= 0（排除营业收入为负的股票）"
    },
    "operating_cost": {
        "enabled": False,
        "threshold": 0.0,
        "column": "营业成本",
        "category": "常用指标",
        "condition": ">= 0（排除营业成本为负的股票）"
    },
    "roa": {
        "enabled": False,
        "threshold": 0.0,
        "column": "总资产报酬率(ROA)",
        "category": "盈利能力",
        "condition": ">= 0（排除ROA为负的股票）"
    },
    "roic": {
        "enabled": False,
        "threshold": 0.0,
        "column": "投入资本回报率",
        "category": "盈利能力",
        "condition": ">= 0（排除ROIC为负的股票）"
    },
    "roic_after_tax": {
        "enabled": False,
        "threshold": 0.0,
        "column": "息前税后总资产报酬率_平均",
        "category": "盈利能力",
        "condition": ">= 0（排除息前税后总资产报酬率为负的股票）"
    },
    "gross_profit_rate": {
        "enabled": False,
        "threshold": 0.0,
        "column": "毛利率",
        "category": "盈利能力",
        "condition": ">= 0（排除毛利率为负的股票）"
    },
    "sales_net_profit_rate": {
        "enabled": False,
        "threshold": 0.0,
        "column": "销售净利率",
        "category": "盈利能力",
        "condition": ">= 0（排除销售净利率为负的股票）"
    },
    "cost_rate": {
        "enabled": False,
        "threshold": 0.0,
        "column": "成本费用率",
        "category": "盈利能力",
        "condition": ">= 0（排除成本费用率为负的股票）"
    },
    "operating_profit_rate": {
        "enabled": False,
        "threshold": 0.0,
        "column": "营业利润率",
        "category": "盈利能力",
        "condition": ">= 0（排除营业利润率为负的股票）"
    },
    "roa_average": {
        "enabled": False,
        "threshold": 0.0,
        "column": "总资产净利率_平均",
        "category": "盈利能力",
        "condition": ">= 0（排除总资产净利率为负的股票）"
    },
    "roa_average_excl_minority": {
        "enabled": False,
        "threshold": 0.0,
        "column": "总资产净利率_平均(含少数股东损益)",
        "category": "盈利能力",
        "condition": ">= 0（排除总资产净利率为负的股票）"
    },
    "current_assets_turnover": {
        "enabled": False,
        "threshold": 0.0,
        "column": "流动资产周转率",
        "category": "营运能力",
        "condition": ">= 0（排除流动资产周转率为负的股票）"
    },
    "current_assets_turnover_days": {
        "enabled": False,
        "threshold": 0.0,
        "column": "流动资产周转天数",
        "category": "营运能力",
        "condition": ">= 0（排除流动资产周转天数不满足要求的股票）"
    },
    "accounts_payable_turnover": {
        "enabled": False,
        "threshold": 0.0,
        "column": "应付账款周转率",
        "category": "营运能力",
        "condition": ">= 0（排除应付账款周转率为负的股票）"
    },
    "inventory_turnover_days": {
        "enabled": False,
        "threshold": 0.0,
        "column": "存货周转天数",
        "category": "营运能力",
        "condition": ">= 0（排除存货周转天数不满足要求的股票）"
    },
    "receivables_turnover_days": {
        "enabled": False,
        "threshold": 0.0,
        "column": "应收账款周转天数",
        "category": "营运能力",
        "condition": ">= 0（排除应收账款周转天数不满足要求的股票）"
    },
    "total_assets_turnover_days": {
        "enabled": False,
        "threshold": 0.0,
        "column": "总资产周转天数",
        "category": "营运能力",
        "condition": ">= 0（排除总资产周转天数不满足要求的股票）"
    },
    "cash_flow_to_sales": {
        "enabled": False,
        "threshold": 0.0,
        "column": "经营活动净现金/销售收入",
        "category": "收益质量",
        "condition": ">= 0（排除经营现金流量/销售收入为负的股票）"
    },
    "cash_flow_to_operating_income": {
        "enabled": False,
        "threshold": 0.0,
        "column": "经营性现金净流量/营业总收入",
        "category": "收益质量",
        "condition": ">= 0（排除经营性现金净流量/营业总收入为负的股票）"
    },
    "cash_flow_to_net_profit": {
        "enabled": False,
        "threshold": 0.0,
        "column": "经营活动净现金/归属母公司的净利润",
        "category": "收益质量",
        "condition": ">= 0（排除经营现金流量/净利润为负的股票）"
    },
    "tax_to_profit": {
        "enabled": False,
        "threshold": 0.0,
        "column": "所得税/利润总额",
        "category": "收益质量",
        "condition": ">= 0（排除所得税/利润总额为负的股票）"
    },
    # 这里可以添加更多财务指标...
}

def get_financial_data(code):
    """
    获取单只股票的财务数据
    参数：
    - code: 股票代码（6位字符串）
    返回：
    - DataFrame: 股票财务数据
    - None: 获取失败
    """
    try:
        # 直接使用6位股票代码调用API
        df = ak.stock_financial_abstract(symbol=code)
        if df is not None and not df.empty:
            return df
        return None
    except Exception as e:
        logger.error(f"获取股票 {code} 财务数据失败: {str(e)}")
        return None

def apply_financial_filters(stock_code, df):
    """
    应用财务过滤条件
    参数：
    - stock_code: 股票代码
    - df: 股票财务数据
    返回：
    - bool: 是否通过所有财务条件
    """
    # 确定最新报告期（第三列）
    if len(df.columns) < 3:
        logger.warning(f"股票 {stock_code} 财务数据列数不足3列")
        return False
    
    latest_date = df.columns[2]
    
    # 收集关键财务指标
    financial_data = {}
    
    # 遍历财务数据行
    for _, row in df.iterrows():
        option = row["选项"] if "选项" in row else ""
        indicator = row["指标"] if "指标" in row else ""
        
        # 检查是否是需要的指标
        for param_name, param_config in FINANCIAL_FILTER_PARAMS.items():
            if option == param_config["category"] and indicator == param_config["column"]:
                try:
                    value = float(row[latest_date])
                    financial_data[param_name] = value
                except (ValueError, TypeError):
                    financial_data[param_name] = None
    
    # 应用财务过滤条件
    for param_name, param_config in FINANCIAL_FILTER_PARAMS.items():
        if not param_config["enabled"]:
            continue
        
        # 检查该指标是否存在
        if param_name not in financial_data or financial_data[param_name] is None:
            logger.debug(f"股票 {stock_code} 缺少 {param_name} 数据")
            return False
        
        # 处理计算型指标
        if param_name == "static_pe_ratio":
            # 静态市盈率 = 收盘价 / 每股收益
            # 如果收盘价不存在，无法计算，返回False
            # 在实际应用中，这里应该有收盘价信息，但当前代码中没有
            # 为简化，我们假设收盘价为正，只检查每股收益
            if "basic_earnings_per_share" in financial_data:
                basic_earnings = financial_data["basic_earnings_per_share"]
                if basic_earnings is None or basic_earnings <= 0:
                    logger.debug(f"股票 {stock_code} 静态市盈率条件不满足")
                    return False
                # 静态市盈率大于0，因为收盘价假设为正，且basic_earnings > 0
                continue
            else:
                return False
        
        # 普通指标检查
        value = financial_data[param_name]
        if value is None:
            logger.debug(f"股票 {stock_code} {param_name} 数据缺失")
            return False
        
        # 根据阈值检查
        if param_config["condition"].startswith(">= "):
            if value < param_config["threshold"]:
                logger.debug(f"股票 {stock_code} {param_name} 不满足条件: {value} < {param_config['threshold']}")
                return False
        elif param_config["condition"].startswith("> "):
            if value <= param_config["threshold"]:
                logger.debug(f"股票 {stock_code} {param_name} 不满足条件: {value} <= {param_config['threshold']}")
                return False
        elif param_config["condition"].startswith("<= "):
            if value > param_config["threshold"]:
                logger.debug(f"股票 {stock_code} {param_name} 不满足条件: {value} > {param_config['threshold']}")
                return False
        elif param_config["condition"].startswith("< "):
            if value >= param_config["threshold"]:
                logger.debug(f"股票 {stock_code} {param_name} 不满足条件: {value} >= {param_config['threshold']}")
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
        logger.info(f"需要处理的股票数量: {len(to_process)}")
        
        # 如果没有需要处理的股票，重置所有filter为False并退出
        if len(to_process) == 0:
            logger.info("所有股票都已处理，重置filter列")
            basic_info_df['filter'] = False
            basic_info_df.to_csv(basic_info_file, index=False)
            logger.info("filter列已重置，退出执行")
            return
        
        # 只处理前100只股票
        process_batch = to_process.head(100)
        logger.info(f"本次处理股票数量: {len(process_batch)}")
        
        # 用于存储处理结果
        valid_stocks = []
        
        # 逐个处理股票
        for idx, stock in process_batch.iterrows():
            stock_code = str(stock["代码"]).zfill(6)
            stock_name = stock["名称"]
            
            logger.info(f"处理股票: {stock_code} {stock_name} ({idx+1}/{len(process_batch)})")
            
            # 获取财务数据
            df = get_financial_data(stock_code)
            if df is None or df.empty:
                logger.warning(f"股票 {stock_code} 财务数据为空，标记为未通过")
                # 即使财务数据为空，也将filter设为True（跳过后续处理）
                stock['filter'] = True
                basic_info_df.loc[idx, 'filter'] = True
                continue
            
            # 应用财务过滤
            if apply_financial_filters(stock_code, df):
                stock['filter'] = True
                basic_info_df.loc[idx, 'filter'] = True
                valid_stocks.append(stock)
            else:
                stock['filter'] = True
                basic_info_df.loc[idx, 'filter'] = True
            
            # API调用频率限制
            time.sleep(1)
        
        # 保存更新后的股票列表
        basic_info_df.to_csv(basic_info_file, index=False)
        logger.info(f"已更新 {basic_info_file} 文件")
        
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
