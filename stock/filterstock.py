#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票列表财务过滤器
功能：
1. 读取all_stocks.csv文件
2. 逐个股票获取财务数据（动态市盈率和净利润）
3. 应用财务条件过滤
4. 将过滤后的股票列表保存回all_stocks.csv

财务过滤条件：
- 动态市盈率 >= 0.1
- 净利润 > 0

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
import akshare as ak
import random
from datetime import datetime
from config import Config
from utils.date_utils import get_beijing_time
from utils.new_git import commit_single_file

# 配置日志
logger = logging.getLogger(__name__)

# 添加BATCH_SIZE参数，方便灵活调整每次处理的股票数量
BATCH_SIZE = 200  # 每次处理的股票数量

# 财务指标过滤参数配置（保留动态市盈率大于等于0.1的股票）（保留净利润正的股票）
FINANCIAL_FILTER_PARAMS = {
    "dynamic_pe": {
        "enabled": False,
        "threshold": 0.1,
        "column": "动态市盈率",
        "category": "估值指标"
    },
    "net_profit": {
        "enabled": True,
        "threshold": 0,
        "column": "净利润",
        "category": "常用指标"
    }
}

def get_dynamic_pe(code):
    """
    使用Baostock获取单只股票动态市盈率
    参数：
    - code: 股票代码（6位字符串）
    返回：
    - float: 动态市盈率
    - None: 获取失败（不会删除股票）
    """
    try:
        # 确保代码是6位字符串
        code = str(code).zfill(6)
        
        # 转换为baostock格式的代码
        bs_code = "sh." + code if code.startswith('6') else "sz." + code
        logger.debug(f"使用Baostock格式的股票代码: {bs_code}")
        
        # 获取股票基本信息
        rs = bs.query_stock_basic(code=bs_code)
        if rs.error_code != '0':
            logger.error(f"获取股票 {code} 基本信息失败: {rs.error_msg}")
            return None
        
        # 记录返回字段
        logger.debug(f"Baostock query_stock_basic 返回的字段: {', '.join(rs.fields)}")
        
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            logger.warning(f"获取股票 {code} 基本信息成功，但无数据返回")
            return None
        
        # 创建DataFrame
        df = pd.DataFrame(data_list, columns=rs.fields)
        logger.debug(f"Baostock返回的数据预览: {df.head(1).to_dict()}")
        
        # 检查是否有peTTM字段
        if 'peTTM' not in df.columns:
            logger.warning(f"股票 {code} 的数据中没有peTTM字段")
            return None
        
        row = df.iloc[0]
        
        # 提取动态市盈率
        peTTM = row.get('peTTM', None)
        
        # 转换数据类型
        try:
            peTTM = float(peTTM) if peTTM is not None else None
        except (ValueError, TypeError):
            logger.error(f"股票 {code} 的动态市盈率值无法转换为浮点数: {peTTM}")
            peTTM = None
        
        if peTTM is not None:
            logger.info(f"股票 {code} 动态市盈率: {peTTM:.2f}")
        else:
            logger.warning(f"股票 {code} 动态市盈率数据为空")
        
        return peTTM
    except Exception as e:
        logger.exception(f"获取股票 {code} 动态市盈率失败")
        return None

def get_net_profit(code):
    """
    使用akshare获取单只股票净利润
    参数：
    - code: 股票代码（6位字符串）
    返回：
    - float: 净利润
    - None: 获取失败（不会删除股票）
    """
    try:
        # 确保代码是6位字符串
        code = str(code).zfill(6)
        logger.debug(f"正在获取股票 {code} 的净利润数据")
        
        # 获取财务摘要数据
        df = ak.stock_financial_abstract(symbol=code)
        
        if df.empty:
            logger.error(f"股票 {code} 返回空财务数据")
            return None
        
        # 记录返回数据的列名
        logger.debug(f"akshare返回的列名: {df.columns.tolist()}")
        
        # 记录前3行数据内容，便于调试
        if not df.empty:
            logger.debug(f"akshare返回的前3行数据: {df.head(3).to_dict()}")
        
        # 按行遍历数据，不依赖列名，直接按位置索引取值
        for index, row in df.iterrows():
            # 检查行数据长度是否足够
            if len(row) < 3:
                logger.debug(f"股票 {code} 第{index}行数据长度不足，跳过")
                continue
            
            # 直接按位置取值，不关心列名
            first_col_value = str(row.iloc[0]).strip()
            second_col_value = str(row.iloc[1]).strip()
            
            logger.debug(f"检查行数据: 第一列='{first_col_value}', 第二列='{second_col_value}'")
            
            # 检查是否符合"常用指标"和"净利润"条件
            if first_col_value == "常用指标" and second_col_value == "净利润":
                # 直接取第三列的值（索引2）
                third_col_value = row.iloc[2]
                logger.debug(f"股票 {code} 找到匹配行，第三列值: {third_col_value}")
                
                # 尝试转换为浮点数
                try:
                    # 清理数据：去除可能的逗号和空格
                    cleaned_value = str(third_col_value).replace(',', '').replace(' ', '').strip()
                    net_profit = float(cleaned_value)
                    logger.info(f"股票 {code} 获取到的净利润值: {net_profit:.2f}")
                    return net_profit
                except (TypeError, ValueError) as e:
                    logger.error(f"股票 {code} 的第三列值无法转换为浮点数: {third_col_value} (错误: {str(e)})")
                    return None
        
        # 如果遍历完所有行都没找到匹配的数据
        logger.warning(f"股票 {code} 未找到'常用指标'下的'净利润'数据")
        return None
        
    except Exception as e:
        logger.exception(f"获取股票 {code} 净利润数据失败: {str(e)}")
        return None
        
def apply_financial_filters(stock_code, dynamic_pe, net_profit):
    """
    应用财务过滤条件
    参数：
    - stock_code: 股票代码
    - dynamic_pe: 动态市盈率
    - net_profit: 净利润
    返回：
    - bool: 是否通过所有财务条件
    """
    # 检查动态市盈率
    if FINANCIAL_FILTER_PARAMS["dynamic_pe"]["enabled"]:
        if dynamic_pe is None:
            logger.debug(f"股票 {stock_code} 动态市盈率数据缺失")
            return False
        threshold = FINANCIAL_FILTER_PARAMS["dynamic_pe"]["threshold"]
        if dynamic_pe < threshold:
            logger.info(f"股票 {stock_code} 动态市盈率不满足条件: {dynamic_pe:.2f} < {threshold}")
            return False
    
    # 检查净利润
    if FINANCIAL_FILTER_PARAMS["net_profit"]["enabled"]:
        if net_profit is None:
            logger.debug(f"股票 {stock_code} 净利润数据缺失")
            return False
        threshold = FINANCIAL_FILTER_PARAMS["net_profit"]["threshold"]
        if net_profit <= threshold:
            logger.info(f"股票 {stock_code} 净利润不满足条件: {net_profit:.2f} <= {threshold}")
            return False
    
    return True

def get_market_value(code):
    """
    使用akshare的stock_zh_a_daily接口获取单只股票的流通市值
    参数：
    - code: 股票代码（6位字符串）
    返回：
    - float: 流通市值（单位：元）
    - None: 获取失败
    """
    try:
        # 确保代码是6位字符串
        code = str(code).zfill(6)
        logger.debug(f"正在获取股票 {code} 的流通市值数据")
        
        # 使用akshare获取股票日线数据
        df = ak.stock_zh_a_daily(symbol=f"sh{code}" if code.startswith('6') else f"sz{code}", 
                               adjust="qfq")
        
        if df.empty:
            logger.error(f"股票 {code} 返回空数据")
            return None
        
        # 检查必要的列
        required_columns = ['close', 'outstanding_share']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"股票 {code} 数据缺少必要列: {col}")
                return None
        
        # 获取最新交易日的数据
        latest_data = df.iloc[-1]
        
        # 计算流通市值：收盘价 * 流通股本
        close_price = latest_data['close']
        outstanding_share = latest_data['outstanding_share']
        
        # 检查数据是否有效
        if pd.isna(close_price) or pd.isna(outstanding_share) or close_price <= 0 or outstanding_share <= 0:
            logger.warning(f"股票 {code} 数据无效: 收盘价={close_price}, 流通股本={outstanding_share}")
            return None
        
        market_value = close_price * outstanding_share
        logger.info(f"股票 {code} 流通市值: {market_value:.2f}元")
        return market_value
    except Exception as e:
        logger.exception(f"获取股票 {code} 流通市值失败")
        return None

def filter_and_update_stocks():
    """
    主函数：过滤股票并更新all_stocks.csv
    修改：只对通过财务过滤或财务数据获取失败的股票获取流通市值
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
        
        # 添加需要的财务指标列（仅当指标启用时）
        for param_name, param_config in FINANCIAL_FILTER_PARAMS.items():
            if param_config["enabled"]:
                if param_config["column"] not in basic_info_df.columns:
                    basic_info_df[param_config["column"]] = float('nan')
                    logger.info(f"添加列 '{param_config['column']}' 到all_stocks.csv文件")
        
        # 添加流通市值和总市值列（如果不存在）
        if "流通市值" not in basic_info_df.columns:
            basic_info_df["流通市值"] = float('nan')
            logger.info("添加流通市值列到all_stocks.csv文件")
        
        if "总市值" not in basic_info_df.columns:
            basic_info_df["总市值"] = float('nan')
            logger.info("添加总市值列到all_stocks.csv文件")
        
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
        
        # 从原始dataframe中删除这BATCH_SIZE只股票
        basic_info_df = basic_info_df[~basic_info_df.index.isin(process_batch.index)]
        logger.info(f"已从原始数据中移除 {len(process_batch)} 只待处理股票，剩余 {len(basic_info_df)} 只")
        
        # 创建临时dataframe用于财务处理
        temp_df = process_batch.copy()
        
        # 登录Baostock
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"Baostock登录失败: {login_result.error_msg}")
            return
        
        try:
            # 逐个处理股票（操作临时dataframe）
            for idx, stock in temp_df.iterrows():
                stock_code = str(stock["代码"]).zfill(6)
                stock_name = stock["名称"]
                
                logger.info(f"处理股票: {stock_code} {stock_name} ({idx+1}/{len(temp_df)})")
                
                # 【关键修改】获取财务指标
                dynamic_pe = None
                net_profit = None
                
                # 获取动态市盈率（如果启用）
                if FINANCIAL_FILTER_PARAMS["dynamic_pe"]["enabled"]:
                    dynamic_pe = get_dynamic_pe(stock_code)
                    # 保存到临时DataFrame
                    if dynamic_pe is not None:
                        temp_df.loc[idx, FINANCIAL_FILTER_PARAMS["dynamic_pe"]["column"]] = dynamic_pe
                        logger.debug(f"已更新股票 {stock_code} 的{FINANCIAL_FILTER_PARAMS['dynamic_pe']['column']}值: {dynamic_pe:.2f}")
                
                # 获取净利润（如果启用）
                if FINANCIAL_FILTER_PARAMS["net_profit"]["enabled"]:
                    net_profit = get_net_profit(stock_code)
                    # 保存到临时DataFrame
                    if net_profit is not None:
                        temp_df.loc[idx, FINANCIAL_FILTER_PARAMS["net_profit"]["column"]] = net_profit
                        logger.debug(f"已更新股票 {stock_code} 的{FINANCIAL_FILTER_PARAMS['net_profit']['column']}值: {net_profit:.2f}")
                
                # 记录获取结果
                logger.debug(f"股票 {stock_code} 获取结果: 动态市盈率={dynamic_pe}, 净利润={net_profit}")
                
                # 【关键修改】判断是否需要获取流通市值
                should_get_market_value = False
                
                # 情况1: 两个财务指标都获取失败
                if dynamic_pe is None and net_profit is None:
                    logger.warning(f"股票 {stock_code} 两个财务指标均获取失败，保留股票")
                    should_get_market_value = True
                
                # 情况2: 财务数据获取成功，应用财务过滤
                else:
                    # 应用财务过滤
                    if apply_financial_filters(stock_code, dynamic_pe, net_profit):
                        temp_df.loc[idx, 'filter'] = True
                        logger.info(f"股票 {stock_code} 通过所有过滤条件")
                        should_get_market_value = True
                    else:
                        logger.info(f"股票 {stock_code} 未通过过滤条件")
                        temp_df.loc[idx, 'filter'] = False
                
                # 【关键修改】只有需要获取流通市值的股票才调用API
                if should_get_market_value:
                    market_value = get_market_value(stock_code)
                    if market_value is not None:
                        temp_df.loc[idx, "流通市值"] = market_value
                        logger.debug(f"已更新股票 {stock_code} 的流通市值: {market_value:.2f}")
                    else:
                        logger.warning(f"股票 {stock_code} 流通市值获取失败，跳过更新")
                
                # API调用频率限制
                time.sleep(random.uniform(2.0, 5.0))
        
        finally:
            # 确保登出
            bs.logout()
        
        # 从临时dataframe中筛选出通过过滤的股票
        passed_stocks = temp_df[temp_df['filter'] == True]
        logger.info(f"本次处理中通过过滤的股票数量: {len(passed_stocks)}")
        
        # 将通过的股票增量合并回原始dataframe
        basic_info_df = pd.concat([basic_info_df, passed_stocks], ignore_index=True)
        logger.info(f"追加通过的股票后，总股票数量: {len(basic_info_df)}")
        
        # 保存更新后的股票列表
        basic_info_df['代码'] = basic_info_df['代码'].apply(lambda x: str(x).strip().zfill(6) if pd.notna(x) else x)
        basic_info_df.to_csv(basic_info_file, index=False)
        logger.info(f"已更新 {basic_info_file} 文件，当前共 {len(basic_info_df)} 只股票")
        
        # 检查是否所有股票都通过过滤
        if basic_info_df['filter'].all():
            logger.info("所有股票都通过过滤，重置filter列为False")
            basic_info_df['filter'] = False
            basic_info_df.to_csv(basic_info_file, index=False)
            logger.info("filter列已重置，为下一次处理做准备")
        
        # 提交到Git仓库 - 使用新的Git工具
        try:
            commit_message = f"更新股票财务过滤结果 - {datetime.now().strftime('%Y%m%d%H%M%S')}"
            commit_success = commit_single_file(basic_info_file, commit_message)
            if commit_success:
                logger.info("已提交过滤后的股票列表到Git仓库")
            else:
                logger.error("过滤后的股票列表提交失败")
        except Exception as e:
            logger.error(f"提交到Git仓库失败: {str(e)}")
        
    except Exception as e:
        logger.exception(f"处理股票列表时发生错误")
        return False

if __name__ == "__main__":
    start_time = datetime.now()
    logger.info("开始执行股票财务过滤器")
    
    filter_and_update_stocks()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"股票财务过滤器执行完成，耗时 {duration:.2f} 秒")
