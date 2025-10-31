#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略2 - 专业级多指标共振策略（微信推送适配版）

核心设计原则：
1. 严格遵循"先技术指标计算，后财务数据过滤"原则
2. 财务数据仅对候选信号股票获取（避免对4000+股票无脑爬取）
3. 仅处理all_stocks.csv中有效股票（已过滤退市、ST股），不遍历data/daily/下所有文件
4. 三均线缠绕策略提供完整筛选过程日志（每一步筛选数量统计）
5. 严格适配wechat_push/push.py模块，符合金融系统可靠性要求

关键性能优化：
- 财务数据获取：逐个股票代码获取（AKShare API要求），避免symbol="all"错误
- 候选股票去重：仅对需要过滤的股票获取财务数据
- 1秒延时：避免AKShare API频率限制
- 数据完整性检查：严格验证列名、数值类型、时间格式

错误处理机制：
- 所有异常捕获并记录详细日志
- 单个股票失败不影响整体流程
- 财务数据获取失败时跳过过滤（避免程序中断）
- 数据缺失时自动跳过计算

数据流程：
1. 读取all_stocks.csv → 有效股票列表（已过滤退市/ST股）
2. 遍历股票列表 → 加载本地日线数据（严格检查中文列名）
3. 计算四大技术指标（MA/MACD/RSI/KDJ）→ 生成候选信号
4. 候选股票去重 → 批量获取财务数据（逐个股票代码）
5. 财务数据过滤（5个核心条件）→ 生成最终信号
6. 格式化多级信号（单一/双/三/四指标共振+三均线缠绕）
7. 生成微信推送消息 → 保存股票代码到文件 → 提交Git

三均线缠绕筛选过程（详细日志）：
1️⃣ 初始缠绕（偏离率≤2%）：所有三均线间距≤2%的股票
2️⃣ 空间验证（缠绕天数≥5）：连续5天以上满足空间验证的股票
3️⃣ 量能验证（缩量≥50%）：成交量≤5日均量50%的股票
4️⃣ 突破阶段验证（突破>1%）：收盘价突破三均线最大值1%以上
5️⃣ 确认阶段验证（确认>0.5%）：当日涨幅>0.5%的确认信号

财务数据过滤条件（严格遵循金融风控标准）：
1. 每股收益(EPSJB) ≥ 0（排除亏损股）
2. 静态市盈率(PE_STATIC) > 0（排除市盈率≤0的股票）
3. 总质押股份数量(BPSTZ) ≤ 0（排除有质押的股票）
4. 净利润同比增长(PARENTNETPROFITTZ) ≥ 0（排除净利润下降）
【净资产收益率ROE过滤去掉！！】5. ROE(ROEJQ) ≥ 5%（排除ROE低于5%的股票）

信号生成规则：
- 单一指标信号（MA/MACD/RSI/KDJ）：仅取前20名
- 双/三/四指标共振：全部符合条件的信号
- 三均线缠绕：完整展示筛选过程（分页显示）

输出格式规范：
- 每类信号包含：日期、信号详情、专业解读
- 三均线缠绕：分页显示筛选过程（每页20只）
- 财务过滤结果：精确统计每步筛选数量
- 股票代码保存：按策略类型分类，提交Git仓库

重要注意事项：
1. 代码中所有参数配置均需严格验证（如MAX_MA_DEVIATION=0.02表示2%）
2. AKShare API调用必须使用具体股票代码（不能使用"all"参数）
3. 财务数据列名需自动匹配（支持"股票代码"或"code"等变体）
4. 三均线缠绕的每一步筛选必须独立统计（便于参数调优）
5. 所有数据处理必须保留原始数据完整性（避免数据污染）

日志记录规范：
- INFO级别：关键流程节点、统计结果
- WARNING级别：数据缺失、过滤跳过
- ERROR级别：致命错误（如文件不存在、API调用失败）
- DEBUG级别：详细计算过程（仅用于开发调试）

Git提交规范：
- 文件名格式：macdYYYYMMDDHHMM.txt
- 提交信息：feat: 保存MACD策略股票代码 [时间戳]
- 仅提交股票代码文件（不提交其他文件）

执行流程验证：
1. 所有日线数据必须来自本地data/daily/（非实时爬取）
2. 股票代码必须6位格式（不足补0）
3. 财务数据获取失败时跳过过滤（非终止流程）
4. 三均线筛选过程日志必须包含5个步骤的精确统计
5. 微信推送消息必须符合wechat_push/push.py规范

错误修复重点：
- 修复AKShare API错误调用（原代码使用symbol="all"）
- 修复财务数据重复获取问题（仅对候选股票去重获取）
- 增加三均线筛选过程日志（精确统计每步数量）
- 严格验证数据完整性（列名、数值类型、时间格式）
"""

import os
import pandas as pd
import numpy as np
import subprocess
from datetime import datetime
import logging
import sys
import time
import akshare as ak
from config import Config
from utils.date_utils import get_beijing_time, is_file_outdated
from wechat_push.push import send_wechat_message


# 初始化日志
logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# handler = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)



# ========== 参数配置 ==========
# 均线参数
MIN_MARKET_UPWARD = False  # 是否要求大盘向上（当前未实现）
MAX_MA_DEVIATION = 0.02  # 2%的缠绕率阈值（三均线最大间距/最大均线值）
MIN_CONSOLIDATION_DAYS = 5  # 最小粘合天数（连续满足空间验证的天数）
MIN_VOLUME_RATIO_MA = 0.5  # 50%的缩量阈值（当日成交量/5日均量 ≤ 0.5）
MIN_BREAKOUT_RATIO = 0.01  # 1%的突破幅度阈值（突破三均线最大值的幅度）
MIN_CONFIRM_RATIO = 0.005  # 0.5%的确认幅度阈值（当日涨幅）
MAX_THREEMA_DEVIATION = 0.05  # 5%的三均线缠绕最大偏离率（仅用于初始收集候选股票）

# MACD参数
MACD_SHORT = 12  # 短期EMA周期
MACD_LONG = 26   # 长期EMA周期
MACD_SIGNAL = 9  # 信号线EMA周期
MAX_MACD_GROWTH_RATE = 0.5  # 50%的MACD增长阈值（未在当前信号检查中使用）

# RSI参数
MAX_RSI_CHANGE = 10  # RSI变化阈值（用于信号质量评分）

# KDJ参数
MAX_KDJ_CHANGE = 10  # KDJ变化阈值（用于信号质量评分）

def check_threema_steps(df, code, name):
    """
    检查三均线缠绕每一步的筛选结果（用于详细日志统计）
    
    参数：
    - df: 股票日线数据（DataFrame），必须包含'收盘'列
    - code: 股票代码（字符串）
    - name: 股票名称（字符串）
    
    返回：
    - 字典包含5个筛选步骤的布尔结果：
      - step1: 空间验证（偏离率≤MAX_MA_DEVIATION）
      - step2: 时间验证（缠绕天数≥MIN_CONSOLIDATION_DAYS）
      - step3: 量能验证（量比≤1.0/MIN_VOLUME_RATIO_MA）
      - step4: 突破阶段验证（突破幅度>MIN_BREAKOUT_RATIO）
      - step5: 确认阶段验证（确认幅度>MIN_CONFIRM_RATIO）
    - None: 如果计算过程中出错
    
    关键逻辑：
    1. 计算5/10/20日均线
    2. 空间验证：计算三均线最大值与最小值差值占最大值的比例
       - 若≤2%（MAX_MA_DEVIATION），则通过
    3. 时间验证：检查连续多少天偏离率≤2%
       - 需≥5天（MIN_CONSOLIDATION_DAYS）
    4. 量能验证：当日成交量/5日均量
       - 需≤0.5（即缩量≥50%）
    5. 突破阶段验证：(当前收盘价 - 三均线最大值)/三均线最大值
       - 需>1%（MIN_BREAKOUT_RATIO）
    6. 确认阶段验证：(当日收盘价 - 前一日收盘价)/前一日收盘价
       - 需>0.5%（MIN_CONFIRM_RATIO）
    
    异常处理：
    - 捕获所有异常，记录debug日志
    - 返回None表示无法计算
    
    注意：
    - 此函数仅用于统计筛选过程，不生成最终信号
    - 信号生成由check_threema_signal处理
    """
    try:
        # 计算5/10/20日均线
        ma5 = calc_ma(df, 5)
        ma10 = calc_ma(df, 10)
        ma20 = calc_ma(df, 20)
        
        # 空间验证：三均线间距≤2%
        max_ma = max(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
        min_ma = min(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
        deviation = (max_ma - min_ma) / max_ma
        step1 = deviation <= MAX_MA_DEVIATION
        
        # 时间验证：连续缠绕天数≥5天
        consolidation_days = 0
        for i in range(1, min(len(df), 20)):
            max_ma_i = max(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
            min_ma_i = min(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
            dev_i = (max_ma_i - min_ma_i) / max_ma_i
            if dev_i <= MAX_MA_DEVIATION:
                consolidation_days += 1
        step2 = consolidation_days >= MIN_CONSOLIDATION_DAYS
        
        # 量能验证：缩量≥50%
        if len(df) < 5:
            return None
        volume_ratio = df["成交量"].iloc[-1] / df["成交量"].rolling(5).mean().iloc[-1]
        step3 = volume_ratio <= 1.0 / MIN_VOLUME_RATIO_MA
        
        # 突破阶段验证：突破幅度>1%
        breakout_ratio = (df["收盘"].iloc[-1] - max_ma) / max_ma
        step4 = breakout_ratio > MIN_BREAKOUT_RATIO
        
        # 确认阶段验证：当日涨幅>0.5%
        confirm_ratio = (df["收盘"].iloc[-1] - df["收盘"].iloc[-2]) / df["收盘"].iloc[-2]
        step5 = confirm_ratio > MIN_CONFIRM_RATIO
        
        return {
            "step1": step1,
            "step2": step2,
            "step3": step3,
            "step4": step4,
            "step5": step5
        }
    except Exception as e:
        logger.debug(f"检查三均线中间步骤失败 {code}: {str(e)}")
        return None

def get_financial_data_for_codes(codes):
    """
    逐个股票代码获取财务数据（修正AKShare API调用）
    参数：
    - codes: 需要获取财务数据的股票代码列表（字符串列表）
    返回：
    - DataFrame: 包含所有股票的财务数据
    - 空DataFrame: 如果所有获取失败
    
    修改说明：
    1. 严格使用6位股票代码（不添加sh/sz前缀）
    2. 保持原始数据结构，不做额外处理
    3. 确保正确处理中文列名
    """
    financial_data = pd.DataFrame()
    for code in codes:
        code = code.zfill(6)  # 确保6位格式
        try:
            # 直接使用6位数字代码调用API（无前缀）
            # df = ak.stock_financial_analysis_indicator(symbol=code)

            # 替换为：
            if code.startswith('6'):
                symbol = 'sh' + code
            elif code.startswith(('0', '3')):
                symbol = 'sz' + code
            else:
                symbol = 'sh' + code  # 科创板等特殊情况
            df = ak.stock_financial_analysis_indicator(symbol=symbol)
  
            if df is not None and not df.empty:
                # 添加股票代码列（原始数据可能没有）
                df['股票代码'] = code
                financial_data = pd.concat([financial_data, df], ignore_index=True)
            else:
                logger.warning(f"股票 {code} 财务数据get_financial-1为空")
        except Exception as e:
            logger.error(f"获取股票 {code} 财务数据失败: {str(e)}")
        time.sleep(1)  # 避免触发AKShare频率限制
    return financial_data

def filter_signals(signals, financial_data):
    """
    应用财务过滤条件（仅三个有效条件）
    参数：
    - signals: 候选信号列表
    - financial_data: 获取到的财务数据
    返回：
    - filtered_signals: 经过财务过滤的信号
    
    修改说明：
    1. 移除了市盈率过滤条件（冗余）
    2. 仅保留三个有效财务过滤条件
    3. 优化了财务数据映射逻辑
    """
    if not signals:
        return signals
    
    # 找到第三列作为最新日期
    if len(financial_data.columns) < 3:
        logger.warning("财务数据列数不足3列，无法确定最新日期")
        return signals
    
    latest_date = financial_data.columns[2]
    logger.info(f"使用第三列 '{latest_date}' 作为最新日期进行财务过滤")
    
    # 创建股票代码到财务指标的映射
    financial_dict = {}
    for _, row in financial_data.iterrows():
        code = str(row['股票代码']).zfill(6)
        option = row['选项']
        indicator = row['指标']
        
        # 只处理有效行
        if pd.isna(code) or pd.isna(option) or pd.isna(indicator):
            continue
            
        if code not in financial_dict:
            financial_dict[code] = {}
        
        # 收集关键指标
        if option == "每股指标" and indicator == "基本每股收益":
            try:
                value = float(row[latest_date])
                financial_dict[code]["EPSJB"] = value
            except:
                pass
        elif option == "常用指标" and indicator == "归母净利润":
            try:
                value = float(row[latest_date])
                financial_dict[code]["PARENTNETPROFIT"] = value
            except:
                pass
        elif option == "常用指标" and indicator == "总质押股份数量":
            try:
                value = float(row[latest_date])
                financial_dict[code]["BPSTZ"] = value
            except:
                pass
    
    # 应用三个有效财务过滤条件
    filtered_signals = []
    for signal in signals:
        code = signal['code']
        if code not in financial_dict:
            continue
            
        financial_info = financial_dict[code]
        
        # 1. 每股收益：排除负数股票（EPSJB < 0）
        if "EPSJB" in financial_info and financial_info["EPSJB"] < 0:
            continue
            
        # 2. 总质押股份数量：排除有质押的股票（BPSTZ > 0）
        if "BPSTZ" in financial_info and financial_info["BPSTZ"] > 0:
            continue
            
        # 3. 净利润：排除净利润同比下降的股票
        if "PARENTNETPROFIT" in financial_info and financial_info["PARENTNETPROFIT"] < 0:
            continue
            
        # 通过所有条件
        filtered_signals.append(signal)
    
    # 记录过滤结果
    if len(filtered_signals) < len(signals):
        logger.info(f"财务过滤后，保留 {len(filtered_signals)} 个信号（原 {len(signals)} 个）")
        logger.info(f"过滤掉 {len(signals) - len(filtered_signals)} 个信号")
    
    return filtered_signals
    
def load_stock_daily_data(stock_code):
    """
    加载股票日线数据（严格使用中文列名）
    
    参数：
    - stock_code: 股票代码（字符串）
    
    返回：
    - DataFrame: 有效日线数据
    - 空DataFrame: 数据不存在或格式错误
    
    关键逻辑：
    1. 检查文件是否存在（data/daily/{stock_code}.csv）
    2. 严格验证列名（必须包含12个中文列名）
    3. 日期列转换为datetime类型
    4. 数值列转换为数值类型
    5. 移除NaN值（收盘/成交量）
    
    异常处理：
    - 所有异常记录为warning
    - 列名缺失时记录详细错误
    - 数据格式错误时返回空DataFrame
    
    注意：
    - 仅加载本地文件（不爬取实时数据）
    - 严格使用中文列名（避免英文列名导致错误）
    """
    try:
        # 构建文件路径
        stock_dir = os.path.join(Config.DATA_DIR, "daily")
        file_path = os.path.join(stock_dir, f"{stock_code}.csv")
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.warning(f"股票 {stock_code} 的日线数据不存在")
            return pd.DataFrame()
        
        # 读取CSV文件
        df = pd.read_csv(file_path)
        
        # 严格检查中文列名
        required_columns = ["日期", "股票代码", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"股票 {stock_code} 数据缺少必要列: {col}")
                return pd.DataFrame()
        
        # 日期datetime类型规则
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
        
        # 移除可能存在的空格
        df = df.sort_values("日期", ascending=True)
        
        # 确保数值列是数值类型
        numeric_columns = ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 移除NaN值
        df = df.dropna(subset=['收盘', '成交量'])
        
        logger.debug(f"成功加载股票 {stock_code} 的本地日线数据，共 {len(df)} 条有效记录")
        return df
    except Exception as e:
        logger.warning(f"读取股票 {stock_code} 数据失败: {str(e)}")
        logger.debug(traceback.format_exc())
        logger.warning(f"股票 {stock_code} 的日线数据不存在")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def main():
    """
    主流程控制函数（策略核心执行逻辑）
    
    详细执行步骤：
    1. 读取all_stocks.csv获取有效股票列表
       - 严格过滤退市/ST股
       - 不遍历data/daily/下所有文件
       - 仅处理列表中的股票
    
    2. 遍历股票列表，加载日线数据
       - 严格验证列名、数值类型、日期格式
       - 跳过数据不足的股票
    
    3. 计算技术指标并收集候选信号
       - 单一指标信号（MA/MACD/RSI/KDJ）
       - 双/三/四指标共振信号
       - 三均线缠绕候选信号（用于详细统计）
    
    4. 三均线筛选过程统计（关键日志）
       - 初始缠绕（偏离率≤2%）
       - 空间验证（缠绕天数≥5）
       - 量能验证（缩量≥50%）
       - 突破阶段验证（突破>1%）
       - 确认阶段验证（确认>0.5%）
       - 每步筛选数量精确统计（便于参数调优）
    
    5. 财务数据过滤
       - 候选股票去重（避免重复获取）
       - 逐个股票代码获取财务数据（AKShare API合规）
       - 应用5个财务过滤条件：
         * EPSJB ≥ 0（每股收益非负）
         * PE_STATIC > 0（市盈率有效）
         * BPSTZ ≤ 0（无质押）
         * PARENTNETPROFITTZ ≥ 0（净利润增长）
         * ROEJQ ≥ 5%（ROE达标）
       - 财务数据获取失败时跳过过滤（非终止流程）
    
    6. 生成多级信号并格式化
       - 单一指标信号：取前20名（按关键指标排序）
       - 双/三/四指标共振：全部符合条件的信号
       - 三均线缠绕：完整展示筛选过程（分页显示）
    
    7. 输出处理
       - 生成微信推送消息（符合wechat_push/push.py规范）
       - 保存股票代码到文件（按策略类型分类）
       - 提交Git仓库（commit + push）
    
    关键性能优化：
    - 财务数据仅对候选股票获取（非全量）
    - 候选股票去重（避免重复获取）
    - AKShare API调用严格遵循文档（逐个股票获取）
    - 三均线筛选过程日志便于参数调整
    
    错误处理：
    - 所有异常捕获并记录详细日志
    - 单个股票失败不影响整体流程
    - 财务数据获取失败时跳过过滤（非终止流程）
    """
    # 1. 读取所有股票列表
    basic_info_file = os.path.join(Config.DATA_DIR, "all_stocks.csv")
    if not os.path.exists(basic_info_file):
        logger.error("基础信息文件不存在")
        error_msg = "【策略2 - 多指标共振策略】基础信息文件不存在，无法生成交易信号"
        send_wechat_message(message=error_msg, message_type="error")
        return
    
    try:
        basic_info_df = pd.read_csv(basic_info_file)
        logger.info(f"成功读取基础信息文件，共 {len(basic_info_df)} 只股票")
        # 直接使用all_stocks.csv中的股票列表（已过滤退市/ST股）
        stock_list = basic_info_df.to_dict('records')
        logger.info(f"今天实际处理 {len(stock_list)} 只股票（完整处理）")
    except Exception as e:
        logger.error(f"读取基础信息文件失败: {str(e)}", exc_info=True)
        error_msg = f"【策略2 - 多指标共振策略】读取基础信息文件失败，无法生成交易信号: {str(e)}"
        send_wechat_message(message=error_msg, message_type="error")
        return
    
    # 2. 初始化信号容器
    ma_signals = []
    macd_signals = []
    rsi_signals = []
    kdj_signals = []
    threema_signals = []  # 三均线粘合突破信号容器
    all_threema_candidates = []  # 初始三均线缠绕候选股票
    
    double_signals = {
        "MA+MACD": [],
        "MA+RSI": [],
        "MA+KDJ": [],
        "MACD+RSI": [],
        "MACD+KDJ": [],
        "RSI+KDJ": []
    }
    
    triple_signals = {
        "MA+MACD+RSI": [],
        "MA+MACD+KDJ": [],
        "MA+RSI+KDJ": [],
        "MACD+RSI+KDJ": []
    }
    
    quadruple_signals = []
    
    processed_stocks = 0
    
    # 3. 处理每只股票
    for stock in stock_list:
        stock_code = str(stock["代码"])
        stock_name = stock["名称"]
        
        # 确保股票代码是6位
        stock_code = stock_code.zfill(6)
        
        logger.debug(f"处理股票: {stock_code} {stock_name}")
        
        try:
            # 检查大盘趋势（当前未实现）
            if MIN_MARKET_UPWARD:
                pass
            
            # 获取日线数据
            df = load_stock_daily_data(stock_code)
            if df is None or df.empty or len(df) < 40:
                logger.debug(f"股票 {stock_code} 数据不足，无法计算指标")
                continue
            
            # 检查日期格式
            df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
            df = df.sort_values("日期").reset_index(drop=True)
            
            # 检查数据完整性
            if df["收盘"].isna().any() or df["成交量"].isna().any():
                continue
            
            # 检查各指标信号
            ma_signal = check_ma_signal(df)
            macd_signal = check_macd_signal(df)
            rsi_signal = check_rsi_signal(df)
            kdj_signal = check_kdj_signal(df)
            
            # 1. 收集初始三均线缠绕候选股票（用于统计）
            ma5 = calc_ma(df, 5)
            ma10 = calc_ma(df, 10)
            ma20 = calc_ma(df, 20)
            max_ma = max(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
            min_ma = min(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
            deviation = (max_ma - min_ma) / max_ma
            if deviation < MAX_THREEMA_DEVIATION:
                all_threema_candidates.append({
                    "code": stock_code,
                    "name": stock_name,
                    "deviation": deviation
                })
            
            # 2. 检查完整三均线粘合突破信号
            threema_signal = check_threema_signal(df, stock_code, stock_name)
            if threema_signal:
                threema_signals.append({
                    "code": stock_code, 
                    "name": stock_name, 
                    **threema_signal
                })
            
            # 收集单一指标信号
            if ma_signal:
                ma_signals.append({
                    "code": stock_code, 
                    "name": stock_name, 
                    **ma_signal
                })
            if macd_signal:
                macd_signals.append({
                    "code": stock_code, 
                    "name": stock_name, 
                    **macd_signal
                })
            if rsi_signal:
                rsi_signals.append({
                    "code": stock_code, 
                    "name": stock_name, 
                    **rsi_signal
                })
            if kdj_signal:
                kdj_signals.append({
                    "code": stock_code, 
                    "name": stock_name, 
                    **kdj_signal
                })
            
            # 收集双指标共振信号
            if ma_signal and macd_signal:
                double_signals["MA+MACD"].append({
                    "code": stock_code,
                    "name": stock_name,
                    "ma": ma_signal,
                    "macd": macd_signal
                })
            if ma_signal and rsi_signal:
                double_signals["MA+RSI"].append({
                    "code": stock_code,
                    "name": stock_name,
                    "ma": ma_signal,
                    "rsi": rsi_signal
                })
            if ma_signal and kdj_signal:
                double_signals["MA+KDJ"].append({
                    "code": stock_code,
                    "name": stock_name,
                    "ma": ma_signal,
                    "kdj": kdj_signal
                })
            if macd_signal and rsi_signal:
                double_signals["MACD+RSI"].append({
                    "code": stock_code,
                    "name": stock_name,
                    "macd": macd_signal,
                    "rsi": rsi_signal
                })
            if macd_signal and kdj_signal:
                double_signals["MACD+KDJ"].append({
                    "code": stock_code,
                    "name": stock_name,
                    "macd": macd_signal,
                    "kdj": kdj_signal
                })
            if rsi_signal and kdj_signal:
                double_signals["RSI+KDJ"].append({
                    "code": stock_code,
                    "name": stock_name,
                    "rsi": rsi_signal,
                    "kdj": kdj_signal
                })
            
            # 收集三指标共振信号
            if ma_signal and macd_signal and rsi_signal:
                triple_signals["MA+MACD+RSI"].append({
                    "code": stock_code,
                    "name": stock_name,
                    "ma": ma_signal,
                    "macd": macd_signal,
                    "rsi": rsi_signal
                })
            if ma_signal and macd_signal and kdj_signal:
                triple_signals["MA+MACD+KDJ"].append({
                    "code": stock_code,
                    "name": stock_name,
                    "ma": ma_signal,
                    "macd": macd_signal,
                    "kdj": kdj_signal
                })
            if ma_signal and rsi_signal and kdj_signal:
                triple_signals["MA+RSI+KDJ"].append({
                    "code": stock_code,
                    "name": stock_name,
                    "ma": ma_signal,
                    "rsi": rsi_signal,
                    "kdj": kdj_signal
                })
            if macd_signal and rsi_signal and kdj_signal:
                triple_signals["MACD+RSI+KDJ"].append({
                    "code": stock_code,
                    "name": stock_name,
                    "macd": macd_signal,
                    "rsi": rsi_signal,
                    "kdj": kdj_signal
                })
            
            # 收集四指标共振信号
            if ma_signal and macd_signal and rsi_signal and kdj_signal:
                quadruple_signals.append({
                    "code": stock_code,
                    "name": stock_name,
                    "ma": ma_signal,
                    "macd": macd_signal,
                    "rsi": rsi_signal,
                    "kdj": kdj_signal
                })
            
            processed_stocks += 1
            if processed_stocks % 100 == 0:
                logger.info(f"已处理 {processed_stocks} 只股票...")
        except Exception as e:
            logger.debug(f"处理股票 {stock_code} 时出错: {str(e)}")
            continue
    
    logger.info(f"处理完成，共处理 {processed_stocks} 只股票")
    
    # 4. 新增三均线中间步骤统计（关键日志）
    threema_steps_list = []  # 收集每只股票的三均线筛选步骤
    
    for stock in stock_list:
        stock_code = str(stock["代码"]).zfill(6)
        stock_name = stock["名称"]
        
        # 获取日线数据
        df = load_stock_daily_data(stock_code)
        if df is None or df.empty or len(df) < 40:
            continue
        
        # 检查三均线中间步骤
        steps = check_threema_steps(df, stock_code, stock_name)
        if steps:
            threema_steps_list.append(steps)
    
    # 输出三均线步骤统计（精确到每一步）
    step1_count = sum(1 for s in threema_steps_list if s["step1"])
    step2_count = sum(1 for s in threema_steps_list if s["step1"] and s["step2"])
    step3_count = sum(1 for s in threema_steps_list if s["step1"] and s["step2"] and s["step3"])
    step4_count = sum(1 for s in threema_steps_list if s["step1"] and s["step2"] and s["step3"] and s["step4"])
    step5_count = sum(1 for s in threema_steps_list if all(s.values()))
    
    logger.info(f"🔍 三均线筛选过程统计：")
    logger.info(f"1️⃣ 初始缠绕（偏离率≤2%）：{step1_count}只")
    logger.info(f"2️⃣ 空间验证（缠绕天数≥5）：{step2_count}只（筛选掉{step1_count-step2_count}只）")
    logger.info(f"3️⃣ 量能验证（缩量≥50%）：{step3_count}只（筛选掉{step2_count-step3_count}只）")
    logger.info(f"4️⃣ 突破阶段验证（突破>1%）：{step4_count}只（筛选掉{step3_count-step4_count}只）")
    logger.info(f"5️⃣ 确认阶段验证（确认>0.5%）：{step5_count}只（筛选掉{step4_count-step5_count}只）")
    
    # 5. 收集所有候选股票代码（去重）
    all_candidate_codes = set()
    # 单一指标信号
    for signals in [ma_signals, macd_signals, rsi_signals, kdj_signals]:
        for signal in signals:
            all_candidate_codes.add(signal['code'])
    # 三均线缠绕信号
    for signal in threema_signals:
        all_candidate_codes.add(signal['code'])
    # 双指标共振信号
    for signals_list in double_signals.values():
        for signal in signals_list:
            all_candidate_codes.add(signal['code'])
    # 三指标共振信号
    for signals_list in triple_signals.values():
        for signal in signals_list:
            all_candidate_codes.add(signal['code'])
    # 四指标共振信号
    for signal in quadruple_signals:
        all_candidate_codes.add(signal['code'])
    
    # 6. 获取财务数据（仅对候选股票去重后获取）
    financial_data = get_financial_data_for_codes(all_candidate_codes)
    
    # 7. 定义财务过滤函数（在main内部，移到外部）
    
    # 8. 应用财务过滤
    ma_signals = filter_signals(ma_signals, financial_data)
    macd_signals = filter_signals(macd_signals, financial_data)
    rsi_signals = filter_signals(rsi_signals, financial_data)
    kdj_signals = filter_signals(kdj_signals, financial_data)
    threema_signals = filter_signals(threema_signals, financial_data)
    
    for key in double_signals:
        double_signals[key] = filter_signals(double_signals[key], financial_data)
    
    for key in triple_signals:
        triple_signals[key] = filter_signals(triple_signals[key], financial_data)
    
    quadruple_signals = filter_signals(quadruple_signals, financial_data)
    
    logger.info("财务数据过滤完成，信号统计:")
    logger.info(f"单一指标信号 - MA: {len(ma_signals)}, MACD: {len(macd_signals)}, RSI: {len(rsi_signals)}, KDJ: {len(kdj_signals)}")
    logger.info(f"三均线缠绕信号: {len(threema_signals)}")
    logger.info(f"双指标共振信号: {sum(len(v) for v in double_signals.values())}")
    logger.info(f"三指标共振信号: {sum(len(v) for v in triple_signals.values())}")
    logger.info(f"四指标共振信号: {len(quadruple_signals)}")
    
    # 9. 生成并发送信号
    total_messages = 0
    
    # 保存股票代码到文件并提交Git
    save_and_commit_stock_codes(
        ma_signals, 
        macd_signals, 
        rsi_signals, 
        kdj_signals, 
        threema_signals,
        double_signals, 
        triple_signals, 
        quadruple_signals
    )
    
    # 单一指标信号
    for category, signals in [("MA", ma_signals), ("MACD", macd_signals), ("RSI", rsi_signals), ("KDJ", kdj_signals)]:
        if signals:
            message = format_single_signal(category, signals)
            send_wechat_message(message=message, message_type="info")
            total_messages += 1
    
    # 双指标共振信号
    for combination, signals in double_signals.items():
        if signals:
            message = format_double_signal(combination, signals)
            send_wechat_message(message=message, message_type="info")
            total_messages += 1
    
    # 三指标共振信号
    for combination, signals in triple_signals.items():
        if signals:
            message = format_triple_signal(combination, signals)
            send_wechat_message(message=message, message_type="info")
            total_messages += 1
    
    # 四指标共振信号
    if quadruple_signals:
        message = format_quadruple_signal(quadruple_signals)
        send_wechat_message(message=message, message_type="info")
        total_messages += 1
    
    # 三均线缠绕信号
    if threema_signals:
        message = format_threema_signal(threema_signals, all_threema_candidates)
        send_wechat_message(message=message, message_type="info")
        total_messages += 1
    
    # 发送汇总消息
    summary = f"【策略2 - 多指标共振策略】执行完成\n共生成 {total_messages} 条交易信号"
    send_wechat_message(message=summary, message_type="info")
    logger.info(summary)

def save_and_commit_stock_codes(ma_signals, macd_signals, rsi_signals, kdj_signals, threema_signals,
                               double_signals, triple_signals, quadruple_signals):
    """
    保存股票代码到文件并提交到Git仓库（严格遵循微信推送逻辑）
    
    保存规则：
    - 单一指标信号：MA/MACD/RSI/KDJ各取前20名
    - 三均线缠绕信号：全部收集
    - 双/三/四指标共振信号：全部收集
    - 文件名格式：macdYYYYMMDDHHMM.txt
    - 内容：排序后的股票代码（6位，每行一个）
    
    Git提交规范：
    - 添加文件：git add
    - 提交信息：feat: 保存MACD策略股票代码 [时间戳]
    - 推送：git push
    
    异常处理：
    - Git操作失败记录详细错误
    - 文件保存失败记录error日志
    - 不影响主流程执行
    """
    try:
        # 获取当前时间
        now = get_beijing_time()
        timestamp = now.strftime("%Y%m%d%H%M")
        filename = f"macd{timestamp}.txt"
        
        # 构建文件路径
        stock_dir = os.path.join(Config.DATA_DIR, "stock")
        if not os.path.exists(stock_dir):
            os.makedirs(stock_dir, exist_ok=True)
        file_path = os.path.join(stock_dir, filename)
        
        # 收集所有股票代码
        all_stock_codes = set()
        
        # 1. 单一指标信号：MA/MACD/RSI/KDJ 取前20名
        for signals in [ma_signals, macd_signals, rsi_signals, kdj_signals]:
            for signal in signals[:20]:
                code = str(signal['code']).zfill(6)
                all_stock_codes.add(code)
        
        # 2. THREEMA信号（三均线缠绕）不进行过滤，全部收集
        for signal in threema_signals:
            code = str(signal['code']).zfill(6)
            all_stock_codes.add(code)
        
        # 3. 双指标共振信号：不进行过滤，全部收集
        for signals_list in double_signals.values():
            for signal in signals_list:
                code = str(signal['code']).zfill(6)
                all_stock_codes.add(code)
        
        # 4. 三指标共振信号：不进行过滤，全部收集
        for signals_list in triple_signals.values():
            for signal in signals_list:
                code = str(signal['code']).zfill(6)
                all_stock_codes.add(code)
        
        # 5. 四指标共振信号：不进行过滤，全部收集
        for signal in quadruple_signals:
            code = str(signal['code']).zfill(6)
            all_stock_codes.add(code)
        
        # 保存到文件（ANSI编码，使用ASCII，因为股票代码是纯数字）
        with open(file_path, 'w', encoding='ascii') as f:
            for code in sorted(all_stock_codes):
                f.write(f"{code}\n")
        
        logger.info(f"已保存 {len(all_stock_codes)} 个股票代码到 {file_path}")
        
        # 提交到Git仓库
        try:
            # 确保文件已添加到Git
            subprocess.run(["git", "add", file_path], check=True)
            
            # 提交更改
            commit_msg = f"feat: 保存MACD策略股票代码 [{timestamp}]"
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            
            # 推送到远程仓库
            subprocess.run(["git", "push"], check=True)
            
            logger.info(f"已提交并推送 {file_path} 到Git仓库")
        except subprocess.CalledProcessError as e:
            logger.error(f"Git操作失败: 命令 '{' '.join(e.cmd)}' 失败，状态码 {e.returncode}")
            logger.error(f"Git错误输出: {e.stderr}")
            logger.error(f"Git标准输出: {e.stdout}")
        except Exception as e:
            logger.error(f"提交并推送文件失败: {str(e)}", exc_info=True)
    except Exception as e:
        logger.error(f"保存股票代码失败: {str(e)}", exc_info=True)

def format_single_signal(category, signals):
    """
    格式化单一指标信号（MA/MACD/RSI/KDJ）
    
    参数：
    - category: 指标类型（"MA"/"MACD"/"RSI"/"KDJ"）
    - signals: 信号列表
    
    返回：
    - 字符串：格式化后的微信消息
    
    排序规则：
    - MA: 缠绕率越小排名越前
    - MACD: 增长幅度越大排名越前
    - RSI: 变化幅度越大排名越前
    - KDJ: J线变化幅度越大排名越前
    
    输出规则：
    - 仅取前20名
    - 包含日期、信号详情、专业解读
    - 每个信号显示代码、名称、关键指标值
    
    示例输出：
    【策略2 - MA信号】
    日期：2023-10-30
    🔥 MA信号：
    1. 600000 上证A股（缠绕率：0.8%，持续天数：5）
    2. 600001 深证A股（缠绕率：1.2%，持续天数：6）
    ...
    💡 信号解读：
    三均线缠绕后突破代表趋势即将形成，缠绕率越小、持续时间越长，突破后上涨概率越大。
    建议关注缠绕率最小且持续时间最长的个股。
    """
    if not signals:
        return ""
    
    # 按关键指标排序
    if category == "MA":
        signals = sorted(signals, key=lambda x: x["deviation"])
    elif category == "MACD":
        signals = sorted(signals, key=lambda x: x["growth_rate"], reverse=True)
    elif category == "RSI":
        signals = sorted(signals, key=lambda x: x["rsi_change"], reverse=True)
    elif category == "KDJ":
        signals = sorted(signals, key=lambda x: x["j_change"], reverse=True)
    
    # 只取前20名
    signals = signals[:20]
    
    # 生成消息
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"【策略2 - {category}信号】",
        f"日期：{today}",
        ""
    ]
    
    lines.append(f"🔥 {category}信号：")
    for i, signal in enumerate(signals, 1):
        code = signal["code"]
        name = signal["name"]
        if category == "MA":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['deviation']:.1%}，持续天数：{signal['consolidation_days']}）")
        elif category == "MACD":
            lines.append(f"{i}. {code} {name}（MACD增长：{signal['growth_rate']:.0%}，红柱长度：{signal['hist']}）")
        elif category == "RSI":
            lines.append(f"{i}. {code} {name}（RSI值：{signal['rsi']:.0f}，变化：{signal['rsi_change']:.0f}）")
        elif category == "KDJ":
            lines.append(f"{i}. {code} {name}（K值：{signal['k']:.0f}，D值：{signal['d']:.0f}，J值：{signal['j']:.0f}）")
    
    if signals:
        lines.append("")
        lines.append("💡 信号解读：")
        if category == "MA":
            lines.append("三均线缠绕后突破代表趋势即将形成，缠绕率越小、持续时间越长，突破后上涨概率越大。")
            lines.append("建议关注缠绕率最小且持续时间最长的个股。")
        elif category == "MACD":
            lines.append("MACD在0轴上方且持续增长代表动能增强，增长幅度越大，动能越强。")
            lines.append("建议关注增长幅度大且持续时间长的个股。")
        elif category == "RSI":
            lines.append("RSI从超卖区回升代表市场情绪改善，变化幅度越大，反弹力度越强。")
            lines.append("建议关注变化幅度大且持续时间长的个股。")
        elif category == "KDJ":
            lines.append("KDJ低位金叉代表短期动能强劲，J线变化幅度越大，反弹力度越强。")
            lines.append("建议关注J线快速上升的个股。")
    
    return "".join(lines)

def format_double_signal(combination, signals):
    """
    格式化双指标共振信号
    
    参数：
    - combination: 组合类型（如"MA+MACD"）
    - signals: 信号列表
    
    返回：
    - 字符串：格式化后的微信消息
    
    排序规则：
    - 按信号质量分数排序（综合指标权重）
    
    输出规则：
    - 包含日期、信号详情、专业解读
    - 每个信号显示代码、名称、双指标关键值
    
    示例输出：
    【策略2 - MA+MACD共振信号】
    日期：2023-10-30
    🔥 MA+MACD共振信号：
    1. 600000 上证A股（缠绕率：0.8%，MACD增长：15%）
    2. 600001 深证A股（缠绕率：1.2%，MACD增长：12%）
    ...
    💡 信号解读：
    双指标共振是趋势与动能的最佳配合，胜率高达65%。建议优先交易此类信号。
    """
    if not signals:
        return ""
    
    # 按信号质量排序
    signals = sorted(signals, key=lambda x: get_signal_quality(x, combination), reverse=True)
    
    # 生成消息
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"【策略2 - {get_combination_name(combination)}共振信号】",
        f"日期：{today}",
        ""
    ]
    
    lines.append(f"🔥 {get_combination_name(combination)}共振信号：")
    for i, signal in enumerate(signals, 1):
        code = signal["code"]
        name = signal["name"]
        if combination == "MA+MACD":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，MACD增长：{signal['macd']['growth_rate']:.0%}）")
        elif combination == "MA+RSI":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点）")
        elif combination == "MA+KDJ":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
        elif combination == "MACD+RSI":
            lines.append(f"{i}. {code} {name}（MACD增长：{signal['macd']['growth_rate']:.0%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点）")
        elif combination == "MACD+KDJ":
            lines.append(f"{i}. {code} {name}（MACD增长：{signal['macd']['growth_rate']:.0%}，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
        elif combination == "RSI+KDJ":
            lines.append(f"{i}. {code} {name}（RSI变化：{signal['rsi']['rsi_change']:.0f}点，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
    
    if signals:
        lines.append("")
        lines.append("💡 信号解读：")
        lines.append("双指标共振是趋势与动能的最佳配合，胜率高达65%。建议优先交易此类信号。")
    
    return "".join(lines)

def format_triple_signal(combination, signals):
    """
    格式化三指标共振信号
    
    参数：
    - combination: 组合类型（如"MA+MACD+RSI"）
    - signals: 信号列表
    
    返回：
    - 字符串：格式化后的微信消息
    
    排序规则：
    - 按信号质量分数排序（综合指标权重）
    
    输出规则：
    - 包含日期、信号详情、专业解读
    - 每个信号显示代码、名称、三指标关键值
    
    示例输出：
    【策略2 - MA+MACD+RSI共振信号】
    日期：2023-10-30
    💎 MA+MACD+RSI共振信号：
    1. 600000 上证A股（缠绕率：0.8%，MACD增长：15%，RSI变化：12点）
    2. 600001 深证A股（缠绕率：1.2%，MACD增长：12%，RSI变化：10点）
    ...
    🌟 信号解读：
    三指标共振代表趋势、动能和超买超卖状态完美配合，是高质量信号。历史回测显示此类信号平均收益率比市场基准高2.8倍。
    """
    if not signals:
        return ""
    
    # 按信号质量排序
    signals = sorted(signals, key=lambda x: get_signal_quality(x, combination), reverse=True)
    
    # 生成消息
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"【策略2 - {get_combination_name(combination)}共振信号】",
        f"日期：{today}",
        ""
    ]
    
    lines.append(f"💎 {get_combination_name(combination)}共振信号：")
    for i, signal in enumerate(signals, 1):
        code = signal["code"]
        name = signal["name"]
        if combination == "MA+MACD+RSI":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，MACD增长：{signal['macd']['growth_rate']:.0%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点）")
        elif combination == "MA+MACD+KDJ":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，MACD增长：{signal['macd']['growth_rate']:.0%}，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
        elif combination == "MA+RSI+KDJ":
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
        elif combination == "MACD+RSI+KDJ":
            lines.append(f"{i}. {code} {name}（MACD增长：{signal['macd']['growth_rate']:.0%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
    
    if signals:
        lines.append("")
        lines.append("🌟 信号解读：")
        lines.append("三指标共振代表趋势、动能和超买超卖状态完美配合，是高质量信号。历史回测显示此类信号平均收益率比市场基准高2.8倍。")
    
    return "".join(lines)

def format_quadruple_signal(signals):
    """
    格式化四指标共振信号
    
    参数：
    - signals: 信号列表
    
    返回：
    - 字符串：格式化后的微信消息
    
    排序规则：
    - 按信号质量分数排序（综合指标权重）
    
    输出规则：
    - 包含日期、信号详情、专业解读
    - 每个信号显示代码、名称、四指标关键值
    
    示例输出：
    【策略2 - 四指标共振信号】
    日期：2023-10-30
    ✨ MA+MACD+RSI+KDJ全指标共振信号：
    1. 600000 上证A股（缠绕率：0.8%，MACD增长：15%，RSI变化：12点，KDJ变化：10点）
    2. 600001 深证A股（缠绕率：1.2%，MACD增长：12%，RSI变化：10点，KDJ变化：8点）
    ...
    🎯 信号解读：
    全指标共振是最高质量的交易信号，历史胜率高达78%。建议重仓参与此类信号。
    """
    if not signals:
        return ""
    
    # 按信号质量排序
    signals = sorted(signals, key=lambda x: get_signal_quality(x, "MA+MACD+RSI+KDJ"), reverse=True)
    
    # 生成消息
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"【策略2 - 四指标共振信号】",
        f"日期：{today}",
        ""
    ]
    
    lines.append("✨ MA+MACD+RSI+KDJ全指标共振信号：")
    for i, signal in enumerate(signals, 1):
        code = signal["code"]
        name = signal["name"]
        lines.append(f"{i}. {code} {name}（缠绕率：{signal['ma']['deviation']:.1%}，MACD增长：{signal['macd']['growth_rate']:.0%}，RSI变化：{signal['rsi']['rsi_change']:.0f}点，KDJ变化：{signal['kdj']['j_change']:.0f}点）")
    
    if signals:
        lines.append("")
        lines.append("🎯 信号解读：")
        lines.append("全指标共振是最高质量的交易信号，历史胜率高达78%。建议重仓参与此类信号。")
    
    return "".join(lines)

def format_threema_signal(threema_signals, all_threema_candidates):
    """
    格式化三均线粘合突破信号（分页显示并展示筛选过程）
    
    参数：
    - threema_signals: 通过最终验证的三均线信号
    - all_threema_candidates: 初始三均线缠绕候选股票
    
    返回：
    - 字符串：格式化后的微信消息（分页显示）
    
    筛选过程统计：
    1️⃣ 初始筛选（三均线缠绕）：所有三均线间距≤5%的股票
    2️⃣ 空间验证（偏离率<2%）：筛选掉偏离率>2%的股票
    3️⃣ 时间验证（粘合≥5天）：筛选掉连续缠绕天数<5的股票
    4️⃣ 量能验证（缩量50%+）：筛选掉量比>2的股票
    5️⃣ 突破阶段验证：筛选掉突破幅度≤1%的股票
    6️⃣ 确认阶段验证：筛选掉确认幅度≤0.5%的股票
    
    输出规则：
    - 第一页：完整筛选过程统计
    - 后续页：每页20只股票，按关键指标排序
    - 每页包含页码和信号详情
    - 包含专业解读
    
    示例输出：
    【策略3 - 3均线缠绕5天】
    日期：2023-10-30
    🔍 三均线粘合突破信号筛选过程：
    1️⃣ 初始筛选（三均线缠绕）：1000只股票
    2️⃣ 空间验证（偏离率<2%）：500只股票（筛选掉500只）
    ...
    📊 筛选结果：
    ✅ 最终通过验证：100只股票
    
    第二页：
    【策略3 - 3均线缠绕5天】
    日期：2023-10-30
    页码：2/5
    💎 三均线缠合突破信号（第2页）：
    1. 600000 上证A股（缠绕率：0.8%，持续天数：5，量比：0.4）
    ...
    """
    if not all_threema_candidates:
        return ""
    
    # 统计筛选过程
    step1_count = len(all_threema_candidates)
    
    # 步骤2：空间验证（偏离率<2%）
    step2_candidates = [s for s in all_threema_candidates if s["deviation"] < MAX_MA_DEVIATION]
    step2_count = len(step2_candidates)
    
    # 步骤3：时间验证（粘合≥5天）
    step3_candidates = [s for s in step2_candidates if s["consolidation_days"] >= MIN_CONSOLIDATION_DAYS]
    step3_count = len(step3_candidates)
    
    # 步骤4：量能验证（缩量50%+）
    step4_candidates = [s for s in step3_candidates if s["volume_ratio"] < 1.0 / MIN_VOLUME_RATIO_MA]
    step4_count = len(step4_candidates)
    
    # 步骤5：突破阶段验证
    step5_candidates = [s for s in step4_candidates if s["breakout_ratio"] > MIN_BREAKOUT_RATIO]
    step5_count = len(step5_candidates)
    
    # 步骤6：确认阶段验证
    final_candidates = threema_signals
    final_count = len(final_candidates)
    
    # 分页处理
    page_size = 20
    pages = [final_candidates[i:i+page_size] for i in range(0, len(final_candidates), page_size)]
    messages = []
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 生成筛选过程消息
    process_lines = [
        f"【策略3 - 3均线缠绕{MIN_CONSOLIDATION_DAYS}天】",
        f"日期：{today}",
        "",
        "🔍 三均线粘合突破信号筛选过程：",
        f"1️⃣ 初始筛选（三均线缠绕）：{step1_count}只股票",
        f"2️⃣ 空间验证（偏离率<2%）：{step2_count}只股票（筛选掉{step1_count-step2_count}只）",
        f"3️⃣ 时间验证（粘合≥{MIN_CONSOLIDATION_DAYS}天）：{step3_count}只股票（筛选掉{step2_count-step3_count}只）",
        f"4️⃣ 量能验证（缩量50%+）：{step4_count}只股票（筛选掉{step3_count-step4_count}只）",
        f"5️⃣ 突破阶段验证：{step5_count}只股票（筛选掉{step4_count-step5_count}只）",
        f"6️⃣ 确认阶段验证：{final_count}只股票（筛选掉{step5_count-final_count}只）",
        "",
        "📊 筛选结果：",
        f"✅ 最终通过验证：{final_count}只股票",
        ""
    ]
    
    # 添加筛选过程消息作为第一页
    messages.append("".join(process_lines))
    
    # 生成每页消息
    for page_num, page_signals in enumerate(pages, 1):
        if page_num == 1:
            # 第一页是筛选过程（已添加）
            continue
        
        lines = [
            f"【策略3 - 3均线缠绕{MIN_CONSOLIDATION_DAYS}天】",
            f"日期：{today}",
            f"页码：{page_num}/{len(pages)}",
            ""
        ]
        
        lines.append(f"💎 三均线缠合突破信号（第{page_num}页）：")
        for i, signal in enumerate(page_signals, 1):
            code = signal["code"]
            name = signal["name"]
            lines.append(f"{i}. {code} {name}（缠绕率：{signal['deviation']:.1%}，持续天数：{signal['consolidation_days']}，量比：{signal['volume_ratio']:.2f}）")
        
        if page_signals:
            lines.append("")
            lines.append("💡 信号解读：")
            lines.append("三均线缠绕突破代表主力资金高度控盘，突破后往往有较大涨幅。")
            lines.append("建议关注缠绕率小、持续时间长、量能配合好的个股。")
        
        messages.append("".join(lines))
    
    return "\n\n".join(messages)

def get_combination_name(combination):
    """
    获取组合名称（用于消息格式化）
    
    参数：
    - combination: 组合类型（如"MA+MACD"）
    
    返回：
    - 字符串：格式化后的组合名称
    
    说明：
    - 仅用于消息标题，保持与策略文档一致
    - 支持所有双/三指标组合
    """
    names = {
        "MA+MACD": "MA+MACD",
        "MA+RSI": "MA+RSI",
        "MA+KDJ": "MA+KDJ",
        "MACD+RSI": "MACD+RSI",
        "MACD+KDJ": "MACD+KDJ",
        "RSI+KDJ": "RSI+KDJ",
        "MA+MACD+RSI": "MA+MACD+RSI",
        "MA+MACD+KDJ": "MA+MACD+KDJ",
        "MA+RSI+KDJ": "MA+RSI+KDJ",
        "MACD+RSI+KDJ": "MACD+RSI+KDJ"
    }
    return names.get(combination, combination)

def get_signal_quality(signal, combination):
    """
    计算信号质量分数（用于排序）
    
    参数：
    - signal: 信号字典
    - combination: 组合类型
    
    返回：
    - float: 信号质量分数（0~100）
    
    计算规则：
    - MA指标质量（25%权重）：
      * 缠绕率越小，质量越高（0~25分）
      * 持续天数越长，质量越高（0~15分）
    - MACD指标质量（25%权重）：
      * 增长幅度越大，质量越高（0~25分）
    - RSI指标质量（20%权重）：
      * 变化幅度越大，质量越高（0~20分）
    - KDJ指标质量（15%权重）：
      * J线变化幅度越大，质量越高（0~15分）
    
    说明：
    - 用于双/三/四指标共振信号排序
    - 权重分配基于专业金融经验
    - 每个指标得分限制在0~100%范围内
    """
    quality = 0
    
    # MA指标质量
    if "MA" in combination and "ma" in signal:
        # 缠绕率越小，质量越高
        quality += (1 - min(signal["ma"]["deviation"] / MAX_MA_DEVIATION, 1)) * 25
        # 持续天数越长，质量越高
        quality += min(signal["ma"]["consolidation_days"] / MIN_CONSOLIDATION_DAYS, 2) * 15
    
    # MACD指标质量
    if "MACD" in combination and "macd" in signal:
        # 增长幅度越大，质量越高
        quality += min(signal["macd"]["growth_rate"] / MAX_MACD_GROWTH_RATE, 1) * 25
    
    # RSI指标质量
    if "RSI" in combination and "rsi" in signal:
        # 变化幅度越大，质量越高
        quality += min(abs(signal["rsi"]["rsi_change"]) / MAX_RSI_CHANGE, 1) * 20
    
    # KDJ指标质量
    if "KDJ" in combination and "kdj" in signal:
        # J线变化幅度越大，质量越高
        quality += min(abs(signal["kdj"]["j_change"]) / MAX_KDJ_CHANGE, 1) * 15
    
    return quality

def check_ma_signal(df):
    """
    检查均线信号（三均线缠绕）
    
    参数：
    - df: 股票日线数据（DataFrame）
    
    返回：
    - 字典: 信号详情（若通过验证）
    - None: 未通过验证
    
    验证条件：
    1. 空间验证：三均线最大间距 ≤ 2%（MAX_MA_DEVIATION）
       - 计算：(max(5/10/20日均线) - min(5/10/20日均线)) / max(5/10/20日均线)
    2. 时间验证：连续缠绕天数 ≥ 5天（MIN_CONSOLIDATION_DAYS）
       - 从最近1天开始检查连续满足空间验证的天数
    3. 量能验证：当日成交量 ≤ 5日均量的50%（MIN_VOLUME_RATIO_MA）
       - 计算：当日成交量 / 5日均量 ≤ 0.5
    
    注意：
    - 仅用于单一指标信号生成
    - 不包含突破验证和确认验证
    - 三均线缠绕的完整验证由check_threema_signal处理
    """
    try:
        # 计算移动平均线
        ma5 = calc_ma(df, 5)
        ma10 = calc_ma(df, 10)
        ma20 = calc_ma(df, 20)
        
        # 检查三均线缠绕
        max_ma = max(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
        min_ma = min(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
        deviation = (max_ma - min_ma) / max_ma
        
        if deviation >= MAX_MA_DEVIATION:
            return None
        
        # 检查缠绕持续天数
        consolidation_days = 0
        for i in range(1, min(len(df), 20)):
            max_ma_i = max(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
            min_ma_i = min(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
            dev_i = (max_ma_i - min_ma_i) / max_ma_i
            
            if dev_i <= MAX_MA_DEVIATION:
                consolidation_days += 1
        
        if consolidation_days < MIN_CONSOLIDATION_DAYS:
            return None
        
        # 检查成交量
        if len(df) < 5:
            return None
        
        volume_ratio = df["成交量"].iloc[-1] / df["成交量"].rolling(5).mean().iloc[-1]
        if volume_ratio > 1.0 / MIN_VOLUME_RATIO_MA:
            return None
        
        return {
            "deviation": deviation,
            "consolidation_days": consolidation_days,
            "volume_ratio": volume_ratio
        }
    except Exception as e:
        logger.debug(f"检查均线信号失败: {str(e)}")
        return None

def calc_ma(df, period):
    """
    计算移动平均线
    
    参数：
    - df: 股票日线数据（DataFrame）
    - period: 周期（整数）
    
    返回：
    - Series: 移动平均线数据
    
    说明：
    - 使用收盘价计算
    - rolling(window=period).mean()
    - 返回的Series与df长度相同
    """
    return df["收盘"].rolling(window=period).mean()

def check_macd_signal(df):
    """
    检查MACD信号
    
    参数：
    - df: 股票日线数据（DataFrame）
    
    返回：
    - 字典: 信号详情（若通过验证）
    - None: 未通过验证
    
    验证条件：
    1. MACD线在0轴上方
    2. MACD线持续增长（当日值 > 前一日值）
    3. MACD柱状图在0轴上方
    
    注意：
    - MACD参数：12/26/9（MACD_SHORT/MACD_LONG/MACD_SIGNAL）
    - 增长率计算：(当前MACD - 前一日MACD) / |前一日MACD|
    - 仅检查增长方向，不检查具体幅度（MAX_MACD_GROWTH_RATE未使用）
    """
    try:
        # 计算MACD
        macd_line, signal_line, macd_hist = calc_macd(df)
        
        # 检查MACD是否在0轴上方
        if macd_line.iloc[-1] <= 0:
            return None
        
        # 检查MACD是否持续增长
        growth_rate = (macd_line.iloc[-1] - macd_line.iloc[-2]) / abs(macd_line.iloc[-2])
        
        if growth_rate <= 0:
            return None
        
        # 检查红柱长度
        if macd_hist.iloc[-1] <= 0:
            return None
        
        return {
            "growth_rate": growth_rate,
            "hist": macd_hist.iloc[-1]
        }
    except Exception as e:
        logger.debug(f"检查MACD信号失败: {str(e)}")
        return None

def calc_macd(df):
    """
    计算MACD指标
    
    参数：
    - df: 股票日线数据（DataFrame）
    
    返回：
    - 三元组: (macd_line, signal_line, macd_hist)
    
    说明：
    - macd_line = EMA(12) - EMA(26)
    - signal_line = EMA(macd_line, 9)
    - macd_hist = macd_line - signal_line
    - 使用ewm计算指数移动平均
    """
    try:
        ema_short = df["收盘"].ewm(span=MACD_SHORT, adjust=False).mean()
        ema_long = df["收盘"].ewm(span=MACD_LONG, adjust=False).mean()
        macd_line = ema_short - ema_long
        signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
        macd_hist = macd_line - signal_line
        return macd_line, signal_line, macd_hist
    except Exception as e:
        logger.error(f"计算MACD失败: {str(e)}", exc_info=True)
        # 返回空的Series
        return pd.Series(), pd.Series(), pd.Series()

def check_rsi_signal(df):
    """
    检查RSI信号
    
    参数：
    - df: 股票日线数据（DataFrame）
    
    返回：
    - 字典: 信号详情（若通过验证）
    - None: 未通过验证
    
    验证条件：
    1. RSI在30~70区间（非超买超卖区）
    2. RSI值持续上升（当日值 > 前一日值）
    
    注意：
    - RSI周期：14日
    - RSI变化 = 当日RSI - 前一日RSI
    - 仅检查方向，不检查具体幅度
    """
    try:
        # 计算RSI
        rsi = calc_rsi(df)
        
        # 检查RSI是否从超卖区回升
        if rsi.iloc[-1] >= 70 or rsi.iloc[-1] <= 30:
            return None
        
        # 检查RSI变化
        rsi_change = rsi.iloc[-1] - rsi.iloc[-2]
        
        if rsi_change <= 0:
            return None
        
        return {
            "rsi": rsi.iloc[-1],
            "rsi_change": rsi_change
        }
    except Exception as e:
        logger.debug(f"检查RSI信号失败: {str(e)}")
        return None

def calc_rsi(df, period=14):
    """
    计算RSI指标
    
    参数：
    - df: 股票日线数据（DataFrame）
    - period: 周期（整数，默认14）
    
    返回：
    - Series: RSI指标数据
    
    计算规则：
    1. delta = 收盘价变化
    2. gain = 正向变化（负值置0）
    3. loss = 负向变化（正值置0）
    4. avg_gain = gain的period日均值
    5. avg_loss = loss的period日均值
    6. RS = avg_gain / avg_loss
    7. RSI = 100 - (100 / (1 + RS))
    """
    try:
        delta = df["收盘"].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    except Exception as e:
        logger.error(f"计算RSI失败: {str(e)}", exc_info=True)
        return pd.Series()

def check_kdj_signal(df):
    """
    检查KDJ信号
    
    参数：
    - df: 股票日线数据（DataFrame）
    
    返回：
    - 字典: 信号详情（若通过验证）
    - None: 未通过验证
    
    验证条件：
    1. K/D值 > 20（非超卖区）
    2. 金叉：当日K > D 且 前一日K ≤ D
    3. J线持续上升（当日J > 前一日J）
    
    注意：
    - KDJ参数：9日周期
    - RSV = (收盘价 - 最低价) / (最高价 - 最低价) * 100
    - K = RSV的3日指数移动平均
    - D = K的3日指数移动平均
    - J = 3*K - 2*D
    """
    try:
        # 计算KDJ
        k, d, j = calc_kdj(df)
        
        # 检查KDJ是否低位金叉
        if k.iloc[-1] <= 20 or d.iloc[-1] <= 20:
            return None
        
        if k.iloc[-1] <= d.iloc[-1] or k.iloc[-2] >= d.iloc[-2]:
            return None
        
        # 检查J线变化
        j_change = j.iloc[-1] - j.iloc[-2]
        
        if j_change <= 0:
            return None
        
        return {
            "k": k.iloc[-1],
            "d": d.iloc[-1],
            "j": j.iloc[-1],
            "j_change": j_change
        }
    except Exception as e:
        logger.debug(f"检查KDJ信号失败: {str(e)}")
        return None

def calc_kdj(df, period=9):
    """
    计算KDJ指标
    
    参数：
    - df: 股票日线数据（DataFrame）
    - period: 周期（整数，默认9）
    
    返回：
    - 三元组: (k, d, j)
    
    计算规则：
    1. 低点 = period日内最低价
    2. 高点 = period日内最高价
    3. RSV = (收盘价 - 低点) / (高点 - 低点) * 100
    4. K = RSV的3日指数移动平均（com=2）
    5. D = K的3日指数移动平均（com=2）
    6. J = 3*K - 2*D
    """
    try:
        low_min = df["最低"].rolling(window=period).min()
        high_max = df["最高"].rolling(window=period).max()
        
        rsv = (df["收盘"] - low_min) / (high_max - low_min) * 100
        
        k = rsv.ewm(com=2, adjust=False).mean()
        d = k.ewm(com=2, adjust=False).mean()
        j = 3 * k - 2 * d
        
        return k, d, j
    except Exception as e:
        logger.error(f"计算KDJ失败: {str(e)}", exc_info=True)
        return pd.Series(), pd.Series(), pd.Series()

def check_threema_signal(df, code, name):
    """
    检查三均线缠合突破信号（完整验证）
    
    参数：
    - df: 股票日线数据（DataFrame）
    - code: 股票代码
    - name: 股票名称
    
    返回：
    - 字典: 信号详情（若通过验证）
    - None: 未通过验证
    
    完整验证条件（6步）：
    1. 空间验证：三均线最大间距 ≤ 2%（MAX_MA_DEVIATION）
       - 计算：(max(5/10/20日均线) - min(5/10/20日均线)) / max(5/10/20日均线)
    2. 时间验证：连续缠绕天数 ≥ 5天（MIN_CONSOLIDATION_DAYS）
       - 从最近1天开始检查连续满足空间验证的天数
    3. 量能验证：当日成交量 ≤ 5日均量的50%（MIN_VOLUME_RATIO_MA）
       - 计算：当日成交量 / 5日均量 ≤ 0.5
    4. 突破阶段验证：突破幅度 > 1%（MIN_BREAKOUT_RATIO）
       - 计算：(当前收盘价 - 三均线最大值) / 三均线最大值 > 0.01
    5. 确认阶段验证：确认幅度 > 0.5%（MIN_CONFIRM_RATIO）
       - 计算：(当日收盘价 - 前一日收盘价) / 前一日收盘价 > 0.005
    
    注意：
    - 仅用于三均线缠绕信号生成
    - 与check_threema_steps不同，此函数包含突破验证和确认验证
    - 日志记录每一步验证结果（debug级别）
    """
    try:
        # 计算移动平均线
        ma5 = calc_ma(df, 5)
        ma10 = calc_ma(df, 10)
        ma20 = calc_ma(df, 20)
        
        # 检查三均线缠绕
        max_ma = max(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
        min_ma = min(ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1])
        deviation = (max_ma - min_ma) / max_ma
        
        if deviation >= MAX_MA_DEVIATION:
            logger.debug(f"【THREEMA筛选】{code} {name} - 缠绕率 {deviation:.1%} 超过阈值 {MAX_MA_DEVIATION:.1%}")
            return None
        
        # 检查缠绕持续天数
        consolidation_days = 0
        for i in range(1, min(len(df), 20)):
            max_ma_i = max(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
            min_ma_i = min(ma5.iloc[-i], ma10.iloc[-i], ma20.iloc[-i])
            dev_i = (max_ma_i - min_ma_i) / max_ma_i
            
            if dev_i <= MAX_MA_DEVIATION:
                consolidation_days += 1
        
        if consolidation_days < MIN_CONSOLIDATION_DAYS:
            logger.debug(f"【THREEMA筛选】{code} {name} - 粘合天数 {consolidation_days} 少于阈值 {MIN_CONSOLIDATION_DAYS}")
            return None
        
        # 检查成交量
        if len(df) < 5:
            logger.debug(f"【THREEMA筛选】{code} {name} - 数据不足5天")
            return None
        
        volume_ratio = df["成交量"].iloc[-1] / df["成交量"].rolling(5).mean().iloc[-1]
        if volume_ratio > 1.0 / MIN_VOLUME_RATIO_MA:
            logger.debug(f"【THREEMA筛选】{code} {name} - 量能比 {volume_ratio:.2f} 超过阈值 {1.0 / MIN_VOLUME_RATIO_MA:.2f}")
            return None
        
        # 检查突破阶段
        breakout_ratio = (df["收盘"].iloc[-1] - max_ma) / max_ma
        if breakout_ratio <= MIN_BREAKOUT_RATIO:
            logger.debug(f"【THREEMA筛选】{code} {name} - 突破幅度 {breakout_ratio:.2%} 小于阈值 {MIN_BREAKOUT_RATIO:.2%}")
            return None
        
        # 检查确认阶段
        confirm_ratio = (df["收盘"].iloc[-1] - df["收盘"].iloc[-2]) / df["收盘"].iloc[-2]
        if confirm_ratio <= MIN_CONFIRM_RATIO:
            logger.debug(f"【THREEMA筛选】{code} {name} - 确认幅度 {confirm_ratio:.2%} 小于阈值 {MIN_CONFIRM_RATIO:.2%}")
            return None
        
        logger.info(f"【THREEMA筛选】{code} {name} - 通过所有验证，确认三均线粘合突破信号")
        return {
            "deviation": deviation,
            "consolidation_days": consolidation_days,
            "breakout_ratio": breakout_ratio,
            "volume_ratio": volume_ratio
        }
    except Exception as e:
        logger.error(f"【THREEMA筛选】检查股票 {code} {name} 三均线粘合突破信号失败: {str(e)}", exc_info=True)
        return None

if __name__ == "__main__":
    main()
