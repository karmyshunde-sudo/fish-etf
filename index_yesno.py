#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#指数 Yes/No 策略执行器
#每天计算指定指数的策略信号并推送微信通知
# 使用的API接口:
# 1. baostock:
#    - bs.login() - 登录baostock
#    - bs.logout() - 退出baostock
#    - bs.query_history_k_data_plus() - 获取历史K线数据
# 2. yfinance:
#    - yf.download() - 下载历史数据
# 3. akshare:
#    - ak.index_zh_a_hist() - 获取A股指数历史行情数据
#    - ak.index_hk_hist() - 获取港股指数历史行情数据  
#    - ak.stock_hk_index_daily_em() - 获取东方财富港股指数行情数据
# 4. pandas:
#    - pd.to_datetime() - 转换日期格式
#    - pd.to_numeric() - 转换数值类型
#    - pd.DataFrame() - 创建数据框
# 5. numpy:
#    - np.isnan() - 检查NaN值
import os
import logging
import pandas as pd
import akshare as ak
import baostock as bs  # 用于A股指数数据
import time
import numpy as np
import random
import yfinance as yf  # 用于国际/港股/美股指数
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time
from wechat_push.push import send_wechat_message
# 初始化日志
logger = logging.getLogger(__name__)
#logger.setLevel(logging.INFO)
#handler = logging.StreamHandler()
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#handler.setFormatter(formatter)
#logger.addHandler(handler)

# =============== 指数配置区 (可在此处修改指数配置) ===============
# 格式: [switch, code, name, description, source, etfs]
# etfs格式: [[code1, name1, description1], [code2, name2, description2], ...]
INDICES_CONFIG = [
    [2, "GC=F", "1、伦敦金现(XAU)", "国际黄金价格", "yfinance", [["518880", "华安黄金ETF", "黄金基金"]]],
    [1, "^HSTECH", "2、恒生科技指数(HSTECH)", "港股科技龙头企业指数", "baostock", [["513130", "华夏恒生科技ETF", "恒生科技ETF"]]],
    [2, "^NDX", "3、纳斯达克100(NDX)", "美国科技股代表指数", "yfinance", [["159892", "华夏纳斯达克100ETF", "纳指科技"], ["513100", "国泰纳斯达克100ETF", "纳斯达克"]]],
    [2, "sh.000016", "4、上证50(SH000016)", "上证50蓝筹股指数", "baostock", [["510050", "华夏上证50ETF", "上证50ETF"]]],
    [2, "sh.000300", "5、沪深300(SH000300)", "A股大盘蓝筹股指数", "baostock", [["510300", "华泰柏瑞沪深300ETF", "沪深300ETF"]]],
    [1, "883418", "6、微盘股(SH883418)", "小微盘股票指数", "baostock", [["510530", "华夏中证500ETF", "微盘股ETF"]]],
    [2, "sz.399006", "7、创业板指(SZ399006)", "创业板龙头公司", "baostock", [["159915", "易方达创业板ETF", "创业板ETF"]]],
    [1, "000688", "8、科创50(SH000688)", "科创板龙头公司", "baostock", [["588000", "华夏科创50ETF", "科创50ETF"]]],
    [1, "899050", "9、北证50(BJ899050)", "北交所龙头公司", "baostock", [["515200", "华夏北证50ETF", "北证50ETF"]]],
    [2, "sh.000905", "10、中证500(SH000905)", "A股中小盘股指数", "baostock", [["510500", "南方中证500ETF", "中证500ETF"]]],
    [1, "HSCEI.HK", "11、恒生国企指数(HSCEI)", "港股国企指数", "baostock", [["510900", "易方达恒生国企ETF", "H股ETF"]]],
    [1, "932000", "12、中证2000(SH932000)", "中盘股指数", "baostock", [["561020", "南方中证2000ETF", "中证2000ETF"]]],
    [2, "sh.000852", "13、中证1000(SH000852)", "中盘股指数", "baostock", [["512100", "南方中证1000ETF", "中证1000ETF"]]],
    [2, "KWEB", "14、中概互联指数(HXC)", "海外上市中国互联网公司", "yfinance", [["513500", "易方达中概互联网ETF", "中概互联"]]],
    [2, "^HSI", "15、恒生综合指数(HSI)", "香港股市综合蓝筹指数", "yfinance", [["513400", "华夏恒生互联网ETF", "恒生ETF"]]]
]

# 将配置数组转换为原始的INDICES结构
INDICES = []
for config in INDICES_CONFIG:
    etfs = [{"code": e[0], "name": e[1], "description": e[2]} for e in config[5]]
    INDICES.append({
        "switch": config[0],
        "code": config[1],
        "name": config[2],
        "description": config[3],
        "source": config[4],
        "etfs": etfs
    })
# =============== 指数配置区结束 ===============

# =============== 消息配置区 ===============
# 消息模板，完全采用数组形式，与指数配置格式一致
# 格式: [signal_type, scenario_type, [message_line1, message_line2, ...]]
SCENARIO_MESSAGES = [
    ["YES", "initial_breakout", [
        "【首次突破】连续{consecutive}天站上20日均线，成交量放大{volume:.1f}%",
        "✅ 操作建议：",
        "  • 核心宽基ETF（{etf_code}）立即建仓30%",
        "  • 卫星行业ETF立即建仓20%",
        "  • 回调至5日均线（约{target_price:.2f}）可加仓20%",
        "⚠️ 止损：买入价下方5%（宽基ETF）或3%（高波动ETF）"
    ]],
    ["YES", "confirmed_breakout", [
        "【首次突破确认】连续{consecutive}天站上20日均线，成交量放大{volume:.1f}%",
        "✅ 操作建议：",
        "  • 核心宽基ETF（{etf_code}）可加仓至50%",
        "  • 卫星行业ETF可加仓至35%",
        "  • 严格跟踪5日均线作为止损位（约{target_price:.2f}）",
        "⚠️ 注意：若收盘跌破5日均线，立即减仓50%"
    ]],
    ["YES", "trend_stable", [
        "【趋势稳健】连续{consecutive}天站上20日均线，偏离率{deviation:.2f}%",
        "✅ 操作建议：",
        "  • 持仓不动，不新增仓位",
        "  • 跟踪止损上移至5日均线（约{target_price:.2f}）",
        "  • 若收盘跌破5日均线，减仓50%",
        "{pattern_msg}"
    ]],
    ["YES", "trend_strong", [
        "【趋势较强】连续{consecutive}天站上20日均线，偏离率{deviation:.2f}%",
        "✅ 操作建议：",
        "  • 观望，不新增仓位",
        "  • 逢高减仓10%-15%（{etf_code}）",
        "  • 若收盘跌破10日均线，减仓30%",
        "{pattern_msg}"
    ]],
    ["YES", "overbought", [
        "【超买风险】连续{consecutive}天站上20日均线，偏离率{deviation:.2f}%",
        "✅ 操作建议：",
        "  • 逢高减仓20%-30%（仅卫星ETF）",
        "  • 当前价格已处高位，避免新增仓位",
        "  • 等待偏离率回落至≤+5%（约{target_price:.2f}）时加回",
        "{pattern_msg}"
    ]],
    ["NO", "initial_breakdown", [
        "【首次跌破】连续{consecutive}天跌破20日均线，成交量放大{volume:.1f}%",
        "✅ 操作建议：",
        "  • 核心宽基ETF（{etf_code}）立即减仓50%",
        "  • 卫星行业ETF立即减仓70%-80%",
        "  • 止损位：20日均线上方5%（约{target_price:.2f}）",
        "⚠️ 若收盘未收回均线，明日继续减仓至20%"
    ]],
    ["NO", "confirmed_breakdown", [
        "【首次跌破确认】连续{consecutive}天跌破20日均线，成交量放大{volume:.1f}%",
        "✅ 操作建议：",
        "  • 核心宽基ETF（{etf_code}）严格止损清仓",
        "  • 卫星行业ETF仅保留20%-30%底仓",
        "  • 严格止损：20日均线下方5%（约{target_price:.2f}）",
        "⚠️ 信号确认，避免侥幸心理"
    ]],
    ["NO", "decline_initial", [
        "【下跌初期】连续{consecutive}天跌破20日均线，偏离率{deviation:.2f}%",
        "✅ 操作建议：",
        "  • 轻仓观望（仓位≤20%）",
        "  • 反弹至均线附近（约{target_price:.2f}）减仓剩余仓位",
        "  • 暂不考虑新增仓位",
        "⚠️ 重点观察：收盘站上5日均线，可轻仓试多"
    ]],
    ["NO", "decline_medium", [
        "【下跌中期】连续{consecutive}天跌破20日均线，偏离率{deviation:.2f}%",
        "✅ 操作建议：",
        "  • 空仓为主，避免抄底",
        "  • 仅核心宽基ETF（{etf_code}）可试仓5%-10%",
        "  • 严格止损：收盘跌破前低即离场",
        "⚠️ 重点观察：行业基本面是否有利空，有利空则清仓"
    ]],
    ["NO", "oversold", [
        "【超卖机会】连续{consecutive}天跌破20日均线，偏离率{deviation:.2f}%",
        "✅ 操作建议：",
        "  • 核心宽基ETF（{etf_code}）小幅加仓10%-15%",
        "  • 目标价：偏离率≥-5%（约{target_price:.2f}）",
        "  • 达到目标即卖出加仓部分",
        "⚠️ 重点观察：若跌破前低，立即止损"
    ]]
]

# 转换为字典结构以便于查找
SCENARIO_MESSAGES_DICT = {}
for message in SCENARIO_MESSAGES:
    signal_type = message[0]
    scenario_type = message[1]
    if signal_type not in SCENARIO_MESSAGES_DICT:
        SCENARIO_MESSAGES_DICT[signal_type] = {}
    SCENARIO_MESSAGES_DICT[signal_type][scenario_type] = message[2]
# =============== 消息配置区结束 ===============

# =============== 根据测试结果优化的数据源配置 ===============
# 测试成功的指数数据源映射
SUCCESSFUL_DATA_SOURCES = {
    # A股指数 - 通过akshare stock_zh_index_daily接口成功
    "000688": {"primary": "akshare", "code": "sh000688", "interface": "stock_zh_index_daily"},
    "399006": {"primary": "akshare", "code": "sz399006", "interface": "stock_zh_index_daily"},
    "000016": {"primary": "akshare", "code": "sh000016", "interface": "stock_zh_index_daily"},
    "000300": {"primary": "akshare", "code": "sh000300", "interface": "stock_zh_index_daily"},
    "000905": {"primary": "akshare", "code": "sh000905", "interface": "stock_zh_index_daily"},
    "000852": {"primary": "akshare", "code": "sh000852", "interface": "stock_zh_index_daily"},
    
    # 港股指数 - 通过yfinance ETF成功（注意：这是ETF，不是指数本身）
    "HSTECH": {"primary": "yfinance", "code": "3077.HK", "interface": "yfinance_etf"},
}

def get_optimized_data_source(index_code):
    """根据测试结果返回优化的数据源配置"""
    if index_code in SUCCESSFUL_DATA_SOURCES:
        return SUCCESSFUL_DATA_SOURCES[index_code]
    return None

# =============== 将指数代码转换为baostock要求的格式 ===============
def convert_index_code_to_baostock_format(code: str) -> str:
    """
    Args:
        code: 原始指数代码
    Returns:
        str: baostock格式的代码
    """
    # baostock指数代码格式映射
    code_mapping = {
        "^HSTECH": "hk.8075",  # 恒生科技指数
        "883418": "sh.883418",  # 微盘股指数
        "000688": "sh.000688",  # 科创50
        "899050": "bj.899050",  # 北证50
        "HSCEI.HK": "hk.8070",  # 恒生国企指数
        "932000": "sh.932000",  # 中证2000
        "GC=F": "",  # 黄金不在baostock中
        "^NDX": "",  # 纳斯达克不在baostock中
        "sh.000016": "sh.000016",  # 上证50
        "sh.000300": "sh.000300",  # 沪深300
        "sz.399006": "sz.399006",  # 创业板指
        "sh.000905": "sh.000905",  # 中证500
        "sh.000852": "sh.000852",  # 中证1000
        "KWEB": "",  # 中概互联不在baostock中
        "^HSI": "hk.800000"  # 恒生指数
    }
    
    return code_mapping.get(code, code)
# =============== 将指数代码转换为baostock要求的格式 ===============

# 策略参数
CRITICAL_VALUE_DAYS = 20  # 计算临界值的周期（20日均线）
DEVIATION_THRESHOLD = 0.02  # 偏离阈值（2%）
PATTERN_CONFIDENCE_THRESHOLD = 0.7  # 形态确认阈值（70%置信度）

def fetch_baostock_data_simplified(index_code: str, days: int = 250) -> pd.DataFrame:
    """
    简化的baostock数据获取函数（不包含登录退出）
    Args:
        index_code: 已转换的baostock格式代码
        days: 获取最近多少天的数据
    Returns:
        pd.DataFrame: 指数日线数据
    """
    try:
        # 如果代码为空，表示不支持该指数
        if not index_code:
            return pd.DataFrame()
            
        # 添加随机延时避免被封
        time.sleep(random.uniform(5.0, 8.0))
        
        # 计算日期范围
        end_date_dt = datetime.now()
        start_date_dt = end_date_dt - timedelta(days=days)
        start_date = start_date_dt.strftime("%Y-%m-%d")
        end_date = end_date_dt.strftime("%Y-%m-%d")
        
        logger.info(f"使用baostock获取指数 {index_code} 数据，时间范围: {start_date} 至 {end_date}")
        
        # 使用baostock获取数据（已经在外层登录）
        rs = bs.query_history_k_data_plus(index_code,
                                         "date,open,high,low,close,volume,amount",
                                         start_date=start_date,
                                         end_date=end_date,
                                         frequency="d",
                                         adjustflag="3")
        # 检查返回结果
        if rs.error_code != '0':
            logger.error(f"获取指数 {index_code} 数据失败: {rs.error_msg}")
            return pd.DataFrame()
            
        # 将数据转换为DataFrame
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        if not data_list:
            logger.warning(f"获取指数 {index_code} 数据为空")
            return pd.DataFrame()
            
        df = pd.DataFrame(data_list, columns=rs.fields)
        # 标准化列名和处理数据格式（保持原有逻辑）
        df = df.rename(columns={
            'date': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'volume': '成交量',
            'amount': '成交额'
        })
        df['日期'] = pd.to_datetime(df['日期'])
        
        # 确保价格列是数值类型
        price_columns = ['开盘', '最高', '最低', '收盘']
        for col in price_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # 确保成交量和成交额是数值类型
        volume_columns = ['成交量', '成交额']
        for col in volume_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # 删除包含NaN的行
        df = df.dropna(subset=price_columns)
        df = df.sort_values('日期').reset_index(drop=True)
        
        if len(df) <= 1:
            logger.warning(f"⚠️ 只获取到{len(df)}条数据，可能是当天数据，无法用于历史分析")
            return pd.DataFrame()
            
        logger.info(f"✅ 通过baostock成功获取到 {len(df)} 条指数数据")
        return df
        
    except Exception as e:
        logger.error(f"通过baostock获取指数 {index_code} 数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def fetch_yfinance_data(index_code: str, days: int = 250) -> pd.DataFrame:
    """
    从yfinance获取国际/港股/美股指数历史数据
    Args:
        index_code: 指数代码
        days: 获取最近多少天的数据
    Returns:
        pd.DataFrame: 指数日线数据
    """
    try:
        # 添加随机延时避免被封（5.0-8.0秒）
        time.sleep(random.uniform(5.0, 8.0))
        # 计算日期范围
        end_date_dt = datetime.now()
        start_date_dt = end_date_dt - timedelta(days=days)
        # 转换为字符串格式
        end_date = end_date_dt.strftime("%Y-%m-%d")
        start_date = start_date_dt.strftime("%Y-%m-%d")
        logger.info(f"使用yfinance获取指数 {index_code} 数据，时间范围: {start_date} 至 {end_date}")
        # 获取数据
        try:
            df = yf.download(index_code, start=start_date, end=end_date, auto_adjust=False)
            # 处理yfinance返回的MultiIndex列名
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
            if df.empty:
                logger.warning(f"yfinance获取指数 {index_code} 数据为空")
                return pd.DataFrame()
            # 标准化列名
            df = df.reset_index()
            df = df.rename(columns={
                'Date': '日期',
                'Open': '开盘',
                'High': '最高',
                'Low': '最低',
                'Close': '收盘',
                'Volume': '成交量'
            })
            # 确保日期列为datetime类型
            df['日期'] = pd.to_datetime(df['日期'])
            # 确保价格列是数值类型
            price_columns = ['开盘', '最高', '最低', '收盘']
            for col in price_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            # 确保成交量是数值类型
            if '成交量' in df.columns:
                df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
            # 添加成交额列（如果不存在）
            if '成交额' not in df.columns:
                df['成交额'] = np.nan
            # 删除包含NaN的行
            if '收盘' in df.columns:
                df = df.dropna(subset=['收盘'])
            # 排序
            df = df.sort_values('日期').reset_index(drop=True)
            # 检查数据量
            if len(df) <= 1:
                logger.warning(f"⚠️ 只获取到{len(df)}条数据，可能是当天数据，无法用于历史分析")
                return pd.DataFrame()
            logger.info(f"✅ 通过yfinance成功获取到 {len(df)} 条指数数据，日期范围: {df['日期'].min()} 至 {df['日期'].max()}")
            return df
        except Exception as e:
            logger.error(f"通过yfinance获取指数 {index_code} 数据失败: {str(e)}", exc_info=True)
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"获取指数 {index_code} 数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def fetch_akshare_data(index_code: str, days: int = 250) -> pd.DataFrame:
    """
    从akshare获取指数历史数据
    Args:
        index_code: 指数代码
        days: 获取最近多少天的数据
    Returns:
        pd.DataFrame: 指数日线数据
    """
    try:
        # 添加随机延时避免被封（5.0-8.0秒）
        time.sleep(random.uniform(5.0, 8.0))
        # 计算日期范围
        end_date_dt = datetime.now()
        start_date_dt = end_date_dt - timedelta(days=days)
        # 转换为字符串格式
        end_date = end_date_dt.strftime("%Y%m%d")
        start_date = start_date_dt.strftime("%Y%m%d")
        logger.info(f"使用akshare获取指数 {index_code} 数据，时间范围: {start_date} 至 {end_date}")
        # 尝试获取数据
        try:
            # 根据指数代码类型选择不同的akshare接口
            if index_code.startswith(('0', '3', '6')):  # A股指数
                df = ak.index_zh_a_hist(
                    symbol=index_code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date
                )
            elif index_code.startswith('H') or index_code.startswith('^'):  # 港股指数
                # 尝试恒生系列指数
                if 'HSI' in index_code or 'HSTECH' in index_code or 'HSCEI' in index_code:
                    df = ak.index_hk_hist(
                        symbol=index_code,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date
                    )
                else:
                    # 其他港股指数
                    df = ak.stock_hk_index_daily_em(
                        symbol=index_code,
                        start_date=start_date,
                        end_date=end_date
                    )
            elif index_code.startswith(('8', '9')):  # 其他A股指数
                df = ak.index_zh_a_hist(
                    symbol=index_code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date
                )
            else:
                # 默认处理
                df = ak.index_zh_a_hist(
                    symbol=index_code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date
                )
            if df.empty:
                logger.warning(f"通过akshare获取指数 {index_code} 数据为空")
                return pd.DataFrame()
            # 标准化列名
            df = df.rename(columns={
                'date': '日期',
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量',
                'amount': '成交额'
            })
            # 确保日期列为datetime类型
            df['日期'] = pd.to_datetime(df['日期'])
            # 确保价格列是数值类型
            price_columns = ['开盘', '最高', '最低', '收盘']
            for col in price_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            # 确保成交量和成交额是数值类型
            volume_columns = ['成交量', '成交额']
            for col in volume_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            # 删除包含NaN的行
            df = df.dropna(subset=['收盘'])
            # 排序
            df = df.sort_values('日期').reset_index(drop=True)
            # 检查数据量
            if len(df) <= 1:
                logger.warning(f"⚠️ 只获取到{len(df)}条数据，可能是当天数据，无法用于历史分析")
                return pd.DataFrame()
            logger.info(f"✅ 通过akshare成功获取到 {len(df)} 条指数数据，日期范围: {df['日期'].min()} 至 {df['日期'].max()}")
            return df
        except Exception as e:
            logger.error(f"通过akshare获取指数 {index_code} 数据失败: {str(e)}", exc_info=True)
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"获取指数 {index_code} 数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def fetch_akshare_stock_zh_index_daily(index_code: str) -> pd.DataFrame:
    """
    使用akshare的stock_zh_index_daily接口获取指数数据（测试成功的接口）
    Args:
        index_code: akshare格式的指数代码（如sh000688）
    Returns:
        pd.DataFrame: 指数日线数据
    """
    try:
        logger.info(f"使用akshare stock_zh_index_daily接口获取指数数据: {index_code}")
        df = ak.stock_zh_index_daily(symbol=index_code)
        if not df.empty:
            # 标准化列名
            df = df.reset_index()
            df = df.rename(columns={
                'date': '日期',
                'open': '开盘', 
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量'
            })
            # 确保日期列为datetime类型
            df['日期'] = pd.to_datetime(df['日期'])
            # 确保价格列是数值类型
            price_columns = ['开盘', '最高', '最低', '收盘']
            for col in price_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            # 确保成交量是数值类型
            if '成交量' in df.columns:
                df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
            # 添加成交额列（如果不存在）
            if '成交额' not in df.columns:
                df['成交额'] = np.nan
                
            logger.info(f"✅ 通过akshare stock_zh_index_daily成功获取 {len(df)} 条指数数据")
            return df
        else:
            logger.warning(f"akshare stock_zh_index_daily获取指数 {index_code} 数据为空")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"通过akshare stock_zh_index_daily获取指数 {index_code} 数据失败: {str(e)}")
        return pd.DataFrame()

# =============== 针对失败指数的增强数据获取函数 ===============
def fetch_failed_indices_enhanced(index_code: str, index_name: str, days: int = 250) -> tuple:
    """
    针对失败指数的增强数据获取函数，尝试多个akshare接口
    Args:
        index_code: 指数代码
        index_name: 指数名称
        days: 获取最近多少天的数据
    Returns:
        tuple: (DataFrame, 实际使用的数据源)
    """
    logger.info(f"开始增强获取失败指数 {index_name}({index_code}) 的数据")
    
    # 计算日期范围
    end_date_dt = datetime.now()
    start_date_dt = end_date_dt - timedelta(days=days)
    start_date = start_date_dt.strftime("%Y%m%d")
    end_date = end_date_dt.strftime("%Y%m%d")
    
    # 根据指数类型尝试不同的接口
    if index_code in ["883418", "932000", "899050"]:  # A股失败指数
        logger.info(f"为A股失败指数 {index_name}({index_code}) 尝试多个akshare接口")
        
        # 接口1: 尝试stock_zh_index_daily
        try:
            if index_code.startswith(('00', '88', '93')):
                market_code = f"sh{index_code}"
            elif index_code.startswith('399'):
                market_code = f"sz{index_code}"
            elif index_code.startswith('899'):
                market_code = f"bj{index_code}"
            else:
                market_code = index_code
                
            df = ak.stock_zh_index_daily(symbol=market_code)
            if not df.empty and len(df) >= CRITICAL_VALUE_DAYS:
                logger.info(f"✅ 通过stock_zh_index_daily成功获取 {index_name} 数据")
                df = df.reset_index()
                df = df.rename(columns={
                    'date': '日期',
                    'open': '开盘', 
                    'high': '最高',
                    'low': '最低',
                    'close': '收盘',
                    'volume': '成交量'
                })
                df['日期'] = pd.to_datetime(df['日期'])
                return df, "akshare_stock_zh_index_daily"
        except Exception as e:
            logger.warning(f"接口stock_zh_index_daily失败: {str(e)}")
        
        # 接口2: 尝试index_zh_a_hist
        try:
            df = ak.index_zh_a_hist(
                symbol=index_code,
                period="daily",
                start_date=start_date,
                end_date=end_date
            )
            if not df.empty and len(df) >= CRITICAL_VALUE_DAYS:
                logger.info(f"✅ 通过index_zh_a_hist成功获取 {index_name} 数据")
                df = df.rename(columns={
                    'date': '日期',
                    'open': '开盘',
                    'high': '最高',
                    'low': '最低',
                    'close': '收盘',
                    'volume': '成交量',
                    'amount': '成交额'
                })
                df['日期'] = pd.to_datetime(df['日期'])
                return df, "akshare_index_zh_a_hist"
        except Exception as e:
            logger.warning(f"接口index_zh_a_hist失败: {str(e)}")
        
        # 接口3: 尝试index_csindex_all (中证指数)
        try:
            df_all = ak.index_csindex_all()
            if not df_all.empty and '指数代码' in df_all.columns:
                # 检查是否包含目标指数
                if index_code in df_all['指数代码'].values:
                    logger.info(f"✅ 在index_csindex_all中找到 {index_name}，但需要单独获取历史数据")
                    # 这里可以进一步处理获取具体指数的历史数据
        except Exception as e:
            logger.warning(f"接口index_csindex_all失败: {str(e)}")
        
        # 接口4: 尝试index_stock_info
        try:
            df_info = ak.index_stock_info()
            if not df_info.empty and '指数代码' in df_info.columns:
                if index_code in df_info['指数代码'].values:
                    logger.info(f"✅ 在index_stock_info中找到 {index_name}，但需要单独获取历史数据")
        except Exception as e:
            logger.warning(f"接口index_stock_info失败: {str(e)}")
    
    elif index_code in ["HSTECH", "HSCEI", "HSI"]:  # 港股失败指数
        logger.info(f"为港股失败指数 {index_name}({index_code}) 尝试多个akshare接口")
        
        # 接口1: 尝试stock_hk_index_daily_em
        try:
            df = ak.stock_hk_index_daily_em(symbol=index_code)
            if not df.empty and len(df) >= CRITICAL_VALUE_DAYS:
                logger.info(f"✅ 通过stock_hk_index_daily_em成功获取 {index_name} 数据")
                df = df.rename(columns={
                    'date': '日期',
                    'open': '开盘',
                    'high': '最高',
                    'low': '最低',
                    'close': '收盘',
                    'volume': '成交量'
                })
                df['日期'] = pd.to_datetime(df['日期'])
                return df, "akshare_stock_hk_index_daily_em"
        except Exception as e:
            logger.warning(f"接口stock_hk_index_daily_em失败: {str(e)}")
        
        # 接口2: 尝试index_global_hist_em
        try:
            df = ak.index_global_hist_em(symbol=index_code)
            if not df.empty and len(df) >= CRITICAL_VALUE_DAYS:
                logger.info(f"✅ 通过index_global_hist_em成功获取 {index_name} 数据")
                df = df.rename(columns={
                    'date': '日期',
                    'open': '开盘',
                    'high': '最高',
                    'low': '最低',
                    'close': '收盘'
                })
                df['日期'] = pd.to_datetime(df['日期'])
                return df, "akshare_index_global_hist_em"
        except Exception as e:
            logger.warning(f"接口index_global_hist_em失败: {str(e)}")
        
        # 接口3: 尝试index_global_spot_em获取实时数据
        try:
            df_spot = ak.index_global_spot_em()
            if not df_spot.empty and '名称' in df_spot.columns:
                # 查找目标指数
                target_row = df_spot[df_spot['名称'].str.contains(index_code, na=False)]
                if not target_row.empty:
                    logger.info(f"✅ 在index_global_spot_em中找到 {index_name} 实时数据")
                    # 这里可以进一步处理获取历史数据
        except Exception as e:
            logger.warning(f"接口index_global_spot_em失败: {str(e)}")
    
    # 所有接口都失败
    logger.warning(f"所有增强接口都无法获取 {index_name}({index_code}) 数据")
    return pd.DataFrame(), "所有增强接口均失败"

def fetch_index_data_smart(index_info: dict, days: int = 250) -> tuple:
    """
    智能数据获取函数，当首选数据源失败时自动切换到备用数据源
    Args:
        index_info: 指数信息字典（包含code, name, source等）
        days: 获取最近多少天的数据
    Returns:
        tuple: (DataFrame, 实际使用的数据源)
    """
    code = index_info["code"]
    name = index_info["name"]
    preferred_source = index_info["source"]
    
    # 检查是否有测试成功的优化数据源
    optimized_source = get_optimized_data_source(code)
    if optimized_source:
        logger.info(f"为指数 {name}({code}) 使用测试成功的优化数据源: {optimized_source}")
        
        try:
            if optimized_source["primary"] == "akshare" and optimized_source["interface"] == "stock_zh_index_daily":
                df = fetch_akshare_stock_zh_index_daily(optimized_source["code"])
                if not df.empty and len(df) >= CRITICAL_VALUE_DAYS:
                    return df, f"akshare_stock_zh_index_daily({optimized_source['code']})"
            
            elif optimized_source["primary"] == "yfinance":
                df = fetch_yfinance_data(optimized_source["code"], days)
                if not df.empty and len(df) >= CRITICAL_VALUE_DAYS:
                    return df, f"yfinance({optimized_source['code']})"
                    
        except Exception as e:
            logger.warning(f"优化数据源获取失败，回退到原始逻辑: {str(e)}")
    
    # 原始的数据源获取逻辑
    data_sources = ["baostock", "akshare", "yfinance"]
    
    # 如果首选数据源不在优先级列表中，将其添加到最前面
    if preferred_source in data_sources:
        # 将首选数据源移到最前面
        data_sources.remove(preferred_source)
        data_sources.insert(0, preferred_source)
    else:
        # 如果首选数据源不在已知列表中，将其添加到最前面
        data_sources.insert(0, preferred_source)
    
    logger.info(f"为指数 {name}({code}) 尝试数据源顺序: {data_sources}")
    
    # 尝试每个数据源，直到成功获取数据
    for source in data_sources:
        logger.info(f"尝试使用 {source} 数据源获取 {name}({code}) 数据")
        
        try:
            if source == "baostock":
                # 转换代码为baostock格式
                baostock_code = convert_index_code_to_baostock_format(code)
                df = fetch_baostock_data_simplified(baostock_code, days)
            elif source == "yfinance":
                df = fetch_yfinance_data(code, days)
            elif source == "akshare":
                df = fetch_akshare_data(code, days)
            else:
                logger.warning(f"未知数据源: {source}")
                continue
            
            # 如果成功获取到数据
            if not df.empty and len(df) >= CRITICAL_VALUE_DAYS:
                logger.info(f"✅ 成功通过 {source} 获取到 {name} 数据")
                return df, source
            
            # 如果数据量不足，继续尝试下一个数据源
            if not df.empty and len(df) < CRITICAL_VALUE_DAYS:
                logger.warning(f"通过 {source} 获取的 {name} 数据量不足，继续尝试其他数据源")
                continue
                
        except Exception as e:
            logger.error(f"通过 {source} 获取 {name} 数据时发生异常: {str(e)}")
            continue
    
    # =============== 新增：如果常规数据源都失败，尝试增强数据获取 ===============
    logger.info(f"常规数据源全部失败，开始增强数据获取: {name}({code})")
    df, enhanced_source = fetch_failed_indices_enhanced(code, name, days)
    if not df.empty and len(df) >= CRITICAL_VALUE_DAYS:
        logger.info(f"✅ 通过增强接口成功获取 {name} 数据")
        return df, f"enhanced_{enhanced_source}"
    
    # 所有数据源都失败
    logger.error(f"所有数据源都无法获取 {name}({code}) 的有效数据")
    return pd.DataFrame(), "所有数据源均失败"

def calculate_critical_value(df: pd.DataFrame) -> float:
    """计算临界值（20日均线）"""
    if len(df) < CRITICAL_VALUE_DAYS:
        logger.warning(f"数据不足{CRITICAL_VALUE_DAYS}天，无法准确计算临界值")
        # 只计算非NaN值的均值
        valid_data = df["收盘"].dropna()
        return valid_data.mean() if not valid_data.empty else 0.0
    # 计算滚动均值，忽略NaN值
    ma = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS, min_periods=1).mean()
    # 返回最后一个有效值
    for i in range(len(ma)-1, -1, -1):
        if not np.isnan(ma.iloc[i]):
            return ma.iloc[i]
    return df["收盘"].dropna().mean()

def calculate_deviation(current: float, critical: float) -> float:
    """计算偏离率"""
    return (current - critical) / critical * 100

def calculate_consecutive_days_above(df: pd.DataFrame, critical_value: float) -> int:
    """计算连续站上均线的天数"""
    if len(df) < 2:
        return 0
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    # 计算均线（使用与主计算相同的逻辑）
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS, min_periods=1).mean().values
    # 从最新日期开始向前检查
    consecutive_days = 0
    for i in range(len(close_prices)-1, -1, -1):
        # 确保有足够的数据计算均线
        if i < CRITICAL_VALUE_DAYS - 1:
            # 使用当前计算的均线值
            if i < len(ma_values) and not np.isnan(ma_values[i]):
                if close_prices[i] >= ma_values[i]:
                    consecutive_days += 1
                else:
                    break
        else:
            # 使用计算出的均线值
            if not np.isnan(close_prices[i]) and not np.isnan(ma_values[i]) and close_prices[i] >= ma_values[i]:
                consecutive_days += 1
            else:
                break
    return consecutive_days

def calculate_consecutive_days_below(df: pd.DataFrame, critical_value: float) -> int:
    """计算连续跌破均线的天数"""
    if len(df) < 2:
        return 0
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    # 计算均线（使用与主计算相同的逻辑）
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS, min_periods=1).mean().values
    # 从最新日期开始向前检查
    consecutive_days = 0
    for i in range(len(close_prices)-1, -1, -1):
        # 确保有足够的数据计算均线
        if i < CRITICAL_VALUE_DAYS - 1:
            # 使用当前计算的均线值
            if i < len(ma_values) and not np.isnan(ma_values[i]):
                if close_prices[i] < ma_values[i]:
                    consecutive_days += 1
                else:
                    break
        else:
            # 使用计算出的均线值
            if not np.isnan(close_prices[i]) and not np.isnan(ma_values[i]) and close_prices[i] < ma_values[i]:
                consecutive_days += 1
            else:
                break
    return consecutive_days

def calculate_volume_change(df: pd.DataFrame) -> float:
    """
    计算成交量变化率
    Args:
        df: ETF日线数据
    Returns:
        float: 成交量变化率
    """
    try:
        if len(df) < 2:
            logger.warning("数据量不足，无法计算成交量变化")
            return 0.0
        # 获取最新两个交易日的成交量
        current_volume = df['成交量'].values[-1]
        previous_volume = df['成交量'].values[-2]
        # 确保是数值类型
        if not isinstance(current_volume, (int, float)) or not isinstance(previous_volume, (int, float)):
            try:
                current_volume = float(current_volume)
                previous_volume = float(previous_volume)
            except:
                logger.warning("成交量数据无法转换为数值类型")
                return 0.0
        # 检查NaN
        if np.isnan(current_volume) or np.isnan(previous_volume) or previous_volume <= 0:
            return 0.0
        # 计算变化率
        volume_change = (current_volume - previous_volume) / previous_volume
        return volume_change
    except Exception as e:
        logger.error(f"计算成交量变化失败: {str(e)}", exc_info=True)
        return 0.0

def calculate_loss_percentage(df: pd.DataFrame) -> float:
    """计算当前亏损比例（相对于最近一次买入点）"""
    if len(df) < 2:
        return 0.0
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    # 计算均线（使用与主计算相同的逻辑）
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS, min_periods=1).mean().values
    # 从最新日期开始向前检查，找到最近一次站上均线的点
    buy_index = -1
    for i in range(len(close_prices)-1, -1, -1):
        # 确保有足够的数据计算均线
        if i < CRITICAL_VALUE_DAYS - 1:
            # 使用当前计算的均线值
            if i < len(ma_values) and not np.isnan(ma_values[i]) and close_prices[i] >= ma_values[i]:
                buy_index = i
                break
        else:
            # 使用计算出的均线值
            if not np.isnan(close_prices[i]) and not np.isnan(ma_values[i]) and close_prices[i] >= ma_values[i]:
                buy_index = i
                break
    # 如果找不到买入点，使用30天前作为参考
    if buy_index == -1:
        buy_index = max(0, len(close_prices) - 30)
    current_price = close_prices[-1]
    buy_price = close_prices[buy_index]
    # 确保是有效数值
    if np.isnan(current_price) or np.isnan(buy_price) or buy_price <= 0:
        return 0.0
    loss_percentage = (current_price - buy_price) / buy_price * 100
    return loss_percentage

def is_in_volatile_market(df: pd.DataFrame) -> tuple:
    """判断是否处于震荡市"""
    if len(df) < 10:
        return False, 0, (0, 0)  # 中文名称通常2-4个字
    # 获取收盘价和均线序列
    close_prices = df["收盘"].values
    # 计算均线（使用与主计算相同的逻辑）
    ma_values = df["收盘"].rolling(window=CRITICAL_VALUE_DAYS, min_periods=1).mean().values
    # 检查是否连续10天在均线附近波动（-5%~+5%）
    last_10_days = df.tail(10)
    deviations = []
    for i in range(len(last_10_days)):
        # 确保有足够的数据计算均线
        if i < CRITICAL_VALUE_DAYS - 1:
            # 使用当前计算的均线值
            if i < len(ma_values) and not np.isnan(ma_values[i]):
                deviation = (close_prices[i] - ma_values[i]) / ma_values[i] * 100
                if not np.isnan(deviation) and abs(deviation) <= 5.0:
                    deviations.append(deviation)
                else:
                    return False, 0, (0, 0)
        else:
            # 使用计算出的均线值
            if not np.isnan(close_prices[i]) and not np.isnan(ma_values[i]):
                deviation = (close_prices[i] - ma_values[i]) / ma_values[i] * 100
                if not np.isnan(deviation) and abs(deviation) <= 5.0:
                    deviations.append(deviation)
                else:
                    return False, 0, (0, 0)
    # 检查价格是否反复穿越均线
    cross_count = 0
    for i in range(len(close_prices)-10, len(close_prices)-1):
        # 确保有足够的数据计算均线
        if i < CRITICAL_VALUE_DAYS - 1:
            # 使用当前计算的均线值
            if i < len(ma_values) and i+1 < len(ma_values) and not np.isnan(ma_values[i]) and not np.isnan(ma_values[i+1]):
                if (close_prices[i] >= ma_values[i] and close_prices[i+1] < ma_values[i+1]) or \
                   (close_prices[i] < ma_values[i] and close_prices[i+1] >= ma_values[i+1]):
                    cross_count += 1
            else:
                continue
        else:
            # 使用计算出的均线值
            if not np.isnan(close_prices[i]) and not np.isnan(close_prices[i+1]) and \
               not np.isnan(ma_values[i]) and not np.isnan(ma_values[i+1]):
                if (close_prices[i] >= ma_values[i] and close_prices[i+1] < ma_values[i+1]) or \
                   (close_prices[i] < ma_values[i] and close_prices[i+1] >= ma_values[i+1]):
                    cross_count += 1
            else:
                continue
    # 至少需要5次穿越才认定为震荡市
    min_cross_count = 5
    is_volatile = cross_count >= min_cross_count
    # 计算最近10天偏离率范围
    if deviations:
        min_deviation = min(deviations)
        max_deviation = max(deviations)
    else:
        min_deviation = 0
        max_deviation = 0
    return is_volatile, cross_count, (min_deviation, max_deviation)

def detect_head_and_shoulders(df: pd.DataFrame) -> dict:
    """检测M头和头肩顶形态"""
    if len(df) < 20:  # 需要足够数据
        return {"pattern_type": "无", "detected": False, "confidence": 0, "peaks": []}
    # 获取收盘价
    close_prices = df["收盘"].values
    # 寻找局部高点
    peaks = []
    for i in range(5, len(close_prices)-5):
        # 确保是有效数值
        if np.isnan(close_prices[i]) or i - 5 < 0 or i + 6 > len(close_prices):
            continue
        # 检查是否为局部高点
        is_peak = True
        for j in range(i-5, i):
            if j < 0 or np.isnan(close_prices[j]):
                continue
            if close_prices[i] <= close_prices[j]:
                is_peak = False
                break
        if not is_peak:
            continue
        for j in range(i+1, i+6):
            if j >= len(close_prices) or np.isnan(close_prices[j]):
                continue
            if close_prices[i] <= close_prices[j]:
                is_peak = False
                break
        if is_peak:
            peaks.append((i, close_prices[i]))
    # 如果找到的高点少于3个，无法形成头肩顶
    if len(peaks) < 3:
        return {"pattern_type": "无", "detected": False, "confidence": 0, "peaks": peaks}
    # 检测M头（两个高点）
    m_top_detected = False
    m_top_confidence = 0.0
    if len(peaks) >= 2:
        # 两个高点，第二个略低于第一个，中间有明显低点
        peak1_idx, peak1_price = peaks[-2]
        peak2_idx, peak2_price = peaks[-1]
        # 检查第二个高点是否低于第一个
        if peak2_price < peak1_price and peak2_price > peak1_price * 0.95:
            # 检查中间是否有明显低点
            if peak1_idx >= len(close_prices) or peak2_idx >= len(close_prices):
                return {"pattern_type": "无", "detected": False, "confidence": 0, "peaks": peaks}
            trough_idx = peak1_idx + np.argmin(close_prices[peak1_idx:peak2_idx])
            if trough_idx >= len(close_prices):
                return {"pattern_type": "无", "detected": False, "confidence": 0, "peaks": peaks}
            trough_price = close_prices[trough_idx]
            # 检查低点是否明显
            if trough_price < peak1_price * 0.97 and trough_price < peak2_price * 0.97:
                m_top_detected = True
                # 计算置信度
                price_diff = (peak1_price - peak2_price) / peak1_price
                trough_depth = (peak1_price - trough_price) / peak1_price
                m_top_confidence = 0.5 + 0.5 * min(price_diff / 0.05, 1) + 0.5 * min(trough_depth / 0.05, 1)
                m_top_confidence = min(m_top_confidence, 1.0)
    # 检测头肩顶（三个高点）
    head_and_shoulders_confidence = 0.0
    head_and_shoulders_detected = False
    if len(peaks) >= 3:
        # 三个高点，中间最高，两侧较低
        shoulder1_idx, shoulder1_price = peaks[-3]
        head_idx, head_price = peaks[-2]
        shoulder2_idx, shoulder2_price = peaks[-1]
        # 检查中间是否为最高点
        if head_price > shoulder1_price and head_price > shoulder2_price:
            # 检查两侧肩膀是否大致对称
            shoulder_similarity = min(shoulder1_price, shoulder2_price) / max(shoulder1_price, shoulder2_price)
            # 检查中间低点
            if shoulder1_idx >= len(close_prices) or head_idx >= len(close_prices) or shoulder2_idx >= len(close_prices):
                return {"pattern_type": "无", "detected": False, "confidence": 0, "peaks": peaks}
            trough1_idx = shoulder1_idx + np.argmin(close_prices[shoulder1_idx:head_idx])
            trough2_idx = head_idx + np.argmin(close_prices[head_idx:shoulder2_idx])
            if trough1_idx >= len(close_prices) or trough2_idx >= len(close_prices):
                return {"pattern_type": "无", "detected": False, "confidence": 0, "peaks": peaks}
            neckline_price = (close_prices[trough1_idx] + close_prices[trough2_idx]) / 2
            # 检查头肩比例是否合理
            if shoulder_similarity > 0.85 and head_price > neckline_price * 1.1:
                head_and_shoulders_detected = True
                # 计算置信度
                shoulder_diff = 1 - shoulder_similarity
                head_height = (head_price - neckline_price) / neckline_price
                head_and_shoulders_confidence = 0.5 + 0.3 * min(shoulder_diff / 0.15, 1) + 0.2 * min(head_height / 0.15, 1)
                head_and_shoulders_confidence = min(head_and_shoulders_confidence, 1.0)
    # 确定主要检测结果
    if head_and_shoulders_detected and head_and_shoulders_confidence > m_top_confidence:
        # 【关键修复】确保confidence是标量值
        return {
            "pattern_type": "头肩顶",
            "detected": True,
            "confidence": float(head_and_shoulders_confidence),
            "peaks": peaks[-3:] if len(peaks) >= 3 else peaks
        }
    elif m_top_detected:
        # 【关键修复】确保confidence是标量值
        return {
            "pattern_type": "M头",
            "detected": True,
            "confidence": float(m_top_confidence),
            "peaks": peaks[-2:]
        }
    else:
        return {
            "pattern_type": "无",
            "detected": False,
            "confidence": 0.0,
            "peaks": peaks[-3:] if len(peaks) >= 3 else peaks
        }

def generate_signal_message(index_info: dict, df: pd.DataFrame, current: float, critical: float, deviation: float) -> str:
    """生成策略信号消息"""
    # 计算连续站上/跌破均线的天数
    consecutive_above = calculate_consecutive_days_above(df, critical)
    consecutive_below = calculate_consecutive_days_below(df, critical)
    # 计算成交量变化
    volume_change = calculate_volume_change(df)
    # 检测M头/头肩顶形态
    pattern_detection = detect_head_and_shoulders(df)
    # 3. 震荡市判断 - 优先级最高
    is_volatile, cross_count, (min_dev, max_dev) = is_in_volatile_market(df)
    if is_volatile:
        # 计算上轨和下轨价格
        upper_band = critical * (1 + max_dev/100)
        lower_band = critical * (1 + min_dev/100)
        return (
            f"【震荡市】连续10日价格反复穿均线（穿越{cross_count}次），偏离率范围[{min_dev:.2f}%~{max_dev:.2f}%]\n"
            f"✅ 操作建议：\n"
            f"  • 上沿操作（价格≈{upper_band:.2f}）：小幅减仓10%-20%\n"
            f"  • 下沿操作（价格≈{lower_band:.2f}）：小幅加仓10%-20%\n"
            f"  • 总仓位严格控制在≤50%\n"
            f"⚠️ 避免频繁交易，等待趋势明朗"
        )
    # 1. YES信号：当前价格 ≥ 20日均线
    if current >= critical:
        # 子条件1：首次突破（价格刚站上均线，连续2-3日站稳+成交量放大20%+）
        if consecutive_above == 1 and volume_change > 0.2:
            return "\n".join(SCENARIO_MESSAGES_DICT["YES"]["initial_breakout"]).format(
                consecutive=consecutive_above,
                volume=volume_change*100,
                etf_code=index_info['etfs'][0]['code'],
                target_price=current * 0.99
            )
        # 子条件1：首次突破（价格刚站上均线，连续2-3日站稳+成交量放大20%+）
        elif 2 <= consecutive_above <= 3 and volume_change > 0.2:
            return "\n".join(SCENARIO_MESSAGES_DICT["YES"]["confirmed_breakout"]).format(
                consecutive=consecutive_above,
                volume=volume_change*100,
                etf_code=index_info['etfs'][0]['code'],
                target_price=current * 0.99
            )
        # 子条件2：持续站稳（价格维持在均线上）
        else:
            # 场景A：偏离率≤+5%（趋势稳健）
            if deviation <= 5.0:
                # 添加M头/头肩顶形态检测
                pattern_msg = ""
                if pattern_detection["detected"]:
                    pattern_name = pattern_detection["pattern_type"]
                    confidence = pattern_detection["confidence"]
                    # 【关键修复】确保confidence是标量值
                    confidence = float(confidence) if isinstance(confidence, (np.ndarray, np.float32)) else confidence
                    if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                        pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），建议减仓10%-15%"
                    elif confidence >= 0.5:
                        pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓5%-10%"
                return "\n".join(SCENARIO_MESSAGES_DICT["YES"]["trend_stable"]).format(
                    consecutive=consecutive_above,
                    deviation=deviation,
                    target_price=current * 0.99,
                    etf_code=index_info['etfs'][0]['code'],
                    pattern_msg=pattern_msg
                )
            # 场景B：+5%＜偏离率≤+10%（趋势较强）
            elif 5.0 < deviation <= 10.0:
                # 添加M头/头肩顶形态检测
                pattern_msg = ""
                if pattern_detection["detected"]:
                    pattern_name = pattern_detection["pattern_type"]
                    confidence = pattern_detection["confidence"]
                    # 【关键修复】确保confidence是标量值
                    confidence = float(confidence) if isinstance(confidence, (np.ndarray, np.float32)) else confidence
                    if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                        pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），立即减仓10%-15%"
                    elif confidence >= 0.5:
                        pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓5%-10%"
                return "\n".join(SCENARIO_MESSAGES_DICT["YES"]["trend_strong"]).format(
                    consecutive=consecutive_above,
                    deviation=deviation,
                    target_price=current * 0.99,
                    etf_code=index_info['etfs'][0]['code'],
                    pattern_msg=pattern_msg
                )
            # 场景C：偏离率＞+10%（超买风险）
            else:
                # 添加M头/头肩顶形态检测
                pattern_msg = ""
                if pattern_detection["detected"]:
                    pattern_name = pattern_detection["pattern_type"]
                    confidence = pattern_detection["confidence"]
                    # 【关键修复】确保confidence是标量值
                    confidence = float(confidence) if isinstance(confidence, (np.ndarray, np.float32)) else confidence
                    if confidence >= PATTERN_CONFIDENCE_THRESHOLD:
                        pattern_msg = f"【重要】{pattern_name}形态已确认（置信度{confidence:.0%}），立即减仓20%-30%"
                    elif confidence >= 0.5:
                        pattern_msg = f"【警告】疑似{pattern_name}形态（置信度{confidence:.0%}），建议减仓15%-25%"
                return "\n".join(SCENARIO_MESSAGES_DICT["YES"]["overbought"]).format(
                    consecutive=consecutive_above,
                    deviation=deviation,
                    target_price=critical * 1.05,
                    etf_code=index_info['etfs'][0]['code'],
                    pattern_msg=pattern_msg
                )
    # 2. NO信号：当前价格 ＜ 20日均线
    else:
        # 计算亏损比例
        loss_percentage = calculate_loss_percentage(df)
        # 子条件1：首次跌破（价格刚跌穿均线，连续1-2日未收回+成交量放大）
        if consecutive_below == 1 and volume_change > 0.2:
            if loss_percentage > -15.0:  # 亏损<15%
                return "\n".join(SCENARIO_MESSAGES_DICT["NO"]["initial_breakdown"]).format(
                    consecutive=consecutive_below,
                    volume=volume_change*100,
                    etf_code=index_info['etfs'][0]['code'],
                    target_price=critical * 1.05
                )
            else:  # 亏损≥15%
                return (
                    f"【首次跌破-严重亏损】连续{consecutive_below}天跌破20日均线，成交量放大{volume_change*100:.1f}%，亏损{loss_percentage:.2f}%\n"
                    f"✅ 操作建议：\n"
                    f"  • 核心宽基ETF（{index_info['etfs'][0]['code']}）立即清仓\n"
                    f"  • 卫星行业ETF保留20%-30%底仓观察\n"
                    f"  • 严格止损：收盘价站上20日均线才考虑回补\n"
                    f"⚠️ 重大亏损信号，避免盲目抄底"
                )
        # 子条件1：首次跌破（价格刚跌穿均线，连续1-2日未收回+成交量放大）
        elif consecutive_below == 2 and volume_change > 0.2:
            return "\n".join(SCENARIO_MESSAGES_DICT["NO"]["confirmed_breakdown"]).format(
                consecutive=consecutive_below,
                volume=volume_change*100,
                etf_code=index_info['etfs'][0]['code'],
                target_price=critical * 0.95
            )
        # 子条件2：持续跌破（价格维持在均线下）
        else:
            # 场景A：偏离率≥-5%（下跌初期）
            if deviation >= -5.0:
                return "\n".join(SCENARIO_MESSAGES_DICT["NO"]["decline_initial"]).format(
                    consecutive=consecutive_below,
                    deviation=deviation,
                    target_price=critical
                )
            # 场景B：-10%≤偏离率＜-5%（下跌中期）
            elif -10.0 <= deviation < -5.0:
                return "\n".join(SCENARIO_MESSAGES_DICT["NO"]["decline_medium"]).format(
                    consecutive=consecutive_below,
                    deviation=deviation,
                    etf_code=index_info['etfs'][0]['code']
                )
            # 场景C：偏离率＜-10%（超卖机会）
            else:
                return "\n".join(SCENARIO_MESSAGES_DICT["NO"]["oversold"]).format(
                    consecutive=consecutive_below,
                    deviation=deviation,
                    target_price=critical * 0.95,
                    etf_code=index_info['etfs'][0]['code']
                )

def generate_report():
    """生成策略报告并推送微信"""
    try:
        # 统计信息
        total_indices = len(INDICES)
        disabled_indices = sum(1 for idx in INDICES if idx.get("switch", 1) == 2)
        enabled_indices = total_indices - disabled_indices
        logger.info(f"共设计{total_indices}个指数，其中{disabled_indices}个指数暂停计算，本次计算{enabled_indices}个指数")
        
        # 登录baostock（一次）
        login_result = bs.login()
        if login_result.error_code != '0':
            logger.error(f"baostock登录失败: {login_result.error_msg}")
        else:
            logger.info("baostock登录成功")
        
        beijing_time = get_beijing_time()
        summary_lines = []
        valid_indices_count = 0
        disabled_messages = []
        
        # 按配置顺序处理
        for idx in INDICES:
            code = idx["code"]
            name = idx["name"]
            preferred_source = idx["source"]
            
            # 处理开关为2的指数
            if idx.get("switch", 1) == 2:
                logger.info(f"跳过开关为2的指数: {name}({code})")
                etf_list = [f"{etf['code']}({etf['description']})" for etf in idx["etfs"]]
                etf_str = "，".join(etf_list)
                disabled_message = f"{name} 【{code}；ETF：{etf_str}】 - 已暂时屏蔽，不作任何YES/NO计算"
                disabled_messages.append(disabled_message)
                # 发送单独的屏蔽消息
                send_wechat_message(disabled_message)
                time.sleep(1)
                continue
                
            # 使用智能数据获取函数
            logger.info(f"为指数 {name}({code}) 尝试首选数据源: {preferred_source}")
            df, actual_source = fetch_index_data_smart(idx)
            
            if df.empty:
                logger.warning(f"无数据: {name}({code})")
                # 数据获取失败的消息
                etf_list = [f"{etf['code']}({etf['description']})" for etf in idx["etfs"]]
                etf_str = "，".join(etf_list)
                message_lines = [
                    f"{name} 【{code}；ETF：{etf_str}】",
                    f"📊 当前：数据获取失败 | 临界值：N/A | 偏离率：N/A",
                    f"❌ 信号：数据获取失败",
                    "──────────────────",
                    f"⚠️ 所有数据源都无法获取有效数据（首选: {preferred_source}，尝试: {actual_source}）",
                    "──────────────────",
                    f"📅 计算时间: {beijing_time.strftime('%Y-%m-%d %H:%M')}",
                    f"📊 实际尝试数据源：{actual_source}"
                ]
                message = "".join(message_lines)
                logger.info(f"推送 {name} 策略信号（数据获取失败）")
                send_wechat_message(message)
                time.sleep(1)
                continue

            # 数据量检查
            if len(df) < CRITICAL_VALUE_DAYS:
                logger.warning(f"指数 {name}({code}) 数据不足{CRITICAL_VALUE_DAYS}天，跳过计算")
                etf_list = [f"{etf['code']}({etf['description']})" for etf in idx["etfs"]]
                etf_str = "，".join(etf_list)
                message_lines = [
                    f"{name} 【{code}；ETF：{etf_str}】",
                    f"📊 当前：数据不足 | 临界值：N/A | 偏离率：N/A",
                    f"⚠️ 信号：数据不足",
                    "──────────────────",
                    f"⚠️ 需要至少{CRITICAL_VALUE_DAYS}天数据进行计算，当前只有{len(df)}天",
                    "──────────────────",
                    f"📅 计算时间: {beijing_time.strftime('%Y-%m-%d %H:%M')}",
                    f"📊 实际使用数据源：{actual_source}（首选: {preferred_source}）"
                ]
                message = "".join(message_lines)
                logger.info(f"推送 {name} 策略信号（数据不足）")
                send_wechat_message(message)
                time.sleep(2)
                continue

            # 正常计算逻辑（保持不变）
            close_price = df['收盘'].values[-1]
            critical_value = calculate_critical_value(df)
            
            # 确保是标量值
            if isinstance(critical_value, pd.Series):
                critical_value = critical_value.values[-1]
            elif isinstance(critical_value, pd.DataFrame):
                critical_value = critical_value.iloc[-1, 0]
                
            try:
                close_price = float(close_price)
                critical_value = float(critical_value)
            except (TypeError, ValueError) as e:
                logger.error(f"转换价格值失败: {str(e)}")
                continue

            # 计算偏离率和信号
            deviation = calculate_deviation(close_price, critical_value)
            status = "YES" if close_price >= critical_value else "NO"
            signal_message = generate_signal_message(idx, df, close_price, critical_value, deviation)
            
            # 构建推送消息
            etf_list = [f"{etf['code']}({etf['description']})" for etf in idx["etfs"]]
            etf_str = "，".join(etf_list)
            signal_symbol = "✅" if status == "YES" else "❌"
            
            message_lines = [
                f"{name} 【{code}；ETF：{etf_str}】",
                f"📊 当前：{close_price:.2f} | 临界值：{critical_value:.2f} | 偏离率：{deviation:.2f}%",
                f"{signal_symbol} 信号：{status} {signal_message}"
            ]
            message = "".join(message_lines)
            
            logger.info(f"推送 {name} 策略信号（使用数据源: {actual_source}）")
            send_wechat_message(message)
            
            # 添加到总结
            name_padding = 10 if len(name) <= 4 else 8
            name_with_padding = f"{name}{' ' * (name_padding - len(name))}"
            summary_line = f"{name_with_padding}【{code}；ETF：{etf_str}】{signal_symbol} 信号：{status} 📊 当前：{close_price:.2f} | 临界值：{critical_value:.2f} | 偏离率：{deviation:.2f}% | 数据源：{actual_source}\n"
            summary_lines.append(summary_line)
            valid_indices_count += 1
            time.sleep(1)

        # 退出baostock
        bs.logout()
        logger.info("baostock已退出")
        
        # 构建总结消息
        final_summary_lines = []
        
        # 添加屏蔽指数的信息
        if disabled_messages:
            final_summary_lines.append("【已屏蔽指数】\n")
            for msg in disabled_messages:
                final_summary_lines.append(f"🔇 {msg}\n")
            final_summary_lines.append("\n")
        
        # 添加正常计算的指数信息
        if summary_lines:
            final_summary_lines.append("【策略信号总结】\n")
            final_summary_lines.extend(summary_lines)
        
        # 如果有任何指数信息，发送总结消息
        if final_summary_lines:
            summary_message = "".join(final_summary_lines)
            logger.info("推送总结消息")
            send_wechat_message(summary_message)
            time.sleep(1)
            
        logger.info(f"所有指数策略报告已成功发送至企业微信（共{valid_indices_count}个有效指数，{len(disabled_messages)}个屏蔽指数）")
        
    except Exception as e:
        logger.error(f"策略执行失败: {str(e)}", exc_info=True)
        # 修正：错误消息与正常信号消息分离
        try:
            send_wechat_message(f"🚨 【错误通知】策略执行异常: {str(e)}")
        except Exception as wechat_error:
            logger.error(f"发送微信消息失败: {str(wechat_error)}", exc_info=True)

if __name__ == "__main__":
    logger.info("===== 开始执行 指数Yes/No策略 =====")
    # 添加延时
    time.sleep(30)
    generate_report()
    logger.info("=== 指数Yes/No策略执行完成 ===")
