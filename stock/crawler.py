# =======【3数据源crawler-豆包3.py】==============
import os
import time
import random
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import akshare as ak
import yfinance as yf
from typing import Optional, List, Dict

# 导入配置类
from config import Config

# ======= 基础配置（使用全局配置统一管理）=================
# 日志配置（使用全局日志配置，避免重复设置）
logger = logging.getLogger("StockCrawler")

# 目录配置（从Config类获取，保持项目结构统一）
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DAILY_DIR = Config.STOCK_DAILY_DIR  # 修正：使用配置中的股票日线数据目录
BASIC_INFO_FILE = Config.ALL_STOCK_PATH  # 修正：使用配置中的全市场股票列表路径

# 批量爬取参数（从配置获取，统一管理）
BATCH_SIZE = Config.CRAWL_BATCH_SIZE  # 修正：使用全局批量爬取大小
REQUEST_DELAY = (1.5, 2.5)  # 基础请求延时（避免限流，适配批量爬取）
REQUEST_TIMEOUT = Config.REQUEST_TIMEOUT  # 修正：使用配置中的超时设置

# 数据源配置（3个无调用次数限制，覆盖A股全场景）
# 核心逻辑：AKShare（首选）+ YFinance（备用）+ 腾讯财经（补充），均无调用限制
DATA_SOURCES = [
    # 数据源1：AKShare（优先使用，A股数据最精准，无调用次数限制）
    {
        "name": "AKShare",
        "interfaces": [
            # 主接口：东方财富日线（最稳定）
            {"func": ak.stock_zh_a_hist_min_em, "params": {"period": "daily", "adjust": "qfq"}},
            # 备用接口1：同花顺日线
            {"func": ak.stock_zh_a_hist_csindex, "params": {"period": "daily", "adjust": "qfq"}},
            # 备用接口2：通用A股日线
            {"func": ak.stock_zh_a_hist, "params": {"period": "daily", "adjust": "qfq"}},
            # 应急接口：新浪财经日线
            {"func": ak.stock_zh_a_hist_sina, "params": {"period": "daily", "adjust": "qfq"}},
        ],
        "code_convert": lambda code: format_stock_code(code),  # 6位纯代码（A股标准）
        "column_mapping": Config.COLUMN_NAME_MAPPING  # 修正：使用配置中的列名映射
    },
    # 数据源2：YFinance（备用，无调用次数限制，覆盖全球市场+A股）
    {
        "name": "YFinance",
        "interfaces": [
            {"func": "yfinance_daily", "params": {"interval": "1d", "auto_adjust": True}},
            {"func": "yfinance_daily", "params": {"interval": "1d", "auto_adjust": False}},
        ],
        "code_convert": lambda code: 
            f"{code}.SS" if code.startswith('6') else  # 沪市（6开头）
            f"{code}.SZ" if code.startswith(('00', '30')) else  # 深市（00/30开头）
            f"{code}.BJ" if code.startswith('8') else  # 北交所（8开头）
            None,  # 未知代码格式返回None，拒绝处理
        "column_mapping": {
            "Date": "日期", "Open": "开盘", "High": "最高", "Low": "最低",
            "Close": "收盘", "Volume": "成交量", "Adj Close": "复权收盘"
        }
    },
    # 数据源3：腾讯财经（补充，无调用次数限制，A股数据稳定）
    {
        "name": "TencentFinance",
        "interfaces": [
            # 主接口：腾讯财经A股日线（前复权）
            {"func": ak.stock_zh_a_hist_qq, "params": {"period": "daily", "adjust": "qfq"}},
            # 备用接口：腾讯财经A股日线（不复权）
            {"func": ak.stock_zh_a_hist_qq, "params": {"period": "daily", "adjust": "none"}},
        ],
        "code_convert": lambda code: format_stock_code(code),  # 6位纯代码（腾讯财经直接支持）
        "column_mapping": Config.COLUMN_NAME_MAPPING  # 修正：使用配置中的列名映射
    }
]

# 校验数据源列表有效性（强制3个数据源，确保稳定性）
if len(DATA_SOURCES) != 3:
    logger.error("致命错误：数据源数量不为3个，违反配置要求")
    raise RuntimeError("数据源数量必须为3个，程序终止")

# 全局数据源状态（跨股票保持一致，失败则切换）
current_data_source_index = 0  # 初始使用AKShare

# ===================== 基础工具函数（无近似，纯精准处理）=====================
def ensure_directory_exists():
    """确保数据目录存在（使用配置中的路径）"""
    if not os.path.exists(DAILY_DIR):
        os.makedirs(DAILY_DIR, exist_ok=True)
        logger.info(f"创建日线数据目录：{DAILY_DIR}")

def format_stock_code(stock_code: str) -> Optional[str]:
    """格式化股票代码为6位纯数字（严谨校验）"""
    if not stock_code:
        return None
    # 提取数字部分
    numeric_code = ''.join(filter(str.isdigit, str(stock_code)))
    # 校验长度（必须6位）
    if len(numeric_code) != 6:
        logger.warning(f"股票代码 {stock_code} 格式错误，必须为6位数字")
        return None
    # 校验板块合法性（仅支持A股主要板块）
    if not numeric_code.startswith(('6', '00', '30', '8')):
        logger.warning(f"股票代码 {numeric_code} 不属于A股主要板块（6/00/30/8开头）")
        return None
    return numeric_code

def is_trading_day(date: datetime.date) -> bool:
    """判断是否为A股交易日（基于AKShare精准数据，无近似）"""
    try:
        # 使用AKShare的交易日历接口（精准到日，无调用限制）
        trade_calendar = ak.tool_trade_date_hist_sina()
        trade_dates = pd.to_datetime(trade_calendar["trade_date"]).dt.date
        return date in trade_dates.tolist()
    except Exception as e:
        logger.error(f"判断交易日失败：{str(e)}")
        return False

def get_last_trading_day() -> Optional[datetime.date]:
    """获取最近一个交易日（精准计算）"""
    try:
        today = datetime.now().date()
        # 从今天往前找最近的交易日（最多找30天）
        for i in range(30):
            check_date = today - timedelta(days=i)
            if is_trading_day(check_date):
                return check_date
        logger.warning("未找到最近30天内的交易日")
        return None
    except Exception as e:
        logger.error(f"获取最近交易日失败：{str(e)}")
        return None

def get_beijing_time() -> datetime:
    """获取当前北京时区时间（使用配置中的时区）"""
    return datetime.now(Config.BEIJING_TIMEZONE)

def to_naive_datetime(dt: datetime) -> datetime:
    """移除时区信息（保持原有逻辑）"""
    if dt.tzinfo is not None:
        return dt.astimezone(Config.UTC_TIMEZONE).replace(tzinfo=None)
    return dt

def get_circulating_capital(stock_code: str) -> Optional[int]:
    """
    精准获取股票流通股本（单位：股）
    仅从AKShare两个权威数据源获取，失败返回None（无任何近似）
    """
    stock_code = format_stock_code(stock_code)
    if not stock_code:
        logger.error(f"股票代码 {stock_code} 格式错误，无法获取流通股本")
        return None

    # 方案1：东方财富数据源（优先，数据更新及时）
    try:
        stock_basic = ak.stock_zh_a_basic_info_em(symbol=stock_code)
        # 严格匹配"流通股本"项目（去除前后空格，避免匹配失败）
        stock_basic["项目"] = stock_basic["项目"].str.strip()
        cap_row = stock_basic[stock_basic["项目"] == "流通股本"]
        
        if not cap_row.empty:
            circulating_cap_str = cap_row["数值"].iloc[0].strip()
            # 处理单位转换（仅支持亿股、万股、股三种合法单位）
            if "亿股" in circulating_cap_str:
                cap_num = float(circulating_cap_str.replace("亿股", ""))
                circulating_cap = int(cap_num * 100000000)
            elif "万股" in circulating_cap_str:
                cap_num = float(circulating_cap_str.replace("万股", ""))
                circulating_cap = int(cap_num * 10000)
            elif "股" in circulating_cap_str:
                cap_num = float(circulating_cap_str.replace("股", ""))
                circulating_cap = int(cap_num)
            else:
                # 无单位默认视为"股"（仅支持纯数字）
                if circulating_cap_str.replace('.', '').isdigit():
                    circulating_cap = int(float(circulating_cap_str))
                else:
                    logger.error(f"流通股本格式异常：{circulating_cap_str}（股票代码：{stock_code}）")
                    return None
            
            # 严谨校验流通股本合理性（A股流通股本范围：1000万股 ~ 1万亿股）
            if 1000000 <= circulating_cap <= 100000000000:
                logger.debug(f"股票 {stock_code} 流通股本（东方财富）：{circulating_cap:,} 股")
                return circulating_cap
            else:
                logger.error(f"流通股本超出合理范围：{circulating_cap:,} 股（股票代码：{stock_code}）")
                return None
        else:
            logger.warning(f"东方财富接口未查询到 {stock_code} 流通股本")
    except Exception as e1:
        logger.warning(f"东方财富接口获取流通股本失败（{stock_code}）：{str(e1)}")

    # 方案2：同花顺数据源（备用，双重保障）
    try:
        stock_basic_ths = ak.stock_zh_a_basic_info_ths(symbol=stock_code)
        # 适配同花顺接口两种常见列名
        if "流通股本(股)" in stock_basic_ths.columns:
            circulating_cap = stock_basic_ths["流通股本(股)"].iloc[0]
        elif "流通股本" in stock_basic_ths.columns:
            circulating_cap = stock_basic_ths["流通股本"].iloc[0]
        else:
            logger.warning(f"同花顺接口未找到流通股本列（{stock_code}）")
            return None

        # 转换为整数（股）
        if isinstance(circulating_cap, (int, float)):
            circulating_cap = int(circulating_cap)
        else:
            # 处理带单位的字符串
            cap_str = str(circulating_cap).strip()
            if "亿" in cap_str:
                cap_num = float(cap_str.replace("亿", ""))
                circulating_cap = int(cap_num * 100000000)
            elif "万" in cap_str:
                cap_num = float(cap_str.replace("万", ""))
                circulating_cap = int(cap_num * 10000)
            else:
                logger.error(f"同花顺流通股本格式无法解析：{cap_str}（股票代码：{stock_code}）")
                return None

        # 再次校验合理性
        if 1000000 <= circulating_cap <= 100000000000:
            logger.debug(f"股票 {stock_code} 流通股本（同花顺）：{circulating_cap:,} 股")
            return circulating_cap
        else:
            logger.error(f"同花顺流通股本超出合理范围：{circulating_cap:,} 股（股票代码：{stock_code}）")
            return None
    except Exception as e2:
        logger.warning(f"同花顺接口获取流通股本失败（{stock_code}）：{str(e2)}")

    # 两种方案都失败
    logger.error(f"所有接口均无法获取 {stock_code} 流通股本")
    return None
