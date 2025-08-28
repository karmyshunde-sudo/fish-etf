import akshare as ak
import pandas as pd
import logging
import time
from config import Config
from retrying import retry

# 初始化日志
logger = logging.getLogger(__name__)

def empty_result_check(result):
    """检查结果是否为空"""
    return result.empty

@retry(stop_max_attempt_number=3, wait_fixed=2000, retry_on_result=empty_result_check)
def crawl_etf_daily_sina(etf_code, start_date, end_date):
    """
    用新浪接口爬取ETF日线数据（备用接口）
    :param etf_code: ETF代码
    :param start_date: 开始日期（YYYY-MM-DD）
    :param end_date: 结束日期（YYYY-MM-DD）
    :return: 标准化中文列名的DataFrame
    """
    try:
        logger.info(f"尝试使用新浪接口爬取ETF {etf_code} 的数据")
        
        # 添加市场前缀
        if etf_code.startswith('5') or etf_code.startswith('6'):
            symbol = f"sh{etf_code}"
        else:
            symbol = f"sz{etf_code}"
        
        # 使用新浪接口
        df = ak.fund_etf_hist_sina(symbol=symbol)
        
        if df.empty:
            logger.warning(f"新浪接口未获取到{etf_code}数据")
            return pd.DataFrame()
        
        # 过滤日期范围
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
        
        if df.empty:
            logger.warning(f"新浪接口获取的{etf_code}数据不在指定时间范围内")
            return pd.DataFrame()
        
        # 列名映射为中文
        col_map = {
            "date": "日期",
            "open": "开盘",
            "high": "最高",
            "low": "最低",
            "close": "收盘",
            "volume": "成交量",
            "amount": "成交额"
        }
        
        df = df.rename(columns=col_map)
        
        # 计算涨跌幅
        if "收盘" in df.columns:
            df["涨跌幅"] = df["收盘"].pct_change().round(4)
            df.loc[0, "涨跌幅"] = 0.0
        
        # 计算涨跌额
        if "收盘" in df.columns:
            df["涨跌额"] = (df["收盘"] - df["收盘"].shift(1)).round(4)
            df.loc[0, "涨跌额"] = 0.0
        
        # 计算振幅
        if "最高" in df.columns and "最低" in df.columns and "收盘" in df.columns:
            df["振幅"] = ((df["最高"] - df["最低"]) / df["收盘"].shift(1) * 100).round(4)
            df.loc[0, "振幅"] = 0.0
        
        logger.info(f"新浪接口成功获取{etf_code}数据，共{len(df)}条")
        return df
    
    except Exception as e:
        logger.error(f"新浪接口爬取{etf_code}失败：{str(e)}")
        # 等待一段时间后重试
        time.sleep(2)
        raise  # 触发重试
