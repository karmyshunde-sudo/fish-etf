import akshare as ak
import pandas as pd
import logging
from config import Config
from retrying import retry

# 初始化日志
logger = logging.getLogger(__name__)

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def crawl_etf_daily_akshare(etf_code, start_date, end_date):
    """
    用AkShare爬取ETF日线数据
    :param etf_code: ETF代码
    :param start_date: 开始日期（YYYY-MM-DD）
    :param end_date: 结束日期（YYYY-MM-DD）
    :return: 标准化中文列名的DataFrame
    """
    try:
        # 爬取ETF日线数据（AkShare接口）
        df = ak.etf_spot_em(symbol=etf_code, start_date=start_date, end_date=end_date)
        
        if df.empty:
            logger.warning(f"AkShare未获取到{etf_code}数据（{start_date}至{end_date}）")
            return pd.DataFrame()
        
        # 列名映射为中文（固化）
        col_map = {
            "trade_date": "日期",
            "open_price": "开盘价",
            "close_price": "收盘价",
            "high_price": "最高价",
            "low_price": "最低价",
            "volume": "成交量",
            "amount": "成交额",
            "pct_change": "涨跌幅"
        }
        
        # 处理可能的列名变化
        available_cols = {}
        for target_col, source_col in col_map.items():
            # 尝试精确匹配
            if source_col in df.columns:
                available_cols[source_col] = target_col
            else:
                # 尝试模糊匹配
                for col in df.columns:
                    if source_col in col:
                        available_cols[col] = target_col
                        break
        
        # 只保留需要的列，并重命名
        df = df.rename(columns=available_cols)
        
        # 确保所有标准列都存在
        for col in Config.STANDARD_COLUMNS.keys():
            if col not in df.columns:
                if col == "涨跌幅":
                    # 计算涨跌幅
                    df[col] = df["收盘价"].pct_change().round(4)
                    df.loc[0, col] = 0.0
                else:
                    # 缺失其他列则返回空
                    logger.warning(f"AkShare数据缺少必要列：{col}")
                    return pd.DataFrame()
        
        df = df[list(Config.STANDARD_COLUMNS.keys())]
        
        # 数据清洗：去重、格式转换
        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        df = df.drop_duplicates(subset=["日期"], keep="last")
        
        logger.info(f"AkShare成功获取{etf_code}数据，共{len(df)}条")
        return df
    
    except Exception as e:
        logger.error(f"AkShare爬取{etf_code}失败：{str(e)}")
        raise  # 触发重试
