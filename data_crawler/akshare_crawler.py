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
        # 使用 fund_etf_hist_em 接口爬取ETF日线数据
        df = ak.fund_etf_hist_em(symbol=etf_code, 
                                period="daily", 
                                start_date=start_date, 
                                end_date=end_date, 
                                adjust="qfq")
        
        if df.empty:
            logger.warning(f"AkShare未获取到{etf_code}数据（{start_date}至{end_date}）")
            return pd.DataFrame()
        
        # 记录返回的列名，用于调试
        logger.info(f"AkShare返回列名: {list(df.columns)}")
        
        # 使用Config中的标准列名映射
        col_map = {}
        for source_col, target_col in Config.STANDARD_COLUMNS.items():
            # 尝试找到对应的源列
            if source_col in df.columns:
                col_map[source_col] = target_col
            else:
                # 尝试模糊匹配
                for actual_col in df.columns:
                    if source_col in actual_col:
                        col_map[actual_col] = target_col
                        break
        
        # 重命名列
        df = df.rename(columns=col_map)
        
        # 确保所有必需列都存在
        required_cols = list(Config.STANDARD_COLUMNS.keys())
        # 排除不需要从akshare获取的列（这些列会在后续处理中添加）
        exclude_cols = ["ETF代码", "ETF名称", "爬取时间"]
        required_data_cols = [col for col in required_cols if col not in exclude_cols]
        
        for col in required_data_cols:
            if col not in df.columns:
                if col == "涨跌额":
                    # 计算涨跌额
                    df[col] = (df["收盘"] - df["收盘"].shift(1)).round(4)
                    df.loc[0, col] = 0.0
                elif col == "振幅":
                    # 计算振幅
                    df[col] = ((df["最高"] - df["最低"]) / df["收盘"].shift(1) * 100).round(4)
                    df.loc[0, col] = 0.0
                elif col == "换手率":
                    # 计算换手率（近似计算）
                    if "成交量" in df.columns and "成交额" in df.columns:
                        df[col] = (df["成交量"] / (df["成交额"] / df["收盘"]) * 100).round(4)
                    else:
                        df[col] = 0.0
                else:
                    logger.warning(f"AkShare数据缺少必要列：{col}")
                    return pd.DataFrame()
        
        # 只保留标准列（排除不需要从akshare获取的列）
        df = df[required_data_cols]
        
        # 数据清洗：去重、格式转换
        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        df = df.drop_duplicates(subset=["日期"], keep="last")
        
        logger.info(f"AkShare成功获取{etf_code}数据，共{len(df)}条")
        return df
    
    except Exception as e:
        logger.error(f"AkShare爬取{etf_code}失败：{str(e)}")
        raise  # 触发重试
