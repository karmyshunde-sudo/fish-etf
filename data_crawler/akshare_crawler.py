import akshare as ak
import pandas as pd
from config import Config
from retrying import retry

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
            print(f"AkShare未获取到{etf_code}数据（{start_date}至{end_date}）")
            return pd.DataFrame()
        
        # AkShare数据源可能的列名映射
        ak_col_map = {
            "trade_date": "日期",
            "open_price": "开盘价",
            "close_price": "收盘价",
            "high_price": "最高价",
            "low_price": "最低价",
            "volume": "成交量",
            "amount": "成交额",
            "pct_change": "涨跌幅",
            "开盘": "开盘价",
            "收盘": "收盘价",
            "最高": "最高价",
            "最低": "最低价",
            "成交量": "成交量",
            "成交额": "成交额",
            "涨跌幅": "涨跌幅"
        }
        
        # 构建可用列的映射关系
        available_cols = {}
        for source_col, target_col in ak_col_map.items():
            if source_col in df.columns:
                available_cols[source_col] = target_col
        
        # 尝试模糊匹配未找到的列
        for target_col in Config.STANDARD_COLUMNS.keys():
            if target_col not in available_cols.values():
                for col in df.columns:
                    if target_col in col or col in target_col:
                        available_cols[col] = target_col
                        break
        
        # 重命名列
        df = df.rename(columns=available_cols)
        
        # 处理缺失的标准列
        for std_col in Config.STANDARD_COLUMNS.keys():
            if std_col not in df.columns:
                # 计算涨跌幅
                if std_col == "涨跌幅":
                    if "收盘价" in df.columns:
                        df["涨跌幅"] = df["收盘价"].pct_change().round(4)
                        df.loc[0, "涨跌幅"] = 0.0  # 首日涨跌幅为0
                    else:
                        print(f"AkShare数据缺少计算涨跌幅所需的收盘价")
                        return pd.DataFrame()
                else:
                    print(f"AkShare数据缺少必要列：{std_col}")
                    return pd.DataFrame()
        
        # 只保留标准列
        df = df[list(Config.STANDARD_COLUMNS.keys())]
        
        # 数据清洗：去重、格式转换
        df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        df = df.drop_duplicates(subset=["日期"], keep="last")
        
        print(f"AkShare成功获取{etf_code}数据，共{len(df)}条")
        return df
    
    except Exception as e:
        print(f"AkShare爬取{etf_code}失败：{str(e)}")
        raise  # 触发重试
