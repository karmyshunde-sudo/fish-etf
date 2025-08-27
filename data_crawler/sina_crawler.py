import requests
import re
import pandas as pd
from config import Config
from retrying import retry

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def crawl_etf_daily_sina(etf_code, start_date, end_date):
    """
    用新浪爬取ETF日线数据（AkShare失败时备用）
    :param etf_code: ETF代码
    :param start_date: 开始日期（YYYY-MM-DD）
    :param end_date: 结束日期（YYYY-MM-DD）
    :return: 标准化中文列名的DataFrame
    """
    try:
        # 新浪ETF日线接口（需拼接代码，深市加sz，沪市加sh）
        market_prefix = "sz" if etf_code.startswith("15") else "sh"
        full_code = f"{market_prefix}{etf_code}"
        url = Config.SINA_ETF_HIST_URL.format(etf_code=full_code)
        
        # 请求数据
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # 解析新浪返回的JS数据（提取日期、开高低收、成交量）
        data_match = re.search(r'klc_data = \[(.*?)\];', response.text, re.S)
        if not data_match:
            print(f"新浪未获取到{etf_code}数据")
            return pd.DataFrame()
        
        # 处理数据列表
        data_str = data_match.group(1)
        data_list = eval(f"[{data_str}]")  # 转换为列表
        if not data_list:
            return pd.DataFrame()
        
        # 新浪数据源列名映射
        sina_col_map = {
            0: "日期",
            1: "开盘价",
            2: "收盘价",
            3: "最高价",
            4: "最低价",
            5: "成交量",
            6: "成交额"
        }
        
        # 构造原始DataFrame
        df = pd.DataFrame(data_list)
        
        # 重命名列名
        rename_dict = {}
        for idx, col_name in sina_col_map.items():
            if idx < len(df.columns):
                rename_dict[idx] = col_name
        df = df.rename(columns=rename_dict)
        
        # 处理缺失的标准列
        for std_col in Config.STANDARD_COLUMNS.keys():
            if std_col not in df.columns:
                # 计算涨跌幅
                if std_col == "涨跌幅":
                    if "收盘价" in df.columns:
                        df["涨跌幅"] = df["收盘价"].pct_change().round(4)
                        df.loc[0, "涨跌幅"] = 0.0  # 首日涨跌幅为0
                    else:
                        print(f"新浪数据缺少计算涨跌幅所需的收盘价")
                        return pd.DataFrame()
                else:
                    print(f"新浪数据缺少必要列：{std_col}")
                    return pd.DataFrame()
        
        # 数据清洗：日期格式转换、筛选时间范围
        df["日期"] = pd.to_datetime(df["日期"], format="%Y-%m-%d").dt.strftime("%Y-%m-%d")
        df = df[(df["日期"] >= start_date) & (df["日期"] <= end_date)]
        
        # 只保留固化中文列名并去重
        df = df[list(Config.STANDARD_COLUMNS.keys())].drop_duplicates(subset=["日期"], keep="last")
        
        print(f"新浪成功获取{etf_code}数据，共{len(df)}条")
        return df
    
    except Exception as e:
        print(f"新浪爬取{etf_code}失败：{str(e)}")
        raise  # 触发重试
