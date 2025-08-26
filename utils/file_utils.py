import os
import pandas as pd
import datetime
from config import Config
from utils.date_utils import get_beijing_time

def init_dirs():
    """初始化数据目录和标记目录"""
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    os.makedirs(Config.FLAG_DIR, exist_ok=True)
    # 确保数据根目录存在
    os.makedirs(os.path.dirname(Config.METADATA_PATH), exist_ok=True)

def load_etf_metadata():
    """加载ETF元数据（记录最后爬取日期）"""
    init_dirs()
    if not os.path.exists(Config.METADATA_PATH):
        # 初次创建：默认最后爬取日期为1年前
        last_date = (get_beijing_time().date() - datetime.timedelta(days=Config.INITIAL_CRAWL_DAYS)).strftime("%Y-%m-%d")
        metadata_df = pd.DataFrame(columns=["etf_code", "last_crawl_date"])
        metadata_df.to_csv(Config.METADATA_PATH, index=False, encoding="utf-8")
        return metadata_df
    return pd.read_csv(Config.METADATA_PATH, encoding="utf-8")

def update_etf_metadata(etf_code, last_date):
    """更新ETF最后爬取日期"""
    metadata_df = load_etf_metadata()
    # 检查该ETF是否已在元数据中
    if etf_code in metadata_df["etf_code"].values:
        metadata_df.loc[metadata_df["etf_code"] == etf_code, "last_crawl_date"] = last_date
    else:
        # 新增ETF记录
        new_row = pd.DataFrame({"etf_code": [etf_code], "last_crawl_date": [last_date]})
        metadata_df = pd.concat([metadata_df, new_row], ignore_index=True)
    metadata_df.to_csv(Config.METADATA_PATH, index=False, encoding="utf-8")

def save_etf_daily_data(etf_code, df):
    """保存ETF日线数据（增量追加，去重）"""
    init_dirs()
    file_path = f"{Config.DATA_DIR}/{etf_code}_日线数据.csv"
    # 确保列名是中文（固化）
    df = df[list(Config.STANDARD_COLUMNS.keys())].copy()
    
    if os.path.exists(file_path):
        # 增量追加：读取已有数据，去重后合并
        existing_df = pd.read_csv(file_path, encoding="utf-8")
        combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=["日期"], keep="last")
        combined_df = combined_df.sort_values("日期", ascending=True)
    else:
        combined_df = df.sort_values("日期", ascending=True)
    
    combined_df.to_csv(file_path, index=False, encoding="utf-8")
    print(f"已保存{etf_code}数据，共{len(combined_df)}条")

def check_flag(flag_template):
    """检查当天是否已推送（flag_template：套利/仓位标记模板）"""
    today = get_beijing_time().date().strftime("%Y%m%d")
    flag_file = flag_template.format(date=today)
    return os.path.exists(flag_file)

def set_flag(flag_template):
    """设置当天推送标记"""
    today = get_beijing_time().date().strftime("%Y%m%d")
    flag_file = flag_template.format(date=today)
    with open(flag_file, "w", encoding="utf-8") as f:
        f.write(f"Pushed at {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")

def load_etf_daily_data(etf_code):
    """加载单只ETF的日线数据"""
    file_path = f"{Config.DATA_DIR}/{etf_code}_日线数据.csv"
    if not os.path.exists(file_path):
        return pd.DataFrame()
    return pd.read_csv(file_path, encoding="utf-8", parse_dates=["日期"])
