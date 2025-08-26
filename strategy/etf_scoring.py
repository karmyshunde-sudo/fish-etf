import pandas as pd
import numpy as np
import akshare as ak
from config import Config
from utils.file_utils import load_etf_daily_data, load_etf_metadata
from data_crawler.etf_list_manager import load_all_etf_list

def get_etf_basic_info(etf_code):
    """获取ETF基本信息（规模、成立日期等）"""
    try:
        # 从AkShare获取ETF详情
        df = ak.etf_fund_info_em(symbol=etf_code)
        # 提取规模（单位：亿元）
        size_row = df[df[0].str.contains("基金规模", na=False)]
        if not size_row.empty:
            size_str = size_row.iloc[0, 1].replace("亿元", "").strip()
            return float(size_str) if size_str.replace(".", "").isdigit() else 0.0
        return 0.0
    except:
        return 0.0  # 获取失败默认规模为0

def calculate_etf_score(etf_code, df):
    """
    增强版综合评分（0-100分）
    维度：规模、流动性、收益、波动率
    """
    # 基础过滤：数据量不足30天或规模过小
    if df.empty or len(df) < 30:
        return 0.0
    
    # 1. 规模得分（≥10亿得满分，<5亿得0分）
    etf_size = get_etf_basic_info(etf_code)
    if etf_size < Config.MIN_ETP_SIZE * 0.5:  # 规模过小直接淘汰
        return 0.0
    size_score = min(etf_size / Config.MIN_ETP_SIZE * 100, 100)  # 超10亿得满分

    # 2. 流动性得分（近30天平均成交额）
    recent_30d = df.tail(30)
    avg_amount = recent_30d["成交额"].mean() / 10000  # 转为万元
    if avg_amount < Config.MIN_DAILY_VOLUME * 0.5:  # 流动性过低直接淘汰
        return 0.0
    liquidity_score = min(avg_amount / Config.MIN_DAILY_VOLUME * 100, 100)

    # 3. 收益得分（近30天收益率）
    return_30d = (recent_30d.iloc[-1]["收盘价"] / recent_30d.iloc[0]["收盘价"] - 1) * 100
    return_score = max(min(return_30d + 5, 100), 0)  # 负收益最低0分

    # 4. 波动率得分（近30天波动率，越低分越高）
    vol_30d = recent_30d["涨跌幅"].std() * 100
    max_vol = 5  # 最大可接受波动率5%
    volatility_score = max(100 - (vol_30d / max_vol * 100), 0)

    # 5. 综合评分（加权求和）
    total_score = (
        size_score * 0.2 +          # 规模权重20%
        liquidity_score * 0.3 +     # 流动性权重30%
        return_score * 0.3 +        # 收益权重30%
        volatility_score * 0.2      # 波动率权重20%
    )
    return round(total_score, 2)

def get_top_rated_etfs(top_n=None):
    """
    从全市场ETF中筛选高分ETF
    - 先按综合评分过滤前20%
    - 若未指定top_n，返回所有高分ETF
    """
    # 1. 获取所有已爬取数据的ETF代码
    metadata_df = load_etf_metadata()
    all_codes = metadata_df["etf_code"].tolist()
    if not all_codes:
        return pd.DataFrame()
    
    # 2. 计算每个ETF的综合评分
    score_list = []
    for etf_code in all_codes:
        df = load_etf_daily_data(etf_code)
        score = calculate_etf_score(etf_code, df)
        if score >= 60:  # 基础分≥60分才纳入考虑
            score_list.append({
                "etf_code": etf_code,
                "etf_name": get_etf_name(etf_code),
                "score": score,
                "size": get_etf_basic_info(etf_code)  # 附加规模信息
            })
    
    if not score_list:
        return pd.DataFrame()
    
    # 3. 按评分排序，取前SCORE_TOP_PERCENT%
    score_df = pd.DataFrame(score_list).sort_values("score", ascending=False)
    top_count = max(10, int(len(score_df) * Config.SCORE_TOP_PERCENT / 100))  # 至少保留10只
    top_df = score_df.head(top_count)
    
    # 4. 若指定了top_n，返回前n只
    if top_n:
        return top_df.head(top_n)
    return top_df

def get_etf_name(etf_code):
    """从全市场列表中获取ETF名称"""
    etf_list = load_all_etf_list()
    name_row = etf_list[etf_list["etf_code"].astype(str) == etf_code]
    if not name_row.empty:
        return name_row.iloc[0]["etf_name"]
    return f"ETF-{etf_code}"  # 未找到时用代码代替
