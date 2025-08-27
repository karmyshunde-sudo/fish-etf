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
        df = ak.fund_etf_info_em(symbol=etf_code)
        if df.empty:
            return 0.0, ""
            
        # 提取规模（单位：亿元）
        size = 0.0
        listing_date = ""
        
        for _, row in df.iterrows():
            if "基金规模" in str(row.iloc[0]):
                size_str = str(row.iloc[1]).replace("亿元", "").strip()
                size = float(size_str) if size_str.replace(".", "").isdigit() else 0.0
            elif "上市日期" in str(row.iloc[0]):
                listing_date = str(row.iloc[1])
                
        return size, listing_date
    except:
        return 0.0, ""  # 获取失败默认规模为0

def calculate_volatility(df, window=30):
    """计算波动率"""
    if len(df) < window:
        return 0.0
    returns = df["涨跌幅"].tail(window)
    return returns.std() * np.sqrt(252)  # 年化波动率

def calculate_sharpe_ratio(df, window=30, risk_free_rate=0.02):
    """计算夏普比率"""
    if len(df) < window:
        return 0.0
    returns = df["涨跌幅"].tail(window)
    excess_returns = returns - risk_free_rate/252
    sharpe = excess_returns.mean() / returns.std() * np.sqrt(252)
    return sharpe

def calculate_max_drawdown(df, window=30):
    """计算最大回撤"""
    if len(df) < window:
        return 0.0
    prices = df["收盘"].tail(window)
    cum_max = prices.cummax()
    drawdown = (cum_max - prices) / cum_max
    return drawdown.max()

def calculate_etf_score(etf_code, df):
    """
    增强版综合评分（0-100分）
    维度：流动性、风险控制、收益能力、溢价率、情绪指标
    """
    # 基础过滤：数据量不足30天
    if df.empty or len(df) < 30:
        return 0.0
    
    # 获取ETF规模信息
    etf_size, listing_date = get_etf_basic_info(etf_code)
    
    # 1. 规模过滤（≥5亿才纳入考虑）
    if etf_size < Config.MIN_ETP_SIZE * 0.5:
        return 0.0
    
    recent_30d = df.tail(30)
    
    # 2. 流动性得分（近30天平均成交额）
    avg_amount = recent_30d["成交额"].mean() / 10000  # 转为万元
    if avg_amount < Config.MIN_DAILY_VOLUME * 0.5:  # 流动性过低直接淘汰
        return 0.0
    liquidity_score = min(avg_amount / Config.MIN_DAILY_VOLUME * 100, 100)

    # 3. 风险控制得分（波动率越低分越高，夏普比率越高分越高）
    volatility = calculate_volatility(recent_30d)
    sharpe_ratio = calculate_sharpe_ratio(recent_30d)
    max_drawdown = calculate_max_drawdown(recent_30d)
    
    # 波动率得分（0-100）
    volatility_score = max(0, 100 - (volatility * 100))
    # 夏普比率得分
    sharpe_score = min(max(sharpe_ratio * 50, 0), 100)  # 夏普2.0得100分
    # 回撤控制得分
    drawdown_score = max(0, 100 - (max_drawdown * 500))  # 回撤20%得0分
    
    risk_score = (volatility_score * 0.4 + sharpe_score * 0.4 + drawdown_score * 0.2)

    # 4. 收益能力得分
    return_30d = (recent_30d.iloc[-1]["收盘"] / recent_30d.iloc[0]["收盘"] - 1) * 100
    return_score = max(min(return_30d + 10, 100), 0)  # 年化收益+10%得100分

    # 5. 情绪指标得分（成交量变化率）
    volume_change = (recent_30d["成交量"].iloc[-1] / recent_30d["成交量"].iloc[-5] - 1) * 100
    sentiment_score = min(max(volume_change + 50, 0), 100)  # 成交量增长50%得100分

    # 6. 综合评分（加权求和）
    weights = Config.SCORE_WEIGHTS
    total_score = (
        liquidity_score * weights['liquidity'] +
        risk_score * weights['risk'] +
        return_score * weights['return'] +
        sentiment_score * weights['sentiment']
    )
    
    # 溢价率得分需要额外数据，这里暂时用固定值
    premium_score = 80  # 默认值
    total_score += premium_score * weights['premium']
    
    return round(total_score, 2)

def get_top_rated_etfs(top_n=None, min_score=60):
    """
    从全市场ETF中筛选高分ETF
    - 先按综合评分过滤
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
        if score >= min_score:
            size, listing_date = get_etf_basic_info(etf_code)
            score_list.append({
                "etf_code": etf_code,
                "etf_name": get_etf_name(etf_code),
                "score": score,
                "size": size,
                "listing_date": listing_date
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
    if etf_list.empty:
        return f"ETF-{etf_code}"
    
    name_row = etf_list[etf_list["etf_code"].astype(str) == str(etf_code)]
    if not name_row.empty:
        return name_row.iloc[0]["etf_name"]
    return f"ETF-{etf_code}"  # 未找到时用代码代替
