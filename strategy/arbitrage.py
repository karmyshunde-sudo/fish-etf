import pandas as pd
import numpy as np
from config import Config
from utils.file_utils import load_etf_daily_data
from .etf_scoring import get_etf_name, get_top_rated_etfs

def calculate_arbitrage_opportunity():
    """
    计算ETF套利机会（基于ETF间价差，考虑交易成本）
    逻辑：找同类型ETF间价差超阈值（含成本）的机会
    """
    print("="*50)
    print("开始计算ETF套利机会")
    print("="*50)
    
    arbitrage_list = []
    # 获取高分ETF列表（前20%）
    top_etfs = get_top_rated_etfs()
    if top_etfs.empty:
        print("无足够高分ETF用于计算套利机会")
        return pd.DataFrame()
    
    # 按类型分组ETF（宽基、行业等）
    # 先获取所有ETF的类型信息
    etf_types = {}
    etf_list = load_all_etf_list()
    for _, row in top_etfs.iterrows():
        code = row["etf_code"]
        name = row["etf_name"]
        # 从名称中提取类型
        if any(keyword in name for keyword in ["上证50", "沪深300", "中证500", "创业板", "中证1000"]):
            etf_types[code] = "宽基ETF"
        elif any(keyword in name for keyword in ["行业", "板块", "主题"]):
            etf_types[code] = "行业ETF"
        else:
            # 尝试从ETF列表中获取
            if not etf_list.empty and code in etf_list["etf_code"].astype(str).values:
                type_row = etf_list[etf_list["etf_code"].astype(str) == code]
                if "etf_type" in type_row.columns:
                    etf_types[code] = type_row.iloc[0]["etf_type"]
                else:
                    etf_types[code] = "其他ETF"
            else:
                etf_types[code] = "其他ETF"
    
    # 按类型分组
    groups = {}
    for code, etf_type in etf_types.items():
        if etf_type not in groups:
            groups[etf_type] = []
        groups[etf_type].append(code)
    
    # 过滤掉数量不足的组
    valid_groups = {k: v for k, v in groups.items() if len(v) >= 2}
    if not valid_groups:
        print("没有足够数量的同类型ETF用于计算套利机会")
        return pd.DataFrame()
    
    for group_name, etf_codes in valid_groups.items():
        print(f"\n--- 处理{group_name} ---")
        # 加载该组所有ETF的最新数据（近5天）
        group_data = {}
        for code in etf_codes:
            df = load_etf_daily_data(code)
            if not df.empty and len(df) >=5:
                group_data[code] = df.tail(5).copy()
                group_data[code]["日期"] = pd.to_datetime(group_data[code]["日期"])
        
        if len(group_data) < 2:  # 至少2只ETF才计算价差
            print(f"{group_name}数据不足，跳过")
            continue
        
        # 计算每对ETF的价差
        codes = list(group_data.keys())
        for i in range(len(codes)):
            for j in range(i+1, len(codes)):
                code_a = codes[i]
                code_b = codes[j]
                df_a = group_data[code_a]
                df_b = group_data[code_b]
                
                # 按日期对齐数据
                merged_df = pd.merge(
                    df_a[["日期", "收盘价"]],
                    df_b[["日期", "收盘价"]],
                    on="日期",
                    suffixes=(f"_{code_a}", f"_{code_b}")
                )
                
                if merged_df.empty:
                    continue
                
                # 计算价差率（(A-B)/B）
                merged_df["价差率"] = (merged_df[f"收盘价_{code_a}"] / merged_df[f"收盘价_{code_b}"] - 1)
                # 最新价差率
                latest_spread = merged_df.iloc[-1]["价差率"]
                # 历史平均价差率（近5天）
                avg_spread = merged_df["价差率"].mean()
                # 价差偏离度（最新-平均）
                spread_deviation = latest_spread - avg_spread
                
                # 判断套利机会：偏离度绝对值超阈值（含交易成本）
                if abs(spread_deviation) >= Config.ARBITRAGE_PROFIT_THRESHOLD + Config.TRADE_COST_RATE:
                    # 确定套利方向：A相对高估则卖A买B，反之卖B买A
                    if spread_deviation > 0:
                        action = f"卖出{get_etf_name(code_a)}（{code_a}），买入{get_etf_name(code_b)}（{code_b}）"
                        profit_rate = spread_deviation - Config.TRADE_COST_RATE  # 扣除成本后的收益率
                    else:
                        action = f"卖出{get_etf_name(code_b)}（{code_b}），买入{get_etf_name(code_a)}（{code_a}）"
                        profit_rate = abs(spread_deviation) - Config.TRADE_COST_RATE
                    
                    arbitrage_list.append({
                        "ETF组": group_name,
                        "套利方向": action,
                        "最新价差率": f"{latest_spread:.2%}",
                        "平均价差率": f"{avg_spread:.2%}",
                        "扣除成本后收益率": f"{profit_rate:.2%}",
                        "数据日期": merged_df.iloc[-1]["日期"].strftime("%Y-%m-%d")
                    })
    
    # 转换为DataFrame并去重（避免重复机会）
    if arbitrage_list:
        arbitrage_df = pd.DataFrame(arbitrage_list).drop_duplicates(subset=["套利方向"])
        print(f"\n找到{len(arbitrage_df)}个套利机会")
        return arbitrage_df
    else:
        print("\n未找到符合条件的套利机会")
        return pd.DataFrame()

def format_arbitrage_message(arbitrage_df):
    """格式化套利机会消息"""
    if arbitrage_df.empty:
        return "【ETF套利机会提示】\n今日未找到符合条件的ETF套利机会（考虑交易成本后）"
    
    message = "【ETF套利机会提示】\n"
    message += f"共发现{len(arbitrage_df)}个套利机会（交易成本：{Config.TRADE_COST_RATE:.2%}）\n\n"
    
    for idx, (_, row) in enumerate(arbitrage_df.iterrows(), 1):
        message += f"{idx}. {row['ETF组']}\n"
        message += f"   操作建议：{row['套利方向']}\n"
        message += f"   最新价差率：{row['最新价差率']} | 平均价差率：{row['平均价差率']}\n"
        message += f"   扣除成本后收益率：{row['扣除成本后收益率']}\n"
        message += f"   数据日期：{row['数据日期']}\n\n"
    
    message += "风险提示：套利需考虑流动性和市场波动，操作前确认交易规则！"
    return message
