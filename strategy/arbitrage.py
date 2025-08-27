import pandas as pd
import numpy as np
from config import Config
from utils.file_utils import load_etf_daily_data
from .etf_scoring import get_etf_name, get_top_rated_etfs
from datetime import datetime

def calculate_premium_rate(etf_code):
    """计算ETF溢价率（需要实时数据，这里用简化版本）"""
    # 实际应用中应该获取实时IOPV和市场价格
    # 这里使用简化版本：随机生成一个溢价率用于演示
    return np.random.uniform(-0.02, 0.02)  # -2%到+2%的随机溢价率

def calculate_arbitrage_opportunity():
    """
    计算ETF套利机会（基于溢价率，考虑交易成本）
    逻辑：找溢价率超阈值（含成本）的机会
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
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for _, row in top_etfs.iterrows():
        etf_code = row["etf_code"]
        etf_name = row["etf_name"]
        
        # 计算溢价率
        premium_rate = calculate_premium_rate(etf_code)
        
        # 计算扣除成本后的套利收益率
        net_profit = abs(premium_rate) - Config.TRADE_COST_RATE
        
        # 判断套利机会：净收益超阈值
        if net_profit >= Config.ARBITRAGE_PROFIT_THRESHOLD:
            if premium_rate > 0:
                action = f"溢价套利：卖出{etf_name}（{etf_code}）"
                direction = "溢价"
            else:
                action = f"折价套利：买入{etf_name}（{etf_code}）"
                direction = "折价"
            
            arbitrage_list.append({
                "ETF代码": etf_code,
                "ETF名称": etf_name,
                "套利方向": action,
                "溢价率": f"{premium_rate:.3%}",
                "交易成本": f"{Config.TRADE_COST_RATE:.3%}",
                "净收益率": f"{net_profit:.3%}",
                "套利类型": direction,
                "发现时间": current_date
            })
    
    # 转换为DataFrame
    if arbitrage_list:
        arbitrage_df = pd.DataFrame(arbitrage_list)
        print(f"\n找到{len(arbitrage_df)}个套利机会")
        
        # 记录套利交易（假设执行）
        record_arbitrage_trades(arbitrage_df)
        
        return arbitrage_df
    else:
        print("\n未找到符合条件的套利机会")
        return pd.DataFrame()

def record_arbitrage_trades(arbitrage_df):
    """记录套利交易"""
    from position import init_trade_record, record_trade
    
    init_trade_record()
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    for _, row in arbitrage_df.iterrows():
        etf_code = row["ETF代码"]
        etf_name = row["ETF名称"]
        premium_rate = float(row["溢价率"].strip('%')) / 100
        net_profit = float(row["净收益率"].strip('%')) / 100
        
        # 获取当前价格（简化处理）
        df = load_etf_daily_data(etf_code)
        if not df.empty:
            price = df.iloc[-1]["收盘"]
        else:
            price = 1.0  # 默认价格
            
        # 确定操作类型
        if "溢价" in row["套利类型"]:
            operation = "卖出"
            reason = "溢价套利机会"
        else:
            operation = "买入"
            reason = "折价套利机会"
        
        # 记录交易
        record_trade(
            trade_date=current_date,
            position_type="套利仓",
            operation=operation,
            etf_code=etf_code,
            etf_name=etf_name,
            price=price,
            quantity=1000,
            amount=price * 1000,
            profit_rate=net_profit * 100,
            hold_days=1,  # 套利持仓1天
            reason=f"{reason}，溢价率：{premium_rate:.3%}"
        )

def format_arbitrage_message(arbitrage_df):
    """格式化套利机会消息"""
    if arbitrage_df.empty:
        return "【ETF套利机会提示】\n今日未找到符合条件的ETF套利机会（考虑交易成本后）"
    
    message = "【ETF套利机会提示】\n"
    message += f"共发现{len(arbitrage_df)}个套利机会（交易成本：{Config.TRADE_COST_RATE:.2%}）\n\n"
    
    for idx, (_, row) in enumerate(arbitrage_df.iterrows(), 1):
        message += f"{idx}. {row['ETF名称']}（{row['ETF代码']}）\n"
        message += f"   操作建议：{row['套利方向']}\n"
        message += f"   溢价率：{row['溢价率']} | 净收益率：{row['净收益率']}\n"
        message += f"   发现时间：{row['发现时间']}\n\n"
    
    message += "⚠️ 套利提示：套利机会通常短暂，需快速执行！次日请关注获利了结机会。"
    return message
