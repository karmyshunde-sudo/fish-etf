import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from config import Config
from utils.file_utils import load_etf_daily_data, init_dirs
from .etf_scoring import get_top_rated_etfs, get_etf_name, get_etf_basic_info

# 仓位持仓记录路径
POSITION_RECORD_PATH = "data/position_record.csv"
TRADE_RECORD_PATH = Config.TRADE_RECORD_FILE

def init_position_record():
    """初始化仓位记录（稳健仓、激进仓各持1只ETF）"""
    init_dirs()
    if not os.path.exists(POSITION_RECORD_PATH):
        # 初始无持仓
        position_df = pd.DataFrame({
            "仓位类型": ["稳健仓", "激进仓"],
            "当前持仓ETF代码": ["", ""],
            "当前持仓ETF名称": ["", ""],
            "持仓成本价": [0.0, 0.0],
            "持仓日期": ["", ""],
            "持仓数量": [0, 0],
            "最新操作": ["未持仓", "未持仓"],
            "操作日期": ["", ""]
        })
        position_df.to_csv(POSITION_RECORD_PATH, index=False, encoding="utf-8")
    return pd.read_csv(POSITION_RECORD_PATH, encoding="utf-8")

def init_trade_record():
    """初始化交易记录"""
    init_dirs()
    if not os.path.exists(TRADE_RECORD_PATH):
        trade_df = pd.DataFrame(columns=[
            "交易日期", "仓位类型", "操作类型", "ETF代码", "ETF名称",
            "价格", "数量", "金额", "收益率", "持仓天数", "原因"
        ])
        trade_df.to_csv(TRADE_RECORD_PATH, index=False, encoding="utf-8")
    return pd.read_csv(TRADE_RECORD_PATH, encoding="utf-8")

def record_trade(trade_date, position_type, operation, etf_code, etf_name, 
                price, quantity, amount, profit_rate=0, hold_days=0, reason=""):
    """记录交易流水"""
    trade_df = init_trade_record()
    
    new_trade = pd.DataFrame([{
        "交易日期": trade_date,
        "仓位类型": position_type,
        "操作类型": operation,
        "ETF代码": etf_code,
        "ETF名称": etf_name,
        "价格": price,
        "数量": quantity,
        "金额": amount,
        "收益率": profit_rate,
        "持仓天数": hold_days,
        "原因": reason
    }])
    
    trade_df = pd.concat([trade_df, new_trade], ignore_index=True)
    trade_df.to_csv(TRADE_RECORD_PATH, index=False, encoding="utf-8")
    
    # 尝试发送交易通知到微信
    try:
        from wechat_push import send_wechat_message
        message = f"【交易执行通知】\n\n"
        message += f"操作: {operation}\n"
        message += f"ETF: {etf_name} ({etf_code})\n"
        message += f"价格: {price:.2f}元\n"
        message += f"数量: {quantity}股\n"
        message += f"金额: {amount:.2f}元\n"
        if profit_rate != 0:
            message += f"收益率: {profit_rate:.2f}%\n"
        message += f"原因: {reason}\n"
        message += f"时间: {trade_date}"
        
        send_wechat_message(message)
    except Exception as e:
        print(f"微信交易通知发送失败: {str(e)}")

def update_position_record(position_type, etf_code, etf_name, cost_price, quantity, action):
    """更新仓位记录"""
    position_df = init_position_record()
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 找到对应仓位行
    idx = position_df[position_df["仓位类型"] == position_type].index[0]
    position_df.loc[idx] = [
        position_type,
        etf_code,
        etf_name,
        cost_price,
        today,
        quantity,
        action,
        today
    ]
    position_df.to_csv(POSITION_RECORD_PATH, index=False, encoding="utf-8")
    return position_df

def calculate_ma_signal(df, short_period=5, long_period=20):
    """计算均线信号"""
    if len(df) < long_period:
        return False, False
    
    df = df.copy()
    df["short_ma"] = df["收盘"].rolling(window=short_period).mean()
    df["long_ma"] = df["收盘"].rolling(window=long_period).mean()
    
    # 检查是否连续两天短期均线上穿长期均线
    signal_days = 0
    for i in range(-Config.BUY_SIGNAL_DAYS, 0):
        if df["short_ma"].iloc[i] > df["long_ma"].iloc[i]:
            signal_days += 1
    
    ma_bullish = signal_days >= Config.BUY_SIGNAL_DAYS
    ma_bearish = df["short_ma"].iloc[-1] < df["long_ma"].iloc[-1]
    
    return ma_bullish, ma_bearish

def calculate_position_strategy():
    """
    计算仓位操作策略（稳健仓、激进仓）
    逻辑：
    - 稳健仓：选综合评分最高的ETF，均线多头则持有/加仓，跌破止损则卖出
    - 激进仓：选近30天收益最高的ETF，波动超阈值则换股
    """
    print("="*50)
    print("开始计算ETF仓位操作策略")
    print("="*50)
    
    # 1. 初始化仓位记录
    position_df = init_position_record()
    init_trade_record()
    
    # 获取评分前5的ETF（用于选仓）
    top_etfs = get_top_rated_etfs(top_n=5)
    if top_etfs.empty:
        print("无有效ETF评分数据，无法计算仓位策略")
        return "【ETF仓位操作提示】\n无有效ETF数据，无法生成操作建议"
    
    # 2. 分别计算稳健仓和激进仓策略
    strategies = {}
    trade_actions = []
    
    # 2.1 稳健仓策略（评分最高+均线策略）
    stable_etf = top_etfs.iloc[0]
    stable_code = stable_etf["etf_code"]
    stable_name = stable_etf["etf_name"]
    stable_df = load_etf_daily_data(stable_code)
    
    # 稳健仓当前持仓
    stable_position = position_df[position_df["仓位类型"] == "稳健仓"].iloc[0]
    strategy, actions = calculate_single_position_strategy(
        position_type="稳健仓",
        current_position=stable_position,
        target_etf_code=stable_code,
        target_etf_name=stable_name,
        etf_df=stable_df,
        is_stable=True
    )
    strategies["稳健仓"] = strategy
    trade_actions.extend(actions)
    
    # 2.2 激进仓策略（近30天收益最高）
    return_list = []
    for _, row in top_etfs.iterrows():
        code = row["etf_code"]
        df = load_etf_daily_data(code)
        if not df.empty and len(df) >= 30:
            return_30d = (df.iloc[-1]["收盘"] / df.iloc[-30]["收盘"] - 1) * 100
            return_list.append({
                "etf_code": code,
                "etf_name": row["etf_name"],
                "return_30d": return_30d,
                "score": row["score"]
            })
    
    if return_list:
        aggressive_etf = max(return_list, key=lambda x: x["return_30d"])
        aggressive_code = aggressive_etf["etf_code"]
        aggressive_name = aggressive_etf["etf_name"]
        aggressive_df = load_etf_daily_data(aggressive_code)
        
        # 激进仓当前持仓
        aggressive_position = position_df[position_df["仓位类型"] == "激进仓"].iloc[0]
        strategy, actions = calculate_single_position_strategy(
            position_type="激进仓",
            current_position=aggressive_position,
            target_etf_code=aggressive_code,
            target_etf_name=aggressive_name,
            etf_df=aggressive_df,
            is_stable=False
        )
        strategies["激进仓"] = strategy
        trade_actions.extend(actions)
    else:
        strategies["激进仓"] = "激进仓：无有效收益数据，暂不调整仓位"
    
    # 3. 执行交易操作
    for action in trade_actions:
        record_trade(**action)
    
    # 4. 格式化消息
    return format_position_message(strategies)

def calculate_single_position_strategy(position_type, current_position, target_etf_code, target_etf_name, etf_df, is_stable):
    """计算单个仓位（稳健/激进）的操作策略"""
    if etf_df.empty or len(etf_df) < Config.MA_LONG_PERIOD:
        return f"{position_type}：目标ETF数据不足，暂不调整", []
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    trade_actions = []
    
    # 计算均线信号
    ma_bullish, ma_bearish = calculate_ma_signal(etf_df, Config.MA_SHORT_PERIOD, Config.MA_LONG_PERIOD)
    latest_close = etf_df.iloc[-1]["收盘"]
    
    # 当前持仓信息
    has_position = not pd.isna(current_position["当前持仓ETF代码"]) and current_position["当前持仓ETF代码"] != ""
    current_code = current_position["当前持仓ETF代码"] if has_position else ""
    current_name = current_position["当前持仓ETF名称"] if has_position else ""
    current_cost = float(current_position["持仓成本价"]) if has_position else 0
    current_quantity = int(current_position["持仓数量"]) if has_position else 0
    position_date = current_position["持仓日期"] if has_position else ""
    
    # 计算持仓收益率（如果有持仓）
    if has_position:
        hold_days = (datetime.now() - datetime.strptime(position_date, "%Y-%m-%d")).days if position_date else 0
        profit_rate = (latest_close / current_cost - 1) * 100
    else:
        hold_days = 0
        profit_rate = 0
    
    # 1. 当前无持仓：判断是否买入
    if not has_position:
        if ma_bullish:  # 均线多头才买入
            # 执行买入
            update_position_record(
                position_type=position_type,
                etf_code=target_etf_code,
                etf_name=target_etf_name,
                cost_price=latest_close,
                quantity=1000,  # 默认买入1000股
                action=f"买入（成本价：{latest_close:.2f}元）"
            )
            # 记录交易
            trade_actions.append({
                "trade_date": current_date,
                "position_type": position_type,
                "operation": "买入",
                "etf_code": target_etf_code,
                "etf_name": target_etf_name,
                "price": latest_close,
                "quantity": 1000,
                "amount": latest_close * 1000,
                "profit_rate": 0,
                "hold_days": 0,
                "reason": "均线多头突破，符合买入条件"
            })
            return f"{position_type}：执行买入【{target_etf_name}（{target_etf_code}）】，成本价：{latest_close:.2f}元", trade_actions
        else:
            return f"{position_type}：当前无持仓，目标ETF未满足买入条件（均线未多头），暂不买入", []
    
    # 2. 判断是否换股（新ETF评分比当前高30%）
    if has_position and current_code != target_etf_code:
        from .etf_scoring import calculate_etf_score
        current_score = calculate_etf_score(current_code, load_etf_daily_data(current_code))
        target_score = calculate_etf_score(target_etf_code, etf_df)
        
        if target_score > current_score * (1 + Config.SWITCH_THRESHOLD) and ma_bullish:
            # 执行换股
            update_position_record(
                position_type=position_type,
                etf_code=target_etf_code,
                etf_name=target_etf_name,
                cost_price=latest_close,
                quantity=1000,
                action=f"换股（卖出{current_name}，买入{target_etf_name}）"
            )
            # 记录卖出交易
            trade_actions.append({
                "trade_date": current_date,
                "position_type": position_type,
                "operation": "卖出",
                "etf_code": current_code,
                "etf_name": current_name,
                "price": latest_close,
                "quantity": current_quantity,
                "amount": latest_close * current_quantity,
                "profit_rate": profit_rate,
                "hold_days": hold_days,
                "reason": f"换股：新ETF评分({target_score})比当前({current_score})高{Config.SWITCH_THRESHOLD*100}%"
            })
            # 记录买入交易
            trade_actions.append({
                "trade_date": current_date,
                "position_type": position_type,
                "operation": "买入",
                "etf_code": target_etf_code,
                "etf_name": target_etf_name,
                "price": latest_close,
                "quantity": 1000,
                "amount": latest_close * 1000,
                "profit_rate": 0,
                "hold_days": 0,
                "reason": "换股操作"
            })
            return f"{position_type}：执行换股\n原持仓：{current_name}（{current_code}）收益率：{profit_rate:.2f}%\n新持仓：{target_etf_name}（{target_etf_code}）\n原因：新ETF评分高出{Config.SWITCH_THRESHOLD*100}%", trade_actions
    
    # 3. 止损判断（跌破止损阈值）
    if has_position and profit_rate <= Config.STOP_LOSS_THRESHOLD * 100:
        # 执行止损
        update_position_record(
            position_type=position_type,
            etf_code="",
            etf_name="",
            cost_price=0.0,
            quantity=0,
            action=f"止损卖出（收益率：{profit_rate:.2f}%）"
        )
        # 记录交易
        trade_actions.append({
            "trade_date": current_date,
            "position_type": position_type,
            "operation": "卖出",
            "etf_code": current_code,
            "etf_name": current_name,
            "price": latest_close,
            "quantity": current_quantity,
            "amount": latest_close * current_quantity,
            "profit_rate": profit_rate,
            "hold_days": hold_days,
            "reason": f"止损：收益率({profit_rate:.2f}%)低于止损阈值({Config.STOP_LOSS_THRESHOLD*100}%)"
        })
        return f"{position_type}：执行止损\n持仓：{current_name}（{current_code}）\n收益率：{profit_rate:.2f}%（跌破止损阈值{Config.STOP_LOSS_THRESHOLD*100:.1f}%）", trade_actions
    
    # 4. 继续持有
    ma_status = "5日均线＞20日均线" if not ma_bearish else "5日均线＜20日均线"
    return f"{position_type}：继续持有【{current_name}（{current_code}）】\n当前价格：{latest_close:.2f}元，成本价：{current_cost:.2f}元\n收益率：{profit_rate:.2f}%，持仓天数：{hold_days}天\n均线状态：{ma_status}", trade_actions

def format_position_message(strategies):
    """格式化仓位策略消息"""
    message = "【ETF仓位操作提示】\n"
    message += "（每个仓位仅持有1只ETF，操作建议基于最新数据）\n\n"
    
    for position_type, content in strategies.items():
        message += f"【{position_type}】\n{content}\n\n"
    
    message += "风险提示：操作前请结合自身风险承受能力，市场波动可能导致策略失效！"
    return message
