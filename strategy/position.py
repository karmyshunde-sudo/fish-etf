import pandas as pd
import os
from config import Config
from utils.file_utils import load_etf_daily_data, init_dirs
from .etf_scoring import get_top_rated_etfs, get_etf_name, get_etf_basic_info

# 仓位持仓记录路径
POSITION_RECORD_PATH = "data/position_record.csv"

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
            "最新操作": ["未持仓", "未持仓"]
        })
        position_df.to_csv(POSITION_RECORD_PATH, index=False, encoding="utf-8")
    return pd.read_csv(POSITION_RECORD_PATH, encoding="utf-8")

def update_position_record(position_type, etf_code, etf_name, cost_price, action):
    """更新仓位记录"""
    position_df = init_position_record()
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    # 找到对应仓位行
    idx = position_df[position_df["仓位类型"] == position_type].index[0]
    position_df.loc[idx] = [
        position_type,
        etf_code,
        etf_name,
        cost_price,
        today,
        action
    ]
    position_df.to_csv(POSITION_RECORD_PATH, index=False, encoding="utf-8")
    return position_df

def calculate_position_strategy():
    """
    计算仓位操作策略（稳健仓、激进仓）
    逻辑：
    - 稳健仓：选综合评分最高的ETF，均线多头（5日>20日）则持有/加仓，跌破止损则卖出
    - 激进仓：选近30天收益最高的ETF，波动超阈值则换股
    """
    print("="*50)
    print("开始计算ETF仓位操作策略")
    print("="*50)
    
    # 1. 初始化仓位记录
    position_df = init_position_record()
    # 获取评分前3的ETF（用于选仓）
    top_etfs = get_top_rated_etfs(top_n=3)
    if top_etfs.empty:
        print("无有效ETF评分数据，无法计算仓位策略")
        return "【ETF仓位操作提示】\n无有效ETF数据，无法生成操作建议"
    
    # 2. 分别计算稳健仓和激进仓策略
    strategies = {}
    # 2.1 稳健仓策略（评分最高+均线策略）
    stable_etf = top_etfs.iloc[0]
    stable_code = stable_etf["etf_code"]
    stable_name = stable_etf["etf_name"]
    stable_df = load_etf_daily_data(stable_code)
    
    # 稳健仓当前持仓
    stable_position = position_df[position_df["仓位类型"] == "稳健仓"].iloc[0]
    strategies["稳健仓"] = calculate_single_position_strategy(
        position_type="稳健仓",
        current_position=stable_position,
        target_etf_code=stable_code,
        target_etf_name=stable_name,
        etf_df=stable_df,
        is_stable=True  # 稳健仓标记
    )
    
    # 2.2 激进仓策略（近30天收益最高）
    # 计算各ETF近30天收益
    return_list = []
    for _, row in top_etfs.iterrows():
        code = row["etf_code"]
        df = load_etf_daily_data(code)
        if not df.empty and len(df) >=30:
            return_30d = (df.iloc[-1]["收盘价"] / df.iloc[-30]["收盘价"] - 1) * 100
            return_list.append({
                "etf_code": code,
                "etf_name": row["etf_name"],
                "return_30d": return_30d
            })
    if return_list:
        aggressive_etf = max(return_list, key=lambda x: x["return_30d"])
        aggressive_code = aggressive_etf["etf_code"]
        aggressive_name = aggressive_etf["etf_name"]
        aggressive_df = load_etf_daily_data(aggressive_code)
        
        # 激进仓当前持仓
        aggressive_position = position_df[position_df["仓位类型"] == "激进仓"].iloc[0]
        strategies["激进仓"] = calculate_single_position_strategy(
            position_type="激进仓",
            current_position=aggressive_position,
            target_etf_code=aggressive_code,
            target_etf_name=aggressive_name,
            etf_df=aggressive_df,
            is_stable=False  # 激进仓标记
        )
    else:
        strategies["激进仓"] = "激进仓：无有效收益数据，暂不调整仓位"
    
    # 3. 格式化消息
    return format_position_message(strategies)

def calculate_single_position_strategy(position_type, current_position, target_etf_code, target_etf_name, etf_df, is_stable):
    """计算单个仓位（稳健/激进）的操作策略"""
    if etf_df.empty or len(etf_df) < Config.MA_LONG_PERIOD:
        return f"{position_type}：目标ETF数据不足，暂不调整"
    
    # 计算均线
    etf_df["5日均线"] = etf_df["收盘价"].rolling(window=Config.MA_SHORT_PERIOD).mean()
    etf_df["20日均线"] = etf_df["收盘价"].rolling(window=Config.MA_LONG_PERIOD).mean()
    latest_close = etf_df.iloc[-1]["收盘价"]
    ma_short = etf_df.iloc[-1]["5日均线"]
    ma_long = etf_df.iloc[-1]["20日均线"]
    
    # 当前无持仓：判断是否买入
    if pd.isna(current_position["当前持仓ETF代码"]) or current_position["当前持仓ETF代码"] == "":
        # 稳健仓：均线多头（5日>20日）才买入；激进仓：直接买入收益最高的
        if (is_stable and ma_short > ma_long) or (not is_stable):
            # 执行买入，更新仓位记录
            update_position_record(
                position_type=position_type,
                etf_code=target_etf_code,
                etf_name=target_etf_name,
                cost_price=latest_close,
                action=f"买入（成本价：{latest_close:.2f}元）"
            )
            return f"{position_type}：当前无持仓，建议买入【{target_etf_name}（{target_etf_code}）】，成本价：{latest_close:.2f}元"
        else:
            return f"{position_type}：当前无持仓，目标ETF未满足买入条件（均线未多头），暂不买入"
    
    # 当前有持仓：判断是否换股、加仓、止损
    current_code = current_position["当前持仓ETF代码"]
    current_name = current_position["当前持仓ETF名称"]
    current_cost = float(current_position["持仓成本价"])
    
    # 1. 判断是否换股（目标ETF≠当前持仓）
    if current_code != target_etf_code:
        # 执行换股，更新仓位记录
        update_position_record(
            position_type=position_type,
            etf_code=target_etf_code,
            etf_name=target_etf_name,
            cost_price=latest_close,
            action=f"换股（卖出{current_name}，买入{target_etf_name}，成本价：{latest_close:.2f}元）"
        )
        return f"{position_type}：建议换股\n原持仓：{current_name}（{current_code}）\n新持仓：{target_etf_name}（{target_etf_code}）\n买入成本价：{latest_close:.2f}元"
    
    # 2. 持仓不变：判断是否加仓/止损
    profit_rate = (latest_close / current_cost - 1) * 100  # 持仓收益率
    
    # 止损判断（跌破止损阈值）
    if profit_rate <= Config.STOP_LOSS_THRESHOLD * 100:
        # 执行止损，清空持仓
        update_position_record(
            position_type=position_type,
            etf_code="",
            etf_name="",
            cost_price=0.0,
            action=f"止损卖出（持仓收益率：{profit_rate:.2f}%）"
        )
        return f"{position_type}：当前持仓【{current_name}（{current_code}）】\n持仓收益率：{profit_rate:.2f}%（跌破止损阈值{Config.STOP_LOSS_THRESHOLD*100:.1f}%）\n建议：止损卖出"
    
    # 加仓判断（稳健仓：均线多头+涨幅超加仓阈值；激进仓：涨幅超加仓阈值）
    if (is_stable and ma_short > ma_long and profit_rate >= Config.ADD_POSITION_THRESHOLD * 100) or \
       (not is_stable and profit_rate >= Config.ADD_POSITION_THRESHOLD * 100):
        return f"{position_type}：当前持仓【{current_name}（{current_code}）】\n持仓收益率：{profit_rate:.2f}%（超加仓阈值{Config.ADD_POSITION_THRESHOLD*100:.1f}%）\n建议：可加仓（参考成本价：{latest_close:.2f}元）"
    
    # 无操作：持仓正常
    return f"{position_type}：当前持仓【{current_name}（{current_code}）】\n持仓收益率：{profit_rate:.2f}%\n均线状态：5日均线{ma_short:.2f}元 {'＞' if ma_short>ma_long else '＜'} 20日均线{ma_long:.2f}元\n建议：继续持有，无需调整"

def format_position_message(strategies):
    """格式化仓位策略消息"""
    message = "【ETF仓位操作提示】\n"
    message += "（每个仓位仅持有1只ETF，操作建议基于最新数据）\n\n"
    
    for position_type, content in strategies.items():
        message += f"【{position_type}】\n{content}\n\n"
    
    message += "风险提示：操作前请结合自身风险承受能力，市场波动可能导致策略失效！"
    return message
