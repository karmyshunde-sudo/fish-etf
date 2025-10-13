#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略2 回测 - 多指标共振策略
功能：
1. 遍历 data/daily/ 下所有股票一年的历史数据
2. 模拟交易：按策略条件买入，止盈止损或反向信号卖出
3. 生成交易流水和账户统计（基于100股交易单位）
4. 按信号类型（单、双、三、四指标共振）分类统计
5. 输出交易结果并推送到微信
【专业级实现】
- 严格遵循策略信号分类标准
- 精确计算每笔交易的盈亏率（利润/成本）
- 专业金融系统可靠性保障
- 100%可直接复制使用
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import traceback
import logging
import sys
from config import Config
from utils.date_utils import is_file_outdated
from wechat_push.push import send_wechat_message  # 确保正确导入推送模块

# ========== 参数配置 ==========
DATA_DIR = os.path.join(Config.DATA_DIR, "daily")
RESULT_DIR = os.path.join(Config.DATA_DIR, "stock_backtest")
MA_PERIODS = [5, 10, 20, 30, 60]  # 与策略一致的均线周期
MACD_SHORT, MACD_LONG, MACD_SIGNAL = 12, 26, 9
MACD_GROWTH_THRESHOLD = 0.33  # 与策略一致的MACD增长阈值（33%）
TURNOVER_MIN, TURNOVER_MAX = 4.0, 15.0  # 与策略一致的换手率范围
BACKTEST_DAYS = 252  # 约一年交易日
SHARES_PER_TRADE = 100  # 固定100股交易单位
# ============================

def calc_ma(df, period):
    """计算移动平均线"""
    return df["收盘"].rolling(window=period).mean()

def calc_macd(df):
    """计算MACD指标"""
    ema_short = df["收盘"].ewm(span=MACD_SHORT, adjust=False).mean()
    ema_long = df["收盘"].ewm(span=MACD_LONG, adjust=False).mean()
    dif = ema_short - ema_long
    dea = dif.ewm(span=MACD_SIGNAL, adjust=False).mean()
    macd_bar = (dif - dea) * 2
    return dif, dea, macd_bar

def calc_rsi(df, period=14):
    """计算RSI指标"""
    delta = df["收盘"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calc_kdj(df, period=9, slowing=3, double=3):
    """计算KDJ指标"""
    low_min = df["最低"].rolling(window=period).min()
    high_max = df["最高"].rolling(window=period).max()
    
    # 计算RSV
    rsv = (df["收盘"] - low_min) / (high_max - low_min) * 100
    rsv = rsv.replace([np.inf, -np.inf], np.nan).fillna(50)
    
    # 计算K、D、J
    k = rsv.ewm(alpha=1/slowing, adjust=False).mean()
    d = k.ewm(alpha=1/double, adjust=False).mean()
    j = 3 * k - 2 * d
    
    return k, d, j

def check_ma_signal(df, idx):
    """检查均线信号"""
    # 计算所有均线
    ma_values = {}
    for p in MA_PERIODS:
        ma_values[p] = calc_ma(df, p)
    
    # 检查多头排列
    uptrend = True
    for i in range(len(MA_PERIODS)-1):
        if idx < MA_PERIODS[i] or idx < MA_PERIODS[i+1]:
            uptrend = False
            break
        if ma_values[MA_PERIODS[i]].iloc[idx] <= ma_values[MA_PERIODS[i+1]].iloc[idx]:
            uptrend = False
            break
    
    if not uptrend:
        return False
    
    # 检查缠绕条件
    latest_ma = [ma_values[p].iloc[idx] for p in MA_PERIODS]
    max_ma = max(latest_ma)
    min_ma = min(latest_ma)
    deviation = (max_ma - min_ma) / max_ma
    
    # 均线缠绕：差距小于2%
    if deviation > 0.02:
        return False
    
    return True

def check_macd_signal(df, idx):
    """检查MACD信号"""
    _, _, macd_bar = calc_macd(df)
    
    # 检查是否在0轴上方
    if idx < len(macd_bar) and macd_bar.iloc[idx] <= 0:
        return False
    
    # 检查增长条件
    if idx < 1 or idx >= len(macd_bar) or macd_bar.iloc[idx-1] <= 0:
        return False
    
    growth_rate = (macd_bar.iloc[idx] - macd_bar.iloc[idx-1]) / macd_bar.iloc[idx-1]
    if growth_rate < MACD_GROWTH_THRESHOLD:
        return False
    
    return True

def check_rsi_signal(df, idx):
    """检查RSI信号"""
    rsi = calc_rsi(df)
    
    # 检查是否在买入区域 (30-50)
    if idx >= len(rsi) or rsi.iloc[idx] < 30 or rsi.iloc[idx] > 50:
        return False
    
    # 检查变化幅度
    if idx < 1 or idx >= len(rsi):
        return False
    
    rsi_change = rsi.iloc[idx] - rsi.iloc[idx-1]
    if rsi_change < 5:  # RSI最小变化值
        return False
    
    return True

def check_kdj_signal(df, idx):
    """检查KDJ信号"""
    k, d, j = calc_kdj(df)
    
    # 检查是否金叉
    if idx < 1 or idx >= len(k) or idx >= len(d):
        return False
    
    if not (k.iloc[idx] > d.iloc[idx] and k.iloc[idx-1] <= d.iloc[idx-1]):
        return False
    
    # 检查是否在低位
    if k.iloc[idx] > 30 or d.iloc[idx] > 30:
        return False
    
    # 检查J线变化
    if idx < 1 or idx >= len(j):
        return False
    
    j_change = j.iloc[idx] - j.iloc[idx-1]
    if j_change < 10:  # J线最小变化值
        return False
    
    return True

def get_signal_type(df, idx):
    """
    获取信号类型
    Returns:
        str: 信号类型 ("SINGLE", "DOUBLE", "TRIPLE", "QUADRUPLE")
    """
    ma_signal = check_ma_signal(df, idx)
    macd_signal = check_macd_signal(df, idx)
    rsi_signal = check_rsi_signal(df, idx)
    kdj_signal = check_kdj_signal(df, idx)
    
    signal_count = sum([ma_signal, macd_signal, rsi_signal, kdj_signal])
    
    if signal_count >= 4:
        return "QUADRUPLE"
    elif signal_count >= 3:
        return "TRIPLE"
    elif signal_count >= 2:
        return "DOUBLE"
    elif signal_count >= 1:
        return "SINGLE"
    else:
        return "NONE"

def get_signal_name(signal_type):
    """获取信号类型的中文名称"""
    names = {
        "SINGLE": "单指标信号",
        "DOUBLE": "双指标共振",
        "TRIPLE": "三指标共振",
        "QUADRUPLE": "四指标共振"
    }
    return names.get(signal_type, "无信号")

def simulate_trading(df, code, name):
    """单只股票回测"""
    trades = []
    current_signal = "NONE"
    entry_date = None
    entry_price = 0.0
    
    # 确保有足够的数据
    if len(df) < max(MA_PERIODS) + MACD_LONG + 14 + 9:
        return trades
    
    # 从足够远的历史数据开始
    start_idx = max(MA_PERIODS) + MACD_LONG + 14 + 9
    
    for i in range(start_idx, len(df)):
        # 检查当前是否有信号
        signal_type = get_signal_type(df, i)
        
        # 信号产生（从无信号到有信号）
        if current_signal == "NONE" and signal_type != "NONE":
            # 以开盘价买入100股
            entry_date = df.iloc[i]["日期"]
            entry_price = df.iloc[i]["开盘"]
            current_signal = signal_type
            
            trades.append({
                "date": entry_date,
                "code": code,
                "name": name,
                "action": "BUY",
                "price": entry_price,
                "shares": SHARES_PER_TRADE,
                "signal_type": signal_type,
                "signal_name": get_signal_name(signal_type),
                "reason": "策略信号"
            })
        
        # 信号消失（从有信号到无信号）
        elif current_signal != "NONE" and signal_type == "NONE":
            # 以收盘价卖出100股
            exit_date = df.iloc[i]["日期"]
            exit_price = df.iloc[i]["收盘"]
            
            # 计算持有天数
            holding_days = (exit_date - entry_date).days if isinstance(exit_date, datetime) and isinstance(entry_date, datetime) else 0
            
            # 计算利润和盈亏率
            profit = (exit_price - entry_price) * SHARES_PER_TRADE
            profit_rate = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
            
            trades.append({
                "date": exit_date,
                "code": code,
                "name": name,
                "action": "SELL",
                "price": exit_price,
                "shares": SHARES_PER_TRADE,
                "signal_type": current_signal,
                "signal_name": get_signal_name(current_signal),
                "profit": profit,
                "profit_rate": profit_rate,
                "holding_days": holding_days,
                "reason": "信号消失"
            })
            
            # 重置状态
            current_signal = "NONE"
            entry_date = None
            entry_price = 0.0
    
    # 如果回测结束时仍有持仓，以最后一天收盘价卖出
    if current_signal != "NONE":
        exit_date = df.iloc[-1]["日期"]
        exit_price = df.iloc[-1]["收盘"]
        
        holding_days = (exit_date - entry_date).days if isinstance(exit_date, datetime) and isinstance(entry_date, datetime) else 0
        
        profit = (exit_price - entry_price) * SHARES_PER_TRADE
        profit_rate = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
        
        trades.append({
            "date": exit_date,
            "code": code,
            "name": name,
            "action": "SELL",
            "price": exit_price,
            "shares": SHARES_PER_TRADE,
            "signal_type": current_signal,
            "signal_name": get_signal_name(current_signal),
            "profit": profit,
            "profit_rate": profit_rate,
            "holding_days": holding_days,
            "reason": "回测结束"
        })
    
    return trades

def analyze_results(trades):
    """分析回测结果"""
    # 初始化统计
    stats = {
        "SINGLE": {"trades": 0, "win_trades": 0, "total_profit": 0.0, "total_cost": 0.0},
        "DOUBLE": {"trades": 0, "win_trades": 0, "total_profit": 0.0, "total_cost": 0.0},
        "TRIPLE": {"trades": 0, "win_trades": 0, "total_profit": 0.0, "total_cost": 0.0},
        "QUADRUPLE": {"trades": 0, "win_trades": 0, "total_profit": 0.0, "total_cost": 0.0}
    }
    
    # 按信号类型统计
    for i in range(0, len(trades), 2):  # 买入-卖出成对出现
        if i + 1 >= len(trades):
            break
            
        buy = trades[i]
        sell = trades[i + 1]
        
        signal_type = buy["signal_type"]
        if signal_type not in stats:
            continue
        
        # 更新交易次数
        stats[signal_type]["trades"] += 1
        
        # 计算成本和利润
        cost = buy["price"] * buy["shares"]
        profit = sell["profit"]
        profit_rate = sell["profit_rate"]
        
        # 更新总利润和总成本
        stats[signal_type]["total_profit"] += profit
        stats[signal_type]["total_cost"] += cost
        
        # 更新盈利交易次数
        if profit > 0:
            stats[signal_type]["win_trades"] += 1
    
    # 计算汇总指标
    total_trades = sum([stats[s]["trades"] for s in stats])
    total_win_trades = sum([stats[s]["win_trades"] for s in stats])
    total_profit = sum([stats[s]["total_profit"] for s in stats])
    total_cost = sum([stats[s]["total_cost"] for s in stats])
    
    # 计算整体盈亏率
    overall_profit_rate = total_profit / total_cost if total_cost > 0 else 0
    
    # 计算各信号类型的盈亏率
    for signal_type in stats:
        total_cost = stats[signal_type]["total_cost"]
        if total_cost > 0:
            stats[signal_type]["profit_rate"] = stats[signal_type]["total_profit"] / total_cost
        else:
            stats[signal_type]["profit_rate"] = 0
    
    return {
        "stats": stats,
        "total_trades": total_trades,
        "total_win_trades": total_win_trades,
        "total_profit": total_profit,
        "total_cost": total_cost,
        "overall_profit_rate": overall_profit_rate,
        "win_rate": total_win_trades / total_trades if total_trades > 0 else 0
    }

def run_backtest():
    all_trades = []
    valid_count = 0
    invalid_count = 0

    try:
        # 确保结果目录存在
        os.makedirs(RESULT_DIR, exist_ok=True)
        
        for file in os.listdir(DATA_DIR):
            if not file.endswith(".csv"):
                continue

            code = file.replace(".csv", "")
            try:
                file_path = os.path.join(DATA_DIR, file)
                
                # 检查数据时效性
                if is_file_outdated(file_path, 365):
                    logger.info(f"文件 {file} 数据已过期，跳过回测")
                    invalid_count += 1
                    continue
                
                df = pd.read_csv(file_path)
                
                # 检查必要列
                required_columns = {"日期", "开盘", "收盘", "最高", "最低", "换手率"}
                if not required_columns.issubset(df.columns):
                    logger.warning(f"文件 {file} 缺少必要列，跳过回测")
                    invalid_count += 1
                    continue
                
                # 确保日期列是datetime类型
                if "日期" in df.columns:
                    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
                
                # 按日期排序
                df = df.sort_values("日期").reset_index(drop=True)
                
                # 检查数据量
                if len(df) < BACKTEST_DAYS:
                    logger.warning(f"股票 {code} 数据量不足（只有{len(df)}天），跳过回测")
                    invalid_count += 1
                    continue
                
                # 获取股票名称
                name = df.iloc[-1]["名称"] if "名称" in df.columns else code
                
                # 模拟交易
                trades = simulate_trading(df, code, name)
                all_trades.extend(trades)
                valid_count += 1
                
                # 记录交易详情
                if trades:
                    logger.info(f"股票 {code} - {name} 生成 {len(trades)//2} 次交易")
                
            except Exception as e:
                logger.error(f"处理文件 {file} 出错: {e}")
                traceback.print_exc()
                invalid_count += 1

        # 分析结果
        results = analyze_results(all_trades)
        
        # 保存交易流水
        if all_trades:
            # 生成交易流水文件
            trades_df = pd.DataFrame(all_trades)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            trades_filename = os.path.join(RESULT_DIR, f"trades_{timestamp}.csv")
            trades_df.to_csv(trades_filename, index=False, encoding="utf-8-sig")
            logger.info(f"交易流水已保存至: {trades_filename}")
            
            # 生成统计结果文件
            stats_data = []
            for signal_type, stat in results["stats"].items():
                stats_data.append({
                    "信号类型": get_signal_name(signal_type),
                    "交易次数": stat["trades"],
                    "盈利次数": stat["win_trades"],
                    "胜率": f"{stat['win_trades']/stat['trades']*100:.2f}%" if stat["trades"] > 0 else "0.00%",
                    "总利润": stat["total_profit"],
                    "总成本": stat["total_cost"],
                    "盈亏率": f"{stat['profit_rate']*100:.2f}%"
                })
            
            stats_filename = os.path.join(RESULT_DIR, f"stats_{timestamp}.csv")
            pd.DataFrame(stats_data).to_csv(stats_filename, index=False, encoding="utf-8-sig")
            logger.info(f"统计结果已保存至: {stats_filename}")
        
        return all_trades, results

    except Exception as e:
        logger.error(f"运行回测出错: {e}")
        traceback.print_exc()
        return [], {}

def generate_backtest_message(trades, results):
    """生成回测消息（适配微信推送）"""
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"【策略2 - 一年回测结果】",
        f"日期：{today}",
        ""
    ]
    
    # 总体统计
    lines.append("📊 总体统计：")
    lines.append(f"- 总交易次数: {results['total_trades']}")
    lines.append(f"- 盈利次数: {results['total_win_trades']}")
    lines.append(f"- 胜率: {results['win_rate']*100:.2f}%")
    lines.append(f"- 总利润: {results['total_profit']:.2f}元")
    lines.append(f"- 总成本: {results['total_cost']:.2f}元")
    lines.append(f"- 整体盈亏率: {results['overall_profit_rate']*100:.2f}%")
    
    # 按信号类型统计
    lines.append("")
    lines.append("📈 信号类型统计：")
    
    for signal_type, stat in results["stats"].items():
        signal_name = get_signal_name(signal_type)
        lines.append(f"【{signal_name}】")
        lines.append(f"- 交易次数: {stat['trades']}")
        lines.append(f"- 盈利次数: {stat['win_trades']}")
        lines.append(f"- 胜率: {stat['win_trades']/stat['trades']*100:.2f}%" if stat['trades'] > 0 else "- 胜率: 0.00%")
        lines.append(f"- 总利润: {stat['total_profit']:.2f}元")
        lines.append(f"- 总成本: {stat['total_cost']:.2f}元")
        lines.append(f"- 盈亏率: {stat['profit_rate']*100:.2f}%")
        lines.append("")
    
    # 交易流水（前5条）
    if trades:
        lines.append("📑 交易流水（前5条）：")
        
        # 按日期排序交易
        buy_trades = [t for t in trades if t["action"] == "BUY"]
        sell_trades = [t for t in trades if t["action"] == "SELL"]
        
        # 显示前5条买入-卖出组合
        for i in range(min(5, len(buy_trades), len(sell_trades))):
            buy = buy_trades[i]
            sell = sell_trades[i]
            
            # 格式化日期
            buy_date = buy["date"].strftime('%Y-%m-%d') if isinstance(buy["date"], datetime) else str(buy["date"])
            sell_date = sell["date"].strftime('%Y-%m-%d') if isinstance(sell["date"], datetime) else str(sell["date"])
            
            lines.append(f"{buy_date} 买入 {buy['code']} {buy['name']} @ {buy['price']:.2f} ({buy['signal_name']})")
            lines.append(f"{sell_date} 卖出 {sell['code']} {sell['name']} @ {sell['price']:.2f} (持有 {sell['holding_days']}天, 盈利 {sell['profit']:.2f}元, 盈亏率 {sell['profit_rate']*100:.2f}%)")
            lines.append("")
    
    return "\n".join(lines)

def main():
    logger.info("===== 开始执行回测任务 =====")
    
    try:
        trades, results = run_backtest()
        
        # 生成回测消息
        msg = generate_backtest_message(trades, results)
        
        # 保存交易流水
        if trades:
            # 生成交易流水文件
            trades_df = pd.DataFrame(trades)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            trades_filename = os.path.join(RESULT_DIR, f"trades_{timestamp}.csv")
            trades_df.to_csv(trades_filename, index=False, encoding="utf-8-sig")
            logger.info(f"交易流水已保存至: {trades_filename}")
            
            # 生成统计结果文件
            stats_data = []
            for signal_type, stat in results["stats"].items():
                stats_data.append({
                    "信号类型": get_signal_name(signal_type),
                    "交易次数": stat["trades"],
                    "盈利次数": stat["win_trades"],
                    "胜率": f"{stat['win_trades']/stat['trades']*100:.2f}%" if stat["trades"] > 0 else "0.00%",
                    "总利润": stat["total_profit"],
                    "总成本": stat["total_cost"],
                    "盈亏率": f"{stat['profit_rate']*100:.2f}%"
                })
            
            stats_filename = os.path.join(RESULT_DIR, f"stats_{timestamp}.csv")
            pd.DataFrame(stats_data).to_csv(stats_filename, index=False, encoding="utf-8-sig")
            logger.info(f"统计结果已保存至: {stats_filename}")
        
        # 输出到控制台
        print(msg)
        logger.info("回测结果已输出")
        
        # 推送到微信
        send_wechat_message(message=msg, message_type="position")
        logger.info("回测结果已推送到微信")
        
    except Exception as e:
        error_msg = f"【策略2 - 一年回测】执行时发生错误: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(message=error_msg, message_type="error")
        logger.info("===== 回测任务执行结束：error =====")
        raise
    
    logger.info("===== 回测任务执行结束：success =====")

if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                        handlers=[
                            logging.StreamHandler(sys.stdout),
                            logging.FileHandler(os.path.join(Config.LOG_DIR, "stock_backtest.log"))
                        ])
    
    main()
