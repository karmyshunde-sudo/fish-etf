# -*- coding: utf-8 -*-
"""
策略2 回测 - 均线缠绕 + MACD + 换手率
功能：
1. 遍历 data/daily/ 下所有股票一年的历史数据
2. 模拟交易：按策略条件买入，止盈止损或反向信号卖出
3. 生成交易流水和账户统计
4. 输出 stdout（供 GitHub Actions 手动触发推送）
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import traceback

# ========== 参数配置 ==========
DATA_DIR = "data/daily"
RESULT_DIR = "data/stock_backtest"
MA_PERIODS = [5, 10, 20]
MACD_SHORT, MACD_LONG, MACD_SIGNAL = 12, 26, 9
MACD_GROWTH_THRESHOLD = 0.4
TURNOVER_MIN, TURNOVER_MAX = 3.0, 10.0
BACKTEST_DAYS = 252  # 约一年交易日
INITIAL_CAPITAL = 100000
# ============================


def calc_ma(df, period):
    return df["收盘"].rolling(window=period).mean()


def calc_macd(df):
    ema_short = df["收盘"].ewm(span=MACD_SHORT, adjust=False).mean()
    ema_long = df["收盘"].ewm(span=MACD_LONG, adjust=False).mean()
    dif = ema_short - ema_long
    dea = dif.ewm(span=MACD_SIGNAL, adjust=False).mean()
    macd_bar = (dif - dea) * 2
    return dif, dea, macd_bar


def simulate_trading(df, code, name):
    """单只股票回测"""
    trades = []
    position = None
    entry_price = 0.0

    for i in range(max(MA_PERIODS) + MACD_LONG, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]

        # 买入逻辑
        ma_vals = [row[f"MA{p}"] for p in MA_PERIODS]
        if max(ma_vals) - min(ma_vals) < row["收盘"] * 0.01:  # 均线缠绕
            if row["MACD"] > 0 and prev["MACD"] > 0:
                if (row["MACD"] - prev["MACD"]) / abs(prev["MACD"]) >= MACD_GROWTH_THRESHOLD:
                    if TURNOVER_MIN <= row["换手率"] <= TURNOVER_MAX:
                        if not position:  # 空仓 -> 买入
                            position = "LONG"
                            entry_price = row["收盘"]
                            trades.append({
                                "date": row["日期"],
                                "code": code,
                                "name": name,
                                "action": "BUY",
                                "price": entry_price,
                                "reason": "策略信号"
                            })

        # 卖出逻辑（仅在有持仓时检查）
        if position == "LONG":
            if prev["MACD"] > 0 and row["MACD"] < prev["MACD"] * (1 - MACD_GROWTH_THRESHOLD):
                trades.append({
                    "date": row["日期"],
                    "code": code,
                    "name": name,
                    "action": "SELL",
                    "price": row["收盘"],
                    "reason": "MACD减弱"
                })
                position = None
            elif row["收盘"] < row["MA5"]:
                trades.append({
                    "date": row["日期"],
                    "code": code,
                    "name": name,
                    "action": "SELL",
                    "price": row["收盘"],
                    "reason": "跌破MA5"
                })
                position = None

    return trades


def run_backtest():
    all_trades = []
    capital = INITIAL_CAPITAL
    equity_curve = [capital]

    try:
        for file in os.listdir(DATA_DIR):
            if not file.endswith(".csv"):
                continue

            code = file.replace(".csv", "")
            try:
                df = pd.read_csv(os.path.join(DATA_DIR, file))
                if not {"日期", "股票代码", "收盘", "换手率"}.issubset(df.columns):
                    continue

                df = df.sort_values("日期").reset_index(drop=True)
                df = df.tail(BACKTEST_DAYS)  # 截取近一年

                # 加指标
                for p in MA_PERIODS:
                    df[f"MA{p}"] = calc_ma(df, p)
                df["DIF"], df["DEA"], df["MACD"] = calc_macd(df)

                name = df.iloc[-1]["股票代码"] if "股票代码" in df.columns else code
                trades = simulate_trading(df, code, name)
                all_trades.extend(trades)

                # 模拟资金曲线
                for t in trades:
                    if t["action"] == "BUY":
                        entry = t["price"]
                    elif t["action"] == "SELL":
                        profit = (t["price"] - entry) / entry * capital * 0.1  # 假设每次10%仓位
                        capital += profit
                        equity_curve.append(capital)

            except Exception as e:
                print(f"处理文件 {file} 出错: {e}")
                traceback.print_exc()

        # 账户统计
        total_return = (capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
        max_drawdown = (max(equity_curve) - min(equity_curve)) / max(equity_curve) * 100 if equity_curve else 0
        win_trades = [t for t in all_trades if t["action"] == "SELL" and t["price"] > entry]
        win_rate = len(win_trades) / max(1, len([t for t in all_trades if t["action"] == "SELL"])) * 100

        summary = {
            "总收益率": f"{total_return:.2f}%",
            "最大回撤": f"{max_drawdown:.2f}%",
            "胜率": f"{win_rate:.2f}%",
            "交易次数": len(all_trades) // 2
        }

        return all_trades, summary

    except Exception as e:
        print(f"运行回测出错: {e}")
        traceback.print_exc()
        return [], {}


def generate_backtest_message(trades, summary):
    today = datetime.today().strftime("%Y-%m-%d")
    lines = [f"【策略2 - 一年回测结果】", f"日期：{today}", ""]

    lines.append("📊 账户汇总：")
    for k, v in summary.items():
        lines.append(f"- {k}: {v}")

    lines.append("\n📑 交易流水：")
    if trades:
        for t in trades[:20]:  # 只显示前20条，避免太长
            if t["action"] == "BUY":
                lines.append(f"买入 {t['code']} {t['name']} @ {t['price']:.2f} （{t['reason']}）")
            else:
                lines.append(f"卖出 {t['code']} {t['name']} @ {t['price']:.2f} （{t['reason']}）")
    else:
        lines.append("无交易记录")

    return "\n".join(lines)


def main():
    trades, summary = run_backtest()
    msg = generate_backtest_message(trades, summary)

    # 保存 CSV 文件
    os.makedirs(RESULT_DIR, exist_ok=True)
    filename = f"{RESULT_DIR}/{datetime.today().strftime('%Y%m%d')}_backtest.csv"

    pd.DataFrame(trades).to_csv(filename, index=False, encoding="utf-8-sig")

    # 输出到控制台（供 GitHub Actions 推送）
    print(msg)


if __name__ == "__main__":
    main()
