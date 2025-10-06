# -*- coding: utf-8 -*-
"""
策略2 - 均线缠绕 + MACD + 换手率信号检测
功能：
1. 遍历 data/daily/ 下所有股票日线数据
2. 计算 MA、MACD、换手率
3. 判断是否满足买入 / 卖出条件
4. 输出明日交易信号（stdout，供 GitHub Actions 推送）
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
import traceback

# ========== 参数配置 ==========
DATA_DIR = "data/daily"
MA_PERIODS = [5, 10, 20]
MACD_SHORT, MACD_LONG, MACD_SIGNAL = 12, 26, 9
MACD_GROWTH_THRESHOLD = 0.4  # MACD柱环比增长40%
TURNOVER_MIN, TURNOVER_MAX = 3.0, 10.0  # 换手率区间（%）
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


def check_signals(df, code, name):
    """判断买入 / 卖出信号"""
    signals = []

    if len(df) < max(MA_PERIODS) + MACD_LONG:
        return signals  # 数据不足

    # 均线
    for p in MA_PERIODS:
        df[f"MA{p}"] = calc_ma(df, p)

    # MACD
    df["DIF"], df["DEA"], df["MACD"] = calc_macd(df)

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    # 均线缠绕条件
    ma_vals = [latest[f"MA{p}"] for p in MA_PERIODS]
    if max(ma_vals) - min(ma_vals) < latest["收盘"] * 0.01:  # 缠绕：差距小于1%
        # MACD条件：柱在0轴上方且环比增长>=40%
        if latest["MACD"] > 0 and prev["MACD"] > 0:
            if prev["MACD"] > 0 and (latest["MACD"] - prev["MACD"]) / abs(prev["MACD"]) >= MACD_GROWTH_THRESHOLD:
                # 换手率条件
                if TURNOVER_MIN <= latest["换手率"] <= TURNOVER_MAX:
                    signals.append({
                        "type": "BUY",
                        "code": code,
                        "name": name,
                        "close": latest["收盘"],
                        "turnover": latest["换手率"]
                    })

    # 卖出信号：MACD柱衰减 >= 40% 或 跌破5日线
    if prev["MACD"] > 0 and latest["MACD"] < prev["MACD"] * (1 - MACD_GROWTH_THRESHOLD):
        signals.append({
            "type": "SELL",
            "code": code,
            "name": name,
            "close": latest["收盘"],
            "reason": "MACD减弱"
        })
    elif latest["收盘"] < latest["MA5"]:
        signals.append({
            "type": "SELL",
            "code": code,
            "name": name,
            "close": latest["收盘"],
            "reason": "跌破MA5"
        })

    return signals


def generate_signal_message(all_signals):
    today = datetime.today().strftime("%Y-%m-%d")
    lines = [f"【策略2 - 均线缠绕MACD换手率信号】", f"日期：{today}", ""]

    buy_signals = [s for s in all_signals if s["type"] == "BUY"]
    sell_signals = [s for s in all_signals if s["type"] == "SELL"]

    if buy_signals:
        lines.append("买入信号：")
        for s in buy_signals:
            lines.append(f"- {s['code']} {s['name']}（收盘价：{s['close']:.2f}，换手率：{s['turnover']:.2f}%）")
    else:
        lines.append("买入信号：无")

    lines.append("")

    if sell_signals:
        lines.append("卖出信号：")
        for s in sell_signals:
            lines.append(f"- {s['code']} {s['name']}（收盘价：{s['close']:.2f}，原因：{s['reason']}）")
    else:
        lines.append("卖出信号：无")

    return "\n".join(lines)


def main():
    all_signals = []

    try:
        # 遍历日线数据
        for file in os.listdir(DATA_DIR):
            if not file.endswith(".csv"):
                continue

            code = file.replace(".csv", "")
            try:
                df = pd.read_csv(os.path.join(DATA_DIR, file))
                if not {"日期", "代码", "收盘", "换手率"}.issubset(df.columns):
                    continue

                df = df.sort_values("日期").reset_index(drop=True)
                name = df.iloc[-1]["代码"] if "代码" in df.columns else code
                signals = check_signals(df, code, name)
                all_signals.extend(signals)

            except Exception as e:
                print(f"文件 {file} 处理出错: {e}")
                traceback.print_exc()

        # 输出信号
        msg = generate_signal_message(all_signals)
        print(msg)

    except Exception as e:
        print(f"运行出错: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
