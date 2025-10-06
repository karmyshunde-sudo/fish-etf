#!/usr/bin/env python3
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
import logging
import sys
from config import Config
from utils.date_utils import is_file_outdated

# ========== 初始化日志 ==========
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# ========== 参数配置 ==========
DATA_DIR = os.path.join(Config.DATA_DIR, "daily")
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
                if "换手率" in df.columns and TURNOVER_MIN <= latest["换手率"] <= TURNOVER_MAX:
                    signals.append({
                        "type": "BUY",
                        "code": code,
                        "name": name,
                        "close": latest["收盘"],
                        "turnover": latest["换手率"]
                    })

    # 卖出信号：MACD柱衰减 >= 40% 或 跌破5日线
    if "MACD" in df.columns and "MA5" in df.columns:
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
    today = datetime.now().strftime("%Y-%m-%d")
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
    valid_count = 0
    invalid_count = 0

    try:
        # 遍历日线数据
        for file in os.listdir(DATA_DIR):
            if not file.endswith(".csv"):
                continue

            code = file.replace(".csv", "")
            try:
                file_path = os.path.join(DATA_DIR, file)
                
                # 检查数据时效性
                if is_file_outdated(file_path, 1):
                    logger.info(f"文件 {file} 数据已过期，跳过处理")
                    invalid_count += 1
                    continue
                
                df = pd.read_csv(file_path)
                if not {"日期", "收盘", "换手率"}.issubset(df.columns):
                    logger.warning(f"文件 {file} 缺少必要列，跳过处理")
                    invalid_count += 1
                    continue
                
                # 确保日期列为字符串格式
                df["日期"] = df["日期"].astype(str)
                
                # 按日期排序
                df = df.sort_values("日期").reset_index(drop=True)
                
                # 获取股票名称
                name = df.iloc[-1]["名称"] if "名称" in df.columns else code
                
                # 检查数据量
                if len(df) < 20:
                    logger.warning(f"股票 {code} 数据量不足(只有{len(df)}天)，跳过处理")
                    invalid_count += 1
                    continue
                
                # 检查必要列
                required_columns = ["日期", "收盘", "换手率"]
                for col in required_columns:
                    if col not in df.columns:
                        logger.warning(f"股票 {code} 缺少必要列: {col}")
                        invalid_count += 1
                        continue
                
                # 检查数据有效性
                if df["收盘"].isna().any() or df["换手率"].isna().any():
                    logger.warning(f"股票 {code} 包含无效数据，跳过处理")
                    invalid_count += 1
                    continue
                
                signals = check_signals(df, code, name)
                all_signals.extend(signals)
                valid_count += 1
                if signals:
                    logger.info(f"股票 {code} - {name} 生成 {len(signals)} 个交易信号")
                else:
                    logger.debug(f"股票 {code} - {name} 未生成交易信号")

            except Exception as e:
                logger.error(f"文件 {file} 处理出错: {e}")
                traceback.print_exc()
                invalid_count += 1

        # 输出信号
        msg = generate_signal_message(all_signals)
        print(msg)
        logger.info(f"股票策略扫描完成: {valid_count}个有效股票，{invalid_count}个无效股票，共 {len(all_signals)} 个交易信号")

    except Exception as e:
        logger.error(f"运行出错: {e}")
        traceback.print_exc()
        print("【策略2 - 均线缠绕MACD换手率信号】\n执行时发生错误，无法生成交易信号")

if __name__ == "__main__":
    main()
