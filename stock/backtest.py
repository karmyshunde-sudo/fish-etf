#!/usr/bin/env python3
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
RESULT_DIR = os.path.join(Config.DATA_DIR, "stock_backtest")
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
    # 使用字典跟踪每只股票的交易状态，解决entry变量被覆盖问题
    stock_status = {
        "position": None,
        "entry_price": 0.0,
        "entry_date": None
    }
    
    for i in range(max(MA_PERIODS) + MACD_LONG, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i - 1]

        # 买入逻辑
        ma_vals = [row[f"MA{p}"] for p in MA_PERIODS]
        if max(ma_vals) - min(ma_vals) < row["收盘"] * 0.01:  # 均线缠绕
            if row["MACD"] > 0 and prev["MACD"] > 0:
                if (row["MACD"] - prev["MACD"]) / abs(prev["MACD"]) >= MACD_GROWTH_THRESHOLD:
                    if TURNOVER_MIN <= row["换手率"] <= TURNOVER_MAX:
                        if stock_status["position"] is None:  # 空仓 -> 买入
                            stock_status["position"] = "LONG"
                            stock_status["entry_price"] = row["收盘"]
                            stock_status["entry_date"] = row["日期"]  # 已是datetime类型
                            trades.append({
                                "date": row["日期"],
                                "code": code,
                                "name": name,
                                "action": "BUY",
                                "price": stock_status["entry_price"],
                                "reason": "策略信号",
                                "entry_date": stock_status["entry_date"]
                            })

        # 卖出逻辑（仅在有持仓时检查）
        if stock_status["position"] == "LONG":
            # 卖出条件1：MACD柱衰减 >= 40%
            if prev["MACD"] > 0 and row["MACD"] < prev["MACD"] * (1 - MACD_GROWTH_THRESHOLD):
                # 【日期datetime类型规则】直接使用datetime对象计算
                holding_days = (row["日期"] - stock_status["entry_date"]).days
                trades.append({
                    "date": row["日期"],
                    "code": code,
                    "name": name,
                    "action": "SELL",
                    "price": row["收盘"],
                    "reason": "MACD减弱",
                    "entry_price": stock_status["entry_price"],
                    "entry_date": stock_status["entry_date"],
                    "holding_days": holding_days
                })
                # 清除持仓状态
                stock_status = {"position": None, "entry_price": 0.0, "entry_date": None}
                
            # 卖出条件2：跌破5日线
            elif row["收盘"] < row["MA5"]:
                # 【日期datetime类型规则】直接使用datetime对象计算
                holding_days = (row["日期"] - stock_status["entry_date"]).days
                trades.append({
                    "date": row["日期"],
                    "code": code,
                    "name": name,
                    "action": "SELL",
                    "price": row["收盘"],
                    "reason": "跌破MA5",
                    "entry_price": stock_status["entry_price"],
                    "entry_date": stock_status["entry_date"],
                    "holding_days": holding_days
                })
                # 清除持仓状态
                stock_status = {"position": None, "entry_price": 0.0, "entry_date": None}

    return trades

def run_backtest():
    all_trades = []
    capital = INITIAL_CAPITAL
    equity_curve = [capital]
    valid_count = 0
    invalid_count = 0

    try:
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
                
                # 检查必要列（修复列名不一致问题）
                if not {"日期", "股票代码", "收盘", "换手率"}.issubset(df.columns):
                    logger.warning(f"文件 {file} 缺少必要列，跳过回测")
                    invalid_count += 1
                    continue
                
                # 【日期datetime类型规则】确保日期列是datetime类型
                if "日期" in df.columns:
                    # 尝试多种日期格式
                    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
                    # 确保日期列是datetime类型
                    if df["日期"].isnull().all():
                        logger.warning(f"股票 {code} 日期格式解析失败，尝试其他格式")
                        # 尝试其他格式
                        df["日期"] = pd.to_datetime(df["日期"], format="%Y/%m/%d", errors="coerce")
                        if df["日期"].isnull().all():
                            logger.error(f"股票 {code} 无法解析日期格式")
                            invalid_count += 1
                            continue
                
                # 按日期排序
                df = df.sort_values("日期").reset_index(drop=True)
                
                # 检查数据量
                if len(df) < BACKTEST_DAYS:
                    logger.warning(f"股票 {code} 数据量不足（只有{len(df)}天），跳过回测")
                    invalid_count += 1
                    continue
                
                # 获取股票名称（修复股票名称获取错误）
                name = df.iloc[-1]["名称"] if "名称" in df.columns else code
                
                # 计算指标
                for p in MA_PERIODS:
                    df[f"MA{p}"] = calc_ma(df, p)
                df["DIF"], df["DEA"], df["MACD"] = calc_macd(df)
                
                # 模拟交易
                trades = simulate_trading(df, code, name)
                all_trades.extend(trades)
                valid_count += 1
                
                # 模拟资金曲线
                for t in trades:
                    if t["action"] == "BUY":
                        entry_price = t["price"]
                    elif t["action"] == "SELL":
                        # 使用交易记录中的买入价，解决entry变量覆盖问题
                        entry_price = t.get("entry_price", entry_price)
                        # 计算收益（假设每次交易使用10%的仓位）
                        position_size = 0.1
                        position_value = capital * position_size
                        profit = (t["price"] - entry_price) / entry_price * position_value
                        capital += profit
                        equity_curve.append(capital)
                        # 记录交易详情
                        logger.info(f"股票 {code} 交易: {t['action']} 价格: {t['price']:.2f}, 收益: {profit:.2f}")
                        
            except Exception as e:
                logger.error(f"处理文件 {file} 出错: {e}")
                traceback.print_exc()
                invalid_count += 1

        # 账户统计
        total_return = (capital - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
        max_drawdown = (max(equity_curve) - min(equity_curve)) / max(equity_curve) * 100 if equity_curve else 0
        win_trades = [t for t in all_trades if t["action"] == "SELL" and t["price"] > t.get("entry_price", 0)]
        total_trades = len([t for t in all_trades if t["action"] == "SELL"])
        win_rate = len(win_trades) / total_trades * 100 if total_trades > 0 else 0

        summary = {
            "总收益率": f"{total_return:.2f}%",
            "最大回撤": f"{max_drawdown:.2f}%",
            "胜率": f"{win_rate:.2f}%",
            "交易次数": len(all_trades) // 2,
            "总交易数": len(all_trades),
            "买入交易数": len([t for t in all_trades if t["action"] == "BUY"]),
            "卖出交易数": len([t for t in all_trades if t["action"] == "SELL"]),
            "有效股票数": valid_count,
            "无效股票数": invalid_count
        }

        return all_trades, summary

    except Exception as e:
        logger.error(f"运行回测出错: {e}")
        traceback.print_exc()
        return [], {}

def generate_backtest_message(trades, summary):
    # 【日期datetime类型规则】使用datetime对象
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [f"【策略2 - 一年回测结果】", f"日期：{today}", ""]
    
    # 添加统计信息
    lines.append("📊 账户汇总：")
    lines.append(f"- 总收益率: {summary['总收益率']}")
    lines.append(f"- 最大回撤: {summary['最大回撤']}")
    lines.append(f"- 胜率: {summary['胜率']}")
    lines.append(f"- 交易次数: {summary['交易次数']} (买入: {summary['买入交易数']}, 卖出: {summary['卖出交易数']})")
    lines.append(f"- 有效股票: {summary['有效股票数']}, 无效股票: {summary['无效股票数']}")

    lines.append("\n📑 交易流水：")
    if trades:
        # 添加详细的交易信息
        buy_trades = [t for t in trades if t["action"] == "BUY"]
        sell_trades = [t for t in trades if t["action"] == "SELL"]
        
        for i, t in enumerate(buy_trades[:10]):  # 只显示前10条买入
            # 【日期datetime类型规则】直接使用datetime对象
            lines.append(f"买入 {t['code']} {t['name']} @ {t['price']:.2f} (日期: {t['date'].strftime('%Y-%m-%d')})")
        
        for i, t in enumerate(sell_trades[:10]):  # 只显示前10条卖出
            holding_days = t.get("holding_days", 0)
            entry_price = t.get("entry_price", 0)
            profit = (t["price"] - entry_price) / entry_price * 100 if entry_price > 0 else 0
            # 【日期datetime类型规则】直接使用datetime对象
            lines.append(f"卖出 {t['code']} {t['name']} @ {t['price']:.2f} (日期: {t['date'].strftime('%Y-%m-%d')}, 持有: {holding_days}天, 收益: {profit:.2f}%)")
    else:
        lines.append("无交易记录")

    return "\n".join(lines)

def main():
    trades, summary = run_backtest()
    msg = generate_backtest_message(trades, summary)

    # 保存 CSV 文件
    os.makedirs(RESULT_DIR, exist_ok=True)
    # 【日期datetime类型规则】使用datetime对象生成文件名
    filename = f"{RESULT_DIR}/{datetime.now().strftime('%Y%m%d')}_backtest.csv"

    # 保存更详细的交易记录
    if trades:
        detailed_trades = []
        for t in trades:
            # 【日期datetime类型规则】确保日期列是datetime类型
            trade = {
                "date": t["date"].strftime('%Y-%m-%d') if isinstance(t["date"], datetime) else t["date"],
                "code": t["code"],
                "name": t["name"],
                "action": t["action"],
                "price": t["price"],
                "reason": t.get("reason", ""),
                "entry_price": t.get("entry_price", 0),
                "entry_date": t.get("entry_date").strftime('%Y-%m-%d') if isinstance(t.get("entry_date"), datetime) else t.get("entry_date", ""),
                "holding_days": t.get("holding_days", 0)
            }
            detailed_trades.append(trade)
        
        pd.DataFrame(detailed_trades).to_csv(filename, index=False, encoding="utf-8-sig")
        logger.info(f"回测交易记录已保存至: {filename}")
    else:
        logger.info("无交易记录，跳过保存")

    # 输出到控制台（供 GitHub Actions 推送）
    print(msg)
    logger.info("回测结果已输出")

if __name__ == "__main__":
    main()
