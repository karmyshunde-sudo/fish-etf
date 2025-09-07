#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF Yes/No 策略执行器
每天计算指定指数的策略信号并推送微信通知
"""

import os
import logging
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)

# 指定计算的指数列表（硬编码）
INDICES = [
    {"code": "000300", "name": "沪深300"},
    {"code": "399006", "name": "创业板指"},
    {"code": "399005", "name": "中小板指"},
    {"code": "000905", "name": "中证500"}
]

# 策略参数（示例值，需根据实际需求调整）
CRITICAL_VALUE_DAYS = 20  # 计算临界值的周期
DEVIATION_THRESHOLD = 0.02  # 偏离阈值（2%）

def calculate_critical_value(df: pd.DataFrame) -> float:
    """计算临界值（示例：20日均线）"""
    return df['收盘'].rolling(window=CRITICAL_VALUE_DAYS).mean().iloc[-1]

def calculate_deviation(current: float, critical: float) -> float:
    """计算偏离率"""
    return (current - critical) / critical * 100

def generate_report():
    """生成策略报告并推送微信"""
    try:
        beijing_time = get_beijing_time()
        report_date = beijing_time.strftime("%Y-%m-%d")
        
        # 准备结果数据
        results = []
        for idx in INDICES:
            code = idx["code"]
            name = idx["name"]
            
            # 从日线数据文件加载数据
            file_path = os.path.join(Config.ETFS_DAILY_DIR, f"{code}.csv")
            if not os.path.exists(file_path):
                logger.error(f"数据文件不存在: {file_path}")
                continue
            
            df = pd.read_csv(file_path)
            if df.empty:
                logger.warning(f"数据为空: {name}({code})")
                continue
            
            # 计算最新数据
            latest_data = df.iloc[-1]
            close_price = latest_data["收盘"]
            critical_value = calculate_critical_value(df)
            deviation = calculate_deviation(close_price, critical_value)
            
            # 简单状态判断逻辑（示例）
            status = "YES" if close_price > critical_value else "NO"
            
            # 构建结果行
            results.append({
                "代码": code,
                "名称": name,
                "涨幅%": round((close_price / df.iloc[-2]["收盘"] - 1) * 100, 2),
                "现价": close_price,
                "临界值点": round(critical_value, 2),
                "状态": status,
                "偏离率": round(deviation, 2),
                "趋势强度": abs(round(deviation, 2))  # 示例强度计算
            })
        
        # 生成Markdown表格
        if not results:
            send_wechat_message("❌ 无有效数据可供计算")
            return
        
        df_result = pd.DataFrame(results)
        df_result.sort_values(by="偏离率", ascending=False, inplace=True)
        table = df_result.to_markdown(index=False)
        
        message = f"📅 北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n" \
                  f"📈 ETF Yes/No 策略信号（{report_date}）\n\n" \
                  f"{table}"
        
        send_wechat_message(message)
    
    except Exception as e:
        logger.error(f"策略执行失败: {str(e)}", exc_info=True)
        send_wechat_message(f"🚨 策略执行异常: {str(e)}")

if __name__ == "__main__":
    generate_report()
