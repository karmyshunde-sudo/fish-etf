#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock_t3.py - 专业级小市值布林带策略（微信推送适配版）

功能特点：
1. 收盘后筛选，次日开盘买入的日间波段策略
2. 小市值(20-50亿)、高换手率(>5%)、布林带下轨策略
3. 严格的止损(-5%)和止盈(+10%)机制
4. 每只股票独立评分，按评分分配仓位(20%-25%)
5. 适配微信推送，每条股票消息间隔3秒
6. 完全兼容现有项目结构

策略核心：
- 收盘后筛选，使用确定性的收盘数据
- 每次持仓3-4只股票，每只25%仓位
- 明确的买入、止损、止盈价格
- 详细的技术指标数据展示
"""

import os
import pandas as pd
import numpy as np
import time
import logging
import sys
from datetime import datetime, timedelta
from config import Config
from wechat_push.push import send_wechat_message

# ========== 策略参数配置 ==========
# 基础筛选条件
MIN_MARKET_CAP = 20.0  # 最小市值(亿元)
MAX_MARKET_CAP = 50.0  # 最大市值(亿元)
MIN_TURNOVER_RATE = 0.05  # 最小换手率(5%)
MAX_TURNOVER_RATE = 0.20  # 最大换手率(20%)，避免过高换手率的风险

# 布林带参数
BOLLINGER_PERIOD = 20     # 布林带周期
BOLLINGER_STD = 2.0       # 标准差倍数
BOLLINGER_THRESHOLD = 0.02  # 接近下轨的阈值(2%)

# 技术指标参数
RSI_PERIOD = 14           # RSI周期
RSI_OVERSOLD = 30         # RSI超卖阈值
VOLUME_MA_PERIOD = 5      # 成交量均线周期
MIN_VOLUME_RATIO = 0.8    # 最小成交量比率(相对5日均量)

# 风险控制参数
STOP_LOSS_PCT = 0.05      # 止损比例(5%)
TAKE_PROFIT_PCT = 0.10    # 止盈比例(10%)
MAX_POSITION_PCT = 0.25   # 单只股票最大仓位(25%)
MIN_POSITION_PCT = 0.20   # 单只股票最小仓位(20%)

# 持仓参数
TARGET_HOLDINGS = 4       # 目标持仓数量(积极方案)

# 数据要求
MIN_DATA_DAYS = 60        # 最小数据天数
# ================================

# ========== 初始化日志 ==========
logger = logging.getLogger(__name__)

def calculate_bollinger_bands(df, period=BOLLINGER_PERIOD, std=BOLLINGER_STD):
    """计算布林带指标"""
    try:
        # 计算中轨(20日移动平均)
        middle_band = df["收盘"].rolling(window=period).mean()
        
        # 计算标准差
        std_dev = df["收盘"].rolling(window=period).std()
        
        # 计算上轨和下轨
        upper_band = middle_band + (std_dev * std)
        lower_band = middle_band - (std_dev * std)
        
        # 计算带宽和百分比位置
        bandwidth = (upper_band - lower_band) / middle_band * 100
        percent_b = (df["收盘"] - lower_band) / (upper_band - lower_band) * 100
        
        return {
            "upper": upper_band,
            "middle": middle_band,
            "lower": lower_band,
            "bandwidth": bandwidth,
            "percent_b": percent_b
        }
    except Exception as e:
        logger.debug(f"计算布林带失败: {str(e)}")
        return None

def calculate_rsi(df, period=RSI_PERIOD):
    """计算RSI指标"""
    try:
        delta = df["收盘"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    except Exception as e:
        logger.debug(f"计算RSI失败: {str(e)}")
        return None

def calculate_volume_indicators(df, period=VOLUME_MA_PERIOD):
    """计算成交量指标"""
    try:
        volume_ma = df["成交量"].rolling(window=period).mean()
        volume_ratio = df["成交量"] / volume_ma
        
        return {
            "volume_ma": volume_ma,
            "volume_ratio": volume_ratio
        }
    except Exception as e:
        logger.debug(f"计算成交量指标失败: {str(e)}")
        return None

def calculate_price_change(df, periods=[1, 3, 5, 10]):
    """计算价格变化率"""
    changes = {}
    for period in periods:
        if len(df) > period:
            changes[f"change_{period}d"] = (df["收盘"].iloc[-1] / df["收盘"].iloc[-period-1] - 1) * 100
        else:
            changes[f"change_{period}d"] = np.nan
    return changes

def calculate_stock_score(row):
    """
    计算股票综合评分(0-100分)
    评分因素:
    1. 市值越小分越高(40分)
    2. 换手率适中分高(20分)
    3. 布林带位置越低分越高(20分)
    4. RSI超卖程度(10分)
    5. 成交量放大程度(10分)
    """
    score = 0
    
    # 1. 市值评分(越小越好)
    market_cap = row.get("market_cap", 50)
    if market_cap <= MIN_MARKET_CAP:
        score += 40
    elif market_cap <= (MIN_MARKET_CAP + MAX_MARKET_CAP) / 2:
        score += 30
    elif market_cap <= MAX_MARKET_CAP:
        score += 20
    
    # 2. 换手率评分(适中为佳)
    turnover = row.get("turnover_rate", 0)
    if MIN_TURNOVER_RATE <= turnover <= 0.08:
        score += 20
    elif 0.08 < turnover <= 0.12:
        score += 15
    elif 0.12 < turnover <= MAX_TURNOVER_RATE:
        score += 10
    
    # 3. 布林带位置评分(越低越好)
    percent_b = row.get("percent_b", 50)
    if percent_b <= 5:
        score += 20
    elif percent_b <= 15:
        score += 15
    elif percent_b <= 25:
        score += 10
    elif percent_b <= 35:
        score += 5
    
    # 4. RSI评分(超卖为佳)
    rsi = row.get("rsi", 50)
    if rsi <= RSI_OVERSOLD:
        score += 10
    elif rsi <= RSI_OVERSOLD + 10:
        score += 7
    elif rsi <= RSI_OVERSOLD + 20:
        score += 4
    
    # 5. 成交量评分(放量为佳)
    volume_ratio = row.get("volume_ratio", 1.0)
    if volume_ratio >= 1.5:
        score += 10
    elif volume_ratio >= 1.2:
        score += 7
    elif volume_ratio >= MIN_VOLUME_RATIO:
        score += 4
    
    return min(score, 100)

def calculate_position_size(score, total_capital=100000):
    """
    根据评分计算仓位大小
    规则:
    - 最小仓位: MIN_POSITION_PCT * total_capital
    - 最大仓位: MAX_POSITION_PCT * total_capital
    - 根据评分线性分配
    """
    min_position = total_capital * MIN_POSITION_PCT
    max_position = total_capital * MAX_POSITION_PCT
    
    # 线性映射: 0分->min_position, 100分->max_position
    position = min_position + (max_position - min_position) * (score / 100)
    
    # 计算股票数量(按收盘价估算)
    close_price = score  # 这里score参数实际上是传入的close_price，需要调整
    # 注意：这里需要实际传入收盘价，暂时返回固定比例
    return MAX_POSITION_PCT  # 返回仓位比例

def format_stock_message(stock_data):
    """格式化单只股票的消息"""
    code = stock_data["code"]
    name = stock_data["name"]
    score = stock_data["score"]
    position_pct = stock_data["position_pct"]
    
    # 获取今日收盘价作为参考买入价
    close_price = stock_data["close"]
    
    # 计算买入、止损、止盈价格
    buy_price = close_price  # 假设次日以收盘价附近买入
    stop_loss = buy_price * (1 - STOP_LOSS_PCT)
    take_profit = buy_price * (1 + TAKE_PROFIT_PCT)
    
    # 计算建议买入数量(按10万本金计算)
    position_value = 100000 * position_pct
    suggested_shares = int(position_value / buy_price / 100) * 100  # 取整百股
    
    lines = [
        f"【📊 T3策略 - {code} {name}】",
        f"📅 筛选日期: {datetime.now().strftime('%Y-%m-%d')}",
        "",
        "📈 技术指标详情:",
        f"• 当前价格: {close_price:.2f}元",
        f"• 市值: {stock_data.get('market_cap', 'N/A'):.1f}亿元",
        f"• 换手率: {stock_data.get('turnover_rate', 0):.2%}",
        "",
        f"• 布林带上轨: {stock_data.get('boll_upper', 0):.2f}元",
        f"• 布林带中轨: {stock_data.get('boll_middle', 0):.2f}元",
        f"• 布林带下轨: {stock_data.get('boll_lower', 0):.2f}元",
        f"• 布林带位置: {stock_data.get('percent_b', 0):.1f}%",
        f"• 布林带带宽: {stock_data.get('bandwidth', 0):.1f}%",
        "",
        f"• RSI({RSI_PERIOD}): {stock_data.get('rsi', 0):.1f}",
        f"• 成交量比率: {stock_data.get('volume_ratio', 0):.2f}倍",
        "",
        "🎯 交易计划:",
        f"• 建议买入价: {buy_price:.2f}元 (次日开盘附近)",
        f"• 止损价格: {stop_loss:.2f}元 (-{STOP_LOSS_PCT*100:.0f}%)",
        f"• 止盈价格: {take_profit:.2f}元 (+{TAKE_PROFIT_PCT*100:.0f}%)",
        f"• 风险收益比: 1:{TAKE_PROFIT_PCT/STOP_LOSS_PCT:.1f}",
        "",
        "💰 仓位管理:",
        f"• 综合评分: {score:.0f}/100分",
        f"• 建议仓位: {position_pct:.1%}",
        f"• 建议股数: {suggested_shares:,}股 (约{position_value:.0f}元)",
        "",
        "📋 筛选说明:",
        f"1. 市值{MIN_MARKET_CAP}-{MAX_MARKET_CAP}亿元",
        f"2. 换手率>{MIN_TURNOVER_RATE:.0%}",
        f"3. 收盘价接近布林带下轨(位置<{BOLLINGER_THRESHOLD*100:.0f}%)",
        f"4. RSI({RSI_PERIOD})<{RSI_OVERSOLD+20}，显示超卖",
        f"5. 成交量大于{MIN_VOLUME_RATIO}倍5日均量",
        "",
        "⚠️ 风险提示:",
        "• 次日开盘买入，严格执行止损",
        "• 单只股票仓位不超过25%",
        "• 总持仓3-4只，分散风险",
        "• 本策略适合10万本金积极型投资者"
    ]
    
    return "\n".join(lines)

def filter_stocks():
    """主筛选函数"""
    # 1. 读取所有股票列表
    basic_info_file = os.path.join(Config.DATA_DIR, "all_stocks.csv")
    if not os.path.exists(basic_info_file):
        logger.error("股票列表文件all_stocks.csv不存在")
        error_msg = "【T3策略】\n股票列表文件不存在，无法筛选"
        send_wechat_message(message=error_msg, message_type="error")
        return []
    
    try:
        basic_info_df = pd.read_csv(basic_info_file)
        logger.info(f"成功读取股票列表，共 {len(basic_info_df)} 只股票")
    except Exception as e:
        logger.error(f"读取股票列表文件失败: {str(e)}")
        error_msg = f"【T3策略】\n读取股票列表文件失败: {str(e)}"
        send_wechat_message(message=error_msg, message_type="error")
        return []
    
    qualified_stocks = []
    
    # 2. 遍历股票进行筛选
    total_stocks = len(basic_info_df)
    processed = 0
    
    for _, row in basic_info_df.iterrows():
        code = str(row["代码"])
        name = row["名称"]
        
        # 检查市值信息
        market_cap = row.get("总市值", row.get("市值", 0))
        if market_cap == 0:
            # 尝试从其他列获取市值
            market_cap = row.get("流通市值", 0)
        
        # 市值筛选
        if market_cap < MIN_MARKET_CAP * 1e8 or market_cap > MAX_MARKET_CAP * 1e8:
            processed += 1
            continue
        
        # 读取日线数据
        file_path = os.path.join(Config.DATA_DIR, "daily", f"{code}.csv")
        if not os.path.exists(file_path):
            processed += 1
            continue
        
        try:
            df = pd.read_csv(file_path)
            
            # 检查数据完整性
            if len(df) < MIN_DATA_DAYS:
                processed += 1
                continue
            
            required_columns = ["日期", "收盘", "最高", "最低", "成交量", "换手率"]
            if not all(col in df.columns for col in required_columns):
                processed += 1
                continue
            
            # 转换日期格式
            df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
            df = df.sort_values("日期").reset_index(drop=True)
            
            # 获取最新数据
            latest = df.iloc[-1]
            
            # 换手率筛选
            turnover_rate = latest.get("换手率", 0)
            if turnover_rate < MIN_TURNOVER_RATE or turnover_rate > MAX_TURNOVER_RATE:
                processed += 1
                continue
            
            # 计算技术指标
            bollinger = calculate_bollinger_bands(df)
            if bollinger is None:
                processed += 1
                continue
            
            rsi = calculate_rsi(df)
            if rsi is None or len(rsi) < 1:
                processed += 1
                continue
            
            volume_indicators = calculate_volume_indicators(df)
            if volume_indicators is None:
                processed += 1
                continue
            
            # 检查布林带位置
            percent_b = bollinger["percent_b"].iloc[-1]
            if percent_b > BOLLINGER_THRESHOLD * 100:
                processed += 1
                continue
            
            # 检查RSI超卖
            current_rsi = rsi.iloc[-1]
            if current_rsi > RSI_OVERSOLD + 20:  # 稍微放宽条件
                processed += 1
                continue
            
            # 检查成交量
            volume_ratio = volume_indicators["volume_ratio"].iloc[-1]
            if volume_ratio < MIN_VOLUME_RATIO:
                processed += 1
                continue
            
            # 计算价格变化
            price_changes = calculate_price_change(df)
            
            # 收集股票数据
            stock_data = {
                "code": code,
                "name": name,
                "close": latest["收盘"],
                "market_cap": market_cap / 1e8,  # 转换为亿元
                "turnover_rate": turnover_rate,
                "boll_upper": bollinger["upper"].iloc[-1],
                "boll_middle": bollinger["middle"].iloc[-1],
                "boll_lower": bollinger["lower"].iloc[-1],
                "bandwidth": bollinger["bandwidth"].iloc[-1],
                "percent_b": percent_b,
                "rsi": current_rsi,
                "volume_ratio": volume_ratio,
                "change_1d": price_changes.get("change_1d", np.nan),
                "change_5d": price_changes.get("change_5d", np.nan),
                "change_10d": price_changes.get("change_10d", np.nan)
            }
            
            # 计算评分
            stock_data["score"] = calculate_stock_score(stock_data)
            
            qualified_stocks.append(stock_data)
            
        except Exception as e:
            logger.debug(f"处理股票 {code} 失败: {str(e)}")
        
        processed += 1
        if processed % 100 == 0:
            logger.info(f"已处理 {processed}/{total_stocks} 只股票，找到 {len(qualified_stocks)} 只符合条件的股票")
    
    logger.info(f"筛选完成，共找到 {len(qualified_stocks)} 只符合条件的股票")
    return qualified_stocks

def allocate_positions(stocks, target_count=TARGET_HOLDINGS):
    """分配仓位"""
    if not stocks:
        return []
    
    # 按评分排序
    sorted_stocks = sorted(stocks, key=lambda x: x["score"], reverse=True)
    
    # 取评分最高的target_count只
    selected_stocks = sorted_stocks[:min(target_count, len(sorted_stocks))]
    
    # 计算总分
    total_score = sum(s["score"] for s in selected_stocks)
    
    # 分配仓位比例
    for stock in selected_stocks:
        if total_score > 0:
            # 根据评分比例分配，但保证在最小和最大仓位之间
            raw_pct = stock["score"] / total_score
            # 调整到目标仓位范围
            adjusted_pct = MIN_POSITION_PCT + (MAX_POSITION_PCT - MIN_POSITION_PCT) * (raw_pct / max(1, len(selected_stocks)))
            stock["position_pct"] = min(adjusted_pct, MAX_POSITION_PCT)
        else:
            stock["position_pct"] = MAX_POSITION_PCT / len(selected_stocks)
    
    return selected_stocks

def send_stock_messages(stocks):
    """发送股票消息到微信"""
    if not stocks:
        no_signal_msg = """【T3策略 - 小市值布林带策略】
        
📅 日期: {date}
        
🔍 筛选结果: 今日未找到符合条件的股票
        
📊 筛选条件:
1. 市值: {min_cap}-{max_cap}亿元
2. 换手率: >{min_turnover:.1%}
3. 布林带位置: <{boll_threshold:.1%}
4. RSI({rsi_period}): <{rsi_threshold}
5. 成交量: >{volume_ratio:.1f}倍5日均量
        
💡 可能原因:
• 市场整体处于高位，超卖股票较少
• 小市值股票普遍换手率不足
• 今日数据尚未更新完全
        
🔄 建议: 保持耐心，等待更好的入场时机"""
        
        formatted_msg = no_signal_msg.format(
            date=datetime.now().strftime("%Y-%m-%d"),
            min_cap=MIN_MARKET_CAP,
            max_cap=MAX_MARKET_CAP,
            min_turnover=MIN_TURNOVER_RATE,
            boll_threshold=BOLLINGER_THRESHOLD*100,
            rsi_period=RSI_PERIOD,
            rsi_threshold=RSI_OVERSOLD+20,
            volume_ratio=MIN_VOLUME_RATIO
        )
        
        send_wechat_message(message=formatted_msg, message_type="position")
        logger.info("未找到符合条件的股票，已发送通知")
        return
    
    # 发送汇总消息
    summary_msg = f"""【T3策略 - 筛选结果汇总】
    
📅 日期: {datetime.now().strftime('%Y-%m-%d')}
    
🎯 找到 {len(stocks)} 只符合条件的股票:
    
"""
    
    for i, stock in enumerate(stocks, 1):
        summary_msg += f"{i}. {stock['code']} {stock['name']} - 评分: {stock['score']:.0f}/100 - 建议仓位: {stock['position_pct']:.1%}\n"
    
    summary_msg += f"""
📊 策略参数:
• 目标持仓: {TARGET_HOLDINGS}只
• 单股仓位: {MIN_POSITION_PCT:.0%}-{MAX_POSITION_PCT:.0%}
• 止损: -{STOP_LOSS_PCT*100:.0%}%
• 止盈: +{TAKE_PROFIT_PCT*100:.0%}%
    
💡 操作建议:
1. 次日开盘附近买入
2. 严格执行止损止盈
3. 保持总持仓{len(stocks)}-{TARGET_HOLDINGS}只
4. 定期复盘调整策略"""
    
    send_wechat_message(message=summary_msg, message_type="position")
    time.sleep(2)
    
    # 发送每只股票的详细消息
    for stock in stocks:
        message = format_stock_message(stock)
        send_wechat_message(message=message, message_type="position")
        time.sleep(3)  # 每条消息间隔3秒
    
    logger.info(f"已发送 {len(stocks)} 只股票的详细分析到微信")

def main():
    """主函数"""
    logger.info("===== 开始执行T3小市值布林带策略 =====")
    
    try:
        # 1. 筛选符合条件的股票
        logger.info("开始筛选股票...")
        qualified_stocks = filter_stocks()
        
        # 2. 分配仓位
        logger.info("分配仓位...")
        selected_stocks = allocate_positions(qualified_stocks)
        
        # 3. 发送消息
        logger.info("发送微信消息...")
        send_stock_messages(selected_stocks)
        
        # 4. 生成策略报告
        if selected_stocks:
            report_msg = f"""【T3策略 - 执行报告】
            
✅ 策略执行完成
            
📈 今日筛选结果:
• 扫描股票总数: 从all_stocks.csv读取
• 符合条件股票: {len(qualified_stocks)}只
• 最终入选股票: {len(selected_stocks)}只
            
🎯 风险控制:
• 最大单股亏损: {STOP_LOSS_PCT*100:.0%}
• 最小盈利目标: {TAKE_PROFIT_PCT*100:.0%}
• 风险收益比: 1:{TAKE_PROFIT_PCT/STOP_LOSS_PCT:.1f}
            
💰 资金管理(10万本金):
• 单股投入: {MIN_POSITION_PCT*100:.0f}%-{MAX_POSITION_PCT*100:.0f}%
• 总持仓比例: {sum(s['position_pct'] for s in selected_stocks):.0%}
• 剩余现金: {(1 - sum(s['position_pct'] for s in selected_stocks)):.0%}
            
⏰ 下一步操作:
• 等待次日开盘
• 按建议价格买入
• 设置止损止盈单
• 每日收盘后重新筛选
            
📊 策略优势:
1. 收盘后筛选，避免盘中噪音
2. 小市值股票，弹性空间大
3. 严格风控，保护本金安全
4. 微信推送，实时接收信号"""
            
            send_wechat_message(message=report_msg, message_type="position")
        
        logger.info("===== T3策略执行完成 =====")
        
    except Exception as e:
        error_msg = f"【T3策略执行错误】\n错误详情: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(message=error_msg, message_type="error")

if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(os.path.join(Config.LOG_DIR, "stock_t3_strategy.log"))
        ]
    )
    
    # 执行策略
    main()
