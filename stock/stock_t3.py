#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock_t3.py - 小市值布林带策略（严格模板版）

严格按照指定模板显示消息
"""

import os
import pandas as pd
import numpy as np
import time
import logging
import sys
import json
from datetime import datetime, timedelta
from config import Config
from wechat_push.push import send_wechat_message

# ========== 策略参数配置 ==========
MIN_MARKET_CAP = 20.0
MAX_MARKET_CAP = 50.0
MIN_TURNOVER_RATE = 0.05
MAX_TURNOVER_RATE = 0.20

BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2.0
BOLLINGER_THRESHOLD = 0.02

RSI_PERIOD = 14
RSI_OVERSOLD = 30
VOLUME_MA_PERIOD = 5
MIN_VOLUME_RATIO = 0.8

STOP_LOSS_PCT = 0.05
TAKE_PROFIT_PCT = 0.10
MAX_POSITION_PCT = 0.25
MIN_POSITION_PCT = 0.20

TARGET_HOLDINGS = 4
MAX_HOLD_DAYS = 10
POSITION_FILE = os.path.join(Config.DATA_DIR, "t3_positions.json")

MIN_DATA_DAYS = 60
# ================================

logger = logging.getLogger(__name__)

class PositionManager:
    """持仓管理器"""
    
    def __init__(self):
        self.positions_file = POSITION_FILE
        self.positions = self.load_positions()
    
    def load_positions(self):
        """加载持仓记录"""
        if os.path.exists(self.positions_file):
            try:
                with open(self.positions_file, 'r', encoding='utf-8') as f:
                    positions = json.load(f)
                logger.info(f"已加载 {len(positions)} 个持仓记录")
                return positions
            except Exception as e:
                logger.error(f"加载持仓文件失败: {str(e)}")
                return []
        return []
    
    def save_positions(self):
        """保存持仓记录"""
        try:
            with open(self.positions_file, 'w', encoding='utf-8') as f:
                json.dump(self.positions, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存持仓文件失败: {str(e)}")
    
    def update_positions(self, current_date):
        """更新持仓状态"""
        updated_positions = []
        sold_positions = []
        
        for position in self.positions:
            try:
                code = position["code"]
                buy_price = position["buy_price"]
                buy_date = position["buy_date"]
                
                # 检查持有天数
                hold_days = (datetime.strptime(current_date, "%Y-%m-%d") - 
                            datetime.strptime(buy_date, "%Y-%m-%d")).days
                
                # 读取最新价格
                file_path = os.path.join(Config.DATA_DIR, "daily", f"{code}.csv")
                current_price = buy_price  # 默认值
                
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    if len(df) > 0:
                        current_price = df.iloc[-1]["收盘"]
                
                position["current_price"] = current_price
                position["hold_days"] = hold_days
                
                # 检查是否应该卖出
                stop_loss = buy_price * (1 - STOP_LOSS_PCT)
                take_profit = buy_price * (1 + TAKE_PROFIT_PCT)
                
                sell_reason = None
                if current_price <= stop_loss:
                    sell_reason = "触发止损"
                elif current_price >= take_profit:
                    sell_reason = "触发止盈"
                elif hold_days >= MAX_HOLD_DAYS:
                    sell_reason = f"持有超过{MAX_HOLD_DAYS}天"
                
                if sell_reason:
                    sold_positions.append({
                        "code": code,
                        "name": position["name"],
                        "reason": sell_reason,
                        "buy_price": buy_price,
                        "sell_price": current_price,
                        "hold_days": hold_days
                    })
                else:
                    updated_positions.append(position)
                    
            except Exception as e:
                logger.error(f"更新持仓 {position.get('code')} 失败: {str(e)}")
                updated_positions.append(position)
        
        self.positions = updated_positions
        self.save_positions()
        return sold_positions
    
    def add_position(self, stock_data, buy_price, position_pct):
        """添加新持仓"""
        new_position = {
            "code": stock_data["code"],
            "name": stock_data["name"],
            "buy_date": datetime.now().strftime("%Y-%m-%d"),
            "buy_price": buy_price,
            "stop_loss": buy_price * (1 - STOP_LOSS_PCT),
            "take_profit": buy_price * (1 + TAKE_PROFIT_PCT),
            "position_pct": position_pct,
            "target_shares": int(100000 * position_pct / buy_price / 100) * 100
        }
        
        self.positions.append(new_position)
        self.save_positions()
    
    def get_current_positions(self):
        """获取当前持仓"""
        return self.positions
    
    def get_holding_codes(self):
        """获取持仓股票代码"""
        return [pos["code"] for pos in self.positions]

# ========== 技术指标函数 ==========
def calculate_bollinger_bands(df):
    """计算布林带指标"""
    try:
        middle_band = df["收盘"].rolling(window=BOLLINGER_PERIOD).mean()
        std_dev = df["收盘"].rolling(window=BOLLINGER_PERIOD).std()
        upper_band = middle_band + (std_dev * BOLLINGER_STD)
        lower_band = middle_band - (std_dev * BOLLINGER_STD)
        bandwidth = (upper_band - lower_band) / middle_band * 100
        percent_b = (df["收盘"] - lower_band) / (upper_band - lower_band) * 100
        return {
            "upper": upper_band.iloc[-1],
            "middle": middle_band.iloc[-1],
            "lower": lower_band.iloc[-1],
            "bandwidth": bandwidth.iloc[-1],
            "percent_b": percent_b.iloc[-1]
        }
    except Exception as e:
        logger.debug(f"计算布林带失败: {str(e)}")
        return None

def calculate_rsi(df):
    """计算RSI指标"""
    try:
        delta = df["收盘"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=RSI_PERIOD).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=RSI_PERIOD).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]
    except Exception as e:
        logger.debug(f"计算RSI失败: {str(e)}")
        return None

def calculate_volume_indicators(df):
    """计算成交量指标"""
    try:
        volume_ma = df["成交量"].rolling(window=VOLUME_MA_PERIOD).mean()
        volume_ratio = df["成交量"].iloc[-1] / volume_ma.iloc[-1]
        return volume_ratio
    except Exception as e:
        logger.debug(f"计算成交量指标失败: {str(e)}")
        return None

def calculate_stock_score(stock_data):
    """计算股票综合评分"""
    score = 0
    
    market_cap = stock_data.get("market_cap", 50)
    if market_cap <= MIN_MARKET_CAP:
        score += 40
    elif market_cap <= (MIN_MARKET_CAP + MAX_MARKET_CAP) / 2:
        score += 30
    elif market_cap <= MAX_MARKET_CAP:
        score += 20
    
    turnover = stock_data.get("turnover_rate", 0)
    if MIN_TURNOVER_RATE <= turnover <= 0.08:
        score += 20
    elif 0.08 < turnover <= 0.12:
        score += 15
    elif 0.12 < turnover <= MAX_TURNOVER_RATE:
        score += 10
    
    percent_b = stock_data.get("percent_b", 50)
    if percent_b <= 5:
        score += 20
    elif percent_b <= 15:
        score += 15
    elif percent_b <= 25:
        score += 10
    elif percent_b <= 35:
        score += 5
    
    rsi = stock_data.get("rsi", 50)
    if rsi <= RSI_OVERSOLD:
        score += 10
    elif rsi <= RSI_OVERSOLD + 10:
        score += 7
    elif rsi <= RSI_OVERSOLD + 20:
        score += 4
    
    volume_ratio = stock_data.get("volume_ratio", 1.0)
    if volume_ratio >= 1.5:
        score += 10
    elif volume_ratio >= 1.2:
        score += 7
    elif volume_ratio >= MIN_VOLUME_RATIO:
        score += 4
    
    return min(score, 100)

def get_trading_suggestion(position):
    """根据持仓情况给出操作建议"""
    buy_price = position["buy_price"]
    current_price = position.get("current_price", buy_price)
    hold_days = position.get("hold_days", 0)
    
    pnl_pct = (current_price / buy_price - 1) * 100
    
    if current_price <= buy_price * (1 - STOP_LOSS_PCT):
        return "清仓"
    elif current_price >= buy_price * (1 + TAKE_PROFIT_PCT):
        return "卖出部分"
    elif hold_days >= MAX_HOLD_DAYS:
        return "清仓（超时）"
    elif pnl_pct >= 5:
        return "继续持有（已有盈利）"
    else:
        return "继续持有"

def format_position_message(position):
    """格式化持仓股票消息"""
    suggestion = get_trading_suggestion(position)
    
    message = f"""【==原有持仓明细及分析==】
💰{position['code']} {position['name']}
📊 持有 {position.get('target_shares', 0):,}股

🎯 交易计划：
• 买入日期：{position['buy_date']}
• 已持有{position.get('hold_days', 0)}天
• 操作建议：{suggestion}
• 当天计算动态止盈价：{position.get('take_profit', 0):.2f}元
• 当天计算动态止损价：{position.get('stop_loss', 0):.2f}元
"""
    return message

def format_new_stock_message(stock_data):
    """格式化新推荐股票消息"""
    score = stock_data["score"]
    close_price = stock_data["close"]
    buy_price = close_price
    stop_loss = buy_price * (1 - STOP_LOSS_PCT)
    take_profit = buy_price * (1 + TAKE_PROFIT_PCT)
    
    message = f"""【====小市值布林带策略====】
💰{stock_data['code']} {stock_data['name']}

🎯 交易计划：
• 综合评分: {score:.0f}/100分
• 建议买入价: {buy_price:.2f}元 (次日开盘附近)
• 止损价格: {stop_loss:.2f}元 (-{STOP_LOSS_PCT*100:.0f}%)
• 止盈价格: {take_profit:.2f}元 (+{TAKE_PROFIT_PCT*100:.0f}%)
• 风险收益比: 1:{TAKE_PROFIT_PCT/STOP_LOSS_PCT:.1f}

📈 技术指标详情:
• 当前价格: {close_price:.2f}元
• 市值: {stock_data.get('market_cap', 0):.1f}亿元
• 换手率: {stock_data.get('turnover_rate', 0):.2%}

• 布林带上轨: {stock_data.get('boll_upper', 0):.2f}元
• 布林带中轨: {stock_data.get('boll_middle', 0):.2f}元
• 布林带下轨: {stock_data.get('boll_lower', 0):.2f}元
• 布林带位置: {stock_data.get('percent_b', 0):.1f}%
• 布林带带宽: {stock_data.get('bandwidth', 0):.1f}%

• RSI({RSI_PERIOD}): {stock_data.get('rsi', 0):.1f}
• 成交量比率: {stock_data.get('volume_ratio', 0):.2f}倍
"""
    return message

def filter_stocks(exclude_codes=None):
    """筛选股票"""
    if exclude_codes is None:
        exclude_codes = []
    
    basic_info_file = os.path.join(Config.DATA_DIR, "all_stocks.csv")
    if not os.path.exists(basic_info_file):
        return []
    
    try:
        basic_info_df = pd.read_csv(basic_info_file)
    except:
        return []
    
    qualified_stocks = []
    
    for _, row in basic_info_df.iterrows():
        code = str(row["代码"])
        
        if code in exclude_codes:
            continue
        
        market_cap = row.get("总市值", row.get("市值", 0))
        if market_cap == 0:
            market_cap = row.get("流通市值", 0)
        
        if market_cap < MIN_MARKET_CAP * 1e8 or market_cap > MAX_MARKET_CAP * 1e8:
            continue
        
        file_path = os.path.join(Config.DATA_DIR, "daily", f"{code}.csv")
        if not os.path.exists(file_path):
            continue
        
        try:
            df = pd.read_csv(file_path)
            if len(df) < MIN_DATA_DAYS:
                continue
            
            df = df.sort_values("日期").reset_index(drop=True)
            latest = df.iloc[-1]
            name = row["名称"]
            
            turnover_rate = latest.get("换手率", 0)
            if turnover_rate < MIN_TURNOVER_RATE or turnover_rate > MAX_TURNOVER_RATE:
                continue
            
            bollinger = calculate_bollinger_bands(df)
            if bollinger is None:
                continue
            
            rsi = calculate_rsi(df)
            if rsi is None:
                continue
            
            volume_ratio = calculate_volume_indicators(df)
            if volume_ratio is None or volume_ratio < MIN_VOLUME_RATIO:
                continue
            
            if bollinger["percent_b"] > BOLLINGER_THRESHOLD * 100:
                continue
            
            if rsi > RSI_OVERSOLD + 20:
                continue
            
            stock_data = {
                "code": code,
                "name": name,
                "close": latest["收盘"],
                "market_cap": market_cap / 1e8,
                "turnover_rate": turnover_rate,
                "boll_upper": bollinger["upper"],
                "boll_middle": bollinger["middle"],
                "boll_lower": bollinger["lower"],
                "bandwidth": bollinger["bandwidth"],
                "percent_b": bollinger["percent_b"],
                "rsi": rsi,
                "volume_ratio": volume_ratio
            }
            
            stock_data["score"] = calculate_stock_score(stock_data)
            qualified_stocks.append(stock_data)
            
        except:
            continue
    
    qualified_stocks.sort(key=lambda x: x["score"], reverse=True)
    return qualified_stocks

def send_stock_messages(positions, new_stocks):
    """发送股票消息，按照模板严格格式"""
    all_messages = []
    
    # 先添加新推荐股票消息
    for stock in new_stocks:
        all_messages.append(format_new_stock_message(stock))
    
    # 添加持仓股票消息
    for position in positions:
        all_messages.append(format_position_message(position))
    
    # 如果没有消息，发送空消息
    if not all_messages:
        send_wechat_message(message="今日无股票推荐和持仓", message_type="position")
        return
    
    # 分批发送，每批最多2只股票
    total_messages = len(all_messages)
    batches = [all_messages[i:i+2] for i in range(0, total_messages, 2)]
    
    for i, batch in enumerate(batches):
        message_header = f"==第{i+1}条/共{len(batches)}条消息=="
        message_body = f"\n==================\n".join(batch)
        full_message = f"{message_header}\n\n{message_body}"
        
        send_wechat_message(message=full_message, message_type="position")
        
        # 如果不是最后一批，等待2秒
        if i < len(batches) - 1:
            time.sleep(2)

def main():
    """主函数"""
    logger.info("===== 开始执行小市值布林带策略 =====")
    
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # 1. 初始化持仓管理器
        position_manager = PositionManager()
        
        # 2. 更新持仓状态
        sold_positions = position_manager.update_positions(current_date)
        
        # 3. 获取当前持仓
        current_positions = position_manager.get_current_positions()
        
        # 4. 筛选新股票（排除已持仓的）
        holding_codes = position_manager.get_holding_codes()
        qualified_stocks = filter_stocks(exclude_codes=holding_codes)
        
        # 5. 分配仓位
        available_slots = max(0, TARGET_HOLDINGS - len(current_positions))
        new_stocks = qualified_stocks[:min(available_slots, len(qualified_stocks))]
        
        # 6. 添加新持仓记录
        for stock in new_stocks:
            position_manager.add_position(stock, stock["close"], MAX_POSITION_PCT)
        
        # 7. 重新获取更新后的持仓
        all_positions = position_manager.get_current_positions()
        
        # 8. 发送消息
        send_stock_messages(all_positions, new_stocks)
        
        logger.info("===== 策略执行完成 =====")
        
    except Exception as e:
        error_msg = f"【策略执行错误】\n错误详情：{str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(message=error_msg, message_type="error")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(os.path.join(Config.LOG_DIR, "stock_t3_strategy.log"))
        ]
    )
    
    main()
