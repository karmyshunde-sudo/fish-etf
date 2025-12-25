#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock_t3.py - 小市值布林带策略（带交易记录和汇总）

新增功能：
1. 交易记录保存和读取
2. 策略执行统计汇总
3. 买入卖出成本利润计算
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
TRADE_RECORDS_FILE = os.path.join(Config.DATA_DIR, "t3_trade_records.json")

MIN_DATA_DAYS = 60
# ================================

logger = logging.getLogger(__name__)

class TradeRecorder:
    """交易记录器"""
    
    def __init__(self):
        self.trade_file = TRADE_RECORDS_FILE
        self.trades = self.load_trades()
    
    def load_trades(self):
        """加载交易记录"""
        if os.path.exists(self.trade_file):
            try:
                with open(self.trade_file, 'r', encoding='utf-8') as f:
                    trades = json.load(f)
                logger.info(f"已加载 {len(trades)} 条交易记录")
                return trades
            except Exception as e:
                logger.error(f"加载交易记录失败: {str(e)}")
                return []
        return []
    
    def save_trades(self):
        """保存交易记录"""
        try:
            with open(self.trade_file, 'w', encoding='utf-8') as f:
                json.dump(self.trades, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存交易记录失败: {str(e)}")
    
    def record_buy(self, stock_data, buy_price, position_pct):
        """记录买入交易"""
        trade = {
            "type": "buy",
            "code": stock_data["code"],
            "name": stock_data["name"],
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S"),
            "price": buy_price,
            "position_pct": position_pct,
            "target_shares": int(100000 * position_pct / buy_price / 100) * 100,
            "amount": 100000 * position_pct  # 按10万本金计算
        }
        self.trades.append(trade)
        self.save_trades()
        logger.info(f"记录买入交易: {stock_data['code']} {stock_data['name']}")
    
    def record_sell(self, position, reason, sell_price):
        """记录卖出交易"""
        trade = {
            "type": "sell",
            "code": position["code"],
            "name": position["name"],
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M:%S"),
            "buy_date": position["buy_date"],
            "buy_price": position["buy_price"],
            "sell_price": sell_price,
            "reason": reason,
            "position_pct": position["position_pct"],
            "pnl_pct": (sell_price / position["buy_price"] - 1) * 100,
            "pnl_amount": (sell_price - position["buy_price"]) * position.get("target_shares", 0)
        }
        self.trades.append(trade)
        self.save_trades()
        logger.info(f"记录卖出交易: {position['code']} {position['name']}")
    
    def get_trade_summary(self):
        """获取交易统计汇总"""
        if not self.trades:
            return None
        
        # 计算策略开始日期（第一笔交易的日期）
        start_date = self.trades[0]["date"]
        
        # 统计买入信息
        buy_trades = [t for t in self.trades if t["type"] == "buy"]
        sell_trades = [t for t in self.trades if t["type"] == "sell"]
        
        total_buy_times = len(buy_trades)
        total_sell_times = len(sell_trades)
        
        # 计算总成本和总利润
        total_cost = sum(t.get("amount", 0) for t in buy_trades)
        total_profit = sum(t.get("pnl_amount", 0) for t in sell_trades)
        
        return {
            "start_date": start_date,
            "total_buy_times": total_buy_times,
            "total_sell_times": total_sell_times,
            "total_cost": total_cost,
            "total_profit": total_profit,
            "profit_rate": (total_profit / total_cost * 100) if total_cost > 0 else 0
        }

class PositionManager:
    """持仓管理器"""
    
    def __init__(self, trade_recorder):
        self.positions_file = POSITION_FILE
        self.trade_recorder = trade_recorder
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
                current_price = buy_price  # 默认使用买入价
                price_read_success = False
                
                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path)
                        if len(df) > 0:
                            df = df.sort_values("日期").reset_index(drop=True)
                            current_price = df.iloc[-1]["收盘"]
                            price_read_success = True
                            logger.debug(f"成功读取股票 {code} 的最新价格: {current_price}")
                    except Exception as e:
                        logger.warning(f"读取股票 {code} 价格失败: {str(e)}")
                
                # 更新持仓信息
                position["current_price"] = current_price
                position["hold_days"] = hold_days
                position["price_read_success"] = price_read_success
                
                # 检查是否应该卖出（只在成功读取价格时检查）
                if price_read_success:
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
                        # 记录卖出交易
                        self.trade_recorder.record_sell(position, sell_reason, current_price)
                        
                        sold_positions.append({
                            "code": code,
                            "name": position["name"],
                            "reason": sell_reason,
                            "buy_price": buy_price,
                            "sell_price": current_price,
                            "hold_days": hold_days,
                            "pnl_pct": (current_price / buy_price - 1) * 100
                        })
                        continue  # 不再添加到持仓中
                
                # 保留持仓
                updated_positions.append(position)
                
            except Exception as e:
                logger.error(f"更新持仓 {position.get('code')} 失败: {str(e)}")
                updated_positions.append(position)
        
        # 更新持仓列表
        self.positions = updated_positions
        self.save_positions()
        
        return sold_positions
    
    def add_position(self, stock_data, buy_price, position_pct):
        """添加新持仓并记录买入交易"""
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
        
        # 记录买入交易
        self.trade_recorder.record_buy(stock_data, buy_price, position_pct)
        
        self.positions.append(new_position)
        self.save_positions()
        logger.info(f"添加新持仓: {stock_data['code']} {stock_data['name']}")
    
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
    
    # 动态止盈止损
    dynamic_stop_loss = buy_price * (1 - STOP_LOSS_PCT)
    dynamic_take_profit = buy_price * (1 + TAKE_PROFIT_PCT)
    
    # 给出操作建议
    if current_price <= dynamic_stop_loss:
        return "清仓（触发止损）", dynamic_take_profit, dynamic_stop_loss
    elif current_price >= dynamic_take_profit:
        return "卖出部分（已达标）", dynamic_take_profit, dynamic_stop_loss
    elif hold_days >= MAX_HOLD_DAYS:
        return "清仓（超时）", dynamic_take_profit, dynamic_stop_loss
    elif pnl_pct >= 5:
        return "继续持有（已有盈利）", dynamic_take_profit, dynamic_stop_loss
    else:
        return "继续持有", dynamic_take_profit, dynamic_stop_loss

def format_position_message(position):
    """格式化持仓股票消息"""
    suggestion, take_profit, stop_loss = get_trading_suggestion(position)
    
    message = f"""【==原有持仓明细及分析==】
💰{position['code']} {position['name']}
📊 持有 {position.get('target_shares', 0):,}股

🎯 交易计划：
• 买入日期：{position['buy_date']}
• 已持有{position.get('hold_days', 0)}天
• 操作建议：{suggestion}
• 当天计算动态止盈价：{take_profit:.2f}元
• 当天计算动态止损价：{stop_loss:.2f}元
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

def format_trade_summary(summary):
    """格式化交易汇总消息"""
    if not summary:
        return "【交易汇总】\n暂无交易记录"
    
    profit_symbol = "🔴" if summary["total_profit"] < 0 else "🟢"
    
    message = f"""【====策略交易汇总====】

📅 策略统计周期：
• 开始日期：{summary['start_date']}
• 结束日期：{datetime.now().strftime('%Y-%m-%d')}
• 运行天数：{(datetime.now() - datetime.strptime(summary['start_date'], '%Y-%m-%d')).days}天

📊 交易统计：
• 累计买入次数：{summary['total_buy_times']}次
• 累计卖出次数：{summary['total_sell_times']}次
• 总买入成本：{summary['total_cost']:,.0f}元
• 总实现利润：{profit_symbol} {summary['total_profit']:+,.0f}元
• 整体盈利率：{profit_symbol} {summary['profit_rate']:+.2f}%

💰 收益分析：
• 平均每次买入成本：{summary['total_cost']/summary['total_buy_times']:,.0f}元
• 平均每次卖出利润：{summary['total_profit']/summary['total_sell_times']:,.0f}元
• 胜率：{(len([t for t in summary.get('sell_trades', []) if t.get('pnl_amount', 0) > 0])/summary['total_sell_times']*100 if summary['total_sell_times'] > 0 else 0):.1f}%

⚠️ 风险提示：
• 历史收益不代表未来表现
• 股市有风险，投资需谨慎
"""
    return message

def filter_stocks(exclude_codes=None):
    """筛选股票"""
    if exclude_codes is None:
        exclude_codes = []
    
    basic_info_file = os.path.join(Config.DATA_DIR, "all_stocks.csv")
    if not os.path.exists(basic_info_file):
        logger.error("股票列表文件 all_stocks.csv 不存在")
        return []
    
    try:
        basic_info_df = pd.read_csv(basic_info_file)
        logger.info(f"读取股票列表，共 {len(basic_info_df)} 只股票")
    except Exception as e:
        logger.error(f"读取股票列表失败: {str(e)}")
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
            
            required_columns = ["日期", "收盘", "成交量", "换手率"]
            if not all(col in df.columns for col in required_columns):
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
            
        except Exception as e:
            logger.debug(f"处理股票 {code} 失败: {str(e)}")
            continue
    
    qualified_stocks.sort(key=lambda x: x["score"], reverse=True)
    logger.info(f"筛选完成，找到 {len(qualified_stocks)} 只符合条件的股票")
    return qualified_stocks

def send_stock_messages(positions, new_stocks):
    """发送股票消息"""
    all_messages = []
    
    # 先添加持仓股票消息
    for position in positions:
        all_messages.append(format_position_message(position))
    
    # 再添加新推荐股票消息
    for stock in new_stocks:
        all_messages.append(format_new_stock_message(stock))
    
    # 如果没有消息，说明既没有持仓也没有新推荐
    if not all_messages:
        no_stock_msg = "今日无股票推荐和持仓"
        send_wechat_message(message=no_stock_msg, message_type="position")
        return False
    
    # 分条发送，每批最多2条消息
    total_batches = (len(all_messages) + 1) // 2  # 向上取整
    for batch_index in range(total_batches):
        start_idx = batch_index * 2
        end_idx = min(start_idx + 2, len(all_messages))
        batch = all_messages[start_idx:end_idx]
        
        # 构建消息
        message_header = f"==第{batch_index + 1}条/共{total_batches}条消息=="
        message_body = "\n\n==================\n\n".join(batch)
        full_message = f"{message_header}\n\n{message_body}"
        
        # 发送消息
        send_wechat_message(message=full_message, message_type="position")
        
        # 如果不是最后一批，等待2秒
        if batch_index < total_batches - 1:
            time.sleep(2)
    
    return True

def main():
    """主函数"""
    logger.info("===== 开始执行小市值布林带策略 =====")
    
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"当前日期: {current_date}")
        
        # 1. 初始化交易记录器和持仓管理器
        trade_recorder = TradeRecorder()
        position_manager = PositionManager(trade_recorder)
        logger.info(f"当前持仓数量: {len(position_manager.positions)}")
        
        # 2. 更新持仓状态（检查是否需要卖出）
        logger.info("更新持仓状态...")
        sold_positions = position_manager.update_positions(current_date)
        
        # 3. 如果有卖出的股票，发送卖出提示
        if sold_positions:
            sell_msg = "【⚠️ 卖出提示】\n\n"
            for pos in sold_positions:
                pnl_symbol = "🔴" if pos.get('pnl_pct', 0) < 0 else "🟢"
                sell_msg += f"• {pos['code']} {pos['name']}\n"
                sell_msg += f"  卖出原因: {pos['reason']}\n"
                sell_msg += f"  买入价: {pos['buy_price']:.2f}元\n"
                sell_msg += f"  卖出价: {pos['sell_price']:.2f}元\n"
                sell_msg += f"  持有天数: {pos['hold_days']}天\n"
                sell_msg += f"  盈亏: {pnl_symbol} {pos.get('pnl_pct', 0):+.2f}%\n\n"
            
            send_wechat_message(message=sell_msg, message_type="position")
            time.sleep(2)
        
        # 4. 获取当前持仓
        current_positions = position_manager.get_current_positions()
        logger.info(f"更新后持仓数量: {len(current_positions)}")
        
        # 5. 筛选新股票（排除已持仓的）
        logger.info("筛选新股票...")
        holding_codes = position_manager.get_holding_codes()
        logger.info(f"排除持仓股票: {holding_codes}")
        
        qualified_stocks = filter_stocks(exclude_codes=holding_codes)
        
        # 6. 分配仓位
        available_slots = max(0, TARGET_HOLDINGS - len(current_positions))
        logger.info(f"可用仓位数量: {available_slots}")
        
        new_stocks = qualified_stocks[:min(available_slots, len(qualified_stocks))]
        
        # 7. 添加新持仓记录
        for stock in new_stocks:
            position_manager.add_position(stock, stock["close"], MAX_POSITION_PCT)
        
        # 8. 重新获取更新后的持仓（包含新添加的）
        all_positions = position_manager.get_current_positions()
        
        # 9. 发送股票消息
        logger.info("发送股票消息...")
        logger.info(f"持仓数量: {len(all_positions)}，新推荐数量: {len(new_stocks)}")
        
        has_stock_messages = send_stock_messages(all_positions, new_stocks)
        
        # 10. 发送交易汇总消息
        logger.info("发送交易汇总消息...")
        trade_summary = trade_recorder.get_trade_summary()
        if trade_summary:
            summary_msg = format_trade_summary(trade_summary)
            send_wechat_message(message=summary_msg, message_type="position")
        else:
            logger.info("暂无交易记录，不发送汇总消息")
        
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
