#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock_t3.py - 小市值布林带策略（终极版：持仓写入验证 + Git提交 + 详细诊断）
"""

import os
import pandas as pd
import numpy as np
import time
import logging
import sys
import json
import hashlib
import re
import subprocess
from datetime import datetime, timedelta
from config import Config
from wechat_push.push import send_wechat_message, send_txt_file
from utils.date_utils import get_beijing_time
from utils.git_utils import commit_files_in_batches

# ========== 策略参数配置 ==========
MIN_MARKET_CAP = 20.0
MAX_MARKET_CAP = 50.0
MIN_TURNOVER_RATE = 0.05
MAX_TURNOVER_RATE = 0.20

BOLLINGER_PERIOD = 20
BOLLINGER_STD = 2.0
BOLLINGER_THRESHOLD = 0.02  # 2%

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

# ===== 统一使用 stock 子目录存储所有策略相关文件 =====
STOCK_DATA_DIR = os.path.join(Config.DATA_DIR, "stock")
POSITION_FILE = os.path.join(STOCK_DATA_DIR, "t3_positions.json")
TRADE_RECORDS_FILE = os.path.join(STOCK_DATA_DIR, "t3_trade_records.json")

MIN_DATA_DAYS = 60
# ================================

logger = logging.getLogger(__name__)

# 确保 stock 子目录存在
os.makedirs(STOCK_DATA_DIR, exist_ok=True)

# ========== Git 状态检查辅助函数 ==========
def is_file_tracked_by_git(file_path):
    try:
        dir_path = os.path.dirname(file_path) or '.'
        result = subprocess.run(
            ['git', 'ls-files', '--error-unmatch', os.path.basename(file_path)],
            cwd=dir_path,
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except:
        return False

def log_git_status(file_path, action_desc):
    tracked = is_file_tracked_by_git(file_path)
    if tracked:
        logger.info(f"✅ {action_desc} 后，文件已被Git跟踪: {file_path}")
    else:
        logger.warning(f"⚠️ {action_desc} 后，文件未被Git跟踪: {file_path}")
    return tracked
# ========================================

# ========== 消息保存函数（与 tickten.py 风格一致）==========
def extract_title_from_message(message):
    lines = message.strip().split('\n')
    if lines:
        title = lines[0].strip()
        title = re.sub(r'[^\w\u4e00-\u9fff]+', '_', title)
        if len(title) > 30:
            title = title[:30]
        return title
    return "未知信号"

def save_message_to_file(message, message_type):
    try:
        stock_dir = STOCK_DATA_DIR
        message_hash = hashlib.md5(message.encode('utf-8')).hexdigest()[:8]
        now = get_beijing_time()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        title = extract_title_from_message(message)
        
        filename = f"t3_{title}_{timestamp}_{message_hash}.txt"
        file_path = os.path.join(stock_dir, filename)

        if os.path.exists(file_path):
            logger.info(f"消息文件已存在，跳过保存: {file_path}")
            return False

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(message)

        logger.info(f"✅ 已保存微信消息到文件: {file_path}")
        success = commit_files_in_batches(file_path, "LAST_FILE")
        if success:
            logger.info(f"✅ 成功提交消息文件到Git仓库: {file_path}")
        else:
            logger.error(f"❌ 提交消息文件到Git仓库失败: {file_path}")
        
        log_git_status(file_path, "保存消息文件")
        return True
    except Exception as e:
        logger.error(f"❌ 保存微信消息文件失败: {str(e)}", exc_info=True)
        return False

def send_and_save_wechat_message(message, message_type):
    send_wechat_message(message=message, message_type=message_type)
    save_message_to_file(message, message_type)
# ============================================================

class TradeRecorder:
    """交易记录器（增强版：支持备份和恢复 + 写入验证 + Git提交）"""
    
    def __init__(self):
        self.trade_file = TRADE_RECORDS_FILE
        self.backup_file = TRADE_RECORDS_FILE + ".bak"
        self.trades = self.load_trades_with_backup()
    
    def _load_from_file(self, filepath):
        if not os.path.exists(filepath):
            logger.debug(f"交易记录文件不存在: {filepath}")
            return None
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list):
                logger.info(f"成功从 {filepath} 加载 {len(data)} 条交易记录")
                return data
            else:
                logger.error(f"文件 {filepath} 格式错误：不是列表")
                return None
        except json.JSONDecodeError as e:
            logger.error(f"文件 {filepath} JSON解析失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"读取文件 {filepath} 失败: {str(e)}")
            return None
    
    def _save_to_file(self, data, filepath):
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                # 验证写入的内容是否与原始数据一致
                with open(filepath, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                if loaded == data:
                    logger.debug(f"数据已保存并验证通过: {filepath} (大小: {os.path.getsize(filepath)} 字节)")
                    return True
                else:
                    logger.error(f"文件内容验证失败: {filepath} 内容与原始数据不符")
                    return False
            else:
                logger.error(f"文件保存后验证失败: {filepath} 不存在或大小为0")
                return False
        except Exception as e:
            logger.error(f"保存文件 {filepath} 失败: {str(e)}")
            return False
    
    def load_trades_with_backup(self):
        trades = self._load_from_file(self.trade_file)
        if trades is not None:
            return trades
        
        logger.warning("主交易记录文件加载失败，尝试从备份恢复...")
        trades = self._load_from_file(self.backup_file)
        if trades is not None:
            logger.info("✅ 成功从备份恢复交易记录")
            self._save_to_file(trades, self.trade_file)
            return trades
        
        logger.error("❌ 无法加载任何交易记录，将初始化空列表。")
        return []
    
    def save_trades(self):
        success = self._save_to_file(self.trades, self.trade_file)
        if success:
            self._save_to_file(self.trades, self.backup_file)
            logger.info(f"交易记录已保存，当前共 {len(self.trades)} 条记录")
            # 模仿 tickten.py：提交到Git仓库
            commit_success = commit_files_in_batches(self.trade_file, "LAST_FILE")
            if commit_success:
                logger.info(f"✅ 成功提交交易记录文件到Git仓库: {self.trade_file}")
            else:
                logger.error(f"❌ 提交交易记录文件到Git仓库失败: {self.trade_file}")
            log_git_status(self.trade_file, "保存交易记录")
        else:
            logger.error("交易记录保存失败！")
    
    def record_buy(self, stock_data, buy_price, position_pct):
        trade = {
            "type": "buy",
            "code": stock_data["code"],
            "name": stock_data["name"],
            "date": datetime.now().strftime("%Y-%m-%d"),
            "price": buy_price,
            "position_pct": position_pct,
            "target_shares": int(100000 * position_pct / buy_price / 100) * 100,
            "amount": 100000 * position_pct
        }
        self.trades.append(trade)
        self.save_trades()
        logger.info(f"记录买入交易: {stock_data['code']} {stock_data['name']}")
    
    def record_sell(self, position, reason, sell_price):
        trade = {
            "type": "sell",
            "code": position["code"],
            "name": position["name"],
            "date": datetime.now().strftime("%Y-%m-%d"),
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
        if not self.trades:
            return None
        start_date = self.trades[0]["date"]
        buy_trades = [t for t in self.trades if t["type"] == "buy"]
        sell_trades = [t for t in self.trades if t["type"] == "sell"]
        total_buy_times = len(buy_trades)
        total_sell_times = len(sell_trades)
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
    """持仓管理器（增强版：支持备份和恢复 + 写入验证 + Git提交 + 内容诊断）"""
    
    def __init__(self, trade_recorder):
        self.positions_file = POSITION_FILE
        self.backup_file = POSITION_FILE + ".bak"
        self.trade_recorder = trade_recorder
        self.positions = self.load_positions_with_backup()
        logger.info(f"初始化持仓管理器，加载到 {len(self.positions)} 个持仓")
    
    def _load_from_file(self, filepath):
        if not os.path.exists(filepath):
            logger.debug(f"持仓文件不存在: {filepath}")
            return None
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list):
                logger.info(f"成功从 {filepath} 加载 {len(data)} 个持仓")
                return data
            else:
                logger.error(f"文件 {filepath} 格式错误：不是列表")
                return None
        except json.JSONDecodeError as e:
            logger.error(f"文件 {filepath} JSON解析失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"读取文件 {filepath} 失败: {str(e)}")
            return None
    
    def _save_to_file(self, data, filepath):
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                # 验证内容一致性
                with open(filepath, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                if loaded == data:
                    logger.debug(f"持仓数据已保存并验证通过: {filepath} (大小: {os.path.getsize(filepath)} 字节, 含 {len(data)} 条记录)")
                    return True
                else:
                    logger.error(f"持仓文件内容验证失败: {filepath}")
                    # 记录错误详情
                    logger.error(f"内存数据长度: {len(data)}, 文件数据长度: {len(loaded)}")
                    if data and len(data) > 0:
                        logger.error(f"内存第一条: {data[0]}")
                    if loaded and len(loaded) > 0:
                        logger.error(f"文件第一条: {loaded[0]}")
                    return False
            else:
                logger.error(f"持仓文件保存后验证失败: {filepath} 不存在或大小为0")
                return False
        except Exception as e:
            logger.error(f"保存持仓文件 {filepath} 失败: {str(e)}")
            return False
    
    def load_positions_with_backup(self):
        positions = self._load_from_file(self.positions_file)
        if positions is not None:
            return positions
        
        logger.warning("主持仓文件加载失败，尝试从备份恢复...")
        positions = self._load_from_file(self.backup_file)
        if positions is not None:
            logger.info("✅ 成功从备份恢复持仓")
            self._save_to_file(positions, self.positions_file)
            return positions
        
        logger.error("❌ 无法加载任何持仓记录，将初始化空列表。")
        return []
    
    def save_positions(self):
        """保存持仓到主文件和备份，并提交到Git"""
        logger.info(f"准备保存持仓，当前内存中持仓数量: {len(self.positions)}")
        if self.positions:
            sample = self.positions[0] if self.positions else {}
            logger.info(f"持仓示例: {sample.get('code', 'N/A')} {sample.get('name', 'N/A')}")
        success = self._save_to_file(self.positions, self.positions_file)
        if success:
            # 保存备份
            self._save_to_file(self.positions, self.backup_file)
            logger.info(f"持仓已保存，当前共 {len(self.positions)} 个持仓")
            
            # 模仿 tickten.py：提交到Git仓库
            commit_success = commit_files_in_batches(self.positions_file, "LAST_FILE")
            if commit_success:
                logger.info(f"✅ 成功提交持仓文件到Git仓库: {self.positions_file}")
            else:
                logger.error(f"❌ 提交持仓文件到Git仓库失败: {self.positions_file}")
            
            log_git_status(self.positions_file, "保存持仓")
        else:
            logger.error("持仓保存失败！")
    
    def update_positions(self, current_date):
        updated_positions = []
        sold_positions = []
        for position in self.positions:
            try:
                code = position["code"]
                buy_price = position["buy_price"]
                buy_date = position["buy_date"]
                
                hold_days = (datetime.strptime(current_date, "%Y-%m-%d") - 
                            datetime.strptime(buy_date, "%Y-%m-%d")).days
                
                file_path = os.path.join(Config.DATA_DIR, "daily", f"{code}.csv")
                current_price = buy_price
                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path)
                        if len(df) > 0:
                            df = df.sort_values("日期").reset_index(drop=True)
                            current_price = df.iloc[-1]["收盘"]
                    except Exception as e:
                        logger.error(f"读取 {code} 日线数据失败: {str(e)}")
                
                position["current_price"] = current_price
                position["hold_days"] = hold_days
                
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
                    self.trade_recorder.record_sell(position, sell_reason, current_price)
                    sold_positions.append({
                        "code": code,
                        "name": position["name"],
                        "reason": sell_reason,
                        "buy_price": buy_price,
                        "sell_price": current_price,
                        "hold_days": hold_days
                    })
                    continue
                
                updated_positions.append(position)
            except Exception as e:
                logger.error(f"更新持仓 {position.get('code')} 失败: {str(e)}")
                updated_positions.append(position)
        
        self.positions = updated_positions
        self.save_positions()
        return sold_positions
    
    def add_position(self, stock_data, buy_price, position_pct):
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
        self.trade_recorder.record_buy(stock_data, buy_price, position_pct)
        self.positions.append(new_position)
        logger.info(f"添加新持仓后，内存持仓数: {len(self.positions)}")
        # 输出前几个持仓的代码，便于调试
        codes = [p['code'] for p in self.positions[-5:]]
        logger.info(f"最近持仓代码: {codes}")
        self.save_positions()
    
    def get_current_positions(self):
        return self.positions
    
    def get_holding_codes(self):
        return [pos["code"] for pos in self.positions]

# ========== 技术指标函数（与原代码相同）==========
def calculate_bollinger_bands(df):
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
        logger.error(f"计算布林带失败: {str(e)}")
        return None

def calculate_rsi(df):
    try:
        delta = df["收盘"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=RSI_PERIOD).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=RSI_PERIOD).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]
    except Exception as e:
        logger.error(f"计算RSI失败: {str(e)}")
        return None

def calculate_volume_indicators(df):
    try:
        volume_ma = df["成交量"].rolling(window=VOLUME_MA_PERIOD).mean()
        volume_ratio = df["成交量"].iloc[-1] / volume_ma.iloc[-1]
        return volume_ratio
    except Exception as e:
        logger.error(f"计算成交量指标失败: {str(e)}")
        return None

def calculate_stock_score(stock_data):
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

def filter_stocks(exclude_codes=None):
    if exclude_codes is None:
        exclude_codes = []
    basic_info_file = os.path.join(Config.DATA_DIR, "all_stocks.csv")
    if not os.path.exists(basic_info_file):
        logger.error("股票列表文件不存在")
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
            logger.error(f"处理股票 {code} 时出错: {str(e)}")
            continue
    qualified_stocks.sort(key=lambda x: x["score"], reverse=True)
    logger.info(f"筛选完成，找到 {len(qualified_stocks)} 只符合条件的股票")
    return qualified_stocks

def format_position_message(position):
    buy_price = position["buy_price"]
    current_price = position.get("current_price", buy_price)
    hold_days = position.get("hold_days", 0)
    pnl_pct = (current_price / buy_price - 1) * 100
    stop_loss = buy_price * (1 - STOP_LOSS_PCT)
    take_profit = buy_price * (1 + TAKE_PROFIT_PCT)
    if current_price <= stop_loss:
        suggestion = "清仓"
    elif current_price >= take_profit:
        suggestion = "卖出部分"
    elif hold_days >= MAX_HOLD_DAYS:
        suggestion = "清仓（超时）"
    elif pnl_pct >= 5:
        suggestion = "继续持有（已有盈利）"
    else:
        suggestion = "继续持有"
    message = f"""【小市值布林带 - 当前持仓明细】
💰{position['code']} {position['name']}
📊 持有 {position.get('target_shares', 0):,}股

🎯 交易计划：
• 买入日期：{position['buy_date']}
• 已持有{hold_days}天
• 操作建议：{suggestion}
• 当天计算动态止盈价：{take_profit:.2f}元
• 当天计算动态止损价：{stop_loss:.2f}元
"""
    return message

def format_new_stock_message(stock_data):
    score = stock_data["score"]
    close_price = stock_data["close"]
    buy_price = close_price
    stop_loss = buy_price * (1 - STOP_LOSS_PCT)
    take_profit = buy_price * (1 + TAKE_PROFIT_PCT)
    message = f"""【小市值布林带 - 新推荐股票】
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

def format_no_stock_message():
    current_date = datetime.now().strftime("%Y-%m-%d")
    message = f"""【小市值布林带 - 暂无符合条件的股票】
        
📅 日期: {current_date}
        
🔍 筛选结果: 今日未找到符合条件的股票
        
📊 筛选条件:
1. 市值: {MIN_MARKET_CAP}-{MAX_MARKET_CAP}亿元
2. 换手率: >{MIN_TURNOVER_RATE*100:.1f}%
3. 布林带位置: <{BOLLINGER_THRESHOLD*100:.1f}%
4. RSI({RSI_PERIOD}): <{RSI_OVERSOLD+20}
5. 成交量: >{MIN_VOLUME_RATIO:.1f}倍5日均量
        
💡 可能原因:
• 市场整体处于高位，超卖股票较少
• 小市值股票普遍换手率不足
• 今日数据尚未更新完全
        
🔄 建议: 保持耐心，等待更好的入场时机"""
    return message

def format_trade_summary(summary):
    if not summary:
        return """【小市值布林带 - 策略交易汇总】

📊 交易统计:
• 暂无交易记录
• 策略处于初始化阶段
• 等待符合条件的股票出现"""
    profit_symbol = "🔴" if summary["total_profit"] < 0 else "🟢"
    message = f"""【小市值布林带 - 策略交易汇总】

📅 策略统计周期:
• 开始日期: {summary['start_date']}
• 结束日期: {datetime.now().strftime('%Y-%m-%d')}
• 运行天数: {(datetime.now() - datetime.strptime(summary['start_date'], '%Y-%m-%d')).days}天

📊 交易统计:
• 累计买入次数: {summary['total_buy_times']}次
• 累计卖出次数: {summary['total_sell_times']}次
• 总买入成本: {summary['total_cost']:,.0f}元
• 总实现利润: {profit_symbol} {summary['total_profit']:+,.0f}元
• 整体盈利率: {profit_symbol} {summary['profit_rate']:+.2f}%"""
    return message

def format_status_message(history_positions_count, new_stocks_count, start_date,
                          positions_file_exists, positions_file_empty, positions_file_size,
                          trades_file_exists, trades_count,
                          positions_file_path, trades_file_path):
    if not positions_file_exists:
        positions_status = f"❌ 不存在 (期望路径: {positions_file_path})"
    elif positions_file_empty:
        positions_status = f"⚠️ 存在但为空 (0字节) - {positions_file_path}"
    else:
        positions_status = f"✅ 存在 ({positions_file_size} 字节, 含 {history_positions_count} 条记录) - {positions_file_path}"
    
    if not trades_file_exists:
        trades_status = f"❌ 不存在 (期望路径: {trades_file_path})"
    else:
        trades_status = f"✅ 存在 (含 {trades_count} 条记录) - {trades_file_path}"
    
    start_date_str = start_date if start_date else "无历史记录"
    
    message = f"""【小市值布林带 - 策略状态】
• 历史持仓加载: {history_positions_count} 只
• 持仓文件: {positions_status}
• 今日新买入: {new_stocks_count} 只
• 📅 累计交易起始: {start_date_str}
• 交易记录文件: {trades_status}
• 运行环境: {os.getenv('RUN_ENV', 'unknown')}
"""
    return message

def send_stock_messages(positions, new_stocks):
    all_messages = []
    for position in positions:
        all_messages.append(format_position_message(position))
    for stock in new_stocks:
        all_messages.append(format_new_stock_message(stock))
    if not all_messages:
        return False
    total_batches = (len(all_messages) + 1) // 2
    for batch_index in range(total_batches):
        start_idx = batch_index * 2
        end_idx = min(start_idx + 2, len(all_messages))
        batch = all_messages[start_idx:end_idx]
        message_header = f"==第{batch_index + 1}条/共{total_batches}条消息=="
        message_body = "\n\n==================\n\n".join(batch)
        full_message = f"{message_header}\n\n{message_body}"
        send_and_save_wechat_message(full_message, "position")
        if batch_index < total_batches - 1:
            time.sleep(2)
    return True

def main():
    logger.info("===== 开始执行小市值布林带策略 =====")
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # 1. 初始化组件
        trade_recorder = TradeRecorder()
        position_manager = PositionManager(trade_recorder)
        
        # 2. 记录历史持仓数量
        history_positions_count = len(position_manager.positions)
        
        # 3. 更新持仓状态
        sold_positions = position_manager.update_positions(current_date)
        
        # 4. 发送卖出提示
        if sold_positions:
            sell_msg = "【小市值布林带 - 卖出提示】\n\n"
            for pos in sold_positions:
                sell_msg += f"• {pos['code']} {pos['name']} - {pos['reason']}\n"
            send_and_save_wechat_message(sell_msg, "position")
            time.sleep(2)
        
        # 5. 获取当前持仓
        current_positions = position_manager.get_current_positions()
        
        # 6. 筛选新股票
        holding_codes = position_manager.get_holding_codes()
        qualified_stocks = filter_stocks(exclude_codes=holding_codes)
        
        # 7. 分配仓位
        available_slots = max(0, TARGET_HOLDINGS - len(current_positions))
        new_stocks = qualified_stocks[:min(available_slots, len(qualified_stocks))]
        
        # 8. 添加新持仓记录
        for stock in new_stocks:
            position_manager.add_position(stock, stock["close"], MAX_POSITION_PCT)
        
        # 9. 重新获取持仓
        all_positions = position_manager.get_current_positions()
        
        # 10. 获取交易汇总
        trade_summary = trade_recorder.get_trade_summary()
        start_date = trade_summary['start_date'] if trade_summary else None
        
        # 11. 检查文件状态（此时文件应该已创建）
        positions_file_exists = os.path.exists(POSITION_FILE)
        positions_file_empty = False
        positions_file_size = 0
        if positions_file_exists:
            positions_file_size = os.path.getsize(POSITION_FILE)
            positions_file_empty = (positions_file_size == 0)
        
        trades_file_exists = os.path.exists(TRADE_RECORDS_FILE)
        trades_count = len(trade_recorder.trades)
        
        # 12. 发送策略状态消息
        status_msg = format_status_message(
            history_positions_count=history_positions_count,
            new_stocks_count=len(new_stocks),
            start_date=start_date,
            positions_file_exists=positions_file_exists,
            positions_file_empty=positions_file_empty,
            positions_file_size=positions_file_size,
            trades_file_exists=trades_file_exists,
            trades_count=trades_count,
            positions_file_path=POSITION_FILE,
            trades_file_path=TRADE_RECORDS_FILE
        )
        send_and_save_wechat_message(status_msg, "position")
        time.sleep(2)
        
        # 13. 发送持仓/推荐消息
        if all_positions or new_stocks:
            send_stock_messages(all_positions, new_stocks)
            time.sleep(2)
        else:
            no_stock_msg = format_no_stock_message()
            send_and_save_wechat_message(no_stock_msg, "position")
        
        # 14. 发送交易汇总消息
        summary_msg = format_trade_summary(trade_summary)
        send_and_save_wechat_message(summary_msg, "position")
        
        logger.info("===== 策略执行完成 =====")
    except Exception as e:
        error_msg = f"【小市值布林带 - 策略执行错误】\n错误详情：{str(e)}"
        logger.error(error_msg, exc_info=True)
        send_and_save_wechat_message(error_msg, "error")

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
