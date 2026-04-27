#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock_t0.py - Trendchannel（趋势通道策略）股票筛选 + 微信推送

策略原理：
- 基于趋势跟踪的价格波动动态趋势通道系统
- 中轨：20日简单移动平均线（MA）
- 上轨：中轨 + 20日ATR × 1.5
- 下轨：中轨 - 20日ATR × 1.5
- 做多：收盘价有效突破上轨（连续2日站上轨，或放量突破3%）
- 做空：收盘价有效跌破下轨（连续2日站下轨，或放量跌破3%）
- 出场：收盘价跌破/突破中轨，或回踩/反弹轨道未企稳

作者：John Tolan（约翰·托兰）
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
# 通道参数
CHANNEL_PERIOD = 20          # 通道周期（日线级别）
ATR_MULTIPLIER = 1.5         # ATR倍数，用于计算通道宽度

# 入场规则参数（适度收紧，平衡信号数量和质量）
ENTRY_CONFIRM_DAYS = 2       # 连续确认天数
ENTRY_VOLUME_BREAK_PCT = 0.03  # 放量突破阈值（3%）
MIN_VOLUME_RATIO = 1.3       # 最小放量倍数（1.3倍）
MIN_TREND_STRENGTH = 0.015   # 最小趋势强度（0.015%）
MAX_BREAK_PCT = 8.0          # 最大突破幅度（8%，超过此幅度视为追高，仅对放量突破生效）

# 出场规则参数
EXIT_ON_MIDDLE = True        # 跌破/突破中轨时平仓
EXIT_ON_LOWER_TOUCH = True   # 回踩下轨未企稳平仓（多单）
EXIT_ON_UPPER_TOUCH = True   # 反弹上轨未承压平仓（空单）

# 移动止损参数
TRAILING_STOP_ENABLED = True  # 启用移动止损
TRAILING_STOP_TO_MIDDLE = True  # 盈利后将止损移至中轨
TRAILING_STOP_PROFIT_PCT = 0.03  # 盈利超过3%启用移动止损

# 股票池筛选参数（从根源减少股票数量）
MIN_MARKET_CAP = 50.0        # 最小市值（50亿元，过滤小盘股）
MAX_MARKET_CAP = 500.0       # 最大市值（500亿元）
MIN_TURNOVER_RATE = 0.02     # 最小换手率（2%）
MIN_VOLUME = 5000000         # 最小成交量（500万股）

# 通道质量过滤（趋势策略核心：只在有真实趋势的股票中找信号）
MIN_CHANNEL_SLOPE = 0.03     # 最小通道斜率（中轨必须明显向上/向下）
MIN_ATR_RATIO = 0.015        # 最小ATR/价格比（1.5%，过滤死水股）
MAX_ATR_RATIO = 0.06         # 最大ATR/价格比（6%，过滤高波动垃圾股）

# 信号质量要求
MIN_SIGNAL_SCORE = 80        # 最低综合评分（80分）
MIN_SIGNAL_STRENGTH = 0.75   # 最低信号强度（0.75）
MAX_POSITIONS = 5            # 最大持仓数量（5个）
MIN_DATA_DAYS = 80           # 最少数据天数（80天）

# ===== 统一使用 stock 子目录存储所有策略相关文件 =====
STOCK_DATA_DIR = os.path.join(Config.DATA_DIR, "stock")
TRENDCHANNEL_DIR = os.path.join(STOCK_DATA_DIR, "trendchannel")
POSITION_FILE = os.path.join(TRENDCHANNEL_DIR, "t0_positions.json")
TRADE_RECORDS_FILE = os.path.join(TRENDCHANNEL_DIR, "t0_trade_records.json")
# ================================

logger = logging.getLogger(__name__)

# 确保目录存在
os.makedirs(STOCK_DATA_DIR, exist_ok=True)
os.makedirs(TRENDCHANNEL_DIR, exist_ok=True)

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

# ========== 消息保存函数（与 stock_t3.py 风格一致）==========
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
        message_hash = hashlib.md5(message.encode('utf-8')).hexdigest()[:8]
        now = get_beijing_time()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        title = extract_title_from_message(message)
        
        filename = f"t0_{title}_{timestamp}_{message_hash}.txt"
        file_path = os.path.join(TRENDCHANNEL_DIR, filename)

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

# ========== TradeRecorder（与 stock_t3.py 一致）==========
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
                with open(filepath, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                if loaded == data:
                    logger.debug(f"数据已保存并验证通过: {filepath}")
                    return True
                else:
                    logger.error(f"文件内容验证失败: {filepath}")
                    return False
            else:
                logger.error(f"文件保存后验证失败: {filepath}")
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
            commit_success = commit_files_in_batches(self.trade_file, "LAST_FILE")
            if commit_success:
                logger.info(f"✅ 成功提交交易记录文件到Git仓库: {self.trade_file}")
            else:
                logger.error(f"❌ 提交交易记录文件到Git仓库失败: {self.trade_file}")
            log_git_status(self.trade_file, "保存交易记录")
        else:
            logger.error("交易记录保存失败！")
    
    def record_buy(self, stock_data, buy_price, direction, position_pct):
        trade = {
            "type": "buy",
            "code": stock_data["code"],
            "name": stock_data["name"],
            "date": datetime.now().strftime("%Y-%m-%d"),
            "price": buy_price,
            "direction": direction,
            "position_pct": position_pct,
            "stop_loss": stock_data.get("stop_loss", 0),
            "target": stock_data.get("target", 0),
            "score": stock_data.get("score", 0),
        }
        self.trades.append(trade)
        self.save_trades()
        logger.info(f"记录买入交易: {stock_data['code']} {stock_data['name']} ({direction})")
    
    def record_sell(self, position, reason, sell_price):
        buy_price = position["buy_price"]
        direction = position.get("direction", "long")
        if direction == "long":
            pnl_pct = (sell_price - buy_price) / buy_price * 100
        else:
            pnl_pct = (buy_price - sell_price) / buy_price * 100
        
        trade = {
            "type": "sell",
            "code": position["code"],
            "name": position["name"],
            "date": datetime.now().strftime("%Y-%m-%d"),
            "buy_date": position["buy_date"],
            "buy_price": buy_price,
            "sell_price": sell_price,
            "direction": direction,
            "reason": reason,
            "pnl_pct": pnl_pct,
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
        return {
            "start_date": start_date,
            "total_buy_times": len(buy_trades),
            "total_sell_times": len(sell_trades),
        }
# ============================================================

# ========== PositionManager（与 stock_t3.py 一致）==========
class PositionManager:
    """持仓管理器（增强版：支持备份和恢复 + 写入验证 + Git提交）"""
    
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
                with open(filepath, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                if loaded == data:
                    logger.debug(f"持仓数据已保存并验证通过: {filepath}")
                    return True
                else:
                    logger.error(f"持仓文件内容验证失败: {filepath}")
                    return False
            else:
                logger.error(f"持仓文件保存后验证失败: {filepath}")
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
        logger.info(f"准备保存持仓，当前内存中持仓数量: {len(self.positions)}")
        success = self._save_to_file(self.positions, self.positions_file)
        if success:
            self._save_to_file(self.positions, self.backup_file)
            logger.info(f"持仓已保存，当前共 {len(self.positions)} 个持仓")
            commit_success = commit_files_in_batches(self.positions_file, "LAST_FILE")
            if commit_success:
                logger.info(f"✅ 成功提交持仓文件到Git仓库: {self.positions_file}")
            else:
                logger.error(f"❌ 提交持仓文件到Git仓库失败: {self.positions_file}")
            log_git_status(self.positions_file, "保存持仓")
        else:
            logger.error("持仓保存失败！")
    
    def update_positions(self, current_date):
        """更新持仓状态，检查是否需要出场"""
        updated_positions = []
        sold_positions = []
        for position in self.positions:
            try:
                code = position["code"]
                buy_price = position["buy_price"]
                buy_date = position["buy_date"]
                direction = position.get("direction", "long")
                
                hold_days = (datetime.strptime(current_date, "%Y-%m-%d") - 
                            datetime.strptime(buy_date, "%Y-%m-%d")).days
                
                file_path = os.path.join(Config.DATA_DIR, "daily", f"{code}.csv")
                current_price = buy_price
                channel_data = None
                
                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path)
                        if len(df) >= MIN_DATA_DAYS:
                            df = df.sort_values("日期").reset_index(drop=True)
                            current_price = df.iloc[-1]["收盘"]
                            # 计算通道数据
                            channel_data = calculate_trendchannel(df)
                    except Exception as e:
                        logger.error(f"读取 {code} 日线数据失败: {str(e)}")
                
                position["current_price"] = current_price
                position["hold_days"] = hold_days
                
                # 如果有通道数据，检查出场信号
                sell_reason = None
                if channel_data:
                    exit_signal = check_exit_signal(direction, channel_data)
                    if exit_signal["signal"]:
                        sell_reason = exit_signal["reason"]
                    elif TRAILING_STOP_ENABLED:
                        # 检查移动止损
                        trailing_stop = calculate_trailing_stop(direction, channel_data, buy_price)
                        if direction == "long" and current_price <= trailing_stop:
                            sell_reason = f"触发移动止损（{trailing_stop:.2f}）"
                        elif direction == "short" and current_price >= trailing_stop:
                            sell_reason = f"触发移动止损（{trailing_stop:.2f}）"
                
                if sell_reason:
                    self.trade_recorder.record_sell(position, sell_reason, current_price)
                    sold_positions.append({
                        "code": code,
                        "name": position["name"],
                        "direction": direction,
                        "reason": sell_reason,
                        "buy_price": buy_price,
                        "sell_price": current_price,
                        "hold_days": hold_days,
                    })
                    continue
                
                # 更新通道数据
                if channel_data:
                    position["middle"] = channel_data["middle"]
                    position["upper"] = channel_data["upper"]
                    position["lower"] = channel_data["lower"]
                
                updated_positions.append(position)
            except Exception as e:
                logger.error(f"更新持仓 {position.get('code')} 失败: {str(e)}")
                updated_positions.append(position)
        
        self.positions = updated_positions
        self.save_positions()
        return sold_positions
    
    def add_position(self, stock_data, buy_price, direction, position_pct):
        new_position = {
            "code": stock_data["code"],
            "name": stock_data["name"],
            "direction": direction,
            "buy_date": datetime.now().strftime("%Y-%m-%d"),
            "buy_price": buy_price,
            "stop_loss": stock_data.get("stop_loss", buy_price * 0.95),
            "target": stock_data.get("target", 0),
            "position_pct": position_pct,
            "score": stock_data.get("score", 0),
            "middle": stock_data.get("middle", 0),
            "upper": stock_data.get("upper", 0),
            "lower": stock_data.get("lower", 0),
            "current_price": buy_price,
            "hold_days": 0,
        }
        self.trade_recorder.record_buy(stock_data, buy_price, direction, position_pct)
        self.positions.append(new_position)
        logger.info(f"添加新持仓后，内存持仓数: {len(self.positions)}")
        self.save_positions()
    
    def get_current_positions(self):
        return self.positions
    
    def get_holding_codes(self):
        return [pos["code"] for pos in self.positions]
# ============================================================

# ========== 技术指标函数（Trendchannel核心）==========
def calculate_true_range(df):
    """计算真实波幅（TR）"""
    high = df["收盘"]
    low = df["最低"]
    close = df["收盘"]
    
    tr1 = df["最高"] - df["最低"]
    tr2 = (df["最高"] - df["收盘"].shift(1)).abs()
    tr3 = (df["最低"] - df["收盘"].shift(1)).abs()
    
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr

def calculate_atr(df, period=CHANNEL_PERIOD):
    """计算平均真实波幅（ATR = MA(TR, 20)）"""
    tr = calculate_true_range(df)
    atr = tr.rolling(window=period).mean()
    return atr

def calculate_trendchannel(df):
    """
    计算趋势通道指标
    - 中轨：20日简单移动平均线（MA）
    - 上轨：中轨 + 20日ATR × 1.5
    - 下轨：中轨 - 20日ATR × 1.5
    """
    try:
        if len(df) < MIN_DATA_DAYS:
            return None
        
        df = df.sort_values("日期").reset_index(drop=True)
        
        middle_band = df["收盘"].rolling(window=CHANNEL_PERIOD).mean()
        atr = calculate_atr(df, CHANNEL_PERIOD)
        upper_band = middle_band + atr * ATR_MULTIPLIER
        lower_band = middle_band - atr * ATR_MULTIPLIER
        
        channel_width = (upper_band - lower_band) / middle_band * 100
        price_position = ((df["收盘"] - lower_band) / (upper_band - lower_band)) * 100
        middle_slope = middle_band.diff() / middle_band.shift(1) * 100
        
        latest = {
            "middle": middle_band.iloc[-1],
            "upper": upper_band.iloc[-1],
            "lower": lower_band.iloc[-1],
            "atr": atr.iloc[-1],
            "channel_width": channel_width.iloc[-1],
            "price_position": price_position.iloc[-1],
            "middle_slope": middle_slope.iloc[-1],
            "close": df["收盘"].iloc[-1],
            "high": df["最高"].iloc[-1],
            "low": df["最低"].iloc[-1],
            "volume": df["成交量"].iloc[-1] if "成交量" in df.columns else 0,
        }
        
        latest["history"] = {
            "close_last_3": df["收盘"].iloc[-3:].tolist() if len(df) >= 3 else [],
            "upper_last_3": upper_band.iloc[-3:].tolist() if len(df) >= 3 else [],
            "lower_last_3": lower_band.iloc[-3:].tolist() if len(df) >= 3 else [],
            "volume_ma": df["成交量"].rolling(window=5).mean().iloc[-1] if "成交量" in df.columns else 0,
        }
        
        return latest
        
    except Exception as e:
        logger.error(f"计算趋势通道失败: {str(e)}")
        return None

def check_long_entry_signal(channel_data):
    """
    检查做多入场信号（突破上轨）
    触发条件：连续2日收盘价站上轨，或放量突破3%以上
    """
    if channel_data is None:
        return {"signal": False, "strength": 0}
    
    close = channel_data["close"]
    upper = channel_data["upper"]
    history = channel_data["history"]
    
    # 【趋势质量过滤】中轨必须明显向上，ATR/价格在合理区间
    middle_slope = channel_data["middle_slope"]
    atr_ratio = channel_data["atr"] / close if close > 0 else 0
    if middle_slope < MIN_CHANNEL_SLOPE:
        return {"signal": False, "strength": 0, "reason": f"中轨斜率{middle_slope:.2f}% < {MIN_CHANNEL_SLOPE*100:.2f}%，趋势强度不足"}
    if atr_ratio < MIN_ATR_RATIO or atr_ratio > MAX_ATR_RATIO:
        return {"signal": False, "strength": 0, "reason": f"ATR/价格={atr_ratio*100:.2f}%不在{MIN_ATR_RATIO*100:.1f}%~{MAX_ATR_RATIO*100:.1f}%区间"}
    
    # 【修复】目标价 = 入场价 + ATR×2（合理盈亏比）
    atr = channel_data["atr"]
    stop_loss = channel_data["middle"]
    risk = close - stop_loss  # 做多：风险 = 入场价 - 止损价
    signal_info = {
        "signal": False,
        "strength": 0,
        "reason": "",
        "break_pct": 0,
        "entry_price": close,
        "stop_loss": stop_loss,
        "target": close + atr * 2,  # 目标价 = 入场价 + 2倍ATR
    }
    
    # 条件1：连续2日收盘价站上轨
    close_last_3 = history.get("close_last_3", [])
    upper_last_3 = history.get("upper_last_3", [])
    
    if len(close_last_3) >= 2 and len(upper_last_3) >= 2:
        if close_last_3[-1] > upper_last_3[-1] and close_last_3[-2] > upper_last_3[-2]:
            signal_info["signal"] = True
            signal_info["strength"] = 0.6
            signal_info["reason"] = f"连续2日收盘价站上轨（收盘价{close:.2f} > 上轨{upper:.2f}）"
            signal_info["break_pct"] = (close - upper) / upper * 100
    
    # 条件2：放量突破3%以上
    break_pct = (close - upper) / upper
    volume_ma = history.get("volume_ma", 1)
    volume_ratio = channel_data["volume"] / volume_ma if volume_ma > 0 else 0
    
    if break_pct >= ENTRY_VOLUME_BREAK_PCT and volume_ratio >= MIN_VOLUME_RATIO:
        signal_info["signal"] = True
        signal_info["strength"] = max(signal_info["strength"], 0.8)
        signal_info["reason"] = f"放量突破上轨（突破{break_pct*100:.1f}%，量比{volume_ratio:.2f}）"
        signal_info["break_pct"] = break_pct * 100
    
    # 趋势强度过滤：中轨斜率必须向上
    if signal_info["signal"]:
        middle_slope = channel_data["middle_slope"]
        if middle_slope < MIN_TREND_STRENGTH:
            signal_info["signal"] = False
            signal_info["reason"] += f"，但趋势强度不足（中轨斜率{middle_slope:.2f}%）"
    
    # 【防止追高】仅对放量突破信号生效（突破幅度>8%视为追高）
    # 连续2日站上轨的信号不受此限制（因为站上轨2天自然幅度较大）
    if signal_info["signal"] and break_pct >= ENTRY_VOLUME_BREAK_PCT:
        if break_pct > MAX_BREAK_PCT:
            signal_info["signal"] = False
            signal_info["reason"] += f"，但突破幅度过大（{break_pct*100:.1f}% > {MAX_BREAK_PCT}%），视为追高，拒绝入场"
    
    return signal_info

def check_short_entry_signal(channel_data):
    """
    检查做空入场信号（跌破下轨）
    触发条件：连续2日收盘价站下轨，或放量跌破3%以上
    """
    if channel_data is None:
        return {"signal": False, "strength": 0}
    
    close = channel_data["close"]
    lower = channel_data["lower"]
    history = channel_data["history"]
    
    # 【趋势质量过滤】中轨必须明显向下，ATR/价格在合理区间
    middle_slope = channel_data["middle_slope"]
    atr_ratio = channel_data["atr"] / close if close > 0 else 0
    if middle_slope > -MIN_CHANNEL_SLOPE:
        return {"signal": False, "strength": 0, "reason": f"中轨斜率{middle_slope:.2f}% > {-MIN_CHANNEL_SLOPE*100:.2f}%，下跌趋势强度不足"}
    if atr_ratio < MIN_ATR_RATIO or atr_ratio > MAX_ATR_RATIO:
        return {"signal": False, "strength": 0, "reason": f"ATR/价格={atr_ratio*100:.2f}%不在{MIN_ATR_RATIO*100:.1f}%~{MAX_ATR_RATIO*100:.1f}%区间"}
    
    # 【修复】目标价 = 入场价 - ATR×2（合理盈亏比）
    atr = channel_data["atr"]
    signal_info = {
        "signal": False,
        "strength": 0,
        "reason": "",
        "break_pct": 0,
        "entry_price": close,
        "stop_loss": channel_data["middle"],
        "target": close - atr * 2,  # 做空：目标价 = 入场价 - 2倍ATR
    }
    
    # 条件1：连续2日收盘价站下轨
    close_last_3 = history.get("close_last_3", [])
    lower_last_3 = history.get("lower_last_3", [])
    
    if len(close_last_3) >= 2 and len(lower_last_3) >= 2:
        if close_last_3[-1] < lower_last_3[-1] and close_last_3[-2] < lower_last_3[-2]:
            signal_info["signal"] = True
            signal_info["strength"] = 0.6
            signal_info["reason"] = f"连续2日收盘价站下轨（收盘价{close:.2f} < 下轨{lower:.2f}）"
            signal_info["break_pct"] = (lower - close) / lower * 100
    
    # 条件2：放量跌破3%以上
    break_pct = (lower - close) / lower
    volume_ma = history.get("volume_ma", 1)
    volume_ratio = channel_data["volume"] / volume_ma if volume_ma > 0 else 0
    
    if break_pct >= ENTRY_VOLUME_BREAK_PCT and volume_ratio >= MIN_VOLUME_RATIO:
        signal_info["signal"] = True
        signal_info["strength"] = max(signal_info["strength"], 0.8)
        signal_info["reason"] = f"放量跌破下轨（跌破{break_pct*100:.1f}%，量比{volume_ratio:.2f}）"
        signal_info["break_pct"] = break_pct * 100
    
    # 趋势强度过滤：中轨斜率必须向下
    if signal_info["signal"]:
        middle_slope = channel_data["middle_slope"]
        if middle_slope > -MIN_TREND_STRENGTH:
            signal_info["signal"] = False
            signal_info["reason"] += f"，但趋势强度不足（中轨斜率{middle_slope:.2f}%）"
    
    # 【防止追跌】仅对放量跌破信号生效（跌破幅度>8%视为追跌）
    if signal_info["signal"] and break_pct >= ENTRY_VOLUME_BREAK_PCT:
        if break_pct > MAX_BREAK_PCT:
            signal_info["signal"] = False
            signal_info["reason"] += f"，但跌破幅度过大（{break_pct*100:.1f}% > {MAX_BREAK_PCT}%），视为追跌，拒绝入场"
    
    return signal_info

def check_exit_signal(position_type, channel_data):
    """
    检查出场信号（回归平仓）
    - 多单：收盘价跌破中轨，或价格回踩下轨未企稳
    - 空单：收盘价突破中轨，或价格反弹上轨未承压
    """
    if channel_data is None:
        return {"signal": False, "reason": ""}
    
    close = channel_data["close"]
    middle = channel_data["middle"]
    upper = channel_data["upper"]
    lower = channel_data["lower"]
    
    exit_info = {
        "signal": False,
        "reason": "",
        "exit_price": close,
        "urgency": 0,
    }
    
    if position_type == "long":
        if close < middle:
            exit_info["signal"] = True
            exit_info["reason"] = f"收盘价跌破中轨（收盘价{close:.2f} < 中轨{middle:.2f}）"
            exit_info["urgency"] = 0.8
        elif EXIT_ON_LOWER_TOUCH and close <= lower * 1.01:
            exit_info["signal"] = True
            exit_info["reason"] = f"价格回踩下轨未企稳（收盘价{close:.2f} ≈ 下轨{lower:.2f}）"
            exit_info["urgency"] = 0.6
    
    elif position_type == "short":
        if close > middle:
            exit_info["signal"] = True
            exit_info["reason"] = f"收盘价突破中轨（收盘价{close:.2f} > 中轨{middle:.2f}）"
            exit_info["urgency"] = 0.8
        elif EXIT_ON_UPPER_TOUCH and close >= upper * 0.99:
            exit_info["signal"] = True
            exit_info["reason"] = f"价格反弹上轨未承压（收盘价{close:.2f} ≈ 上轨{upper:.2f}）"
            exit_info["urgency"] = 0.6
    
    return exit_info

def calculate_trailing_stop(position_type, channel_data, buy_price):
    """计算移动止损价格"""
    if not TRAILING_STOP_ENABLED or channel_data is None:
        return buy_price * 0.95
    
    close = channel_data["close"]
    middle = channel_data["middle"]
    lower = channel_data["lower"]
    upper = channel_data["upper"]
    
    if position_type == "long":
        profit_pct = (close - buy_price) / buy_price
        if profit_pct > TRAILING_STOP_PROFIT_PCT:
            if TRAILING_STOP_TO_MIDDLE:
                return middle
            else:
                return lower
        else:
            return buy_price * 0.95
    
    elif position_type == "short":
        profit_pct = (buy_price - close) / buy_price
        if profit_pct > TRAILING_STOP_PROFIT_PCT:
            if TRAILING_STOP_TO_MIDDLE:
                return middle
            else:
                return upper
        else:
            return buy_price * 1.05
    
    return buy_price

def calculate_stock_score(channel_data):
    """计算股票综合评分"""
    if channel_data is None:
        return 0
    
    score = 0
    
    # 1. 趋势强度（40分）
    middle_slope = abs(channel_data["middle_slope"])
    if middle_slope > 0.5:
        score += 40
    elif middle_slope > 0.3:
        score += 30
    elif middle_slope > 0.1:
        score += 20
    elif middle_slope > 0.05:
        score += 10
    
    # 2. 价格位置（30分）
    price_position = channel_data["price_position"]
    if price_position > 90 or price_position < 10:
        score += 30
    elif price_position > 80 or price_position < 20:
        score += 20
    elif price_position > 70 or price_position < 30:
        score += 10
    
    # 3. 成交量配合（20分）
    volume_ma = channel_data["history"].get("volume_ma", 1)
    volume_ratio = channel_data["volume"] / volume_ma if volume_ma > 0 else 0
    if volume_ratio > 2.0:
        score += 20
    elif volume_ratio > 1.5:
        score += 15
    elif volume_ratio > 1.2:
        score += 10
    elif volume_ratio > 1.0:
        score += 5
    
    # 4. 通道宽度（10分）
    channel_width = channel_data["channel_width"]
    if 3 < channel_width < 10:
        score += 10
    elif 2 < channel_width < 15:
        score += 7
    elif 1 < channel_width < 20:
        score += 4
    
    return min(score, 100)
# ============================================================

# ========== 股票筛选函数 ==========
def filter_stocks_for_signals(exclude_codes=None):
    """
    筛选所有股票，返回Trendchannel策略信号
    """
    if exclude_codes is None:
        exclude_codes = []
    
    basic_info_file = os.path.join(Config.DATA_DIR, "all_stocks.csv")
    if not os.path.exists(basic_info_file):
        logger.error("股票列表文件不存在")
        return [], []
    
    try:
        basic_info_df = pd.read_csv(basic_info_file)
        logger.info(f"读取股票列表，共 {len(basic_info_df)} 只股票")
    except Exception as e:
        logger.error(f"读取股票列表失败: {str(e)}")
        return [], []
    
    long_signals = []
    short_signals = []
    processed_count = 0
    
    for _, row in basic_info_df.iterrows():
        code = str(row["代码"])
        if code in exclude_codes:
            continue
        
        market_cap = row.get("总市值", row.get("市值", 0)) / 1e8
        if market_cap < MIN_MARKET_CAP or market_cap > MAX_MARKET_CAP:
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
            
            # 过滤换手率
            turnover_rate = latest.get("换手率", 0)
            if turnover_rate < MIN_TURNOVER_RATE:
                continue
            
            # 过滤成交量
            volume = latest.get("成交量", 0)
            if volume < MIN_VOLUME:
                continue
            
            # 计算趋势通道
            channel_data = calculate_trendchannel(df)
            if channel_data is None:
                continue
            
            # 检查做多信号
            long_signal = check_long_entry_signal(channel_data)
            if long_signal["signal"] and long_signal["strength"] >= MIN_SIGNAL_STRENGTH:
                score = calculate_stock_score(channel_data)
                if score >= MIN_SIGNAL_SCORE:
                    stock_data = {
                        "code": code,
                        "name": name,
                        "close": latest["收盘"],
                        "market_cap": market_cap,
                        "turnover_rate": turnover_rate,
                        "score": score,
                        "entry_price": long_signal["entry_price"],
                        "stop_loss": long_signal["stop_loss"],
                        "target": long_signal["target"],
                        "break_pct": long_signal["break_pct"],
                        "reason": long_signal["reason"],
                        "middle": channel_data["middle"],
                        "upper": channel_data["upper"],
                        "lower": channel_data["lower"],
                        "atr": channel_data["atr"],
                        "channel_width": channel_data["channel_width"],
                        "volume_ratio": channel_data["volume"] / channel_data["history"].get("volume_ma", 1) if channel_data["history"].get("volume_ma", 0) > 0 else 0,
                    }
                    long_signals.append(stock_data)
            
            # 检查做空信号
            short_signal = check_short_entry_signal(channel_data)
            if short_signal["signal"] and short_signal["strength"] >= MIN_SIGNAL_STRENGTH:
                score = calculate_stock_score(channel_data)
                if score >= MIN_SIGNAL_SCORE:
                    stock_data = {
                        "code": code,
                        "name": name,
                        "close": latest["收盘"],
                        "market_cap": market_cap,
                        "turnover_rate": turnover_rate,
                        "score": score,
                        "entry_price": short_signal["entry_price"],
                        "stop_loss": short_signal["stop_loss"],
                        "target": short_signal["target"],
                        "break_pct": short_signal["break_pct"],
                        "reason": short_signal["reason"],
                        "middle": channel_data["middle"],
                        "upper": channel_data["upper"],
                        "lower": channel_data["lower"],
                        "atr": channel_data["atr"],
                        "channel_width": channel_data["channel_width"],
                        "volume_ratio": channel_data["volume"] / channel_data["history"].get("volume_ma", 1) if channel_data["history"].get("volume_ma", 0) > 0 else 0,
                    }
                    short_signals.append(stock_data)
            
            processed_count += 1
            
            if processed_count % 200 == 0:
                logger.info(f"已处理 {processed_count} 只股票，发现 {len(long_signals)} 个做多信号，{len(short_signals)} 个做空信号")
                
        except Exception as e:
            logger.error(f"处理股票 {code} 时出错: {str(e)}")
            continue
    
    long_signals.sort(key=lambda x: x["score"], reverse=True)
    short_signals.sort(key=lambda x: x["score"], reverse=True)
    
    logger.info(f"筛选完成，处理 {processed_count} 只股票，发现 {len(long_signals)} 个做多信号，{len(short_signals)} 个做空信号")
    return long_signals, short_signals
# ============================================================

# ========== 消息格式化函数 ==========
def format_long_signal_message(stock_data):
    """格式化做多信号消息"""
    score = stock_data["score"]
    entry_price = stock_data["entry_price"]
    stop_loss = stock_data["stop_loss"]
    target = stock_data["target"]
    risk_reward = (target - entry_price) / (entry_price - stop_loss) if entry_price > stop_loss else 0
    
    message = f"""【趋势通道 - 做多信号】
💰 {stock_data['code']} {stock_data['name']}

🎯 交易计划：
• 综合评分: {score:.0f}/100分
• 建议买入价: {entry_price:.2f}元（次日开盘附近）
• 止损价格: {stop_loss:.2f}元（中轨，-{(entry_price - stop_loss)/entry_price*100:.1f}%）
• 目标价格: {target:.2f}元（+{(target - entry_price)/entry_price*100:.1f}%）
• 风险收益比: 1:{risk_reward:.1f}

📈 技术指标：
• 当前价格: {entry_price:.2f}元
• 突破上轨: +{stock_data['break_pct']:.1f}%
• 通道宽度: {stock_data['channel_width']:.1f}%
• ATR(20): {stock_data['atr']:.3f}
• 中轨(MA20): {stock_data['middle']:.2f}元
• 上轨: {stock_data['upper']:.2f}元
• 下轨: {stock_data['lower']:.2f}元
• 量比: {stock_data['volume_ratio']:.2f}倍

📌 触发原因: {stock_data['reason']}
"""
    return message

def format_short_signal_message(stock_data):
    """格式化做空信号消息"""
    score = stock_data["score"]
    entry_price = stock_data["entry_price"]
    stop_loss = stock_data["stop_loss"]
    target = stock_data["target"]
    risk_reward = (entry_price - target) / (stop_loss - entry_price) if stop_loss > entry_price else 0
    
    message = f"""【趋势通道 - 做空信号】
💰 {stock_data['code']} {stock_data['name']}

🎯 交易计划：
• 综合评分: {score:.0f}/100分
• 建议卖出价: {entry_price:.2f}元（次日开盘附近）
• 止损价格: {stop_loss:.2f}元（中轨，+{(stop_loss - entry_price)/entry_price*100:.1f}%）
• 目标价格: {target:.2f}元（-{(entry_price - target)/entry_price*100:.1f}%）
• 风险收益比: 1:{risk_reward:.1f}

📉 技术指标：
• 当前价格: {entry_price:.2f}元
• 跌破下轨: {stock_data['break_pct']:.1f}%
• 通道宽度: {stock_data['channel_width']:.1f}%
• ATR(20): {stock_data['atr']:.3f}
• 中轨(MA20): {stock_data['middle']:.2f}元
• 上轨: {stock_data['upper']:.2f}元
• 下轨: {stock_data['lower']:.2f}元
• 量比: {stock_data['volume_ratio']:.2f}倍

📌 触发原因: {stock_data['reason']}
"""
    return message

def format_position_message(position):
    """格式化持仓状态消息"""
    direction = "做多" if position.get("direction", "long") == "long" else "做空"
    buy_price = position["buy_price"]
    current_price = position.get("current_price", buy_price)
    hold_days = position.get("hold_days", 0)
    
    if direction == "做多":
        profit_pct = (current_price - buy_price) / buy_price * 100
    else:
        profit_pct = (buy_price - current_price) / buy_price * 100
    
    profit_symbol = "🔴" if profit_pct < 0 else "🟢"
    
    if profit_pct >= 10:
        suggestion = "持有（已有较好盈利）"
    elif profit_pct >= 5:
        suggestion = "持有（继续观察）"
    elif profit_pct >= 0:
        suggestion = "持有（微利，关注止损）"
    elif profit_pct >= -3:
        suggestion = "持有（小幅亏损，关注止损）"
    else:
        suggestion = "考虑出场（亏损较大）"
    
    trailing_stop = position.get("stop_loss", buy_price * 0.95)
    middle = position.get("middle", 0)
    upper = position.get("upper", 0)
    lower = position.get("lower", 0)
    
    message = f"""【趋势通道 - 当前持仓】
💰 {position['code']} {position['name']}（{direction}）

📊 持仓状态：
• 买入日期: {position['buy_date']}
• 已持有{hold_days}天
• 买入价格: {buy_price:.2f}元
• 当前价格: {current_price:.2f}元
• 浮动盈亏: {profit_symbol} {profit_pct:+.1f}%
• 操作建议: {suggestion}

🎯 交易计划：
• 移动止损: {trailing_stop:.2f}元
• 通道状态: 中轨{middle:.2f} | 上轨{upper:.2f} | 下轨{lower:.2f}
"""
    return message

def format_no_signal_message(long_count, short_count):
    """格式化无信号消息"""
    current_date = datetime.now().strftime("%Y-%m-%d")
    message = f"""【趋势通道 - 今日无入场信号】
        
📅 日期: {current_date}
        
🔍 扫描结果:
• 做多信号: {long_count} 个
• 做空信号: {short_count} 个
        
📊 可能原因:
• 市场处于震荡期，未出现有效突破
• 趋势强度不足，不满足入场条件
• 综合评分未达到最低要求
        
💡 建议: 保持耐心，等待有效突破信号
"""
    return message

def format_trade_summary(summary):
    """格式化交易汇总消息"""
    if not summary:
        return """【趋势通道 - 策略交易汇总】

📊 交易统计:
• 暂无交易记录
• 策略处于初始化阶段"""
    
    message = f"""【趋势通道 - 策略交易汇总】

📅 策略统计周期:
• 开始日期: {summary['start_date']}
• 结束日期: {datetime.now().strftime('%Y-%m-%d')}

📊 交易统计:
• 累计买入次数: {summary['total_buy_times']}次
• 累计卖出次数: {summary['total_sell_times']}次"""
    return message

def format_status_message(history_positions_count, new_stocks_count, start_date,
                          positions_file_exists, positions_file_empty, positions_file_size,
                          trades_file_exists, trades_count,
                          positions_file_path, trades_file_path):
    """格式化策略状态消息"""
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
    
    message = f"""【趋势通道 - 策略状态】
• 历史持仓加载: {history_positions_count} 只
• 持仓文件: {positions_status}
• 今日新信号: {new_stocks_count} 只
• 📅 累计交易起始: {start_date_str}
• 交易记录文件: {trades_status}
• 运行环境: {os.getenv('RUN_ENV', 'unknown')}
"""
    return message
# ============================================================

# ========== 消息发送函数 ==========
def send_stock_messages(long_signals, short_signals, positions):
    """批量发送信号和持仓消息"""
    all_messages = []
    
    for signal in long_signals:
        all_messages.append(format_long_signal_message(signal))
    for signal in short_signals:
        all_messages.append(format_short_signal_message(signal))
    for position in positions:
        all_messages.append(format_position_message(position))
    
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
# ============================================================

# ========== 主函数 ==========
def main():
    logger.info("===== 开始执行趋势通道(Trendchannel)策略 =====")
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # 1. 初始化组件
        trade_recorder = TradeRecorder()
        position_manager = PositionManager(trade_recorder)
        
        # 2. 记录历史持仓数量
        history_positions_count = len(position_manager.positions)
        
        # 3. 更新持仓状态（检查出场信号）
        sold_positions = position_manager.update_positions(current_date)
        
        # 4. 发送卖出提示
        if sold_positions:
            sell_msg = "【趋势通道 - 出场提示】\n\n"
            for pos in sold_positions:
                direction = "做多" if pos.get("direction", "long") == "long" else "做空"
                profit = (pos["sell_price"] - pos["buy_price"]) / pos["buy_price"] * 100 if pos["direction"] == "long" else (pos["buy_price"] - pos["sell_price"]) / pos["buy_price"] * 100
                sell_msg += f"• {pos['code']} {pos['name']}（{direction}）- {pos['reason']} | 盈亏: {profit:+.1f}%\n"
            send_and_save_wechat_message(sell_msg, "position")
            time.sleep(2)
        
        # 5. 获取当前持仓
        current_positions = position_manager.get_current_positions()
        
        # 6. 筛选新信号
        holding_codes = position_manager.get_holding_codes()
        long_signals, short_signals = filter_stocks_for_signals(exclude_codes=holding_codes)
        
        # 7. 分配仓位（限制最大持仓数量）
        all_signals = long_signals + short_signals
        all_signals.sort(key=lambda x: x["score"], reverse=True)
        available_slots = max(0, MAX_POSITIONS - len(current_positions))
        new_signals = all_signals[:min(available_slots, len(all_signals))]
        
        # 8. 添加新持仓记录
        for signal in new_signals:
            direction = "long" if signal["break_pct"] > 0 and signal.get("reason", "").find("上轨") >= 0 else "short"
            position_manager.add_position(signal, signal["entry_price"], direction, 0.10)
        
        # 9. 重新获取持仓
        all_positions = position_manager.get_current_positions()
        
        # 10. 获取交易汇总
        trade_summary = trade_recorder.get_trade_summary()
        start_date = trade_summary['start_date'] if trade_summary else None
        
        # 11. 检查文件状态
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
            new_stocks_count=len(new_signals),
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
        
        # 13. 发送信号/持仓消息
        if all_signals or all_positions:
            send_stock_messages(long_signals, short_signals, all_positions)
            time.sleep(2)
        else:
            no_signal_msg = format_no_signal_message(len(long_signals), len(short_signals))
            send_and_save_wechat_message(no_signal_msg, "position")
        
        # 14. 发送交易汇总消息
        summary_msg = format_trade_summary(trade_summary)
        send_and_save_wechat_message(summary_msg, "position")
        
        logger.info("===== 策略执行完成 =====")
    except Exception as e:
        error_msg = f"【趋势通道 - 策略执行错误】\n错误详情：{str(e)}"
        logger.error(error_msg, exc_info=True)
        send_and_save_wechat_message(error_msg, "error")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(os.path.join(Config.LOG_DIR, "stock_t0_strategy.log"))
        ]
    )
    main()
