# position.py
import pandas as pd
import os
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from config import Config
from utils.file_utils import load_etf_daily_data, init_dirs
from .etf_scoring import get_top_rated_etfs, get_etf_name, get_etf_basic_info

# 初始化日志
logger = logging.getLogger(__name__)

# 仓位持仓记录路径
POSITION_RECORD_PATH = os.path.join(Config.BASE_DIR, "data", "position_record.csv")
TRADE_RECORD_PATH = Config.TRADE_RECORD_FILE

def init_position_record() -> pd.DataFrame:
    """
    初始化仓位记录（稳健仓、激进仓各持1只ETF）
    :return: 仓位记录的DataFrame
    """
    try:
        init_dirs()
        if not os.path.exists(POSITION_RECORD_PATH):
            logger.info("初始化仓位记录文件")
            # 初始无持仓
            position_df = pd.DataFrame({
                "仓位类型": ["稳健仓", "激进仓"],
                "当前持仓ETF代码": ["", ""],
                "当前持仓ETF名称": ["", ""],
                "持仓成本价": [0.0, 0.0],
                "持仓日期": ["", ""],
                "持仓数量": [0, 0],
                "最新操作": ["未持仓", "未持仓"],
                "操作日期": ["", ""],
                "创建时间": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 2,
                "更新时间": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 2
            })
            position_df.to_csv(POSITION_RECORD_PATH, index=False, encoding="utf-8")
            return position_df
        
        # 读取现有仓位记录
        position_df = pd.read_csv(POSITION_RECORD_PATH, encoding="utf-8")
        
        # 确保包含所有必要的列
        required_columns = [
            "仓位类型", "当前持仓ETF代码", "当前持仓ETF名称", "持仓成本价",
            "持仓日期", "持仓数量", "最新操作", "操作日期"
        ]
        
        for col in required_columns:
            if col not in position_df.columns:
                position_df[col] = ""
        
        logger.info(f"仓位记录加载成功，共{len(position_df)}条记录")
        return position_df
        
    except Exception as e:
        logger.error(f"初始化仓位记录失败: {str(e)}")
        # 返回空的DataFrame但包含必要的列
        return pd.DataFrame(columns=[
            "仓位类型", "当前持仓ETF代码", "当前持仓ETF名称", "持仓成本价",
            "持仓日期", "持仓数量", "最新操作", "操作日期"
        ])

def init_trade_record() -> pd.DataFrame:
    """
    初始化交易记录
    :return: 交易记录的DataFrame
    """
    try:
        init_dirs()
        if not os.path.exists(TRADE_RECORD_PATH):
            logger.info("初始化交易记录文件")
            trade_df = pd.DataFrame(columns=[
                "交易日期", "仓位类型", "操作类型", "ETF代码", "ETF名称",
                "价格", "数量", "金额", "收益率", "持仓天数", "原因", "记录时间"
            ])
            trade_df.to_csv(TRADE_RECORD_PATH, index=False, encoding="utf-8")
            return trade_df
        
        # 读取现有交易记录
        trade_df = pd.read_csv(TRADE_RECORD_PATH, encoding="utf-8")
        logger.info(f"交易记录加载成功，共{len(trade_df)}条记录")
        return trade_df
        
    except Exception as e:
        logger.error(f"初始化交易记录失败: {str(e)}")
        # 返回空的DataFrame但包含必要的列
        return pd.DataFrame(columns=[
            "交易日期", "仓位类型", "操作类型", "ETF代码", "ETF名称",
            "价格", "数量", "金额", "收益率", "持仓天数", "原因", "记录时间"
        ])

def record_trade(
    trade_date: str, 
    position_type: str, 
    operation: str, 
    etf_code: str, 
    etf_name: str, 
    price: float, 
    quantity: int, 
    amount: float, 
    profit_rate: float = 0, 
    hold_days: int = 0, 
    reason: str = ""
) -> bool:
    """
    记录交易流水
    :return: 是否成功记录交易
    """
    try:
        trade_df = init_trade_record()
        
        new_trade = pd.DataFrame([{
            "交易日期": trade_date,
            "仓位类型": position_type,
            "操作类型": operation,
            "ETF代码": etf_code,
            "ETF名称": etf_name,
            "价格": price,
            "数量": quantity,
            "金额": amount,
            "收益率": profit_rate,
            "持仓天数": hold_days,
            "原因": reason,
            "记录时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }])
        
        trade_df = pd.concat([trade_df, new_trade], ignore_index=True)
        trade_df.to_csv(TRADE_RECORD_PATH, index=False, encoding="utf-8")
        
        logger.info(f"记录交易: {operation} {etf_name}({etf_code}) {quantity}股 @ {price:.2f}元")
        
        # 尝试发送交易通知到微信
        try:
            from wechat_push import send_wechat_message
            message = f"【交易执行通知】\n\n"
            message += f"操作: {operation}\n"
            message += f"ETF: {etf_name} ({etf_code})\n"
            message += f"价格: {price:.2f}元\n"
            message += f"数量: {quantity}股\n"
            message += f"金额: {amount:.2f}元\n"
            if profit_rate != 0:
                message += f"收益率: {profit_rate:.2f}%\n"
            message += f"原因: {reason}\n"
            message += f"时间: {trade_date}"
            
            send_wechat_message(message)
        except Exception as e:
            logger.error(f"微信交易通知发送失败: {str(e)}")
        
        return True
        
    except Exception as e:
        logger.error(f"记录交易失败: {str(e)}")
        return False

def update_position_record(
    position_type: str, 
    etf_code: str, 
    etf_name: str, 
    cost_price: float, 
    quantity: int, 
    action: str
) -> pd.DataFrame:
    """
    更新仓位记录
    :return: 更新后的仓位记录DataFrame
    """
    try:
        position_df = init_position_record()
        today = datetime.now().strftime("%Y-%m-%d")
        
        # 找到对应仓位行
        mask = position_df["仓位类型"] == position_type
        if not mask.any():
            logger.warning(f"未找到仓位类型: {position_type}，将创建新记录")
            new_row = pd.DataFrame([{
                "仓位类型": position_type,
                "当前持仓ETF代码": etf_code,
                "当前持仓ETF名称": etf_name,
                "持仓成本价": cost_price,
                "持仓日期": today,
                "持仓数量": quantity,
                "最新操作": action,
                "操作日期": today,
                "创建时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }])
            position_df = pd.concat([position_df, new_row], ignore_index=True)
        else:
            idx = position_df[mask].index[0]
            position_df.loc[idx] = [
                position_type,
                etf_code,
                etf_name,
                cost_price,
                today,
                quantity,
                action,
                today,
                position_df.loc[idx, "创建时间"] if "创建时间" in position_df.columns else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ]
        
        position_df.to_csv(POSITION_RECORD_PATH, index=False, encoding="utf-8")
        logger.info(f"更新仓位记录: {position_type} - {action}")
        return position_df
        
    except Exception as e:
        logger.error(f"更新仓位记录失败: {str(e)}")
        return pd.DataFrame()

def calculate_ma_signal(df: pd.DataFrame, short_period: int = 5, long_period: int = 20) -> Tuple[bool, bool]:
    """
    计算均线信号
    :param df: ETF历史数据
    :param short_period: 短期均线周期
    :param long_period: 长期均线周期
    :return: (多头信号, 空头信号)
    """
    if df is None or df.empty or len(df) < long_period:
        logger.warning(f"数据不足，无法计算{short_period}/{long_period}日均线信号")
        return False, False
    
    try:
        df = df.copy()
        df["short_ma"] = df["收盘"].rolling(window=short_period).mean()
        df["long_ma"] = df["收盘"].rolling(window=long_period).mean()
        
        # 检查是否连续几天短期均线上穿长期均线
        signal_days = 0
        for i in range(-Config.BUY_SIGNAL_DAYS, 0):
            if i >= -len(df) and df["short_ma"].iloc[i] > df["long_ma"].iloc[i]:
                signal_days += 1
        
        ma_bullish = signal_days >= Config.BUY_SIGNAL_DAYS
        ma_bearish = len(df) > 0 and df["short_ma"].iloc[-1] < df["long_ma"].iloc[-1]
        
        logger.debug(f"均线信号计算: 多头={ma_bullish}, 空头={ma_bearish}")
        return ma_bullish, ma_bearish
        
    except Exception as e:
        logger.error(f"计算均线信号失败: {str(e)}")
        return False, False

def calculate_position_strategy() -> str:
    """
    计算仓位操作策略（稳健仓、激进仓）
    :return: 策略消息字符串
    """
    try:
        logger.info("="*50)
        logger.info("开始计算ETF仓位操作策略")
        logger.info("="*50)
        
        # 1. 初始化仓位记录
        position_df = init_position_record()
        init_trade_record()
        
        # 获取稳健仓评分前5的ETF（使用稳健仓参数）
        stable_top_etfs = get_top_rated_etfs(top_n=5, position_type="稳健仓")
        if stable_top_etfs.empty:
            logger.warning("无有效ETF评分数据，无法计算稳健仓策略")
            stable_strategy = "【稳健仓】无有效ETF数据，无法生成操作建议"
        else:
            # 2.1 稳健仓策略（评分最高+均线策略）
            stable_etf = stable_top_etfs.iloc[0]
            stable_code = stable_etf["etf_code"]
            stable_name = stable_etf["etf_name"]
            stable_df = load_etf_daily_data(stable_code)
            
            # 稳健仓当前持仓
            stable_position = position_df[position_df["仓位类型"] == "稳健仓"]
            if stable_position.empty:
                logger.warning("未找到稳健仓记录，使用默认值")
                stable_position = pd.Series({
                    "当前持仓ETF代码": "",
                    "当前持仓ETF名称": "",
                    "持仓成本价": 0.0,
                    "持仓日期": "",
                    "持仓数量": 0
                })
            else:
                stable_position = stable_position.iloc[0]
            
            strategy, actions = calculate_single_position_strategy(
                position_type="稳健仓",
                current_position=stable_position,
                target_etf_code=stable_code,
                target_etf_name=stable_name,
                etf_df=stable_df,
                is_stable=True
            )
            stable_strategy = strategy
            trade_actions = actions
        # 获取激进仓评分前5的ETF（使用激进仓参数）
        aggressive_top_etfs = get_top_rated_etfs(top_n=5, position_type="激进仓")
        if aggressive_top_etfs.empty:
            logger.warning("无有效ETF评分数据，无法计算激进仓策略")
            aggressive_strategy = "【激进仓】无有效ETF数据，无法生成操作建议"
        else:
            # 2.2 激进仓策略（近30天收益最高）
            return_list = []
            for _, row in aggressive_top_etfs.iterrows():
                code = row["etf_code"]
                df = load_etf_daily_data(code)
                if not df.empty and len(df) >= 30:
                    try:
                        return_30d = (df.iloc[-1]["收盘"] / df.iloc[-30]["收盘"] - 1) * 100
                        return_list.append({
                            "etf_code": code,
                            "etf_name": row["etf_name"],
                            "return_30d": return_30d,
                            "score": row["score"]
                        })
                    except (IndexError, KeyError):
                        logger.warning(f"计算ETF {code} 30天收益失败")
                        continue
            
            if return_list:
                aggressive_etf = max(return_list, key=lambda x: x["return_30d"])
                aggressive_code = aggressive_etf["etf_code"]
                aggressive_name = aggressive_etf["etf_name"]
                aggressive_df = load_etf_daily_data(aggressive_code)
                
                # 激进仓当前持仓
                aggressive_position = position_df[position_df["仓位类型"] == "激进仓"]
                if aggressive_position.empty:
                    logger.warning("未找到激进仓记录，使用默认值")
                    aggressive_position = pd.Series({
                        "当前持仓ETF代码": "",
                        "当前持仓ETF名称": "",
                        "持仓成本价": 0.0,
                        "持仓日期": "",
                        "持仓数量": 0
                    })
                else:
                    aggressive_position = aggressive_position.iloc[0]
                
                strategy, actions = calculate_single_position_strategy(
                    position_type="激进仓",
                    current_position=aggressive_position,
                    target_etf_code=aggressive_code,
                    target_etf_name=aggressive_name,
                    etf_df=aggressive_df,
                    is_stable=False
                )
                aggressive_strategy = strategy
                if 'trade_actions' in locals():
                    trade_actions.extend(actions)
                else:
                    trade_actions = actions
            else:
                aggressive_strategy = "激进仓：无有效收益数据，暂不调整仓位"
        
        # 合并策略结果
        strategies = {
            "稳健仓": stable_strategy,
            "激进仓": aggressive_strategy
        }
        
        # 3. 执行交易操作（如果存在交易动作）
        if 'trade_actions' in locals() and trade_actions:
            for action in trade_actions:
                record_trade(**action)
        
        # 4. 格式化消息
        return format_position_message(strategies)
        
    except Exception as e:
        logger.error(f"计算仓位策略失败: {str(e)}")
        return "【ETF仓位操作提示】\n计算仓位策略时发生错误，请检查日志"

def calculate_single_position_strategy(
    position_type: str, 
    current_position: pd.Series, 
    target_etf_code: str, 
    target_etf_name: str, 
    etf_df: pd.DataFrame, 
    is_stable: bool
) -> Tuple[str, List[Dict]]:
    """
    计算单个仓位（稳健/激进）的操作策略
    :return: (策略描述, 交易动作列表)
    """
    if etf_df.empty or len(etf_df) < Config.MA_LONG_PERIOD:
        return f"{position_type}：目标ETF数据不足，暂不调整", []
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    trade_actions = []
    
    try:
        # 计算均线信号
        ma_bullish, ma_bearish = calculate_ma_signal(etf_df, Config.MA_SHORT_PERIOD, Config.MA_LONG_PERIOD)
        latest_close = etf_df.iloc[-1]["收盘"]
        
        # 当前持仓信息
        has_position = not pd.isna(current_position["当前持仓ETF代码"]) and current_position["当前持仓ETF代码"] != ""
        current_code = current_position["当前持仓ETF代码"] if has_position else ""
        current_name = current_position["当前持仓ETF名称"] if has_position else ""
        current_cost = float(current_position["持仓成本价"]) if has_position else 0
        current_quantity = int(current_position["持仓数量"]) if has_position else 0
        position_date = current_position["持仓日期"] if has_position else ""
        
        # 计算持仓收益率（如果有持仓）
        if has_position and position_date:
            try:
                hold_days = (datetime.now() - datetime.strptime(position_date, "%Y-%m-%d")).days
                profit_rate = (latest_close / current_cost - 1) * 100
            except (ValueError, TypeError):
                logger.warning(f"计算持仓收益率失败，使用默认值")
                hold_days = 0
                profit_rate = 0
        else:
            hold_days = 0
            profit_rate = 0
        
        # 1. 当前无持仓：判断是否买入
        if not has_position:
            if ma_bullish:  # 均线多头才买入
                # 执行买入
                update_position_record(
                    position_type=position_type,
                    etf_code=target_etf_code,
                    etf_name=target_etf_name,
                    cost_price=latest_close,
                    quantity=1000,  # 默认买入1000股
                    action=f"买入（成本价：{latest_close:.2f}元）"
                )
                # 记录交易
                trade_actions.append({
                    "trade_date": current_date,
                    "position_type": position_type,
                    "operation": "买入",
                    "etf_code": target_etf_code,
                    "etf_name": target_etf_name,
                    "price": latest_close,
                    "quantity": 1000,
                    "amount": latest_close * 1000,
                    "profit_rate": 0,
                    "hold_days": 0,
                    "reason": "均线多头突破，符合买入条件"
                })
                return f"{position_type}：执行买入【{target_etf_name}（{target_etf_code}）】，成本价：{latest_close:.2f}元", trade_actions
            else:
                return f"{position_type}：当前无持仓，目标ETF未满足买入条件（均线未多头），暂不买入", []
        
        # 2. 判断是否换股（新ETF评分比当前高30%）
        if has_position and current_code != target_etf_code:
            from .etf_scoring import calculate_etf_score
            current_score = calculate_etf_score(current_code, load_etf_daily_data(current_code))
            target_score = calculate_etf_score(target_etf_code, etf_df)
            
            if target_score > current_score * (1 + Config.SWITCH_THRESHOLD) and ma_bullish:
                # 执行换股
                update_position_record(
                    position_type=position_type,
                    etf_code=target_etf_code,
                    etf_name=target_etf_name,
                    cost_price=latest_close,
                    quantity=1000,
                    action=f"换股（卖出{current_name}，买入{target_etf_name}）"
                )
                # 记录卖出交易
                trade_actions.append({
                    "trade_date": current_date,
                    "position_type": position_type,
                    "operation": "卖出",
                    "etf_code": current_code,
                    "etf_name": current_name,
                    "price": latest_close,
                    "quantity": current_quantity,
                    "amount": latest_close * current_quantity,
                    "profit_rate": profit_rate,
                    "hold_days": hold_days,
                    "reason": f"换股：新ETF评分({target_score})比当前({current_score})高{Config.SWITCH_THRESHOLD*100}%"
                })
                # 记录买入交易
                trade_actions.append({
                    "trade_date": current_date,
                    "position_type": position_type,
                    "operation": "买入",
                    "etf_code": target_etf_code,
                    "etf_name": target_etf_name,
                    "price": latest_close,
                    "quantity": 1000,
                    "amount": latest_close * 1000,
                    "profit_rate": 0,
                    "hold_days": 0,
                    "reason": "换股操作"
                })
                return f"{position_type}：执行换股\n原持仓：{current_name}（{current_code}）收益率：{profit_rate:.2f}%\n新持仓：{target_etf_name}（{target_etf_code}）\n原因：新ETF评分高出{Config.SWITCH_THRESHOLD*100}%", trade_actions
        
        # 3. 止损判断（跌破止损阈值）
        if has_position and profit_rate <= Config.STOP_LOSS_THRESHOLD * 100:
            # 执行止损
            update_position_record(
                position_type=position_type,
                etf_code="",
                etf_name="",
                cost_price=0.0,
                quantity=0,
                action=f"止损卖出（收益率：{profit_rate:.2f}%）"
            )
            # 记录交易
            trade_actions.append({
                "trade_date": current_date,
                "position_type": position_type,
                "operation": "卖出",
                "etf_code": current_code,
                "etf_name": current_name,
                "price": latest_close,
                "quantity": current_quantity,
                "amount": latest_close * current_quantity,
                "profit_rate": profit_rate,
                "hold_days": hold_days,
                "reason": f"止损：收益率({profit_rate:.2f}%)低于止损阈值({Config.STOP_LOSS_THRESHOLD*100}%)"
            })
            return f"{position_type}：执行止损\n持仓：{current_name}（{current_code}）\n收益率：{profit_rate:.2f}%（跌破止损阈值{Config.STOP_LOSS_THRESHOLD*100:.1f}%）", trade_actions
        
        # 4. 继续持有
        ma_status = "5日均线＞20日均线" if not ma_bearish else "5日均线＜20日均线"
        return f"{position_type}：继续持有【{current_name}（{current_code}）】\n当前价格：{latest_close:.2f}元，成本价：{current_cost:.2f}元\n收益率：{profit_rate:.2f}%，持仓天数：{hold_days}天\n均线状态：{ma_status}", trade_actions
        
    except Exception as e:
        logger.error(f"计算{position_type}策略失败: {str(e)}")
        return f"{position_type}：计算策略时发生错误", []

def format_position_message(strategies: Dict[str, str]) -> str:
    """
    格式化仓位策略消息
    :param strategies: 策略字典
    :return: 格式化后的消息字符串
    """
    try:
        message = "【ETF仓位操作提示】\n"
        message += "（每个仓位仅持有1只ETF，操作建议基于最新数据）\n\n"
        
        for position_type, content in strategies.items():
            message += f"【{position_type}】\n{content}\n\n"
        
        message += "风险提示：操作前请结合自身风险承受能力，市场波动可能导致策略失效！"
        return message
        
    except Exception as e:
        logger.error(f"格式化仓位消息失败: {str(e)}")
        return "【ETF仓位操作提示】\n生成仓位消息时发生错误"

# 模块初始化
try:
    logger.info("仓位管理模块初始化完成")
except Exception as e:
    print(f"仓位管理模块初始化失败: {str(e)}")
# 0828-1256【position.py代码】一共406行代码
