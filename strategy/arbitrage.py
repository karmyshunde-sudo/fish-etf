# arbitrage.py
import pandas as pd
import numpy as np
import logging
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
from config import Config
from utils.file_utils import load_etf_daily_data
from .etf_scoring import get_etf_name, get_top_rated_etfs

# 初始化日志
logger = logging.getLogger(__name__)

def calculate_premium_rate(etf_code: str) -> float:
    """
    计算ETF溢价率（需要实时数据，这里用简化版本）
    :param etf_code: ETF代码
    :return: 溢价率（小数形式，如0.01表示1%）
    """
    try:
        # 实际应用中应该获取实时IOPV和市场价格
        # 这里使用简化版本：随机生成一个溢价率用于演示
        premium_rate = np.random.uniform(-0.02, 0.02)  # -2%到+2%的随机溢价率
        logger.debug(f"ETF {etf_code} 溢价率: {premium_rate:.4f}")
        return premium_rate
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 溢价率失败: {str(e)}")
        return 0.0

def calculate_arbitrage_opportunity() -> pd.DataFrame:
    """
    计算ETF套利机会（基于溢价率，考虑交易成本）
    逻辑：找溢价率超阈值（含成本）的机会
    :return: 包含套利机会的DataFrame
    """
    try:
        logger.info("="*50)
        logger.info("开始计算ETF套利机会")
        logger.info("="*50)
        
        arbitrage_list = []
        # 获取高分ETF列表（前20%）
        top_etfs = get_top_rated_etfs()
        if top_etfs.empty:
            logger.warning("无足够高分ETF用于计算套利机会")
            return pd.DataFrame()
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"分析 {len(top_etfs)} 只高分ETF的套利机会")
        
        for idx, row in top_etfs.iterrows():
            try:
                etf_code = row["etf_code"]
                etf_name = row["etf_name"]
                
                # 计算溢价率
                premium_rate = calculate_premium_rate(etf_code)
                
                # 计算扣除成本后的套利收益率
                net_profit = abs(premium_rate) - Config.TRADE_COST_RATE
                
                # 判断套利机会：净收益超阈值
                if net_profit >= Config.ARBITRAGE_PROFIT_THRESHOLD:
                    if premium_rate > 0:
                        action = f"溢价套利：卖出{etf_name}（{etf_code}）"
                        direction = "溢价"
                    else:
                        action = f"折价套利：买入{etf_name}（{etf_code}）"
                        direction = "折价"
                    
                    arbitrage_list.append({
                        "ETF代码": etf_code,
                        "ETF名称": etf_name,
                        "套利方向": action,
                        "溢价率": f"{premium_rate:.3%}",
                        "交易成本": f"{Config.TRADE_COST_RATE:.3%}",
                        "净收益率": f"{net_profit:.3%}",
                        "套利类型": direction,
                        "发现时间": current_date
                    })
                    logger.info(f"发现套利机会: {etf_name}({etf_code}) {direction}套利, 净收益: {net_profit:.3%}")
                
            except Exception as e:
                logger.error(f"分析ETF {row.get('etf_code', '未知')} 套利机会时发生错误: {str(e)}")
                continue
        
        # 转换为DataFrame
        if arbitrage_list:
            arbitrage_df = pd.DataFrame(arbitrage_list)
            logger.info(f"找到 {len(arbitrage_df)} 个套利机会")
            
            # 记录套利交易（假设执行）
            record_arbitrage_trades(arbitrage_df)
            
            return arbitrage_df
        else:
            logger.info("未找到符合条件的套利机会")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"计算套利机会时发生未预期错误: {str(e)}")
        return pd.DataFrame()

def record_arbitrage_trades(arbitrage_df: pd.DataFrame) -> bool:
    """
    记录套利交易
    :param arbitrage_df: 套利机会DataFrame
    :return: 是否成功记录交易
    """
    try:
        from position import init_trade_record, record_trade
        
        init_trade_record()
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        for _, row in arbitrage_df.iterrows():
            try:
                etf_code = row["ETF代码"]
                etf_name = row["ETF名称"]
                premium_rate = float(row["溢价率"].strip('%')) / 100
                net_profit = float(row["净收益率"].strip('%')) / 100
                
                # 获取当前价格（简化处理）
                df = load_etf_daily_data(etf_code)
                if not df.empty:
                    price = df.iloc[-1]["收盘"]
                else:
                    price = 1.0  # 默认价格
                    logger.warning(f"无法获取ETF {etf_code} 价格，使用默认值 1.0")
                
                # 确定操作类型
                if "溢价" in row["套利类型"]:
                    operation = "卖出"
                    reason = "溢价套利机会"
                else:
                    operation = "买入"
                    reason = "折价套利机会"
                
                # 记录交易
                record_trade(
                    trade_date=current_date,
                    position_type="套利仓",
                    operation=operation,
                    etf_code=etf_code,
                    etf_name=etf_name,
                    price=price,
                    quantity=1000,
                    amount=price * 1000,
                    profit_rate=net_profit * 100,
                    hold_days=1,  # 套利持仓1天
                    reason=f"{reason}，溢价率：{premium_rate:.3%}"
                )
                
            except Exception as e:
                logger.error(f"记录ETF {row.get('ETF代码', '未知')} 套利交易时发生错误: {str(e)}")
                continue
        
        logger.info(f"成功记录 {len(arbitrage_df)} 个套利交易")
        return True
        
    except Exception as e:
        logger.error(f"记录套利交易时发生未预期错误: {str(e)}")
        return False

def format_arbitrage_message(arbitrage_df: pd.DataFrame) -> str:
    """
    格式化套利机会消息
    :param arbitrage_df: 套利机会DataFrame
    :return: 格式化后的消息字符串
    """
    try:
        if arbitrage_df.empty:
            return "【ETF套利机会提示】\n今日未找到符合条件的ETF套利机会（考虑交易成本后）"
        
        message = "【ETF套利机会提示】\n"
        message += f"共发现 {len(arbitrage_df)} 个套利机会（交易成本：{Config.TRADE_COST_RATE:.2%}）\n\n"
        
        for idx, (_, row) in enumerate(arbitrage_df.iterrows(), 1):
            message += f"{idx}. {row['ETF名称']}（{row['ETF代码']}）\n"
            message += f"   操作建议：{row['套利方向']}\n"
            message += f"   溢价率：{row['溢价率']} | 净收益率：{row['净收益率']}\n"
            message += f"   发现时间：{row['发现时间']}\n\n"
        
        message += "⚠️ 套利提示：套利机会通常短暂，需快速执行！次日请关注获利了结机会。"
        return message
        
    except Exception as e:
        logger.error(f"格式化套利消息时发生错误: {str(e)}")
        return "【ETF套利机会提示】\n生成套利消息时发生错误"

def check_arbitrage_exit_signals() -> bool:
    """
    检查套利退出信号（持有1天后）
    :return: 是否成功检查退出信号
    """
    try:
        from position import init_trade_record
        from wechat_push import send_wechat_message
        
        init_trade_record()
        trade_df = pd.read_csv(Config.TRADE_RECORD_FILE, encoding="utf-8")
        
        # 获取昨天的日期
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # 查找昨天执行的套利交易
        yesterday_arbitrage = trade_df[
            (trade_df["交易日期"] == yesterday) & 
            (trade_df["仓位类型"] == "套利仓") &
            (trade_df["操作类型"] == "买入")
        ]
        
        if not yesterday_arbitrage.empty:
            exit_messages = []
            for _, trade in yesterday_arbitrage.iterrows():
                # 建议卖出套利持仓
                exit_messages.append(
                    f"套利持仓退出建议: 卖出 {trade['ETF名称']} ({trade['ETF代码']})，"
                    f"买入价: {trade['价格']}元，建议获利了结"
                )
            
            if exit_messages:
                message = "【套利持仓退出提示】\n\n" + "\n".join(exit_messages)
                message += "\n\n💡 套利持仓建议持有不超过1天，请及时了结！"
                
                # 发送微信消息
                send_wechat_message(message)
                logger.info("套利退出提示已发送")
                
        return True
        
    except Exception as e:
        logger.error(f"检查套利退出信号失败: {str(e)}")
        return False

def get_real_time_premium_rate(etf_code: str) -> Optional[float]:
    """
    尝试获取实时溢价率（实际生产环境中应实现此函数）
    :param etf_code: ETF代码
    :return: 实时溢价率或None（如果无法获取）
    """
    try:
        # 实际生产环境中，这里应该调用实时数据API
        # 例如使用AkShare或其他金融数据API获取实时IOPV和市场价格
        # 这里返回None表示未实现
        
        logger.warning(f"实时溢价率获取功能未实现，ETF: {etf_code}")
        return None
        
    except Exception as e:
        logger.error(f"获取实时溢价率失败: {str(e)}")
        return None

def simulate_real_time_data(etf_code: str) -> float:
    """
    模拟实时数据获取（用于演示和测试）
    :param etf_code: ETF代码
    :return: 模拟的溢价率
    """
    try:
        # 基于历史数据模拟实时溢价率
        df = load_etf_daily_data(etf_code)
        if df.empty or len(df) < 5:
            return np.random.uniform(-0.02, 0.02)
        
        # 使用最近5天的波动性来模拟实时溢价率
        recent_volatility = df["涨跌幅"].tail(5).std()
        premium_rate = np.random.normal(0, recent_volatility * 2)
        
        # 限制溢价率范围在±5%以内
        premium_rate = np.clip(premium_rate, -0.05, 0.05)
        
        logger.debug(f"模拟ETF {etf_code} 实时溢价率: {premium_rate:.4f}")
        return premium_rate
        
    except Exception as e:
        logger.error(f"模拟实时数据失败: {str(e)}")
        return np.random.uniform(-0.02, 0.02)

# 模块初始化
try:
    logger.info("套利策略模块初始化完成")
except Exception as e:
    print(f"套利策略模块初始化失败: {str(e)}")
# 0828-1256【arbitrage.py代码】一共202行代码
