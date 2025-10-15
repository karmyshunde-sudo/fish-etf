#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票技术分析策略3
接收股票代码，分析技术指标并生成报告
严格遵循项目架构原则：只负责计算，不涉及数据爬取和消息格式化
"""

import pandas as pd
import numpy as np
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time
)
from wechat_push.push import send_wechat_message
# 专业修复：从正确的模块导入函数
from stock.crawler import fetch_stock_daily_data, save_stock_daily_data

# 初始化日志
logger = logging.getLogger(__name__)

def load_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """
    加载股票日线数据
    
    Args:
        stock_code: 股票代码
    
    Returns:
        pd.DataFrame: 股票日线数据
    """
    try:
        # 构建文件路径
        file_path = os.path.join(Config.DATA_DIR, "daily", f"{stock_code}.csv")
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.warning(f"股票 {stock_code} 日线数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        # 读取CSV文件，明确指定数据类型
        df = pd.read_csv(
            file_path,
            encoding="utf-8",
            dtype={
                "日期": str,
                "开盘": float,
                "最高": float,
                "最低": float,
                "收盘": float,
                "成交量": float,
                "成交额": float,
                "换手率": float,
                "流通市值": float
            }
        )
        
        # 检查必需列
        required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"股票 {stock_code} 数据缺少必要列: {', '.join(missing_columns)}")
            return pd.DataFrame()
        
        # 确保日期列为字符串格式
        df["日期"] = df["日期"].astype(str)
        
        # 按日期排序并去重
        df = df.sort_values("日期").drop_duplicates(subset=["日期"], keep="last")
        
        # 移除未来日期的数据
        today = datetime.now().strftime("%Y-%m-%d")
        df = df[df["日期"] <= today]
        
        return df
    
    except Exception as e:
        logger.error(f"加载股票 {stock_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def ensure_stock_data(stock_code: str, days: int = 365) -> bool:
    """
    确保有指定股票的日线数据，如果没有则爬取
    
    Args:
        stock_code: 股票代码
        days: 需要爬取的天数
    
    Returns:
        bool: 是否成功获取数据
    """
    # 检查数据是否存在
    df = load_stock_daily_data(stock_code)
    
    if not df.empty:
        logger.info(f"已找到股票 {stock_code} 的日线数据，共 {len(df)} 条记录")
        return True
    
    # 数据不存在，开始爬取
    logger.info(f"股票 {stock_code} 日线数据不存在，开始爬取...")
    
    try:
        # 爬取数据 - 专业修复：调用正确的函数
        df = fetch_stock_daily_data(stock_code)
        
        # 保存数据 - 专业修复：调用正确的函数
        if not df.empty:
            save_stock_daily_data(stock_code, df)
            
            # 再次检查数据
            df = load_stock_daily_data(stock_code)
            if not df.empty:
                logger.info(f"成功获取股票 {stock_code} 日线数据，共 {len(df)} 条记录")
                return True
    
        logger.error(f"爬取股票 {stock_code} 日线数据失败")
        return False
    
    except Exception as e:
        logger.error(f"爬取股票 {stock_code} 日线数据时出错: {str(e)}", exc_info=True)
        return False

def calculate_technical_indicators(df: pd.DataFrame) -> Dict[str, Any]:
    """
    计算技术指标
    
    Args:
        df: 股票日线数据
    
    Returns:
        Dict[str, Any]: 技术指标结果
    """
    try:
        # 确保有足够的数据
        if len(df) < 30:
            logger.warning("数据量不足，无法准确计算技术指标")
            return {}
        
        # 获取收盘价序列
        close = df["收盘"].values
        high = df["最高"].values
        low = df["最低"].values
        volume = df["成交量"].values
        
        # 1. 移动平均线
        ma5 = df["收盘"].rolling(5).mean().iloc[-1]
        ma10 = df["收盘"].rolling(10).mean().iloc[-1]
        ma20 = df["收盘"].rolling(20).mean().iloc[-1]
        ma50 = df["收盘"].rolling(50).mean().iloc[-1]
        ma100 = df["收盘"].rolling(100).mean().iloc[-1]
        ma250 = df["收盘"].rolling(250).mean().iloc[-1]
        
        # 2. MACD指标
        macd_line, signal_line, _ = calculate_macd(df)
        macd_value = macd_line.iloc[-1] - signal_line.iloc[-1]  # MACD柱状图
        
        # 3. RSI指标
        rsi_value = calculate_rsi(df, 14)
        
        # 4. 布林带
        upper_band, middle_band, lower_band = calculate_bollinger_bands(df, 20, 2)
        
        # 5. 量比
        avg_volume_5d = df["成交量"].rolling(5).mean().iloc[-1]
        volume_ratio = volume[-1] / avg_volume_5d if avg_volume_5d > 0 else 0
        
        # 6. 换手率
        turnover_rate = df["换手率"].iloc[-1] if "换手率" in df.columns else 0
        
        # 7. 流通市值
        market_cap = df["流通市值"].iloc[-1] if "流通市值" in df.columns else 0
        
        # 8. 过去5个交易日成交量
        last_5_volume = df["成交量"].tail(5).mean()
        
        # 9. 均线形态
        ma_trend = "多头排列" if ma5 > ma10 > ma20 > ma50 > ma100 > ma250 else \
                  "空头排列" if ma5 < ma10 < ma20 < ma50 < ma100 < ma250 else "震荡"
        
        # 10. 当前价格与各均线的偏离率
        current_price = close[-1]
        deviation_ma5 = (current_price - ma5) / ma5 * 100 if ma5 > 0 else 0
        deviation_ma20 = (current_price - ma20) / ma20 * 100 if ma20 > 0 else 0
        deviation_ma250 = (current_price - ma250) / ma250 * 100 if ma250 > 0 else 0
        
        # 11. 布林带状态
        bollinger_status = "上轨" if current_price > upper_band else \
                          "中轨" if current_price > middle_band else "下轨"
        
        # 12. RSI状态
        rsi_status = "超买" if rsi_value > 70 else "超卖" if rsi_value < 30 else "中性"
        
        # 13. MACD状态
        macd_status = "金叉" if macd_value > 0 else "死叉" if macd_value < 0 else "震荡"
        
        return {
            # 均线系统
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma50": ma50,
            "ma100": ma100,
            "ma250": ma250,
            "ma_trend": ma_trend,
            "deviation_ma5": deviation_ma5,
            "deviation_ma20": deviation_ma20,
            "deviation_ma250": deviation_ma250,
            
            # MACD指标
            "macd_line": macd_line.iloc[-1],
            "signal_line": signal_line.iloc[-1],
            "macd_value": macd_value,
            "macd_status": macd_status,
            
            # RSI指标
            "rsi_value": rsi_value,
            "rsi_status": rsi_status,
            
            # 布林带
            "upper_band": upper_band,
            "middle_band": middle_band,
            "lower_band": lower_band,
            "bollinger_status": bollinger_status,
            
            # 量能指标
            "volume_ratio": volume_ratio,
            "turnover_rate": turnover_rate,
            "market_cap": market_cap,
            "last_5_volume": last_5_volume,
            
            # 其他指标
            "current_price": current_price
        }
    
    except Exception as e:
        logger.error(f"计算技术指标失败: {str(e)}", exc_info=True)
        return {}

def generate_analysis_report(stock_code: str, stock_name: str, indicators: Dict[str, Any]) -> str:
    """
    生成技术分析报告
    
    Args:
        stock_code: 股票代码
        stock_name: 股票名称
        indicators: 技术指标结果
    
    Returns:
        str: 分析报告内容
    """
    try:
        # 获取当前时间
        beijing_time = get_beijing_time()
        
        # 开始构建报告
        report = f"【{stock_code}】{stock_name} 技术分析报告\n"
        report += f"📅 分析日期：{beijing_time.strftime('%Y-%m-%d %H:%M')}\n\n"
        
        # 1. 关键技术指标动态
        report += "1. 关键技术指标动态\n"
        report += f"   • 均线系统：{indicators['ma_trend']}\n"
        report += f"   • MACD指标：{indicators['macd_status']} (DIF: {indicators['macd_line']:.4f}, DEA: {indicators['signal_line']:.4f})\n"
        report += f"   • RSI指标：{indicators['rsi_value']:.2f}，{indicators['rsi_status']}区域\n"
        report += f"   • 布林带：上轨{indicators['upper_band']:.4f} | 中轨{indicators['middle_band']:.4f} | 下轨{indicators['lower_band']:.4f}\n"
        report += f"   • 量比：{indicators['volume_ratio']:.2f}，换手率：{indicators['turnover_rate']:.2f}%\n\n"
        
        # 2. 价格位置分析
        report += "2. 价格位置分析\n"
        report += f"   • 当前价格：{indicators['current_price']:.4f}\n"
        report += f"   • 5日均线：{indicators['ma5']:.4f} (偏离率：{indicators['deviation_ma5']:.2f}%)\n"
        report += f"   • 20日均线：{indicators['ma20']:.4f} (偏离率：{indicators['deviation_ma20']:.2f}%)\n"
        report += f"   • 250日均线：{indicators['ma250']:.4f} (偏离率：{indicators['deviation_ma250']:.2f}%)\n"
        report += f"   • 布林带位置：{indicators['bollinger_status']}\n\n"
        
        # 3. 资金流向与市场情绪
        report += "3. 资金流向与市场情绪\n"
        report += f"   • 过甡去5日平均成交量：{indicators['last_5_volume']:.0f}\n"
        report += f"   • 流通市值：{indicators['market_cap']:.2f}亿\n\n"
        
        # 4. 操作建议
        report += "4. 操作建议\n"
        
        # 根据指标生成具体建议
        current_price = indicators["current_price"]
        
        # 4.1 趋势判断
        if indicators["ma_trend"] == "多头排列":
            report += "   • 趋势判断：处于多头排列，中长期趋势向好\n"
        elif indicators["ma_trend"] == "空头排列":
            report += "   • 趋势判断：处于空头排列，中长期趋势偏弱\n"
        else:
            report += "   • 趋势判断：处于震荡趋势，方向不明\n"
        
        # 4.2 做T关键价格
        # 支撑位
        support1 = indicators["ma20"] * 0.98  # 20日均线下方2%
        support2 = indicators["lower_band"]  # 布林带下轨
        
        # 阻力位
        resistance1 = indicators["ma20"] * 1.02  # 20日均线上方2%
        resistance2 = indicators["upper_band"]  # 布林带上轨
        
        # 中轴线
        middle_line = (support1 + resistance1) / 2
        
        report += f"   • 中轴线：{middle_line:.4f}\n"
        report += f"   • 支撑区间：{min(support1, support2):.4f} - {max(support1, support2):.4f}\n"
        report += f"   • 阻力区间：{min(resistance1, resistance2):.4f} - {max(resistance1, resistance2):.4f}\n"
        
        # 4.3 具体操作建议
        if indicators["ma_trend"] == "多头排列" and indicators["rsi_status"] == "中性":
            report += "   • 操作建议：可适当持仓，回调至支撑位附近可加仓\n"
            report += f"     - 建仓点：{support1:.4f}附近\n"
            report += f"     - 止损点：{support1 * 0.98:.4f}（跌破支撑位2%）\n"
            report += f"     - 目标价：{resistance1:.4f}（阻力位）\n"
        
        elif indicators["ma_trend"] == "空头排列" and indicators["rsi_status"] == "中性":
            report += "   • 操作建议：谨慎操作，反弹至阻力位附近可减仓\n"
            report += f"     - 减仓点：{resistance1:.4f}附近\n"
            report += f"     - 止损点：{resistance1 * 1.02:.4f}（突破阻力位2%）\n"
            report += f"     - 目标价：{support1:.4f}（支撑位）\n"
        
        elif indicators["rsi_status"] == "超买":
            report += "   • 操作建议：短期超买，注意回调风险\n"
            report += f"     - 减仓点：{current_price:.4f}附近\n"
            report += f"     - 止损点：{resistance1:.4f}（突破阻力位）\n"
            report += f"     - 目标价：{middle_line:.4f}（中轴线）\n"
        
        elif indicators["rsi_status"] == "超卖":
            report += "   • 操作建议：短期超卖，可考虑低吸\n"
            report += f"     - 建仓点：{current_price:.4f}附近\n"
            report += f"     - 止损点：{support1:.4f}（跌破支撑位）\n"
            report += f"     - 目标价：{middle_line:.4f}（中轴线）\n"
        
        else:
            report += "   • 操作建议：市场处于震荡，可做区间操作\n"
            report += f"     - 下沿操作（价格≈{support1:.4f}）：小幅加仓10%-20%\n"
            report += f"     - 上沿操作（价格≈{resistance1:.4f}）：小幅减仓10%-20%\n"
            report += "     - 总仓位严格控制在≤50%\n"
        
        # 5. 风险提示
        report += "\n5. 风险提示\n"
        
        if indicators["volume_ratio"] > 2.0:
            report += "   • 量比过高，注意短期波动风险\n"
        
        if indicators["rsi_value"] > 75:
            report += "   • RSI严重超买，警惕回调风险\n"
        
        if indicators["rsi_value"] < 25:
            report += "   • RSI严重超卖，注意反弹机会\n"
        
        if indicators["deviation_ma20"] > 15.0:
            report += "   • 价格大幅偏离20日均线，警惕均值回归\n"
        
        if indicators["deviation_ma20"] < -15.0:
            report += "   • 价格大幅低于20日均线，注意反弹机会\n"
        
        # 6. 更新时间与版本
        report += f"\n⏰ 更新时间: {beijing_time.strftime('%Y-%m-%d %H:%M')}\n"
        report += "📊 策略版本: 股票技术分析策略 v3.0.0\n"
        
        return report
    
    except Exception as e:
        logger.error(f"生成分析报告失败: {str(e)}", exc_info=True)
        return "【股票技术分析】生成报告时发生错误，请检查日志"

def analyze_stock_strategy(stock_code: str) -> Dict[str, Any]:
    """
    分析股票策略
    
    Args:
        stock_code: 股票代码
    
    Returns:
        Dict[str, Any]: 分析结果
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始分析股票 {stock_code} (UTC: {utc_now}, CST: {beijing_now})")
        
        # 1. 确保有股票数据
        if not ensure_stock_data(stock_code):
            error_msg = f"无法获取股票 {stock_code} 的日线数据"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "timestamp": beijing_now.isoformat()
            }
        
        # 2. 加载股票数据
        df = load_stock_daily_data(stock_code)
        if df.empty:
            error_msg = f"加载股票 {stock_code} 日线数据后为空"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "timestamp": beijing_now.isoformat()
            }
        
        # 3. 获取股票名称
        stock_name = get_stock_name(stock_code)
        
        # 4. 计算技术指标
        indicators = calculate_technical_indicators(df)
        if not indicators:
            error_msg = f"计算股票 {stock_code} 技术指标失败"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "timestamp": beijing_now.isoformat()
            }
        
        # 5. 生成分析报告
        report = generate_analysis_report(stock_code, stock_name, indicators)
        
        # 6. 推送到微信
        send_wechat_message(message=report, message_type="stock_analysis")
        
        # 7. 返回结果
        return {
            "status": "success",
            "message": "股票技术分析完成",
            "stock_code": stock_code,
            "stock_name": stock_name,
            "analysis_time_utc": utc_now.strftime("%Y-%m-%d %H:%M"),
            "analysis_time_beijing": beijing_now.strftime("%Y-%m-%d %H:%M")
        }
    
    except Exception as e:
        error_msg = f"分析股票 {stock_code} 失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 尝试发送错误消息
        try:
            send_wechat_message(
                message=f"股票技术分析失败: {str(e)}",
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        
        # 返回错误响应
        return {
            "status": "error",
            "message": error_msg,
            "stock_code": stock_code,
            "timestamp": get_beijing_time().isoformat()
        }

def calculate_macd(df: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    计算MACD指标
    
    Args:
        df: 股票日线数据
        fast_period: 快速线周期
        slow_period: 慢速线周期
        signal_period: 信号线周期
    
    Returns:
        Tuple[pd.Series, pd.Series, pd.Series]: MACD线, 信号线, MACD柱
    """
    # 计算快线EMA
    fast_ema = df['收盘'].ewm(span=fast_period, adjust=False).mean()
    
    # 计算慢线EMA
    slow_ema = df['收盘'].ewm(span=slow_period, adjust=False).mean()
    
    # 计算MACD线
    macd_line = fast_ema - slow_ema
    
    # 计算信号线
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    
    # 计算MACD柱
    macd_hist = macd_line - signal_line
    
    return macd_line, signal_line, macd_hist

def calculate_rsi(df: pd.DataFrame, period: int = 14) -> float:
    """
    计算RSI指标
    
    Args:
        df: 股票日线数据
        period: 计算周期
    
    Returns:
        float: RSI值
    """
    # 计算价格变化
    delta = df['收盘'].diff()
    
    # 分离上涨和下跌
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    # 计算平均涨跌幅
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    # 避免除以零错误
    avg_loss = avg_loss.replace(0, 0.0001)
    
    # 计算相对强度
    rs = avg_gain / avg_loss
    
    # 计算RSI
    rsi = 100 - (100 / (1 + rs))
    
    # 返回最新RSI值
    return rsi.iloc[-1]

def calculate_bollinger_bands(df: pd.DataFrame, window: int = 20, num_std: float = 2) -> Tuple[float, float, float]:
    """
    计算布林带
    
    Args:
        df: 股票日线数据
        window: 窗口大小
        num_std: 标准差倍数
    
    Returns:
        Tuple[float, float, float]: 上轨, 中轨, 下轨
    """
    # 计算中轨（移动平均线）
    middle_band = df['收盘'].rolling(window=window).mean().iloc[-1]
    
    # 计算标准差
    std = df['收盘'].rolling(window=window).std().iloc[-1]
    
    # 计算上下轨
    upper_band = middle_band + (std * num_std)
    lower_band = middle_band - (std * num_std)
    
    return upper_band, middle_band, lower_band

def get_stock_name(stock_code: str) -> str:
    """
    获取股票名称
    
    Args:
        stock_code: 股票代码
    
    Returns:
        str: 股票名称
    """
    try:
        # 尝试从stock_list.csv获取股票名称
        stock_list_path = os.path.join(Config.DATA_DIR, "stock_list.csv")
        if os.path.exists(stock_list_path):
            stock_list = pd.read_csv(stock_list_path, encoding="utf-8")
            if "代码" in stock_list.columns and "名称" in stock_list.columns:
                stock_info = stock_list[stock_list["代码"] == stock_code]
                if not stock_info.empty:
                    return stock_info["名称"].values[0]
        
        # 如果没有找到，尝试使用akshare获取
        try:
            import akshare as ak
            stock_info = ak.stock_info_a_code_name()
            if not stock_info.empty and "code" in stock_info.columns and "name" in stock_info.columns:
                stock_info = stock_info[stock_info["code"] == stock_code]
                if not stock_info.empty:
                    return stock_info["name"].values[0]
        except ImportError:
            logger.warning("akshare 模块未安装，无法获取股票名称")
        
        # 如果还是找不到，返回默认值
        return stock_code
    
    except Exception as e:
        logger.warning(f"获取股票名称失败: {str(e)}，使用默认值")
        return stock_code

if __name__ == "__main__":
    # 从环境变量获取股票代码
    import os
    stock_code = os.getenv("INPUT_STOCK_CODE", "000001.SZ")
    
    logger.info(f"===== 开始执行股票技术分析策略 (股票代码: {stock_code}) =====")
    logger.info(f"UTC时间：{get_utc_time().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"北京时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 执行分析
    result = analyze_stock_strategy(stock_code)
    
    # 记录任务完成
    logger.info(f"===== 任务执行结束：{result['status']} =====")
    
    # 输出JSON格式的结果
    import json
    print(json.dumps(result, indent=2, ensure_ascii=False))
