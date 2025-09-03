#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微信推送模块
提供企业微信消息推送功能，支持文本和Markdown格式
特别优化了时区处理，确保所有时间显示为北京时间
"""

import os
import requests
import time
import logging
import json
import pandas as pd
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time
)

# 初始化日志
logger = logging.getLogger(__name__)

# 消息发送频率控制
_last_send_time = 0
_MIN_SEND_INTERVAL = 1.0  # 最小发送间隔(秒)，避免消息过密被封
_MAX_MESSAGE_LENGTH = 2000  # 企业微信消息最大长度(字符)
_MESSAGE_CHUNK_SIZE = 1500  # 消息分块大小(字符)

# 发送失败重试配置
_MAX_RETRIES = 3
_RETRY_DELAYS = [1, 2, 3]  # 重试延迟(秒) - 修改为更短的重试间隔
_REQUEST_TIMEOUT = 5  # 新增：请求超时时间(秒)

# 错误消息缓存，用于避免重复发送相同错误
_error_cache: Dict[str, float] = {}
# 错误消息冷却时间（秒）
ERROR_COOLDOWN = 30
# 消息发送间隔（秒）
MESSAGE_SEND_INTERVAL = 1.5

def _extract_error_type(error_message: str) -> str:
    """
    从错误消息中提取错误类型
    
    Args:
        error_message: 完整的错误消息
    
    Returns:
        str: 错误类型标识
    """
    # 提取Traceback中的关键错误信息
    if "Traceback" in error_message:
        # 尝试提取最后一行错误
        lines = error_message.split("\n")
        for line in reversed(lines):
            if "Error" in line or "Exception" in line or "KeyError" in line:
                return line.strip()
    
    # 提取"KeyError: 'xxx'"格式
    if "KeyError" in error_message:
        import re
        match = re.search(r"KeyError: '([^']+)'", error_message)
        if match:
            return f"KeyError: '{match.group(1)}'"
    
    # 返回前50个字符作为错误类型
    return error_message[:50]

def _should_send_error(error_type: str) -> bool:
    """
    检查是否应该发送错误消息
    
    Args:
        error_type: 错误类型标识
    
    Returns:
        bool: 是否应该发送
    """
    current_time = time.time()
    
    # 检查是否在冷却期内
    if error_type in _error_cache:
        if current_time - _error_cache[error_type] < ERROR_COOLDOWN:
            return False
    
    # 更新最后发送时间
    _error_cache[error_type] = current_time
    return True

def _should_send_message(message_tag: str) -> bool:
    """
    检查是否应该发送消息（避免相同类型消息过于频繁）
    
    Args:
        message_tag: 消息类型标识
    
    Returns:
        bool: 是否应该发送
    """
    current_time = time.time()
    
    # 检查是否在冷却期内
    if message_tag in _error_cache:
        if current_time - _error_cache[message_tag] < ERROR_COOLDOWN:
            return False
    
    return True

def _update_last_send_time(message_tag: str) -> None:
    """
    更新消息最后发送时间
    
    Args:
        message_tag: 消息类型标识
    """
    _error_cache[message_tag] = time.time()

def _get_message_tag(message: str) -> str:
    """
    获取消息类型标识
    
    Args:
        message: 消息内容
    
    Returns:
        str: 消息类型标识
    """
    # 提取关键标识
    if "KeyError" in message:
        import re
        match = re.search(r"KeyError: '([^']+)'", message)
        if match:
            return f"KeyError_{match.group(1)}"
    
    if "SettingWithCopyWarning" in message:
        return "SettingWithCopyWarning"
    
    if "api freq out of limit" in message:
        return "API_Freq_Limit"
    
    # 按消息前缀分类
    if message.startswith("【系统错误】"):
        return "System_Error"
    if message.startswith("【ETF策略日报】"):
        return "Daily_Report"
    if message.startswith("【ETF策略】"):
        return "Strategy_Message"
    
    # 默认使用消息的哈希值前10位
    import hashlib
    return hashlib.md5(message[:100].encode()).hexdigest()[:10]

def _check_message_length(message: str) -> List[str]:
    """
    检查消息长度并进行分片处理
    :param message: 原始消息
    :return: 分片后的消息列表
    """
    if not message or len(message) <= _MAX_MESSAGE_LENGTH:
        return [message]
    
    logger.warning(f"消息过长({len(message)}字符)，进行分片处理")
    
    # 按段落分割消息
    paragraphs = message.split('\n\n')
    chunks = []
    current_chunk = ""
    
    for paragraph in paragraphs:
        # 如果当前块加上新段落不会超过限制
        if len(current_chunk) + len(paragraph) + 2 <= _MESSAGE_CHUNK_SIZE:
            if current_chunk:
                current_chunk += "\n\n" + paragraph
            else:
                current_chunk = paragraph
        else:
            # 如果当前块有内容，先保存
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = ""
            
            # 如果段落本身就很长，需要进一步分割
            if len(paragraph) > _MESSAGE_CHUNK_SIZE:
                # 按句子分割
                sentences = paragraph.split('\n')
                for sentence in sentences:
                    if len(current_chunk) + len(sentence) + 1 <= _MESSAGE_CHUNK_SIZE:
                        if current_chunk:
                            current_chunk += "\n" + sentence
                        else:
                            current_chunk = sentence
                    else:
                        if current_chunk:
                            chunks.append(current_chunk)
                            current_chunk = sentence
                        else:
                            # 单句就超过限制，强制分割
                            chunks.append(sentence[:_MESSAGE_CHUNK_SIZE])
                            current_chunk = sentence[_MESSAGE_CHUNK_SIZE:]
            else:
                current_chunk = paragraph
    
    # 添加分片标记
    if len(chunks) > 1:
        for i, chunk in enumerate(chunks):
            chunks[i] = f"【消息分片 {i+1}/{len(chunks)}】\n\n{chunk}"
    
    logger.info(f"消息已分割为 {len(chunks)} 个分片")
    return chunks

def _rate_limit() -> None:
    """
    速率限制，避免消息发送过于频繁
    """
    global _last_send_time
    current_time = time.time()
    elapsed = current_time - _last_send_time
    
    if elapsed < _MIN_SEND_INTERVAL:
        sleep_time = _MIN_SEND_INTERVAL - elapsed
        logger.debug(f"速率限制，等待 {sleep_time:.2f} 秒")
        time.sleep(sleep_time)
    
    _last_send_time = time.time()

def _send_single_message(webhook: str, message: str, retry_count: int = 0) -> bool:
    """
    发送单条消息到企业微信
    :param webhook: 企业微信Webhook地址
    :param message: 消息内容
    :param retry_count: 当前重试次数
    :return: 是否发送成功
    """
    try:
        # 速率限制
        _rate_limit()
        
        # 控制消息发送速率
        current_time = time.time()
        elapsed = current_time - _last_send_time
        if elapsed < MESSAGE_SEND_INTERVAL:
            sleep_time = MESSAGE_SEND_INTERVAL - elapsed
            logger.debug(f"消息发送速率控制，等待 {sleep_time:.2f} 秒")
            time.sleep(sleep_time)
        _last_send_time = time.time()
        
        # 企业微信文本消息格式
        payload = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        
        logger.debug(f"发送消息到企业微信 (重试 {retry_count}): {message[:100]}...")
        response = requests.post(webhook, json=payload, timeout=_REQUEST_TIMEOUT)  # 修改为使用_REQUEST_TIMEOUT
        response.raise_for_status()
        result = response.json()
        
        if result.get("errcode") == 0:
            logger.info("微信消息发送成功")
            return True
        else:
            error_msg = result.get('errmsg', '未知错误')
            logger.error(f"微信消息发送失败: {error_msg}")
            return False
            
    except requests.exceptions.Timeout:
        logger.error(f"微信消息发送超时 (重试 {retry_count})")
        return False
    except requests.exceptions.ConnectionError:
        logger.error(f"网络连接错误 (重试 {retry_count})")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"请求异常: {str(e)} (重试 {retry_count})")
        return False
    except Exception as e:
        logger.error(f"发送消息时发生未预期错误: {str(e)} (重试 {retry_count})", exc_info=True)
        return False

def _format_discount_message(df: pd.DataFrame) -> str:
    """
    格式化折价机会消息
    
    Args:
        df: 折价机会DataFrame
    
    Returns:
        str: 格式化后的消息
    """
    try:
        if df.empty:
            return "【折价机会】\n未发现有效折价套利机会"
        
        # 生成消息内容
        content = "【以下ETF市场价格低于净值，可以考虑买入】\n\n"
        content += "💡 说明：当ETF市场价格低于IOPV（基金份额参考净值）时，表明ETF折价交易\n"
        content += f"📊 筛选条件：基金规模≥{Config.GLOBAL_MIN_FUND_SIZE}亿元，日均成交额≥{Config.GLOBAL_MIN_AVG_VOLUME}万元\n"
        content += f"💰 交易成本：{Config.TRADE_COST_RATE*100:.2f}%（含印花税和佣金）\n"
        content += f"🎯 折价阈值：折价率超过{Config.DISCOUNT_THRESHOLD*100:.2f}%\n"
        content += f"⭐ 综合评分：≥{Config.ARBITRAGE_SCORE_THRESHOLD:.1f}\n\n"
        
        # 添加折价机会
        for i, (_, row) in enumerate(df.head(3).iterrows(), 1):
            content += f"{i}. {row['ETF名称']} ({row['ETF代码']})\n"
            content += f"   💹 折价率: {abs(row['折溢价率']):.2f}%\n"
            content += f"   📈 市场价格: {row['市场价格']:.3f}元\n"
            content += f"   📊 IOPV: {row['IOPV']:.3f}元\n"
            content += f"   🏦 基金规模: {row['规模']:.2f}亿元\n"
            content += f"   💰 日均成交额: {row['日均成交额']:.2f}万元\n"
            content += f"   ⭐ 综合评分: {row['综合评分']:.1f}\n\n"
        
        # 添加其他机会数量
        if len(df) > 3:
            content += f"• 还有 {len(df) - 3} 个折价机会...\n"
        
        # 添加风险提示
        content += (
            "\n⚠️ 风险提示：\n"
            "1. 市场价格低于净值是短期现象，不一定能立即获利\n"
            "2. 实际交易中可能因价格变动导致机会消失\n"
            "3. 一级市场套利需要大额资金和特殊权限，散户无法直接操作\n"
            "4. 本策略综合评分考虑了折溢价率、流动性、波动率、成分股稳定性等因素\n"
            "5. 请结合市场整体情况谨慎决策，避免因成分股问题导致的假性套利机会\n"
            "6. 本策略仅供参考，不构成投资建议\n"
        )
        
        return content
    
    except Exception as e:
        error_msg = f"生成折价消息内容失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return f"【折价策略】生成消息内容时发生错误，请检查日志"

def _format_premium_message(df: pd.DataFrame) -> str:
    """
    格式化溢价机会消息
    
    Args:
        df: 溢价机会DataFrame
    
    Returns:
        str: 格式化后的消息
    """
    try:
        if df.empty:
            return "【溢价机会】\n未发现有效溢价套利机会"
        
        # 生成消息内容
        content = "【以下ETF市场价格高于净值，若你只在二级市场交易注意规避风险】\n\n"
        content += "💡 说明：当ETF市场价格高于IOPV（基金份额参考净值）时，表明ETF溢价交易\n"
        content += f"📊 筛选条件：基金规模≥{Config.GLOBAL_MIN_FUND_SIZE}亿元，日均成交额≥{Config.GLOBAL_MIN_AVG_VOLUME}万元\n"
        content += f"💰 交易成本：{Config.TRADE_COST_RATE*100:.2f}%（含印花税和佣金）\n"
        content += f"🎯 溢价阈值：溢价率超过{Config.PREMIUM_THRESHOLD*100:.2f}%\n"
        content += f"⭐ 综合评分：≥{Config.ARBITRAGE_SCORE_THRESHOLD:.1f}\n\n"
        
        # 添加溢价机会
        for i, (_, row) in enumerate(df.head(3).iterrows(), 1):
            content += f"{i}. {row['ETF名称']} ({row['ETF代码']})\n"
            content += f"   💹 溢价率: {row['折溢价率']:.2f}%\n"
            content += f"   📈 市场价格: {row['市场价格']:.3f}元\n"
            content += f"   📊 IOPV: {row['IOPV']:.3f}元\n"
            content += f"   🏦 基金规模: {row['规模']:.2f}亿元\n"
            content += f"   💰 日均成交额: {row['日均成交额']:.2f}万元\n"
            content += f"   ⭐ 综合评分: {row['综合评分']:.1f}\n\n"
        
        # 添加其他机会数量
        if len(df) > 3:
            content += f"• 还有 {len(df) - 3} 个溢价机会...\n"
        
        # 添加风险提示
        content += (
            "\n⚠️ 风险提示：\n"
            "1. 市场价格高于净值是短期现象，不一定能立即获利\n"
            "2. 实际交易中可能因价格变动导致机会消失\n"
            "3. 二级市场交易者应避免在溢价过高时买入，可能导致亏损\n"
            "4. 本策略综合评分考虑了溢价率、流动性、波动率、成分股稳定性等因素\n"
            "5. 请结合市场整体情况谨慎决策，避免因成分股问题导致的假性套利机会\n"
            "6. 本策略仅供参考，不构成投资建议\n"
        )
        
        return content
    
    except Exception as e:
        error_msg = f"生成溢价消息内容失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return f"【溢价策略】生成消息内容时发生错误，请检查日志"

def _apply_message_template(message: Union[str, pd.DataFrame], message_type: str) -> str:
    """
    应用对应类型的消息模板
    :param message: 原始消息内容（可以是字符串或DataFrame）
    :param message_type: 消息类型
    :return: 格式化后的消息
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 生成GitHub日志链接
        github_run_id = os.getenv("GITHUB_RUN_ID", "unknown")
        github_repository = os.getenv("GITHUB_REPOSITORY", "karmyshunde-sudo/fish-etf")
        log_url = f"https://github.com/{github_repository}/actions/runs/{github_run_id}" if github_run_id != "unknown" else "无法获取日志链接"
        
        # 特殊处理套利消息
        if message_type == "discount" and isinstance(message, pd.DataFrame):
            message = _format_discount_message(message)
        elif message_type == "premium" and isinstance(message, pd.DataFrame):
            message = _format_premium_message(message)
        
        # 确保message是字符串
        if not isinstance(message, str):
            message = str(message)
        
        # 根据消息类型应用不同的模板
        if message_type == "task":
            return (
                f"{message}\n\n"
                "──────────────────\n"
                f"🕒 UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🕒 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                "──────────────────\n"
                f"🔗 日志链接: {log_url}\n"
                "📊 数据来源：AkShare | 环境：生产"
            )
        elif message_type == "discount":
            return (
                f"{message}\n\n"
                "──────────────────\n"
                f"🕒 UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🕒 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                "──────────────────\n"
                f"🔗 数据来源: {log_url}\n"
                "📊 环境：生产"
            )
        elif message_type == "premium":
            return (
                f"{message}\n\n"
                "──────────────────\n"
                f"🕒 UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🕒 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                "──────────────────\n"
                f"🔗 数据来源: {log_url}\n"
                "📊 环境：生产"
            )
        elif message_type == "position":
            return (
                f"{message}\n\n"
                "──────────────────\n"
                f"🕒 UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🕒 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                "──────────────────\n"
                f"🔗 数据来源: {log_url}\n"
                "📊 环境：生产"
            )
        elif message_type == "error":
            return (
                f"⚠️ {message}\n\n"
                "──────────────────\n"
                f"🕒 UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🕒 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                "──────────────────\n"
                f"🔗 日志链接: {log_url}\n"
                "📊 数据来源：AkShare | 环境：生产"
            )
        elif message_type == "daily_report":
            return (
                f"{message}\n\n"
                "──────────────────\n"
                f"🕒 UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🕒 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                "──────────────────\n"
                f"🔗 数据来源: {log_url}\n"
                "📊 环境：生产"
            )
        else:  # default
            return (
                f"{message}\n\n"
                "──────────────────\n"
                f"🕒 UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🕒 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
                "──────────────────\n"
                "📊 数据来源：AkShare | 环境：生产"
            )
    except Exception as e:
        logger.error(f"应用消息模板失败: {str(e)}", exc_info=True)
        # 返回一个基本格式的消息
        return (
            f"{message}\n\n"
            "──────────────────\n"
            "🕒 时间: 无法获取\n"
            "──────────────────\n"
            "📊 数据来源：AkShare | 环境：生产\n"
            "⚠️ 注意: 消息格式化过程中发生错误"
        )

def send_wechat_message(message: Union[str, pd.DataFrame], 
                       message_type: str = "default",
                       webhook: Optional[str] = None) -> bool:
    """
    发送消息到企业微信，自动应用消息模板
    
    Args:
        message: 消息内容（纯业务内容，可以是字符串或DataFrame）
        message_type: 消息类型（task, discount, premium, position, error, daily_report等）
        webhook: 企业微信Webhook地址
        
    Returns:
        bool: 是否成功发送
    """
    try:
        # 检查是否为空消息
        if message is None:
            logger.warning("尝试发送空消息，已忽略")
            return False
        
        # 类型安全转换：确保message是字符串
        if isinstance(message, pd.DataFrame):
            # 检查DataFrame是否为空
            if message.empty:
                logger.warning("尝试发送空DataFrame，已忽略")
                return False
            # 转换为字符串（使用更友好的格式）
            message = _format_dataframe_as_string(message)
        elif not isinstance(message, str):
            message = str(message)
        
        # 检查是否为空字符串
        if not message.strip():
            logger.warning("尝试发送空字符串消息，已忽略")
            return False
        
        # 特殊处理错误消息，避免频繁发送
        if message_type == "error":
            # 提取错误类型（例如"KeyError: 'fundamental'"）
            error_type = _extract_error_type(message)
            
            # 检查是否在冷却期内
            if not _should_send_error(error_type):
                logger.info(f"错误消息在冷却期内，跳过发送: {error_type}")
                return False
        
        # 从环境变量获取Webhook（优先于配置文件）
        if webhook is None:
            webhook = os.getenv("WECOM_WEBHOOK", Config.WECOM_WEBHOOK)
        if not webhook:
            logger.error("企业微信Webhook未配置，无法发送消息")
            return False
            
        # 应用消息模板
        full_message = _apply_message_template(message, message_type)
        
        # 检查消息长度并进行分片
        message_chunks = _check_message_length(full_message)
        
        # 发送所有消息分片
        all_success = True
        for i, chunk in enumerate(message_chunks):
            # 对于分片消息，添加分片标识
            if len(message_chunks) > 1:
                logger.info(f"发送消息分片 {i+1}/{len(message_chunks)}")
                
            # 重试机制
            success = False
            for retry in range(_MAX_RETRIES):
                if _send_single_message(webhook, chunk, retry):
                    success = True
                    break
                else:
                    if retry < _MAX_RETRIES - 1:
                        delay = _RETRY_DELAYS[retry]
                        logger.warning(f"发送失败，{delay}秒后重试 ({retry+1}/{_MAX_RETRIES})")
                        time.sleep(delay)
                        
            if not success:
                logger.error(f"消息分片 {i+1} 发送失败，已达最大重试次数")
                all_success = False
                
        return all_success
        
    except Exception as e:
        logger.error(f"发送微信消息时发生未预期错误: {str(e)}", exc_info=True)
        return False

def send_wechat_markdown(message: str, 
                        message_type: str = "default",
                        webhook: Optional[str] = None) -> bool:
    """
    发送Markdown格式消息到企业微信
    
    Args:
        message: Markdown格式消息
        message_type: 消息类型
        webhook: 企业微信Webhook地址
        
    Returns:
        bool: 是否成功发送
    """
    try:
        # 从环境变量获取Webhook（优先于配置文件）
        if webhook is None:
            webhook = os.getenv("WECOM_WEBHOOK", Config.WECOM_WEBHOOK)
        
        if not webhook:
            logger.error("企业微信Webhook未配置，无法发送消息")
            return False
        
        # 速率限制
        _rate_limit()
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 生成GitHub日志链接
        github_run_id = os.getenv("GITHUB_RUN_ID", "unknown")
        github_repository = os.getenv("GITHUB_REPOSITORY", "karmyshunde-sudo/fish-etf")
        log_url = f"https://github.com/{github_repository}/actions/runs/{github_run_id}" if github_run_id != "unknown" else "无法获取日志链接"
        
        # 添加统一的页脚
        footer = (
            "\n\n"
            "──────────────────\n"
            f"🕒 **UTC时间**: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"🕒 **北京时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "──────────────────\n"
            f"🔗 **数据来源**: [GitHub Actions]({log_url})\n"
            "📊 **环境**: 生产"
        )
        
        # 完整消息
        full_message = message + footer
        
        # 企业微信Markdown消息格式
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": full_message
            }
        }
        
        logger.debug(f"发送Markdown消息到企业微信: {message[:100]}...")
        response = requests.post(webhook, json=payload, timeout=_REQUEST_TIMEOUT)  # 修改为使用_REQUEST_TIMEOUT
        response.raise_for_status()
        result = response.json()
        
        if result.get("errcode") == 0:
            logger.info("微信Markdown消息发送成功")
            return True
        else:
            error_msg = result.get('errmsg', '未知错误')
            logger.error(f"微信Markdown消息发送失败: {error_msg}")
            return False
            
    except Exception as e:
        logger.error(f"发送微信Markdown消息时发生错误: {str(e)}", exc_info=True)
        return False

def send_task_completion_notification(task: str, result: Dict[str, Any]):
    """
    发送任务完成通知到企业微信
    
    Args:
        task: 任务名称
        result: 任务执行结果
    """
    try:
        if result["status"] == "success":
            status_emoji = "✅"
            status_msg = "成功"
        elif result["status"] == "skipped":
            status_emoji = "⏭️"
            status_msg = "已跳过"
        else:
            status_emoji = "❌"
            status_msg = "失败"
        
        # 构建任务总结消息
        summary_msg = (
            f"【任务执行】{task}\n\n"
            f"{status_emoji} **状态**: {status_msg}\n"
            f"📝 **详情**: {result.get('message', '无详细信息')}\n"
        )
        
        # 添加任务特定信息
        if task == "update_etf_list" and result["status"] == "success":
            # 从消息中提取ETF数量（格式："全市场ETF列表更新完成，共XXX只"）
            count = 0
            message = result.get('message', '')
            if "共" in message and "只" in message:
                try:
                    count = int(message.split("共")[1].split("只")[0])
                except:
                    pass
            summary_msg += f"📊 **ETF数量**: {count}只\n"
            
            # 添加数据来源信息
            source = result.get('source', '未知')
            summary_msg += f"来源: {source}\n"
            
            # 添加列表有效期信息
            try:
                file_path = Config.ALL_ETFS_PATH
                if os.path.exists(file_path):
                    last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
                    expiration = last_modified + timedelta(days=Config.ETF_LIST_UPDATE_INTERVAL)
                    summary_msg += f"📅 **生成时间**: {last_modified.strftime('%Y-%m-%d %H:%M')}\n"
                    summary_msg += f"⏳ **过期时间**: {expiration.strftime('%Y-%m-%d %H:%M')}\n"
            except Exception as e:
                logger.error(f"获取ETF列表文件信息失败: {str(e)}")
                summary_msg += "📅 **列表有效期信息**: 获取失败\n"
        
        elif task == "crawl_etf_daily" and result["status"] == "success":
            summary_msg += "📈 **数据爬取**: 完成\n"
            
        elif task == "calculate_arbitrage" and result["status"] == "success":
            summary_msg += "🔍 **套利机会**: 已推送\n"
            
        elif task == "calculate_position" and result["status"] == "success":
            summary_msg += "💼 **仓位策略**: 已推送\n"
        
        # 发送任务总结通知（使用task类型）
        send_wechat_message(summary_msg, message_type="task")
        logger.info(f"已发送任务完成通知: {task} - {status_msg}")
        
    except Exception as e:
        logger.error(f"发送任务完成通知失败: {str(e)}", exc_info=True)

def test_webhook_connection(webhook: Optional[str] = None) -> bool:
    """
    测试企业微信Webhook连接是否正常
    
    Args:
        webhook: 企业微信Webhook地址
        
    Returns:
        bool: 连接是否正常
    """
    try:
        if webhook is None:
            webhook = os.getenv("WECOM_WEBHOOK", Config.WECOM_WEBHOOK)
        
        if not webhook:
            logger.error("企业微信Webhook未配置")
            return False
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 发送测试消息
        test_message = (
            "✅ **【测试消息】**\n\n"
            "**状态**: 企业微信Webhook连接测试成功\n"
            f"**测试时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}（北京时间）\n\n"
            "──────────────────\n"
            f"🕒 **UTC时间**: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"🕒 **北京时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "──────────────────\n"
            "📊 **数据来源**: AkShare | **环境**: 生产"
        )
        
        logger.info("开始测试Webhook连接")
        success = send_wechat_message(test_message, message_type="default", webhook=webhook)
        
        if success:
            logger.info("Webhook连接测试成功")
        else:
            logger.error("Webhook连接测试失败")
        
        return success
        
    except Exception as e:
        logger.error(f"测试Webhook连接时发生错误: {str(e)}", exc_info=True)
        return False

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    
    # 初始化错误缓存
    _error_cache = {}
    
    # 初始化日志
    logger.info("微信推送模块初始化完成")
    
    # 测试Webhook连接（仅在调试模式下）
    if os.getenv("DEBUG", "").lower() in ("true", "1", "yes"):
        logger.info("调试模式启用，测试Webhook连接")
        test_webhook_connection()
except Exception as e:
    logger.error(f"微信推送模块初始化失败: {str(e)}", exc_info=True)
    
    try:
        # 退回到基础日志配置
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(f"微信推送模块初始化失败: {str(e)}")
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(f"微信推送模块初始化失败: {str(e)}")
