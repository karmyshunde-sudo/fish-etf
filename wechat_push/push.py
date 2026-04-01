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
_last_send_time = 0.0  # 使用浮点数，避免时区问题
_MIN_SEND_INTERVAL = 3.5  # 最小发送间隔(秒)，确保每分钟不超过17条消息
_MAX_MESSAGE_LENGTH = 2000  # 企业微信消息最大长度(字符)
_MESSAGE_CHUNK_SIZE = 1500  # 消息分块大小(字符)

# 发送失败重试配置
_MAX_RETRIES = 3
_RETRY_DELAYS = [2, 4, 8]  # 重试延迟(秒)，指数退避策略
_REQUEST_TIMEOUT = 5.0  # 请求超时时间(秒)

# 错误消息缓存，用于避免重复发送相同错误
_error_message_cache = {}  # 存储错误消息及其上次发送时间
# 错误消息冷却时间（秒）
_ERROR_COOLDOWN = 300  # 5分钟内相同错误只发送一次

def get_github_actions_url() -> str:
    """获取GitHub Actions运行日志链接"""
    github_run_id = os.getenv("GITHUB_RUN_ID", "unknown")
    github_repository = os.getenv("GITHUB_REPOSITORY", "karmyshunde-sudo/fish-etf")
    
    if github_run_id == "unknown" or not github_run_id:
        return "无法获取日志链接"
    
    return f"https://github.com/{github_repository}/actions/runs/{github_run_id}"

def _extract_scalar_value(value, default=0.0, log_prefix=""):
    """
    安全地从各种类型中提取标量值
    
    Args:
        value: 可能是标量、Series、DataFrame、字符串等
        default: 默认值，如果无法提取标量值
        log_prefix: 日志前缀，用于标识调用位置
    
    Returns:
        float: 标量值
    """
    try:
        # 如果已经是标量值，直接返回
        if isinstance(value, (int, float)):
            return float(value)
        
        # 如果是字符串，尝试转换为浮点数
        if isinstance(value, str):
            # 尝试移除非数字字符
            cleaned_str = ''.join(c for c in value if c.isdigit() or c in ['.', '-'])
            if cleaned_str:
                result = float(cleaned_str)
                logger.debug(f"{log_prefix}从字符串提取标量值: '{value}' -> {result}")
                return result
            logger.warning(f"{log_prefix}无法从字符串 '{value}' 提取有效数字，使用默认值{default}")
            return default
        
        # 如果是pandas对象，尝试提取标量值
        if isinstance(value, (pd.Series, pd.DataFrame)):
            # 尝试获取第一个值
            if value.size > 0:
                # 尝试使用.values.flatten()[0]（最可靠）
                try:
                    result = float(value.values.flatten()[0])
                    logger.debug(f"{log_prefix}通过.values.flatten()[0]提取标量值: {result}")
                    return result
                except Exception as e:
                    # 尝试使用.item()
                    try:
                        result = float(value.item())
                        logger.debug(f"{log_prefix}通过.item()提取标量值: {result}")
                        return result
                    except Exception as e2:
                        # 尝试使用.iloc[0]
                        try:
                            valid_values = value[~pd.isna(value)]
                            if not valid_values.empty:
                                result = float(valid_values.iloc[0])
                                logger.debug(f"{log_prefix}通过.iloc[0]提取标量值: {result}")
                                return result
                        except Exception as e3:
                            pass
            
            logger.error(f"{log_prefix}无法从pandas对象提取标量值(size={value.size})，使用默认值{default}")
            return default
        
        # 尝试直接转换为浮点数
        result = float(value)
        logger.debug(f"{log_prefix}直接转换为浮点数: {result}")
        return result
    
    except Exception as e:
        logger.error(f"{log_prefix}无法从类型 {type(value)} 中提取标量值: {str(e)}，使用默认值{default}")
        return default

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
    if error_type in _error_message_cache:
        last_sent = _error_message_cache[error_type]
        # 如果在冷却期内，视为重复消息
        if current_time - last_sent < _ERROR_COOLDOWN:
            return False
    
    # 更新缓存
    _error_message_cache[error_type] = current_time
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
    if message_tag in _error_message_cache:
        last_sent = _error_message_cache[message_tag]
        if current_time - last_sent < _ERROR_COOLDOWN:
            return False
    
    return True

def _update_last_send_time(message_tag: str) -> None:
    """
    更新消息最后发送时间
    
    Args:
        message_tag: 消息类型标识
    """
    _error_message_cache[message_tag] = time.time()

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
    
    # 添加最后一个块
    if current_chunk:
        chunks.append(current_chunk)
    
    # 添加分片标记
    if len(chunks) > 1:
        for i, chunk in enumerate(chunks):
            chunks[i] = f"【消息分片 {i+1}/{len(chunks)}】\n\n{chunk}"
    
    logger.info(f"消息已分割为 {len(chunks)} 个分片")
    return chunks

def _rate_limit() -> None:
    """
    速率限制，避免消息发送过于频繁
    严格遵守企业微信API调用频率限制
    """
    global _last_send_time
    current_time = time.time()
    elapsed = current_time - _last_send_time
    
    # 确保至少间隔_MIN_SEND_INTERVAL秒
    if elapsed < _MIN_SEND_INTERVAL:
        sleep_time = _MIN_SEND_INTERVAL - elapsed
        logger.debug(f"速率限制：等待 {sleep_time:.2f} 秒以遵守API调用频率限制")
        time.sleep(sleep_time)
    
    # 更新最后发送时间
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
        
        # 企业微信文本消息格式
        payload = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        
        logger.debug(f"发送消息到企业微信 (重试 {retry_count}): {message[:100]}...")
        response = requests.post(webhook, json=payload, timeout=_REQUEST_TIMEOUT)
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

def _get_recommendation(score: float) -> str:
    """
    根据评分获取推荐级别
    
    Args:
        score: ETF评分（0-100分）
    
    Returns:
        str: 推荐级别
    """
    try:
        score = float(score)
        if score >= 80:
            return "强烈推荐买入"
        elif score >= 60:
            return "推荐买入"
        elif score >= 40:
            return "可以考虑买入"
        elif score >= 20:
            return "观望"
        else:
            return "不建议买入"
    except Exception as e:
        logger.error(f"获取推荐级别失败: {str(e)}，使用默认值'不建议买入'")
        return "不建议买入"

def _format_discount_message(df: pd.DataFrame) -> List[str]:
    """
    格式化折价机会消息，分页处理
    
    Args:
        df: 折价机会DataFrame
    
    Returns:
        List[str]: 分页后的消息列表
    """
    try:
        # 兼容两种列名：如果只有"折价率"列，则添加"折溢价率"列
        if "折价率" in df.columns and "折溢价率" not in df.columns:
            df = df.copy()
            df["折溢价率"] = df["折价率"]
        
        # 按折价率降序排序（最高折价优先）
        df = df.sort_values(by='折溢价率', ascending=True).reset_index(drop=True)
        
        if df.empty:
            return ["【折价机会】\n未发现有效折价套利机会"]
        
        # 每页显示的ETF数量
        ETFS_PER_PAGE = 5
        total_etfs = len(df)
        total_pages = (total_etfs + ETFS_PER_PAGE - 1) // ETFS_PER_PAGE  # 向上取整
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 生成GitHub日志链接
        log_url = get_github_actions_url()
        
        messages = []
        
        # 第1页：封面页
        if total_pages > 0:
            page1 = (
                "【以下ETF市场价格低于净值，可以考虑买入】\n\n"
                f"💓共{total_etfs}只ETF，分{total_pages}条消息推送，这是第1/{total_pages}条消息\n\n"
                "💡 说明：当ETF市场价格低于IOPV（基金份额参考净值）时，表明ETF折价交易\n"
                f"📊 筛选条件：基金规模≥{Config.GLOBAL_MIN_FUND_SIZE}亿元，日均成交额≥{Config.GLOBAL_MIN_AVG_VOLUME}万元\n"
                f"💰 交易成本：{Config.TRADE_COST_RATE*100:.2f}%（含印花税和佣金）\n"
                f"🎯 折价阈值：折价率超过{Config.DISCOUNT_THRESHOLD*100:.2f}%\n"
                f"⭐ 综合评分：≥{Config.ARBITRAGE_SCORE_THRESHOLD:.1f}"
            )
            messages.append(page1)
        
        # 后续页：ETF详情（每页5只ETF）
        for page in range(total_pages):
            start_idx = page * ETFS_PER_PAGE
            end_idx = min(start_idx + ETFS_PER_PAGE, total_etfs)
            
            # 生成当前页的ETF详情
            content = f"【现价比净值低，买入。 {page+1}/{total_pages}】\n"
            
            for i, (_, row) in enumerate(df.iloc[start_idx:end_idx].iterrows(), start_idx + 1):
                etf_code = str(row.get('ETF代码', '未知'))
                etf_name = str(row.get('ETF名称', '未知'))
                
                premium_discount = _extract_scalar_value(row.get('折溢价率', 0.0), log_prefix=f"ETF {etf_code} 折溢价率: ")
                market_price = _extract_scalar_value(row.get('市场价格', 0.0), log_prefix=f"ETF {etf_code} 市场价格: ")
                iopv = _extract_scalar_value(row.get('IOPV', 0.0), log_prefix=f"ETF {etf_code} IOPV: ")
                fund_size = _extract_scalar_value(row.get('规模', 0.0), log_prefix=f"ETF {etf_code} 规模: ")
                avg_volume = _extract_scalar_value(row.get('日均成交额', 0.0), log_prefix=f"ETF {etf_code} 日均成交额: ")
                score = _extract_scalar_value(row.get('综合评分', 0.0), log_prefix=f"ETF {etf_code} 综合评分: ")
                
                # 明确指出评分是否低于阈值
                score_info = f"{abs(score):.2f}分"
                if score < Config.ARBITRAGE_SCORE_THRESHOLD:
                    score_info += f" ⚠️(低于阈值{Config.ARBITRAGE_SCORE_THRESHOLD:.1f})"
                
                content += (
                    f"\n{i}. {etf_name} ({etf_code})\n"
                    f"   ⭐ 综合评分: {score_info}\n"
                    f"   💹 折价率: {abs(premium_discount):.2f}%\n"
                    f"   📈 市场价格: {market_price:.3f}元\n"
                    f"   📊 IOPV: {iopv:.3f}元\n"
                    f"   🏦 基金规模: {fund_size:.2f}亿元\n"
                    f"   💰 日均成交额: {avg_volume:.2f}万元\n"
                )
                
                # 添加额外说明，特别是对高折价但低评分的情况
                if abs(premium_discount) > 5.0 and score < Config.ARBITRAGE_SCORE_THRESHOLD:
                    content += f"   📌 说明: 高折价机会({abs(premium_discount):.2f}%)，但综合评分较低，建议谨慎操作\n"
                elif abs(premium_discount) > Config.DISCOUNT_THRESHOLD * 2:
                    content += f"   📌 说明: 极高折价机会，建议重点关注\n"
                elif score < Config.ARBITRAGE_SCORE_THRESHOLD:
                    content += f"   📌 说明: 折价率达标，但综合评分较低，建议谨慎操作\n"
            
            messages.append(content)
        
        return messages
    
    except Exception as e:
        error_msg = f"生成折价消息内容失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return [f"【折价策略】生成消息内容时发生错误，请检查日志"]

# 溢价消息函数同步修改
def _format_premium_message(df: pd.DataFrame) -> List[str]:
    """
    格式化溢价机会消息，分页处理
    
    Args:
        df: 溢价机会DataFrame
    
    Returns:
        List[str]: 分页后的消息列表
    """
    try:
        # 兼容两种列名：如果只有"折价率"列，则添加"折溢价率"列
        if "折价率" in df.columns and "折溢价率" not in df.columns:
            df = df.copy()
            df["折溢价率"] = df["折价率"]
        
        # 按溢价率降序排序（最高溢价优先）
        df = df.sort_values(by='折溢价率', ascending=False).reset_index(drop=True)
        
        if df.empty:
            return ["【溢价机会】\n未发现有效溢价套利机会"]
        
        # 每页显示的ETF数量
        ETFS_PER_PAGE = 5
        total_etfs = len(df)
        total_pages = (total_etfs + ETFS_PER_PAGE - 1) // ETFS_PER_PAGE  # 向上取整
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        messages = []
        
        # 第1页：封面页
        if total_pages > 0:
            page1 = (
                "【以下ETF市场价格高于净值，可以考虑卖出】\n\n"
                f"💓共{total_etfs}只ETF，分{total_pages}条消息推送，这是第1/{total_pages}条消息\n\n"
                "💡 说明：当ETF市场价格高于IOPV（基金份额参考净值）时，表明ETF溢价交易\n"
                f"📊 筛选条件：基金规模≥{Config.GLOBAL_MIN_FUND_SIZE}亿元，日均成交额≥{Config.GLOBAL_MIN_AVG_VOLUME}万元\n"
                f"💰 交易成本：{Config.TRADE_COST_RATE*100:.2f}%（含印花税和佣金）\n"
                f"🎯 溢价阈值：溢价率超过{Config.PREMIUM_THRESHOLD*100:.2f}%\n"
                f"⭐ 综合评分：≥{Config.ARBITRAGE_SCORE_THRESHOLD:.1f}"
            )
            messages.append(page1)
        
        # 后续页：ETF详情（每页5只ETF）
        for page in range(total_pages):
            start_idx = page * ETFS_PER_PAGE
            end_idx = min(start_idx + ETFS_PER_PAGE, total_etfs)
            
            # 生成当前页的ETF详情
            content = f"【现价比净值高，卖出。 {page+1}/{total_pages}】\n"
            
            for i, (_, row) in enumerate(df.iloc[start_idx:end_idx].iterrows(), start_idx + 1):
                etf_code = str(row.get('ETF代码', '未知'))
                etf_name = str(row.get('ETF名称', '未知'))
                
                premium_discount = _extract_scalar_value(row.get('折溢价率', 0.0), log_prefix=f"ETF {etf_code} 折溢价率: ")
                market_price = _extract_scalar_value(row.get('市场价格', 0.0), log_prefix=f"ETF {etf_code} 市场价格: ")
                iopv = _extract_scalar_value(row.get('IOPV', 0.0), log_prefix=f"ETF {etf_code} IOPV: ")
                fund_size = _extract_scalar_value(row.get('规模', 0.0), log_prefix=f"ETF {etf_code} 规模: ")
                avg_volume = _extract_scalar_value(row.get('日均成交额', 0.0), log_prefix=f"ETF {etf_code} 日均成交额: ")
                score = _extract_scalar_value(row.get('综合评分', 0.0), log_prefix=f"ETF {etf_code} 综合评分: ")
                
                # 明确指出评分是否低于阈值
                score_info = f"{score:.2f}分"
                if score < Config.ARBITRAGE_SCORE_THRESHOLD:
                    score_info += f" ⚠️(低于阈值{Config.ARBITRAGE_SCORE_THRESHOLD:.1f})"
                
                content += (
                    f"\n{i}. {etf_name} ({etf_code})\n"
                    f"   ⭐ 综合评分: {score_info}\n"
                    f"   💹 溢价率: {abs(premium_discount):.2f}%\n"
                    f"   📈 市场价格: {market_price:.3f}元\n"
                    f"   📊 IOPV: {iopv:.3f}元\n"
                    f"   🏦 基金规模: {fund_size:.2f}亿元\n"
                    f"   💰 日均成交额: {avg_volume:.2f}万元\n"
                )
                
                # 添加额外说明，特别是对高溢价但低评分的情况
                if premium_discount > 5.0 and score < Config.ARBITRAGE_SCORE_THRESHOLD:
                    content += f"   📌 说明: 高溢价机会({premium_discount:.2f}%)，但综合评分较低，建议谨慎操作\n"
                elif premium_discount > Config.PREMIUM_THRESHOLD * 2:
                    content += f"   📌 说明: 极高溢价机会，建议重点关注\n"
                elif score < Config.ARBITRAGE_SCORE_THRESHOLD:
                    content += f"   📌 说明: 溢价率达标，但综合评分较低，建议谨慎操作\n"
            
            messages.append(content)
        
        return messages
    
    except Exception as e:
        error_msg = f"生成溢价消息内容失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return [f"【溢价策略】生成消息内容时发生错误，请检查日志"]

def _format_position_message(strategies: Dict[str, str]) -> List[str]:
    """
    格式化仓位策略消息，分页处理
    
    Args:
        strategies: 策略字典
    
    Returns:
        List[str]: 分页后的消息列表
    """
    try:
        # 每页显示的策略数量
        STRATEGIES_PER_PAGE = 1  # 每页只显示一个仓位类型
        total_strategies = len(strategies)
        total_pages = (total_strategies + STRATEGIES_PER_PAGE - 1) // STRATEGIES_PER_PAGE  # 向上取整
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 生成GitHub日志链接
        log_url = get_github_actions_url()
        
        # 页脚模板
        footer = (
            "\n==================\n"
            f"📅 UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📅 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "==================\n"
            # f"🔗 【GIT：fish-etf】: {log_url}\n"
            "📊 环境：【GIT：fish-etf】"
        )
        
        messages = []
        
        # 生成每页的策略详情
        for page, (position_type, strategy) in enumerate(strategies.items(), 1):
            content = (
                f"【ETF仓位操作提示】\n"
                f"（每个仓位仅持有1只ETF，操作建议基于最新数据）\n\n"
                f"【{position_type}】\n{strategy}"
            )
            
            # 添加页脚
            # content += footer
            messages.append(content)
        
        return messages
    
    except Exception as e:
        error_msg = f"生成仓位消息内容失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return [f"【仓位策略】生成消息内容时发生错误，请检查日志"]

def _apply_message_template(message: Union[str, pd.DataFrame, Dict], message_type: str) -> Union[str, List[str]]:
    """
    应用对应类型的消息模板
    :param message: 原始消息内容（可以是字符串、DataFrame或字典）
    :param message_type: 消息类型
    :return: 格式化后的消息（字符串或消息列表）
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        log_url = get_github_actions_url()
        
        # 全局消息脚模板
        footer = (
            "\n==================\n"
            f"📅 北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "📊 环境：Git-fish-etf"
        )
        
        # 根据消息类型应用不同的模板
        if message_type == "task":
            return f"{message}{footer}"
        elif message_type == "position":
            return f"{message}{footer}"
        elif message_type == "error":
            return f"⚠️ {message}{footer}"
        elif message_type == "daily_report":
            return f"{message}{footer}"
        else:
            return f"{message}{footer}"
    except Exception as e:
        logger.error(f"应用消息模板失败: {str(e)}", exc_info=True)
        # 返回一个基本格式的消息
        return (
            f"{message}"
            "\n==================\n"
            "📅 时间: 无法获取\n"
            "\n==================\n"
            "📊 数据来源：Git-fish-etf\n"
            "⚠️ 注意: 消息格式化过程中发生错误\n"
        )

def send_wechat_message(message: Union[str, pd.DataFrame, Dict], 
                       message_type: str = "default",
                       webhook: Optional[str] = None) -> bool:
    """
    发送消息到企业微信，自动应用消息模板
    
    Args:
        message: 消息内容（纯业务内容，可以是字符串、DataFrame或字典）
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
            messages = _format_dataframe_as_string(message)
        elif isinstance(message, dict):
            # 如果是字典，检查是否为空
            if not message:
                logger.warning("尝试发送空字典，已忽略")
                return False
            messages = [str(message)]
        else:
            messages = [str(message)]
        
        # 检查是否为空字符串
        if not any(msg.strip() for msg in messages):
            logger.warning("尝试发送空字符串消息，已忽略")
            return False
        
        # 特殊处理错误消息，避免频繁发送
        if message_type == "error":
            # 提取错误类型（例如"KeyError: 'fundamental'"）
            error_type = _extract_error_type(messages[0])
            
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
            
        # 应用消息模板并发送所有分页消息
        all_success = True
        for msg in messages:
            # 仅对position类型应用底部格式
            if message_type == "position":
                # 【日期datetime类型规则】确保日期在内存中是datetime类型
                beijing_time = get_beijing_time()
                utc_time = get_utc_time()
                # 添加底部格式
                footer = f"\n\n==================\n"
                # footer += f"📅 UTC时间: {utc_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                # footer += f"📅 北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                # footer += "📊 环境：生产\n"
                footer += f"📅 北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                footer += "📊 环境：Git-fish-etf"
                
                full_message = msg + footer
            else:
                full_message = _apply_message_template(msg, message_type)
            
            # 检查消息长度并进行分片
            messages_to_send = _check_message_length(full_message)
            
            for i, msg_part in enumerate(messages_to_send):
                # 速率限制
                _rate_limit()
                
                # 重试机制
                success = False
                for retry in range(_MAX_RETRIES):
                    if _send_single_message(webhook, msg_part, retry):
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

def _format_dataframe_as_string(df: pd.DataFrame) -> List[str]:
    """
    将 DataFrame格式化为多条消息（每条不超过2000字符）
    
    Args:
        df: 要格式化的DataFrame
        
    Returns:
        List[str]: 分页后的消息列表
    """
    try:
        if df.empty:
            return ["没有找到符合条件的数据"]
        
        # 检查是否包含ETF特定列
        has_etf_info = "ETF代码" in df.columns and "ETF名称" in df.columns
        # 兼容两种列名：折溢价率 和 折价率
        has_premium_discount = "折溢价率" in df.columns or "折价率" in df.columns
        
        if has_etf_info and has_premium_discount:
            # 统一列名：如果只有"折价率"列，则重命名为"折溢价率"
            if "折价率" in df.columns and "折溢价率" not in df.columns:
                df = df.copy()
                df["折溢价率"] = df["折价率"]
            
            # 直接调用已定义的专用格式化函数
            if df["折溢价率"].min() < 0:
                return _format_discount_message(df)
            else:
                return _format_premium_message(df)
        
        # 非ETF场景
        logger.warning("尝试格式化非ETF数据，这可能表示代码逻辑有误")
        return [f"找到 {len(df)} 条数据，但不是ETF数据格式"]
    
    except Exception as e:
        logger.error(f"格式化DataFrame为字符串失败: {str(e)}", exc_info=True)
        return ["数据格式化错误，请检查日志"]

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
        log_url = get_github_actions_url()
        
        # 添加统一的页脚
        footer = (
            "\n\n"
            "──────────────────\n"
            f"🕒 **UTC时间**: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📅 **北京时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "──────────────────\n"
            # f"🔗 【GIT：fish-etf】: {log_url}\n"
            f"🔗 【GIT：fish-etf】\n"
        )
        
        # 完整消息
        # full_message = message + footer
        full_message = message
        
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
            f"**📅测试时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}（北京时间）\n\n"
            "\n──────────────────\n"
            f"🕒 **UTC时间**: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📅 **北京时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "\n──────────────────\n"
            f"🔗 消息来源【GIT：fish-etf】\n"
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
    _error_message_cache = {}
    
    # 清理过期的错误消息缓存（每天清理一次）
    def _cleanup_error_cache():
        current_time = time.time()
        expired_keys = []
        for msg, timestamp in _error_message_cache.items():
            if current_time - timestamp > 86400:  # 24小时
                expired_keys.append(msg)
        
        for key in expired_keys:
            del _error_message_cache[key]
        
        if expired_keys:
            logger.debug(f"清理了 {len(expired_keys)} 条过期的错误消息缓存")
    
    # 定期清理缓存
    import threading
    def _cache_cleanup_thread():
        while True:
            time.sleep(3600)  # 每小时清理一次
            _cleanup_error_cache()
    
    cleanup_thread = threading.Thread(target=_cache_cleanup_thread, daemon=True)
    cleanup_thread.start()
    
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
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(f"微信推送模块初始化失败: {str(e)}")
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(f"微信推送模块初始化失败: {str(e)}")

def send_txt_file(file_path: str, title: str = "股票代码清单", 
                 message_type: str = "position", 
                 webhook: Optional[str] = None) -> bool:
    """
    读取文本文件内容并通过微信发送完整内容
    
    Args:
        file_path: 文本文件路径
        title: 消息标题
        message_type: 消息类型
        webhook: 企业微信Webhook地址
        
    Returns:
        bool: 是否成功发送
    """
    try:
        if not file_path or not os.path.exists(file_path):
            logger.error(f"文本文件不存在: {file_path}")
            send_wechat_message(f"❌ 文本文件不存在: {os.path.basename(file_path)}", message_type="error")
            return False
        
        # 读取文件内容
        with open(file_path, 'r', encoding='ascii') as f:
            file_content = f.read().strip()
        
        if not file_content:
            logger.warning("文本文件为空")
            send_wechat_message("⚠️ 文本文件为空", message_type=message_type)
            return False
        
        # 统计内容行数
        content_lines = [line.strip() for line in file_content.split('\n') if line.strip()]
        line_count = len(content_lines)
        
        # 获取当前时间
        beijing_time = get_beijing_time()
        
        # 构造文件内容消息
        file_message = (
            f"📋 {title}\n"
            f"══════════════════════\n"
            f"📅 生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📊 内容数量: {line_count} 条\n"
            f"📁 文件名称: {os.path.basename(file_path)}\n"
            f"══════════════════════\n"
            f"完整内容:\n"
        )
        
        # 添加完整文件内容
        for i, line in enumerate(content_lines, 1):
            file_message += f"{line}\n"
        
        file_message += (
            f"══════════════════════\n"
            f"💡 提示: 共 {line_count} 条记录，已完整显示"
        )
        
        # 发送文件内容
        logger.info(f"发送文本文件内容，共 {line_count} 条记录，文件: {os.path.basename(file_path)}")
        success = send_wechat_message(message=file_message, message_type=message_type, webhook=webhook)
        
        if success:
            logger.info(f"✅ 文本文件内容发送成功: {os.path.basename(file_path)}")
            return True
        else:
            logger.error(f"❌ 文本文件内容发送失败: {os.path.basename(file_path)}")
            # 尝试发送简化版本
            simple_message = (
                f"📋 {title} ({line_count}条)\n"
                f"文件: {os.path.basename(file_path)}\n"
                f"前10条: {', '.join(content_lines[:10])}{'...' if len(content_lines) > 10 else ''}"
            )
            send_wechat_message(message=simple_message, message_type=message_type, webhook=webhook)
            return False
        
    except Exception as e:
        logger.error(f"发送文本文件内容失败: {str(e)}")
        # 发送错误通知但不要中断主流程
        error_msg = f"⚠️ 文本文件发送失败: {str(e)}"
        send_wechat_message(message=error_msg, message_type="error", webhook=webhook)
        return False
