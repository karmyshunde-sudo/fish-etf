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
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import (
    get_current_times,
    format_dual_time,
    get_beijing_time,
    get_utc_time,
    is_market_open,
    is_trading_day,
    is_file_outdated
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
_RETRY_DELAYS = [1, 3, 5]  # 重试延迟(秒)

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
        
        # 企业微信文本消息格式
        payload = {
            "msgtype": "text",
            "text": {
                "content": message
            }
        }
        
        logger.debug(f"发送消息到企业微信 (重试 {retry_count}): {message[:100]}...")
        response = requests.post(webhook, json=payload, timeout=10)
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

def send_wechat_message(message: str, webhook: Optional[str] = None) -> bool:
    """
    发送结构化微信消息（Markdown格式），针对不同场景使用不同模板
    
    Args:
        message: 消息内容
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
            
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 根据消息内容自动识别消息类型
        if "【任务执行】" in message:
            formatted_message = _format_task_message(message, utc_now, beijing_now)
        elif "【套利机会】" in message:
            formatted_message = _format_arbitrage_message(message, utc_now, beijing_now)
        elif "【仓位策略】" in message:
            formatted_message = _format_position_message(message, utc_now, beijing_now)
        elif "【每日报告】" in message:
            formatted_message = _format_daily_report(message, utc_now, beijing_now)
        elif "执行失败" in message or "错误" in message or "异常" in message:
            formatted_message = _format_error_message(message, utc_now, beijing_now)
        else:
            # 默认格式
            formatted_message = _format_default_message(message, utc_now, beijing_now)
        
        # 检查消息长度并进行分片
        message_chunks = _check_message_length(formatted_message)
        
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

def _format_task_message(message: str, utc_now: datetime, beijing_now: datetime) -> str:
    """格式化任务执行消息"""
    # 提取任务信息
    task_name = message.split("【任务执行】")[1].split("\n")[0].strip()
    
    # 提取状态信息
    status_line = next((line for line in message.split("\n") if "状态:" in line), "")
    status = status_line.split("状态:")[1].strip() if status_line else "未知"
    
    # 提取详情信息
    detail_line = next((line for line in message.split("\n") if "详情:" in line), "")
    detail = detail_line.split("详情:")[1].strip() if detail_line else "无详情"
    
    # 生成Markdown格式消息
    status_emoji = "✅" if "成功" in status else "❌" if "失败" in status else "⏭️"
    
    return (
        f"{status_emoji} **【任务执行】{task_name}**\n\n"
        f"**状态**: {status}\n"
        f"**详情**: {detail}\n\n"
        "──────────────────\n"
        f"🕒 **UTC时间**: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🕒 **北京时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        "──────────────────\n"
        "📊 数据来源：AkShare | 环境：生产"
    )

def _format_arbitrage_message(message: str, utc_now: datetime, beijing_now: datetime) -> str:
    """格式化套利机会消息"""
    # 提取关键信息
    opportunities = []
    for line in message.split("\n"):
        if "• 溢价" in line or "• 折价" in line:
            opportunities.append(line.strip())
    
    # 生成Markdown格式消息
    return (
        "🔍 **【套利机会】**\n\n"
        f"{'🏆 今日最佳套利机会:' if opportunities else '🔍 未发现有效套利机会'}\n"
        f"{chr(10).join(opportunities[:3])}\n\n"
        f"{'• 还有 ' + str(len(opportunities)-3) + ' 个机会...' if len(opportunities) > 3 else ''}\n\n"
        "──────────────────\n"
        f"🕒 **UTC时间**: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🕒 **北京时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        "──────────────────\n"
        "📊 数据来源：AkShare | 环境：生产"
    )

def _format_position_message(message: str, utc_now: datetime, beijing_now: datetime) -> str:
    """格式化仓位策略消息"""
    # 提取关键信息
    etfs = []
    for line in message.split("\n"):
        if "•" in line and ("ETF" in line or "规模" in line):
            etfs.append(line.strip())
    
    # 生成Markdown格式消息
    return (
        "💼 **【仓位策略】**\n\n"
        f"{'🏆 推荐ETF组合:' if etfs else '⚠️ 未找到符合条件的ETF'}\n"
        f"{chr(10).join(etfs[:5])}\n\n"
        "💡 **仓位建议**\n"
        "• 稳健型投资者：建议配置5-10只ETF，每只仓位10%-20%\n"
        "• 激进型投资者：可集中配置3-5只ETF，每只仓位20%-30%\n\n"
        "──────────────────\n"
        f"🕒 **UTC时间**: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🕒 **北京时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        "──────────────────\n"
        "📊 数据来源：AkShare | 环境：生产"
    )

def _format_daily_report(message: str, utc_now: datetime, beijing_now: datetime) -> str:
    """格式化每日报告消息"""
    # 提取关键信息
    market_info = []
    hot_etfs = []
    in_position = []
    
    for line in message.split("\n"):
        if "• 全市场ETF总数" in line or "• 平均基金规模" in line:
            market_info.append(line.strip())
        elif "🔥 热门ETF" in line or ("• 规模" in line and "成交额" in line):
            hot_etfs.append(line.strip())
        elif "💡 仓位建议" in line or ("稳健型" in line or "激进型" in line):
            in_position.append(line.strip())
    
    # 生成Markdown格式消息
    return (
        "📈 **【每日报告】**\n\n"
        "**📊 市场概况**\n"
        f"{chr(10).join(market_info)}\n\n"
        "**🔥 热门ETF**\n"
        f"{chr(10).join(hot_etfs[1:4])}\n\n"
        "**💡 仓位建议**\n"
        f"{chr(10).join(in_position[1:3])}\n\n"
        "──────────────────\n"
        f"🕒 **UTC时间**: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🕒 **北京时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        "──────────────────\n"
        "📊 数据来源：AkShare | 环境：生产"
    )

def _format_error_message(message: str, utc_now: datetime, beijing_now: datetime) -> str:
    """格式化错误消息"""
    # 提取任务名称
    task = "未知任务"
    if "【GIT-fish-etf】[" in message:
        task = message.split("【GIT-fish-etf】[")[1].split("]")[0]
    
    # 提取错误详情
    error_detail = "无详细信息"
    if "执行失败" in message:
        error_detail = message.split("执行失败")[1].strip().lstrip("：").lstrip(":")
    
    # 生成GitHub日志链接
    github_run_id = os.getenv("GITHUB_RUN_ID", "unknown")
    repo = os.getenv("GITHUB_REPOSITORY", "karmyshunde-sudo/fish-etf")
    log_url = f"https://github.com/{repo}/actions/runs/{github_run_id}" if github_run_id != "unknown" else "无法获取日志链接"
    
    # 生成Markdown格式消息
    return (
        "⚠️ **【系统异常】**\n\n"
        f"**任务**: `{task}`\n"
        "**状态**: ❌ 执行失败\n\n"
        "**错误详情**:\n"
        f"> {error_detail}\n\n"
        "**日志链接**:\n"
        f"> [点击查看完整日志]({log_url})\n\n"
        "──────────────────\n"
        f"🕒 **UTC时间**: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🕒 **北京时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        "──────────────────\n"
        "📊 数据来源：AkShare | 环境：生产"
    )

def _format_default_message(message: str, utc_now: datetime, beijing_now: datetime) -> str:
    """格式化默认消息"""
    return (
        "ℹ️ **【系统消息】**\n\n"
        f"{message}\n\n"
        "──────────────────\n"
        f"🕒 **UTC时间**: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🕒 **北京时间**: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        "──────────────────\n"
        "📊 数据来源：AkShare | 环境：生产"
    )

def send_wechat_markdown(message: str, webhook: Optional[str] = None) -> bool:
    """
    发送Markdown格式消息到企业微信
    
    Args:
        message: Markdown格式消息
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
        
        # 企业微信Markdown消息格式
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": message
            }
        }
        
        logger.debug(f"发送Markdown消息到企业微信: {message[:100]}...")
        response = requests.post(webhook, json=payload, timeout=10)
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
        
        # 生成双时区时间字符串
        time_info = (
            f"\n     UTC时间: {utc_now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"     北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        # 发送测试消息
        test_message = (
            "【测试消息】\n"
            "企业微信Webhook连接测试成功\n\n"
            f"🕒 消息生成时间：{time_info}"
        )
        
        logger.info("开始测试Webhook连接")
        success = send_wechat_message(test_message, webhook)
        
        if success:
            logger.info("Webhook连接测试成功")
        else:
            logger.error("Webhook连接测试失败")
        
        return success
        
    except Exception as e:
        logger.error(f"测试Webhook连接时发生错误: {str(e)}", exc_info=True)
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
        
        # 构建任务总结消息（保持原有格式）
        summary_msg = (
            f"【任务执行】{task}\n\n"
            f"{status_emoji} 状态: {status_msg}\n"
            f"📝 详情: {result.get('message', '无详细信息')}\n"
        )
        
        # 添加任务特定信息（保持原有格式）
        if task == "update_etf_list" and result["status"] == "success":
            count = result.get('count', 0)
            source = result.get('source', '未知')
            summary_msg += (
                f"📊 ETF数量: {count}只\n"
                f" sourceMapping: {source}\n"
            )
            
            # 添加列表有效期信息（保持原有格式）
            try:
                last_modified_utc = result.get('last_modified_utc', '未知')
                last_modified_beijing = result.get('last_modified_beijing', '未知')
                expiration_utc = result.get('expiration_utc', '未知')
                expiration_beijing = result.get('expiration_beijing', '未知')
                summary_msg += (
                    f"📅 生成时间: {last_modified_beijing}\n"
                    f"⏳ 过期时间: {expiration_beijing}\n"
                )
            except:
                pass
                
        elif task == "crawl_etf_daily" and result["status"] == "success":
            summary_msg += "📈 数据爬取: 完成\n"
            
        elif task == "calculate_arbitrage" and result["status"] == "success":
            summary_msg += "🔍 套利机会: 已推送\n"
            
        elif task == "calculate_position" and result["status"] == "success":
            summary_msg += "💼 仓位策略: 已推送\n"
        
        # 发送任务总结通知
        send_wechat_message(summary_msg)
        logger.info(f"已发送任务完成通知: {task} - {status_msg}")
        
    except Exception as e:
        logger.error(f"发送任务完成通知失败: {str(e)}", exc_info=True)

# 模块初始化
try:
    logger.info("微信推送模块初始化完成")
    
    # 测试Webhook连接（仅在调试模式下）
    if os.getenv("DEBUG", "").lower() in ("true", "1", "yes"):
        logger.info("调试模式启用，测试Webhook连接")
        test_webhook_connection()
except Exception as e:
    logger.error(f"微信推送模块初始化失败: {str(e)}", exc_info=True)
