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

def _format_arbitrage_message(df: pd.DataFrame) -> str:
    """
    格式化套利机会消息
    
    Args:
        df: 套利机会DataFrame
    
    Returns:
        str: 格式化后的消息
    """
    try:
        if df.empty:
            return "【套利机会】\n未发现有效套利机会"
        
        # 生成消息内容
        content = "【ETF溢价套利机会】\n\n"
        content += "💡 套利原理：当ETF市场价格高于IOPV（基金份额参考净值）时，可申购ETF份额并卖出获利\n"
        content += f"📊 筛选条件：基金规模≥{Config.GLOBAL_MIN_FUND_SIZE}亿元，日均成交额≥{Config.GLOBAL_MIN_AVG_VOLUME}万元\n"
        content += f"💰 交易成本：{Config.TRADE_COST_RATE*100:.2f}%（含印花税和佣金）\n"
        content += f"🎯 套利阈值：收益率超过{Config.ARBITRAGE_THRESHOLD*100:.2f}%\n\n"
        
        # 添加套利机会
        for i, (_, row) in enumerate(df.head(3).iterrows(), 1):
            direction = "溢价" if row["折溢价率"] > 0 else "折价"
            content += f"{i}. {row['ETF名称']} ({row['ETF代码']})\n"
            content += f"   💹 {direction}率: {abs(row['折溢价率']):.2f}%\n"
            content += f"   📈 市场价格: {row['市场价格']:.3f}元\n"
            content += f"   📊 IOPV: {row['IOPV']:.3f}元\n"
            content += f"   🏦 基金规模: {row['规模']:.2f}亿元\n"
            content += f"   💰 日均成交额: {row['日均成交额']:.2f}万元\n\n"
        
        # 添加其他机会数量
        if len(df) > 3:
            content += f"• 还有 {len(df) - 3} 个套利机会...\n"
        
        # 添加风险提示
        content += (
            "\n⚠️ 风险提示：\n"
            "1. 套利机会转瞬即逝，请及时操作\n"
            "2. 实际交易中可能因价格变动导致套利失败\n"
            "3. 本策略仅供参考，不构成投资建议\n"
        )
        
        return content
    
    except Exception as e:
        error_msg = f"生成套利消息内容失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return f"【套利策略】生成消息内容时发生错误，请检查日志"

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
        if message_type == "arbitrage" and isinstance(message, pd.DataFrame):
            message = _format_arbitrage_message(message)
        
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
        elif message_type == "arbitrage":
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
        message_type: 消息类型（task, arbitrage, position, error, daily_report等）
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
