# wechat_push.py
import os
import requests
import time
import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from config import Config

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
        logger.error(f"发送消息时发生未预期错误: {str(e)} (重试 {retry_count})")
        return False

def send_wechat_message(message: str, webhook: Optional[str] = None) -> bool:
    """发送消息到企业微信，自动添加固定末尾，支持消息分片和重试机制
    :param message: 消息内容
    :param webhook: 企业微信Webhook地址，如果为None则使用配置中的地址
    :return: 是否成功发送"""
    try:
        # 从环境变量获取Webhook（优先于配置文件）
        if webhook is None:
            webhook = os.getenv("WECOM_WEBHOOK", Config.WECOM_WEBHOOK)
        if not webhook:
            logger.error("企业微信Webhook未配置，无法发送消息")
            return False
            
        # 添加固定末尾
        full_message = f"{message}{Config.WECOM_MESFOOTER}"
        
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
        logger.error(f"发送微信消息时发生未预期错误: {str(e)}")
        return False

def send_wechat_markdown(message: str, webhook: Optional[str] = None) -> bool:
    """
    发送Markdown格式消息到企业微信
    :param message: Markdown格式消息
    :param webhook: 企业微信Webhook地址
    :return: 是否成功发送
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
        logger.error(f"发送微信Markdown消息时发生错误: {str(e)}")
        return False

def test_webhook_connection(webhook: Optional[str] = None) -> bool:
    """
    测试企业微信Webhook连接是否正常
    :param webhook: 企业微信Webhook地址
    :return: 连接是否正常
    """
    try:
        if webhook is None:
            webhook = os.getenv("WECOM_WEBHOOK", Config.WECOM_WEBHOOK)
        
        if not webhook:
            logger.error("企业微信Webhook未配置")
            return False
        
        # 发送测试消息
        test_message = "【测试消息】\n企业微信Webhook连接测试成功\n\n发送时间: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return send_wechat_message(test_message, webhook)
        
    except Exception as e:
        logger.error(f"测试Webhook连接时发生错误: {str(e)}")
        return False

# 模块初始化
try:
    logger.info("微信推送模块初始化完成")
    
    # 测试Webhook连接（仅在调试模式下）
    if os.getenv("DEBUG", "").lower() in ("true", "1", "yes"):
        logger.info("调试模式启用，测试Webhook连接")
        test_webhook_connection()
except Exception as e:
    logger.error(f"微信推送模块初始化失败: {str(e)}")
# 0828-1256【wechat_push.py代码】一共190行代码
