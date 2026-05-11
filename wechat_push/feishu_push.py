#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书推送模块 - 兼容现有飞书自建应用
支持通过飞书OpenAPI发送消息（使用App ID + App Secret）
"""

import os
import requests
import time
import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime
from config import Config
from utils.date_utils import get_beijing_time

logger = logging.getLogger(__name__)

# 消息发送频率控制
_last_send_time = 0.0
_MIN_SEND_INTERVAL = 3.5
_MAX_MESSAGE_LENGTH = 4000

# 发送失败重试配置
_MAX_RETRIES = 3
_RETRY_DELAYS = [2, 4, 8]
_REQUEST_TIMEOUT = 10.0

# 飞书API配置
_FEISHU_API_BASE_URL = "https://open.feishu.cn/open-apis"

# Access Token 缓存
_access_token_info = {
    "token": "",
    "expire_time": 0
}

def _get_access_token(app_id: str, app_secret: str) -> Optional[str]:
    """获取飞书开放平台 Access Token"""
    global _access_token_info
    
    current_time = time.time()
    
    if _access_token_info["token"] and current_time < _access_token_info["expire_time"] - 60:
        return _access_token_info["token"]
    
    try:
        url = f"{_FEISHU_API_BASE_URL}/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": app_id,
            "app_secret": app_secret
        }
        
        response = requests.post(url, json=payload, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") == 0:
            _access_token_info["token"] = result.get("tenant_access_token", "")
            _access_token_info["expire_time"] = current_time + result.get("expire", 7200)
            logger.info("成功获取飞书Access Token")
            return _access_token_info["token"]
        else:
            logger.error(f"获取Access Token失败: {result.get('msg', '未知错误')}")
            return None
            
    except Exception as e:
        logger.error(f"获取Access Token异常: {str(e)}")
        return None

def _send_single_message(access_token: str, receive_id: str, receive_id_type: str, message: str, retry_count: int = 0) -> bool:
    """使用飞书OpenAPI发送消息"""
    try:
        url = f"{_FEISHU_API_BASE_URL}/im/v1/messages?receive_id_type={receive_id_type}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        payload = {
            "receive_id": receive_id,
            "msg_type": "text",
            "content": json.dumps({
                "text": message
            })
        }
        
        logger.debug(f"发送消息 (重试 {retry_count}): {message[:100]}...")
        response = requests.post(url, headers=headers, json=payload, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") == 0:
            logger.info("飞书消息发送成功")
            return True
        else:
            error_msg = result.get('msg', '未知错误')
            logger.error(f"飞书消息发送失败: {error_msg}")
            return False
            
    except requests.exceptions.Timeout:
        logger.error(f"飞书消息发送超时 (重试 {retry_count})")
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

def _rate_limit() -> None:
    global _last_send_time
    current_time = time.time()
    elapsed = current_time - _last_send_time
    
    if elapsed < _MIN_SEND_INTERVAL:
        sleep_time = _MIN_SEND_INTERVAL - elapsed
        logger.debug(f"速率限制：等待 {sleep_time:.2f} 秒")
        time.sleep(sleep_time)
    
    _last_send_time = current_time

def _split_message(message: str) -> List[str]:
    """拆分长消息"""
    if not message or len(message) <= _MAX_MESSAGE_LENGTH:
        return [message]
    
    logger.warning(f"消息过长({len(message)}字符)，进行分片处理")
    
    paragraphs = message.split('\n\n')
    chunks = []
    current_chunk = ""
    
    for paragraph in paragraphs:
        if len(current_chunk) + len(paragraph) + 2 <= _MAX_MESSAGE_LENGTH:
            if current_chunk:
                current_chunk += "\n\n" + paragraph
            else:
                current_chunk = paragraph
        else:
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = ""
            current_chunk = paragraph
    
    if current_chunk:
        chunks.append(current_chunk)
    
    if len(chunks) > 1:
        for i, chunk in enumerate(chunks):
            chunks[i] = f"【消息分片 {i+1}/{len(chunks)}】\n\n{chunk}"
    
    return chunks

def send_feishu_message(message: str, 
                       app_id: Optional[str] = None,
                       app_secret: Optional[str] = None,
                       user_id: Optional[str] = None,
                       chat_id: Optional[str] = None) -> bool:
    """
    发送消息到飞书（使用您现有的自建应用）
    
    支持两种模式：
    1. 私聊模式（优先）：配置 user_id，发送给个人用户
    2. 群组模式：配置 chat_id，发送到群组
    
    Args:
        message: 消息内容
        app_id: 飞书应用ID（从环境变量 FEISHU_APP_ID 获取）
        app_secret: 飞书应用密钥（从环境变量 FEISHU_APP_SECRET 获取）
        user_id: 用户ID（从环境变量 FEISHU_USER_ID 获取，私聊时使用）
        chat_id: 群组ID（从环境变量 FEISHU_CHAT_ID 获取，群聊时使用）
    
    Returns:
        bool: 是否发送成功
    """
    try:
        if not message or not message.strip():
            logger.warning("尝试发送空消息，已忽略")
            return False
        
        # 从环境变量获取配置
        if not app_id:
            app_id = os.getenv("FEISHU_APP_ID", "")
        if not app_secret:
            app_secret = os.getenv("FEISHU_APP_SECRET", "")
        if not user_id:
            user_id = os.getenv("FEISHU_USER_ID", "")
        if not chat_id:
            chat_id = os.getenv("FEISHU_CHAT_ID", "")
        
        # 检查配置
        if not app_id or not app_secret:
            logger.error("飞书应用ID或密钥未配置")
            return False
        
        # 优先使用私聊模式（user_id）
        if user_id:
            receive_id = user_id
            receive_id_type = "user_id"
            logger.info("使用私聊模式发送消息")
        elif chat_id:
            receive_id = chat_id
            receive_id_type = "chat_id"
            logger.info("使用群组模式发送消息")
        else:
            logger.error("飞书用户ID或群组ID未配置")
            return False
        
        # 获取Access Token
        access_token = _get_access_token(app_id, app_secret)
        if not access_token:
            logger.error("无法获取Access Token")
            return False
        
        # 添加时间戳
        beijing_time = get_beijing_time()
        footer = (
            f"\n\n==================\n"
            f"📅 北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "📊 环境：Git-fish-etf"
        )
        full_message = message + footer
        
        # 拆分消息
        messages_to_send = _split_message(full_message)
        
        # 发送所有消息
        all_success = True
        for msg_part in messages_to_send:
            _rate_limit()
            
            success = False
            for retry in range(_MAX_RETRIES):
                if _send_single_message(access_token, receive_id, receive_id_type, msg_part, retry):
                    success = True
                    break
                else:
                    if retry < _MAX_RETRIES - 1:
                        time.sleep(_RETRY_DELAYS[retry])
            
            if not success:
                all_success = False
        
        return all_success
        
    except Exception as e:
        logger.error(f"发送飞书消息失败: {str(e)}", exc_info=True)
        return False

def send_futures_report(futures_data: Dict[str, Any], 
                       app_id: Optional[str] = None,
                       app_secret: Optional[str] = None,
                       user_id: Optional[str] = None,
                       chat_id: Optional[str] = None) -> bool:
    """
    发送期货报告到飞书
    
    Args:
        futures_data: 期货数据字典
        app_id: 飞书应用ID
        app_secret: 飞书应用密钥
        user_id: 用户ID（私聊时使用）
        chat_id: 群组ID（群聊时使用）
    
    Returns:
        bool: 是否发送成功
    """
    try:
        beijing_time = get_beijing_time()
        
        report_lines = []
        report_lines.append(f"{beijing_time.strftime('%Y-%m-%d %H:%M:%S')}，期货行情数据：IC01/IC06/IC09、IF、IH 明细如下——")
        report_lines.append("")
        
        futures_df = futures_data.get("futures_data")
        if futures_df is not None and not futures_df.empty:
            for _, row in futures_df.iterrows():
                contract = row['合约代码']
                price = row['最新价']
                source = row['数据源']
                report_lines.append(f"- {contract}: {price} ({source})")
        else:
            report_lines.append("- ❌ 无法获取期货行情数据")
        
        report_lines.append("")
        report_lines.append("🌎 **外盘指数表现**")
        
        external_df = futures_data.get("external_data")
        if external_df is not None and not external_df.empty:
            for _, row in external_df.iterrows():
                name = row['指数名称']
                price = row['最新价']
                change = row['涨跌幅']
                change_str = f"▲{change:.2f}%" if change >= 0 else f"▼{abs(change):.2f}%"
                report_lines.append(f"- {name}: {price:.2f} {change_str}")
        else:
            report_lines.append("- ❌ 无法获取外盘数据")
        
        report_lines.append("")
        report_lines.append("💧 **升贴水分析**")
        
        roll_df = futures_data.get("roll_analysis")
        if roll_df is not None and not roll_df.empty:
            for _, row in roll_df.iterrows():
                contract = row['合约代码']
                basis = row['基差']
                basis_rate = row['基差率']
                basis_type = "贴水" if row["贴水"] else "升水" if row["升水"] else "平水"
                status = "⚠️" if row["建议移仓"] else "✅"
                report_lines.append(f"- {contract}: 基差 {basis:.2f} ({basis_rate:.2f}%) [{basis_type}] {status}")
        else:
            report_lines.append("- ❌ 无法计算升贴水")
        
        report_lines.append("")
        report_lines.append("📋 **移仓建议**")
        if roll_df is not None and not roll_df.empty:
            need_roll = roll_df[roll_df["建议移仓"]]
            if not need_roll.empty:
                report_lines.append("需要关注的移仓机会：")
                for _, row in need_roll.iterrows():
                    report_lines.append(f"  • {row['合约代码']}: 年化贴水 {abs(row['年化基差率']):.2f}%")
            else:
                report_lines.append("当前无强烈移仓需求")
        else:
            report_lines.append("无法判断移仓时机")
        
        report_lines.append("")
        report_lines.append("💡 **下一步计划**")
        report_lines.append("1. 持续监控持仓风险度")
        report_lines.append("2. 根据贴水情况判断是否需要移仓")
        
        full_message = "\n".join(report_lines)
        
        return send_feishu_message(full_message, app_id, app_secret, user_id, chat_id)
        
    except Exception as e:
        logger.error(f"发送期货报告失败: {str(e)}", exc_info=True)
        return False

def send_task_completion_notification(task: str, result: Dict[str, Any], 
                                     app_id: Optional[str] = None,
                                     app_secret: Optional[str] = None,
                                     user_id: Optional[str] = None,
                                     chat_id: Optional[str] = None) -> bool:
    """
    发送任务完成通知
    
    Args:
        task: 任务名称
        result: 任务执行结果
        app_id: 飞书应用ID
        app_secret: 飞书应用密钥
        user_id: 用户ID（私聊时使用）
        chat_id: 群组ID（群聊时使用）
    
    Returns:
        bool: 是否发送成功
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
        
        beijing_time = get_beijing_time()
        
        summary_msg = (
            f"【任务执行】{task}\n\n"
            f"{status_emoji} **状态**: {status_msg}\n"
            f"📝 **详情**: {result.get('message', '无详细信息')}\n"
            f"\n📅 北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            "📊 环境：Git-fish-etf"
        )
        
        return send_feishu_message(summary_msg, app_id, app_secret, user_id, chat_id)
        
    except Exception as e:
        logger.error(f"发送任务完成通知失败: {str(e)}", exc_info=True)
        return False

def test_connection(app_id: Optional[str] = None,
                   app_secret: Optional[str] = None,
                   user_id: Optional[str] = None,
                   chat_id: Optional[str] = None) -> bool:
    """测试飞书连接"""
    try:
        test_message = "✅ 飞书消息发送测试成功！"
        return send_feishu_message(test_message, app_id, app_secret, user_id, chat_id)
    except Exception as e:
        logger.error(f"测试连接失败: {str(e)}", exc_info=True)
        return False

try:
    Config.init_dirs()
    logger.info("飞书推送模块初始化完成")
    
    if os.getenv("DEBUG", "").lower() in ("true", "1", "yes"):
        logger.info("调试模式启用，测试飞书连接")
        test_connection()
except Exception as e:
    logger.error(f"飞书推送模块初始化失败: {str(e)}", exc_info=True)
