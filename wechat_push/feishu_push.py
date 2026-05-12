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

# User Access Token 缓存（用于用户身份发送）
_user_access_token_info = {
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

def _get_user_access_token(app_id: str, app_secret: str, refresh_token: Optional[str] = None) -> Optional[str]:
    """获取用户身份的 Access Token（自动刷新）"""
    global _user_access_token_info
    
    current_time = time.time()
    
    # 检查缓存是否有效
    if _user_access_token_info["token"] and current_time < _user_access_token_info["expire_time"] - 60:
        return _user_access_token_info["token"]
    
    try:
        # 优先使用 refresh_token 刷新
        if refresh_token:
            url = f"{_FEISHU_API_BASE_URL}/authen/v1/refresh_access_token"
            payload = {
                "app_id": app_id,
                "app_secret": app_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token"
            }
        else:
            # 如果没有 refresh_token，尝试使用授权码（需要环境变量配置）
            code = os.environ.get("FEISHU_AUTH_CODE", "")
            if not code:
                logger.warning("未配置 FEISHU_AUTH_CODE 或 FEISHU_REFRESH_TOKEN，无法获取 user_access_token")
                return None
            
            url = f"{_FEISHU_API_BASE_URL}/authen/v1/access_token"
            payload = {
                "app_id": app_id,
                "app_secret": app_secret,
                "code": code,
                "grant_type": "authorization_code"
            }
        
        response = requests.post(url, json=payload, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") == 0:
            _user_access_token_info["token"] = result.get("access_token", "")
            _user_access_token_info["expire_time"] = current_time + result.get("expire", 7200)
            
            # 保存新的 refresh_token（如果返回了）
            if "refresh_token" in result:
                logger.info("获取 user_access_token 成功")
            return _user_access_token_info["token"]
        else:
            logger.error(f"获取 user_access_token 失败: {result.get('msg', '未知错误')}")
            return None
            
    except Exception as e:
        logger.error(f"获取 user_access_token 异常: {str(e)}")
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
        try:
            response_text = e.response.text if hasattr(e.response, 'text') else 'N/A'
            logger.error(f"请求异常: {str(e)} (重试 {retry_count})")
            logger.error(f"响应内容: {response_text}")
        except:
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
        
        # 优先使用私聊模式（user_id/open_id）
        if user_id:
            receive_id = user_id
            # 判断是user_id还是open_id
            if user_id.startswith("ou_"):
                receive_id_type = "open_id"
            else:
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

def send_feishu_message_as_user(message: str, 
                               app_id: Optional[str] = None,
                               app_secret: Optional[str] = None,
                               refresh_token: Optional[str] = None,
                               chat_id: Optional[str] = None) -> bool:
    """
    以用户身份发送消息（用于触发机器人回复）
    
    使用 user_access_token 发送消息，可以触发飞书机器人的自动回复
    支持自动获取和刷新 user_access_token
    
    Args:
        message: 消息内容
        app_id: 飞书应用ID（从环境变量 FEISHU_APP_ID 获取）
        app_secret: 飞书应用密钥（从环境变量 FEISHU_APP_SECRET 获取）
        refresh_token: 用户刷新令牌（从环境变量 FEISHU_REFRESH_TOKEN 获取）
        chat_id: 群组ID（从环境变量 FEISHU_CHAT_ID 获取）
    
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
        if not refresh_token:
            refresh_token = os.getenv("FEISHU_REFRESH_TOKEN", "")
        if not chat_id:
            chat_id = os.getenv("FEISHU_CHAT_ID", "")
        
        # 检查配置
        if not app_id or not app_secret:
            logger.error("飞书应用ID或密钥未配置")
            return False
        
        if not chat_id:
            logger.error("群组ID (FEISHU_CHAT_ID) 未配置")
            return False
        
        # 自动获取/刷新 user_access_token
        user_access_token = _get_user_access_token(app_id, app_secret, refresh_token)
        if not user_access_token:
            logger.error("无法获取 user_access_token")
            return False
        
        url = f"{_FEISHU_API_BASE_URL}/im/v1/messages?receive_id_type=chat_id"
        headers = {
            "Authorization": f"Bearer {user_access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        payload = {
            "receive_id": chat_id,
            "msg_type": "text",
            "content": json.dumps({
                "text": message
            })
        }
        
        logger.info("以用户身份发送消息")
        response = requests.post(url, headers=headers, json=payload, timeout=_REQUEST_TIMEOUT)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") == 0:
            logger.info("用户身份消息发送成功")
            return True
        else:
            logger.error(f"用户身份消息发送失败: {result.get('msg', '未知错误')}")
            return False
            
    except Exception as e:
        logger.error(f"发送用户身份消息失败: {str(e)}", exc_info=True)
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

def send_futures_report_and_teaching(report_file_path: Optional[str] = None) -> bool:
    """发送期货报告 + IC滚动教学（工作流调用入口）"""
    import glob
    if not report_file_path:
        files = sorted(glob.glob('data/futures/futures_report_*.txt'), reverse=True)
        report_file_path = files[0] if files else None
    if not report_file_path:
        send_feishu_message("⚠️ 期货数据获取成功，但未找到报告文件")
        return False
    
    with open(report_file_path, 'r', encoding='utf-8') as f:
        report = f.read()
    
    send_feishu_message(report)
    print("✅ 消息1: 期货报告已发送")
    
    ic_prices = {}
    for line in report.split('\n'):
        line = line.strip()
        for code in ['IC01', 'IC03', 'IC06', 'IC09']:
            if f'{code}:' in line and 'IC' in line:
                parts = line.split(f'{code}:')
                if len(parts) > 1:
                    price_str = parts[1].strip().split()[0]
                    try:
                        ic_prices[code] = float(price_str)
                    except:
                        pass
    
    p = ic_prices
    if len(p) >= 4:
        ic01, ic03, ic06, ic09 = p.get('IC01', 0), p.get('IC03', 0), p.get('IC06', 0), p.get('IC09', 0)
        spread_06 = round(ic06 - ic01, 2) if ic01 else 0
        pct_06 = round(spread_06 / ic01 * 100, 2) if ic01 else 0
        teaching = (
            f"🦀 【IC期货滚动实战教学】\n\n"
            f"📊 当前行情数据：\n"
            f"┌──────────┬────────────┬────────────┐\n"
            f"│ 合约     │ 价格       │ 升水/贴水  │\n"
            f"├──────────┼────────────┼────────────┤\n"
            f"│ IC01(当月)│ {ic01:.2f}   │ 基准       │\n"
            f"│ IC03      │ {ic03:.2f}   │ +{round(ic03-ic01,2):.2f}       │\n"
            f"│ IC06      │ {ic06:.2f}   │ +{spread_06:.2f}({pct_06:+.1f}%)│\n"
            f"│ IC09      │ {ic09:.2f}   │ +{round(ic09-ic01,2):.2f}       │\n"
            f"└──────────┴────────────┴────────────┘\n\n"
            f"💡 核心概念：「滚动」= 平掉近月合约 + 开仓远月合约\n\n"
            f"📌 当前价差结构分析：\n"
            f"• IC01→IC06 价差: {spread_06:+.2f}点 ({'远月升水' if spread_06 > 0 else '远月贴水'})\n\n"
            f"⚠️ 滚动时机判断：\n"
            f"✅ 适合滚动: 近月到期<5日 | 升水合理 | 持仓方向与趋势一致\n"
            f"❌ 不适合: 远月贴水 | 价差异常扩大 | 流动性不足\n\n"
            f"🎯 实战建议：\n"
            f"• 持有多单: 距离到期还有时间，暂不急于滚动\n"
            f"• 持有空单: 远月升水有利(空近买远=赚升水)，可等待更优时机\n\n"
            f"📅 下一步: 每日监控价差 | 关注中证500指数 | 到期前5日必须完成滚动\n\n"
            f"==================\n"
            f"📊 数据来源: Git-fish-etf 工作流自动推送"
        )
    else:
        teaching = (
            "🦀 【IC期货滚动实战教学】\n\n"
            "⚠️ 无法从报告中解析IC价格数据\n\n"
            "💡 核心概念：「滚动」= 平掉近月合约 + 开仓远月合约\n\n"
            "⚠️ 滚动时机判断：\n"
            "✅ 适合滚动: 近月到期<5日 | 升水合理 | 持仓方向与趋势一致\n"
            "❌ 不适合: 远月贴水 | 价差异常扩大 | 流动性不足\n\n"
            "==================\n"
            "📊 数据来源: Git-fish-etf 工作流自动推送"
        )
    
    send_feishu_message(teaching)
    print("✅ 消息2: IC滚动教学已发送")
    return True

try:
    Config.init_dirs()
    logger.info("飞书推送模块初始化完成")
    
    if os.getenv("DEBUG", "").lower() in ("true", "1", "yes"):
        logger.info("调试模式启用，测试飞书连接")
        test_connection()
except Exception as e:
    logger.error(f"飞书推送模块初始化失败: {str(e)}", exc_info=True)
