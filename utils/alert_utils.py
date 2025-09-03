#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
紧急警报工具模块
提供系统异常时的紧急通知功能
"""

import logging
import time
from typing import Optional

# 配置日志
logger = logging.getLogger(__name__)

# 最近发送警报的时间戳
_last_alert_time = 0
# 警报冷却时间（秒）
_ALERT_COOLDOWN = 300  # 5分钟

def send_urgent_alert(message: str, priority: int = 1) -> bool:
    """
    发送紧急警报
    
    Args:
        message: 警报消息
        priority: 优先级（1-高，2-中，3-低）
    
    Returns:
        bool: 是否成功发送
    """
    global _last_alert_time
    
    # 检查是否在冷却期内
    current_time = time.time()
    if current_time - _last_alert_time < _ALERT_COOLDOWN:
        logger.warning("警报冷却期内，跳过发送")
        return False
    
    try:
        # 根据优先级设置消息前缀
        prefix = "🚨" if priority == 1 else "⚠️" if priority == 2 else "ℹ️"
        
        # 构建完整消息
        full_message = f"{prefix} 【系统紧急警报】{message}"
        
        # 这里应该调用实际的警报发送函数
        # 例如：send_wechat_message(full_message, message_type="alert")
        logger.error(full_message)
        
        # 更新最后发送时间
        _last_alert_time = current_time
        return True
    
    except Exception as e:
        logger.error(f"发送紧急警报失败: {str(e)}", exc_info=True)
        return False

# 模块初始化
try:
    logger.info("紧急警报工具模块初始化完成")
except Exception as e:
    error_msg = f"紧急警报工具模块初始化失败: {str(e)}"
    logger.error(error_msg)
    
    try:
        # 退回到基础日志
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(error_msg)
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(error_msg)
