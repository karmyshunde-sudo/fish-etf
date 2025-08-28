import os
import requests
from config import Config

def send_wechat_message(message):
    """发送消息到企业微信，自动添加固定末尾"""
    # 从环境变量获取Webhook（优先于配置文件）
    webhook = os.getenv("WECOM_WEBHOOK", Config.WECOM_WEBHOOK)
    if not webhook:
        print("企业微信Webhook未配置，无法发送消息")
        return False
    
    try:
        # 添加固定末尾
        full_message = f"{message}\n\n{Config.WECOM_MESFOOTER}"
        # 企业微信文本消息格式
        payload = {
            "msgtype": "text",
            "text": {
                "content": full_message
            }
        }
        
        response = requests.post(webhook, json=payload, timeout=10)
        response.raise_for_status()
        result = response.json()
        
        if result.get("errcode") == 0:
            print("微信消息发送成功")
            return True
        else:
            print(f"微信消息发送失败：{result.get('errmsg')}")
            return False
    
    except Exception as e:
        print(f"微信消息发送异常：{str(e)}")
        return False
