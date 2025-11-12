#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理旧文件脚本（最终修复版）
功能：
1. 严格清理 data/flags 和 data/logs 目录下超过15天的文件
2. 仅生成简洁的清理结果摘要（避免消息过长）
3. 不处理Git提交（由工作流统一处理）
4. 修复微信消息发送逻辑，确保消息简洁
"""

import os
import logging
import shutil
import re
import pytz
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time
from wechat_push.push import send_wechat_message

# 初始化日志
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("cleanup.log"),
        logging.StreamHandler()
    ]
)

# 配置清理参数
DAYS_THRESHOLD = 15
FLAGS_DIR = os.path.join(Config.DATA_DIR, "flags")
LOGS_DIR = os.path.join(Config.DATA_DIR, "logs")
CLEANUP_DIRS = {
    "flags": FLAGS_DIR,
    "logs": LOGS_DIR
}

def extract_date_from_filename(filename: str) -> datetime:
    """
    从文件名中提取日期
    支持的日期格式：
    1. YYYYMMDD（如20251031）
    2. YYYY-MM-DD（如2025-10-31）
    3. YYYYMMDD_HHMMSS（如20250827_065100）
    
    返回北京时间的datetime对象，如果无法解析则返回None
    """
    # 尝试匹配YYYYMMDD格式
    pattern1 = r'(\d{8})'
    match = re.search(pattern1, filename)
    if match:
        date_str = match.group(1)
        try:
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            return date_obj.replace(tzinfo=pytz.timezone('Asia/Shanghai'))
        except ValueError:
            pass
    
    # 尝试匹配YYYY-MM-DD格式
    pattern2 = r'(\d{4}-\d{2}-\d{2})'
    match = re.search(pattern2, filename)
    if match:
        date_str = match.group(1)
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            return date_obj.replace(tzinfo=pytz.timezone('Asia/Shanghai'))
        except ValueError:
            pass
    
    # 尝试匹配YYYYMMDD_HHMMSS格式
    pattern3 = r'(\d{8})_\d{6}'
    match = re.search(pattern3, filename)
    if match:
        date_str = match.group(1)
        try:
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            return date_obj.replace(tzinfo=pytz.timezone('Asia/Shanghai'))
        except ValueError:
            pass
    
    return None

def get_file_time_beijing(file_path: str) -> datetime:
    """
    获取文件的时间，并转换为北京时间
    只使用文件名日期进行判断
    """
    try:
        filename = os.path.basename(file_path)
        file_date = extract_date_from_filename(filename)
        if file_date:
            return file_date
        
        # 如果无法从文件名提取日期，则回退到修改时间
        timestamp = os.path.getmtime(file_path)
        file_time = datetime.fromtimestamp(timestamp)
        if file_time.tzinfo is None:
            file_time = file_time.replace(tzinfo=pytz.utc)
        return file_time.astimezone(pytz.timezone('Asia/Shanghai'))
    except Exception as e:
        logger.error(f"获取文件 {file_path} 时间失败: {str(e)}")
        return None

def cleanup_old_files(directory: str, days: int) -> tuple:
    """
    清理指定目录中超过指定天数的文件
    
    Args:
        directory: 要清理的目录路径
        days: 保留文件的天数阈值
    
    Returns:
        tuple: (成功标志, 删除文件列表, 错误信息)
    """
    if not os.path.exists(directory):
        return True, [], f"目录不存在: {directory}"
    
    # 使用与原始爬虫一致的北京时间计算
    beijing_time = get_beijing_time()
    cutoff_time = beijing_time - timedelta(days=days)
    
    deleted_files = []
    errors = []
    total_files = 0
    old_files = 0
    
    # 确保临时目录存在
    temp_dir = os.path.join(Config.DATA_DIR, "temp", "cleanup_backup")
    os.makedirs(temp_dir, exist_ok=True)
    
    # 遍历目录中的所有文件（不递归子目录）
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        total_files += 1
        
        # 只处理文件，跳过目录
        if os.path.isfile(file_path):
            try:
                # 获取文件的北京时间
                file_time_beijing = get_file_time_beijing(file_path)
                if not file_time_beijing:
                    continue
                
                # 检查文件日期
                if file_time_beijing < cutoff_time:
                    old_files += 1
                    
                    # 先备份文件到临时目录（安全操作）
                    backup_path = os.path.join(temp_dir, filename)
                    shutil.copy2(file_path, backup_path)
                    
                    # 确认可以安全删除后，再删除文件
                    os.remove(file_path)
                    deleted_files.append(filename)
                    logger.info(f"已删除: {file_path} (文件名日期: {file_time_beijing.strftime('%Y-%m-%d %H:%M:%S')})")
            except Exception as e:
                error_msg = f"删除 {filename} 失败: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
    
    logger.info(f"清理统计: 总文件数={total_files}, 超{DAYS_THRESHOLD}天文件数={old_files}, 实际删除文件数={len(deleted_files)}")
    return len(errors) == 0, deleted_files, "\n".join(errors) if errors else ""

def main():
    """主清理程序"""
    # 确保使用北京时间
    beijing_time = get_beijing_time()
    cleanup_time = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
    fifteen_days_ago = (beijing_time - timedelta(days=DAYS_THRESHOLD)).strftime("%Y-%m-%d")
    success = True
    results = {}
    total_deleted = 0
    
    logger.info(f"=== 开始清理旧文件 ({cleanup_time}) ===")
    logger.info(f"清理阈值: {DAYS_THRESHOLD}天前 ({fifteen_days_ago})")
    logger.info(f"当前北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"服务器时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # 处理每个指定目录
    for dir_name, directory in CLEANUP_DIRS.items():
        logger.info(f"开始清理 {directory} 目录...")
        dir_success, deleted_files, error_msg = cleanup_old_files(directory, DAYS_THRESHOLD)
        
        if error_msg:
            logger.error(f"{directory} 清理错误: {error_msg}")
        
        results[dir_name] = {
            "success": dir_success,
            "deleted_files": deleted_files,
            "error": error_msg
        }
        total_deleted += len(deleted_files)
        success = success and dir_success
    
    # 构建极简的清理摘要
    if total_deleted > 0:
        message = f"✅ 文件清理完成\n\n"
        for dir_name, res in results.items():
            if res["deleted_files"]:
                message += f"📁 {dir_name}:\n"
                message += f"  - 删除: {len(res['deleted_files'])} 个\n"
        message += f"\n清理时间: {cleanup_time}\n"
        message += f"阈值: {DAYS_THRESHOLD}天前"
    else:
        message = f"ℹ️ 未发现需要清理的文件\n"
        message += f"阈值: {DAYS_THRESHOLD}天前\n"
        message += f"清理时间: {cleanup_time}"
    
    # 添加错误信息（如果有）
    for dir_name, res in results.items():
        if not res["success"] and res["error"]:
            success = False
            message += f"\n\n⚠️ {dir_name} 错误:\n{res['error']}"
    
    # 推送微信消息
    sent_success = False
    try:
        sent_success = send_wechat_message(message)
        if sent_success:
            logger.info("✅ 微信消息推送成功")
        else:
            logger.error("❌ 微信消息推送失败")
    except Exception as e:
        logger.error(f"❌ 微信消息推送失败: {str(e)}")
    
    # 打印最终状态
    if success:
        if sent_success:
            logger.info(f"✅ 清理完成 - 成功删除 {total_deleted} 个文件")
        else:
            logger.error(f"⚠️ 清理完成 - 清理操作成功但微信消息发送失败")
    else:
        logger.error("❌ 清理完成 - 但存在错误")
        if not sent_success:
            logger.error("❌ 微信消息推送失败")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error_msg = f"清理脚本执行失败: {str(e)}"
        logger.exception(error_msg)
        try:
            send_wechat_message(
                f"❌ 清理脚本执行失败:\n{error_msg}"
            )
        except:
            pass
        raise
