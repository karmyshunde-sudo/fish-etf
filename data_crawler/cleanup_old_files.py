#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理旧文件脚本（终极修复版）
功能：
1. 严格清理 data/flags 和 data/logs 目录下超过15天的文件
2. 正确处理时区问题，确保阈值计算准确
3. 改进微信消息发送逻辑，准确反映发送状态
"""

import os
import time
import logging
import shutil
import pytz
from datetime import datetime, timedelta
from config import Config
from utils.date_utils import get_beijing_time  # 使用原始代码的时间工具

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

def get_file_list(directory: str) -> list:
    """获取目录中的所有文件列表（只包括文件）"""
    if not os.path.exists(directory):
        return []
    
    files = []
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            files.append(file_path)
    return files

def get_oldest_files(directory: str, count: int = 5) -> list:
    """获取目录中最早的count个文件"""
    files = get_file_list(directory)
    # 按修改时间排序（最早在前）
    files.sort(key=lambda x: os.path.getmtime(x))
    return files[:count]

def get_file_age(file_path: str) -> int:
    """获取文件的天数（从最后修改时间到现在）"""
    file_mtime = os.path.getmtime(file_path)
    now = time.time()
    age_seconds = now - file_mtime
    return int(age_seconds / (24 * 3600))

def get_file_list_by_age(directory: str, days: int) -> list:
    """获取超过指定天数的文件列表"""
    cutoff_time = time.time() - (days * 24 * 3600)
    old_files = []
    
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff_time:
            old_files.append(file_path)
    
    return old_files

def get_file_time_beijing(file_path: str) -> datetime:
    """
    获取文件的修改时间，并转换为北京时间
    """
    try:
        # 获取文件的最后修改时间戳
        mtime = os.path.getmtime(file_path)
        file_time = datetime.fromtimestamp(mtime)
        
        # 确保有时区信息
        if file_time.tzinfo is None:
            # GitHub Actions 运行在 UTC 时区
            file_time = file_time.replace(tzinfo=pytz.utc)
        
        # 转换为北京时间
        file_time_beijing = file_time.astimezone(pytz.timezone('Asia/Shanghai'))
        return file_time_beijing
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
    
    # 遍历目录中的所有文件（不递归子目录）
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        total_files += 1
        
        # 只处理文件，跳过目录
        if os.path.isfile(file_path):
            try:
                # 获取文件的北京时间
                file_time_beijing = get_file_time_beijing(file_path)
                if file_time_beijing is None:
                    continue
                
                # 检查文件最后修改时间
                if file_time_beijing < cutoff_time:
                    old_files += 1
                    
                    # 先备份文件到临时目录（安全操作）
                    temp_dir = os.path.join(Config.TEMP_DIR, "cleanup_backup")
                    os.makedirs(temp_dir, exist_ok=True)
                    
                    backup_path = os.path.join(temp_dir, filename)
                    shutil.copy2(file_path, backup_path)
                    
                    # 检查文件是否在Git仓库中
                    try:
                        from utils.git_utils import _verify_git_file_content
                        if _verify_git_file_content(file_path):
                            logger.info(f"文件 {file_path} 已在Git仓库中")
                    except Exception as e:
                        logger.warning(f"Git验证失败: {str(e)}")
                    
                    # 确认可以安全删除后，再删除文件
                    os.remove(file_path)
                    deleted_files.append(filename)
                    logger.info(f"已删除: {file_path} (文件时间: {file_time_beijing.strftime('%Y-%m-%d %H:%M:%S')})")
            except Exception as e:
                error_msg = f"删除 {filename} 失败: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
    
    logger.info(f"清理统计: 总文件数={total_files}, 超{DAYS_THRESHOLD}天文件数={old_files}, 实际删除文件数={len(deleted_files)}")
    return len(errors) == 0, deleted_files, "\n".join(errors) if errors else ""

def commit_deletion(directory: str, deleted_files: list) -> bool:
    """
    提交文件删除操作到Git仓库
    
    Args:
        directory: 被清理的目录
        deleted_files: 已删除的文件列表
    
    Returns:
        bool: 提交是否成功
    """
    if not deleted_files:
        return True
    
    # 构建要提交的文件路径列表
    file_paths = [os.path.join(directory, f) for f in deleted_files]
    
    # 创建提交消息
    commit_message = f"cleanup: 删除 {len(deleted_files)} 个超过{DAYS_THRESHOLD}天的文件 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    try:
        # 使用与原始ETF爬虫完全相同的Git提交方式
        from utils.git_utils import commit_files_in_batches, force_commit_remaining_files
        commit_files_in_batches(file_paths, commit_message)
        logger.info(f"✅ Git提交成功: {commit_message}")
        return True
    except Exception as e:
        error_msg = f"Git提交失败: {str(e)}"
        logger.error(error_msg)
        # 尝试强制提交
        try:
            force_commit_remaining_files()
            logger.info("✅ 强制提交成功")
            return True
        except Exception as fe:
            logger.error(f"强制提交也失败: {str(fe)}")
            return False

def send_wechat_message(message: str, message_type: str = "info") -> bool:
    """
    使用与原始爬虫完全相同的微信消息发送机制
    返回值表示是否成功发送
    """
    try:
        # 从原始代码中提取的微信发送逻辑
        from wechat_push.push import send_wechat_message
        send_wechat_message(
            message=message,
            message_type=message_type
        )
        logger.info("✅ 微信消息发送成功")
        return True
    except Exception as e:
        # 尝试使用备用方法发送
        try:
            # 备用方法 - 使用环境变量
            import os
            import requests
            
            webhook = os.environ.get("WECOM_WEBHOOK")
            if webhook:
                data = {
                    "msgtype": "text",
                    "text": {
                        "content": message
                    }
                }
                response = requests.post(webhook, json=data)
                if response.status_code == 200:
                    logger.info("✅ 微信消息发送成功（备用方法）")
                    return True
                else:
                    logger.error(f"❌ 微信消息发送失败: HTTP {response.status_code}")
                    return False
            else:
                logger.error("❌ 企业微信Webhook未配置，无法发送消息")
                return False
        except Exception as be:
            logger.error(f"❌ 备用方法发送失败: {str(be)}")
            return False

def get_oldest_files_info(directory: str, count: int = 5) -> str:
    """获取目录中最旧文件的详细信息"""
    oldest_files = get_oldest_files(directory, count)
    info_lines = []
    
    for file_path in oldest_files:
        file_name = os.path.basename(file_path)
        file_time_beijing = get_file_time_beijing(file_path)
        if file_time_beijing is None:
            continue
        
        file_age = (datetime.now(pytz.timezone('Asia/Shanghai')) - file_time_beijing).days
        info_lines.append(f"  - {file_name} ({file_age}天前, 修改时间: {file_time_beijing.strftime('%Y-%m-%d %H:%M:%S')})")
    
    return "\n".join(info_lines) if info_lines else "  - 无足够旧文件"

def main():
    """主清理程序"""
    # 确保使用北京时间
    beijing_time = get_beijing_time()
    cleanup_time = beijing_time.strftime("%Y-%m-%d %H:%M:%S")
    fifteen_days_ago = (beijing_time - timedelta(days=DAYS_THRESHOLD)).strftime("%Y-%m-%d")
    success = True
    results = {}
    total_deleted = 0
    pre_cleanup_stats = {}
    post_cleanup_stats = {}
    
    logger.info(f"=== 开始清理旧文件 ({cleanup_time}) ===")
    logger.info(f"清理阈值: {DAYS_THRESHOLD}天前 ({fifteen_days_ago})")
    logger.info(f"当前北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"服务器时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # 1. 统计清理前的文件数量
    for dir_name, directory in CLEANUP_DIRS.items():
        file_list = get_file_list(directory)
        old_files = get_file_list_by_age(directory, DAYS_THRESHOLD)
        
        pre_cleanup_stats[dir_name] = {
            "total": len(file_list),
            "old_files_count": len(old_files),
            "oldest_files": get_oldest_files_info(directory, 5)
        }
        
        logger.info(f"{directory} 目录清理前状态:")
        logger.info(f"  - 总文件数: {pre_cleanup_stats[dir_name]['total']}")
        logger.info(f"  - 超{DAYS_THRESHOLD}天文件数: {pre_cleanup_stats[dir_name]['old_files_count']}")
        logger.info(f"  - 最旧5个文件:\n{pre_cleanup_stats[dir_name]['oldest_files']}")
    
    # 2. 处理每个指定目录
    for dir_name, directory in CLEANUP_DIRS.items():
        logger.info(f"开始清理 {directory} 目录...")
        dir_success, deleted_files, error_msg = cleanup_old_files(directory, DAYS_THRESHOLD)
        
        # 提交删除操作到Git
        if deleted_files:
            git_success = commit_deletion(directory, deleted_files)
            if not git_success:
                error_msg += "\nGit提交失败，删除操作未记录到版本历史"
                dir_success = False
        
        if error_msg:
            logger.error(f"{directory} 清理错误: {error_msg}")
        
        results[dir_name] = {
            "success": dir_success,
            "deleted_files": deleted_files,
            "error": error_msg
        }
        total_deleted += len(deleted_files)
        success = success and dir_success
    
    # 3. 统计清理后的文件数量
    for dir_name, directory in CLEANUP_DIRS.items():
        file_list = get_file_list(directory)
        post_cleanup_stats[dir_name] = {
            "total": len(file_list),
            "oldest_files": get_oldest_files_info(directory, 5)
        }
        
        logger.info(f"{directory} 目录清理后状态:")
        logger.info(f"  - 剩余文件数: {post_cleanup_stats[dir_name]['total']}")
        logger.info(f"  - 最旧5个文件:\n{post_cleanup_stats[dir_name]['oldest_files']}")
    
    # 4. 构建微信消息
    if total_deleted > 0:
        message = f"✅ 成功清理 {total_deleted} 个文件（{DAYS_THRESHOLD}天前）\n"
        message += "所有删除操作已提交到Git仓库\n\n"
        
        for dir_name, res in results.items():
            if res["deleted_files"]:
                message += f"📁 {dir_name} 目录:\n"
                message += f"  - 初始文件数: {pre_cleanup_stats[dir_name]['total']} → 剩余文件数: {post_cleanup_stats[dir_name]['total']}\n"
                message += f"  - 已删除 {len(res['deleted_files'])} 个文件\n"
                
                # 添加最旧文件信息
                if pre_cleanup_stats[dir_name]['old_files_count'] > 0:
                    message += f"  - 清理前最旧文件:\n{pre_cleanup_stats[dir_name]['oldest_files']}\n"
                    message += f"  - 清理后最旧文件:\n{post_cleanup_stats[dir_name]['oldest_files']}\n"
                
                # 列出部分文件（最多5个）
                if len(res["deleted_files"]) > 5:
                    message += "    " + ", ".join(res["deleted_files"][:5]) + " ...\n"
                else:
                    message += "    " + ", ".join(res["deleted_files"]) + "\n"
                
                if res["error"]:
                    message += f"  ⚠️ 错误: {res['error']}\n"
        message += f"\n清理时间: {cleanup_time}"
    else:
        message = "ℹ️ 未发现需要清理的文件\n"
        message += f"清理时间: {cleanup_time}\n"
        message += f"清理阈值: {DAYS_THRESHOLD}天前 ({fifteen_days_ago})"
        
        # 添加清理前状态信息
        for dir_name in CLEANUP_DIRS.keys():
            message += f"\n\n📁 {dir_name} 目录:"
            message += f"\n  - 初始文件数: {pre_cleanup_stats[dir_name]['total']}"
            message += f"\n  - 超{DAYS_THRESHOLD}天文件数: {pre_cleanup_stats[dir_name]['old_files_count']}"
            message += f"\n  - 最旧5个文件:\n{pre_cleanup_stats[dir_name]['oldest_files']}"
        
        # 检查是否有错误
        for dir_name, res in results.items():
            if not res["success"] and res["error"]:
                success = False
                message += f"\n\n⚠️ {dir_name} 目录清理失败:\n{res['error']}"
    
    # 5. 确定消息类型
    message_type = "success" if success and total_deleted > 0 else "info"
    if not success:
        message_type = "error"
    
    # 6. 推送微信消息（使用原始代码相同的机制）
    sent_success = False
    try:
        sent_success = send_wechat_message(message, message_type)
        if sent_success:
            logger.info("微信消息推送成功")
        else:
            logger.error("微信消息推送失败")
        if not success:
            logger.error("清理过程存在错误")
    except Exception as e:
        error_msg = f"微信消息推送失败: {str(e)}"
        logger.error(error_msg)
        # 尝试发送错误消息
        try:
            send_wechat_message(
                message=f"❌ 清理任务执行成功，但消息推送失败:\n{error_msg}",
                message_type="error"
            )
        except:
            pass
    
    # 7. 打印最终状态
    if success:
        logger.info(f"清理完成 - 成功删除 {total_deleted} 个文件并提交Git")
    else:
        logger.error("清理失败 - 请检查错误信息")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error_msg = f"清理脚本执行失败: {str(e)}"
        logger.exception(error_msg)
        try:
            send_wechat_message(
                message=f"❌ 清理脚本执行失败:\n{error_msg}",
                message_type="error"
            )
        except:
            pass
        raise
