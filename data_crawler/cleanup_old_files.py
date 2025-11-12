#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理旧文件脚本（带Git提交功能）
功能：
1. 清理 data/flags 和 data/logs 目录下超过15天的文件
2. 确保只删除指定目录下的文件，其他文件不受影响
3. 将删除操作提交到Git仓库
4. 通过微信推送详细清理结果
"""

import os
import time
import logging
import shutil
from datetime import datetime, timedelta
from config import Config
from wechat_push.push import send_wechat_message
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files, _verify_git_file_content

# 初始化日志
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
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

def cleanup_old_files(directory: str, days: int) -> tuple:
    """
    清理指定目录中超过指定天数的文件，并记录Git操作
    
    Args:
        directory: 要清理的目录路径
        days: 保留文件的天数阈值
    
    Returns:
        tuple: (成功标志, 删除文件列表, 错误信息)
    """
    if not os.path.exists(directory):
        return True, [], f"目录不存在: {directory}"
    
    cutoff_time = time.time() - (days * 24 * 3600)
    deleted_files = []
    errors = []
    
    # 遍历目录中的所有文件（不递归子目录）
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        
        # 只处理文件，跳过目录
        if os.path.isfile(file_path):
            try:
                # 检查文件最后修改时间
                if os.path.getmtime(file_path) < cutoff_time:
                    # 检查文件是否在Git仓库中
                    if _verify_git_file_content(file_path):
                        logger.info(f"文件 {file_path} 已在Git仓库中")
                    
                    # 先删除本地文件
                    os.remove(file_path)
                    deleted_files.append(filename)
                    logger.info(f"已删除: {file_path}")
            except Exception as e:
                error_msg = f"删除 {filename} 失败: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
    
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
        # 使用与原始代码相同的Git提交方式
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

def main():
    """主清理程序"""
    cleanup_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fifteen_days_ago = (datetime.now() - timedelta(days=DAYS_THRESHOLD)).strftime("%Y-%m-%d")
    success = True
    results = {}
    total_deleted = 0
    
    logger.info(f"=== 开始清理旧文件 ({cleanup_time}) ===")
    logger.info(f"清理阈值: {DAYS_THRESHOLD}天前 ({fifteen_days_ago})")
    
    # 处理每个指定目录
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
    
    # 构建微信消息
    if total_deleted > 0:
        message = f"✅ 成功清理 {total_deleted} 个文件（15天前）\n"
        message += "所有删除操作已提交到Git仓库\n\n"
        
        for dir_name, res in results.items():
            if res["deleted_files"]:
                message += f"📁 {dir_name} 目录:\n"
                message += f"  - 已删除 {len(res['deleted_files'])} 个文件\n"
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
        
        # 检查是否有错误
        for dir_name, res in results.items():
            if not res["success"] and res["error"]:
                success = False
                message += f"\n\n⚠️ {dir_name} 目录清理失败:\n{res['error']}"
    
    # 确定消息类型
    message_type = "success" if success and total_deleted > 0 else "info"
    if not success:
        message_type = "error"
    
    # 推送微信消息
    try:
        send_wechat_message(
            message=message,
            message_type=message_type
        )
        logger.info("微信消息推送成功")
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
    
    # 打印最终状态
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