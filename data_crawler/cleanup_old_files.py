#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理旧文件脚本（终极修复版 - 优化微信消息发送逻辑）
功能：
1. 严格清理 data/flags 和 data/logs 目录下超过15天的文件
2. 从文件名中提取日期信息进行清理判断
3. 优化微信消息发送逻辑，确保准确报告发送结果
"""

import os
import time
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

def get_oldest_files_by_filename_date(directory: str, count: int = 5) -> list:
    """获取目录中按文件名日期排序的最早的count个文件"""
    files = get_file_list(directory)
    
    # 创建包含文件路径和日期的元组
    files_with_dates = []
    for file_path in files:
        filename = os.path.basename(file_path)
        file_date = extract_date_from_filename(filename)
        if file_date:
            files_with_dates.append((file_path, file_date))
    
    # 按日期排序（最早在前）
    files_with_dates.sort(key=lambda x: x[1])
    
    # 返回文件路径
    return [item[0] for item in files_with_dates[:count]]

def get_oldest_files_by_mtime(directory: str, count: int = 5) -> list:
    """获取目录中按修改时间排序的最早的count个文件"""
    files = get_file_list(directory)
    # 按修改时间排序（最早在前）
    files.sort(key=lambda x: os.path.getmtime(x))
    return files[:count]

def get_file_time_beijing(file_path: str, use_filename_date: bool = True) -> datetime:
    """
    获取文件的时间，并转换为北京时间
    Args:
        file_path: 文件路径
        use_filename_date: 是否使用文件名日期（True）或修改时间（False）
    """
    try:
        if use_filename_date:
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
        else:
            # 使用修改时间
            timestamp = os.path.getmtime(file_path)
            file_time = datetime.fromtimestamp(timestamp)
            if file_time.tzinfo is None:
                file_time = file_time.replace(tzinfo=pytz.utc)
            return file_time.astimezone(pytz.timezone('Asia/Shanghai'))
    except Exception as e:
        logger.error(f"获取文件 {file_path} 时间失败: {str(e)}")
        return None

def get_file_age(file_path: str, use_filename_date: bool = True) -> int:
    """获取文件的天数（从文件名日期或修改时间到现在）"""
    file_time = get_file_time_beijing(file_path, use_filename_date)
    if not file_time:
        return 0
    
    now = datetime.now(pytz.timezone('Asia/Shanghai'))
    age = now - file_time
    return age.days

def get_file_list_by_age(directory: str, days: int) -> list:
    """获取超过指定天数的文件列表"""
    cutoff_time = datetime.now(pytz.timezone('Asia/Shanghai')) - timedelta(days=days)
    old_files = []
    
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            try:
                # 使用文件名日期判断
                file_time = get_file_time_beijing(file_path, True)
                if file_time and file_time < cutoff_time:
                    old_files.append(file_path)
            except Exception as e:
                logger.error(f"文件 {file_path} 时间判断失败: {str(e)}")
    
    return old_files

def get_oldest_files_info(directory: str, count: int = 5, use_filename_date: bool = True) -> str:
    """获取目录中最旧文件的详细信息"""
    if use_filename_date:
        oldest_files = get_oldest_files_by_filename_date(directory, count)
        time_type = "文件名日期"
    else:
        oldest_files = get_oldest_files_by_mtime(directory, count)
        time_type = "修改时间"
    
    info_lines = []
    
    for file_path in oldest_files:
        file_name = os.path.basename(file_path)
        file_time_beijing = get_file_time_beijing(file_path, use_filename_date)
        if not file_time_beijing:
            continue
        
        file_age = (datetime.now(pytz.timezone('Asia/Shanghai')) - file_time_beijing).days
        info_lines.append(f"  - {file_name} ({file_age}天前, {time_type}: {file_time_beijing.strftime('%Y-%m-%d %H:%M:%S')})")
    
    return "\n".join(info_lines) if info_lines else f"  - 无足够旧文件 ({time_type})"

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
                file_time_beijing = get_file_time_beijing(file_path, True)
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

def commit_deletion(directory: str, deleted_files: list) -> bool:
    """
    提交文件删除操作到Git仓库（正确处理已删除文件）
    
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
    
    # 使用与ETF爬取脚本完全一致的commit message格式
    commit_message = f"cleanup: 删除 {len(deleted_files)} 个超过{DAYS_THRESHOLD}天的文件 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    try:
        # 使用Git工具函数执行命令
        from utils.git_utils import run_git_command
        
        # 关键修复：使用 git add -A 添加所有变更（包括已删除文件）
        run_git_command(['git', 'add', '-A'])
        
        # 提交变更
        run_git_command(['git', 'commit', '-m', commit_message])
        
        logger.info(f"✅ Git提交成功: {commit_message}")
        return True
    except Exception as e:
        error_msg = f"Git提交失败: {str(e)}"
        logger.error(error_msg)
        
        # 添加详细的Git状态诊断
        try:
            git_status = run_git_command(['git', 'status', '--short'], capture_output=True)
            logger.error(f"Git状态:\n{git_status}")
        except Exception as se:
            logger.error(f"无法获取Git状态: {str(se)}")
            
        return False

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
            "oldest_files_fname": get_oldest_files_info(directory, 5, True),
            "oldest_files_mtime": get_oldest_files_info(directory, 5, False)
        }
        
        logger.info(f"{directory} 目录清理前状态:")
        logger.info(f"  - 总文件数: {pre_cleanup_stats[dir_name]['total']}")
        logger.info(f"  - 超{DAYS_THRESHOLD}天文件数: {pre_cleanup_stats[dir_name]['old_files_count']}")
        logger.info(f"  - 基于文件名日期的最旧5个文件:\n{pre_cleanup_stats[dir_name]['oldest_files_fname']}")
        logger.info(f"  - 基于修改时间的最旧5个文件:\n{pre_cleanup_stats[dir_name]['oldest_files_mtime']}")
    
    # 2. 处理每个指定目录
    for dir_name, directory in CLEANUP_DIRS.items():
        logger.info(f"开始清理 {directory} 目录...")
        dir_success, deleted_files, error_msg = cleanup_old_files(directory, DAYS_THRESHOLD)
        
        # 提交删除操作到Git（使用与ETF爬取脚本完全一致的方式）
        if deleted_files:
            git_success = commit_deletion(directory, deleted_files)
            if not git_success:
                error_msg += "\nGit提交失败，删除操作未记录到版本历史"
                dir_success = False
                success = False  # 标记为失败
        
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
            "oldest_files_fname": get_oldest_files_info(directory, 5, True),
            "oldest_files_mtime": get_oldest_files_info(directory, 5, False)
        }
        
        logger.info(f"{directory} 目录清理后状态:")
        logger.info(f"  - 剩余文件数: {post_cleanup_stats[dir_name]['total']}")
        logger.info(f"  - 基于文件名日期的最旧5个文件:\n{post_cleanup_stats[dir_name]['oldest_files_fname']}")
        logger.info(f"  - 基于修改时间的最旧5个文件:\n{post_cleanup_stats[dir_name]['oldest_files_mtime']}")
    
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
                    message += f"  - 清理前基于文件名日期的最旧文件:\n{pre_cleanup_stats[dir_name]['oldest_files_fname']}\n"
                    message += f"  - 清理前基于修改时间的最旧文件:\n{pre_cleanup_stats[dir_name]['oldest_files_mtime']}\n"
                    message += f"  - 清理后基于文件名日期的最旧文件:\n{post_cleanup_stats[dir_name]['oldest_files_fname']}\n"
                    message += f"  - 清理后基于修改时间的最旧文件:\n{post_cleanup_stats[dir_name]['oldest_files_mtime']}\n"
                
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
            message += f"\n  - 基于文件名日期的最旧5个文件:\n{pre_cleanup_stats[dir_name]['oldest_files_fname']}"
            message += f"\n  - 基于修改时间的最旧5个文件:\n{pre_cleanup_stats[dir_name]['oldest_files_mtime']}"
        
        # 检查是否有错误
        for dir_name, res in results.items():
            if not res["success"] and res["error"]:
                success = False
                message += f"\n\n⚠️ {dir_name} 目录清理失败:\n{res['error']}"
    
    # 5. 推送微信消息 - 修复：明确区分消息发送状态
    sent_success = False
    try:
        # 正确检查send_wechat_message的返回值
        sent_success = send_wechat_message(message)
        if sent_success:
            logger.info("✅ 微信消息推送成功")
        else:
            logger.error("❌ 微信消息推送失败：企业微信Webhook未配置")
    except Exception as e:
        error_msg = f"❌ 微信消息推送失败: {str(e)}"
        logger.error(error_msg)
        # 不再尝试发送额外的错误消息（避免递归调用）
    
    # 6. 打印最终状态 - 清晰区分不同类型的失败
    if success:
        if sent_success:
            logger.info(f"✅ 清理完成 - 成功删除 {total_deleted} 个文件并提交Git")
        else:
            logger.error(f"⚠️ 清理完成 - 清理操作成功但微信消息发送失败")
    else:
        logger.error("❌ 清理完成 - 但存在错误")
        # 详细报告错误原因
        if not success:
            logger.error("❌ Git提交失败：删除操作未记录到版本历史")
        if not sent_success:
            logger.error("❌ 微信消息推送失败：企业微信Webhook未配置")

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
