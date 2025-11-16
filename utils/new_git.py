#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新版通用Git工具模块
【完全通用设计 - 不包含任何硬编码路径】
- 支持任意目录的文件提交
- 支持单个文件、小批次、剩余文件提交
- 完全解耦，可被任何模块调用
- 线程安全，错误处理完善
"""

import os
import logging
import subprocess
import time
import threading
import requests
from datetime import datetime
import base64

# 初始化日志
logger = logging.getLogger(__name__)

# 线程锁，确保Git操作线程安全
_git_lock = threading.Lock()

def get_repo_root():
    """获取仓库根目录"""
    return os.environ.get('GITHUB_WORKSPACE', os.getcwd())

def get_current_branch():
    """获取当前分支名称"""
    branch = os.environ.get('GITHUB_REF', 'main')
    if branch.startswith('refs/heads/'):
        return branch.split('refs/heads/')[1]
    return branch

def get_github_token():
    """安全获取GitHub令牌"""
    return os.environ.get('GITHUB_TOKEN', '').strip()

def wait_for_git_unlock(repo_root, max_retries=15, retry_delay=2):
    """等待 Git 索引锁释放"""
    index_lock = os.path.join(repo_root, '.git', 'index.lock')
    retry_count = 0
    
    while os.path.exists(index_lock) and retry_count < max_retries:
        logger.warning(f"Git索引锁存在，等待解锁... ({retry_count+1}/{max_retries})")
        time.sleep(retry_delay)
        retry_count += 1
    
    if os.path.exists(index_lock):
        logger.error("Git索引锁长时间存在，强制删除")
        try:
            os.remove(index_lock)
            return True
        except Exception as e:
            logger.error(f"无法删除索引锁: {str(e)}")
            return False
    
    return True

def verify_git_file_content(file_path):
    """
    验证文件内容是否真正存在于远程仓库
    """
    try:
        repo_root = get_repo_root()
        relative_path = os.path.relpath(file_path, repo_root)
        repo = os.environ.get('GITHUB_REPOSITORY')
        token = get_github_token()
        branch = get_current_branch()
        
        # 尝试使用GitHub API验证
        if token and repo:
            url = f"https://api.github.com/repos/{repo}/contents/{relative_path}?ref={branch}"
            headers = {
                "Authorization": f"token {token}",
                "Accept": "application/vnd.github.v3+json"
            }
            
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                remote_content = response.json()['content']
                with open(file_path, "rb") as f:
                    local_content = f.read()
                
                local_content_str = local_content.decode('utf-8').replace('\r\n', '\n')
                remote_content_str = base64.b64decode(remote_content).decode('utf-8').replace('\r\n', '\n')
                
                if local_content_str == remote_content_str:
                    logger.info("✅ 文件内容验证通过：工作目录与远程Git仓库一致")
                    return True
                else:
                    logger.error("❌ 文件内容不匹配：工作目录与远程Git仓库不一致")
                    return False
            else:
                logger.warning(f"API验证失败: HTTP {response.status_code}")
        
        # 尝试使用git ls-remote验证
        result = subprocess.run(
            ["git", "ls-tree", "-r", f"origin/{branch}", "--name-only", relative_path],
            cwd=repo_root,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 and result.stdout.strip():
            logger.info("✅ 文件存在验证通过：文件存在于远程Git仓库")
            return True
        else:
            logger.error("❌ 文件不存在于远程仓库")
            return False
    
    except Exception as e:
        logger.error(f"验证远程文件失败: {str(e)}", exc_info=True)
        return False

def safe_git_commit_files(file_paths, commit_message, max_retries=3):
    """
    通用的安全Git提交函数
    
    Args:
        file_paths: 文件路径列表或单个文件路径
        commit_message: 提交消息
        max_retries: 最大重试次数
    
    Returns:
        bool: 操作是否成功
    """
    repo_root = get_repo_root()
    
    # 确保file_paths是列表
    if not isinstance(file_paths, list):
        file_paths = [file_paths]
    
    # 获取线程锁，确保Git操作线程安全
    with _git_lock:
        for attempt in range(max_retries):
            try:
                # 1. 等待Git解锁
                if not wait_for_git_unlock(repo_root):
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    return False
                
                # 2. 清理Git状态
                logger.info("🔄 清理Git状态...")
                try:
                    subprocess.run(['git', 'reset', '--hard', 'HEAD'], check=True, cwd=repo_root)
                    subprocess.run(['git', 'clean', '-fd'], check=True, cwd=repo_root)
                except Exception as e:
                    logger.warning(f"Git状态清理警告: {e}")
                
                # 3. 拉取最新更改
                logger.info("🔄 拉取远程更新...")
                try:
                    subprocess.run(['git', 'pull', '--rebase'], check=True, cwd=repo_root)
                except Exception as e:
                    logger.warning(f"拉取远程更新警告: {e}")
                
                # 4. 添加文件到暂存区
                logger.info(f"📁 添加 {len(file_paths)} 个文件到暂存区...")
                files_added = False
                for file_path in file_paths:
                    if file_path and os.path.exists(file_path):
                        subprocess.run(['git', 'add', file_path], check=True, cwd=repo_root)
                        logger.debug(f"✅ 已添加: {file_path}")
                        files_added = True
                    else:
                        logger.warning(f"⚠️ 文件不存在: {file_path}")
                
                if not files_added:
                    logger.info("📝 没有文件需要添加")
                    return True
                
                # 5. 检查是否有变更
                result = subprocess.run(
                    ['git', 'diff', '--cached', '--exit-code'], 
                    cwd=repo_root, 
                    capture_output=True,
                    text=True
                )
                
                if result.returncode == 0:
                    logger.info("📝 没有变更需要提交")
                    return True
                
                # 6. 提交
                logger.info(f"💾 提交更改: {commit_message}")
                subprocess.run(['git', 'commit', '-m', commit_message], check=True, cwd=repo_root)
                
                # 7. 推送
                logger.info("🚀 推送到远程仓库...")
                subprocess.run(['git', 'push'], check=True, cwd=repo_root)
                
                # 8. 验证提交
                for file_path in file_paths:
                    if file_path and os.path.exists(file_path):
                        if verify_git_file_content(file_path):
                            logger.info(f"✅ 文件验证通过: {os.path.basename(file_path)}")
                        else:
                            logger.warning(f"⚠️ 文件验证警告: {os.path.basename(file_path)}")
                
                logger.info("✅ Git提交成功")
                return True
                
            except Exception as e:
                logger.error(f"Git提交失败 (尝试 {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"⏳ 将在 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error("❌ Git提交失败，已达最大重试次数")
                    return False

def commit_single_file(file_path, commit_message):
    """
    提交单个文件
    
    Args:
        file_path: 单个文件路径
        commit_message: 提交消息
    
    Returns:
        bool: 操作是否成功
    """
    try:
        # 确保提交消息包含 [skip ci]
        if "[skip ci]" not in commit_message:
            commit_message = f"{commit_message} [skip ci]"
        
        logger.info(f"提交单个文件: {os.path.basename(file_path)}")
        return safe_git_commit_files([file_path], commit_message)
    
    except Exception as e:
        logger.error(f"提交单个文件失败: {str(e)}", exc_info=True)
        return False

def commit_batch_files(file_paths, commit_message=None):
    """
    提交一批文件（通用批次提交）
    
    Args:
        file_paths: 文件路径列表
        commit_message: 提交消息（可选）
    
    Returns:
        bool: 操作是否成功
    """
    try:
        if not file_paths:
            logger.info("文件列表为空，无需提交")
            return True
        
        # 创建提交消息
        if not commit_message:
            commit_message = f"feat: 批量提交{len(file_paths)}个文件 [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
        else:
            # 确保所有自定义提交消息也包含 [skip ci]
            if "[skip ci]" not in commit_message:
                commit_message = f"{commit_message} [skip ci]"
        
        logger.info(f"批量提交 {len(file_paths)} 个文件: {commit_message}")
        return safe_git_commit_files(file_paths, commit_message)
        
    except Exception as e:
        logger.error(f"批量提交文件失败: {str(e)}", exc_info=True)
        return False

def commit_remaining_files(file_paths, commit_message=None):
    """
    提交剩余文件（不足一个完整批次的文件）
    
    Args:
        file_paths: 剩余文件路径列表
        commit_message: 提交消息（可选）
    
    Returns:
        bool: 操作是否成功
    """
    try:
        if not file_paths:
            logger.info("没有剩余文件需要提交")
            return True
        
        # 创建提交消息
        if not commit_message:
            commit_message = f"feat: 提交剩余{len(file_paths)}个文件 [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
        else:
            if "[skip ci]" not in commit_message:
                commit_message = f"{commit_message} [skip ci]"
        
        logger.info(f"提交剩余 {len(file_paths)} 个文件: {commit_message}")
        return safe_git_commit_files(file_paths, commit_message)
        
    except Exception as e:
        logger.error(f"提交剩余文件失败: {str(e)}", exc_info=True)
        return False
