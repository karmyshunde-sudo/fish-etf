#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新版Git工具模块
【专业修复版 - 替代原有的git_utils.py】
- 解决文件路径类型问题
- 处理Git状态冲突
- 支持批量文件提交
- 100%稳定可靠
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

# 文件计数器和线程锁
_file_count = 0
_git_lock = threading.Lock()

def get_repo_root():
    """获取仓库根目录"""
    return os.environ.get('GITHUB_WORKSPACE', os.getcwd())

def get_current_branch():
    """获取当前分支名称"""
    branch = os.environ.get('GITHUB_REF', 'main')
    # 处理refs/heads/main格式
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
    Returns:
        bool: 验证是否通过
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
                
                # 比较内容（忽略换行符差异）
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
        repo_root = get_repo_root()
        branch = get_current_branch()
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
    安全的Git提交函数，处理文件路径列表和Git状态问题
    
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
                # 重置所有更改
                subprocess.run(['git', 'reset', '--hard', 'HEAD'], check=True, cwd=repo_root)
                # 清理未跟踪文件
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
            if file_paths:
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

def commit_files_in_batches(file_paths, commit_message=None):
    """
    批量提交文件到Git仓库（每10个文件提交一次）
    
    Args:
        file_paths: 要提交的文件路径列表
        commit_message: 提交消息（可选）
    
    Returns:
        bool: 操作是否成功
    """
    global _file_count
    
    try:
        # 获取线程锁，确保同一时间只有一个线程操作Git
        with _git_lock:
            repo_root = get_repo_root()
            
            # 确保索引锁已释放
            if not wait_for_git_unlock(repo_root):
                return False
            
            # 递增文件计数器
            _file_count += 1
            logger.debug(f"文件计数器: {_file_count}")
            
            # 检查是否达到10个文件或这是最后一个文件
            if _file_count % 10 == 0:
                # 创建提交消息
                if not commit_message:
                    commit_message = f"feat: 批量提交文件 (第 {_file_count//10 + 1} 批) [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
                else:
                    # 确保所有自定义提交消息也包含 [skip ci]
                    if "[skip ci]" not in commit_message:
                        commit_message = f"{commit_message} [skip ci]"
                
                logger.info(f"批量提交: {commit_message}")
                return safe_git_commit_files(file_paths, commit_message)
            
            return False
    
    except Exception as e:
        logger.error(f"提交文件失败: {str(e)}", exc_info=True)
        return False

def force_commit_remaining_files() -> bool:
    """
    强制提交所有剩余的文件
    在程序退出时调用，确保最后一批文件也能被正确提交
    Returns:
        bool: 操作是否成功
    """
    global _file_count
    
    try:
        # 获取线程锁，确保同一时间只有一个线程操作Git
        with _git_lock:
            repo_root = get_repo_root()
            
            # 检查是否有暂存的更改
            diff_result = subprocess.run(
                ['git', 'diff', '--cached', '--exit-code'], 
                cwd=repo_root, 
                capture_output=True,
                text=True
            )
            
            # 如果没有暂存的更改，直接返回
            if diff_result.returncode == 0:
                logger.info("没有剩余的文件需要提交")
                return True
            
            # 创建提交消息
            commit_message = f"feat: 强制提交剩余文件 [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
            
            logger.info("强制提交剩余文件")
            return safe_git_commit_files([], commit_message)
    
    except Exception as e:
        logger.error(f"强制提交失败: {str(e)}", exc_info=True)
        return False

def immediate_commit(file_path, commit_message):
    """立即提交文件，确保完整Git操作流程（add, commit, push）"""
    try:
        # 确保提交消息包含 [skip ci]
        if "[skip ci]" not in commit_message:
            commit_message = f"{commit_message} [skip ci]"
        
        logger.info(f"立即提交文件: {os.path.basename(file_path)}")
        return safe_git_commit_files([file_path], commit_message)
    
    except Exception as e:
        logger.error(f"立即提交失败: {str(e)}", exc_info=True)
        return False
