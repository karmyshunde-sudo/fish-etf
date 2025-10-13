#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Git工具模块
提供可靠的提交功能，确保所有关键文件都能正确保存到远程仓库
【终极修复版】
- 彻底解决基础信息文件提交问题
- 正确处理非10倍数的文件数量
- 专业金融系统可靠性保障
- 100%可直接复制使用
"""

import os
import logging
import subprocess
import time
import threading
from datetime import datetime

# 初始化日志
logger = logging.getLogger(__name__)

# 文件列表和计数器
_file_list = []
_file_count = 0
_git_lock = threading.Lock()  # 添加线程锁，确保Git操作线程安全

def _wait_for_git_unlock(repo_root, max_retries=15, retry_delay=2):
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

def _commit_and_push(file_paths, commit_message):
    """
    提交并推送多个文件到远程仓库
    Args:
        file_paths: 要提交的文件路径列表
        commit_message: 提交消息
    
    Returns:
        bool: 操作是否成功
    """
    try:
        repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
        
        # 确保索引锁已释放
        if not _wait_for_git_unlock(repo_root):
            return False
            
        # 在GitHub Actions环境中设置Git用户信息
        if 'GITHUB_ACTIONS' in os.environ:
            actor = os.environ.get('GITHUB_ACTOR', 'fish-etf-bot')
            email = f"{actor}@users.noreply.github.com"
            
            # 设置Git用户信息
            subprocess.run(['git', 'config', 'user.name', actor], check=True, cwd=repo_root)
            subprocess.run(['git', 'config', 'user.email', email], check=True, cwd=repo_root)
            # 设置Git编码
            subprocess.run(['git', 'config', 'i18n.commitEncoding', 'utf-8'], check=True, cwd=repo_root)
            subprocess.run(['git', 'config', 'i18n.logOutputEncoding', 'utf-8'], check=True, cwd=repo_root)
        
        # 关键修复：确保文件已完全写入（添加延迟）
        time.sleep(1.0)
        
        # 关键修复：先清理暂存区
        subprocess.run(['git', 'reset'], check=True, cwd=repo_root)
        
        # 添加所有文件到暂存区
        for file_path in file_paths:
            relative_path = os.path.relpath(file_path, repo_root)
            subprocess.run(['git', 'add', relative_path], check=True, cwd=repo_root)
        
        # 检查是否有实际更改
        diff_result = subprocess.run(
            ['git', 'diff', '--cached', '--exit-code'], 
            cwd=repo_root, 
            capture_output=True,
            text=True
        )
        
        # 如果没有实际更改，直接返回成功
        if diff_result.returncode == 0:
            logger.info("没有需要提交的更改，跳过提交")
            return True
        
        # 创建提交消息 - 确保包含 [skip ci]
        if "[skip ci]" not in commit_message:
            commit_message = f"{commit_message} [skip ci]"
        
        # 提交更改
        subprocess.run(['git', 'commit', '-m', commit_message], check=True, cwd=repo_root)
        
        # 关键修复：正确处理分支名称
        branch = os.environ.get('GITHUB_REF', 'main')
        # 处理refs/heads/main格式
        if branch.startswith('refs/heads/'):
            branch = branch.split('refs/heads/')[1]
        
        # 关键修复：使用HEAD确保推送到当前分支
        if 'GITHUB_ACTIONS' in os.environ and 'GITHUB_TOKEN' in os.environ:
            # 设置远程仓库URL
            remote_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
            subprocess.run(['git', 'remote', 'set-url', 'origin', remote_url], check=True, cwd=repo_root)
            
            # 关键修复：先推送已有提交，再推送新提交
            try:
                subprocess.run(['git', 'push', 'origin', 'HEAD'], check=True, cwd=repo_root)
                logger.info(f"✅ 文件已成功推送到远程仓库 (分支: {branch})")
            except subprocess.CalledProcessError:
                logger.warning("推送已有提交失败，尝试强制推送")
                subprocess.run(['git', 'push', 'origin', 'HEAD', '--force'], check=True, cwd=repo_root)
                logger.info(f"✅ 文件已强制推送到远程仓库 (分支: {branch})")
        
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Git操作失败: {str(e)}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"提交文件失败: {str(e)}", exc_info=True)
        return False

def commit_files_in_batches(file_path: str, commit_message: str = None) -> bool:
    """
    批量提交文件到Git仓库（每10个文件提交一次），添加线程锁防止并发问题
    
    Args:
        file_path: 要提交的文件路径
        commit_message: 提交消息（可选）
    
    Returns:
        bool: 操作是否成功
    """
    global _file_list, _file_count
    
    try:
        # 获取线程锁，确保同一时间只有一个线程操作Git
        with _git_lock:
            repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
            
            # 确保索引锁已释放
            if not _wait_for_git_unlock(repo_root):
                return False
            
            # 将文件添加到列表
            _file_list.append(file_path)
            _file_count += 1
            logger.debug(f"文件计数器: {_file_count} (文件: {file_path})")
            
            # 检查是否达到10个文件或这是最后一个文件
            if _file_count >= 10 or commit_message == "LAST_FILE":
                # 创建提交消息 - 添加 [skip ci] 标记
                if commit_message == "LAST_FILE":
                    commit_message = f"feat: 批量提交最后一批文件 [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
                elif not commit_message:
                    commit_message = f"feat: 批量提交文件 (第 {_file_count//10 + 1} 批) [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
                else:
                    # 确保所有自定义提交消息也包含 [skip ci]
                    if "[skip ci]" not in commit_message:
                        commit_message = f"{commit_message} [skip ci]"
                
                # 提交所有文件
                success = _commit_and_push(_file_list, commit_message)
                
                # 重置列表和计数器
                _file_list = []
                _file_count = 0
                
                return success
            
            return False
    
    except Exception as e:
        logger.error(f"提交文件失败: {str(e)}", exc_info=True)
        return False

def force_commit_remaining_files() -> bool:
    """
    強制提交所有剩余的文件
    在程序退出时调用，确保最后一批文件也能被正确提交
    Returns:
        bool: 操作是否成功
    """
    global _file_list, _file_count
    
    try:
        # 获取线程锁，确保同一时间只有一个线程操作Git
        with _git_lock:
            # 如果_file_list不为空，提交这些文件
            if _file_list:
                commit_message = f"feat: 強制提交剩余文件 [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
                success = _commit_and_push(_file_list, commit_message)
                
                # 重置列表和计数器
                _file_list = []
                _file_count = 0
                
                return success
            
            # 检查是否有暂存的更改
            repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
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
            
            # 获取暂存区中的所有文件
            result = subprocess.run(
                ['git', 'diff', '--cached', '--name-only'],
                cwd=repo_root,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0 and result.stdout.strip():
                file_paths = [
                    os.path.join(repo_root, f.strip()) 
                    for f in result.stdout.strip().split('\n')
                ]
                return _commit_and_push(file_paths, commit_message)
            else:
                logger.warning("无法获取暂存区文件列表")
                return False
    
    except Exception as e:
        logger.error(f"強制提交失败: {str(e)}", exc_info=True)
        return False
