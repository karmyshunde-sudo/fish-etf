#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Git工具模块
提供批量提交功能，每10个文件提交一次
添加了线程锁机制，解决多线程环境下的Git并发问题
"""

import os
import logging
import subprocess
import time
import threading
from datetime import datetime

# 初始化日志
logger = logging.getLogger(__name__)

# 文件计数器和线程锁
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

def _immediate_commit(file_path: str, commit_message: str) -> bool:
    """立即提交文件，不等待计数器，并添加 [skip ci]"""
    try:
        repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
        
        # 确保索引锁已释放
        if not _wait_for_git_unlock(repo_root):
            return False
            
        relative_path = os.path.relpath(file_path, repo_root)
        
        # 在GitHub Actions环境中设置Git用户信息
        if 'GITHUB_ACTIONS' in os.environ:
            actor = os.environ.get('GITHUB_ACTOR', 'fish-etf-bot')
            email = f"{actor}@users.noreply.github.com"
            
            # 设置Git用户信息
            subprocess.run(['git', 'config', 'user.name', actor], check=True, cwd=repo_root)
            subprocess.run(['git', 'config', 'user.email', email], check=True, cwd=repo_root)
        
        # 添加文件到暂存区
        subprocess.run(['git', 'add', relative_path], check=True, cwd=repo_root)
        
        # 关键修复：检查是否有实际的更改需要提交
        # 检查暂存区是否有更改
        diff_result = subprocess.run(['git', 'diff', '--cached', '--exit-code'], 
                                   cwd=repo_root, 
                                   capture_output=True,
                                   text=True)
        
        # 如果没有实际更改，直接返回成功
        if diff_result.returncode == 0:
            logger.info(f"没有需要提交的更改，跳过提交: {relative_path}")
            return True
        
        # 创建提交消息 - 确保包含 [skip ci]
        if "[skip ci]" not in commit_message:
            commit_message = f"{commit_message} [skip ci]"
        
        # 提交更改
        try:
            subprocess.run(['git', 'commit', '-m', commit_message], 
                          check=True, 
                          cwd=repo_root,
                          capture_output=True)
            logger.info(f"✅ 立即提交成功: {commit_message}")
            return True
        except subprocess.CalledProcessError as e:
            # 捕获并处理空提交错误
            if "nothing to commit" in str(e.output).lower() or "nothing to commit" in str(e.stderr).lower():
                logger.info(f"没有需要提交的更改: {relative_path}")
                return True
            else:
                raise e
    
    except subprocess.CalledProcessError as e:
        logger.error(f"立即提交失败: {str(e)}", exc_info=True)
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
    global _file_count
    
    try:
        # 获取线程锁，确保同一时间只有一个线程操作Git
        with _git_lock:
            repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
            
            # 确保索引锁已释放
            if not _wait_for_git_unlock(repo_root):
                return False
            
            # 检查是否是基础信息文件 - 立即提交
            if "all_stocks.csv" in file_path:
                # 特殊处理基础信息文件，立即提交
                return _immediate_commit(file_path, commit_message or "feat: 更新股票基础信息")
            
            # 递增文件计数器
            _file_count += 1
            logger.debug(f"文件计数器: {_file_count}")
            
            # 获取文件相对于仓库根目录的路径
            relative_path = os.path.relpath(file_path, repo_root)
            
            # 在GitHub Actions环境中设置Git用户信息
            if 'GITHUB_ACTIONS' in os.environ:
                # 使用GitHub Actor作为用户名
                actor = os.environ.get('GITHUB_ACTOR', 'fish-etf-bot')
                # 使用GitHub提供的noreply邮箱
                email = f"{actor}@users.noreply.github.com"
                
                # 设置Git用户信息
                subprocess.run(['git', 'config', 'user.name', actor], check=True, cwd=repo_root)
                subprocess.run(['git', 'config', 'user.email', email], check=True, cwd=repo_root)
            
            # 添加文件到暂存区
            subprocess.run(['git', 'add', relative_path], check=True, cwd=repo_root)
            
            # 检查是否达到10个文件或这是最后一个文件
            if _file_count % 10 == 0 or commit_message == "LAST_FILE":
                # 创建提交消息 - 添加 [skip ci] 标记
                if commit_message == "LAST_FILE":
                    commit_message = f"feat: 批量提交最后一批文件 [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
                elif not commit_message:
                    commit_message = f"feat: 批量提交文件 (第 {_file_count//10 + 1} 批) [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
                else:
                    # 确保所有自定义提交消息也包含 [skip ci]
                    if "[skip ci]" not in commit_message:
                        commit_message = f"{commit_message} [skip ci]"
                
                # 拉取远程仓库的最新更改（先拉取再推送，减少冲突）
                branch = os.environ.get('GITHUB_REF', 'refs/heads/main').split('/')[-1]
                try:
                    subprocess.run(['git', 'pull', 'origin', branch, '--no-rebase'], check=True, cwd=repo_root)
                except subprocess.CalledProcessError:
                    logger.warning("拉取远程仓库更改时可能有冲突，但继续推送")
                
                # 提交更改
                subprocess.run(['git', 'commit', '-m', commit_message], check=True, cwd=repo_root)
                
                # 推送到远程仓库
                if 'GITHUB_ACTIONS' in os.environ and 'GITHUB_TOKEN' in os.environ:
                    remote_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
                    subprocess.run(['git', 'remote', 'set-url', 'origin', remote_url], check=True, cwd=repo_root)
                
                # 推送更改
                subprocess.run(['git', 'push', 'origin', branch], check=True, cwd=repo_root)
                
                logger.info(f"✅ 批量提交成功: {commit_message}")
                return True
            
            return False
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Git操作失败: {str(e)}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"提交文件失败: {str(e)}", exc_info=True)
        return False
