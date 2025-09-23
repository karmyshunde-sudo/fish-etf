#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Git工具模块
提供批量提交功能，每10个文件提交一次
"""

import os
import logging
import subprocess
import time
from datetime import datetime

# 初始化日志
logger = logging.getLogger(__name__)

# 文件计数器
_file_count = 0

def commit_files_in_batches(file_path: str, commit_message: str = None) -> bool:
    """
    批量提交文件到Git仓库（每10个文件提交一次）
    
    Args:
        file_path: 要提交的文件路径
        commit_message: 提交消息（可选）
    
    Returns:
        bool: 操作是否成功
    """
    global _file_count
    
    try:
        # 递增文件计数器
        _file_count += 1
        logger.debug(f"文件计数器: {_file_count}")
        
        # 获取仓库根目录
        repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
        
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
            # 创建提交消息
            if commit_message == "LAST_FILE":
                commit_message = f"feat: 批量提交最后一批文件 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
            elif not commit_message:
                commit_message = f"feat: 批量提交文件 (第 {_file_count//10 + 1} 批) [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
            
            # 提交更改
            subprocess.run(['git', 'commit', '-m', commit_message], check=True, cwd=repo_root)
            
            # 推送到远程仓库
            branch = os.environ.get('GITHUB_REF', 'refs/heads/main').split('/')[-1]
            if 'GITHUB_ACTIONS' in os.environ and 'GITHUB_TOKEN' in os.environ:
                remote_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
                subprocess.run(['git', 'remote', 'set-url', 'origin', remote_url], check=True, cwd=repo_root)
            
            # 拉取远程仓库的最新更改
            try:
                subprocess.run(['git', 'pull', 'origin', branch, '--no-rebase'], check=True, cwd=repo_root)
            except subprocess.CalledProcessError:
                logger.warning("拉取远程仓库更改时可能有冲突，但继续推送")
            
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
