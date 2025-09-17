#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Git操作工具模块
提供Git提交和推送功能
"""

import os
import logging
import subprocess
from datetime import datetime

# 初始化日志
logger = logging.getLogger(__name__)

def commit_and_push_file(file_path: str, commit_message: str = None) -> bool:
    """
    提交并推送单个文件到Git仓库
    
    Args:
        file_path: 要提交的文件路径
        commit_message: 提交消息（可选）
    
    Returns:
        bool: 操作是否成功
    """
    try:
        # 获取当前时间作为默认提交消息
        if not commit_message:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            commit_message = f"feat: 自动更新ETF日线数据 - {current_time}"
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.error(f"文件不存在，无法提交: {file_path}")
            return False
        
        # 获取文件相对于仓库根目录的路径
        repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
        relative_path = os.path.relpath(file_path, repo_root)
        
        # 在GitHub Actions环境中设置Git用户信息
        if 'GITHUB_ACTIONS' in os.environ:
            logger.info("检测到GitHub Actions环境，设置Git用户信息")
            # 使用GitHub Actor作为用户名
            actor = os.environ.get('GITHUB_ACTOR', 'fish-etf-bot')
            # 使用GitHub提供的noreply邮箱
            email = f"{actor}@users.noreply.github.com"
            
            # 设置Git用户信息
            subprocess.run(['git', 'config', 'user.name', actor], check=True, cwd=repo_root)
            subprocess.run(['git', 'config', 'user.email', email], check=True, cwd=repo_root)
            logger.info(f"已设置Git用户: {actor} <{email}>")
        
        # 添加文件到暂存区
        add_cmd = ['git', 'add', relative_path]
        subprocess.run(add_cmd, check=True, cwd=repo_root)
        logger.info(f"已添加文件到暂存区: {relative_path}")
        
        # 提交更改
        commit_cmd = ['git', 'commit', '-m', commit_message]
        subprocess.run(commit_cmd, check=True, cwd=repo_root)
        logger.info(f"已提交更改: {commit_message}")
        
        # 推送到远程仓库
        branch = os.environ.get('GITHUB_REF', 'refs/heads/main').split('/')[-1]
        # 使用GITHUB_TOKEN进行身份验证
        if 'GITHUB_ACTIONS' in os.environ and 'GITHUB_TOKEN' in os.environ:
            remote_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
            subprocess.run(['git', 'remote', 'set-url', 'origin', remote_url], check=True, cwd=repo_root)
        
        # 关键修复：先拉取再推送，避免冲突
        logger.info("执行 git pull 以获取远程最新更改")
        try:
            # 先拉取远程仓库的最新更改
            subprocess.run(['git', 'pull', 'origin', branch], check=True, cwd=repo_root)
        except subprocess.CalledProcessError as e:
            logger.warning(f"git pull 失败，可能没有新更改: {str(e)}")
        
        push_cmd = ['git', 'push', 'origin', branch]
        subprocess.run(push_cmd, check=True, cwd=repo_root)
        logger.info(f"已推送到远程仓库: origin/{branch}")
        
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Git操作失败: {str(e)}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"提交文件失败: {str(e)}", exc_info=True)
        return False
