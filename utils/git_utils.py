#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Git操作工具模块
提供Git提交和推送功能
"""

import os
import logging
import subprocess
import time  # 添加time模块用于重试等待
from datetime import datetime

# 初始化日志
logger = logging.getLogger(__name__)

try:
    import git
    GIT_AVAILABLE = True
    logger.info("✅ GitPython模块已成功导入")
except ImportError:
    GIT_AVAILABLE = False
    logger.warning("⚠️ GitPython模块未安装，Git功能将不可用")
    logger.warning("⚠️ 请安装GitPython: pip install GitPython")

def commit_and_push_file(file_path: str, commit_message: str = None) -> bool:
    """
    提交并推送单个文件到Git仓库（先拉取再推送，避免冲突）
    
    Args:
        file_path: 要提交的文件路径
        commit_message: 提交消息（可选）
    
    Returns:
        bool: 操作是否成功
    """
    try:
        # 从文件路径中提取股票代码（如果是股票数据文件）
        stock_code = None
        if "data/daily/" in file_path:
            stock_code = os.path.basename(file_path).replace(".csv", "")
            logger.info(f"股票 {stock_code} 正在提交数据到GitHub仓库...")
        else:
            logger.info(f"正在提交文件 {os.path.basename(file_path)} 到GitHub仓库...")
        
        # 获取当前时间作为默认提交消息
        if not commit_message:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if stock_code:
                commit_message = f"自动更新股票 {stock_code} 数据 [{current_time}]"
            else:
                commit_message = f"feat: 自动更新数据文件 - {current_time}"
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            if stock_code:
                logger.error(f"股票 {stock_code} 文件不存在，无法提交")
            else:
                logger.error(f"文件 {file_path} 不存在，无法提交")
            return False
        
        # 获取文件相对于仓库根目录的路径
        repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
        relative_path = os.path.relpath(file_path, repo_root)
        
        # 在GitHub Actions环境中设置Git用户信息
        if 'GITHUB_ACTIONS' in os.environ:
            logger.debug("检测到GitHub Actions环境，设置Git用户信息")
            # 使用GitHub Actor作为用户名
            actor = os.environ.get('GITHUB_ACTOR', 'fish-etf-bot')
            # 使用GitHub提供的noreply邮箱
            email = f"{actor}@users.noreply.github.com"
            
            # 设置Git用户信息
            subprocess.run(['git', 'config', 'user.name', actor], check=True, cwd=repo_root)
            subprocess.run(['git', 'config', 'user.email', email], check=True, cwd=repo_root)
            logger.debug(f"已设置Git用户: {actor} <{email}>")
        
        # 添加文件到暂存区
        add_cmd = ['git', 'add', relative_path]
        subprocess.run(add_cmd, check=True, cwd=repo_root)
        if stock_code:
            logger.debug(f"股票 {stock_code} 已添加文件到暂存区: {relative_path}")
        else:
            logger.debug(f"已添加文件到暂存区: {relative_path}")
        
        # 提交更改
        commit_cmd = ['git', 'commit', '-m', commit_message]
        subprocess.run(commit_cmd, check=True, cwd=repo_root)
        if stock_code:
            logger.debug(f"股票 {stock_code} 已提交更改: {commit_message}")
        else:
            logger.debug(f"已提交更改: {commit_message}")
        
        # 推送到远程仓库
        branch = os.environ.get('GITHUB_REF', 'refs/heads/main').split('/')[-1]
        # 使用GITHUB_TOKEN进行身份验证
        if 'GITHUB_ACTIONS' in os.environ and 'GITHUB_TOKEN' in os.environ:
            remote_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
            subprocess.run(['git', 'remote', 'set-url', 'origin', remote_url], check=True, cwd=repo_root)
        
        # ===== 关键修复：添加 --no-rebase 参数 =====
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 尝试拉取远程仓库的最新更改（添加 --no-rebase 参数）
                logger.debug(f"尝试拉取远程仓库最新更改 (尝试 {attempt+1}/{max_retries})")
                subprocess.run(['git', 'pull', 'origin', branch, '--no-rebase'], 
                              check=True, cwd=repo_root)
                
                # 推送更改
                push_cmd = ['git', 'push', 'origin', branch]
                subprocess.run(push_cmd, check=True, cwd=repo_root)
                
                # 推送成功
                if stock_code:
                    logger.info(f"股票 {stock_code} 已推送到远程仓库: origin/{branch}")
                    logger.info(f"股票 {stock_code} 数据已成功提交并推送到GitHub仓库")
                else:
                    logger.info(f"已推送到远程仓库: origin/{branch}")
                return True
                
            except subprocess.CalledProcessError as e:
                if attempt < max_retries - 1:
                    # 指数退避等待
                    wait_time = 2 ** attempt
                    if stock_code:
                        logger.warning(f"股票 {stock_code} Git操作失败，{wait_time}秒后重试 ({attempt+1}/{max_retries}): {str(e)}")
                    else:
                        logger.warning(f"Git操作失败，{wait_time}秒后重试 ({attempt+1}/{max_retries}): {str(e)}")
                    time.sleep(wait_time)
                else:
                    if stock_code:
                        logger.error(f"股票 {stock_code} Git操作失败，超过最大重试次数: {str(e)}", exc_info=True)
                    else:
                        logger.error(f"Git操作失败，超过最大重试次数: {str(e)}", exc_info=True)
                    return False
        # ===== 修复结束 =====
    
    except subprocess.CalledProcessError as e:
        if stock_code:
            logger.error(f"股票 {stock_code} Git操作失败: {str(e)}", exc_info=True)
        else:
            logger.error(f"Git操作失败: {str(e)}", exc_info=True)
        return False
    except Exception as e:
        if stock_code:
            logger.error(f"股票 {stock_code} 提交文件失败: {str(e)}", exc_info=True)
        else:
            logger.error(f"提交文件失败: {str(e)}", exc_info=True)
        return False

def commit_and_push_etf_list(etf_count: int, source: str) -> None:
    """
    专门用于提交ETF列表更新的Git提交函数
    
    Args:
        etf_count: ETF数量
        source: 数据来源（如"AkShare"、"新浪"等）
    
    Raises:
        Exception: 如果Git操作失败
    """
    try:
        # 获取仓库根目录 - 使用与commit_and_push_file相同的方式
        repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
        logger.info(f"🔍 检测到仓库根目录: {repo_root}")
        
        # 获取ETF列表文件的绝对路径
        etf_list_path = os.path.join(repo_root, Config.ALL_ETFS_PATH)
        
        # 检查文件是否存在
        if not os.path.exists(etf_list_path):
            logger.error(f"ETF列表文件不存在，无法提交: {etf_list_path}")
            raise FileNotFoundError(f"ETF列表文件不存在: {etf_list_path}")
        
        # 获取文件相对于仓库根目录的路径
        relative_path = os.path.relpath(etf_list_path, repo_root)
        logger.info(f"ETF列表相对路径: {relative_path}")
        
        # 创建提交消息
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        commit_message = f"feat: 更新ETF列表 - {etf_count}只ETF (来源: {source}) [{current_time}]"
        
        # 在GitHub Actions环境中设置Git用户信息
        if 'GITHUB_ACTIONS' in os.environ:
            logger.debug("检测到GitHub Actions环境，设置Git用户信息")
            # 使用GitHub Actor作为用户名
            actor = os.environ.get('GITHUB_ACTOR', 'ETF-List-Updater')
            # 使用GitHub提供的noreply邮箱
            email = f"{actor}@users.noreply.github.com"
            
            # 设置Git用户信息
            subprocess.run(['git', 'config', 'user.name', actor], check=True, cwd=repo_root)
            subprocess.run(['git', 'config', 'user.email', email], check=True, cwd=repo_root)
            logger.debug(f"已设置Git用户: {actor} <{email}>")
        
        # 添加文件到暂存区
        add_cmd = ['git', 'add', relative_path]
        subprocess.run(add_cmd, check=True, cwd=repo_root)
        logger.debug(f"已添加ETF列表文件到暂存区: {relative_path}")
        
        # 检查是否有更改需要提交
        status = subprocess.run(['git', 'status', '--porcelain'], 
                               capture_output=True, text=True, check=True, cwd=repo_root)
        
        if status.stdout.strip():
            # 提交更改
            commit_cmd = ['git', 'commit', '-m', commit_message]
            subprocess.run(commit_cmd, check=True, cwd=repo_root)
            logger.info(f"✅ 已提交ETF列表更改: {commit_message}")
            
            # 推送到远程仓库
            branch = os.environ.get('GITHUB_REF', 'refs/heads/main').split('/')[-1]
            # 使用GITHUB_TOKEN进行身份验证
            if 'GITHUB_ACTIONS' in os.environ and 'GITHUB_TOKEN' in os.environ:
                remote_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
                subprocess.run(['git', 'remote', 'set-url', 'origin', remote_url], 
                              check=True, cwd=repo_root)
            
            # 拉取远程仓库的最新更改（添加 --no-rebase 参数避免冲突）
            logger.debug("尝试拉取远程仓库最新更改")
            try:
                subprocess.run(['git', 'pull', 'origin', branch, '--no-rebase', '--rebase'], 
                              check=True, cwd=repo_root)
            except subprocess.CalledProcessError:
                logger.warning("拉取远程仓库更改时可能有冲突，但继续推送")
            
            # 推送更改
            push_cmd = ['git', 'push', 'origin', branch]
            subprocess.run(push_cmd, check=True, cwd=repo_root)
            logger.info(f"✅ ETF列表已推送到远程仓库: origin/{branch}")
        else:
            logger.info("ℹ️ 没有需要提交的ETF列表更改")
            
    except subprocess.CalledProcessError as e:
        error_msg = f"ETF列表Git操作失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg) from e
    except Exception as e:
        error_msg = f"ETF列表提交过程中发生错误: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg) from e
            
    except Exception as e:
        logger.error(f"❌ ETF列表Git操作失败: {str(e)}", exc_info=True)
        raise
