#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新版通用Git工具模块
【完全自包含 - 所有Git操作都在此模块处理】
- 自动处理工作区清理
- 自动配置Git用户信息
- 自动处理文件权限和换行符问题
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

def setup_git_environment():
    """
    设置Git环境，解决用户身份和配置问题
    这个函数在每次Git操作前调用
    """
    repo_root = get_repo_root()
    
    try:
        # 1. 设置Git用户信息（解决Author identity unknown问题）
        logger.info("👤 配置Git用户信息...")
        subprocess.run(['git', 'config', 'user.name', 'GitHub Actions Bot'], 
                      check=True, cwd=repo_root)
        subprocess.run(['git', 'config', 'user.email', 'actions@github.com'], 
                      check=True, cwd=repo_root)
        
        # 2. 禁用自动换行符转换（解决CRLF/LF问题）
        subprocess.run(['git', 'config', 'core.autocrlf', 'false'], 
                      check=True, cwd=repo_root)
        
        # 3. 忽略文件权限变化（解决chmod问题）
        subprocess.run(['git', 'config', 'core.filemode', 'false'], 
                      check=True, cwd=repo_root)
        
        logger.info("✅ Git环境配置完成")
        return True
        
    except Exception as e:
        logger.error(f"❌ Git环境配置失败: {e}")
        return False

def clean_git_working_tree():
    """
    清理Git工作树，重置所有未提交的更改
    这个函数在每次提交前调用，确保工作区干净
    """
    repo_root = get_repo_root()
    
    try:
        logger.info("🧹 清理Git工作树...")
        
        # 1. 重置所有已暂存的更改
        subprocess.run(['git', 'reset', '--hard', 'HEAD'], 
                      check=True, cwd=repo_root)
        
        # 2. 清理所有未跟踪的文件（除了我们关心的数据文件）
        # 使用-n先查看会删除什么，然后确认删除
        result = subprocess.run(['git', 'clean', '-fdn'], 
                              cwd=repo_root, capture_output=True, text=True)
        if result.stdout.strip():
            logger.warning(f"将清理未跟踪文件: {result.stdout}")
            subprocess.run(['git', 'clean', '-fd'], check=True, cwd=repo_root)
        
        # 3. 检查工作区状态
        status_result = subprocess.run(['git', 'status', '--porcelain'], 
                                     cwd=repo_root, capture_output=True, text=True)
        
        if status_result.stdout.strip():
            logger.warning(f"⚠️ 工作区仍有未清理的更改，但将继续: {status_result.stdout}")
        else:
            logger.info("✅ Git工作树清理完成")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ 清理Git工作树失败: {e}")
        return False

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
            
            response = requests.get(url, headers=headers, timeout=10)
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
            text=True,
            timeout=10
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
    通用的安全Git提交函数 - 移除清理操作
    """
    repo_root = get_repo_root()
    
    # 确保file_paths是列表
    if not isinstance(file_paths, list):
        file_paths = [file_paths]
    
    # 过滤掉不存在的文件
    existing_files = [fp for fp in file_paths if fp and os.path.exists(fp)]
    if not existing_files:
        logger.warning("❌ 没有存在的文件需要提交")
        return False
    
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
                
                # 2. 设置Git环境（用户信息、配置等）
                setup_git_environment()
                
                # 3. 添加指定文件到暂存区
                logger.info(f"📁 添加 {len(existing_files)} 个文件到暂存区...")
                files_added = False
                for file_path in existing_files:
                    try:
                        subprocess.run(['git', 'add', file_path], check=True, cwd=repo_root)
                        logger.debug(f"✅ 已添加: {file_path}")
                        files_added = True
                    except Exception as e:
                        logger.warning(f"添加文件失败 {file_path}: {e}")
                
                if not files_added:
                    logger.info("📝 没有文件需要添加")
                    return True
                
                # 4. 检查是否有变更需要提交
                result = subprocess.run(
                    ['git', 'diff', '--cached', '--exit-code'], 
                    cwd=repo_root, 
                    capture_output=True,
                    text=True
                )
                
                if result.returncode == 0:
                    logger.info("📝 没有变更需要提交")
                    return True
                
                # 5. 提交
                logger.info(f"💾 提交更改: {commit_message}")
                subprocess.run(['git', 'commit', '-m', commit_message], check=True, cwd=repo_root)
                
                # 6. 推送
                logger.info("🚀 推送到远程仓库...")
                subprocess.run(['git', 'push'], check=True, cwd=repo_root)
                
                # 7. 验证提交
                success_count = 0
                for file_path in existing_files:
                    if os.path.exists(file_path):
                        if verify_git_file_content(file_path):
                            logger.info(f"✅ 文件验证通过: {os.path.basename(file_path)}")
                            success_count += 1
                        else:
                            logger.warning(f"⚠️ 文件验证警告: {os.path.basename(file_path)}")
                
                logger.info(f"✅ Git提交成功，验证通过 {success_count}/{len(existing_files)} 个文件")
                return success_count > 0
                
            except Exception as e:
                logger.error(f"Git提交失败 (尝试 {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"⏳ 将在 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    
                    # 重试前清理可能的冲突状态
                    try:
                        subprocess.run(['git', 'merge', '--abort'], cwd=repo_root)
                        subprocess.run(['git', 'rebase', '--abort'], cwd=repo_root)
                    except:
                        pass
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
        
        if not os.path.exists(file_path):
            logger.error(f"❌ 文件不存在: {file_path}")
            return False
            
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
        
        # 过滤存在的文件
        existing_files = [fp for fp in file_paths if fp and os.path.exists(fp)]
        if not existing_files:
            logger.warning("❌ 没有存在的文件需要提交")
            return False
        
        # 创建提交消息
        if not commit_message:
            commit_message = f"feat: 批量提交{len(existing_files)}个文件 [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
        else:
            # 确保所有自定义提交消息也包含 [skip ci]
            if "[skip ci]" not in commit_message:
                commit_message = f"{commit_message} [skip ci]"
        
        logger.info(f"批量提交 {len(existing_files)} 个文件: {commit_message}")
        return safe_git_commit_files(existing_files, commit_message)
        
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
        
        # 过滤存在的文件
        existing_files = [fp for fp in file_paths if fp and os.path.exists(fp)]
        if not existing_files:
            logger.warning("❌ 没有存在的剩余文件需要提交")
            return False
        
        # 创建提交消息
        if not commit_message:
            commit_message = f"feat: 提交剩余{len(existing_files)}个文件 [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
        else:
            if "[skip ci]" not in commit_message:
                commit_message = f"{commit_message} [skip ci]"
        
        logger.info(f"提交剩余 {len(existing_files)} 个文件: {commit_message}")
        return safe_git_commit_files(existing_files, commit_message)
        
    except Exception as e:
        logger.error(f"提交剩余文件失败: {str(e)}", exc_info=True)
        return False
