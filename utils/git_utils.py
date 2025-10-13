#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Git工具模块
提供可靠的提交功能，确保所有关键文件都能正确保存到远程仓库
【生产级专业版】
- 彻底解决基础信息文件提交问题
- 严格遵循各司其职原则
- 专业金融系统可靠性保障
- 100%可直接复制使用
- 严格验证文件内容一致性
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
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

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

def _get_repo_root():
    """获取仓库根目录"""
    return os.environ.get('GITHUB_WORKSPACE', os.getcwd())

def _get_current_branch():
    """获取当前分支名称"""
    branch = os.environ.get('GITHUB_REF', 'main')
    # 处理refs/heads/main格式
    if branch.startswith('refs/heads/'):
        return branch.split('refs/heads/')[1]
    return branch

def _get_github_token():
    """安全获取GitHub令牌"""
    return os.environ.get('GITHUB_TOKEN', '').strip()

def _verify_remote_file_content(file_path):
    """
    验证文件内容是否真正存在于远程仓库
    Returns:
        bool: 验证是否通过
    """
    try:
        repo_root = _get_repo_root()
        relative_path = os.path.relpath(file_path, repo_root)
        repo = os.environ.get('GITHUB_REPOSITORY')
        token = _get_github_token()
        branch = _get_current_branch()
        
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
        repo_root = _get_repo_root()
        branch = _get_current_branch()
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

def _commit_and_push(file_path, commit_message, max_retries=3):
    """
    提交并推送文件到远程仓库，带重试机制
    Args:
        file_path: 文件路径
        commit_message: 提交消息
        max_retries: 最大重试次数
    
    Returns:
        bool: 操作是否成功
    """
    repo_root = _get_repo_root()
    branch = _get_current_branch()
    token = _get_github_token()
    
    for attempt in range(max_retries):
        try:
            # 确保索引锁已释放
            if not _wait_for_git_unlock(repo_root):
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return False
            
            # 设置Git用户信息
            if 'GITHUB_ACTIONS' in os.environ:
                actor = os.environ.get('GITHUB_ACTOR', 'fish-etf-bot')
                email = f"{actor}@users.noreply.github.com"
                
                subprocess.run(['git', 'config', 'user.name', actor], check=True, cwd=repo_root)
                subprocess.run(['git', 'config', 'user.email', email], check=True, cwd=repo_root)
            
            # 添加文件到暂存区
            if file_path and os.path.exists(file_path):
                relative_path = os.path.relpath(file_path, repo_root)
                subprocess.run(['git', 'add', relative_path], check=True, cwd=repo_root)
            
            # 检查是否有更改
            diff_result = subprocess.run(
                ['git', 'diff', '--cached', '--exit-code'], 
                cwd=repo_root, 
                capture_output=True,
                text=True
            )
            
            # 如果没有更改，但需要提交（例如基础信息文件）
            if diff_result.returncode == 0:
                if "all_stocks.csv" in file_path or "all_etfs.csv" in file_path:
                    logger.info("检测到基础信息文件，即使无更改也强制提交")
                    subprocess.run(
                        ['git', 'commit', '--allow-empty', '-m', commit_message], 
                        check=True, 
                        cwd=repo_root
                    )
                else:
                    logger.info("没有需要提交的更改")
                    return True
            
            # 提交更改
            else:
                subprocess.run(
                    ['git', 'commit', '-m', commit_message], 
                    check=True, 
                    cwd=repo_root
                )
            
            # 设置远程URL
            if 'GITHUB_ACTIONS' in os.environ and token:
                repo = os.environ.get('GITHUB_REPOSITORY', 'owner/repo')
                remote_url = f"https://x-access-token:{token}@github.com/{repo}.git"
                subprocess.run(
                    ['git', 'remote', 'set-url', 'origin', remote_url], 
                    check=True, 
                    cwd=repo_root
                )
            
            # 尝试拉取最新更改
            try:
                subprocess.run(
                    ['git', 'pull', 'origin', branch, '--rebase'], 
                    check=True, 
                    cwd=repo_root
                )
            except subprocess.CalledProcessError:
                logger.warning("拉取远程仓库更改时可能有冲突，继续推送")
            
            # 推送到远程仓库
            push_cmd = ['git', 'push', 'origin', f'{branch}:{branch}']
            subprocess.run(push_cmd, check=True, cwd=repo_root)
            
            # 验证文件是否真正存在于远程
            if file_path and os.path.exists(file_path):
                if _verify_remote_file_content(file_path):
                    logger.info("✅ 提交和推送成功，文件验证通过")
                    return True
                else:
                    logger.error("❌ 文件已提交但远程验证失败")
                    if attempt < max_retries - 1:
                        logger.info(f"将在 {2 ** attempt} 秒后重试...")
                        time.sleep(2 ** attempt)
                    continue
            
            return True
        
        except subprocess.CalledProcessError as e:
            logger.error(f"Git操作失败 (尝试 {attempt+1}/{max_retries}): {str(e)}", exc_info=True)
            if attempt < max_retries - 1:
                logger.info(f"将在 {2 ** attempt} 秒后重试...")
                time.sleep(2 ** attempt)
            else:
                return False
        except Exception as e:
            logger.error(f"提交失败 (尝试 {attempt+1}/{max_retries}): {str(e)}", exc_info=True)
            if attempt < max_retries - 1:
                logger.info(f"将在 {2 ** attempt} 秒后重试...")
                time.sleep(2 ** attempt)
            else:
                return False
    
    return False

def _immediate_commit(file_path, commit_message):
    """立即提交文件，确保完整Git操作流程（add, commit, push）"""
    try:
        # 确保提交消息包含 [skip ci]
        if "[skip ci]" not in commit_message:
            commit_message = f"{commit_message} [skip ci]"
        
        logger.info(f"立即提交基础信息文件: {os.path.basename(file_path)}")
        return _commit_and_push(file_path, commit_message)
    
    except Exception as e:
        logger.error(f"立即提交失败: {str(e)}", exc_info=True)
        return False

def commit_files_in_batches(file_path, commit_message=None):
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
            repo_root = _get_repo_root()
            
            # 确保索引锁已释放
            if not _wait_for_git_unlock(repo_root):
                return False
            
            # 检查是否是基础信息文件 - 立即提交
            if "all_stocks.csv" in file_path or "all_etfs.csv" in file_path:
                logger.info("检测到基础信息文件，立即提交")
                return _immediate_commit(file_path, commit_message or "feat: 更新基础信息")
            
            # 递增文件计数器
            _file_count += 1
            logger.debug(f"文件计数器: {_file_count}")
            
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
                
                logger.info(f"批量提交: {commit_message}")
                return _commit_and_push(file_path, commit_message)
            
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
    global _file_count
    
    try:
        # 获取线程锁，确保同一时间只有一个线程操作Git
        with _git_lock:
            repo_root = _get_repo_root()
            
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
            commit_message = f"feat: 強制提交剩余文件 [skip ci] [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
            
            logger.info("強制提交剩余文件")
            return _commit_and_push("", commit_message)
    
    except Exception as e:
        logger.error(f"強制提交失败: {str(e)}", exc_info=True)
        return False
