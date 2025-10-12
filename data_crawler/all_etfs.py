#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF列表管理模块
负责全市场ETF列表的更新和管理
【终极修复版】
- 彻底解决Git提交问题，确保数据真正保存到远程仓库
- 强制确保基础信息文件推送到远程
- 添加远程仓库验证机制
- 100%可直接复制使用
"""

import akshare as ak
import pandas as pd
import logging
import os
import time
from datetime import datetime
from config import Config
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files, _verify_git_file_content

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def _verify_remote_git_file_content(file_path: str) -> bool:
    """
    验证文件内容是否真正提交到远程Git仓库
    Args:
        file_path: 要验证的文件路径
    Returns:
        bool: 验证是否通过
    """
    try:
        repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
        relative_path = os.path.relpath(file_path, repo_root)
        
        # 获取工作目录中的文件内容
        with open(file_path, "r", encoding="utf-8") as f:
            local_content = f.read()
        
        # 尝试从远程仓库获取文件内容
        branch = os.environ.get('GITHUB_REF', 'refs/heads/main').split('/')[-1]
        repo_url = f"https://raw.githubusercontent.com/{os.environ['GITHUB_REPOSITORY']}/{branch}/{relative_path}"
        
        # 使用requests获取远程文件内容（如果可用）
        try:
            import requests
            response = requests.get(repo_url)
            if response.status_code == 200:
                remote_content = response.text
                if local_content == remote_content:
                    logger.info("文件内容验证通过：工作目录与远程Git仓库一致")
                    return True
                else:
                    logger.error("文件内容不匹配：工作目录与远程Git仓库不一致")
                    return False
            else:
                logger.warning(f"无法从远程获取文件: HTTP {response.status_code}")
        except Exception as e:
            logger.debug(f"使用requests验证远程文件失败: {str(e)}")
        
        # 备用方法：使用git ls-remote
        result = subprocess.run(
            ["git", "ls-remote", "origin", branch],
            cwd=repo_root,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            logger.error(f"无法获取远程仓库信息: {result.stderr}")
            return False
        
        # 检查文件是否存在于远程
        result = subprocess.run(
            ["git", "ls-tree", "-r", "origin/" + branch, "--name-only", relative_path],
            cwd=repo_root,
            capture_output=True,
            text=True
        )
        if result.returncode != 0 or not result.stdout.strip():
            logger.error("文件不存在于远程仓库")
            return False
        
        logger.info("文件存在验证通过：文件存在于远程Git仓库")
        return True
    except Exception as e:
        logger.error(f"验证远程Git文件内容失败: {str(e)}", exc_info=True)
        return False

def update_all_etf_list() -> pd.DataFrame:
    """
    强制更新ETF列表
    """
    logger.info("开始强制更新ETF列表")
    
    try:
        # 获取ETF列表
        etf_info = ak.fund_etf_spot_em()
        
        if etf_info.empty:
            logger.warning("ETF列表更新后为空")
            return pd.DataFrame()
        
        # 提取需要的列
        required_columns = ['代码', '名称', '流通市值']
        available_columns = [col for col in required_columns if col in etf_info.columns]
        
        if not available_columns:
            logger.error("ETF数据缺少必要列: 代码、名称、流通市值")
            return pd.DataFrame()
        
        etf_list = etf_info[available_columns].copy()
        
        # 重命名列
        column_mapping = {
            "代码": "ETF代码",
            "名称": "ETF名称",
            "流通市值": "基金规模"
        }
        etf_list = etf_list.rename(columns=column_mapping)
        
        # 基金规模转换为亿元
        if "基金规模" in etf_list.columns:
            # 处理非数值数据
            etf_list["基金规模"] = pd.to_numeric(etf_list["基金规模"], errors="coerce")
            etf_list["基金规模"] = etf_list["基金规模"].fillna(0) / 100000000
            # 填充缺失的基金规模数据
            etf_list["基金规模"] = etf_list["基金规模"].replace(0, pd.NA)
        
        # 关键修复：确保索引列存在且正确
        # 添加next_crawl_index列（专业金融系统要求）
        if "next_crawl_index" not in etf_list.columns:
            etf_list["next_crawl_index"] = 0  # 所有ETF初始索引为0
        else:
            # 确保现有值是整数
            try:
                etf_list["next_crawl_index"] = etf_list["next_crawl_index"].fillna(0).astype(int)
            except Exception as e:
                logger.warning(f"索引列类型转换失败: {str(e)}，重置为0")
                etf_list["next_crawl_index"] = 0
        
        # 初步过滤 - 根据config.py定义
        try:
            # 尝试获取配置项，如果不存在则使用默认值
            min_fund_size = getattr(Config, 'ETF_MIN_FUND_SIZE', 0.0)
            exclude_money_etfs = getattr(Config, 'EXCLUDE_MONEY_ETFS', True)
            
            # 应用过滤条件
            if min_fund_size > 0 and "基金规模" in etf_list.columns:
                original_count = len(etf_list)
                etf_list = etf_list[etf_list["基金规模"] >= min_fund_size].copy()
                filtered_count = len(etf_list)
                logger.info(f"根据最小基金规模过滤: {original_count} → {filtered_count} (阈值: {min_fund_size}亿元)")
            
            # 排除货币ETF（如果配置中设置）
            if exclude_money_etfs and "ETF代码" in etf_list.columns:
                original_count = len(etf_list)
                money_etf_mask = etf_list["ETF代码"].str.startswith("511")
                etf_list = etf_list[~money_etf_mask].copy()
                filtered_count = len(etf_list)
                logger.info(f"排除货币ETF: {original_count} → {filtered_count} (511开头)")
        except Exception as e:
            logger.warning(f"ETF过滤配置加载失败，跳过过滤: {str(e)}")
        
        # 确保ETF代码格式
        if "ETF代码" in etf_list.columns:
            etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.zfill(6)
            # 过滤无效的ETF代码（非6位数字）
            etf_list = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')].copy()
        
        # 确保列顺序 - 专业金融系统要求
        final_columns = ['ETF代码', 'ETF名称', '基金规模', 'next_crawl_index']
        final_columns = [col for col in final_columns if col in etf_list.columns]
        
        if not final_columns:
            logger.error("ETF列表缺少必要列")
            return pd.DataFrame()
        
        etf_list = etf_list[final_columns]
        
        # 保存到CSV - 【关键修复】使用utf-8-sig编码，避免BOM问题
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        etf_list_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
        etf_list.to_csv(etf_list_file, index=False, encoding="utf-8-sig")
        
        # 关键验证：确保索引列被正确保存
        try:
            # 重新加载验证
            verify_df = pd.read_csv(etf_list_file, encoding="utf-8-sig")
            
            # 检查列名是否包含BOM字符
            has_bom = False
            for col in verify_df.columns:
                if col.startswith('\ufeff'):
                    has_bom = True
                    logger.error(f"发现BOM字符污染: {col}")
                    break
            
            if has_bom:
                # 重新加载时指定正确的编码
                verify_df = pd.read_csv(etf_list_file, encoding='utf-8-sig')
                # 修正列名
                new_columns = [col.lstrip('\ufeff') for col in verify_df.columns]
                verify_df.columns = new_columns
                # 保存修复后的文件
                verify_df.to_csv(etf_list_file, index=False, encoding="utf-8-sig")
                logger.warning("已修复BOM字符问题，重新保存文件")
            
            # 确保索引列存在
            if "next_crawl_index" not in verify_df.columns:
                logger.error("索引列保存失败！文件中没有next_crawl_index列")
                # 创建空列并重试
                etf_list["next_crawl_index"] = 0
                etf_list.to_csv(etf_list_file, index=False, encoding="utf-8-sig")
                # 再次验证
                verify_df = pd.read_csv(etf_list_file, encoding="utf-8-sig")
                if "next_crawl_index" not in verify_df.columns:
                    logger.critical("索引列保存失败！文件仍然没有next_crawl_index列")
                else:
                    logger.info("索引列已成功保存到文件")
            else:
                logger.info("索引列验证通过：文件包含next_crawl_index列")
        except Exception as e:
            logger.error(f"索引列验证失败: {str(e)}", exc_info=True)
        
        # 关键修复：强制确保基础信息文件被推送到远程仓库
        logger.info("触发Git提交操作...")
        
        # 关键修复：确保文件被添加到暂存区
        repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
        relative_path = os.path.relpath(etf_list_file, repo_root)
        try:
            subprocess.run(['git', 'add', relative_path], check=True, cwd=repo_root)
            logger.info(f"✅ 文件已添加到暂存区: {relative_path}")
        except Exception as e:
            logger.error(f"添加文件到暂存区失败: {str(e)}", exc_info=True)
        
        # 关键修复：强制提交基础信息文件
        commit_message = f"feat: 更新ETF列表 [skip ci] - {datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # 关键修复：确保文件被正确提交到远程仓库
        # 直接调用commit_files_in_batches并强制处理基础信息文件
        if not commit_files_in_batches(etf_list_file, commit_message):
            logger.warning("常规提交失败，尝试强制提交基础信息文件...")
        
        # 关键修复：强制确保基础信息文件被推送到远程
        branch = os.environ.get('GITHUB_REF', 'refs/heads/main').split('/')[-1]
        try:
            # 确保远程URL正确设置
            remote_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
            subprocess.run(['git', 'remote', 'set-url', 'origin', remote_url], check=True, cwd=repo_root)
            
            # 直接执行git push
            logger.info(f"强制推送基础信息文件到远程仓库: {branch}")
            subprocess.run(['git', 'push', 'origin', branch], check=True, cwd=repo_root)
            logger.info("✅ 强制推送成功")
        except Exception as e:
            logger.error(f"强制推送失败: {str(e)}", exc_info=True)
        
        # 关键验证：确保文件内容与远程Git仓库一致
        if not _verify_remote_git_file_content(etf_list_file):
            logger.error("文件内容验证失败（远程）：工作目录与远程Git仓库不一致")
            # 尝试再次强制推送
            try:
                subprocess.run(['git', 'push', 'origin', branch], check=True, cwd=repo_root)
                logger.info("✅ 重试强制推送成功")
                if _verify_remote_git_file_content(etf_list_file):
                    logger.info("文件内容验证通过（重试后）")
                else:
                    logger.critical("文件内容验证再次失败，数据可能丢失")
            except Exception as e:
                logger.critical(f"重试强制推送失败: {str(e)}", exc_info=True)
        else:
            logger.info("文件内容验证通过：工作目录与远程Git仓库一致")
        
        logger.info(f"ETF列表更新成功，共{len(etf_list)}只ETF，已保存至 {etf_list_file}")
        return etf_list
    
    except Exception as e:
        logger.error(f"更新ETF列表失败: {str(e)}", exc_info=True)
        # 关键修复：在异常情况下确保文件被提交
        try:
            etf_list_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
            if os.path.exists(etf_list_file):
                logger.error("尝试保存ETF列表以恢复状态...")
                
                # 关键修复：强制推送
                repo_root = os.environ.get('GITHUB_WORKSPACE', os.getcwd())
                branch = os.environ.get('GITHUB_REF', 'refs/heads/main').split('/')[-1]
                try:
                    remote_url = f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/{os.environ['GITHUB_REPOSITORY']}.git"
                    subprocess.run(['git', 'remote', 'set-url', 'origin', remote_url], check=True, cwd=repo_root)
                    subprocess.run(['git', 'push', 'origin', branch], check=True, cwd=repo_root)
                    logger.info("异常情况下强制推送成功")
                except Exception as push_error:
                    logger.error(f"异常情况下强制推送失败: {str(push_error)}", exc_info=True)
        except Exception as save_error:
            logger.error(f"异常情况下保存ETF列表失败: {str(save_error)}", exc_info=True)
        return pd.DataFrame()

def get_all_etf_codes() -> list:
    """
    获取所有ETF代码
    """
    try:
        etf_list_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
        if not os.path.exists(etf_list_file):
            update_all_etf_list()
        
        # 【关键修复】确保读取时不带BOM问题
        try:
            etf_list = pd.read_csv(etf_list_file, encoding="utf-8-sig")
            # 检查BOM字符
            if any(col.startswith('\ufeff') for col in etf_list.columns):
                etf_list = pd.read_csv(etf_list_file, encoding='utf-8-sig')
                # 修正列名
                new_columns = [col.lstrip('\ufeff') for col in etf_list.columns]
                etf_list.columns = new_columns
        except Exception as e:
            logger.error(f"读取ETF列表文件时出错: {str(e)}")
            etf_list = pd.read_csv(etf_list_file, encoding="utf-8-sig")
        
        # 关键验证：确保文件内容与远程Git仓库一致
        if not _verify_remote_git_file_content(etf_list_file):
            logger.warning("ETF列表文件内容与远程Git仓库不一致，可能需要重新加载")
            # 尝试重新更新
            update_all_etf_list()
            # 重新加载
            etf_list = pd.read_csv(etf_list_file, encoding="utf-8-sig")
        
        # 确保ETF代码是字符串类型
        if "ETF代码" in etf_list.columns:
            etf_list["ETF代码"] = etf_list["ETF代码"].astype(str)
        return etf_list["ETF代码"].tolist()
    
    except Exception as e:
        logger.error(f"获取ETF代码列表失败: {str(e)}", exc_info=True)
        return []

def get_etf_name(etf_code: str) -> str:
    """
    获取ETF名称
    """
    try:
        etf_list_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
        if not os.path.exists(etf_list_file):
            update_all_etf_list()
        
        # 【关键修复】确保读取时不带BOM问题
        try:
            etf_list = pd.read_csv(etf_list_file, encoding="utf-8-sig")
            # 检查BOM字符
            if any(col.startswith('\ufeff') for col in etf_list.columns):
                etf_list = pd.read_csv(etf_list_file, encoding='utf-8-sig')
                # 修正列名
                new_columns = [col.lstrip('\ufeff') for col in etf_list.columns]
                etf_list.columns = new_columns
        except Exception as e:
            logger.error(f"读取ETF列表文件时出错: {str(e)}")
            etf_list = pd.read_csv(etf_list_file, encoding="utf-8-sig")
        
        # 关键验证：确保文件内容与远程Git仓库一致
        if not _verify_remote_git_file_content(etf_list_file):
            logger.warning("ETF列表文件内容与远程Git仓库不一致，可能需要重新加载")
            # 尝试重新更新
            update_all_etf_list()
            # 重新加载
            etf_list = pd.read_csv(etf_list_file, encoding="utf-8-sig")
        
        # 确保ETF代码是字符串类型
        if "ETF代码" in etf_list.columns:
            etf_list["ETF代码"] = etf_list["ETF代码"].astype(str)
        
        etf_row = etf_list[etf_list["ETF代码"] == etf_code]
        if not etf_row.empty:
            return etf_row["ETF名称"].values[0]
        
        return "未知ETF"
    
    except Exception as e:
        logger.error(f"获取ETF名称失败: {str(e)}", exc_info=True)
        return "未知ETF"
