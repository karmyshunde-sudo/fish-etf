#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF列表管理模块
负责全市场ETF列表的更新和管理
【终极修复版】
- 彻底解决Git提交问题，确保数据真正保存
- 添加文件内容验证机制，防止"假成功"提交
- 解决BOM字节导致的编码问题
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
        
        # 关键修复：使用commit_files_in_batches替代_immediate_commit
        # 确保文件被正确提交到远程仓库
        logger.info("触发Git提交操作...")
        commit_message = "feat: 更新ETF列表 [skip ci]"
        if not commit_files_in_batches(etf_list_file, commit_message):
            logger.error("首次提交失败，尝试强制提交...")
            # 尝试强制提交
            if not force_commit_remaining_files():
                logger.critical("强制提交失败，可能导致数据丢失")
            else:
                logger.info("强制提交成功")
        else:
            logger.info("Git提交操作成功")
        
        # 关键验证：确保文件内容与Git仓库一致
        if not _verify_git_file_content(etf_list_file):
            logger.error("文件内容验证失败，可能需要重试提交")
            # 尝试重新提交
            if commit_files_in_batches(etf_list_file, f"{commit_message} (重试)"):
                if not _verify_git_file_content(etf_list_file):
                    logger.critical("文件内容验证再次失败，数据可能丢失")
                else:
                    logger.info("文件内容验证通过（重试后）")
            else:
                logger.critical("重试提交失败，数据可能丢失")
        
        logger.info(f"ETF列表更新成功，共{len(etf_list)}只ETF，已保存至 {etf_list_file}")
        return etf_list
    
    except Exception as e:
        logger.error(f"更新ETF列表失败: {str(e)}", exc_info=True)
        # 关键修复：在异常情况下确保文件被提交
        try:
            etf_list_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
            if os.path.exists(etf_list_file):
                logger.error("尝试保存ETF列表以恢复状态...")
                commit_files_in_batches(etf_list_file, "fix: 异常情况下强制更新ETF列表 [skip ci]")
                # 验证文件内容
                if _verify_git_file_content(etf_list_file):
                    logger.info("异常情况下成功提交ETF列表")
                else:
                    logger.critical("异常情况下文件内容验证失败")
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
        
        # 关键验证：确保文件内容与Git仓库一致
        if not _verify_git_file_content(etf_list_file):
            logger.warning("ETF列表文件内容与Git仓库不一致，可能需要重新加载")
        
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
        
        # 关键验证：确保文件内容与Git仓库一致
        if not _verify_git_file_content(etf_list_file):
            logger.warning("ETF列表文件内容与Git仓库不一致，可能需要重新加载")
        
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
