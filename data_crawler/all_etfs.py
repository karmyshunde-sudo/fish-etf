#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF列表管理模块
负责全市场ETF列表的更新和管理
【精简版】
- 仅关注核心功能：获取ETF列表并保存
- 遵循单一职责原则
- 依赖工具模块处理Git操作
- 代码简洁高效
"""

import akshare as ak
import pandas as pd
import logging
import os
from datetime import datetime
from config import Config
from utils.git_utils import commit_files_in_batches

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def update_all_etf_list() -> pd.DataFrame:
    """
    更新ETF列表
    """
    logger.info("开始更新ETF列表")
    
    try:
        # 获取ETF列表
        etf_info = ak.fund_etf_spot_em()
        if etf_info.empty:
            logger.warning("ETF列表为空")
            return pd.DataFrame()
        
        # 提取需要的列
        required_columns = ['代码', '名称', '流通市值']
        available_columns = [col for col in required_columns if col in etf_info.columns]
        if not available_columns:
            logger.error("ETF数据缺少必要列")
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
            etf_list["基金规模"] = pd.to_numeric(etf_list["基金规模"], errors="coerce")
            etf_list["基金规模"] = etf_list["基金规模"].fillna(0) / 100000000
            etf_list["基金规模"] = etf_list["基金规模"].replace(0, pd.NA)
        
        # 添加next_crawl_index列
        etf_list["next_crawl_index"] = 0
        
        # 初步过滤
        try:
            min_fund_size = getattr(Config, 'ETF_MIN_FUND_SIZE', 0.0)
            exclude_money_etfs = getattr(Config, 'EXCLUDE_MONEY_ETFS', True)
            
            if min_fund_size > 0 and "基金规模" in etf_list.columns:
                etf_list = etf_list[etf_list["基金规模"] >= min_fund_size].copy()
            
            if exclude_money_etfs and "ETF代码" in etf_list.columns:
                etf_list = etf_list[~etf_list["ETF代码"].str.startswith("511")].copy()
        except Exception as e:
            logger.warning(f"ETF过滤配置加载失败: {str(e)}")
        
        # 确保ETF代码格式
        if "ETF代码" in etf_list.columns:
            etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.zfill(6)
            etf_list = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')].copy()
        
        # 确保列顺序
        final_columns = ['ETF代码', 'ETF名称', '基金规模', 'next_crawl_index']
        final_columns = [col for col in final_columns if col in etf_list.columns]
        if not final_columns:
            logger.error("ETF列表缺少必要列")
            return pd.DataFrame()
        
        etf_list = etf_list[final_columns]
        
        # 保存到CSV
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        etf_list_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
        etf_list.to_csv(etf_list_file, index=False, encoding="utf-8-sig")
        
        # 提交更新
        commit_message = "feat: 更新ETF列表 [skip ci]"
        commit_files_in_batches(etf_list_file, commit_message)
        
        logger.info(f"ETF列表更新成功，共{len(etf_list)}只ETF")
        return etf_list
    
    except Exception as e:
        logger.error(f"更新ETF列表失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_all_etf_codes() -> list:
    """
    获取所有ETF代码
    """
    try:
        etf_list_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
        if not os.path.exists(etf_list_file):
            update_all_etf_list()
        
        etf_list = pd.read_csv(etf_list_file, encoding="utf-8-sig")
        
        # 检查BOM字符
        if any(col.startswith('\ufeff') for col in etf_list.columns):
            etf_list = pd.read_csv(etf_list_file, encoding='utf-8-sig')
            new_columns = [col.lstrip('\ufeff') for col in etf_list.columns]
            etf_list.columns = new_columns
        
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
        
        etf_list = pd.read_csv(etf_list_file, encoding="utf-8-sig")
        
        # 检查BOM字符
        if any(col.startswith('\ufeff') for col in etf_list.columns):
            etf_list = pd.read_csv(etf_list_file, encoding='utf-8-sig')
            new_columns = [col.lstrip('\ufeff') for col in etf_list.columns]
            etf_list.columns = new_columns
        
        if "ETF代码" in etf_list.columns:
            etf_list["ETF代码"] = etf_list["ETF代码"].astype(str)
        
        etf_row = etf_list[etf_list["ETF代码"] == etf_code]
        if not etf_row.empty:
            return etf_row["ETF名称"].values[0]
        
        return "未知ETF"
    
    except Exception as e:
        logger.error(f"获取ETF名称失败: {str(e)}", exc_info=True)
        return "未知ETF"
