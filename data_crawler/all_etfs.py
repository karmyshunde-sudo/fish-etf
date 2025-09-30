#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF列表管理模块
负责全市场ETF列表的更新和管理
- 每周日强制更新
- 手动触发更新
- 不考虑7天文件有效期
"""

import akshare as ak
import pandas as pd
import logging
import os
from datetime import datetime
from config import Config
from utils.date_utils import get_beijing_time

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
        logger.info("尝试从AkShare获取ETF列表...")
        etf_info = ak.fund_etf_spot_em()
        
        if etf_info.empty:
            logger.warning("ETF列表更新后为空")
            return pd.DataFrame()
        
        # 记录返回的列名，用于调试
        logger.debug(f"AkShare返回列名: {list(etf_info.columns)}")
        
        # 提取需要的列
        required_columns = ["代码", "名称", "流通市值"]
        missing_columns = [col for col in required_columns if col not in etf_info.columns]
        
        if missing_columns:
            logger.error(f"ETF列表数据缺少必要列: {', '.join(missing_columns)}")
            # 尝试修复列名
            for col in missing_columns:
                if col == "代码" and "code" in etf_info.columns:
                    etf_info = etf_info.rename(columns={"code": "代码"})
                elif col == "名称" and "name" in etf_info.columns:
                    etf_info = etf_info.rename(columns={"name": "名称"})
                elif col == "流通市值" and "成交额" in etf_info.columns:
                    etf_info = etf_info.rename(columns={"成交额": "流通市值"})
        
        # 确保包含所有需要的列
        for col in required_columns:
            if col not in etf_info.columns:
                etf_info[col] = "" if col != "流通市值" else 0.0
        
        # 提取需要的列
        etf_list = etf_info[required_columns]
        
        # 重命名列
        etf_list = etf_list.rename(columns={
            "代码": "ETF代码",
            "名称": "ETF名称",
            "流通市值": "基金规模"
        })
        
        # 基金规模转换为亿元
        etf_list["基金规模"] = etf_list["基金规模"].astype(float) / 100000000
        
        # 确保ETF代码格式
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.zfill(6)
        
        # 保存到CSV
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        etf_list_file = os.path.join(Config.DATA_DIR, "all_etfs.csv")
        etf_list.to_csv(etf_list_file, index=False, encoding="utf-8-sig")
        
        logger.info(f"ETF列表更新成功，共{len(etf_list)}只ETF，已保存至 {etf_list_file}")
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
            logger.info("ETF列表文件不存在，开始更新...")
            update_all_etf_list()
        
        etf_list = pd.read_csv(etf_list_file)
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
            logger.info("ETF列表文件不存在，开始更新...")
            update_all_etf_list()
        
        etf_list = pd.read_csv(etf_list_file)
        etf_row = etf_list[etf_list["ETF代码"].astype(str).str.zfill(6) == etf_code]
        if not etf_row.empty:
            return etf_row["ETF名称"].values[0]
        
        return "未知ETF"
    
    except Exception as e:
        logger.error(f"获取ETF名称失败: {str(e)}", exc_info=True)
        return "未知ETF"
