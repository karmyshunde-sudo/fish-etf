# 新文件: utils/data_processor.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据处理工具模块
提供DataFrame清洗、格式化、标准化等数据处理功能
"""

import pandas as pd
import numpy as np
import logging
from typing import List

# 配置日志
logger = logging.getLogger(__name__)

def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    确保DataFrame包含所有必需列
    
    Args:
        df: 输入DataFrame
    
    Returns:
        pd.DataFrame: 包含所有必需列的DataFrame
    """
    try:
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        required_columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额']
        
        # 检查并添加缺失列
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"数据中缺少必要列: {col}")
                df.loc[:, col] = np.nan
        
        # 确保日期列格式正确
        if '日期' in df.columns:
            # 使用loc避免SettingWithCopyWarning
            df.loc[:, '日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        
        return df
    
    except Exception as e:
        logger.error(f"确保必需列失败: {str(e)}", exc_info=True)
        return df

def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    清洗和格式化数据
    
    Args:
        df: 输入DataFrame
    
    Returns:
        pd.DataFrame: 清洗和格式化后的DataFrame
    """
    try:
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 去重（基于日期）
        df = df.drop_duplicates(subset=['日期'], keep='last')
        
        # 按日期排序
        df = df.sort_values('日期', ascending=False)
        
        # 处理数值列
        numeric_columns = ['开盘', '最高', '最低', '收盘', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        for col in numeric_columns:
            if col in df.columns:
                # 使用loc避免SettingWithCopyWarning
                df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')
        
        # 确保日期列格式正确
        if '日期' in df.columns:
            # 使用loc避免SettingWithCopyWarning
            df.loc[:, '日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        
        return df
    
    except Exception as e:
        logger.error(f"数据清洗和格式化失败: {str(e)}", exc_info=True)
        return df

def limit_to_one_year_data(df: pd.DataFrame, end_date: str) -> pd.DataFrame:
    """
    限制数据为最近1年的数据
    
    Args:
        df: 原始DataFrame
        end_date: 结束日期
        
    Returns:
        pd.DataFrame: 限制为1年数据后的DataFrame
    """
    if df.empty:
        return df
    
    try:
        # 计算1年前的日期
        one_year_ago = (pd.to_datetime(end_date) - timedelta(days=365)).strftime("%Y-%m-%d")
        
        # 确保日期列存在
        if "日期" not in df.columns:
            logger.warning("数据中缺少日期列，无法限制为1年数据")
            return df
        
        # 创建DataFrame的副本，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 转换日期列
        df.loc[:, "日期"] = pd.to_datetime(df["日期"], errors='coerce')
        
        # 过滤数据
        mask = df["日期"] >= pd.to_datetime(one_year_ago)
        df = df.loc[mask]
        
        logger.info(f"数据已限制为最近1年（从 {one_year_ago} 至 {end_date}），剩余 {len(df)} 条数据")
        return df
    except Exception as e:
        logger.error(f"限制数据为1年时发生错误: {str(e)}", exc_info=True)
        return df
