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
    限制数据量为1年（365天）
    
    Args:
        df: 输入DataFrame
        end_date: 结束日期（YYYY-MM-DD）
    
    Returns:
        pd.DataFrame: 限制为1年数据的DataFrame
    """
    try:
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        
        # 确保日期列是datetime类型
        df['日期'] = pd.to_datetime(df['日期'])
        
        # 计算开始日期
        end_date_dt = pd.to_datetime(end_date)
        start_date_dt = end_date_dt - pd.DateOffset(days=365)
        
        # 过滤数据
        df = df[(df['日期'] >= start_date_dt) & (df['日期'] <= end_date_dt)]
        
        # 按日期排序
        df = df.sort_values('日期', ascending=False)
        
        # 转换日期回字符串格式
        df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')
        
        return df
    
    except Exception as e:
        logger.error(f"限制数据量为1年失败: {str(e)}", exc_info=True)
        return df
