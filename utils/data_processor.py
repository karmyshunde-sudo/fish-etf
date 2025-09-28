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
    通用数据清洗函数 - 适用于股票和ETF数据
    """
    try:
        if df.empty:
            logger.debug("数据为空，跳过清洗")
            return df
            
        # 创建DataFrame的深拷贝，避免SettingWithCopyWarning
        df = df.copy(deep=True)
        original_count = len(df)
        logger.debug(f"清洗前数据量: {original_count} 条")
        
        # 处理日期列
        if "日期" in df.columns:
            # 确保日期列是字符串类型
            df["日期"] = df["日期"].astype(str)
            
            # 尝试转换为日期格式
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
            
            # 删除无效日期
            invalid_dates = df["日期"].isna().sum()
            if invalid_dates > 0:
                logger.warning(f"发现 {invalid_dates} 条无效日期，将被删除")
                df = df.dropna(subset=["日期"])
                
                # 检查删除后是否为空
                if df.empty:
                    logger.warning("所有日期均无效，无法继续处理")
                    return pd.DataFrame()
                    
            logger.debug(f"日期处理后数据量: {len(df)} 条")
        else:
            logger.error("数据缺少'日期'列，无法正确处理")
            return pd.DataFrame()
        
        # 检查数据量变化
        if len(df) == 0:
            logger.warning("清洗后数据为空，请检查原始数据")
            return pd.DataFrame()
        elif len(df) < original_count * 0.5:
            logger.warning(f"清洗后数据量显著减少 ({original_count} -> {len(df)})，可能存在数据问题")
        
        # 确保其他列存在
        required_columns = ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]
        for col in required_columns:
            if col in df.columns:
                # 转换为数值类型
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 处理NaN值
                if col in ["开盘", "最高", "最低", "收盘"]:
                    df[col] = df[col].ffill()
                    
                    # 如果仍有NaN，尝试用相邻值填充
                    if df[col].isna().any():
                        logger.warning(f"列 '{col}' 仍存在NaN值，尝试用相邻值填充")
                        df[col] = df[col].interpolate(method='linear')
            else:
                logger.warning(f"数据缺少关键列: {col}")
        
        # 移除NaN值
        df = df.dropna(subset=["收盘", "成交量"])
        
        # 确保数据按日期排序
        if "日期" in df.columns:
            df = df.sort_values("日期")
        
        # 检查最终数据量
        final_count = len(df)
        logger.debug(f"清洗后数据量: {final_count} 条")
        
        if final_count == 0:
            logger.warning("清洗后数据为空，请检查原始数据")
            return pd.DataFrame()
            
        return df
        
    except Exception as e:
        logger.error(f"清洗数据失败: {str(e)}", exc_info=True)
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
