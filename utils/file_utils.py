#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件操作工具模块
提供文件读写、标志文件管理、目录操作等常用功能
特别优化了增量数据保存功能
"""

import os
import json
import csv
import logging
import shutil
import tempfile
import pandas as pd
from typing import Any, Dict, List, Optional, Union, TextIO
from datetime import datetime
from pathlib import Path

# 配置日志
logger = logging.getLogger(__name__)

def load_etf_daily_data(etf_code: str, data_dir: Optional[Union[str, Path]] = None) -> pd.DataFrame:
    """
    加载ETF日线数据
    
    Args:
        etf_code: ETF代码
        data_dir: 数据目录，如果为None则使用Config.DATA_DIR
        
    Returns:
        pd.DataFrame: ETF日线数据DataFrame
    """
    try:
        # 导入Config类来获取默认数据目录
        from config import Config
        
        if data_dir is None:
            data_dir = Config.DATA_DIR
        
        data_dir = Path(data_dir)
        file_path = data_dir / f"{etf_code}.csv"
        
        if not file_path.exists():
            logger.warning(f"ETF日线数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        # 读取CSV文件
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # 确保日期列存在并转换为datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        logger.debug(f"加载ETF日线数据成功: {etf_code}, 共{len(df)}行")
        return df
        
    except Exception as e:
        logger.error(f"加载ETF日线数据失败 {etf_code}: {str(e)}")
        return pd.DataFrame()

def ensure_dir_exists(dir_path: Union[str, Path]) -> bool:
    """
    确保目录存在，如果不存在则创建
    
    Args:
        dir_path: 目录路径
        
    Returns:
        bool: 目录存在或创建成功返回True，否则返回False
    """
    try:
        dir_path = Path(dir_path)
        dir_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"确保目录存在: {dir_path}")
        return True
    except Exception as e:
        logger.error(f"创建目录失败 {dir_path}: {str(e)}")
        return False

def check_flag(flag_file_path: Union[str, Path]) -> bool:
    """
    检查标志文件是否存在（用于判断当天是否已执行某任务）
    
    Args:
        flag_file_path: 标志文件路径
        
    Returns:
        bool: 如果标志文件存在且是当天创建的返回True，否则返回False
    """
    try:
        flag_file = Path(flag_file_path)
        
        # 检查文件是否存在
        if not flag_file.exists():
            logger.debug(f"标志文件不存在: {flag_file}")
            return False
        
        # 检查文件修改时间是否为今天
        from utils.date_utils import get_beijing_time, is_same_day
        file_mtime = datetime.fromtimestamp(flag_file.stat().st_mtime)
        today = get_beijing_time().date()
        
        if is_same_day(file_mtime, today):
            logger.debug(f"标志文件存在且是今天创建的: {flag_file}")
            return True
        else:
            logger.debug(f"标志文件存在但不是今天创建的: {flag_file}")
            return False
            
    except Exception as e:
        logger.error(f"检查标志文件失败 {flag_file_path}: {str(e)}")
        return False

def set_flag(flag_file_path: Union[str, Path]) -> bool:
    """
    设置标志文件（标记当天已执行某任务）
    
    Args:
        flag_file_path: 标志文件路径
        
    Returns:
        bool: 成功创建标志文件返回True，否则返回False
    """
    try:
        flag_file = Path(flag_file_path)
        
        # 确保目录存在
        ensure_dir_exists(flag_file.parent)
        
        # 创建标志文件
        with open(flag_file, 'w') as f:
            f.write(f"Flag created at: {datetime.now().isoformat()}\n")
        
        logger.debug(f"标志文件已创建: {flag_file}")
        return True
        
    except Exception as e:
        logger.error(f"创建标志文件失败 {flag_file_path}: {str(e)}")
        return False

def clear_flag(flag_file_path: Union[str, Path]) -> bool:
    """
    清除标志文件
    
    Args:
        flag_file_path: 标志文件路径
        
    Returns:
        bool: 成功删除标志文件返回True，否则返回False
    """
    try:
        flag_file = Path(flag_file_path)
        
        if flag_file.exists():
            flag_file.unlink()
            logger.debug(f"标志文件已删除: {flag_file}")
        else:
            logger.debug(f"标志文件不存在，无需删除: {flag_file}")
            
        return True
        
    except Exception as e:
        logger.error(f"删除标志文件失败 {flag_file_path}: {str(e)}")
        return False

def read_json(file_path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """
    读取JSON文件
    
    Args:
        file_path: JSON文件路径
        
    Returns:
        Optional[Dict]: 解析后的JSON数据，失败返回None
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.debug(f"JSON文件读取成功: {file_path}")
        return data
    except Exception as e:
        logger.error(f"读取JSON文件失败 {file_path}: {str(e)}")
        return None

def write_json(file_path: Union[str, Path], data: Dict[str, Any], 
               indent: int = 2, ensure_ascii: bool = False) -> bool:
    """
    写入JSON文件
    
    Args:
        file_path: JSON文件路径
        data: 要写入的数据
        indent: 缩进空格数
        ensure_ascii: 是否确保ASCII编码
        
    Returns:
        bool: 成功写入返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        
        # 确保目录存在
        ensure_dir_exists(file_path.parent)
        
        # 使用临时文件确保原子性写入
        temp_file = file_path.with_suffix('.tmp')
        
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii)
        
        # 重命名临时文件为目标文件
        temp_file.replace(file_path)
        
        logger.debug(f"JSON文件写入成功: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"写入JSON文件失败 {file_path}: {str(e)}")
        return False

def read_csv(file_path: Union[str, Path]) -> Optional[List[Dict[str, Any]]]:
    """
    读取CSV文件
    
    Args:
        file_path: CSV文件路径
        
    Returns:
        Optional[List[Dict]]: CSV数据列表，失败返回None
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        logger.debug(f"CSV文件读取成功: {file_path}, 共{len(data)}行")
        return data
    except Exception as e:
        logger.error(f"读取CSV文件失败 {file_path}: {str(e)}")
        return None

def write_csv(file_path: Union[str, Path], data: List[Dict[str, Any]], 
              fieldnames: Optional[List[str]] = None) -> bool:
    """
    写入CSV文件
    
    Args:
        file_path: CSV文件路径
        data: 要写入的数据列表
        fieldnames: 列名列表，如果为None则使用数据中的键
        
    Returns:
        bool: 成功写入返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        
        # 确保目录存在
        ensure_dir_exists(file_path.parent)
        
        # 确定列名
        if fieldnames is None and data:
            fieldnames = list(data[0].keys())
        
        # 使用临时文件确保原子性写入
        temp_file = file_path.with_suffix('.tmp')
        
        with open(temp_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        # 重命名临时文件为目标文件
        temp_file.replace(file_path)
        
        logger.debug(f"CSV文件写入成功: {file_path}, 共{len(data)}行")
        return True
        
    except Exception as e:
        logger.error(f"写入CSV文件失败 {file_path}: {str(e)}")
        return False

def append_csv(file_path: Union[str, Path], data: List[Dict[str, Any]], 
               fieldnames: Optional[List[str]] = None) -> bool:
    """
    追加数据到CSV文件（增量爬取专用）
    
    Args:
        file_path: CSV文件路径
        data: 要追加的数据列表
        fieldnames: 列名列表，如果为None则尝试从现有文件读取或使用数据中的键
        
    Returns:
        bool: 成功追加返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        
        # 确保目录存在
        ensure_dir_exists(file_path.parent)
        
        # 确定列名
        if fieldnames is None:
            if data:
                fieldnames = list(data[0].keys())
            elif file_path.exists():
                # 尝试从现有文件读取列名
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames:
                        fieldnames = reader.fieldnames
        
        if not fieldnames:
            logger.error(f"无法确定CSV列名: {file_path}")
            return False
        
        # 使用临时文件确保原子性追加
        temp_file = file_path.with_suffix('.tmp')
        
        # 复制现有文件内容到临时文件（如果存在）
        if file_path.exists():
            shutil.copy2(file_path, temp_file)
        
        # 追加新数据
        file_exists = temp_file.exists()
        mode = 'a' if file_exists else 'w'
        
        with open(temp_file, mode, encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            # 如果文件不存在或为空，写入表头
            if not file_exists or temp_file.stat().st_size == 0:
                writer.writeheader()
            
            # 写入新数据
            writer.writerows(data)
        
        # 重命名临时文件为目标文件
        temp_file.replace(file_path)
        
        logger.info(f"CSV数据追加成功: {file_path}, 追加{len(data)}行")
        return True
        
    except Exception as e:
        logger.error(f"追加CSV数据失败 {file_path}: {str(e)}")
        return False

def incremental_save(file_path: Union[str, Path], data: List[Dict[str, Any]], 
                     key_field: str, timestamp_field: str = "timestamp") -> bool:
    """
    增量保存数据，避免重复记录（基于关键字段和时间戳）
    
    Args:
        file_path: 文件路径
        data: 要保存的数据列表
        key_field: 用于去重的关键字段名
        timestamp_field: 时间戳字段名，用于确定数据新旧
        
    Returns:
        bool: 成功保存返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        
        # 读取现有数据
        existing_data = []
        if file_path.exists():
            existing_data = read_csv(file_path) or []
        
        # 创建现有数据的映射（基于关键字段）
        existing_map = {item[key_field]: item for item in existing_data}
        
        # 合并数据（新数据覆盖旧数据）
        merged_data = existing_data.copy()
        new_count = 0
        update_count = 0
        
        for new_item in data:
            key = new_item.get(key_field)
            if not key:
                logger.warning(f"数据缺少关键字段 {key_field}: {new_item}")
                continue
                
            if key in existing_map:
                # 检查时间戳确定是否需要更新
                existing_timestamp = existing_map[key].get(timestamp_field, "")
                new_timestamp = new_item.get(timestamp_field, "")
                
                if new_timestamp > existing_timestamp:
                    # 更新现有记录
                    for i, item in enumerate(merged_data):
                        if item.get(key_field) == key:
                            merged_data[i] = new_item
                            update_count += 1
                            break
                # 如果时间戳相同或更旧，跳过
            else:
                # 添加新记录
                merged_data.append(new_item)
                new_count += 1
        
        # 保存合并后的数据
        if write_csv(file_path, merged_data):
            logger.info(f"增量保存完成: {file_path}, 新增{new_count}条, 更新{update_count}条")
            return True
        else:
            return False
            
    except Exception as e:
        logger.error(f"增量保存失败 {file_path}: {str(e)}")
        return False

def get_csv_writer(file_path: Union[str, Path], 
                   fieldnames: List[str]) -> Optional[tuple]:
    """
    获取CSV文件写入器（用于流式增量写入）
    
    Args:
        file_path: CSV文件路径
        fieldnames: 列名列表
        
    Returns:
        Optional[tuple]: (文件对象, 写入器)元组，失败返回None
    """
    try:
        file_path = Path(file_path)
        
        # 确保目录存在
        ensure_dir_exists(file_path.parent)
        
        # 检查文件是否存在以确定是否需要写入表头
        file_exists = file_path.exists()
        
        # 以追加模式打开文件
        f = open(file_path, 'a', encoding='utf-8', newline='')
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        # 如果文件不存在或为空，写入表头
        if not file_exists or file_path.stat().st_size == 0:
            writer.writeheader()
        
        return f, writer
        
    except Exception as e:
        logger.error(f"创建CSV写入器失败 {file_path}: {str(e)}")
        return None

def close_csv_writer(file_obj: TextIO) -> bool:
    """
    关闭CSV文件写入器
    
    Args:
        file_obj: 文件对象
        
    Returns:
        bool: 成功关闭返回True，否则返回False
    """
    try:
        file_obj.close()
        return True
    except Exception as e:
        logger.error(f"关闭文件失败: {str(e)}")
        return False

def safe_delete(file_path: Union[str, Path]) -> bool:
    """
    安全删除文件（如果存在）
    
    Args:
        file_path: 文件路径
        
    Returns:
        bool: 成功删除或文件不存在返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        
        if file_path.exists():
            if file_path.is_file():
                file_path.unlink()
                logger.debug(f"文件已删除: {file_path}")
            else:
                shutil.rmtree(file_path)
                logger.debug(f"目录已删除: {file_path}")
        else:
            logger.debug(f"文件不存在，无需删除: {file_path}")
            
        return True
        
    except Exception as e:
        logger.error(f"删除文件失败 {file_path}: {str(e)}")
        return False

def get_file_size(file_path: Union[str, Path]) -> Optional[int]:
    """
    获取文件大小（字节）
    
    Args:
        file_path: 文件路径
        
    Returns:
        Optional[int]: 文件大小（字节），失败返回None
    """
    try:
        file_path = Path(file_path)
        
        if file_path.exists() and file_path.is_file():
            size = file_path.stat().st_size
            logger.debug(f"文件大小: {file_path} - {size}字节")
            return size
        else:
            logger.warning(f"文件不存在或不是文件: {file_path}")
            return None
            
    except Exception as e:
        logger.error(f"获取文件大小失败 {file_path}: {str(e)}")
        return None

def backup_file(file_path: Union[str, Path], 
                backup_dir: Optional[Union[str, Path]] = None,
                suffix: str = ".bak") -> Optional[Path]:
    """
    备份文件
    
    Args:
        file_path: 要备份的文件路径
        backup_dir: 备份目录，如果为None则使用原文件所在目录
        suffix: 备份文件后缀
        
    Returns:
        Optional[Path]: 备份文件路径，失败返回None
    """
    try:
        file_path = Path(file_path)
        
        if not file_path.exists() or not file_path.is_file():
            logger.warning(f"文件不存在或不是文件，无法备份: {file_path}")
            return None
        
        # 确定备份目录
        if backup_dir is None:
            backup_dir = file_path.parent
        else:
            backup_dir = Path(backup_dir)
            ensure_dir_exists(backup_dir)
        
        # 生成备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"{file_path.stem}_{timestamp}{suffix}"
        
        # 复制文件
        shutil.copy2(file_path, backup_file)
        
        logger.info(f"文件已备份: {file_path} -> {backup_file}")
        return backup_file
        
    except Exception as e:
        logger.error(f"备份文件失败 {file_path}: {str(e)}")
        return None

def backup_incremental_data(data_dir: Union[str, Path], 
                            backup_dir: Union[str, Path],
                            days_to_keep: int = 7) -> int:
    """
    备份增量数据文件（按日期归档）
    
    Args:
        data_dir: 数据目录
        backup_dir: 备份目录
        days_to_keep: 保留天数
        
    Returns:
        int: 备份的文件数量
    """
    try:
        data_dir = Path(data_dir)
        backup_dir = Path(backup_dir)
        
        if not data_dir.exists() or not data_dir.is_dir():
            logger.warning(f"数据目录不存在: {data_dir}")
            return 0
        
        # 确保备份目录存在
        ensure_dir_exists(backup_dir)
        
        # 创建日期子目录
        date_str = datetime.now().strftime("%Y%m%d")
        daily_backup_dir = backup_dir / date_str
        ensure_dir_exists(daily_backup_dir)
        
        # 备份所有CSV文件
        backup_count = 0
        for csv_file in data_dir.glob("*.csv"):
            backup_file = daily_backup_dir / csv_file.name
            shutil.copy2(csv_file, backup_file)
            backup_count += 1
        
        logger.info(f"增量数据备份完成: 备份了{backup_count}个文件到{daily_backup_dir}")
        
        # 清理旧备份
        clean_old_files(backup_dir, "*", days_to_keep)
        
        return backup_count
        
    except Exception as e:
        logger.error(f"备份增量数据失败: {str(e)}")
        return 0

def clean_old_files(dir_path: Union[str, Path], 
                    pattern: str = "*", 
                    days: int = 30) -> int:
    """
    清理指定目录中超过指定天数的文件
    
    Args:
        dir_path: 目录路径
        pattern: 文件模式匹配
        days: 保留天数
        
    Returns:
        int: 删除的文件数量
    """
    try:
        dir_path = Path(dir_path)
        
        if not dir_path.exists() or not dir_path.is_dir():
            logger.warning(f"目录不存在或不是目录: {dir_path}")
            return 0
        
        from utils.date_utils import get_beijing_time
        cutoff_time = get_beijing_time().timestamp() - (days * 24 * 3600)
        deleted_count = 0
        
        for file_path in dir_path.glob(pattern):
            if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                safe_delete(file_path)
                deleted_count += 1
        
        logger.info(f"清理旧文件完成: {dir_path}, 删除了{deleted_count}个文件")
        return deleted_count
        
    except Exception as e:
        logger.error(f"清理旧文件失败 {dir_path}: {str(e)}")
        return 0

def atomic_write(file_path: Union[str, Path], content: str, 
                 mode: str = 'w', encoding: str = 'utf-8') -> bool:
    """
    原子性写入文件（使用临时文件确保写入完整性）
    
    Args:
        file_path: 文件路径
        content: 要写入的内容
        mode: 写入模式
        encoding: 编码格式
        
    Returns:
        bool: 成功写入返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        
        # 确保目录存在
        ensure_dir_exists(file_path.parent)
        
        # 创建临时文件
        temp_file = file_path.with_suffix('.tmp')
        
        # 写入临时文件
        with open(temp_file, mode, encoding=encoding) as f:
            f.write(content)
        
        # 重命名临时文件为目标文件
        temp_file.replace(file_path)
        
        logger.debug(f"原子性写入文件成功: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"原子性写入文件失败 {file_path}: {str(e)}")
        return False

def list_files(dir_path: Union[str, Path], pattern: str = "*") -> List[Path]:
    """
    列出目录中匹配模式的所有文件
    
    Args:
        dir_path: 目录路径
        pattern: 文件模式匹配
        
    Returns:
        List[Path]: 匹配的文件路径列表
    """
    try:
        dir_path = Path(dir_path)
        
        if not dir_path.exists() or not dir_path.is_dir():
            logger.warning(f"目录不存在或不是目录: {dir_path}")
        return []
        
        files = list(dir_path.glob(pattern))
        logger.debug(f"列出文件: {dir_path}/{pattern} -> 找到{len(files)}个文件")
        return files
        
    except Exception as e:
        logger.error(f"列出文件失败 {dir_path}/{pattern}: {str(e)}")
        return []

def file_exists(file_path: Union[str, Path]) -> bool:
    """
    检查文件是否存在
    
    Args:
        file_path: 文件路径
        
    Returns:
        bool: 文件存在返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        exists = file_path.exists() and file_path.is_file()
        logger.debug(f"文件存在检查: {file_path} -> {exists}")
        return exists
    except Exception as e:
        logger.error(f"文件存在检查失败 {file_path}: {str(e)}")
        return False

def dir_exists(dir_path: Union[str, Path]) -> bool:
    """
    检查目录是否存在
    
    Args:
        dir_path: 目录路径
        
    Returns:
        bool: 目录存在返回True，否则返回False
    """
    try:
        dir_path = Path(dir_path)
        exists = dir_path.exists() and dir_path.is_dir()
        logger.debug(f"目录存在检查: {dir_path} -> {exists}")
        return exists
    except Exception as e:
        logger.error(f"目录存在检查失败 {dir_path}: {str(e)}")
        return False

def get_file_mtime(file_path: Union[str, Path]) -> Optional[datetime]:
    """
    获取文件修改时间
    
    Args:
        file_path: 文件路径
        
    Returns:
        Optional[datetime]: 文件修改时间，失败返回None
    """
    try:
        file_path = Path(file_path)
        
        if file_path.exists() and file_path.is_file():
            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            logger.debug(f"文件修改时间: {file_path} -> {mtime}")
            return mtime
        else:
            logger.warning(f"文件不存在或不是文件: {file_path}")
            return None
            
    except Exception as e:
        logger.error(f"获取文件修改时间失败 {file_path}: {str(e)}")
        return None
