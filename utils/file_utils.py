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
from typing import Any, Dict, List, Optional, Union, TextIO, Tuple
from datetime import datetime, timedelta, timezone  # 确保timedelta已正确导入
from pathlib import Path

# 导入init_dirs函数
from config import Config

# 配置日志
logger = logging.getLogger(__name__)

# 重新导出init_dirs函数，使其可以从file_utils模块导入
init_dirs = Config.init_dirs

def ensure_chinese_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    确保DataFrame使用中文列名
    
    Args:
        df: 原始DataFrame
    
    Returns:
        pd.DataFrame: 使用中文列名的DataFrame
    """
    if df.empty:
        return df
    
    # 如果已经是中文列名，直接返回
    if all(col in Config.CHINESE_COLUMNS for col in df.columns):
        return df
    
    # 尝试标准化列名
    col_map = {}
    for eng_col, chn_col in Config.COLUMN_NAME_MAPPING.items():
        # 尝试精确匹配
        if eng_col in df.columns:
            col_map[eng_col] = chn_col
        # 尝试模糊匹配
        else:
            for actual_col in df.columns:
                if eng_col.lower() in actual_col.lower():
                    col_map[actual_col] = chn_col
                    break
    
    # 重命名列
    if col_map:
        df = df.rename(columns=col_map)
        logger.debug(f"列名标准化: {col_map}")
    
    # 确保所有必需列都存在
    for chn_col in Config.CHINESE_COLUMNS:
        if chn_col not in df.columns:
            # 尝试从其他列推导
            if chn_col == "涨跌幅" and "涨跌额" in df.columns and "前收盘" in df.columns:
                df["涨跌幅"] = (df["涨跌额"] / df["前收盘"]) * 100
            elif chn_col == "涨跌额" and "收盘" in df.columns and "前收盘" in df.columns:
                df["涨跌额"] = df["收盘"] - df["前收盘"]
            elif chn_col == "振幅" and "最高" in df.columns and "最低" in df.columns and "前收盘" in df.columns:
                df["振幅"] = ((df["最高"] - df["最低"]) / df["前收盘"]) * 100
            # 其他列的推导逻辑...
            else:
                df[chn_col] = None  # 填充缺失列
    
    # 只保留标准中文列
    df = df[Config.CHINESE_COLUMNS]
    return df

def load_etf_daily_data(etf_code: str, data_dir: Optional[Union[str, Path]] = None) -> pd.DataFrame:
    """
    加载ETF日线数据
    
    Args:
        etf_code: ETF代码
        data_dir: 数据目录，如果为None则使用Config.ETFS_DAILY_DIR  # 修复：使用ETFS_DAILY_DIR
    
    Returns:
        pd.DataFrame: ETF日线数据
    """
    try:
        if data_dir is None:
            data_dir = Config.ETFS_DAILY_DIR  # 关键修复：使用ETFS_DAILY_DIR
        data_dir = Path(data_dir)
        
        # 构建文件路径
        file_path = data_dir / f"{etf_code}.csv"
        
        # 检查文件是否存在
        if not file_path.exists():
            logger.warning(f"ETF日线数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        # 读取CSV文件
        df = pd.read_csv(file_path)
        
        # 转换日期列
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        elif "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"])
        
        logger.debug(f"加载ETF日线数据成功: {etf_code}，共{len(df)}条")
        return df
    
    except Exception as e:
        logger.error(f"加载ETF日线数据失败 {etf_code}: {str(e)}")
        return pd.DataFrame()

def load_etf_metadata(file_path: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
    """
    加载ETF元数据
    
    Args:
        file_path: 元数据文件路径，如果为None则使用Config.METADATA_PATH
    
    Returns:
        Dict[str, Any]: ETF元数据字典
    """
    try:
        # 导入Config类来获取默认元数据文件路径
        from config import Config
        if file_path is None:
            file_path = Config.METADATA_PATH
        file_path = Path(file_path)
        
        if not file_path.exists():
            logger.warning(f"ETF元数据文件不存在: {file_path}")
            return {}
        
        # 读取JSON文件
        metadata = read_json(file_path)
        if metadata is None:
            logger.warning(f"ETF元数据文件读取失败: {file_path}")
            return {}
        
        logger.debug(f"加载ETF元数据成功: {file_path}, 共{len(metadata)}条记录")
        return metadata
    
    except Exception as e:
        logger.error(f"加载ETF元数据失败 {file_path}: {str(e)}")
        return {}

def save_etf_metadata(metadata: Dict[str, Any], file_path: Optional[Union[str, Path]] = None) -> bool:
    """
    保存ETF元数据
    
    Args:
        metadata: ETF元数据字典
        file_path: 元数据文件路径，如果为None则使用Config.METADATA_PATH
    
    Returns:
        bool: 成功保存返回True，否则返回False
    """
    try:
        # 导入Config类来获取默认元数据文件路径
        from config import Config
        if file_path is None:
            file_path = Config.METADATA_PATH
        file_path = Path(file_path)
        
        # 保存JSON文件
        success = write_json(file_path, metadata)
        if success:
            logger.debug(f"ETF元数据保存成功: {file_path}")
        else:
            logger.error(f"ETF元数据保存失败: {file_path}")
        
        return success
    
    except Exception as e:
        logger.error(f"保存ETF元数据失败 {file_path}: {str(e)}")
        return False

def read_json(file_path: Union[str, Path]) -> Optional[Dict]:
    """
    读取JSON文件
    
    Args:
        file_path: JSON文件路径
    
    Returns:
        Optional[Dict]: 读取的JSON数据，失败返回None
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            logger.warning(f"JSON文件不存在: {file_path}")
            return None
        
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    except Exception as e:
        logger.error(f"读取JSON文件失败 {file_path}: {str(e)}")
        return None

def write_json(file_path: Union[str, Path], data: Dict) -> bool:
    """
    写入JSON文件
    
    Args:
        file_path: JSON文件路径
        data: 要写入的数据
    
    Returns:
        bool: 写入成功返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        # 确保目录存在
        ensure_dir_exists(file_path.parent)
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return True
    
    except Exception as e:
        logger.error(f"写入JSON文件失败 {file_path}: {str(e)}")
        return False

def ensure_dir_exists(path: str) -> None:
    """
    确保目录存在，如果不存在则创建
    """
    try:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            logger.info(f"创建目录: {path}")
        
        # 确保目录有写入权限
        if not os.access(path, os.W_OK):
            try:
                os.chmod(path, 0o777)
                logger.info(f"设置目录权限: {path}")
            except Exception as e:
                logger.warning(f"无法设置目录权限: {str(e)}")
    except Exception as e:
        logger.error(f"创建目录失败: {str(e)}", exc_info=True)
        raise

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
            f.write(f"Flag created at: {datetime.now().isoformat()}")
        
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
        bool: 成功清除返回True，否则返回False
    """
    try:
        flag_file = Path(flag_file_path)
        
        # 检查文件是否存在
        if not flag_file.exists():
            logger.debug(f"标志文件不存在，无需清除: {flag_file}")
            return True
        
        # 删除文件
        flag_file.unlink()
        logger.debug(f"标志文件已清除: {flag_file}")
        return True
    
    except Exception as e:
        logger.error(f"清除标志文件失败 {flag_file_path}: {str(e)}")
        return False

def get_file_mtime(file_path: Union[str, Path]) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    获取文件修改时间（UTC与北京时间）
    
    Args:
        file_path: 文件路径
    
    Returns:
        Tuple[Optional[datetime], Optional[datetime]]: (UTC时间, 北京时间)
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists() or not file_path.is_file():
            logger.warning(f"文件不存在或不是文件: {file_path}")
            return None, None
        
        # 获取文件修改时间戳
        timestamp = file_path.stat().st_mtime
        
        # 使用config.py中定义的时区
        utc_time = datetime.fromtimestamp(timestamp, tz=Config.UTC_TIMEZONE)
        beijing_time = datetime.fromtimestamp(timestamp, tz=Config.BEIJING_TIMEZONE)
        
        logger.debug(f"获取文件修改时间: {file_path} -> UTC: {utc_time}, CST: {beijing_time}")
        return utc_time, beijing_time
    
    except Exception as e:
        logger.error(f"获取文件修改时间失败 {file_path}: {str(e)}", exc_info=True)
        return None, None

def is_file_outdated(file_path: Union[str, Path], max_age_days: int) -> bool:
    """
    判断文件是否过期
    
    Args:
        file_path: 文件路径
        max_age_days: 最大年龄（天）
    
    Returns:
        bool: 如果文件过期返回True，否则返回False
    """
    if not os.path.exists(file_path):
        logger.debug(f"文件不存在: {file_path}")
        return True
    
    try:
        # 简单方法：直接计算时间差（秒）
        current_timestamp = time.time()
        file_timestamp = os.path.getmtime(file_path)
        days_since_update = (current_timestamp - file_timestamp) / (24 * 3600)
        
        need_update = days_since_update >= max_age_days
        
        if need_update:
            logger.info(f"文件已过期({days_since_update:.1f}天)，需要更新")
        else:
            logger.debug(f"文件未过期({days_since_update:.1f}天)，无需更新")
            
        return need_update
    
    except Exception as e:
        logger.error(f"检查文件更新状态失败: {str(e)}", exc_info=True)
        # 出错时保守策略是要求更新
        return True

def read_csv(file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """
    读取CSV文件
    
    Args:
        file_path: CSV文件路径
        **kwargs: 传递给pd.read_csv的参数
    
    Returns:
        pd.DataFrame: 读取的CSV数据
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            logger.warning(f"CSV文件不存在: {file_path}")
            return pd.DataFrame()
        
        df = pd.read_csv(file_path, **kwargs)
        logger.debug(f"读取CSV文件成功: {file_path}，共{len(df)}行")
        return df
    
    except Exception as e:
        logger.error(f"读取CSV文件失败 {file_path}: {str(e)}")
        return pd.DataFrame()

def write_csv(df: pd.DataFrame, file_path: Union[str, Path], **kwargs) -> bool:
    """
    写入CSV文件
    
    Args:
        df: 要写入的DataFrame
        file_path: CSV文件路径
        **kwargs: 传递给df.to_csv的参数
    
    Returns:
        bool: 写入成功返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        # 确保目录存在
        ensure_dir_exists(file_path.parent)
        
        # 默认参数
        default_kwargs = {
            "index": False,
            "encoding": "utf-8-sig"
        }
        # 合并参数
        merged_kwargs = {**default_kwargs, **kwargs}
        
        df.to_csv(file_path, **merged_kwargs)
        logger.debug(f"写入CSV文件成功: {file_path}")
        return True
    
    except Exception as e:
        logger.error(f"写入CSV文件失败 {file_path}: {str(e)}")
        return False

def backup_file(file_path: Union[str, Path], backup_dir: Optional[Union[str, Path]] = None) -> bool:
    """
    备份文件
    
    Args:
        file_path: 要备份的文件路径
        backup_dir: 备份目录，如果为None则使用默认备份目录
    
    Returns:
        bool: 备份成功返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            logger.warning(f"要备份的文件不存在: {file_path}")
            return False
        
        # 确定备份目录
        if backup_dir is None:
            backup_dir = Path(Config.BACKUP_DIR)
        else:
            backup_dir = Path(backup_dir)
        
        # 创建备份目录
        ensure_dir_exists(backup_dir)
        
        # 生成备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
        backup_path = backup_dir / backup_name
        
        # 复制文件
        shutil.copy2(file_path, backup_path)
        logger.info(f"文件备份成功: {file_path} -> {backup_path}")
        return True
    
    except Exception as e:
        logger.error(f"文件备份失败 {file_path}: {str(e)}")
        return False

def get_file_size(file_path: Union[str, Path]) -> int:
    """
    获取文件大小（字节）
    
    Args:
        file_path: 文件路径
    
    Returns:
        int: 文件大小，失败返回-1
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists() or not file_path.is_file():
            logger.warning(f"文件不存在或不是文件: {file_path}")
            return -1
        
        return file_path.stat().st_size
    
    except Exception as e:
        logger.error(f"获取文件大小失败 {file_path}: {str(e)}")
        return -1

def read_text(file_path: Union[str, Path]) -> Optional[str]:
    """
    读取文本文件
    
    Args:
        file_path: 文本文件路径
    
    Returns:
        Optional[str]: 读取的文本内容，失败返回None
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            logger.warning(f"文本文件不存在: {file_path}")
            return None
        
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    
    except Exception as e:
        logger.error(f"读取文本文件失败 {file_path}: {str(e)}")
        return None

def write_text(content: str, file_path: Union[str, Path]) -> bool:
    """
    写入文本文件
    
    Args:
        content: 要写入的文本内容
        file_path: 文本文件路径
    
    Returns:
        bool: 写入成功返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        # 确保目录存在
        ensure_dir_exists(file_path.parent)
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        
        return True
    
    except Exception as e:
        logger.error(f"写入文本文件失败 {file_path}: {str(e)}")
        return False

def safe_file_operation(operation, *args, **kwargs):
    """
    安全执行文件操作
    
    Args:
        operation: 要执行的文件操作函数
        *args: 传递给操作函数的位置参数
        **kwargs: 传递给操作函数的关键字参数
    
    Returns:
        操作函数的返回值
    """
    try:
        return operation(*args, **kwargs)
    except Exception as e:
        logger.error(f"安全文件操作失败: {str(e)}")
        return None

def get_file_list(directory: Union[str, Path], pattern: str = "*") -> List[Path]:
    """
    获取目录中符合模式的文件列表
    
    Args:
        directory: 目录路径
        pattern: 文件匹配模式，默认为"*"（所有文件）
    
    Returns:
        List[Path]: 符合条件的文件路径列表
    """
    try:
        directory = Path(directory)
        if not directory.exists() or not directory.is_dir():
            logger.warning(f"目录不存在或不是目录: {directory}")
            return []
        
        return list(directory.glob(pattern))
    
    except Exception as e:
        logger.error(f"获取文件列表失败 {directory}: {str(e)}")
        return []

def create_temp_file(prefix: str = "temp_", suffix: str = "", dir: Optional[str] = None) -> str:
    """
    创建临时文件
    
    Args:
        prefix: 文件名前缀
        suffix: 文件名后缀
        dir: 临时文件目录，如果为None则使用系统默认
    
    Returns:
        str: 临时文件路径
    """
    try:
        with tempfile.NamedTemporaryFile(prefix=prefix, suffix=suffix, dir=dir, delete=False) as f:
            return f.name
    
    except Exception as e:
        logger.error(f"创建临时文件失败: {str(e)}")
        return ""

def create_temp_dir(prefix: str = "temp_", suffix: str = "", dir: Optional[str] = None) -> str:
    """
    创建临时目录
    
    Args:
        prefix: 目录名前缀
        suffix: 目录名后缀
        dir: 临时目录父目录，如果为None则使用系统默认
    
    Returns:
        str: 临时目录路径
    """
    try:
        return tempfile.mkdtemp(prefix=prefix, suffix=suffix, dir=dir)
    
    except Exception as e:
        logger.error(f"创建临时目录失败: {str(e)}")
        return ""

def move_file(src: Union[str, Path], dst: Union[str, Path]) -> bool:
    """
    移动文件
    
    Args:
        src: 源文件路径
        dst: 目标文件路径
    
    Returns:
        bool: 移动成功返回True，否则返回False
    """
    try:
        src = Path(src)
        dst = Path(dst)
        
        if not src.exists():
            logger.warning(f"源文件不存在: {src}")
            return False
        
        # 确保目标目录存在
        ensure_dir_exists(dst.parent)
        
        # 移动文件
        shutil.move(str(src), str(dst))
        logger.info(f"文件移动成功: {src} -> {dst}")
        return True
    
    except Exception as e:
        logger.error(f"移动文件失败 {src} -> {dst}: {str(e)}")
        return False

def copy_file(src: Union[str, Path], dst: Union[str, Path]) -> bool:
    """
    复制文件
    
    Args:
        src: 源文件路径
        dst: 目标文件路径
    
    Returns:
        bool: 复制成功返回True，否则返回False
    """
    try:
        src = Path(src)
        dst = Path(dst)
        
        if not src.exists():
            logger.warning(f"源文件不存在: {src}")
            return False
        
        # 确保目标目录存在
        ensure_dir_exists(dst.parent)
        
        # 复制文件
        shutil.copy2(str(src), str(dst))
        logger.info(f"文件复制成功: {src} -> {dst}")
        return True
    
    except Exception as e:
        logger.error(f"复制文件失败 {src} -> {dst}: {str(e)}")
        return False

def delete_file(file_path: Union[str, Path]) -> bool:
    """
    删除文件
    
    Args:
        file_path: 文件路径
    
    Returns:
        bool: 删除成功返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        
        if not file_path.exists() or not file_path.is_file():
            logger.warning(f"文件不存在或不是文件: {file_path}")
            return False
        
        # 删除文件
        file_path.unlink()
        logger.info(f"文件删除成功: {file_path}")
        return True
    
    except Exception as e:
        logger.error(f"删除文件失败 {file_path}: {str(e)}")
        return False

def delete_directory(dir_path: Union[str, Path]) -> bool:
    """
    删除目录
    
    Args:
        dir_path: 目录路径
    
    Returns:
        bool: 删除成功返回True，否则返回False
    """
    try:
        dir_path = Path(dir_path)
        
        if not dir_path.exists() or not dir_path.is_dir():
            logger.warning(f"目录不存在或不是目录: {dir_path}")
            return False
        
        # 删除目录
        shutil.rmtree(dir_path)
        logger.info(f"目录删除成功: {dir_path}")
        return True
    
    except Exception as e:
        logger.error(f"删除目录失败 {dir_path}: {str(e)}")
        return False

def read_parquet(file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """
    读取Parquet文件
    
    Args:
        file_path: Parquet文件路径
        **kwargs: 传递给pd.read_parquet的参数
    
    Returns:
        pd.DataFrame: 读取的Parquet数据
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            logger.warning(f"Parquet文件不存在: {file_path}")
            return pd.DataFrame()
        
        df = pd.read_parquet(file_path, **kwargs)
        logger.debug(f"读取Parquet文件成功: {file_path}，共{len(df)}行")
        return df
    
    except Exception as e:
        logger.error(f"读取Parquet文件失败 {file_path}: {str(e)}")
        return pd.DataFrame()

def write_parquet(df: pd.DataFrame, file_path: Union[str, Path], **kwargs) -> bool:
    """
    写入Parquet文件
    
    Args:
        df: 要写入的DataFrame
        file_path: Parquet文件路径
        **kwargs: 传递给df.to_parquet的参数
    
    Returns:
        bool: 写入成功返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        # 确保目录存在
        ensure_dir_exists(file_path.parent)
        
        # 默认参数
        default_kwargs = {
            "index": False,
            "engine": "pyarrow"
        }
        # 合并参数
        merged_kwargs = {**default_kwargs, **kwargs}
        
        df.to_parquet(file_path, **merged_kwargs)
        logger.debug(f"写入Parquet文件成功: {file_path}")
        return True
    
    except Exception as e:
        logger.error(f"写入Parquet文件失败 {file_path}: {str(e)}")
        return False

def get_file_info(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    获取文件信息
    
    Args:
        file_path: 文件路径
    
    Returns:
        Dict[str, Any]: 文件信息字典
    """
    try:
        file_path = Path(file_path)
        
        if not file_path.exists():
            logger.warning(f"文件不存在: {file_path}")
            return {}
        
        # 获取文件统计信息
        stat = file_path.stat()
        
        # 构建文件信息
        file_info = {
            "path": str(file_path),
            "name": file_path.name,
            "stem": file_path.stem,
            "suffix": file_path.suffix,
            "size": stat.st_size,
            "created": datetime.fromtimestamp(stat.st_ctime),
            "modified": datetime.fromtimestamp(stat.st_mtime),
            "accessed": datetime.fromtimestamp(stat.st_atime),
            "is_file": file_path.is_file(),
            "is_dir": file_path.is_dir()
        }
        
        logger.debug(f"获取文件信息成功: {file_path}")
        return file_info
    
    except Exception as e:
        logger.error(f"获取文件信息失败 {file_path}: {str(e)}")
        return {}

def read_excel(file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """
    读取Excel文件
    
    Args:
        file_path: Excel文件路径
        **kwargs: 传递给pd.read_excel的参数
    
    Returns:
        pd.DataFrame: 读取的Excel数据
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            logger.warning(f"Excel文件不存在: {file_path}")
            return pd.DataFrame()
        
        df = pd.read_excel(file_path, **kwargs)
        logger.debug(f"读取Excel文件成功: {file_path}，共{len(df)}行")
        return df
    
    except Exception as e:
        logger.error(f"读取Excel文件失败 {file_path}: {str(e)}")
        return pd.DataFrame()

def write_excel(df: pd.DataFrame, file_path: Union[str, Path], **kwargs) -> bool:
    """
    写入Excel文件
    
    Args:
        df: 要写入的DataFrame
        file_path: Excel文件路径
        **kwargs: 传递给df.to_excel的参数
    
    Returns:
        bool: 写入成功返回True，否则返回False
    """
    try:
        file_path = Path(file_path)
        # 确保目录存在
        ensure_dir_exists(file_path.parent)
        
        # 默认参数
        default_kwargs = {
            "index": False
        }
        # 合并参数
        merged_kwargs = {**default_kwargs, **kwargs}
        
        df.to_excel(file_path, **merged_kwargs)
        logger.debug(f"写入Excel文件成功: {file_path}")
        return True
    
    except Exception as e:
        logger.error(f"写入Excel文件失败 {file_path}: {str(e)}")
        return False

# ==========================
# 新增：套利增量推送功能
# ==========================

def load_arbitrage_status() -> Dict[str, str]:
    """
    加载套利推送状态
    
    Returns:
        Dict[str, str]: {ETF代码: 最后推送日期} 的字典
    """
    try:
        # 检查套利状态文件是否存在
        status_file = Path(Config.ARBITRAGE_STATUS_FILE)
        if not status_file.exists():
            logger.info("套利状态文件不存在，将创建新的状态文件")
            return {}
        
        # 读取套利状态文件
        with open(status_file, 'r', encoding='utf-8') as f:
            status = json.load(f)
        
        logger.debug(f"成功加载套利状态，共 {len(status)} 条记录")
        return status
    
    except Exception as e:
        logger.error(f"加载套利状态失败: {str(e)}", exc_info=True)
        return {}

def save_arbitrage_status(status: Dict[str, str]) -> bool:
    """
    保存套利推送状态
    
    Args:
        status: 套利推送状态字典
    
    Returns:
        bool: 是否成功保存
    """
    try:
        # 确保目录存在
        status_dir = Path(Config.ARBITRAGE_STATUS_FILE).parent
        if not status_dir.exists():
            os.makedirs(status_dir, exist_ok=True)
        
        # 保存套利状态
        with open(Config.ARBITRAGE_STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status, f, indent=2, ensure_ascii=False)
        
        logger.debug(f"套利状态保存成功，共 {len(status)} 条记录")
        return True
    
    except Exception as e:
        logger.error(f"保存套利状态失败: {str(e)}", exc_info=True)
        return False

def should_push_arbitrage(etf_code: str) -> bool:
    """
    检查是否应该推送该ETF的套利机会
    
    Args:
        etf_code: ETF代码
    
    Returns:
        bool: 是否应该推送
    """
    try:
        # 获取当前北京时间
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        
        # 加载套利状态
        status = load_arbitrage_status()
        
        # 如果ETF从未推送过，或者上次推送不是今天，则应该推送
        return etf_code not in status or status[etf_code] != today
    
    except Exception as e:
        logger.error(f"检查是否应该推送套利机会失败: {str(e)}", exc_info=True)
        # 出错时保守策略：允许推送
        return True

def mark_arbitrage_pushed(etf_code: str) -> bool:
    """
    标记ETF套利机会已推送
    
    Args:
        etf_code: ETF代码
    
    Returns:
        bool: 是否成功标记
    """
    try:
        # 获取当前北京时间
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        
        # 加载套利状态
        status = load_arbitrage_status()
        
        # 更新状态
        status[etf_code] = today
        
        # 保存更新后的状态
        return save_arbitrage_status(status)
    
    except Exception as e:
        logger.error(f"标记ETF套利机会已推送失败: {str(e)}", exc_info=True)
        return False

def clear_expired_arbitrage_status() -> bool:
    """
    清理过期的套利状态记录（保留最近7天的记录）
    
    Returns:
        bool: 是否成功清理
    """
    try:
        # 加载套利状态
        status = load_arbitrage_status()
        if not status:
            return True
        
        # 获取当前日期
        from utils.date_utils import get_beijing_time
        current_date = get_beijing_time()
        
        # 计算7天前的日期
        seven_days_ago = (current_date - timedelta(days=7)).strftime("%Y-%m-%d")
        
        # 过滤掉过期的记录
        updated_status = {etf: date for etf, date in status.items() if date >= seven_days_ago}
        
        # 如果有记录被删除，保存更新后的状态
        if len(updated_status) < len(status):
            logger.info(f"清理过期套利状态记录：移除了 {len(status) - len(updated_status)} 条记录")
            return save_arbitrage_status(updated_status)
        
        return True
    
    except Exception as e:
        logger.error(f"清理套利状态记录失败: {str(e)}", exc_info=True)
        return False

def get_arbitrage_push_count() -> Dict[str, int]:
    """
    获取套利推送统计信息
    
    Returns:
        Dict[str, int]: 包含总推送量和今日推送量的字典
    """
    try:
        # 加载套利状态
        status = load_arbitrage_status()
        
        # 获取当前日期
        from utils.date_utils import get_beijing_time
        today = get_beijing_time().strftime("%Y-%m-%d")
        
        # 统计总推送量和今日推送量
        total_count = len(status)
        today_count = sum(1 for date in status.values() if date == today)
        
        logger.debug(f"套利推送统计: 总推送量={total_count}, 今日推送量={today_count}")
        return {
            "total": total_count,
            "today": today_count
        }
    
    except Exception as e:
        logger.error(f"获取套利推送统计失败: {str(e)}", exc_info=True)
        return {
            "total": 0,
            "today": 0
        }

def get_arbitrage_push_history(days: int = 7) -> Dict[str, int]:
    """
    获取套利推送历史记录
    
    Args:
        days: 查询天数
    
    Returns:
        Dict[str, int]: {日期: 推送数量} 的字典
    """
    try:
        # 加载套利状态
        status = load_arbitrage_status()
        
        # 初始化历史记录
        history = {}
        
        # 获取当前日期
        from utils.date_utils import get_beijing_time
        current_date = get_beijing_time()
        
        # 生成过去days天的日期列表
        for i in range(days):
            date = (current_date - timedelta(days=i)).strftime("%Y-%m-%d")
            history[date] = 0
        
        # 统计每天的推送数量
        for date in status.values():
            if date in history:
                history[date] += 1
        
        logger.debug(f"获取套利推送历史记录成功，共 {len(history)} 天")
        return history
    
    except Exception as e:
        logger.error(f"获取套利推送历史记录失败: {str(e)}", exc_info=True)
        return {}

# ==========================
# 模块初始化
# ==========================

# 在模块加载时，确保必要的目录存在并清理过期的套利状态
try:
    # 确保FLAG_DIR存在
    if not os.path.exists(Config.FLAG_DIR):
        os.makedirs(Config.FLAG_DIR, exist_ok=True)
        logger.info(f"创建FLAG_DIR目录: {Config.FLAG_DIR}")
    
    # 清理过期的套利状态记录
    if os.path.exists(Config.ARBITRAGE_STATUS_FILE):
        clear_expired_arbitrage_status()
    
    # 初始化日志
    logger.info("文件工具模块初始化完成")
    
except Exception as e:
    error_msg = f"文件工具模块初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
    
    try:
        # 退回到基础日志配置
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(error_msg)
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(error_msg)
