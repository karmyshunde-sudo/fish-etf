# etf_list_manager.py
import akshare as ak
import pandas as pd
import os
import logging
import requests
import time
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from retrying import retry
from config import Config

# 初始化日志
logger = logging.getLogger(__name__)

# 列表更新频率（天）
LIST_UPDATE_INTERVAL = 7

# 请求超时设置（秒）
REQUEST_TIMEOUT = 30

# 缓存变量，避免重复加载
_etf_list_cache = None
_last_load_time = None

def load_all_etf_list() -> pd.DataFrame:
    """
    加载全市场ETF列表，使用缓存机制避免重复加载
    :return: 包含ETF信息的DataFrame
    """
    global _etf_list_cache, _last_load_time
    
    # 检查缓存是否有效（5分钟内）
    if (_etf_list_cache is not None and 
        _last_load_time is not None and 
        (datetime.now() - _last_load_time).total_seconds() < 300):
        logger.debug("使用缓存的ETF列表")
        return _etf_list_cache.copy()
    
    # 更新ETF列表
    _etf_list_cache = update_all_etf_list()
    _last_load_time = datetime.now()
    
    return _etf_list_cache.copy() if _etf_list_cache is not None else pd.DataFrame()

def is_list_need_update() -> bool:
    """
    判断是否需要更新全市场ETF列表
    :return: 如果需要更新返回True，否则返回False
    """
    if not os.path.exists(Config.ALL_ETFS_PATH):
        logger.info("ETF列表文件不存在，需要更新")
        return True
        
    try:
        # 获取文件最后修改时间（转换为东八区时区）
        last_modify_time = datetime.fromtimestamp(os.path.getmtime(Config.ALL_ETFS_PATH))
        last_modify_time = last_modify_time.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))
        
        # 计算距离上次更新的天数
        days_since_update = (datetime.now(timezone(timedelta(hours=8))) - last_modify_time).days
        need_update = days_since_update >= LIST_UPDATE_INTERVAL
        
        if need_update:
            logger.info(f"ETF列表已过期({days_since_update}天)，需要更新")
        else:
            logger.debug(f"ETF列表未过期({days_since_update}天)，无需更新")
            
        return need_update
    except Exception as e:
        logger.error(f"检查ETF列表更新状态失败: {str(e)}")
        # 出错时保守策略是要求更新
        return True

def retry_if_network_error(exception: Exception) -> bool:
    """
    重试条件：网络相关错误
    :param exception: 异常对象
    :return: 如果是网络错误返回True，否则返回False
    """
    return isinstance(exception, (requests.RequestException, ConnectionError, TimeoutError))

@retry(
    stop_max_attempt_number=3,
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    retry_on_exception=retry_if_network_error
)
def fetch_all_etfs_akshare() -> pd.DataFrame:
    """
    使用AkShare接口获取ETF列表（带规模和成交额筛选）
    :return: 包含ETF信息的DataFrame
    """
    try:
        logger.info("尝试从AkShare获取ETF列表...")
        # 调用fund_etf_spot_em接口
        etf_info = ak.fund_etf_spot_em()
        
        if etf_info.empty:
            logger.warning("AkShare返回空的ETF列表")
            return pd.DataFrame()
        
        # 记录返回的列名，用于调试
        logger.debug(f"AkShare返回列名: {list(etf_info.columns)}")
        
        # 标准化列名映射
        column_mapping = {}
        for col in etf_info.columns:
            if "代码" in col:
                column_mapping[col] = "ETF代码"
            elif "名称" in col:
                column_mapping[col] = "ETF名称"
            elif "规模" in col:
                column_mapping[col] = "基金规模"
            elif "成交额" in col or "金额" in col:
                column_mapping[col] = "日均成交额"
        
        # 重命名列
        etf_info = etf_info.rename(columns=column_mapping)
        
        # 确保包含所有需要的列
        required_columns = Config.ETF_STANDARD_COLUMNS + ["日均成交额"]
        for col in required_columns:
            if col not in etf_info.columns:
                etf_info[col] = ""
        
        # 数据清洗：确保代码为6位数字
        etf_info["ETF代码"] = etf_info["ETF代码"].astype(str).str.strip().str.zfill(6)
        etf_info = etf_info[etf_info["ETF代码"].str.match(r'^\d{6}$')]
        
        # 筛选条件：基金规模和日均成交额
        etf_info["基金规模"] = etf_info["基金规模"].apply(convert_fund_size)
        etf_info["日均成交额"] = etf_info["日均成交额"].apply(convert_volume)
        
        # 应用筛选条件
        filtered_etfs = etf_info[
            (etf_info["基金规模"] >= Config.MIN_FUND_SIZE) &
            (etf_info["日均成交额"] >= Config.MIN_AVG_VOLUME)
        ].copy()
        
        # 添加完整代码列（带市场前缀）
        filtered_etfs["完整代码"] = filtered_etfs["ETF代码"].apply(get_full_etf_code)
        
        # 按基金规模降序排序
        filtered_etfs = filtered_etfs.sort_values("基金规模", ascending=False)
        
        # 移除日均成交额列（不保存在文件中）
        filtered_etfs = filtered_etfs[Config.ETF_STANDARD_COLUMNS]
        
        logger.info(f"AkShare获取到{len(etf_info)}只ETF，筛选后剩余{len(filtered_etfs)}只")
        return filtered_etfs.drop_duplicates(subset="ETF代码")
        
    except Exception as e:
        error_msg = f"AkShare接口错误: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)

def get_full_etf_code(etf_code: str) -> str:
    """
    根据ETF代码获取完整代码（带市场前缀）
    :param etf_code: ETF代码
    :return: 完整代码（带市场前缀）
    """
    if not etf_code or not isinstance(etf_code, str):
        return ""
        
    etf_code = str(etf_code).strip().zfill(6)
    if etf_code.startswith(('5', '6', '9')):
        return f"sh.{etf_code}"
    else:
        return f"sz.{etf_code}"

def convert_fund_size(size_str: Any) -> float:
    """
    将基金规模字符串转换为数值（单位：亿元）
    :param size_str: 规模字符串
    :return: 规模数值（亿元）
    """
    try:
        if isinstance(size_str, (int, float)):
            return float(size_str)
        
        size_str = str(size_str).strip()
        if "亿" in size_str:
            return float(size_str.replace("亿", "").replace(",", "").strip())
        elif "万" in size_str:
            return float(size_str.replace("万", "").replace(",", "").strip()) / 10000
        else:
            return float(size_str) if size_str else 0.0
    except (ValueError, TypeError):
        logger.warning(f"转换基金规模失败: {size_str}")
        return 0.0

def convert_volume(volume_str: Any) -> float:
    """
    将成交额字符串转换为数值（单位：万元）
    :param volume_str: 成交额字符串
    :return: 成交额数值（万元）
    """
    try:
        if isinstance(volume_str, (int, float)):
            return float(volume_str)
        
        volume_str = str(volume_str).strip()
        if "亿" in volume_str:
            return float(volume_str.replace("亿", "").replace(",", "").strip()) * 10000
        elif "万" in volume_str:
            return float(volume_str.replace("万", "").replace(",", "").strip())
        else:
            return float(volume_str) if volume_str else 0.0
    except (ValueError, TypeError):
        logger.warning(f"转换成交额失败: {volume_str}")
        return 0.0

@retry(
    stop_max_attempt_number=2,
    wait_exponential_multiplier=1000,
    wait_exponential_max=5000,
    retry_on_exception=retry_if_network_error
)
def fetch_all_etfs_sina() -> pd.DataFrame:
    """
    新浪接口兜底获取ETF列表（带超时控制）
    :return: 包含ETF信息的DataFrame
    """
    try:
        logger.info("尝试从新浪获取ETF列表...")
        url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList"
        params = {"page": 1, "num": 1000, "sort": "symbol", "asc": 1}
        
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        # 处理新浪接口返回的数据
        try:
            etf_data = response.json()
        except ValueError:
            # 如果JSON解析失败，尝试eval
            try:
                etf_data = eval(response.text)
            except:
                logger.error("新浪接口返回的数据格式无法解析")
                return pd.DataFrame()
        
        # 确保数据是列表格式
        if not isinstance(etf_data, list):
            logger.warning("新浪接口返回的数据不是列表格式")
            return pd.DataFrame()
        
        # 创建DataFrame
        if etf_data:
            etf_list = pd.DataFrame(etf_data)
            # 检查必要的列是否存在
            if "symbol" in etf_list.columns and "name" in etf_list.columns:
                etf_list = etf_list.rename(columns={
                    "symbol": "完整代码",
                    "name": "ETF名称"
                })
                
                # 提取纯数字代码
                etf_list["ETF代码"] = etf_list["完整代码"].str[-6:].str.strip()
                
                # 添加空白的基金规模列
                etf_list["基金规模"] = 0.0
                
                # 确保包含所有需要的列
                for col in Config.ETF_STANDARD_COLUMNS:
                    if col not in etf_list.columns:
                        etf_list[col] = ""
                
                etf_list = etf_list[Config.ETF_STANDARD_COLUMNS]
                
                # 按基金规模降序排序
                etf_list = etf_list.sort_values("基金规模", ascending=False)
                
                logger.info(f"新浪获取到{len(etf_list)}只ETF")
                return etf_list.drop_duplicates(subset="ETF代码")
            else:
                logger.warning("新浪接口返回的数据缺少必要列")
                return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        else:
            logger.warning("新浪接口返回空数据")
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
            
    except Exception as e:
        error_msg = f"新浪接口错误: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)

def read_csv_with_encoding(file_path: str) -> pd.DataFrame:
    """
    读取CSV文件，自动兼容UTF-8和GBK编码
    :param file_path: 文件路径
    :return: 读取的DataFrame
    """
    encodings = ["utf-8", "gbk", "latin-1", "utf-8-sig"]
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            # 确保包含所有需要的列
            required_columns = Config.ETF_STANDARD_COLUMNS
            for col in required_columns:
                if col not in df.columns:
                    df[col] = ""
            return df[required_columns].copy()
        except (UnicodeDecodeError, LookupError, KeyError) as e:
            logger.debug(f"尝试编码 {encoding} 失败: {str(e)}")
            continue
    raise Exception(f"无法解析文件 {file_path}，尝试了编码: {encodings}")

def update_all_etf_list() -> pd.DataFrame:
    """
    更新全市场ETF列表（三级降级策略）
    :return: 包含ETF信息的DataFrame
    """
    try:
        Config.init_dirs()
        primary_etf_list = None
        
        if is_list_need_update():
            logger.info("🔍 尝试更新全市场ETF列表...")
            
            # 1. 尝试AkShare接口
            try:
                etf_list = fetch_all_etfs_akshare()
                if not etf_list.empty:
                    # 确保包含所有需要的列
                    required_columns = Config.ETF_STANDARD_COLUMNS
                    for col in required_columns:
                        if col not in etf_list.columns:
                            etf_list[col] = ""
                    etf_list = etf_list[required_columns]
                    
                    # 按基金规模降序排序
                    etf_list = etf_list.sort_values("基金规模", ascending=False)
                    
                    etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                    logger.info(f"✅ AkShare更新成功（{len(etf_list)}只ETF）")
                    primary_etf_list = etf_list
                else:
                    logger.warning("AkShare返回空的ETF列表")
            except Exception as e:
                logger.error(f"❌ AkShare更新失败: {str(e)}")
            
            # 2. 尝试新浪接口（仅当AkShare失败时）
            if primary_etf_list is None:
                try:
                    etf_list = fetch_all_etfs_sina()
                    if not etf_list.empty:
                        # 确保包含所有需要的列
                        required_columns = Config.ETF_STANDARD_COLUMNS
                        for col in required_columns:
                            if col not in etf_list.columns:
                                etf_list[col] = ""
                        etf_list = etf_list[required_columns]
                        
                        etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                        logger.info(f"✅ 新浪接口更新成功（{len(etf_list)}只ETF）")
                        primary_etf_list = etf_list
                    else:
                        logger.warning("新浪接口返回空的ETF列表")
                except Exception as e:
                    logger.error(f"❌ 新浪接口更新失败: {str(e)}")
            
            # 新增逻辑：第一次初始化时同步补充兜底文件
            backup_file_exists = os.path.exists(Config.BACKUP_ETFS_PATH)
            backup_file_empty = False
            if backup_file_exists:
                backup_file_empty = os.path.getsize(Config.BACKUP_ETFS_PATH) == 0
            
            if not backup_file_exists or backup_file_empty:
                logger.info("🔄 检测到兜底文件未初始化，开始同步数据...")
                
                if primary_etf_list is not None and not primary_etf_list.empty:
                    backup_df = primary_etf_list.copy()
                    backup_df.to_csv(Config.BACKUP_ETFS_PATH, index=False, encoding="utf-8")
                    logger.info(f"✅ 已从新获取数据同步兜底文件（{len(backup_df)}条记录）")
                
                elif os.path.exists(Config.ALL_ETFS_PATH) and os.path.getsize(Config.ALL_ETFS_PATH) > 0:
                    try:
                        all_etfs_df = read_csv_with_encoding(Config.ALL_ETFS_PATH)
                        all_etfs_df.to_csv(Config.BACKUP_ETFS_PATH, index=False, encoding="utf-8")
                        logger.info(f"✅ 已从本地all_etfs.csv同步兜底文件（{len(all_etfs_df)}条记录）")
                    except Exception as e:
                        logger.error(f"❌ 从all_etfs.csv同步兜底文件失败: {str(e)}")
            
            # 3. 尝试兜底文件（如果主数据源都失败）
            if primary_etf_list is None:
                if os.path.exists(Config.BACKUP_ETFS_PATH):
                    try:
                        backup_df = read_csv_with_encoding(Config.BACKUP_ETFS_PATH)
                        
                        # 验证必要列
                        required_columns = Config.ETF_STANDARD_COLUMNS
                        for col in required_columns:
                            if col not in backup_df.columns:
                                backup_df[col] = ""
                        
                        # 数据清洗
                        backup_df["ETF代码"] = backup_df["ETF代码"].astype(str).str.strip().str.zfill(6)
                        backup_df = backup_df[backup_df["ETF代码"].str.match(r'^\d{6}$')]
                        backup_df = backup_df[required_columns].drop_duplicates()
                        
                        # 按基金规模降序排序
                        backup_df = backup_df.sort_values("基金规模", ascending=False)
                        
                        logger.info(f"✅ 兜底文件加载成功（{len(backup_df)}只ETF）")
                        return backup_df
                    except Exception as e:
                        logger.error(f"❌ 兜底文件处理失败: {str(e)}")
                        # 返回空DataFrame但包含所有列
                        empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
                        return empty_df
                else:
                    logger.error(f"❌ 兜底文件不存在: {Config.BACKUP_ETFS_PATH}")
                    # 返回空DataFrame但包含所有列
                    empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
                    return empty_df
            
            return primary_etf_list
        
        else:
            logger.info("ℹ️ 无需更新，加载本地ETF列表")
            try:
                etf_list = read_csv_with_encoding(Config.ALL_ETFS_PATH)
                # 确保包含所有需要的列
                required_columns = Config.ETF_STANDARD_COLUMNS
                for col in required_columns:
                    if col not in etf_list.columns:
                        etf_list[col] = ""
                
                # 按基金规模降序排序
                etf_list = etf_list.sort_values("基金规模", ascending=False)
                
                return etf_list
            except Exception as e:
                logger.error(f"❌ 本地文件加载失败: {str(e)}")
                # 返回空DataFrame但包含所有列
                empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
                return empty_df
                
    except Exception as e:
        logger.error(f"❌ 更新ETF列表时发生未预期错误: {str(e)}")
        # 返回空DataFrame但包含所有列
        return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)

def get_filtered_etf_codes() -> List[str]:
    """
    获取过滤后的有效ETF代码列表
    :return: ETF代码列表
    """
    try:
        etf_list = update_all_etf_list()
        if etf_list.empty:
            logger.warning("⚠️ 无有效ETF代码列表")
            return []
        
        # 确保ETF代码为字符串类型
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.strip()
        valid_codes = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]["ETF代码"].tolist()
        logger.info(f"📊 有效ETF代码数量: {len(valid_codes)}")
        return valid_codes
    except Exception as e:
        logger.error(f"获取有效ETF代码列表失败: {str(e)}")
        return []

# 初始化模块
try:
    Config.init_dirs()
    logger.info("ETF列表管理器初始化完成")
except Exception as e:
    logger.error(f"ETF列表管理器初始化失败: {str(e)}")
# 0828-1256【etf_list_manager.py代码】一共389行代码
