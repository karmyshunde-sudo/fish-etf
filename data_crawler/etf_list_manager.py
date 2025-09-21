import os
import akshare as ak
import pandas as pd
import logging
import requests
import time
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from retrying import retry
from config import Config
from utils.date_utils import is_file_outdated, get_beijing_time  # 确保导入get_beijing_time

# 初始化日志
logger = logging.getLogger(__name__)

# 缓存变量，避免重复加载
_etf_list_cache = None
_last_load_time = None

def load_all_etf_list() -> pd.DataFrame:
    """加载全市场ETF列表，使用缓存机制避免重复加载
    :return: 包含ETF信息的DataFrame
    """
    global _etf_list_cache, _last_load_time
    
    # 检查缓存是否有效（5分钟内）
    if (_etf_list_cache is not None and 
        _last_load_time is not None and 
        (datetime.now() - _last_load_time).total_seconds() < 300):
        
        # 严格验证缓存数据
        if not isinstance(_etf_list_cache, pd.DataFrame):
            logger.warning("ETF列表缓存不是DataFrame类型，将重新加载")
        elif _etf_list_cache.empty:
            logger.warning("ETF列表缓存为空，将重新加载")
        elif not validate_etf_list(_etf_list_cache):
            logger.warning("ETF列表缓存验证失败，将重新加载")
        else:
            logger.debug(f"使用缓存的ETF列表 (共{_etf_list_cache.shape[0]}条记录)")
            # 创建深拷贝并确保ETF代码格式正确
            cached_df = _etf_list_cache.copy(deep=True)
            
            # 确保ETF代码是字符串类型且格式正确
            if "ETF代码" in cached_df.columns:
                # 检查列是否包含非字符串值
                has_non_string = cached_df["ETF代码"].apply(lambda x: not isinstance(x, str)).any()
                
                # 如果列包含非字符串值，或者列是数值类型，则进行转换
                if has_non_string or pd.api.types.is_numeric_dtype(cached_df["ETF代码"]):
                    cached_df.loc[:, "ETF代码"] = cached_df["ETF代码"].astype(str)
                
                # 确保ETF代码是6位数字
                cached_df.loc[:, "ETF代码"] = cached_df["ETF代码"].str.strip().str.zfill(6)
            
            return cached_df
    
    # 更新ETF列表
    try:
        new_etf_list = update_all_etf_list()
        
        # 严格验证新获取的数据
        if not isinstance(new_etf_list, pd.DataFrame):
            logger.error("update_all_etf_list() 返回的不是DataFrame类型")
            _etf_list_cache = pd.DataFrame()
        elif new_etf_list.empty:
            logger.warning("update_all_etf_list() 返回空DataFrame")
            _etf_list_cache = pd.DataFrame()
        else:
            # 验证ETF列表
            if not validate_etf_list(new_etf_list):
                logger.warning("ETF列表验证失败，尝试修复...")
                new_etf_list = repair_etf_list(new_etf_list)
                if not validate_etf_list(new_etf_list):
                    logger.error("ETF列表修复失败，返回空DataFrame")
                    return pd.DataFrame()
            
            # 创建深拷贝避免SettingWithCopyWarning
            _etf_list_cache = new_etf_list.copy(deep=True)
            
            # 确保包含必要列
            required_columns = Config.ETF_STANDARD_COLUMNS
            missing_columns = [col for col in required_columns if col not in _etf_list_cache.columns]
            
            if missing_columns:
                logger.error(f"ETF列表缺少必要列: {', '.join(missing_columns)}")
                logger.debug(f"实际列名: {list(_etf_list_cache.columns)}")
                # 尝试修复缺失的列
                for col in missing_columns:
                    if col == "ETF代码" and "代码" in _etf_list_cache.columns:
                        _etf_list_cache.rename(columns={"代码": "ETF代码"}, inplace=True)
                    elif col == "ETF名称" and "名称" in _etf_list_cache.columns:
                        _etf_list_cache.rename(columns={"名称": "ETF名称"}, inplace=True)
                    else:
                        _etf_list_cache[col] = "" if col != "基金规模" else 0.0
            
            # 确保ETF代码是字符串类型且格式正确
            if "ETF代码" in _etf_list_cache.columns:
                # 检查列是否包含非字符串值
                has_non_string = _etf_list_cache["ETF代码"].apply(lambda x: not isinstance(x, str)).any()
                
                # 如果列包含非字符串值，或者列是数值类型，则进行转换
                if has_non_string or pd.api.types.is_numeric_dtype(_etf_list_cache["ETF代码"]):
                    _etf_list_cache.loc[:, "ETF代码"] = _etf_list_cache["ETF代码"].astype(str)
                
                # 确保ETF代码是6位数字
                _etf_list_cache.loc[:, "ETF代码"] = _etf_list_cache["ETF代码"].str.strip().str.zfill(6)
                
                # 过滤无效的ETF代码（非6位数字）
                _etf_list_cache = _etf_list_cache[
                    _etf_list_cache["ETF代码"].str.match(r'^\d{6}$')
                ].copy()
            
            logger.info(f"成功加载ETF列表，共{_etf_list_cache.shape[0]}条有效记录")
        
        _last_load_time = datetime.now()
        return _etf_list_cache.copy() if _etf_list_cache is not None else pd.DataFrame()
    
    except Exception as e:
        logger.error(f"加载ETF列表时发生异常: {str(e)}", exc_info=True)
        # 尝试返回空DataFrame而不是抛出异常
        return pd.DataFrame()

def update_all_etf_list() -> pd.DataFrame:
    """更新ETF列表（优先使用本地文件，若需更新则从网络获取）
    :return: 包含ETF信息的DataFrame
    """
    # ===== 关键修复：添加周日强制更新逻辑 =====
    # 获取当前北京时间
    beijing_time = get_beijing_time()
    # 判断是否为周日（星期日的索引是6，星期一的索引是0）
    is_sunday = beijing_time.weekday() == 6
    
    # 检查是否需要更新 - 周日强制更新
    if is_sunday or not os.path.exists(Config.ALL_ETFS_PATH) or is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
        logger.info(f"{'[强制更新] ' if is_sunday else ''}ETF列表文件不存在或已过期，尝试从网络获取...")
        try:
            primary_etf_list = None
            
            # 1. 尝试AkShare接口
            logger.info("尝试从AkShare获取ETF列表...")
            primary_etf_list = fetch_all_etfs_akshare()
            
            if not primary_etf_list.empty:
                # 验证ETF列表
                if not validate_etf_list(primary_etf_list):
                    logger.warning("ETF列表验证失败，尝试修复...")
                    primary_etf_list = repair_etf_list(primary_etf_list)
                    if not validate_etf_list(primary_etf_list):
                        logger.error("ETF列表修复失败，跳过保存")
                        primary_etf_list = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
                
                # 确保包含所有需要的列
                required_columns = Config.ETF_STANDARD_COLUMNS
                for col in required_columns:
                    if col not in primary_etf_list.columns:
                        primary_etf_list[col] = ""
                primary_etf_list = primary_etf_list[required_columns]
                # 按基金规模降序排序
                primary_etf_list = primary_etf_list.sort_values("基金规模", ascending=False)
                
                # 保存前再次验证
                if validate_etf_list(primary_etf_list):
                    # 确保ETF代码格式正确
                    primary_etf_list["ETF代码"] = primary_etf_list["ETF代码"].astype(str).str.strip().str.zfill(6)
                    # 过滤无效的ETF代码
                    primary_etf_list = primary_etf_list[primary_etf_list["ETF代码"].str.match(r'^\d{6}$')]
                    
                    primary_etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                    logger.info(f"✅ AkShare更新成功（{len(primary_etf_list)}只ETF）")
                    # 标记数据来源
                    primary_etf_list.source = "AkShare"
                else:
                    logger.error("ETF列表验证失败，跳过保存")
            else:
                logger.warning("AkShare返回空的ETF列表")
            
            # 2. 如果AkShare失败，尝试新浪接口
            if primary_etf_list is None or primary_etf_list.empty:
                logger.info("尝试从新浪获取ETF列表...")
                primary_etf_list = fetch_all_etfs_sina()
                
                if not primary_etf_list.empty:
                    # 验证ETF列表
                    if not validate_etf_list(primary_etf_list):
                        logger.warning("ETF列表验证失败，尝试修复...")
                        primary_etf_list = repair_etf_list(primary_etf_list)
                        if not validate_etf_list(primary_etf_list):
                            logger.error("ETF列表修复失败，跳过保存")
                            primary_etf_list = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
                    
                    # 确保包含所有需要的列
                    required_columns = Config.ETF_STANDARD_COLUMNS
                    for col in required_columns:
                        if col not in primary_etf_list.columns:
                            primary_etf_list[col] = ""
                    primary_etf_list = primary_etf_list[required_columns]
                    
                    # 保存前再次验证
                    if validate_etf_list(primary_etf_list):
                        # 确保ETF代码格式正确
                        primary_etf_list["ETF代码"] = primary_etf_list["ETF代码"].astype(str).str.strip().str.zfill(6)
                        # 过滤无效的ETF代码
                        primary_etf_list = primary_etf_list[primary_etf_list["ETF代码"].str.match(r'^\d{6}$')]
                        
                        primary_etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                        logger.info(f"✅ 新浪接口更新成功（{len(primary_etf_list)}只ETF）")
                        # 标记数据来源
                        primary_etf_list.source = "新浪"
                    else:
                        logger.error("ETF列表验证失败，跳过保存")
                else:
                    logger.warning("新浪接口返回空的ETF列表")
            
            # 3. 如果前两者都失败，使用兜底文件
            if primary_etf_list is None or primary_etf_list.empty:
                logger.info("尝试加载兜底ETF列表文件...")
                if os.path.exists(Config.BACKUP_ETFS_PATH):
                    try:
                        backup_df = read_csv_with_encoding(Config.BACKUP_ETFS_PATH)
                        # 确保ETF代码格式正确
                        backup_df["ETF代码"] = backup_df["ETF代码"].astype(str).str.strip().str.zfill(6)
                        backup_df = backup_df[backup_df["ETF代码"].str.match(r'^\d{6}$')]
                        # 确保包含所有需要的列
                        required_columns = Config.ETF_STANDARD_COLUMNS
                        for col in required_columns:
                            if col not in backup_df.columns:
                                backup_df[col] = ""
                        backup_df = backup_df[required_columns].drop_duplicates()
                        # 按基金规模降序排序
                        backup_df = backup_df.sort_values("基金规模", ascending=False)
                        
                        # 验证ETF列表
                        if validate_etf_list(backup_df):
                            logger.info(f"✅ 兜底文件加载成功（{len(backup_df)}只ETF）")
                            # 标记数据来源
                            backup_df.source = "兜底文件"
                            
                            # 保存兜底文件为当前ETF列表
                            backup_df.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                            return backup_df
                        else:
                            logger.warning("兜底文件验证失败，尝试修复...")
                            backup_df = repair_etf_list(backup_df)
                            if validate_etf_list(backup_df):
                                backup_df.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                                logger.info(f"✅ 兜底文件修复后加载成功（{len(backup_df)}只ETF）")
                                backup_df.source = "兜底文件(修复后)"
                                return backup_df
                    except Exception as e:
                        logger.error(f"❌ 兜底文件处理失败: {str(e)}")
                
                # 如果兜底文件也不存在或处理失败，返回空DataFrame但包含所有列
                logger.error("❌ 无法获取ETF列表，所有数据源均失败")
                empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
                # 标记数据来源
                empty_df.source = "无数据源"
                return empty_df
            
            # 返回前验证最终结果
            if not primary_etf_list.empty and validate_etf_list(primary_etf_list):
                return primary_etf_list
            else:
                logger.warning("获取的ETF列表验证失败，返回空DataFrame")
                return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        except Exception as e:
            logger.error(f"❌ 更新ETF列表时发生未预期错误: {str(e)}", exc_info=True)
            # 尝试加载兜底文件作为最后手段
            if os.path.exists(Config.BACKUP_ETFS_PATH):
                try:
                    backup_df = read_csv_with_encoding(Config.BACKUP_ETFS_PATH)
                    backup_df["ETF代码"] = backup_df["ETF代码"].astype(str).str.strip().str.zfill(6)
                    backup_df = backup_df[backup_df["ETF代码"].str.match(r'^\d{6}$')]
                    required_columns = Config.ETF_STANDARD_COLUMNS
                    for col in required_columns:
                        if col not in backup_df.columns:
                            backup_df[col] = ""
                    backup_df = backup_df[required_columns].drop_duplicates()
                    backup_df = backup_df.sort_values("基金规模", ascending=False)
                    
                    # 验证兜底文件
                    if validate_etf_list(backup_df):
                        logger.warning("⚠️ 使用兜底文件作为最后手段")
                        # 标记数据来源
                        backup_df.source = "兜底文件(异常)"
                        return backup_df
                    else:
                        logger.warning("兜底文件验证失败，尝试修复...")
                        backup_df = repair_etf_list(backup_df)
                        if validate_etf_list(backup_df):
                            logger.warning("⚠️ 使用修复后的兜底文件作为最后手段")
                            backup_df.source = "兜底文件(异常修复后)"
                            return backup_df
                except Exception as e:
                    logger.error(f"❌ 兜底文件加载也失败: {str(e)}", exc_info=True)
            
            # 返回空DataFrame但包含所有列
            empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
            # 标记数据来源
            empty_df.source = "无数据源(异常)"
            return empty_df
    
    else:
        logger.info("ℹ️ 无需更新，加载本地ETF列表")
        try:
            etf_list = read_csv_with_encoding(Config.ALL_ETFS_PATH)
            # 确保包含所有需要的列
            required_columns = Config.ETF_STANDARD_COLUMNS
            for col in required_columns:
                if col not in etf_list.columns:
                    etf_list[col] = ""
            # 确保ETF代码格式正确
            etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.strip().str.zfill(6)
            # 过滤无效的ETF代码
            etf_list = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]
            # 按基金规模降序排序
            etf_list = etf_list.sort_values("基金规模", ascending=False)
            
            # 验证ETF列表
            if validate_etf_list(etf_list):
                # 标记数据来源
                etf_list.source = "本地缓存"
                return etf_list
            else:
                logger.warning("本地ETF列表验证失败，尝试修复...")
                etf_list = repair_etf_list(etf_list)
                if validate_etf_list(etf_list):
                    etf_list.source = "本地缓存(修复后)"
                    return etf_list
                else:
                    logger.error("本地ETF列表修复失败，尝试从网络获取")
                    return update_all_etf_list()
        except Exception as e:
            logger.error(f"❌ 本地文件加载失败: {str(e)}", exc_info=True)
            # 尝试加载兜底文件
            if os.path.exists(Config.BACKUP_ETFS_PATH):
                try:
                    backup_df = read_csv_with_encoding(Config.BACKUP_ETFS_PATH)
                    backup_df["ETF代码"] = backup_df["ETF代码"].astype(str).str.strip().str.zfill(6)
                    backup_df = backup_df[backup_df["ETF代码"].str.match(r'^\d{6}$')]
                    required_columns = Config.ETF_STANDARD_COLUMNS
                    for col in required_columns:
                        if col not in backup_df.columns:
                            backup_df[col] = ""
                    backup_df = backup_df[required_columns].drop_duplicates()
                    backup_df = backup_df.sort_values("基金规模", ascending=False)
                    
                    # 验证兜底文件
                    if validate_etf_list(backup_df):
                        logger.warning("⚠️ 本地文件加载失败，使用兜底文件")
                        # 标记数据来源
                        backup_df.source = "兜底文件(本地加载失败)"
                        return backup_df
                    else:
                        logger.warning("兜底文件验证失败，尝试修复...")
                        backup_df = repair_etf_list(backup_df)
                        if validate_etf_list(backup_df):
                            backup_df.source = "兜底文件(本地加载失败修复后)"
                            return backup_df
                except Exception as e:
                    logger.error(f"❌ 兜底文件加载也失败: {str(e)}", exc_info=True)
            
            # 返回空DataFrame但包含所有列
            empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
            # 标记数据来源
            empty_df.source = "无数据源(本地加载失败)"
            return empty_df

def retry_if_network_error(exception: Exception) -> bool:
    """重试条件：网络相关错误
    :param exception: 异常对象
    :return: 如果是网络错误返回True，否则返回False"""
    return isinstance(exception, (requests.RequestException, ConnectionError, TimeoutError))

@retry(stop_max_attempt_number=3,
       wait_exponential_multiplier=1000,
       wait_exponential_max=10000,
       retry_on_exception=retry_if_network_error)

def fetch_all_etfs_akshare() -> pd.DataFrame:
    """使用AkShare接口获取ETF列表（带规模和成交额筛选）
    :return: 包含ETF信息的DataFrame"""
    try:
        logger.info("尝试从AkShare获取ETF列表...")
        # 调用fund_etf_spot_em接口
        etf_info = ak.fund_etf_spot_em()
        if etf_info.empty:
            logger.warning("AkShare返回空的ETF列表")
            return pd.DataFrame()
        
        # 记录返回的列名，用于调试
        logger.debug(f"AkShare返回列名: {list(etf_info.columns)}")
        
        # 标准化列名映射（根据实际返回列名修正）
        column_mapping = {}
        for col in etf_info.columns:
            if "代码" in col:
                column_mapping[col] = "ETF代码"
            elif "名称" in col:
                column_mapping[col] = "ETF名称"
            elif "流通市值" in col or "最新规模" in col or "规模" in col:
                column_mapping[col] = "基金规模"
            elif "成交额" in col or "日均成交额" in col:
                column_mapping[col] = "日均成交额"
            elif "涨跌幅" in col:
                column_mapping[col] = "涨跌幅"
            elif "净值" in col:
                column_mapping[col] = "净值"
        
        # 重命名列
        etf_info = etf_info.rename(columns=column_mapping)
        
        # 确保包含所有需要的列
        required_columns = Config.ETF_STANDARD_COLUMNS
        for col in required_columns:
            if col not in etf_info.columns:
                # 对于基金规模，尝试从其他可能的列获取
                if col == "基金规模" and ("最新规模" in etf_info.columns or "规模" in etf_info.columns):
                    if "最新规模" in etf_info.columns:
                        etf_info["基金规模"] = etf_info["最新规模"]
                    else:
                        etf_info["基金规模"] = etf_info["规模"]
                else:
                    etf_info[col] = "" if col != "基金规模" else 0.0
        
        # 数据清洗：确保代码为6位数字
        # 修复：先确保ETF代码列是字符串类型
        etf_info["ETF代码"] = etf_info["ETF代码"].astype(str)
        etf_info["ETF代码"] = etf_info["ETF代码"].str.strip().str.zfill(6)
        
        valid_etfs = etf_info[etf_info["ETF代码"].str.match(r'^\d{6}$', na=False)].copy()
        
        # 转换数据类型并处理单位
        # 流通市值单位为元，转换为亿元（除以1亿）
        valid_etfs["基金规模"] = pd.to_numeric(valid_etfs["基金规模"], errors="coerce")
        # 如果基金规模单位是亿元，不需要转换；如果是万元，转换为亿元
        if not valid_etfs.empty and valid_etfs["基金规模"].max() < 1000:  # 如果最大规模小于1000，可能是亿元单位
            pass
        else:  # 否则可能是万元单位，转换为亿元
            valid_etfs["基金规模"] = valid_etfs["基金规模"] / 10000
        
        # 检查是否有"日均成交额"列，如果有，转换为万元
        if "日均成交额" in valid_etfs.columns:
            valid_etfs["日均成交额"] = pd.to_numeric(valid_etfs["日均成交额"], errors="coerce") / 10000
        
        # 筛选条件：使用Config中定义的筛选参数
        filtered_etfs = valid_etfs[
            (valid_etfs["基金规模"] >= Config.GLOBAL_MIN_FUND_SIZE)
        ].copy()
        
        # 如果没有ETF通过筛选，返回原始数据（不筛选）
        if filtered_etfs.empty:
            logger.warning(f"ETF筛选条件过于严格，无符合要求的ETF（规模≥{Config.GLOBAL_MIN_FUND_SIZE}亿），返回全部ETF")
            filtered_etfs = valid_etfs.copy()
        
        filtered_etfs = filtered_etfs[Config.ETF_STANDARD_COLUMNS]
        logger.info(f"AkShare成功获取ETF列表，共 {len(filtered_etfs)} 条有效记录")
        return filtered_etfs
    
    except Exception as e:
        logger.error(f"获取ETF列表失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def fetch_all_etfs_sina() -> pd.DataFrame:
    """新浪接口兜底获取ETF列表（带超时控制）
    :return: 包含ETF信息的DataFrame"""
    try:
        logger.info("尝试从新浪获取ETF列表...")
        url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList  "
        params = {"page": 1, "num": 1000, "sort": "symbol", "asc": 1}
        response = requests.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)
        response.raise_for_status()
        
        # 处理新浪接口返回的数据
        try:
            # 尝试直接解析JSON
            etf_data = response.json()
        except ValueError:
            # 如果JSON解析失败，尝试处理可能的JavaScript格式
            try:
                # 移除可能的JavaScript前缀
                etf_data_str = response.text.replace('var data=', '').strip(';')
                # 尝试解析为JSON
                etf_data = json.loads(etf_data_str)
            except Exception as e:
                logger.error(f"JSON解析失败，尝试eval: {str(e)}")
                try:
                    # 作为最后手段使用eval
                    etf_data = eval(etf_data_str)
                except Exception as e:
                    logger.error(f"新浪接口返回的数据格式无法解析: {str(e)}")
                    return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        # 确保数据是列表格式
        if isinstance(etf_data, dict):
            # 尝试从常见字段中提取数据
            if 'data' in etf_data and isinstance(etf_data['data'], list):
                etf_data = etf_data['data']
            elif 'list' in etf_data and isinstance(etf_data['list'], list):
                etf_data = etf_data['list']
            elif 'result' in etf_data and 'data' in etf_data['result'] and isinstance(etf_data['result']['data'], list):
                etf_data = etf_data['result']['data']
            else:
                logger.warning("新浪接口返回的是字典但没有预期的数据结构")
                return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        # 确保etf_data是列表
        if not isinstance(etf_data, list):
            logger.error(f"新浪接口返回的数据不是列表格式: {type(etf_data)}")
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        # 创建DataFrame
        if not etf_data:
            logger.warning("新浪接口返回空列表")
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        etf_list = pd.DataFrame(etf_data)
        
        # 检查必要的列是否存在
        required_columns = ['symbol', 'name']
        missing_columns = [col for col in required_columns if col not in etf_list.columns]
        if missing_columns:
            logger.warning(f"新浪接口返回的数据缺少必要列: {', '.join(missing_columns)}")
            # 尝试从其他列名映射
            column_mapping = {}
            if 'symbol' in missing_columns and 'code' in etf_list.columns:
                column_mapping['code'] = 'symbol'
            if 'name' in missing_columns and 'name' in etf_list.columns:
                column_mapping['name'] = 'name'
            
            if column_mapping:
                etf_list = etf_list.rename(columns=column_mapping)
                missing_columns = [col for col in required_columns if col not in etf_list.columns]
            
            if missing_columns:
                logger.error(f"无法修复缺失的列: {', '.join(missing_columns)}")
                return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        # 重命名列
        etf_list = etf_list.rename(columns={
            "symbol": "完整代码",
            "name": "ETF名称"
        })
        
        # 提取纯数字代码（使用正则表达式确保6位数字）
        etf_list["ETF代码"] = etf_list["完整代码"].astype(str).str.extract(r'(\d{6})', expand=False)
        
        # 过滤有效的6位数字ETF代码
        valid_etfs = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$', na=False)].copy()
        
        if valid_etfs.empty:
            logger.warning("提取后无有效ETF代码")
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        # 添加基金规模列（如果可能）
        if "amount" in valid_etfs.columns:
            valid_etfs["基金规模"] = pd.to_numeric(valid_etfs["amount"], errors="coerce") / 10000
        else:
            valid_etfs["基金规模"] = 0.0
        
        # 确保包含所有需要的列
        for col in Config.ETF_STANDARD_COLUMNS:
            if col not in valid_etfs.columns:
                valid_etfs[col] = ""
        
        valid_etfs = valid_etfs[Config.ETF_STANDARD_COLUMNS]
        # 按基金规模降序排序
        valid_etfs = valid_etfs.sort_values("基金规模", ascending=False)
        
        logger.info(f"✅ 新浪接口成功获取{len(valid_etfs)}只ETF")
        return valid_etfs.drop_duplicates(subset="ETF代码")
    
    except Exception as e:
        error_msg = f"❌ 新浪接口错误: {str(e)}"
        logger.error(error_msg)
        return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)

def read_csv_with_encoding(file_path: str) -> pd.DataFrame:
    """读取CSV文件，自动兼容UTF-8和GBK编码
    :param file_path: 文件路径
    :return: 读取的DataFrame
    """
    # 定义明确的列数据类型
    dtype_dict = {
        "ETF代码": str,
        "ETF名称": str,
        "基金规模": float,
        "完整代码": str
    }
    
    try:
        return pd.read_csv(
            file_path, 
            encoding='utf-8',
            dtype={k: v for k, v in dtype_dict.items() if k != "完整代码"},  # 完整代码可能不存在
            keep_default_na=False  # 避免将空字符串转换为NaN
        )
    except UnicodeDecodeError:
        try:
            return pd.read_csv(
                file_path, 
                encoding='gbk',
                dtype={k: v for k, v in dtype_dict.items() if k != "完整代码"},
                keep_default_na=False
            )
        except Exception as e:
            logger.error(f"读取CSV文件失败: {str(e)}")
            return pd.DataFrame()

def get_filtered_etf_codes(min_size: float = None, exclude_money_etfs: bool = True) -> list:
    """获取过滤后的有效ETF代码列表
    :param min_size: 最小基金规模(亿元)，如果为None则使用Config.GLOBAL_MIN_FUND_SIZE
    :param exclude_money_etfs: 是否排除货币ETF(511开头)，默认True
    :return: ETF代码列表
    """
    try:
        etf_list = load_all_etf_list()
        if etf_list.empty:
            logger.warning("⚠️ 无有效ETF代码列表")
            return []
        
        # 使用配置中的默认值
        min_size = min_size if min_size is not None else Config.GLOBAL_MIN_FUND_SIZE
        
        # 确保ETF代码为字符串类型
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.strip()
        
        # 筛选有效ETF代码（6位数字）
        valid_etfs = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]
        
        # 应用规模过滤
        if "基金规模" in valid_etfs.columns:
            valid_etfs = valid_etfs[valid_etfs["基金规模"] >= min_size]
        
        # 应用货币ETF过滤（511开头）
        if exclude_money_etfs:
            valid_etfs = valid_etfs[~valid_etfs["ETF代码"].str.startswith("511")]
        
        valid_codes = valid_etfs["ETF代码"].tolist()
        logger.info(f"📊 有效ETF代码数量: {len(valid_codes)} (筛选条件: 规模≥{min_size}亿, {'排除' if exclude_money_etfs else '包含'}货币ETF)")
        return valid_codes
    except Exception as e:
        logger.error(f"获取有效ETF代码列表失败: {str(e)}")
        return []

def get_etf_name(etf_code: str) -> str:
    """根据ETF代码获取ETF名称
    :param etf_code: ETF代码
    :return: ETF名称
    """
    try:
        etf_list = load_all_etf_list()
        if etf_list.empty:
            logger.warning("ETF列表为空，无法获取ETF名称")
            return f"ETF-{etf_code}"
        
        # 确保ETF代码格式正确
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.strip().str.zfill(6)
        name_row = etf_list[etf_list["ETF代码"].astype(str).str.strip().str.zfill(6) == etf_code]
        if not name_row.empty:
            return name_row.iloc[0]["ETF名称"]
        else:
            logger.debug(f"未在全市场列表中找到ETF代码: {etf_code}")
            return f"ETF-{etf_code}"
    except Exception as e:
        logger.error(f"获取ETF名称失败: {str(e)}")
        return f"ETF-{etf_code}"

def validate_etf_list(etf_list: pd.DataFrame) -> bool:
    """验证ETF列表数据的完整性
    :param etf_list: ETF列表DataFrame
    :return: 数据是否有效
    """
    if etf_list.empty:
        logger.error("ETF列表为空")
        return False
    
    # 检查必要列
    required_columns = Config.ETF_STANDARD_COLUMNS
    missing_columns = [col for col in required_columns if col not in etf_list.columns]
    if missing_columns:
        logger.error(f"ETF列表缺少必要列: {', '.join(missing_columns)}")
        return False
    
    # 检查ETF代码格式
    invalid_codes = etf_list[~etf_list["ETF代码"].str.match(r'^\d{6}$')]
    if not invalid_codes.empty:
        logger.warning(f"ETF列表中发现 {len(invalid_codes)} 个无效ETF代码")
    
    # 检查基金规模是否为正数
    invalid_sizes = etf_list[etf_list["基金规模"] <= 0]
    if not invalid_sizes.empty:
        logger.warning(f"ETF列表中发现 {len(invalid_sizes)} 个基金规模≤0的ETF")
    
    return True

def repair_etf_list(etf_list: pd.DataFrame) -> pd.DataFrame:
    """修复ETF列表中的问题
    :param etf_list: ETF列表DataFrame
    :return: 修复后的ETF列表
    """
    if etf_list.empty:
        return etf_list
    
    # 创建深拷贝
    repaired_list = etf_list.copy(deep=True)
    
    # 修复ETF代码
    if "ETF代码" in repaired_list.columns:
        # 确保ETF代码是字符串类型
        repaired_list["ETF代码"] = repaired_list["ETF代码"].astype(str)
        # 移除非数字字符
        repaired_list["ETF代码"] = repaired_list["ETF代码"].str.replace(r'\D', '', regex=True)
        # 确保是6位数字
        repaired_list["ETF代码"] = repaired_list["ETF代码"].str.zfill(6)
        # 过滤无效的ETF代码
        repaired_list = repaired_list[repaired_list["ETF代码"].str.match(r'^\d{6}$')]
    
    # 修复基金规模
    if "基金规模" in repaired_list.columns:
        # 确保基金规模是数值类型
        repaired_list["基金规模"] = pd.to_numeric(repaired_list["基金规模"], errors="coerce")
        # 用平均值填充NaN
        if repaired_list["基金规模"].isna().any():
            mean_size = repaired_list["基金规模"].mean()
            repaired_list["基金规模"].fillna(mean_size, inplace=True)
    
    # 检查并修复列名
    for col in Config.ETF_STANDARD_COLUMNS:
        if col not in repaired_list.columns:
            if col == "ETF代码" and "代码" in repaired_list.columns:
                repaired_list.rename(columns={"代码": "ETF代码"}, inplace=True)
            elif col == "ETF名称" and "名称" in repaired_list.columns:
                repaired_list.rename(columns={"名称": "ETF名称"}, inplace=True)
            elif col == "基金规模" and ("最新规模" in repaired_list.columns or "规模" in repaired_list.columns):
                if "最新规模" in repaired_list.columns:
                    repaired_list["基金规模"] = repaired_list["最新规模"]
                else:
                    repaired_list["基金规模"] = repaired_list["规模"]
            else:
                repaired_list[col] = 0.0 if col == "基金规模" else ""
    
    return repaired_list
