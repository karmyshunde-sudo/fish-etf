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
    """从AkShare获取ETF列表
    :return: 包含ETF信息的DataFrame"""
    try:
        logger.info("尝试从AkShare获取ETF列表...")
        
        # 正确获取ETF分类
        category_df = ak.fund_etf_category_sina(symbol="全部")
        if category_df.empty:
            logger.warning("AkShare ETF分类接口返回空数据")
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        logger.info(f"获取到{len(category_df)}个ETF分类，开始获取各分类ETF列表...")
        
        # 遍历每个分类获取ETF列表
        all_etfs = []
        for _, category in category_df.iterrows():
            category_name = category.get('name', '')
            logger.debug(f"正在获取'{category_name}'分类的ETF列表...")
            
            try:
                # 根据分类获取ETF列表
                etf_df = ak.fund_etf_category_sina(symbol=category_name)
                if not etf_df.empty:
                    # 添加分类信息
                    etf_df['category'] = category_name
                    all_etfs.append(etf_df)
                else:
                    logger.debug(f"'{category_name}'分类下无ETF数据")
            except Exception as e:
                logger.warning(f"获取'{category_name}'分类ETF列表失败: {str(e)}")
                continue
        
        if not all_etfs:
            logger.warning("所有ETF分类均未获取到有效ETF数据")
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        # 合并所有ETF数据
        combined_df = pd.concat(all_etfs, ignore_index=True)
        logger.info(f"成功获取{len(combined_df)}只ETF基础数据")
        
        # 重命名列以匹配标准列名
        column_mapping = {
            "code": "ETF代码",
            "name": "ETF名称",
            "new_price": "最新价",
            "change": "涨跌额",
            "increase": "涨跌幅",
            "volume": "成交量",
            "amount": "成交额",
            "turnover_rate": "换手率",
            "total_share": "总份额",
            "net_value": "单位净值",
            "discount_rate": "折价率"
        }
        
        # 只保留存在的列进行重命名
        existing_columns = [col for col in column_mapping.keys() if col in combined_df.columns]
        rename_mapping = {col: column_mapping[col] for col in existing_columns}
        combined_df = combined_df.rename(columns=rename_mapping)
        
        # 确保ETF代码为6位数字
        if "ETF代码" in combined_df.columns:
            combined_df["ETF代码"] = combined_df["ETF代码"].astype(str).str.strip().str.zfill(6)
            # 仅保留6位数字的ETF代码
            valid_etfs = combined_df[combined_df["ETF代码"].str.match(r'^\d{6}$')].copy()
            logger.info(f"筛选后剩余{len(valid_etfs)}只有效ETF")
        else:
            logger.error("返回数据中缺少ETF代码列")
            valid_etfs = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        # 添加基金规模列（如果必要列存在）
        if not valid_etfs.empty and "单位净值" in valid_etfs.columns and "总份额" in valid_etfs.columns:
            valid_etfs["基金规模"] = pd.to_numeric(valid_etfs["单位净值"], errors="coerce") * \
                                    pd.to_numeric(valid_etfs["总份额"], errors="coerce") / 10000
        else:
            valid_etfs["基金规模"] = 0.0
        
        # 确保包含所有需要的列
        for col in Config.ETF_STANDARD_COLUMNS:
            if col not in valid_etfs.columns:
                valid_etfs[col] = ""
        
        valid_etfs = valid_etfs[Config.ETF_STANDARD_COLUMNS]
        # 按基金规模降序排序
        valid_etfs = valid_etfs.sort_values("基金规模", ascending=False)
        
        return valid_etfs.drop_duplicates(subset="ETF代码")
    
    except Exception as e:
        error_msg = f"AkShare接口错误: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)

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
    """新浪接口获取ETF列表
    :return: 包含ETF信息的DataFrame"""
    try:
        logger.info("尝试从新浪获取ETF列表...")
        url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList"
        params = {"page": 1, "num": 1000, "sort": "symbol", "asc": 1}
        response = requests.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)
        response.raise_for_status()
        
        # 处理新浪接口返回的数据
        try:
            etf_data = response.json()
        except ValueError:
            # 如果JSON解析失败，尝试eval
            try:
                etf_data_str = response.text.replace('var data=', '').strip(';')
                etf_data = eval(etf_data_str)
            except Exception as e:
                logger.error(f"新浪接口返回的数据格式无法解析: {str(e)}")
                return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        # 确保数据是列表格式
        if not isinstance(etf_data, list):
            logger.warning("新浪接口返回的数据不是列表格式")
            # 尝试从可能的嵌套结构中提取数据
            if isinstance(etf_data, dict) and 'data' in etf_data:
                etf_data = etf_data['data']
            elif isinstance(etf_data, dict) and 'list' in etf_data:
                etf_data = etf_data['list']
            else:
                return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
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
                etf_list["ETF代码"] = etf_list["完整代码"].str.extract(r'(\d{6})')
                # 过滤有效的6位数字ETF代码
                etf_list = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$', na=False)]
                
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
        
        logger.warning("新浪接口返回的数据缺少必要列")
        return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
    
    except Exception as e:
        error_msg = f"新浪接口错误: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)

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
    """更新ETF列表（优先使用本地文件，若需更新则从网络获取）
    :return: 包含ETF信息的DataFrame"""
    # 检查是否需要更新
    if not os.path.exists(Config.ALL_ETFS_PATH) or is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
        logger.info("ETF列表文件不存在或已过期，尝试从网络获取...")
        try:
            primary_etf_list = None
            
            # 1. 尝试AkShare接口
            logger.info("尝试从AkShare获取ETF列表...")
            primary_etf_list = fetch_all_etfs_akshare()
            
            if not primary_etf_list.empty:
                # 确保包含所有需要的列
                for col in Config.ETF_STANDARD_COLUMNS:
                    if col not in primary_etf_list.columns:
                        primary_etf_list[col] = ""
                primary_etf_list = primary_etf_list[Config.ETF_STANDARD_COLUMNS]
                # 按基金规模降序排序
                primary_etf_list = primary_etf_list.sort_values("基金规模", ascending=False)
                primary_etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                logger.info(f"✅ AkShare更新成功（{len(primary_etf_list)}只ETF）")
            else:
                logger.warning("AkShare返回空的ETF列表")
            
            # 2. 如果AkShare失败，尝试新浪接口
            if primary_etf_list is None or primary_etf_list.empty:
                logger.info("尝试从新浪获取ETF列表...")
                primary_etf_list = fetch_all_etfs_sina()
                
                if not primary_etf_list.empty:
                    # 确保包含所有需要的列
                    for col in Config.ETF_STANDARD_COLUMNS:
                        if col not in primary_etf_list.columns:
                            primary_etf_list[col] = ""
                    primary_etf_list = primary_etf_list[Config.ETF_STANDARD_COLUMNS]
                    primary_etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                    logger.info(f"✅ 新浪接口更新成功（{len(primary_etf_list)}只ETF）")
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
                        logger.info(f"✅ 兜底文件加载成功（{len(backup_df)}只ETF）")
                        
                        # 保存兜底文件为当前ETF列表
                        backup_df.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                        return backup_df
                    except Exception as e:
                        logger.error(f"❌ 兜底文件处理失败: {str(e)}")
                
                # 如果兜底文件也不存在或处理失败，返回空DataFrame但包含所有列
                logger.error("❌ 无法获取ETF列表，所有数据源均失败")
                empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
                return empty_df
            
            return primary_etf_list
        
        except Exception as e:
            logger.error(f"❌ 更新ETF列表时发生未预期错误: {str(e)}")
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
                    logger.warning("⚠️ 使用兜底文件作为最后手段")
                    return backup_df
                except Exception as e:
                    logger.error(f"❌ 兜底文件加载也失败: {str(e)}")
            
            # 返回空DataFrame但包含所有列
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
    
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
                    logger.warning("⚠️ 本地文件加载失败，使用兜底文件")
                    return backup_df
                except Exception as e:
                    logger.error(f"❌ 兜底文件加载也失败: {str(e)}")
            
            # 返回空DataFrame但包含所有列
            empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
            return empty_df

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
