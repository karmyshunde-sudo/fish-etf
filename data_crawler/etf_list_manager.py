import os
import akshare as ak
import pandas as pd
import logging
import requests
import time
import json
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
        logger.debug("使用缓存的ETF列表")
        return _etf_list_cache.copy()
    
    # 更新ETF列表
    _etf_list_cache = update_all_etf_list()
    _last_load_time = datetime.now()
    return _etf_list_cache.copy() if _etf_list_cache is not None else pd.DataFrame()

def update_all_etf_list() -> pd.DataFrame:
    """更新ETF列表（优先使用本地文件，若需更新则从网络获取）
    :return: 包含ETF信息的DataFrame
    """
    # 检查是否需要更新
    if not os.path.exists(Config.ALL_ETFS_PATH) or is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
        logger.info("ETF列表文件不存在或已过期，尝试从网络获取...")
        try:
            primary_etf_list = None
            
            # 1. 尝试AkShare接口
            logger.info("尝试从AkShare获取ETF列表...")
            primary_etf_list = fetch_all_etfs_akshare()
            
            if not primary_etf_list.empty:
                # 补充完整代码、上市日期和其他信息
                primary_etf_list = enrich_etf_data(primary_etf_list)
                
                # 确保包含所有需要的列
                required_columns = Config.ETF_STANDARD_COLUMNS
                for col in required_columns:
                    if col not in primary_etf_list.columns:
                        primary_etf_list[col] = ""
                primary_etf_list = primary_etf_list[required_columns]
                # 按基金规模降序排序
                primary_etf_list = primary_etf_list.sort_values("基金规模", ascending=False)
                primary_etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8-sig")
                logger.info(f"✅ AkShare更新成功（{len(primary_etf_list)}只ETF）")
                # 标记数据来源
                primary_etf_list.source = "AkShare"
            else:
                logger.warning("AkShare返回空的ETF列表")
            
            # 2. 如果AkShare失败，尝试新浪接口
            if primary_etf_list is None or primary_etf_list.empty:
                logger.info("尝试从新浪获取ETF列表...")
                primary_etf_list = fetch_all_etfs_sina()
                
                if not primary_etf_list.empty:
                    # 补充完整代码、上市日期和其他信息
                    primary_etf_list = enrich_etf_data(primary_etf_list)
                    
                    # 确保包含所有需要的列
                    required_columns = Config.ETF_STANDARD_COLUMNS
                    for col in required_columns:
                        if col not in primary_etf_list.columns:
                            primary_etf_list[col] = ""
                    primary_etf_list = primary_etf_list[required_columns]
                    primary_etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8-sig")
                    logger.info(f"✅ 新浪接口更新成功（{len(primary_etf_list)}只ETF）")
                    # 标记数据来源
                    primary_etf_list.source = "新浪"
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
                        # 补充完整代码、上市日期和其他信息
                        backup_df = enrich_etf_data(backup_df)
                        # 确保包含所有需要的列
                        required_columns = Config.ETF_STANDARD_COLUMNS
                        for col in required_columns:
                            if col not in backup_df.columns:
                                backup_df[col] = ""
                        backup_df = backup_df[required_columns].drop_duplicates()
                        # 按基金规模降序排序
                        backup_df = backup_df.sort_values("基金规模", ascending=False)
                        logger.info(f"✅ 兜底文件加载成功（{len(backup_df)}只ETF）")
                        # 标记数据来源
                        backup_df.source = "兜底文件"
                        
                        # 保存兜底文件为当前ETF列表
                        backup_df.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8-sig")
                        return backup_df
                    except Exception as e:
                        logger.error(f"❌ 兜底文件处理失败: {str(e)}")
                
                # 如果兜底文件也不存在或处理失败，返回空DataFrame但包含所有列
                logger.error("❌ 无法获取ETF列表，所有数据源均失败")
                empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
                # 标记数据来源
                empty_df.source = "无数据源"
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
                    # 补充完整代码、上市日期和其他信息
                    backup_df = enrich_etf_data(backup_df)
                    required_columns = Config.ETF_STANDARD_COLUMNS
                    for col in required_columns:
                        if col not in backup_df.columns:
                            backup_df[col] = ""
                    backup_df = backup_df[required_columns].drop_duplicates()
                    backup_df = backup_df.sort_values("基金规模", ascending=False)
                    logger.warning("⚠️ 使用兜底文件作为最后手段")
                    # 标记数据来源
                    backup_df.source = "兜底文件(异常)"
                    return backup_df
                except Exception as e:
                    logger.error(f"❌ 兜底文件加载也失败: {str(e)}")
            
            # 返回空DataFrame但包含所有列
            empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
            # 标记数据来源
            empty_df.source = "无数据源(异常)"
            return empty_df
    
    else:
        logger.info("ℹ️ 无需更新，加载本地ETF列表")
        try:
            etf_list = read_csv_with_encoding(Config.ALL_ETFS_PATH)
            # 补充完整代码、上市日期和其他信息
            etf_list = enrich_etf_data(etf_list)
            # 确保包含所有需要的列
            required_columns = Config.ETF_STANDARD_COLUMNS
            for col in required_columns:
                if col not in etf_list.columns:
                    etf_list[col] = ""
            # 按基金规模降序排序
            etf_list = etf_list.sort_values("基金规模", ascending=False)
            # 标记数据来源
            etf_list.source = "本地缓存"
            return etf_list
        except Exception as e:
            logger.error(f"❌ 本地文件加载失败: {str(e)}")
            # 尝试加载兜底文件
            if os.path.exists(Config.BACKUP_ETFS_PATH):
                try:
                    backup_df = read_csv_with_encoding(Config.BACKUP_ETFS_PATH)
                    backup_df["ETF代码"] = backup_df["ETF代码"].astype(str).str.strip().str.zfill(6)
                    backup_df = backup_df[backup_df["ETF代码"].str.match(r'^\d{6}$')]
                    # 补充完整代码、上市日期和其他信息
                    backup_df = enrich_etf_data(backup_df)
                    required_columns = Config.ETF_STANDARD_COLUMNS
                    for col in required_columns:
                        if col not in backup_df.columns:
                            backup_df[col] = ""
                    backup_df = backup_df[required_columns].drop_duplicates()
                    backup_df = backup_df.sort_values("基金规模", ascending=False)
                    logger.warning("⚠️ 本地文件加载失败，使用兜底文件")
                    # 标记数据来源
                    backup_df.source = "兜底文件(本地加载失败)"
                    return backup_df
                except Exception as e:
                    logger.error(f"❌ 兜底文件加载也失败: {str(e)}")
            
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
        # 使用正确的接口：fund_etf_spot_em
        etf_info = ak.fund_etf_spot_em()
        if etf_info.empty:
            logger.warning("AkShare返回空的ETF列表")
            return pd.DataFrame()
        
        # 记录返回的列名，用于调试
        logger.debug(f"AkShare返回列名: {list(etf_info.columns)}")
        
        # 标准化列名映射
        column_mapping = {
            "代码": "ETF代码",
            "名称": "ETF名称",
            "最新价": "最新价格",
            "IOPV实时估值": "IOPV",
            "基金折价率": "折溢价率",
            "成交量": "成交量",
            "成交额": "成交额",
            "涨跌幅": "涨跌幅",
            "涨跌额": "涨跌额",
            "换手率": "换手率",
            "更新时间": "更新时间",
            "规模": "基金规模"  # 明确映射基金规模
        }
        
        # 重命名列
        etf_info = etf_info.rename(columns=column_mapping)
        
        # 确保包含所有需要的列
        required_columns = Config.ETF_STANDARD_COLUMNS
        for col in required_columns:
            if col not in etf_info.columns:
                etf_info[col] = ""
        
        # 数据清洗：确保代码为6位数字
        etf_info["ETF代码"] = etf_info["ETF代码"].astype(str).str.strip().str.zfill(6)
        valid_etfs = etf_info[etf_info["ETF代码"].str.match(r'^\d{6}$', na=False)].copy()
        
        # 处理基金规模：提取数字部分并转换为数值
        if "基金规模" in valid_etfs.columns:
            # 提取数字部分
            valid_etfs["基金规模"] = valid_etfs["基金规模"].astype(str).str.extract('([0-9.]+)', expand=False)
            valid_etfs["基金规模"] = pd.to_numeric(valid_etfs["基金规模"], errors="coerce")
            
            # === 关键修复：确保基金规模单位为"亿元" ===
            # AkShare fund_etf_spot_em 接口返回的规模单位是"亿元"，无需转换
            # 但为了安全起见，添加单位验证
            if not valid_etfs.empty and valid_etfs["基金规模"].mean() > 1000:
                # 如果平均规模大于1000，可能是"万元"单位，需要转换
                logger.warning("检测到基金规模可能为'万元'单位，进行单位转换")
                valid_etfs["基金规模"] = valid_etfs["基金规模"] / 10000
            elif not valid_etfs.empty and valid_etfs["基金规模"].mean() > 100000000:
                # 如果平均规模大于1亿，可能是"元"单位，需要转换
                logger.warning("检测到基金规模可能为'元'单位，进行单位转换")
                valid_etfs["基金规模"] = valid_etfs["基金规模"] / 100000000
            # ======================================
        
        # 处理成交额：假设原始数据单位是"元"，转换为"万元"
        if "成交额" in valid_etfs.columns:
            valid_etfs["成交额"] = pd.to_numeric(valid_etfs["成交额"], errors="coerce") / 10000
        
        # 确保包含所有需要的列
        for col in required_columns:
            if col not in valid_etfs.columns:
                valid_etfs[col] = ""
        
        valid_etfs = valid_etfs[required_columns]
        logger.info(f"AkShare获取到{len(etf_info)}只ETF，筛选后剩余{len(valid_etfs)}只")
        return valid_etfs.drop_duplicates(subset="ETF代码")
    
    except Exception as e:
        error_msg = f"AkShare接口错误: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return pd.DataFrame()  # 返回空DataFrame但不抛出异常

@retry(stop_max_attempt_number=3,
       wait_exponential_multiplier=1000,
       wait_exponential_max=10000,
       retry_on_exception=retry_if_network_error)

@retry(stop_max_attempt_number=3,
       wait_exponential_multiplier=1000,
       wait_exponential_max=10000,
       retry_on_exception=retry_if_network_error)
def fetch_all_etfs_sina() -> pd.DataFrame:
    """新浪接口兜底获取ETF列表（带超时控制）
    :return: 包含ETF信息的DataFrame"""
    try:
        logger.info("尝试从新浪获取ETF列表...")
        url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList"
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
            "symbol": "ETF代码",
            "name": "ETF名称",
            "date": "上市日期",
            "amount": "基金规模"  # 明确映射基金规模
        })
        
        # 确保ETF代码为6位数字
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.strip().str.zfill(6)
        
        # 过滤有效的6位数字ETF代码
        valid_etfs = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$', na=False)].copy()
        
        if valid_etfs.empty:
            logger.warning("提取后无有效ETF代码")
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        # === 关键修复：确保基金规模单位为"亿元" ===
        if "基金规模" in valid_etfs.columns:
            # 提取数字部分
            valid_etfs["基金规模"] = valid_etfs["基金规模"].astype(str).str.extract('([0-9.]+)', expand=False)
            valid_etfs["基金规模"] = pd.to_numeric(valid_etfs["基金规模"], errors="coerce")
            
            # 新浪接口返回的规模单位通常是"万元"，需要转换为"亿元"
            if not valid_etfs.empty and valid_etfs["基金规模"].mean() > 10:
                logger.info(f"新浪接口返回的基金规模单位为'万元'，转换为'亿元'（平均规模: {valid_etfs['基金规模'].mean():.2f}万元）")
                valid_etfs["基金规模"] = valid_etfs["基金规模"] / 10000
            else:
                logger.info("新浪接口返回的基金规模单位为'亿元'，无需转换")
        # ======================================
        
        # 确保上市日期格式正确
        if "上市日期" in valid_etfs.columns:
            # 处理可能的日期格式，确保是YYYY-MM-DD
            valid_etfs["上市日期"] = pd.to_datetime(valid_etfs["上市日期"], errors="coerce").dt.strftime("%Y-%m-%d")
            # 处理NaT值
            valid_etfs["上市日期"] = valid_etfs["上市日期"].fillna("")
        
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

def enrich_etf_data(df: pd.DataFrame) -> pd.DataFrame:
    """补充完整代码和上市日期，并添加其他有用信息"""
    try:
        # 补充完整代码
        def get_full_code(code: str) -> str:
            code = str(code).strip().zfill(6)
            if code.startswith("51"):
                return f"sh{code}"
            elif code.startswith("5") or code.startswith("159"):
                return f"sz{code}"
            return code  # 保持原样，可能是一些特殊代码
        
        df["完整代码"] = df["ETF代码"].apply(get_full_code)
        
        # 添加额外列（如果不存在）
        additional_columns = [
            "基金类型", "成立日期", "基金规模（最新）", "基金管理人", 
            "基金托管人", "跟踪标的", "业绩比较基准", "单位净值", 
            "累计净值", "近 1 月涨幅", "近 3 月涨幅", "近 6 月涨幅", 
            "近 1 年涨幅", "成立以来涨幅"
        ]
        for col in additional_columns:
            if col not in df.columns:
                df[col] = ""
        
        # 补充额外信息
        logger.info("正在补充ETF额外信息...")
        for idx, row in df.iterrows():
            etf_code = row["ETF代码"]
            full_code = row["完整代码"]  # 使用完整代码
            
            try:
                # 将完整代码转换为AkShare接口需要的格式（移除点号）
                # 从 "sh.510300" 转换为 "sh510300"（8位不带点号）
                akshare_code = full_code.replace(".", "")
                
                # 获取ETF基本信息 - 修复：使用正确的参数名
                info = ak.fund_etf_fund_info_em(fund=akshare_code)
                if not info.empty:
                    # 提取成立日期作为上市日期
                    listing_date = info["成立日期"].iloc[0]
                    df.at[idx, "上市日期"] = listing_date
                    
                    # 提取其他信息
                    df.at[idx, "基金类型"] = info["基金类型"].iloc[0]
                    df.at[idx, "基金规模（最新）"] = info["基金规模（最新）"].iloc[0]
                    df.at[idx, "基金管理人"] = info["基金管理人"].iloc[0]
                    df.at[idx, "基金托管人"] = info["基金托管人"].iloc[0]
                    df.at[idx, "跟踪标的"] = info["跟踪标的"].iloc[0]
                    df.at[idx, "业绩比较基准"] = info["业绩比较基准"].iloc[0]
                    df.at[idx, "单位净值"] = info["单位净值"].iloc[0]
                    df.at[idx, "累计净值"] = info["累计净值"].iloc[0]
                    df.at[idx, "近 1 月涨幅"] = info["近 1 月涨幅"].iloc[0]
                    df.at[idx, "近 3 月涨幅"] = info["近 3 月涨幅"].iloc[0]
                    df.at[idx, "近 6 月涨幅"] = info["近 6 月涨幅"].iloc[0]
                    df.at[idx, "近 1 年涨幅"] = info["近 1 年涨幅"].iloc[0]
                    df.at[idx, "成立以来涨幅"] = info["成立以来涨幅"].iloc[0]
                    
                    # 确保基金规模单位为"亿元"
                    if "基金规模（最新）" in df.columns:
                        # 提取数字部分
                        size_str = str(df.at[idx, "基金规模（最新）"])
                        size_num = ''.join(filter(lambda x: x.isdigit() or x == '.', size_str))
                        if size_num:
                            size_value = float(size_num)
                            # 如果包含"万"字，表示单位是"万份"
                            if "万" in size_str:
                                # 万份转为亿份，再乘以单位净值估算规模
                                size_value = size_value * 0.0001  # 万份转为亿份
                            # 如果包含"亿"字，已经是亿份
                            df.at[idx, "基金规模"] = size_value
                        else:
                            df.at[idx, "基金规模"] = 0.0
                    
                    logger.debug(f"ETF {etf_code} 信息补充成功")
                else:
                    logger.warning(f"ETF {etf_code} 无基本信息，无法补充信息")
            except Exception as e:
                logger.error(f"获取ETF {etf_code} 信息失败: {str(e)}")
        
        return df
    
    except Exception as e:
        logger.error(f"❌ 补充ETF数据失败: {str(e)}")
        return df

def read_csv_with_encoding(file_path: str) -> pd.DataFrame:
    """读取CSV文件，自动兼容UTF-8和GBK编码
    :param file_path: 文件路径
    :return: 读取的DataFrame
    """
    try:
        return pd.read_csv(file_path, encoding='utf-8-sig')
    except UnicodeDecodeError:
        try:
            return pd.read_csv(file_path, encoding='gbk')
        except Exception as e:
            logger.error(f"读取CSV文件失败: {str(e)}")
            return pd.DataFrame()

def get_filtered_etf_codes() -> list:
    """获取过滤后的有效ETF代码列表
    :return: ETF代码列表
    """
    try:
        etf_list = load_all_etf_list()
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
