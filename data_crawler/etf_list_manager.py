import akshare as ak
import pandas as pd
import os
import logging
import requests
from datetime import datetime, timezone, timedelta
from utils.date_utils import get_beijing_time
from utils.file_utils import init_dirs
from retrying import retry
from config import Config

# 初始化日志
logger = logging.getLogger(__name__)

# 列表更新频率（天）
LIST_UPDATE_INTERVAL = 7

def load_all_etf_list():
    """加载全市场ETF列表"""
    return update_all_etf_list()

def is_list_need_update():
    """判断是否需要更新全市场ETF列表（修复时区计算）"""
    if not os.path.exists(Config.ALL_ETFS_PATH):
        return True
    # 获取文件最后修改时间（转换为东八区时区）
    last_modify_time = datetime.fromtimestamp(os.path.getmtime(Config.ALL_ETFS_PATH))
    last_modify_time = last_modify_time.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))
    # 计算距离上次更新的天数
    days_since_update = (get_beijing_time() - last_modify_time).days
    return days_since_update >= LIST_UPDATE_INTERVAL

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_all_etfs_akshare():
    """使用AkShare接口获取ETF列表"""
    try:
        # 调用fund_etf_spot_em接口
        etf_info = ak.fund_etf_spot_em()
        
        # 标准化列名
        etf_list = etf_info.rename(columns={
            "代码": "ETF代码",
            "名称": "ETF名称",
            "上市日期": "上市日期"  # 添加上市日期列
        })
        
        # 确保包含所有标准列和上市日期列
        all_columns = Config.ETF_STANDARD_COLUMNS + ["上市日期"]
        etf_list = etf_list[all_columns]
        
        # 数据清洗：确保代码为6位数字
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.strip().str.zfill(6)
        etf_list = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]
        
        return etf_list.drop_duplicates(subset="ETF代码")
    except Exception as e:
        error_msg = f"AkShare接口错误: {str(e)}"
        logger.warning(f"⚠️ {error_msg}")
        raise Exception(error_msg)

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_all_etfs_sina():
    """新浪接口兜底获取ETF列表"""
    try:
        url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList"
        params = {"page": 1, "num": 1000, "sort": "symbol", "asc": 1}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        etf_data = response.json() if response.text.startswith("[") else eval(response.text)
        etf_list = pd.DataFrame(etf_data)[["symbol", "name"]]
        
        etf_list = etf_list.rename(columns={
            "symbol": "ETF代码",
            "name": "ETF名称"
        })
        
        # 添加空白的上市日期列（新浪接口不提供此信息）
        etf_list["上市日期"] = ""
        etf_list = etf_list[Config.ETF_STANDARD_COLUMNS + ["上市日期"]]
        
        etf_list["ETF代码"] = etf_list["ETF代码"].str[-6:].str.strip()
        
        return etf_list.drop_duplicates(subset="ETF代码")
    except Exception as e:
        error_msg = f"新浪接口错误: {str(e)}"
        logger.warning(f"⚠️ {error_msg}")
        raise Exception(error_msg)

def read_csv_with_encoding(file_path):
    """读取CSV文件，自动兼容UTF-8和GBK编码"""
    encodings = ["utf-8", "gbk", "latin-1"]
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            # 确保包含所有需要的列
            required_columns = Config.ETF_STANDARD_COLUMNS + ["上市日期"]
            for col in required_columns:
                if col not in df.columns:
                    df[col] = ""
            return df[required_columns].copy()
        except (UnicodeDecodeError, LookupError, KeyError) as e:
            continue
    raise Exception(f"无法解析文件 {file_path}，尝试了编码: {encodings}")

def update_all_etf_list():
    """更新全市场ETF列表（三级降级策略）"""
    Config.init_dirs()
    primary_etf_list = None
    
    if is_list_need_update():
        logger.info("🔍 尝试更新全市场ETF列表...")
        
        # 1. 尝试AkShare接口
        try:
            etf_list = fetch_all_etfs_akshare()
            # 确保包含所有需要的列
            required_columns = Config.ETF_STANDARD_COLUMNS + ["上市日期"]
            for col in required_columns:
                if col not in etf_list.columns:
                    etf_list[col] = ""
            etf_list = etf_list[required_columns]
            
            etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
            logger.info(f"✅ AkShare更新成功（{len(etf_list)}只ETF）")
            primary_etf_list = etf_list
        except Exception as e:
            logger.error(f"❌ AkShare更新失败: {str(e)}")
        
        # 2. 尝试新浪接口（仅当AkShare失败时）
        if primary_etf_list is None:
            try:
                etf_list = fetch_all_etfs_sina()
                # 确保包含所有需要的列
                required_columns = Config.ETF_STANDARD_COLUMNS + ["上市日期"]
                for col in required_columns:
                    if col not in etf_list.columns:
                        etf_list[col] = ""
                etf_list = etf_list[required_columns]
                
                etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                logger.info(f"✅ 新浪接口更新成功（{len(etf_list)}只ETF）")
                primary_etf_list = etf_list
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
                    required_columns = Config.ETF_STANDARD_COLUMNS + ["上市日期"]
                    for col in required_columns:
                        if col not in backup_df.columns:
                            backup_df[col] = ""
                    
                    # 数据清洗
                    backup_df["ETF代码"] = backup_df["ETF代码"].astype(str).str.strip().str.zfill(6)
                    backup_df = backup_df[backup_df["ETF代码"].str.match(r'^\d{6}$')]
                    backup_df = backup_df[required_columns].drop_duplicates()
                    
                    logger.info(f"✅ 兜底文件加载成功（{len(backup_df)}只ETF）")
                    return backup_df
                except Exception as e:
                    logger.error(f"❌ 兜底文件处理失败: {str(e)}")
                    # 返回空DataFrame但包含所有列
                    empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS + ["上市日期"])
                    return empty_df
            else:
                logger.error(f"❌ 兜底文件不存在: {Config.BACKUP_ETFS_PATH}")
                # 返回空DataFrame但包含所有列
                empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS + ["上市日期"])
                return empty_df
        
        return primary_etf_list
    
    else:
        logger.info("ℹ️ 无需更新，加载本地ETF列表")
        try:
            etf_list = read_csv_with_encoding(Config.ALL_ETFS_PATH)
            # 确保包含所有需要的列
            required_columns = Config.ETF_STANDARD_COLUMNS + ["上市日期"]
            for col in required_columns:
                if col not in etf_list.columns:
                    etf_list[col] = ""
            return etf_list[required_columns]
        except Exception as e:
            logger.error(f"❌ 本地文件加载失败: {str(e)}")
            # 返回空DataFrame但包含所有列
            empty_df = pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS + ["上市日期"])
            return empty_df

def get_filtered_etf_codes():
    """获取过滤后的有效ETF代码列表"""
    etf_list = update_all_etf_list()
    if etf_list.empty:
        logger.warning("⚠️ 无有效ETF代码列表")
        return []
    
    # 确保ETF代码为字符串类型
    etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.strip()
    valid_codes = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]["ETF代码"].tolist()
    logger.info(f"📊 有效ETF代码数量: {len(valid_codes)}")
    return valid_codes
