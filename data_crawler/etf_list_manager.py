import akshare as ak
import pandas as pd
import os
from datetime import datetime
from utils.date_utils import get_beijing_time
from utils.file_utils import init_dirs
from retrying import retry
from config import Config

# 列表更新频率（天）
LIST_UPDATE_INTERVAL = 7

def load_all_etf_list():
    """原有函数，保持不变，供其他模块引用"""
    return update_all_etf_list()

def is_list_need_update():
    """判断是否需要更新全市场ETF列表，逻辑不变"""
    if not os.path.exists(Config.ALL_ETFS_PATH):
        return True
    last_modify_time = datetime.fromtimestamp(os.path.getmtime(Config.ALL_ETFS_PATH))
    days_since_update = (get_beijing_time() - last_modify_time).days
    return days_since_update >= LIST_UPDATE_INTERVAL

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_all_etfs_akshare():
    """使用指定的fund_etf_spot_em接口获取ETF列表，保持原有逻辑"""
    try:
        # 调用正确接口：fund_etf_spot_em
        etf_info = ak.fund_etf_spot_em()
        
        # 标准化列名（与原有逻辑对齐）
        etf_list = etf_info.rename(columns={
            "代码": "ETF代码",
            "名称": "ETF名称"
        })[Config.ETF_STANDARD_COLUMNS]  # 使用标准列确保结构一致
        
        # 数据清洗：确保代码为6位数字，逻辑不变
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.strip().str.zfill(6)
        etf_list = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]
        
        return etf_list.drop_duplicates(subset="ETF代码")
    except Exception as e:
        error_msg = f"AkShare接口错误: {str(e)}"
        print(f"⚠️ {error_msg}")
        raise Exception(error_msg)

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_all_etfs_sina():
    """新浪接口兜底，逻辑不变"""
    try:
        url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList"
        params = {"page": 1, "num": 1000, "sort": "symbol", "asc": 1}
        import requests
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        etf_data = response.json() if response.text.startswith("[") else eval(response.text)
        etf_list = pd.DataFrame(etf_data)[["symbol", "name"]]
        
        etf_list = etf_list.rename(columns={
            "symbol": "ETF代码",
            "name": "ETF名称"
        })[Config.ETF_STANDARD_COLUMNS]  # 使用标准列确保结构一致
        
        etf_list["ETF代码"] = etf_list["ETF代码"].str[-6:].str.strip()
        
        return etf_list.drop_duplicates(subset="ETF代码")
    except Exception as e:
        error_msg = f"新浪接口错误: {str(e)}"
        print(f"⚠️ {error_msg}")
        raise Exception(error_msg)

def read_csv_with_encoding(file_path):
    """读取CSV文件，自动兼容UTF-8和GBK编码，逻辑不变"""
    encodings = ["utf-8", "gbk", "latin-1"]
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            # 确保只返回标准列
            return df[Config.ETF_STANDARD_COLUMNS].copy()
        except (UnicodeDecodeError, LookupError, KeyError) as e:
            continue
    raise Exception(f"无法解析文件 {file_path}，尝试了编码: {encodings}")

def update_all_etf_list():
    """更新全市场ETF列表（三级降级策略），仅添加初始化同步兜底文件逻辑"""
    Config.init_dirs()  # 使用Config的初始化方法
    primary_etf_list = None
    
    if is_list_need_update():
        print("🔍 尝试更新全市场ETF列表...")
        
        # 1. 尝试AkShare接口
        try:
            etf_list = fetch_all_etfs_akshare()
            etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
            print(f"✅ AkShare更新成功（{len(etf_list)}只ETF）")
            primary_etf_list = etf_list
        except Exception as e:
            print(f"❌ AkShare更新失败: {str(e)}")
        
        # 2. 尝试新浪接口（仅当AkShare失败时）
        if primary_etf_list is None:
            try:
                etf_list = fetch_all_etfs_sina()
                etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                print(f"✅ 新浪接口更新成功（{len(etf_list)}只ETF）")
                primary_etf_list = etf_list
            except Exception as e:
                print(f"❌ 新浪接口更新失败: {str(e)}")
        
        # -------------------------
        # 新增逻辑：第一次初始化时同步补充karmy_etf.csv数据
        # -------------------------
        # 检查兜底文件是否不存在或为空
        backup_file_exists = os.path.exists(Config.BACKUP_ETFS_PATH)
        backup_file_empty = backup_file_exists and os.path.getsize(Config.BACKUP_ETFS_PATH) == 0
        
        if not backup_file_exists or backup_file_empty:
            print("🔄 检测到兜底文件未初始化，开始同步数据...")
            
            # 优先使用刚获取的primary_etf_list
            if primary_etf_list is not None and not primary_etf_list.empty:
                backup_df = primary_etf_list[Config.ETF_STANDARD_COLUMNS].copy()
                backup_df.to_csv(Config.BACKUP_ETFS_PATH, index=False, encoding="utf-8")
                print(f"✅ 已从新获取数据同步兜底文件（{len(backup_df)}条记录）")
            
            # 如果没有新获取的数据，尝试从已存在的all_etfs.csv同步
            elif os.path.exists(Config.ALL_ETFS_PATH) and os.path.getsize(Config.ALL_ETFS_PATH) > 0:
                try:
                    all_etfs_df = read_csv_with_encoding(Config.ALL_ETFS_PATH)
                    all_etfs_df.to_csv(Config.BACKUP_ETFS_PATH, index=False, encoding="utf-8")
                    print(f"✅ 已从本地all_etfs.csv同步兜底文件（{len(all_etfs_df)}条记录）")
                except Exception as e:
                    print(f"❌ 从all_etfs.csv同步兜底文件失败: {str(e)}")
        
        # 3. 尝试兜底文件（如果主数据源都失败）
        if primary_etf_list is None:
            if os.path.exists(Config.BACKUP_ETFS_PATH):
                try:
                    backup_df = read_csv_with_encoding(Config.BACKUP_ETFS_PATH)
                    
                    # 验证必要列（确保结构正确）
                    if not set(Config.ETF_STANDARD_COLUMNS).issubset(backup_df.columns):
                        missing_cols = set(Config.ETF_STANDARD_COLUMNS) - set(backup_df.columns)
                        raise Exception(f"兜底文件缺少必要列: {missing_cols}")
                    
                    # 数据清洗（与原有逻辑一致）
                    backup_df["ETF代码"] = backup_df["ETF代码"].astype(str).str.strip().str.zfill(6)
                    backup_df = backup_df[backup_df["ETF代码"].str.match(r'^\d{6}$')]
                    backup_df = backup_df[Config.ETF_STANDARD_COLUMNS].drop_duplicates()
                    
                    print(f"✅ 兜底文件加载成功（{len(backup_df)}只ETF）")
                    return backup_df
                except Exception as e:
                    print(f"❌ 兜底文件处理失败: {str(e)}")
                    return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
            else:
                print(f"❌ 兜底文件不存在: {Config.BACKUP_ETFS_PATH}")
                return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        return primary_etf_list
    
    else:
        print("ℹ️ 无需更新，加载本地ETF列表")
        try:
            # 加载时确保只返回标准列
            return read_csv_with_encoding(Config.ALL_ETFS_PATH)
        except Exception as e:
            print(f"❌ 本地文件加载失败: {str(e)}")
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)

def get_filtered_etf_codes():
    """获取过滤后的有效ETF代码列表，逻辑不变"""
    etf_list = update_all_etf_list()
    if etf_list.empty:
        print("⚠️ 无有效ETF代码列表")
        return []
    
    # 最终过滤确保代码有效性，逻辑不变
    valid_codes = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]["ETF代码"].tolist()
    print(f"📊 有效ETF代码数量: {len(valid_codes)}")
    return valid_codes
    
