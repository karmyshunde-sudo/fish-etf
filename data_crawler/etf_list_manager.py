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

# -------------------------
# 保留原有函数名，确保其他模块能正常导入
# -------------------------
def load_all_etf_list():
    """兼容旧代码：加载全市场ETF列表（实际调用update_all_etf_list）"""
    return update_all_etf_list()

def is_list_need_update():
    """判断是否需要更新全市场ETF列表"""
    if not os.path.exists(Config.ALL_ETFS_PATH):
        return True
    last_modify_time = datetime.fromtimestamp(os.path.getmtime(Config.ALL_ETFS_PATH))
    days_since_update = (get_beijing_time() - last_modify_time).days
    return days_since_update >= LIST_UPDATE_INTERVAL

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_all_etfs_akshare():
    """从AkShare获取ETF列表（适配1.17.41版本）"""
    try:
        etf_info = ak.etf_fund_info_em()
        etf_list = etf_info[etf_info["交易场所"] != "场外"]
        etf_list = etf_list.rename(columns={
            "基金代码": "ETF代码",
            "基金简称": "ETF名称"
        })[["ETF代码", "ETF名称"]]
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.strip().str.zfill(6)
        etf_list = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]
        return etf_list.drop_duplicates(subset="ETF代码")
    except Exception as e:
        raise Exception(f"AkShare接口错误: {str(e)}")

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_all_etfs_sina():
    """新浪接口兜底（AkShare失败时使用）"""
    try:
        url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList"
        params = {"page": 1, "num": 1000, "sort": "symbol", "asc": 1}
        response = ak.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        etf_data = response.json() if response.text.startswith("[") else eval(response.text)
        etf_list = pd.DataFrame(etf_data)[["symbol", "name"]]
        etf_list = etf_list.rename(columns={
            "symbol": "ETF代码",
            "name": "ETF名称"
        })
        etf_list["ETF代码"] = etf_list["ETF代码"].str[-6:].str.strip()
        return etf_list.drop_duplicates(subset="ETF代码")
    except Exception as e:
        raise Exception(f"新浪接口错误: {str(e)}")

def read_csv_with_encoding(file_path):
    """读取CSV文件，自动兼容UTF-8和GBK编码"""
    encodings = ["utf-8", "gbk", "latin-1"]
    for encoding in encodings:
        try:
            return pd.read_csv(file_path, encoding=encoding)
        except (UnicodeDecodeError, LookupError):
            continue
    raise Exception(f"无法解析文件 {file_path}，尝试了编码: {encodings}")

def update_all_etf_list():
    """更新全市场ETF列表（三级降级策略）"""
    init_dirs()
    if is_list_need_update():
        print("🔍 尝试更新全市场ETF列表...")
        try:
            etf_list = fetch_all_etfs_akshare()
            etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
            print(f"✅ AkShare更新成功（{len(etf_list)}只ETF）")
            return etf_list
        except Exception as e:
            print(f"❌ AkShare更新失败: {str(e)}")
        
        try:
            etf_list = fetch_all_etfs_sina()
            etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
            print(f"✅ 新浪接口更新成功（{len(etf_list)}只ETF）")
            return etf_list
        except Exception as e:
            print(f"❌ 新浪接口更新失败: {str(e)}")
        
        if os.path.exists(Config.BACKUP_ETFS_PATH):
            try:
                backup_df = read_csv_with_encoding(Config.BACKUP_ETFS_PATH)
                if "ETF代码" not in backup_df.columns:
                    raise Exception("兜底文件缺少'ETF代码'列")
                if "ETF名称" not in backup_df.columns:
                    backup_df["ETF名称"] = backup_df["ETF代码"].apply(lambda x: f"ETF-{str(x).strip()}")
                backup_df["ETF代码"] = backup_df["ETF代码"].astype(str).str.strip().str.zfill(6)
                backup_df = backup_df[backup_df["ETF代码"].str.match(r'^\d{6}$')]
                backup_df = backup_df[["ETF代码", "ETF名称"]].drop_duplicates()
                print(f"✅ 兜底文件加载成功（{len(backup_df)}只ETF）")
                return backup_df
            except Exception as e:
                print(f"❌ 兜底文件处理失败: {str(e)}")
                return pd.DataFrame()
        else:
            print(f"❌ 兜底文件不存在: {Config.BACKUP_ETFS_PATH}")
            return pd.DataFrame()
    else:
        print("ℹ️ 无需更新，加载本地ETF列表")
        try:
            return read_csv_with_encoding(Config.ALL_ETFS_PATH)
        except Exception as e:
            print(f"❌ 本地文件加载失败: {str(e)}")
            return pd.DataFrame()

def get_filtered_etf_codes():
    """获取过滤后的有效ETF代码列表"""
    etf_list = update_all_etf_list()
    if etf_list.empty:
        print("⚠️ 无有效ETF代码列表")
        return []
    valid_codes = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]["ETF代码"].tolist()
    print(f"📊 有效ETF代码数量: {len(valid_codes)}")
    return valid_codes
