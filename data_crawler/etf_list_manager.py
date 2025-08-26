import akshare as ak
import pandas as pd
import os
from datetime import datetime
from utils.date_utils import get_beijing_time
from utils.file_utils import init_dirs
from retrying import retry
from config import Config  # 导入完整配置

# 列表更新频率（天）
LIST_UPDATE_INTERVAL = 7

def is_list_need_update():
    """判断是否需要更新全市场ETF列表"""
    if not os.path.exists(Config.ALL_ETFS_PATH):
        return True
    last_modify_time = datetime.fromtimestamp(os.path.getmtime(Config.ALL_ETFS_PATH))
    days_since_update = (get_beijing_time() - last_modify_time).days
    return days_since_update >= LIST_UPDATE_INTERVAL

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_all_etfs_akshare():
    """从AkShare获取ETF列表（适配1.17.41版本，改用新接口）"""
    try:
        # 使用 akshare 1.17.41 可用的 ETF 信息接口
        etf_info = ak.etf_fund_info_em()  # 全市场ETF基础信息
        # 筛选场内ETF（排除场外联接基金）
        etf_list = etf_info[etf_info["交易场所"] != "场外"]  
        # 提取必要列并清洗
        etf_list = etf_list.rename(columns={
            "基金代码": "ETF代码",
            "基金简称": "ETF名称"
        })[["ETF代码", "ETF名称"]]
        # 确保代码为6位数字
        etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.zfill(6)
        etf_list = etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]
        return etf_list.drop_duplicates(subset="ETF代码")
    except Exception as e:
        print(f"⚠️ AkShare 1.17.41 获取ETF列表失败: {str(e)}")
        raise

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_all_etfs_sina():
    """新浪接口兜底（AkShare失败时用）"""
    try:
        # 新浪ETF列表接口（直接获取场内ETF）
        url = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getETFList"
        params = {"page": 1, "num": 1000, "sort": "symbol", "asc": 1}
        response = ak.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        # 兼容新浪返回的 JSON 格式
        etf_data = response.json() if response.text.startswith("[") else eval(response.text)
        etf_list = pd.DataFrame(etf_data)[["symbol", "name"]]
        etf_list = etf_list.rename(columns={
            "symbol": "ETF代码",
            "name": "ETF名称"
        })
        # 清洗代码（去掉市场前缀，如 sh510050 → 510050）
        etf_list["ETF代码"] = etf_list["ETF代码"].str[-6:]
        return etf_list.drop_duplicates(subset="ETF代码")
    except Exception as e:
        print(f"⚠️ 新浪接口获取ETF列表失败: {str(e)}")
        raise

def update_all_etf_list():
    """更新全市场ETF列表（AkShare → 新浪 → 兜底文件 三级降级）"""
    # 初始化必要目录（确保配置中的路径存在）
    Config.init_dirs()
    
    if is_list_need_update():
        print("🔍 尝试用 AkShare 更新全市场ETF列表...")
        try:
            etf_list = fetch_all_etfs_akshare()
            etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
            print(f"✅ AkShare 更新成功（{len(etf_list)}只ETF）")
            return etf_list
        except:
            print("❌ AkShare 失败，尝试新浪接口...")
            try:
                etf_list = fetch_all_etfs_sina()
                etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                print(f"✅ 新浪接口更新成功（{len(etf_list)}只ETF）")
                return etf_list
            except:
                print("❌ 新浪接口失败，启用兜底文件...")
                if os.path.exists(Config.BACKUP_ETFS_PATH):
                    # 强制编码兼容（UTF-8优先，GBK fallback）
                    try:
                        backup_df = pd.read_csv(Config.BACKUP_ETFS_PATH, encoding="utf-8")
                    except:
                        backup_df = pd.read_csv(Config.BACKUP_ETFS_PATH, encoding="gbk")
                    
                    # 强制补全必要列（匹配STANDARD_COLUMNS）
                    if "ETF名称" not in backup_df.columns:
                        backup_df["ETF名称"] = backup_df["ETF代码"].apply(lambda x: f"ETF-{x}")
                    
                    # 清洗代码格式
                    backup_df["ETF代码"] = backup_df["ETF代码"].astype(str).str.zfill(6)
                    backup_df = backup_df[backup_df["ETF代码"].str.match(r'^\d{6}$')]
                    backup_df = backup_df[["ETF代码", "ETF名称"]].drop_duplicates()
                    
                    print(f"✅ 兜底文件加载成功（{len(backup_df)}只ETF）")
                    return backup_df
                else:
                    print(f"❌ 兜底文件 {Config.BACKUP_ETFS_PATH} 不存在")
                    return pd.DataFrame()
    else:
        print("ℹ️  全市场ETF列表无需更新，直接加载本地文件")
        # 加载时同样处理编码兼容
        try:
            return pd.read_csv(Config.ALL_ETFS_PATH, encoding="utf-8")
        except:
            return pd.read_csv(Config.ALL_ETFS_PATH, encoding="gbk")

def get_filtered_etf_codes():
    """获取需爬取的ETF代码（自动过滤无效代码）"""
    etf_list = update_all_etf_list()
    if etf_list.empty:
        print("⚠️  无有效ETF列表，返回空")
        return []
    # 仅返回6位数字代码
    return etf_list[etf_list["ETF代码"].str.match(r'^\d{6}$')]["ETF代码"].tolist()
