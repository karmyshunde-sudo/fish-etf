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

def is_list_need_update():
    """判断是否需要更新全市场ETF列表"""
    if not os.path.exists(Config.ALL_ETFS_PATH):
        return True
    last_modify_time = datetime.fromtimestamp(os.path.getmtime(Config.ALL_ETFS_PATH))
    days_since_update = (get_beijing_time() - last_modify_time).days
    return days_since_update >= LIST_UPDATE_INTERVAL

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_all_etfs():
    """从AkShare获取全市场ETF列表（主逻辑）"""
    try:
        sh_etf = ak.stock_etf_category_sina(symbol="上海")
        sz_etf = ak.stock_etf_category_sina(symbol="深圳")
        etf_list = pd.concat([sh_etf, sz_etf], ignore_index=True)
        
        # 处理不同版本的列名
        if "代码" in etf_list.columns:
            etf_list = etf_list.rename(columns={"代码": "etf_code", "名称": "etf_name"})
        elif "基金代码" in etf_list.columns:
            etf_list = etf_list.rename(columns={"基金代码": "etf_code", "基金名称": "etf_name"})
        else:
            # 尝试模糊匹配
            code_col = next(col for col in etf_list.columns if "代码" in col)
            name_col = next(col for col in etf_list.columns if "名称" in col)
            etf_list = etf_list.rename(columns={code_col: "etf_code", name_col: "etf_name"})
        
        etf_list = etf_list.drop_duplicates(subset=["etf_code"], keep="last")
        etf_list = etf_list[etf_list["etf_code"].str.match(r'^\d{6}$')]
        return etf_list[["etf_code", "etf_name"]]
    except Exception as e:
        print(f"⚠️ AkShare拉取全市场ETF列表失败：{str(e)}")
        raise  # 触发重试，重试失败后走兜底逻辑

def update_all_etf_list():
    """更新全市场ETF列表，失败则使用兜底文件"""
    init_dirs()
    if is_list_need_update():
        print("🔍 尝试更新全市场ETF列表...")
        try:
            etf_list = fetch_all_etfs()
            etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
            print(f"✅ 全市场ETF列表更新完成（{len(etf_list)}只）")
            return etf_list
        except Exception as e:
            print(f"❌ 全市场ETF列表更新失败，启用兜底文件...")
            # 读取兜底文件的ETF代码，自动补充名称
            if os.path.exists(Config.BACKUP_ETFS_PATH):
                backup_df = pd.read_csv(Config.BACKUP_ETFS_PATH, encoding="utf-8")
                # 校验兜底文件列名（必须含"etf_code"列或"ETF代码"列）
                if "etf_code" not in backup_df.columns:
                    if "ETF代码" in backup_df.columns:
                        backup_df = backup_df.rename(columns={"ETF代码": "etf_code"})
                    else:
                        print(f"❌ 兜底文件列名错误，需包含'ETF代码'列")
                        return pd.DataFrame()
                
                # 去重并筛选6位数字代码
                backup_df = backup_df[backup_df["etf_code"].astype(str).str.match(r'^\d{6}$')].drop_duplicates(subset=["etf_code"])
                
                # 补充名称列
                if "etf_name" not in backup_df.columns:
                    backup_df["etf_name"] = backup_df["etf_code"].apply(lambda x: f"ETF-{x}")
                elif "ETF名称" in backup_df.columns:
                    backup_df = backup_df.rename(columns={"ETF名称": "etf_name"})
                
                # 只保留必要列
                backup_df = backup_df[["etf_code", "etf_name"]]
                print(f"✅ 兜底文件加载完成（{len(backup_df)}只ETF）")
                return backup_df
            else:
                print(f"❌ 兜底文件 {Config.BACKUP_ETFS_PATH} 不存在")
                return pd.DataFrame()
    else:
        print("ℹ️  全市场ETF列表无需更新，直接加载本地文件")
        return load_all_etf_list()

def load_all_etf_list():
    """加载ETF列表，优先级：本地更新列表 > 兜底文件"""
    if os.path.exists(Config.ALL_ETFS_PATH):
        try:
            df = pd.read_csv(Config.ALL_ETFS_PATH, encoding="utf-8")
            if not df.empty and "etf_code" in df.columns:
                return df
        except Exception as e:
            print(f"⚠️  加载本地ETF列表失败：{str(e)}")
    
    # 本地列表为空或损坏，启用兜底文件
    print("⚠️  本地ETF列表无效，启用兜底文件...")
    if os.path.exists(Config.BACKUP_ETFS_PATH):
        try:
            backup_df = pd.read_csv(Config.BACKUP_ETFS_PATH, encoding="utf-8")
            if "ETF代码" in backup_df.columns:
                backup_df = backup_df.rename(columns={"ETF代码": "etf_code"})
            
            if "etf_code" in backup_df.columns:
                backup_df = backup_df[backup_df["etf_code"].astype(str).str.match(r'^\d{6}$')].drop_duplicates()
                
                if "etf_name" not in backup_df.columns:
                    backup_df["etf_name"] = backup_df["etf_code"].apply(lambda x: f"ETF-{x}")
                elif "ETF名称" in backup_df.columns:
                    backup_df = backup_df.rename(columns={"ETF名称": "etf_name"})
                
                return backup_df[["etf_code", "etf_name"]]
        except Exception as e:
            print(f"⚠️  加载兜底ETF列表失败：{str(e)}")
    
    print("❌ 无有效ETF列表（本地+兜底均失效）")
    return pd.DataFrame()

def get_filtered_etf_codes():
    """获取需爬取的ETF代码（基于兜底列表）"""
    etf_list = load_all_etf_list()
    if etf_list.empty:
        print("⚠️  无有效ETF列表，返回空")
        return []
    # 返回所有6位代码（后续爬取时会逐一获取详细数据）
    return etf_list["etf_code"].astype(str).tolist()
