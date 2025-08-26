import akshare as ak
import pandas as pd
import os
from datetime import datetime
from utils.date_utils import get_beijing_time
from utils.file_utils import init_dirs
from retrying import retry
from config import Config  # 确保导入Config

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
    """从AkShare 1.17.41获取全市场ETF列表（适配旧版本接口）"""
    try:
        # akshare 1.17.41可用的ETF列表接口：分上海和深圳市场获取
        sh_etf = ak.stock_etf_category_sina(symbol="上海ETF")  # 1.17.41版本的正确参数
        sz_etf = ak.stock_etf_category_sina(symbol="深圳ETF")  # 1.17.41版本的正确参数
        
        # 合并沪深市场ETF
        etf_list = pd.concat([sh_etf, sz_etf], ignore_index=True)
        
        # 处理1.17.41版本的列名（固定为"代码"和"名称"）
        if "代码" in etf_list.columns and "名称" in etf_list.columns:
            etf_list = etf_list.rename(columns={"代码": "etf_code", "名称": "etf_name"})
        else:
            # 兼容可能的列名变化
            code_col = next(col for col in etf_list.columns if "代码" in col)
            name_col = next(col for col in etf_list.columns if "名称" in col)
            etf_list = etf_list.rename(columns={code_col: "etf_code", name_col: "etf_name"})
        
        # 数据清洗：确保代码为6位数字，去重
        etf_list["etf_code"] = etf_list["etf_code"].astype(str).str.strip()
        etf_list = etf_list[etf_list["etf_code"].str.match(r'^\d{6}$')]  # 过滤非6位代码
        etf_list = etf_list.drop_duplicates(subset=["etf_code"], keep="last")
        
        return etf_list[["etf_code", "etf_name"]]
    except Exception as e:
        print(f"⚠️ AkShare 1.17.41拉取全市场ETF列表失败：{str(e)}")
        raise  # 触发重试

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
                try:
                    backup_df = pd.read_csv(Config.BACKUP_ETFS_PATH, encoding="utf-8")
                    
                    # 严格校验兜底文件列名
                    if "ETF代码" not in backup_df.columns:
                        print(f"❌ 兜底文件必须包含'ETF代码'列，请检查文件格式")
                        return pd.DataFrame()
                    
                    # 列名转换为内部使用的etf_code
                    backup_df = backup_df.rename(columns={"ETF代码": "etf_code"})
                    
                    # 数据清洗：确保代码为6位数字，去重
                    backup_df["etf_code"] = backup_df["etf_code"].astype(str).str.strip()
                    backup_df = backup_df[backup_df["etf_code"].str.match(r'^\d{6}$')]
                    backup_df = backup_df.drop_duplicates(subset=["etf_code"])
                    
                    # 补充名称列（若不存在）
                    if "etf_name" not in backup_df.columns:
                        if "ETF名称" in backup_df.columns:
                            backup_df = backup_df.rename(columns={"ETF名称": "etf_name"})
                        else:
                            backup_df["etf_name"] = backup_df["etf_code"].apply(lambda x: f"ETF-{x}")
                    
                    # 只保留必要列
                    backup_df = backup_df[["etf_code", "etf_name"]]
                    print(f"✅ 兜底文件加载完成（{len(backup_df)}只ETF）")
                    return backup_df
                except Exception as e:
                    print(f"❌ 兜底文件解析失败：{str(e)}")
                    return pd.DataFrame()
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
                # 确保代码格式正确
                df["etf_code"] = df["etf_code"].astype(str).str.strip()
                return df[df["etf_code"].str.match(r'^\d{6}$')]
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
                backup_df["etf_code"] = backup_df["etf_code"].astype(str).str.strip()
                backup_df = backup_df[backup_df["etf_code"].str.match(r'^\d{6}$')].drop_duplicates()
                
                if "etf_name" not in backup_df.columns:
                    if "ETF名称" in backup_df.columns:
                        backup_df = backup_df.rename(columns={"ETF名称": "etf_name"})
                    else:
                        backup_df["etf_name"] = backup_df["etf_code"].apply(lambda x: f"ETF-{x}")
                
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
    return etf_list["etf_code"].tolist()
