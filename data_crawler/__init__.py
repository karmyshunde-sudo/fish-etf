import time
import pandas as pd
import datetime
from config import Config
from utils.date_utils import get_last_trading_day, get_beijing_time
from utils.file_utils import load_etf_metadata, update_etf_metadata, save_etf_daily_data
from .akshare_crawler import crawl_etf_daily_akshare
from .sina_crawler import crawl_etf_daily_sina
from .etf_list_manager import update_all_etf_list, get_filtered_etf_codes, load_all_etf_list

def crawl_etf_daily_incremental():
    """
    带双休眠机制的批量增量爬取
    - 批次划分：每批50只ETF
    - 批次间休眠：30秒（避免IP频繁请求）
    - 单只间休眠：3秒（进一步降低反爬风险）
    """
    print("="*50)
    print("开始批量增量爬取ETF日线数据（带双休眠机制）")
    print("="*50)
    
    # 1. 更新全市场ETF列表（每周自动更新）
    update_all_etf_list()
    # 2. 获取初步筛选后的ETF代码列表
    etf_codes = get_filtered_etf_codes()
    if not etf_codes:
        print("无有效ETF代码，爬取任务终止")
        return
    total_etfs = len(etf_codes)
    print(f"待爬取ETF总数：{total_etfs}只")
    
    # 3. 加载元数据（记录每个ETF的最后爬取日期）
    metadata_df = load_etf_metadata()
    last_trade_day = get_last_trading_day().strftime("%Y-%m-%d")
    
    # 4. 计算批次信息（每批50只）
    batch_size = Config.CRAWL_BATCH_SIZE
    total_batches = (total_etfs + batch_size - 1) // batch_size
    print(f"共分为 {total_batches} 个批次，每批 {batch_size} 只ETF")
    
    # 5. 按批次执行爬取
    for batch_idx in range(total_batches):
        # 计算当前批次的ETF范围
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, total_etfs)
        batch_codes = etf_codes[start_idx:end_idx]
        batch_num = batch_idx + 1
        
        print(f"\n" + "="*30)
        print(f"正在处理批次 {batch_num}/{total_batches}")
        print(f"ETF范围：{start_idx+1}-{end_idx}只（共{len(batch_codes)}只）")
        print("="*30)
        
        # 遍历当前批次的每只ETF
        for code_idx, etf_code in enumerate(batch_codes, 1):
            print(f"\n--- 批次{batch_num} - 第{code_idx}只 / 共{len(batch_codes)}只 ---")
            print(f"ETF代码：{etf_code} | 名称：{get_etf_name(etf_code)}")
            
            # 初始化元数据（若该ETF是首次爬取）
            if etf_code not in metadata_df["etf_code"].values:
                init_date = (get_last_trading_day() - datetime.timedelta(days=Config.INITIAL_CRAWL_DAYS)).strftime("%Y-%m-%d")
                # 新增到元数据
                metadata_df = pd.concat([
                    metadata_df,
                    pd.DataFrame({"etf_code": [etf_code], "last_crawl_date": [init_date]})
                ], ignore_index=True)
                update_etf_metadata(etf_code, init_date)
                print(f"首次爬取该ETF，默认初始日期：{init_date}")
            
            # 获取该ETF的最后爬取日期
            last_crawl_date = metadata_df[metadata_df["etf_code"] == etf_code]["last_crawl_date"].iloc[0]
            print(f"上次爬取日期：{last_crawl_date} | 目标爬取至：{last_trade_day}")
            
            # 数据已最新，跳过
            if last_crawl_date >= last_trade_day:
                print(f"✅ 数据已最新，无需爬取")
                # 非最后一只，休眠3秒
                if code_idx < len(batch_codes):
                    print(f"⏳ 单只间休眠3秒...")
                    time.sleep(3)
                continue
            
            # 尝试爬取（AkShare为主，新浪备用）
            crawl_success = False
            df = pd.DataFrame()
            try:
                print(f"🔍 尝试AkShare爬取...")
                df = crawl_etf_daily_akshare(
                    etf_code=etf_code,
                    start_date=last_crawl_date,
                    end_date=last_trade_day
                )
                crawl_success = True
                print(f"✅ AkShare爬取成功，共{len(df)}条数据")
            except Exception as e:
                print(f"❌ AkShare爬取失败：{str(e)[:50]}...")
                try:
                    print(f"🔍 切换新浪数据源爬取...")
                    df = crawl_etf_daily_sina(
                        etf_code=etf_code,
                        start_date=last_crawl_date,
                        end_date=last_trade_day
                    )
                    crawl_success = True
                    print(f"✅ 新浪爬取成功，共{len(df)}条数据")
                except Exception as e2:
                    print(f"❌ 新浪爬取也失败：{str(e2)[:50]}...")
            
            # 爬取成功则保存数据
            if crawl_success and not df.empty:
                save_etf_daily_data(etf_code=etf_code, df=df)
                update_etf_metadata(etf_code=etf_code, last_date=last_trade_day)
                print(f"📥 数据已保存并更新元数据")
            elif crawl_success and df.empty:
                print(f"ℹ️  未获取到新数据（可能该ETF无交易）")
            else:
                print(f"⚠️  双源爬取失败，该ETF本次跳过")
            
            # 单只间休眠（除了当前批次的最后一只）
            if code_idx < len(batch_codes):
                print(f"⏳ 单只间休眠3秒...")
                time.sleep(3)
        
        # 批次间休眠（除了最后一个批次）
        if batch_num < total_batches:
            print(f"\n" + "="*30)
            print(f"批次{batch_num}处理完成，休眠30秒再开始下一批次...")
            print("="*30)
            time.sleep(30)
    
    print("\n" + "="*50)
    print(f"所有批次处理完成！共处理{total_etfs}只ETF")
    print("="*50)

# 辅助函数：获取ETF名称（避免循环导入）
def get_etf_name(etf_code):
    """根据ETF代码获取名称，修复列名匹配问题"""
    etf_list = load_all_etf_list()  # 复用加载ETF列表的函数
    if etf_list.empty:
        return "未知名称（无有效ETF列表）"
    
    # 关键修复：使用数据中实际的列名 "ETF代码" 进行匹配
    # 同时确保代码格式统一（去除首尾空格、补全6位）
    target_code = str(etf_code).strip().zfill(6)
    name_row = etf_list[
        etf_list["ETF代码"].astype(str).str.strip().str.zfill(6) == target_code
    ]
    
    if not name_row.empty:
        return name_row.iloc[0]["ETF名称"]  # 返回对应名称
    else:
        return f"未知名称（代码：{etf_code} 未匹配到数据）"
