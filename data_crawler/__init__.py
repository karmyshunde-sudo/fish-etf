import os
import time
import pandas as pd
import logging
from datetime import datetime, date
from retrying import retry
import akshare as ak
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday
from config import Config
from .etf_list_manager import update_all_etf_list, get_filtered_etf_codes, load_all_etf_list
from utils.date_utils import get_beijing_time
from utils.file_utils import init_dirs

# 初始化日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 定义中国股市节假日日历
class ChinaStockHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday("2025元旦", month=1, day=1),
        Holiday("2025春节", month=1, day=29, observance=lambda d: d + pd.DateOffset(days=+5)),
        Holiday("2025清明节", month=4, day=4),
        Holiday("2025劳动节", month=5, day=1),
        Holiday("2025端午节", month=6, day=2),
        Holiday("2025中秋节", month=9, day=8),
        Holiday("2025国庆节", month=10, day=1, observance=lambda d: d + pd.DateOffset(days=+6)),
    ]

# 重试装饰器配置
def retry_if_exception(exception):
    return isinstance(exception, (ConnectionError, TimeoutError, Exception))

@retry(
    stop_max_attempt_number=3,
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    retry_on_exception=retry_if_exception
)
def akshare_retry(func, *args, **kwargs):
    """带重试机制的akshare函数调用封装"""
    return func(*args, **kwargs)

def is_trading_day(check_date: date) -> bool:
    """判断是否为A股交易日"""
    if check_date.weekday() >= 5:
        return False
    
    china_bd = CustomBusinessDay(calendar=ChinaStockHolidayCalendar())
    try:
        return pd.Timestamp(check_date) == (pd.Timestamp(check_date) + 0 * china_bd)
    except Exception as e:
        logger.error(f"交易日判断失败: {str(e)}", exc_info=True)
        return False

def get_etf_name(etf_code):
    """根据ETF代码获取名称"""
    etf_list = load_all_etf_list()
    if etf_list.empty:
        return "未知名称"
    
    target_code = str(etf_code).strip().zfill(6)
    name_row = etf_list[
        etf_list["ETF代码"].astype(str).str.strip().str.zfill(6) == target_code
    ]
    
    if not name_row.empty:
        return name_row.iloc[0]["ETF名称"]
    else:
        return f"未知名称({etf_code})"

def crawl_etf_daily_incremental():
    """增量爬取ETF日线数据"""
    logger.info("===== 开始执行任务：crawl_etf_daily =====")
    current_time = get_beijing_time()
    logger.info(f"当前时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}（北京时间）")
    
    if not is_trading_day(current_time.date()):
        logger.info(f"今日{current_time.date()}非交易日，无需爬取日线数据")
        return
    
    init_dirs()
    # 修复：使用Config中实际存在的日线数据目录属性（假设为ETF_DAILY_PATH）
    os.makedirs(Config.ETF_DAILY_PATH, exist_ok=True)
    # 修复：使用Config中实际存在的已完成列表路径属性（假设为ETF_DAILY_COMPLETED_FILE）
    completed_file = Config.ETF_DAILY_COMPLETED_FILE
    
    completed_codes = set()
    if os.path.exists(completed_file):
        with open(completed_file, "r", encoding="utf-8") as f:
            completed_codes = set(line.strip() for line in f if line.strip())
        logger.info(f"已完成爬取的ETF数量：{len(completed_codes)}")
    
    all_codes = get_filtered_etf_codes()
    to_crawl_codes = [code for code in all_codes if code not in completed_codes]
    total = len(to_crawl_codes)
    
    if total == 0:
        logger.info("所有ETF日线数据均已爬取完成，无需继续")
        return
    
    logger.info(f"待爬取ETF总数：{total}只")
    
    batch_size = 50
    batches = [to_crawl_codes[i:i+batch_size] for i in range(0, total, batch_size)]
    logger.info(f"共分为 {len(batches)} 个批次，每批 {batch_size} 只ETF")
    
    for batch_idx, batch in enumerate(batches, 1):
        batch_num = len(batch)
        logger.info(f"==============================")
        logger.info(f"正在处理批次 {batch_idx}/{len(batches)}")
        logger.info(f"ETF范围：{batch_idx*batch_size - batch_size + 1}-{min(batch_idx*batch_size, total)}只（共{batch_num}只）")
        logger.info(f"==============================")
        
        for idx, etf_code in enumerate(batch, 1):
            try:
                logger.info(f"--- 批次{batch_idx} - 第{idx}只 / 共{batch_num}只 ---")
                etf_name = get_etf_name(etf_code)
                logger.info(f"ETF代码：{etf_code} | 名称：{etf_name}")
                
                df = akshare_retry(
                    ak.fund_etf_hist_em,
                    symbol=etf_code,
                    period="daily",
                    adjust="qfq"
                )
                
                if df.empty:
                    logger.warning(f"⚠️ 爬取结果为空，跳过保存")
                    continue
                
                df = df.rename(columns={
                    "日期": "date",
                    "开盘价": "open",
                    "最高价": "high",
                    "最低价": "low",
                    "收盘价": "close",
                    "成交量": "volume",
                    "成交额": "amount",
                    "涨跌幅": "pct_change"
                })
                
                df["etf_code"] = etf_code
                df["etf_name"] = etf_name
                df["crawl_time"] = current_time.strftime("%Y-%m-%d %H:%M:%S")
                
                # 同步修改保存路径为实际的Config属性
                save_path = os.path.join(Config.ETF_DAILY_PATH, f"{etf_code}.csv")
                df.to_csv(save_path, index=False, encoding="utf-8")
                logger.info(f"✅ 保存成功：{save_path}（{len(df)}条数据）")
                
                with open(completed_file, "a", encoding="utf-8") as f:
                    f.write(f"{etf_code}\n")
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ 爬取失败：{str(e)}", exc_info=True)
                time.sleep(3)
                continue
        
        if batch_idx < len(batches):
            logger.info(f"批次{batch_idx}处理完成，休眠10秒后继续...")
            time.sleep(10)
    
    logger.info("===== 所有待爬取ETF处理完毕 =====")
    
