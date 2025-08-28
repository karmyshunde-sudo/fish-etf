import os
import time
import pandas as pd
import logging
from datetime import datetime, date, timedelta
from retrying import retry
import akshare as ak
from pandas.tseries.offsets import CustomBusinessDay
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday
from config import Config
from .etf_list_manager import update_all_etf_list, get_filtered_etf_codes, load_all_etf_list
from .akshare_crawler import crawl_etf_daily_akshare
from .sina_crawler import crawl_etf_daily_sina
from utils.date_utils import get_beijing_time
from utils.file_utils import init_dirs

# 初始化日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 定义中国股市节假日日历（2025年）
class ChinaStockHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday("元旦", month=1, day=1),
        Holiday("春节", month=1, day=29, observance=lambda d: d + pd.DateOffset(days=+5)),
        Holiday("清明节", month=4, day=4),
        Holiday("劳动节", month=5, day=1, observance=lambda d: d + pd.DateOffset(days=+2)),
        Holiday("端午节", month=6, day=2),
        Holiday("中秋节", month=9, day=8),
        Holiday("国庆节", month=10, day=1, observance=lambda d: d + pd.DateOffset(days=+6)),
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
    """带重试机制的函数调用封装"""
    return func(*args, **kwargs)

def is_trading_day(check_date: date) -> bool:
    """判断是否为A股交易日"""
    if check_date.weekday() >= 5:  # 周六或周日
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

def get_last_crawl_date(etf_code, etf_daily_dir):
    """获取最后一次爬取的日期"""
    file_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
    if not os.path.exists(file_path):
        # 没有存量数据，返回最近一年的日期
        current_date = get_beijing_time().date()
        start_date = (current_date - timedelta(days=Config.INITIAL_CRAWL_DAYS)).strftime("%Y-%m-%d")
        return start_date
    
    try:
        df = pd.read_csv(file_path, encoding="utf-8")
        if df.empty or "date" not in df.columns:
            # 文件为空或没有date列，返回最近一年的日期
            current_date = get_beijing_time().date()
            start_date = (current_date - timedelta(days=Config.INITIAL_CRAWL_DAYS)).strftime("%Y-%m-%d")
            return start_date
        
        last_date = df["date"].max()
        # 计算下一个交易日作为开始日期
        china_bd = CustomBusinessDay(calendar=ChinaStockHolidayCalendar())
        next_date = (pd.to_datetime(last_date) + china_bd).strftime("%Y-%m-%d")
        return next_date
    except Exception as e:
        logger.warning(f"获取{etf_code}最后爬取日期失败: {str(e)}，将使用最近一年日期")
        current_date = get_beijing_time().date()
        start_date = (current_date - timedelta(days=Config.INITIAL_CRAWL_DAYS)).strftime("%Y-%m-%d")
        return start_date

def record_failed_etf(etf_daily_dir, etf_code, etf_name, error_message=None):
    """记录失败的ETF信息"""
    failed_file = os.path.join(etf_daily_dir, "failed_etfs.txt")
    timestamp = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(failed_file, "a", encoding="utf-8") as f:
        if error_message:
            f.write(f"{etf_code}|{etf_name}|{timestamp}|{error_message}\n")
        else:
            f.write(f"{etf_code}|{etf_name}|{timestamp}\n")

def crawl_etf_daily_incremental():
    """增量爬取ETF日线数据（单只保存+断点续爬逻辑）"""
    logger.info("===== 开始执行任务：crawl_etf_daily =====")
    current_time = get_beijing_time()
    current_date = current_time.date()
    logger.info(f"当前时间：{current_time.strftime('%Y-%m-%d %H:%M:%S')}（北京时间）")
    
    # 非交易日且未到补爬时间（18点后允许补爬）
    if not is_trading_day(current_date) and current_time.hour < 18:
        logger.info(f"今日{current_date}非交易日且未到补爬时间，无需爬取日线数据")
        return
    
    # 初始化目录
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    etf_daily_dir = os.path.join(root_dir, "data", "etf_daily")
    os.makedirs(etf_daily_dir, exist_ok=True)
    logger.info(f"✅ 确保目录存在: {etf_daily_dir}")
    
    # 已完成列表路径
    completed_file = os.path.join(etf_daily_dir, "etf_daily_completed.txt")
    
    # 加载已完成列表
    completed_codes = set()
    if os.path.exists(completed_file):
        with open(completed_file, "r", encoding="utf-8") as f:
            completed_codes = set(line.strip() for line in f if line.strip())
        logger.info(f"已完成爬取的ETF数量：{len(completed_codes)}")
    
    # 获取待爬取ETF列表
    all_codes = get_filtered_etf_codes()
    to_crawl_codes = [code for code in all_codes if code not in completed_codes]
    total = len(to_crawl_codes)
    
    if total == 0:
        logger.info("所有ETF日线数据均已爬取完成，无需继续")
        return
    
    logger.info(f"待爬取ETF总数：{total}只")
    
    # 分批爬取（每批50只）
    batch_size = 50
    batches = [to_crawl_codes[i:i+batch_size] for i in range(0, total, batch_size)]
    logger.info(f"共分为 {len(batches)} 个批次，每批 {batch_size} 只ETF")
    
    # 逐批、逐只爬取
    for batch_idx, batch in enumerate(batches, 1):
        batch_num = len(batch)
        logger.info(f"==============================")
        logger.info(f"正在处理批次 {batch_idx}/{len(batches)}")
        logger.info(f"ETF范围：{batch_idx*batch_size - batch_size + 1}-{min(batch_idx*batch_size, total)}只（共{batch_num}只）")
        logger.info(f"==============================")
        
        for idx, etf_code in enumerate(batch, 1):
            try:
                # 打印当前进度
                logger.info(f"--- 批次{batch_idx} - 第{idx}只 / 共{batch_num}只 ---")
                etf_name = get_etf_name(etf_code)
                logger.info(f"ETF代码：{etf_code} | 名称：{etf_name}")
                
                # 确定爬取时间范围（增量爬取）
                start_date = get_last_crawl_date(etf_code, etf_daily_dir)
                end_date = current_date.strftime("%Y-%m-%d")
                
                if start_date > end_date:
                    logger.info(f"📅 无新数据需要爬取（上次爬取至{start_date}）")
                    # 标记为已完成
                    with open(completed_file, "a", encoding="utf-8") as f:
                        f.write(f"{etf_code}\n")
                    continue
                
                logger.info(f"📅 爬取时间范围：{start_date} 至 {end_date}")
                
                # 先尝试AkShare爬取
                df = crawl_etf_daily_akshare(etf_code, start_date, end_date)
                
                # AkShare失败则尝试新浪爬取
                if df.empty:
                    logger.warning("⚠️ AkShare未获取到数据，尝试使用新浪接口")
                    df = crawl_etf_daily_sina(etf_code, start_date, end_date)
                
                # 数据校验
                if df.empty:
                    logger.warning(f"⚠️ 所有接口均未获取到数据，跳过保存")
                    # 记录失败日志，但不标记为已完成，以便下次重试
                    record_failed_etf(etf_daily_dir, etf_code, etf_name)
                    continue
                
                # 统一列名（转为英文列名，使用config.py中的标准定义）
                col_map = Config.STANDARD_COLUMNS
                df = df.rename(columns=col_map)
                
                # 补充ETF基本信息
                df["etf_code"] = etf_code
                df["etf_name"] = etf_name
                df["crawl_time"] = current_time.strftime("%Y-%m-%d %H:%M:%S")
                
                # 确保所有标准列都存在
                for col in Config.STANDARD_COLUMNS.values():
                    if col not in df.columns:
                        # 填充缺失的列（除了etf_code, etf_name, crawl_time已经在上面添加）
                        if col == "amplitude" and "振幅" in df.columns:
                            df[col] = df["振幅"]
                        elif col == "price_change" and "涨跌额" in df.columns:
                            df[col] = df["涨跌额"]
                        elif col == "turnover" and "换手率" in df.columns:
                            df[col] = df["换手率"]
                        else:
                            df[col] = None  # 填充空值
                
                # 只保留标准列
                df = df[list(Config.STANDARD_COLUMNS.values())]
                
                # 处理已有数据的追加逻辑
                save_path = os.path.join(etf_daily_dir, f"{etf_code}.csv")
                if os.path.exists(save_path):
                    existing_df = pd.read_csv(save_path, encoding="utf-8")
                    # 去重后合并
                    combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=["date"], keep="last")
                    # 按日期排序
                    combined_df["date"] = pd.to_datetime(combined_df["date"])
                    combined_df = combined_df.sort_values("date").reset_index(drop=True)
                    combined_df["date"] = combined_df["date"].dt.strftime("%Y-%m-%d")
                    df = combined_df
                
                # 保存数据
                df.to_csv(save_path, index=False, encoding="utf-8")
                logger.info(f"✅ 保存成功：{save_path}（共{len(df)}条数据）")
                
                # 记录已完成
                with open(completed_file, "a", encoding="utf-8") as f:
                    f.write(f"{etf_code}\n")
                
                # 单只爬取后短休眠
                time.sleep(1)
                
            except Exception as e:
                # 单只失败不中断，记录日志后继续
                logger.error(f"❌ 爬取失败：{str(e)}", exc_info=True)
                # 记录失败日志
                record_failed_etf(etf_daily_dir, etf_code, etf_name, str(e))
                time.sleep(3)  # 失败后延长休眠
                continue
        
        # 批次间长休眠（减轻服务器压力）
        if batch_idx < len(batches):
            logger.info(f"批次{batch_idx}处理完成，休眠10秒后继续...")
            time.sleep(10)
    
    logger.info("===== 所有待爬取ETF处理完毕 =====")
