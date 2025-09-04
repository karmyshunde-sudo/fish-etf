# sina_crawler.py
import akshare as ak
import pandas as pd
import logging
import time
import re
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
from config import Config
from retrying import retry
from data_crawler.etf_list_manager import load_all_etf_list  # 新增：导入load_all_etf_list

# 初始化日志
logger = logging.getLogger(__name__)

# 重试配置
MAX_RETRY_ATTEMPTS = 3
RETRY_WAIT_FIXED = 2000  # 毫秒
RETRY_WAIT_EXPONENTIAL_MAX = 10000  # 毫秒

def empty_result_check(result: pd.DataFrame) -> bool:
    """
    检查新浪接口返回结果是否为空
    :param result: 新浪接口返回的DataFrame
    :return: 如果结果为空返回True，否则返回False
    """
    return result is None or result.empty

def retry_if_sina_error(exception: Exception) -> bool:
    """
    重试条件：新浪接口相关错误
    :param exception: 异常对象
    :return: 如果是新浪接口错误返回True，否则返回False
    """
    return isinstance(exception, (ValueError, ConnectionError, TimeoutError))

@retry(
    stop_max_attempt_number=MAX_RETRY_ATTEMPTS,
    wait_fixed=RETRY_WAIT_FIXED,
    retry_on_result=empty_result_check,
    retry_on_exception=retry_if_sina_error
)
def crawl_etf_daily_sina(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    用新浪接口爬取ETF日线数据（备用接口）
    :param etf_code: ETF代码 (6位数字)
    :param start_date: 开始日期 (YYYY-MM-DD)
    :param end_date: 结束日期 (YYYY-MM-DD)
    :return: 标准化中文列名的DataFrame
    """
    try:
        # 验证输入参数
        if not validate_etf_code(etf_code):
            logger.error(f"ETF代码格式无效: {etf_code}")
            return pd.DataFrame()
            
        if not validate_date_range(start_date, end_date):
            logger.error(f"日期范围无效: {start_date} 至 {end_date}")
            return pd.DataFrame()
            
        logger.info(f"尝试使用新浪接口爬取ETF {etf_code} 的数据，时间范围: {start_date} 至 {end_date}")
        
        # 添加市场前缀
        symbol = get_symbol_with_market_prefix(etf_code)
        
        # 使用新浪接口
        df = ak.fund_etf_hist_sina(symbol=symbol)
        
        if df.empty:
            logger.warning(f"新浪接口未获取到{etf_code}数据")
            return pd.DataFrame()
        
        # 标准化列名
        df = standardize_column_names(df)
        
        # 过滤日期范围
        df = filter_by_date_range(df, start_date, end_date)
        
        if df.empty:
            logger.warning(f"新浪接口获取的{etf_code}数据不在指定时间范围内 ({start_date} 至 {end_date})")
            return pd.DataFrame()
        
        # 确保所有必需列都存在
        df = ensure_required_columns(df)
        
        # 数据清洗和格式化
        df = clean_and_format_data(df)
        
        # 新增：确保返回的DataFrame包含"上市日期"列
        # 从ETF列表中获取上市日期
        etf_list = load_all_etf_list()
        target_code = str(etf_code).strip().zfill(6)
        listing_date_row = etf_list[
            etf_list["ETF代码"].astype(str).str.strip().str.zfill(6) == target_code
        ]
        
        if not listing_date_row.empty:
            listing_date = listing_date_row.iloc[0]["上市日期"]
            # 使用.loc避免SettingWithCopyWarning
            df.loc[:, "上市日期"] = listing_date
        else:
            # 使用.loc避免SettingWithCopyWarning
            df.loc[:, "上市日期"] = ""
        
        logger.info(f"新浪接口成功获取{etf_code}数据，共{len(df)}条")
        return df
    
    except Exception as e:
        logger.error(f"新浪接口爬取{etf_code}失败：{str(e)}")
        # 等待一段时间后重试
        time.sleep(2)
        raise  # 触发重试

def get_symbol_with_market_prefix(etf_code: str) -> str:
    """
    根据ETF代码获取带市场前缀的代码
    :param etf_code: ETF代码
    :return: 带市场前缀的代码
    """
    if etf_code.startswith('5') or etf_code.startswith('6') or etf_code.startswith('9'):
        return f"sh{etf_code}"
    else:
        return f"sz{etf_code}"

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    标准化列名（英文转中文）
    :param df: 原始DataFrame
    :return: 标准化列名的DataFrame
    """
    if df.empty:
        return df
        
    # 新浪接口返回的列名映射
    col_map = {
        "date": "日期",
        "open": "开盘",
        "high": "最高",
        "low": "最低",
        "close": "收盘",
        "volume": "成交量",
        "amount": "成交额",
        "listing_date": "上市日期",  # 新增：处理上市日期
        "issue_date": "上市日期"     # 新增：处理上市日期
    }
    
    # 重命名列
    df = df.rename(columns=col_map)
    logger.debug(f"列名映射完成: {col_map}")
    
    return df

def filter_by_date_range(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """
    按日期范围过滤数据
    :param df: 原始DataFrame
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 过滤后的DataFrame
    """
    if df.empty or "日期" not in df.columns:
        return df
        
    try:
        # 确保日期列为字符串格式
        df["日期"] = df["日期"].astype(str)
        
        # 过滤日期范围
        df = df[(df["日期"] >= start_date) & (df["日期"] <= end_date)]
        return df
    except Exception as e:
        logger.error(f"按日期范围过滤数据失败: {str(e)}")
        return pd.DataFrame()

def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    确保所有必需列都存在，缺失的列进行计算或填充
    :param df: 原始DataFrame
    :return: 包含所有必需列的DataFrame
    """
    if df.empty:
        return df
        
    # 确保所有必需列都存在
    # 修正：Config.STANDARD_COLUMNS 是列表，不是字典
    required_cols = Config.STANDARD_COLUMNS
    # 排除不需要从新浪接口获取的列（这些列会在后续处理中添加）
    exclude_cols = ["ETF代码", "ETF名称", "爬取时间", "上市日期"]  # 新增：添加"上市日期"到排除列表
    required_data_cols = [col for col in required_cols if col not in exclude_cols]
    
    for col in required_data_cols:
        if col not in df.columns:
            try:
                if col == "涨跌幅" and "收盘" in df.columns:
                    # 计算涨跌幅
                    df[col] = df["收盘"].pct_change().round(4)
                    # 使用.loc避免SettingWithCopyWarning
                    df.loc[0, col] = 0.0
                    logger.debug(f"计算涨跌幅列完成")
                    
                elif col == "涨跌额" and "收盘" in df.columns:
                    # 计算涨跌额
                    df[col] = (df["收盘"] - df["收盘"].shift(1)).round(4)
                    # 使用.loc避免SettingWithCopyWarning
                    df.loc[0, col] = 0.0
                    logger.debug(f"计算涨跌额列完成")
                    
                elif col == "振幅" and "最高" in df.columns and "最低" in df.columns and "收盘" in df.columns:
                    # 计算振幅
                    df[col] = ((df["最高"] - df["最低"]) / df["收盘"].shift(1) * 100).round(4)
                    # 使用.loc避免SettingWithCopyWarning
                    df.loc[0, col] = 0.0
                    logger.debug(f"计算振幅列完成")
                    
                elif col == "换手率" and "成交量" in df.columns and "成交额" in df.columns and "收盘" in df.columns:
                    # 计算换手率（近似计算）
                    df[col] = (df["成交量"] / (df["成交额"] / df["收盘"]) * 100).round(4)
                    logger.debug(f"计算换手率列完成")
                    
                else:
                    logger.warning(f"新浪数据缺少必要列：{col}，使用默认值填充")
                    df[col] = 0.0
            except Exception as e:
                logger.error(f"计算列 {col} 时发生错误: {str(e)}")
                df[col] = 0.0
    
    return df

def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    数据清洗和格式化
    :param df: 原始DataFrame
    :return: 清洗后的DataFrame
    """
    if df.empty:
        return df
        
    try:
        # 日期格式转换
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        
        # 新增：处理上市日期列
        if "上市日期" in df.columns:
            # 处理可能的日期格式
            try:
                # 尝试转换为标准日期格式
                df["上市日期"] = pd.to_datetime(df["上市日期"], errors="coerce").dt.strftime("%Y-%m-%d")
                # 处理NaT值
                df["上市日期"] = df["上市日期"].fillna("")
            except Exception as e:
                logger.warning(f"处理上市日期列时出错: {str(e)}，将保留原始值")
        
        # 去重
        if "日期" in df.columns:
            df = df.drop_duplicates(subset=["日期"], keep="last")
        
        # 数值列处理
        numeric_columns = ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
        for col in numeric_columns:
            if col in df.columns:
                # 使用.loc避免SettingWithCopyWarning
                df.loc[:, col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        
        # 排序
        if "日期" in df.columns:
            df = df.sort_values("日期")
        
        return df
    except Exception as e:
        logger.error(f"数据清洗和格式化时发生错误: {str(e)}")
        return df

def validate_date_range(start_date: str, end_date: str) -> bool:
    """
    验证日期范围是否有效
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 如果日期范围有效返回True，否则返回False
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start > end:
            logger.error(f"开始日期 {start_date} 不能晚于结束日期 {end_date}")
            return False
            
        if start > datetime.now():
            logger.error(f"开始日期 {start_date} 不能晚于当前日期")
            return False
            
        return True
    except ValueError:
        logger.error(f"日期格式无效，应为 YYYY-MM-DD: {start_date} 或 {end_date}")
        return False

def validate_etf_code(etf_code: str) -> bool:
    """
    验证ETF代码是否有效
    :param etf_code: ETF代码
    :return: 如果ETF代码有效返回True，否则返回False
    """
    if not etf_code or not isinstance(etf_code, str):
        logger.error("ETF代码不能为空")
        return False
        
    # 移除可能的前缀
    clean_code = re.sub(r"^(sh|sz)?", "", etf_code)
    
    # 检查是否为6位数字
    if not re.match(r"^\d{6}$", clean_code):
        logger.error(f"ETF代码格式无效: {etf_code}")
        return False
        
    return True

def get_alternative_sina_interface(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    尝试使用其他新浪接口获取数据（备用方法）
    :param etf_code: ETF代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 获取到的DataFrame
    """
    try:
        symbol = get_symbol_with_market_prefix(etf_code)
        logger.debug(f"尝试使用备用新浪接口获取ETF {symbol} 数据")
        
        # 尝试使用其他AkShare函数
        df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        
        if not df.empty:
            logger.info(f"备用新浪接口成功获取ETF {etf_code} 数据")
            return df
        else:
            logger.warning(f"备用新浪接口未获取到ETF {etf_code} 数据")
            return pd.DataFrame()
    except Exception as e:
        logger.warning(f"备用新浪接口调用失败: {str(e)}")
        return pd.DataFrame()

def fallback_to_alternative_interface(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    降级到备用接口获取数据
    :param etf_code: ETF代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 获取到的DataFrame
    """
    logger.info(f"主新浪接口失败，尝试备用接口获取ETF {etf_code} 数据")
    
    # 尝试多个备用接口
    alternative_interfaces = [
        lambda: get_alternative_sina_interface(etf_code, start_date, end_date),
        # 可以添加更多备用接口
    ]
    
    for i, interface in enumerate(alternative_interfaces):
        try:
            df = interface()
            if not df.empty:
                logger.info(f"第{i+1}个备用接口成功获取ETF {etf_code} 数据")
                return df
        except Exception as e:
            logger.warning(f"第{i+1}个备用接口调用失败: {str(e)}")
            continue
    
    logger.warning(f"所有备用接口均无法获取ETF {etf_code} 数据")
    return pd.DataFrame()

# 模块初始化
try:
    logger.info("新浪爬虫模块初始化完成")
except Exception as e:
    print(f"新浪爬虫模块初始化失败: {str(e)}")
