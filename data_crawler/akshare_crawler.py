# akshare_crawler.py
import akshare as ak
import pandas as pd
import logging
import time
import re
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
from config import Config
from retrying import retry

# 初始化日志
logger = logging.getLogger(__name__)

# 重试配置
MAX_RETRY_ATTEMPTS = 3
RETRY_WAIT_FIXED = 2000  # 毫秒
RETRY_WAIT_EXPONENTIAL_MAX = 10000  # 毫秒

print(f"AkShare版本: {ak.__version__}")

# 查看可用接口
print([func for func in dir(ak) if 'etf' in func or 'fund' in func])

def empty_result_check(result: pd.DataFrame) -> bool:
    """
    检查AkShare返回结果是否为空
    :param result: AkShare返回的DataFrame
    :return: 如果结果为空返回True，否则返回False
    """
    return result is None or result.empty

def retry_if_akshare_error(exception: Exception) -> bool:
    """
    重试条件：AkShare相关错误
    :param exception: 异常对象
    :return: 如果是AkShare错误返回True，否则返回False
    """
    return isinstance(exception, (ValueError, ConnectionError, TimeoutError))

@retry(
    stop_max_attempt_number=MAX_RETRY_ATTEMPTS,
    wait_fixed=RETRY_WAIT_FIXED,
    retry_on_result=empty_result_check,
    retry_on_exception=retry_if_akshare_error
)
def crawl_etf_daily_akshare(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    用AkShare爬取ETF日线数据
    :param etf_code: ETF代码 (6位数字)
    :param start_date: 开始日期 (YYYY-MM-DD)
    :param end_date: 结束日期 (YYYY-MM-DD)
    :return: 标准化中文列名的DataFrame
    """
    try:
        logger.info(f"开始爬取ETF {etf_code} 的数据，时间范围：{start_date} 至 {end_date}")
        
        # 尝试多种AkShare接口
        df = try_multiple_akshare_interfaces(etf_code, start_date, end_date)
        
        if df.empty:
            logger.warning(f"AkShare未获取到{etf_code}数据（{start_date}至{end_date}）")
            return pd.DataFrame()
        
        # 记录返回的列名，用于调试
        logger.debug(f"AkShare返回列名: {list(df.columns)}")
        
        # 标准化列名
        df = standardize_column_names(df)
        
        # 确保所有必需列都存在
        df = ensure_required_columns(df)
        
        # 数据清洗：去重、格式转换
        df = clean_and_format_data(df)
        
        logger.info(f"AkShare成功获取{etf_code}数据，共{len(df)}条")
        return df
    
    except Exception as e:
        logger.error(f"AkShare爬取{etf_code}失败：{str(e)}")
        # 等待一段时间后重试
        time.sleep(2)
        raise  # 触发重试

def try_multiple_akshare_interfaces(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    尝试多种AkShare接口获取ETF数据
    :param etf_code: ETF代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 获取到的DataFrame
    """
    interfaces = [
        lambda: try_fund_etf_hist_em(etf_code, start_date, end_date),
        lambda: try_fund_etf_hist_sina(etf_code)  # 移除了start_date和end_date参数
    ]
    
    for i, interface in enumerate(interfaces):
        try:
            logger.debug(f"尝试第{i+1}种接口获取ETF {etf_code} 数据")
            df = interface()
            if not df.empty:
                # 对返回的数据进行日期过滤
                df['date'] = pd.to_datetime(df['date'])
                mask = (df['date'] >= pd.to_datetime(start_date)) & (df['date'] <= pd.to_datetime(end_date))
                df = df.loc[mask]
                
                if not df.empty:
                    logger.info(f"第{i+1}种接口成功获取ETF {etf_code} 数据")
                    return df
        except Exception as e:
            logger.warning(f"第{i+1}种接口调用失败: {str(e)}")
            continue
    
    logger.warning(f"所有AkShare接口均无法获取ETF {etf_code} 数据")
    return pd.DataFrame()

def try_fund_etf_hist_em(etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    尝试使用 fund_etf_hist_em 接口
    :param etf_code: ETF代码
    :param start_date: 开始日期
    :param end_date: 结束日期
    :return: 获取到的DataFrame
    """
    try:
        logger.debug(f"尝试使用 fund_etf_hist_em 接口获取ETF {etf_code} 数据")
        df = ak.fund_etf_hist_em(
            symbol=etf_code, 
            period="daily", 
            start_date=start_date, 
            end_date=end_date, 
            adjust="qfq"
        )
        return df
    except Exception as e:
        logger.warning(f"fund_etf_hist_em 接口调用失败: {str(e)}")
        return pd.DataFrame()

def try_fund_etf_hist_sina(etf_code: str) -> pd.DataFrame:  # 移除了start_date和end_date参数
    """
    尝试使用 fund_etf_hist_sina 接口
    :param etf_code: ETF代码
    :return: 获取到的DataFrame
    """
    try:
        # 添加市场前缀（上海或深圳）
        symbol = get_symbol_with_market_prefix(etf_code)
        logger.debug(f"尝试使用 fund_etf_hist_sina 接口获取ETF {symbol} 数据")
        
        # 移除了period、start_date和end_date参数
        df = ak.fund_etf_hist_sina(symbol=symbol)
        return df
    except Exception as e:
        logger.warning(f"fund_etf_hist_sina 接口调用失败: {str(e)}")
        return pd.DataFrame()

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
    标准化列名（中文映射）
    :param df: 原始DataFrame
    :return: 标准化列名的DataFrame
    """
    if df.empty:
        return df
        
    # 使用Config中的标准列名映射
    col_map = {}
    for source_col, target_col in Config.STANDARD_COLUMNS.items():
        # 尝试找到对应的源列
        if source_col in df.columns:
            col_map[source_col] = target_col
        else:
            # 尝试模糊匹配
            for actual_col in df.columns:
                if source_col in actual_col:
                    col_map[actual_col] = target_col
                    break
    
    # 重命名列
    if col_map:
        df = df.rename(columns=col_map)
        logger.debug(f"列名映射完成: {col_map}")
    
    return df

def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    确保所有必需列都存在，缺失的列进行计算或填充
    :param df: 原始DataFrame
    :return: 包含所有必需列的DataFrame
    """
    if df.empty:
        return df
        
    # 确保所有必需列都存在
    required_cols = list(Config.STANDARD_COLUMNS.keys())
    # 排除不需要从akshare获取的列（这些列会在后续处理中添加）
    exclude_cols = ["ETF代码", "ETF名称", "爬取时间"]
    required_data_cols = [col for col in required_cols if col not in exclude_cols]
    
    for col in required_data_cols:
        if col not in df.columns:
            try:
                if col == "涨跌额" and "收盘" in df.columns:
                    # 计算涨跌额
                    df[col] = (df["收盘"] - df["收盘"].shift(1)).round(4)
                    df.loc[0, col] = 0.0
                    logger.debug(f"计算涨跌额列完成")
                    
                elif col == "振幅" and "最高" in df.columns and "最低" in df.columns and "收盘" in df.columns:
                    # 计算振幅
                    df[col] = ((df["最高"] - df["最低"]) / df["收盘"].shift(1) * 100).round(4)
                    df.loc[0, col] = 0.0
                    logger.debug(f"计算振幅列完成")
                    
                elif col == "换手率" and "成交量" in df.columns and "成交额" in df.columns and "收盘" in df.columns:
                    # 计算换手率（近似计算）
                    df[col] = (df["成交量"] / (df["成交额"] / df["收盘"]) * 100).round(4)
                    logger.debug(f"计算换手率列完成")
                    
                else:
                    logger.warning(f"AkShare数据缺少必要列：{col}，使用默认值填充")
                    df[col] = 0.0
            except Exception as e:
                logger.error(f"计算列 {col} 时发生错误: {str(e)}")
                df[col] = 0.0
    
    # 只保留标准列（排除不需要从akshare获取的列）
    try:
        df = df[required_data_cols]
    except KeyError as e:
        logger.error(f"筛选标准列时发生错误: {str(e)}")
        # 尝试保留所有可用列
        available_cols = [col for col in required_data_cols if col in df.columns]
        df = df[available_cols]
    
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
        
        # 去重
        if "日期" in df.columns:
            df = df.drop_duplicates(subset=["日期"], keep="last")
        
        # 数值列处理
        numeric_columns = ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        
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

# 模块初始化
try:
    logger.info("AkShare爬虫模块初始化完成")
except Exception as e:
    print(f"AkShare爬虫模块初始化失败: {str(e)}")
# 0828-1256【akshare_crawler.py代码】一共246行代码
