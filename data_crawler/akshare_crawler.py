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
                if 'date' in df.columns:
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
    """尝试使用 fund_etf_hist_em 接口
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

def try_fund_etf_hist_sina(etf_code: str) -> pd.DataFrame:
    """尝试使用 fund_etf_hist_sina 接口
    :param etf_code: ETF代码
    :return: 获取到的DataFrame
    """
    try:
        # 添加市场前缀（上海或深圳）
        symbol = get_symbol_with_market_prefix(etf_code)
        logger.debug(f"尝试使用 fund_etf_hist_sina 接口获取ETF {symbol} 数据")
        # 调用新浪接口

        df = ak.fund_etf_hist_sina(symbol=symbol)
        # 新浪接口返回的数据可能需要特殊处理
        if not df.empty:
            # 新浪接口返回的列名可能是英文，需要转换为中文
            column_mapping = {
                'date': '日期',
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量',
                'amount': '成交额'
            }
            # 重命名列
            df = df.rename(columns=column_mapping)
            # 确保日期列存在
            if '日期' not in df.columns and 'date' in df.columns:
                df = df.rename(columns={'date': '日期'})
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

def standardize_column_names(df: pd.DataFrame, source: str = "akshare") -> pd.DataFrame:
    """标准化列名（中文映射）
    :param df: 原始DataFrame
    :param source: 数据源名称（"akshare"、"sina"等）
    :return: 标准化列名的DataFrame
    """
    if df.empty:
        return df
    
    # 【新增】关键日志：输出原始列名，帮助诊断问题
    logger.info(f"📊 {source}数据源返回的原始列名: {list(df.columns)}")
    
    # 针对不同数据源的特殊处理
    if source == "sina":
        # 【新增】新浪接口的特定列名映射规则
        sina_col_map = {
            "date": "日期",
            "open": "开盘",
            "close": "收盘",
            "high": "最高",
            "low": "最低",
            "volume": "成交量",
            "amount": "成交额",
            "pre_close": "前收盘"
        }
        for src, tgt in sina_col_map.items():
            if src in df.columns:
                df = df.rename(columns={src: tgt})
                logger.debug(f"🔄 新浪列名映射: {src} -> {tgt}")
    
    elif source == "akshare":
        # 【新增】AkShare接口的特定列名映射规则
        akshare_col_map = {
            "日期": "日期",
            "date": "日期",
            "datetime": "日期",
            "open": "开盘",
            "op": "开盘",
            "close": "收盘",
            "cl": "收盘",
            "high": "最高",
            "hi": "最高",
            "low": "最低",
            "lo": "最低",
            "volume": "成交量",
            "vol": "成交量",
            "amount": "成交额",
            "amt": "成交额",
            "change": "涨跌额",
            "pct_chg": "涨跌幅",
            "pre_close": "前收盘"
        }
        for src, tgt in akshare_col_map.items():
            if src in df.columns:
                df = df.rename(columns={src: tgt})
                logger.debug(f"🔄 AkShare列名映射: {src} -> {tgt}")
    
    # 【改进】更精确的模糊匹配逻辑
    col_map = {}
    for target_col in Config.STANDARD_COLUMNS.keys():
        # 排除不需要处理的列
        if target_col in ["ETF代码", "ETF名称", "爬取时间"]:
            continue
            
        # 精确匹配
        if target_col in df.columns:
            continue
            
        # 检查是否有相似列名
        similar_cols = []
        for actual_col in df.columns:
            # 更精确的匹配逻辑：检查是否包含关键标识
            if (target_col == "日期" and ("date" in actual_col.lower() or "time" in actual_col.lower())):
                similar_cols.append(actual_col)
            elif (target_col == "开盘" and ("open" in actual_col.lower())):
                similar_cols.append(actual_col)
            elif (target_col == "收盘" and ("close" in actual_col.lower())):
                similar_cols.append(actual_col)
            elif (target_col == "最高" and ("high" in actual_col.lower())):
                similar_cols.append(actual_col)
            elif (target_col == "最低" and ("low" in actual_col.lower())):
                similar_cols.append(actual_col)
            elif (target_col == "成交量" and ("vol" in actual_col.lower())):
                similar_cols.append(actual_col)
            elif (target_col == "成交额" and ("amount" in actual_col.lower() or "amt" in actual_col.lower())):
                similar_cols.append(actual_col)
        
        if similar_cols:
            # 选择最可能的列（通常是最短的列名）
            best_match = min(similar_cols, key=len)
            col_map[best_match] = target_col
            logger.info(f"🔍 自动匹配列名: {best_match} -> {target_col}")
    
    # 重命名列
    if col_map:
        df = df.rename(columns=col_map)
    
    # 【新增】关键日志：显示映射后的列名
    logger.info(f"✅ 标准化后的列名: {list(df.columns)}")
    
    # 检查哪些必需列仍然缺失
    missing_cols = []
    for col in Config.STANDARD_COLUMNS.keys():
        if col not in df.columns and col not in ["ETF代码", "ETF名称", "爬取时间"]:
            missing_cols.append(col)
    
    if missing_cols:
        logger.warning(f"⚠️ 数据源仍缺少必要列：{', '.join(missing_cols)}")
    
    return df
def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """确保所有必需列都存在，缺失的列进行计算或填充
    :param df: 原始DataFrame
    :return: 包含所有必需列的DataFrame
    """
    if df.empty:
        return df
    
    # 确定关键列（缺少这些列的数据不可用）
    critical_cols = ["日期", "开盘", "收盘", "最高", "最低", "成交量"]
    
    # 检查关键列是否存在
    missing_critical = [col for col in critical_cols if col not in df.columns]
    if missing_critical:
        logger.error(f"❌ 关键列缺失: {', '.join(missing_critical)} - 无法进行有效分析")
        return pd.DataFrame()  # 返回空DataFrame，避免后续处理
    
    # 常规列处理
    for col in Config.STANDARD_COLUMNS.keys():
        if col in ["ETF代码", "ETF名称", "爬取时间"]:
            continue
            
        if col not in df.columns:
            try:
                if col == "涨跌额" and "收盘" in df.columns and "前收盘" in df.columns:
                    df.loc[:, col] = (df["收盘"] - df["前收盘"]).round(4)
                elif col == "涨跌幅" and "收盘" in df.columns and "前收盘" in df.columns:
                    df.loc[:, col] = ((df["收盘"] - df["前收盘"]) / df["前收盘"] * 100).round(2)
                elif col == "振幅" and "最高" in df.columns and "最低" in df.columns and "前收盘" in df.columns:
                    df.loc[:, col] = ((df["最高"] - df["最低"]) / df["前收盘"] * 100).round(2)
                else:
                    # 非关键列可以安全填充
                    df.loc[:, col] = 0.0
                    logger.debug(f"ℹ️ 填充非关键列 {col} 为默认值 0.0")
            except Exception as e:
                logger.error(f"❌ 计算列 {col} 时出错: {str(e)}")
                df.loc[:, col] = 0.0
    
    return df

def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    """数据清洗：去重、格式转换"""
    if df.empty:
        return df
    
    # 去重
    df = df.drop_duplicates()
    
    # 格式转换 - 使用loc避免SettingWithCopyWarning
    numeric_cols = ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅", "涨跌额"]
    for col in numeric_cols:
        if col in df.columns:
            # 使用loc确保修改原始DataFrame
            df.loc[:, col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    # 日期格式化
    if "日期" in df.columns:
        df.loc[:, "日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
    
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
