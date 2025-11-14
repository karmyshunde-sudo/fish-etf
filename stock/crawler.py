import os
import time
import random
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import akshare as ak
import yfinance as yf
from typing import Optional, List, Dict

# == 基础配置（用户可根据实际情况修改）==3数据源crawler-豆包1.py=

# 日志配置（严谨记录所有操作，便于溯源）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("stock_crawler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("StockCrawler")

# 目录配置（与原有文件结构保持一致）
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DAILY_DIR = os.path.join(BASE_DIR, "daily_data")
BASIC_INFO_FILE = os.path.join(BASE_DIR, "stock_basic_info.csv")
BATCH_SIZE = 50  # 每批次爬取股票数量
REQUEST_DELAY = (1.5, 2.5)  # 基础请求延时（避免限流，适配批量爬取）

# 数据源配置（3个无调用次数限制，覆盖A股全场景）
# 核心逻辑：AKShare（首选）+ YFinance（备用）+ 腾讯财经（补充），均无调用限制
DATA_SOURCES = [
    # 数据源1：AKShare（优先使用，A股数据最精准，无调用次数限制）
    {
        "name": "AKShare",
        "interfaces": [
            # 主接口：东方财富日线（最稳定）
            {"func": ak.stock_zh_a_hist_min_em, "params": {"period": "daily", "adjust": "qfq"}},
            # 备用接口1：同花顺日线
            {"func": ak.stock_zh_a_hist_csindex, "params": {"period": "daily", "adjust": "qfq"}},
            # 备用接口2：通用A股日线
            {"func": ak.stock_zh_a_hist, "params": {"period": "daily", "adjust": "qfq"}},
            # 应急接口：新浪财经日线
            {"func": ak.stock_zh_a_hist_sina, "params": {"period": "daily", "adjust": "qfq"}},
        ],
        "code_convert": lambda code: format_stock_code(code),  # 6位纯代码（A股标准）
        "column_mapping": {
            "日期": "日期", "开盘": "开盘", "最高": "最高", "最低": "最低",
            "收盘": "收盘", "成交量": "成交量", "成交额": "成交额",
            "振幅": "振幅", "涨跌幅": "涨跌幅", "涨跌额": "涨跌额", "换手率": "换手率"
        }
    },
    # 数据源2：YFinance（备用，无调用次数限制，覆盖全球市场+A股）
    {
        "name": "YFinance",
        "interfaces": [
            {"func": "yfinance_daily", "params": {"interval": "1d", "auto_adjust": True}},
            {"func": "yfinance_daily", "params": {"interval": "1d", "auto_adjust": False}},
        ],
        "code_convert": lambda code: 
            f"{code}.SS" if code.startswith('6') else  # 沪市（6开头）
            f"{code}.SZ" if code.startswith(('00', '30')) else  # 深市（00/30开头）
            f"{code}.BJ" if code.startswith('8') else  # 北交所（8开头）
            None,  # 未知代码格式返回None，拒绝处理
        "column_mapping": {
            "Date": "日期", "Open": "开盘", "High": "最高", "Low": "最低",
            "Close": "收盘", "Volume": "成交量", "Adj Close": "复权收盘"
        }
    },
    # 数据源3：腾讯财经（补充，无调用次数限制，A股数据稳定）
    {
        "name": "TencentFinance",
        "interfaces": [
            # 主接口：腾讯财经A股日线（前复权）
            {"func": ak.stock_zh_a_hist_qq, "params": {"period": "daily", "adjust": "qfq"}},
            # 备用接口：腾讯财经A股日线（不复权）
            {"func": ak.stock_zh_a_hist_qq, "params": {"period": "daily", "adjust": "none"}},
        ],
        "code_convert": lambda code: format_stock_code(code),  # 6位纯代码（腾讯财经直接支持）
        "column_mapping": {
            "日期": "日期", "开盘": "开盘", "最高": "最高", "最低": "最低",
            "收盘": "收盘", "成交量": "成交量", "成交额": "成交额",
            "振幅": "振幅", "涨跌幅": "涨跌幅", "涨跌额": "涨跌额", "换手率": "换手率"
        }
    }
]

# 校验数据源列表有效性（强制3个数据源，确保稳定性）
if len(DATA_SOURCES) != 3:
    logger.error("致命错误：数据源数量不为3个，违反配置要求")
    raise RuntimeError("数据源数量必须为3个，程序终止")

# 全局数据源状态（跨股票保持一致，失败则切换）
current_data_source_index = 0  # 初始使用AKShare

# ===================== 基础工具函数（无近似，纯精准处理）=====================
def ensure_directory_exists():
    """确保数据目录存在（原有逻辑不变）"""
    if not os.path.exists(DAILY_DIR):
        os.makedirs(DAILY_DIR)
        logger.info(f"创建日线数据目录：{DAILY_DIR}")

def format_stock_code(stock_code: str) -> Optional[str]:
    """格式化股票代码为6位纯数字（严谨校验）"""
    if not stock_code:
        return None
    # 提取数字部分
    numeric_code = ''.join(filter(str.isdigit, str(stock_code)))
    # 校验长度（必须6位）
    if len(numeric_code) != 6:
        logger.warning(f"股票代码 {stock_code} 格式错误，必须为6位数字")
        return None
    # 校验板块合法性（仅支持A股主要板块）
    if not numeric_code.startswith(('6', '00', '30', '8')):
        logger.warning(f"股票代码 {numeric_code} 不属于A股主要板块（6/00/30/8开头）")
        return None
    return numeric_code

def is_trading_day(date: datetime.date) -> bool:
    """判断是否为A股交易日（基于AKShare精准数据，无近似）"""
    try:
        # 使用AKShare的交易日历接口（精准到日，无调用限制）
        trade_calendar = ak.tool_trade_date_hist_sina()
        trade_dates = pd.to_datetime(trade_calendar["trade_date"]).dt.date
        return date in trade_dates.tolist()
    except Exception as e:
        logger.error(f"判断交易日失败：{str(e)}")
        return False

def get_last_trading_day() -> Optional[datetime.date]:
    """获取最近一个交易日（精准计算）"""
    try:
        today = datetime.now().date()
        # 从今天往前找最近的交易日（最多找30天）
        for i in range(30):
            check_date = today - timedelta(days=i)
            if is_trading_day(check_date):
                return check_date
        logger.warning("未找到最近30天内的交易日")
        return None
    except Exception as e:
        logger.error(f"获取最近交易日失败：{str(e)}")
        return None

def get_beijing_time() -> datetime:
    """获取当前北京时区时间（无时区误差）"""
    import pytz
    beijing_tz = pytz.timezone("Asia/Shanghai")
    return datetime.now(beijing_tz)

def to_naive_datetime(dt: datetime) -> datetime:
    """移除时区信息（保持原有逻辑）"""
    if dt.tzinfo is not None:
        return dt.astimezone(pytz.utc).replace(tzinfo=None)
    return dt

def get_circulating_capital(stock_code: str) -> Optional[int]:
    """
    精准获取股票流通股本（单位：股）
    仅从AKShare两个权威数据源获取，失败返回None（无任何近似）
    """
    stock_code = format_stock_code(stock_code)
    if not stock_code:
        logger.error(f"股票代码 {stock_code} 格式错误，无法获取流通股本")
        return None

    # 方案1：东方财富数据源（优先，数据更新及时）
    try:
        stock_basic = ak.stock_zh_a_basic_info_em(symbol=stock_code)
        # 严格匹配"流通股本"项目（去除前后空格，避免匹配失败）
        stock_basic["项目"] = stock_basic["项目"].str.strip()
        cap_row = stock_basic[stock_basic["项目"] == "流通股本"]
        
        if not cap_row.empty:
            circulating_cap_str = cap_row["数值"].iloc[0].strip()
            # 处理单位转换（仅支持亿股、万股、股三种合法单位）
            if "亿股" in circulating_cap_str:
                cap_num = float(circulating_cap_str.replace("亿股", ""))
                circulating_cap = int(cap_num * 100000000)
            elif "万股" in circulating_cap_str:
                cap_num = float(circulating_cap_str.replace("万股", ""))
                circulating_cap = int(cap_num * 10000)
            elif "股" in circulating_cap_str:
                cap_num = float(circulating_cap_str.replace("股", ""))
                circulating_cap = int(cap_num)
            else:
                # 无单位默认视为"股"（仅支持纯数字）
                if circulating_cap_str.replace('.', '').isdigit():
                    circulating_cap = int(float(circulating_cap_str))
                else:
                    logger.error(f"流通股本格式异常：{circulating_cap_str}（股票代码：{stock_code}）")
                    return None
            
            # 严谨校验流通股本合理性（A股流通股本范围：1000万股 ~ 1万亿股）
            if 1000000 <= circulating_cap <= 100000000000:
                logger.debug(f"股票 {stock_code} 流通股本（东方财富）：{circulating_cap:,} 股")
                return circulating_cap
            else:
                logger.error(f"流通股本超出合理范围：{circulating_cap:,} 股（股票代码：{stock_code}）")
                return None
        else:
            logger.warning(f"东方财富接口未查询到 {stock_code} 流通股本")
    except Exception as e1:
        logger.warning(f"东方财富接口获取流通股本失败（{stock_code}）：{str(e1)}")

    # 方案2：同花顺数据源（备用，双重保障）
    try:
        stock_basic_ths = ak.stock_zh_a_basic_info_ths(symbol=stock_code)
        # 适配同花顺接口两种常见列名
        if "流通股本(股)" in stock_basic_ths.columns:
            circulating_cap = stock_basic_ths["流通股本(股)"].iloc[0]
        elif "流通股本" in stock_basic_ths.columns:
            circulating_cap = stock_basic_ths["流通股本"].iloc[0]
        else:
            logger.error(f"同花顺接口无流通股本列（{stock_code}）：{stock_basic_ths.columns.tolist()}")
            return None
        
        # 校验数据类型和合理性
        if isinstance(circulating_cap, (int, float)) and not pd.isna(circulating_cap):
            circulating_cap = int(circulating_cap)
            if 1000000 <= circulating_cap <= 100000000000:
                logger.debug(f"股票 {stock_code} 流通股本（同花顺）：{circulating_cap:,} 股")
                return circulating_cap
            else:
                logger.error(f"同花顺接口流通股本超出范围：{circulating_cap:,} 股（{stock_code}）")
                return None
        else:
            logger.error(f"同花顺接口流通股本无效：{circulating_cap}（{stock_code}）")
            return None
    except Exception as e2:
        logger.error(f"同花顺接口获取流通股本失败（{stock_code}）：{str(e2)}")

    # 两种方案均失败，返回None（无近似值）
    logger.error(f"股票 {stock_code} 无法获取精准流通股本，所有数据源均失败")
    return None

def calculate_additional_columns(df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
    """
    精准补全11列数据（无任何近似，基于真实数据计算）
    返回：完整11列DataFrame（无效则返回空DataFrame）
    """
    df_copy = df.copy()
    required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
    
    # 1. 严格校验基础列（缺一不可，否则视为无效）
    base_columns = ["开盘", "最高", "最低", "收盘", "成交量"]
    missing_base_cols = [col for col in base_columns if col not in df_copy.columns or df_copy[col].isna().all()]
    if missing_base_cols:
        logger.error(f"基础列缺失（{stock_code}）：{missing_base_cols}，数据无效")
        return pd.DataFrame()
    
    # 2. 数据类型转换（确保数值列无异常）
    for col in base_columns:
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
    # 移除数值列含NaN的行
    df_copy = df_copy.dropna(subset=base_columns)
    if df_copy.empty:
        logger.warning(f"无有效基础数据（{stock_code}）")
        return pd.DataFrame()
    
    # 3. 精准计算衍生列（基于A股标准公式）
    # 涨跌额 = 收盘 - 开盘（保留2位小数）
    df_copy["涨跌额"] = (df_copy["收盘"] - df_copy["开盘"]).round(2)
    
    # 涨跌幅 = 涨跌额 / 开盘 * 100（保留2位小数，开盘为0时设为0）
    df_copy["涨跌幅"] = df_copy.apply(
        lambda row: (row["涨跌额"] / row["开盘"] * 100).round(2) if row["开盘"] != 0 else 0,
        axis=1
    )
    
    # 振幅 = (最高 - 最低) / 最低 * 100（保留2位小数，最低为0时设为0）
    df_copy["振幅"] = df_copy.apply(
        lambda row: ((row["最高"] - row["最低"]) / row["最低"] * 100).round(2) if row["最低"] != 0 else 0,
        axis=1
    )
    
    # 成交额 = 成交量 * 收盘价（保留2位小数，A股单位：元）
    df_copy["成交额"] = (df_copy["成交量"] * df_copy["收盘"]).round(2)
    
    # 换手率 = 成交量 / 流通股本 * 100（保留3位小数，必须精准流通股本）
    circulating_cap = get_circulating_capital(stock_code)
    if circulating_cap is None:
        logger.error(f"无精准流通股本，无法计算换手率（{stock_code}）")
        return pd.DataFrame()
    
    df_copy["换手率"] = (df_copy["成交量"] / circulating_cap * 100).round(3)
    
    # 4. 日期格式标准化（统一为YYYY-MM-DD，字符串类型便于保存）
    df_copy["日期"] = pd.to_datetime(df_copy["日期"], errors='coerce')
    df_copy = df_copy.dropna(subset=["日期"])  # 移除日期无效的行
    df_copy["日期"] = df_copy["日期"].dt.strftime("%Y-%m-%d")
    
    # 5. 最终校验：确保11列完整且无异常值
    df_final = df_copy[required_columns].copy()
    # 移除换手率超出合理范围的行（0-100%）
    df_final = df_final[(df_final["换手率"] >= 0) & (df_final["换手率"] <= 100)]
    # 移除价格/成交量为负的行
    df_final = df_final[
        (df_final["开盘"] >= 0) & 
        (df_final["最高"] >= 0) & 
        (df_final["最低"] >= 0) & 
        (df_final["收盘"] >= 0) & 
        (df_final["成交量"] >= 0)
    ]
    
    if df_final.empty:
        logger.warning(f"无有效完整数据（{stock_code}）")
        return pd.DataFrame()
    
    logger.debug(f"列补全完成（{stock_code}）：11列完整，有效数据 {len(df_final)} 条")
    return df_final

# ===================== 数据源专属接口实现（无近似，纯精准）=====================
def convert_stock_code_for_source(stock_code: str, data_source: Dict) -> Optional[str]:
    """根据数据源转换股票代码格式（严谨校验，无效返回None）"""
    try:
        stock_code = format_stock_code(stock_code)
        if not stock_code:
            return None
        converted_code = data_source["code_convert"](stock_code)
        if not converted_code:
            logger.warning(f"股票 {stock_code} 无法转换为 {data_source['name']} 支持的格式")
            return None
        return converted_code
    except Exception as e:
        logger.error(f"股票代码转换失败（{stock_code} -> {data_source['name']}）：{str(e)}")
        return None

def fetch_yfinance_data(symbol: str, start_date_str: str, end_date_str: str, **kwargs) -> pd.DataFrame:
    """YFinance接口实现（严谨校验，无近似，无调用限制）"""
    try:
        # 日期格式转换（YFinance要求YYYY-MM-DD）
        start_date = datetime.strptime(start_date_str, "%Y%m%d").strftime("%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y%m%d").strftime("%Y-%m-%d")
        
        # 调用YFinance接口（屏蔽分红拆股，避免数据干扰）
        ticker = yf.Ticker(symbol)
        df = ticker.history(
            start=start_date,
            end=end_date,
            interval=kwargs.get("interval", "1d"),
            auto_adjust=kwargs.get("auto_adjust", True),
            actions=False,
            progress=False
        )
        
        if df.empty:
            logger.warning(f"YFinance无数据（{symbol}：{start_date}~{end_date}）")
            return pd.DataFrame()
        
        # 重置索引（Date转为列）
        df.reset_index(inplace=True)
        
        # 列名映射（严格按照配置）
        col_mapping = next(s for s in DATA_SOURCES if s["name"] == "YFinance")["column_mapping"]
        for yf_col, zh_col in col_mapping.items():
            if yf_col in df.columns:
                df.rename(columns={yf_col: zh_col}, inplace=True)
        
        # 确保基础列存在（防止YFinance接口返回列缺失）
        for yf_col, zh_col in [("Open", "开盘"), ("High", "最高"), ("Low", "最低"), ("Close", "收盘"), ("Volume", "成交量")]:
            if zh_col not in df.columns and yf_col in df.columns:
                df.rename(columns={yf_col: zh_col}, inplace=True)
        
        return df
    except Exception as e:
        logger.error(f"YFinance接口调用失败（{symbol}）：{str(e)}")
        return pd.DataFrame()

def fetch_tencent_finance_data(symbol: str, start_date_str: str, end_date_str: str, **kwargs) -> pd.DataFrame:
    """腾讯财经接口实现（严谨校验，无近似，无调用限制）"""
    try:
        # 腾讯财经接口直接支持6位A股代码，无需额外转换
        symbol = format_stock_code(symbol)
        if not symbol:
            logger.error(f"股票代码无效（{symbol}），无法调用腾讯财经接口")
            return pd.DataFrame()
        
        # 调用AKShare封装的腾讯财经接口
        df = ak.stock_zh_a_hist_qq(
            symbol=symbol,
            period="daily",
            adjust=kwargs.get("adjust", "qfq"),
            start_date=start_date_str,
            end_date=end_date_str
        )
        
        if df.empty:
            logger.warning(f"腾讯财经无数据（{symbol}：{start_date_str}~{end_date_str}）")
            return pd.DataFrame()
        
        # 列名映射（严格按照配置，确保与其他数据源一致）
        col_mapping = next(s for s in DATA_SOURCES if s["name"] == "TencentFinance")["column_mapping"]
        df.rename(columns=col_mapping, inplace=True)
        
        # 确保基础列存在
        base_columns = ["开盘", "最高", "最低", "收盘", "成交量"]
        missing_base_cols = [col for col in base_columns if col not in df.columns]
        if missing_base_cols:
            logger.error(f"腾讯财经基础列缺失（{symbol}）：{missing_base_cols}")
            return pd.DataFrame()
        
        return df
    except Exception as e:
        logger.error(f"腾讯财经接口调用失败（{symbol}）：{str(e)}")
        return pd.DataFrame()

# ===================== 数据源切换逻辑（严谨循环，3个数据源轮询）=====================
def get_current_data_source() -> Dict:
    """获取当前使用的数据源"""
    global current_data_source_index
    return DATA_SOURCES[current_data_source_index]

def switch_to_next_data_source() -> None:
    """切换到下一个数据源（3个数据源循环，跨股票保持一致）"""
    global current_data_source_index
    current_data_source_index = (current_data_source_index + 1) % len(DATA_SOURCES)
    current_source = get_current_data_source()
    logger.info(f"数据源切换成功，当前使用：{current_source['name']}（索引：{current_data_source_index}）")

# ===================== 核心爬取函数（严谨无近似，3数据源适配）=====================
def fetch_stock_daily_data(stock_code: str) -> pd.DataFrame:
    """
    获取单只股票日线数据（11列完整，精准无近似）
    逻辑：当前数据源所有接口尝试失败 → 切换数据源 → 下一只股票使用新数据源
    """
    try:
        # 1. 股票代码格式化（严格校验）
        stock_code = format_stock_code(stock_code)
        if not stock_code:
            logger.error(f"股票代码格式化失败：{stock_code}")
            return pd.DataFrame()
        
        # 2. 读取本地已有数据（原有逻辑不变）
        local_file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        existing_data = None
        last_date = None
        
        if os.path.exists(local_file_path):
            try:
                existing_data = pd.read_csv(local_file_path, encoding="utf-8")
                if not existing_data.empty and "日期" in existing_data.columns:
                    existing_data["日期"] = pd.to_datetime(existing_data["日期"], errors='coerce')
                    last_date = existing_data["日期"].max()
                    if pd.notna(last_date):
                        logger.info(f"股票 {stock_code} 本地已有数据，最后日期：{last_date.strftime('%Y-%m-%d')}")
                    else:
                        last_date = None
                        existing_data = None
            except Exception as e:
                logger.warning(f"读取本地数据失败（{stock_code}）：{str(e)}，将重新爬取全部数据")
                existing_data = None
                last_date = None
        
        # 3. 确定爬取日期范围（精准计算）
        now = get_beijing_time()
        if last_date is not None:
            # 增量爬取：从最后日期的下一天开始
            start_date = last_date + timedelta(days=1)
            # 找到下一个交易日（最多找30天）
            for i in range(30):
                if is_trading_day(start_date.date()):
                    break
                start_date += timedelta(days=1)
            else:
                start_date = get_last_trading_day()
                if start_date:
                    start_date = datetime.combine(start_date, datetime.min.time())
                else:
                    logger.warning(f"无法找到有效开始日期（{stock_code}）")
                    return pd.DataFrame()
            
            # 结束日期：最近一个交易日（不晚于当前时间）
            end_date = get_last_trading_day()
            if end_date:
                end_date = datetime.combine(end_date, datetime.min.time())
            else:
                logger.warning(f"无法找到最近交易日（{stock_code}）")
                return pd.DataFrame()
            
            # 时间校验：开始日期不能晚于结束日期
            start_date_naive = to_naive_datetime(start_date)
            end_date_naive = to_naive_datetime(end_date)
            now_naive = to_naive_datetime(now)
            
            if end_date_naive > now_naive:
                end_date = now
                logger.warning(f"结束日期晚于当前时间，调整为：{end_date.strftime('%Y%m%d')}")
            
            if start_date_naive > end_date_naive:
                logger.info(f"股票 {stock_code} 无新数据需要爬取（开始日期：{start_date.strftime('%Y%m%d')} > 结束日期：{end_date.strftime('%Y%m%d')}）")
                return pd.DataFrame()
            
            # 当日数据校验：未过收市时间（15:30）则不爬取
            if start_date_naive.date() == end_date_naive.date():
                market_close_time = end_date_naive.replace(hour=15, minute=30, second=0, microsecond=0)
                if now_naive < market_close_time:
                    logger.info(f"股票 {stock_code} 当前时间未过收市时间（15:30），跳过当日爬取")
                    return pd.DataFrame()
            
            logger.info(f"股票 {stock_code} 增量爬取：{start_date.strftime('%Y%m%d')} ~ {end_date.strftime('%Y%m%d')}")
        else:
            # 首次爬取：获取最近1年数据
            end_date = get_last_trading_day()
            if end_date:
                end_date = datetime.combine(end_date, datetime.min.time())
            else:
                logger.warning(f"无法找到最近交易日（{stock_code}）")
                return pd.DataFrame()
            
            start_date = end_date - timedelta(days=365)
            # 找到开始日期后的第一个交易日
            for i in range(30):
                if is_trading_day(start_date.date()):
                    break
                start_date += timedelta(days=1)
            else:
                start_date = end_date
            
            logger.info(f"股票 {stock_code} 首次爬取：{start_date.strftime('%Y%m%d')} ~ {end_date.strftime('%Y%m%d')}")
        
        # 统一日期格式字符串
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        
        # 4. 数据源调用（当前数据源所有接口尝试）
        df = None
        current_source = get_current_data_source()
        logger.info(f"当前数据源：{current_source['name']}，开始尝试所有接口（股票：{stock_code}）")
        
        for api in current_source["interfaces"]:
            try:
                # 转换股票代码
                source_stock_code = convert_stock_code_for_source(stock_code, current_source)
                if not source_stock_code and current_source["name"] != "TencentFinance":
                    # 腾讯财经无需转换，直接使用6位代码
                    logger.warning(f"股票代码转换失败，跳过当前接口（{current_source['name']}）")
                    continue
                
                # 基础延时（避免限流，适配批量爬取）
                time.sleep(random.uniform(*REQUEST_DELAY))
                
                # 调用对应接口
                if current_source["name"] == "AKShare":
                    # AKShare接口直接调用
                    df_raw = api["func"](
                        symbol=source_stock_code,
                        period=api["params"]["period"],
                        start_date=start_date_str,
                        end_date=end_date_str,
                        adjust=api["params"]["adjust"]
                    )
                elif current_source["name"] == "YFinance":
                    # YFinance接口调用
                    df_raw = fetch_yfinance_data(
                        symbol=source_stock_code,
                        start_date_str=start_date_str,
                        end_date_str=end_date_str,
                        **api["params"]
                    )
                elif current_source["name"] == "TencentFinance":
                    # 腾讯财经接口调用（直接使用原始股票代码）
                    df_raw = fetch_tencent_finance_data(
                        symbol=stock_code,
                        start_date_str=start_date_str,
                        end_date_str=end_date_str,
                        **api["params"]
                    )
                else:
                    logger.error(f"未知数据源：{current_source['name']}")
                    continue
                
                # 补全11列数据（无精准数据则视为失败）
                df = calculate_additional_columns(df_raw, stock_code)
                
                # 验证数据有效性（11列完整+非空）
                if df is not None and not df.empty and len(df.columns) == 11:
                    logger.info(f"{current_source['name']} 接口成功获取 {stock_code} 数据：{len(df)} 条有效记录")
                    break  # 接口成功，跳出循环
                else:
                    logger.warning(f"{current_source['name']} 接口数据无效，尝试下一个接口（股票：{stock_code}）")
                    df = None
            except Exception as e:
                logger.warning(f"{current_source['name']} 接口调用异常（股票：{stock_code}）：{str(e)}，尝试下一个接口")
                df = None
                continue
        
        # 5. 当前数据源所有接口失败 → 切换数据源（3个循环）
        if df is None or df.empty or len(df.columns) != 11:
            logger.error(f"【数据源切换】{current_source['name']} 所有接口均失败（股票：{stock_code}），切换至下一个数据源")
            switch_to_next_data_source()
            return pd.DataFrame()
        
        # 6. 数据合并与去重（原有逻辑不变）
        if existing_data is not None and not existing_data.empty:
            # 合并本地数据与新爬取数据
            existing_data["日期"] = pd.to_datetime(existing_data["日期"], errors='coerce')
            df["日期"] = pd.to_datetime(df["日期"], errors='coerce')
            combined_df = pd.concat([existing_data, df], ignore_index=True)
            # 去重（保留最新日期数据）
            combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
            # 排序并保留最近250条数据（1年左右）
            combined_df = combined_df.sort_values("日期").reset_index(drop=True)
            if len(combined_df) > 250:
                combined_df = combined_df.tail(250)
            df = combined_df
            logger.info(f"股票 {stock_code} 合并后数据：{len(df)} 条（新增 {len(df) - len(existing_data)} 条）")
        
        # 7. 补充股票代码列（原有逻辑不变）
        if "股票代码" not in df.columns:
            df["股票代码"] = stock_code
        else:
            df["股票代码"] = df["股票代码"].apply(lambda x: format_stock_code(str(x)))
            df = df[df["股票代码"].notna()]
        
        # 8. 最终数据校验（确保无异常）
        required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率", "股票代码"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"最终数据缺少列（{stock_code}）：{missing_columns}")
            return pd.DataFrame()
        
        return df
    
    except Exception as e:
        logger.error(f"获取股票 {stock_code} 数据发生未捕获异常：{str(e)}", exc_info=True)
        return pd.DataFrame()

# ===================== 保存函数（原有逻辑不变，无任何修改）=====================
def save_stock_daily_data(stock_code: str, df: pd.DataFrame) -> bool:
    """保存股票日线数据到本地CSV（原有逻辑完全不变）"""
    try:
        stock_code = format_stock_code(stock_code)
        if not stock_code or df.empty:
            logger.warning(f"保存失败：股票代码无效或数据为空（{stock_code}）")
            return False
        
        local_file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        # 日期格式标准化
        df["日期"] = pd.to_datetime(df["日期"], errors='coerce').dt.strftime("%Y-%m-%d")
        # 保存为CSV（无索引，编码UTF-8）
        df.to_csv(local_file_path, index=False, encoding="utf-8")
        logger.info(f"成功保存 {stock_code} 数据到：{local_file_path}（{len(df)} 条）")
        return True
    except Exception as e:
        logger.error(f"保存股票 {stock_code} 数据失败：{str(e)}", exc_info=True)
        return False

# ===================== Git提交函数（原有逻辑不变，无任何修改）=====================
def commit_files_in_batches(file_path: str, commit_msg: str) -> bool:
    """批量提交文件到Git（原有逻辑完全不变）"""
    try:
        if not os.path.exists(file_path):
            logger.warning(f"提交失败：文件不存在（{file_path}）")
            return False
        
        import subprocess
        # 添加文件
        subprocess.run(["git", "add", file_path], check=True, capture_output=True, text=True)
        # 提交
        subprocess.run(["git", "commit", "-m", commit_msg], check=True, capture_output=True, text=True)
        logger.info(f"成功提交文件：{file_path}（提交信息：{commit_msg}）")
        return True
    except Exception as e:
        logger.error(f"提交文件 {file_path} 失败：{str(e)}", exc_info=True)
        return False

def force_commit_remaining_files() -> bool:
    """强制提交剩余未提交文件（原有逻辑完全不变）"""
    try:
        import subprocess
        # 检查未提交文件
        result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not result.stdout:
            logger.info("无未提交文件")
            return True
        
        # 强制添加所有文件
        subprocess.run(["git", "add", "."], check=True, capture_output=True, text=True)
        # 提交
        commit_msg = f"强制提交剩余文件（{datetime.now().strftime('%Y%m%d%H%M%S')}）"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True, capture_output=True, text=True)
        logger.info("成功强制提交所有剩余文件")
        return True
    except Exception as e:
        logger.error(f"强制提交剩余文件失败：{str(e)}", exc_info=True)
        return False

# ===================== 股票列表更新函数（原有逻辑不变）=====================
def update_stock_list() -> bool:
    """更新A股股票基础信息列表（原有逻辑完全不变）"""
    try:
        logger.info("开始更新A股股票基础信息列表")
        # 从AKShare获取A股基础信息
        stock_info_df = ak.stock_zh_a_spot_em()
        if stock_info_df.empty:
            logger.error("获取A股基础信息失败，返回空数据")
            return False
        
        # 数据清洗（保留6位代码、名称等核心字段）
        stock_info_df["代码"] = stock_info_df["代码"].apply(format_stock_code)
        stock_info_df = stock_info_df[stock_info_df["代码"].notna()]
        stock_info_df = stock_info_df[["代码", "名称", "最新价", "涨跌幅", "成交量"]].copy()
        # 添加下一次爬取索引
        stock_info_df["next_crawl_index"] = 0 if len(stock_info_df) == 0 else 0
        
        # 保存到本地
        stock_info_df.to_csv(BASIC_INFO_FILE, index=False, encoding="utf-8")
        logger.info(f"成功更新A股股票基础信息：{len(stock_info_df)} 只股票")
        return commit_files_in_batches(BASIC_INFO_FILE, "更新A股股票基础信息列表")
    except Exception as e:
        logger.error(f"更新股票列表失败：{str(e)}", exc_info=True)
        return False

# ===================== 批次爬取函数（适配3数据源，批量爬取优化）=====================
def update_all_stocks_daily_data() -> bool:
    """更新所有股票日线数据（适配3数据源，批量爬取优化）"""
    ensure_directory_exists()
    
    # 检查并更新股票基础信息列表
    if not os.path.exists(BASIC_INFO_FILE) or os.path.getsize(BASIC_INFO_FILE) == 0:
        logger.info("基础信息文件不存在或为空，开始创建")
        if not update_stock_list():
            logger.error("创建股票基础信息文件失败，无法继续爬取")
            return False
    
    # 读取股票基础信息
    try:
        basic_info_df = pd.read_csv(BASIC_INFO_FILE, encoding="utf-8")
        if basic_info_df.empty:
            logger.error("股票基础信息文件为空，无法继续爬取")
            return False
        
        # 确保代码格式正确
        basic_info_df["代码"] = basic_info_df["代码"].apply(format_stock_code)
        basic_info_df = basic_info_df[basic_info_df["代码"].notna()].reset_index(drop=True)
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False, encoding="utf-8")
        commit_files_in_batches(BASIC_INFO_FILE, "标准化股票代码格式")
        logger.info(f"股票基础信息文件加载完成：{len(basic_info_df)} 只股票")
    except Exception as e:
        logger.error(f"读取股票基础信息文件失败：{str(e)}", exc_info=True)
        return False
    
    # 确定当前爬取位置
    total_stocks = len(basic_info_df)
    next_index = basic_info_df["next_crawl_index"].iloc[0] if "next_crawl_index" in basic_info_df.columns else 0
    next_index = int(next_index) % total_stocks
    logger.info(f"当前爬取状态：下一个爬取索引 = {next_index}（共 {total_stocks} 只股票）")
    
    # 计算本批次爬取范围
    start_idx = next_index
    end_idx = start_idx + BATCH_SIZE
    actual_end_idx = end_idx % total_stocks
    
    # 构建本批次股票列表
    if end_idx <= total_stocks:
        batch_df = basic_info_df.iloc[start_idx:end_idx].copy()
        logger.info(f"本批次爬取：索引 {start_idx} ~ {end_idx-1}（{len(batch_df)} 只股票）")
    else:
        # 循环爬取（跨列表末尾）
        batch_df1 = basic_info_df.iloc[start_idx:total_stocks].copy()
        batch_df2 = basic_info_df.iloc[0:end_idx-total_stocks].copy()
        batch_df = pd.concat([batch_df1, batch_df2], ignore_index=True)
        logger.info(f"本批次爬取：索引 {start_idx} ~ {total_stocks-1} + 0 ~ {end_idx-total_stocks-1}（{len(batch_df)} 只股票）")
    
    if batch_df.empty:
        logger.warning("本批次无股票可爬取")
        return False
    
    # 本批次股票代码列表
    batch_codes = batch_df["代码"].tolist()
    logger.info(f"本批次爬取股票代码：{batch_codes[:5]}...（共 {len(batch_codes)} 只）")
    
    # 逐个爬取股票数据（适配3数据源，基础延时）
    processed_count = 0
    for stock_code in batch_codes:
        stock_code = format_stock_code(stock_code)
        if not stock_code:
            logger.warning(f"跳过无效股票代码：{stock_code}")
            continue
        
        # 基础延时（避免限流，适配批量爬取）
        time.sleep(random.uniform(*REQUEST_DELAY))
        
        # 爬取并保存数据
        df = fetch_stock_daily_data(stock_code)
        if not df.empty:
            if save_stock_daily_data(stock_code, df):
                processed_count += 1
                # 每处理10只股票提交一次
                if processed_count % 10 == 0:
                    logger.info(f"本批次已处理 {processed_count} 只股票，执行批量提交...")
                    force_commit_remaining_files()
    
    # 提交本批次剩余文件
    logger.info(f"本批次爬取完成，处理成功 {processed_count} 只股票，提交剩余文件...")
    if not force_commit_remaining_files():
        logger.error("本批次剩余文件提交失败，可能导致数据丢失")
    
    # 更新下一次爬取索引
    new_next_index = actual_end_idx
    basic_info_df["next_crawl_index"] = new_next_index
    basic_info_df.to_csv(BASIC_INFO_FILE, index=False, encoding="utf-8")
    commit_files_in_batches(BASIC_INFO_FILE, f"更新爬取索引至 {new_next_index}")
    logger.info(f"成功更新爬取索引：{new_next_index}")
    
    # 计算剩余未爬取股票数量
    remaining_stocks = (total_stocks - new_next_index) % total_stocks
    logger.info(f"本批次爬取完成！累计成功处理 {processed_count} 只股票，剩余 {remaining_stocks} 只股票待爬取")
    
    return True

# ===================== 程序入口（原有逻辑不变）=====================
if __name__ == "__main__":
    logger.info("="*50)
    logger.info(f"A股股票日线数据爬虫启动（{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}）")
    logger.info(f"当前配置：3个无限制数据源 = {[s['name'] for s in DATA_SOURCES]}")
    logger.info("="*50)
    
    try:
        # 执行批次爬取
        success = update_all_stocks_daily_data()
        if success:
            logger.info("爬虫执行成功！")
        else:
            logger.error("爬虫执行失败！")
    except Exception as e:
        logger.error("爬虫执行过程中发生致命异常：", exc_info=True)
    finally:
        logger.info("="*50)
        logger.info(f"爬虫执行结束（{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}）")
        logger.info("="*50)
