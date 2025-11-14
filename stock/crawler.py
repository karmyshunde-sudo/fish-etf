# =======【3数据源crawler-豆包2.py】==============
import os
import logging
import pandas as pd
import numpy as np
import akshare as ak
import yfinance as yf
import time
import random
import json
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict
import pytz

# ======= 保留你仓库的所有相对导入（完全不修改）=======
from config import Config
from utils.date_utils import is_trading_day, get_last_trading_day, get_beijing_time
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files
# 导入股票列表更新模块（保留你原有逻辑）
from stock.all_stocks import update_stock_list

# ======= 基础配置（完全依赖你的 Config 类，不硬编码任何路径）=======
# 日志配置（保留你仓库的日志逻辑，不覆盖原有配置）
logger = logging.getLogger("StockCrawler")  # 沿用你仓库的日志器名称

# 从 Config 读取路径（100% 适配你仓库的配置）
DAILY_DIR = Config.DAILY_DATA_DIR
BASIC_INFO_FILE = Config.STOCK_BASIC_INFO_FILE
STOCK_LIST_FILE = Config.STOCK_LIST_FILE  # 新增：从Config获取all_stocks.csv路径（假设你Config中已配置，对应all_stocks.csv）

# 爬取配置（保留原逻辑，可通过 Config 动态调整）
BATCH_SIZE = Config.STOCK_CRAWL_BATCH_SIZE if hasattr(Config, 'STOCK_CRAWL_BATCH_SIZE') else 8
REQUEST_DELAY = (1.5, 2.5)  # 可移到 Config 中，保持兼容

# 全局统一必须列（与你原有数据结构完全对齐，11列缺一不可）
REQUIRED_COLUMNS = [
    "日期", "开盘", "最高", "最低", "收盘", "成交量",
    "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"
]

# 数据源配置（保留你原有的3个数据源，仅补全 YFinance 列映射）
DATA_SOURCES = [
    # 数据源1：AKShare（完全保留你原有配置）
    {
        "name": "AKShare",
        "interfaces": [
            {"func": ak.stock_zh_a_hist_min_em, "params": {"period": "daily", "adjust": "qfq"}},
            {"func": ak.stock_zh_a_hist_csindex, "params": {"period": "daily", "adjust": "qfq"}},
            {"func": ak.stock_zh_a_hist, "params": {"period": "daily", "adjust": "qfq"}},
            {"func": ak.stock_zh_a_hist_sina, "params": {"period": "daily", "adjust": "qfq"}},
        ],
        "code_convert": lambda code: format_stock_code(code),
        "column_mapping": {
            "日期": "日期", "开盘": "开盘", "最高": "最高", "最低": "最低",
            "收盘": "收盘", "成交量": "成交量", "成交额": "成交额",
            "振幅": "振幅", "涨跌幅": "涨跌幅", "涨跌额": "涨跌额", "换手率": "换手率"
        }
    },
    # 数据源2：YFinance（补全列映射，后续计算缺失5列）
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
            None,
        "column_mapping": {
            "Date": "日期", "Open": "开盘", "High": "最高", "Low": "最低",
            "Close": "收盘", "Volume": "成交量", "Adj Close": "复权收盘"
        }
    },
    # 数据源3：腾讯财经（完全保留你原有配置）
    {
        "name": "TencentFinance",
        "interfaces": [
            {"func": ak.stock_zh_a_hist_qq, "params": {"period": "daily", "adjust": "qfq"}},
            {"func": ak.stock_zh_a_hist_qq, "params": {"period": "daily", "adjust": "none"}},
        ],
        "code_convert": lambda code: format_stock_code(code),
        "column_mapping": {
            "日期": "日期", "开盘": "开盘", "最高": "最高", "最低": "最低",
            "收盘": "收盘", "成交量": "成交量", "成交额": "成交额",
            "振幅": "振幅", "涨跌幅": "涨跌幅", "涨跌额": "涨跌额", "换手率": "换手率"
        }
    }
]

# 数据源数量校验（保留你原有的校验逻辑）
if len(DATA_SOURCES) != 3:
    logger.error("致命错误：数据源数量不为3个，违反配置要求")
    raise RuntimeError("数据源数量必须为3个，程序终止")

# 全局数据源状态（保留你原有逻辑）
current_data_source_index = 0  # 初始使用AKShare


# ======= 工具函数（保留你原有函数，仅补充必要逻辑）=======
def ensure_directory_exists():
    """确保数据目录存在（保留你原有逻辑，依赖 Config 路径）"""
    if not os.path.exists(DAILY_DIR):
        os.makedirs(DAILY_DIR, exist_ok=True)
        logger.info(f"创建日线数据目录：{DAILY_DIR}")


def format_stock_code(stock_code: str) -> Optional[str]:
    """格式化股票代码为6位纯数字（完全保留你原有逻辑）"""
    if not stock_code:
        return None
    # 提取数字部分
    numeric_code = ''.join(filter(str.isdigit, str(stock_code)))
    # 校验长度（必须6位）
    if len(numeric_code) != 6:
        logger.warning(f"股票代码 {stock_code} 格式错误，必须为6位数字")
        return None
    # 校验板块合法性
    if not numeric_code.startswith(('6', '00', '30', '8')):
        logger.warning(f"股票代码 {numeric_code} 不属于A股主要板块（6/00/30/8开头）")
        return None
    return numeric_code


def get_circulating_capital(stock_code: str) -> Optional[int]:
    """获取股票流通股本（单位：股）- 用于计算换手率（保留你原有逻辑）"""
    stock_code = format_stock_code(stock_code)
    if not stock_code:
        logger.error(f"股票代码 {stock_code} 格式错误，无法获取流通股本")
        return None

    # 方案1：东方财富数据源（保留你原有逻辑）
    try:
        stock_basic = ak.stock_zh_a_basic_info_em(symbol=stock_code)
        stock_basic["项目"] = stock_basic["项目"].str.strip()
        cap_row = stock_basic[stock_basic["项目"] == "流通股本"]
        
        if not cap_row.empty:
            circulating_cap_str = cap_row["数值"].iloc[0].strip()
            # 处理单位转换
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
                if circulating_cap_str.replace('.', '').isdigit():
                    circulating_cap = int(float(circulating_cap_str))
                else:
                    logger.error(f"流通股本格式异常：{circulating_cap_str}（股票代码：{stock_code}）")
                    return None
            
            # 校验合理性（保留你原有范围）
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

    # 方案2：同花顺数据源（保留你原有逻辑）
    try:
        stock_basic_ths = ak.stock_zh_a_basic_info_ths(symbol=stock_code)
        if "流通股本(股)" in stock_basic_ths.columns:
            circulating_cap = stock_basic_ths["流通股本(股)"].iloc[0]
            if isinstance(circulating_cap, (int, float)) and not np.isnan(circulating_cap):
                circulating_cap = int(circulating_cap)
                if 1000000 <= circulating_cap <= 100000000000:
                    logger.debug(f"股票 {stock_code} 流通股本（同花顺）：{circulating_cap:,} 股")
                    return circulating_cap
            logger.error(f"同花顺接口返回异常值：{circulating_cap}（股票代码：{stock_code}）")
        else:
            logger.warning(f"同花顺接口未找到流通股本字段（{stock_code}）")
    except Exception as e2:
        logger.warning(f"同花顺接口获取流通股本失败（{stock_code}）：{str(e2)}")

    logger.error(f"所有接口均无法获取 {stock_code} 流通股本")
    return None


def validate_stock_data(df: pd.DataFrame, stock_code: str) -> bool:
    """新增：校验股票数据的完整性和合理性（不改变你原有保存逻辑，仅增加校验步骤）"""
    # 1. 校验列完整性（必须包含所有必须列）
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        logger.error(f"数据列缺失（{stock_code}）：{missing_cols}，校验失败")
        return False

    # 2. 校验数据非空
    if df.empty:
        logger.warning(f"数据为空（{stock_code}），校验失败")
        return False

    # 3. 校验核心数值列非负（价格、成交量等不能为负）
    numeric_cols = ["开盘", "最高", "最低", "收盘", "成交量", "成交额"]
    for col in numeric_cols:
        if df[col].min() < 0:
            logger.error(f"数值异常（{stock_code} - {col}）：存在负数，校验失败")
            return False

    # 4. 校验日期格式（必须为YYYY-MM-DD，与你原有数据格式一致）
    try:
        pd.to_datetime(df["日期"], format="%Y-%m-%d", errors="raise")
    except Exception as e:
        logger.error(f"日期格式异常（{stock_code}）：{str(e)}，校验失败")
        return False

    # 5. 校验换手率范围（0-100%，符合A股规则）
    if df["换手率"].max() > 100 or df["换手率"].min() < 0:
        logger.error(f"换手率异常（{stock_code}）：超出0-100%范围，校验失败")
        return False

    logger.debug(f"数据校验通过（{stock_code}）：{len(df)}行，11列完整")
    return True


# ======= 核心爬取逻辑（保留你原有逻辑，仅补充 YFinance 列补全）=======
def yfinance_daily(symbol: str, start_date: str, end_date: str, interval: str, auto_adjust: bool) -> pd.DataFrame:
    """YFinance日线数据获取封装（保留你原有逻辑）"""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(
            start=start_date,
            end=end_date,
            interval=interval,
            auto_adjust=auto_adjust,
            actions=False  # 屏蔽分红拆股数据，避免干扰（新增优化，不影响原有逻辑）
        )
        if df.empty:
            logger.warning(f"YFinance未获取到 {symbol} 数据")
            return pd.DataFrame()
        
        # 转换索引为日期列，统一列名（保留你原有兼容逻辑）
        df = df.reset_index()
        if "Datetime" in df.columns:
            df.rename(columns={"Datetime": "Date"}, inplace=True)
        return df
    except Exception as e:
        logger.error(f"YFinance获取数据失败（{symbol}）：{str(e)}")
        return pd.DataFrame()


def calculate_yfinance_missing_columns(df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
    """新增：计算YFinance缺失的5个列，补全为11列结构（不改变你原有数据格式）"""
    df_copy = df.copy()

    # 1. 计算涨跌额 = 收盘 - 开盘（保留2位小数，与A股数据格式一致）
    df_copy["涨跌额"] = (df_copy["收盘"] - df_copy["开盘"]).round(2)

    # 2. 计算涨跌幅 = 涨跌额 / 开盘 × 100（保留2位小数，开盘为0时设为0）
    df_copy["涨跌幅"] = df_copy.apply(
        lambda row: (row["涨跌额"] / row["开盘"] * 100).round(2) if row["开盘"] != 0 else 0.0,
        axis=1
    )

    # 3. 计算振幅 = (最高 - 最低) / 最低 × 100（保留2位小数，最低为0时设为0）
    df_copy["振幅"] = df_copy.apply(
        lambda row: ((row["最高"] - row["最低"]) / row["最低"] * 100).round(2) if row["最低"] != 0 else 0.0,
        axis=1
    )

    # 4. 计算成交额 = 成交量 × 收盘价（A股单位：元，保留2位小数，与你原有数据单位一致）
    df_copy["成交额"] = (df_copy["成交量"] * df_copy["收盘"]).round(2)

    # 5. 计算换手率 = 成交量 / 流通股本 × 100（保留3位小数，无流通股本时设为0）
    circulating_cap = get_circulating_capital(stock_code)
    if circulating_cap and circulating_cap > 0:
        df_copy["换手率"] = (df_copy["成交量"] / circulating_cap * 100).round(3)
    else:
        df_copy["换手率"] = 0.0
        logger.warning(f"无有效流通股本，换手率设为0（{stock_code}）")

    # 只保留必须列，确保与你原有数据结构完全一致
    return df_copy[REQUIRED_COLUMNS]


def fetch_stock_data(stock_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    从数据源获取股票数据（保留你原有逻辑，仅新增 YFinance 列补全和数据校验）
    """
    global current_data_source_index
    stock_code = format_stock_code(stock_code)
    if not stock_code:
        return None

    # 尝试所有数据源（保留你原有切换逻辑）
    for _ in range(len(DATA_SOURCES)):
        source = DATA_SOURCES[current_data_source_index]
        logger.info(f"使用数据源 {source['name']} 爬取 {stock_code} 数据（{start_date}~{end_date}）")
        
        # 转换股票代码适配当前数据源（保留你原有逻辑）
        converted_code = source["code_convert"](stock_code)
        if not converted_code:
            logger.warning(f"数据源 {source['name']} 不支持该股票代码格式")
            current_data_source_index = (current_data_source_index + 1) % len(DATA_SOURCES)
            continue

        # 尝试当前数据源的所有接口（保留你原有逻辑）
        for interface in source["interfaces"]:
            try:
                # 随机延时避免限流（保留你原有逻辑）
                time.sleep(random.uniform(*REQUEST_DELAY))
                
                # 调用接口获取原始数据（保留你原有逻辑）
                if source["name"] == "YFinance" and interface["func"] == "yfinance_daily":
                    df_raw = yfinance_daily(
                        symbol=converted_code,
                        start_date=start_date,
                        end_date=end_date,** interface["params"]
                    )
                else:
                    df_raw = interface["func"](
                        symbol=converted_code,
                        start_date=start_date,
                        end_date=end_date,
                        **interface["params"]
                    )

                if df_raw.empty:
                    logger.warning(f"数据源 {source['name']} 接口返回空数据")
                    continue

                # 列名映射（保留你原有逻辑）
                df_mapped = df_raw.rename(columns=source["column_mapping"])

                # 新增：YFinance 补全缺失列
                if source["name"] == "YFinance":
                    df_complete = calculate_yfinance_missing_columns(df_mapped, stock_code)
                else:
                    # AKShare/腾讯财经已完整，直接保留必须列（与你原有数据结构一致）
                    df_complete = df_mapped[REQUIRED_COLUMNS].copy()

                # 数据格式标准化（日期统一为YYYY-MM-DD，与你原有数据格式一致）
                df_complete["日期"] = pd.to_datetime(df_complete["日期"]).dt.strftime("%Y-%m-%d")

                # 新增：数据校验（通过后才返回，不影响你原有逻辑）
                if validate_stock_data(df_complete, stock_code):
                    logger.info(f"数据源 {source['name']} 成功获取 {stock_code} 有效数据（{len(df_complete)}行）")
                    return df_complete
                else:
                    logger.warning(f"数据源 {source['name']} 数据校验失败，尝试下一个接口")

            except Exception as e:
                logger.error(f"数据源 {source['name']} 接口调用失败：{str(e)}")
                continue

        # 当前数据源所有接口失败，切换数据源（保留你原有逻辑）
        logger.warning(f"数据源 {source['name']} 所有接口失败，切换至下一个数据源")
        current_data_source_index = (current_data_source_index + 1) % len(DATA_SOURCES)

    logger.error(f"所有数据源均无法获取 {stock_code} 有效数据")
    return None


def save_stock_data(stock_code: str, df: pd.DataFrame) -> bool:
    """保存股票数据到本地CSV文件（保留你原有逻辑，仅新增二次校验）"""
    # 新增：二次校验（防止遗漏，不改变你原有保存逻辑）
    if not validate_stock_data(df, stock_code):
        logger.error(f"数据校验失败，拒绝保存（{stock_code}）")
        return False

    try:
        ensure_directory_exists()
        file_path = os.path.join(DAILY_DIR, f"{stock_code}.csv")
        
        # 处理日期格式统一（保留你原有逻辑）
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
        
        # 如果文件已存在，合并数据去重（保留你原有逻辑）
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path, encoding="utf-8")
            # 新增：确保现有文件列结构一致
            if list(existing_df.columns) != REQUIRED_COLUMNS:
                logger.error(f"本地文件列结构不一致（{stock_code}），拒绝合并")
                return False
            combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=["日期"], keep="last")
            combined_df = combined_df.sort_values(by="日期").reset_index(drop=True)
        else:
            combined_df = df.sort_values(by="日期").reset_index(drop=True)

        # 保存文件（保留你原有编码和格式）
        combined_df.to_csv(file_path, index=False, encoding="utf-8")
        logger.info(f"数据已保存至 {file_path}（{len(combined_df)}行）")
        
        # 保留你原有 git 提交逻辑（关键！不删除任何 git 相关代码）
        commit_files_in_batches([file_path], batch_size=10, commit_msg=f"更新股票 {stock_code} 日线数据（{df['日期'].min()}~{df['日期'].max()}）")
        
        return True
    except Exception as e:
        logger.error(f"保存数据失败（{stock_code}）：{str(e)}")
        return False


def batch_crawl_stocks(stock_codes: List[str], start_date: str, end_date: str) -> Dict[str, bool]:
    """批量爬取股票数据（保留你原有逻辑，完全不修改）"""
    result = {}
    # 按批次处理（保留你原有逻辑）
    for i in range(0, len(stock_codes), BATCH_SIZE):
        batch = stock_codes[i:i+BATCH_SIZE]
        logger.info(f"开始处理批次 {i//BATCH_SIZE + 1}（{len(batch)}只股票）")
        
        for code in batch:
            df = fetch_stock_data(code, start_date, end_date)
            success = save_stock_data(code, df) if df is not None else False
            result[code] = success
            
            # 批次内股票间隔（保留你原有逻辑）
            time.sleep(random.uniform(0.5, 1.5))
        
        # 批次间间隔（保留你原有逻辑）
        if i + BATCH_SIZE < len(stock_codes):
            time.sleep(random.uniform(3, 5))
    
    # 保留你原有逻辑：强制提交剩余文件
    force_commit_remaining_files(commit_msg="批量爬取完成，提交剩余股票数据")
    
    return result


# ======= 主函数（修复股票列表更新逻辑，严格还原你的原有判断）=======
def main():
    """主函数：爬取最近交易日的股票数据（保留你原有逻辑）"""
    try:
        # 修复核心逻辑：检查all_stocks.csv是否存在，不存在才更新（完全还原你的原有逻辑）
        logger.info("检查股票列表文件是否存在...")
        if not os.path.exists(STOCK_LIST_FILE):
            logger.info("未找到股票列表文件（all_stocks.csv），开始更新股票列表...")
            update_stock_list()
            logger.info("股票列表更新完成")
        else:
            logger.info("股票列表文件（all_stocks.csv）已存在，跳过更新")

        ensure_directory_exists()
        # 保留你原有逻辑：使用仓库的 date_utils 获取最近交易日
        last_trade_day = get_last_trading_day()
        if not last_trade_day:
            logger.error("无法确定最近交易日，程序退出")
            return

        # 定义爬取日期范围（保留你原有逻辑：最近30天）
        start_date = (last_trade_day - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = last_trade_day.strftime("%Y-%m-%d")
        logger.info(f"爬取日期范围：{start_date} 至 {end_date}")

        # 从基础信息文件获取股票列表（保留你原有逻辑）
        if not os.path.exists(BASIC_INFO_FILE):
            logger.error(f"股票基础信息文件不存在：{BASIC_INFO_FILE}")
            return

        basic_df = pd.read_csv(BASIC_INFO_FILE, encoding="utf-8")
        if "代码" not in basic_df.columns:
            logger.error("基础信息文件缺少'代码'列")
            return

        # 提取并格式化股票代码（保留你原有逻辑）
        stock_codes = basic_df["代码"].dropna().unique().tolist()
        stock_codes = [format_stock_code(code) for code in stock_codes if format_stock_code(code)]
        stock_codes = list(set(stock_codes))  # 去重
        logger.info(f"共获取到 {len(stock_codes)} 只有效股票代码")

        if not stock_codes:
            logger.error("无有效股票代码，程序退出")
            return

        # 执行批量爬取（保留你原有逻辑）
        results = batch_crawl_stocks(stock_codes, start_date, end_date)
        
        # 输出爬取结果统计（保留你原有逻辑）
        success_count = sum(results.values())
        logger.info(f"爬取完成：成功 {success_count}/{len(results)} 只股票")

        # 保留你原有逻辑：输出失败代码（如果需要）
        fail_codes = [code for code, success in results.items() if not success]
        if fail_codes:
            logger.warning(f"爬取失败的股票代码：{fail_codes[:10]}...（共{len(fail_codes)}只）")

    except Exception as e:
        logger.error(f"程序执行失败：{str(e)}", exc_info=True)
        raise  # 保留你原有异常抛出逻辑


if __name__ == "__main__":
    main()
