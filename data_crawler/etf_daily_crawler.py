import os
import sys
import shutil
import tempfile
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from utils.logger import logger
from utils.git_utils import commit_files_in_batches, force_commit_remaining_files, _verify_git_file_content
from utils.etf_utils import fetch_etf_daily_k_data, get_etf_name
from utils.constants import (
    DAILY_DIR, BASIC_INFO_FILE, BATCH_SIZE,
    RECENT_DATE, DAYS_PER_TASK, DATA_START_DATE
)

# ============================================================
# ✅ ETF 日线数据结构 & 精度规范化函数
# ============================================================
def normalize_etf_daily_data(df: pd.DataFrame, etf_code: str, etf_name: str) -> pd.DataFrame:
    """
    规范化ETF日线数据结构和数据精度，使其与data/etf/daily/159222.csv一致
    """
    expected_columns = [
        "日期", "开盘", "最高", "最低", "收盘",
        "成交量", "成交额", "振幅", "涨跌幅", "涨跌额",
        "换手率", "IOPV", "折价率", "溢价率",
        "ETF代码", "ETF名称", "爬取时间"
    ]

    # 确保存在必要列
    for col in expected_columns:
        if col not in df.columns:
            df[col] = np.nan

    # 处理数值精度
    four_decimal_cols = [
        "开盘", "最高", "最低", "收盘",
        "成交额", "振幅", "涨跌幅", "涨跌额",
        "换手率", "IOPV", "折价率", "溢价率"
    ]
    int_cols = ["成交量"]

    for col in four_decimal_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(4)
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # ETF基本信息
    df["ETF代码"] = etf_code
    df["ETF名称"] = etf_name
    df["爬取时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 日期格式化
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["日期"])

    # 重新排列列顺序
    df = df[expected_columns]
    df = df.sort_values("日期", ascending=True).reset_index(drop=True)

    return df


# ============================================================
# ✅ 批量保存函数（增加格式化处理）
# ============================================================
def save_etf_daily_data_batch(etf_data_dict: dict) -> int:
    """
    批量保存ETF日线数据，每个ETF一个CSV文件
    """
    if not etf_data_dict:
        return 0

    os.makedirs(DAILY_DIR, exist_ok=True)
    saved_count = 0

    for etf_code, df in etf_data_dict.items():
        if df.empty:
            continue

        etf_name = get_etf_name(etf_code)
        df = normalize_etf_daily_data(df, etf_code, etf_name)

        save_path = os.path.join(DAILY_DIR, f"{etf_code}.csv")

        # 写入临时文件后再移动，防止部分写入损坏
        with tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8-sig", newline="") as temp_file:
            df.to_csv(temp_file.name, index=False, encoding="utf-8-sig")
        shutil.move(temp_file.name, save_path)

        saved_count += 1
        logger.info(f"✅ 数据已保存至: {save_path}")

    return saved_count


# ============================================================
# ✅ 主爬取逻辑
# ============================================================
def crawl_etf_daily_data(etf_codes: list[str], start_date: str, end_date: str):
    logger.info(f"开始爬取ETF日线数据，时间范围：{start_date} → {end_date}")
    etf_data_dict = {}

    for etf_code in etf_codes:
        etf_name = get_etf_name(etf_code)
        logger.info(f"📈 正在获取 {etf_name} ({etf_code}) 日线数据...")

        try:
            df = fetch_etf_daily_k_data(etf_code, start_date, end_date)
            if df is not None and not df.empty:
                etf_data_dict[etf_code] = df
            else:
                logger.warning(f"⚠️ {etf_code} 未返回有效数据")

        except Exception as e:
            logger.error(f"❌ 获取 {etf_code} 数据失败: {e}")
            continue

    if not etf_data_dict:
        logger.warning("⚠️ 未获取到任何ETF数据，跳过保存")
        return

    logger.info("开始批量保存ETF数据...")
    saved_count = save_etf_daily_data_batch(etf_data_dict)
    logger.info(f"✅ 批量保存完成，共保存 {saved_count} 只ETF。")

    # Git操作
    try:
        os.system("git add data/etf/daily/*.csv")
        commit_success = force_commit_remaining_files()
        if commit_success:
            logger.info("✅ 数据文件已成功提交至Git仓库。")
        else:
            logger.warning("⚠️ 数据文件提交至Git仓库失败。")
    except Exception as e:
        logger.error(f"❌ 提交数据文件时发生错误: {e}")


# ============================================================
# ✅ 入口函数
# ============================================================
def run_etf_daily_crawler():
    logger.info("🚀 启动ETF日线数据爬取任务")

    try:
        # 从基础文件读取ETF代码
        if not os.path.exists(BASIC_INFO_FILE):
            logger.error(f"❌ ETF基础信息文件不存在: {BASIC_INFO_FILE}")
            return

        all_etf_info = pd.read_csv(BASIC_INFO_FILE, dtype=str)
        if all_etf_info.empty or "代码" not in all_etf_info.columns:
            logger.error("❌ ETF基础信息文件无效或缺少‘代码’列")
            return

        etf_codes = all_etf_info["代码"].dropna().unique().tolist()
        logger.info(f"共读取 {len(etf_codes)} 只ETF代码。")

        # 设置爬取区间
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=DAYS_PER_TASK)).strftime("%Y-%m-%d")

        crawl_etf_daily_data(etf_codes, start_date, end_date)

        logger.info("🏁 ETF日线数据爬取任务完成。")

    except Exception as e:
        logger.error(f"❌ 运行ETF日线爬取任务时出现错误: {e}")


# ============================================================
# ✅ 脚本直接执行入口
# ============================================================
if __name__ == "__main__":
    run_etf_daily_crawler()
