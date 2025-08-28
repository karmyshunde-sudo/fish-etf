# etf_scoring.py
import pandas as pd
import numpy as np
import akshare as ak
import logging
from config import Config
from utils.file_utils import load_etf_daily_data, load_etf_metadata
from data_crawler.etf_list_manager import load_all_etf_list

# 初始化日志
logger = logging.getLogger(__name__)

def get_etf_basic_info(etf_code):
    """
    从AkShare获取ETF基本信息（规模、成立日期等）
    :param etf_code: ETF代码 (6位数字)
    :return: tuple (基金规模(单位:亿元), 上市日期字符串)
    """
    size, listing_date = 0.0, ""
    try:
        logger.debug(f"尝试获取ETF基本信息，代码: {etf_code}")
        df = ak.fund_etf_info_em(symbol=etf_code)
        if df.empty:
            logger.warning(f"AkShare未返回ETF {etf_code} 的基本信息")
            return size, listing_date

        for _, row in df.iterrows():
            field_name = str(row.iloc[0])
            field_value = str(row.iloc[1])
            if "基金规模" in field_name:
                try:
                    size_str = field_value.replace("亿元", "").strip()
                    size = float(size_str) if size_str.replace('.', '', 1).isdigit() else 0.0
                except ValueError:
                    logger.warning(f"转换基金规模失败: {field_value}, ETF: {etf_code}")
                    size = 0.0
            elif "上市日期" in field_name:
                listing_date = field_value

        logger.debug(f"ETF {etf_code} 基本信息 - 规模: {size}亿元, 上市日期: {listing_date}")
        return size, listing_date

    except Exception as e:
        logger.error(f"获取ETF {etf_code} 基本信息时发生异常: {str(e)}")
        return 0.0, ""

def calculate_volatility(df, window=30):
    """计算年化波动率"""
    if df is None or df.empty or len(df) < window:
        logger.warning(f"数据不足或为空，无法计算{window}日波动率")
        return 0.0
    try:
        returns = df["涨跌幅"].tail(window)
        annualized_volatility = returns.std() * np.sqrt(252)
        return annualized_volatility
    except KeyError:
        logger.error("DataFrame中缺少'涨跌幅'列")
        return 0.0
    except Exception as e:
        logger.error(f"计算波动率时发生错误: {str(e)}")
        return 0.0

def calculate_sharpe_ratio(df, window=30, risk_free_rate=0.02):
    """计算年化夏普比率"""
    if df is None or df.empty or len(df) < window:
        logger.warning(f"数据不足或为空，无法计算{window}日夏普比率")
        return 0.0
    try:
        returns = df["涨跌幅"].tail(window)
        excess_returns = returns - risk_free_rate / 252
        sharpe_ratio = (excess_returns.mean() / returns.std()) * np.sqrt(252) if returns.std() != 0 else 0.0
        return sharpe_ratio
    except KeyError:
        logger.error("DataFrame中缺少'涨跌幅'列")
        return 0.0
    except Exception as e:
        logger.error(f"计算夏普比率时发生错误: {str(e)}")
        return 0.0

def calculate_max_drawdown(df, window=30):
    """计算最大回撤"""
    if df is None or df.empty or len(df) < window:
        logger.warning(f"数据不足或为空，无法计算{window}日内最大回撤")
        return 0.0
    try:
        prices = df["收盘"].tail(window)
        cum_max = prices.cummax()
        drawdown = (cum_max - prices) / cum_max
        max_dd = drawdown.max()
        # 处理可能的NaN值
        return max_dd if not np.isnan(max_dd) else 0.0
    except KeyError:
        logger.error("DataFrame中缺少'收盘'列")
        return 0.0
    except Exception as e:
        logger.error(f"计算最大回撤时发生错误: {str(e)}")
        return 0.0

def calculate_etf_score(etf_code, df):
    """
    增强版综合评分（0-100分）
    维度：流动性、风险控制、收益能力、溢价率、情绪指标
    :param etf_code: ETF代码
    :param df: 包含ETF历史数据的DataFrame
    :return: 综合评分 (float)
    """
    if df is None or df.empty:
        logger.warning(f"ETF {etf_code} 的数据为空，评分为0")
        return 0.0

    min_data_points = 30
    if len(df) < min_data_points:
        logger.info(f"ETF {etf_code} 的数据量({len(df)})不足{min_data_points}天，评分为0")
        return 0.0

    try:
        etf_size, listing_date = get_etf_basic_info(etf_code)
        min_size_threshold = Config.MIN_ETP_SIZE * 0.5
        if etf_size < min_size_threshold:
            logger.info(f"ETF {etf_code} 的规模({etf_size}亿元)低于阈值({min_size_threshold}亿元)，评分为0")
            return 0.0

        recent_30d = df.tail(30).copy()

        # 1. 流动性得分（近30天平均成交额）
        try:
            avg_amount = recent_30d["成交额"].mean() / 10000
            min_volume_threshold = Config.MIN_DAILY_VOLUME * 0.5
            if avg_amount < min_volume_threshold:
                logger.info(f"ETF {etf_code} 的流动性({avg_amount:.2f}万元)低于阈值({min_volume_threshold}万元)，评分为0")
                return 0.0
            liquidity_score = min(avg_amount / Config.MIN_DAILY_VOLUME * 100, 100)
        except KeyError:
            logger.error(f"DataFrame中缺少'成交额'列，ETF: {etf_code}")
            liquidity_score = 0.0

        # 2. 风险控制得分
        volatility = calculate_volatility(recent_30d)
        sharpe_ratio = calculate_sharpe_ratio(recent_30d)
        max_drawdown = calculate_max_drawdown(recent_30d)

        volatility_score = max(0, 100 - (volatility * 100))
        sharpe_score = min(max(sharpe_ratio * 50, 0), 100)
        drawdown_score = max(0, 100 - (max_drawdown * 500))

        risk_score = (volatility_score * 0.4 + sharpe_score * 0.4 + drawdown_score * 0.2)

        # 3. 收益能力得分
        try:
            return_30d = (recent_30d.iloc[-1]["收盘"] / recent_30d.iloc[0]["收盘"] - 1) * 100
            return_score = max(min(return_30d + 10, 100), 0)
        except KeyError:
            logger.error(f"DataFrame中缺少'收盘'列，无法计算收益得分, ETF: {etf_code}")
            return_score = 0.0
        except IndexError:
            logger.error(f"数据索引错误，无法计算收益得分, ETF: {etf_code}")
            return_score = 0.0

        # 4. 情绪指标得分（成交量变化率）
        try:
            if len(recent_30d) >= 5:
                volume_change = (recent_30d["成交量"].iloc[-1] / recent_30d["成交量"].iloc[-5] - 1) * 100
                sentiment_score = min(max(volume_change + 50, 0), 100)
            else:
                sentiment_score = 50
                logger.debug(f"数据量不足计算情绪得分，使用默认值, ETF: {etf_code}")
        except KeyError:
            logger.error(f"DataFrame中缺少'成交量'列，无法计算情绪得分, ETF: {etf_code}")
            sentiment_score = 50
        except IndexError:
            logger.error(f"数据索引错误，无法计算情绪得分, ETF: {etf_code}")
            sentiment_score = 50
        except ZeroDivisionError:
            logger.warning(f"除零错误，计算情绪得分时成交量可能为0, ETF: {etf_code}")
            sentiment_score = 50

        # 5. 溢价率得分 (此处简化处理，实际应获取实时IOPV数据)
        # 使用默认中等分数，避免硬编码高分造成偏差
        premium_score = 50

        # 6. 综合评分（加权求和）
        weights = Config.SCORE_WEIGHTS
        try:
            total_score = (
                liquidity_score * weights['liquidity'] +
                risk_score * weights['risk'] +
                return_score * weights['return'] +
                sentiment_score * weights['sentiment'] +
                premium_score * weights['premium']
            )
            total_score = round(total_score, 2)
            logger.debug(f"ETF {etf_code} 综合评分计算完成: {total_score}")
        except KeyError as e:
            logger.error(f"配置中缺少权重键: {e}")
            total_score = 0.0

        return total_score

    except Exception as e:
        logger.error(f"计算ETF {etf_code} 综合评分时发生未预期错误: {str(e)}")
        return 0.0

def get_top_rated_etfs(top_n=None, min_score=60):
    """
    从全市场ETF中筛选高分ETF
    :param top_n: 返回前N名，为None则返回所有高于min_score的ETF
    :param min_score: 最低评分阈值
    :return: 包含ETF代码、名称、评分等信息的DataFrame
    """
    try:
        metadata_df = load_etf_metadata()
        if metadata_df is None or metadata_df.empty:
            logger.warning("元数据为空，无法获取ETF列表")
            return pd.DataFrame()

        all_codes = metadata_df["etf_code"].tolist()
        if not all_codes:
            logger.warning("元数据中无ETF代码")
            return pd.DataFrame()

        score_list = []
        logger.info(f"开始计算 {len(all_codes)} 只ETF的综合评分...")
        for etf_code in all_codes:
            try:
                df = load_etf_daily_data(etf_code)
                score = calculate_etf_score(etf_code, df)
                if score >= min_score:
                    size, listing_date = get_etf_basic_info(etf_code)
                    etf_name = get_etf_name(etf_code)
                    score_list.append({
                        "etf_code": etf_code,
                        "etf_name": etf_name,
                        "score": score,
                        "size": size,
                        "listing_date": listing_date
                    })
                    logger.debug(f"ETF {etf_code} 评分: {score}")
            except Exception as e:
                logger.error(f"处理ETF {etf_code} 时发生错误：{str(e)}")
                # logger.error(f处理ETF {etf_code} 时发生错误: {str(e)}")
                continue

        if not score_list:
            logger.info(f"没有ETF达到最低评分阈值 {min_score}")
            return pd.DataFrame()

        score_df = pd.DataFrame(score_list).sort_values("score", ascending=False)
        total_etfs = len(score_df)
        top_percent = Config.SCORE_TOP_PERCENT
        top_count = max(10, int(total_etfs * top_percent / 100))
        top_df = score_df.head(top_count)

        logger.info(f"评分完成。共{total_etfs}只ETF评分≥{min_score}，取前{top_percent}%({top_count}只)")

        if top_n is not None and top_n > 0:
            return top_df.head(top_n)
        return top_df

    except Exception as e:
        logger.error(f"获取高分ETF列表时发生错误: {str(e)}")
        return pd.DataFrame()

def get_etf_name(etf_code):
    """从全市场列表中获取ETF名称"""
    if not etf_code or not isinstance(etf_code, str):
        return f"ETF-INVALID-CODE"

    clean_code = str(etf_code).strip().zfill(6)
    try:
        etf_list = load_all_etf_list()
        if etf_list.empty:
            logger.warning("全市场ETF列表为空")
            return f"ETF-{clean_code}"

        name_row = etf_list[etf_list["ETF代码"].astype(str) == clean_code]
        if not name_row.empty:
            return name_row.iloc[0]["ETF名称"]
        else:
            logger.debug(f"未在全市场列表中找到ETF代码: {clean_code}")
            return f"ETF-{clean_code}"
    except Exception as e:
        logger.error(f"获取ETF名称时发生错误: {str(e)}, 代码: {etf_code}")
        return f"ETF-{clean_code}"
# 0828-1256【etf_scoring.py代码】一共233行代码
