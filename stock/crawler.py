#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票日线数据增量爬取器
每5分钟运行一次，每次只爬取100只股票
使用智能多源调度系统确保"快、稳、准"
"""
import os
import logging
import pandas as pd
import time
import akshare as ak
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from config import Config
from utils.date_utils import get_beijing_time, get_utc_time
from utils.git_utils import commit_and_push_file
from concurrent.futures import ThreadPoolExecutor
import random
import requests

# ===== 智能数据源管理器（核心组件）=====
class DataSourceManager:
    """智能数据源管理器，实现动态负载均衡"""
    
    def __init__(self):
        # 定义所有可用数据源及其特性
        self.sources = {
            "akshare": {
                "status": "active",  # active, throttled, failed
                "last_throttle_time": None,
                "throttle_count": 0,
                "current_delay": 0.3,  # 初始延迟较低
                "success_count": 0,
                "failure_count": 0,
                "priority": 1,  # 优先级（越低越好）
                "max_concurrent": 3,  # 最大并发数
                "last_used": 0,
                "success_rate": 1.0  # 初始成功率100%
            },
            "sina": {
                "status": "active",
                "last_throttle_time": None,
                "throttle_count": 0,
                "current_delay": 0.4,
                "success_count": 0,
                "failure_count": 0,
                "priority": 2,
                "max_concurrent": 2,
                "last_used": 0,
                "success_rate": 1.0
            }
        }
        self.total_requests = 0
        self.successful_requests = 0
        self.last_reset_time = time.time()
        
    def get_best_source(self, stock_code: str = None) -> str:
        """获取当前最佳数据源，考虑多种因素"""
        # 每小时重置统计
        if time.time() - self.last_reset_time > 3600:
            self._reset_statistics()
        
        # 获取可用数据源
        available_sources = [
            (name, info) for name, info in self.sources.items() 
            if info["status"] == "active" and 
               (time.time() - info["last_used"] > info["current_delay"])
        ]
        
        if not available_sources:
            # 所有数据源都在冷却中，等待最短冷却时间
            min_wait = min(
                info["current_delay"] - (time.time() - info["last_used"])
                for _, info in self.sources.items()
                if info["status"] == "active"
            )
            time.sleep(max(0.1, min_wait))
            return self.get_best_source(stock_code)
        
        # 按优先级、延迟、成功率排序
        available_sources.sort(key=lambda x: (
            x[1]["priority"],
            x[1]["current_delay"],
            -x[1]["success_rate"],
            x[1]["last_used"]
        ))
        
        return available_sources[0][0]
    
    def report_success(self, source_name: str):
        """报告请求成功，更新数据源状态"""
        if source_name in self.sources:
            source = self.sources[source_name]
            source["success_count"] += 1
            source["failure_count"] = 0
            source["last_used"] = time.time()
            
            # 更新成功率
            self.successful_requests += 1
            self.total_requests += 1
            source["success_rate"] = source["success_count"] / max(1, source["success_count"] + source["failure_count"])
            
            # 如果连续成功，可以尝试降低延迟
            if source["success_count"] > 5 and source["current_delay"] > 0.3:
                source["current_delay"] = max(0.3, source["current_delay"] * 0.9)
    
    def report_failure(self, source_name: str, is_throttled: bool = False):
        """报告请求失败，更新数据源状态"""
        if source_name in self.sources:
            source = self.sources[source_name]
            source["failure_count"] += 1
            source["success_count"] = max(0, source["success_count"] - 2)  # 成功率降低
            self.total_requests += 1
            
            # 更新成功率
            source["success_rate"] = source["success_count"] / max(1, source["success_count"] + source["failure_count"])
            
            if is_throttled:
                source["throttle_count"] += 1
                source["last_throttle_time"] = time.time()
                
                # 被限流，增加延迟
                source["current_delay"] = min(5.0, source["current_delay"] * 1.2)
                
                # 如果频繁被限流，暂时禁用
                if source["throttle_count"] > 3:
                    source["status"] = "throttled"
                    logger.warning(f"数据源 {source_name} 被限流，暂时禁用")
            
            # 如果失败次数过多，暂时禁用
            if source["failure_count"] > 8:
                source["status"] = "throttled"
                logger.warning(f"数据源 {source_name} 失败次数过多，暂时禁用")
    
    def _reset_statistics(self):
        """每小时重置统计，给数据源恢复机会"""
        for source in self.sources.values():
            source["success_count"] = max(0, source["success_count"] - 5)
            source["failure_count"] = max(0, source["failure_count"] - 3)
            
            # 恢复被限流的数据源
            if (source["status"] == "throttled" and 
                source["last_throttle_time"] and 
                time.time() - source["last_throttle_time"] > 1800):  # 30分钟后尝试恢复
                source["status"] = "active"
                source["current_delay"] = max(0.5, source["current_delay"] * 0.7)
                logger.info(f"数据源 {list(self.sources.keys())[list(self.sources.values()).index(source)]} 已恢复")
        
        self.last_reset_time = time.time()

# 初始化日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 股票基础信息文件路径
BASIC_INFO_FILE = "data/all_stocks.csv"
# 日线数据存储目录
DAILY_DATA_DIR = os.path.join(Config.DATA_DIR, "daily")

# 每次爬取的股票数量
STOCKS_PER_RUN = 100
# 每批提交的股票数量
BATCH_COMMIT_SIZE = 10
# 最大重试次数
MAX_RETRIES = 3

# 全局数据源管理器
DATA_SOURCE_MANAGER = DataSourceManager()

def ensure_daily_data_dir():
    """确保日线数据目录存在"""
    os.makedirs(DAILY_DATA_DIR, exist_ok=True)
    logger.info(f"已确保日线数据目录存在: {DAILY_DATA_DIR}")

def fetch_from_akshare(stock_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """从AkShare获取股票数据"""
    # 确定市场前缀
    section = get_stock_section(stock_code)
    if section == "沪市主板" or section == "科创板":
        market_prefix = "sh"
    else:  # 深市主板、创业板
        market_prefix = "sz"
    
    # 尝试多种可能的股票代码格式
    possible_codes = [
        f"{market_prefix}{stock_code}",  # "sh000001"
        f"{stock_code}.{'SZ' if market_prefix == 'sz' else 'SH'}",  # "000001.SZ"
        stock_code,  # "000001"
        f"{stock_code}.{market_prefix.upper()}",  # "000001.SH"
        f"{market_prefix}{stock_code}.XSHG" if market_prefix == "sh" else f"{market_prefix}{stock_code}.XSHE",  # 交易所格式
    ]
    
    # 尝试使用多种接口获取数据
    df = None
    successful_code = None
    
    # 先尝试使用stock_zh_a_hist接口
    for code in possible_codes:
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                  start_date=start_date, end_date=end_date, 
                                  adjust="qfq")
            if not df.empty:
                successful_code = code
                break
        except Exception as e:
            logger.debug(f"使用stock_zh_a_hist接口获取股票 {code} 失败: {str(e)}")
    
    # 如果stock_zh_a_hist接口失败，尝试其他接口
    if df is None or df.empty:
        for code in possible_codes:
            try:
                df = ak.stock_zh_a_daily(symbol=code, 
                                       start_date=start_date, 
                                       end_date=end_date, 
                                       adjust="qfq")
                if not df.empty:
                    successful_code = code
                    break
            except Exception as e:
                logger.debug(f"使用stock_zh_a_daily接口获取股票 {code} 失败: {str(e)}")
    
    if df is not None and not df.empty and successful_code:
        logger.debug(f"成功通过AkShare获取股票 {successful_code} 数据")
        return df
    
    return None

def fetch_from_sina(stock_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """从新浪获取股票数据"""
    # 确定市场前缀
    section = get_stock_section(stock_code)
    if section == "沪市主板" or section == "科创板":
        market_prefix = "sh"
    else:  # 深市主板、创业板
        market_prefix = "sz"
    
    # 构造股票代码（新浪格式）
    full_code = f"{market_prefix}{stock_code}"
    
    try:
        # 新浪接口
        url = f"https://stock.finance.sina.com.cn/hq/api/jsonp_v2.php/WizardService2/stockhq?symbol={full_code}"
        response = requests.get(url, timeout=10)
        
        # 处理JSONP响应
        if response.status_code == 200:
            # 提取JSON数据（需要处理JSONP格式）
            json_data = response.text
            if json_data.startswith('var data='):
                json_data = json_data.replace('var data=', '').rstrip(';')
            
            try:
                data = json.loads(json_data)
                # 处理数据...
                # 这里简化了，实际需要解析JSON结构
                if 'data' in data and 'hq' in data['data']:
                    hq_data = data['data']['hq']
                    # 转换为DataFrame
                    df = pd.DataFrame(hq_data, columns=['日期', '开盘', '最高', '最低', '收盘', '成交量'])
                    return df
            except Exception as e:
                logger.debug(f"解析新浪数据失败: {str(e)}")
    except Exception as e:
        logger.debug(f"通过新浪获取股票 {full_code} 失败: {str(e)}")
    
    return None

def fetch_stock_data_with_retry(stock_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """使用最佳数据源获取股票数据"""
    original_stock_code = str(stock_code)
    stock_code = original_stock_code.zfill(6)
    
    for attempt in range(MAX_RETRIES):
        try:
            # 获取最佳数据源
            source = DATA_SOURCE_MANAGER.get_best_source(stock_code)
            logger.debug(f"股票 {stock_code} (原始: {original_stock_code}) 尝试使用数据源: {source} (尝试 {attempt+1}/{MAX_RETRIES})")
            
            # 根据数据源获取数据
            if source == "akshare":
                df = fetch_from_akshare(stock_code, start_date, end_date)
            elif source == "sina":
                df = fetch_from_sina(stock_code, start_date, end_date)
            else:
                logger.error(f"未知数据源: {source}")
                df = None
            
            if df is not None and not df.empty:
                # 报告成功
                DATA_SOURCE_MANAGER.report_success(source)
                
                # 确保日期列存在
                if "日期" not in df.columns:
                    logger.warning(f"股票 {stock_code} (原始: {original_stock_code}) 数据缺少'日期'列")
                    return None
                
                # 检查是否有必要的列
                required_columns = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    logger.warning(f"股票 {stock_code} (原始: {original_stock_code}) 数据缺少必要列: {', '.join(missing_columns)}")
                    return None
                
                # 确保日期列格式正确
                if "日期" in df.columns:
                    # 处理不同的日期格式
                    if df["日期"].dtype == 'object':
                        # 尝试多种日期格式
                        try:
                            df["日期"] = pd.to_datetime(df["日期"], format='%Y-%m-%d')
                        except:
                            try:
                                df["日期"] = pd.to_datetime(df["日期"], format='%Y%m%d')
                            except:
                                df["日期"] = pd.to_datetime(df["日期"])
                    
                    # 转换为字符串格式
                    df["日期"] = df["日期"].astype(str)
                    # 确保日期格式为YYYY-MM-DD
                    df["日期"] = df["日期"].str.replace(r'(\d{4})/(\d{1,2})/(\d{1,2})', 
                                                      lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", 
                                                      regex=True)
                    # 处理其他可能的格式
                    df["日期"] = df["日期"].str.replace(r'(\d{4})-(\d{1,2}) (\d{1,2})', 
                                                      lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}", 
                                                      regex=True)
                    # 移除可能存在的空格
                    df["日期"] = df["日期"].str.strip()
                    df = df.sort_values("日期", ascending=True)
                
                # 限制为最近1年的数据
                one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
                df = df[df["日期"] >= one_year_ago]
                
                logger.info(f"股票 {stock_code} (原始: {original_stock_code}) ✅ 成功通过 {source} 获取数据，共 {len(df)} 天（{start_date} 至 {end_date}）")
                return df
            
            # 如果数据为空，视为失败
            logger.debug(f"股票 {stock_code} (原始: {original_stock_code}) 从 {source} 获取数据为空")
            DATA_SOURCE_MANAGER.report_failure(source)
            
        except Exception as e:
            # 检测是否是限流错误
            is_throttled = "429" in str(e) or "Too Many Requests" in str(e) or "请求过于频繁" in str(e)
            DATA_SOURCE_MANAGER.report_failure(source, is_throttled)
            logger.debug(f"股票 {stock_code} (原始: {original_stock_code}) 从 {source} 获取数据失败: {str(e)}")
    
    logger.warning(f"股票 {stock_code} (原始: {original_stock_code}) 获取数据失败，所有数据源均无效")
    return None

def get_stock_section(stock_code: str) -> str:
    """
    获取股票所属板块
    
    Args:
        stock_code: 股票代码（不带市场前缀）
    
    Returns:
        str: 板块名称
    """
    # 确保股票代码是字符串
    stock_code = str(stock_code).zfill(6)
    
    # 移除可能的市场前缀
    if stock_code.lower().startswith(('sh', 'sz')):
        stock_code = stock_code[2:]
    
    # 确保股票代码是6位数字
    stock_code = stock_code.zfill(6)
    
    # 股票板块配置
    MARKET_SECTIONS = {
        "沪市主板": {"prefix": ["60"]},
        "深市主板": {"prefix": ["00"]},
        "创业板": {"prefix": ["30"]},
        "科创板": {"prefix": ["688"]}
    }
    
    # 根据股票代码前缀判断板块
    for section, config in MARKET_SECTIONS.items():
        for prefix in config["prefix"]:
            if stock_code.startswith(prefix):
                return section
    
    return "其他板块"

def process_stock_for_crawl(stock_code: str) -> Optional[str]:
    """处理单只股票的爬取任务，返回需要提交的文件路径"""
    try:
        # 计算日期范围
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        
        # 获取日线数据
        df = fetch_stock_data_with_retry(stock_code, start_date, end_date)
        
        if df is not None and not df.empty:
            # 保存数据
            file_path = os.path.join(DAILY_DATA_DIR, f"{stock_code}.csv")
            
            # 如果文件已存在，先读取并合并
            if os.path.exists(file_path):
                existing_df = pd.read_csv(file_path)
                # 合并数据
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                # 去重并按日期排序
                combined_df = combined_df.drop_duplicates(subset=["日期"], keep="last")
                combined_df = combined_df.sort_values("日期", ascending=True)
                # 限制为最近1年数据
                one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
                combined_df = combined_df[combined_df["日期"] >= one_year_ago]
                # 保存更新后的数据
                combined_df.to_csv(file_path, index=False)
            else:
                # 新建文件
                df.to_csv(file_path, index=False)
            
            return file_path  # 返回需要提交的文件路径
        
        return None
    except Exception as e:
        logger.error(f"处理股票 {stock_code} 时出错: {str(e)}", exc_info=True)
        return None

def main():
    """主函数：股票日线数据增量爬取"""
    try:
        logger.info("===== 开始执行股票日线数据增量爬取 =====")
        
        # 确保日线数据目录存在
        ensure_daily_data_dir()
        
        # 1. 加载股票基础信息
        if not os.path.exists(BASIC_INFO_FILE):
            logger.error(f"股票基础信息文件 {BASIC_INFO_FILE} 不存在，请先运行策略获取基础信息")
            return
        
        basic_info_df = pd.read_csv(BASIC_INFO_FILE)
        if basic_info_df.empty:
            logger.error("股票基础信息为空，无法继续")
            return
        
        # 确保股票代码是6位字符串
        basic_info_df["code"] = basic_info_df["code"].astype(str).str.zfill(6)
        # 保存修改后的基础信息
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        logger.info(f"已确保股票代码为6位格式，共 {len(basic_info_df)} 条记录")
        
        # 2. 确保 next_crawl_index 列存在
        if "next_crawl_index" not in basic_info_df.columns:
            basic_info_df["next_crawl_index"] = 0
            # 保存修改
            basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
            logger.info("已添加 next_crawl_index 列到股票基础信息")
        
        # 获取当前爬取索引
        current_index = int(basic_info_df["next_crawl_index"].iloc[0])
        total_stocks = len(basic_info_df)
        
        logger.info(f"当前爬取索引: {current_index} (共 {total_stocks} 只股票)")
        
        # 3. 确定要爬取的股票范围
        start_index = current_index
        end_index = min(current_index + STOCKS_PER_RUN, total_stocks)
        
        # 如果到达末尾，从头开始
        if start_index >= total_stocks:
            start_index = 0
            end_index = min(STOCKS_PER_RUN, total_stocks)
            current_index = 0
            logger.info("已到达股票列表末尾，从头开始爬取")
        
        # 4. 爬取指定范围的股票
        stock_codes = basic_info_df["code"].tolist()
        stocks_to_crawl = stock_codes[start_index:end_index]
        
        logger.info(f"本次将爬取 {len(stocks_to_crawl)} 只股票 (索引 {start_index} 到 {end_index-1})")
        
        # 收集需要提交的文件
        files_to_commit = []
        
        # 并行处理股票
        success_count = 0
        with ThreadPoolExecutor(max_workers=5) as executor:  # 增加线程数，因为有多个数据源
            results = executor.map(process_stock_for_crawl, stocks_to_crawl)
            for i, file_path in enumerate(results):
                if file_path:
                    success_count += 1
                    files_to_commit.append(file_path)
                    
                    # 每10个股票提交一次
                    if len(files_to_commit) >= BATCH_COMMIT_SIZE:
                        commit_message = f"批量更新{BATCH_COMMIT_SIZE}只股票数据 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
                        logger.info(f"正在批量提交 {len(files_to_commit)} 个股票数据文件到GitHub仓库...")
                        
                        for f in files_to_commit:
                            stock_code = os.path.basename(f).replace(".csv", "")
                            try:
                                if commit_and_push_file(f, commit_message):
                                    logger.info(f"股票 {stock_code} 数据已成功提交并推送到GitHub仓库")
                                else:
                                    logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败，但继续执行爬取")
                            except Exception as e:
                                logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败: {str(e)}，但继续执行爬取")
                        
                        files_to_commit = []  # 重置列表
                
                logger.info(f"已处理 {i+1}/{len(stocks_to_crawl)} 只股票，成功: {success_count}")
        
        # 提交剩余的文件
        if files_to_commit:
            commit_message = f"批量更新剩余{len(files_to_commit)}只股票数据 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
            logger.info(f"正在提交剩余的 {len(files_to_commit)} 个股票数据文件到GitHub仓库...")
            
            for f in files_to_commit:
                stock_code = os.path.basename(f).replace(".csv", "")
                try:
                    if commit_and_push_file(f, commit_message):
                        logger.info(f"股票 {stock_code} 数据已成功提交并推送到GitHub仓库")
                    else:
                        logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败，但继续执行爬取")
                except Exception as e:
                    logger.warning(f"提交股票 {stock_code} 数据到GitHub仓库失败: {str(e)}，但继续执行爬取")
        
        # 5. 更新 next_crawl_index
        next_index = end_index if end_index < total_stocks else 0
        basic_info_df["next_crawl_index"] = next_index
        basic_info_df.to_csv(BASIC_INFO_FILE, index=False)
        
        logger.info(f"股票基础信息已更新，next_crawl_index 设置为 {next_index}")
        
        # 提交更新后的股票基础信息
        try:
            logger.info("正在提交更新后的股票基础信息到GitHub仓库...")
            commit_message = f"自动更新 next_crawl_index 为 {next_index} [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
            if commit_and_push_file(BASIC_INFO_FILE, commit_message):
                logger.info("股票基础信息已成功提交并推送到GitHub仓库")
            else:
                logger.warning("提交股票基础信息到GitHub仓库失败，但爬取任务已完成")
        except Exception as e:
            logger.warning(f"提交股票基础信息到GitHub仓库失败: {str(e)}，但爬取任务已完成")
        
        logger.info("===== 股票日线数据增量爬取完成 =====")
    
    except Exception as e:
        logger.error(f"股票日线数据增量爬取失败: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
