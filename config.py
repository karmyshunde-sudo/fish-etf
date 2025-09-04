#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置模块
提供项目全局配置参数，包括路径、日志、策略参数等
特别优化了时区相关配置，确保所有时间显示为北京时间
"""

import os
import logging
import sys
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime, timezone, timedelta  # 确保timedelta已正确导入

# 先定义获取基础目录的函数，避免类定义时的循环引用问题
def _get_base_dir() -> str:
    """获取项目根目录路径"""
    try:
        # 优先使用GITHUB_WORKSPACE环境变量（GitHub Actions环境）
        base_dir = os.environ.get('GITHUB_WORKSPACE')
        if base_dir and os.path.exists(base_dir):
            return os.path.abspath(base_dir)
        
        # 尝试基于当前文件位置计算项目根目录
        current_file_path = os.path.abspath(__file__)
        base_dir = os.path.dirname(os.path.dirname(current_file_path))
        
        # 确保目录存在
        if os.path.exists(base_dir):
            return os.path.abspath(base_dir)
        
        # 作为最后手段，使用当前工作目录
        return os.path.abspath(os.getcwd())
    except Exception as e:
        print(f"获取项目根目录失败: {str(e)}", file=sys.stderr)
        # 退回到当前工作目录
        return os.path.abspath(os.getcwd())

class Config:
    """
    全局配置类：数据源配置、策略参数、文件路径管理
    所有配置项均有默认值，并支持从环境变量覆盖
    """
    
    # -------------------------
    # 0. 时区定义
    # -------------------------
    # 严格遵守要求：在config.py中定义两个变量，分别保存平台时间UTC，北京时间UTC+8
    UTC_TIMEZONE = timezone.utc
    BEIJING_TIMEZONE = timezone(timedelta(hours=8))
    
    # -------------------------
    # 0.1 新增：交易时间配置
    # -------------------------
    TRADING_START_TIME: str = "09:30"  # 交易开始时间
    TRADING_END_TIME: str = "15:00"    # 交易结束时间
    ARBITRAGE_THRESHOLD: float = 0.5   # 折溢价率阈值（百分比）
    
    # -------------------------
    # 1. 数据源配置
    # -------------------------
    # 初次爬取默认时间范围（1年）
    INITIAL_CRAWL_DAYS: int = 365

    # ETF列表更新间隔（天）
    ETF_LIST_UPDATE_INTERVAL: int = 7  
    # 每7天更新一次ETF列表
    
    # 英文列名到中文列名的映射
    COLUMN_NAME_MAPPING: Dict[str, str] = {
        "date": "日期",
        "open": "开盘",
        "close": "收盘",
        "high": "最高",
        "low": "最低",
        "volume": "成交量",
        "amount": "成交额",
        "amplitude": "振幅",
        "pct_change": "涨跌幅",
        "price_change": "涨跌额",
        "turnover": "换手率",
        "etf_code": "ETF代码",
        "etf_name": "ETF名称",
        "crawl_time": "爬取时间",
        "market_price": "市场价格",
        "iopv": "IOPV",
        "premium_discount": "折溢价率",
        "net_value_time": "净值时间"
    }

    # 标准列名（中文）- 修复：添加STANDARD_COLUMNS属性
    STANDARD_COLUMNS: list = list(COLUMN_NAME_MAPPING.values())
    
    # 中文列名集合（用于验证）
    CHINESE_COLUMNS: list = list(COLUMN_NAME_MAPPING.values())
    
    # ETF列表标准列（确保all_etfs.csv和karmy_etf.csv结构一致）
    ETF_STANDARD_COLUMNS: list = ["ETF代码", "ETF名称", "完整代码", "基金规模"]
    
    # 新浪数据源备用接口
    SINA_ETF_HIST_URL: str = "https://finance.sina.com.cn/realstock/company/  {etf_code}/hisdata/klc_kl.js"
    
    # 批量爬取批次大小
    CRAWL_BATCH_SIZE: int = 50  # 每批50只ETF

    # -------------------------
    # 2. 策略参数配置
    # -------------------------
    # 套利策略：交易成本（印花税0.1%+佣金0.02%）
    TRADE_COST_RATE: float = 0.0012  # 0.12%
    
    # 套利阈值（收益率超过该值才推送）
    ARBITRAGE_PROFIT_THRESHOLD: float = 0.005  # 0.5%
    
    # 显示阈值（折溢价率超过该值才在消息中显示）
    MIN_ARBITRAGE_DISPLAY_THRESHOLD: float = 3.0  # 3.0% - 仅显示显著的套利机会
    
    # 综合评分筛选阈值（仅保留评分前N%的ETF）
    SCORE_TOP_PERCENT: int = 20  # 保留前20%高分ETF
    
    # 最低规模阈值（亿元）
    MIN_ETP_SIZE: float = 10.0  # 规模≥10亿
    
    # 最低日均成交额阈值（万元）
    MIN_DAILY_VOLUME: float = 5000.0  # 日均成交额≥5000万
    
    # 仓位策略参数（均线策略）
    MA_SHORT_PERIOD: int = 5    # 短期均线（5日）
    MA_LONG_PERIOD: int = 20    # 长期均线（20日）
    ADD_POSITION_THRESHOLD: float = 0.03  # 加仓阈值（涨幅超3%）
    STOP_LOSS_THRESHOLD: float = -0.05    # 止损阈值（跌幅超5%")
    
    # 评分维度权重
    SCORE_TOP_PERCENT: int = 20  # 保留前20%高分ETF
    
    # 稳健仓、激进仓ETF综合评分维度权重
    # 用于评估ETF本身的投资价值，不涉及具体交易策略
    SCORE_WEIGHTS: Dict[str, float] = {
        'liquidity': 0.20,    # 流动性评分权重（日均成交额）
        'risk': 0.25,         # 风险控制评分权重（波动率、夏普比率、最大回撤）
        'return': 0.25,       # 收益能力评分权重（30天收益率）
        'sentiment': 0.15,    # 情绪指标评分权重（成交量变化率）
        'fundamental': 0.15   # 基本面评分权重（规模、成立时间）
    }
    
    # 套利机会综合评分维度权重
    # 用于评估套利交易机会的质量，包含折溢价率等套利特有指标
    ARBITRAGE_SCORE_WEIGHTS: Dict[str, float] = {
        'premium_discount': 0.30,       # 折溢价率评分权重（核心指标）
        'liquidity': 0.15,              # 流动性评分权重（日均成交额）
        'risk': 0.15,                   # 风险控制评分权重（波动率、夏普比率、最大回撤）
        'return': 0.10,                 # 收益能力评分权重（30天收益率）
        'market_sentiment': 0.10,       # 市场情绪评分权重（成交量变化率）
        'fundamental': 0.10,            # 基本面评分权重（规模、成立时间）
        'component_stability': 0.10     # 成分股稳定性评分权重（波动率、规模）
    }
    
    # 买入信号条件
    BUY_SIGNAL_DAYS: int = 2  # 连续几天信号持续才买入
    
    # 换股条件
    SWITCH_THRESHOLD: float = 0.3  # 新ETF比原ETF综合评分高出30%则换股

    # -------------------------
    # 2.1 新增：套利综合评分配置
    # -------------------------
    # 明确区分折价和溢价阈值
    DISCOUNT_THRESHOLD: float = 0.5   # 折价阈值（0.5%）
    PREMIUM_THRESHOLD: float = 0.5    # 溢价阈值（0.5%）
    
    # 行业平均折价率（用于附加条件加分）
    INDUSTRY_AVG_DISCOUNT: float = -0.15  # 行业平均折价率（-0.15%）
    
    # 显示阈值（用于消息中）
    MIN_DISCOUNT_DISPLAY_THRESHOLD: float = 0.3  # 显示折价机会的最小阈值（0.3%）
    MIN_PREMIUM_DISPLAY_THRESHOLD: float = 0.3   # 显示溢价机会的最小阈值（0.3%）
    
    # 基本筛选条件（用于消息中）
    MIN_FUND_SIZE: float = 10.0      # 基金规模阈值(亿元)
    MIN_AVG_VOLUME: float = 5000.0   # 日均成交额阈值(万元)
    
    # 综合评分阈值配置
    ARBITRAGE_SCORE_THRESHOLD: float = 70.0  # 综合评分阈值
    
    # -------------------------
    # 3. 文件路径配置 - 基于仓库根目录的路径
    # -------------------------
    # 获取仓库根目录（优先使用GITHUB_WORKSPACE环境变量）
    @staticmethod
    def get_base_dir() -> str:
        """获取项目根目录路径"""
        return _get_base_dir()
    
    # 修复：使用静态方法调用而不是类方法调用
    BASE_DIR: str = _get_base_dir()
    
    # 数据存储路径
    DATA_DIR: str = os.path.join(BASE_DIR, "data")
    ETFS_DAILY_DIR: str = os.path.join(DATA_DIR, "etf_daily")
    
    # ETF元数据（记录最后爬取日期）
    METADATA_PATH: str = os.path.join(DATA_DIR, "etf_metadata.csv")
    
    # 策略结果标记（避免单日重复推送）
    FLAG_DIR: str = os.path.join(DATA_DIR, "flags")
    
    # 套利状态文件 - 用于记录每个ETF的推送状态（增量推送功能）
    ARBITRAGE_STATUS_FILE: str = os.path.join(FLAG_DIR, "arbitrage_status.json")
    
    # 折价状态文件
    DISCOUNT_STATUS_FILE: str = os.path.join(FLAG_DIR, "discount_status.json")
    
    # 溢价状态文件
    PREMIUM_STATUS_FILE: str = os.path.join(FLAG_DIR, "premium_status.json")
    
    # 套利结果标记文件（保留用于兼容性）
    @staticmethod
    def get_arbitrage_flag_file(date_str: Optional[str] = None) -> str:
        """获取套利标记文件路径"""
        try:
            # 尝试使用北京时间
            from utils.date_utils import get_beijing_time
            date = date_str or get_beijing_time().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"arbitrage_pushed_{date}.txt")
        except ImportError:
            # 回退到简单实现（仅用于初始化阶段）
            date = date_str or datetime.now().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"arbitrage_pushed_{date}.txt")
        except Exception as e:
            logging.error(f"获取套利标记文件路径失败: {str(e)}", exc_info=True)
            return os.path.join(Config.FLAG_DIR, "arbitrage_pushed_error.txt")
    
    # 折价标记文件
    @staticmethod
    def get_discount_flag_file(date_str: Optional[str] = None) -> str:
        """获取折价标记文件路径"""
        try:
            # 尝试使用北京时间
            from utils.date_utils import get_beijing_time
            date = date_str or get_beijing_time().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"discount_pushed_{date}.txt")
        except ImportError:
            # 回退到简单实现（仅用于初始化阶段）
            date = date_str or datetime.now().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"discount_pushed_{date}.txt")
        except Exception as e:
            logging.error(f"获取折价标记文件路径失败: {str(e)}", exc_info=True)
            return os.path.join(Config.FLAG_DIR, "discount_pushed_error.txt")
    
    # 溢价标记文件
    @staticmethod
    def get_premium_flag_file(date_str: Optional[str] = None) -> str:
        """获取溢价标记文件路径"""
        try:
            # 尝试使用北京时间
            from utils.date_utils import get_beijing_time
            date = date_str or get_beijing_time().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"premium_pushed_{date}.txt")
        except ImportError:
            # 回退到简单实现（仅用于初始化阶段）
            date = date_str or datetime.now().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"premium_pushed_{date}.txt")
        except Exception as e:
            logging.error(f"获取溢价标记文件路径失败: {str(e)}", exc_info=True)
            return os.path.join(Config.FLAG_DIR, "premium_pushed_error.txt")
    
    # 仓位策略结果标记文件
    @staticmethod
    def get_position_flag_file(date_str: Optional[str] = None) -> str:
        """获取仓位标记文件路径"""
        try:
            # 尝试使用北京时间
            from utils.date_utils import get_beijing_time
            date = date_str or get_beijing_time().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"position_pushed_{date}.txt")
        except ImportError:
            # 回退到简单实现（仅用于初始化阶段）
            date = date_str or datetime.now().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"position_pushed_{date}.txt")
        except Exception as e:
            logging.error(f"获取仓位标记文件路径失败: {str(e)}", exc_info=True)
            return os.path.join(Config.FLAG_DIR, "position_pushed_error.txt")
    
    # 交易记录文件
    TRADE_RECORD_FILE: str = os.path.join(DATA_DIR, "trade_records.csv")
    
    # 全市场ETF列表存储路径
    ALL_ETFS_PATH: str = os.path.join(DATA_DIR, "all_etfs.csv")
    
    # 兜底ETF列表路径
    BACKUP_ETFS_PATH: str = os.path.join(DATA_DIR, "karmy_etf.csv")

    # -------------------------
    # 4. 日志配置
    # -------------------------
    @staticmethod
    def setup_logging(log_level: Optional[str] = None,
                     log_file: Optional[str] = None) -> None:
        """
        配置日志系统
        :param log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        :param log_file: 日志文件路径，如果为None则只输出到控制台
        """
        try:
            level = log_level or Config.LOG_LEVEL
            log_format = Config.LOG_FORMAT
            
            # 创建根日志记录器
            root_logger = logging.getLogger()
            root_logger.setLevel(level)
            
            # 清除现有处理器
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
            
            # 创建格式化器
            formatter = logging.Formatter(log_format)
            
            # 创建控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)
            
            # 创建文件处理器（如果指定了日志文件）
            if log_file:
                try:
                    # 确保日志目录存在
                    log_dir = os.path.dirname(log_file)
                    if log_dir and not os.path.exists(log_dir):
                        os.makedirs(log_dir, exist_ok=True)
                    
                    file_handler = logging.FileHandler(log_file, encoding='utf-8')
                    file_handler.setLevel(level)
                    file_handler.setFormatter(formatter)
                    root_logger.addHandler(file_handler)
                    logging.info(f"日志文件已配置: {log_file}")
                except Exception as e:
                    logging.error(f"配置日志文件失败: {str(e)}", exc_info=True)
        except Exception as e:
            logging.error(f"配置日志系统失败: {str(e)}", exc_info=True)
    
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_DIR: str = os.path.join(BASE_DIR, "logs")  # 日志目录配置
    LOG_FILE: str = os.path.join(LOG_DIR, "etf_strategy.log")  # 日志文件路径

    # -------------------------
    # 5. 新增：网络请求配置
    # -------------------------
    # 请求超时设置（秒）
    REQUEST_TIMEOUT: int = 30
    
    # -------------------------
    # 6. 企业微信机器人配置
    # 企业微信消息固定末尾（用于标识消息来源）
    # -------------------------
    # 直接作为类属性，确保其他模块能直接访问
    WECOM_WEBHOOK: str = os.getenv("WECOM_WEBHOOK", "")

    WECOM_MESFOOTER: str = (
        "\n\n"
        "【GIT-fish-etf】\n"
        "📊 数据来源：AkShare | 环境：生产\n"
        "🕒 消息生成时间：{current_time}"
    )
    
    # -------------------------
    # 7. ETF筛选配置
    # -------------------------
    # ETF筛选参数 - 全局默认值
    GLOBAL_MIN_FUND_SIZE: float = 10.0  # 默认基金规模≥10亿元
    GLOBAL_MIN_AVG_VOLUME: float = 5000.0  # 默认日均成交额≥5000万元

    # 仓位类型特定参数
    STRATEGY_PARAMETERS = {
        "稳健仓": {
            "min_fund_size": GLOBAL_MIN_FUND_SIZE,
            "min_avg_volume": GLOBAL_MIN_AVG_VOLUME
        },
        "激进仓": {
            "min_fund_size": 2.0,  # 放宽至2亿元
            "min_avg_volume": 1000.0  # 放宽至1000万元
        }
    }


    # -------------------------
    # 8. 配置验证方法
    # -------------------------
    @staticmethod
    def validate_config() -> Dict[str, Any]:
        """
        验证配置是否有效，返回验证结果
        :return: 包含验证结果的字典
        """
        results = {}
        
        try:
            # 检查必要的目录是否存在或可创建
            required_dirs = [
                Config.DATA_DIR, 
                Config.ETFS_DAILY_DIR,
                Config.FLAG_DIR, 
                Config.LOG_DIR
            ]
            for dir_path in required_dirs:
                try:
                    if not os.path.exists(dir_path):
                        os.makedirs(dir_path, exist_ok=True)
                    results[f"dir_{os.path.basename(dir_path)}"] = {
                        "status": "OK", 
                        "path": dir_path,
                        "writable": os.access(dir_path, os.W_OK)
                    }
                except Exception as e:
                    results[f"dir_{os.path.basename(dir_path)}"] = {
                        "status": "ERROR", 
                        "path": dir_path,
                        "error": str(e)
                    }
            
            # 验证基础评分权重
            required_score_weights = ['liquidity', 'risk', 'return', 'sentiment', 'fundamental']
            for key in required_score_weights:
                if key not in Config.SCORE_WEIGHTS:
                    results.setdefault("score_weights", []).append(f"缺少必要键: {key}")
            
            score_sum = sum(Config.SCORE_WEIGHTS.values())
            if abs(score_sum - 1.0) > 0.001:
                results["score_weights"] = results.get("score_weights", []) + [f"总和应为1.0，实际为{score_sum:.4f}"]
            
            # 验证套利评分权重
            required_arbitrage_weights = [
                'premium_discount', 'liquidity', 'risk', 'return', 
                'market_sentiment', 'fundamental', 'component_stability'
            ]
            for key in required_arbitrage_weights:
                if key not in Config.ARBITRAGE_SCORE_WEIGHTS:
                    results.setdefault("arbitrage_weights", []).append(f"缺少必要键: {key}")
            
            arbitrage_sum = sum(Config.ARBITRAGE_SCORE_WEIGHTS.values())
            if abs(arbitrage_sum - 1.0) > 0.001:
                results["arbitrage_weights"] = results.get("arbitrage_weights", []) + [f"总和应为1.0，实际为{arbitrage_sum:.4f}"]
            
            # 检查微信配置
            results["wechat"] = {
                "status": "OK" if Config.WECOM_WEBHOOK else "WARNING",
                "webhook_configured": bool(Config.WECOM_WEBHOOK)
            }
            
            # 检查折价/溢价阈值配置
            results["thresholds"] = {
                "status": "OK" if Config.DISCOUNT_THRESHOLD > 0 and Config.PREMIUM_THRESHOLD > 0 else "WARNING",
                "discount_threshold": Config.DISCOUNT_THRESHOLD,
                "premium_threshold": Config.PREMIUM_THRESHOLD
            }
            
            # 检查是否存在未使用的配置项
            unused_keys = []
            if 'premium' in Config.SCORE_WEIGHTS:
                unused_keys.append("'premium' (基础评分中不应包含溢价率)")
            
            if unused_keys:
                results["unused_keys"] = {
                    "status": "WARNING",
                    "keys": unused_keys,
                    "message": "以下配置项未被使用"
                }
            
            return results
        except Exception as e:
            logging.error(f"配置验证失败: {str(e)}", exc_info=True)
            return {
                "error": {
                    "status": "ERROR",
                    "message": str(e)
                }
            }

    # -------------------------
    # 9. 路径初始化方法
    # -------------------------
    @staticmethod
    def init_dirs() -> bool:
        """
        初始化所有必要目录
        :return: 是否成功初始化所有目录
        """
        try:
            # 确保数据目录存在
            dirs_to_create = [
                Config.DATA_DIR,
                Config.ETFS_DAILY_DIR,
                Config.FLAG_DIR,
                Config.LOG_DIR,
                os.path.dirname(Config.TRADE_RECORD_FILE),
                os.path.dirname(Config.ALL_ETFS_PATH),
                os.path.dirname(Config.BACKUP_ETFS_PATH)
            ]
            
            for dir_path in dirs_to_create:
                if dir_path and not os.path.exists(dir_path):
                    os.makedirs(dir_path, exist_ok=True)
                    logging.info(f"创建目录: {dir_path}")
            
            # 初始化日志
            Config.setup_logging(log_file=Config.LOG_FILE)
            
            # 验证配置
            validation = Config.validate_config()
            has_errors = any(result["status"] == "ERROR" for result in validation.values())
            
            if has_errors:
                logging.warning("配置验证发现错误:")
                for key, result in validation.items():
                    if result["status"] == "ERROR":
                        logging.warning(f"  {key}: {result}")
            
            return not has_errors
            
        except Exception as e:
            logging.error(f"初始化目录失败: {str(e)}", exc_info=True)
            return False

# -------------------------
# 初始化配置
# -------------------------
try:
    # 首先尝试初始化基础目录
    base_dir = _get_base_dir()
    
    # 重新定义关键路径，确保它们基于正确的base_dir
    Config.BASE_DIR = base_dir
    Config.DATA_DIR = os.path.join(base_dir, "data")
    Config.ETFS_DAILY_DIR = os.path.join(Config.DATA_DIR, "etf_daily")
    Config.FLAG_DIR = os.path.join(Config.DATA_DIR, "flags")
    Config.LOG_DIR = os.path.join(base_dir, "logs")
    Config.LOG_FILE = os.path.join(Config.LOG_DIR, "etf_strategy.log")
    
    # 确保ARBITRAGE_STATUS_FILE路径正确
    Config.ARBITRAGE_STATUS_FILE = os.path.join(Config.FLAG_DIR, "arbitrage_status.json")
    Config.DISCOUNT_STATUS_FILE = os.path.join(Config.FLAG_DIR, "discount_status.json")
    Config.PREMIUM_STATUS_FILE = os.path.join(Config.FLAG_DIR, "premium_status.json")
    
    # 设置基础日志配置
    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format=Config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # 系统启动时立即验证配置
    Config.validate_config()
    
    # 初始化目录
    if Config.init_dirs():
        logging.info("配置初始化完成")
    else:
        logging.warning("配置初始化完成，但存在警告")
        
except Exception as e:
    # 创建一个临时的、基本的日志配置
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # 记录错误但继续执行
    logging.error(f"配置初始化失败: {str(e)}", exc_info=True)
    logging.info("已设置基础日志配置，继续执行")

# -------------------------
# 额外验证 - 确保关键配置项存在
# -------------------------
def _validate_critical_config():
    """验证关键配置项是否存在"""
    try:
        critical_configs = [
            "WECOM_WEBHOOK",
            "REQUEST_TIMEOUT",
            "BASE_DIR",
            "DATA_DIR",
            "ETFS_DAILY_DIR",
            "LOG_DIR",
            "LOG_FILE",
            "ALL_ETFS_PATH",
            "BACKUP_ETFS_PATH",
            "UTC_TIMEZONE",  # 新增验证项
            "BEIJING_TIMEZONE",  # 新增验证项
            "STANDARD_COLUMNS",  # 新增验证项
            "MIN_ARBITRAGE_DISPLAY_THRESHOLD",  # 新增验证项
            "ARBITRAGE_STATUS_FILE",  # 新增验证项
            "DISCOUNT_STATUS_FILE",  # 新增验证项
            "PREMIUM_STATUS_FILE",  # 新增验证项
            "ARBITRAGE_SCORE_WEIGHTS",  # 新增验证项
            "DISCOUNT_THRESHOLD",  # 新增验证项
            "PREMIUM_THRESHOLD",  # 新增验证项
            "ARBITRAGE_SCORE_THRESHOLD",  # 新增验证项
            "MIN_DISCOUNT_DISPLAY_THRESHOLD",  # 新增验证项
            "MIN_PREMIUM_DISPLAY_THRESHOLD",  # 新增验证项
            "MIN_FUND_SIZE",  # 新增验证项
            "MIN_AVG_VOLUME",  # 新增验证项
            "SCORE_WEIGHTS",  # 新增验证项
            "INDUSTRY_AVG_DISCOUNT"  # 新增验证项
        ]
        
        for config_name in critical_configs:
            if not hasattr(Config, config_name):
                logging.error(f"关键配置项缺失: {config_name}")
                # 尝试修复
                if config_name == "WECOM_WEBHOOK":
                    setattr(Config, "WECOM_WEBHOOK", "")
                    logging.warning("已添加缺失的WECOM_WEBHOOK配置项")
                elif config_name == "REQUEST_TIMEOUT":
                    setattr(Config, "REQUEST_TIMEOUT", 30)
                    logging.warning("已添加缺失的REQUEST_TIMEOUT配置项")
                elif config_name == "ETFS_DAILY_DIR":
                    setattr(Config, "ETFS_DAILY_DIR", os.path.join(Config.DATA_DIR, "etf_daily"))
      
