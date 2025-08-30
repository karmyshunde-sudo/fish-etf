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
from datetime import datetime, timezone, timedelta

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
    # 1. 数据源配置
    # -------------------------
    # 初次爬取默认时间范围（1年）
    INITIAL_CRAWL_DAYS: int = 365

    # ETF列表更新间隔（天）
    ETF_LIST_UPDATE_INTERVAL: int = 7  
    # 每7天更新一次ETF列表
    
    # 中文列名映射（固化，所有数据源统一）
    STANDARD_COLUMNS: Dict[str, str] = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "涨跌幅": "pct_change",
        "涨跌额": "price_change",
        "换手率": "turnover",
        "ETF代码": "etf_code",
        "ETF名称": "etf_name",
        "爬取时间": "crawl_time"
    }
    
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
    SCORE_WEIGHTS: Dict[str, float] = {
        'liquidity': 0.20,  # 流动性评分权重
        'risk': 0.25,       # 风险控制评分权重
        'return': 0.25,     # 收益能力评分权重
        'premium': 0.15,    # 溢价率评分权重
        'sentiment': 0.15   # 情绪指标评分权重
    }
    
    # 买入信号条件
    BUY_SIGNAL_DAYS: int = 2  # 连续几天信号持续才买入
    
    # 换股条件
    SWITCH_THRESHOLD: float = 0.3  # 新ETF比原ETF综合评分高出30%则换股

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
    
    # 套利结果标记文件
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
        "🌍 UTC时间：{utc_time}\n"
        "⏰ 北京时间：{beijing_time}"
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
            
            # 检查权重配置是否合理
            total_weight = sum(Config.SCORE_WEIGHTS.values())
            results["weights"] = {
                "status": "OK" if abs(total_weight - 1.0) < 0.001 else "WARNING",
                "total": total_weight,
                "expected": 1.0
            }

            # 检查微信配置
            results["wechat"] = {
                "status": "OK" if Config.WECOM_WEBHOOK else "WARNING",
                "webhook_configured": bool(Config.WECOM_WEBHOOK)
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
    
    # 设置基础日志配置
    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format=Config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
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
            "BACKUP_ETFS_PATH"
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
                    logging.warning("已添加缺失的ETFS_DAILY_DIR配置项")
    except Exception as e:
        logging.error(f"配置验证过程中发生错误: {str(e)}", exc_info=True)

# 执行额外验证
try:
    _validate_critical_config()
except Exception as e:
    logging.error(f"配置验证过程中发生错误: {str(e)}", exc_info=True)

# -------------------------
# 检查环境变量
# -------------------------
try:
    wecom_webhook = os.getenv("WECOM_WEBHOOK")
    if wecom_webhook:
        logging.info("检测到WECOM_WEBHOOK环境变量已设置")
    else:
        logging.warning("WECOM_WEBHOOK环境变量未设置，微信推送可能无法工作")
        
    # 确保Config中的WECOM_WEBHOOK与环境变量一致
    Config.WECOM_WEBHOOK = wecom_webhook or ""
except Exception as e:
    logging.error(f"检查环境变量时出错: {str(e)}", exc_info=True)

# -------------------------
# 时区检查
# -------------------------
try:
    # 尝试获取当前北京时间
    from utils.date_utils import get_beijing_time, get_utc_time
    beijing_time = get_beijing_time()
    utc_time = get_utc_time()
    
    logging.info(f"当前北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"当前UTC时间: {utc_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 验证时区设置
    if beijing_time.tzinfo is None or utc_time.tzinfo is None:
        logging.warning("时区信息不完整，可能存在时区问题")
    else:
        logging.info(f"北京时间时区: {beijing_time.tzname()}")
        logging.info(f"UTC时间时区: {utc_time.tzname()}")
        
        # 验证时差是否正确（北京时间应比UTC时间早8小时）
        time_diff = beijing_time - utc_time
        if abs(time_diff.total_seconds() - 28800) > 60:  # 8小时=28800秒，允许1分钟误差
            logging.warning(f"时区偏移不正确: 北京时间与UTC时间相差 {time_diff.total_seconds()/3600:.2f} 小时")
        else:
            logging.info("时区设置验证通过")
            
except ImportError:
    logging.warning("无法导入date_utils模块，时区检查跳过")
except Exception as e:
    logging.error(f"时区检查失败: {str(e)}", exc_info=True)
