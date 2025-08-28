# config.py
import os
import logging
from typing import Dict, Any, Optional

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
    SINA_ETF_HIST_URL: str = "https://finance.sina.com.cn/realstock/company/{etf_code}/hisdata/klc_kl.js"
    
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
    @classmethod
    def get_base_dir(cls) -> str:
        """获取项目根目录路径"""
        try:
            base_dir = os.environ.get('GITHUB_WORKSPACE')
            if not base_dir:
                # 默认基于当前文件位置计算项目根目录
                current_file_path = os.path.abspath(__file__)
                base_dir = os.path.dirname(os.path.dirname(current_file_path))
            return os.path.abspath(base_dir)
        except Exception as e:
            logging.error(f"获取项目根目录失败: {str(e)}")
            # 退回到当前工作目录
            return os.path.abspath(os.getcwd())
    
    BASE_DIR: str = get_base_dir.__func__()
    
    # 数据存储路径
    DATA_DIR: str = os.path.join(BASE_DIR, "data", "etf_daily")
    
    # ETF元数据（记录最后爬取日期）
    METADATA_PATH: str = os.path.join(BASE_DIR, "data", "etf_metadata.csv")
    
    # 策略结果标记（避免单日重复推送）
    FLAG_DIR: str = os.path.join(BASE_DIR, "data", "flags")
    
    # 套利结果标记文件
    @classmethod
    def get_arbitrage_flag_file(cls, date_str: Optional[str] = None) -> str:
        """获取套利标记文件路径"""
        from datetime import datetime
        date = date_str or datetime.now().strftime("%Y-%m-%d")
        return os.path.join(cls.FLAG_DIR, f"arbitrage_pushed_{date}.txt")
    
    # 仓位策略结果标记文件
    @classmethod
    def get_position_flag_file(cls, date_str: Optional[str] = None) -> str:
        """获取仓位标记文件路径"""
        from datetime import datetime
        date = date_str or datetime.now().strftime("%Y-%m-%d")
        return os.path.join(cls.FLAG_DIR, f"position_pushed_{date}.txt")
    
    # 交易记录文件
    TRADE_RECORD_FILE: str = os.path.join(BASE_DIR, "data", "trade_records.csv")
    
    # 全市场ETF列表存储路径
    ALL_ETFS_PATH: str = os.path.join(BASE_DIR, "data", "all_etfs.csv")
    
    # 兜底ETF列表路径
    BACKUP_ETFS_PATH: str = os.path.join(BASE_DIR, "data", "karmy_etf.csv")

    # -------------------------
    # 4. 微信推送配置
    # -------------------------
    @classmethod
    def get_wecom_webhook(cls) -> str:
        """获取企业微信机器人Webhook（从环境变量或配置获取）"""
        try:
            # 优先从环境变量获取
            webhook = os.environ.get("WECOM_WEBHOOK")
            if webhook:
                return webhook
                
            # 其次从类属性获取
            if hasattr(cls, '_WECOM_WEBHOOK') and cls._WECOM_WEBHOOK:
                return cls._WECOM_WEBHOOK
                
            # 最后尝试从配置文件读取（如果有）
            config_file = os.path.join(cls.BASE_DIR, "config.ini")
            if os.path.exists(config_file):
                import configparser
                parser = configparser.ConfigParser()
                parser.read(config_file)
                if 'wechat' in parser and 'webhook' in parser['wechat']:
                    webhook = parser['wechat']['webhook']
                    cls._WECOM_WEBHOOK = webhook
                    return webhook
                    
            logging.warning("企业微信Webhook未配置")
            return ""
        except Exception as e:
            logging.error(f"获取企业微信Webhook配置失败: {str(e)}")
            return ""
    
    # 消息末尾固定内容
    WECOM_MESFOOTER: str = "【消息由GIT-fish-etf发送】"

    # -------------------------
    # 5. 日志配置
    # -------------------------
    @classmethod
    def setup_logging(cls, 
                     log_level: Optional[str] = None,
                     log_file: Optional[str] = None) -> None:
        """
        配置日志系统
        :param log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        :param log_file: 日志文件路径，如果为None则只输出到控制台
        """
        level = log_level or cls.LOG_LEVEL
        log_format = cls.LOG_FORMAT
        
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
                logging.error(f"配置日志文件失败: {str(e)}")
    
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_FILE: str = os.path.join(BASE_DIR, "logs", "etf_strategy.log")

    # -------------------------
    # 6. ETF筛选配置
    # -------------------------
    # ETF筛选参数
    MIN_FUND_SIZE: float = 5.0  # 最小基金规模（亿元）
    MIN_AVG_VOLUME: float = 1000.0  # 最小日均成交量（万股）

    # -------------------------
    # 7. 配置验证方法
    # -------------------------
    @classmethod
    def validate_config(cls) -> Dict[str, Any]:
        """
        验证配置是否有效，返回验证结果
        :return: 包含验证结果的字典
        """
        results = {}
        
        # 检查必要的目录是否存在或可创建
        required_dirs = [cls.DATA_DIR, cls.FLAG_DIR, os.path.dirname(cls.LOG_FILE)]
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
        total_weight = sum(cls.SCORE_WEIGHTS.values())
        results["weights"] = {
            "status": "OK" if abs(total_weight - 1.0) < 0.001 else "WARNING",
            "total": total_weight,
            "expected": 1.0
        }
        
        # 检查微信配置
        webhook = cls.get_wecom_webhook()
        results["wechat"] = {
            "status": "OK" if webhook else "WARNING",
            "webhook_configured": bool(webhook)
        }
        
        return results

    # -------------------------
    # 路径初始化方法
    # -------------------------
    @classmethod
    def init_dirs(cls) -> bool:
        """
        初始化所有必要目录
        :return: 是否成功初始化所有目录
        """
        try:
            # 确保数据目录存在
            dirs_to_create = [
                cls.DATA_DIR,
                cls.FLAG_DIR,
                os.path.dirname(cls.TRADE_RECORD_FILE),
                os.path.dirname(cls.ALL_ETFS_PATH),
                os.path.dirname(cls.BACKUP_ETFS_PATH),
                os.path.dirname(cls.LOG_FILE)
            ]
            
            for dir_path in dirs_to_create:
                if dir_path and not os.path.exists(dir_path):
                    os.makedirs(dir_path, exist_ok=True)
                    logging.info(f"创建目录: {dir_path}")
            
            # 初始化日志
            cls.setup_logging(log_file=cls.LOG_FILE)
            
            # 验证配置
            validation = cls.validate_config()
            has_errors = any(result["status"] == "ERROR" for result in validation.values())
            
            if has_errors:
                logging.warning("配置验证发现错误:")
                for key, result in validation.items():
                    if result["status"] == "ERROR":
                        logging.warning(f"  {key}: {result}")
            
            return not has_errors
            
        except Exception as e:
            logging.error(f"初始化目录失败: {str(e)}")
            return False

# 初始化配置
try:
    Config.init_dirs()
    logging.info("配置初始化完成")
except Exception as e:
    print(f"配置初始化失败: {str(e)}")
    # 退回到基础日志配置
    logging.basicConfig(level=Config.LOG_LEVEL, format=Config.LOG_FORMAT)
    logging.error(f"配置初始化失败: {str(e)}")
# 0828-1256【config.py代码】一共265行代码
