import os

class Config:
    """全局配置：数据源、策略参数、文件路径"""
    # -------------------------
    # 1. 数据源配置
    # -------------------------
    # 初次爬取默认时间范围（1年）
    INITIAL_CRAWL_DAYS = 365
    # 中文列名映射（固化，所有数据源统一）
    STANDARD_COLUMNS = {
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
    ETF_STANDARD_COLUMNS = ["ETF代码", "ETF名称", "完整代码", "基金规模"]
    # 新浪数据源备用接口
    SINA_ETF_HIST_URL = "https://finance.sina.com.cn/realstock/company/{etf_code}/hisdata/klc_kl.js"
    # 批量爬取批次大小
    CRAWL_BATCH_SIZE = 50  # 每批50只ETF

    # -------------------------
    # 2. 策略参数配置
    # -------------------------
    # 套利策略：交易成本（印花税0.1%+佣金0.02%）
    TRADE_COST_RATE = 0.0012  # 0.12%
    # 套利阈值（收益率超过该值才推送）
    ARBITRAGE_PROFIT_THRESHOLD = 0.005  # 0.5%
    # 综合评分筛选阈值（仅保留评分前N%的ETF）
    SCORE_TOP_PERCENT = 20  # 保留前20%高分ETF
    # 最低规模阈值（亿元）
    MIN_ETP_SIZE = 10  # 规模≥10亿
    # 最低日均成交额阈值（万元）
    MIN_DAILY_VOLUME = 5000  # 日均成交额≥5000万
    
    # 仓位策略参数（均线策略）
    MA_SHORT_PERIOD = 5    # 短期均线（5日）
    MA_LONG_PERIOD = 20    # 长期均线（20日）
    ADD_POSITION_THRESHOLD = 0.03  # 加仓阈值（涨幅超3%）
    STOP_LOSS_THRESHOLD = -0.05    # 止损阈值（跌幅超5%")
    
    # 评分维度权重
    SCORE_WEIGHTS = {
        'liquidity': 0.20,  # 流动性评分权重
        'risk': 0.25,       # 风险控制评分权重
        'return': 0.25,     # 收益能力评分权重
        'premium': 0.15,    # 溢价率评分权重
        'sentiment': 0.15   # 情绪指标评分权重
    }
    
    # 买入信号条件
    BUY_SIGNAL_DAYS = 2  # 连续几天信号持续才买入
    
    # 换股条件
    SWITCH_THRESHOLD = 0.3  # 新ETF比原ETF综合评分高出30%则换股

    # -------------------------
    # 3. 文件路径配置 - 基于仓库根目录的路径
    # -------------------------
    # 获取仓库根目录（优先使用GITHUB_WORKSPACE环境变量）
    GITHUB_WORKSPACE = os.environ.get('GITHUB_WORKSPACE', 
                                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    BASE_DIR = os.path.abspath(GITHUB_WORKSPACE)
    
    # 数据存储路径
    DATA_DIR = os.path.join(BASE_DIR, "data/etf_daily")
    # ETF元数据（记录最后爬取日期）
    METADATA_PATH = os.path.join(BASE_DIR, "data/etf_metadata.csv")
    # 策略结果标记（避免单日重复推送）
    FLAG_DIR = os.path.join(BASE_DIR, "data/flags")
    # 套利结果标记文件
    ARBITRAGE_FLAG_FILE = os.path.join(FLAG_DIR, "arbitrage_pushed_{date}.txt")
    # 仓位策略结果标记文件
    POSITION_FLAG_FILE = os.path.join(FLAG_DIR, "position_pushed_{date}.txt")
    # 交易记录文件
    TRADE_RECORD_FILE = os.path.join(BASE_DIR, "data/trade_records.csv")
    # 全市场ETF列表存储路径
    ALL_ETFS_PATH = os.path.join(BASE_DIR, "data/all_etfs.csv")
    # 兜底ETF列表路径
    BACKUP_ETFS_PATH = os.path.join(BASE_DIR, "data/karmy_etf.csv")

    # -------------------------
    # 4. 微信推送配置
    # -------------------------
    # 企业微信机器人Webhook（从GitHub Secrets获取）
    WECOM_WEBHOOK = ""
    # 消息末尾固定内容
    WECOM_MESFOOTER = "【消息由fish-etf策略系统发送】"

    # -------------------------
    # 5. 日志配置
    # -------------------------
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # -------------------------
    # 6. ETF筛选配置
    # -------------------------
    # ETF筛选参数
    MIN_FUND_SIZE = 5.0  # 最小基金规模（亿元）
    MIN_AVG_VOLUME = 1000  # 最小日均成交量（万股）

    # -------------------------
    # 路径初始化方法
    # -------------------------
    @classmethod
    def init_dirs(cls):
        """初始化所有必要目录"""
        import os
        # 确保数据目录存在
        if not os.path.exists(cls.DATA_DIR):
            os.makedirs(cls.DATA_DIR, exist_ok=True)
        # 确保标记目录存在
        if not os.path.exists(cls.FLAG_DIR):
            os.makedirs(cls.FLAG_DIR, exist_ok=True)
        # 确保根数据目录存在（用于存放all_etfs.csv等）
        root_data_dir = os.path.dirname(cls.ALL_ETFS_PATH)
        if not os.path.exists(root_data_dir):
            os.makedirs(root_data_dir, exist_ok=True)
        # 确保交易记录目录存在
        trade_record_dir = os.path.dirname(cls.TRADE_RECORD_FILE)
        if not os.path.exists(trade_record_dir):
            os.makedirs(trade_record_dir, exist_ok=True)
