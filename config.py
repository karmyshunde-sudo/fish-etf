class Config:
    """全局配置：数据源、策略参数、文件路径（完整保留原有配置）"""
    
    # -------------------------
    # 1. 数据源配置
    # -------------------------
    INITIAL_CRAWL_DAYS = 365  # 初次爬取默认时间范围（1年）
    STANDARD_COLUMNS = {      # 中文列名映射（固化，所有数据源统一）
        "日期": "date",
        "开盘价": "open",
        "收盘价": "close",
        "最高价": "high",
        "最低价": "low",
        "成交量": "volume",
        "成交额": "amount",
        "涨跌幅": "pct_change"
    }
    SINA_ETF_HIST_URL = "https://finance.sina.com.cn/realstock/company/{etf_code}/hisdata/klc_kl.js"  # 新浪数据源备用接口
    CRAWL_BATCH_SIZE = 50    # 批量爬取批次大小（每批50只ETF）

    # -------------------------
    # 2. 策略参数配置
    # -------------------------
    TRADE_COST_RATE = 0.0012       # 套利策略：交易成本（印花税0.1%+佣金0.02% → 0.12%）
    ARBITRAGE_PROFIT_THRESHOLD = 0.005  # 套利阈值（收益率超过0.5%才推送）
    SCORE_TOP_PERCENT = 20         # 综合评分筛选阈值（仅保留评分前20%的ETF）
    MIN_ETP_SIZE = 10              # 最低规模阈值（亿元，规模≥10亿）
    MIN_DAILY_VOLUME = 5000        # 最低日均成交额阈值（万元，日均≥5000万）
    
    # 仓位策略参数（均线策略）
    MA_SHORT_PERIOD = 5      # 短期均线（5日）
    MA_LONG_PERIOD = 20      # 长期均线（20日）
    ADD_POSITION_THRESHOLD = 0.03  # 加仓阈值（涨幅超3%）
    STOP_LOSS_THRESHOLD = -0.05    # 止损阈值（跌幅超5%）

    # -------------------------
    # 3. 文件路径配置
    # -------------------------
    DATA_DIR = "data/etf_daily"        # 数据存储路径
    METADATA_PATH = "data/etf_metadata.csv"  # ETF元数据（记录最后爬取日期）
    FLAG_DIR = "data/flags"            # 策略结果标记（避免单日重复推送）
    ARBITRAGE_FLAG_FILE = f"{FLAG_DIR}/arbitrage_pushed_{{date}}.txt"  # 套利结果标记文件
    POSITION_FLAG_FILE = f"{FLAG_DIR}/position_pushed_{{date}}.txt"    # 仓位策略结果标记文件
    ALL_ETFS_PATH = "data/all_etfs.csv"       # 全市场ETF列表存储路径
    BACKUP_ETFS_PATH = "data/karmy_etf.csv"   # 兜底ETF列表路径

    # -------------------------
    # 4. 微信推送配置
    # -------------------------
    WECOM_WEBHOOK = ""  # 企业微信机器人Webhook（从GitHub Secrets获取）
    WECOM_MESFOOTER = "【消息由fish-etf策略系统发送】"  # 消息末尾固定内容

    # -------------------------
    # 5. 日志配置
    # -------------------------
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # -------------------------
    # 新增：路径初始化（确保目录存在）
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
