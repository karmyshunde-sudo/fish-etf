# stock/stock_source.py
import logging
import pandas as pd
import baostock as bs
from datetime import datetime, timedelta

# ===== 配置 =====
# 硬编码测试股票代码 (带正确前缀)
TEST_STOCK_CODE = "sh.600000"  # 浦发银行 - 官方示例要求前缀

# ===== 初始化 =====
logger = logging.getLogger("StockCrawler")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_stock_daily_data_from_sources(stock_code: str = TEST_STOCK_CODE, 
                                    start_date: datetime = None, 
                                    end_date: datetime = None,
                                    existing_data: pd.DataFrame = None) -> pd.DataFrame:
    """
    仅使用Baostock获取股票日线数据的调试函数
    完全遵循Baostock官方示例格式
    
    Args:
        stock_code: 股票代码（必须包含前缀，如"sh.600000"）
        start_date: 数据起始日期（默认为一年前）
        end_date: 数据结束日期（默认为今天）
        existing_data: 已有数据（此函数不使用）
    
    Returns:
        pd.DataFrame: Baostock返回的原始数据
    """
    logger.info("=" * 80)
    logger.info("开始测试Baostock数据源连接 - 严格遵循官方示例格式")
    logger.info("=" * 80)
    
    # 设置默认日期范围 - 严格使用"YYYY-MM-DD"格式
    if start_date is None:
        start_date = datetime.now() - timedelta(days=30)
    if end_date is None:
        end_date = datetime.now()
    
    # 日期格式化为"YYYY-MM-DD"
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # 日志记录关键信息
    logger.info(f"测试股票代码: {stock_code}")
    logger.info(f"日期范围: {start_date_str} 到 {end_date_str}")
    logger.info(f"Baostock版本: {bs.__version__}")
    
    try:
        # ===== 1. 登录Baostock =====
        logger.info("尝试登录Baostock...")
        login_result = bs.login()
        
        # 记录登录结果
        logger.debug(f"登录返回类型: {type(login_result)}")
        logger.debug(f"登录返回内容: {login_result}")
        
        if login_result.error_code != '0':
            logger.error(f"Baostock登录失败: 错误码 {login_result.error_code}, 错误信息: {login_result.error_msg}")
            logger.error("登录失败，请检查网络连接或Baostock服务状态")
            return pd.DataFrame()
        else:
            logger.info("Baostock登录成功！")
            logger.info(f"会话ID: {login_result.session_id}")
        
        # ===== 2. 构造查询参数 =====
        # 转换为Baostock需要的格式
        bs_code = stock_code
        logger.info(f"转换后的Baostock代码: {bs_code}")
        
        # 详细日志记录查询参数
        logger.info("准备查询历史K线数据，参数详情:")
        logger.info(f"  - code: {bs_code}")
        logger.info(f"  - fields: date,code,open,high,low,close,volume,amount")
        logger.info(f"  - start_date: {start_date_str}")  # 严格使用"YYYY-MM-DD"格式
        logger.info(f"  - end_date: {end_date_str}")      # 严格使用"YYYY-MM-DD"格式
        logger.info(f"  - frequency: d")
        logger.info(f"  - adjustflag: 3 (不复权)")
        
        # ===== 3. 执行查询 =====
        logger.info("正在执行查询...")
        rs = bs.query_history_k_data_plus(
            code=bs_code,
            fields="date,code,open,high,low,close,volume,amount",
            start_date=start_date_str,  # 严格使用"YYYY-MM-DD"格式
            end_date=end_date_str,      # 严格使用"YYYY-MM-DD"格式
            frequency="d",
            adjustflag="3"
        )
        
        # 记录查询结果的详细信息
        logger.debug(f"查询结果类型: {type(rs)}")
        if rs is None:
            logger.error("查询返回None，请检查网络连接或Baostock服务状态")
            return pd.DataFrame()
        
        # 记录错误信息
        logger.info(f"查询状态 - error_code: {rs.error_code}")
        logger.info(f"查询状态 - error_msg: {rs.error_msg}")
        
        # ===== 4. 处理查询结果 =====
        if rs.error_code != '0':
            logger.error(f"Baostock查询失败: {rs.error_msg}")
            return pd.DataFrame()
        
        # 尝试获取数据
        logger.info("尝试获取数据行...")
        data_list = []
        while rs.next():
            row = rs.get_row_data()
            data_list.append(row)
            logger.debug(f"获取到数据行: {row}")
        
        logger.info(f"共获取到 {len(data_list)} 行数据")
        
        # 创建DataFrame
        if data_list:
            df = pd.DataFrame(data_list, columns=rs.fields)
            logger.info("数据转换成功，前5行预览:")
            logger.info(f"{df.head().to_string()}")
            return df
        else:
            logger.warning("查询成功但无数据返回")
            return pd.DataFrame()
            
    except Exception as e:
        logger.exception(f"Baostock数据获取过程中发生意外错误: {str(e)}")
        return pd.DataFrame()
    
    finally:
        # ===== 5. 确保登出 =====
        try:
            logger.info("尝试登出Baostock...")
            logout_result = bs.logout()
            if logout_result.error_code == '0':
                logger.info("Baostock登出成功")
            else:
                logger.error(f"Baostock登出失败: {logout_result.error_msg}")
        except Exception as logout_error:
            logger.error(f"登出时发生异常: {str(logout_error)}")
        
        logger.info("=" * 80)
        logger.info("Baostock数据源测试结束")
        logger.info("=" * 80)

def _fetch_baostock_data(symbol: str, start_date: str, end_date: str, data_days: int, **kwargs) -> pd.DataFrame:
    """封装Baostock的API调用（与get_stock_daily_data_from_sources功能相同）"""
    # 在这里，start_date和end_date已经是字符串
    # 将它们转换为datetime，然后重新格式化为"YYYY-MM-DD"
    start_date_obj = datetime.strptime(start_date, "%Y%m%d") if len(start_date) == 8 else datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y%m%d") if len(end_date) == 8 else datetime.strptime(end_date, "%Y-%m-%d")
    
    start_date_str = start_date_obj.strftime("%Y-%m-%d")
    end_date_str = end_date_obj.strftime("%Y-%m-%d")
    
    return get_stock_daily_data_from_sources(
        stock_code=symbol,
        start_date=datetime.strptime(start_date_str, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date_str, "%Y-%m-%d")
    )

if __name__ == "__main__":
    # 直接运行此模块进行测试
    logger.info("======= 运行Baostock连接测试 =======")
    
    # 测试获取最近30天数据
    test_end_date = datetime.now()
    test_start_date = test_end_date - timedelta(days=30)
    
    # 直接调用获取数据函数
    df = get_stock_daily_data_from_sources(
        stock_code=TEST_STOCK_CODE,
        start_date=test_start_date,
        end_date=test_end_date
    )
    
    # 检查结果
    if df.empty:
        logger.error("测试结果: 未获取到任何数据")
    else:
        logger.info(f"测试结果: 成功获取 {len(df)} 条数据")
        logger.info("数据列: " + ", ".join(df.columns))
        logger.info(f"日期范围: {df['date'].min()} 到 {df['date'].max()}")
    
    logger.info("======= Baostock连接测试完成 =======")
