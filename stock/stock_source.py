# stock/stock_source.py
import logging
import baostock as bs
from datetime import datetime

# ===== 配置 =====
# 硬编码测试股票代码 (必须包含前缀)
TEST_STOCK_CODE = "sh.600000"  # 浦发银行

# ===== 初始化 =====
logger = logging.getLogger("StockCrawler")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_stock_daily_data_from_sources():
    """
    严格遵循Baostock官方示例的调试函数
    """
    logger.info("=" * 80)
    logger.info("开始测试Baostock数据源连接 - 严格遵循官方示例")
    logger.info("=" * 80)
    
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
            return None
        else:
            logger.info("Baostock登录成功！")
            logger.info(f"会话ID: {login_result.session_id}")
        
        # ===== 2. 官方示例调用 =====
        logger.info("执行官方示例查询...")
        
        # 完全按照官方示例格式
        rs = bs.query_history_k_data_plus(
            "sh.600000",
            "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
            start_date='2024-07-01',
            end_date='2024-12-31',
            frequency="d",
            adjustflag="3"
        )
        
        # 记录查询结果的详细信息
        logger.debug(f"查询结果类型: {type(rs)}")
        
        # 记录错误信息
        if rs is None:
            logger.error("查询返回None，请检查网络连接或Baostock服务状态")
            return None
        
        logger.info('query_history_k_data_plus respond error_code:'+rs.error_code)
        logger.info('query_history_k_data_plus respond  error_msg:'+rs.error_msg)
        
        if rs.error_code != '0':
            logger.error(f"Baostock查询失败: {rs.error_msg}")
            return None
        
        # 尝试获取数据
        logger.info("尝试获取数据行...")
        data_list = []
        while rs.next():
            row = rs.get_row_data()
            data_list.append(row)
            logger.debug(f"获取到数据行: {row}")
        
        logger.info(f"共获取到 {len(data_list)} 行数据")
        
        if data_list:
            # 创建DataFrame
            import pandas as pd
            df = pd.DataFrame(data_list, columns=rs.fields)
            logger.info("数据转换成功，前5行预览:")
            logger.info(f"{df.head().to_string()}")
            return df
        else:
            logger.warning("查询成功但无数据返回")
            return None
            
    except Exception as e:
        logger.exception(f"Baostock数据获取过程中发生意外错误: {str(e)}")
        return None
    
    finally:
        # ===== 3. 确保登出 =====
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

if __name__ == "__main__":
    # 直接运行此模块进行测试
    logger.info("======= 运行Baostock连接测试 =======")
    
    # 直接调用获取数据函数
    df = get_stock_daily_data_from_sources()
    
    # 检查结果
    if df is None:
        logger.error("测试结果: 未获取到任何数据")
    else:
        logger.info(f"测试结果: 成功获取 {len(df)} 条数据")
        logger.info("数据列: " + ", ".join(df.columns))
        logger.info(f"日期范围: {df['date'].min()} 到 {df['date'].max()}")
    
    logger.info("======= Baostock连接测试完成 =======")
