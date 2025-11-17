#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import akshare as ak
import baostock as bs
import yfinance as yf
import time
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 添加缺失的 generate_report 函数 - 简化版本只用于测试数据源
def generate_report():
    """主程序调用的入口函数 - 测试版本"""
    logger.info("===== 开始执行指数代码测试 =====")
    
    # 按顺序测试各个数据源的代码
    test_baostock_codes()
    test_yfinance_codes()
    test_akshare_codes()
    test_hk_index_akshare()
    find_working_codes()
    
    logger.info("=== 指数代码测试完成 ===")
    
    # 返回简单的结果以兼容主程序
    return {
        "status": "success",
        "task": "index_yesno",
        "message": "数据源测试完成",
        "timestamp": datetime.now().isoformat()
    }

def test_baostock_codes():
    """测试baostock中的指数代码"""
    print("=== 测试baostock指数代码 ===")
    
    # 登录baostock
    login_result = bs.login()
    if login_result.error_code != '0':
        print(f"baostock登录失败: {login_result.error_msg}")
        return
    
    test_codes = [
        "sh.000016", "sh.000300", "sh.000905", "sh.000852",  # 标准A股指数
        "sh.000688", "sh.883418", "sh.932000", "bj.899050",  # 需要测试的指数
        "sz.399006",  # 创业板
    ]
    
    for code in test_codes:
        try:
            print(f"测试代码: {code}")
            rs = bs.query_history_k_data_plus(
                code, "date,close", 
                start_date="2024-01-01", 
                end_date="2024-01-10"
            )
            if rs.error_code == '0':
                data_list = []
                while rs.next():
                    data_list.append(rs.get_row_data())
                if data_list:
                    print(f"✅ {code}: 成功获取 {len(data_list)} 条数据")
                else:
                    print(f"❌ {code}: 数据为空")
            else:
                print(f"❌ {code}: 错误 - {rs.error_msg}")
        except Exception as e:
            print(f"❌ {code}: 异常 - {str(e)}")
        time.sleep(1)
    
    bs.logout()

def test_yfinance_codes():
    """测试yfinance中的指数代码"""
    print("\n=== 测试yfinance指数代码 ===")
    
    test_codes = [
        "^HSTECH", "HSTECH", "HSTECH.HK", "3077.HK",  # 恒生科技指数
        "HSCEI.HK", "HSCE.HK", "2828.HK",  # 恒生国企指数
        "^HSI", "HSI", "HSI.HK",  # 恒生指数
        "GC=F", "GLD",  # 黄金
        "^NDX", "QQQ",  # 纳斯达克
        "KWEB",  # 中概互联
    ]
    
    for code in test_codes:
        try:
            print(f"测试代码: {code}")
            df = yf.download(code, start="2024-01-01", end="2024-01-10", auto_adjust=False)
            if not df.empty:
                print(f"✅ {code}: 成功获取 {len(df)} 条数据")
                print(f"   列名: {list(df.columns)}")
            else:
                print(f"❌ {code}: 数据为空")
        except Exception as e:
            print(f"❌ {code}: 异常 - {str(e)}")
        time.sleep(1)

def test_akshare_codes():
    """测试akshare中的指数代码"""
    print("\n=== 测试akshare指数代码 ===")
    
    test_codes = [
        "000688", "399006", "000016", "000300",  # A股主要指数
        "000905", "000852", "883418", "932000",  # 其他A股指数
        "899050",  # 北证50
    ]
    
    for code in test_codes:
        try:
            print(f"测试代码: {code}")
            # 尝试不同的akshare接口
            try:
                df = ak.index_zh_a_hist(symbol=code, period="daily", start_date="20240101", end_date="20240110")
                if not df.empty:
                    print(f"✅ {code}: 成功获取 {len(df)} 条数据 (index_zh_a_hist)")
                    continue
            except Exception as e1:
                pass
            
            # 尝试其他接口
            try:
                df = ak.stock_zh_index_daily(symbol=f"sh{code}" if code.startswith(('00', '88', '93')) else f"sz{code}")
                if not df.empty:
                    print(f"✅ {code}: 成功获取 {len(df)} 条数据 (stock_zh_index_daily)")
                    continue
            except Exception as e2:
                pass
            
            print(f"❌ {code}: 所有接口都失败")
            
        except Exception as e:
            print(f"❌ {code}: 异常 - {str(e)}")
        time.sleep(1)

def test_hk_index_akshare():
    """测试akshare中的港股指数"""
    print("\n=== 测试akshare港股指数 ===")
    
    # 测试akshare的港股指数接口
    try:
        # 获取港股指数列表
        hk_index_list = ak.index_hk_spot()
        print("港股指数列表:")
        print(hk_index_list[['代码', '名称']].head(10))
    except Exception as e:
        print(f"获取港股指数列表失败: {str(e)}")
    
    # 测试具体港股指数
    hk_codes = ["HSTECH", "HSCEI", "HSI"]
    for code in hk_codes:
        try:
            print(f"测试港股指数: {code}")
            df = ak.index_hk_daily(symbol=code)
            if not df.empty:
                print(f"✅ {code}: 成功获取 {len(df)} 条数据")
            else:
                print(f"❌ {code}: 数据为空")
        except Exception as e:
            print(f"❌ {code}: 异常 - {str(e)}")
        time.sleep(1)

def find_working_codes():
    """找出实际可用的代码格式"""
    print("\n=== 寻找实际可用的代码格式 ===")
    
    # 测试组合
    test_combinations = {
        "恒生科技指数": [
            {"source": "yfinance", "code": "HSTECH.HK"},
            {"source": "yfinance", "code": "3077.HK"},  # 恒生科技指数ETF
            {"source": "yfinance", "code": "3033.HK"},  # 南方恒生科技ETF
            {"source": "akshare", "code": "HSTECH"},
        ],
        "恒生国企指数": [
            {"source": "yfinance", "code": "HSCEI.HK"},
            {"source": "yfinance", "code": "2828.HK"},  # 恒生国企指数ETF
            {"source": "akshare", "code": "HSCEI"},
        ],
        "科创50": [
            {"source": "baostock", "code": "sh.000688"},
            {"source": "akshare", "code": "000688"},
        ],
        "微盘股": [
            {"source": "baostock", "code": "sh.883418"},
            {"source": "akshare", "code": "883418"},
        ],
        "北证50": [
            {"source": "baostock", "code": "bj.899050"},
            {"source": "akshare", "code": "899050"},
        ],
        "中证2000": [
            {"source": "baostock", "code": "sh.932000"},
            {"source": "akshare", "code": "932000"},
        ]
    }
    
    for index_name, combinations in test_combinations.items():
        print(f"\n--- {index_name} ---")
        for combo in combinations:
            source = combo["source"]
            code = combo["code"]
            try:
                if source == "yfinance":
                    df = yf.download(code, start="2024-01-01", end="2024-01-10", auto_adjust=False)
                elif source == "baostock":
                    login_result = bs.login()
                    if login_result.error_code == '0':
                        rs = bs.query_history_k_data_plus(code, "date,close", start_date="2024-01-01", end_date="2024-01-10")
                        if rs.error_code == '0':
                            data_list = []
                            while rs.next():
                                data_list.append(rs.get_row_data())
                            df = pd.DataFrame(data_list) if data_list else pd.DataFrame()
                        else:
                            df = pd.DataFrame()
                        bs.logout()
                    else:
                        df = pd.DataFrame()
                elif source == "akshare":
                    if code.isdigit():  # A股指数
                        df = ak.index_zh_a_hist(symbol=code, period="daily", start_date="20240101", end_date="20240110")
                    else:  # 港股指数
                        df = ak.index_hk_daily(symbol=code)
                
                if not df.empty and len(df) > 0:
                    print(f"✅ {source}: {code} - 成功 ({len(df)}条数据)")
                else:
                    print(f"❌ {source}: {code} - 无数据")
            except Exception as e:
                print(f"❌ {source}: {code} - 错误: {str(e)}")
            time.sleep(1)

if __name__ == "__main__":
    # 直接运行测试
    print("开始测试各个数据源的指数代码...")
    test_baostock_codes()
    test_yfinance_codes()
    test_akshare_codes()
    test_hk_index_akshare()
    find_working_codes()
    print("\n=== 测试完成 ===")
