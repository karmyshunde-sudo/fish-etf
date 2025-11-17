#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import akshare as ak
import baostock as bs
import yfinance as yf
import requests
import time
import logging
from datetime import datetime, timedelta
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_real_index_data(index_code, index_name, start_date="2024-01-01", end_date="2024-01-10"):
    """获取真实的指数数据（不使用ETF替代）"""
    
    print(f"\n=== 获取 {index_name}({index_code}) 真实指数数据 ===")
    
    results = {}
    
    # 方法1: 尝试akshare的A股指数接口
    try:
        print("尝试akshare A股指数接口...")
        # 使用正确的akshare接口
        df = ak.index_zh_a_hist(symbol=index_code, period="daily", 
                              start_date=start_date.replace("-", ""), 
                              end_date=end_date.replace("-", ""))
        if not df.empty:
            results["akshare"] = {
                "data": df,
                "source": f"index_zh_a_hist({index_code})",
                "data_points": len(df)
            }
            print(f"✅ akshare成功获取 {len(df)} 条真实指数数据")
            return results
    except Exception as e:
        print(f"akshare A股指数接口失败: {str(e)}")
    
    # 方法2: 尝试akshare的stock_zh_index_daily接口
    try:
        print("尝试akshare stock_zh_index_daily接口...")
        # 确定市场前缀
        if index_code.startswith(('00', '88', '93')):
            market_code = f"sh{index_code}"
        elif index_code.startswith('399'):
            market_code = f"sz{index_code}"
        elif index_code.startswith('899'):
            market_code = f"bj{index_code}"
        else:
            market_code = index_code
            
        df = ak.stock_zh_index_daily(symbol=market_code)
        if not df.empty:
            # 过滤日期范围
            df.index = pd.to_datetime(df.index)
            mask = (df.index >= start_date) & (df.index <= end_date)
            filtered_df = df[mask]
            if not filtered_df.empty:
                results["akshare_daily"] = {
                    "data": filtered_df,
                    "source": f"stock_zh_index_daily({market_code})",
                    "data_points": len(filtered_df)
                }
                print(f"✅ akshare_daily成功获取 {len(filtered_df)} 条真实指数数据")
                return results
    except Exception as e:
        print(f"akshare daily接口失败: {str(e)}")
    
    # 方法3: 尝试baostock
    try:
        print("尝试baostock...")
        lg = bs.login()
        if lg.error_code == '0':
            # 确定baostock代码格式
            if index_code.startswith(('00', '88', '93')):
                bs_code = f"sh.{index_code}"
            elif index_code.startswith('399'):
                bs_code = f"sz.{index_code}"
            elif index_code.startswith('899'):
                bs_code = f"bj.{index_code}"
            else:
                bs_code = index_code
                
            rs = bs.query_history_k_data_plus(
                bs_code, 
                "date,code,open,high,low,close,volume,amount,turn,pctChg", 
                start_date=start_date, 
                end_date=end_date
            )
            if rs.error_code == '0':
                data_list = []
                while rs.next():
                    data_list.append(rs.get_row_data())
                if data_list:
                    df = pd.DataFrame(data_list, columns=rs.fields)
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    # 转换数据类型
                    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctChg']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    results["baostock"] = {
                        "data": df,
                        "source": f"baostock({bs_code})",
                        "data_points": len(df)
                    }
                    print(f"✅ baostock成功获取 {len(df)} 条真实指数数据")
                    bs.logout()
                    return results
            bs.logout()
    except Exception as e:
        print(f"baostock失败: {str(e)}")
    
    # 方法4: 尝试腾讯财经实时接口（获取指数数据）
    try:
        print("尝试腾讯财经接口...")
        tencent_df = get_tencent_index_data(index_code, start_date, end_date)
        if tencent_df is not None and not tencent_df.empty:
            results["tencent"] = {
                "data": tencent_df,
                "source": f"tencent({index_code})",
                "data_points": len(tencent_df)
            }
            print(f"✅ 腾讯财经成功获取 {len(tencent_df)} 条真实指数数据")
            return results
    except Exception as e:
        print(f"腾讯财经失败: {str(e)}")
    
    # 方法5: 尝试新浪财经接口
    try:
        print("尝试新浪财经接口...")
        sina_df = get_sina_index_data(index_code, start_date, end_date)
        if sina_df is not None and not sina_df.empty:
            results["sina"] = {
                "data": sina_df,
                "source": f"sina({index_code})",
                "data_points": len(sina_df)
            }
            print(f"✅ 新浪财经成功获取 {len(sina_df)} 条真实指数数据")
            return results
    except Exception as e:
        print(f"新浪财经失败: {str(e)}")
    
    print(f"❌ {index_name} 所有真实指数数据源都失败")
    return results

def get_tencent_index_data(index_code, start_date, end_date):
    """从腾讯财经获取指数数据"""
    try:
        # 腾讯财经指数接口
        if index_code.startswith(('00', '88', '93')):
            tencent_code = f"sh{index_code}"
        elif index_code.startswith('399'):
            tencent_code = f"sz{index_code}"
        elif index_code.startswith('899'):
            tencent_code = f"bj{index_code}"
        else:
            tencent_code = index_code
            
        url = "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
        params = {
            'param': f'{tencent_code},day,{start_date.replace("-", "")},{end_date.replace("-", "")},500,qfq',
            '_var': 'kline_dayqfq',
            'r': '0.12345678901234567'
        }
        
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'data' in data and tencent_code in data['data']:
                kline_data = data['data'][tencent_code].get('day', [])
                if kline_data:
                    df = pd.DataFrame(kline_data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount'])
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    # 转换数值类型
                    numeric_cols = ['open', 'close', 'high', 'low', 'volume', 'amount']
                    for col in numeric_cols:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    return df
    except Exception as e:
        logger.warning(f"腾讯财经指数接口错误: {str(e)}")
    
    return None

def get_sina_index_data(index_code, start_date, end_date):
    """从新浪财经获取指数数据"""
    try:
        # 新浪财经指数接口
        if index_code.startswith(('00', '88', '93')):
            sina_code = f"sh{index_code}"
        elif index_code.startswith('399'):
            sina_code = f"sz{index_code}"
        elif index_code.startswith('899'):
            sina_code = f"bj{index_code}"
        else:
            sina_code = index_code
            
        url = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {
            'symbol': sina_code,
            'scale': '240',  # 日线
            'datalen': '100'  # 数据长度
        }
        
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['day'] = pd.to_datetime(df['day'])
                df.set_index('day', inplace=True)
                # 重命名列
                df.rename(columns={
                    'open': 'open',
                    'high': 'high', 
                    'low': 'low',
                    'close': 'close',
                    'volume': 'volume'
                }, inplace=True)
                # 转换数值类型
                numeric_cols = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # 过滤日期范围
                mask = (df.index >= start_date) & (df.index <= end_date)
                return df[mask]
    except Exception as e:
        logger.warning(f"新浪财经指数接口错误: {str(e)}")
    
    return None

def get_hk_index_real_data(index_code, index_name, start_date="2024-01-01", end_date="2024-01-10"):
    """获取港股真实指数数据"""
    print(f"\n=== 获取港股指数 {index_name}({index_code}) 真实数据 ===")
    
    results = {}
    
    # 方法1: 尝试akshare港股指数新接口
    try:
        print("尝试akshare港股指数接口...")
        # 使用akshare的港股指数接口
        if hasattr(ak, 'index_hk_hist'):
            df = ak.index_hk_hist(symbol=index_code, period="每日", start_date=start_date.replace("-", ""), end_date=end_date.replace("-", ""))
            if not df.empty:
                results["akshare_hk"] = {
                    "data": df,
                    "source": f"index_hk_hist({index_code})",
                    "data_points": len(df)
                }
                print(f"✅ akshare港股成功获取 {len(df)} 条真实指数数据")
                return results
    except Exception as e:
        print(f"akshare港股接口失败: {str(e)}")
    
    # 方法2: 尝试yfinance的真实港股指数
    try:
        print("尝试yfinance港股指数...")
        # yfinance中港股指数的正确格式
        yf_codes = [
            f"{index_code}.HK",
            f"{index_code}",
            f"^{index_code}"
        ]
        
        for yf_code in yf_codes:
            try:
                df = yf.download(yf_code, start=start_date, end=end_date, auto_adjust=False)
                if not df.empty:
                    results["yfinance_hk"] = {
                        "data": df,
                        "source": f"yfinance({yf_code})",
                        "data_points": len(df)
                    }
                    print(f"✅ yfinance成功获取 {len(df)} 条真实港股指数数据")
                    return results
            except:
                continue
    except Exception as e:
        print(f"yfinance港股指数失败: {str(e)}")
    
    # 方法3: 尝试其他数据源的港股指数
    try:
        print("尝试其他数据源港股指数...")
        # 这里可以添加其他专门提供港股指数的数据源
        pass
    except Exception as e:
        print(f"其他港股数据源失败: {str(e)}")
    
    print(f"❌ 港股指数 {index_name} 所有真实数据源都失败")
    return results

def test_all_real_indices():
    """测试所有指数的真实数据获取（不使用ETF）"""
    print("=== 开始测试真实指数数据获取（不使用ETF替代）===")
    
    test_cases = [
        # A股指数
        {"code": "000688", "name": "科创50", "type": "A"},
        {"code": "899050", "name": "北证50", "type": "A"}, 
        {"code": "932000", "name": "中证2000", "type": "A"},
        {"code": "883418", "name": "微盘股", "type": "A"},
        {"code": "000300", "name": "沪深300", "type": "A"},
        {"code": "000016", "name": "上证50", "type": "A"},
        {"code": "399006", "name": "创业板指", "type": "A"},
        
        # 港股指数
        {"code": "HSTECH", "name": "恒生科技", "type": "HK"},
        {"code": "HSCEI", "name": "国企指数", "type": "HK"},
        {"code": "HSI", "name": "恒生指数", "type": "HK"},
    ]
    
    results = {}
    
    for case in test_cases:
        if case["type"] == "A":
            results[case["code"]] = get_real_index_data(case["code"], case["name"])
        else:
            results[case["code"]] = get_hk_index_real_data(case["code"], case["name"])
        
        time.sleep(1)  # 避免请求过于频繁
    
    # 生成报告
    print("\n" + "="*60)
    print("真实指数数据获取结果报告:")
    print("="*60)
    
    success_count = 0
    for case in test_cases:
        code = case["code"]
        name = case["name"]
        result = results[code]
        
        if result:
            success_count += 1
            print(f"✅ {name}({code}): 成功")
            for source, info in result.items():
                print(f"   数据源: {info['source']}, 数据点: {info['data_points']}条")
                # 显示前几行数据样例
                data_preview = info['data'].head(3) if len(info['data']) > 0 else "无数据"
                print(f"   数据样例:\n{data_preview}\n")
        else:
            print(f"❌ {name}({code}): 失败")
    
    print(f"\n总结: {success_count}/{len(test_cases)} 个指数成功获取真实数据")
    return results

def verify_index_data_quality(results):
    """验证指数数据质量"""
    print("\n" + "="*50)
    print("指数数据质量验证:")
    print("="*50)
    
    for index_code, result_dict in results.items():
        if not result_dict:
            print(f"❌ {index_code}: 无数据")
            continue
            
        for source, info in result_dict.items():
            df = info['data']
            print(f"\n📊 {index_code} - {source}:")
            print(f"   数据形状: {df.shape}")
            print(f"   时间范围: {df.index.min()} 到 {df.index.max()}")
            print(f"   包含的列: {list(df.columns)}")
            
            # 检查数据完整性
            if 'close' in df.columns:
                print(f"   收盘价范围: {df['close'].min():.2f} - {df['close'].max():.2f}")
            
            # 检查缺失值
            missing = df.isnull().sum().sum()
            print(f"   缺失值总数: {missing}")

if __name__ == "__main__":
    # 执行真实指数数据测试
    print("开始获取真实指数数据（不使用ETF替代）...")
    test_results = test_all_real_indices()
    
    # 验证数据质量
    verify_index_data_quality(test_results)
    
    print("\n=== 真实指数数据测试完成 ===")
    
    # 提供可用的数据源建议
    print("\n" + "="*60)
    print("推荐的真实指数数据源:")
    print("="*60)
    print("""
A股指数推荐数据源:
1. akshare.index_zh_a_hist() - 最稳定的A股指数接口
2. baostock - 需要登录，但数据较全
3. 腾讯财经/新浪财经 - 备用数据源

港股指数推荐数据源:
1. akshare.index_hk_hist() - 港股指数专用接口
2. yfinance - 使用正确的指数代码格式

注意: 坚决不使用ETF数据替代指数数据，确保投资决策准确性！
""")
