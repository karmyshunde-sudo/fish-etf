from .arbitrage import calculate_arbitrage_opportunity, format_arbitrage_message
from .position import calculate_position_strategy
from .etf_scoring import get_top_rated_etfs
import os
import pandas as pd
from datetime import datetime, timedelta
from config import Config

def run_all_strategies():
    """运行所有策略并返回结果"""
    print("开始运行所有ETF策略...")
    
    # 1. 运行套利策略
    print("\n" + "="*50)
    print("运行套利策略")
    print("="*50)
    arbitrage_df = calculate_arbitrage_opportunity()
    arbitrage_msg = format_arbitrage_message(arbitrage_df)
    
    # 2. 运行仓位策略
    print("\n" + "="*50)
    print("运行仓位策略")
    print("="*50)
    position_msg = calculate_position_strategy()
    
    # 3. 返回所有策略结果
    return {
        "arbitrage": arbitrage_msg,
        "position": position_msg,
        "arbitrage_df": arbitrage_df
    }

def get_daily_report():
    """生成每日策略报告"""
    strategies = run_all_strategies()
    
    report = "【ETF量化策略每日报告】\n\n"
    report += "📊 套利机会分析：\n"
    report += strategies["arbitrage"] + "\n\n"
    report += "📈 仓位操作建议：\n"
    report += strategies["position"] + "\n\n"
    report += "💡 温馨提示：以上建议仅供参考，请结合市场情况谨慎决策！"
    
    return report

def send_daily_report_via_wechat():
    """生成并发送每日策略报告到微信"""
    try:
        from wechat_push import send_wechat_message
        
        # 检查是否已经发送过今日报告
        today = datetime.now().strftime("%Y-%m-%d")
        report_sent_flag = os.path.join(Config.FLAG_DIR, f"report_sent_{today}.txt")
        
        if os.path.exists(report_sent_flag):
            print("今日报告已发送，跳过重复发送")
            return True
            
        # 生成报告
        report = get_daily_report()
        
        # 发送到微信
        success = send_wechat_message(report)
        
        if success:
            # 标记已发送
            os.makedirs(os.path.dirname(report_sent_flag), exist_ok=True)
            with open(report_sent_flag, "w") as f:
                f.write(today)
            print("每日策略报告已成功发送到微信")
        else:
            print("微信消息发送失败")
            
        return success
        
    except Exception as e:
        print(f"发送微信报告失败: {str(e)}")
        return False

def check_arbitrage_exit_signals():
    """检查套利退出信号（持有1天后）"""
    try:
        from position import init_trade_record
        from wechat_push import send_wechat_message
        
        init_trade_record()
        trade_df = pd.read_csv(Config.TRADE_RECORD_FILE, encoding="utf-8")
        
        # 获取昨天的日期
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # 查找昨天执行的套利交易
        yesterday_arbitrage = trade_df[
            (trade_df["交易日期"] == yesterday) & 
            (trade_df["仓位类型"] == "套利仓") &
            (trade_df["操作类型"] == "买入")
        ]
        
        if not yesterday_arbitrage.empty:
            exit_messages = []
            for _, trade in yesterday_arbitrage.iterrows():
                # 建议卖出套利持仓
                exit_messages.append(
                    f"套利持仓退出建议: 卖出 {trade['ETF名称']} ({trade['ETF代码']})，"
                    f"买入价: {trade['价格']}元，建议获利了结"
                )
            
            if exit_messages:
                message = "【套利持仓退出提示】\n\n" + "\n".join(exit_messages)
                message += "\n\n💡 套利持仓建议持有不超过1天，请及时了结！"
                
                # 发送微信消息
                send_wechat_message(message)
                print("套利退出提示已发送")
                
        return True
        
    except Exception as e:
        print(f"检查套利退出信号失败: {str(e)}")
        return False
