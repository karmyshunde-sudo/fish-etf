import datetime
import pytz
import akshare as ak
import pandas as pd

def get_beijing_time():
    """获取当前北京时间（带时区）"""
    tz_sh = pytz.timezone("Asia/Shanghai")
    return datetime.datetime.now(tz_sh)

def get_last_trading_day(date=None):
    """获取指定日期的上一个交易日（默认今天）"""
    if date is None:
        date = get_beijing_time().date()
    
    try:
        # AkShare交易日历（兼容字段变化，动态匹配列名）
        cal_df = ak.tool_trade_date_hist_sina()
        # 动态匹配日期列和开盘列
        date_col = next(col for col in cal_df.columns if "date" in col.lower())
        open_col = next(col for col in cal_df.columns if "open" in col.lower())
        
        cal_df[date_col] = pd.to_datetime(cal_df[date_col])
        cal_df[open_col] = cal_df[open_col].astype(int)
        
        # 筛选已过的交易日
        past_trades = cal_df[
            (cal_df[date_col].dt.date <= date) & (cal_df[open_col] == 1)
        ].sort_values(date_col, ascending=False)
        
        if not past_trades.empty:
            return past_trades.iloc[0][date_col].date()
        return date  # 降级：返回原日期
    
    except Exception as e:
        # 降级逻辑：非周末往前推1天，周末推到周五
        if date.weekday() == 0:  # 周一
            return date - datetime.timedelta(days=3)
        elif date.weekday() in [6]:  # 周日
            return date - datetime.timedelta(days=2)
        else:
            return date - datetime.timedelta(days=1)

def get_date_range(days=365, end_date=None):
    """获取日期范围（默认结束日期为上一交易日）"""
    if end_date is None:
        end_date = get_last_trading_day()
    start_date = end_date - datetime.timedelta(days=days)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
