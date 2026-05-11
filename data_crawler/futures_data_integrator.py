#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
期货数据整合器 - 自动获取财经网站数据并发送到飞书
支持：IC/IF/IH 股指期货、外盘指数等
"""

import pandas as pd
import logging
import os
from datetime import datetime
from typing import Dict, Any

from config import Config
from utils.date_utils import get_beijing_time
from .futures_data_source import FuturesDataSource, get_futures_report, FUTURES_CODES, EXTERNAL_INDICES

logger = logging.getLogger(__name__)

class FuturesDataIntegrator:
    def __init__(self):
        self.fds = FuturesDataSource()
        self.data_dir = os.path.join(Config.DATA_DIR, "futures")
        os.makedirs(self.data_dir, exist_ok=True)
    
    def _save_data(self, df: pd.DataFrame, filename: str) -> str:
        try:
            filepath = os.path.join(self.data_dir, filename)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"数据已保存至: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"保存数据失败: {str(e)}")
            return ""
    
    def _save_report(self, report: str) -> str:
        try:
            filepath = os.path.join(self.data_dir, f"futures_report_{get_beijing_time().strftime('%Y%m%d_%H%M%S')}.txt")
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"报告已保存至: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"保存报告失败: {str(e)}")
            return ""
    
    def run_auto_fetch(self) -> Dict[str, Any]:
        logger.info("执行自动获取期货数据")
        
        futures_data = self.fds.fetch_futures_data()
        external_data = self.fds.fetch_external_indices()
        roll_analysis = self.fds.analyze_roll_opportunity(futures_data)
        
        report = get_futures_report(futures_data, external_data, roll_analysis)
        self._save_report(report)
        
        return {
            "status": "success" if not futures_data.empty else "failed",
            "futures_data": futures_data,
            "external_data": external_data,
            "roll_analysis": roll_analysis,
            "report": report
        }

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    integrator = FuturesDataIntegrator()
    result = integrator.run_auto_fetch()
    
    print("\n" + "="*50)
    print("📊 期货数据获取结果")
    print("="*50)
    print(result["report"])
    
    return result

if __name__ == "__main__":
    main()