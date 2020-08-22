# -*- coding: utf-8 -*-

import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler
from zvt.informer.informer import WeWorkInformer

from zvt.recorders.eastmoney.finance.china_stock_cash_flow_recorder import ChinaStockCashFlowRecorder, ChinaStockCashFlowSeasonalRecorder
from zvt import init_log

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()
wework = WeWorkInformer()


@sched.scheduled_job('cron', day_of_week='tue,sat', hour=4, minute=00)
def run():
    err_count = 0

    while True:
        try:
            ChinaStockCashFlowSeasonalRecorder(sleeping_time=0.0).run()
            ChinaStockCashFlowRecorder(sleeping_time=0.0).run()

            wework.send_finished_message('现金流量表')
            err_count = 0
            break
        except Exception as e:
            err_count += 1

            if err_count >= 10:
                wework.send_message(f'现金流量表下载出错超过 {err_count} 次, 请检查...\n{e}')
                err_count = 0

            logger.exception('finance cash flow runner error:{}'.format(e))
            time.sleep(60)


if __name__ == '__main__':
    init_log('eastmoney_finance_cash_flow.log')

    run()

    sched.start()

    sched._thread.join()
