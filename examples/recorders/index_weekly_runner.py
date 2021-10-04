# -*- coding: utf-8 -*-

import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler
from zvt.informer.informer import WeWorkInformer

from zvt import init_log
from zvt.recorders.exchange.exchange_index_recorder import ExchangeIndexRecorder
from zvt.recorders.exchange.exchange_index_stock_recorder import ExchangeIndexStockRecorder

from zvt.recorders.tencent.tencent_index_kdata_recorder import (
    TencenIndexKdataRecorder
)

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()
wework = WeWorkInformer()

@sched.scheduled_job('cron', day_of_week='sat', hour=16, minute=30)
def run():
    err_count = 0
    while True:
        try:
            ExchangeIndexRecorder(sleeping_time=0.0).run()
            ExchangeIndexStockRecorder(sleeping_time=0.0).run()
            TencenIndexKdataRecorder(sleeping_time=0.0).run()

            wework.send_finished_message('股票列表、日线数据')
            err_count = 0

            break
        except Exception as e:
            err_count += 1

            if err_count >= 10:
                wework.send_message(f'股票列表、日线数据下载出错超过 {err_count} 次，请检查...\n{e}')
                err_count = 0

            logger.exception('meta runner error:{}'.format(e))
            time.sleep(60)


if __name__ == '__main__':
    init_log('index_weekly_runner.log')
    run()
    sched.start()

    sched._thread.join()
