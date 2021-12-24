# -*- coding: utf-8 -*-

import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler
from zvt.informer.informer import WeWorkInformer

from zvt import init_log
from zvt.recorders.eastmoney.meta.eastmoney_stock_meta_recorder import (
    EastmoneyChinaStockDetailRecorder
)
from zvt.recorders.joudou.quotes.joudou_stock_kdata_recorder import (
    JoudouChinaStockKdataRecorder
)
from zvt.recorders.tencent.tencent_index_kdata_recorder import (
    TencenIndexKdataRecorder
)

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()
wework = WeWorkInformer()


@sched.scheduled_job('cron', hour=16, minute=30)
def run():
    err_count = 0
    while True:
        try:
            EastmoneyChinaStockDetailRecorder(sleeping_time=0.0).run()
            JoudouChinaStockKdataRecorder(sleeping_time=0.0).run()
            # TencenIndexKdataRecorder(codes=['000001']).run()

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
    init_log('kdata_daily_runner.log')
    run()
    sched.start()

    sched._thread.join()
