# -*- coding: utf-8 -*-

import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler
from zvt.informer.informer import WeWorkInformer

from zvt import init_log
from zvt.recorders.eastmoney.meta.china_stock_category_recorder import (
    EastmoneyChinaBlockRecorder,
    EastmoneyChinaBlockStockRecorder
)
from zvt.recorders.eastmoney.meta.china_stock_meta_recorder import EastmoneyChinaStockDetailRecorder

from zvt.recorders.sina.meta.sina_china_stock_category_recorder import (
    SwChinaBlockRecorder,
    SwChinaBlockStockRecorder,
    SinaChinaBlockRecorder,
    SinaChinaBlockStockRecorder
)

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()
wework = WeWorkInformer()


@sched.scheduled_job('cron', day_of_week='sun', hour=00, minute=00)
def run():
    err_count = 0
    while True:
        try:
            EastmoneyChinaStockDetailRecorder(sleeping_time=0.0).run()
            EastmoneyChinaBlockRecorder(sleeping_time=0.0).run()
            EastmoneyChinaBlockStockRecorder(sleeping_time=0.0).run()

            SinaChinaBlockRecorder().run()
            SwChinaBlockRecorder().run()
            SinaChinaBlockStockRecorder().run()
            SwChinaBlockStockRecorder().run()

            wework.send_finished_message('股票元数据')
            err_count = 0

            break
        except Exception as e:
            err_count += 1

            if err_count >= 10:
                wework.send_message(f'股票元数据下载出错超过 {err_count} 次，请检查...\n{e}')
                err_count = 0

            logger.exception('meta runner error:{}'.format(e))
            time.sleep(60)


if __name__ == '__main__':
    init_log('eastmoney_china_stock_meta.log')

    run()

    sched.start()

    sched._thread.join()
