# -*- coding: utf-8 -*-

import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler
from zvt.informer.informer import WeWorkInformer

from zvt.recorders.joudou.fundamental.stock_valuation_recorder import JoudouChinaStockValuationRecorder
from zvt import init_log

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()
wework = WeWorkInformer()


@sched.scheduled_job('cron', day_of_week='mon,wed,fri', hour=00, minute=15)
def run():
    err_count = 0

    while True:
        try:
            JoudouChinaStockValuationRecorder(sleeping_time=1.0).run()

            wework.send_finished_message('股票估值信息')
            err_count = 0
            break
        except Exception as e:
            err_count += 1

            if err_count >= 10:
                wework.send_message(f'股票估值信息下载出错超过 {err_count} 次，请检查...\n{e}')
                err_count = 0

            logger.exception('valuation runner error:{}'.format(e))
            time.sleep(60)


if __name__ == '__main__':
    init_log('joudou_valuation_summary.log')

    run()

    sched.start()

    sched._thread.join()
