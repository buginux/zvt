# -*- coding: utf-8 -*-

import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler
from zvt.informer.informer import WeWorkInformer

from zvt import init_log
from zvt.recorders.joudou.announcement.announcement_recorder import JoudouStockAnnouncementRecorder

logger = logging.getLogger(__name__)

sched = BackgroundScheduler()
wework = WeWorkInformer()


@sched.scheduled_job('cron', hour=17, minute=00)
def run():
    err_count = 0
    while True:
        try:
            JoudouStockAnnouncementRecorder().run()

            wework.send_finished_message('资金消息面数据下载完成...')
            err_count = 0

            break
        except Exception as e:
            err_count += 1

            if err_count >= 10:
                wework.send_message(f'资金消息面数据下载出错超过 {err_count} 次，请检查...\n{e}')
                err_count = 0

            logger.exception('meta runner error:{}'.format(e))
            time.sleep(60)


if __name__ == '__main__':
    init_log('joudou_china_stock_announcement.log')

    run()

    sched.start()

    sched._thread.join()
