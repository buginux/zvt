# -*- coding: utf-8 -*-

import os

import pandas as pd

from zvt import init_log
from zvt.contract import IntervalLevel
from zvt.contract.api import df_to_db
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.consts import ZVT_HOME
from zvt.domain import StockDetail, Stock1dKdata
from zvt.utils.time_utils import to_time_str, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601


class XbxStockKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'em'
    entity_schema = StockDetail

    provider = 'xbx'
    data_schema = Stock1dKdata

    def record(self, entity, start, end, size, timestamps):
        stock_directories = os.path.join(ZVT_HOME, 'xbx_stock_day_data_pro/stock')
        filename = f'{entity.exchange.lower()}{entity.code}.csv'
        fullpath = os.path.join(stock_directories, filename)

        if not os.path.exists(fullpath):
            self.logger.info(f'{fullpath} does not exists, skip...')
            return

        df = pd.read_csv(fullpath, encoding='GBK', skiprows=1)
        df = df[['股票代码', '股票名称', '交易日期', '开盘价', '最高价', '最低价', '收盘价', '前收盘价', '成交量',
                 '成交额']]
        df['交易日期'] = df['交易日期'].str.replace('-', '')
        df['交易日期'] = pd.to_datetime(df['交易日期'], format='%Y%m%d')
        df['股票代码'] = df['股票代码'].str.slice(start=2)
        df.rename(columns={
            '股票代码': 'code',
            '股票名称': 'name',
            '交易日期': 'timestamp',
            '开盘价': 'open',
            '最高价': 'high',
            '最低价': 'low',
            '收盘价': 'close',
            '前收盘价': 'pre_close',
            '成交量': 'volume',
            '成交额': 'turnover'
        }, inplace=True)

        df['entity_id'] = entity.id
        df['provider'] = 'xbx'
        df['level'] = self.level.value

        def generate_kdata_id(se):
            if self.level >= IntervalLevel.LEVEL_1DAY:
                return '{}_{}'.format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_DAY))
            else:
                return '{}_{}'.format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

        df['id'] = df[['entity_id', 'timestamp']].apply(generate_kdata_id, axis=1)
        df = df.drop_duplicates(subset='id', keep='last')

        df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)


if __name__ == '__main__':
    init_log('xbx_stock_kdata.log')
    XbxStockKdataRecorder(sleeping_time=0.0, end_timestamp='2021-09-22', codes=['301083', '001219', '605567']).run()
