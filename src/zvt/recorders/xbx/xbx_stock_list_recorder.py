# -*- coding: utf-8 -*-

import os

import pandas as pd

from zvt.contract.api import df_to_db
from zvt.contract.recorder import Recorder
from zvt.consts import ZVT_HOME
from zvt.domain import Stock, StockDetail


class XbxStockListRecorder(Recorder):
    data_schema = Stock
    provider = 'eastmoney'

    def run(self):
        stock_directory = os.path.join(ZVT_HOME, 'xbx_stock_day_data_pro/stock')
        _, _, filenames = next(os.walk(stock_directory), (None, None, []))
        filenames = [f.split('.')[0] for f in filenames]

        stock_info_list = []
        for file in filenames:
            exchange = file[:2]
            if exchange == 'bj':
                continue

            stock_info = {
                'code': file[2:],
                'exchange': file[:2],
            }
            stock_info_list.append(stock_info)

        df = pd.DataFrame(stock_info_list)
        df['entity_type'] = 'stock'
        df['id'] = df[['entity_type', 'exchange', 'code']].apply(lambda x: '_'.join(x.astype(str)), axis=1)
        df['entity_id'] = df['id']
        df = df.dropna(axis=0, how='any')
        df = df.drop_duplicates(subset='id', keep='last')

        df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=False)
        df_to_db(df=df, data_schema=StockDetail, provider=self.provider, force_update=False)

        self.logger.info(df.tail())
        self.logger.info("Persist stock list from xbx data success")


if __name__ == '__main__':
    XbxStockListRecorder().run()

__all__ = ['XbxStockListRecorder']