# -*- coding: utf-8 -*-

import pandas as pd
import requests

from tenacity import retry, wait_fixed, stop_after_attempt

from zvt import init_log
from zvt.contract import IntervalLevel
from zvt.contract.api import df_to_db
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import StockDetail, Stock1dKdata
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601


class JoudouChinaStockKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'em'
    entity_schema = StockDetail

    provider = 'xbx'
    data_schema = Stock1dKdata

    url = 'https://www.joudou.com/stockinfogate/kchartdata/{}.{}'

    def record(self, entity, start, end, size, timestamps):
        data_resp = self.query_historical_data(self.url.format(entity.code, entity.exchange.upper()))
        if isinstance(data_resp, list) and len(data_resp) == 0:
            self.logger.info(f'No kdata for {entity.id}, skip...')
            return None

        origin_price = data_resp.get('origin', [])
        if len(origin_price) == 0:
            self.logger.info(f'No kdata for {entity.id}, skip...')
            return None

        origin_df = pd.DataFrame(origin_price)
        origin_df.columns = ['timestamp', 'close', 'open', 'high', 'low', 'price_change', 'change_pct', 'ma', 'pre_close']

        turnover = data_resp.get('turnover', [])
        turnover_df = pd.DataFrame(turnover)
        turnover_df.columns = ['timestamp', 'volume', 'turnover', 'turnover_rate']

        df = pd.concat([origin_df, turnover_df], axis=1)
        df = df.loc[:, ~df.columns.duplicated()]

        if pd_is_not_null(df):
            df['name'] = entity.name
            df['entity_id'] = entity.id
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d')
            df['provider'] = 'xbx'
            df['level'] = self.level.value
            df['code'] = entity.code

            def generate_kdata_id(se):
                if self.level >= IntervalLevel.LEVEL_1DAY:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_DAY))
                else:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

            df['id'] = df[['entity_id', 'timestamp']].apply(generate_kdata_id, axis=1)
            df = df.drop_duplicates(subset='id', keep='last')
            df = df[df['timestamp'] >= start]

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)
        else:
            self.logger.info(f'No kdata for {entity.id}')

    @staticmethod
    @retry(stop=stop_after_attempt(20), wait=wait_fixed(10))
    def query_historical_data(url):
        resp = requests.get(url)
        resp.raise_for_status()

        json_resp = resp.json()
        data_resp = json_resp.get('data', {})

        return data_resp


if __name__ == '__main__':
    init_log('joudou_china_stock_1d_kdata.log')
    JoudouChinaStockKdataRecorder(level=IntervalLevel('1d'), sleeping_time=0, codes=['600087'], real_time=False).run()