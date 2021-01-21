# -*- coding: utf-8 -*-

import pandas as pd
import requests

from zvt import init_log
from zvt.contract import IntervalLevel
from zvt.contract.api import df_to_db
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Stock, StockKdataCommon, Stock1dKdata
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601


class JoudouChinaStockKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'eastmoney'
    entity_schema = Stock

    provider = 'joudou'
    data_schema = Stock1dKdata

    url = 'https://www.joudou.com/stockinfogate/kchartdata/{}.{}'

    def __init__(self,
                 exchanges=['sh', 'sz'],
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=True,
                 sleeping_time=0,
                 default_size=2000,
                 real_time=False,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 level=IntervalLevel.LEVEL_1DAY,
                 kdata_use_begin_time=False,
                 close_hour=15,
                 close_minute=0,
                 one_day_trading_minutes=4 * 60):
        level = IntervalLevel(level)
        super(JoudouChinaStockKdataRecorder, self).__init__('stock', exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)

    def record(self, entity, start, end, size, timestamps):
        resp = requests.get(self.url.format(entity.code, entity.exchange.upper()))
        json_resp = resp.json()
        data_resp = json_resp.get('data', {})

        origin_price = data_resp.get('origin', [])
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
            df['provider'] = 'joudou'
            df['level'] = self.level.value
            df['code'] = entity.code

            def generate_kdata_id(se):
                if self.level >= IntervalLevel.LEVEL_1DAY:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_DAY))
                else:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

            df['id'] = df[['entity_id', 'timestamp']].apply(generate_kdata_id, axis=1)
            df = df.drop_duplicates(subset='id', keep='last')

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=True)

        return None


from zvt.api.quote import get_kdata

if __name__ == '__main__':
    init_log('joudou_china_stock_1d_kdata.log')
    JoudouChinaStockKdataRecorder(level=IntervalLevel('1d'), sleeping_time=0, codes=['000100', '000600'], real_time=False).run()