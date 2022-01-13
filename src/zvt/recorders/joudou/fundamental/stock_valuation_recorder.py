# -*- coding: utf-8 -*-

import requests
import pandas as pd

from tenacity import retry, stop_after_attempt, wait_fixed
from zvt.contract.api import df_to_db
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str
from zvt.domain import Stock, StockValuation


class JoudouChinaStockValuationRecorder(TimeSeriesDataRecorder):
    entity_provider = 'eastmoney'
    entity_schema = Stock

    provider = 'joudou'
    data_schema = StockValuation

    def record(self, entity, start, end, size, timestamps):
        dividend_df = self.query_joudou_dividend_rate(entity)
        valuation_df = self.query_joudou_valuation(entity)

        if pd_is_not_null(dividend_df) and pd_is_not_null(valuation_df):
            df = pd.merge(dividend_df, valuation_df, on='timestamp', sort=True)
            df['entity_id'] = entity.id
            df['code'] = entity.code
            df['name'] = entity.name
            df["id"] = df["timestamp"].apply(lambda x: "{}_{}".format(entity.id, to_time_str(x)))
            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

        return None

    @staticmethod
    @retry(stop=stop_after_attempt(20), wait=wait_fixed(10))
    def query_joudou_dividend_rate(security_item):
        url = 'https://m.joudou.com/p/www/stockinfogate/commonapi?' \
              'name=divirate_history&secucode={}.{}'
        query_url = url.format(security_item.code, security_item.exchange.upper())
        response = requests.get(query_url)

        data_list = response.json().get('data', [])
        if len(data_list) == 0:
            return None

        df = pd.DataFrame(data_list)
        df.columns = ['timestamp', 'dividend_rate']
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    @staticmethod
    @retry(stop=stop_after_attempt(20), wait=wait_fixed(10))
    def query_joudou_valuation(entity):
        url = 'https://www.joudou.com/stockinfogate/commonapi?' \
              'name=pepbps_history&secucode={}.{}'
        query_url = url.format(entity.code, entity.exchange.upper())
        response = requests.get(query_url)

        data_list = response.json().get('data', {}).get('data', [])
        if data_list is None:
            return None

        df = pd.DataFrame(data_list)
        df.columns = ['timestamp', 'pe_ttm', 'pcf', 'pb', 'ps']
        df['pe'] = df['pe_ttm']
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

if __name__ == '__main__':
    JoudouChinaStockValuationRecorder(codes=None, sleeping_time=0.0, force_update=True).run()

__all__ = ['JoudouChinaStockValuationRecorder']
