# -*- coding: utf-8 -*-

import requests
import pandas as pd

from tenacity import retry, stop_after_attempt, wait_fixed
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.utils.time_utils import to_pd_timestamp, TIME_FORMAT_DAY
from zvt.domain import Stock, Announcement


class JoudouStockAnnouncementrecorder(TimeSeriesDataRecorder):
    entity_provider = 'eastmoney'
    entity_schema = Stock

    provider = 'joudou'
    data_schema = Announcement

    url = 'https://m.joudou.com/p/www/stockinfogate/commonapi?name=summary&secucode={}.{}'
    
    def __init__(self, codes=None, exchanges=None):
        super(JoudouStockAnnouncementrecorder, self).__init__(codes=codes, exchanges=exchanges,
                                                              force_update=True, sleeping_time=0.0)

    def generate_domain_id(self, entity, original_data, time_fmt=TIME_FORMAT_DAY):
        return f"{entity.id}_{original_data['announcement_id']}"

    def record(self, entity, start, end, size, timestamps):
        fetch_url = self.url.format(entity.code, entity.exchange.upper())
        buy_sell = self.retry_get_buy_sell(fetch_url)

        finance_df = pd.DataFrame(buy_sell)
        if finance_df.empty:
            return []

        finance_df.rename(columns={'id': 'announcement_id', 'url': 'url_component'}, inplace=True)
        finance_df['timestamp'] = pd.to_datetime(finance_df['date'])
        finance_df['stockholders_meeting_date'] = pd.to_datetime(finance_df['stockholders_meeting_date'], format='%Y%m%d')
        finance_df['trade_start_date'] = pd.to_datetime(finance_df['trade_start_date'], format='%Y%m%d')
        finance_df['code'] = entity.code
        finance_df['name'] = entity.name

        return finance_df.to_dict(orient='records')

    @staticmethod
    @retry(stop=stop_after_attempt(60), wait=wait_fixed(10))
    def retry_get_buy_sell(url):
        json_response = requests.get(url).json()
        data = json_response.get('data', {})
        finance_response = data.get('finance', {})
        buy_sell = finance_response.get('addbuy', []) + finance_response.get('sell', [])

        return buy_sell


__all__ = ['JoudouStockAnnouncementrecorder']

if __name__ == '__main__':
    JoudouStockAnnouncementrecorder(codes=['000001', '000002']).run()


