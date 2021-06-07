# -*- coding: utf-8 -*-

import io
import requests
import pandas as pd

from tenacity import retry, stop_after_attempt, wait_fixed
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.utils.time_utils import now_pd_timestamp, to_pd_timestamp, to_time_str, TIME_FORMAT_DAY1
from zvt.contract.api import get_data
from zvt.domain import Stock, StockValuation


class JoudouChinaStockValuationRecorder(TimeSeriesDataRecorder):
    entity_provider = 'eastmoney'
    entity_schema = Stock

    provider = 'joudou'
    data_schema = StockValuation

    url = 'http://quotes.money.163.com/service/chddata.html?code={}{}&start={}&end={}&fields=TCLOSE;TURNOVER;TCAP;MCAP'

    def record(self, entity, start, end, size, timestamps):
        exchange_flag = 0 if entity.exchange == 'sh' else 1

        if self.start_timestamp:
            start = max(self.start_timestamp, to_pd_timestamp(start))
        end = now_pd_timestamp() + pd.Timedelta(days=1)

        start_timestamp = to_time_str(start, fmt=TIME_FORMAT_DAY1)
        end_timestamp = to_time_str(end, fmt=TIME_FORMAT_DAY1)

        url = self.url.format(exchange_flag, entity.code, start_timestamp, end_timestamp)
        df = self.query_netease_valuation(url)

        if df.empty:
            return []

        df.columns = ['timestamp', 'code', 'name', 'close', 'turnover_ratio', 'market_cap', 'circulating_market_cap']
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['code'] = df['code'].apply(lambda x: x[1:])
        df['turnover_ratio'] = pd.to_numeric(df['turnover_ratio'], errors='coerce')
        df['capitalization'] = df['market_cap'] / df['close']
        df['circulating_cap'] = df['circulating_market_cap'] / df['close']

        return df.to_dict(orient='records')

    def on_finish_entity(self, entity):
        datas = get_data(data_schema=StockValuation, entity_id=entity.id, provider='joudou', return_type='domain',
                         session=self.session,
                         filters=[self.data_schema.dividend_rate.is_(None),
                                  self.data_schema.timestamp >= to_pd_timestamp('2000-01-01')])
        if datas:
            start = datas[0].timestamp
            end = now_pd_timestamp()

            # 从九斗数据获取股息率
            df = self.query_joudou_dividend_rate(entity, start, end)

            if df is not None and not df.empty:
                for data in datas:
                    if data.timestamp in df.index:
                        data.dividend_rate = df.loc[data.timestamp, 'dividend_rate']
                self.session.commit()
            self.logger.info(f'({entity.code}{entity.name})股息率更新完成...')

        datas = get_data(data_schema=StockValuation, entity_id=entity.id, provider='joudou', return_type='domain',
                         session=self.session,
                         filters=[self.data_schema.pe_ttm.is_(None),
                                  self.data_schema.timestamp >= to_pd_timestamp('2000-01-01')])

        if datas:
            start = datas[0].timestamp
            end = now_pd_timestamp()

            # 从九斗数据获取估值信息
            df = self.query_joudou_valuation(entity, start, end)

            if df is not None and not df.empty:
                for data in datas:
                    if data.timestamp in df.index:
                        data.pe = df.loc[data.timestamp, 'pe']
                        data.pe_ttm = df.loc[data.timestamp, 'pe_ttm']
                        data.pb = df.loc[data.timestamp, 'pb']
                        data.ps = df.loc[data.timestamp, 'ps']
                        data.pcf = df.loc[data.timestamp, 'pcf']
                self.session.commit()
            self.logger.info(f'({entity.code}{entity.name})估值更新完成...')

    @staticmethod
    @retry(stop=stop_after_attempt(20), wait=wait_fixed(10))
    def query_netease_valuation(url):
        response = requests.get(url)
        df = pd.read_csv(io.BytesIO(response.content), encoding='GBK')
        return df

    @staticmethod
    @retry(stop=stop_after_attempt(20), wait=wait_fixed(10))
    def query_joudou_dividend_rate(security_item, start, end):
        url = 'https://m.joudou.com/p/www/stockinfogate/commonapi?' \
              'name=divirate_history&secucode={}.{}'
        query_url = url.format(security_item.code, security_item.exchange.upper())
        response = requests.get(query_url)

        data_list = response.json().get('data', [])

        df = pd.DataFrame(data_list)
        df.columns = ['timestamp', 'dividend_rate']
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df.set_index('timestamp', drop=True, inplace=True)

        return df[start: end]

    @staticmethod
    @retry(stop=stop_after_attempt(20), wait=wait_fixed(10))
    def query_joudou_valuation(entity, start, end):
        url = 'https://www.joudou.com/stockinfogate/commonapi?' \
              'name=pepbps_history&secucode={}.{}'
        query_url = url.format(entity.code, entity.exchange.upper())
        response = requests.get(query_url)

        data_list = response.json().get('data', {}).get('data', [])
        df = pd.DataFrame(data_list)
        df.columns = ['timestamp', 'pe_ttm', 'pcf', 'pb', 'ps']
        df['pe'] = df['pe_ttm']
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df.set_index('timestamp', drop=True, inplace=True)

        return df[start: end]


__all__ = ['JoudouChinaStockValuationRecorder']

if __name__ == '__main__':
    JoudouChinaStockValuationRecorder(codes=['000001', '000002'], sleeping_time=1.0).run()