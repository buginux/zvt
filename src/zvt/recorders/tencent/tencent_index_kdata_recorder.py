# -*- coding: utf-8 -*-
import pandas as pd
import requests

from random import randint

from zvt.api.kdata import generate_kdata_id
from zvt.contract import IntervalLevel
from zvt.contract.api import df_to_db
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Index, Index1dKdata
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601


class TencenIndexKdataRecorder(FixedCycleDataRecorder):
    default_size = 50000
    entity_provider = 'exchange'
    entity_schema = Index

    provider = 'tencent'
    data_schema = Index1dKdata

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data['timestamp'],
                                 level=IntervalLevel(IntervalLevel.LEVEL_1DAY))

    def record(self, entity, start, end, size, timestamps):
        random_stamp = self.generate_random_stamp()
        code = f'{entity.exchange.lower()}{entity.code}'
        url = f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,,,{size},bfq&r=0.{random_stamp}'
        resp = requests.get(url)
        resp.raise_for_status()

        resp_json = resp.json()

        msg = resp_json['msg']
        data = resp_json['data']
        if len(data) == 0:
            self.logger.info(f'{entity.id} kdata is empty, skip...')
            if len(msg) > 0:
                self.logger.warning(f'{entity.id} msg: f{msg}')
            return None

        klines = data.get(code, {}).get('day', {})
        df = pd.DataFrame(klines)

        if pd_is_not_null(df):
            df.rename(columns={
                0: 'timestamp',
                1: 'open',
                2: 'close',
                3: 'high',
                4: 'low',
                5: 'volume',
                6: 'status'
            }, inplace=True)
            df['name'] = entity.name
            df['entity_id'] = entity.id
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['provider'] = 'tencent'
            df['level'] = IntervalLevel.LEVEL_1DAY.value
            df['code'] = entity.code

            def kdata_id(se):
                if self.level >= IntervalLevel.LEVEL_1DAY:
                    return "{}_{}".format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_DAY))
                else:
                    return "{}_{}".format(se['entity_id'],
                                          to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

            df['id'] = df[['entity_id', 'timestamp']].apply(kdata_id, axis=1)

            df = df.drop_duplicates(subset='id', keep='last')
            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider,
                     force_update=self.force_update)

    @staticmethod
    def generate_random_stamp(n=16):
        """
        创建一个 n 位的随机整数
        """

        start = 10 ** (n - 1)
        end = (10 ** n) - 1

        return str(randint(start, end))


if __name__ == '__main__':
    TencenIndexKdataRecorder(codes=None, sleeping_time=0.0).run()

__all__ = ['TencenIndexKdataRecorder']
