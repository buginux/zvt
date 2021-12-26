# -*- coding: utf-8 -*-

import baostock as bs
import pandas as pd
from tenacity import *

from zvt.api.kdata import generate_kdata_id, get_kdata_schema, get_kdata
from zvt.contract import IntervalLevel, AdjustType
from zvt.contract.api import df_to_db
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.domain import Stock, StockKdataCommon
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, TIME_FORMAT_DAY, TIME_FORMAT_ISO8601


def bs_func_decorator(func):
    def wrap_func(self, *args, **kwargs):
        retry_n = 5

        for i in range(retry_n):
            rs = func(self, *args, **kwargs)

            if rs.error_code == '0':
                return rs.get_data()
            else:
                bs.login()

    return wrap_func


class BSStockKdataRecorder(FixedCycleDataRecorder):
    entity_provider = 'eastmoney'
    entity_schema = Stock

    provider = 'baostock'
    data_schema = StockKdataCommon

    def __init__(self,
                 codes=None,
                 level=IntervalLevel.LEVEL_1DAY,
                 adjust_type=AdjustType.bfq):
        level = IntervalLevel(level)
        adjust_type = AdjustType(adjust_type)
        self.data_schema = get_kdata_schema(entity_type='stock', level=level, adjust_type=adjust_type)

        super().__init__(codes=codes, level=level)

        self.adjust_type = adjust_type
        self.bs_data_fields = 'date,open,high,low,close,preclose,volume,amount,turn,tradestatus,pctChg,isST'
        self.bs_trading_level = self.to_bs_trading_level(level)
        self.bs_adjust_flag = self.to_bs_adjust_flag(adjust_type)

        bs.login()

    def generate_domain_id(self, entity, original_data):
        return generate_kdata_id(entity_id=entity.id, timestamp=original_data['timestamp'], level=self.level)

    def record(self, entity, start, end, size, timestamps):
        ticker = f'{entity.exchange}.{entity.code}'

        if self.should_reset_qfq(ticker, start):
            start_str = to_time_str(entity.timestamp)
            self.force_update = True
        else:
            start_str = to_time_str(start)
        end_str = to_time_str(self.end_timestamp) if self.end_timestamp is not None else None

        df = self.query_baostock_history_k_data_plus(ticker=ticker, start=start_str, end=end_str)
        if pd_is_not_null(df):
            for col in ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'pctChg', 'turn']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            df['name'] = entity.name
            df.rename(columns={
                'date': 'timestamp',
                'amount': 'turnover',
                'pctChg': 'change_pct',
                'turn': 'turnover_rate',
                'tradestatus': 'trade_status',
                'isST': 'is_st'
            }, inplace=True)

            df['entity_id'] = entity.id
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['provider'] = 'baostock'
            df['level'] = self.level.value

            def generate_kdata_id(se):
                if self.level >= IntervalLevel.LEVEL_1DAY:
                    return '{}_{}'.format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_DAY))
                else:
                    return '{}_{}'.format(se['entity_id'], to_time_str(se['timestamp'], fmt=TIME_FORMAT_ISO8601))

            df['id'] = df[['entity_id', 'timestamp']].apply(generate_kdata_id, axis=1)

            df = df.drop_duplicates(subset='id', keep='last')

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)

        return None

    def should_reset_qfq(self, ticker, start_date):
        adjust_factor_df = self.query_baostock_adjust_factor(ticker, to_time_str(start_date))
        if pd_is_not_null(adjust_factor_df):
            adjust_factor_df['dividOperateDate'] = pd.to_datetime(adjust_factor_df['dividOperateDate'])

            return (self.adjust_type == AdjustType.qfq) and (adjust_factor_df['dividOperateDate'].iloc[-1] >= start_date)
        else:
            return False

    @bs_func_decorator
    def query_baostock_history_k_data_plus(self, ticker, start, end):
        return bs.query_history_k_data_plus(ticker, self.bs_data_fields, start_date=start, end_date=end,
                                          frequency=self.bs_trading_level,
                                          adjustflag=self.bs_adjust_flag)

    @bs_func_decorator
    def query_baostock_adjust_factor(self, ticker, date):
        return bs.query_adjust_factor(code=ticker, end_date=date)

    @staticmethod
    def to_bs_trading_level(trading_level: IntervalLevel):
        if trading_level == IntervalLevel.LEVEL_1DAY:
            return "d"

    @staticmethod
    def to_bs_adjust_flag(adjust_type: AdjustType):
        if adjust_type == AdjustType.hfq:
            return "1"
        elif adjust_type == AdjustType.qfq:
            return "2"
        else:
            return "3"


if __name__ == '__main__':
    BSStockKdataRecorder(codes=['000001'], adjust_type=AdjustType.qfq).run()

# the __all__ is generated
__all__ = ['BSStockKdataRecorder']


