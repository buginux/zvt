# -*- coding: utf-8 -*-

import io

import pandas as pd
import requests

from zvt.contract.api import df_to_db
from zvt.contract.recorder import Recorder
from zvt.domain import Stock, StockDetail
from zvt.recorders.consts import DEFAULT_SH_HEADER, DEFAULT_SZ_HEADER
from zvt.utils.time_utils import to_pd_timestamp


class ExchangeStockMetaRecorder(Recorder):
    data_schema = Stock
    provider = "exchange"

    original_page_url = "http://www.sse.com.cn/assortment/stock/list/share/"

    def run(self):
        # 沪市主板股票列表
        url = (
            "http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1"
        )
        resp = requests.get(url, headers=DEFAULT_SH_HEADER)
        self.download_stock_list(response=resp, exchange="sh")

        # 科创板股票列表
        url = (
            "http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=8"
        )
        resp = requests.get(url, headers=DEFAULT_SH_HEADER)
        self.download_stock_list(response=resp, exchange="sh")

        # 沪市退市股票列表
        url = (
            "http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=5"
        )
        resp = requests.get(url, headers=DEFAULT_SH_HEADER)
        self.download_stock_list(response=resp, exchange="sh", is_delist=True)

        # 深市股票列表
        url = "http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab1&random=0.20932135244582617"
        resp = requests.get(url, headers=DEFAULT_SZ_HEADER)
        self.download_stock_list(response=resp, exchange="sz")

        # 深市退市股票列表
        url = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1793_ssgs&TABKEY=tab2&random=0.03226506186367084"
        resp = requests.get(url, headers=DEFAULT_SZ_HEADER)
        self.download_stock_list(response=resp, exchange="sz", is_delist=True)

    def download_stock_list(self, response, exchange, is_delist=False):
        df = None
        if exchange == "sh":
            if is_delist:
                columns = ["原公司代码", "原公司简称", "上市日期", "终止上市日期"]
                parse_date_columns = ["上市日期", "终止上市日期"]
            else:
                columns = ["公司代码", "公司简称", "上市日期"]
                parse_date_columns = ["上市日期"]

            # 脏数据处理
            response.encoding = 'GB2312'
            response_text = response.text
            response_text = response_text.replace("*ST 云城", "*ST云城")
            
            df = pd.read_csv(
                io.StringIO(response_text),
                sep="\s+",
                encoding="GB2312",
                dtype=str,
                parse_dates=parse_date_columns,
            )
            if df is not None:
                df = df.loc[:, columns]

        elif exchange == "sz":
            if is_delist:
                columns = ["证券代码", "证券简称", "上市日期", "终止上市日期"]
                parse_date_columns = ["上市日期", "终止上市日期"]
                sheet_name = "终止上市"
            else:
                columns = ["A股代码", "A股简称", "A股上市日期"]
                parse_date_columns = ["A股上市日期"]
                sheet_name = "A股列表"

            df = pd.read_excel(io.BytesIO(response.content), sheet_name=sheet_name, dtype=str, parse_dates=parse_date_columns)
            if df is not None:
                df = df.loc[:, columns]

        if df is not None:
            if is_delist:
                df.columns = ["code", "name", "list_date", "end_date"]
            else:
                df.columns = ["code", "name", "list_date"]

            df = df.dropna(subset=["code"])
            df = df[~df['code'].str.startswith('20')]

            # handle the dirty data
            df["list_date"] = df["list_date"].apply(lambda x: to_pd_timestamp(x))
            if is_delist:
                df["end_date"] = df["end_date"].apply(lambda x: to_pd_timestamp(x))
            df["exchange"] = exchange
            df["entity_type"] = "stock"
            df["id"] = df[["entity_type", "exchange", "code"]].apply(lambda x: "_".join(x.astype(str)), axis=1)
            df["entity_id"] = df["id"]
            df["timestamp"] = df["list_date"]
            df = df.dropna(axis=0, how="any")
            df = df.drop_duplicates(subset=("id"), keep="last")

            df_to_db(df=df, data_schema=self.data_schema, provider=self.provider, force_update=False)
            # persist StockDetail too
            df_to_db(df=df, data_schema=StockDetail, provider=self.provider, force_update=False)

            self.logger.info(df.tail())
            self.logger.info("persist stock list successs")


if __name__ == "__main__":
    recorder = ExchangeStockMetaRecorder()
    recorder.run()
    
# the __all__ is generated
__all__ = ["ExchangeStockMetaRecorder"]
