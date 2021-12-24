# -*- coding: utf-8 -*-

import requests

from tenacity import retry, wait_fixed, stop_after_attempt

from zvt.contract.api import get_entities
from zvt.contract.recorder import Recorder
from zvt.recorders.exchange.exchange_stock_meta_recorder import ExchangeStockMetaRecorder
from zvt.recorders.em.meta.em_stock_meta_recorder import EMStockRecorder
from zvt.recorders.xbx.xbx_stock_list_recorder import XbxStockListRecorder
from zvt.domain.meta.stock_meta import StockDetail, Stock
from zvt.utils.time_utils import to_pd_timestamp
from zvt.utils.utils import to_float, pct_to_float


class EastmoneyStockRecorder(ExchangeStockMetaRecorder):
    data_schema = Stock
    provider = "eastmoney"

class EastmoneyStockDetailRecorder(Recorder):
    provider = "eastmoney"
    data_schema = StockDetail

    def __init__(self, force_update=False, sleeping_time=0.0, code=None, codes=None) -> None:
        super().__init__(force_update, sleeping_time)

        # Get stock list from xbx data directory
        XbxStockListRecorder().run()

        # Get stock list from eastmoney
        EMStockRecorder().run()

        # Get stock list from exchange
        EastmoneyStockRecorder().run()

        if codes is None and code is not None:
            self.codes = [code]
        else:
            self.codes = codes
        filters = None
        if not self.force_update:
            filters = [StockDetail.profile.is_(None)]

        self.entities = get_entities(
            session=self.session,
            entity_schema=StockDetail,
            exchanges=None,
            codes=self.codes,
            filters=filters,
            return_type="domain",
            provider=self.provider,
        )

    def run(self):
        for security_item in self.entities:
            assert isinstance(security_item, StockDetail)

            if security_item.exchange == "sh":
                fc = "{}01".format(security_item.code)
            if security_item.exchange == "sz":
                fc = "{}02".format(security_item.code)

            # 基本资料
            param = {"color": "w", "fc": fc, "SecurityCode": "SZ300059"}
            resp_json = self.fetch_basic_information(param)

            name = resp_json['SecurityNameA']
            if (security_item.name is None or len(security_item.name) == 0) and len(name) > 0:
                security_item.name = name

            security_item.previous_name = resp_json['PreviousName']
            security_item.profile = resp_json['CompRofile']
            security_item.main_business = resp_json['MainBusiness']
            security_item.date_of_establishment = to_pd_timestamp(resp_json['FoundDate'])

            # 关联行业
            industries = ",".join(resp_json["Industry"].split("-"))
            security_item.industries = industries

            # 关联概念
            security_item.concept_indices = resp_json["Block"]

            # 关联地区
            security_item.area_indices = resp_json["Provice"]

            # 发行相关
            param = {"color": "w", "fc": fc}
            resp_json = self.fetch_issue_information(param)

            listed_date_string = resp_json['ListedDate']
            # 未上市新股为 '--'
            if listed_date_string != '--':
                security_item.list_date = to_pd_timestamp(listed_date_string)
                security_item.timestamp = to_pd_timestamp(listed_date_string)
            security_item.issue_pe = to_float(resp_json['PEIssued'])
            security_item.price = to_float(resp_json['IssuePrice'])
            security_item.issues = to_float(resp_json['ShareIssued'])
            security_item.raising_fund = to_float((resp_json['NetCollection']))
            security_item.net_winning_rate = pct_to_float(resp_json['LotRateOn'])

            self.session.commit()
            self.logger.info('finish recording stock meta for:{}'.format(security_item.code))

    @staticmethod
    @retry(wait=wait_fixed(20), stop=stop_after_attempt(10))
    def fetch_basic_information(param):
        resp = requests.post('https://emh5.eastmoney.com/api/GongSiGaiKuang/GetJiBenZiLiao', json=param)
        resp.encoding = 'utf8'
        resp_json = resp.json()['Result']['JiBenZiLiao']

        return resp_json

    @staticmethod
    @retry(wait=wait_fixed(20), stop=stop_after_attempt(10))
    def fetch_issue_information(param):
        resp = requests.post('https://emh5.eastmoney.com/api/GongSiGaiKuang/GetFaXingXiangGuan', json=param)
        resp.encoding = 'utf8'
        resp_json = resp.json()['Result']['FaXingXiangGuan']

        return resp_json


if __name__ == "__main__":
    # init_log('china_stock_meta.log')
    StockDetail.record_data(codes=['000338', '000777'], provider='eastmoney')

# the __all__ is generated
__all__ = ['EastmoneyStockRecorder', 'EastmoneyStockDetailRecorder']
