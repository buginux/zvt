# -*- coding: utf-8 -*-
import pandas as pd
import requests
from tenacity import retry, wait_fixed, stop_after_attempt

from zvt.api.utils import to_report_period_type
from zvt.contract.api import get_data
from zvt.domain import FinanceFactor, ReportPeriod
from zvt.recorders.eastmoney.common import company_type_flag, get_fc, EastmoneyTimestampsDataRecorder, \
    call_eastmoney_api, get_from_path_fields
from zvt.recorders.joinquant.common import to_jq_entity_id
from zvt.utils.pd_utils import index_df
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, to_pd_timestamp


def to_jq_report_period(timestamp):
    the_date = to_pd_timestamp(timestamp)
    report_period = to_report_period_type(timestamp)
    if report_period == ReportPeriod.year.value:
        return '{}'.format(the_date.year)
    if report_period == ReportPeriod.season1.value:
        return '{}q1'.format(the_date.year)
    if report_period == ReportPeriod.half_year.value:
        return '{}q2'.format(the_date.year)
    if report_period == ReportPeriod.season3.value:
        return '{}q3'.format(the_date.year)

    assert False


class BaseChinaStockFinanceRecorder(EastmoneyTimestampsDataRecorder):
    finance_report_type = None
    data_type = 1
    report_type = 1

    timestamps_fetching_url = 'https://emh5.eastmoney.com/api/CaiWuFenXi/GetCompanyReportDateList'
    timestamp_list_path_fields = ['CompanyReportDateList']
    timestamp_path_fields = ['ReportDate']

    def __init__(self, exchanges=None, entity_ids=None, code=None, codes=None, day_data=False,
                 force_update=False, sleeping_time=5, real_time=False,
                 fix_duplicate_way='add', start_timestamp=None, end_timestamp=None) -> None:
        super().__init__(force_update, sleeping_time, exchanges, entity_ids, code, codes, day_data, real_time=real_time,
                         fix_duplicate_way=fix_duplicate_way, start_timestamp=start_timestamp,
                         end_timestamp=end_timestamp)

        try:
            self.fetch_jq_timestamp = True
        except Exception as e:
            self.fetch_jq_timestamp = False
            self.logger.warning(
                f'joinquant account not ok,the timestamp(publish date) for finance would be not correct', e)

    def init_timestamps(self, entity):
        param = {
            "color": "w",
            "fc": get_fc(entity),
            "DataType": self.data_type
        }

        if self.finance_report_type == 'LiRunBiaoList' or self.finance_report_type == 'XianJinLiuLiangBiaoList':
            param['ReportType'] = self.report_type

        timestamp_json_list = call_eastmoney_api(url=self.timestamps_fetching_url,
                                                 path_fields=self.timestamp_list_path_fields,
                                                 param=param)

        if self.timestamp_path_fields:
            timestamps = [get_from_path_fields(data, self.timestamp_path_fields) for data in timestamp_json_list]

        return [to_pd_timestamp(t) for t in timestamps]

    def generate_request_param(self, security_item, start, end, size, timestamps):
        if len(timestamps) <= 10:
            param = {
                "color": "w",
                "fc": get_fc(security_item),
                "corpType": company_type_flag(security_item),
                # 0 means get all types
                "reportDateType": 0,
                "endDate": '',
                "latestCount": size
            }
        else:
            param = {
                "color": "w",
                "fc": get_fc(security_item),
                "corpType": company_type_flag(security_item),
                # 0 means get all types
                "reportDateType": 0,
                "endDate": to_time_str(timestamps[10]),
                "latestCount": 10
            }

        if self.finance_report_type == 'LiRunBiaoList' or self.finance_report_type == 'XianJinLiuLiangBiaoList':
            param['reportType'] = self.report_type

        return param

    def generate_path_fields(self, security_item):
        comp_type = company_type_flag(security_item)

        if comp_type == "3":
            return ['{}_YinHang'.format(self.finance_report_type)]
        elif comp_type == "2":
            return ['{}_BaoXian'.format(self.finance_report_type)]
        elif comp_type == "1":
            return ['{}_QuanShang'.format(self.finance_report_type)]
        elif comp_type == "4":
            return ['{}_QiYe'.format(self.finance_report_type)]

    def record(self, entity, start, end, size, timestamps):
        # different with the default timestamps handling
        param = self.generate_request_param(entity, start, end, size, timestamps)
        self.logger.info('request param:{}'.format(param))

        return self.api_wrapper.request(url=self.url, param=param, method=self.request_method,
                                        path_fields=self.generate_path_fields(entity))

    def get_original_time_field(self):
        return 'ReportDate'

    @retry(wait=wait_fixed(20), stop=stop_after_attempt(10))
    def get_report_date(self, security_item):
        notice_date_url = 'http://datacenter.eastmoney.com/api/data/get?type=RPT_LICO_FN_CPD&sty=SECURITY_CODE,SECURITY_NAME_ABBR,TRADE_MARKET_CODE,TRADE_MARKET,SECURITY_TYPE_CODE,SECURITY_TYPE,UPDATE_DATE,REPORTDATE,BASIC_EPS,TOTAL_OPERATE_INCOME,PARENT_NETPROFIT,YSTZ,SJLTZ,NOTICE_DATE,ORG_CODE,TRADE_MARKET_ZJG,ISNEW,QDATE,DATATYPE,DATAYEAR,DATEMMDD&p=1&ps=200&filter=(SECURITY_CODE%3D%22{}%22)&st=REPORTDATE,EITIME&sr=-1,-1&source=DataCenter&client=WEB'
        url = notice_date_url.format(security_item.code)

        data = requests.get(url)
        data.raise_for_status()

        json_data = data.json()

        success = json_data.get('success', False)
        if not success:
            return None

        result_data = json_data['result']['data']
        df = pd.DataFrame(result_data)

        return df

    def fill_timestamp(self, security_item, records):
        try:
            df = self.get_report_date(security_item)

            if pd_is_not_null(df):
                df = df.set_index('REPORTDATE')
                for record in records:
                    report_date_str = record.report_date.strftime('%Y-%m-%d 00:00:00')
                    if report_date_str in df.index:
                        record.timestamp = to_pd_timestamp(df.at[report_date_str, 'NOTICE_DATE'])
                        record.update_date = to_pd_timestamp(df.at[report_date_str, 'UPDATE_DATE'])

                self.logger.info('fill {} {} report notice date'.format(self.data_schema, security_item.id))
                self.session.commit()
        except Exception as e:
            self.logger.error(e)

    def on_finish_entity(self, entity):
        super().on_finish_entity(entity)

        # fill the timestamp for report published date
        the_data_list = get_data(data_schema=self.data_schema,
                                 provider=self.provider,
                                 entity_id=entity.id,
                                 order=self.data_schema.timestamp.asc(),
                                 return_type='domain',
                                 session=self.session,
                                 filters=[self.data_schema.timestamp == self.data_schema.report_date])
        if the_data_list:
            if self.data_schema == FinanceFactor:
                self.fill_timestamp(entity, the_data_list)
            else:
                df = FinanceFactor.query_data(entity_id=entity.id,
                                              columns=[FinanceFactor.timestamp, FinanceFactor.report_date,
                                                       FinanceFactor.id],
                                              filters=[FinanceFactor.timestamp != FinanceFactor.report_date,
                                                       FinanceFactor.report_date >= the_data_list[0].report_date,
                                                       FinanceFactor.report_date <= the_data_list[-1].report_date, ])

                if pd_is_not_null(df):
                    index_df(df, index='report_date', time_field='report_date')

                for the_data in the_data_list:
                    if (df is not None) and (not df.empty) and the_data.report_date in df.index:
                        the_data.timestamp = df.at[the_data.report_date, 'timestamp']
                        self.logger.info(
                            'db fill {} {} timestamp:{} for report_date:{}'.format(self.data_schema, entity.id,
                                                                                   the_data.timestamp,
                                                                                   the_data.report_date))
                        self.session.commit()
                    else:
                        # self.logger.info(
                        #     'waiting jq fill {} {} timestamp:{} for report_date:{}'.format(self.data_schema,
                        #                                                                    security_item.id,
                        #                                                                    the_data.timestamp,
                        #                                                                    the_data.report_date))
                        self.fill_timestamp(entity, the_data_list)


# the __all__ is generated
__all__ = ['to_jq_report_period', 'BaseChinaStockFinanceRecorder']
