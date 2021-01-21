# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Float

from zvt.contract.register import register_schema
from zvt.domain.quotes import StockKdataCommon

KdataBase = declarative_base()


class Stock1dKdata(KdataBase, StockKdataCommon):
    __tablename__ = 'stock_1d_kdata'

    pre_close = Column(Float)


register_schema(providers=['joinquant', 'joudou'], db_name='stock_1d_kdata', schema_base=KdataBase, entity_type='stock')

# the __all__ is generated
__all__ = ['Stock1dKdata']