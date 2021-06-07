# -*- coding: utf-8 -*-

from sqlalchemy import Column, String, DateTime, Float, Integer
from sqlalchemy.ext.declarative import declarative_base

from zvt.contract import Mixin
from zvt.contract.register import register_schema

AnnouncementBase = declarative_base()


class Announcement(AnnouncementBase, Mixin):
    __tablename__ = 'announcements'

    code = Column(String(length=32))
    name = Column(String(length=32))

    # 增持
    category = Column(String(length=32))
    # 金额
    amount = Column(Float)
    stockholders_meeting_date = Column(DateTime)
    type = Column(Integer)
    # 星级评价
    star = Column(Float)
    # 公告标题
    title = Column(String(length=1024))
    # 当前状态
    status = Column(Integer)
    # 公告编号
    announcement_id = Column(String(length=32))
    # 均价
    stock_price = Column(Float)
    data_type = Column(String(length=32))
    # url 地址
    url_component = Column(String(length=128))
    evt_type = Column(String(length=32))
    # 占股比
    percent = Column(Float)
    trade_start_date = Column(DateTime)


register_schema(providers=['joudou'], db_name='announcements', schema_base=AnnouncementBase)

__all__ = ['Announcement']
