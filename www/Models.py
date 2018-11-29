#!/usr/bin/python3
# -*- coding: utf-8 -*-

import time, uuid
from orm import Model, StringField, BooleanField, FloatField, TextField


def next_id():
    return '%015d%s000' % (int(time.time() * 1000), uuid.uuid4().hex)


class User(Model):
    """ User类映射MySQL数据库中的User表 """
    __table__ = 'users'  # 表名
    id = StringField(primary_key=True, default=next_id, ddl='varchar(50)')  # 主键
    email = StringField(ddl='varchar(50)')  # 作为登陆账号
    passwd = StringField(ddl='varchar(50)')  # 列
    admin = BooleanField()  # 列
    name = StringField(ddl='varchar(50)')  # 列
    image = StringField(ddl='varchar(500)')  # 列
    created_at = FloatField(default=time.time)  # 列
