#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, logging
import aiomysql


def log(sql, args=()):
    """
    记录sql操作
    :param sql: sql语句
    :param args: 格式化参数
    :return:None
    """
    logging.info('SQL: %s' % sql)


async def create_pool(loop, **kw):
    """
    创建全局连接池
    :param loop: 默认使用asyncio.get_event_loop()
    :param kw: 关键字参数,用于传递host, port, user, password, db等数据库连接参数
    :return:None
    """
    logging.info('create database connection pool')
    global __pool   # 将连接池定义为全局私有变量(obj)
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),       # 主机ip，默认本机
        port=kw.get('port', 3306),              # 端口，默认3306
        user=kw.get('user', 'root'),            # 用户，默认root
        password=kw.get('password', 'root'),    # 用户口令，默认root
        db=kw['db'],                            # 选择数据库
        charset=kw.get('charset', 'utf8'),      # 设置数据库编码，默认utf8
        autocommit=kw.get('autocommit', True),  # 设置自动提交事务，默认打开
        maxsize=kw.get('maxsize', 10),          # 设置最大连接数，默认10
        minsize=kw.get('minsize', 1),           # 设置最小连接数，默认1
        loop=loop,                              # 需要传递一个事件循环实例，若无特别声明，默认使用asyncio.get_event_loop()
    )


async def select(sql, args, size=None):
    """
    实现sql语句：select(查询)
    :param sql: 查询sql
    :param args: sql占位符的参数集
    :param size: 返回size条的记录, limit n
    :return: 返回查询的记录
    """
    log(sql, args)
    global __pool   # 使用全局变量__pool
    async with __pool.acquire as conn:  # 从连接池中获取一个连接，使用完后自动释放
        async with conn.cursor(aiomysql.DictCursor) as cursor:  # 创建一个游标，返回由dict组成的list，使用完后自动释放
            await cursor.execute(sql.replace('?', '%s'), args or ())    # 执行sql，mysql的占位符是%s，和python一样，为了coding方便，先用sql的占位符?写sql语句，最后执行时转换过来
            if size:
                rs = await cursor.fetchmany(size)   # 只读取size条记录
            else:
                rs = await cursor.fetchall()    # rs是一个list，每个元素都是一个dict，一个dict代表一行记录

        logging.info('rows returned: %s' % len(rs))
        return rs


async def execute(sql, args, autocommit=True):
    """
    实现sql语句：INSERT, UPDATE, DELETE.
    :param sql: sql语句
    :param args: sql占位符对应的参数集
    :param autocommit:  自动提交事务
    :return:
    """
    log(sql)
    async with __pool.acquire as conn:  # 获取一个连接
        if not autocommit:
            await conn.begin()  # 协程启动
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:  # 创建一个字典游标，返回字典类型为元素的list
                await cursor.execute(sql.replace('?', '%s') % args or ())   # 执行sql
                affected = cursor.rowcount  # 获得影响的行数

            if not autocommit:
                await conn.commit()     # 提交事务

        except BaseException as e:
            if not autocommit:
                await conn.rollback()   # 回滚当前启动的协程
            raise
        return affected     # 返回受影响的行数


def create_args_string(num):
    """
    按参数个数制作占位符字符串，用于生产sql
    :param num: 占位符个数
    :return: 占位符字符串
    """
    L = []
    for i in range(num):    # sql占位符是?，num是多少就插入多少个占位符
        L.append('?')
    return ', '.join(L)     # 将list拼接成字符串返回，例如：num=3:'?, ?, ?


class Field(object):
    """ 定义一个数据类型基类，用于衍生各种在orm中对应数据库的数据类型的类 """

    def __init__(self, name, column_type, primary_key, default):
        """ 参数：字段名，数据类型，主键，默认值 """
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        """ print(Field_object)时，返回类名Field，数据类型和字段名 """
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    """ 定义一个字符串类， 继承Field, 在orm中对应数据库的字符类型，默认varchar(100) """

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        """ 参数：字段名，主键，默认值，数据类型 """
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    """ 定义一个布尔类，继承Field, 在orm中对应数据库的布尔类型 """

    def __init__(self, name=None, default=False):
        """ 参数：字段名，默认值 """
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    """ 定义整型类，继承Field，在orm中对应数据库的整型类型，默认BIGINT """

    def __init__(self, name=None, column_type='bigint', primary_key=False, default=0):
        """ 参数：字段名，主键，默认值，数据类型 """
        super().__init__(name, column_type, primary_key, default)


class FloatField(Field):
    """ 定义浮点型类，继承Field, 在orm中对应数据库的 REAL 双精度浮动数类型 """

    def __init__(self, name=None, column_type='real', primary_key=False, default=0.0):
        """ 参数：字段名，主键，默认值，数据类型 """
        super().__init__(name, column_type, primary_key, default)


class TextFiedl(Field):
    """ 定义文本型类，继承Field, 在orm中对应数据的 TEXT 长文本类型 """

    def __init__(self, name=None, column_type='text', default=None):
        """ 参数：字段名，主键，默认值，数据类型 """
        super().__init__(name, column_type, False, default)


class ModelMetaclass(type):
    """ 定义一个元类，定制类与数据库的各种映射关系，对继承的类都可以使用orm """

    def __new__(cls, name, bases, attrs):
        """ 用metaclass=ModelMetaclass创建类时，通过这个方法生成类 """
        if name == 'Model':     # 定制Model类（排除本身）
            return type.__new__(cls, name, bases, attrs)    # 当前准备创建的类的对象、类名、类继承的基类集合、类的方法集合
        tableName = attrs.get('__table__', None) or name    # 获取表名，默认None, 或为类名
        logging.info('found model: %s (table: %s)' % (name, tableName))     # 类名、表名
        mappings = dict()   # 用于存储字段名和对应的数据类型
        fields = []     # 用于存储非主键的列
        primaryKey = None   # 用于主键查重，默认为None

        for k, v in attrs.items():      # 遍历attrs方法集合
            if isinstance(k, Field):    # 提取数据类的列
                logging.info(' found mappings: %s ==> %s' % (k, v))
                mappings[k] = v     # 存储列名和数据类型
                if v.primary_key:   # 查找主键和查重，有重复则抛出异常
                    if primaryKey:
                        raise BaseException('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)    # 存储非主键字段

        if not primaryKey:  # 主键不存在，则抛出异常
            raise BaseException('Primary key not found.')
        for k in mappings.keys():   # 过滤掉列，只剩方法
            attrs.pop(k)

        escaped_fields = list(map(lambda f: '`%s`' % f, fields))    # 给非主键列加``(可执行命令)区别于''(字符串效果)
        fields_str = ', '.join(escaped_fields)
        attrs['__mappings__'] = mappings    # 保持主键和列的映射关系
        attrs['__table__'] = tableName      # 表名
        attrs['__primary_key__'] = primaryKey   # 主键名
        attrs['__fields__'] = fields    # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, `%s` from `%s`' % (primaryKey, fields_str, tableName)    # 构造select语句，查询全表
        attrs['__insert__'] = 'insert into `%s` (`%s`, `%s`) value (%s)' % (tableName, fields_str, primaryKey, create_args_string(len(escaped_fields) + 1))     # 构造insert语句
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)     # 构造update语句，根据主键更新对应一行记录，？占位符，待传入更新值和主键
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)     # 构建delete语句，根据主键删除对应的行
        return type.__new__(cls, name, bases, attrs)    # 返回当前准备创建的类的对象、类的名字、类继承的基类集合、类的方法集合























