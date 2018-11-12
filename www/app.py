#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web


def index(request):
    """一个处理函数，返回带HTML代码的响应 """
    return web.Response(body=b'<h1>Awesome</h1>', content_type='text/html')


async def init(loop):
    """服务器运行程序：创建web实例，该实例绑定路由和处理函数，运行服务器，监听端口请求，送到路由处理"""
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 8080)
    logging.info('Server started at http://127.0.0.1:8080')
    return srv


loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
