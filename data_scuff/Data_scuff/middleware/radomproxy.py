# -*- coding: utf-8 -*-
# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
'''
Created on 16-May-2018

@author: srinivasan
'''

import base64
import logging
import random
import re

logger = logging.getLogger(__name__)


class Mode:
    RANDOMIZE_PROXY_EVERY_REQUESTS, RANDOMIZE_PROXY_ONCE = range(2)


class RandomProxy:

    def __init__(self, settings):
        self.proxies = {}
        self.settings = settings
        if settings.get('CUSTOM_PROXY_URL', None):
            custom_proxy = settings.get('CUSTOM_PROXY_URL', None)
            parts = re.match('(\w+://)(\w+:\w+@)?(.+)', custom_proxy)
            if not parts:
                raise Exception("not a valid url {}".format(custom_proxy))
            if parts.group(2):
                user_pass = parts.group(2)[:-1]
            else:
                user_pass = ''
            self.proxies[parts.group(1) + parts.group(3)] = user_pass
            self.mode = Mode.RANDOMIZE_PROXY_ONCE
            self.chosen_proxy = random.choice(list(self.proxies.keys()))
        else:
            self.proxy_list = settings.get('PROXY_LIST')
            self.mode = settings.get('PROXY_MODE')
            fin = open(self.proxy_list)
            for line in fin.readlines():
                parts = re.match('(\w+://)(\w+:\w+@)?(.+)', line)
                if not parts:
                    continue
                if parts.group(2):
                    user_pass = parts.group(2)[:-1]
                else:
                    user_pass = ''
                self.proxies[parts.group(1) + parts.group(3)] = user_pass
            fin.close()
            if self.mode == Mode.RANDOMIZE_PROXY_ONCE:
                self.chosen_proxy = random.choice(list(self.proxies.keys()))

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        if 'proxy' in request.meta or  \
            self.settings.getbool('RANDOM_PROXY_DISABLED', False):
            return
        if self.mode == Mode.RANDOMIZE_PROXY_EVERY_REQUESTS:
            proxy_address = random.choice(list(self.proxies.keys()))
            # print("++++++++++++++++ random_proxy1:", proxy_address)
        else:
            proxy_address = self.chosen_proxy
            # print("++++++++++++++++ random_proxy2:", proxy_address)
        proxy_user_pass = self.proxies.get(proxy_address, None)
        if proxy_user_pass:
            request.meta['proxy'] = self.__changeProtocolAndget(request.url, proxy_address)
            basic_auth = 'Basic ' + base64.b64encode(proxy_user_pass.encode()).decode()
            request.headers['Proxy-Authorization'] = basic_auth
    
    def __changeProtocolAndget(self, __url, proxy_address):
        from urllib.parse import urlsplit, urlunsplit
        url = list(urlsplit(proxy_address))
        url[0] = urlsplit(__url).scheme
        return urlunsplit(url)
    
    def process_exception(self, request, exception, spider):
        if 'proxy' in request.meta:
            proxy = request.meta['proxy']
            logger.info('Removing failed proxy <%s>, %d proxies left' % (proxy, len(self.proxies)))
            try:
                if proxy in self.proxies:
                    del self.proxies[proxy]
                    del request.meta['proxy']
                self.chosen_proxy = random.choice(list(self.proxies.keys()))
            except ValueError:
                pass
