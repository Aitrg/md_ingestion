# -*- coding: utf-8 -*-
# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
'''
Created on 16-May-2018

@author: srinivasan
'''

import logging
from random import choice

from scrapy.exceptions import NotConfigured


logger = logging.getLogger(__name__)


class RotateUserAgentMiddleware:
    
    def __init__(self, user_agents):
        self.user_agents = user_agents

    @classmethod
    def from_crawler(cls, crawler):
        user_agents = crawler.settings.get('USER_AGENT_CHOICES', [])
        if not user_agents:
            raise NotConfigured("USER_AGENT_CHOICES not set or empty")
        o = cls(user_agents)
        return o

    def process_request(self, request, spider):
        if self.user_agents:
            user_agent = choice(self.user_agents)
            request.headers.setdefault(b'User-Agent', user_agent)
            