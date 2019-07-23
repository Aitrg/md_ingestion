'''
Created on 13-Jun-2018

@author: srinivasan
'''

import logging

from scrapy.exceptions import IgnoreRequest

logger = logging.getLogger(__name__)


class NYStatelicIgnoreErrorResponse:
    
    stats_name = 'dropErrorResponse'
    
    def __init__(self, stats):
        self.stats = stats
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)
    
    def process_response(self, request, response, spider):
        if response.xpath('//title/text()').extract_first() is not None and 'error' in response.xpath('//title/text()').extract_first().lower():
            logger.info("Ignore this {} URL because of error msg".format(response.url))
            self.stats.inc_value('{}/Error'.format(self.stats_name), spider=spider)
            raise IgnoreRequest("Ignore this {} URL because of error msg".format(response.url))
        return response
