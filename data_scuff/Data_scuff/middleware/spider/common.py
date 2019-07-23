'''
Created on 29-Jun-2018

@author: srinivasan
'''
from scrapy import signals
from Data_scuff.utils.utils import Utils
from scrapy.exceptions import CloseSpider
import os


class CollectFailedUrlMiddleWare:
    
    def __init__(self, stats, settings):
        self.stats = stats
        self.settings = settings
        self.failed_urls = []
        self.failed_urls_info = []
        
    @classmethod
    def from_crawler(cls, crawler):
        o = cls(crawler.stats, crawler.settings)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o
    
    def  process_spider_input(self, response, spider):
        if response.status in self.settings.getlist('ERROR_CODES',
                [400, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414,
                 415, 416, 417, 418, 426, 428]):
            self.stats.inc_value('failed_url_count')
            self.failed_urls.append(response.url)
            self.failed_urls_info.append({'url':str(response.url),
                                      'status':str(response.status),
                                      'request':str(response.request),
                                      'headers':str(response.headers)})
    
    def process_spider_exception(self, response, exception, spider):
        ex_class = "{}.{}".format(exception.__class__.__module__, exception.__class__.__name__)
        self.stats.inc_value('downloader/exception_count', spider=spider)
        self.stats.inc_value('downloader/exception_type_count/%s' % ex_class, spider=spider)
    
    def spider_closed(self, spider):
        self.stats.set_value('failed_urls', ','.join(self.failed_urls))
        if self.failed_urls_info:
            import csv
            keys = self.failed_urls_info[0].keys()
            with open(os.path.join(self.settings['JOBDIR'],
                                   'failed_url_info.csv'), 'w') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(self.failed_urls_info)


class ParameterException(Exception):
    pass


class AddparameterProxyMiddleWare:

    def __init__(self, crawler, settings):
        self.settings = settings
        self.crawler = crawler
        
    @classmethod
    def from_crawler(cls, crawler):
        o = cls(crawler, crawler.settings)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        return o
    
    def spider_opened(self, spider):
        if hasattr(spider, 'proxyserver') and spider.proxyserver:
            if Utils.is_valid_url(spider.proxyserver):
                self.settings['CUSTOM_PROXY_URL'] = spider.proxyserver
            else:
                raise  CloseSpider("Proxyserver url({}) not valid.".format(spider.proxyserver))
