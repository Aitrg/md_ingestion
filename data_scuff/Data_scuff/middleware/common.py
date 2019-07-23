# -*- coding: utf-8 -*-
# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
'''
Created on 05-Jun-2018

@author: srinivasan
'''

import logging
from scrapy import signals
from scrapy.http.response.html import HtmlResponse
from scrapy.utils.response import response_status_message
from selenium.webdriver.support.wait import WebDriverWait
import time
import tldextract

from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.exceptions import IgnoreRequest, NotConfigured

from Data_scuff.extensions.exception.seleniumexception import SeleniumExtensionsException
from Data_scuff.extensions.request import SeleniumRequest
from Data_scuff.selenium.driver import Driver

logger = logging.getLogger(__name__)


class SeleniumMiddleware:
    
    def __init__(self, driver_name, driver_type): 
        try:
            from shutil import which
            self.driver = Driver(driver_type).getDriver(executable_path=which(driver_name),
                                                        run_headless=True, load_images=False)
        except Exception as e:
            raise SeleniumExtensionsException("problem to get selenium driver-{}", e)
    
    @classmethod
    def from_crawler(cls, crawler):
        driver_name = crawler.settings.get('SELENIUM_DRIVER_NAME')
        driver_type = crawler.settings.get('SELENIUM_DRIVER_TYPE')
        if not driver_name :
            raise NotConfigured('SELENIUM_DRIVER_NAME and SELENIUM_DRIVER_TYPE  must be set')
        middleware = cls(driver_name=driver_name, driver_type=driver_type)
        crawler.signals.connect(middleware.spider_closed, signals.spider_closed)
        return middleware
    
    def process_request(self, request, spider):
        if not isinstance(request, SeleniumRequest):
            return request
        self.driver.get(request.url)

        for cookie_name, cookie_value in request.cookies.items():
            self.driver.add_cookie(
                {
                    'name': cookie_name,
                    'value': cookie_value
                }
            )
        if request.wait_until:
            WebDriverWait(self.driver, request.wait_time).until(
                request.wait_until
            )
        body = str.encode(self.driver.page_source)
        request.meta.update({'driver': self.driver})
        return HtmlResponse(
            self.driver.current_url,
            body=body,
            encoding='utf-8',
            request=request)
    
    def spider_closed(self):
        self.driver.quit()


class BlockDomainMiddleWare:
    
    def __init__(self, settings):
        self.settings = settings
    
    def process_request(self, request, spider):
        if self.settings.get('BLOCK_DOMAINS', []) or (hasattr(spider, 'blocked_domains') and spider.blocked_domains):
            blockedDomains = self.settings.getlist('BLOCK_DOMAINS', []) + spider.blocked_domains
            domain_obj = tldextract.extract(request.url)
            domain = domain_obj.registered_domain
            logger.info("Current domain {}".format(domain))
            if(domain in blockedDomains):
                logger.info("Blocked domain: {} (url: {})".format(domain, request.url))
                raise IgnoreRequest("URL blocked: {}".format(request.url))
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

"""
Don't allow the scrapy the already scrap data based on the url 
"""


class DropDuplicateUrlMiddleWare:
    
    stats_name = 'dropDuplicateUrl'

    def __init__(self, stats):
        self.stats = stats
        self.urls_seen = set()
    
    def process_request(self, request, spider):
        if request.url not in getattr(spider, 'start_urls', []) + getattr(spider, 'dont_dropUrls', []):
            import hashlib
            value = hashlib.md5(request.url.encode('utf-8')).hexdigest()
            if value in self.urls_seen:
                self.stats.inc_value('{}/Error'.format(self.stats_name), spider=spider)
                logger.info("Ignore this {} URL it's already scraped".format(request.url))
                raise IgnoreRequest("Ignore this {} URL it's already scraped".format(request.url))
            else:
                self.urls_seen.add(value)
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats)

"""
Delay the Request until page download completed
"""


class DelayedRequestMiddleware:
    
    def __init__(self, settings):
        self.settings = settings
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)
    
    def process_request(self, request, spider):
        if self.settings.getbool('DEALY_REQ_ENABLED', False) or \
            request.meta.get('delay_request', None):
            self.crawler.engine.pause()
            time.sleep(self.settings.getint("DEALY_SECONDS", default=30))
            self.crawler.engine.unpause() 


class TooManyRequestsRetryMiddleware(RetryMiddleware):

    def __init__(self, crawler):
        super(TooManyRequestsRetryMiddleware, self).__init__(crawler.settings)
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        elif response.status in [429, 500, 405, 403, 409]:
            self.crawler.engine.pause()
            time.sleep(60)
            self.crawler.engine.unpause()
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        elif response.status in [501]:
            self.crawler.engine.pause()
            time.sleep(300)
            self.crawler.engine.unpause()
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response

        elif response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response 
