'''
Created on 19-May-2018

@author: srinivasan
'''
import logging
import os

from scrapy import signals
from scrapy.exceptions import NotConfigured

from Data_scuff.utils.fileutils import FileUtils

logger = logging.getLogger(__name__)


class ClearDownloadPath:
    
    def __init__(self, settings):
        self.settings = settings

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('CLEAN_DOWNLOAD_PATH_ENABLED'):
            raise NotConfigured
        ext = cls(crawler.settings)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext
    
    def spider_closed(self, spider):
        logger.info("started to Clean the Download Folder in spider %s", spider.name)
        download_path = os.path.join(self.settings.get('SELENIUM_DOWNLOAD_PATH', '/tmp'), spider.name)
        if  FileUtils.isExist(download_path):
            FileUtils.deletePath(download_path)


class SpiderOpenCloseLogging:

    def __init__(self, item_count):
        self.item_count = item_count
        self.items_scraped = 0

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('MYEXT_ENABLED'):
            raise NotConfigured
        item_count = crawler.settings.getint('MYEXT_ITEMCOUNT', 1000)
        ext = cls(item_count)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        return ext

    def spider_opened(self, spider):
        logger.info("opened spider %s", spider.name)

    def spider_closed(self, spider):
        logger.info("closed spider %s", spider.name)

    def item_scraped(self, item, spider):
        self.items_scraped += 1
        if self.items_scraped % self.item_count == 0:
            logger.info("scraped %d items", self.items_scraped)
            
