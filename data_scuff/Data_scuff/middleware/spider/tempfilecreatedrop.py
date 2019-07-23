# -*- coding: utf-8 -*-
# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

'''
Created on 19-May-2018

@author: srinivasan
'''

from Data_scuff.utils.fileutils import FileUtils
from scrapy import signals


class CreateDropTempFolderMiddleware:
    
    def __init__(self, settings):
        self._settings = settings
        self.path = settings.get('TEMP_FILE_PATH')
            
    @classmethod
    def from_crawler(cls, crawler):
        o = cls(crawler.settings)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o 
    
    def spider_closed(self, spider):
        if  self.path and  FileUtils.isExist(self.path):
            FileUtils.deletePath(self.path)
    
    def spider_opened(self, spider):
        if not self.path or not FileUtils.isExist(self.path):
            self.path = FileUtils.createTempFolder()
            self._settings.overrides['TEMP_FILE_PATH'] = self.path