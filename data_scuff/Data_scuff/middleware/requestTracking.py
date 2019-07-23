'''
Created on 05-Feb-2019

@author: srinivasan
'''
import csv
import datetime
import errno
import io
import os
from scrapy import signals
from scrapy.utils.python import to_native_str
import six
import uuid


class RequestResponseTracking:
    
    def __init__(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        self.tracks = []
        self.order_of_keys = ['unique_id', 'type', 'url', 'method', 'search_key',
                               'callback', 'status'] + self.settings.getlist('TRACKING_OPTIONAL_PARAMS', [])
        self.tracking_chunksize = self.settings.getint('TRACKING_CHUNK_SIZE', default=200)
        self.encoding = 'utf-8'
        self._headers_not_written = True
    
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline
    
    """
    """

    def process_request(self, request, spider):
        request.meta['unique_id'] = str(uuid.uuid1())
        opt = request.meta.get('optional', {})
        req = {'unique_id':request.meta['unique_id'],
             'type':'request',
             'url':request.url,
             'method':request.method,
             'search_key':request.meta.get('search_key', ''),
             'callback':self.__getCallBackName(request.callback) if request.callback else '',
             'status':None}
        self.tracks.append({**req, **opt})

    """
    """

    def process_response(self, request, response, spider):
        if len(self.tracks) > self.tracking_chunksize:
            self.crawler.engine.pause()
            self.__writeTrackingUrl(spider)
            self.crawler.engine.unpause()
        else:
            opt = request.meta.get('optional', {})
            req = {'unique_id':request.meta.get('unique_id', ''),
                 'type':'response',
                 'url':response.url,
                 'method':request.method,
                 'search_key':request.meta.get('search_key', ''),
                 'callback':self.__getCallBackName(request.callback) if request.callback else '',
                 'status':response.status}
            self.tracks.append({**req, **opt})
        return response
    
    """
    
    """

    def __getCallBackName(self, callback):
        if hasattr(callback, '__name__'):
            return  callback.__name__
        elif hasattr(callback, 'func'):
            return callback.func.__name__
        else:
            return callback
    
    """
    
    """

    def _build_row(self, values):
        for s in values:
            try:
                yield to_native_str(s, self.encoding)
            except TypeError:
                yield s
    
    """
    spider_closed - closed the spider
    """

    def spider_closed(self, spider):
        if len(self.tracks) > 0:
            self.__writeTrackingUrl(spider)
        if hasattr(self, 'file') and  self.file:
            self.file.close()

    """
    Write Tracking URL
    """

    def __writeTrackingUrl(self, spider):
        if self._headers_not_written:
            suffix = datetime.datetime.now().strftime ("%Y%m%d")
            if  hasattr(spider, 'start') and  spider.start:
                remove_spec = lambda x: ''.join(e for e in x if e.isalnum())
                suffix = '{}_{}_{}'.format(suffix, remove_spec(spider.start), remove_spec(spider.end))
            filename = os.path.join(self.settings.get('REQUEST_TRACKING_PATH'),
                                          self.settings.get('JIRA_ID'),
                                          'trackingRequest_{}.csv'
                                          .format(suffix))
            if not os.path.exists(os.path.dirname(filename)):
                try:
                    os.makedirs(os.path.dirname(filename))
                except OSError as exc:
                    if exc.errno != errno.EEXIST:
                        raise
            self.file = open(filename, 'w+b')
            stream = io.TextIOWrapper(
                self.file,
                line_buffering=False,
                write_through=True,
                encoding=self.encoding
            ) if six.PY3 else self.file
            self.csv_writer = csv.writer(stream, delimiter='|', quotechar='"',
                                          quoting=csv.QUOTE_MINIMAL)
            self.csv_writer.writerow(self.order_of_keys)
            self._headers_not_written = False
        for val in self.tracks:
            self.csv_writer.writerow(list(self._build_row(self.__getrowValues(val))))
        self.file.flush()
        self.tracks.clear()
    
    """
    Get Row values
    """

    def __getrowValues(self, val_dict):
        return [ val_dict.pop(key, '') for key in self.order_of_keys]
