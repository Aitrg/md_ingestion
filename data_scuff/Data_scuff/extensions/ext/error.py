'''
Created on 09-Feb-2019

@author: srinivasan
'''
import csv
import datetime
import errno
import io
import os
from scrapy import signals
from scrapy.utils.python import to_native_str
from six import StringIO
import six
import time


class CollectSpiderError:
    
    def __init__(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        self.failure_msg = []
        self._headers_not_written = True
        self.failure_chunksize = 10
        self.encoding = 'utf-8'
        self.filename = os.path.join(self.settings.get('REQUEST_TRACKING_PATH'),
                                      self.settings.get('JIRA_ID'),
                                      'logicalfailure_{}.csv'
                                      .format(datetime.datetime.now().strftime ("%Y%m%d")))
        if not os.path.exists(os.path.dirname(self.filename)):
            try:
                os.makedirs(os.path.dirname(self.filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        self.file = open(self.filename, 'w+b')
        self.stream = io.TextIOWrapper(
            self.file,
            line_buffering=False,
            write_through=True,
            encoding=self.encoding
        ) if six.PY3 else self.file
        self.csv_writer = csv.writer(self.stream, delimiter='|', quotechar='"',
                                      quoting=csv.QUOTE_MINIMAL)
    
    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler)
        crawler.signals.connect(obj.spider_error,
                                 signal=signals.spider_error)
        crawler.signals.connect(obj.spider_closed,
                                signals.spider_closed)
        return obj

    def spider_error(self, failure, response, spider):
        traceback = StringIO()
        failure.printTraceback(file=traceback)
        msg = {
            'unique_id':response.request.meta['unique_id'],
            'time': time.time(),
            'status': response.status,
            'url': response.url,
            'headers': dict(response.headers),
            'body': response.body,
            'spider': spider.name,
            'failure': failure,
            'meta':dict(response.request.meta),
            'traceback': "\n".join(traceback.getvalue().split("\n")[-5:]),
            }
        self.failure_msg.append(msg)
        if len(self.failure_msg) > self.failure_chunksize:
            self.crawler.engine.pause()
            self.__writeTrackingUrl()
            self.crawler.engine.unpause()
            
    def spider_closed(self, spider):
        if len(self.failure_msg) > 0:
            self.__writeTrackingUrl()

    def _build_row(self, values):
        for s in values:
            try:
                yield to_native_str(s, self.encoding)
            except TypeError:
                yield s
    
    def __writeTrackingUrl(self):
        if self._headers_not_written:
            self._headers_not_written = False
            len_val = [len(i) for i in self.failure_msg]
            position = len_val.index(max(len_val))
            row = list(self._build_row(self.failure_msg[position].keys()))
            self.csv_writer.writerow(row)
        for val in self.failure_msg:
            self.csv_writer.writerow(list(self._build_row(val.values())))
        self.failure_msg.clear()
