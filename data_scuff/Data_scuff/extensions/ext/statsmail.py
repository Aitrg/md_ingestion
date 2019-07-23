'''
Created on 10-Aug-2018

@author: srinivasan
'''
from collections import defaultdict
import datetime
import gzip
from io import BytesIO
import logging

from jinja2.environment import Environment
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.mail import MailSender
from scrapy.utils.serialize import ScrapyJSONEncoder

from Data_scuff import  config

logger = logging.getLogger(__name__)


def format_size(size):
    for x in ['bytes', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return "{:3.1f} {}".format(size, x)
        size /= 1024.0


class GzipCompressor(gzip.GzipFile):
    
    extension = '.gz'
    mimetype = 'application/gzip'

    def __init__(self):
        super(GzipCompressor, self).__init__(fileobj=PlainCompressor(), mode='wb')
        self.read = self.fileobj.read


class PlainCompressor(BytesIO):
    extension = ''
    mimetype = 'text/plain'

    def read(self, *args, **kwargs):
        self.seek(0)
        return BytesIO.read(self, *args, **kwargs)

    @property
    def size(self):
        return len(self.getvalue())


class StatsMailSend:
    
    def __init__(self, crawler, compressor):
        self.stats = crawler.stats
        self.settings = crawler.settings
        self.bots_name = crawler.settings.get('BOT_NAME')
        self.files = defaultdict(compressor)
        self.encoder = ScrapyJSONEncoder()

    @classmethod
    def from_crawler(cls, crawler):
        compression = crawler.settings.get('STATUSMAILER_COMPRESSION')
        if not compression:
            compressor = PlainCompressor
        elif compression.lower().startswith('gz'):
            compressor = GzipCompressor
        else:
            raise NotConfigured
        instance = cls(crawler, compressor)
        crawler.signals.connect(instance.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(instance.item_dropped, signal=signals.item_dropped)
        crawler.signals.connect(instance.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(instance.spider_error, signal=signals.spider_error)
        crawler.signals.connect(instance.spider_closed, signal=signals.spider_closed)
        return instance
    
    def spider_opened(self, spider):
        logger.info("spider started to send Mail %s", spider.name)
        self.start_time = datetime.datetime.now()
    
    def item_scraped(self, item, response, spider):
        self.files[spider.name + '-items.json'].write(bytes(self.encoder.encode(item), 'utf-8'))
    
    def item_dropped(self, item, response, exception, spider):
        self.files[spider.name + '-dropped-items.json'].write(bytes(self.encoder.encode(item), 'utf-8'))
        self.files[spider.name + '-dropped-items.json'].write(bytes('\n', 'utf-8'))
    
    def spider_error(self, failure, response, spider):
        self.files[spider.name + '-errors.log'].write(bytes(response.url + '\n', 'utf-8'))
        self.files[spider.name + '-errors.log'].write(bytes(failure.getTraceback(), 'utf-8'))
    
    def spider_closed(self, spider, reason):
        jira_id = spider.custom_settings['JIRA_ID']
        self.finish_time = datetime.datetime.now()
        self.used_time = self.finish_time - self.start_time
        files = []
        for name, compressed in self.files.items():
            compressed.fileobj.write(compressed.compress.flush())
            gzip.write32u(compressed.fileobj, compressed.crc)
            gzip.write32u(compressed.fileobj, compressed.size & 0xffffffff)
            files.append((name + compressed.extension, compressed.mimetype, compressed))
        try:
            size = self.files[spider.name + '-items.json'].size
        except KeyError:
            size = 0
        stats = spider.crawler.stats.get_stats()
        dqr_status = stats.pop('columns_stats_information', {})
        if ('downloader/exception_count' in stats and stats['downloader/exception_count'] > 0) \
            or ('log_count/ERROR' in stats and stats['log_count/ERROR'] > 0):
            subject = "failed"
        else:
            subject = "succeed"
        mailsender = MailSender.from_settings(self.settings)
        mailsender.send(to=self.settings.getlist('JOB_NOTIFICATION_EMAILS'),
                        subject='JIRA ID:{}  job ends with {}'.format(jira_id, subject),
                        # attachs=files,
                        body=Environment().from_string(config.HTML).render({'stats':stats,
                                                                            'dqr_status':dqr_status,
                                                                            'jira':jira_id,
                                                                            'size':format_size(size)}),
                        mimetype='text/html', _callback=self._catch_mail_sent)
    
    def _catch_mail_sent(self, **kwargs):
        logger.info("Mail Send Notification")
       
