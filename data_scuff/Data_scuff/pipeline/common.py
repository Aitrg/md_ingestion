'''
Created on 02-Jun-2018

@author: srinivasan
'''
import dbm
import json
import logging
import os
import time

from scrapy import  signals
from scrapy.exceptions import DropItem, NotConfigured
from scrapy.utils.project import data_path
from scrapy.utils.serialize import ScrapyJSONEncoder

logger = logging.getLogger(__name__)


class DropDuplicateItemPipeline:
    
    stats_name = 'dropdeduppipeline'

    def __init__(self, dir__, stats, settings):
        self.dir = dir__
        self.stats = stats
        self.dbpath = None
        self.unique_fields = settings.getlist('UNIQUE_FIELDS', None)
    
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        if not settings.getbool('DROP_DUPLICATES_ENABLED'):
            raise NotConfigured
        dir__ = data_path(settings.get('CACHE_DIR', 'cache'))
        dedup = cls(dir__, crawler.stats, settings)
        crawler.signals.connect(dedup.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(dedup.spider_closed, signal=signals.spider_closed)
        return dedup
    
    def spider_opened(self, spider):
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
        self.dbpath = os.path.join(self.dir,
                                   '{}_{}.db'.format(spider.name,
                                                      int(time.time())))
        try:
            self.db = dbm.open(self.dbpath, 'c') 
        except Exception as e:
            logger.warning("Failed to Create database-{} due to exception {}"
                           .format(self.dbpath, e))
            if os.path.exists(self.dbpath):
                os.remove(self.dbpath)
    
    def spider_closed(self, spider):
        if self.db:
            self.db.close()
        if os.path.exists(self.dbpath):
                os.remove(self.dbpath)

    def process_item(self, item, spider):
        import hashlib
        val = dict(item)
        if self.unique_fields:
            val = {k:v for k, v in val.items() if k in self.unique_fields}
        else:
            for not_re in ['sourceName', 'ingestion_timestamp', 'url']:
                if not_re in val:
                    val.pop(not_re)
        value = hashlib.md5(json.dumps(val, sort_keys=True).encode('utf-8')).hexdigest()
        if self.db.get(value):
            self.stats.inc_value('{}/skipped'.format(self.stats_name), spider=spider)
            raise DropItem('Duplicate Item Found: %s' % item)
        else:
            self.db[value] = b'new'
        return item


class DropDuplicateUrlPipeline:

    def __init__(self):
        self.urls_seen = set()

    def process_item(self, item, spider):
        if item['url']:
            import hashlib
            value = hashlib.md5(item['url']).hexdigest()
            if value in self.urls_seen:
                raise DropItem("Duplicate item found: %s" % item['url'])
            else:
                self.urls_seen.add(value)
        return item


class FilterWordsPipeline:
    
    def __init__(self, settings, stats):
        self.stats = stats
        self.settings = settings
        self.words_to_filter = settings.getlist('FILTER_WORDS', '')
        
    def process_item(self, item, spider):
        dict(item)
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler.stats)

    
class KafkaPipeline:
    
    stats_name = 'KafkaPipeline'
    
    def __init__(self, settings, stats):
        from pykafka.client import KafkaClient
        self.stats = stats
        self.settings = settings
        self.encoder = ScrapyJSONEncoder()
        self.kafka = KafkaClient(hosts=self.settings.get('KAFKA_HOST')
                                  +":" + str(self.settings.get('KAFKA_PORT')))
        self.producer = self.kafka.topics[self.settings['KAFKA_TOPIC']
                                        ].get_sync_producer(
                                            min_queued_messages=1)
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler.stats)
    
    def process_item(self, item, spider):
        itemval = item if isinstance(item, dict) else dict(item)
        itemval['spider'] = spider.name
        msg = self.encoder.encode(itemval)
        self.producer.produce(msg)
        self.stats.inc_value('{}/produce'.format(self.stats_name), spider=spider)
        logger.msg("Item sent to Kafka", logger.DEBUG)
        return itemval
