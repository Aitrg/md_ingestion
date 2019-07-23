# -*- coding: utf-8 -*-
'''
Created on 09-Aug-2018

@author: srinivasan
'''

from __future__ import print_function, unicode_literals

from datetime import datetime
import hashlib
import itertools
import logging
import os
import pprint
from scrapy import signals
from scrapy.exceptions import NotConfigured
from statistics import mean, median, stdev
import yaml

logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)


class ColumnStatsExtension:
    
    stats_name = 'columns_stats_information'

    def __init__(self, crawler, skip_columns=[]):
        self.stats = crawler.stats
        self.items_scraped = 0
        self.column_counts = {}
        self.skip_columns = skip_columns
    
    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool('COLUMNSTATS_ENABLED'):
            raise NotConfigured
        ext = cls(crawler, crawler.settings.getlist('COLUMNSTATS_SKIP_COLUMNS', []))
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        return ext
    
    def spider_opened(self, spider):
        logger.info("spider started to collecting data for DQR Reports %s", spider.name)

    def spider_closed(self, spider):
        logger.info("closed spider %s", spider.name)
        report_data = self.__build_Column_summary()
        message = 'Field stats:\n{}'.format(pprint.pformat(report_data))
        self.stats.set_value(self.stats_name, report_data)
        logger.info(message)

    def item_scraped(self, item, spider):
        self.items_scraped += 1
        self.__computeItem(item)
    
    def __computeItem(self, item):
        for name, value in item.items():
            if name not in self.skip_columns:
                if name not in self.column_counts:
                    self.column_counts[name] = 0
                if self.__isvalidValue(value):
                    self.column_counts[name] += 1
    
    def __build_Column_summary(self, field_counts=None, columns_summary=None):
        if field_counts is None:
            column_counts = self.column_counts
            columns_summary = {}
        for name, value in column_counts.items():
            columns_summary[name] = {"count":value,
                                     "coverage":"{}%".format(int(value) * 100 / self.items_scraped)}
        return columns_summary
    
    def __isvalidValue(self, value):
        if not value:
            return False
        else:
            val = ''.join(e for e in value[0] if e.isalnum())
            if val == '':
                return False
            else:
                return True


class SaveStatisticsinJobDirectory:
    
    def __init__(self, crawler):
        if not crawler.settings.get('JOBDIR', None):
            raise NotConfigured()
        self.path = os.path.join(crawler.settings.get('JOBDIR', None), 'stats.json')
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        instance = cls(crawler)
        crawler.signals.connect(instance.spider_closed, signal=signals.spider_closed)
        return instance
    
    def spider_closed(self, spider, reason):
        if not hasattr(self, 'stats'):
            return;
        dumppy_stats = self.stats.get_stats()
        scraped_num = dumppy_stats.get('item_scraped_count', 0)
        if scraped_num > 0 and hasattr(spider, 'statistics'):
            info = {}
            info['name'] = spider.name
            urls = set(i['spider_url'] for i in spider.statistics)
            info['urls'] = yaml.safe_dump(urls)
            info['hash'] = hashlib.sha256(info['urls']).hexdigest()
            info['finish_reason'] = dumppy_stats.get('finish_reason')
            info['start_time'] = dumppy_stats.get('start_time')
            info['finish_time'] = dumppy_stats.get('finish_time')
            used_time = info['finish_time'] - info['start_time']
            info['used_time'] = used_time.total_seconds()
            info['scraped'] = scraped_num
            info['dropped'] = dumppy_stats.get('item_dropped_count', 0)
            info['errors'] = dumppy_stats.get('log_count/ERROR', 0)
            import json
            with open(self.path, 'w') as fp:
                json.dump(info, fp, indent=4)


class SlotStats:

    def __init__(self, crawler):
        crawler.signals.connect(self.response_downloaded, signal=signals.response_downloaded)
        crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)
        self.stats = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def response_downloaded(self, response, request, spider):
        self.stats.append({
            'time': datetime.now(),
            'slot': request.meta['download_slot'],
            'url': request.url,
        })

    def spider_closed(self, spider, reason):
        lines = []
        keyfunc = lambda s: s['slot']
        sorted_stats = list(sorted(self.stats, key=keyfunc))
        for slot, stats in itertools.groupby(sorted_stats, keyfunc):
            prev_time = None
            for stat in stats:
                stat['delay'] = (stat['time'] - prev_time).total_seconds() if prev_time else None
                prev_time = stat['time']
                lines.append('{slot}\t{delay}\t{time}\t{url}'.format(**stat))

        logger.debug('Slot details:\n%s', '\n'.join(lines))

        lines = []
        for slot, stats in itertools.groupby(sorted_stats, keyfunc):
            delays = [stat['delay'] for stat in stats if stat['delay'] is not None]
            if len(delays) == 0:
                lines.append('{0}\tcount:{1}'.format(slot, len(delays)))
                continue

            lines.append('{0}\tcount:{1}\tmin:{2}\tmax:{3}\tmean:{4}\tmedian:{5}\tstdev:{6}'.format(
                slot, len(delays), min(delays), max(delays), mean(delays), median(delays),
                stdev(delays) if len(delays) >= 2 else None))

        logger.info('Slot delay stats:\n%s', '\n'.join(lines))

