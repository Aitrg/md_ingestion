# -*- coding: utf-8 -*-
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
'''
Created on 22-May-2018

@author: srinivasan
'''

import csv
import io
import os

from scrapy import signals
from scrapy.conf import settings
from scrapy.exporters import CsvItemExporter, XmlItemExporter, \
    JsonLinesItemExporter
import six

from Data_scuff.utils.fileutils import FileUtils
from Data_scuff.utils.utils import Utils


class CustomCsvItemExporter(CsvItemExporter):
    
    def __init__(self, file, include_headers_line=True, join_multivalued=',', **kwargs):
        self.file = file
        self.null_header = kwargs.pop('null_header', None)
        self._configure(kwargs, dont_fail=True)
        if not self.encoding:
            self.encoding = 'utf-8'
        self.include_headers_line = include_headers_line
        self.stream = io.TextIOWrapper(
            file,
            line_buffering=False,
            write_through=True,
            encoding=self.encoding,
            newline=''  
        ) if six.PY3 else file
        self.csv_writer = csv.writer(self.stream, **kwargs)
        self._headers_not_written = True
        self._join_multivalued = join_multivalued
    
    def _write_headers_and_set_fields_to_export(self, item):
        if self.include_headers_line:
            if not self.fields_to_export:
                if isinstance(item, dict):
                    self.fields_to_export = list(item.keys())
                else:
                    self.fields_to_export = list(item.fields.keys())
            row = list(self._build_row(self.fields_to_export))
            if self.null_header:
                self.csv_writer.writerow(['' if r in self.null_header else r  for r in row])
            else:
                self.csv_writer.writerow(row)


class MixinCreateFolder:
    
    def createFolder(self, outpath):
        from pathlib import Path
        p = Path(outpath).parent
        if not p.exists():
            p.mkdir(parents=True)


class ChunkWiseCsvFileWriter(MixinCreateFolder):
    
    def __init__(self, settings, file_name, delimiter,
                 fields_to_export, null_header, customHeader=False, topHeader=None):
        self.settings = settings
        self.file_name = file_name
        self.delimiter = delimiter
        self.fields_to_export = fields_to_export
        self.customHeader = customHeader
        self.chunk_folder = "chunk_{}".format(Utils.getingestion_timestamp())
        self.topHeader = topHeader
        self.null_header = null_header
        self.items = []
        self.chunk_number = 0
        self.job_dir = settings.get('JOB_DIR_PAUSE_RESUME')
        self.appendMode = False
    
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        output_file_name = settings.get('FILE_NAME')
        delimiter = settings.get('CSV_DELIMITER', ',')
        fields_to_export = settings.get('FIELDS_TO_EXPORT', [])
        null_header = settings.get('NULL_HEADERS', [])
        top_header = settings.get('TOP_HEADER', {})
        if top_header:
            pipeline = cls(settings, output_file_name, delimiter, fields_to_export, null_header, True, top_header)
        else:
            pipeline = cls(settings, output_file_name, delimiter, fields_to_export, null_header)
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def process_item(self, item, spider):
        import copy 
        self.items.append(copy.deepcopy(item))
        if len(self.items) > 5:
            if os.path.getsize(self.exporter.file.name) > \
                self.settings.getint('CHUNK_FILE_SIZE', 512000):
                self.exporter.finish_exporting()
                self.exporter.file.close()
                # create the new file
                self.chunk_number += 1
                self.__createChunkFile(spider)
            # partially write con
            self.__chunkWrite(spider)
            self.items = []
        return item
    
    def spider_opened(self, spider):
        self.__checkAppendMode(spider.name)
        self.__createChunkFile(spider)
    
    def __checkAppendMode(self, name):
        if self.job_dir:
            seenPath = os.path.join(self.job_dir, name, 'requests.seen')
            if FileUtils.isExist(seenPath) and os.stat(seenPath).st_size > 10:
                self.appendMode = True
    
    def spider_closed(self, spider):
        if self.exporter:
            self.__chunkWrite(spider)
            self.exporter.finish_exporting()
            self.exporter.file.close()
    
    def __chunkWrite(self, spider):
        if not self.items:
            return
        for item in self.items:
            self.exporter.export_item(item)
    
    def __createChunkFile(self, spider):
        remove_spec = lambda x: ''.join(e for e in x if e.isalnum())
        if self.file_name:
            l = list(os.path.splitext(self.file_name))
            if self.chunk_number != 0:
                l.insert(1, "_file_{}".format(str(self.chunk_number)))
            if hasattr(spider, 'start') and  spider.start:
                if self.chunk_number != 0:
                    l.insert(2, "_{}_{}".format(remove_spec(spider.start), remove_spec(spider.end)))
                else:
                    l.insert(1, "_{}_{}".format(remove_spec(spider.start), remove_spec(spider.end)))
            file_name = "".join(l)
        if self.appendMode:
            outpath = os.path.join(self.settings.get('STORAGE_DIR'), self.settings.get('JIRA_ID'),
                                      'resume_{}'.format(Utils.getingestion_timestamp()),
                                      file_name if self.file_name else '{}_file_{}.csv'.format(spider.name, str(self.chunk_number)))
        else:
            outpath = os.path.join(self.settings.get('STORAGE_DIR'), self.settings.get('JIRA_ID'),
                                      file_name if self.file_name else '{}_file_{}.csv'.format(spider.name, str(self.chunk_number)))
        self.createFolder(outpath)
        self.file = open(outpath, 'w+b')
        kwargs = {'delimiter': self.delimiter}
        if self.fields_to_export :
            kwargs['fields_to_export'] = self.fields_to_export
        if self.null_header:
            kwargs['null_header'] = self.null_header
        self.exporter = CustomCsvItemExporter(self.file, **kwargs)
        self.exporter.start_exporting()
        if self.customHeader:
            fields = self.fields_to_export
            values = [self.topHeader.get(i) for i in fields]
            self.exporter.csv_writer.writerow(values)


class CsvFileWriter(MixinCreateFolder):
    
    def __init__(self, settings, file_name, delimiter,
                 fields_to_export, null_header, customHeader=False, topHeader=None):
        self.file_name = file_name
        self.delimiter = delimiter
        self.fields_to_export = fields_to_export
        self.customHeader = customHeader
        self.topHeader = topHeader
        self.null_header = null_header
        self._settings = settings
        self.job_dir = settings.get('JOB_DIR_PAUSE_RESUME')
        self.appendMode = False
        
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        output_file_name = settings.get('FILE_NAME')
        delimiter = settings.get('CSV_DELIMITER', ',')
        fields_to_export = settings.get('FIELDS_TO_EXPORT', [])
        null_header = settings.get('NULL_HEADERS', [])
        top_header = settings.get('TOP_HEADER', {})
        if top_header:
            pipeline = cls(settings, output_file_name, delimiter, fields_to_export, null_header, True, top_header)
        else:
            pipeline = cls(settings, output_file_name, delimiter, fields_to_export, null_header)
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        if  hasattr(spider, 'start') and  spider.start:
            l = list(os.path.splitext(self.file_name))
            remove_spec = lambda x: ''.join(e for e in x if e.isalnum())
            l.insert(1, "_{}_{}".format(remove_spec(spider.start), remove_spec(spider.end)))
            self.file_name = "".join(l)
        if self.appendMode:
            outpath = os.path.join(settings.get('STORAGE_DIR'), self._settings.get('JIRA_ID'),
                                   'resume_{}'.format(Utils.getingestion_timestamp()), self.file_name 
                                      if self.file_name  else '%s_items.csv' % spider.name)
        else:     
            outpath = os.path.join(settings.get('STORAGE_DIR'), self._settings.get('JIRA_ID'), self.file_name 
                                      if self.file_name  else '%s_items.csv' % spider.name)
        self.createFolder(outpath)
        self.file = open(outpath, 'w+b')
        kwargs = {'delimiter': self.delimiter}
        if self.fields_to_export :
            kwargs['fields_to_export'] = self.fields_to_export
        if self.null_header:
            kwargs['null_header'] = self.null_header
        self.exporter = CustomCsvItemExporter(self.file, **kwargs)
        self.exporter.start_exporting()
    
    def __checkAppendMode(self, name):
        if self.job_dir:
            seenPath = os.path.join(self.job_dir, name, 'requests.seen')
            if FileUtils.isExist(seenPath) and os.stat(seenPath).st_size > 10:
                self.appendMode = True
        
    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        outpath = self.file.name
        self.file.close()
        if outpath and os.stat(outpath).st_size <= 100:
            os.remove(outpath)

    def process_item(self, item, spider):
        if self.customHeader:
            self.customHeader = False
            fields = self.fields_to_export if self.fields_to_export else item.fields
            values = [self.topHeader.get(i) for i in fields]
            self.exporter.csv_writer.writerow(values)
        self.exporter.export_item(item)
        return item


class JsonFileWriter(MixinCreateFolder):
    
    def __init__(self, settings):
        self._settings = settings

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        outpath = os.path.join(settings.get('STORAGE_DIR'), self._settings.get('JIRA_ID'),
                                      self.file_name if self.file_name 
                         else '%s_items.json' % spider.name)
        self.createFolder(outpath)
        self.file = open(outpath, 'w+b')
        self.exporter = JsonLinesItemExporter(self.file)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class XmlFileWriter(MixinCreateFolder):
    
    def __init__(self, settings):
        self._settings = settings

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        outpath = os.path.join(settings.get('STORAGE_DIR'), self._settings.get('JIRA_ID'),
                                      self.file_name if self.file_name 
                         else '%s_items.xml' % spider.name)
        self.createFolder(outpath)
        self.file = open(outpath, 'w+b')
        self.exporter = XmlItemExporter(self.file)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item
